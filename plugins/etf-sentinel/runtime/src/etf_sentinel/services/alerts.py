from __future__ import annotations

import hashlib
import hmac
import json
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from etf_sentinel.audit import append_audit
from etf_sentinel.config import Settings
from etf_sentinel.enums import DISCLAIMER, AlertSeverity, AlertStatus, DataMode, SignalState
from etf_sentinel.models import Alert, DataSnapshot, ScheduledEvent, Signal
from etf_sentinel.providers.base import (
    LicenseGateError,
    ProviderSchemaError,
    authorize_provider,
    normalize_url,
)
from etf_sentinel.services.fact_integrity import verify_scheduled_event_record

CANDIDATE_STATES = {
    SignalState.ENTRY_CANDIDATE.value,
    SignalState.REDUCE_CANDIDATE.value,
    SignalState.EXIT_CANDIDATE.value,
}
ALERT_TIMEZONE = ZoneInfo("Asia/Shanghai")


class AlertIntegrityError(RuntimeError):
    pass


def alert_content_hash(alert: Alert) -> str:
    """Hash immutable alert content while excluding mutable delivery/ack fields."""

    def timestamp(value: datetime) -> str:
        aware = value if value.tzinfo else value.replace(tzinfo=UTC)
        return aware.astimezone(UTC).isoformat()

    payload = {
        "dedupe_key": alert.dedupe_key,
        "alert_type": alert.alert_type,
        "severity": alert.severity,
        "title": alert.title,
        "message": alert.message,
        "provider_code": alert.provider_code,
        "source_object_type": alert.source_object_type,
        "source_object_id": alert.source_object_id,
        "instrument_id": alert.instrument_id,
        "signal_id": alert.signal_id,
        "data_as_of": timestamp(alert.data_as_of),
        "latency_status": alert.latency_status,
        "trigger_reason": alert.trigger_reason,
        "confidence": alert.confidence,
        "source_links": alert.source_links,
        "invalidation_conditions": alert.invalidation_conditions,
        "disclaimer": alert.disclaimer,
        "channel": alert.channel,
        "created_at": timestamp(alert.created_at),
    }
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str).encode()
    ).hexdigest()


def verify_alert_content(alert: Alert) -> bool:
    return isinstance(alert.content_hash, str) and hmac.compare_digest(
        alert.content_hash, alert_content_hash(alert)
    )


def _allow_existing_legacy_alert_or_raise(alert: Alert, *, context: str) -> None:
    """Permit dedupe against explicitly migrated rows without trusting/displaying them."""
    if alert.content_hash is None and alert.source_object_type.startswith("LegacyUnverified"):
        return
    if not verify_alert_content(alert):
        raise AlertIntegrityError(f"{context}内容完整性校验失败，已拒绝幂等重放。")


def create_signal_alerts(
    session: Session,
    signals: list[Signal],
    *,
    settings: Settings,
    now: datetime,
    commit: bool = True,
) -> list[Alert]:
    created: list[Alert] = []
    for signal in signals:
        if signal.state == SignalState.DATA_STALE.value:
            health_alert = create_alert(
                session,
                alert_type="DATA_HEALTH",
                severity=AlertSeverity.WARN,
                title="数据已过期，候选信号已失败关闭",
                trigger_reason="DATA_STALE：系统只发数据健康告警，不生成建仓或退出候选。",
                signal=signal,
                settings=settings,
                now=now,
            )
            if health_alert is not None:
                created.append(health_alert)
            continue
        elif signal.state == SignalState.BLOCKED_BY_RISK.value:
            if any(str(rule).startswith("PORTFOLIO_") for rule in signal.risk_rules_hit):
                continue
            risk_alert = create_alert(
                session,
                alert_type="RISK_GATE",
                severity=AlertSeverity.WARN,
                title="候选状态被风险门禁阻断",
                trigger_reason=", ".join(signal.risk_rules_hit) or "RISK_GATE",
                signal=signal,
                settings=settings,
                now=now,
            )
            if risk_alert is not None:
                created.append(risk_alert)
            continue
        if signal.state in CANDIDATE_STATES and _candidate_state_changed(session, signal):
            state_alert = create_alert(
                session,
                alert_type="SIGNAL_STATE_CHANGE",
                severity=AlertSeverity.INFO,
                title=f"候选状态：{signal.state}",
                trigger_reason=f"综合分数 {signal.composite_score:.3f} 跨越候选阈值。",
                signal=signal,
                settings=settings,
                now=now,
            )
            if state_alert is not None:
                created.append(state_alert)
        if abs(signal.news_score) >= settings.news_risk_reduction_threshold:
            news_alert = create_alert(
                session,
                alert_type="NEWS_EXPOSURE",
                severity=AlertSeverity.WARN,
                title="新闻风险与 ETF 暴露高度相关",
                trigger_reason=(
                    f"新闻分项得分为 {signal.news_score:.3f}；"
                    "该分项进入综合分数时受配置权重上限约束。"
                ),
                signal=signal,
                settings=settings,
                now=now,
            )
            if news_alert is not None:
                created.append(news_alert)
    if commit:
        session.commit()
    else:
        session.flush()
    return created


def create_scheduled_event_alerts(
    session: Session,
    events: list[ScheduledEvent],
    *,
    settings: Settings,
    now: datetime,
    commit: bool = True,
) -> list[Alert]:
    created: list[Alert] = []
    for event in events:
        gate_reasons = scheduled_event_gate_reasons(session, event, now=now)
        if gate_reasons:
            append_audit(
                session,
                event_type="SCHEDULED_EVENT_BLOCKED",
                object_type="ScheduledEvent",
                object_id=event.id,
                details={"rules": gate_reasons},
            )
            continue
        scheduled_at = event.scheduled_at
        if scheduled_at.tzinfo is None:
            scheduled_at = scheduled_at.replace(tzinfo=UTC)
        current = now if now.tzinfo else now.replace(tzinfo=UTC)
        hours_until = (
            scheduled_at.astimezone(UTC) - current.astimezone(UTC)
        ).total_seconds() / 3600
        if not (0 <= hours_until <= event.alert_lead_hours):
            continue
        dedupe_key = hashlib.sha256(
            (
                f"KNOWN_EVENT:{event.provider_code}:{event.event_code}:"
                f"{event.revision}:{scheduled_at.isoformat()}"
            ).encode()
        ).hexdigest()
        existing = session.scalar(select(Alert).where(Alert.dedupe_key == dedupe_key))
        if existing is not None:
            _allow_existing_legacy_alert_or_raise(existing, context="预警")
            created.append(existing)
            continue
        status = _delivery_status(session, settings=settings, now=current)
        alert = Alert(
            dedupe_key=dedupe_key,
            alert_type="KNOWN_EVENT_WINDOW",
            severity=AlertSeverity.INFO.value,
            status=status.value,
            title=f"已知事件风险窗口：{event.name_zh}",
            message=(
                f"计划时间：{scheduled_at.isoformat()}；提前 {hours_until:.1f} 小时提醒；"
                "该预警只针对已经公开的日程，不代表预知突发事件。"
            ),
            provider_code=event.provider_code,
            source_object_type="ScheduledEvent",
            source_object_id=event.id,
            instrument_id=None,
            signal_id=None,
            data_as_of=event.as_of,
            latency_status="SCHEDULED",
            trigger_reason=f"进入配置的 {event.alert_lead_hours} 小时提前风险窗口。",
            confidence=1.0,
            source_links=[event.source_uri],
            invalidation_conditions=["官方取消、延期或更正已公布日程"],
            disclaimer=DISCLAIMER,
            channel="IN_APP",
            delivery_attempts=1 if status == AlertStatus.SENT else 0,
            created_at=current,
            sent_at=current if status == AlertStatus.SENT else None,
        )
        alert.content_hash = alert_content_hash(alert)
        session.add(alert)
        session.flush()
        append_audit(
            session,
            event_type="KNOWN_EVENT_ALERT_RECORDED",
            object_type="ScheduledEvent",
            object_id=event.id,
            details={
                "event_code": event.event_code,
                "scheduled_at": scheduled_at.isoformat(),
                "lead_hours": event.alert_lead_hours,
            },
        )
        created.append(alert)
    if commit:
        session.commit()
    else:
        session.flush()
    return created


def eligible_scheduled_events(
    session: Session, events: list[ScheduledEvent], *, now: datetime
) -> list[ScheduledEvent]:
    return [event for event in events if not scheduled_event_gate_reasons(session, event, now=now)]


def scheduled_event_gate_reasons(
    session: Session, event: ScheduledEvent, *, now: datetime
) -> list[str]:
    reasons: list[str] = []
    if not verify_scheduled_event_record(event):
        reasons.append("EVENT_RECORD_INTEGRITY_FAILED")
    current = now if now.tzinfo else now.replace(tzinfo=UTC)
    if event.status != "SCHEDULED":
        reasons.append("EVENT_NOT_SCHEDULED")
    if not event.is_current:
        reasons.append("EVENT_REVISION_NOT_CURRENT")
    announced_at = event.first_announced_at
    if announced_at.tzinfo is None:
        announced_at = announced_at.replace(tzinfo=UTC)
    scheduled_at = event.scheduled_at
    if scheduled_at.tzinfo is None:
        scheduled_at = scheduled_at.replace(tzinfo=UTC)
    published_at = event.published_at
    first_seen_at = event.first_seen_at
    available_at = event.available_at
    as_of = event.as_of
    event_time = event.event_time
    ingested_at = event.ingested_at
    for field_name, value in {
        "published_at": published_at,
        "first_seen_at": first_seen_at,
        "available_at": available_at,
        "as_of": as_of,
        "event_time": event_time,
        "ingested_at": ingested_at,
    }.items():
        if value.tzinfo is None:
            if field_name == "published_at":
                published_at = value.replace(tzinfo=UTC)
            elif field_name == "first_seen_at":
                first_seen_at = value.replace(tzinfo=UTC)
            elif field_name == "available_at":
                available_at = value.replace(tzinfo=UTC)
            elif field_name == "as_of":
                as_of = value.replace(tzinfo=UTC)
            else:
                if field_name == "event_time":
                    event_time = value.replace(tzinfo=UTC)
                else:
                    ingested_at = value.replace(tzinfo=UTC)
    current_utc = current.astimezone(UTC)
    if announced_at.astimezone(UTC) > current_utc:
        reasons.append("NOT_YET_PUBLICLY_ANNOUNCED")
    if published_at.astimezone(UTC) > current_utc:
        reasons.append("PUBLISHED_IN_FUTURE")
    if first_seen_at.astimezone(UTC) > current_utc:
        reasons.append("NOT_YET_FIRST_SEEN")
    if available_at.astimezone(UTC) > current_utc:
        reasons.append("NOT_YET_AVAILABLE")
    if as_of.astimezone(UTC) > current_utc:
        reasons.append("AS_OF_IN_FUTURE")
    if ingested_at.astimezone(UTC) > current_utc:
        reasons.append("INGESTED_IN_FUTURE")
    if announced_at.astimezone(UTC) > scheduled_at.astimezone(UTC):
        reasons.append("ANNOUNCEMENT_AFTER_EVENT")
    if announced_at.astimezone(UTC) > first_seen_at.astimezone(UTC):
        reasons.append("FIRST_SEEN_BEFORE_ANNOUNCEMENT")
    if published_at.astimezone(UTC) > first_seen_at.astimezone(UTC):
        reasons.append("FIRST_SEEN_BEFORE_PUBLISHED")
    if first_seen_at.astimezone(UTC) > available_at.astimezone(UTC):
        reasons.append("AVAILABLE_BEFORE_FIRST_SEEN")
    if as_of.astimezone(UTC) > available_at.astimezone(UTC):
        reasons.append("AVAILABLE_BEFORE_AS_OF")
    if available_at.astimezone(UTC) > ingested_at.astimezone(UTC):
        reasons.append("INGESTED_BEFORE_AVAILABLE")
    if event_time.astimezone(UTC) != scheduled_at.astimezone(UTC):
        reasons.append("EVENT_TIME_MISMATCH")
    if scheduled_at.astimezone(UTC) < current.astimezone(UTC):
        reasons.append("EVENT_ALREADY_PASSED")
    if event.data_mode not in {item.value for item in DataMode}:
        reasons.append("INVALID_DATA_MODE")
    if not event.license_scope or "UNCLEAR" in event.license_scope.upper():
        reasons.append("LICENSE_SCOPE_UNCLEAR")
    blocked_quality_flags = {
        "HASH_MISMATCH",
        "LICENSE_UNCLEAR",
        "MIGRATED_LINEAGE_REVIEW_REQUIRED",
        "SCHEMA_UNVERIFIED",
        "TIME_VALIDATION_FAILED",
    }
    if blocked_quality_flags.intersection(set(event.quality_flags or [])):
        reasons.append("EVENT_QUALITY_BLOCKED")
    if len(event.raw_payload_hash) != 64 or any(
        character not in "0123456789abcdef" for character in event.raw_payload_hash.lower()
    ):
        reasons.append("RAW_PAYLOAD_HASH_INVALID")
    try:
        normalize_url(event.source_uri)
    except ProviderSchemaError:
        reasons.append("SOURCE_URL_INVALID")
    try:
        authorize_provider(
            session,
            event.provider_code,
            purposes={"display", "algorithm", "cache"},
            at=datetime.now(UTC),
        )
        for region in event.regions or []:
            authorize_provider(
                session,
                event.provider_code,
                purposes={"display", "algorithm", "cache"},
                at=datetime.now(UTC),
                region=str(region),
            )
    except LicenseGateError:
        reasons.append("PROVIDER_LICENSE_BLOCKED")
    return list(dict.fromkeys(reasons))


def create_system_alert(
    session: Session,
    *,
    dedupe_scope: str,
    severity: AlertSeverity,
    title: str,
    trigger_reason: str,
    data_as_of: datetime,
    settings: Settings,
    source_links: list[str] | None = None,
    invalidation_conditions: list[str] | None = None,
    now: datetime | None = None,
    alert_type: str = "SYSTEM_HEALTH",
    commit: bool = True,
) -> Alert:
    """Record an internal operational alert without exposing exception details."""
    current = now or datetime.now(UTC)
    dedupe_prefix = "SYSTEM" if alert_type == "SYSTEM_HEALTH" else alert_type
    dedupe_key = hashlib.sha256(f"{dedupe_prefix}:{dedupe_scope}".encode()).hexdigest()
    existing = session.scalar(select(Alert).where(Alert.dedupe_key == dedupe_key))
    if existing is not None:
        _allow_existing_legacy_alert_or_raise(existing, context="系统预警")
        return existing
    status = _delivery_status(session, settings=settings, now=current)
    alert = Alert(
        dedupe_key=dedupe_key,
        alert_type=alert_type,
        severity=severity.value,
        status=status.value,
        title=title,
        message=(
            f"数据截止：{data_as_of.isoformat()}；延迟状态：UNAVAILABLE；"
            f"置信度：100.00%；触发原因：{trigger_reason}"
        ),
        provider_code=None,
        source_object_type="System",
        source_object_id=None,
        instrument_id=None,
        signal_id=None,
        data_as_of=data_as_of,
        latency_status="UNAVAILABLE",
        trigger_reason=trigger_reason,
        confidence=1.0,
        source_links=source_links or [],
        invalidation_conditions=invalidation_conditions
        or ["任务经授权人员处置后重试成功，且数据健康门禁重新通过"],
        disclaimer=DISCLAIMER,
        channel="IN_APP",
        delivery_attempts=1 if status == AlertStatus.SENT else 0,
        created_at=current,
        sent_at=current if status == AlertStatus.SENT else None,
    )
    alert.content_hash = alert_content_hash(alert)
    session.add(alert)
    session.flush()
    append_audit(
        session,
        event_type="SYSTEM_ALERT_RECORDED",
        object_type="Alert",
        object_id=alert.id,
        details={
            "severity": severity.value,
            "status": status.value,
            "channel": "IN_APP",
            "dedupe_key": dedupe_key,
        },
    )
    if commit:
        session.commit()
    else:
        session.flush()
    return alert


def create_alert(
    session: Session,
    *,
    alert_type: str,
    severity: AlertSeverity,
    title: str,
    trigger_reason: str,
    signal: Signal,
    settings: Settings,
    now: datetime,
) -> Alert | None:
    raw_dedupe = f"{alert_type}:{signal.idempotency_key}:{signal.state}"
    dedupe_key = hashlib.sha256(raw_dedupe.encode()).hexdigest()
    existing = session.scalar(select(Alert).where(Alert.dedupe_key == dedupe_key))
    if existing is not None:
        _allow_existing_legacy_alert_or_raise(existing, context="预警")
        return existing
    if settings.alert_cooldown_minutes > 0:
        cooldown_start = now - timedelta(minutes=settings.alert_cooldown_minutes)
        recent = session.scalar(
            select(Alert)
            .where(
                Alert.alert_type == alert_type,
                Alert.instrument_id == signal.instrument_id,
                Alert.channel == "IN_APP",
                Alert.created_at >= cooldown_start,
                Alert.status.in_(
                    [
                        AlertStatus.SENT.value,
                        AlertStatus.PENDING.value,
                        AlertStatus.ACKNOWLEDGED.value,
                    ]
                ),
            )
            .order_by(Alert.created_at.desc())
            .limit(1)
        )
        if recent is not None:
            _allow_existing_legacy_alert_or_raise(recent, context="冷却期预警")
            return recent
    status = _delivery_status(session, settings=settings, now=now)
    snapshot = session.get(DataSnapshot, signal.data_snapshot_id)
    provider_code = snapshot.provider_code if snapshot is not None else None
    alert = Alert(
        dedupe_key=dedupe_key,
        alert_type=alert_type,
        severity=severity.value,
        status=status.value,
        title=title,
        message=(
            f"数据截止：{signal.data_as_of.isoformat()}；延迟状态：{signal.latency_status}；"
            f"置信度：{signal.confidence:.2%}；触发原因：{trigger_reason}"
        ),
        provider_code=provider_code,
        source_object_type="Signal",
        source_object_id=signal.id,
        instrument_id=signal.instrument_id,
        signal_id=signal.id,
        data_as_of=signal.data_as_of,
        latency_status=signal.latency_status,
        trigger_reason=trigger_reason,
        confidence=signal.confidence,
        source_links=signal.source_links,
        invalidation_conditions=signal.invalidation_conditions,
        disclaimer=DISCLAIMER,
        channel="IN_APP",
        delivery_attempts=1 if status == AlertStatus.SENT else 0,
        created_at=now,
        sent_at=now if status == AlertStatus.SENT else None,
    )
    alert.content_hash = alert_content_hash(alert)
    session.add(alert)
    session.flush()
    append_audit(
        session,
        event_type="ALERT_RECORDED",
        object_type="Alert",
        object_id=alert.id,
        details={
            "type": alert_type,
            "severity": severity.value,
            "status": status.value,
            "channel": "IN_APP",
            "dedupe_key": dedupe_key,
        },
    )
    return alert


def acknowledge_alert(
    session: Session, alert_id: str, actor: str = "authorized_internal_user"
) -> Alert:
    alert = session.get(Alert, alert_id)
    if alert is None:
        raise KeyError("预警不存在。")
    if not verify_alert_content(alert):
        raise AlertIntegrityError("预警内容完整性校验失败，已拒绝确认。")
    if alert.acknowledged_at is None:
        previous_status = alert.status
        alert.status = AlertStatus.ACKNOWLEDGED.value
        alert.acknowledged_at = datetime.now(UTC)
        alert.acknowledged_by = actor
        append_audit(
            session,
            event_type="ALERT_ACKNOWLEDGED",
            object_type="Alert",
            object_id=alert.id,
            actor=actor,
            details={"previous_status": previous_status},
        )
        session.commit()
    return alert


def _in_quiet_hours(now: datetime, start_hour: int, end_hour: int) -> bool:
    aware = now if now.tzinfo else now.replace(tzinfo=UTC)
    hour = aware.astimezone(ALERT_TIMEZONE).hour
    if start_hour == end_hour:
        return False
    if start_hour < end_hour:
        return start_hour <= hour < end_hour
    return hour >= start_hour or hour < end_hour


def _delivery_status(session: Session, *, settings: Settings, now: datetime) -> AlertStatus:
    aware = now if now.tzinfo else now.replace(tzinfo=UTC)
    local_now = aware.astimezone(ALERT_TIMEZONE)
    day_start = local_now.replace(hour=0, minute=0, second=0, microsecond=0).astimezone(UTC)
    # Acknowledgement mutates status but does not undo an actual delivery.  Count
    # the immutable delivery timestamp so acknowledging alerts cannot reset the
    # daily cap.
    sent_today = (
        session.scalar(
            select(func.count(Alert.id)).where(
                Alert.sent_at.is_not(None),
                Alert.sent_at >= day_start,
            )
        )
        or 0
    )
    if sent_today >= settings.alert_daily_limit:
        return AlertStatus.SUPPRESSED
    if _in_quiet_hours(now, settings.quiet_hours_start, settings.quiet_hours_end):
        return AlertStatus.PENDING
    return AlertStatus.SENT


def _candidate_state_changed(session: Session, signal: Signal) -> bool:
    previous = session.scalar(
        select(Signal)
        .where(
            Signal.instrument_id == signal.instrument_id,
            Signal.id != signal.id,
            Signal.recorded_at <= signal.recorded_at,
        )
        .order_by(Signal.recorded_at.desc())
        .limit(1)
    )
    return previous is None or previous.state != signal.state
