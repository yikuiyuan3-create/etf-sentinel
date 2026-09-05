from __future__ import annotations

from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import func, select

from etf_sentinel.enums import DISCLAIMER, AlertSeverity, AlertStatus, DataMode
from etf_sentinel.main import _alert_display_view
from etf_sentinel.models import Alert, AuditLog, ScheduledEvent
from etf_sentinel.services.alerts import (
    AlertIntegrityError,
    _in_quiet_hours,
    acknowledge_alert,
    create_scheduled_event_alerts,
    create_system_alert,
    scheduled_event_gate_reasons,
    verify_alert_content,
)
from etf_sentinel.services.fact_integrity import (
    scheduled_event_record_hash,
    verify_scheduled_event_record,
)
from etf_sentinel.services.ingestion import register_default_providers


def _scheduled_event(
    now: datetime,
    *,
    code: str,
    status: str = "SCHEDULED",
    first_announced_at: datetime | None = None,
    first_seen_at: datetime | None = None,
    available_at: datetime | None = None,
) -> ScheduledEvent:
    scheduled_at = now + timedelta(hours=12)
    published_at = now - timedelta(days=2)
    announced = first_announced_at or published_at
    seen = first_seen_at or max(published_at, announced)
    available = available_at or seen
    event = ScheduledEvent(
        event_code=code,
        name_zh="已公布日期的测试事件",
        event_type="MACRO_RELEASE",
        scheduled_at=scheduled_at,
        event_time=scheduled_at,
        published_at=published_at,
        first_announced_at=announced,
        first_seen_at=seen,
        available_at=available,
        as_of=published_at,
        ingested_at=max(now, available),
        revision="calendar-test-v1",
        timezone="UTC",
        currency=None,
        latency_class="FIXED_TEST",
        alert_lead_hours=48,
        regions=["中国"],
        asset_classes=["股票"],
        source_uri=f"https://example.invalid/calendar/{code.lower()}",
        provider_code="demo_fixture",
        data_mode=DataMode.DEMO_FIXTURE.value,
        license_scope="INTERNAL_TEST_AND_DEMONSTRATION_ONLY",
        quality_flags=[],
        raw_payload_hash="a" * 64,
        status=status,
        is_current=True,
    )
    event.record_hash = scheduled_event_record_hash(event)
    return event


@pytest.mark.parametrize(
    ("hour", "expected"),
    [(21, False), (22, True), (23, True), (0, True), (6, True), (7, False)],
)
def test_quiet_hours_wrap_midnight(hour: int, expected: bool) -> None:
    now = datetime(2025, 1, 2, hour, tzinfo=ZoneInfo("Asia/Shanghai"))

    assert _in_quiet_hours(now, start_hour=22, end_hour=7) is expected


def test_quiet_hours_converts_utc_to_shanghai_business_clock() -> None:
    # 14:30 UTC is 22:30 in Shanghai and must enter the configured quiet period.
    assert _in_quiet_hours(datetime(2025, 1, 2, 14, 30, tzinfo=UTC), start_hour=22, end_hour=7)


def test_system_alert_is_complete_and_deduplicated_without_repeat_delivery(
    db_session, test_settings
) -> None:
    now = datetime(2025, 1, 2, 2, 0, tzinfo=UTC)  # 10:00 Asia/Shanghai

    first = create_system_alert(
        db_session,
        dedupe_scope="pipeline:preflight:test",
        severity=AlertSeverity.CRITICAL,
        title="数据处理失败",
        trigger_reason="PIPELINE_PREFLIGHT_FAILED",
        data_as_of=now,
        settings=test_settings,
        source_links=["internal://pipeline-health"],
        now=now,
    )
    repeated = create_system_alert(
        db_session,
        dedupe_scope="pipeline:preflight:test",
        severity=AlertSeverity.CRITICAL,
        title="不应覆盖的标题",
        trigger_reason="SHOULD_NOT_DUPLICATE",
        data_as_of=now,
        settings=test_settings,
        now=now + timedelta(minutes=1),
    )

    assert repeated.id == first.id
    assert first.alert_type == "SYSTEM_HEALTH"
    assert first.severity == AlertSeverity.CRITICAL.value
    assert first.status == AlertStatus.SENT.value
    assert first.channel == "IN_APP"
    assert first.provider_code is None
    assert first.source_object_type == "System"
    assert first.source_object_id is None
    assert verify_alert_content(first)
    assert first.data_as_of == now
    assert first.latency_status == "UNAVAILABLE"
    assert first.confidence == pytest.approx(1.0)
    assert first.trigger_reason == "PIPELINE_PREFLIGHT_FAILED"
    assert first.source_links == ["internal://pipeline-health"]
    assert first.invalidation_conditions
    assert first.disclaimer == DISCLAIMER
    assert db_session.scalar(select(func.count(Alert.id))) == 1


def test_acknowledging_sent_alert_does_not_reset_daily_delivery_cap(
    db_session, test_settings
) -> None:
    now = datetime(2025, 1, 2, 2, 0, tzinfo=UTC)  # 10:00 Asia/Shanghai
    limited = test_settings.model_copy(update={"alert_daily_limit": 1})
    delivered = create_system_alert(
        db_session,
        dedupe_scope="delivered-before-ack",
        severity=AlertSeverity.WARN,
        title="首条已发送告警",
        trigger_reason="TEST_FIRST_DELIVERY",
        data_as_of=now,
        settings=limited,
        now=now,
    )
    assert delivered.status == AlertStatus.SENT.value
    assert delivered.sent_at == now

    acknowledged = acknowledge_alert(db_session, delivered.id, actor="test-reviewer")
    assert acknowledged.status == AlertStatus.ACKNOWLEDGED.value
    audit = db_session.scalar(
        select(AuditLog).where(
            AuditLog.event_type == "ALERT_ACKNOWLEDGED",
            AuditLog.object_id == delivered.id,
        )
    )
    assert audit is not None
    assert audit.details["previous_status"] == AlertStatus.SENT.value

    second = create_system_alert(
        db_session,
        dedupe_scope="different-alert-after-ack",
        severity=AlertSeverity.WARN,
        title="第二条不同告警",
        trigger_reason="TEST_SECOND_DELIVERY",
        data_as_of=now,
        settings=limited,
        now=now + timedelta(minutes=1),
    )
    assert second.id != delivered.id
    assert second.status == AlertStatus.SUPPRESSED.value
    assert second.sent_at is None
    assert second.delivery_attempts == 0


def test_acknowledge_audit_preserves_true_pending_previous_status(
    db_session, test_settings
) -> None:
    quiet_time = datetime(2025, 1, 2, 15, 0, tzinfo=UTC)  # 23:00 Asia/Shanghai
    pending = create_system_alert(
        db_session,
        dedupe_scope="pending-before-ack",
        severity=AlertSeverity.INFO,
        title="静默时段待办",
        trigger_reason="TEST_PENDING_ACK",
        data_as_of=quiet_time,
        settings=test_settings,
        now=quiet_time,
    )
    assert pending.status == AlertStatus.PENDING.value
    assert pending.sent_at is None

    acknowledge_alert(db_session, pending.id, actor="test-reviewer")
    audit = db_session.scalar(
        select(AuditLog).where(
            AuditLog.event_type == "ALERT_ACKNOWLEDGED",
            AuditLog.object_id == pending.id,
        )
    )
    assert audit is not None
    assert audit.details["previous_status"] == AlertStatus.PENDING.value


def test_tampered_alert_content_blocks_idempotent_reuse_and_acknowledgement(
    db_session, test_settings
) -> None:
    now = datetime(2025, 1, 2, 2, 0, tzinfo=UTC)
    alert = create_system_alert(
        db_session,
        dedupe_scope="tamper-detection",
        severity=AlertSeverity.CRITICAL,
        title="原始系统预警",
        trigger_reason="ORIGINAL_REASON",
        data_as_of=now,
        settings=test_settings,
        now=now,
    )
    assert verify_alert_content(alert)
    alert.trigger_reason = "TAMPERED_REASON"
    db_session.commit()
    assert not verify_alert_content(alert)

    with pytest.raises(AlertIntegrityError, match="完整性"):
        create_system_alert(
            db_session,
            dedupe_scope="tamper-detection",
            severity=AlertSeverity.CRITICAL,
            title="原始系统预警",
            trigger_reason="ORIGINAL_REASON",
            data_as_of=now,
            settings=test_settings,
            now=now,
        )
    with pytest.raises(AlertIntegrityError, match="完整性"):
        acknowledge_alert(db_session, alert.id, actor="test-reviewer")


def test_explicitly_migrated_legacy_alert_dedupes_but_never_becomes_trusted_content(
    db_session, test_settings
) -> None:
    now = datetime(2025, 1, 2, 2, 0, tzinfo=UTC)
    legacy = create_system_alert(
        db_session,
        dedupe_scope="migrated-legacy-dedupe",
        severity=AlertSeverity.WARN,
        title="迁移前预警",
        trigger_reason="LEGACY",
        data_as_of=now,
        settings=test_settings,
        now=now,
    )
    legacy.source_object_type = "LegacyUnverifiedSystem"
    legacy.content_hash = None
    db_session.commit()

    replayed = create_system_alert(
        db_session,
        dedupe_scope="migrated-legacy-dedupe",
        severity=AlertSeverity.WARN,
        title="不得覆盖历史行",
        trigger_reason="REPLAY",
        data_as_of=now,
        settings=test_settings,
        now=now + timedelta(minutes=1),
    )
    view = _alert_display_view(db_session, legacy)

    assert replayed.id == legacy.id
    assert db_session.scalar(select(func.count(Alert.id))) == 1
    assert view["display_blocked_reason"] == "ALERT_CONTENT_INTEGRITY_FAILED"
    assert view["confidence"] is None
    assert view["source_links"] == []


def test_scheduled_event_obeys_daily_limit_and_future_announcement_gate(
    db_session, test_settings
) -> None:
    register_default_providers(db_session)
    now = datetime(2025, 1, 2, 2, 0, tzinfo=UTC)  # 10:00 Asia/Shanghai
    limited = test_settings.model_copy(update={"alert_daily_limit": 1})
    create_system_alert(
        db_session,
        dedupe_scope="daily-limit-primer",
        severity=AlertSeverity.WARN,
        title="已占用今日额度",
        trigger_reason="TEST",
        data_as_of=now,
        settings=limited,
        now=now,
    )
    eligible = _scheduled_event(now, code="TEST-KNOWN-EVENT")
    future_announcement = _scheduled_event(
        now,
        code="TEST-NOT-PUBLIC",
        first_announced_at=now + timedelta(hours=1),
        first_seen_at=now + timedelta(hours=1),
        available_at=now + timedelta(hours=1),
    )
    db_session.add_all([eligible, future_announcement])
    db_session.commit()

    alerts = create_scheduled_event_alerts(
        db_session,
        [eligible, future_announcement],
        settings=limited,
        now=now,
    )

    assert len(alerts) == 1
    assert alerts[0].status == AlertStatus.SUPPRESSED.value
    assert alerts[0].delivery_attempts == 0
    assert alerts[0].alert_type == "KNOWN_EVENT_WINDOW"
    assert alerts[0].provider_code == "demo_fixture"
    assert alerts[0].source_object_type == "ScheduledEvent"
    assert alerts[0].source_object_id == eligible.id
    assert verify_alert_content(alerts[0])
    assert alerts[0].disclaimer == DISCLAIMER
    assert alerts[0].source_links == [eligible.source_uri]


def test_future_available_or_cancelled_calendar_event_never_emits_alert(
    db_session, test_settings
) -> None:
    register_default_providers(db_session)
    now = datetime(2025, 1, 2, 2, 0, tzinfo=UTC)
    future_available = _scheduled_event(
        now,
        code="TEST-FUTURE-AVAILABLE",
        first_seen_at=now - timedelta(hours=1),
        available_at=now + timedelta(minutes=1),
    )
    cancelled = _scheduled_event(now, code="TEST-CANCELLED", status="CANCELLED")
    db_session.add_all([future_available, cancelled])
    db_session.commit()

    assert (
        create_scheduled_event_alerts(
            db_session,
            [future_available, cancelled],
            settings=test_settings,
            now=now,
        )
        == []
    )
    assert db_session.scalar(select(func.count(Alert.id))) == 0


def test_tampered_calendar_fact_never_emits_or_reuses_an_event_alert(
    db_session, test_settings
) -> None:
    register_default_providers(db_session)
    now = datetime(2025, 1, 2, 2, 0, tzinfo=UTC)
    event = _scheduled_event(now, code="TEST-TAMPERED-CALENDAR")
    db_session.add(event)
    db_session.commit()
    assert verify_scheduled_event_record(event)
    [trusted_alert] = create_scheduled_event_alerts(
        db_session,
        [event],
        settings=test_settings,
        now=now,
    )
    assert verify_alert_content(trusted_alert)

    event.name_zh = "被未授权改写的事件名称"
    db_session.commit()

    assert not verify_scheduled_event_record(event)
    assert "EVENT_RECORD_INTEGRITY_FAILED" in scheduled_event_gate_reasons(
        db_session, event, now=now
    )
    assert (
        create_scheduled_event_alerts(
            db_session,
            [event],
            settings=test_settings,
            now=now,
        )
        == []
    )
    blocked_view = _alert_display_view(db_session, trusted_alert)
    assert blocked_view["display_blocked_reason"] == "ALERT_EVENT_LINEAGE_INVALID"
    assert blocked_view["confidence"] is None
    assert blocked_view["source_links"] == []
    assert db_session.scalar(select(func.count(Alert.id))) == 1
