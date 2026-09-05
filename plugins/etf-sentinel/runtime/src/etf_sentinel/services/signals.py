from __future__ import annotations

import hashlib
import hmac
import json
import math
import os
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import Session

from etf_sentinel.audit import append_audit
from etf_sentinel.config import Settings
from etf_sentinel.enums import DataMode, ModelStatus, SignalState
from etf_sentinel.models import (
    DataSnapshot,
    EtfInstrument,
    ModelVersion,
    NewsEvent,
    ProviderRegistry,
    Signal,
)
from etf_sentinel.providers.base import (
    LicenseGateError,
    ProviderSchemaError,
    authorize_provider,
    content_hash,
    normalize_url,
    validate_fact_frame,
)
from etf_sentinel.services.fact_integrity import verify_news_event_record
from etf_sentinel.services.features import InsufficientHistoryError, compute_rule_based_factors
from etf_sentinel.services.ingestion import SnapshotIntegrityError, verify_snapshot_frame
from etf_sentinel.services.risk import assess_signal_risk


def code_version() -> str:
    explicit = os.getenv("CODE_VERSION")
    if explicit:
        return explicit[:80]
    git_prefix = ""
    try:
        head = Path(".git/HEAD").read_text(encoding="utf-8").strip()
        if head.startswith("ref: "):
            ref_path = Path(".git") / head.removeprefix("ref: ")
            if ref_path.is_file():
                git_prefix = ref_path.read_text(encoding="utf-8").strip()[:12]
        else:
            git_prefix = head[:12]
    except OSError:
        pass
    package_root = Path(__file__).resolve().parents[1]
    digest = hashlib.sha256()
    sources = sorted(package_root.rglob("*.py"))
    manifest = Path.cwd() / "pyproject.toml"
    if manifest.is_file():
        sources.append(manifest)
    for source in sources:
        relative = source.name if source == manifest else str(source.relative_to(package_root))
        digest.update(relative.encode())
        digest.update(b"\0")
        digest.update(source.read_bytes())
        digest.update(b"\0")
    tree_version = f"tree-{digest.hexdigest()[:16]}"
    return f"{git_prefix}+{tree_version}" if git_prefix else tree_version


def signal_record_hash(signal: Signal) -> str:
    """Hash immutable signal output and lineage; current/archive state is intentionally excluded."""
    feature_values = dict(signal.feature_values or {})
    feature_values.pop("signal_record_hash", None)

    def timestamp(value: datetime) -> str:
        parsed = pd.Timestamp(value)
        parsed = parsed.tz_localize("UTC") if parsed.tzinfo is None else parsed.tz_convert("UTC")
        return parsed.isoformat()

    payload = {
        "idempotency_key": signal.idempotency_key,
        "instrument_id": signal.instrument_id,
        "data_snapshot_id": signal.data_snapshot_id,
        "model_version_id": signal.model_version_id,
        "data_as_of": timestamp(signal.data_as_of),
        "available_at": timestamp(signal.available_at),
        "generated_at": timestamp(signal.generated_at),
        "horizon_days": signal.horizon_days,
        "state": signal.state,
        "probability": signal.probability,
        "confidence": signal.confidence,
        "confidence_interval": signal.confidence_interval,
        "market_score": signal.market_score,
        "macro_score": signal.macro_score,
        "news_score": signal.news_score,
        "liquidity_score": signal.liquidity_score,
        "composite_score": signal.composite_score,
        "supporting_evidence": signal.supporting_evidence,
        "opposing_evidence": signal.opposing_evidence,
        "source_links": signal.source_links,
        "feature_values": feature_values,
        "risk_rules_hit": signal.risk_rules_hit,
        "trigger_conditions": signal.trigger_conditions,
        "invalidation_conditions": signal.invalidation_conditions,
        "suggested_risk_budget_min": signal.suggested_risk_budget_min,
        "suggested_risk_budget_max": signal.suggested_risk_budget_max,
        "limitations": signal.limitations,
        "data_mode": signal.data_mode,
        "latency_status": signal.latency_status,
        "code_version": signal.code_version,
    }
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str).encode()
    ).hexdigest()


def verify_signal_record(signal: Signal) -> bool:
    stored = (signal.feature_values or {}).get("signal_record_hash")
    return isinstance(stored, str) and hmac.compare_digest(stored, signal_record_hash(signal))


def _news_event_lineage_payload(event: NewsEvent) -> dict[str, object]:
    def timestamp(value: datetime | None) -> str | None:
        if value is None:
            return None
        parsed = pd.Timestamp(value)
        parsed = parsed.tz_localize("UTC") if parsed.tzinfo is None else parsed.tz_convert("UTC")
        return parsed.isoformat()

    return {
        "id": event.id,
        "provider_code": event.provider_code,
        "normalized_url": event.normalized_url,
        "source_uri": event.source_uri,
        "title": event.title,
        "short_summary": event.short_summary,
        "source_name": event.source_name,
        "event_time": timestamp(event.event_time),
        "published_at": timestamp(event.published_at),
        "first_seen_at": timestamp(event.first_seen_at),
        "available_at": timestamp(event.available_at),
        "as_of": timestamp(event.as_of),
        "ingested_at": timestamp(event.ingested_at),
        "revision": event.revision,
        "timezone": event.timezone,
        "currency": event.currency,
        "latency_class": event.latency_class,
        "license_scope": event.license_scope,
        "data_mode": event.data_mode,
        "quality_flags": sorted(event.quality_flags or []),
        "raw_payload_hash": event.raw_payload_hash,
        "dedupe_hash": event.dedupe_hash,
        "cluster_key": event.cluster_key,
        "entities": event.entities,
        "exposures": event.exposures,
        "relevance": event.relevance,
        "direction": event.direction,
        "severity": event.severity,
        "novelty": event.novelty,
        "source_grade": event.source_grade,
        "fact_record_hash": event.record_hash,
    }


def build_news_lineage(events: list[NewsEvent]) -> dict[str, object]:
    records = sorted(
        (_news_event_lineage_payload(event) for event in events),
        key=lambda item: str(item["id"]),
    )
    record_hashes = [
        {
            "id": str(record["id"]),
            "provider_code": str(record["provider_code"]),
            "revision": str(record["revision"]),
            "available_at": record["available_at"],
            "raw_payload_hash": str(record["raw_payload_hash"]),
            "record_hash": content_hash(record),
        }
        for record in records
    ]
    return {
        "status": "AVAILABLE" if records else "NONE_AVAILABLE",
        "lineage_hash": content_hash(records),
        "events": record_hashes,
    }


def verify_signal_news_lineage(
    session: Session, signal: Signal, *, snapshot: DataSnapshot | None = None
) -> tuple[bool, str | None]:
    session.flush()
    stored = (signal.feature_values or {}).get("news_lineage")
    if not isinstance(stored, dict) or not isinstance(stored.get("lineage_hash"), str):
        return False, None
    current_snapshot = snapshot or session.get(
        DataSnapshot, signal.data_snapshot_id, populate_existing=True
    )
    if current_snapshot is None:
        return False, None
    current_events = eligible_news_events(
        session,
        snapshot=current_snapshot,
        evaluation_time=signal.generated_at,
    )
    current = build_news_lineage(current_events)
    if not hmac.compare_digest(
        str(stored.get("lineage_hash")), str(current.get("lineage_hash"))
    ) or stored.get("events") != current.get("events"):
        return False, str(current.get("lineage_hash"))
    return True, str(current.get("lineage_hash"))


def decision_policy_hash(
    session: Session,
    *,
    snapshot: DataSnapshot,
    settings: Settings,
    macro_snapshot: DataSnapshot | None = None,
    model: ModelVersion | None = None,
    news_lineage_hash: str | None = None,
    portfolio_risk_context_hash: str | None = None,
) -> str:
    session.flush()
    current_snapshot = session.get(DataSnapshot, snapshot.id, populate_existing=True)
    if current_snapshot is None:
        raise SnapshotIntegrityError("决策策略关联的行情快照不存在。")
    snapshot = current_snapshot
    if macro_snapshot is not None:
        current_macro = session.get(DataSnapshot, macro_snapshot.id, populate_existing=True)
        if current_macro is None:
            raise SnapshotIntegrityError("决策策略关联的宏观快照不存在。")
        macro_snapshot = current_macro
    registry = session.scalar(
        select(ProviderRegistry)
        .where(ProviderRegistry.provider_code == snapshot.provider_code)
        .execution_options(populate_existing=True)
    )
    rule_model = (
        session.get(ModelVersion, model.id, populate_existing=True)
        if model is not None
        else session.scalar(
            select(ModelVersion)
            .where(ModelVersion.model_name == "RuleBasedV1", ModelVersion.version == "1.0.0")
            .execution_options(populate_existing=True)
        )
    )
    model_metrics = rule_model.metrics if rule_model is not None else {}
    instruments = list(
        session.scalars(
            select(EtfInstrument)
            .where(EtfInstrument.provider_code == snapshot.provider_code)
            .order_by(EtfInstrument.id)
            .execution_options(populate_existing=True)
        )
    )
    policy = {
        "snapshot_provider": snapshot.provider_code,
        "snapshot_dataset_type": snapshot.dataset_type,
        "snapshot_hash": snapshot.snapshot_hash,
        "snapshot_config_hash": snapshot.config_hash,
        "snapshot_license_scope": snapshot.license_scope,
        "snapshot_quality_flags": sorted(snapshot.quality_flags or []),
        "instrument_master": [
            {
                "id": instrument.id,
                "mic": instrument.mic,
                "region": instrument.region,
                "asset_class": instrument.asset_class,
                "industry": instrument.industry,
                "currency": instrument.currency,
                "leveraged": instrument.leveraged,
                "inverse": instrument.inverse,
                "active": instrument.active,
                "liquidity_tier": instrument.liquidity_tier,
            }
            for instrument in instruments
        ],
        "macro_snapshot": None
        if macro_snapshot is None
        else {
            "provider_code": macro_snapshot.provider_code,
            "dataset_type": macro_snapshot.dataset_type,
            "snapshot_hash": macro_snapshot.snapshot_hash,
            "config_hash": macro_snapshot.config_hash,
            "data_mode": macro_snapshot.data_mode,
            "license_scope": macro_snapshot.license_scope,
            "quality_flags": sorted(macro_snapshot.quality_flags or []),
            "revision": macro_snapshot.revision,
            "row_count": macro_snapshot.row_count,
        },
        "news_lineage_hash": news_lineage_hash,
        "portfolio_risk_context_hash": portfolio_risk_context_hash,
        "data_mode": snapshot.data_mode,
        "code_version": code_version(),
        "risk": {
            "data_stale_after_minutes": settings.data_stale_after_minutes,
            "global_kill_switch": settings.global_kill_switch,
            "single_etf_cap": settings.single_etf_cap,
            "asset_class_cap": settings.asset_class_cap,
            "industry_cap": settings.industry_cap,
            "region_cap": settings.region_cap,
            "portfolio_volatility_target": settings.portfolio_volatility_target,
            "cash_floor": settings.cash_floor,
            "max_turnover": settings.max_turnover,
            "max_simulation_loss": settings.max_simulation_loss,
            "max_drawdown": settings.max_drawdown,
            "news_risk_reduction_threshold": settings.news_risk_reduction_threshold,
            "news_factor_weight_cap": settings.news_factor_weight_cap,
        },
        "provider_registry": None
        if registry is None
        else {
            "status": registry.review_status,
            "expires_at": registry.expires_at.isoformat() if registry.expires_at else None,
            "markets": sorted(registry.markets or []),
            "regions": sorted(registry.regions or []),
            "rights": [
                registry.display_right,
                registry.non_display_algorithm_right,
                registry.derivative_right,
                registry.cache_right,
                registry.training_right,
                registry.redistribution_right,
            ],
        },
        "model_governance": None
        if rule_model is None
        else {
            "name": rule_model.model_name,
            "version": rule_model.version,
            "status": rule_model.status,
            "feature_version": rule_model.feature_version,
            "training_snapshot_hash": rule_model.training_snapshot_hash,
            "governance_scope": model_metrics.get("governance_scope"),
            "real_data_candidate_gate": model_metrics.get("real_data_candidate_gate"),
            "drift_status": model_metrics.get("drift_status"),
            "approved_by": rule_model.approved_by,
            "approved_at": (rule_model.approved_at.isoformat() if rule_model.approved_at else None),
        },
    }
    return hashlib.sha256(
        json.dumps(policy, ensure_ascii=False, sort_keys=True).encode()
    ).hexdigest()


def ensure_rule_model(session: Session) -> ModelVersion:
    session.flush()
    model = session.scalar(
        select(ModelVersion)
        .where(ModelVersion.model_name == "RuleBasedV1", ModelVersion.version == "1.0.0")
        .execution_options(populate_existing=True)
    )
    if model is None:
        model = ModelVersion(
            model_name="RuleBasedV1",
            version="1.0.0",
            status=ModelStatus.EXPERIMENTAL.value,
            feature_version="rule-features-v1",
            metrics={
                "type": "interpretable_baseline",
                "calibration": "heuristic_probability",
                "governance_scope": "DEMO_FIXTURE_ONLY",
                "real_data_candidate_gate": "BLOCKED",
                "drift_status": "NOT_APPLICABLE_TO_FIXED_FIXTURE",
            },
            limitations=[
                "概率由规则分数映射，不能解释为确定性收益预测。",
                "固定演示数据不代表真实市场效果。",
                "新闻权重被限制为最多 15%。",
            ],
        )
        session.add(model)
        session.flush()
    return model


def rule_model_governance_rules(model: ModelVersion | None, *, data_mode: str) -> list[str]:
    """Return fail-closed rules for the model that controls candidate states."""
    if model is None:
        return ["MODEL_VERSION_MISSING"]
    if model.model_name != "RuleBasedV1" or model.version != "1.0.0":
        return ["MODEL_VERSION_UNEXPECTED"]
    metrics = model.metrics or {}
    rules: list[str] = []
    if model.feature_version != "rule-features-v1":
        rules.append("MODEL_FEATURE_VERSION_UNEXPECTED")
    if model.status != ModelStatus.EXPERIMENTAL.value:
        rules.append(f"MODEL_STATUS_{model.status}")
    if data_mode == DataMode.DEMO_FIXTURE.value:
        if metrics.get("governance_scope") != "DEMO_FIXTURE_ONLY":
            rules.append("MODEL_GOVERNANCE_SCOPE_INVALID")
        if metrics.get("real_data_candidate_gate") != "BLOCKED":
            rules.append("MODEL_REAL_DATA_GATE_INVALID")
        if metrics.get("drift_status") != "NOT_APPLICABLE_TO_FIXED_FIXTURE":
            rules.append("MODEL_DRIFT_GATE_FAILED")
    else:
        # Phase one contains no approved real-data model path.  A status change
        # alone must never unlock it.
        rules.append("REAL_DATA_MODEL_GATE_BLOCKED")
    return list(dict.fromkeys(rules))


def generate_signals(
    session: Session,
    *,
    snapshot: DataSnapshot,
    frame: pd.DataFrame,
    evaluation_time: datetime,
    settings: Settings,
    macro_snapshot: DataSnapshot | None = None,
    macro_frame: pd.DataFrame | None = None,
    commit: bool = True,
) -> list[Signal]:
    session.flush()
    current_snapshot = session.get(DataSnapshot, snapshot.id, populate_existing=True)
    if current_snapshot is None:
        raise SnapshotIntegrityError("信号输入快照不存在。")
    snapshot = current_snapshot
    if macro_snapshot is not None:
        current_macro = session.get(DataSnapshot, macro_snapshot.id, populate_existing=True)
        if current_macro is None:
            raise SnapshotIntegrityError("宏观输入快照不存在。")
        macro_snapshot = current_macro
    if snapshot.dataset_type != "MARKET_BARS":
        raise ProviderSchemaError("信号输入快照类型必须为 MARKET_BARS。")
    if not snapshot.license_scope or "UNCLEAR" in snapshot.license_scope.upper():
        raise LicenseGateError("行情快照许可范围为空或不明确，已阻断信号生成。")
    verify_snapshot_frame(
        frame,
        expected_hash=snapshot.snapshot_hash,
        expected_rows=snapshot.row_count,
    )
    validate_fact_frame(frame)
    if set(frame["provider"].dropna().astype(str).unique()) != {snapshot.provider_code}:
        raise ProviderSchemaError("行情事实供应商与快照登记不一致。")
    if set(frame["license_scope"].dropna().astype(str).unique()) != {snapshot.license_scope}:
        raise LicenseGateError("行情事实许可范围与快照登记不一致，已阻断信号生成。")
    if set(frame["data_mode"].dropna().astype(str).unique()) != {snapshot.data_mode}:
        raise ProviderSchemaError("行情事实 data_mode 与快照登记不一致。")
    current = pd.Timestamp(evaluation_time)
    current = current.tz_localize("UTC") if current.tzinfo is None else current.tz_convert("UTC")
    point_in_time_frame = frame.copy()
    for column in ("event_time", "published_at", "first_seen_at", "available_at", "as_of"):
        point_in_time_frame[column] = pd.to_datetime(
            point_in_time_frame[column], utc=True, errors="raise"
        )
    point_in_time_frame = point_in_time_frame.loc[
        (point_in_time_frame["event_time"] <= current)
        & (point_in_time_frame["published_at"] <= current)
        & (point_in_time_frame["first_seen_at"] <= current)
        & (point_in_time_frame["available_at"] <= current)
        & (point_in_time_frame["as_of"] <= current)
    ].copy()

    # Local import avoids a module cycle: the ledger reuses signal integrity
    # helpers, while this critical entry point must independently calculate the
    # current portfolio gate rather than trusting caller-supplied flags.
    from etf_sentinel.services.ledger import assess_portfolio_risk_gate

    portfolio_gate = assess_portfolio_risk_gate(
        session,
        point_in_time_frame,
        settings=settings,
        evaluation_time=evaluation_time,
    )
    portfolio_risk_rules = portfolio_gate.rules
    portfolio_risk_context_hash = portfolio_gate.context_hash

    model = ensure_rule_model(session)
    instruments = list(
        session.scalars(
            select(EtfInstrument).where(
                EtfInstrument.active.is_(True),
                EtfInstrument.provider_code == snapshot.provider_code,
            )
        )
    )
    news = eligible_news_events(session, snapshot=snapshot, evaluation_time=evaluation_time)
    news_lineage = build_news_lineage(news)
    policy_hash = decision_policy_hash(
        session,
        snapshot=snapshot,
        settings=settings,
        macro_snapshot=macro_snapshot,
        model=model,
        news_lineage_hash=str(news_lineage["lineage_hash"]),
        portfolio_risk_context_hash=portfolio_risk_context_hash,
    )
    model_rules = rule_model_governance_rules(model, data_mode=snapshot.data_mode)
    macro_score, macro_lineage, macro_links = _eligible_macro_context(
        session,
        market_snapshot=snapshot,
        macro_snapshot=macro_snapshot,
        macro_frame=macro_frame,
        evaluation_time=evaluation_time,
    )
    six_month_returns: dict[str, float] = {}
    for instrument in instruments:
        bars = point_in_time_frame.loc[
            point_in_time_frame["instrument_id"] == instrument.id
        ].sort_values("event_time")
        prices = bars["total_return_close"].astype(float)
        six_month_returns[instrument.id] = (
            float(prices.iloc[-1] / prices.iloc[-127] - 1) if len(prices) > 126 else 0.0
        )
    universe_return = float(pd.Series(six_month_returns).median()) if six_month_returns else 0.0
    created: list[Signal] = []
    for instrument in instruments:
        bars = point_in_time_frame.loc[point_in_time_frame["instrument_id"] == instrument.id].copy()
        risk = assess_signal_risk(
            session,
            instrument=instrument,
            snapshot=snapshot,
            bars=bars,
            evaluation_time=evaluation_time,
            settings=settings,
        )
        if portfolio_risk_rules:
            risk = type(risk)(
                blocked=True,
                stale=risk.stale,
                rules_hit=list(dict.fromkeys([*risk.rules_hit, *portfolio_risk_rules])),
            )
        if model_rules:
            risk = type(risk)(
                blocked=True,
                stale=risk.stale,
                rules_hit=list(dict.fromkeys([*risk.rules_hit, *model_rules])),
            )
        factor = None
        try:
            factor = compute_rule_based_factors(
                bars,
                instrument,
                universe_six_month_return=universe_return,
                news_events=news,
                evaluation_time=evaluation_time,
                macro_score=macro_score,
                news_weight_cap=settings.news_factor_weight_cap,
                macro_evidence=(
                    {
                        "factor": "宏观风险状态",
                        "value": macro_score,
                        "explanation": "当时可知的固定演示宏观 vintage 均值。",
                    }
                    if macro_score is not None
                    else None
                ),
                macro_source_links=macro_links,
            )
        except InsufficientHistoryError:
            risk = type(risk)(
                blocked=True,
                stale=risk.stale,
                rules_hit=risk.rules_hit + ["INSUFFICIENT_HISTORY"],
            )
        if risk.stale:
            state = SignalState.DATA_STALE
        elif risk.blocked:
            state = SignalState.BLOCKED_BY_RISK
        elif factor is None:
            state = SignalState.BLOCKED_BY_RISK
        else:
            state = state_from_score(factor.composite_score)
        latest_as_of = (
            pd.to_datetime(bars["as_of"], utc=True).max().to_pydatetime()
            if not bars.empty
            else evaluation_time
        )
        latest_available = (
            pd.to_datetime(bars["available_at"], utc=True).max().to_pydatetime()
            if not bars.empty
            else evaluation_time
        )
        composite = factor.composite_score if factor else 0.0
        probability = float(1 / (1 + math.exp(-2.6 * composite)))
        confidence = float(
            max(0.05, min(0.90, 0.45 + len(bars) / 2000 - len(risk.rules_hit) * 0.12))
        )
        half_width = 0.18 + (1 - confidence) * 0.12
        annual_volatility = factor.values.get("annualized_volatility", 1.0) if factor else 1.0
        inverse_vol_budget = min(
            settings.single_etf_cap,
            settings.portfolio_volatility_target / max(annual_volatility, 0.05) / 20,
        )
        raw_key = (
            f"{snapshot.snapshot_hash}:{instrument.id}:{model.id}:20:"
            f"{evaluation_time.isoformat()}:{policy_hash}"
        )
        idempotency_key = hashlib.sha256(raw_key.encode()).hexdigest()
        existing = session.scalar(select(Signal).where(Signal.idempotency_key == idempotency_key))
        if existing is not None:
            if not verify_signal_record(existing):
                raise SnapshotIntegrityError("信号记录完整性校验失败，已拒绝幂等重放。")
            prior_current = list(
                session.scalars(
                    select(Signal).where(
                        Signal.instrument_id == instrument.id,
                        Signal.horizon_days == 20,
                        Signal.is_current.is_(True),
                        Signal.id != existing.id,
                    )
                )
            )
            for prior in prior_current:
                prior.is_current = False
                append_audit(
                    session,
                    event_type="SIGNAL_SUPERSEDED",
                    object_type="Signal",
                    object_id=prior.id,
                    details={"replacement_policy_hash": policy_hash},
                )
            if not existing.is_current:
                existing.is_current = True
                append_audit(
                    session,
                    event_type="SIGNAL_REACTIVATED",
                    object_type="Signal",
                    object_id=existing.id,
                    details={"decision_policy_hash": policy_hash},
                )
            created.append(existing)
            continue
        prior_current = list(
            session.scalars(
                select(Signal).where(
                    Signal.instrument_id == instrument.id,
                    Signal.horizon_days == 20,
                    Signal.is_current.is_(True),
                )
            )
        )
        for prior in prior_current:
            prior.is_current = False
            append_audit(
                session,
                event_type="SIGNAL_SUPERSEDED",
                object_type="Signal",
                object_id=prior.id,
                details={"replacement_policy_hash": policy_hash},
            )
        feature_values: dict[str, object] = dict(factor.values) if factor else {}
        feature_values["decision_policy_hash"] = policy_hash
        feature_values["macro_lineage"] = macro_lineage
        feature_values["news_lineage"] = news_lineage
        feature_values["portfolio_risk_context_hash"] = portfolio_risk_context_hash
        feature_values["portfolio_risk_rules"] = portfolio_risk_rules or []
        signal = Signal(
            idempotency_key=idempotency_key,
            instrument_id=instrument.id,
            data_snapshot_id=snapshot.id,
            model_version_id=model.id,
            data_as_of=latest_as_of,
            available_at=latest_available,
            generated_at=evaluation_time,
            horizon_days=20,
            state=state.value,
            probability=probability,
            confidence=confidence,
            confidence_interval=[
                max(0.0, probability - half_width),
                min(1.0, probability + half_width),
            ],
            market_score=factor.market_score if factor else 0.0,
            macro_score=factor.macro_score if factor else 0.0,
            news_score=factor.news_score if factor else 0.0,
            liquidity_score=factor.liquidity_score if factor else 0.0,
            composite_score=composite,
            supporting_evidence=factor.supporting_evidence if factor else [],
            opposing_evidence=factor.opposing_evidence if factor else [],
            source_links=(factor.source_links if factor else []) + [snapshot.source_uri],
            feature_values=feature_values,
            risk_rules_hit=risk.rules_hit,
            trigger_conditions=[
                "综合分数达到候选阈值",
                "数据许可、时效、流动性及全局风控门禁全部通过",
            ],
            invalidation_conditions=[
                "数据过期或供应商许可失效",
                "综合分数跌破对应状态阈值",
                "波动率、价差或组合集中度触发风控",
            ],
            suggested_risk_budget_min=0.0 if risk.blocked else inverse_vol_budget * 0.5,
            suggested_risk_budget_max=0.0 if risk.blocked else inverse_vol_budget,
            limitations=[
                "仅为概率情景和候选状态，不是交易指令。",
                "RuleBasedV1 的概率为启发式情景强度，未经真实样本概率校准。",
                "新闻因子不能单独决定候选状态。",
                (
                    "宏观因子来自已校验的当时可知 vintage 快照。"
                    if macro_score is not None
                    else "本次无可用宏观快照；宏观权重已剔除并对其他因子重归一。"
                ),
                "演示候选仅用于虚构标的的端到端闭环测试；"
                "任何真实数据在人工复核、漂移与回测门禁完成前均失败关闭。"
                if snapshot.data_mode == DataMode.DEMO_FIXTURE.value
                else "真实数据权利和质量必须持续复核。",
            ],
            data_mode=snapshot.data_mode,
            latency_status="STALE" if risk.stale else "ON_TIME",
            code_version=code_version(),
            is_current=True,
        )
        signal.feature_values = {
            **feature_values,
            "signal_record_hash": signal_record_hash(signal),
        }
        session.add(signal)
        session.flush()
        append_audit(
            session,
            event_type="SIGNAL_GENERATED" if not risk.blocked else "SIGNAL_RISK_BLOCKED",
            object_type="Signal",
            object_id=signal.id,
            details={
                "instrument_id": instrument.id,
                "snapshot_hash": snapshot.snapshot_hash,
                "model": f"{model.model_name}:{model.version}",
                "state": signal.state,
                "risk_rules": risk.rules_hit,
                "data_mode": signal.data_mode,
            },
        )
        created.append(signal)
    # Re-read mutable governance records immediately before the transaction
    # boundary.  This prevents a long-running Session from committing outputs
    # after another transaction has revoked a provider or rejected the model.
    final_model = session.get(ModelVersion, model.id, populate_existing=True)
    final_snapshot = session.get(DataSnapshot, snapshot.id, populate_existing=True)
    if final_snapshot is None:
        raise SnapshotIntegrityError("提交前行情快照已不存在。")
    final_model_rules = rule_model_governance_rules(final_model, data_mode=final_snapshot.data_mode)
    if final_model_rules != model_rules:
        raise SnapshotIntegrityError(f"提交前模型治理状态已变化：{','.join(final_model_rules)}")
    if final_model_rules and any(
        item.state not in {SignalState.BLOCKED_BY_RISK.value, SignalState.DATA_STALE.value}
        for item in created
    ):
        raise SnapshotIntegrityError("模型治理未通过但出现未阻断信号，已失败关闭。")
    final_news = eligible_news_events(
        session, snapshot=final_snapshot, evaluation_time=evaluation_time
    )
    final_news_lineage = build_news_lineage(final_news)
    if final_news_lineage != news_lineage:
        raise SnapshotIntegrityError("提交前新闻集合或来源血缘已变化，已失败关闭。")
    final_portfolio_gate = assess_portfolio_risk_gate(
        session,
        point_in_time_frame,
        settings=settings,
        evaluation_time=evaluation_time,
    )
    if final_portfolio_gate != portfolio_gate:
        raise SnapshotIntegrityError("提交前组合风险门禁已变化，已失败关闭。")
    final_policy_hash = decision_policy_hash(
        session,
        snapshot=final_snapshot,
        settings=settings,
        macro_snapshot=macro_snapshot,
        model=final_model,
        news_lineage_hash=str(final_news_lineage["lineage_hash"]),
        portfolio_risk_context_hash=final_portfolio_gate.context_hash,
    )
    for instrument in instruments:
        authorize_provider(
            session,
            final_snapshot.provider_code,
            purposes={"display", "algorithm", "derivative", "cache"},
            at=datetime.now(UTC),
            market=instrument.mic,
            region=instrument.region,
        )
    if any(
        (item.feature_values or {}).get("decision_policy_hash") != final_policy_hash
        or not verify_signal_record(item)
        for item in created
    ):
        raise SnapshotIntegrityError("提交前决策策略或信号完整性已变化，已失败关闭。")
    if commit:
        session.commit()
    else:
        session.flush()
    return created


def _eligible_macro_context(
    session: Session,
    *,
    market_snapshot: DataSnapshot,
    macro_snapshot: DataSnapshot | None,
    macro_frame: pd.DataFrame | None,
    evaluation_time: datetime,
) -> tuple[float | None, dict[str, object], list[str]]:
    if macro_snapshot is None and macro_frame is None:
        return None, {"status": "UNAVAILABLE_REWEIGHTED"}, []
    if macro_snapshot is None or macro_frame is None:
        raise ProviderSchemaError("宏观快照与数据必须同时提供。")
    if macro_snapshot.dataset_type != "MACRO_FACTS":
        raise ProviderSchemaError("宏观数据快照类型无效。")
    if not macro_snapshot.license_scope or "UNCLEAR" in macro_snapshot.license_scope.upper():
        raise LicenseGateError("宏观快照许可范围为空或不明确。")
    if (
        macro_snapshot.provider_code != market_snapshot.provider_code
        or macro_snapshot.data_mode != market_snapshot.data_mode
    ):
        raise ProviderSchemaError("宏观与行情快照的供应商或 data_mode 不一致。")
    verify_snapshot_frame(
        macro_frame,
        expected_hash=macro_snapshot.snapshot_hash,
        expected_rows=macro_snapshot.row_count,
    )
    validate_fact_frame(macro_frame)
    if set(macro_frame["provider"].dropna().astype(str).unique()) != {macro_snapshot.provider_code}:
        raise ProviderSchemaError("宏观事实供应商与快照登记不一致。")
    if set(macro_frame["license_scope"].dropna().astype(str).unique()) != {
        macro_snapshot.license_scope
    }:
        raise LicenseGateError("宏观事实许可范围与快照登记不一致。")
    if set(macro_frame["data_mode"].dropna().astype(str).unique()) != {macro_snapshot.data_mode}:
        raise ProviderSchemaError("宏观事实 data_mode 与快照登记不一致。")
    blocked_snapshot_flags = {
        "HASH_MISMATCH",
        "LICENSE_UNCLEAR",
        "SCHEMA_UNVERIFIED",
        "TIME_VALIDATION_FAILED",
    }
    if blocked_snapshot_flags.intersection(set(macro_snapshot.quality_flags or [])):
        raise ProviderSchemaError("宏观快照质量门禁未通过。")
    authorize_provider(
        session,
        macro_snapshot.provider_code,
        purposes={"algorithm", "derivative", "cache"},
        at=datetime.now(UTC),
    )
    current = pd.Timestamp(evaluation_time)
    current = current.tz_localize("UTC") if current.tzinfo is None else current.tz_convert("UTC")
    facts = macro_frame.copy()
    for column in ("event_time", "published_at", "first_seen_at", "available_at", "as_of"):
        facts[column] = pd.to_datetime(facts[column], utc=True, errors="raise")
    facts = facts.loc[
        (facts["event_time"] <= current)
        & (facts["published_at"] <= current)
        & (facts["first_seen_at"] <= current)
        & (facts["available_at"] <= current)
        & (facts["as_of"] <= current)
        & (facts["provider"].astype(str) == macro_snapshot.provider_code)
        & (facts["data_mode"].astype(str) == macro_snapshot.data_mode)
    ].copy()
    blocked_flags = {"LICENSE_UNCLEAR", "SCHEMA_UNVERIFIED", "TIME_VALIDATION_FAILED"}
    facts = facts.loc[
        facts["quality_flags"].map(
            lambda values: (
                not blocked_flags.intersection(
                    set(values.tolist())
                    if hasattr(values, "tolist")
                    else set(values)
                    if isinstance(values, (list, tuple, set))
                    else {str(values)}
                )
            )
        )
    ]
    if facts.empty:
        raise ProviderSchemaError("评估时点无已授权且当时可知的宏观事实。")
    facts = facts.sort_values(
        ["series_code", "available_at", "first_seen_at", "revision"]
    ).drop_duplicates("series_code", keep="last")
    values = pd.to_numeric(facts["value"], errors="coerce")
    if values.isna().any() or not values.map(math.isfinite).all():
        raise ProviderSchemaError("宏观事实包含非有限数值。")
    score = float(values.clip(-1, 1).mean())
    lineage: dict[str, object] = {
        "status": "AVAILABLE",
        "snapshot_id": macro_snapshot.id,
        "snapshot_hash": macro_snapshot.snapshot_hash,
        "data_mode": macro_snapshot.data_mode,
        "series": [
            {
                "series_code": str(row["series_code"]),
                "vintage": str(row.get("vintage", row["revision"])),
                "available_at": pd.Timestamp(row["available_at"]).isoformat(),
                "raw_payload_hash": str(row["raw_payload_hash"]),
            }
            for row in facts.to_dict(orient="records")
        ],
    }
    return score, lineage, list(dict.fromkeys(facts["source_uri"].astype(str).tolist()))


def state_from_score(score: float) -> SignalState:
    if score >= 0.38:
        return SignalState.ENTRY_CANDIDATE
    if score >= 0.14:
        return SignalState.WATCH
    if score > -0.18:
        return SignalState.NO_ACTION
    if score > -0.38:
        return SignalState.REDUCE_CANDIDATE
    return SignalState.EXIT_CANDIDATE


def eligible_news_events(
    session: Session,
    *,
    snapshot: DataSnapshot,
    evaluation_time: datetime,
    purposes: set[str] | None = None,
    require_exposure: bool = True,
) -> list[NewsEvent]:
    session.flush()
    rows = list(
        session.scalars(
            select(NewsEvent)
            .where(
                NewsEvent.available_at <= evaluation_time,
                NewsEvent.first_seen_at <= evaluation_time,
                NewsEvent.data_mode == snapshot.data_mode,
            )
            .execution_options(populate_existing=True)
        )
    )
    blocked_flags = {
        "LICENSE_UNCLEAR",
        "TIME_VALIDATION_FAILED",
        "PROMPT_INJECTION_SUSPECTED",
        "ENTITY_MAPPING_REQUIRED",
        "ENTITY_MATCH_INSUFFICIENT",
        "MISSING_SOURCE_URL",
    }
    approved: list[NewsEvent] = []
    checked_providers: dict[str, bool] = {}
    current = evaluation_time if evaluation_time.tzinfo else evaluation_time.replace(tzinfo=UTC)
    for event in rows:
        if not verify_news_event_record(event):
            raise SnapshotIntegrityError("新闻结构化事实记录哈希不匹配，已失败关闭。")
        event_time = (
            event.event_time if event.event_time.tzinfo else event.event_time.replace(tzinfo=UTC)
        )
        available = (
            event.available_at
            if event.available_at.tzinfo
            else event.available_at.replace(tzinfo=UTC)
        )
        first_seen = (
            event.first_seen_at
            if event.first_seen_at.tzinfo
            else event.first_seen_at.replace(tzinfo=UTC)
        )
        as_of = event.as_of if event.as_of.tzinfo else event.as_of.replace(tzinfo=UTC)
        published = event.published_at
        if published is not None and published.tzinfo is None:
            published = published.replace(tzinfo=UTC)
        if (
            event_time > current
            or event_time > first_seen
            or event_time > available
            or first_seen > available
            or available > current
            or as_of > current
            or (published is not None and (published > available or published > current))
        ):
            continue
        if (
            not event.source_uri
            or not event.license_scope
            or "UNCLEAR" in event.license_scope.upper()
        ):
            continue
        try:
            normalized = normalize_url(event.source_uri)
        except ProviderSchemaError:
            continue
        expected_dedupe = content_hash(
            {
                "url": normalized,
                "title": event.title.lower(),
                "cluster": event.cluster_key,
            }
        )
        if event.normalized_url != normalized or not hmac.compare_digest(
            event.dedupe_hash, expected_dedupe
        ):
            continue
        if len(event.raw_payload_hash) != 64 or any(
            character not in "0123456789abcdef" for character in event.raw_payload_hash.lower()
        ):
            continue
        if require_exposure and (not event.entities or not event.exposures):
            continue
        if blocked_flags.intersection(event.quality_flags or []):
            continue
        if event.provider_code not in checked_providers:
            try:
                authorize_provider(
                    session,
                    event.provider_code,
                    purposes=purposes or {"algorithm", "derivative", "cache"},
                    at=datetime.now(UTC),
                )
            except LicenseGateError:
                checked_providers[event.provider_code] = False
            else:
                checked_providers[event.provider_code] = True
        if checked_providers[event.provider_code]:
            approved.append(event)
    return approved
