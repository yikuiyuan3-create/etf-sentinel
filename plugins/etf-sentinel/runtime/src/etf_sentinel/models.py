from __future__ import annotations

import uuid
from datetime import UTC, date, datetime
from typing import Any

from sqlalchemy import (
    JSON,
    Boolean,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    event,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from etf_sentinel.database import Base


def utc_now() -> datetime:
    return datetime.now(UTC)


def new_uuid() -> str:
    return str(uuid.uuid4())


class ProviderRegistry(Base):
    __tablename__ = "provider_registry"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid)
    provider_code: Mapped[str] = mapped_column(String(80), unique=True, index=True)
    display_name: Mapped[str] = mapped_column(String(160))
    purpose: Mapped[str] = mapped_column(Text)
    markets: Mapped[list[str]] = mapped_column(JSON, default=list)
    regions: Mapped[list[str]] = mapped_column(JSON, default=list)
    display_right: Mapped[bool] = mapped_column(Boolean, default=False)
    non_display_algorithm_right: Mapped[bool] = mapped_column(Boolean, default=False)
    derivative_right: Mapped[bool] = mapped_column(Boolean, default=False)
    cache_right: Mapped[bool] = mapped_column(Boolean, default=False)
    training_right: Mapped[bool] = mapped_column(Boolean, default=False)
    redistribution_right: Mapped[bool] = mapped_column(Boolean, default=False)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    review_status: Mapped[str] = mapped_column(String(24), index=True)
    reviewed_by: Mapped[str | None] = mapped_column(String(120), nullable=True)
    reviewed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    terms_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    notes: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class EtfInstrument(Base):
    __tablename__ = "etf_instruments"
    __table_args__ = (
        UniqueConstraint("mic", "provider_symbol", "provider_code"),
        Index("ix_etf_active_liquid", "active", "liquidity_tier"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    name_zh: Mapped[str] = mapped_column(String(160))
    figi: Mapped[str | None] = mapped_column(String(24), nullable=True, index=True)
    isin: Mapped[str | None] = mapped_column(String(24), nullable=True, index=True)
    mic: Mapped[str] = mapped_column(String(12))
    currency: Mapped[str] = mapped_column(String(3))
    exchange: Mapped[str] = mapped_column(String(80))
    provider_code: Mapped[str] = mapped_column(String(80))
    provider_symbol: Mapped[str] = mapped_column(String(80))
    asset_class: Mapped[str] = mapped_column(String(40), index=True)
    industry: Mapped[str] = mapped_column(String(80), default="综合")
    region: Mapped[str] = mapped_column(String(80), default="全球")
    leveraged: Mapped[bool] = mapped_column(Boolean, default=False)
    inverse: Mapped[bool] = mapped_column(Boolean, default=False)
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    inception_date: Mapped[date] = mapped_column(Date)
    termination_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    liquidity_tier: Mapped[int] = mapped_column(Integer, default=1)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class DataSnapshot(Base):
    __tablename__ = "data_snapshots"
    __table_args__ = (UniqueConstraint("snapshot_hash"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid)
    provider_code: Mapped[str] = mapped_column(String(80), index=True)
    dataset_type: Mapped[str] = mapped_column(String(40), index=True)
    source_uri: Mapped[str] = mapped_column(Text)
    as_of: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    data_mode: Mapped[str] = mapped_column(String(24), index=True)
    license_scope: Mapped[str] = mapped_column(Text)
    parquet_uri: Mapped[str] = mapped_column(Text)
    snapshot_hash: Mapped[str] = mapped_column(String(64))
    config_hash: Mapped[str] = mapped_column(String(64))
    row_count: Mapped[int] = mapped_column(Integer)
    quality_flags: Mapped[list[str]] = mapped_column(JSON, default=list)
    revision: Mapped[str] = mapped_column(String(40), default="1")


class NewsEvent(Base):
    __tablename__ = "news_events"
    __table_args__ = (
        UniqueConstraint("dedupe_hash"),
        Index("ix_news_available_relevance", "available_at", "relevance"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid)
    provider_code: Mapped[str] = mapped_column(String(80), index=True)
    normalized_url: Mapped[str] = mapped_column(Text)
    source_uri: Mapped[str] = mapped_column(Text)
    title: Mapped[str] = mapped_column(String(500))
    short_summary: Mapped[str] = mapped_column(Text)
    source_name: Mapped[str] = mapped_column(String(160))
    event_time: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    as_of: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    revision: Mapped[str] = mapped_column(String(40), default="1")
    timezone: Mapped[str] = mapped_column(String(64))
    currency: Mapped[str | None] = mapped_column(String(3), nullable=True)
    latency_class: Mapped[str] = mapped_column(String(40))
    license_scope: Mapped[str] = mapped_column(Text)
    data_mode: Mapped[str] = mapped_column(String(24), index=True)
    quality_flags: Mapped[list[str]] = mapped_column(JSON, default=list)
    raw_payload_hash: Mapped[str] = mapped_column(String(64))
    dedupe_hash: Mapped[str] = mapped_column(String(64))
    cluster_key: Mapped[str] = mapped_column(String(100), index=True)
    entities: Mapped[list[dict[str, Any]]] = mapped_column(JSON, default=list)
    exposures: Mapped[list[dict[str, Any]]] = mapped_column(JSON, default=list)
    relevance: Mapped[float] = mapped_column(Float)
    direction: Mapped[float] = mapped_column(Float)
    severity: Mapped[float] = mapped_column(Float)
    novelty: Mapped[float] = mapped_column(Float)
    source_grade: Mapped[float] = mapped_column(Float)
    record_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)


class ScheduledEvent(Base):
    __tablename__ = "scheduled_events"
    __table_args__ = (
        UniqueConstraint(
            "provider_code",
            "event_code",
            "revision",
            name="uq_scheduled_events_provider_event_revision",
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid)
    event_code: Mapped[str] = mapped_column(String(100), index=True)
    name_zh: Mapped[str] = mapped_column(String(300))
    event_type: Mapped[str] = mapped_column(String(80), index=True)
    scheduled_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    event_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    published_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    first_announced_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    as_of: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    revision: Mapped[str] = mapped_column(String(40), default="1")
    timezone: Mapped[str] = mapped_column(String(64))
    currency: Mapped[str | None] = mapped_column(String(3), nullable=True)
    latency_class: Mapped[str] = mapped_column(String(40))
    alert_lead_hours: Mapped[int] = mapped_column(Integer, default=48)
    regions: Mapped[list[str]] = mapped_column(JSON, default=list)
    asset_classes: Mapped[list[str]] = mapped_column(JSON, default=list)
    source_uri: Mapped[str] = mapped_column(Text)
    provider_code: Mapped[str] = mapped_column(String(80), index=True)
    data_mode: Mapped[str] = mapped_column(String(24), index=True)
    license_scope: Mapped[str] = mapped_column(Text)
    quality_flags: Mapped[list[str]] = mapped_column(JSON, default=list)
    raw_payload_hash: Mapped[str] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(String(24), default="SCHEDULED")
    is_current: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    record_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class ModelVersion(Base):
    __tablename__ = "model_versions"
    __table_args__ = (UniqueConstraint("model_name", "version"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid)
    model_name: Mapped[str] = mapped_column(String(100), index=True)
    version: Mapped[str] = mapped_column(String(40))
    status: Mapped[str] = mapped_column(String(24), index=True)
    feature_version: Mapped[str] = mapped_column(String(40))
    training_snapshot_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    metrics: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    limitations: Mapped[list[str]] = mapped_column(JSON, default=list)
    approved_by: Mapped[str | None] = mapped_column(String(120), nullable=True)
    approved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class Signal(Base):
    __tablename__ = "signals"
    __table_args__ = (
        UniqueConstraint("idempotency_key"),
        Index("ix_signal_instrument_generated", "instrument_id", "generated_at"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid)
    idempotency_key: Mapped[str] = mapped_column(String(64))
    instrument_id: Mapped[str] = mapped_column(ForeignKey("etf_instruments.id"), index=True)
    data_snapshot_id: Mapped[str] = mapped_column(ForeignKey("data_snapshots.id"), index=True)
    model_version_id: Mapped[str] = mapped_column(ForeignKey("model_versions.id"), index=True)
    data_as_of: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    generated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    recorded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, index=True
    )
    is_current: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    horizon_days: Mapped[int] = mapped_column(Integer)
    state: Mapped[str] = mapped_column(String(40), index=True)
    probability: Mapped[float] = mapped_column(Float)
    confidence: Mapped[float] = mapped_column(Float)
    confidence_interval: Mapped[list[float]] = mapped_column(JSON)
    market_score: Mapped[float] = mapped_column(Float)
    macro_score: Mapped[float] = mapped_column(Float)
    news_score: Mapped[float] = mapped_column(Float)
    liquidity_score: Mapped[float] = mapped_column(Float)
    composite_score: Mapped[float] = mapped_column(Float)
    supporting_evidence: Mapped[list[dict[str, Any]]] = mapped_column(JSON, default=list)
    opposing_evidence: Mapped[list[dict[str, Any]]] = mapped_column(JSON, default=list)
    source_links: Mapped[list[str]] = mapped_column(JSON, default=list)
    feature_values: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    risk_rules_hit: Mapped[list[str]] = mapped_column(JSON, default=list)
    trigger_conditions: Mapped[list[str]] = mapped_column(JSON, default=list)
    invalidation_conditions: Mapped[list[str]] = mapped_column(JSON, default=list)
    suggested_risk_budget_min: Mapped[float] = mapped_column(Float)
    suggested_risk_budget_max: Mapped[float] = mapped_column(Float)
    limitations: Mapped[list[str]] = mapped_column(JSON, default=list)
    data_mode: Mapped[str] = mapped_column(String(24), index=True)
    latency_status: Mapped[str] = mapped_column(String(24))
    code_version: Mapped[str] = mapped_column(String(80))
    snapshot: Mapped[DataSnapshot] = relationship()
    instrument: Mapped[EtfInstrument] = relationship()
    model_version: Mapped[ModelVersion] = relationship()


class Alert(Base):
    __tablename__ = "alerts"
    __table_args__ = (
        UniqueConstraint("dedupe_key"),
        Index("ix_alert_status_created", "status", "created_at"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid)
    dedupe_key: Mapped[str] = mapped_column(String(128))
    alert_type: Mapped[str] = mapped_column(String(60), index=True)
    severity: Mapped[str] = mapped_column(String(16), index=True)
    status: Mapped[str] = mapped_column(String(24), index=True)
    title: Mapped[str] = mapped_column(String(300))
    message: Mapped[str] = mapped_column(Text)
    provider_code: Mapped[str | None] = mapped_column(String(80), nullable=True, index=True)
    source_object_type: Mapped[str] = mapped_column(String(40), default="LegacyUnverified")
    source_object_id: Mapped[str | None] = mapped_column(String(80), nullable=True, index=True)
    content_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    instrument_id: Mapped[str | None] = mapped_column(
        ForeignKey("etf_instruments.id"), nullable=True, index=True
    )
    signal_id: Mapped[str | None] = mapped_column(ForeignKey("signals.id"), nullable=True)
    data_as_of: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    latency_status: Mapped[str] = mapped_column(String(24))
    trigger_reason: Mapped[str] = mapped_column(Text)
    confidence: Mapped[float] = mapped_column(Float)
    source_links: Mapped[list[str]] = mapped_column(JSON, default=list)
    invalidation_conditions: Mapped[list[str]] = mapped_column(JSON, default=list)
    disclaimer: Mapped[str] = mapped_column(Text)
    channel: Mapped[str] = mapped_column(String(40), default="IN_APP")
    delivery_attempts: Mapped[int] = mapped_column(Integer, default=0)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    acknowledged_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    acknowledged_by: Mapped[str | None] = mapped_column(String(120), nullable=True)


class SimulationDecision(Base):
    __tablename__ = "simulation_decisions"
    __table_args__ = (UniqueConstraint("idempotency_key"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid)
    idempotency_key: Mapped[str] = mapped_column(String(128))
    signal_id: Mapped[str] = mapped_column(ForeignKey("signals.id"), index=True)
    instrument_id: Mapped[str] = mapped_column(ForeignKey("etf_instruments.id"), index=True)
    decision_type: Mapped[str] = mapped_column(String(40))
    target_weight: Mapped[float] = mapped_column(Float)
    decided_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    earliest_fill_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(String(24), default="PENDING")
    rationale: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class SimulationFill(Base):
    __tablename__ = "simulation_fills"
    __table_args__ = (UniqueConstraint("idempotency_key"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid)
    idempotency_key: Mapped[str] = mapped_column(String(128))
    decision_id: Mapped[str] = mapped_column(ForeignKey("simulation_decisions.id"), index=True)
    instrument_id: Mapped[str] = mapped_column(ForeignKey("etf_instruments.id"), index=True)
    filled_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    side: Mapped[str] = mapped_column(String(8))
    quantity: Mapped[float] = mapped_column(Float)
    executable_price: Mapped[float] = mapped_column(Float)
    gross_amount: Mapped[float] = mapped_column(Float)
    fee: Mapped[float] = mapped_column(Float)
    slippage: Mapped[float] = mapped_column(Float)
    fx_rate: Mapped[float] = mapped_column(Float)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class SimulationLedger(Base):
    __tablename__ = "simulation_ledger"
    __table_args__ = (UniqueConstraint("idempotency_key"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid)
    idempotency_key: Mapped[str] = mapped_column(String(128))
    fill_id: Mapped[str | None] = mapped_column(ForeignKey("simulation_fills.id"), nullable=True)
    instrument_id: Mapped[str | None] = mapped_column(
        ForeignKey("etf_instruments.id"), nullable=True, index=True
    )
    entry_type: Mapped[str] = mapped_column(String(32))
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    cash_delta: Mapped[float] = mapped_column(Float, default=0)
    quantity_delta: Mapped[float] = mapped_column(Float, default=0)
    fee_amount: Mapped[float] = mapped_column(Float, default=0)
    memo: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class BacktestExperiment(Base):
    __tablename__ = "backtest_experiments"
    __table_args__ = (UniqueConstraint("experiment_key"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid)
    experiment_key: Mapped[str] = mapped_column(String(64))
    name: Mapped[str] = mapped_column(String(160))
    snapshot_id: Mapped[str] = mapped_column(ForeignKey("data_snapshots.id"))
    model_version_id: Mapped[str | None] = mapped_column(
        ForeignKey("model_versions.id"), nullable=True
    )
    code_version: Mapped[str] = mapped_column(String(80))
    random_seed: Mapped[int] = mapped_column(Integer)
    config: Mapped[dict[str, Any]] = mapped_column(JSON)
    metrics: Mapped[dict[str, Any]] = mapped_column(JSON)
    baseline_metrics: Mapped[dict[str, Any]] = mapped_column(JSON)
    periods: Mapped[dict[str, Any]] = mapped_column(JSON)
    leakage_checks: Mapped[dict[str, Any]] = mapped_column(JSON)
    status: Mapped[str] = mapped_column(String(24))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class TaskRun(Base):
    __tablename__ = "task_runs"
    __table_args__ = (UniqueConstraint("idempotency_key"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid)
    task_name: Mapped[str] = mapped_column(String(120), index=True)
    idempotency_key: Mapped[str] = mapped_column(String(160))
    status: Mapped[str] = mapped_column(String(32), index=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    error_code: Mapped[str | None] = mapped_column(String(80), nullable=True)
    safe_error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    result_summary: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)


class AuditLog(Base):
    __tablename__ = "audit_logs"
    __table_args__ = (Index("ix_audit_occurred_event", "occurred_at", "event_type"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_uuid)
    event_type: Mapped[str] = mapped_column(String(80), index=True)
    actor: Mapped[str] = mapped_column(String(120), default="system")
    object_type: Mapped[str] = mapped_column(String(80))
    object_id: Mapped[str | None] = mapped_column(String(80), nullable=True)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    trace_id: Mapped[str] = mapped_column(String(80), index=True)
    details: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    previous_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    record_hash: Mapped[str] = mapped_column(String(64), unique=True)


@event.listens_for(AuditLog, "before_update")
@event.listens_for(AuditLog, "before_delete")
def _prevent_audit_mutation(*_: Any, **__: Any) -> None:
    raise ValueError("审计日志为追加写，禁止更新或删除。")
