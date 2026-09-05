from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict


class HealthResponse(BaseModel):
    status: str
    trading_mode: str
    data_mode: str
    database: str


class SignalResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    instrument_id: str
    data_snapshot_id: str
    model_version_id: str
    horizon_days: int
    state: str
    probability: float
    confidence: float
    confidence_interval: list[float]
    market_score: float
    macro_score: float
    news_score: float
    liquidity_score: float
    composite_score: float
    data_as_of: datetime
    available_at: datetime
    data_mode: str
    latency_status: str
    risk_rules_hit: list[str]
    supporting_evidence: list[dict[str, Any]]
    opposing_evidence: list[dict[str, Any]]
    source_links: list[str]
    feature_values: dict[str, Any]
    trigger_conditions: list[str]
    invalidation_conditions: list[str]
    suggested_risk_budget_min: float
    suggested_risk_budget_max: float
    limitations: list[str]
    code_version: str
    is_current: bool


class ApiEnvelope(BaseModel):
    data_mode: str
    disclaimer: str
    data: Any
