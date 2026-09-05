from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

import numpy as np
import pandas as pd
from sqlalchemy.orm import Session

from etf_sentinel.config import Settings
from etf_sentinel.enums import DataMode
from etf_sentinel.models import DataSnapshot, EtfInstrument
from etf_sentinel.providers.base import LicenseGateError, authorize_provider


@dataclass(frozen=True)
class RiskAssessment:
    blocked: bool
    stale: bool
    rules_hit: list[str] = field(default_factory=list)


def assess_signal_risk(
    session: Session,
    *,
    instrument: EtfInstrument,
    snapshot: DataSnapshot,
    bars: pd.DataFrame,
    evaluation_time: datetime,
    settings: Settings,
) -> RiskAssessment:
    rules: list[str] = []
    if instrument.leveraged:
        rules.append("LEVERAGED_ETF_FORBIDDEN")
    if instrument.inverse:
        rules.append("INVERSE_ETF_FORBIDDEN")
    if not instrument.active:
        rules.append("INSTRUMENT_INACTIVE")
    if instrument.liquidity_tier > 2:
        rules.append("LOW_LIQUIDITY_FORBIDDEN")
    if settings.global_kill_switch:
        rules.append("GLOBAL_KILL_SWITCH")
    if bars.empty:
        rules.append("NO_AVAILABLE_BARS")
        return RiskAssessment(blocked=True, stale=True, rules_hit=rules)
    try:
        authorize_provider(
            session,
            snapshot.provider_code,
            purposes={"display", "algorithm", "derivative", "cache"},
            at=datetime.now(UTC),
            market=instrument.mic,
            region=instrument.region,
        )
    except LicenseGateError:
        rules.append("PROVIDER_LICENSE_BLOCKED")
    if "provider" not in bars.columns:
        rules.append("DATA_PROVIDER_MISSING")
    else:
        fact_providers = set(bars["provider"].dropna().astype(str).unique())
        if fact_providers != {snapshot.provider_code}:
            rules.append("DATA_PROVIDER_MISMATCH")
    if "license_scope" not in bars.columns:
        rules.append("LICENSE_SCOPE_MISSING")
    else:
        fact_scopes = set(bars["license_scope"].dropna().astype(str).unique())
        if fact_scopes != {snapshot.license_scope}:
            rules.append("LICENSE_SCOPE_MISMATCH")
    modes = set(bars["data_mode"].astype(str).unique())
    if modes != {snapshot.data_mode}:
        rules.append("DATA_MODE_MISMATCH")
    if not modes.issubset({item.value for item in DataMode}):
        rules.append("INVALID_DATA_MODE")
    latest_available = pd.to_datetime(bars["available_at"], utc=True).max().to_pydatetime()
    current = evaluation_time if evaluation_time.tzinfo else evaluation_time.replace(tzinfo=UTC)
    event_times = pd.to_datetime(bars["event_time"], utc=True, errors="coerce")
    as_of_times = pd.to_datetime(bars["as_of"], utc=True, errors="coerce")
    invalid_fact_time = event_times.isna().any() or as_of_times.isna().any()
    if invalid_fact_time:
        rules.append("INVALID_FACT_TIME")
    if (event_times > current).any() or (as_of_times > current).any():
        rules.append("FUTURE_FACT_TIME")
    stale_after = timedelta(minutes=settings.data_stale_after_minutes)
    if invalid_fact_time:
        stale = True
    else:
        latest_fact_time = min(event_times.max().to_pydatetime(), as_of_times.max().to_pydatetime())
        stale = (
            current.astimezone(UTC) - latest_available.astimezone(UTC) > stale_after
            or current.astimezone(UTC) - latest_fact_time.astimezone(UTC) > stale_after
        )
    if stale:
        rules.append("DATA_STALE")
    latest = bars.sort_values("event_time").iloc[-1]
    if pd.isna(latest.get("close")) or float(latest.get("close", 0)) <= 0:
        rules.append("INVALID_PRICE")
    spread = float(latest.get("bid_ask_spread_bps", 9999))
    if pd.isna(spread) or spread > 25:
        rules.append("SPREAD_TOO_WIDE_OR_UNKNOWN")
    if len(bars.tail(10)) < 10:
        rules.append("INSUFFICIENT_RECENT_BARS")
    if snapshot.data_mode != DataMode.DEMO_FIXTURE.value:
        rules.append("REAL_DATA_CANDIDATE_GOVERNANCE_BLOCKED")
    blocking_quality_flags = {
        "CORPORATE_ACTION_ADJUSTMENT_NOT_VERIFIED",
        "LICENSE_UNCLEAR",
        "SCHEMA_UNVERIFIED",
        "TIME_VALIDATION_FAILED",
    }
    row_flags: set[str] = set()
    for flags in bars.get("quality_flags", pd.Series(dtype=object)).dropna():
        values = flags.tolist() if isinstance(flags, np.ndarray) else flags
        if not isinstance(values, (list, tuple, set)):
            values = [values]
        row_flags.update(str(flag) for flag in values)
    if blocking_quality_flags.intersection(row_flags | set(snapshot.quality_flags or [])):
        rules.append("DATA_QUALITY_GATE_BLOCKED")
    return RiskAssessment(blocked=bool(rules), stale=stale, rules_hit=rules)


def concentration_rules(
    proposed_weights: dict[str, float],
    instruments: dict[str, EtfInstrument],
    settings: Settings,
) -> list[str]:
    rules: list[str] = []
    for instrument_id, weight in proposed_weights.items():
        if weight > settings.single_etf_cap + 1e-9:
            rules.append(f"SINGLE_ETF_CAP:{instrument_id}")
    for attribute, cap, code in [
        ("asset_class", settings.asset_class_cap, "ASSET_CLASS_CAP"),
        ("industry", settings.industry_cap, "INDUSTRY_CAP"),
        ("region", settings.region_cap, "REGION_CAP"),
    ]:
        totals: dict[str, float] = {}
        for instrument_id, weight in proposed_weights.items():
            key = str(getattr(instruments[instrument_id], attribute))
            totals[key] = totals.get(key, 0.0) + weight
        for key, total in totals.items():
            if total > cap + 1e-9:
                rules.append(f"{code}:{key}")
    if sum(proposed_weights.values()) > 1 - settings.cash_floor + 1e-9:
        rules.append("CASH_FLOOR")
    return rules
