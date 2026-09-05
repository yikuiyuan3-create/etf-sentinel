from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import numpy as np
import pandas as pd

from etf_sentinel.models import EtfInstrument, NewsEvent


class InsufficientHistoryError(ValueError):
    pass


@dataclass(frozen=True)
class FactorResult:
    values: dict[str, float]
    market_score: float
    macro_score: float
    news_score: float
    liquidity_score: float
    composite_score: float
    supporting_evidence: list[dict[str, Any]]
    opposing_evidence: list[dict[str, Any]]
    source_links: list[str]


def _finite(value: float, fallback: float = 0.0) -> float:
    return float(value) if math.isfinite(float(value)) else fallback


def _clip(value: float, low: float = -1.0, high: float = 1.0) -> float:
    return float(np.clip(_finite(value), low, high))


def _return_over(series: pd.Series, periods: int) -> float:
    if len(series) <= periods or series.iloc[-periods - 1] <= 0:
        return 0.0
    return float(series.iloc[-1] / series.iloc[-periods - 1] - 1)


def _max_drawdown(series: pd.Series) -> float:
    running_max = series.cummax()
    drawdown = series / running_max - 1
    return float(drawdown.min())


def compute_rule_based_factors(
    bars: pd.DataFrame,
    instrument: EtfInstrument,
    *,
    universe_six_month_return: float,
    news_events: list[NewsEvent],
    evaluation_time: datetime,
    macro_score: float | None,
    news_weight_cap: float,
    macro_evidence: dict[str, Any] | None = None,
    macro_source_links: list[str] | None = None,
) -> FactorResult:
    ordered = bars.sort_values("event_time").copy()
    if len(ordered) < 253:
        raise InsufficientHistoryError("RuleBasedV1 至少需要 253 个当时可知的交易日。")
    total_return = ordered["total_return_close"].astype(float)
    returns = total_return.pct_change().dropna()
    recent_returns = returns.tail(63)
    momentum_1m = _return_over(total_return, 21)
    momentum_3m = _return_over(total_return, 63)
    momentum_6m = _return_over(total_return, 126)
    momentum_12m = _return_over(total_return, 252)
    sma_50 = float(total_return.tail(50).mean())
    sma_200 = float(total_return.tail(200).mean())
    trend = total_return.iloc[-1] / sma_200 - 1 if sma_200 > 0 else 0.0
    trend_strength = (sma_50 / sma_200 - 1) / max(float(returns.tail(126).std()), 1e-6)
    volatility = float(recent_returns.std(ddof=1) * np.sqrt(252))
    downside = recent_returns[recent_returns < 0]
    downside_volatility = float(downside.std(ddof=1) * np.sqrt(252)) if len(downside) > 1 else 0.0
    max_drawdown = _max_drawdown(total_return.tail(252))
    relative_strength = momentum_6m - universe_six_month_return
    latest = ordered.iloc[-1]
    median_notional = float(
        (ordered["close"].astype(float) * ordered["volume"].astype(float)).tail(63).median()
    )
    spread_bps = _finite(float(latest.get("bid_ask_spread_bps", np.nan)), 50.0)
    volume_ratio = float(ordered["volume"].iloc[-1] / max(ordered["volume"].tail(63).median(), 1))
    expense_ratio = _finite(float(latest.get("expense_ratio", np.nan)), 0.01)
    nav_premium_bps = abs(_finite(float(latest.get("nav_premium_bps", np.nan)), 25.0))
    tracking_error = _finite(float(latest.get("tracking_error", np.nan)), 0.03)

    momentum_score = (
        _clip(
            0.15 * momentum_1m + 0.25 * momentum_3m + 0.30 * momentum_6m + 0.30 * momentum_12m,
            -0.35,
            0.35,
        )
        / 0.35
    )
    trend_score = _clip(0.7 * np.tanh(trend * 10) + 0.3 * np.tanh(trend_strength / 4))
    relative_score = _clip(relative_strength / 0.20)
    risk_score = _clip(-(volatility / 0.35) + max_drawdown / 0.25)
    market_score_value = _clip(
        0.45 * momentum_score + 0.25 * trend_score + 0.20 * relative_score + 0.10 * risk_score
    )
    liquidity_score = _clip(
        0.45 * np.tanh(math.log10(max(median_notional, 1)) - 6)
        + 0.25 * np.tanh(volume_ratio - 1)
        + 0.30 * (1 - min(spread_bps / 25, 1)),
        0,
        1,
    )
    quality_score = _clip(
        1
        - min(expense_ratio / 0.02, 1) * 0.35
        - min(nav_premium_bps / 50, 1) * 0.30
        - min(tracking_error / 0.05, 1) * 0.35,
        0,
        1,
    )
    news_score, news_evidence, news_links = score_news_for_instrument(
        news_events, instrument, evaluation_time=evaluation_time
    )
    bounded_news_weight = min(max(news_weight_cap, 0.0), 0.15)
    remaining_weight = 1 - bounded_news_weight
    macro_available = macro_score is not None
    macro_value = _clip(macro_score if macro_score is not None else 0.0)
    component_weights = {
        "market": 0.56,
        "liquidity": 0.20,
        "quality": 0.10,
    }
    if macro_available:
        component_weights["macro"] = 0.14
    weight_total = sum(component_weights.values())
    non_news_score = (
        component_weights["market"] * market_score_value
        + component_weights["liquidity"] * (liquidity_score * 2 - 1)
        + component_weights["quality"] * (quality_score * 2 - 1)
        + component_weights.get("macro", 0.0) * macro_value
    ) / weight_total
    composite = _clip(remaining_weight * non_news_score + bounded_news_weight * news_score)
    values = {
        "momentum_1m": momentum_1m,
        "momentum_3m": momentum_3m,
        "momentum_6m": momentum_6m,
        "momentum_12m": momentum_12m,
        "sma_50": sma_50,
        "sma_200": sma_200,
        "trend": trend,
        "trend_strength": trend_strength,
        "annualized_volatility": volatility,
        "downside_volatility": downside_volatility,
        "max_drawdown": max_drawdown,
        "relative_strength": relative_strength,
        "median_notional_63d": median_notional,
        "volume_ratio": volume_ratio,
        "bid_ask_spread_bps": spread_bps,
        "expense_ratio": expense_ratio,
        "nav_premium_bps": nav_premium_bps,
        "tracking_error": tracking_error,
        "quality_score": quality_score,
        "macro_available": 1.0 if macro_available else 0.0,
    }
    evidence: list[dict[str, Any]] = [
        {
            "factor": "中期动量",
            "value": momentum_6m,
            "explanation": "使用当时可知的总回报序列计算 6 个月动量。",
        },
        {
            "factor": "趋势",
            "value": trend,
            "explanation": "最新总回报指数相对 200 日均线。",
        },
        {
            "factor": "流动性",
            "value": liquidity_score,
            "explanation": "由成交额、成交量变化和价差共同评估。",
        },
    ]
    if macro_available:
        evidence.append(
            macro_evidence
            or {
                "factor": "宏观风险状态",
                "value": macro_value,
                "explanation": "使用当时可知的宏观 vintage 汇总。",
            }
        )
    evidence += news_evidence
    supporting = [item for item in evidence if float(item.get("value", 0)) >= 0]
    opposing = [item for item in evidence if float(item.get("value", 0)) < 0]
    if volatility > 0.30:
        opposing.append(
            {"factor": "高波动", "value": -volatility, "explanation": "年化波动率超过 30%。"}
        )
    if max_drawdown < -0.20:
        opposing.append(
            {"factor": "回撤", "value": max_drawdown, "explanation": "近一年最大回撤较高。"}
        )
    return FactorResult(
        values={key: _finite(value) for key, value in values.items()},
        market_score=market_score_value,
        macro_score=macro_value,
        news_score=news_score,
        liquidity_score=liquidity_score,
        composite_score=composite,
        supporting_evidence=supporting,
        opposing_evidence=opposing,
        source_links=list(dict.fromkeys(news_links + (macro_source_links or []))),
    )


def score_news_for_instrument(
    events: list[NewsEvent],
    instrument: EtfInstrument,
    *,
    evaluation_time: datetime,
) -> tuple[float, list[dict[str, Any]], list[str]]:
    if evaluation_time.tzinfo is None:
        evaluation_time = evaluation_time.replace(tzinfo=UTC)
    weighted: list[tuple[float, NewsEvent, str]] = []
    for event in events:
        available = event.available_at
        if available.tzinfo is None:
            available = available.replace(tzinfo=UTC)
        if available.astimezone(UTC) > evaluation_time.astimezone(UTC):
            continue
        if not event.source_uri or event.data_mode not in {
            "LIVE_LICENSED",
            "DELAYED",
            "HISTORICAL",
            "DEMO_FIXTURE",
        }:
            continue
        if "PROMPT_INJECTION_SUSPECTED" in (event.quality_flags or []):
            continue
        reason = ""
        confidence = 0.0
        for exposure in event.exposures or []:
            kind = exposure.get("type")
            value = exposure.get("value")
            if kind == "ASSET_CLASS" and value == instrument.asset_class:
                reason = f"通过资产类别暴露关联：{instrument.asset_class}"
                confidence = float(exposure.get("confidence", 0))
                break
            if kind == "REGION" and value == instrument.region:
                reason = f"通过地区暴露关联：{instrument.region}"
                confidence = float(exposure.get("confidence", 0))
                break
            if kind == "INDUSTRY" and value == instrument.industry:
                reason = f"通过行业暴露关联：{instrument.industry}"
                confidence = float(exposure.get("confidence", 0))
                break
        if confidence < 0.60:
            continue
        age_hours = max(
            0.0,
            (evaluation_time.astimezone(UTC) - available.astimezone(UTC)).total_seconds() / 3600,
        )
        decay = math.exp(-math.log(2) * age_hours / 72)
        value = (
            event.direction
            * event.relevance
            * event.severity
            * event.novelty
            * event.source_grade
            * confidence
            * decay
        )
        weighted.append((value, event, reason))
    if not weighted:
        return 0.0, [], []
    total = float(np.clip(sum(item[0] for item in weighted), -1, 1))
    evidence = [
        {
            "factor": "新闻事件",
            "value": float(value),
            "explanation": reason,
            "title": event.title,
            "source": event.source_name,
            "first_seen_at": event.first_seen_at.isoformat(),
        }
        for value, event, reason in sorted(weighted, key=lambda item: abs(item[0]), reverse=True)[
            :5
        ]
    ]
    links = [event.source_uri for _, event, _ in weighted]
    return total, evidence, links
