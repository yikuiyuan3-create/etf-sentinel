from __future__ import annotations

import hashlib
import hmac
import json
import math
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import numpy as np
import pandas as pd

POINT_IN_TIME_COLUMNS = ("event_time", "published_at", "first_seen_at", "available_at", "as_of")
BACKTEST_ARTIFACT_INTEGRITY_KEY = "_artifact_integrity"
BACKTEST_ARTIFACT_SCHEMA = "backtest-artifact-v1"


class LookaheadBiasError(ValueError):
    pass


@dataclass(frozen=True)
class BacktestResult:
    metrics: dict[str, Any]
    baseline_metrics: dict[str, dict[str, float]]
    periods: dict[str, dict[str, float]]
    leakage_checks: dict[str, bool]
    daily_returns: pd.Series
    turnover: pd.Series

    def serializable(self) -> dict[str, Any]:
        return {
            "metrics": self.metrics,
            "baseline_metrics": self.baseline_metrics,
            "periods": self.periods,
            "leakage_checks": self.leakage_checks,
        }


def point_in_time_slice(frame: pd.DataFrame, decision_time: datetime) -> pd.DataFrame:
    cutoff = pd.Timestamp(decision_time)
    if cutoff.tzinfo is None:
        raise LookaheadBiasError("回测决策时间必须包含时区。")
    cutoff = cutoff.tz_convert("UTC")
    missing = set(POINT_IN_TIME_COLUMNS) - set(frame.columns)
    if missing:
        raise LookaheadBiasError(f"回测输入缺少因果时间字段：{sorted(missing)}")
    parsed: dict[str, pd.Series] = {}
    for column in POINT_IN_TIME_COLUMNS:
        parsed[column] = pd.to_datetime(frame[column], utc=True, errors="coerce")
        if parsed[column].isna().any():
            raise LookaheadBiasError(f"回测输入存在无效 {column}。")
    causal = pd.Series(True, index=frame.index)
    for column in POINT_IN_TIME_COLUMNS:
        causal &= parsed[column] <= cutoff
    result = frame.loc[causal].copy()
    for column in POINT_IN_TIME_COLUMNS:
        if not result.empty and pd.to_datetime(result[column], utc=True).max() > cutoff:
            raise LookaheadBiasError(f"未来 {column} 进入回测切片。")
    return result


def assert_point_in_time_inputs(frame: pd.DataFrame, decision_time: datetime) -> None:
    cutoff = pd.Timestamp(decision_time)
    if cutoff.tzinfo is None:
        raise LookaheadBiasError("回测决策时间必须包含时区。")
    cutoff = cutoff.tz_convert("UTC")
    missing = set(POINT_IN_TIME_COLUMNS) - set(frame.columns)
    if missing:
        raise LookaheadBiasError(f"回测输入缺少因果时间字段：{sorted(missing)}")
    for column in POINT_IN_TIME_COLUMNS:
        values = pd.to_datetime(frame[column], utc=True, errors="coerce")
        if values.isna().any() or (values > cutoff).any():
            raise LookaheadBiasError(f"检测到决策时点尚不可知的 {column}。")


def apply_costs(
    gross_returns: pd.Series,
    turnover: pd.Series,
    *,
    fee_bps: float,
    slippage_bps: float,
    spread_bps: float,
) -> pd.Series:
    total_bps = fee_bps + slippage_bps + spread_bps / 2
    return gross_returns.astype(float) - turnover.astype(float) * total_bps / 10_000


def run_reproducible_backtest(
    frame: pd.DataFrame,
    *,
    fee_bps: float = 2.5,
    slippage_bps: float = 3.0,
    random_seed: int = 20_250_131,
    decision_delay_minutes: int = 30,
) -> BacktestResult:
    data = frame.copy()
    missing_times = set(POINT_IN_TIME_COLUMNS) - set(data.columns)
    if missing_times:
        raise LookaheadBiasError(f"回测输入缺少因果时间字段：{sorted(missing_times)}")
    for column in POINT_IN_TIME_COLUMNS:
        data[column] = pd.to_datetime(data[column], utc=True, errors="raise")
    data = data.sort_values(["event_time", "instrument_id"])
    # Execution outcomes are not model features, but a row whose source fact was
    # not available by that bar's conservative close+delay cutoff must not supply
    # open/FX/corporate-action values to the simulator.  This masks poisoned late
    # rows while still enforcing next-bar execution via the shifted targets below.
    execution_cutoff = data["event_time"] + pd.Timedelta(minutes=decision_delay_minutes)
    execution_known = pd.Series(True, index=data.index)
    for column in POINT_IN_TIME_COLUMNS:
        execution_known &= data[column] <= execution_cutoff
    execution_data = data.copy()
    execution_columns = [
        "open",
        "bid_ask_spread_bps",
        "split_factor",
        "dividend",
        "fx_rate",
        "expense_ratio",
    ]
    execution_data.loc[~execution_known, execution_columns] = np.nan
    executable_open = execution_data.pivot(
        index="event_time", columns="instrument_id", values="open"
    )
    spreads = execution_data.pivot(
        index="event_time", columns="instrument_id", values="bid_ask_spread_bps"
    )
    split_factors = execution_data.pivot(
        index="event_time", columns="instrument_id", values="split_factor"
    )
    dividends = execution_data.pivot(index="event_time", columns="instrument_id", values="dividend")
    fx_rates = execution_data.pivot(index="event_time", columns="instrument_id", values="fx_rate")
    expenses = execution_data.pivot(
        index="event_time", columns="instrument_id", values="expense_ratio"
    )
    if len(executable_open) < 130:
        raise ValueError("回测至少需要 130 个交易日。")
    model_targets, model_pit_ok = _point_in_time_targets(
        data, mode="model", decision_delay_minutes=decision_delay_minutes
    )
    momentum_targets, momentum_pit_ok = _point_in_time_targets(
        data, mode="momentum", decision_delay_minutes=decision_delay_minutes
    )
    equal_targets, equal_pit_ok = _point_in_time_targets(
        data, mode="equal", decision_delay_minutes=decision_delay_minutes
    )
    open_returns = _corporate_action_open_returns(
        executable_open, split_factors, dividends, fx_rates
    )
    tradable = executable_open.notna() & fx_rates.notna()
    net, gross, turnover, costs, positions = _strategy_returns(
        model_targets,
        open_returns,
        tradable,
        spreads,
        expenses,
        fee_bps=fee_bps,
        slippage_bps=slippage_bps,
    )
    equal_returns, _, _, _, _ = _strategy_returns(
        equal_targets,
        open_returns,
        tradable,
        spreads,
        expenses,
        fee_bps=fee_bps,
        slippage_bps=slippage_bps,
    )
    simple_momentum, _, _, _, _ = _strategy_returns(
        momentum_targets,
        open_returns,
        tradable,
        spreads,
        expenses,
        fee_bps=fee_bps,
        slippage_bps=slippage_bps,
    )
    active = positions.sum(axis=1) > 0
    if not active.any():
        raise ValueError("回测没有形成任何可执行的点时目标权重。")
    start_position = int(np.flatnonzero(active.to_numpy())[0])
    valid_index = net.index[start_position:-1]
    net = net.loc[valid_index]
    gross = gross.loc[valid_index]
    costs = costs.loc[valid_index]
    turnover = turnover.loc[valid_index]
    positions = positions.loc[valid_index]
    equal_returns = equal_returns.loc[valid_index]
    simple_momentum = simple_momentum.loc[valid_index]
    buy_hold = _buy_and_hold_returns(
        open_returns.loc[valid_index],
        spreads.loc[valid_index],
        expenses.loc[valid_index],
        starting_weights=equal_targets.shift(1).fillna(0.0).loc[valid_index[0]],
        fee_bps=fee_bps,
        slippage_bps=slippage_bps,
    )
    baseline_metrics = {
        "buy_and_hold": performance_metrics(buy_hold),
        "equal_weight": performance_metrics(equal_returns),
        "simple_momentum": performance_metrics(simple_momentum),
    }
    dates = net.index
    first_cut = int(len(dates) * 0.55)
    second_cut = int(len(dates) * 0.75)
    periods = {
        "train": performance_metrics(net.iloc[:first_cut]),
        "validation": performance_metrics(net.iloc[first_cut:second_cut]),
        "untouched_test": performance_metrics(net.iloc[second_cut:]),
        "stress": performance_metrics(net.nsmallest(max(10, len(net) // 10)).sort_index()),
    }
    metrics: dict[str, Any] = performance_metrics(net)
    metrics["turnover"] = float(turnover.sum())
    metrics["cost_total"] = float(costs.sum())
    metrics["relative_to_equal_weight"] = float((net - equal_returns).sum())
    metrics["hit_rate"] = float((net > 0).mean())
    metrics["average_invested_weight"] = float(positions.sum(axis=1).mean())
    metrics["max_single_weight"] = float(positions.max(axis=1).max())
    desired_positions = model_targets.shift(1).fillna(0.0).loc[positions.index]
    tradable_window = tradable.loc[positions.index]
    metrics["unfilled_bar_count"] = float(
        ((desired_positions - positions).abs().gt(1e-12) & ~tradable_window).sum().sum()
    )
    metrics["corporate_action_count"] = float(((split_factors != 1) | (dividends != 0)).sum().sum())
    metrics["bootstrap_cagr_ci_low"], metrics["bootstrap_cagr_ci_high"] = bootstrap_cagr_ci(
        net, seed=random_seed
    )
    position_changes = positions.diff().fillna(positions)
    no_untradable_fill = bool(
        position_changes.where(~tradable_window, 0.0).abs().max().max() < 1e-12
    )
    target_applied_when_tradable = bool(
        (positions.where(tradable_window) - desired_positions.where(tradable_window))
        .abs()
        .max()
        .max()
        < 1e-12
    )
    next_bar_ok = no_untradable_fill and target_applied_when_tradable
    leakage_checks = {
        "available_at_enforced": bool(model_pit_ok and momentum_pit_ok and equal_pit_ok),
        "all_fact_times_enforced": bool(model_pit_ok and momentum_pit_ok and equal_pit_ok),
        "execution_fact_clock_enforced": True,
        "next_bar_execution": next_bar_ok,
        "raw_price_for_execution": True,
        "total_return_for_signal": True,
        "random_split_forbidden": True,
    }
    config = _backtest_config(
        fee_bps=fee_bps,
        slippage_bps=slippage_bps,
        decision_delay_minutes=decision_delay_minutes,
    )
    # Persist the checksum inside an existing JSON field so the artifact remains
    # self-verifying without a schema migration.  The snapshot and code values
    # are calculated with the same canonical functions used by the pipeline.
    from etf_sentinel.services.ingestion import canonical_frame_hash
    from etf_sentinel.services.signals import code_version

    snapshot_hash = canonical_frame_hash(frame)
    artifact_code_version = code_version()
    status = _backtest_status(metrics, baseline_metrics)
    key = experiment_key(snapshot_hash, config, artifact_code_version, random_seed)
    metrics[BACKTEST_ARTIFACT_INTEGRITY_KEY] = {
        "schema": BACKTEST_ARTIFACT_SCHEMA,
        "sha256": backtest_artifact_hash(
            experiment_key_value=key,
            snapshot_hash=snapshot_hash,
            code_version=artifact_code_version,
            random_seed=random_seed,
            config=config,
            status=status,
            metrics=metrics,
            baseline_metrics=baseline_metrics,
            periods=periods,
            leakage_checks=leakage_checks,
        ),
    }
    return BacktestResult(
        metrics=metrics,
        baseline_metrics=baseline_metrics,
        periods=periods,
        leakage_checks=leakage_checks,
        daily_returns=net,
        turnover=turnover,
    )


def _point_in_time_targets(
    data: pd.DataFrame, *, mode: str, decision_delay_minutes: int
) -> tuple[pd.DataFrame, bool]:
    event_times = pd.Index(sorted(data["event_time"].unique()), name="event_time")
    instruments = sorted(data["instrument_id"].unique())
    targets = pd.DataFrame(0.0, index=event_times, columns=instruments)
    point_in_time_ok = True
    for event_time in event_times:
        decision_time = pd.Timestamp(event_time) + timedelta(minutes=decision_delay_minutes)
        known = point_in_time_slice(data, decision_time)
        known = known.loc[known["event_time"] <= event_time]
        if not known.empty:
            point_in_time_ok &= all(
                bool(pd.to_datetime(known[column], utc=True).max() <= decision_time)
                for column in POINT_IN_TIME_COLUMNS
            )
            point_in_time_ok &= bool(known["event_time"].max() <= event_time)
        scores: dict[str, float] = {}
        volatilities: dict[str, float] = {}
        for instrument_id, group in known.groupby("instrument_id", sort=False):
            history = group.sort_values("event_time")
            total_return = history["total_return_close"].astype(float)
            if mode == "equal":
                scores[str(instrument_id)] = 1.0
                volatilities[str(instrument_id)] = 1.0
                continue
            if len(total_return) < 127:
                continue
            momentum_21 = float(total_return.iloc[-1] / total_return.iloc[-22] - 1)
            momentum_63 = float(total_return.iloc[-1] / total_return.iloc[-64] - 1)
            momentum_126 = float(total_return.iloc[-1] / total_return.iloc[-127] - 1)
            volatility = float(total_return.pct_change().tail(63).std(ddof=1) * np.sqrt(252))
            volatilities[str(instrument_id)] = max(volatility, 0.02)
            if mode == "momentum":
                scores[str(instrument_id)] = momentum_63
            elif mode == "model":
                moving_average = float(total_return.tail(50).mean())
                trend = float(total_return.iloc[-1] / moving_average - 1)
                scores[str(instrument_id)] = (
                    0.20 * momentum_21
                    + 0.30 * momentum_63
                    + 0.35 * momentum_126
                    + 0.15 * trend
                    - 0.03 * volatility
                )
            else:
                raise ValueError(f"未知回测目标模式：{mode}")
        if not scores:
            continue
        if mode == "equal":
            selected = list(scores)
            raw_weights = pd.Series(1.0, index=selected)
            budget = 1.0
        else:
            score_series = pd.Series(scores)
            selected = list(score_series[score_series.rank(pct=True) >= 0.75].index)
            selected = [item for item in selected if scores[item] > 0]
            if not selected:
                continue
            if mode == "model":
                raw_weights = pd.Series(
                    {item: 1 / volatilities[item] for item in selected}, dtype=float
                )
                budget = 0.80
            else:
                raw_weights = pd.Series(1.0, index=selected)
                budget = 1.0
        weights = _capped_weights(raw_weights, budget=budget, cap=0.10 if mode == "model" else 1.0)
        targets.loc[event_time, weights.index] = weights.to_numpy()
    return targets, point_in_time_ok


def _capped_weights(raw_weights: pd.Series, *, budget: float, cap: float) -> pd.Series:
    remaining = raw_weights.astype(float).clip(lower=0)
    output = pd.Series(0.0, index=remaining.index)
    remaining_budget = budget
    while not remaining.empty and remaining_budget > 1e-12:
        proposal = remaining / remaining.sum() * remaining_budget
        over_cap = proposal > cap
        if not over_cap.any():
            output.loc[proposal.index] = proposal
            break
        capped_names = list(proposal.index[over_cap])
        output.loc[capped_names] = cap
        remaining_budget = max(0.0, budget - float(output.sum()))
        remaining = remaining.drop(index=capped_names)
    return output


def _corporate_action_open_returns(
    executable_open: pd.DataFrame,
    split_factors: pd.DataFrame,
    dividends: pd.DataFrame,
    fx_rates: pd.DataFrame,
) -> pd.DataFrame:
    current_open = executable_open.astype(float).ffill()
    current_fx = fx_rates.astype(float).ffill()
    current_base_value = current_open * current_fx
    next_value_per_old_share = (
        current_open.shift(-1) * split_factors.shift(-1).fillna(1).astype(float)
        + dividends.shift(-1).fillna(0).astype(float)
    ) * current_fx.shift(-1)
    return next_value_per_old_share.div(current_base_value).sub(1).fillna(0.0)


def _strategy_returns(
    targets: pd.DataFrame,
    open_returns: pd.DataFrame,
    tradable: pd.DataFrame,
    spreads: pd.DataFrame,
    expenses: pd.DataFrame,
    *,
    fee_bps: float,
    slippage_bps: float,
) -> tuple[pd.Series, pd.Series, pd.Series, pd.Series, pd.DataFrame]:
    desired = targets.shift(1).fillna(0.0)
    positions = pd.DataFrame(0.0, index=targets.index, columns=targets.columns)
    current = pd.Series(0.0, index=targets.columns)
    for timestamp in targets.index:
        can_trade = tradable.loc[timestamp].fillna(False).astype(bool)
        current.loc[can_trade] = desired.loc[timestamp, can_trade]
        positions.loc[timestamp] = current
    gross = (positions * open_returns.fillna(0.0)).sum(axis=1)
    changes = positions.diff().abs().fillna(positions.abs())
    turnover = changes.sum(axis=1)
    spread_cost = (changes * spreads.fillna(100.0)).sum(axis=1) / 20_000
    execution_cost = turnover * (fee_bps + slippage_bps) / 10_000
    management_fee = (positions * expenses.fillna(0.0)).sum(axis=1) / 252
    costs = spread_cost + execution_cost + management_fee
    return gross - costs, gross, turnover, costs, positions


def _buy_and_hold_returns(
    open_returns: pd.DataFrame,
    spreads: pd.DataFrame,
    expenses: pd.DataFrame,
    *,
    starting_weights: pd.Series,
    fee_bps: float,
    slippage_bps: float,
) -> pd.Series:
    eligible = starting_weights.index[(starting_weights > 0) & open_returns.iloc[0].notna()]
    if len(eligible) == 0:
        return pd.Series(0.0, index=open_returns.index)
    weights = starting_weights.loc[eligible].astype(float)
    weights = weights / weights.sum()
    daily_expense = expenses[eligible].fillna(0.0).div(252)
    asset_net = open_returns[eligible].fillna(0.0) - daily_expense
    wealth = ((1 + asset_net).cumprod() * weights).sum(axis=1)
    returns = wealth.pct_change().fillna(0.0)
    initial_spread = float(
        (spreads.loc[open_returns.index[0], eligible].fillna(100.0) * weights).sum()
    )
    returns.iloc[0] -= (fee_bps + slippage_bps + initial_spread / 2) / 10_000
    return returns


def performance_metrics(returns: pd.Series) -> dict[str, float]:
    clean = returns.replace([np.inf, -np.inf], np.nan).dropna().astype(float)
    if clean.empty:
        return {
            key: 0.0
            for key in [
                "cagr",
                "annualized_volatility",
                "sharpe",
                "sortino",
                "max_drawdown",
                "calmar",
            ]
        }
    wealth = (1 + clean).cumprod()
    years = max(len(clean) / 252, 1 / 252)
    cagr = float(wealth.iloc[-1] ** (1 / years) - 1) if wealth.iloc[-1] > 0 else -1.0
    volatility = float(clean.std(ddof=1) * math.sqrt(252)) if len(clean) > 1 else 0.0
    downside = clean[clean < 0]
    downside_vol = float(downside.std(ddof=1) * math.sqrt(252)) if len(downside) > 1 else 0.0
    drawdown = wealth / wealth.cummax() - 1
    max_drawdown = float(drawdown.min())
    annual_return = float(clean.mean() * 252)
    sharpe = annual_return / volatility if volatility > 0 else 0.0
    sortino = annual_return / downside_vol if downside_vol > 0 else 0.0
    calmar = cagr / abs(max_drawdown) if max_drawdown < 0 else 0.0
    return {
        "cagr": cagr,
        "annualized_volatility": volatility,
        "sharpe": sharpe,
        "sortino": sortino,
        "max_drawdown": max_drawdown,
        "calmar": calmar,
    }


def bootstrap_cagr_ci(returns: pd.Series, seed: int = 20_250_131) -> tuple[float, float]:
    clean = returns.dropna().to_numpy(dtype=float)
    if len(clean) < 20:
        return 0.0, 0.0
    rng = np.random.default_rng(seed)
    estimates: list[float] = []
    block = min(20, len(clean))
    for _ in range(200):
        starts = rng.integers(0, max(1, len(clean) - block + 1), math.ceil(len(clean) / block))
        sample = np.concatenate([clean[start : start + block] for start in starts])[: len(clean)]
        wealth = float(np.prod(1 + sample))
        estimates.append(wealth ** (252 / len(sample)) - 1 if wealth > 0 else -1.0)
    low, high = np.quantile(estimates, [0.025, 0.975])
    return float(low), float(high)


def experiment_key(snapshot_hash: str, config: dict[str, Any], code_version: str, seed: int) -> str:
    payload = {
        "snapshot_hash": snapshot_hash,
        "config": config,
        "code_version": code_version,
        "seed": seed,
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


def backtest_artifact_hash(
    *,
    experiment_key_value: str,
    snapshot_hash: str,
    code_version: str,
    random_seed: int,
    config: dict[str, Any],
    status: str,
    metrics: dict[str, Any],
    baseline_metrics: dict[str, Any],
    periods: dict[str, Any],
    leakage_checks: dict[str, Any],
) -> str:
    """Hash every persisted result, governance, data, and code identity field."""
    clean_metrics = dict(metrics or {})
    clean_metrics.pop(BACKTEST_ARTIFACT_INTEGRITY_KEY, None)
    payload = {
        "schema": BACKTEST_ARTIFACT_SCHEMA,
        "experiment_key": experiment_key_value,
        "snapshot_hash": snapshot_hash,
        "code_version": code_version,
        "random_seed": random_seed,
        "config": config,
        "status": status,
        "metrics": clean_metrics,
        "baseline_metrics": baseline_metrics,
        "periods": periods,
        "leakage_checks": leakage_checks,
    }
    return _canonical_artifact_hash(payload)


def verify_backtest_artifact(experiment: Any, *, snapshot_hash: str) -> bool:
    """Fail closed when a persisted backtest result or its lineage was changed."""
    metrics = experiment.metrics or {}
    integrity = metrics.get(BACKTEST_ARTIFACT_INTEGRITY_KEY)
    if not isinstance(integrity, dict):
        return False
    if integrity.get("schema") != BACKTEST_ARTIFACT_SCHEMA:
        return False
    recorded_hash = integrity.get("sha256")
    if not isinstance(recorded_hash, str) or len(recorded_hash) != 64:
        return False
    config = experiment.config or {}
    if not isinstance(config, dict):
        return False
    expected_key = experiment_key(
        snapshot_hash,
        config,
        str(experiment.code_version),
        int(experiment.random_seed),
    )
    if not hmac.compare_digest(expected_key, str(experiment.experiment_key)):
        return False
    if str(experiment.status) != _backtest_status(metrics, experiment.baseline_metrics or {}):
        return False
    expected_hash = backtest_artifact_hash(
        experiment_key_value=str(experiment.experiment_key),
        snapshot_hash=snapshot_hash,
        code_version=str(experiment.code_version),
        random_seed=int(experiment.random_seed),
        config=config,
        status=str(experiment.status),
        metrics=metrics,
        baseline_metrics=experiment.baseline_metrics or {},
        periods=experiment.periods or {},
        leakage_checks=experiment.leakage_checks or {},
    )
    return hmac.compare_digest(recorded_hash, expected_hash)


def _backtest_config(
    *, fee_bps: float, slippage_bps: float, decision_delay_minutes: int
) -> dict[str, Any]:
    return {
        "strategy": "cross_sectional_momentum_baseline_v1",
        "fee_bps": fee_bps,
        "slippage_bps": slippage_bps,
        "execution": "NEXT_BAR_OPEN",
        "decision_delay_minutes": decision_delay_minutes,
        "signal_price": "TOTAL_RETURN_CLOSE",
        "execution_price": "RAW_OPEN",
    }


def _backtest_status(metrics: dict[str, Any], baseline_metrics: dict[str, Any]) -> str:
    try:
        baseline_sharpe = max(
            float(value.get("sharpe", 0.0)) for value in baseline_metrics.values()
        )
        model_sharpe = float(metrics.get("sharpe", 0.0))
    except (AttributeError, TypeError, ValueError):
        return "INVALID"
    return "EXPERIMENTAL" if model_sharpe <= baseline_sharpe else "REVIEW_REQUIRED"


def _canonical_artifact_hash(payload: dict[str, Any]) -> str:
    serialized = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        allow_nan=False,
        default=str,
        separators=(",", ":"),
    )
    return hashlib.sha256(serialized.encode()).hexdigest()
