from __future__ import annotations

from datetime import UTC, datetime
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import pytest

from etf_sentinel.providers.demo import DemoFixtureProvider, default_demo_identifiers
from etf_sentinel.services.backtest import (
    LookaheadBiasError,
    _corporate_action_open_returns,
    apply_costs,
    assert_point_in_time_inputs,
    point_in_time_slice,
    run_reproducible_backtest,
)


def test_corporate_action_and_fx_open_returns_preserve_economic_value() -> None:
    index = pd.date_range("2025-01-02", periods=2, tz="UTC")
    columns = ["ETF"]

    split_return = _corporate_action_open_returns(
        pd.DataFrame([100.0, 50.0], index=index, columns=columns),
        pd.DataFrame([1.0, 2.0], index=index, columns=columns),
        pd.DataFrame([0.0, 0.0], index=index, columns=columns),
        pd.DataFrame([1.0, 1.0], index=index, columns=columns),
    )
    dividend_return = _corporate_action_open_returns(
        pd.DataFrame([100.0, 100.0], index=index, columns=columns),
        pd.DataFrame([1.0, 1.0], index=index, columns=columns),
        pd.DataFrame([0.0, 5.0], index=index, columns=columns),
        pd.DataFrame([1.0, 1.0], index=index, columns=columns),
    )
    fx_return = _corporate_action_open_returns(
        pd.DataFrame([100.0, 100.0], index=index, columns=columns),
        pd.DataFrame([1.0, 1.0], index=index, columns=columns),
        pd.DataFrame([0.0, 0.0], index=index, columns=columns),
        pd.DataFrame([1.0, 1.1], index=index, columns=columns),
    )

    assert split_return.iloc[0, 0] == pytest.approx(0.0)
    assert dividend_return.iloc[0, 0] == pytest.approx(0.05)
    assert fx_return.iloc[0, 0] == pytest.approx(0.10)


def test_future_available_at_is_excluded_and_rejected_from_direct_inputs() -> None:
    decision_time = datetime(2025, 1, 2, 8, 0, tzinfo=UTC)
    fact_times = [
        datetime(2025, 1, 2, 7, 59, tzinfo=UTC),
        datetime(2025, 1, 2, 8, 1, tzinfo=UTC),
    ]
    frame = pd.DataFrame({"row": ["known", "future"]})
    for column in ("event_time", "published_at", "first_seen_at", "available_at", "as_of"):
        frame[column] = fact_times

    sliced = point_in_time_slice(frame, decision_time)

    assert sliced["row"].tolist() == ["known"]
    with pytest.raises(LookaheadBiasError, match="尚不可知"):
        assert_point_in_time_inputs(frame, decision_time)


def test_naive_decision_clock_is_rejected() -> None:
    frame = pd.DataFrame(
        {
            column: [datetime(2025, 1, 1, tzinfo=UTC)]
            for column in (
                "event_time",
                "published_at",
                "first_seen_at",
                "available_at",
                "as_of",
            )
        }
    )

    with pytest.raises(LookaheadBiasError, match="必须包含时区"):
        point_in_time_slice(frame, datetime(2025, 1, 2))


def test_dst_fold_and_cross_timezone_cutoff_do_not_admit_future_fact() -> None:
    # 01:30 occurs twice when New York leaves DST; fold=1 is 06:30 UTC.
    decision_time = datetime(2024, 11, 3, 1, 30, tzinfo=ZoneInfo("America/New_York"), fold=1)
    fact_times = [
        datetime(2024, 11, 3, 6, 29, tzinfo=UTC),
        datetime(2024, 11, 3, 6, 31, tzinfo=UTC),
    ]
    frame = pd.DataFrame({"row": ["before", "after"]})
    for column in ("event_time", "published_at", "first_seen_at", "available_at", "as_of"):
        frame[column] = fact_times

    assert point_in_time_slice(frame, decision_time)["row"].tolist() == ["before"]


def test_higher_cost_or_slippage_cannot_increase_any_net_period_return() -> None:
    gross = pd.Series([0.01, -0.02, 0.03, 0.0])
    turnover = pd.Series([1.0, 0.25, 0.0, 2.0])
    low = apply_costs(gross, turnover, fee_bps=1, slippage_bps=1, spread_bps=2)
    high = apply_costs(gross, turnover, fee_bps=10, slippage_bps=15, spread_bps=20)

    assert (high <= low + 1e-15).all()
    assert (high.loc[turnover > 0] < low.loc[turnover > 0]).all()


def test_reproducible_backtest_cost_increase_does_not_increase_net_result() -> None:
    provider = DemoFixtureProvider()
    identifiers = default_demo_identifiers(provider)[:6]
    frame = provider.fetch_bars(
        identifiers,
        datetime(2024, 1, 1).date(),
        datetime(2025, 1, 31).date(),
    )

    low = run_reproducible_backtest(frame, fee_bps=0.0, slippage_bps=0.0)
    high = run_reproducible_backtest(frame, fee_bps=20.0, slippage_bps=30.0)

    assert np.allclose(low.turnover.to_numpy(), high.turnover.to_numpy())
    assert (high.daily_returns <= low.daily_returns + 1e-15).all()
    assert high.daily_returns.sum() <= low.daily_returns.sum() + 1e-15
    assert high.metrics["cost_total"] >= low.metrics["cost_total"]


def test_same_fixture_produces_identical_backtest_output() -> None:
    provider = DemoFixtureProvider()
    frame = provider.fetch_bars(
        default_demo_identifiers(provider)[:4],
        datetime(2024, 1, 1).date(),
        datetime(2025, 1, 31).date(),
    )

    first = run_reproducible_backtest(frame)
    second = run_reproducible_backtest(frame)

    assert first.serializable() == second.serializable()
    pd.testing.assert_series_equal(first.daily_returns, second.daily_returns)


def test_backtest_rejects_or_excludes_rows_unavailable_until_after_test_end() -> None:
    provider = DemoFixtureProvider()
    frame = provider.fetch_bars(
        default_demo_identifiers(provider)[:6],
        datetime(2024, 1, 1).date(),
        datetime(2025, 1, 31).date(),
    )
    victim_id = sorted(frame["instrument_id"].unique())[0]
    unavailable = frame["instrument_id"].eq(victim_id)
    clean = frame.loc[~unavailable].copy()
    contaminated = frame.copy()
    contaminated.loc[unavailable, "available_at"] = datetime(2030, 1, 1, tzinfo=UTC)
    contaminated.loc[unavailable, "total_return_close"] = np.linspace(
        1.0, 100_000.0, int(unavailable.sum())
    )

    clean_result = run_reproducible_backtest(clean)
    try:
        future_result = run_reproducible_backtest(contaminated)
    except LookaheadBiasError:
        return  # Explicit fail-closed rejection also satisfies the requirement.

    pd.testing.assert_series_equal(
        future_result.daily_returns,
        clean_result.daily_returns,
        check_names=False,
        obj="rows unavailable during the whole test must not affect returns",
    )


def test_future_single_bar_execution_and_corporate_action_pollution_cannot_affect_positions() -> (
    None
):
    provider = DemoFixtureProvider()
    frame = provider.fetch_bars(
        default_demo_identifiers(provider)[:6],
        datetime(2024, 1, 1).date(),
        datetime(2025, 1, 31).date(),
    )
    victim_id = sorted(frame["instrument_id"].unique())[0]
    victim_rows = frame.index[frame["instrument_id"].eq(victim_id)]
    contaminated_index = victim_rows[-10]
    clean = frame.drop(index=contaminated_index).reset_index(drop=True)
    contaminated = frame.copy()
    contaminated.loc[contaminated_index, "available_at"] = datetime(2030, 1, 1, tzinfo=UTC)
    contaminated.loc[contaminated_index, "open"] = 1_000_000_000.0
    contaminated.loc[contaminated_index, "fx_rate"] = 99.0
    contaminated.loc[contaminated_index, "split_factor"] = 10.0
    contaminated.loc[contaminated_index, "dividend"] = 1_000_000.0

    clean_result = run_reproducible_backtest(clean)
    try:
        contaminated_result = run_reproducible_backtest(contaminated)
    except LookaheadBiasError:
        return

    pd.testing.assert_series_equal(
        contaminated_result.daily_returns,
        clean_result.daily_returns,
        check_names=False,
    )
    assert contaminated_result.baseline_metrics["equal_weight"] == pytest.approx(
        clean_result.baseline_metrics["equal_weight"]
    )
    assert contaminated_result.baseline_metrics["buy_and_hold"] == pytest.approx(
        clean_result.baseline_metrics["buy_and_hold"]
    )
