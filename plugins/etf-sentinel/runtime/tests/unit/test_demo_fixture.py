from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from etf_sentinel.enums import DataMode
from etf_sentinel.providers.base import FACT_COLUMNS
from etf_sentinel.providers.demo import (
    DemoFixtureProvider,
    dataframe_hash,
    default_demo_identifiers,
)


def test_demo_pool_has_exactly_sixty_nonleveraged_noninverse_liquid_etfs() -> None:
    rows = DemoFixtureProvider().fetch_etf_metadata()

    assert len(rows) == 60
    assert len({row["id"] for row in rows}) == 60
    assert len({row["provider_symbol"] for row in rows}) == 60
    assert {row["asset_class"] for row in rows} == {"股票", "债券", "商品", "地区", "行业"}
    assert all(row["leveraged"] is False for row in rows)
    assert all(row["inverse"] is False for row in rows)
    assert all(row["active"] is True for row in rows)
    assert all(row["liquidity_tier"] <= 2 for row in rows)


def test_demo_market_facts_are_reproducible_and_unmistakably_labeled() -> None:
    provider = DemoFixtureProvider()
    identifiers = default_demo_identifiers(provider)[:2]

    first = provider.fetch_bars(identifiers, date(2024, 1, 1), date(2025, 1, 31))
    second = provider.fetch_bars(identifiers, date(2024, 1, 1), date(2025, 1, 31))

    assert dataframe_hash(first) == dataframe_hash(second)
    assert FACT_COLUMNS.issubset(first.columns)
    assert set(first["data_mode"]) == {DataMode.DEMO_FIXTURE.value}
    assert first["source_uri"].str.contains("fixtures", regex=False).all()
    assert first["license_scope"].eq("INTERNAL_TEST_AND_DEMONSTRATION_ONLY").all()
    assert first["raw_payload_hash"].str.fullmatch(r"[0-9a-f]{64}").all()


def test_demo_fixture_carries_split_dividend_and_distinct_price_series() -> None:
    provider = DemoFixtureProvider()
    first_identifier = default_demo_identifiers(provider)[0]
    frame = provider.fetch_bars([first_identifier], date(2023, 11, 13), date(2025, 1, 31))

    split_rows = frame.loc[frame["split_factor"] != 1]
    dividend_rows = frame.loc[frame["dividend"] > 0]
    assert len(split_rows) == 1
    assert len(dividend_rows) == 1
    assert not frame["close"].equals(frame["total_return_close"])
    assert pd.to_datetime(frame["event_time"], utc=True).dt.tz is not None


def test_demo_corporate_action_days_preserve_raw_adjusted_and_total_return_economics() -> None:
    provider = DemoFixtureProvider()
    first_identifier = default_demo_identifiers(provider)[0]
    frame = provider.fetch_bars(
        [first_identifier], date(2023, 11, 13), date(2025, 1, 31)
    ).reset_index(drop=True)

    # ``close`` is executable raw price. ``adjusted_close`` only removes split
    # discontinuities, while ``total_return_close`` additionally reinvests cash
    # distributions.  Keep all three meanings independently observable.
    cumulative_split = frame["split_factor"].cumprod()
    for index, row in frame.iterrows():
        assert float(row["adjusted_close"]) == pytest.approx(
            float(row["close"] * cumulative_split.iloc[index]),
            rel=1e-8,
            abs=2e-6,
        )

    action_indexes = frame.index[(frame["split_factor"] != 1.0) | (frame["dividend"] > 0.0)]
    assert len(action_indexes) == 2
    for index in action_indexes:
        assert index > 0
        previous = frame.iloc[index - 1]
        current = frame.iloc[index]
        raw_holder_growth = (
            float(current["close"]) * float(current["split_factor"]) + float(current["dividend"])
        ) / float(previous["close"])
        reported_total_return_growth = float(current["total_return_close"]) / float(
            previous["total_return_close"]
        )
        assert raw_holder_growth == pytest.approx(
            reported_total_return_growth,
            rel=1e-8,
            abs=2e-8,
        )

    split_row = frame.loc[frame["split_factor"] != 1.0].iloc[0]
    assert float(split_row["close"]) != pytest.approx(float(split_row["adjusted_close"]))
    dividend_row = frame.loc[frame["dividend"] > 0.0].iloc[0]
    assert float(dividend_row["total_return_close"]) > float(dividend_row["adjusted_close"])
