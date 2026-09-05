from __future__ import annotations

import numpy as np
import pandas as pd

from etf_sentinel.enums import ModelStatus
from etf_sentinel.services import logistic_model
from etf_sentinel.services.logistic_model import purged_walk_forward_splits


def _classification_bars(rows: int = 320) -> pd.DataFrame:
    rng = np.random.default_rng(20250131)
    returns = rng.normal(0.0002, 0.012, rows)
    price = 100 * np.exp(np.cumsum(returns))
    event_time = pd.bdate_range("2023-01-02", periods=rows, tz="UTC")
    benchmark_returns = rng.normal(0.0001, 0.006, rows)
    return pd.DataFrame(
        {
            "instrument_id": "ETF-A",
            "revision": "v1",
            "event_time": event_time,
            "published_at": event_time + pd.Timedelta(minutes=5),
            "first_seen_at": event_time + pd.Timedelta(minutes=10),
            "available_at": event_time + pd.Timedelta(minutes=10),
            "as_of": event_time,
            "total_return_close": price,
            "benchmark_total_return_close": 100 * np.exp(np.cumsum(benchmark_returns)),
            "benchmark_published_at": event_time + pd.Timedelta(minutes=5),
            "benchmark_first_seen_at": event_time + pd.Timedelta(minutes=10),
            "benchmark_available_at": event_time + pd.Timedelta(minutes=10),
            "benchmark_as_of": event_time,
            "benchmark_constituent_count": 2,
            "volume": rng.integers(1_000_000, 2_000_000, rows),
            "bid_ask_spread_bps": np.full(rows, 4.0),
        }
    )


def test_walk_forward_split_is_ordered_and_enforces_purge_gap() -> None:
    splits = purged_walk_forward_splits(300, folds=3, min_train=100, purge=20, embargo=5)

    assert splits
    for train, test in splits:
        assert train.max() < test.min()
        assert test.min() - train.max() > 20


def test_each_walk_forward_fold_reserves_both_purge_and_embargo_gap() -> None:
    purge = 20
    embargo = 7
    splits = purged_walk_forward_splits(
        360,
        folds=4,
        min_train=120,
        purge=purge,
        embargo=embargo,
    )

    assert splits
    for fold, (train, test) in enumerate(splits):
        excluded_gap = int(test.min()) - int(train.max()) - 1
        assert excluded_gap >= purge + embargo, (
            f"fold {fold} excluded only {excluded_gap} samples; "
            f"required purge({purge}) + embargo({embargo})"
        )


def test_model_that_scores_worse_than_baseline_is_rejected(monkeypatch) -> None:
    scores = iter([0.35, 0.20])
    monkeypatch.setattr(logistic_model, "brier_score_loss", lambda *_args, **_kwargs: next(scores))

    result = logistic_model.evaluate_logistic_baseline(_classification_bars(), horizon_days=5)

    assert result.brier_score > result.baseline_brier_score
    assert result.status is ModelStatus.REJECTED


def test_unapproved_logistic_model_never_self_promotes_to_champion() -> None:
    result = logistic_model.evaluate_logistic_baseline(_classification_bars(), horizon_days=5)

    assert result.status in {ModelStatus.EXPERIMENTAL, ModelStatus.REJECTED}
    assert result.status is not ModelStatus.CHAMPION


def test_late_logistic_vintage_is_excluded_without_changing_point_in_time_dataset() -> None:
    bars = _classification_bars()
    expected_features, expected_labels = logistic_model.build_logistic_dataset(
        bars, horizon_days=5, cost_bps=8.0
    )
    late_revision = bars.iloc[[180]].copy()
    late_revision["available_at"] = late_revision["event_time"] + pd.Timedelta(days=1)
    late_revision["total_return_close"] = 1_000_000.0

    actual_features, actual_labels = logistic_model.build_logistic_dataset(
        pd.concat([bars, late_revision], ignore_index=True),
        horizon_days=5,
        cost_bps=8.0,
    )

    pd.testing.assert_frame_equal(actual_features, expected_features)
    pd.testing.assert_series_equal(actual_labels, expected_labels)


def test_late_other_instrument_revision_cannot_change_target_features_or_labels() -> None:
    target = _classification_bars()
    raw_columns = [
        "instrument_id",
        "revision",
        "event_time",
        "published_at",
        "first_seen_at",
        "available_at",
        "as_of",
        "total_return_close",
        "volume",
        "bid_ask_spread_bps",
    ]
    target_raw = target[raw_columns].copy()
    peer = target_raw.copy()
    peer["instrument_id"] = "ETF-B"
    peer["total_return_close"] *= 0.95
    timely_universe = pd.concat([target_raw, peer], ignore_index=True)
    _known, expected_benchmark = logistic_model.point_in_time_universe_benchmark(timely_universe)
    expected_target = target_raw.merge(
        expected_benchmark,
        on="event_time",
        how="inner",
        validate="one_to_one",
    )
    expected_features, expected_labels = logistic_model.build_logistic_dataset(
        expected_target,
        horizon_days=5,
        cost_bps=8.0,
    )

    late_revision = peer.iloc[[180]].copy()
    late_revision["revision"] = "v2-late"
    late_revision["total_return_close"] = 1_000_000_000.0
    for column in ("published_at", "first_seen_at", "available_at", "as_of"):
        late_revision[column] = late_revision["event_time"] + pd.Timedelta(days=1)
    contaminated = pd.concat([timely_universe, late_revision], ignore_index=True)
    _known, actual_benchmark = logistic_model.point_in_time_universe_benchmark(contaminated)
    actual_target = target_raw.merge(
        actual_benchmark,
        on="event_time",
        how="inner",
        validate="one_to_one",
    )
    actual_features, actual_labels = logistic_model.build_logistic_dataset(
        actual_target,
        horizon_days=5,
        cost_bps=8.0,
    )

    pd.testing.assert_frame_equal(actual_benchmark, expected_benchmark)
    pd.testing.assert_frame_equal(actual_features, expected_features)
    pd.testing.assert_series_equal(actual_labels, expected_labels)
