from __future__ import annotations

from datetime import timedelta
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import func, select

from etf_sentinel.enums import AlertSeverity, DataMode, TaskStatus
from etf_sentinel.models import (
    Alert,
    AuditLog,
    BacktestExperiment,
    DataSnapshot,
    ModelVersion,
    TaskRun,
)
from etf_sentinel.providers.base import ProviderSchemaError
from etf_sentinel.services import pipeline
from etf_sentinel.services.ingestion import (
    SnapshotIntegrityError,
    canonical_frame_hash,
    register_default_providers,
)
from etf_sentinel.services.pipeline import (
    _run_and_record_logistic,
    read_snapshot_with_duckdb,
    run_demo_pipeline,
)
from etf_sentinel.services.signals import _eligible_macro_context


def test_parquet_readback_rejects_content_tampering(tmp_path) -> None:
    path = tmp_path / "immutable.parquet"
    frame = pd.DataFrame(
        {
            "instrument_id": ["ETF-1", "ETF-1"],
            "event_time": pd.to_datetime(["2025-01-02T07:00:00Z", "2025-01-03T07:00:00Z"]),
            "available_at": pd.to_datetime(["2025-01-02T07:10:00Z", "2025-01-03T07:10:00Z"]),
            "revision": ["v1", "v1"],
            "open": [100.0, 101.0],
        }
    )
    expected_hash = canonical_frame_hash(frame)
    frame.to_parquet(path, index=False)
    read_snapshot_with_duckdb(path, expected_hash=expected_hash, expected_rows=len(frame))

    tampered = frame.copy()
    tampered.loc[1, "open"] = 10_000.0
    tampered.to_parquet(path, index=False)

    with pytest.raises(SnapshotIntegrityError, match="哈希"):
        read_snapshot_with_duckdb(path, expected_hash=expected_hash, expected_rows=len(frame))


def test_pipeline_preflight_failure_records_failed_task_and_critical_health_alert(
    db_session, test_settings, monkeypatch
) -> None:
    secret_canary = "SECRET_CANARY_MUST_NOT_BE_PERSISTED"

    def fail_preflight(*_args, **_kwargs):
        raise SnapshotIntegrityError(secret_canary)

    monkeypatch.setattr(pipeline, "ingest_demo_snapshot", fail_preflight)

    with pytest.raises(SnapshotIntegrityError, match=secret_canary):
        run_demo_pipeline(db_session, test_settings)

    task = db_session.scalar(select(TaskRun))
    alert = db_session.scalar(select(Alert))
    assert task is not None
    assert task.status == TaskStatus.FAILED.value
    assert task.error_code == "SnapshotIntegrityError"
    assert task.safe_error_message
    assert secret_canary not in task.safe_error_message
    assert alert is not None
    assert alert.alert_type == "SYSTEM_HEALTH"
    assert alert.severity == AlertSeverity.CRITICAL.value
    assert alert.latency_status == "UNAVAILABLE"
    assert alert.trigger_reason
    assert alert.invalidation_conditions
    assert secret_canary not in alert.message


def test_macro_quality_flag_from_duckdb_ndarray_still_fails_closed(
    db_session, test_settings
) -> None:
    register_default_providers(db_session)
    current = test_settings.demo_evaluation_time
    event_time = current - timedelta(hours=2)
    available_at = current - timedelta(hours=1)
    macro_frame = pd.DataFrame(
        [
            {
                "series_code": "DEMO_RISK",
                "value": 0.5,
                "provider": "demo_fixture",
                "source_uri": "https://example.invalid/macro/demo-risk",
                "event_time": event_time,
                "published_at": event_time,
                "first_seen_at": available_at,
                "available_at": available_at,
                "as_of": event_time,
                "ingested_at": available_at,
                "revision": "v1",
                "vintage": "v1",
                "timezone": "UTC",
                "currency": None,
                "latency_class": "FIXED_DEMO",
                "license_scope": "INTERNAL_TEST_AND_DEMONSTRATION_ONLY",
                "data_mode": DataMode.DEMO_FIXTURE.value,
                "quality_flags": np.array(["LICENSE_UNCLEAR"], dtype=object),
                "raw_payload_hash": "a" * 64,
            }
        ]
    )
    market_snapshot = DataSnapshot(
        provider_code="demo_fixture",
        dataset_type="MARKET_BARS",
        source_uri="internal://market-test",
        as_of=event_time,
        available_at=available_at,
        data_mode=DataMode.DEMO_FIXTURE.value,
        license_scope="INTERNAL_TEST_AND_DEMONSTRATION_ONLY",
        parquet_uri="unused-market.parquet",
        snapshot_hash="b" * 64,
        config_hash="c" * 64,
        row_count=1,
        quality_flags=[],
        revision="v1",
    )
    macro_snapshot = DataSnapshot(
        provider_code="demo_fixture",
        dataset_type="MACRO_FACTS",
        source_uri="internal://macro-test",
        as_of=event_time,
        available_at=available_at,
        data_mode=DataMode.DEMO_FIXTURE.value,
        license_scope="INTERNAL_TEST_AND_DEMONSTRATION_ONLY",
        parquet_uri="unused-macro.parquet",
        snapshot_hash=canonical_frame_hash(macro_frame),
        config_hash="d" * 64,
        row_count=len(macro_frame),
        quality_flags=[],
        revision="v1",
    )
    db_session.add_all([market_snapshot, macro_snapshot])
    db_session.commit()

    with pytest.raises(ProviderSchemaError, match="无已授权"):
        _eligible_macro_context(
            db_session,
            market_snapshot=market_snapshot,
            macro_snapshot=macro_snapshot,
            macro_frame=macro_frame,
            evaluation_time=current,
        )

    assert db_session.scalar(select(func.count(Alert.id))) == 0


@pytest.mark.parametrize("tampered_field", ["metrics", "status", "config"])
def test_backtest_idempotent_replay_recalculates_and_rejects_persisted_tampering(
    db_session,
    monkeypatch,
    tampered_field: str,
) -> None:
    snapshot = DataSnapshot(
        provider_code="demo_fixture",
        dataset_type="MARKET_BARS",
        source_uri="internal://backtest-replay",
        as_of=pd.Timestamp("2025-01-31T07:00:00Z").to_pydatetime(),
        available_at=pd.Timestamp("2025-01-31T07:10:00Z").to_pydatetime(),
        data_mode=DataMode.DEMO_FIXTURE.value,
        license_scope="INTERNAL_TEST_AND_DEMONSTRATION_ONLY",
        parquet_uri="unused-backtest-replay.parquet",
        snapshot_hash="e" * 64,
        config_hash="f" * 64,
        row_count=1,
        quality_flags=[],
        revision="test-v1",
    )
    db_session.add(snapshot)
    db_session.flush()
    deterministic = SimpleNamespace(
        metrics={"cagr": 0.01, "sharpe": 0.1, "cost_total": 0.001},
        baseline_metrics={"equal_weight": {"sharpe": 0.2}},
        periods={"test": {"cagr": 0.01}},
        leakage_checks={"next_bar_execution": True},
    )
    monkeypatch.setattr(
        pipeline, "run_reproducible_backtest", lambda *_args, **_kwargs: deterministic
    )

    recorded = pipeline._run_and_record_backtest(
        snapshot=snapshot, session=db_session, frame=pd.DataFrame()
    )
    assert isinstance(recorded, BacktestExperiment)
    experiment_count = db_session.scalar(select(func.count(BacktestExperiment.id)))
    audit_count = db_session.scalar(
        select(func.count(AuditLog.id)).where(AuditLog.event_type == "BACKTEST_RECORDED")
    )
    if tampered_field == "metrics":
        recorded.metrics = {**recorded.metrics, "cagr": 999.0}
    elif tampered_field == "status":
        recorded.status = "APPROVED_WITHOUT_REVIEW"
    else:
        recorded.config = {**recorded.config, "fee_bps": -1.0}
    db_session.commit()

    with pytest.raises(SnapshotIntegrityError, match="回测实验重放结果"):
        pipeline._run_and_record_backtest(
            snapshot=snapshot,
            session=db_session,
            frame=pd.DataFrame(),
        )
    assert db_session.scalar(select(func.count(BacktestExperiment.id))) == experiment_count
    assert (
        db_session.scalar(
            select(func.count(AuditLog.id)).where(AuditLog.event_type == "BACKTEST_RECORDED")
        )
        == audit_count
    )


def _logistic_replay_frame() -> pd.DataFrame:
    rng = np.random.default_rng(20_250_131)
    event_time = pd.bdate_range("2023-01-02", periods=340, tz="UTC")
    frames = []
    for instrument_id, drift in [("ETF-A", 0.0004), ("ETF-B", -0.0001)]:
        returns = rng.normal(drift, 0.012, len(event_time))
        frames.append(
            pd.DataFrame(
                {
                    "instrument_id": instrument_id,
                    "event_time": event_time,
                    "published_at": event_time + pd.Timedelta(minutes=5),
                    "first_seen_at": event_time + pd.Timedelta(minutes=10),
                    "available_at": event_time + pd.Timedelta(minutes=10),
                    "as_of": event_time,
                    "revision": "v1",
                    "total_return_close": 100 * np.exp(np.cumsum(returns)),
                    "volume": rng.integers(1_000_000, 2_000_000, len(event_time)),
                    "bid_ask_spread_bps": np.full(len(event_time), 4.0),
                }
            )
        )
    return pd.concat(frames, ignore_index=True)


def test_logistic_evaluation_replay_is_idempotent_and_new_snapshot_gets_new_lineage(
    db_session,
) -> None:
    frame = _logistic_replay_frame()
    first_snapshot = SimpleNamespace(snapshot_hash="1" * 64)

    first = _run_and_record_logistic(db_session, first_snapshot, frame)
    model_count = db_session.scalar(
        select(func.count(ModelVersion.id)).where(ModelVersion.model_name == "LogisticBaselineV1")
    )
    audit_count = db_session.scalar(
        select(func.count(AuditLog.id)).where(AuditLog.event_type == "MODEL_EVALUATED")
    )
    replay = _run_and_record_logistic(db_session, first_snapshot, frame)

    assert replay == first
    assert model_count == 2
    assert audit_count == 2
    assert (
        db_session.scalar(
            select(func.count(ModelVersion.id)).where(
                ModelVersion.model_name == "LogisticBaselineV1"
            )
        )
        == model_count
    )
    assert (
        db_session.scalar(
            select(func.count(AuditLog.id)).where(AuditLog.event_type == "MODEL_EVALUATED")
        )
        == audit_count
    )

    earlier_cutoff_frame = frame.loc[frame["event_time"] < frame["event_time"].max()].copy()
    changed_cutoff = _run_and_record_logistic(db_session, first_snapshot, earlier_cutoff_frame)
    assert {item["id"] for item in first}.isdisjoint(item["id"] for item in changed_cutoff)
    assert {item["version"] for item in first}.isdisjoint(
        item["version"] for item in changed_cutoff
    )
    assert {item["config"]["evaluation_cutoff"] for item in first}.isdisjoint(
        item["config"]["evaluation_cutoff"] for item in changed_cutoff
    )
    assert {item["config"]["universe_input_hash"] for item in first}.isdisjoint(
        item["config"]["universe_input_hash"] for item in changed_cutoff
    )

    changed_snapshot = _run_and_record_logistic(
        db_session, SimpleNamespace(snapshot_hash="2" * 64), frame
    )
    assert {item["id"] for item in first + changed_cutoff}.isdisjoint(
        item["id"] for item in changed_snapshot
    )
    assert {item["version"] for item in first + changed_cutoff}.isdisjoint(
        item["version"] for item in changed_snapshot
    )
    assert (
        db_session.scalar(
            select(func.count(ModelVersion.id)).where(
                ModelVersion.model_name == "LogisticBaselineV1"
            )
        )
        == 6
    )
    assert (
        db_session.scalar(
            select(func.count(AuditLog.id)).where(AuditLog.event_type == "MODEL_EVALUATED")
        )
        == 6
    )
    training_hashes = set(
        db_session.scalars(
            select(ModelVersion.training_snapshot_hash).where(
                ModelVersion.model_name == "LogisticBaselineV1"
            )
        )
    )
    assert training_hashes == {"1" * 64, "2" * 64}


@pytest.mark.parametrize("tampered_field", ["metrics", "status", "limitations"])
def test_logistic_same_key_replay_rejects_persisted_artifact_tampering(
    db_session, tampered_field: str
) -> None:
    frame = _logistic_replay_frame()
    snapshot = SimpleNamespace(snapshot_hash="3" * 64)
    first = _run_and_record_logistic(db_session, snapshot, frame)
    model = db_session.get(ModelVersion, first[0]["id"])
    assert model is not None
    model_count = db_session.scalar(
        select(func.count(ModelVersion.id)).where(ModelVersion.model_name == "LogisticBaselineV1")
    )
    audit_count = db_session.scalar(
        select(func.count(AuditLog.id)).where(AuditLog.event_type == "MODEL_EVALUATED")
    )
    if tampered_field == "metrics":
        model.metrics = {**model.metrics, "brier_score": 999.0}
    elif tampered_field == "status":
        model.status = "CHAMPION"
    else:
        model.limitations = [*model.limitations, "未授权改写"]
    db_session.commit()

    with pytest.raises(SnapshotIntegrityError, match="Logistic 模型工件完整性"):
        _run_and_record_logistic(db_session, snapshot, frame)

    assert (
        db_session.scalar(
            select(func.count(ModelVersion.id)).where(
                ModelVersion.model_name == "LogisticBaselineV1"
            )
        )
        == model_count
    )
    assert (
        db_session.scalar(
            select(func.count(AuditLog.id)).where(AuditLog.event_type == "MODEL_EVALUATED")
        )
        == audit_count
    )
