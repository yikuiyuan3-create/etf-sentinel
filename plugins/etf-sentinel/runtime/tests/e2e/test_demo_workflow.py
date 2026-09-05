from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from sqlalchemy import func, select

from etf_sentinel.enums import DISCLAIMER, DataMode, ModelStatus
from etf_sentinel.models import (
    Alert,
    AuditLog,
    BacktestExperiment,
    DataSnapshot,
    EtfInstrument,
    ModelVersion,
    NewsEvent,
    Signal,
    SimulationDecision,
    SimulationFill,
    SimulationLedger,
    TaskRun,
)
from etf_sentinel.services import pipeline
from etf_sentinel.services.alerts import verify_alert_content
from etf_sentinel.services.ingestion import SnapshotIntegrityError
from etf_sentinel.services.pipeline import run_demo_pipeline
from etf_sentinel.services.signals import verify_signal_record


@pytest.mark.e2e
def test_demo_pipeline_is_complete_traceable_and_idempotent(
    db_session, test_settings, monkeypatch
) -> None:
    first = run_demo_pipeline(db_session, test_settings)
    counts_after_first = {
        model: db_session.scalar(select(func.count(model.id)))
        for model in [
            DataSnapshot,
            EtfInstrument,
            NewsEvent,
            Signal,
            Alert,
            SimulationDecision,
            SimulationFill,
            SimulationLedger,
            BacktestExperiment,
            TaskRun,
        ]
    }
    repeated = run_demo_pipeline(db_session, test_settings)
    counts_after_replay = {
        model: db_session.scalar(select(func.count(model.id))) for model in counts_after_first
    }

    assert first["data_mode"] == DataMode.DEMO_FIXTURE.value
    assert first["etf_count"] == 60
    assert first["signal_count"] == 60
    assert first["fact_count"] > first["point_in_time_fact_count"] > 0
    assert first["macro_fact_count"] > 0
    assert first["macro_snapshot_id"]
    assert len(first["macro_snapshot_hash"]) == 64
    assert first["alert_count"] > 0
    assert first["fill_count"] > 0
    assert first["backtest_status"] in {"EXPERIMENTAL", "REVIEW_REQUIRED"}
    assert repeated["idempotent_replay"] is True
    assert counts_after_replay == counts_after_first

    signals = list(db_session.scalars(select(Signal)))
    assert len(signals) == 60
    assert all(signal.data_mode == DataMode.DEMO_FIXTURE.value for signal in signals)
    assert all(signal.data_snapshot_id for signal in signals)
    assert all(signal.model_version_id for signal in signals)
    assert all(signal.code_version for signal in signals)
    assert all(signal.source_links for signal in signals)
    assert all(signal.available_at <= signal.generated_at for signal in signals)
    assert all(signal.is_current for signal in signals)
    assert all(verify_signal_record(signal) for signal in signals)
    assert all(
        signal.feature_values["macro_lineage"]["status"] == "AVAILABLE"
        and signal.feature_values["macro_lineage"]["snapshot_id"] == first["macro_snapshot_id"]
        and signal.feature_values["macro_lineage"]["snapshot_hash"] == first["macro_snapshot_hash"]
        for signal in signals
    )
    assert {snapshot.dataset_type for snapshot in db_session.scalars(select(DataSnapshot))} == {
        "MARKET_BARS",
        "MACRO_FACTS",
    }

    alerts = list(db_session.scalars(select(Alert)))
    assert alerts
    assert all(alert.channel == "IN_APP" for alert in alerts)
    assert all(alert.disclaimer == DISCLAIMER for alert in alerts)
    assert all(alert.data_as_of and alert.latency_status for alert in alerts)
    assert all(alert.trigger_reason and alert.invalidation_conditions for alert in alerts)

    models = list(db_session.scalars(select(ModelVersion)))
    logistic = [model for model in models if model.model_name == "LogisticBaselineV1"]
    assert {model.metrics["horizon_days"] for model in logistic} == {5, 20}
    assert all(
        model.version.startswith(f"1.0.0-h{model.metrics['horizon_days']}-")
        and model.metrics["evaluation_key"].startswith(model.version.rsplit("-", 1)[-1])
        for model in logistic
    )
    assert all(
        model.status in {ModelStatus.EXPERIMENTAL.value, ModelStatus.REJECTED.value}
        for model in logistic
    )

    backtest = db_session.scalar(select(BacktestExperiment))
    assert backtest is not None
    strongest_baseline = max(
        metrics.get("sharpe", 0.0) for metrics in backtest.baseline_metrics.values()
    )
    if backtest.metrics.get("sharpe", 0.0) <= strongest_baseline:
        assert backtest.status in {"EXPERIMENTAL", "REJECTED"}

    # A successful replay must recalculate and compare persisted backtest output,
    # rather than blindly trusting an earlier SUCCEEDED task.
    original_backtest_metrics = dict(backtest.metrics)
    deterministic_backtest = SimpleNamespace(
        metrics=original_backtest_metrics,
        baseline_metrics=backtest.baseline_metrics,
        periods=backtest.periods,
        leakage_checks=backtest.leakage_checks,
    )
    monkeypatch.setattr(
        pipeline,
        "run_reproducible_backtest",
        lambda *_args, **_kwargs: deterministic_backtest,
    )
    monkeypatch.setattr(
        pipeline,
        "_run_and_record_logistic",
        lambda *_args, **_kwargs: first["logistic_models"],
    )
    backtest.metrics = {**original_backtest_metrics, "cagr": 999.0}
    db_session.commit()

    with pytest.raises(SnapshotIntegrityError, match="回测实验重放结果"):
        run_demo_pipeline(db_session, test_settings)

    failed_task = db_session.get(TaskRun, first["task_id"])
    assert failed_task is not None
    assert failed_task.status == "FAILED"
    assert failed_task.error_code == "SnapshotIntegrityError"
    assert (
        db_session.scalar(
            select(func.count(AuditLog.id)).where(AuditLog.event_type == "PIPELINE_FAILED")
        )
        == 1
    )

    # Restore the canonical backtest payload, then independently prove that a
    # Signal record mutation also fails the replay and records the failed task.
    backtest.metrics = original_backtest_metrics
    tampered_signal = signals[0]
    tampered_signal.suggested_risk_budget_max += 0.001
    db_session.commit()

    with pytest.raises(SnapshotIntegrityError, match="信号记录完整性"):
        run_demo_pipeline(db_session, test_settings)

    failed_task = db_session.get(TaskRun, first["task_id"])
    assert failed_task is not None
    assert failed_task.status == "FAILED"
    assert failed_task.error_code == "SnapshotIntegrityError"
    assert (
        db_session.scalar(
            select(func.count(AuditLog.id)).where(AuditLog.event_type == "PIPELINE_FAILED")
        )
        == 2
    )


@pytest.mark.e2e
def test_kill_switch_policy_creates_new_task_and_supersedes_prior_candidates(
    db_session, test_settings, monkeypatch
) -> None:
    first = run_demo_pipeline(db_session, test_settings)
    fills_before = db_session.scalar(select(func.count(SimulationFill.id)))
    assert fills_before and fills_before > 0

    # Keep this governance state-transition test focused and deterministic: the
    # first run above exercised both model evaluations; later policy variants
    # may reuse those persisted research artifacts without retraining them.
    backtest = db_session.scalar(select(BacktestExperiment))
    assert backtest is not None
    monkeypatch.setattr(
        pipeline,
        "_run_and_record_backtest",
        lambda *_args, **_kwargs: backtest,
    )
    monkeypatch.setattr(
        pipeline,
        "_run_and_record_logistic",
        lambda *_args, **_kwargs: first["logistic_models"],
    )

    # The first run's next-bar fills occur on 2025-01-31.  Move the decision
    # clock forward to that close and tighten the cap so the portfolio-level
    # pre-signal gate, not an instrument-local rule, is what blocks the batch.
    breached_settings = test_settings.model_copy(
        update={
            "demo_evaluation_time": datetime(2025, 1, 31, 8, 0, tzinfo=UTC),
            "single_etf_cap": 0.000001,
        }
    )
    alerts_before_breach = db_session.scalar(select(func.count(Alert.id)))
    breached = run_demo_pipeline(db_session, breached_settings)
    breached_signal_ids = set(
        db_session.scalars(select(Signal.id).where(Signal.is_current.is_(True)))
    )
    alerts_after_breach = db_session.scalar(select(func.count(Alert.id)))
    repeated_breach = run_demo_pipeline(db_session, breached_settings)

    portfolio_alerts = list(
        db_session.scalars(select(Alert).where(Alert.alert_type == "PORTFOLIO_RISK"))
    )
    breached_signal_alerts = list(
        db_session.scalars(select(Alert).where(Alert.signal_id.in_(breached_signal_ids)))
    )
    breached_current = list(
        db_session.scalars(select(Signal).where(Signal.id.in_(breached_signal_ids)))
    )
    assert breached["task_id"] != first["task_id"]
    assert breached["candidate_count"] == 0
    assert breached["blocked_count"] == 60
    assert repeated_breach["idempotent_replay"] is True
    assert alerts_after_breach >= alerts_before_breach + 1
    assert db_session.scalar(select(func.count(Alert.id))) == alerts_after_breach
    assert len(portfolio_alerts) == 1
    assert verify_alert_content(portfolio_alerts[0])
    assert portfolio_alerts[0].signal_id is None
    assert portfolio_alerts[0].source_object_type == "System"
    assert breached_signal_alerts == []
    assert len(breached_current) == 60
    assert all(signal.state == "BLOCKED_BY_RISK" for signal in breached_current)
    assert all(
        any(str(rule).startswith("PORTFOLIO_") for rule in signal.risk_rules_hit)
        for signal in breached_current
    )
    assert db_session.scalar(select(func.count(SimulationFill.id))) == fills_before

    killed_settings = test_settings.model_copy(
        update={
            "demo_evaluation_time": datetime(2025, 1, 31, 8, 0, tzinfo=UTC),
            "global_kill_switch": True,
        }
    )

    killed = run_demo_pipeline(db_session, killed_settings)
    repeated = run_demo_pipeline(db_session, killed_settings)

    all_signals = list(db_session.scalars(select(Signal)))
    current = [signal for signal in all_signals if signal.is_current]
    prior = [signal for signal in all_signals if not signal.is_current]
    assert killed["task_id"] != first["task_id"]
    assert killed["candidate_count"] == 0
    assert killed["blocked_count"] == 60
    assert repeated["idempotent_replay"] is True
    assert db_session.scalar(select(func.count(TaskRun.id))) == 3
    assert len(all_signals) == 180
    assert len(current) == 60
    assert len(prior) == 120
    assert all(signal.state == "BLOCKED_BY_RISK" for signal in current)
    assert all("GLOBAL_KILL_SWITCH" in signal.risk_rules_hit for signal in current)
    assert db_session.scalar(select(func.count(SimulationFill.id))) == fills_before


@pytest.mark.e2e
def test_demo_cli_runs_without_any_provider_api_key(tmp_path) -> None:
    repository_root = str(__file__).rsplit("/tests/", maxsplit=1)[0]
    environment = os.environ.copy()
    for secret_name in ["TWELVE_DATA_API_KEY", "ALERT_WEBHOOK_URL"]:
        environment.pop(secret_name, None)
    environment.update(
        {
            "PYTHONPATH": f"{repository_root}/src",
            "APP_ENV": "test",
            "TRADING_MODE": "paper",
            "DATABASE_URL": f"sqlite:///{tmp_path / 'cli.sqlite3'}",
            "SNAPSHOT_DIR": str(tmp_path / "snapshots"),
            "EXPORT_DIR": str(tmp_path / "exports"),
            "TWELVE_DATA_ENABLED": "false",
            "GDELT_ENABLED": "false",
        }
    )

    result = subprocess.run(  # noqa: S603 - fixed interpreter and module, isolated environment
        [sys.executable, "-m", "etf_sentinel.cli", "demo"],
        cwd=repository_root,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
        # This cold-start E2E also fits/calibrates both research horizons. It is
        # a completion test, not a 60-second performance SLO; allow a bounded
        # budget on hosts sharing CPU with the Compose stack.
        timeout=180,
    )

    assert result.returncode == 0, result.stderr
    summary = json.loads(result.stdout)
    assert summary["data_mode"] == DataMode.DEMO_FIXTURE.value
    assert summary["etf_count"] == 60
    assert summary["signal_count"] == 60
