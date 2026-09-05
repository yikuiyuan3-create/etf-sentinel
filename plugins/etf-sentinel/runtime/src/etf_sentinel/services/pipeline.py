from __future__ import annotations

import hashlib
import json
import math
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from etf_sentinel.audit import append_audit
from etf_sentinel.config import Settings
from etf_sentinel.enums import AlertSeverity, TaskStatus
from etf_sentinel.models import (
    BacktestExperiment,
    DataSnapshot,
    EtfInstrument,
    ModelVersion,
    ScheduledEvent,
    TaskRun,
)
from etf_sentinel.providers.base import authorize_provider
from etf_sentinel.services.alerts import (
    create_scheduled_event_alerts,
    create_signal_alerts,
    create_system_alert,
)
from etf_sentinel.services.backtest import experiment_key, run_reproducible_backtest
from etf_sentinel.services.ingestion import (
    SnapshotIntegrityError,
    canonical_frame_hash,
    ingest_demo_snapshot,
    verify_snapshot_frame,
)
from etf_sentinel.services.ledger import (
    apply_corporate_actions,
    assert_current_execution_governance,
    assess_portfolio_risk_gate,
    portfolio_state,
    simulate_candidate_fills,
)
from etf_sentinel.services.logistic_model import (
    LOGISTIC_ARTIFACT_INTEGRITY_KEY,
    evaluate_logistic_baseline,
    point_in_time_universe_benchmark,
    verify_logistic_model_artifact,
)
from etf_sentinel.services.signals import (
    build_news_lineage,
    code_version,
    decision_policy_hash,
    eligible_news_events,
    ensure_rule_model,
    generate_signals,
)


def read_snapshot_with_duckdb(
    path: str | Path,
    *,
    expected_hash: str | None = None,
    expected_rows: int | None = None,
) -> pd.DataFrame:
    resolved = str(Path(path).resolve())
    connection = duckdb.connect(database=":memory:")
    try:
        # Dataset-specific ordering is part of the canonical hash, not the reader:
        # macro snapshots do not have an instrument_id column.
        frame = connection.execute("SELECT * FROM read_parquet(?)", [resolved]).fetch_df()
        if (expected_hash is None) != (expected_rows is None):
            raise ValueError("快照校验必须同时提供哈希和行数。")
        if expected_hash is not None and expected_rows is not None:
            verify_snapshot_frame(
                frame,
                expected_hash=expected_hash,
                expected_rows=expected_rows,
            )
        return frame
    except duckdb.Error as exc:
        raise SnapshotIntegrityError("快照无法读取，已失败关闭。") from exc
    finally:
        connection.close()


def run_demo_pipeline(session: Session, settings: Settings) -> dict[str, Any]:
    attempt_started_at = datetime.now(UTC)
    try:
        return _execute_demo_pipeline(session, settings)
    except Exception as exc:
        _record_pipeline_failure(
            session,
            settings=settings,
            attempt_started_at=attempt_started_at,
            error=exc,
        )
        raise


def _execute_demo_pipeline(session: Session, settings: Settings) -> dict[str, Any]:
    snapshot = ingest_demo_snapshot(session, settings, commit=False)
    macro_snapshot = session.scalar(
        select(DataSnapshot)
        .where(
            DataSnapshot.provider_code == snapshot.provider_code,
            DataSnapshot.dataset_type == "MACRO_FACTS",
            DataSnapshot.data_mode == snapshot.data_mode,
        )
        .order_by(DataSnapshot.available_at.desc())
    )
    if macro_snapshot is None:
        raise SnapshotIntegrityError("演示宏观快照缺失，已失败关闭。")
    rule_model = ensure_rule_model(session)
    task_news_lineage = build_news_lineage(
        eligible_news_events(
            session,
            snapshot=snapshot,
            evaluation_time=settings.demo_evaluation_time,
        )
    )
    policy_hash = decision_policy_hash(
        session,
        snapshot=snapshot,
        settings=settings,
        macro_snapshot=macro_snapshot,
        model=rule_model,
        news_lineage_hash=str(task_news_lineage["lineage_hash"]),
    )
    raw_task_key = (
        f"{snapshot.snapshot_hash}:{settings.demo_evaluation_time.isoformat()}:{policy_hash}"
    )
    task_key = f"daily-demo:{hashlib.sha256(raw_task_key.encode()).hexdigest()}"
    previous = session.scalar(select(TaskRun).where(TaskRun.idempotency_key == task_key))
    if previous is not None and previous.status == TaskStatus.RUNNING.value:
        started_at = previous.started_at
        if started_at.tzinfo is None:
            started_at = started_at.replace(tzinfo=UTC)
        if datetime.now(UTC) - started_at <= timedelta(seconds=720):
            return {
                **previous.result_summary,
                "task_id": previous.id,
                "task_status": TaskStatus.RUNNING.value,
                "idempotent_replay": True,
            }
        previous.status = TaskStatus.FAILED.value
        previous.completed_at = datetime.now(UTC)
        previous.error_code = "TASK_LEASE_EXPIRED"
        previous.safe_error_message = "上次任务超过租约期，已标记失败并安全重试。"
        append_audit(
            session,
            event_type="STALE_TASK_RECOVERED",
            object_type="TaskRun",
            object_id=previous.id,
            details={"lease_seconds": 720},
        )
        session.commit()
    idempotent_replay = previous is not None and previous.status == TaskStatus.SUCCEEDED.value
    if previous is None:
        task = TaskRun(
            task_name="daily_demo_pipeline",
            idempotency_key=task_key,
            status=TaskStatus.RUNNING.value,
            started_at=datetime.now(UTC),
        )
        session.add(task)
        try:
            session.commit()
        except IntegrityError:
            session.rollback()
            existing = session.scalar(select(TaskRun).where(TaskRun.idempotency_key == task_key))
            if existing is not None:
                return {
                    **existing.result_summary,
                    "task_id": existing.id,
                    "task_status": existing.status,
                    "idempotent_replay": True,
                }
            raise
    else:
        task = previous
        task.status = TaskStatus.RUNNING.value
        task.started_at = datetime.now(UTC)
        task.completed_at = None
        task.error_code = None
        task.safe_error_message = None
        session.commit()
    try:
        full_frame = read_snapshot_with_duckdb(
            snapshot.parquet_uri,
            expected_hash=snapshot.snapshot_hash,
            expected_rows=snapshot.row_count,
        )
        macro_frame = read_snapshot_with_duckdb(
            macro_snapshot.parquet_uri,
            expected_hash=macro_snapshot.snapshot_hash,
            expected_rows=macro_snapshot.row_count,
        )
        evaluation_time = pd.Timestamp(settings.demo_evaluation_time)
        if evaluation_time.tzinfo is None:
            evaluation_time = evaluation_time.tz_localize("UTC")
        available_at = pd.to_datetime(full_frame["available_at"], utc=True)
        event_time = pd.to_datetime(full_frame["event_time"], utc=True)
        published_at = pd.to_datetime(full_frame["published_at"], utc=True)
        first_seen_at = pd.to_datetime(full_frame["first_seen_at"], utc=True)
        as_of = pd.to_datetime(full_frame["as_of"], utc=True)
        point_in_time_frame = full_frame.loc[
            (available_at <= evaluation_time)
            & (event_time <= evaluation_time)
            & (published_at <= evaluation_time)
            & (first_seen_at <= evaluation_time)
            & (as_of <= evaluation_time)
        ].copy()
        existing_state = portfolio_state(session, through=settings.demo_evaluation_time)
        if existing_state.positions:
            authorize_provider(
                session,
                snapshot.provider_code,
                purposes={"display", "algorithm", "derivative", "cache"},
                at=datetime.now(UTC),
            )
            for instrument_id in existing_state.positions:
                instrument = session.get(EtfInstrument, instrument_id, populate_existing=True)
                if instrument is None or instrument.provider_code != snapshot.provider_code:
                    raise SnapshotIntegrityError("组合持仓主数据与行情快照血缘不一致。")
                authorize_provider(
                    session,
                    snapshot.provider_code,
                    purposes={"display", "algorithm", "derivative", "cache"},
                    at=datetime.now(UTC),
                    market=instrument.mic,
                    region=instrument.region,
                )
            apply_corporate_actions(
                session,
                full_frame,
                through=settings.demo_evaluation_time,
            )
        portfolio_gate = assess_portfolio_risk_gate(
            session,
            point_in_time_frame,
            settings=settings,
            evaluation_time=settings.demo_evaluation_time,
        )
        backtest = _run_and_record_backtest(session, snapshot, full_frame, commit=False)
        logistic = _run_and_record_logistic(
            session,
            snapshot,
            point_in_time_frame,
            evaluation_time=settings.demo_evaluation_time,
            commit=False,
        )
        signals = generate_signals(
            session,
            snapshot=snapshot,
            # The service verifies the complete immutable snapshot before it
            # derives its own point-in-time slice.  Passing a pre-sliced frame
            # would make the registered snapshot hash unverifiable.
            frame=full_frame,
            evaluation_time=settings.demo_evaluation_time,
            settings=settings,
            macro_snapshot=macro_snapshot,
            macro_frame=macro_frame,
            commit=False,
        )
        alerts = create_signal_alerts(
            session,
            signals,
            settings=settings,
            now=settings.demo_evaluation_time,
            commit=False,
        )
        portfolio_alert_count = 0
        if portfolio_gate.rules:
            create_system_alert(
                session,
                dedupe_scope=(
                    f"{settings.demo_evaluation_time.isoformat()}:{portfolio_gate.context_hash}"
                ),
                severity=AlertSeverity.CRITICAL,
                title="模拟组合风险门禁已触发",
                trigger_reason=", ".join(portfolio_gate.rules),
                data_as_of=settings.demo_evaluation_time,
                settings=settings,
                invalidation_conditions=["组合风险回到配置阈值内且经授权人员复核后重新运行"],
                now=settings.demo_evaluation_time,
                alert_type="PORTFOLIO_RISK",
                commit=False,
            )
            portfolio_alert_count = 1
        event_alerts = create_scheduled_event_alerts(
            session,
            list(session.scalars(select(ScheduledEvent))),
            settings=settings,
            now=settings.demo_evaluation_time,
            commit=False,
        )
        fills = simulate_candidate_fills(
            session,
            signals=signals,
            full_frame=full_frame,
            settings=settings,
            commit=False,
        )
        state = portfolio_state(session)
        summary = {
            "task_id": task.id,
            "snapshot_id": snapshot.id,
            "snapshot_hash": snapshot.snapshot_hash,
            "macro_snapshot_id": macro_snapshot.id,
            "macro_snapshot_hash": macro_snapshot.snapshot_hash,
            "macro_fact_count": len(macro_frame),
            "data_mode": snapshot.data_mode,
            "etf_count": int(full_frame["instrument_id"].nunique()),
            "fact_count": int(len(full_frame)),
            "point_in_time_fact_count": int(len(point_in_time_frame)),
            "signal_count": len(signals),
            "candidate_count": sum("CANDIDATE" in item.state for item in signals),
            "blocked_count": sum(
                item.state in {"BLOCKED_BY_RISK", "DATA_STALE"} for item in signals
            ),
            "alert_count": len(alerts) + len(event_alerts) + portfolio_alert_count,
            "fill_count": len(fills),
            "cash": state.cash,
            "position_count": len(state.positions),
            "total_fees": state.total_fees,
            "backtest_experiment_id": backtest.id,
            "backtest_status": backtest.status,
            "logistic_models": logistic,
            "idempotent_replay": idempotent_replay,
        }
        task.status = TaskStatus.SUCCEEDED.value
        task.completed_at = datetime.now(UTC)
        task.result_summary = summary
        append_audit(
            session,
            event_type="PIPELINE_COMPLETED",
            object_type="TaskRun",
            object_id=task.id,
            details={key: value for key, value in summary.items() if key != "task_id"},
        )
        assert_current_execution_governance(
            session,
            signals=[item for item in signals if item.state == "ENTRY_CANDIDATE"],
            full_frame=full_frame,
            settings=settings,
        )
        session.commit()
        return summary
    except Exception:
        session.rollback()
        raise


def _record_pipeline_failure(
    session: Session,
    *,
    settings: Settings,
    attempt_started_at: datetime,
    error: Exception,
) -> None:
    session.rollback()
    failed = session.scalar(
        select(TaskRun)
        .where(
            TaskRun.task_name == "daily_demo_pipeline",
            TaskRun.status == TaskStatus.RUNNING.value,
        )
        .order_by(TaskRun.started_at.desc())
        .limit(1)
    )
    completed_at = datetime.now(UTC)
    if failed is None:
        failure_key = hashlib.sha256(
            (
                f"preflight:{settings.demo_evaluation_time.isoformat()}:"
                f"{attempt_started_at.isoformat()}:{type(error).__name__}"
            ).encode()
        ).hexdigest()
        failed = TaskRun(
            task_name="daily_demo_pipeline",
            idempotency_key=f"failed-attempt:{failure_key}",
            status=TaskStatus.FAILED.value,
            started_at=attempt_started_at,
        )
        session.add(failed)
        session.flush()
    failed.status = TaskStatus.FAILED.value
    failed.completed_at = completed_at
    failed.error_code = type(error).__name__[:80]
    failed.safe_error_message = "流水线失败；详情见受控服务日志。"
    append_audit(
        session,
        event_type="PIPELINE_FAILED",
        object_type="TaskRun",
        object_id=failed.id,
        details={"error_type": type(error).__name__},
    )
    session.flush()
    create_system_alert(
        session,
        dedupe_scope=(
            f"PIPELINE_FAILED:{settings.demo_evaluation_time.date().isoformat()}:"
            f"{type(error).__name__}"
        ),
        severity=AlertSeverity.CRITICAL,
        title="日度研究流水线失败",
        trigger_reason="数据、许可、特征、信号或回测任务未完成；已失败关闭候选输出。",
        data_as_of=completed_at,
        settings=settings,
        invalidation_conditions=["授权人员完成故障处置并重跑全链路成功"],
    )
    # create_system_alert returns early for an existing dedupe key, so the
    # failure latch must own its transaction boundary explicitly.
    session.commit()


def _run_and_record_backtest(
    session: Session,
    snapshot,
    frame: pd.DataFrame,
    *,
    commit: bool = True,
) -> BacktestExperiment:
    version = code_version()
    fee_bps = 2.5
    slippage_bps = 3.0
    decision_delay_minutes = 30
    config = {
        "strategy": "cross_sectional_momentum_baseline_v1",
        "fee_bps": fee_bps,
        "slippage_bps": slippage_bps,
        "execution": "NEXT_BAR_OPEN",
        "decision_delay_minutes": decision_delay_minutes,
        "signal_price": "TOTAL_RETURN_CLOSE",
        "execution_price": "RAW_OPEN",
    }
    key = experiment_key(snapshot.snapshot_hash, config, version, 20_250_131)
    result = run_reproducible_backtest(
        frame,
        fee_bps=fee_bps,
        slippage_bps=slippage_bps,
        decision_delay_minutes=decision_delay_minutes,
    )
    baseline_sharpe = max(value.get("sharpe", 0.0) for value in result.baseline_metrics.values())
    status = (
        "EXPERIMENTAL"
        if result.metrics.get("sharpe", 0.0) <= baseline_sharpe
        else "REVIEW_REQUIRED"
    )
    expected_payload = {
        "experiment_key": key,
        "name": "CrossSectionalMomentumBaselineV1 可复现演示回测",
        "snapshot_id": snapshot.id,
        "model_version_id": None,
        "code_version": version,
        "random_seed": 20_250_131,
        "config": config,
        "metrics": result.metrics,
        "baseline_metrics": result.baseline_metrics,
        "periods": result.periods,
        "leakage_checks": result.leakage_checks,
        "status": status,
    }
    existing = session.scalar(
        select(BacktestExperiment).where(BacktestExperiment.experiment_key == key)
    )
    if existing is not None:
        stored_payload = {key: getattr(existing, key) for key in expected_payload}
        if _stable_payload_hash(stored_payload) != _stable_payload_hash(expected_payload):
            raise SnapshotIntegrityError("回测实验重放结果或治理状态不一致，已失败关闭。")
        return existing
    experiment = BacktestExperiment(
        **expected_payload,
    )
    session.add(experiment)
    session.flush()
    append_audit(
        session,
        event_type="BACKTEST_RECORDED",
        object_type="BacktestExperiment",
        object_id=experiment.id,
        details={
            "experiment_key": key,
            "snapshot_hash": snapshot.snapshot_hash,
            "status": status,
            "baseline_sharpe": baseline_sharpe,
            "model_sharpe": result.metrics.get("sharpe", 0.0),
        },
    )
    if commit:
        session.commit()
    else:
        session.flush()
    return experiment


def _stable_payload_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            allow_nan=False,
            default=str,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()


def _run_and_record_logistic(
    session: Session,
    snapshot,
    point_in_time_frame: pd.DataFrame,
    *,
    evaluation_time: datetime | None = None,
    commit: bool = True,
) -> list[dict[str, Any]]:
    first_instrument = sorted(point_in_time_frame["instrument_id"].unique())[0]
    if evaluation_time is None:
        evaluation_cutoff = pd.to_datetime(
            point_in_time_frame["available_at"], utc=True, errors="raise"
        ).max()
    else:
        evaluation_cutoff = pd.Timestamp(evaluation_time)
        evaluation_cutoff = (
            evaluation_cutoff.tz_localize("UTC")
            if evaluation_cutoff.tzinfo is None
            else evaluation_cutoff.tz_convert("UTC")
        )
    universe_input_hash = canonical_frame_hash(point_in_time_frame)
    known_frame, benchmark = point_in_time_universe_benchmark(
        point_in_time_frame, decision_delay_minutes=30
    )
    bars = known_frame.loc[known_frame["instrument_id"] == first_instrument].merge(
        benchmark, on="event_time", how="inner", validate="one_to_one"
    )
    model_input_hash = canonical_frame_hash(bars)
    output: list[dict[str, Any]] = []
    for horizon in (5, 20):
        evaluation_config = {
            "horizon_days": horizon,
            "cost_bps": 8.0,
            "decision_delay_minutes": 30,
            "folds": 3,
            "embargo_samples": 5,
            "random_seed": 20_250_131,
            "benchmark": "POINT_IN_TIME_UNIVERSE_MEAN_TOTAL_RETURN",
            "label": "FUTURE_NET_EXCESS_RETURN_POSITIVE",
            "evaluation_cutoff": evaluation_cutoff.isoformat(),
            "selected_instrument_id": str(first_instrument),
            "universe_input_hash": universe_input_hash,
            "model_input_hash": model_input_hash,
        }
        artifact_code_version = code_version()
        evaluation_key = hashlib.sha256(
            json.dumps(
                {
                    "snapshot_hash": snapshot.snapshot_hash,
                    "code_version": artifact_code_version,
                    "feature_version": "logistic-features-v1",
                    "config": evaluation_config,
                },
                ensure_ascii=False,
                sort_keys=True,
            ).encode()
        ).hexdigest()
        evaluation = evaluate_logistic_baseline(bars, horizon_days=horizon)
        version = f"1.0.0-h{horizon}-{evaluation_key[:12]}"
        model = session.scalar(
            select(ModelVersion).where(
                ModelVersion.model_name == "LogisticBaselineV1",
                ModelVersion.version == version,
            )
        )
        metrics: dict[str, Any] = {
            "evaluation_key": evaluation_key,
            "code_version": artifact_code_version,
            "config": evaluation_config,
            "horizon_days": horizon,
            "brier_score": evaluation.brier_score,
            "baseline_brier_score": evaluation.baseline_brier_score,
            "expected_calibration_error": evaluation.calibration_error,
            "samples": evaluation.samples,
        }
        if model is None:
            model = ModelVersion(
                model_name="LogisticBaselineV1",
                version=version,
                status=evaluation.status.value,
                feature_version="logistic-features-v1",
                training_snapshot_hash=snapshot.snapshot_hash,
                metrics=metrics,
                limitations=[
                    "仅估计扣除成本后的超额收益为正概率，不预测价格。",
                    "固定演示样本不能用于模型晋级。",
                    "必须经人工批准后才能成为 champion。",
                ],
            )
            session.add(model)
            session.flush()
            append_audit(
                session,
                event_type="MODEL_EVALUATED",
                object_type="ModelVersion",
                object_id=model.id,
                details={"status": model.status, **metrics},
            )
        else:
            if not verify_logistic_model_artifact(model):
                raise SnapshotIntegrityError("Logistic 模型工件完整性校验失败，已失败关闭。")
            if model.training_snapshot_hash != snapshot.snapshot_hash:
                raise SnapshotIntegrityError("Logistic 模型版本与训练快照不一致，已失败关闭。")
            stored = model.metrics or {}
            if (
                stored.get("evaluation_key") != evaluation_key
                or stored.get("code_version") != artifact_code_version
                or stored.get("config") != evaluation_config
                or model.status != evaluation.status.value
            ):
                raise SnapshotIntegrityError("Logistic 模型实验血缘或状态不一致，已失败关闭。")
            for metric_name in (
                "brier_score",
                "baseline_brier_score",
                "expected_calibration_error",
            ):
                if not math.isclose(
                    float(stored.get(metric_name, math.nan)),
                    float(metrics[metric_name]),
                    rel_tol=1e-12,
                    abs_tol=1e-12,
                ):
                    raise SnapshotIntegrityError("Logistic 重放结果不可复现，已失败关闭。")
            if int(stored.get("samples", -1)) != evaluation.samples:
                raise SnapshotIntegrityError("Logistic 重放样本数不一致，已失败关闭。")
            metrics = dict(stored)
        public_metrics = {
            key: value for key, value in metrics.items() if key != LOGISTIC_ARTIFACT_INTEGRITY_KEY
        }
        output.append(
            {"id": model.id, "version": version, "status": model.status, **public_metrics}
        )
    if commit:
        session.commit()
    else:
        session.flush()
    return output
