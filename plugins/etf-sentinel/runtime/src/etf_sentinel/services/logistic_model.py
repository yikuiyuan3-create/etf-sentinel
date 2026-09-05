from __future__ import annotations

import hashlib
import hmac
import json
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sqlalchemy import event

from etf_sentinel.enums import ModelStatus
from etf_sentinel.models import ModelVersion

POINT_IN_TIME_CLOCKS = ("event_time", "published_at", "first_seen_at", "available_at", "as_of")
BENCHMARK_CLOCKS = (
    "benchmark_published_at",
    "benchmark_first_seen_at",
    "benchmark_available_at",
    "benchmark_as_of",
)
LOGISTIC_ARTIFACT_INTEGRITY_KEY = "_artifact_integrity"
LOGISTIC_ARTIFACT_SCHEMA = "logistic-model-artifact-v1"
LOGISTIC_BASELINE_LIMITATIONS = (
    "仅估计扣除成本后的超额收益为正概率，不预测价格。",
    "固定演示样本不能用于模型晋级。",
    "必须经人工批准后才能成为 champion。",
)


@dataclass(frozen=True)
class LogisticEvaluation:
    horizon_days: int
    brier_score: float
    baseline_brier_score: float
    calibration_error: float
    samples: int
    status: ModelStatus


def point_in_time_universe_benchmark(
    frame: pd.DataFrame, *, decision_delay_minutes: int = 30
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build a cross-sectional benchmark only from facts known at each decision clock."""
    required = {
        "instrument_id",
        "revision",
        "total_return_close",
        *POINT_IN_TIME_CLOCKS,
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"点时基准输入缺少字段：{', '.join(missing)}")
    known = frame.copy()
    for column in POINT_IN_TIME_CLOCKS:
        known[column] = pd.to_datetime(known[column], utc=True, errors="raise")
    decision_clock = known["event_time"] + pd.Timedelta(minutes=decision_delay_minutes)
    causal = pd.Series(True, index=known.index)
    for column in POINT_IN_TIME_CLOCKS:
        causal &= known[column] <= decision_clock
    known = known.loc[causal].copy()
    if known.empty:
        raise ValueError("点时基准在决策时钟前没有可知事实。")
    known = (
        known.sort_values(
            [
                "instrument_id",
                "event_time",
                "available_at",
                "first_seen_at",
                "revision",
            ]
        )
        .drop_duplicates(["instrument_id", "event_time"], keep="last")
        .reset_index(drop=True)
    )
    benchmark = (
        known.groupby("event_time", as_index=False)
        .agg(
            benchmark_total_return_close=("total_return_close", "mean"),
            benchmark_published_at=("published_at", "max"),
            benchmark_first_seen_at=("first_seen_at", "max"),
            benchmark_available_at=("available_at", "max"),
            benchmark_as_of=("as_of", "max"),
            benchmark_constituent_count=("instrument_id", "nunique"),
        )
        .sort_values("event_time")
        .reset_index(drop=True)
    )
    return known, benchmark


def purged_walk_forward_splits(
    sample_count: int,
    *,
    folds: int,
    min_train: int,
    purge: int,
    embargo: int,
) -> list[tuple[np.ndarray, np.ndarray]]:
    exclusion_gap = purge + embargo
    if sample_count <= min_train + exclusion_gap + folds:
        raise ValueError("样本不足以进行 purge/embargo walk-forward。")
    remaining = sample_count - min_train - exclusion_gap
    test_size = max(1, remaining // folds)
    splits: list[tuple[np.ndarray, np.ndarray]] = []
    for fold in range(folds):
        train_end = min_train + fold * test_size
        test_start = train_end + exclusion_gap
        test_end = min(sample_count, test_start + test_size)
        train_indices = np.arange(0, train_end)
        test_indices = np.arange(test_start, test_end)
        if len(test_indices) == 0:
            continue
        if train_indices[-1] + exclusion_gap >= test_indices[0]:
            raise AssertionError("purge/embargo 排除窗口未生效。")
        splits.append((train_indices, test_indices))
    return splits


def build_logistic_dataset(
    bars: pd.DataFrame,
    *,
    horizon_days: int,
    cost_bps: float,
    decision_delay_minutes: int = 30,
) -> tuple[pd.DataFrame, pd.Series]:
    ordered = bars.sort_values("event_time").copy()
    if "benchmark_total_return_close" not in ordered.columns:
        raise ValueError("LogisticBaselineV1 输入必须包含点时基准总回报序列。")
    required_clocks = {*POINT_IN_TIME_CLOCKS, *BENCHMARK_CLOCKS}
    missing_clocks = sorted(required_clocks - set(ordered.columns))
    if missing_clocks:
        raise ValueError(f"LogisticBaselineV1 输入缺少点时血缘字段：{', '.join(missing_clocks)}")
    event_time = pd.to_datetime(ordered["event_time"], utc=True, errors="raise")
    decision_clock = event_time + pd.Timedelta(minutes=decision_delay_minutes)
    causal = pd.Series(True, index=ordered.index)
    for column in (*POINT_IN_TIME_CLOCKS[1:], *BENCHMARK_CLOCKS):
        values = pd.to_datetime(ordered[column], utc=True, errors="raise")
        causal &= values <= decision_clock
    ordered = ordered.loc[causal].copy()
    if ordered.duplicated(["event_time"]).any():
        raise ValueError("LogisticBaselineV1 拒绝未解析的同时点多 vintage 输入。")
    price = ordered["total_return_close"].astype(float)
    benchmark = ordered["benchmark_total_return_close"].astype(float)
    returns = price.pct_change()
    features = pd.DataFrame(
        {
            "momentum_21": price.pct_change(21),
            "momentum_63": price.pct_change(63),
            "momentum_126": price.pct_change(126),
            "volatility_21": returns.rolling(21).std() * np.sqrt(252),
            "downside_21": returns.clip(upper=0).rolling(21).std() * np.sqrt(252),
            "trend_50": price / price.rolling(50).mean() - 1,
            "volume_ratio": ordered["volume"].astype(float)
            / ordered["volume"].astype(float).rolling(21).median(),
            "spread_bps": ordered["bid_ask_spread_bps"].astype(float),
        }
    )
    future_excess_net = (
        price.shift(-horizon_days) / price
        - benchmark.shift(-horizon_days) / benchmark
        - cost_bps / 10_000
    )
    label = (future_excess_net > 0).astype(int)
    valid = features.notna().all(axis=1) & future_excess_net.notna()
    return features.loc[valid].reset_index(drop=True), label.loc[valid].reset_index(drop=True)


def evaluate_logistic_baseline(
    bars: pd.DataFrame,
    *,
    horizon_days: int,
    cost_bps: float = 8.0,
) -> LogisticEvaluation:
    features, label = build_logistic_dataset(
        bars,
        horizon_days=horizon_days,
        cost_bps=cost_bps,
        decision_delay_minutes=30,
    )
    splits = purged_walk_forward_splits(
        len(features), folds=3, min_train=max(80, horizon_days * 4), purge=horizon_days, embargo=5
    )
    calibrated_probabilities: list[float] = []
    targets: list[int] = []
    baseline_probabilities: list[float] = []
    for train_indices, test_indices in splits:
        calibration_size = max(20, len(train_indices) // 5)
        calibration_indices = train_indices[-calibration_size:]
        calibration_start = int(calibration_indices[0])
        calibration_gap = horizon_days + 5
        fit_indices = train_indices[train_indices + calibration_gap < calibration_start]
        if len(fit_indices) == 0:
            continue
        if int(fit_indices[-1]) + calibration_gap >= calibration_start:
            raise AssertionError("基模型与校准窗口未实施 purge/embargo。")
        if len(np.unique(label.iloc[fit_indices])) < 2:
            continue
        model = Pipeline(
            [
                ("scale", StandardScaler()),
                ("model", LogisticRegression(C=0.5, max_iter=1000, random_state=20_250_131)),
            ]
        )
        model.fit(features.iloc[fit_indices], label.iloc[fit_indices])
        raw_calibration = model.decision_function(features.iloc[calibration_indices]).reshape(-1, 1)
        calibration_target = label.iloc[calibration_indices]
        if len(np.unique(calibration_target)) < 2:
            continue
        calibrator = LogisticRegression(C=1.0, max_iter=1000, random_state=20_250_131)
        calibrator.fit(raw_calibration, calibration_target)
        raw_test = model.decision_function(features.iloc[test_indices]).reshape(-1, 1)
        calibrated = calibrator.predict_proba(raw_test)[:, 1]
        momentum_baseline = np.where(features.iloc[test_indices]["momentum_63"] > 0, 0.58, 0.42)
        calibrated_probabilities.extend(calibrated.tolist())
        baseline_probabilities.extend(momentum_baseline.tolist())
        targets.extend(label.iloc[test_indices].astype(int).tolist())
    if not targets:
        raise ValueError("没有可用于校准评估的 walk-forward 样本。")
    target_array = np.asarray(targets)
    model_probability = np.asarray(calibrated_probabilities)
    baseline_probability = np.asarray(baseline_probabilities)
    score = float(brier_score_loss(target_array, model_probability))
    baseline_score = float(brier_score_loss(target_array, baseline_probability))
    calibration_error = expected_calibration_error(target_array, model_probability)
    status = ModelStatus.EXPERIMENTAL if score < baseline_score else ModelStatus.REJECTED
    return LogisticEvaluation(
        horizon_days=horizon_days,
        brier_score=score,
        baseline_brier_score=baseline_score,
        calibration_error=calibration_error,
        samples=len(targets),
        status=status,
    )


def expected_calibration_error(
    targets: np.ndarray, probabilities: np.ndarray, bins: int = 10
) -> float:
    total = len(targets)
    error = 0.0
    boundaries = np.linspace(0, 1, bins + 1)
    for low, high in zip(boundaries[:-1], boundaries[1:], strict=True):
        mask = (probabilities >= low) & (
            probabilities <= high if high == 1 else probabilities < high
        )
        if not mask.any():
            continue
        accuracy = targets[mask].mean()
        confidence = probabilities[mask].mean()
        error += mask.sum() / total * abs(float(accuracy - confidence))
    return float(error)


def logistic_model_artifact_hash(model: Any) -> str:
    """Return the canonical checksum of persisted LogisticBaselineV1 evidence."""
    metrics = dict(model.metrics or {})
    metrics.pop(LOGISTIC_ARTIFACT_INTEGRITY_KEY, None)
    payload = {
        "schema": LOGISTIC_ARTIFACT_SCHEMA,
        "model_name": model.model_name,
        "version": model.version,
        "status": model.status,
        "feature_version": model.feature_version,
        "training_snapshot_hash": model.training_snapshot_hash,
        "metrics": metrics,
        "limitations": list(model.limitations or []),
        "approved_by": model.approved_by,
        "approved_at": _normalized_timestamp(model.approved_at),
    }
    serialized = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        allow_nan=False,
        default=str,
        separators=(",", ":"),
    )
    return hashlib.sha256(serialized.encode()).hexdigest()


def seal_logistic_model_artifact(model: Any) -> None:
    """Embed a canonical checksum in the model's existing metrics JSON."""
    if model.model_name != "LogisticBaselineV1":
        return
    metrics = dict(model.metrics or {})
    metrics.pop(LOGISTIC_ARTIFACT_INTEGRITY_KEY, None)
    model.metrics = metrics
    checksum = logistic_model_artifact_hash(model)
    metrics[LOGISTIC_ARTIFACT_INTEGRITY_KEY] = {
        "schema": LOGISTIC_ARTIFACT_SCHEMA,
        "sha256": checksum,
    }
    model.metrics = metrics


def verify_logistic_model_artifact(model: Any) -> bool:
    """Validate persisted result, limitations, training lineage, status, and version."""
    if model.model_name != "LogisticBaselineV1":
        return False
    metrics = model.metrics or {}
    integrity = metrics.get(LOGISTIC_ARTIFACT_INTEGRITY_KEY)
    if not isinstance(integrity, dict):
        return False
    if integrity.get("schema") != LOGISTIC_ARTIFACT_SCHEMA:
        return False
    recorded_hash = integrity.get("sha256")
    if not isinstance(recorded_hash, str) or len(recorded_hash) != 64:
        return False
    if model.feature_version != "logistic-features-v1":
        return False
    if list(model.limitations or []) != list(LOGISTIC_BASELINE_LIMITATIONS):
        return False
    training_snapshot_hash = model.training_snapshot_hash
    config = metrics.get("config")
    artifact_code_version = metrics.get("code_version")
    evaluation_key = metrics.get("evaluation_key")
    if not all(
        isinstance(value, str) and value
        for value in (training_snapshot_hash, artifact_code_version, evaluation_key)
    ) or not isinstance(config, dict):
        return False
    expected_evaluation_key = hashlib.sha256(
        json.dumps(
            {
                "snapshot_hash": training_snapshot_hash,
                "code_version": artifact_code_version,
                "feature_version": model.feature_version,
                "config": config,
            },
            ensure_ascii=False,
            sort_keys=True,
        ).encode()
    ).hexdigest()
    if not hmac.compare_digest(str(evaluation_key), expected_evaluation_key):
        return False
    try:
        horizon = int(metrics["horizon_days"])
        brier_score = float(metrics["brier_score"])
        baseline_brier_score = float(metrics["baseline_brier_score"])
        samples = int(metrics["samples"])
        calibration_error = float(metrics["expected_calibration_error"])
    except (KeyError, TypeError, ValueError):
        return False
    if horizon not in {5, 20} or samples <= 0:
        return False
    finite_metrics = (brier_score, baseline_brier_score, calibration_error)
    if not all(np.isfinite(value) for value in finite_metrics):
        return False
    expected_version = f"1.0.0-h{horizon}-{expected_evaluation_key[:12]}"
    if model.version != expected_version:
        return False
    expected_status = (
        ModelStatus.EXPERIMENTAL.value
        if brier_score < baseline_brier_score
        else ModelStatus.REJECTED.value
    )
    if model.status != expected_status:
        return False
    return hmac.compare_digest(recorded_hash, logistic_model_artifact_hash(model))


@event.listens_for(ModelVersion, "before_insert")
def _seal_logistic_model_before_insert(
    _mapper: Any, _connection: Any, target: ModelVersion
) -> None:
    seal_logistic_model_artifact(target)


def _normalized_timestamp(value: Any) -> str | None:
    if value is None:
        return None
    parsed = pd.Timestamp(value)
    parsed = parsed.tz_localize("UTC") if parsed.tzinfo is None else parsed.tz_convert("UTC")
    return parsed.isoformat()
