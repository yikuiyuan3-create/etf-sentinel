"""Wall-clock monitoring, separate from fixed-clock daily research and execution.

Reports consume the same current display gates as the dashboard. This module never
fetches unlicensed data, advances fixture timestamps, creates signals or simulates fills.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from etf_sentinel.audit import append_audit
from etf_sentinel.config import Settings
from etf_sentinel.enums import AlertSeverity
from etf_sentinel.models import TaskRun
from etf_sentinel.services.alerts import create_system_alert

MONITOR_TASK_NAME = "hourly_risk_monitor"


def monitoring_window(now: datetime, hours: int) -> tuple[datetime, datetime]:
    if hours not in (1, 2) or now.tzinfo is None:
        raise ValueError("监测仅允许 1 或 2 小时，且必须使用含时区的时间。")
    # Asia/Shanghai's UTC+8 boundaries coincide with UTC for 1/2-hour schedules.
    current = now.astimezone(UTC)
    start = current.replace(hour=current.hour // hours * hours, minute=0, second=0, microsecond=0)
    return start, start + timedelta(hours=hours)


def _aware(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def schedule_state(task: TaskRun | None, now: datetime, hours: int) -> str:
    if task is None:
        return "NOT_RUN"
    if task.status != "SUCCEEDED" or task.completed_at is None:
        return "FAILED"
    if _aware(task.completed_at) < now - timedelta(hours=hours, minutes=5):
        return "OVERDUE"
    return "CURRENT"


def record_monitoring_check(
    session: Session,
    settings: Settings,
    *,
    now: datetime,
    build_report: Callable[[], dict[str, Any]],
) -> dict[str, Any]:
    """Caller holds the global Redis lease; a DB unique key survives worker restarts.

    All report/audit/health-alert writes commit together. Stored results are audit
    records only: the HTTP endpoint rechecks rights instead of serving this cache.
    """
    start, _end = monitoring_window(now, settings.monitoring_interval_hours)
    key = f"monitor:{settings.monitoring_interval_hours}:{start.isoformat()}"
    task = session.scalar(select(TaskRun).where(TaskRun.idempotency_key == key))
    if task is not None and task.status == "SUCCEEDED":
        return {"status": "SUCCEEDED", "task_id": task.id, "idempotent_replay": True}
    if task is None:
        task = TaskRun(
            task_name=MONITOR_TASK_NAME, idempotency_key=key, started_at=now, status="RUNNING"
        )
        session.add(task)
    else:
        task.status = "RUNNING"
        task.started_at = now
        task.completed_at = None
        task.error_code = None
        task.safe_error_message = None
    session.flush()
    task_id = task.id
    try:
        report = build_report()
        if report.get("data_mode") != "DEMO_FIXTURE":
            raise ValueError("真实数据监测尚未获批")
        task.status = "SUCCEEDED"
        task.completed_at = datetime.now(UTC)
        task.result_summary = {
            **report,
            "schedule_status": "CURRENT",
            "last_scheduled_at": task.completed_at.isoformat(),
        }
        reason = report.get("blocked_reason") or "LIVE_DATA_LICENSE_AND_PIPELINE_NOT_APPROVED"
        # Constant scope: repeated checks do not re-send the same unresolved issue.
        create_system_alert(
            session,
            settings=settings,
            dedupe_scope=f"monitor:{reason}",
            severity=AlertSeverity.WARN,
            title="定时监测：真实行情未启用或数据门禁阻断",
            trigger_reason=str(reason),
            data_as_of=datetime.fromisoformat(report["market_data_as_of"])
            if report.get("market_data_as_of")
            else now,
            invalidation_conditions=["获得对应数据许可并完成真实流水线验收，数据门禁通过"],
            now=now,
            commit=False,
        )
        append_audit(
            session,
            event_type="MONITORING_CHECK_COMPLETED",
            object_type="TaskRun",
            object_id=task_id,
            details={
                "idempotency_key": key,
                "data_mode": report["data_mode"],
                "analysis_status": report["analysis_status"],
                "data_snapshot_ids": report["data_snapshot_ids"],
                "code_version": report["code_version"],
            },
        )
        session.commit()
        return {"status": "SUCCEEDED", "task_id": task_id, "idempotent_replay": False}
    except Exception:
        session.rollback()
        # No exception payload, provider body, headers or environment in persisted logs.
        previous = session.scalar(
            select(TaskRun)
            .where(TaskRun.task_name == MONITOR_TASK_NAME)
            .order_by(TaskRun.started_at.desc())
            .limit(1)
        )
        episode = (
            (previous.result_summary or {}).get("failure_episode")
            if previous is not None and previous.status == "FAILED"
            else None
        ) or key
        task = session.scalar(select(TaskRun).where(TaskRun.idempotency_key == key))
        if task is None:
            task = TaskRun(task_name=MONITOR_TASK_NAME, idempotency_key=key, started_at=now)
            session.add(task)
        task.status = "FAILED"
        task.completed_at = datetime.now(UTC)
        task.error_code = "MONITORING_CHECK_FAILED"
        task.safe_error_message = "定时检查失败，停止展示分析；请检查服务健康并安全重试。"
        task.result_summary = {
            "data_mode": "DEMO_FIXTURE",
            "analysis_status": "BLOCKED",
            "failure_episode": episode,
        }
        session.flush()
        create_system_alert(
            session,
            settings=settings,
            dedupe_scope=f"monitor:task_failed:{episode}",
            severity=AlertSeverity.CRITICAL,
            title="定时风险检查失败",
            trigger_reason="MONITORING_CHECK_FAILED",
            data_as_of=now,
            now=now,
            commit=False,
        )
        append_audit(
            session,
            event_type="MONITORING_CHECK_FAILED",
            object_type="TaskRun",
            object_id=task.id,
            details={"idempotency_key": key},
        )
        session.commit()
        raise RuntimeError("MONITORING_CHECK_FAILED") from None
