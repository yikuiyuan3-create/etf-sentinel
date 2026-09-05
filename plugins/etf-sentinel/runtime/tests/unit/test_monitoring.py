import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError
from sqlalchemy import func, select

from etf_sentinel.config import Settings
from etf_sentinel.models import Alert, AuditLog, SimulationFill, TaskRun
from etf_sentinel.services.monitoring import (
    MONITOR_TASK_NAME,
    monitoring_window,
    record_monitoring_check,
    schedule_state,
)


@pytest.mark.parametrize("hours", [1, 2])
def test_monitor_window_uses_real_clock_not_fixture_clock(hours):
    now = datetime.fromisoformat("2026-09-05T09:47:00+08:00")
    start, end = monitoring_window(now, hours)
    assert start == datetime(2026, 9, 5, 1 if hours == 1 else 0, tzinfo=UTC)
    assert end == datetime(2026, 9, 5, 2, tzinfo=UTC)
    assert monitoring_window(end, hours)[0] == end


@pytest.mark.parametrize("hours", [0, 3, -1, 24])
def test_monitor_interval_only_one_or_two_hours(hours):
    with pytest.raises(ValidationError):
        Settings(_env_file=None, monitoring_interval_hours=hours)


def test_monitor_rejects_naive_time():
    with pytest.raises(ValueError):
        monitoring_window(datetime(2026, 9, 5), 1)


@pytest.mark.parametrize("hours", [1, 2])
def test_celery_hour_schedule_environment(hours):
    env = {**os.environ, "PYTHONPATH": "src", "MONITORING_INTERVAL_HOURS": str(hours)}
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "from etf_sentinel.tasks import celery_app; "
            "s=celery_app.conf.beat_schedule; "
            "print(sorted(s['hourly-risk-monitor']['schedule'].hour)); "
            "assert s['daily-close-research-pipeline']['schedule'].hour == {16}",
        ],
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=20,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == str(list(range(0, 24, hours)))


def test_schedule_overdue_does_not_follow_api_refresh_clock():
    now = datetime(2026, 9, 5, 4, 6, tzinfo=UTC)
    task = TaskRun(
        task_name=MONITOR_TASK_NAME,
        status="SUCCEEDED",
        started_at=now - timedelta(hours=2),
        completed_at=now - timedelta(hours=2),
        idempotency_key="test",
    )
    assert schedule_state(None, now, 1) == "NOT_RUN"
    assert schedule_state(task, now, 1) == "OVERDUE"
    task.completed_at = now - timedelta(minutes=4)
    assert schedule_state(task, now, 1) == "CURRENT"
    task.status = "FAILED"
    assert schedule_state(task, now, 1) == "FAILED"


def report(now):
    return {
        "data_mode": "DEMO_FIXTURE",
        "checked_at": now.isoformat(),
        "market_data_as_of": "2025-01-30T07:00:00+00:00",
        "live_data_status": "COMPLIANCE_BLOCKED",
        "analysis_status": "DEMO_ANALYSIS",
        "blocked_reason": None,
        "data_snapshot_ids": ["fixed-demo"],
        "code_version": "test",
        "findings": ["固定夹具，不是新行情"],
    }


def test_monitor_replay_and_new_hour_never_create_fills(db_session, test_settings):
    now = datetime(2026, 9, 5, 4, tzinfo=UTC)
    first = record_monitoring_check(
        db_session, test_settings, now=now, build_report=lambda: report(now)
    )
    second = record_monitoring_check(
        db_session, test_settings, now=now, build_report=lambda: pytest.fail("重复时不能再次计算")
    )
    third = record_monitoring_check(
        db_session,
        test_settings,
        now=now + timedelta(hours=1),
        build_report=lambda: report(now + timedelta(hours=1)),
    )
    assert first["status"] == third["status"] == "SUCCEEDED"
    assert second["idempotent_replay"] is True
    assert db_session.scalar(select(func.count(TaskRun.id))) == 2
    assert db_session.scalar(select(func.count(SimulationFill.id))) == 0
    assert db_session.scalar(select(func.count(Alert.id))) == 1
    assert (
        db_session.scalar(
            select(func.count(AuditLog.id)).where(
                AuditLog.event_type == "MONITORING_CHECK_COMPLETED"
            )
        )
        == 2
    )


def test_monitor_failure_is_recorded_without_exception_contents(db_session, test_settings):
    now = datetime(2026, 9, 5, 4, tzinfo=UTC)

    def fail():
        raise TimeoutError("SECRET_CANARY_DO_NOT_LOG")

    with pytest.raises(RuntimeError, match="MONITORING_CHECK_FAILED"):
        record_monitoring_check(db_session, test_settings, now=now, build_report=fail)
    task = db_session.scalar(select(TaskRun))
    assert task.status == "FAILED"
    assert "SECRET_CANARY" not in str(task.result_summary) + str(task.safe_error_message)
    retry = record_monitoring_check(
        db_session, test_settings, now=now, build_report=lambda: report(now)
    )
    assert retry["status"] == "SUCCEEDED"
    assert db_session.scalar(select(func.count(TaskRun.id))) == 1


def test_new_failure_after_recovery_gets_new_alert(db_session, test_settings):
    now = datetime(2026, 9, 5, 4, tzinfo=UTC)

    def fail():
        raise TimeoutError("unavailable")

    for offset in [0, 1]:
        with pytest.raises(RuntimeError):
            record_monitoring_check(
                db_session, test_settings, now=now + timedelta(hours=offset), build_report=fail
            )
    assert db_session.scalar(select(func.count(Alert.id))) == 1
    record_monitoring_check(
        db_session, test_settings, now=now + timedelta(hours=2), build_report=lambda: report(now)
    )
    with pytest.raises(RuntimeError):
        record_monitoring_check(
            db_session, test_settings, now=now + timedelta(hours=3), build_report=fail
        )
    assert db_session.scalar(select(func.count(Alert.id))) == 3


@pytest.mark.parametrize(("retries", "expected_delay"), [(0, 5), (1, 61)])
def test_orphaned_redis_lease_retries_instead_of_acknowledging(
    monkeypatch, retries, expected_delay
):
    from celery.exceptions import Retry

    from etf_sentinel import tasks

    class BusyRedis:
        def set(self, *_args, **_kwargs):
            return False

        def ttl(self, *_args):
            return 60

        def close(self):
            pass

    monkeypatch.setattr(tasks.Redis, "from_url", lambda *_args, **_kwargs: BusyRedis())

    def retry(*, exc, countdown):
        assert str(exc) == "MONITORING_LEASE_BUSY"
        assert countdown == expected_delay
        raise Retry(when=countdown)

    monkeypatch.setattr(tasks.hourly_monitor, "retry", retry)
    tasks.hourly_monitor.push_request(retries=retries)
    try:
        with pytest.raises(Retry):
            tasks.hourly_monitor.run()
    finally:
        tasks.hourly_monitor.pop_request()


def test_audit_head_is_not_selected_by_wall_clock(db_session):
    from sqlalchemy import update

    from etf_sentinel.audit import append_audit

    first = append_audit(db_session, event_type="FIRST", object_type="Test")
    # Fault injection: a clock jumping forwards must not determine the chain tail.
    db_session.execute(
        update(AuditLog)
        .where(AuditLog.id == first.id)
        .values(occurred_at=datetime(2099, 1, 1, tzinfo=UTC))
    )
    second = append_audit(db_session, event_type="SECOND", object_type="Test")
    third = append_audit(db_session, event_type="THIRD", object_type="Test")
    assert second.previous_hash == first.record_hash
    assert third.previous_hash == second.record_hash
