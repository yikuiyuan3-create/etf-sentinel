from datetime import UTC, datetime, timedelta

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import func, select

from etf_sentinel import main
from etf_sentinel.database import get_db
from etf_sentinel.models import Alert, AuditLog, ProviderRegistry, Signal, SimulationFill, TaskRun
from etf_sentinel.services.monitoring import MONITOR_TASK_NAME, record_monitoring_check
from etf_sentinel.services.pipeline import run_demo_pipeline


def test_monitor_cannot_hide_missing_or_failed_daily_pipeline(
    db_session, monkeypatch, test_settings
):
    monkeypatch.setattr(main, "settings", test_settings)
    now = datetime.now(UTC)
    db_session.add(
        TaskRun(
            task_name=MONITOR_TASK_NAME,
            idempotency_key="monitoring-only",
            status="SUCCEEDED",
            started_at=now,
            completed_at=now,
        )
    )
    db_session.commit()
    assert main._candidate_output_health(db_session) == (True, "PIPELINE_NOT_RUN")
    db_session.add(
        TaskRun(
            task_name="daily_demo_pipeline",
            idempotency_key="failed-research",
            status="FAILED",
            started_at=now - timedelta(hours=1),
        )
    )
    db_session.commit()
    assert main._candidate_output_health(db_session) == (True, "PIPELINE_FAILED")


@pytest.mark.integration
def test_hourly_checks_are_read_only_for_signals_and_fills_and_recheck_license(
    db_session,
    monkeypatch,
    test_settings,
):
    monkeypatch.setattr(main, "settings", test_settings)
    run_demo_pipeline(db_session, test_settings)

    def override_db():
        yield db_session

    main.app.dependency_overrides[get_db] = override_db

    def count(model):
        return db_session.scalar(select(func.count(model.id)))

    before_signals, before_fills = count(Signal), count(SimulationFill)
    try:
        with TestClient(main.app) as client:
            response = client.get("/api/v1/monitoring")
            assert response.status_code == 200
            value = response.json()
            assert value["data_mode"] == "DEMO_FIXTURE"
            initial = value["data"]
            assert initial["analysis_status"] == "DEMO_ANALYSIS"
            assert initial["live_data_status"] == "COMPLIANCE_BLOCKED"
            assert initial["counts"]["signals"] == 60
            assert initial["market_data_as_of"].startswith("2025-01-30")
            assert initial["source_links"] and initial["data_snapshot_ids"]
            assert initial["schedule_status"] == "NOT_RUN"
            now = datetime.now(UTC)
            result = record_monitoring_check(
                db_session,
                test_settings,
                now=now,
                build_report=lambda: main.build_monitoring_report(db_session),
            )
            assert result["status"] == "SUCCEEDED"
            counts = {model: count(model) for model in [Alert, AuditLog, TaskRun]}
            replay = record_monitoring_check(
                db_session,
                test_settings,
                now=now,
                build_report=lambda: main.build_monitoring_report(db_session),
            )
            assert replay["idempotent_replay"]
            assert counts == {model: count(model) for model in counts}
            fresh = client.get("/api/v1/monitoring").json()["data"]
            assert fresh["schedule_status"] == "CURRENT"
            assert fresh["market_data_as_of"] == initial["market_data_as_of"]
            assert count(Signal) == before_signals
            assert count(SimulationFill) == before_fills
            provider = db_session.scalar(
                select(ProviderRegistry).where(ProviderRegistry.provider_code == "demo_fixture")
            )
            provider.expires_at = now - timedelta(seconds=1)
            db_session.commit()
            blocked = client.get("/api/v1/monitoring").json()["data"]
            assert blocked["analysis_status"] == "BLOCKED"
            assert blocked["counts"]["signals"] == 0
            assert blocked["source_links"] == []
            assert blocked["data_snapshot_ids"] == []
            assert blocked["market_data_as_of"] is None
            # Existing report is not returned from cache after rights expire.
            assert blocked["blocked_reason"]
            assert count(SimulationFill) == before_fills
            assert client.post("/api/v1/monitoring").status_code == 405
    finally:
        main.app.dependency_overrides.clear()
