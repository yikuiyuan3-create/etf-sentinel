"""Exercise the deployed Demo through PostgreSQL, Redis, Celery and HTTP.

Run with: docker compose exec -T web python /app/scripts/verify-compose.py
Only the fixed Demo is accepted. Two real queue deliveries exercise the lock and
idempotent replay; no configuration, provider approval, or business data is reset.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

import httpx
from redis import Redis
from sqlalchemy import func, select, text

from etf_sentinel.config import get_settings
from etf_sentinel.database import SessionLocal, engine
from etf_sentinel.enums import DISCLAIMER
from etf_sentinel.models import (
    Alert,
    BacktestExperiment,
    DataSnapshot,
    EtfInstrument,
    ModelVersion,
    NewsEvent,
    ScheduledEvent,
    Signal,
    SimulationDecision,
    SimulationFill,
    SimulationLedger,
    TaskRun,
)
from etf_sentinel.services.alerts import verify_alert_content
from etf_sentinel.services.backtest import verify_backtest_artifact
from etf_sentinel.services.fact_integrity import (
    verify_news_event_record,
    verify_scheduled_event_record,
)
from etf_sentinel.services.ledger import assert_simulation_ledger_integrity, portfolio_state
from etf_sentinel.services.logistic_model import verify_logistic_model_artifact
from etf_sentinel.services.signals import code_version, verify_signal_record
from etf_sentinel.tasks import celery_app


def check(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def database_evidence() -> dict:
    with SessionLocal() as session:
        snapshots = list(session.scalars(select(DataSnapshot)))
        check(bool(snapshots), "Demo snapshots are missing")
        check(
            all(row.data_mode == "DEMO_FIXTURE" for row in snapshots),
            "Compose acceptance only permits DEMO_FIXTURE",
        )
        market = next(row for row in snapshots if row.dataset_type == "MARKET_BARS")
        for model, validator in [
            (Signal, verify_signal_record),
            (Alert, verify_alert_content),
            (NewsEvent, verify_news_event_record),
            (ScheduledEvent, verify_scheduled_event_record),
        ]:
            rows = list(session.scalars(select(model)))
            check(bool(rows) and all(validator(row) for row in rows), f"{model.__name__} integrity")
        backtests = list(session.scalars(select(BacktestExperiment)))
        check(
            bool(backtests)
            and all(
                verify_backtest_artifact(row, snapshot_hash=market.snapshot_hash)
                for row in backtests
            ),
            "Backtest integrity",
        )
        logistic = list(
            session.scalars(
                select(ModelVersion).where(ModelVersion.model_name == "LogisticBaselineV1")
            )
        )
        check(
            len(logistic) >= 2
            and {row.metrics.get("horizon_days") for row in logistic} == {5, 20}
            and all(verify_logistic_model_artifact(row) for row in logistic),
            "Logistic artifact integrity",
        )
        assert_simulation_ledger_integrity(session)
        portfolio = portfolio_state(session)
        counts = {
            model.__tablename__: session.scalar(select(func.count()).select_from(model))
            for model in [
                EtfInstrument,
                DataSnapshot,
                NewsEvent,
                ScheduledEvent,
                Signal,
                Alert,
                SimulationDecision,
                SimulationFill,
                SimulationLedger,
                BacktestExperiment,
                ModelVersion,
                TaskRun,
            ]
        }
        current_count = session.scalar(
            select(func.count()).select_from(Signal).where(Signal.is_current.is_(True))
        )
        check(
            counts["etf_instruments"] == 60 and counts["signals"] >= 60 and current_count == 60,
            "Demo universe/current signals",
        )
        check(counts["simulation_fills"] > 0, "Demo fills missing")
        return {
            "migration": session.execute(
                text("select version_num from alembic_version")
            ).scalar_one(),
            "counts": counts,
            "snapshot_hashes": {row.dataset_type: row.snapshot_hash for row in snapshots},
            "cash": portfolio.cash,
            "positions": len(portfolio.positions),
            "fees": portfolio.total_fees,
            "model_statuses": sorted(row.status for row in logistic),
            "integrity": "PASS",
        }


def main() -> None:
    settings = get_settings()
    check(settings.trading_mode == "paper", "Only paper mode is allowed")
    check(engine.dialect.name == "postgresql", "PostgreSQL is required for Compose acceptance")
    redis_client = Redis.from_url(settings.redis_url, decode_responses=True)
    check(redis_client.ping(), "Redis ping failed")
    with engine.connect() as connection:
        postgres_version = connection.execute(text("show server_version")).scalar_one()

    before = database_evidence()
    checks: dict[str, int] = {}
    with httpx.Client(base_url="http://127.0.0.1:8000", trust_env=False, timeout=60) as client:
        paths = [
            "/health",
            "/health/live",
            "/health/ready",
            "/api/v1/status",
            "/api/v1/etfs",
            "/api/v1/signals",
            "/api/v1/alerts",
            "/api/v1/portfolio",
            "/api/v1/backtests/latest",
            "/api/v1/providers",
            "/api/v1/models",
            "/api/v1/audit",
            "/",
            "/openapi.json",
        ]
        for path in paths:
            response = client.get(path)
            check(response.status_code == 200, f"HTTP {path}: {response.status_code}")
            checks[path] = response.status_code
            if path.startswith("/api/v1/"):
                body = response.json()
                check(body["data_mode"] == "DEMO_FIXTURE", f"Data mode: {path}")
                check(body["disclaimer"] == DISCLAIMER, f"Disclaimer: {path}")
            if path == "/":
                check(
                    "DEMO_FIXTURE" in response.text and DISCLAIMER in response.text,
                    "Dashboard labels",
                )
        check(client.get("/health/ready").json()["status"] == "ok", "Readiness failed closed")
        signals = client.get("/api/v1/signals").json()["data"]
        check(len(signals) == 60, "Visible signals missing")
        detail = client.get(f"/signals/{signals[0]['id']}")
        check(detail.status_code == 200 and DISCLAIMER in detail.text, "Signal detail")
        checks["/signals/{id}"] = detail.status_code
        routes = client.get("/openapi.json").json()["paths"]
        check(not any("order" in route or "broker" in route for route in routes), "Forbidden route")

    replies = celery_app.control.ping(timeout=10)
    check(bool(replies), "No Celery worker replied")
    # Real Redis broker + prefork worker + Redis result backend, never eager mode.
    check(not celery_app.conf.task_always_eager, "Celery eager mode is not accepted")
    jobs = [celery_app.send_task("etf_sentinel.daily_pipeline") for _ in range(2)]
    results = [job.get(timeout=300, propagate=True) for job in jobs]
    statuses = [result["status"] for result in results]
    check(
        all(value in {"SUCCEEDED", "SKIPPED_DUPLICATE"} for value in statuses),
        "Queue result failed",
    )
    check("SUCCEEDED" in statuses, "No queue replay completed")
    check(
        all(
            result.get("idempotent_replay") is True
            for result in results
            if result["status"] == "SUCCEEDED"
        ),
        "Queue replay was not idempotent",
    )
    date_key = settings.demo_evaluation_time.astimezone(UTC).date().isoformat()
    check(not redis_client.exists(f"etf-sentinel:lock:daily:{date_key}"), "Task lock not released")
    after = database_evidence()
    check(before == after, "Queue replay changed business counts, cash, or hashes")
    print(
        json.dumps(
            {
                "checked_at": datetime.now(UTC).isoformat(),
                "data_mode": "DEMO_FIXTURE",
                "code_version": code_version(),
                "postgres_version": postgres_version,
                "redis_version": redis_client.info("server")["redis_version"],
                "http": checks,
                "celery": {
                    "worker_replies": len(replies),
                    "statuses": statuses,
                    "eager": False,
                    "lock_released": True,
                },
                "database": after,
                "replay_unchanged": before == after,
                "result": "PASS",
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
