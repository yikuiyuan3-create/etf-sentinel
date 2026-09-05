"""Fixed-Demo upgrade / hourly queue acceptance; never resets or replaces data.

--upgrade runs the existing versioned Demo pipeline and proves old ledger rows are
unchanged. Rehearse on an isolated restored copy before using on the source Demo.
Without the flag, exercise the real Celery/Redis worker and the monitoring HTTP API.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
from pathlib import Path

import httpx
from redis import Redis

from etf_sentinel.config import get_settings
from etf_sentinel.database import SessionLocal
from etf_sentinel.services.pipeline import run_demo_pipeline
from etf_sentinel.tasks import celery_app

SPEC = importlib.util.spec_from_file_location(
    "hourly_recovery_support", Path(__file__).with_name("recovery-support.py")
)
assert SPEC is not None and SPEC.loader is not None
support = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(support)

LEDGER_TABLES = {"simulation_decisions", "simulation_fills", "simulation_ledger"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--upgrade", action="store_true")
    args = parser.parse_args()
    before = support.evidence()  # Includes Demo-only and secret-free environment gates.
    settings = get_settings()
    if args.upgrade:
        with SessionLocal() as session:
            first = run_demo_pipeline(session, settings)
        after = support.evidence()
        preserved = LEDGER_TABLES | {
            "data_snapshots",
            "etf_instruments",
            "news_events",
            "scheduled_events",
            "provider_registry",
        }
        support.require(
            all(before["tables"][name] == after["tables"][name] for name in preserved),
            "Upgrade changed existing ledger or source facts; stop rollout",
        )
        with SessionLocal() as session:
            replay = run_demo_pipeline(session, settings)
        repeated = support.evidence()
        support.require(replay.get("idempotent_replay") is True, "Upgrade replay not idempotent")
        support.require(
            all(
                after["tables"][name] == repeated["tables"][name]
                for name in set(after["tables"]) - {"task_runs", "audit_logs"}
            ),
            "Upgrade replay changed business records",
        )
        result = {
            "upgrade_idempotent_first": first.get("idempotent_replay"),
            "ledger_and_sources_preserved": True,
            "new_version_replay_unchanged": True,
        }
    else:
        support.require(not celery_app.conf.task_always_eager, "Real worker required")
        jobs = [celery_app.send_task("etf_sentinel.hourly_monitor") for _ in range(2)]
        values = [job.get(timeout=150) for job in jobs]
        support.require(any(item["status"] == "SUCCEEDED" for item in values), "No completed job")
        support.require(
            all(item["status"] in {"SUCCEEDED", "SKIPPED_DUPLICATE"} for item in values),
            "Unexpected job failure",
        )
        after = support.evidence()
        preserved = set(before["tables"]) - {"task_runs", "audit_logs", "alerts"}
        support.require(
            all(before["tables"][name] == after["tables"][name] for name in preserved),
            "Hourly check changed research/fill/source records",
        )
        replay = celery_app.send_task("etf_sentinel.hourly_monitor").get(timeout=150)
        support.require(replay.get("idempotent_replay") is True, "Hourly replay not deduplicated")
        repeated = support.evidence()
        support.require(after["tables"] == repeated["tables"], "Duplicate changed stored rows")
        with Redis.from_url(settings.redis_url, socket_timeout=5) as redis:
            support.require(not redis.exists("etf-sentinel:lock:hourly-monitor"), "Lock leaked")
        with httpx.Client(base_url="http://127.0.0.1:8000", timeout=30, trust_env=False) as client:
            response = client.get("/api/v1/monitoring")
            support.require(response.status_code == 200, "Monitoring HTTP failed")
            report = response.json()["data"]
            support.require(report["schedule_status"] == "CURRENT", "Scheduler not current")
            support.require(report["analysis_status"] == "DEMO_ANALYSIS", "Analysis blocked")
            support.require(report["live_data_status"] == "COMPLIANCE_BLOCKED", "Live gate lost")
            support.require(report["counts"]["signals"] == 60, "Signal lineage missing")
        result = {
            "queue_results": values,
            "idempotent_replay": True,
            "lock_released": True,
            "research_and_ledger_unchanged": True,
            "http_report": report,
        }
    print(
        json.dumps(
            {
                "result": "PASS",
                "data_mode": "DEMO_FIXTURE",
                **result,
                "before": before,
                "after": repeated,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
