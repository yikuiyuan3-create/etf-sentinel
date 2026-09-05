"""Append four concurrency probes only to an isolated Demo recovery database.

Run inside a recovery app container with --execute. No source database writes,
record deletion, secret output, or cleanup are performed by this script.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import re
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier
from uuid import uuid4

from sqlalchemy import create_engine, select
from sqlalchemy.engine import make_url
from sqlalchemy.orm import sessionmaker


def require(condition: bool, reason: str) -> None:
    if not condition:
        raise RuntimeError(reason)


def validate_target(environment: dict[str, str]):
    """Validate without importing app settings or opening a database connection."""
    require(environment.get("APP_ENV") == "demo", "APP_ENV_NOT_DEMO")
    require(environment.get("TRADING_MODE") == "paper", "TRADING_MODE_NOT_PAPER")
    try:
        target = make_url(environment.get("DATABASE_URL", ""))
    except Exception:
        raise RuntimeError("INVALID_DATABASE_TARGET") from None
    require(target.get_backend_name() == "postgresql", "POSTGRESQL_REQUIRED")
    require(
        bool(target.host)
        and target.host.startswith("etf-sentinel-recovery-")
        and re.fullmatch(r"etf-sentinel-recovery-[a-z0-9][a-z0-9-]*", target.host) is not None
        and target.host != "postgres",
        "ISOLATED_RECOVERY_HOST_REQUIRED",
    )
    # Query-string connection overrides must not redirect a recovery-looking URL.
    require(not target.query, "DATABASE_QUERY_OVERRIDES_FORBIDDEN")
    return target


def execute() -> dict:
    target = validate_target(dict(os.environ))
    spec = importlib.util.spec_from_file_location(
        "audit_probe_recovery_support", Path(__file__).with_name("recovery-support.py")
    )
    require(spec is not None and spec.loader is not None, "RECOVERY_SUPPORT_UNAVAILABLE")
    support = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(support)
    support.demo_environment(dict(os.environ))

    from etf_sentinel.audit import append_audit
    from etf_sentinel.config import get_settings
    from etf_sentinel.models import AuditLog

    settings = get_settings()
    require(settings.app_env == "demo" and settings.trading_mode == "paper", "CONFIG_NOT_DEMO")
    require(make_url(settings.database_url) == target, "EFFECTIVE_DATABASE_TARGET_MISMATCH")
    engine = create_engine(
        target,
        pool_pre_ping=True,
        connect_args={
            "connect_timeout": 5,
            "options": "-c statement_timeout=15000 -c lock_timeout=10000",
        },
    )
    require(engine.dialect.name == "postgresql", "POSTGRESQL_DIALECT_REQUIRED")
    sessions = sessionmaker(bind=engine, expire_on_commit=False)

    def rows() -> list[dict]:
        with engine.connect() as connection:
            return [dict(row) for row in connection.execute(select(AuditLog.__table__)).mappings()]

    try:
        before = rows()
        support.audit_chain(before)
        before_by_id = {row["id"]: row for row in before}
        barrier = Barrier(2, timeout=20)
        run_id = str(uuid4())

        def worker(worker_number: int) -> list[str]:
            inserted = []
            with sessions() as session:
                for sequence in range(2):
                    barrier.wait()
                    record = append_audit(
                        session,
                        event_type="AUDIT_CONCURRENCY_PROBE",
                        object_type="RecoveryVerification",
                        object_id=run_id,
                        actor="isolated-recovery-probe",
                        details={"worker": worker_number, "sequence": sequence},
                    )
                    session.commit()
                    inserted.append(record.id)
            return inserted

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(worker, index) for index in range(2)]
            inserted_ids = {record_id for future in futures for record_id in future.result()}

        after = rows()
        chain = support.audit_chain(after)
        after_by_id = {row["id"]: row for row in after}
        require(len(after) == len(before) + 4, "UNEXPECTED_AUDIT_COUNT")
        require(len(inserted_ids) == 4, "UNEXPECTED_PROBE_COUNT")
        require(
            all(after_by_id.get(record_id) == row for record_id, row in before_by_id.items()),
            "EXISTING_AUDIT_RECORD_CHANGED",
        )
        require(set(after_by_id) - set(before_by_id) == inserted_ids, "UNEXPECTED_NEW_AUDIT_ROWS")
        require(
            all(
                after_by_id[key]["event_type"] == "AUDIT_CONCURRENCY_PROBE" for key in inserted_ids
            ),
            "UNEXPECTED_PROBE_EVENT_TYPE",
        )
        return {
            "result": "PASS",
            "scope": "ISOLATED_RECOVERY_DEMO_ONLY",
            "workers": 2,
            "commits_per_worker": 2,
            "appended": 4,
            "before_count": len(before),
            "after_count": len(after),
            "audit_chain": chain,
            "existing_records": "UNCHANGED",
        }
    finally:
        engine.dispose()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true", help="Append probes to a recovery copy")
    args = parser.parse_args()
    if not args.execute:
        parser.print_help()
        return 0
    try:
        result = execute()
    except Exception:
        # Database exceptions can embed URLs, credentials and SQL parameters.
        print(json.dumps({"result": "FAILED", "reason": "TARGET_GUARD_OR_AUDIT_VALIDATION_FAILED"}))
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
