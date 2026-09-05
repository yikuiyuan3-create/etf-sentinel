"""Demo-only recovery primitives and commands run inside the existing app image.

This module deliberately does not import the application until a container
command is selected. Its archive and fingerprint boundaries are unit-testable
without Docker, database credentials, or a running application.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import stat
import sys
import tarfile
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path, PurePosixPath
from typing import Any

FORBIDDEN_FLAGS = (
    "ENABLE_PUBLIC_SERVICE",
    "ENABLE_PAID_SUBSCRIPTIONS",
    "ENABLE_PURCHASE_REDIRECT",
    "ENABLE_THIRD_PARTY_FUNDS",
    "ENABLE_LIVE_TRADING",
    "ENABLE_ORDER_ENTRY",
    "TWELVE_DATA_ENABLED",
    "GDELT_ENABLED",
    "EMAIL_ALERTS_ENABLED",
    "WEBHOOK_ALERTS_ENABLED",
)
ARCHIVE_FILES = {"database.dump", "snapshots.tar", "source-evidence.json"}


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def demo_environment(environment: dict[str, str]) -> None:
    require(environment.get("APP_ENV") == "demo", "Only fixed demo APP_ENV is supported")
    require(environment.get("TRADING_MODE") == "paper", "Only paper is supported")
    require(
        environment.get("MARKET_DATA_PROVIDER") == "demo_fixture",
        "Only demo_fixture is supported",
    )
    for key in FORBIDDEN_FLAGS:
        require(environment.get(key, "false").lower() == "false", f"Forbidden flag: {key}")
    for key in ("TWELVE_DATA_API_KEY", "ALERT_WEBHOOK_URL"):
        require(not environment.get(key), "Configured external secrets are not supported")
    require(
        environment.get("SNAPSHOT_DIR") == "/app/var/snapshots"
        and environment.get("EXPORT_DIR") == "/app/var/exports",
        "Only the fixed Compose volume layout is supported",
    )


def canonical(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): canonical(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [canonical(item) for item in value]
    if isinstance(value, datetime):
        require(value.tzinfo is not None, "Naive database timestamp")
        return {"datetime_utc": value.astimezone(UTC).isoformat(timespec="microseconds")}
    if isinstance(value, date):
        return {"date": value.isoformat()}
    if isinstance(value, Decimal):
        return {"decimal": str(value)}
    if isinstance(value, bytes):
        return {"bytes": value.hex()}
    return value


def row_fingerprint(rows: list[dict]) -> dict:
    serialized = sorted(
        json.dumps(canonical(row), sort_keys=True, ensure_ascii=False, allow_nan=False)
        for row in rows
    )
    return {
        "count": len(rows),
        "sha256": hashlib.sha256("\n".join(serialized).encode()).hexdigest(),
    }


def fixture_provider_or_internal_health(table: str, row: dict) -> bool:
    if row.get("provider_code") == "demo_fixture":
        return True
    # Operational alerts are not vendor facts. Their signed payload is independently
    # checked by database_evidence; do not extend this exception to other records.
    return (
        table == "alerts"
        and row.get("provider_code") is None
        and row.get("source_object_type") == "System"
        and row.get("alert_type") in {"SYSTEM_HEALTH", "PORTFOLIO_RISK"}
        and row.get("instrument_id") is None
        and row.get("signal_id") is None
    )


def file_hash(file_path: Path) -> str:
    require(file_path.is_file() and not file_path.is_symlink(), "Not a regular backup file")
    digest = hashlib.sha256()
    with file_path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def private_write(file_path: Path, value: dict) -> None:
    descriptor = os.open(file_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w") as stream:
        json.dump(value, stream, ensure_ascii=False, sort_keys=True, indent=2)
        stream.write("\n")


def verify_manifest(run_dir: Path) -> dict:
    require(run_dir.is_dir() and not run_dir.is_symlink(), "Unsafe backup directory")
    require(stat.S_IMODE(run_dir.stat().st_mode) == 0o700, "Backup directory must be 0700")
    manifest_path = run_dir / "manifest.json"
    for name in ("manifest.json", "manifest.sha256"):
        control = run_dir / name
        require(control.is_file() and not control.is_symlink(), "Unsafe manifest control file")
        require(stat.S_IMODE(control.stat().st_mode) == 0o600, "Manifest control must be 0600")
    expected_manifest_hash = (run_dir / "manifest.sha256").read_text().strip()
    require(file_hash(manifest_path) == expected_manifest_hash, "Manifest hash mismatch")
    manifest = json.loads(manifest_path.read_text())
    require(manifest.get("data_mode") == "DEMO_FIXTURE", "Nonfixture archive rejected")
    require(set(manifest["files"]) == ARCHIVE_FILES, "Unexpected archive member list")
    for name, expected in manifest["files"].items():
        file_path = run_dir / name
        require(file_path.parent == run_dir, "Backup path escaped")
        require(stat.S_IMODE(file_path.stat().st_mode) == 0o600, "Backup file must be 0600")
        require(file_hash(file_path) == expected["sha256"], "Archive checksum mismatch")
        require(file_path.stat().st_size == expected["bytes"], "Archive length mismatch")
    validate_tar(run_dir / "snapshots.tar", manifest["snapshots"])
    return manifest


def validate_tar(archive_path: Path, expected: dict) -> None:
    seen: set[str] = set()
    with tarfile.open(archive_path, "r:") as archive:
        for member in archive:
            member_path = PurePosixPath(member.name)
            require(
                member.isfile()
                and not member_path.is_absolute()
                and len(member_path.parts) == 2
                and member_path.parts[0] == "snapshots"
                and member_path.suffix == ".parquet"
                and ".." not in member_path.parts,
                "Unsafe snapshot archive member",
            )
            require(member.name in expected and member.name not in seen, "Unexpected snapshot")
            seen.add(member.name)
            stream = archive.extractfile(member)
            require(stream is not None, "Snapshot payload missing")
            digest = hashlib.sha256()
            for chunk in iter(lambda stream=stream: stream.read(1024 * 1024), b""):
                digest.update(chunk)
            require(digest.hexdigest() == expected[member.name]["file_sha256"], "Snapshot tamper")
            require(member.size == expected[member.name]["bytes"], "Snapshot length mismatch")
    require(seen == set(expected), "Missing snapshot files")


def audit_chain(rows: list[dict]) -> dict:
    require(bool(rows), "Missing audit chain")
    known: dict[str, dict] = {}
    successors: dict[str | None, str] = {}
    payload_keys = (
        "event_type",
        "object_type",
        "object_id",
        "actor",
        "trace_id",
        "details",
        "previous_hash",
    )
    for row in rows:
        payload = {key: row[key] for key in payload_keys}
        expected = hashlib.sha256(
            json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str).encode()
        ).hexdigest()
        require(expected == row["record_hash"], "Audit payload hash mismatch")
        require(expected not in known, "Duplicate audit hash")
        require(row["previous_hash"] not in successors, "Audit chain fork")
        successors[row["previous_hash"]] = expected
        known[expected] = row
    visited: set[str] = set()
    current = successors.get(None)
    while current is not None:
        require(current not in visited, "Audit chain cycle")
        visited.add(current)
        current = successors.get(current)
    require(visited == set(known), "Broken audit chain")
    return {"count": len(rows), "structure": "PASS", "payload_hashes": "PASS"}


def evidence() -> dict:
    from sqlalchemy import MetaData, select

    from etf_sentinel.config import get_settings
    from etf_sentinel.database import SessionLocal, engine
    from etf_sentinel.models import DataSnapshot, ProviderRegistry
    from etf_sentinel.services.pipeline import read_snapshot_with_duckdb
    from etf_sentinel.services.signals import code_version

    settings = get_settings()
    demo_environment(dict(os.environ))
    require(engine.dialect.name == "postgresql", "Only PostgreSQL is supported")
    require(
        all(
            file.name == ".gitkeep" and file.is_file() and file.stat().st_size == 0
            for file in settings.export_dir.iterdir()
        ),
        "Exports are nonempty: define a reviewed backup whitelist before proceeding",
    )
    with SessionLocal() as session:
        registry = list(session.scalars(select(ProviderRegistry)))
        require(
            all(
                row.provider_code == "demo_fixture" or row.review_status != "APPROVED"
                for row in registry
            ),
            "A real provider is approved: Demo backup exception cannot be used",
        )
        snapshots = {}
        for row in session.scalars(select(DataSnapshot)):
            require(row.data_mode == "DEMO_FIXTURE", "Nonfixture snapshot")
            require(row.provider_code == "demo_fixture", "Nonfixture provider")
            file_path = Path(row.parquet_uri)
            require(
                file_path.parent == settings.snapshot_dir
                and file_path.resolve().parent == settings.snapshot_dir.resolve()
                and not file_path.is_symlink(),
                "Snapshot path outside fixed volume",
            )
            read_snapshot_with_duckdb(
                file_path, expected_hash=row.snapshot_hash, expected_rows=row.row_count
            )
            snapshots[f"snapshots/{file_path.name}"] = {
                "file_sha256": file_hash(file_path),
                "bytes": file_path.stat().st_size,
                "snapshot_hash": row.snapshot_hash,
                "row_count": row.row_count,
            }
        require(bool(snapshots), "No snapshots")
        require(
            {
                f"snapshots/{file.name}"
                for file in settings.snapshot_dir.iterdir()
                if file.name != ".gitkeep"
            }
            == set(snapshots),
            "Unregistered snapshot files require review before backup",
        )
    metadata = MetaData()
    metadata.reflect(bind=engine)
    tables = {}
    with engine.connect().execution_options(isolation_level="REPEATABLE READ") as connection:
        with connection.begin():
            connection.exec_driver_sql("SET TRANSACTION READ ONLY")
            for name, table in sorted(metadata.tables.items()):
                rows = [dict(row) for row in connection.execute(select(table)).mappings()]
                for row in rows:
                    if "data_mode" in row:
                        require(row["data_mode"] == "DEMO_FIXTURE", "Nonfixture fact record")
                    if "provider_code" in row and name != "provider_registry":
                        require(
                            fixture_provider_or_internal_health(name, row),
                            "Nonfixture fact provider",
                        )
                tables[name] = row_fingerprint(rows)
                if name == "audit_logs":
                    audit = audit_chain(rows)
    script_path = Path(__file__).with_name("verify-compose.py")
    spec = importlib.util.spec_from_file_location("compose_acceptance", script_path)
    require(spec is not None and spec.loader is not None, "Missing integrity verifier")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return {
        "data_mode": "DEMO_FIXTURE",
        "code_version": code_version(),
        "tables": tables,
        "snapshots": snapshots,
        "audit_chain": audit,
        "database": module.database_evidence(),
    }


def idle() -> dict:
    from redis import Redis
    from sqlalchemy import func, select

    from etf_sentinel.config import get_settings
    from etf_sentinel.database import SessionLocal
    from etf_sentinel.models import TaskRun
    from etf_sentinel.tasks import celery_app

    demo_environment(dict(os.environ))
    inspector = celery_app.control.inspect(timeout=10)
    for method in (inspector.active, inspector.reserved, inspector.scheduled):
        replies = method()
        require(bool(replies), "Worker did not confirm idle state")
        require(all(not tasks for tasks in replies.values()), "Worker has pending tasks")
    client = Redis.from_url(get_settings().redis_url, socket_timeout=10)
    # The default queue plus Kombu priority queues; no queue payload is logged.
    require(not list(client.scan_iter(match="etf-sentinel:lock:*")), "Active pipeline lock")
    for key in client.scan_iter(match="celery*"):
        if client.type(key) == b"list":
            require(client.llen(key) == 0, "Broker queue is not empty")
    for key in ("unacked", "unacked_index"):
        require(not client.exists(key), "Unacknowledged broker delivery")
    with SessionLocal() as session:
        running = session.scalar(
            select(func.count()).select_from(TaskRun).where(TaskRun.status == "RUNNING")
        )
        require(running == 0, "Database contains a RUNNING task")
    return {"workers_idle": True, "broker_pending": 0, "locks": 0, "database_running_tasks": 0}


def replay() -> dict:
    from etf_sentinel.config import get_settings
    from etf_sentinel.database import SessionLocal
    from etf_sentinel.services.pipeline import run_demo_pipeline

    before = evidence()
    with SessionLocal() as session:
        result = run_demo_pipeline(session, get_settings())
    after = evidence()
    require(result.get("idempotent_replay") is True, "Restored replay did not reuse task key")
    # A replay legitimately refreshes TaskRun and appends a PIPELINE_COMPLETED audit.
    immutable_tables = set(before["tables"]) - {"task_runs", "audit_logs"}
    require(
        all(before["tables"][name] == after["tables"][name] for name in immutable_tables),
        "Restored replay changed business rows",
    )
    require(before["database"] == after["database"], "Restored replay changed ledger or counts")
    return {"idempotent_replay": True, "business_tables_unchanged": True, "after": after}


def http_check() -> dict:
    import httpx

    from etf_sentinel.enums import DISCLAIMER

    with httpx.Client(base_url="http://127.0.0.1:8000", trust_env=False, timeout=30) as client:
        health = client.get("/health/ready")
        require(health.status_code == 200, "Restored web did not respond")
        require(health.json()["status"] == "degraded_fail_closed", "Kill switch not enforced")
        response = client.get("/api/v1/signals")
        require(response.status_code == 200, "Restored signal endpoint failed")
        payload = response.json()
        require(payload["data"] == [], "Kill switch leaked candidate signals")
        require(payload["data_mode"] == "DEMO_FIXTURE", "Restored mode mismatch")
        require(payload["disclaimer"] == DISCLAIMER, "Restored disclaimer missing")
    return {"http": 200, "kill_switch": True, "candidate_count": 0, "data_mode": "DEMO_FIXTURE"}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "command", choices=["evidence", "idle", "replay", "http", "archive", "extract"]
    )
    args = parser.parse_args()
    if args.command == "archive":
        proof = evidence()
        with tarfile.open(fileobj=sys.stdout.buffer, mode="w|") as archive:
            for name in sorted(proof["snapshots"]):
                archive.add(Path("/app/var") / name, arcname=name, recursive=False)
        return
    if args.command == "extract":
        manifest = verify_manifest(Path("/backup"))
        destination = Path("/app/var")
        require(not any(destination.iterdir()), "Restore volume is not empty")
        # validate_tar above rejects links/traversal/duplicates before any writes.
        with tarfile.open("/backup/snapshots.tar", "r:") as archive:
            archive.extractall(destination, filter="data")
        (destination / "exports").mkdir()
        for name in manifest["snapshots"]:
            os.chmod(destination / name, 0o600)
        # Use the existing image account, not a guessed UID from another image.
        import pwd

        account = pwd.getpwnam("sentinel")
        for current, directories, files in os.walk(destination):
            os.chown(current, account.pw_uid, account.pw_gid)
            os.chmod(current, 0o700)
            for name in directories + files:
                os.chown(Path(current) / name, account.pw_uid, account.pw_gid)
        print(json.dumps({"restored_snapshots": len(manifest["snapshots"])}))
        return
    function = {"evidence": evidence, "idle": idle, "replay": replay, "http": http_check}[
        args.command
    ]
    print(json.dumps(function(), ensure_ascii=False, sort_keys=True, indent=2))


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        # No exception text: third-party exceptions may contain DSNs or raw rows.
        print(json.dumps({"result": "FAIL", "error_type": type(error).__name__}), file=sys.stderr)
        raise SystemExit(1) from None
