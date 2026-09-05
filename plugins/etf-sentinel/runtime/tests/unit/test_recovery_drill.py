from __future__ import annotations

import hashlib
import importlib.util
import io
import json
import os
import tarfile
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pytest

SCRIPT_DIR = Path(__file__).resolve().parents[2] / "scripts"
SPEC = importlib.util.spec_from_file_location(
    "recovery_support", SCRIPT_DIR / "recovery-support.py"
)
assert SPEC is not None and SPEC.loader is not None
support = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(support)


def fixed_environment():
    return {
        "APP_ENV": "demo",
        "TRADING_MODE": "paper",
        "MARKET_DATA_PROVIDER": "demo_fixture",
        "SNAPSHOT_DIR": "/app/var/snapshots",
        "EXPORT_DIR": "/app/var/exports",
    }


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("APP_ENV", "production"),
        ("TRADING_MODE", "live"),
        ("MARKET_DATA_PROVIDER", "twelve_data"),
        ("GDELT_ENABLED", "true"),
        ("TWELVE_DATA_ENABLED", "true"),
        ("EMAIL_ALERTS_ENABLED", "true"),
        ("WEBHOOK_ALERTS_ENABLED", "true"),
        ("ENABLE_ORDER_ENTRY", "true"),
        ("ENABLE_PUBLIC_SERVICE", "true"),
        ("TWELVE_DATA_API_KEY", "test-not-a-real-key"),
        ("ALERT_WEBHOOK_URL", "https://example.invalid"),
        ("SNAPSHOT_DIR", "/outside/snapshots"),
    ],
)
def test_nonfixture_or_external_configuration_is_refused(key, value):
    environment = fixed_environment()
    environment[key] = value
    with pytest.raises(RuntimeError):
        support.demo_environment(environment)


def test_fixed_demo_configuration_is_accepted():
    support.demo_environment(fixed_environment())


def test_internal_health_is_not_mistaken_for_external_vendor_fact():
    row = {
        "provider_code": None,
        "source_object_type": "System",
        "alert_type": "SYSTEM_HEALTH",
        "instrument_id": None,
        "signal_id": None,
    }
    assert support.fixture_provider_or_internal_health("alerts", row)
    assert not support.fixture_provider_or_internal_health("news_events", row)
    assert not support.fixture_provider_or_internal_health(
        "alerts", {**row, "provider_code": "real"}
    )
    assert not support.fixture_provider_or_internal_health("alerts", {**row, "signal_id": "id"})
    assert not support.fixture_provider_or_internal_health("alerts", {**row, "alert_type": "NEWS"})


def test_fingerprint_is_order_independent_timezone_canonical_and_content_complete():
    utc = datetime(2025, 1, 1, tzinfo=UTC)
    local = utc.astimezone(timezone(timedelta(hours=8)))
    first = [{"id": 1, "details": {"a": 1, "b": 2}, "time": utc}, {"id": 2, "v": 3}]
    second = [{"v": 3, "id": 2}, {"time": local, "details": {"b": 2, "a": 1}, "id": 1}]
    assert support.row_fingerprint(first) == support.row_fingerprint(second)
    second[1]["details"]["b"] = 99
    assert support.row_fingerprint(first) != support.row_fingerprint(second)
    assert support.row_fingerprint(first)["count"] == 2


def make_archive(archive_path: Path, name="snapshots/demo.parquet", *, kind=tarfile.REGTYPE):
    payload = b"deterministic archive boundary fixture, not market data"
    member = tarfile.TarInfo(name)
    member.type = kind
    member.size = len(payload) if kind == tarfile.REGTYPE else 0
    member.linkname = "/outside"
    with tarfile.open(archive_path, "w") as archive:
        archive.addfile(member, io.BytesIO(payload) if member.isfile() else None)
    return {name: {"file_sha256": hashlib.sha256(payload).hexdigest(), "bytes": len(payload)}}


@pytest.mark.parametrize(
    "name",
    [
        "/snapshots/demo.parquet",
        "snapshots/../../escape.parquet",
        "exports/key.json",
        "demo.parquet",
    ],
)
def test_archive_rejects_path_traversal_and_unapproved_contents(tmp_path, name):
    archive_path = tmp_path / "unsafe.tar"
    expected = make_archive(archive_path, name)
    with pytest.raises(RuntimeError, match="Unsafe snapshot archive member"):
        support.validate_tar(archive_path, expected)


@pytest.mark.parametrize("kind", [tarfile.SYMTYPE, tarfile.LNKTYPE, tarfile.DIRTYPE])
def test_archive_rejects_links_and_special_members(tmp_path, kind):
    archive_path = tmp_path / "unsafe.tar"
    expected = make_archive(archive_path, kind=kind)
    with pytest.raises(RuntimeError, match="Unsafe snapshot archive member"):
        support.validate_tar(archive_path, expected)


def fixture_backup(tmp_path):
    run_dir = tmp_path / "demo-backup"
    run_dir.mkdir(mode=0o700)
    snapshots = make_archive(run_dir / "snapshots.tar")
    (run_dir / "database.dump").write_bytes(b"test-only-placeholder-for-boundary-checks")
    support.private_write(run_dir / "source-evidence.json", {"data_mode": "DEMO_FIXTURE"})
    for name in support.ARCHIVE_FILES:
        os.chmod(run_dir / name, 0o600)
    manifest = {
        "data_mode": "DEMO_FIXTURE",
        "snapshots": snapshots,
        "files": {
            name: {
                "sha256": support.file_hash(run_dir / name),
                "bytes": (run_dir / name).stat().st_size,
            }
            for name in support.ARCHIVE_FILES
        },
    }
    support.private_write(run_dir / "manifest.json", manifest)
    (run_dir / "manifest.sha256").write_text(support.file_hash(run_dir / "manifest.json"))
    os.chmod(run_dir / "manifest.sha256", 0o600)
    return run_dir


def test_valid_manifest_verifies_and_private_evidence_is_exclusive(tmp_path):
    run_dir = fixture_backup(tmp_path)
    assert support.verify_manifest(run_dir)["data_mode"] == "DEMO_FIXTURE"
    with pytest.raises(FileExistsError):
        support.private_write(run_dir / "source-evidence.json", {})


@pytest.mark.parametrize(
    "name", ["database.dump", "snapshots.tar", "source-evidence.json", "manifest.json"]
)
def test_modified_archive_or_manifest_fails_before_restore(tmp_path, name):
    run_dir = fixture_backup(tmp_path)
    with (run_dir / name).open("ab") as stream:
        stream.write(b"tampered")
    with pytest.raises(RuntimeError, match="[Hh]ash|checksum"):
        support.verify_manifest(run_dir)


def test_manifest_rejects_broad_permissions_and_symlink(tmp_path):
    run_dir = fixture_backup(tmp_path)
    os.chmod(run_dir, 0o755)  # noqa: S103 - exercise rejection of broad backup permissions.
    with pytest.raises(RuntimeError, match="0700"):
        support.verify_manifest(run_dir)
    os.chmod(run_dir, 0o700)
    dump_path = run_dir / "database.dump"
    renamed = run_dir / "old.dump"
    dump_path.rename(renamed)
    dump_path.symlink_to(renamed)
    with pytest.raises(RuntimeError, match="regular backup file"):
        support.verify_manifest(run_dir)


def audit_row(previous_hash, trace_id="test-trace"):
    payload = {
        "event_type": "TEST",
        "object_type": "Demo",
        "object_id": None,
        "actor": "test",
        "trace_id": trace_id,
        "details": {"fixture": True},
        "previous_hash": previous_hash,
    }
    digest = hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True).encode()
    ).hexdigest()
    return {**payload, "record_hash": digest}


def test_audit_verifies_payload_and_chain_without_using_timestamps():
    first = audit_row(None)
    second = audit_row(first["record_hash"])
    assert support.audit_chain([second, first])["structure"] == "PASS"
    second["details"] = {"fixture": False}
    with pytest.raises(RuntimeError, match="payload hash"):
        support.audit_chain([first, second])


def test_audit_rejects_orphan_and_fork():
    root = audit_row(None)
    orphan = audit_row("missing-predecessor")
    with pytest.raises(RuntimeError, match="Broken audit chain"):
        support.audit_chain([root, orphan])
    child_one = audit_row(root["record_hash"], "one")
    child_two = audit_row(root["record_hash"], "two")
    with pytest.raises(RuntimeError, match="fork"):
        support.audit_chain([root, child_one, child_two])


def test_source_restart_attempts_every_stopped_service_even_after_a_failure(monkeypatch):
    spec = importlib.util.spec_from_file_location(
        "recovery_drill", SCRIPT_DIR / "recovery-drill.py"
    )
    assert spec is not None and spec.loader is not None
    drill = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(drill)
    attempted = []

    class DockerBoundary:
        def run(self, args, **_kwargs):
            attempted.append(args)
            if args[-1] == "original-web-id":
                raise RuntimeError("simulated service start failure")

    monkeypatch.setattr(drill, "wait_running", lambda *_args, **_kwargs: None)
    sources = {name: {"Id": f"original-{name}-id"} for name in ("web", "worker", "beat")}
    with pytest.raises(RuntimeError, match="Source service recovery failed"):
        drill.resume_source_services(DockerBoundary(), sources, ["beat", "worker", "web"])
    assert attempted == [
        ["start", "original-web-id"],
        ["start", "original-worker-id"],
        ["start", "original-beat-id"],
    ]


def test_manifest_nonfixture_rejected_even_when_control_checksum_is_updated(tmp_path):
    run_dir = fixture_backup(tmp_path)
    manifest_path = run_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["data_mode"] = "LIVE_LICENSED"
    manifest_path.write_text(json.dumps(manifest))
    (run_dir / "manifest.sha256").write_text(support.file_hash(manifest_path))
    with pytest.raises(RuntimeError, match="Nonfixture archive"):
        support.verify_manifest(run_dir)
