"""Back up the active fixed Demo and restore to a new, isolated Docker target.

No arguments: print usage, no Docker calls. --execute explicitly permits a
brief pause of the existing web/worker/beat. Source volumes are never restored
over or removed. All new target resources and backup archives are retained.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import secrets
import shutil
import signal
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import quote, urlparse

SUPPORT_PATH = Path(__file__).with_name("recovery-support.py")
SPEC = importlib.util.spec_from_file_location("recovery_support", SUPPORT_PATH)
assert SPEC is not None and SPEC.loader is not None
support = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(support)


class Docker:
    def __init__(self) -> None:
        self.binary = shutil.which("docker")
        support.require(self.binary is not None, "Docker CLI is unavailable")
        self.stage = "initialization"

    def run(self, args, *, environment=None, output=None, input_stream=None, timeout=600):
        result = subprocess.run(  # noqa: S603 - fixed executable, no shell, no user command.
            [self.binary, *args],
            env={**os.environ, **(environment or {})},
            stdout=output if output else subprocess.PIPE,
            stdin=input_stream,
            stderr=subprocess.PIPE,
            check=False,
            timeout=timeout,
        )
        support.require(result.returncode == 0, f"Docker stage failed: {self.stage}")
        return result.stdout.decode() if result.stdout else ""

    def json(self, args, **kwargs):
        return json.loads(self.run(args, **kwargs))

    def inspect(self, name):
        return self.json(["inspect", name])[0]


def env_dict(container: dict) -> dict[str, str]:
    return dict(item.split("=", 1) for item in container["Config"]["Env"])


def wait_running(docker: Docker, container: str, *, health: bool = True, seconds=120) -> None:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        state = docker.inspect(container)["State"]
        if state["Running"] and (not health or state.get("Health", {}).get("Status") == "healthy"):
            return
        time.sleep(2)
    raise RuntimeError("Container did not reach its expected running/healthy state")


def resume_source_services(docker: Docker, sources: dict, stopped: list[str]) -> None:
    """Try every originally stopped service even when another restart fails."""
    failures = []
    for service in ("web", "worker", "beat"):
        if service in stopped:
            try:
                docker.run(["start", sources[service]["Id"]], timeout=120)
                wait_running(docker, sources[service]["Id"], seconds=180)
            except Exception:
                failures.append(service)
    support.require(not failures, "Source service recovery failed")


def execute(workspace: Path) -> dict:
    support.require((workspace / "docker-compose.yml").is_file(), "Wrong workspace")
    docker = Docker()
    sources = {}
    for service in ("postgres", "redis", "web", "worker", "beat"):
        matches = docker.run(
            [
                "ps",
                "--filter",
                "label=com.docker.compose.project=etf-sentinel",
                "--filter",
                f"label=com.docker.compose.service={service}",
                "--format",
                "{{.ID}}",
            ]
        ).splitlines()
        support.require(len(matches) == 1, "Expected exactly one running source service")
        sources[service] = docker.inspect(matches[0])
        labels = sources[service]["Config"]["Labels"]
        support.require(
            Path(labels.get("com.docker.compose.project.working_dir", "")).resolve()
            == workspace.resolve(),
            "Source service belongs to another workspace",
        )
        wait_running(docker, sources[service]["Id"])
    original_env = env_dict(sources["web"])
    database_uri = urlparse(original_env.get("DATABASE_URL", ""))
    redis_uri = urlparse(original_env.get("REDIS_URL", ""))
    support.require(
        database_uri.scheme == "postgresql+psycopg"
        and database_uri.hostname == "postgres"
        and database_uri.port == 5432
        and database_uri.username == "etf_sentinel"
        and database_uri.path == "/etf_sentinel"
        and redis_uri.scheme == "redis"
        and redis_uri.hostname == "redis"
        and redis_uri.port == 6379
        and redis_uri.path == "/0",
        "Source must use its own fixed Compose PostgreSQL/Redis services",
    )
    for service in ("web", "worker", "beat"):
        support.demo_environment(env_dict(sources[service]))
        support.require(sources[service]["Image"] == sources["web"]["Image"], "App image mismatch")
    ports = sources["web"]["HostConfig"]["PortBindings"]
    support.require(
        ports == {"8000/tcp": [{"HostIp": "127.0.0.1", "HostPort": "18080"}]},
        "Source must be the approved localhost:18080 Demo",
    )
    network_names = set(sources["web"]["NetworkSettings"]["Networks"])
    support.require(len(network_names) == 1, "Unexpected source network topology")
    source_network = next(iter(network_names))
    for service in sources:
        support.require(
            set(sources[service]["NetworkSettings"]["Networks"]) == network_names,
            "Unexpected source network topology",
        )
    snapshot_volumes = set()
    for service, destination, compose_volume in [
        ("postgres", "/var/lib/postgresql/data", "postgres_data"),
        ("web", "/app/var", "snapshot_data"),
        ("worker", "/app/var", "snapshot_data"),
        ("beat", "/app/var", "snapshot_data"),
    ]:
        mounts = [item for item in sources[service]["Mounts"] if item["Destination"] == destination]
        support.require(
            len(mounts) == 1 and mounts[0]["Type"] == "volume", "Unexpected source volume"
        )
        volume = docker.json(["volume", "inspect", mounts[0]["Name"]])[0]
        support.require(
            volume["Labels"].get("com.docker.compose.project") == "etf-sentinel"
            and volume["Labels"].get("com.docker.compose.volume") == compose_volume,
            "Source volume identity does not match this Compose project",
        )
        if compose_volume == "snapshot_data":
            snapshot_volumes.add(mounts[0]["Name"])
    support.require(len(snapshot_volumes) == 1, "App snapshot volumes differ")
    source_pg_env = env_dict(sources["postgres"])
    support.require(
        source_pg_env.get("POSTGRES_DB") == "etf_sentinel"
        and source_pg_env.get("POSTGRES_USER") == "etf_sentinel",
        "Only the fixed Demo database is supported",
    )
    # Copy only configured application settings, not arbitrary image/host secrets.
    setting_keys = {
        key
        for key in original_env
        if key not in {"PATH", "HOME", "HOSTNAME"}
        and (
            key in support.FORBIDDEN_FLAGS
            or key
            in {
                "APP_ENV",
                "APP_HOST",
                "APP_PORT",
                "TRADING_MODE",
                "DATABASE_URL",
                "REDIS_URL",
                "SNAPSHOT_DIR",
                "EXPORT_DIR",
                "MARKET_DATA_PROVIDER",
                "DEMO_EVALUATION_TIME",
                "DATA_STALE_AFTER_MINUTES",
                "GLOBAL_KILL_SWITCH",
                "MONITORING_INTERVAL_HOURS",
                "CONTAINER_LOCALHOST_BOUND",
                "SINGLE_ETF_CAP",
                "ASSET_CLASS_CAP",
                "INDUSTRY_CAP",
                "REGION_CAP",
                "PORTFOLIO_VOLATILITY_TARGET",
                "CASH_FLOOR",
                "MAX_TURNOVER",
                "MAX_SIMULATION_LOSS",
                "MAX_DRAWDOWN",
                "NEWS_RISK_REDUCTION_THRESHOLD",
                "NEWS_FACTOR_WEIGHT_CAP",
                "ALERT_COOLDOWN_MINUTES",
                "ALERT_DAILY_LIMIT",
                "QUIET_HOURS_START",
                "QUIET_HOURS_END",
                "AUTH_ENABLED",
                "LOG_LEVEL",
                "CODE_VERSION",
            }
        )
    }
    source_env = {key: original_env[key] for key in setting_keys}
    for service in ("worker", "beat"):
        environment = env_dict(sources[service])
        support.require(
            all(environment.get(key) == value for key, value in source_env.items()),
            "Source application policies differ",
        )
    app_image = sources["web"]["Image"]
    pg_image = sources["postgres"]["Image"]
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + "-" + secrets.token_hex(4)
    resource_prefix = "etf-sentinel-recovery-" + run_id.lower()
    root_dir = workspace / "var" / "recovery-drills"
    support.require(not root_dir.is_symlink(), "Unsafe recovery root")
    support.require(root_dir.resolve().is_relative_to(workspace.resolve()), "Recovery path escaped")
    root_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(root_dir, 0o700)
    run_dir = root_dir / run_id
    run_dir.mkdir(mode=0o700)
    script_mount = f"type=bind,src={workspace / 'scripts'},dst=/app/recovery-scripts,readonly"
    label = f"org.etf-sentinel.recovery={run_id}"
    helper_names = []
    target_containers = []
    stopped_sources = []
    source_resumed = False
    report = {
        "run_id": run_id,
        "data_mode": "DEMO_FIXTURE",
        "result": "FAIL",
        "source_image": app_image,
        "restore_image": app_image,
        "postgres_image": pg_image,
        "source_port": "127.0.0.1:18080",
        "resources_retained": True,
        "scope": "Fixed Demo exception only; no production data, Redis queue, secrets or exports",
        "rpo_definition": (
            "Zero committed Demo rows lost at the quiesced dump point; not a production RPO SLA"
        ),
    }

    def helper(command, *, target=False, output=None, extra=None, kill=False):
        helper_name = resource_prefix + "-helper-" + secrets.token_hex(3)
        helper_names.append(helper_name)
        environment = dict(target_env if target else source_env)
        if kill:
            environment["GLOBAL_KILL_SWITCH"] = "true"
        args = [
            "run",
            "--rm",
            "--name",
            helper_name,
            "--label",
            label,
            "--network",
            target_network if target else source_network,
            "--mount",
            script_mount,
        ]
        if target:
            args += ["--mount", f"type=volume,src={snapshot_volume},dst=/app/var,volume-nocopy"]
        else:
            args += ["--volumes-from", sources["web"]["Id"] + ":ro"]
        args += extra or []
        for key in sorted(environment):
            args += ["--env", key]
        args += [app_image, "python", "/app/recovery-scripts/recovery-support.py", command]
        result = docker.run(args, environment=environment, output=output)
        return json.loads(result) if not output else None

    def resume_sources():
        nonlocal source_resumed
        resume_source_services(docker, sources, stopped_sources)
        source_resumed = True

    target_network = resource_prefix + "-net"
    database_volume = resource_prefix + "-pg"
    snapshot_volume = resource_prefix + "-snapshots"
    pg_container = resource_prefix + "-postgres"
    web_container = resource_prefix + "-web"
    password = secrets.token_urlsafe(36)
    target_env = {
        **source_env,
        "DATABASE_URL": f"postgresql+psycopg://etf_sentinel:{quote(password)}@{pg_container}:5432/etf_sentinel",
        "REDIS_URL": "redis://127.0.0.1:1/0",
        "APP_HOST": "127.0.0.1",
    }
    try:
        docker.stage = "source preflight"
        helper("evidence")
        helper("idle")
        docker.stage = "quiesce source"
        # Record before stopping so finally can recover even a partial/timeout stop.
        stopped_sources.append("beat")
        docker.run(["stop", "--time", "30", sources["beat"]["Id"]])
        report["idle"] = helper("idle")
        for service in ("worker", "web"):
            stopped_sources.append(service)
            docker.run(["stop", "--time", "60", sources[service]["Id"]])
        backup_start = time.monotonic()
        report["backup_point_utc"] = datetime.now(UTC).isoformat()
        docker.stage = "backup evidence"
        before = helper("evidence")
        support.private_write(run_dir / "source-evidence.json", before)
        docker.stage = "pg_dump"
        with (run_dir / "database.dump").open("xb") as output:
            os.chmod(run_dir / "database.dump", 0o600)
            docker.run(
                [
                    "exec",
                    sources["postgres"]["Id"],
                    "pg_dump",
                    "-U",
                    "etf_sentinel",
                    "-d",
                    "etf_sentinel",
                    "-Fc",
                    "--no-owner",
                    "--no-acl",
                ],
                output=output,
            )
        docker.stage = "snapshot archive"
        with (run_dir / "snapshots.tar").open("xb") as output:
            os.chmod(run_dir / "snapshots.tar", 0o600)
            helper("archive", output=output)
        support.require(before == helper("evidence"), "Source changed during backup")
        manifest = {
            "run_id": run_id,
            "data_mode": "DEMO_FIXTURE",
            "source_image": app_image,
            "migration": before["database"]["migration"],
            "snapshots": before["snapshots"],
            "files": {
                name: {
                    "sha256": support.file_hash(run_dir / name),
                    "bytes": (run_dir / name).stat().st_size,
                }
                for name in sorted(support.ARCHIVE_FILES)
            },
        }
        support.private_write(run_dir / "manifest.json", manifest)
        descriptor = os.open(
            run_dir / "manifest.sha256", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600
        )
        with os.fdopen(descriptor, "w") as output:
            output.write(support.file_hash(run_dir / "manifest.json") + "\n")
        support.verify_manifest(run_dir)
        report["backup_seconds"] = round(time.monotonic() - backup_start, 3)
        report["backup_manifest_sha256"] = support.file_hash(run_dir / "manifest.json")
        docker.stage = "resume source"
        resume_sources()
        restore_start = time.monotonic()
        docker.stage = "isolated resource creation"
        for kind, name in (
            ("network", target_network),
            ("volume", database_volume),
            ("volume", snapshot_volume),
        ):
            existing = docker.run([kind, "ls", "--format", "{{.Name}}"])
            support.require(name not in existing.splitlines(), "Restore target already exists")
        existing_containers = docker.run(["ps", "-a", "--format", "{{.Names}}"])
        support.require(
            pg_container not in existing_containers.splitlines()
            and web_container not in existing_containers.splitlines(),
            "Target container exists",
        )
        docker.run(["network", "create", "--internal", "--label", label, target_network])
        for volume in (database_volume, snapshot_volume):
            docker.run(["volume", "create", "--label", label, volume])
            support.require(
                docker.json(["volume", "inspect", volume])[0]["Labels"].get(
                    "org.etf-sentinel.recovery"
                )
                == run_id,
                "Restore volume ownership mismatch",
            )
        support.require(
            docker.json(["network", "inspect", target_network])[0]["Internal"],
            "Restore network has external routing",
        )
        target_containers.append(pg_container)
        docker.run(
            [
                "run",
                "-d",
                "--name",
                pg_container,
                "--label",
                label,
                "--network",
                target_network,
                "--mount",
                f"type=volume,src={database_volume},dst=/var/lib/postgresql/data",
                "--env",
                "POSTGRES_PASSWORD",
                "--env",
                "POSTGRES_DB=etf_sentinel",
                "--env",
                "POSTGRES_USER=etf_sentinel",
                "--health-cmd",
                "pg_isready -U etf_sentinel -d etf_sentinel",
                "--health-interval",
                "2s",
                "--health-timeout",
                "3s",
                "--health-retries",
                "30",
                pg_image,
            ],
            environment={"POSTGRES_PASSWORD": password},
        )
        wait_running(docker, pg_container)
        support.verify_manifest(run_dir)  # Fail before pg_restore on any artifact tamper.
        docker.stage = "pg_restore"
        with (run_dir / "database.dump").open("rb") as input_stream:
            docker.run(
                [
                    "exec",
                    "-i",
                    pg_container,
                    "pg_restore",
                    "--exit-on-error",
                    "--no-owner",
                    "--no-acl",
                    "-U",
                    "etf_sentinel",
                    "-d",
                    "etf_sentinel",
                ],
                input_stream=input_stream,
            )
        docker.stage = "restore actual snapshots"
        helper(
            "extract",
            target=True,
            extra=["--user", "root", "--mount", f"type=bind,src={run_dir},dst=/backup,readonly"],
        )
        docker.stage = "restored integrity"
        restored = helper("evidence", target=True)
        support.require(before == restored, "Full-table source/restore fingerprints differ")
        support.private_write(run_dir / "restored-evidence.json", restored)
        report["restore_seconds"] = round(time.monotonic() - restore_start, 3)
        report["all_table_fingerprints_equal"] = True
        report["tables_verified"] = len(restored["tables"])
        report["snapshots_verified"] = len(restored["snapshots"])
        report["migration"] = restored["database"]["migration"]
        report["audit_chain"] = restored["audit_chain"]
        docker.stage = "isolated replay"
        replay_result = helper("replay", target=True)
        support.private_write(run_dir / "replay-evidence.json", replay_result)
        report["idempotent_replay"] = replay_result["idempotent_replay"]
        report["replay_business_tables_unchanged"] = replay_result["business_tables_unchanged"]
        docker.stage = "restored web kill switch"
        target_containers.append(web_container)
        web_env = {**target_env, "GLOBAL_KILL_SWITCH": "true"}
        args = [
            "run",
            "-d",
            "--name",
            web_container,
            "--label",
            label,
            "--network",
            target_network,
            "--mount",
            f"type=volume,src={snapshot_volume},dst=/app/var",
            "--mount",
            script_mount,
        ]
        for key in sorted(web_env):
            args += ["--env", key]
        docker.run([*args, app_image, "etf-sentinel", "serve"], environment=web_env)
        wait_running(docker, web_container, seconds=150)
        report["restored_web"] = docker.json(
            ["exec", web_container, "python", "/app/recovery-scripts/recovery-support.py", "http"]
        )
        for container in target_containers:
            details = docker.inspect(container)
            support.require(not details["HostConfig"]["PortBindings"], "Restore exposed host port")
            support.require(
                set(details["NetworkSettings"]["Networks"]) == {target_network},
                "Restore joined a source/external network",
            )
        report["isolated_network_internal"] = True
        report["host_ports_published"] = False
        report["result"] = "PASS"
    except BaseException as error:
        report["failure_stage"] = docker.stage
        report["error_type"] = type(error).__name__
    finally:
        # No rm, volume deletion, down, source restore, or network prune occurs here.
        if not source_resumed:
            try:
                resume_sources()
            except Exception:
                report["result"] = "FAIL"
                report["source_resume_error"] = True
        cleanup_failures = []
        try:
            running = set(docker.run(["ps", "--format", "{{.Names}}"]).splitlines())
        except Exception:
            running = set()
            report["result"] = "FAIL"
            report["target_state_unknown"] = True
        for container in helper_names + target_containers:
            if container in running:
                try:
                    support.require(
                        docker.inspect(container)["Config"]["Labels"].get(
                            "org.etf-sentinel.recovery"
                        )
                        == run_id,
                        "Container name reused by an unrelated owner; refusing to stop",
                    )
                    docker.run(["stop", "--time", "30", container], timeout=90)
                except Exception:
                    cleanup_failures.append(container)
        if cleanup_failures:
            report["result"] = "FAIL"
            report["target_stop_errors"] = cleanup_failures
        report["source_services_restored"] = source_resumed
        report["completed_at"] = datetime.now(UTC).isoformat()
        report["restore_resources"] = {
            "network": target_network,
            "database_volume": database_volume,
            "snapshot_volume": snapshot_volume,
            "containers": target_containers,
        }
        support.private_write(run_dir / "report.json", report)
    print(
        json.dumps(
            {
                "result": report["result"],
                "evidence_directory": str(run_dir),
                "source_services_restored": source_resumed,
            },
            ensure_ascii=False,
        )
    )
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true", help="Pause the fixed local Demo and run")
    parser.add_argument("--verify-manifest", type=Path, help="Offline integrity check; no Docker")
    args = parser.parse_args()
    if args.verify_manifest:
        support.verify_manifest(args.verify_manifest.resolve())
        print(
            json.dumps(
                {"result": "PASS", "check": "archive_integrity", "data_mode": "DEMO_FIXTURE"}
            )
        )
        return
    if not args.execute:
        parser.print_help()
        return
    os.umask(0o077)

    def interrupted(_signum, _frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, interrupted)
    result = execute(Path(__file__).resolve().parents[1])
    raise SystemExit(0 if result["result"] == "PASS" else 1)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(json.dumps({"result": "FAIL", "error_type": type(error).__name__}), file=sys.stderr)
        raise SystemExit(1) from None
