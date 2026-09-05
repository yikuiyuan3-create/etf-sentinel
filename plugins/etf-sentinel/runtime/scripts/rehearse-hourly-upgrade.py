"""Upgrade only an explicitly selected, retained isolated Demo recovery copy.

Uses the copy's existing in-memory container configuration, never prints its DB
password. Original Compose volumes/network are rejected. Backup archives remain
untouched; the restored scratch DB is advanced to the selected new image version.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import shutil
import subprocess
import time
from pathlib import Path

from etf_sentinel.config import Settings


def run(args, *, env=None, timeout=600):
    binary = shutil.which("docker")
    if binary is None:
        raise RuntimeError("Docker is not installed")
    value = subprocess.run(  # noqa: S603 - fixed Docker operations, validated local resources.
        [binary, *args], env=env, check=False, capture_output=True, text=True, timeout=timeout
    )
    if value.returncode:
        raise RuntimeError("Isolated Docker upgrade failed; inspect retained copy, not source")
    return value.stdout


def inspect(name):
    return json.loads(run(["inspect", name]))[0]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backup", type=Path, required=True)
    parser.add_argument("--image", required=True, help="Exact local sha256 image ID")
    args = parser.parse_args()
    workspace = Path.cwd().resolve()
    backup = args.backup.resolve()
    if not backup.is_relative_to(workspace / "var" / "recovery-drills"):
        raise ValueError("Backup must be inside this workspace's recovery-drills")
    spec = importlib.util.spec_from_file_location(
        "upgrade_support", workspace / "scripts" / "recovery-support.py"
    )
    assert spec is not None and spec.loader is not None
    support = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(support)
    support.verify_manifest(backup)
    report = json.loads((backup / "report.json").read_text())
    support.require(report["result"] == "PASS", "Recovery did not pass")
    image = inspect(args.image)
    support.require(
        args.image.startswith("sha256:") and image["Id"] == args.image,
        "Immutable local image ID required",
    )
    resources = report["restore_resources"]
    db_name, web_name = resources["containers"]
    db, web = inspect(db_name), inspect(web_name)
    network = inspect(resources["network"])
    support.require(network["Internal"] is True, "Recovery network is not isolated")
    for container in [db, web]:
        support.require(
            container["Config"]["Labels"].get("org.etf-sentinel.recovery") == report["run_id"],
            "Recovery owner mismatch",
        )
        support.require(not container["State"]["Running"], "Recovery copy must be stopped")
        support.require(not container["HostConfig"].get("PortBindings"), "Recovery has host ports")
        support.require(
            set(container["NetworkSettings"]["Networks"]) == {resources["network"]},
            "Recovery network mismatch",
        )
    support.require(
        resources["snapshot_volume"].startswith("etf-sentinel-recovery-"),
        "Source snapshot volume rejected",
    )
    env_values = dict(item.split("=", 1) for item in web["Config"]["Env"])
    allowed = {key.upper() for key in Settings.model_fields}
    env_values = {key: value for key, value in env_values.items() if key in allowed}
    env_values.update(GLOBAL_KILL_SWITCH="false", MONITORING_INTERVAL_HOURS="1")
    support.demo_environment(env_values)
    support.require(db_name in env_values["DATABASE_URL"], "DB target does not match recovery copy")
    result_path = backup / f"hourly-upgrade-{args.image[7:19]}.json"
    support.require(not result_path.exists(), "Upgrade evidence already exists; do not overwrite")
    helper = "etf-sentinel-hourly-upgrade-" + report["run_id"].lower() + "-" + args.image[7:19]
    try:
        run(["start", db_name])
        for attempt in range(15):
            try:
                run(["exec", db_name, "pg_isready", "-U", "etf_sentinel"], timeout=5)
                break
            except RuntimeError:
                if attempt == 14:
                    raise
                time.sleep(1)
        command = [
            "run",
            "--name",
            helper,
            "--network",
            resources["network"],
            "--label",
            f"org.etf-sentinel.recovery={report['run_id']}",
            "--mount",
            f"type=volume,src={resources['snapshot_volume']},dst=/app/var",
            "--mount",
            f"type=bind,src={workspace / 'scripts'},dst=/app/upgrade-scripts,readonly",
        ]
        for key in sorted(env_values):
            command += ["--env", key]
        output = run(
            [
                *command,
                args.image,
                "python",
                "/app/upgrade-scripts/verify-hourly-compose.py",
                "--upgrade",
            ],
            env={**os.environ, **env_values},
        )
        evidence = json.loads(output)
        support.require(evidence["result"] == "PASS", "Upgrade assertions failed")
        baseline = json.loads((backup / "source-evidence.json").read_text())
        for table in (
            "simulation_decisions",
            "simulation_fills",
            "simulation_ledger",
            "data_snapshots",
            "etf_instruments",
            "provider_registry",
        ):
            support.require(
                baseline["tables"][table] == evidence["after"]["tables"][table],
                "Original backup ledger/source differs after upgrade",
            )
        evidence["original_backup_ledger_unchanged"] = True
        support.private_write(result_path, {"image": args.image, **evidence})
        print(
            json.dumps(
                {
                    "result": "PASS",
                    "evidence": str(result_path),
                    "source_modified": False,
                    "scratch_copy_upgraded": True,
                }
            )
        )
    finally:
        run(["stop", "--time", "30", db_name], timeout=60)


if __name__ == "__main__":
    main()
