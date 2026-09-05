"""Stdlib-only launcher tests. No Docker daemon is started or modified."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import subprocess
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from pathlib import Path
from unittest.mock import patch

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "demo.py"
SPEC = importlib.util.spec_from_file_location("etf_plugin_demo", SCRIPT)
assert SPEC and SPEC.loader
demo = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(demo)
REPOSITORY = SCRIPT.parents[3]
COMPOSE = """name: etf-sentinel
services:
  web:
    ports:
      - "127.0.0.1:${APP_PORT:-8000}:8000"
"""


class DemoLauncherTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix=".plugin-demo-test-", dir=REPOSITORY)
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.runtime = self.root / "runtime"
        self.runtime.mkdir()
        for relative in demo.REQUIRED_FILES:
            target = self.runtime / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(
                COMPOSE if relative == "docker-compose.yml" else "fixture\n", encoding="utf-8"
            )
        migration = self.runtime / "alembic/versions/0001_initial.py"
        migration.parent.mkdir()
        migration.write_text("# migration fixture\n", encoding="utf-8")
        self.workspace = self.root / "independent-demo"
        runtime_patch = patch.object(demo, "RUNTIME_ROOT", self.runtime)
        runtime_patch.start()
        self.addCleanup(runtime_patch.stop)
        environment_patch = patch.dict(
            os.environ, {"PATH": "/usr/bin:/bin", "HOME": str(Path.home())}, clear=True
        )
        environment_patch.start()
        self.addCleanup(environment_patch.stop)

    def invoke(self, *arguments: str) -> tuple[int, dict]:
        stdout, stderr = StringIO(), StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            exit_code = demo.main(list(arguments))
        self.assertFalse(stdout.getvalue() and stderr.getvalue())
        return exit_code, json.loads(stdout.getvalue() or stderr.getvalue())

    def prepare(self) -> dict:
        status, result = self.invoke("prepare", "--workspace", str(self.workspace))
        self.assertEqual(status, 0, result)
        return result

    def assert_error(self, expected: str, *arguments: str) -> dict:
        status, result = self.invoke(*arguments)
        self.assertEqual(status, 2)
        self.assertEqual(result["status"], "ERROR")
        self.assertEqual(result["code"], expected)
        return result

    def start_error(self, expected: str, *extra: str) -> dict:
        return self.assert_error(expected, "start", "--workspace", str(self.workspace), *extra)

    def config(self, port: int = 18081) -> dict:
        project = (
            "etf-sentinel-plugin-" + hashlib.sha256(str(self.workspace).encode()).hexdigest()[:10]
        )
        settings = {
            "TRADING_MODE": "paper",
            "MARKET_DATA_PROVIDER": "demo_fixture",
            "APP_ENV": "demo",
        }
        services = {
            name: {"environment": settings.copy()}
            for name in (
                "postgres",
                "redis",
                "migrate",
                "seed",
                "web",
                "worker",
                "beat",
            )
        }
        services["web"]["ports"] = [
            {"host_ip": "127.0.0.1", "published": str(port), "target": 8000, "protocol": "tcp"}
        ]
        return {
            "name": project,
            "services": services,
            "volumes": {
                name: {"name": f"{project}_{name}"}
                for name in ("postgres_data", "redis_data", "snapshot_data")
            },
            "networks": {"default": {"name": f"{project}_default"}},
        }

    def context_result(
        self, endpoint: str = "unix:///var/run/docker.sock"
    ) -> subprocess.CompletedProcess:
        content = [{"Name": "local-fixture", "Endpoints": {"docker": {"Host": endpoint}}}]
        return subprocess.CompletedProcess([], 0, json.dumps(content), "")

    def test_prepare_copies_bytes_and_writes_bound_manifest(self) -> None:
        result = self.prepare()
        self.assertEqual(result["status"], "PREPARED")
        self.assertFalse(result["started"])
        self.assertEqual(result["data_mode"], "DEMO_FIXTURE")
        marker = json.loads((self.workspace / demo.MARKER).read_text())
        self.assertEqual(marker["workspace"], str(self.workspace))
        self.assertEqual(marker["trading_mode"], "paper")
        self.assertEqual((self.workspace / "docker-compose.yml").read_bytes(), COMPOSE.encode())
        self.assertEqual(
            marker["files"]["docker-compose.yml"], hashlib.sha256(COMPOSE.encode()).hexdigest()
        )

    def test_prepare_never_overwrites_existing_directory(self) -> None:
        self.workspace.mkdir()
        original = self.workspace / "user-file"
        original.write_text("retain me")
        self.assert_error("WORKSPACE_EXISTS", "prepare", "--workspace", str(self.workspace))
        self.assertEqual(original.read_text(), "retain me")
        self.assertEqual(list(self.workspace.iterdir()), [original])

    def test_prepare_rejects_existing_file(self) -> None:
        self.workspace.write_text("retain me")
        self.assert_error("WORKSPACE_EXISTS", "prepare", "--workspace", str(self.workspace))
        self.assertEqual(self.workspace.read_text(), "retain me")

    def test_prepare_requires_existing_parent(self) -> None:
        self.assert_error(
            "PARENT_MISSING", "prepare", "--workspace", str(self.root / "missing/child")
        )
        self.assertFalse((self.root / "missing").exists())

    def test_rejects_relative_root_home_and_plugin_cache(self) -> None:
        cases = (
            ("relative", "INVALID_WORKSPACE"),
            ("/", "UNSAFE_WORKSPACE"),
            (str(Path.home()), "UNSAFE_WORKSPACE"),
            (str(demo.PLUGIN_ROOT), "PLUGIN_CACHE_REFUSED"),
            (str(demo.PLUGIN_ROOT / "private-copy"), "PLUGIN_CACHE_REFUSED"),
            (str(self.root) + "/../escape", "INVALID_WORKSPACE"),
        )
        for target, error in cases:
            with self.subTest(target=target):
                self.assert_error(error, "prepare", "--workspace", target)

    def test_rejects_symlink_workspace_and_parent(self) -> None:
        linked_parent = self.root / "linked-parent"
        linked_parent.symlink_to(self.runtime, target_is_directory=True)
        for target in (linked_parent, linked_parent / "new-child"):
            with self.subTest(target=target):
                self.assert_error("SYMLINK_REFUSED", "prepare", "--workspace", str(target))

    def test_runtime_symlink_file_not_followed(self) -> None:
        outside = self.root / "outside-private"
        outside.write_text("not for export")
        (self.runtime / "link").symlink_to(outside)
        self.assert_error("SYMLINK_REFUSED", "prepare", "--workspace", str(self.workspace))
        self.assertFalse(self.workspace.exists())

    def test_runtime_symlink_directory_not_followed(self) -> None:
        (self.runtime / "linked-dir").symlink_to(self.root, target_is_directory=True)
        self.assert_error("SYMLINK_REFUSED", "prepare", "--workspace", str(self.workspace))
        self.assertFalse(self.workspace.exists())

    def test_missing_runtime_refused(self) -> None:
        with patch.object(demo, "RUNTIME_ROOT", self.root / "absent"):
            self.assert_error("RUNTIME_MISSING", "prepare", "--workspace", str(self.workspace))

    def test_incomplete_runtime_refused(self) -> None:
        (self.runtime / "uv.lock").unlink()
        self.assert_error("RUNTIME_INCOMPLETE", "prepare", "--workspace", str(self.workspace))
        self.assertFalse(self.workspace.exists())

    def test_runtime_missing_migrations_refused(self) -> None:
        (self.runtime / "alembic/versions/0001_initial.py").unlink()
        self.assert_error("RUNTIME_INCOMPLETE", "prepare", "--workspace", str(self.workspace))

    def test_runtime_nonempty_dotenv_refused_without_output(self) -> None:
        (self.runtime / ".env").write_text("TOKEN=sentinel-secret-fixture")
        result = self.assert_error("DOTENV_REFUSED", "prepare", "--workspace", str(self.workspace))
        self.assertNotIn("sentinel-secret-fixture", json.dumps(result))
        self.assertFalse(self.workspace.exists())

    def test_default_start_is_plan_and_does_not_invoke_docker(self) -> None:
        self.prepare()
        with patch.object(demo.subprocess, "run") as process:
            status, result = self.invoke("start", "--workspace", str(self.workspace))
        process.assert_not_called()
        self.assertEqual(status, 0)
        self.assertEqual(result["status"], "PLAN")
        self.assertFalse(result["started"])
        self.assertEqual(result["url"], "http://127.0.0.1:18081/")
        self.assertFalse(result["initial_monitor_enqueued"])
        self.assertEqual(result["interval_hours"], 1)
        self.assertTrue(result["project_name"].startswith("etf-sentinel-plugin-"))
        self.assertNotEqual(result["project_name"], "etf-sentinel")

    def test_custom_interval_and_port_plan(self) -> None:
        self.prepare()
        status, result = self.invoke(
            "start", "--workspace", str(self.workspace), "--port", "18999", "--interval-hours", "2"
        )
        self.assertEqual(status, 0)
        self.assertEqual(result["url"], "http://127.0.0.1:18999/")
        self.assertEqual(result["interval_hours"], 2)

    def test_invalid_intervals_ports_and_unknown_flags_fail_without_injection(self) -> None:
        self.prepare()
        for extra in (
            ("--interval-hours", "3"),
            ("--port", "18081;touch secret"),
            ("--execute", "--volume", "/:/data"),
            ("--exec",),
        ):
            with self.subTest(extra=extra):
                result = self.start_error("INVALID_ARGUMENTS", *extra)
                self.assertNotIn("secret", json.dumps(result))
        for port in ("-1", "0", "80", "65536"):
            with self.subTest(port=port):
                self.start_error("INVALID_PORT", "--port", port)

    def test_no_start_without_marker_or_when_workspace_moved(self) -> None:
        self.workspace.mkdir()
        self.start_error("NOT_PREPARED")
        self.workspace.rmdir()
        self.prepare()
        destination = self.root / "moved-copy"
        self.workspace.rename(destination)
        self.assert_error("NOT_PREPARED", "start", "--workspace", str(destination))

    def test_start_rejects_nonempty_dotenv_even_demo_content(self) -> None:
        self.prepare()
        (self.workspace / ".env").write_text("TRADING_MODE=paper\n")
        self.start_error("DOTENV_REFUSED")

    def test_start_allows_empty_dotenv(self) -> None:
        self.prepare()
        (self.workspace / ".env").touch()
        status, result = self.invoke("start", "--workspace", str(self.workspace))
        self.assertEqual(status, 0, result)

    def test_start_rejects_dotenv_symlink(self) -> None:
        self.prepare()
        (self.workspace / ".env").symlink_to(self.root / "missing-secret")
        self.start_error("SYMLINK_REFUSED")

    def test_start_rejects_marker_symlink(self) -> None:
        self.prepare()
        marker = self.workspace / demo.MARKER
        original = self.root / "original-marker"
        marker.rename(original)
        marker.symlink_to(original)
        self.start_error("SYMLINK_REFUSED")

    def test_start_rejects_file_modification(self) -> None:
        self.prepare()
        (self.workspace / "Dockerfile").write_text("RUN unwanted-command\n")
        self.start_error("RUNTIME_MODIFIED")

    def test_start_rejects_unexpected_override_file(self) -> None:
        self.prepare()
        (self.workspace / "compose.override.yml").write_text("services: {}")
        self.start_error("RUNTIME_MODIFIED")

    def test_start_rejects_live_mode_case_insensitively(self) -> None:
        self.prepare()
        for values in (
            {"TRADING_MODE": "live"},
            {"trading_mode": " LIVE "},
            {"TRADING_MODE": "live", "trading_mode": "paper"},
        ):
            with self.subTest(values=values), patch.dict(os.environ, values):
                self.start_error("LIVE_MODE_REFUSED")

    def test_start_rejects_real_provider(self) -> None:
        self.prepare()
        with patch.dict(os.environ, {"MARKET_DATA_PROVIDER": "twelve_data"}):
            self.start_error("LIVE_PROVIDER_REFUSED")

    def test_start_rejects_production_environment(self) -> None:
        self.prepare()
        with patch.dict(os.environ, {"APP_ENV": "production"}):
            self.start_error("PRODUCTION_ENV_REFUSED")

    def test_every_dangerous_flag_rejected(self) -> None:
        self.prepare()
        for flag in demo.DISABLED_FLAGS:
            with self.subTest(flag=flag), patch.dict(os.environ, {flag: "true"}):
                self.start_error("UNSAFE_FLAG")

    def test_rejects_remote_docker_host(self) -> None:
        self.prepare()
        with patch.dict(os.environ, {"DOCKER_HOST": "tcp://remote.example:2376"}):
            self.start_error("REMOTE_DOCKER_REFUSED")

    def test_execute_uses_fixed_argv_private_project_and_filtered_environment(self) -> None:
        self.workspace = self.root / "中文 path;echo untouched"
        self.prepare()
        secret = "never-printed-test-value"  # noqa: S105 - deliberately non-secret test marker
        dangerous_environment = {
            key: secret
            for key in (
                "TOKEN",
                "POSTGRES_PASSWORD",
                "DATABASE_URL",
                "TWELVE_DATA_API_KEY",
                "COMPOSE_FILE",
                "PYTHONPATH",
                "HTTP_PROXY",
            )
        }
        successful = [
            self.context_result(),
            subprocess.CompletedProcess([], 0, json.dumps(self.config()), ""),
            subprocess.CompletedProcess([], 0, "", ""),
            subprocess.CompletedProcess([], 0, "fixture-task-id", ""),
        ]
        with (
            patch.dict(os.environ, dangerous_environment),
            patch.object(demo.subprocess, "run", side_effect=successful) as process,
        ):
            status, result = self.invoke("start", "--workspace", str(self.workspace), "--execute")
        self.assertEqual(status, 0, result)
        self.assertEqual(result["status"], "STARTED")
        self.assertTrue(result["started"])
        self.assertTrue(result["initial_monitor_enqueued"])
        self.assertEqual(result["initial_monitor_completion"], "NOT_CHECKED")
        self.assertEqual(process.call_count, 4)
        self.assertEqual(process.call_args_list[0].args[0], ["docker", "context", "inspect"])
        call = process.call_args_list[2]
        argv = call.args[0]
        self.assertEqual(argv[-3:], ["up", "--build", "--detach"])
        self.assertEqual(argv.count(str(self.workspace)), 1)
        self.assertEqual(argv[argv.index("--file") + 1], str(self.workspace / "docker-compose.yml"))
        self.assertEqual(call.kwargs["cwd"], self.workspace)
        self.assertNotIn("shell", call.kwargs)
        self.assertEqual(call.kwargs["timeout"], 1200)
        environment = call.kwargs["env"]
        for key in dangerous_environment:
            self.assertNotIn(key, environment)
        self.assertEqual(environment["TRADING_MODE"], "paper")
        self.assertEqual(environment["MARKET_DATA_PROVIDER"], "demo_fixture")
        self.assertEqual(environment["APP_PORT"], "18081")
        self.assertEqual(environment["MONITORING_INTERVAL_HOURS"], "1")
        self.assertTrue(all(environment[key] == "false" for key in demo.DISABLED_FLAGS))
        self.assertNotIn(secret, json.dumps(result))
        dispatch = process.call_args_list[3]
        self.assertEqual(dispatch.args[0][:-8], argv[:-3])
        self.assertEqual(
            dispatch.args[0][-8:],
            [
                "exec",
                "-T",
                "worker",
                "celery",
                "-A",
                "etf_sentinel.tasks:celery_app",
                "call",
                "etf_sentinel.hourly_monitor",
            ],
        )
        self.assertEqual(dispatch.kwargs["timeout"], 30)
        self.assertEqual(dispatch.kwargs["env"], environment)
        self.assertNotIn("fixture-task-id", json.dumps(result))

    def test_failed_up_never_enqueues_initial_analysis(self) -> None:
        self.prepare()
        outcomes = [
            self.context_result(),
            subprocess.CompletedProcess([], 0, json.dumps(self.config()), ""),
            subprocess.CompletedProcess([], 1, "", "failure-fixture"),
        ]
        with patch.object(demo.subprocess, "run", side_effect=outcomes) as process:
            self.start_error("DOCKER_FAILED", "--execute")
        self.assertEqual(process.call_count, 3)
        self.assertEqual(process.call_args.args[0][-3:], ["up", "--build", "--detach"])

    def test_initial_dispatch_failure_reports_started_resources_without_cleanup(self) -> None:
        self.prepare()
        for failure in (
            subprocess.CompletedProcess([], 1, "private-fixture", "private-fixture"),
            subprocess.TimeoutExpired("private-fixture", 30),
        ):
            outcomes = [
                self.context_result(),
                subprocess.CompletedProcess([], 0, json.dumps(self.config()), ""),
                subprocess.CompletedProcess([], 0, "", ""),
                failure,
            ]
            with (
                self.subTest(failure=failure),
                patch.object(
                    demo.subprocess,
                    "run",
                    side_effect=outcomes,
                ) as process,
            ):
                result = self.start_error("INITIAL_MONITOR_DISPATCH_FAILED", "--execute")
                self.assertEqual(process.call_count, 4)
                self.assertIn("服务可能已经启动", result["message"])
                self.assertIn("不会自动停止或清理", result["message"])
                self.assertNotIn("private-fixture", json.dumps(result))

    def test_execute_failure_suppresses_subprocess_output(self) -> None:
        self.prepare()
        failure = subprocess.CompletedProcess([], 1, "token=secret-value", "password=secret-value")
        with patch.object(demo.subprocess, "run", return_value=failure) as process:
            result = self.start_error("DOCKER_FAILED", "--execute")
        self.assertEqual(process.call_count, 1)
        self.assertNotIn("secret-value", json.dumps(result))

    def test_missing_docker_and_timeout_report_safe_errors(self) -> None:
        self.prepare()
        for error, code in (
            (FileNotFoundError("secret-path"), "DOCKER_NOT_FOUND"),
            (subprocess.TimeoutExpired("secret-argv", 1), "DOCKER_TIMEOUT"),
        ):
            with self.subTest(code=code), patch.object(demo.subprocess, "run", side_effect=error):
                result = self.start_error(code, "--execute")
                self.assertNotIn("secret-", json.dumps(result))

    def test_invalid_resolved_config_never_launches(self) -> None:
        self.prepare()
        cases = []
        exposed = self.config()
        exposed["services"]["web"]["ports"][0]["host_ip"] = "0.0.0.0"  # noqa: S104 - rejection test
        cases.append(exposed)
        reused = self.config()
        reused["volumes"]["postgres_data"]["name"] = "etf-sentinel_postgres_data"
        cases.append(reused)
        bind = self.config()
        bind["services"]["web"]["volumes"] = [{"type": "bind", "source": "/", "target": "/host"}]
        cases.append(bind)
        live = self.config()
        live["services"]["web"]["environment"]["TRADING_MODE"] = "live"
        cases.append(live)
        for config in cases:
            with (
                self.subTest(config=config),
                patch.object(
                    demo.subprocess,
                    "run",
                    side_effect=[
                        self.context_result(),
                        subprocess.CompletedProcess([], 0, json.dumps(config), ""),
                    ],
                ) as process,
            ):
                self.start_error("UNSAFE_COMPOSE", "--execute")
                self.assertEqual(process.call_count, 2)

    def test_invalid_compose_json_never_launches(self) -> None:
        self.prepare()
        with patch.object(
            demo.subprocess,
            "run",
            side_effect=[self.context_result(), subprocess.CompletedProcess([], 0, "not json", "")],
        ) as process:
            self.start_error("INVALID_COMPOSE_CONFIG", "--execute")
        self.assertEqual(process.call_count, 2)

    def test_execute_rejects_remote_default_context_before_compose(self) -> None:
        self.prepare()
        for endpoint in (
            "ssh://server.example",
            "tcp://server.example:2376",
            "npipe:////remote-host/pipe/docker_engine",
        ):
            with (
                self.subTest(endpoint=endpoint),
                patch.object(
                    demo.subprocess, "run", return_value=self.context_result(endpoint)
                ) as process,
            ):
                self.start_error("REMOTE_DOCKER_REFUSED", "--execute")
                self.assertEqual(process.call_count, 1)

    def test_explicit_remote_context_overrides_local_docker_host_and_is_refused(self) -> None:
        self.prepare()
        with (
            patch.dict(
                os.environ,
                {"DOCKER_CONTEXT": "remote-fixture", "DOCKER_HOST": "unix:///var/run/docker.sock"},
            ),
            patch.object(
                demo.subprocess, "run", return_value=self.context_result("ssh://server.example")
            ) as process,
        ):
            self.start_error("REMOTE_DOCKER_REFUSED", "--execute")
        self.assertEqual(process.call_count, 1)

    def test_invalid_context_schema_and_json_never_reach_compose(self) -> None:
        self.prepare()
        for content in ("not json", "[]", "{}", '[{"Endpoints": {}}]'):
            with (
                self.subTest(content=content),
                patch.object(
                    demo.subprocess,
                    "run",
                    return_value=subprocess.CompletedProcess([], 0, content, ""),
                ) as process,
            ):
                self.start_error("INVALID_DOCKER_CONTEXT", "--execute")
                self.assertEqual(process.call_count, 1)

    def test_compose_nonlocal_binding_refused_before_copy(self) -> None:
        (self.runtime / "docker-compose.yml").write_text(COMPOSE.replace("127.0.0.1:", "0.0.0.0:"))
        self.assert_error("UNSAFE_COMPOSE", "prepare", "--workspace", str(self.workspace))
        self.assertFalse(self.workspace.exists())

    def test_stop_reset_delete_commands_are_not_supported(self) -> None:
        for action in ("stop", "reset", "delete"):
            with self.subTest(action=action):
                self.assert_error("INVALID_ARGUMENTS", action, "--workspace", str(self.workspace))


if __name__ == "__main__":
    unittest.main()
