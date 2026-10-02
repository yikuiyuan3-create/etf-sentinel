"""Read-only diagnosis must not claim startup or package/API acceptance."""

import contextlib
import importlib.util
import io
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

SCRIPT = Path(__file__).resolve().parents[1] / "scripts/doctor.py"
spec = importlib.util.spec_from_file_location("plugin_doctor", SCRIPT)
doctor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(doctor)


class DoctorTests(unittest.TestCase):
    def test_missing_or_broken_client_is_safe_at_process_start(self):
        for source in (None, "def invalid syntax"):
            with self.subTest(source=source), tempfile.TemporaryDirectory() as temporary:
                scripts = Path(temporary) / "scripts"
                scripts.mkdir()
                script = scripts / "doctor.py"
                script.write_bytes(SCRIPT.read_bytes())
                if source is not None:
                    (scripts / "sentinel.py").write_text(source)
                process = subprocess.run(  # noqa: S603 - copied source in a fresh test directory.
                    [sys.executable, str(script)], capture_output=True, text=True, check=False
                )
                self.assertEqual(process.returncode, 2)
                self.assertEqual(json.loads(process.stdout)["error_code"], "CLIENT_UNAVAILABLE")
                self.assertEqual(process.stderr, "")
                self.assertNotIn(temporary, process.stdout)

    def test_healthy_service_does_not_require_docker(self):
        with patch.object(doctor.client, "run", return_value={"status": "OK"}) as read:
            with patch.object(doctor.shutil, "which", return_value=None):
                result = doctor.diagnose("http://127.0.0.1:18081")
        self.assertEqual(result["status"], "OK")
        read.assert_called_once_with("status", base_url="http://127.0.0.1:18081")
        for check in (
            "docker_daemon_checked",
            "package_hashes_checked",
            "signal_api_compatibility_checked",
        ):
            self.assertFalse(result["checks"][check])
        self.assertFalse(result["checks"]["docker_cli_available"])

    def test_missing_package_is_blocked_without_leaking_path(self):
        with tempfile.TemporaryDirectory() as temporary:
            with patch.object(doctor.client, "run", return_value={"status": "OK"}):
                result = doctor.diagnose(root=Path(temporary))
            self.assertNotIn(temporary, json.dumps(result))
        self.assertEqual(result["status"], "BLOCKED")
        self.assertIsNone(result["plugin_version"])

    def test_untrusted_metadata_cannot_enter_output(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / ".codex-plugin").mkdir()
            (root / ".codex-plugin/plugin.json").write_text(
                json.dumps({"name": "etf-sentinel", "version": "PRIVATE_INPUT"})
            )
            with patch.object(doctor.client, "run", return_value={"status": "OK"}):
                result = doctor.diagnose(root=root)
        self.assertEqual(result["status"], "BLOCKED")
        self.assertNotIn("PRIVATE_INPUT", json.dumps(result))

    def test_service_block_is_preserved(self):
        service = {"status": "BLOCKED", "error_code": "DATA_HEALTH_BLOCKED"}
        with patch.object(doctor.client, "run", return_value=service):
            result = doctor.diagnose()
        self.assertEqual(result["status"], "BLOCKED")
        self.assertEqual(result["service"], service)

    def test_invalid_url_is_safe_and_does_not_connect(self):
        result = doctor.diagnose("https://private.invalid")
        self.assertEqual(result["service"]["error_code"], "INVALID_BASE_URL")
        self.assertNotIn("private.invalid", json.dumps(result))

    def test_invalid_argument_returns_structured_error(self):
        stream = io.StringIO()
        with contextlib.redirect_stdout(stream):
            code = doctor.main(["--secret-input"])
        self.assertEqual(code, 2)
        self.assertEqual(json.loads(stream.getvalue())["error_code"], "INVALID_ARGUMENT")
        self.assertNotIn("secret-input", stream.getvalue())


if __name__ == "__main__":
    unittest.main()
