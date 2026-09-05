"""Dependency-free contract and loopback HTTP tests for the read-only plugin."""

from __future__ import annotations

import contextlib
import copy
import importlib.util
import json
import os
import subprocess
import sys
import threading
import time
import unittest
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "sentinel.py"
spec = importlib.util.spec_from_file_location("sentinel_client", SCRIPT)
client = importlib.util.module_from_spec(spec)
spec.loader.exec_module(client)


def envelope(data, mode="DEMO_FIXTURE"):
    return {"data_mode": mode, "disclaimer": client.DISCLAIMER, "data": data}


def fixtures():
    now = datetime.now(UTC)
    timestamp = now.isoformat()
    cutoff = (now - timedelta(days=500)).isoformat()
    monitoring = {
        "data_mode": "DEMO_FIXTURE",
        "interval_hours": 1,
        "checked_at": timestamp,
        "next_check_at": (now + timedelta(hours=1)).isoformat(),
        "last_scheduled_at": timestamp,
        "schedule_status": "CURRENT",
        "market_data_as_of": cutoff,
        "market_data_age_hours": 12000.0,
        "live_data_status": "COMPLIANCE_BLOCKED",
        "analysis_status": "DEMO_ANALYSIS",
        "blocked_reason": None,
        "counts": {"signals": 1, "blocked": 0, "watch": 0, "candidates": 1},
        "findings": ["PRIVATE_INTERNAL_TEXT"],
    }
    signal = {
        "id": "00000000-0000-4000-8000-000000000001",
        "instrument_id": "00000000-0000-4000-8000-000000000002",
        "data_snapshot_id": "00000000-0000-4000-8000-000000000003",
        "model_version_id": "00000000-0000-4000-8000-000000000004",
        "horizon_days": 20,
        "state": "ENTRY_CANDIDATE",
        "probability": 0.51,
        "confidence": 0.42,
        "market_score": 0.65,
        "macro_score": 0.43,
        "news_score": 0.25,
        "liquidity_score": 0.8,
        "composite_score": 0.55,
        "data_as_of": cutoff,
        "available_at": cutoff,
        "data_mode": "DEMO_FIXTURE",
        "latency_status": "ON_TIME",
        "risk_rules_hit": [],
        "code_version": "tree-12345678",
        "is_current": True,
        "feature_values": {"portfolio": "PRIVATE_INTERNAL_TEXT"},
        "supporting_evidence": [{"secret": "PRIVATE_INTERNAL_TEXT"}],
        "source_links": ["https://private.invalid/PRIVATE_INTERNAL_TEXT"],
    }
    return {
        "/health/ready": {
            "status": "ok",
            "trading_mode": "paper",
            "database": "ok",
            "data_mode": "DEMO_FIXTURE",
        },
        "/api/v1/monitoring": envelope(monitoring),
        "/api/v1/signals": envelope([signal]),
        "/api/v1/providers": envelope(
            [
                {
                    "provider_code": "demo_fixture",
                    "review_status": "APPROVED",
                    "expires_at": None,
                    "purpose": "PRIVATE_INTERNAL_TEXT",
                    "rights": {
                        name: name != "redistribution"
                        for name in (
                            "display",
                            "algorithm",
                            "derivative",
                            "cache",
                            "training",
                            "redistribution",
                        )
                    },
                }
            ]
        ),
        "/api/v1/models": envelope(
            [
                {
                    "id": "00000000-0000-4000-8000-000000000005",
                    "model_name": "LogisticBaselineV1",
                    "version": "1.0.0-h20-12345678",
                    "status": "REJECTED",
                    "display_blocked_reason": None,
                    "approved_by": "PRIVATE_INTERNAL_TEXT",
                    "metrics": {
                        "brier_score": 0.3,
                        "baseline_brier_score": 0.2,
                        "expected_calibration_error": 0.1,
                        "horizon_days": 20,
                        "samples": 500,
                        "config": {"credential": "PRIVATE_INTERNAL_TEXT"},
                    },
                }
            ]
        ),
    }


@contextlib.contextmanager
def local_server(routes):
    calls = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            calls.append((self.command, self.path))
            value = routes[self.path]
            headers = {"Content-Type": "application/json"}
            status = 200
            if isinstance(value, tuple):
                status, headers, value, delay = value
                time.sleep(delay)
            body = value if isinstance(value, bytes) else json.dumps(value).encode()
            try:
                self.send_response(status)
                for key, val in headers.items():
                    self.send_header(key, val)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", calls
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


class SentinelClientTests(unittest.TestCase):
    def assert_blocked(self, result, code=None):
        self.assertEqual(result["status"], "BLOCKED")
        if code is not None:
            self.assertEqual(result["error_code"], code)
        self.assertNotIn("ENTRY_CANDIDATE", json.dumps(result))
        self.assertNotIn("PRIVATE_INTERNAL_TEXT", json.dumps(result))
        self.assertEqual(result["disclaimer"], client.DISCLAIMER)

    def test_status_real_loopback_and_fixed_demo_label(self):
        with local_server(fixtures()) as (base, calls):
            result = client.run("status", base_url=base)
        self.assertEqual(result["status"], "OK")
        self.assertEqual(result["data_mode"], "DEMO_FIXTURE")
        self.assertEqual(result["data"]["monitoring"]["interval_hours"], 1)
        self.assertEqual(calls, [("GET", "/health/ready"), ("GET", "/api/v1/monitoring")])
        self.assertNotIn("PRIVATE_INTERNAL_TEXT", json.dumps(result))

    def test_successful_cli_is_one_json_document(self):
        with local_server(fixtures()) as (base, _):
            result = subprocess.run(  # noqa: S603 - owned script and local test server; no shell.
                [sys.executable, str(SCRIPT), "--base-url", base, "signals"],
                text=True,
                capture_output=True,
                timeout=10,
            )
        self.assertEqual(result.returncode, 0, result.stderr)
        data = json.loads(result.stdout)
        self.assertEqual(data["data"][0]["state"], "ENTRY_CANDIDATE")
        self.assertNotIn("PRIVATE_INTERNAL_TEXT", result.stdout)
        self.assertEqual(result.stderr, "")

    def test_all_external_or_ambiguous_url_variants_are_blocked(self):
        invalid = [
            "https://127.0.0.1:18080",
            "http://example.com:80",
            "http://127.0.0.2:80",
            "http://[::1]:80",
            "http://user:secret@localhost:80",
            "http://localhost:80/",
            "http://localhost:80/path",
            "http://localhost:80?key=secret",
            "http://localhost:80#secret",
            "http://127.1:80",
            "http://localhost",
            "http://localhost:0",
            "http://localhost:65536",
            " http://localhost:80",
            "http://localhost:80\n",
            "http://localhost.:80",
            "http://localhost:080",
        ]
        for url in invalid:
            with self.subTest(url=url):
                self.assert_blocked(client.run("signals", base_url=url), "INVALID_BASE_URL")

    def test_localhost_is_normalized_and_proxy_environment_ignored(self):
        with local_server(fixtures()) as (base, _):
            with patch.dict(
                os.environ,
                {
                    "http_proxy": "http://127.0.0.1:1",
                    "HTTP_PROXY": "http://127.0.0.1:1",
                    "NO_PROXY": "",
                },
            ):
                result = client.run("status", base_url=base.replace("127.0.0.1", "localhost"))
        self.assertEqual(result["status"], "OK")

    def test_redirect_is_not_followed_even_to_loopback(self):
        routes = fixtures()
        routes["/health/ready"] = (
            302,
            {"Location": "/api/v1/signals"},
            b"PRIVATE_INTERNAL_TEXT",
            0,
        )
        with local_server(routes) as (base, calls):
            result = client.run("signals", base_url=base)
        self.assert_blocked(result, "REDIRECT_BLOCKED")
        self.assertEqual(calls, [("GET", "/health/ready")])

    def test_timeout_has_no_fallback_or_sensitive_error_body(self):
        routes = fixtures()
        routes["/health/ready"] = (
            200,
            {"Content-Type": "application/json"},
            b"PRIVATE_INTERNAL_TEXT",
            0.2,
        )
        with local_server(routes) as (base, _):
            result = client.run("signals", base_url=base, timeout=0.03)
        self.assert_blocked(result, "TIMEOUT")

    def test_bad_json_is_not_returned(self):
        routes = fixtures()
        routes["/health/ready"] = b'{"secret":"PRIVATE_INTERNAL_TEXT"'
        with local_server(routes) as (base, _):
            self.assert_blocked(client.run("signals", base_url=base), "INVALID_JSON")

    def test_duplicate_json_key_is_rejected(self):
        routes = fixtures()
        routes["/health/ready"] = b'{"data_mode":"LIVE_LICENSED","data_mode":"DEMO_FIXTURE"}'
        with local_server(routes) as (base, _):
            self.assert_blocked(client.run("status", base_url=base), "INVALID_JSON")

    def test_wrong_content_type_and_http_errors_do_not_escape(self):
        for status, content_type, code in [
            (200, "text/html", "INVALID_RESPONSE_SCHEMA"),
            (429, "application/json", "HTTP_ERROR"),
            (500, "application/json", "HTTP_ERROR"),
        ]:
            routes = fixtures()
            routes["/health/ready"] = (
                status,
                {"Content-Type": content_type},
                b"PRIVATE_INTERNAL_TEXT",
                0,
            )
            with local_server(routes) as (base, _):
                self.assert_blocked(client.run("signals", base_url=base), code)

    def test_response_body_larger_than_two_megabytes_is_blocked(self):
        routes = fixtures()
        routes["/health/ready"] = b" " * (2 * 1024 * 1024 + 1)
        with local_server(routes) as (base, _):
            self.assert_blocked(client.run("signals", base_url=base), "RESPONSE_TOO_LARGE")

    def test_non_demo_health_and_envelopes_never_return_analysis(self):
        for path in ("/health/ready", "/api/v1/monitoring", "/api/v1/signals"):
            routes = fixtures()
            routes[path]["data_mode"] = "LIVE_LICENSED"
            with local_server(routes) as (base, _):
                result = client.run("signals", base_url=base)
            self.assert_blocked(result, "NON_DEMO_MODE_BLOCKED")
            self.assertEqual(result["data_mode"], "LIVE_LICENSED")

    def test_live_trading_is_blocked(self):
        routes = fixtures()
        routes["/health/ready"]["trading_mode"] = "live"
        with local_server(routes) as (base, _):
            self.assert_blocked(client.run("signals", base_url=base), "UNSAFE_TRADING_MODE")

    def test_missing_required_field_fails_closed(self):
        for path, key in [
            ("/health/ready", "database"),
            ("/api/v1/monitoring", "checked_at"),
            ("/api/v1/signals", "probability"),
        ]:
            routes = fixtures()
            item = routes[path] if path == "/health/ready" else routes[path]["data"]
            if isinstance(item, list):
                item = item[0]
            del item[key]
            with local_server(routes) as (base, _):
                self.assert_blocked(client.run("signals", base_url=base), "INVALID_RESPONSE_SCHEMA")

    def test_monitoring_fail_closed_and_never_fetches_candidates(self):
        for field, value in [
            ("schedule_status", "OVERDUE"),
            ("schedule_status", "NOT_RUN"),
            ("schedule_status", "FAILED"),
            ("analysis_status", "BLOCKED"),
        ]:
            routes = fixtures()
            routes["/api/v1/monitoring"]["data"][field] = value
            with local_server(routes) as (base, calls):
                self.assert_blocked(client.run("signals", base_url=base), "DATA_HEALTH_BLOCKED")
            self.assertNotIn(("GET", "/api/v1/signals"), calls)

    def test_status_hides_candidate_counts_when_blocked(self):
        routes = fixtures()
        routes["/api/v1/monitoring"]["data"]["analysis_status"] = "BLOCKED"
        with local_server(routes) as (base, _):
            result = client.run("status", base_url=base)
        self.assert_blocked(result, "DATA_HEALTH_BLOCKED")
        self.assertIsNone(result["data"]["monitoring"]["counts"])

    def test_old_response_and_false_current_scheduler_are_blocked(self):
        for key, hours in [("checked_at", 1), ("last_scheduled_at", 4)]:
            routes = fixtures()
            routes["/api/v1/monitoring"]["data"][key] = (
                datetime.now(UTC) - timedelta(hours=hours)
            ).isoformat()
            with local_server(routes) as (base, _):
                self.assert_blocked(client.run("signals", base_url=base))

    def test_stale_or_non_demo_signal_never_leaks_other_candidates(self):
        for key, value in [
            ("latency_status", "STALE"),
            ("state", "DATA_STALE"),
            ("data_mode", "HISTORICAL"),
            ("is_current", False),
        ]:
            routes = fixtures()
            bad = copy.deepcopy(routes["/api/v1/signals"]["data"][0])
            bad[key] = value
            routes["/api/v1/signals"]["data"].append(bad)
            with local_server(routes) as (base, _):
                self.assert_blocked(client.run("signals", base_url=base, limit=1))

    def test_limit_and_all_returned_rows_validated_before_truncation(self):
        routes = fixtures()
        row = routes["/api/v1/signals"]["data"][0]
        routes["/api/v1/signals"]["data"] = [copy.deepcopy(row) for _ in range(25)]
        with local_server(routes) as (base, _):
            self.assertEqual(len(client.run("signals", base_url=base)["data"]), 10)
            self.assertEqual(len(client.run("signals", base_url=base, limit=20)["data"]), 20)
            self.assert_blocked(client.run("signals", base_url=base, limit=21), "INVALID_ARGUMENT")

    def test_provider_output_drops_purpose_and_uses_boolean_rights(self):
        with local_server(fixtures()) as (base, _):
            result = client.run("providers", base_url=base)
        self.assertEqual(result["data"][0]["review_status"], "APPROVED")
        self.assertNotIn("PRIVATE_INTERNAL_TEXT", json.dumps(result))

    def test_rejected_model_keeps_failure_and_only_calibration_numbers(self):
        with local_server(fixtures()) as (base, _):
            result = client.run("models", base_url=base)
        self.assertEqual(result["data"][0]["status"], "REJECTED")
        self.assertEqual(result["data"][0]["calibration"]["brier_score"], 0.3)
        self.assertNotIn("PRIVATE_INTERNAL_TEXT", json.dumps(result))

    def test_non_finite_or_unexpected_schema_is_rejected(self):
        for value in [float("nan"), float("inf"), "0.5", True, -0.1, 1.1]:
            routes = fixtures()
            routes["/api/v1/signals"]["data"][0]["probability"] = value
            with local_server(routes) as (base, _):
                self.assert_blocked(client.run("signals", base_url=base))

    def test_signed_factor_scores_are_retained_without_turning_into_probabilities(self):
        routes = fixtures()
        row = routes["/api/v1/signals"]["data"][0]
        for key in ("market_score", "macro_score", "news_score", "composite_score"):
            row[key] = -0.25
        with local_server(routes) as (base, _):
            result = client.run("signals", base_url=base)
        self.assertEqual(result["status"], "OK")
        self.assertEqual(result["data"][0]["market_score"], -0.25)
        self.assertEqual(result["data"][0]["probability"], 0.51)

    def test_invalid_signed_factor_and_blocked_candidate_are_rejected(self):
        for key, value in [
            ("market_score", -1.01),
            ("news_score", 1.01),
            ("risk_rules_hit", ["PORTFOLIO_LIMIT:PRIVATE_INTERNAL_TEXT"]),
        ]:
            routes = fixtures()
            routes["/api/v1/signals"]["data"][0][key] = value
            with local_server(routes) as (base, _):
                self.assert_blocked(client.run("signals", base_url=base))

    def test_cli_invalid_argument_is_safe_json_not_input_echo(self):
        result = subprocess.run(  # noqa: S603 - fixed test arguments and owned script; no shell.
            [sys.executable, str(SCRIPT), "PRIVATE_INTERNAL_TEXT"],
            text=True,
            capture_output=True,
            timeout=10,
        )
        self.assertEqual(result.returncode, 2)
        self.assert_blocked(json.loads(result.stdout), "INVALID_ARGUMENT")
        self.assertNotIn("PRIVATE_INTERNAL_TEXT", result.stderr)


if __name__ == "__main__":
    unittest.main()
