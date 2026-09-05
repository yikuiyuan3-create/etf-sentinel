#!/usr/bin/env python3
"""Read-only, loopback-only ETF Sentinel projection. Python standard library only.

This client never starts services, gathers market data, changes server state, or
returns private portfolio/audit/news payloads. Version 0.1 permits DEMO_FIXTURE
research only; a live backend is deliberately blocked, not silently relabelled.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
from datetime import UTC, datetime, timedelta
from http.client import HTTPException
from urllib.error import HTTPError, URLError
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener
from uuid import UUID

DEFAULT_BASE_URL = "http://127.0.0.1:18080"
MAX_BODY_BYTES = 2 * 1024 * 1024
DEFAULT_TIMEOUT = 5.0
DISCLAIMER = (
    "本系统仅供本公司授权人员开展内部研究和风险监测。内容由统计模型和人工智能生成，"
    "可能错误、遗漏或滞后，不构成证券投资建议、收益承诺或交易指令；历史或回测表现不代表未来。"
    "任何投资决定须由授权人员结合独立资料、风险承受能力和持牌机构意见审慎作出，"
    "交易仅可在合法持牌券商端人工确认。"
)
MODES = {"DEMO_FIXTURE", "LIVE_LICENSED", "DELAYED", "HISTORICAL"}
STATES = {
    "NO_ACTION",
    "WATCH",
    "ENTRY_CANDIDATE",
    "HOLD",
    "REDUCE_CANDIDATE",
    "EXIT_CANDIDATE",
    "BLOCKED_BY_RISK",
    "DATA_STALE",
}
MODEL_STATUSES = {
    "CHAMPION",
    "CHALLENGER",
    "EXPERIMENTAL",
    "REJECTED",
    "ROLLED_BACK",
    "ARTIFACT_INTEGRITY_BLOCKED_HISTORICAL_AUDIT_ONLY",
    "LICENSE_BLOCKED_HISTORICAL_AUDIT_ONLY",
}
RIGHTS = ("display", "algorithm", "derivative", "cache", "training", "redistribution")
TOKEN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\Z")
CODE = re.compile(r"[A-Z][A-Z0-9_]{0,127}\Z")


class SafeError(Exception):
    """Only constant error codes and an enumerated mode cross the output boundary."""

    def __init__(self, code, mode="UNKNOWN"):
        self.code = code
        self.mode = mode if mode in MODES else "UNKNOWN"
        super().__init__(code)


def _schema(condition):
    if not condition:
        raise SafeError("INVALID_RESPONSE_SCHEMA")


def _fields(value, keys):
    _schema(isinstance(value, dict) and all(key in value for key in keys))


def _number(value, low=0, high=1):
    _schema(type(value) in (float, int) and math.isfinite(value) and low <= value <= high)
    return value


def _integer(value, low, high):
    _schema(type(value) is int and low <= value <= high)
    return value


def _token(value, pattern=TOKEN):
    _schema(isinstance(value, str) and pattern.fullmatch(value) is not None)
    return value


def _uuid(value):
    _schema(isinstance(value, str) and len(value) == 36)
    try:
        _schema(str(UUID(value)) == value.lower())
    except (ValueError, AttributeError):
        raise SafeError("INVALID_RESPONSE_SCHEMA") from None
    return value


def _time(value, nullable=False):
    if value is None and nullable:
        return None
    _schema(isinstance(value, str) and len(value) <= 40)
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        _schema(result.tzinfo is not None and result.utcoffset() is not None)
        return result.astimezone(UTC)
    except ValueError:
        raise SafeError("INVALID_RESPONSE_SCHEMA") from None


def _demo(value):
    _schema(isinstance(value, str) and value in MODES)
    if value != "DEMO_FIXTURE":
        raise SafeError("NON_DEMO_MODE_BLOCKED", value)


def _base_url(value):
    # No DNS resolution for localhost, userinfo, URL escapes, IPv6, path or query.
    match = re.fullmatch(r"http://(?:127\.0\.0\.1|localhost):([1-9][0-9]{0,4})", value)
    if not match or not 1 <= int(match[1]) <= 65535:
        raise SafeError("INVALID_BASE_URL")
    return f"http://127.0.0.1:{int(match[1])}"


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        fp.close()
        raise SafeError("REDIRECT_BLOCKED")


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _invalid_constant(value):
    raise ValueError("non-finite JSON number")


class SentinelClient:
    def __init__(self, base_url=DEFAULT_BASE_URL, timeout=DEFAULT_TIMEOUT):
        self.base_url = _base_url(base_url)
        if type(timeout) not in (int, float) or not math.isfinite(timeout) or not 0 < timeout <= 10:
            raise SafeError("INVALID_ARGUMENT")
        self.timeout = timeout
        # Explicitly ignore HTTP_PROXY, ALL_PROXY and system proxy settings.
        self.opener = build_opener(ProxyHandler({}), _NoRedirect())
        self.mode = "UNKNOWN"

    def get(self, path):
        _schema(
            path
            in {
                "/health/ready",
                "/api/v1/monitoring",
                "/api/v1/signals",
                "/api/v1/providers",
                "/api/v1/models",
            }
        )
        request = Request(  # noqa: S310 - strict loopback HTTP base and fixed path allowlist above.
            self.base_url + path,
            method="GET",
            headers={
                "Accept": "application/json",
                "Accept-Encoding": "identity",
                "Cache-Control": "no-cache, no-store",
                "User-Agent": "ETF-Sentinel-Readonly-Plugin/0.1",
            },
        )
        try:
            with self.opener.open(request, timeout=self.timeout) as response:
                if response.status != 200:
                    raise SafeError("HTTP_ERROR")
                _schema(response.headers.get_content_type() == "application/json")
                _schema(response.headers.get("Content-Encoding", "identity").lower() == "identity")
                length = response.headers.get("Content-Length")
                if length is not None:
                    _schema(length.isascii() and length.isdigit())
                    if int(length) > MAX_BODY_BYTES:
                        raise SafeError("RESPONSE_TOO_LARGE")
                body = response.read(MAX_BODY_BYTES + 1)
                if len(body) > MAX_BODY_BYTES:
                    raise SafeError("RESPONSE_TOO_LARGE")
        except HTTPError as exc:
            code = "REDIRECT_BLOCKED" if 300 <= exc.code < 400 else "HTTP_ERROR"
            exc.close()
            raise SafeError(code) from None
        except TimeoutError:
            raise SafeError("TIMEOUT") from None
        except URLError as exc:
            code = "TIMEOUT" if isinstance(exc.reason, TimeoutError) else "NETWORK_ERROR"
            raise SafeError(code) from None
        except (OSError, ValueError, HTTPException):
            raise SafeError("NETWORK_ERROR") from None
        try:
            return json.loads(
                body.decode("utf-8"),
                object_pairs_hook=_unique_object,
                parse_constant=_invalid_constant,
            )
        except (ValueError, UnicodeError, RecursionError):
            raise SafeError("INVALID_JSON") from None

    def envelope(self, path):
        value = self.get(path)
        _fields(value, ("data_mode", "disclaimer", "data"))
        _demo(value["data_mode"])
        _schema(value["disclaimer"] == DISCLAIMER)
        return value["data"]

    def health(self):
        value = self.get("/health/ready")
        _fields(value, ("status", "trading_mode", "data_mode", "database"))
        _demo(value["data_mode"])
        self.mode = value["data_mode"]
        if value["trading_mode"] != "paper":
            raise SafeError("UNSAFE_TRADING_MODE")
        _schema(value["status"] in {"ok", "degraded_fail_closed"} and value["database"] == "ok")
        return {key: value[key] for key in ("status", "trading_mode", "data_mode", "database")}

    def monitoring(self):
        value = self.envelope("/api/v1/monitoring")
        keys = (
            "data_mode",
            "interval_hours",
            "checked_at",
            "next_check_at",
            "last_scheduled_at",
            "schedule_status",
            "market_data_as_of",
            "market_data_age_hours",
            "live_data_status",
            "analysis_status",
            "blocked_reason",
            "counts",
        )
        _fields(value, keys)
        _demo(value["data_mode"])
        _integer(value["interval_hours"], 1, 2)
        checked, following, last, cutoff = (
            _time(value[key], key in {"last_scheduled_at", "market_data_as_of"})
            for key in ("checked_at", "next_check_at", "last_scheduled_at", "market_data_as_of")
        )
        _schema(value["schedule_status"] in {"CURRENT", "NOT_RUN", "OVERDUE", "FAILED"})
        _schema(value["analysis_status"] in {"DEMO_ANALYSIS", "BLOCKED"})
        _schema(value["live_data_status"] == "COMPLIANCE_BLOCKED")
        if value["blocked_reason"] is not None:
            _token(value["blocked_reason"], CODE)
        age = value["market_data_age_hours"]
        if age is not None:
            _number(age, 0, 1e8)
        _fields(value["counts"], ("signals", "blocked", "watch", "candidates"))
        counts = {
            key: _integer(value["counts"][key], 0, 1000000)
            for key in ("signals", "blocked", "watch", "candidates")
        }
        _schema(sum(counts[key] for key in ("blocked", "watch", "candidates")) <= counts["signals"])
        now = datetime.now(UTC)
        if not -60 <= (now - checked).total_seconds() <= 300:
            raise SafeError("STALE_MONITORING_RESPONSE")
        _schema(checked < following <= checked + timedelta(hours=2, minutes=1))
        _schema(cutoff is None or cutoff <= checked)
        _schema(last is None or last <= checked + timedelta(seconds=60))
        healthy = (
            value["schedule_status"] == "CURRENT"
            and value["analysis_status"] == "DEMO_ANALYSIS"
            and value["blocked_reason"] is None
            and last is not None
            and cutoff is not None
            and age is not None
            and now - last <= timedelta(hours=value["interval_hours"], minutes=5)
        )
        result = {key: value[key] for key in keys}
        # Fixed fixture timestamps remain explicit; they are not marketed as fresh market data.
        result["counts"] = counts if healthy else None
        return result, healthy

    def signals(self, limit):
        rows = self.envelope("/api/v1/signals")
        _schema(isinstance(rows, list) and len(rows) <= 200)
        result = []
        for value in rows:
            keys = (
                "id",
                "instrument_id",
                "data_snapshot_id",
                "model_version_id",
                "horizon_days",
                "state",
                "probability",
                "confidence",
                "market_score",
                "macro_score",
                "news_score",
                "liquidity_score",
                "composite_score",
                "data_as_of",
                "available_at",
                "data_mode",
                "latency_status",
                "risk_rules_hit",
                "code_version",
                "is_current",
            )
            _fields(value, keys)
            _demo(value["data_mode"])
            _schema(value["state"] in STATES and type(value["is_current"]) is bool)
            _schema(value["latency_status"] in {"ON_TIME", "STALE"})
            if (
                value["latency_status"] != "ON_TIME"
                or value["state"] == "DATA_STALE"
                or value["is_current"] is not True
            ):
                raise SafeError("DATA_HEALTH_BLOCKED")
            for key in ("id", "instrument_id", "data_snapshot_id", "model_version_id"):
                _uuid(value[key])
            _integer(value["horizon_days"], 1, 252)
            for key in ("probability", "confidence", "liquidity_score"):
                _number(value[key])
            for key in ("market_score", "macro_score", "news_score", "composite_score"):
                _number(value[key], -1, 1)
            cutoff, available = _time(value["data_as_of"]), _time(value["available_at"])
            _schema(cutoff <= available <= datetime.now(UTC))
            _token(value["code_version"])
            rules = value["risk_rules_hit"]
            _schema(isinstance(rules, list) and len(rules) <= 100)
            # Risk suffixes can contain portfolio identifiers or exposures; never export them.
            safe_rules = []
            for rule in rules:
                _schema(isinstance(rule, str) and len(rule) <= 256)
                safe_rules.append(_token(rule.split(":", 1)[0], CODE))
            if safe_rules and "CANDIDATE" in value["state"]:
                raise SafeError("DATA_HEALTH_BLOCKED")
            row = {key: value[key] for key in keys}
            row["risk_rules_hit"] = sorted(set(safe_rules))
            result.append(row)
        # Validate the full bounded response before returning even a single candidate.
        return result[:limit]

    def providers(self):
        rows = self.envelope("/api/v1/providers")
        _schema(isinstance(rows, list) and len(rows) <= 100)
        result = []
        for value in rows:
            _fields(value, ("provider_code", "review_status", "expires_at", "rights"))
            _token(value["provider_code"])
            _schema(value["review_status"] in {"APPROVED", "PENDING", "BLOCKED", "EXPIRED"})
            _time(value["expires_at"], nullable=True)
            _fields(value["rights"], RIGHTS)
            _schema(all(type(value["rights"][key]) is bool for key in RIGHTS))
            result.append(
                {
                    "provider_code": value["provider_code"],
                    "review_status": value["review_status"],
                    "expires_at": value["expires_at"],
                    "rights": {key: value["rights"][key] for key in RIGHTS},
                }
            )
        return result

    def models(self):
        rows = self.envelope("/api/v1/models")
        _schema(isinstance(rows, list) and len(rows) <= 100)
        result = []
        for value in rows:
            _fields(
                value,
                ("id", "model_name", "version", "status", "display_blocked_reason", "metrics"),
            )
            _uuid(value["id"])
            _token(value["model_name"])
            _token(value["version"])
            _schema(value["status"] in MODEL_STATUSES and isinstance(value["metrics"], dict))
            reason = value["display_blocked_reason"]
            if reason is not None:
                _token(reason, CODE)
            calibration = {}
            if reason is None and "BLOCKED" not in value["status"]:
                for key in ("brier_score", "baseline_brier_score", "expected_calibration_error"):
                    if key in value["metrics"]:
                        calibration[key] = _number(value["metrics"][key])
                for key, high in (("horizon_days", 252), ("samples", 100000000)):
                    if key in value["metrics"]:
                        calibration[key] = _integer(value["metrics"][key], 1, high)
            result.append(
                {
                    "id": value["id"],
                    "model_name": value["model_name"],
                    "version": value["version"],
                    "status": value["status"],
                    "display_blocked_reason": reason,
                    "calibration": calibration,
                }
            )
        return result


def _output(command, *, status="OK", mode="UNKNOWN", data=None, error_code=None):
    result = {
        "command": command if command in {"status", "signals", "providers", "models"} else None,
        "status": status,
        "data_mode": mode,
        "disclaimer": DISCLAIMER,
        "data": data,
    }
    if error_code:
        result["error_code"] = error_code
    return result


def run(command, *, base_url=DEFAULT_BASE_URL, limit=10, timeout=DEFAULT_TIMEOUT):
    client = None
    try:
        if (
            command not in {"status", "signals", "providers", "models"}
            or type(limit) is not int
            or not 1 <= limit <= 20
        ):
            raise SafeError("INVALID_ARGUMENT")
        client = SentinelClient(base_url, timeout)
        health = client.health()
        if command == "providers":
            return _output(command, mode=client.mode, data=client.providers())
        monitoring, healthy = client.monitoring()
        healthy = healthy and health["status"] == "ok"
        if command == "status":
            if not healthy:
                monitoring["counts"] = None
            return _output(
                command,
                status="OK" if healthy else "BLOCKED",
                mode=client.mode,
                data={"health": health, "monitoring": monitoring},
                error_code=None if healthy else "DATA_HEALTH_BLOCKED",
            )
        if not healthy:
            raise SafeError("DATA_HEALTH_BLOCKED")
        data = client.signals(limit) if command == "signals" else client.models()
        return _output(command, mode=client.mode, data=data)
    except SafeError as exc:
        mode = exc.mode if exc.mode != "UNKNOWN" else client.mode if client else "UNKNOWN"
        return _output(command, status="BLOCKED", mode=mode, error_code=exc.code)
    except (TypeError, ValueError, KeyError, OverflowError, RecursionError):
        return _output(
            command,
            status="BLOCKED",
            mode=client.mode if client else "UNKNOWN",
            error_code="INVALID_RESPONSE_SCHEMA",
        )


class _Parser(argparse.ArgumentParser):
    def error(self, message):
        raise SafeError("INVALID_ARGUMENT")


def main(argv=None):
    parser = _Parser(
        description="ETF Sentinel 只读插件：仅 localhost DEMO_FIXTURE 内部研究；不采集、不交易。"
    )
    parser.add_argument(
        "--base-url",
        default=DEFAULT_BASE_URL,
        help="仅允许 http://127.0.0.1:端口 或 http://localhost:端口",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ("status", "signals", "providers", "models"):
        subparser = subparsers.add_parser(command)
        if command == "signals":
            subparser.add_argument("--limit", type=int, default=10, help="仅 1–20 条，默认 10")
    try:
        args = parser.parse_args(argv)
        result = run(args.command, base_url=args.base_url, limit=getattr(args, "limit", 10))
    except SafeError as exc:
        result = _output(None, status="BLOCKED", error_code=exc.code)
    print(json.dumps(result, ensure_ascii=False, allow_nan=False, separators=(",", ":")))
    return 0 if result["status"] == "OK" else 2


if __name__ == "__main__":
    sys.exit(main())
