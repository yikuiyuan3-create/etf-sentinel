#!/usr/bin/env python3
"""Read-only local diagnostics; never starts Docker or changes configuration."""

from __future__ import annotations

import argparse
import importlib.util
import json
import re
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BASE_URL = "http://127.0.0.1:18080"


def _load_client():
    if sys.version_info < (3, 11):  # noqa: UP036 - safe diagnosis on older client Python.
        return None
    try:
        spec = importlib.util.spec_from_file_location(
            "sentinel_doctor_client", ROOT / "scripts/sentinel.py"
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    except (OSError, ImportError, ValueError, AttributeError, SyntaxError):
        return None


client = _load_client()


def diagnose(base_url=DEFAULT_BASE_URL, *, root=ROOT):
    """Only enumerated checks and the safe status projection cross the boundary."""
    if client is None:
        return {
            "command": "doctor",
            "status": "BLOCKED",
            "error_code": "CLIENT_UNAVAILABLE"
            if sys.version_info >= (3, 11)
            else "PYTHON_UNSUPPORTED",
            "suggestions_zh": ["确认 Python 3.11+，重新下载完整发布包并核对文件清单。"],
        }
    version = None
    package_ok = False
    try:
        metadata = json.loads((root / ".codex-plugin/plugin.json").read_text(encoding="utf-8"))
        candidate = metadata.get("version")
        package_ok = metadata.get("name") == "etf-sentinel" and isinstance(candidate, str)
        package_ok = package_ok and re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", candidate) is not None
        if package_ok:
            version = candidate
    except (OSError, ValueError, AttributeError):
        pass
    required = ("pyproject.toml", "uv.lock", "docker-compose.yml", "src/etf_sentinel/main.py")
    runtime_present = all((root / "runtime" / name).is_file() for name in required)
    python_ok = sys.version_info >= (3, 11)
    docker_available = shutil.which("docker") is not None
    service = client.run("status", base_url=base_url)
    suggestions = []
    if not package_ok or not runtime_present:
        suggestions.append("下载完整发布包，并在发布包根目录运行 tools/verify_package.py。")
    if not python_ok:
        suggestions.append("插件客户端需要 Python 3.11 或更新版本。")
    if not docker_available:
        suggestions.append(
            "未找到 Docker 命令；已有服务仍可读取，新建独立 Demo 需要 Docker Compose。"
        )
    if service["status"] != "OK":
        suggestions.append("核对本机端口及服务状态；数据门禁恢复前停止解释候选。")
    checks = {
        "python_supported": python_ok,
        "plugin_metadata_valid": bool(package_ok),
        "runtime_files_present": runtime_present,
        "docker_cli_available": docker_available,
        "docker_daemon_checked": False,
        "package_hashes_checked": False,
        "signal_api_compatibility_checked": False,
    }
    return {
        "command": "doctor",
        "status": "OK"
        if python_ok and package_ok and runtime_present and service["status"] == "OK"
        else "BLOCKED",
        "plugin_version": version,
        "checks": checks,
        "service": service,
        "suggestions_zh": suggestions,
        "disclaimer": client.DISCLAIMER,
    }


class _InvalidArgument(ValueError):
    pass


class _Parser(argparse.ArgumentParser):
    def error(self, message):
        raise _InvalidArgument()


def main(argv=None):
    parser = _Parser(description="ETF Sentinel 只读诊断：不启动服务、不修改配置。")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    try:
        args = parser.parse_args(argv)
        result = diagnose(args.base_url)
    except _InvalidArgument:
        result = {"command": "doctor", "status": "BLOCKED", "error_code": "INVALID_ARGUMENT"}
    print(json.dumps(result, ensure_ascii=False, allow_nan=False))
    return 0 if result["status"] == "OK" else 2


if __name__ == "__main__":
    sys.exit(main())
