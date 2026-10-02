#!/usr/bin/env python3
"""Verify the clean release inventory. Hashes detect drift, not author authenticity."""

from __future__ import annotations

import ast
import hashlib
import json
import re
import sys
import tomllib
from pathlib import Path

MANIFEST = "PACKAGE_MANIFEST.json"
IGNORED_DIRS = {".git", "__pycache__", ".pytest_cache", ".ruff_cache", ".mypy_cache"}
BLOCKED_PARTS = {".env", ".venv", "var", "work", "backups", ".uv-cache", ".uv-python"}
BLOCKED_EXTENSIONS = {
    ".db",
    ".sqlite",
    ".sqlite3",
    ".parquet",
    ".dump",
    ".log",
    ".pem",
    ".key",
    ".p12",
    ".pfx",
    ".pyc",
    ".zip",
    ".tar",
    ".gz",
}
PLUGIN = "plugins/etf-sentinel"
NAME = "etf-sentinel"
SECRET_PATTERNS = (
    re.compile(r"\bsk-[A-Za-z0-9_-]{20,}\b"),
    re.compile(r"\b(?:gh[pousr]_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{30,})\b"),
    re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    re.compile(r"/(?:Users|Volumes)/[^\s\"']+"),
    re.compile(r"\b[A-Za-z0-9._%+-]+@(?:gmail|outlook|qq)\.com\b", re.I),
    re.compile(r"RFQ-\d{6,}", re.I),
)


class PackageError(ValueError):
    pass


def _unique_object(pairs: list[tuple[str, object]]) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise PackageError("DUPLICATE_JSON_KEY")
        result[key] = value
    return result


def read_json(path: Path) -> dict:
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 1024 * 1024:
        raise PackageError("INVALID_METADATA_FILE")
    value = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=_unique_object)
    if not isinstance(value, dict):
        raise PackageError("INVALID_METADATA_FILE")
    return value


def metadata(root: Path) -> dict:
    """Require the package, runtime, registry and safe defaults to describe one release."""
    plugin = read_json(root / PLUGIN / ".codex-plugin/plugin.json")
    version = plugin.get("version")
    if (
        plugin.get("name") != NAME
        or not isinstance(version, str)
        or not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?", version)
    ):
        raise PackageError("INVALID_PLUGIN_IDENTITY")
    project = tomllib.loads((root / PLUGIN / "runtime/pyproject.toml").read_text("utf-8"))
    if (
        project.get("project", {}).get("name") != NAME
        or project["project"].get("version") != version
    ):
        raise PackageError("RUNTIME_VERSION_MISMATCH")
    initializer = ast.parse(
        (root / PLUGIN / "runtime/src/etf_sentinel/__init__.py").read_text("utf-8")
    )
    declared_versions = [
        node.value.value
        for node in initializer.body
        if isinstance(node, ast.Assign)
        and isinstance(node.value, ast.Constant)
        and any(
            isinstance(target, ast.Name) and target.id == "__version__" for target in node.targets
        )
    ]
    if declared_versions != [version]:
        raise PackageError("RUNTIME_DECLARED_VERSION_MISMATCH")
    lock = tomllib.loads((root / PLUGIN / "runtime/uv.lock").read_text("utf-8"))
    packages = [item for item in lock.get("package", []) if item.get("name") == NAME]
    if len(packages) != 1 or packages[0].get("version") != version:
        raise PackageError("LOCK_VERSION_MISMATCH")
    marketplace = read_json(root / ".agents/plugins/marketplace.json")
    entries = marketplace.get("plugins")
    if not isinstance(entries, list) or len(entries) != 1 or not isinstance(entries[0], dict):
        raise PackageError("INVALID_MARKETPLACE")
    if entries[0].get("name") != NAME or entries[0].get("source") != {
        "source": "local",
        "path": "./" + PLUGIN,
    }:
        raise PackageError("MARKETPLACE_IDENTITY_MISMATCH")
    settings = {}
    for line in (root / PLUGIN / "runtime/.env.example").read_text("utf-8").splitlines():
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            if key in settings:
                raise PackageError("DUPLICATE_DEMO_SETTING")
            settings[key] = value
    expected = {
        "TRADING_MODE": "paper",
        "MARKET_DATA_PROVIDER": "demo_fixture",
        "ENABLE_LIVE_TRADING": "false",
        "ENABLE_ORDER_ENTRY": "false",
        "ENABLE_PUBLIC_SERVICE": "false",
    }
    if any(settings.get(key) != value for key, value in expected.items()):
        raise PackageError("UNSAFE_DEMO_DEFAULTS")
    return {
        "schema_version": 1,
        "name": NAME,
        "version": version,
        "data_mode": "DEMO_FIXTURE",
        "trading_mode": "paper",
    }


def inventory(root: Path) -> dict[str, str]:
    import os

    result = {}
    if root.is_symlink() or not root.is_dir():
        raise PackageError("INVALID_PACKAGE_ROOT")
    for current, directories, filenames in os.walk(root, followlinks=False):
        for name in (*directories, *filenames):
            item = Path(current) / name
            if item.is_symlink():
                raise PackageError("SYMLINK_REFUSED")
        directories[:] = [name for name in directories if name not in IGNORED_DIRS]
        for name in filenames:
            item = Path(current) / name
            relative = item.relative_to(root)
            if relative.as_posix() == MANIFEST:
                continue
            if (
                set(relative.parts) & BLOCKED_PARTS
                or item.suffix.lower() in BLOCKED_EXTENSIONS
                or (name.startswith(".env.") and name != ".env.example")
                or name == ".DS_Store"
            ):
                raise PackageError("PRIVATE_RUNTIME_FILE_REFUSED")
            if not item.is_file() or item.stat().st_size > 4 * 1024 * 1024:
                raise PackageError("FILE_TYPE_OR_SIZE_REFUSED")
            content = item.read_bytes()
            try:
                decoded = content.decode("utf-8")
            except UnicodeError:
                raise PackageError("NON_TEXT_FILE_REFUSED") from None
            if any(pattern.search(decoded) for pattern in SECRET_PATTERNS):
                raise PackageError("SENSITIVE_CONTENT_REFUSED")
            result[relative.as_posix()] = hashlib.sha256(content).hexdigest()
    return dict(sorted(result.items()))


def verify(root: Path) -> dict:
    manifest = read_json(root / MANIFEST)
    actual = inventory(root)
    expected = metadata(root)
    if type(manifest.get("schema_version")) is not int or any(
        manifest.get(key) != value for key, value in expected.items()
    ):
        raise PackageError("MANIFEST_METADATA_MISMATCH")
    if set(manifest) != set(expected) | {"files"}:
        raise PackageError("INVALID_MANIFEST_FIELDS")
    if manifest.get("files") != actual or not actual:
        raise PackageError("PACKAGE_CONTENT_MISMATCH")
    return {"status": "VERIFIED", "files": len(actual), "version": manifest.get("version")}


def main() -> int:
    try:
        print(json.dumps(verify(Path(__file__).resolve().parents[1]), ensure_ascii=False))
        return 0
    except (OSError, ValueError, TypeError, KeyError, SyntaxError):
        print(json.dumps({"status": "BLOCKED", "reason": "PACKAGE_VERIFICATION_FAILED"}))
        return 2


if __name__ == "__main__":
    sys.exit(main())
