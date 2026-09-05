#!/usr/bin/env python3
"""Verify the clean release inventory. Hashes detect drift, not author authenticity."""

from __future__ import annotations

import hashlib
import json
import re
import sys
from pathlib import Path

MANIFEST = "PACKAGE_MANIFEST.json"
IGNORED_DIRS = {".git", "__pycache__", ".pytest_cache", ".ruff_cache"}
BLOCKED_PARTS = {".env", ".venv", "var", "work", "backups", ".uv-cache", ".uv-python"}
BLOCKED_EXTENSIONS = {".db", ".sqlite", ".sqlite3", ".parquet", ".dump", ".log", ".pem", ".key"}
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
            if set(relative.parts) & BLOCKED_PARTS or item.suffix.lower() in BLOCKED_EXTENSIONS:
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
    manifest_path = root / MANIFEST
    if manifest_path.is_symlink() or manifest_path.stat().st_size > 1024 * 1024:
        raise PackageError("INVALID_MANIFEST")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(manifest, dict) or manifest.get("schema_version") != 1:
        raise PackageError("INVALID_MANIFEST")
    actual = inventory(root)
    if manifest.get("files") != actual or not actual:
        raise PackageError("PACKAGE_CONTENT_MISMATCH")
    return {"status": "VERIFIED", "files": len(actual), "version": manifest.get("version")}


def main() -> int:
    try:
        print(json.dumps(verify(Path(__file__).resolve().parents[1]), ensure_ascii=False))
        return 0
    except (OSError, ValueError, TypeError):
        print(json.dumps({"status": "BLOCKED", "reason": "PACKAGE_VERIFICATION_FAILED"}))
        return 2


if __name__ == "__main__":
    sys.exit(main())
