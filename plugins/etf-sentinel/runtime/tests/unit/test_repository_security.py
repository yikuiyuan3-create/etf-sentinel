from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

HIGH_CONFIDENCE_SECRET_PATTERNS = {
    "OpenAI-style secret": re.compile(r"\bsk-[A-Za-z0-9_-]{20,}\b"),
    "AWS access key": re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    "private key material": re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
}


def test_repository_contains_no_high_confidence_real_secret_pattern() -> None:
    git = shutil.which("git")
    assert git is not None
    result = subprocess.run(  # noqa: S603 - resolved local git with fixed arguments
        [git, "ls-files", "--cached", "--others", "--exclude-standard"],
        capture_output=True,
        text=True,
        check=True,
        timeout=10,
    )
    findings: list[str] = []
    for relative in result.stdout.splitlines():
        path = Path(relative)
        if not path.is_file() or path.suffix.lower() in {
            ".png",
            ".jpg",
            ".jpeg",
            ".gif",
            ".parquet",
            ".sqlite",
            ".db",
        }:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        for label, pattern in HIGH_CONFIDENCE_SECRET_PATTERNS.items():
            if pattern.search(text):
                findings.append(f"{path}: {label}")

    assert findings == []


def test_production_source_does_not_import_unapproved_fact_sources() -> None:
    production_text = "\n".join(
        path.read_text(encoding="utf-8") for path in Path("src").rglob("*.py")
    ).lower()

    assert "import yfinance" not in production_text
    assert "import akshare" not in production_text
    assert "query1.finance.yahoo.com" not in production_text
