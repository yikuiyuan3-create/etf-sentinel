#!/usr/bin/env python3
"""Build a deterministic, reviewed Git inventory; never publish or include runtime data."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path

from verify_package import MANIFEST, PackageError, inventory, metadata, verify

ZIP_TIME = (2020, 1, 1, 0, 0, 0)


def tracked_inventory(root: Path) -> dict[str, int]:
    git = shutil.which("git")
    if git is None:
        raise PackageError("GIT_REQUIRED")
    top = subprocess.run(  # noqa: S603 - resolved Git executable with fixed read-only arguments
        [git, "rev-parse", "--show-toplevel"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
        timeout=15,
    ).stdout.strip()
    if Path(top).resolve() != root.resolve():
        raise PackageError("REPOSITORY_ROOT_REQUIRED")
    output = subprocess.run(  # noqa: S603 - resolved Git executable with fixed read-only arguments
        [git, "ls-files", "--stage", "--full-name", "-z"],
        cwd=root,
        check=True,
        capture_output=True,
        timeout=15,
    ).stdout
    files = {}
    for entry in output.split(b"\0"):
        if not entry:
            continue
        header, filename = entry.split(b"\t", 1)
        mode, _object_id, stage = header.split()
        name = filename.decode("utf-8")
        if mode not in {b"100644", b"100755"} or stage != b"0":
            raise PackageError("GIT_NON_REGULAR_OR_UNMERGED_FILE")
        if name in files or Path(name).is_absolute() or ".." in Path(name).parts:
            raise PackageError("INVALID_GIT_INVENTORY")
        files[name] = 0o755 if mode == b"100755" else 0o644
    if not files or MANIFEST not in files:
        raise PackageError("MANIFEST_MUST_BE_TRACKED")
    actual = inventory(root)
    if set(files) != set(actual) | {MANIFEST}:
        raise PackageError("GIT_AND_DISK_INVENTORY_MISMATCH")
    return dict(sorted(files.items()))


def write_manifest(root: Path) -> dict:
    """An explicit maintainer operation, never implied by building the archive."""
    tracked_inventory(root)
    result = {**metadata(root), "files": inventory(root)}
    destination = root / MANIFEST
    if destination.is_symlink():
        raise PackageError("SYMLINK_REFUSED")
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=root, delete=False) as handle:
        temporary = Path(handle.name)
        try:
            handle.write(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
    try:
        temporary.replace(destination)
    finally:
        temporary.unlink(missing_ok=True)
    return verify(root)


def build(root: Path, output_dir: Path) -> dict:
    if root.is_symlink() or not root.is_dir():
        raise PackageError("INVALID_PACKAGE_ROOT")
    root = root.resolve()
    if output_dir.is_symlink() or output_dir.exists():
        raise PackageError("OUTPUT_ALREADY_EXISTS")
    output_dir = output_dir.resolve()
    if output_dir == root or root in output_dir.parents:
        raise PackageError("OUTPUT_MUST_BE_OUTSIDE_REPOSITORY")
    if not output_dir.parent.is_dir():
        raise PackageError("OUTPUT_PARENT_MUST_EXIST")
    files = tracked_inventory(root)
    result = verify(root)
    archive_name = f"etf-sentinel-v{result['version']}.zip"
    # Buffer only after verification; verify again to detect edits during the snapshot.
    contents = {name: (root / name).read_bytes() for name in files}
    expected = json.loads(contents[MANIFEST])["files"]
    if any(
        hashlib.sha256(contents[name]).hexdigest() != digest for name, digest in expected.items()
    ):
        raise PackageError("SOURCE_CHANGED_DURING_BUILD")
    if (
        tracked_inventory(root) != files
        or verify(root) != result
        or (root / MANIFEST).read_bytes() != contents[MANIFEST]
    ):
        raise PackageError("SOURCE_CHANGED_DURING_BUILD")
    output_dir.mkdir()
    try:
        archive = output_dir / archive_name
        # ZIP_STORED is portable and byte-identical across Python/zlib versions.
        with zipfile.ZipFile(archive, "x", compression=zipfile.ZIP_STORED) as bundle:
            for name, mode in files.items():
                info = zipfile.ZipInfo(name, date_time=ZIP_TIME)
                info.create_system = 3
                info.external_attr = (0o100000 | mode) << 16
                bundle.writestr(info, contents[name])
        digest = hashlib.sha256(archive.read_bytes()).hexdigest()
        (output_dir / "SHA256SUMS").write_text(f"{digest}  {archive_name}\n", encoding="utf-8")
    except BaseException:
        shutil.rmtree(output_dir)
        raise
    return {
        "status": "BUILT",
        "version": result["version"],
        "files": len(files),
        "archive": str(archive),
        "sha256": digest,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    operation = parser.add_mutually_exclusive_group(required=True)
    operation.add_argument(
        "--write-manifest",
        action="store_true",
        help="Explicitly refresh hashes of the staged file inventory; no archive.",
    )
    operation.add_argument(
        "--output-dir",
        type=Path,
        help="Create a new directory outside the repository; refuse overwrite.",
    )
    args = parser.parse_args(argv)
    try:
        result = (
            write_manifest(args.root) if args.write_manifest else build(args.root, args.output_dir)
        )
        print(json.dumps(result, ensure_ascii=False))
        return 0
    except (OSError, ValueError, TypeError, KeyError, SyntaxError, subprocess.SubprocessError):
        # Do not echo Git errors or local paths that could contain credentials.
        print(json.dumps({"status": "BLOCKED", "reason": "RELEASE_BUILD_FAILED"}))
        return 2


if __name__ == "__main__":
    sys.exit(main())
