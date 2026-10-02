"""Release tests use synthetic temporary repositories; no service or GitHub writes."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
from build_release import build, tracked_inventory, write_manifest  # noqa: E402
from verify_package import MANIFEST, PLUGIN, PackageError, verify  # noqa: E402


class ReleaseToolsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="etf-release-test-")
        self.addCleanup(self.temporary.cleanup)
        self.parent = Path(self.temporary.name)
        self.root = self.parent / "repo"
        self.root.mkdir()
        self.git = shutil.which("git")
        if self.git is None:
            self.skipTest("Git required")
        self.run_git("init", "-q")
        self.write(
            f"{PLUGIN}/.codex-plugin/plugin.json",
            json.dumps(
                {
                    "name": "etf-sentinel",
                    "version": "0.2.0",
                }
            ),
        )
        self.write(
            f"{PLUGIN}/runtime/pyproject.toml",
            '[project]\nname = "etf-sentinel"\nversion = "0.2.0"\n',
        )
        self.write(f"{PLUGIN}/runtime/src/etf_sentinel/__init__.py", '__version__ = "0.2.0"\n')
        self.write(
            f"{PLUGIN}/runtime/uv.lock",
            'version = 1\n[[package]]\nname = "etf-sentinel"\nversion = "0.2.0"\n',
        )
        self.write(
            f"{PLUGIN}/runtime/.env.example",
            "\n".join(
                [
                    "TRADING_MODE=paper",
                    "MARKET_DATA_PROVIDER=demo_fixture",
                    "ENABLE_LIVE_TRADING=false",
                    "ENABLE_ORDER_ENTRY=false",
                    "ENABLE_PUBLIC_SERVICE=false",
                    "",
                ]
            ),
        )
        self.write(
            ".agents/plugins/marketplace.json",
            json.dumps(
                {
                    "plugins": [
                        {
                            "name": "etf-sentinel",
                            "source": {"source": "local", "path": "./" + PLUGIN},
                        }
                    ]
                }
            ),
        )
        self.write("README.md", "Public synthetic Demo package.\n")
        self.write("scripts/run.sh", "#!/bin/sh\nexit 0\n")
        self.write(MANIFEST, "{}\n")
        self.run_git("add", ".")
        self.run_git("update-index", "--chmod=+x", "scripts/run.sh")
        write_manifest(self.root)

    def write(self, name: str, content: str) -> None:
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")

    def run_git(self, *arguments: str) -> None:
        subprocess.run(  # noqa: S603 - resolved Git and fixed test arguments in a temporary fixture
            [self.git, *arguments], cwd=self.root, check=True, capture_output=True, timeout=15
        )

    def read_manifest(self) -> dict:
        return json.loads((self.root / MANIFEST).read_text("utf-8"))

    def mutate_manifest(self, **changes: object) -> None:
        self.write(MANIFEST, json.dumps({**self.read_manifest(), **changes}))

    def assert_build_blocked(self) -> None:
        target = self.parent / "blocked-output"
        with self.assertRaises(PackageError):
            build(self.root, target)
        self.assertFalse(target.exists())

    def test_two_builds_are_byte_identical_despite_filesystem_mtime_and_permissions(self) -> None:
        first = build(self.root, self.parent / "first")
        os.utime(self.root / "README.md", (1_800_000_000, 1_800_000_000))
        (self.root / "README.md").chmod(0o600)
        second = build(self.root, self.parent / "second")
        archive = Path(first["archive"])
        self.assertEqual(archive.read_bytes(), Path(second["archive"]).read_bytes())
        self.assertEqual(first["sha256"], hashlib.sha256(archive.read_bytes()).hexdigest())
        self.assertEqual(
            (archive.parent / "SHA256SUMS").read_text(),
            f"{first['sha256']}  etf-sentinel-v0.2.0.zip\n",
        )
        with zipfile.ZipFile(archive) as bundle:
            self.assertEqual(bundle.namelist(), sorted(tracked_inventory(self.root)))
            self.assertEqual(bundle.getinfo("scripts/run.sh").external_attr >> 16, 0o100755)
            self.assertEqual(bundle.getinfo("README.md").external_attr >> 16, 0o100644)
            self.assertTrue(
                all(info.date_time == (2020, 1, 1, 0, 0, 0) for info in bundle.infolist())
            )
            extracted = self.parent / "extracted"
            bundle.extractall(extracted)
        self.assertEqual(verify(extracted)["status"], "VERIFIED")

    def test_tampered_content_is_not_silently_rehashed_by_build(self) -> None:
        self.write("README.md", "Changed without explicit review.\n")
        old_manifest = (self.root / MANIFEST).read_bytes()
        self.assert_build_blocked()
        self.assertEqual((self.root / MANIFEST).read_bytes(), old_manifest)
        self.assertEqual(write_manifest(self.root)["status"], "VERIFIED")

    def test_extra_untracked_file_is_rejected_even_by_manifest_writer(self) -> None:
        self.write("unexpected.txt", "Not reviewed.\n")
        self.assert_build_blocked()
        with self.assertRaises(PackageError):
            write_manifest(self.root)

    def test_new_staged_file_requires_explicit_manifest_refresh(self) -> None:
        self.write("CHANGELOG.md", "New release.\n")
        self.run_git("add", "CHANGELOG.md")
        self.assert_build_blocked()
        write_manifest(self.root)
        self.assertEqual(build(self.root, self.parent / "ready")["status"], "BUILT")

    def test_missing_tracked_file_is_rejected(self) -> None:
        (self.root / "README.md").unlink()
        self.assert_build_blocked()

    def test_private_files_refused_even_when_staged(self) -> None:
        for name in (".env", ".env.production", "local.db", "local.sqlite3", "var/state.txt"):
            with self.subTest(name=name):
                self.write(name, "fixture\n")
                self.run_git("add", "-f", name)
                self.assert_build_blocked()
                with self.assertRaises(PackageError):
                    write_manifest(self.root)
                self.run_git("rm", "--cached", name)
                (self.root / name).unlink()

    def test_secrets_refused_without_echoing_content(self) -> None:
        # Construct only synthetic credentials at runtime so the test source is publishable.
        samples = [
            "sk-" + "a" * 24,
            "ghp_" + "b" * 36,
            "AKIA" + "X" * 16,
            "-----BEGIN " + "PRIVATE KEY-----",
        ]
        for sample in samples:
            with self.subTest(sample_length=len(sample)):
                self.write("README.md", sample)
                self.assert_build_blocked()
                with self.assertRaises(PackageError):
                    write_manifest(self.root)

    def test_local_path_refused(self) -> None:
        self.write("README.md", "/" + "Users" + "/synthetic/private.txt")
        self.assert_build_blocked()

    def test_file_and_directory_symlinks_refused(self) -> None:
        for target in (self.root / "README.md", self.parent):
            link = self.root / "linked"
            link.symlink_to(target)
            self.assert_build_blocked()
            link.unlink()

    def test_manifest_symlink_refused(self) -> None:
        source = self.parent / "outside.json"
        source.write_bytes((self.root / MANIFEST).read_bytes())
        (self.root / MANIFEST).unlink()
        (self.root / MANIFEST).symlink_to(source)
        self.assert_build_blocked()

    def test_tracked_cache_refused_but_untracked_cache_excluded(self) -> None:
        self.write("__pycache__/test.pyc", "disposable cache")
        build(self.root, self.parent / "without-cache")
        self.run_git("add", "-f", "__pycache__/test.pyc")
        self.assert_build_blocked()

    def test_output_cannot_be_inside_source_or_replace_existing_directory(self) -> None:
        for output in (self.root / "dist", self.root, self.parent):
            with self.subTest(output=output.name), self.assertRaises(PackageError):
                build(self.root, output)
        self.assertFalse((self.root / "dist").exists())

    def test_manifest_metadata_mismatch_is_rejected(self) -> None:
        original = self.read_manifest()
        for key, value in (
            ("name", "other"),
            ("version", "9.9.9"),
            ("data_mode", "LIVE"),
            ("trading_mode", "live"),
            ("schema_version", True),
        ):
            with self.subTest(key=key):
                self.write(MANIFEST, json.dumps({**original, key: value}))
                self.assert_build_blocked()

    def test_runtime_version_mismatch_blocks_manifest_refresh(self) -> None:
        self.write(
            f"{PLUGIN}/runtime/pyproject.toml",
            '[project]\nname = "etf-sentinel"\nversion = "0.1.0"\n',
        )
        with self.assertRaisesRegex(PackageError, "RUNTIME_VERSION_MISMATCH"):
            write_manifest(self.root)

    def test_marketplace_identity_mismatch_blocks_manifest_refresh(self) -> None:
        self.write(
            ".agents/plugins/marketplace.json",
            json.dumps(
                {
                    "plugins": [
                        {
                            "name": "other",
                            "source": {"source": "local", "path": "./" + PLUGIN},
                        }
                    ]
                }
            ),
        )
        with self.assertRaisesRegex(PackageError, "MARKETPLACE_IDENTITY_MISMATCH"):
            write_manifest(self.root)

    def test_runtime_declared_version_mismatch_blocks_manifest_refresh(self) -> None:
        self.write(f"{PLUGIN}/runtime/src/etf_sentinel/__init__.py", '__version__ = "0.1.0"\n')
        with self.assertRaisesRegex(PackageError, "RUNTIME_DECLARED_VERSION_MISMATCH"):
            write_manifest(self.root)

    def test_lock_version_mismatch_blocks_manifest_refresh(self) -> None:
        self.write(
            f"{PLUGIN}/runtime/uv.lock",
            'version = 1\n[[package]]\nname = "etf-sentinel"\nversion = "0.1.0"\n',
        )
        with self.assertRaisesRegex(PackageError, "LOCK_VERSION_MISMATCH"):
            write_manifest(self.root)

    def test_live_default_blocks_manifest_refresh(self) -> None:
        path = self.root / PLUGIN / "runtime/.env.example"
        path.write_text(path.read_text().replace("TRADING_MODE=paper", "TRADING_MODE=live"))
        with self.assertRaisesRegex(PackageError, "UNSAFE_DEMO_DEFAULTS"):
            write_manifest(self.root)

    def test_duplicate_manifest_key_is_rejected(self) -> None:
        self.write(MANIFEST, '{"version":"0.2.0",' + (self.root / MANIFEST).read_text()[1:])
        with self.assertRaisesRegex(PackageError, "DUPLICATE_JSON_KEY"):
            verify(self.root)

    def test_duplicate_plugin_key_is_rejected(self) -> None:
        self.write(
            f"{PLUGIN}/.codex-plugin/plugin.json",
            '{"name":"etf-sentinel","version":"0.1.0","version":"0.2.0"}',
        )
        with self.assertRaisesRegex(PackageError, "DUPLICATE_JSON_KEY"):
            write_manifest(self.root)


if __name__ == "__main__":
    unittest.main()
