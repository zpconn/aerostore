"""Hash-pinned TLC provisioning with mocked network responses only."""
from __future__ import annotations

import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import warnings
import zipfile

import tlc_download

ROOT = Path(__file__).resolve().parents[1]
MEMBER = "extension/tools/tla2tools.jar"
JAR = b"the exact reviewed TLC artifact"


def sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def archive_bytes(entries: list[tuple[str, bytes]]) -> bytes:
    output = io.BytesIO()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)  # Deliberately duplicated ZIP entries.
        with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for name, value in entries:
                archive.writestr(name, value)
    return output.getvalue()


class TlcDownloadTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory(prefix="tlc-download-test-")
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.jar = self.root / "tools/tla2tools.jar"
        self.pin = {"release_url": "https://example.invalid/tla2tools.jar", "sha256": sha256(JAR)}

    def provision(self, payload: bytes, pin: dict | None = None):
        with patch.object(tlc_download.urllib.request, "urlopen", return_value=io.BytesIO(payload)) as request:
            tlc_download.ensure_tlc(pin or self.pin, self.jar, allow_download=True)
            request.assert_called_once_with((pin or self.pin)["release_url"], timeout=60)

    def archive_pin(self, payload: bytes):
        return self.pin | {"release_url": "https://example.invalid/dated-extension.vsix",
                           "archive_member": MEMBER, "archive_sha256": sha256(payload)}

    def test_legacy_direct_jar_pin(self):
        self.provision(JAR)
        self.assertEqual(self.jar.read_bytes(), JAR)
        self.assertEqual(list(self.jar.parent.iterdir()), [self.jar])

    def test_pinned_vsix_reads_only_the_exact_member_without_extracting_paths(self):
        payload = archive_bytes([(MEMBER, JAR), ("../escape", b"do not extract"), ("other.jar", b"other jar")])
        self.provision(payload, self.archive_pin(payload))
        self.assertEqual(self.jar.read_bytes(), JAR)
        self.assertFalse((self.root / "escape").exists())
        self.assertEqual(list(self.jar.parent.iterdir()), [self.jar])

    def test_missing_tool_requires_explicit_download(self):
        with patch.object(tlc_download.urllib.request, "urlopen") as request:
            with self.assertRaisesRegex(ValueError, "missing TLC jar"):
                tlc_download.ensure_tlc(self.pin, self.jar)
            request.assert_not_called()
        self.assertFalse(self.jar.exists())

    def test_installed_valid_tool_is_not_rewritten_or_downloaded(self):
        self.jar.parent.mkdir()
        self.jar.write_bytes(JAR)
        before = self.jar.stat()
        with patch.object(tlc_download.urllib.request, "urlopen") as request:
            tlc_download.ensure_tlc(self.pin, self.jar, allow_download=True)
            request.assert_not_called()
        after = self.jar.stat()
        self.assertEqual((before.st_ino, before.st_mtime_ns), (after.st_ino, after.st_mtime_ns))
        self.assertEqual(self.jar.read_bytes(), JAR)

    def test_installed_mismatched_tool_is_not_replaced(self):
        self.jar.parent.mkdir()
        self.jar.write_bytes(b"retained historical tool")
        with patch.object(tlc_download.urllib.request, "urlopen") as request:
            with self.assertRaisesRegex(ValueError, "installed TLC JAR SHA-256 mismatch"):
                tlc_download.ensure_tlc(self.pin, self.jar, allow_download=True)
            request.assert_not_called()
        self.assertEqual(self.jar.read_bytes(), b"retained historical tool")

    def test_symlink_destination_is_not_followed_or_replaced(self):
        self.jar.parent.mkdir()
        other = self.root / "other.jar"
        other.write_bytes(JAR)
        self.jar.symlink_to(other)
        with patch.object(tlc_download.urllib.request, "urlopen") as request:
            with self.assertRaisesRegex(ValueError, "symlink"):
                tlc_download.ensure_tlc(self.pin, self.jar, allow_download=True)
            request.assert_not_called()
        self.assertTrue(self.jar.is_symlink())
        self.assertEqual(other.read_bytes(), JAR)

    def test_bad_direct_jar_hash_leaves_destination_absent(self):
        with self.assertRaisesRegex(ValueError, "downloaded TLC JAR SHA-256 mismatch"):
            self.provision(b"rolling replacement")
        self.assertFalse(self.jar.exists())

    def test_archive_outer_hash_is_checked_before_parsing(self):
        with self.assertRaisesRegex(ValueError, "TLC archive SHA-256 mismatch"):
            self.provision(b"not even a zip", self.archive_pin(b"different archive"))
        self.assertFalse(self.jar.exists())

    def test_archive_inner_hash_mismatch_is_fatal(self):
        payload = archive_bytes([(MEMBER, b"different JAR")])
        with self.assertRaisesRegex(ValueError, "downloaded TLC JAR SHA-256 mismatch"):
            self.provision(payload, self.archive_pin(payload))
        self.assertFalse(self.jar.exists())

    def test_missing_duplicate_and_directory_members_are_rejected(self):
        for entries in ([("wrong.jar", JAR)], [(MEMBER, JAR), (MEMBER, JAR)], [(MEMBER + "/", JAR)]):
            with self.subTest(entries=entries):
                payload = archive_bytes(entries)
                with self.assertRaisesRegex(ValueError, "exactly one pinned JAR member"):
                    self.provision(payload, self.archive_pin(payload))
                self.assertFalse(self.jar.exists())

    def test_invalid_archive_is_fatal(self):
        payload = b"not a ZIP"
        with self.assertRaisesRegex(ValueError, "cannot read pinned TLC archive"):
            self.provision(payload, self.archive_pin(payload))
        self.assertFalse(self.jar.exists())

    def test_archive_requires_outer_hash_and_canonical_member(self):
        invalid = [{"archive_member": MEMBER}, {"archive_sha256": "a" * 64},
                   {"archive_member": None}, {"archive_member": MEMBER, "archive_sha256": "invalid"}]
        invalid += [{"archive_member": member, "archive_sha256": "a" * 64}
                    for member in ("../tool.jar", "/tool.jar", "./tool.jar", "a\\tool.jar", "", ".")]
        for extra in invalid:
            with self.subTest(extra=extra), patch.object(tlc_download.urllib.request, "urlopen") as request:
                with self.assertRaises(ValueError):
                    tlc_download.ensure_tlc(self.pin | extra, self.jar, allow_download=True)
                request.assert_not_called()

    def test_download_limit_is_enforced(self):
        with patch.object(tlc_download, "MAX_BYTES", 8):
            with self.assertRaisesRegex(ValueError, "TLC download exceeds"):
                self.provision(JAR)
        self.assertFalse(self.jar.exists())

    def test_decompressed_member_limit_is_enforced(self):
        oversized = b"a" * 4096
        payload = archive_bytes([(MEMBER, oversized)])
        self.assertLess(len(payload), 512)
        pin = self.archive_pin(payload) | {"sha256": sha256(oversized)}
        with patch.object(tlc_download, "MAX_BYTES", 512):
            with self.assertRaisesRegex(ValueError, "archive member exceeds"):
                self.provision(payload, pin)
        self.assertFalse(self.jar.exists())

    def test_publication_race_preserves_the_newly_installed_tool(self):
        original_link = os.link

        def competing_install(source, destination):
            destination.write_bytes(b"another retained tool")
            return original_link(source, destination)

        with patch.object(tlc_download.os, "link", side_effect=competing_install):
            with self.assertRaises(FileExistsError):
                self.provision(JAR)
        self.assertEqual(self.jar.read_bytes(), b"another retained tool")
        self.assertEqual(list(self.jar.parent.iterdir()), [self.jar])

    def test_failed_atomic_publish_cleans_only_its_own_temporary_file(self):
        self.jar.parent.mkdir()
        retained = self.jar.parent / "unrelated.download"
        retained.write_bytes(b"retained diagnostic")
        with patch.object(tlc_download.os, "link", side_effect=OSError("injected publication failure")):
            with self.assertRaisesRegex(OSError, "injected publication failure"):
                self.provision(JAR)
        self.assertFalse(self.jar.exists())
        self.assertEqual(list(self.jar.parent.iterdir()), [retained])

    def test_committed_pins_have_valid_download_schema_and_same_jar_identity(self):
        pins = [json.loads((ROOT / path).read_text()) for path in
                ("verification/tla/toolchain.json", "verification/service_protocol/toolchain.json")]
        for pin in pins:
            tlc_download.validate_pin(pin)
        self.assertEqual(pins[0]["sha256"], pins[1]["sha256"])

    def test_runner_can_load_its_helper_when_imported_by_path(self):
        spec = importlib.util.spec_from_file_location("tlc_runner_fixture", ROOT / "scripts/check_tla.py")
        runner = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(runner)
        self.assertEqual(Path(runner.tlc_download.__file__), ROOT / "scripts/tlc_download.py")


if __name__ == "__main__":
    unittest.main()
