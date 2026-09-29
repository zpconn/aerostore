#!/usr/bin/env python3
"""Small artifact/provenance tests; never invoke Cargo or a benchmark."""

import io
import json
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import hyperfeed_screen_capture as capture


class CaptureTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.repo = self.root / "repo"
        (self.repo / "scripts").mkdir(parents=True)
        (self.repo / "Cargo.toml").write_text("[workspace]\nmembers=[]\n")
        (self.repo / "Cargo.lock").write_text("version=4\n")
        (self.repo / "scripts/qualify_hyperfeed.py").write_text("# frozen driver\n")
        self.snapshot = capture.qualification.snapshot_sources(self.repo)
        self.binary = self.root / "historical-benchmark"
        self.binary.write_bytes(b"historical executable")
        self.dependency = self.root / "runtime.so"
        self.dependency.write_bytes(b"runtime")

    def archive(self, extras=()):
        archive = self.root / "source.tar.gz"
        with tarfile.open(archive, "w:gz") as stream:
            for name in self.snapshot["files"]:
                stream.add(self.repo / name, arcname=name, recursive=False)
            for name, kind in extras:
                member = tarfile.TarInfo(name)
                if kind == "link":
                    member.type, member.linkname = tarfile.SYMTYPE, "/tmp/escape"
                    stream.addfile(member)
                else:
                    member.size = 1
                    stream.addfile(member, io.BytesIO(b"x"))
        return archive

    def receipt(self):
        archive = self.archive()
        value = {"passed": True, "completed": True, "exit_code": 0, "source_stable": True,
                 "source_before": self.snapshot, "source_after": self.snapshot,
                 "source_archive_path": str(archive), "source_archive_sha256": capture.digest(archive),
                 "binary_path": str(self.binary), "binary_sha256": capture.digest(self.binary),
                 "compiler": "rustc historical", "cargo": "cargo historical", "command": ["cargo", "build"],
                 "build_environment": {"RUSTFLAGS": "--cfg historical"}, "features": [],
                 "runtime_after": {"checks": [{"matches": True, **capture.identity(self.dependency)}]}}
        path = self.root / "historical-receipt.json"
        capture.write_json(path, value)
        return path

    def imported(self):
        with patch.object(capture, "runtime_dependencies", return_value=[capture.identity(self.dependency)]):
            return capture.import_capture(self.receipt(), self.root / "capture")

    def test_import_preserves_binary_source_and_distinguishes_build(self):
        value = self.imported()
        self.assertEqual(value["source"]["sha256"], self.snapshot["sha256"])
        self.assertEqual(value["benchmark"]["sha256"], capture.digest(self.binary))
        self.assertEqual(value["mode"], "imported_trusted_build_receipt")
        self.assertEqual(capture.validate_capture(self.root / "capture"), value)
        self.assertEqual(value["comparison_build_identity"]["flags"], ["--cfg", "historical"])

    def test_import_rejects_changed_historical_binary(self):
        receipt = self.receipt()
        self.binary.write_bytes(b"changed")
        with self.assertRaisesRegex(ValueError, "historical benchmark changed"):
            capture.import_capture(receipt, self.root / "capture")

    def test_import_rejects_unsuccessful_receipt(self):
        receipt = self.receipt()
        value = json.loads(receipt.read_text())
        value["passed"] = False
        capture.write_json(receipt, value)
        with self.assertRaisesRegex(ValueError, "did not pass"):
            capture.import_capture(receipt, self.root / "capture")

    def test_archive_rejects_traversal_links_duplicates_and_extra_files(self):
        for index, extra in enumerate((("../escape", "file"), ("link", "link"),
                                       ("Cargo.toml", "file"), ("extra.rs", "file"))):
            with self.subTest(extra=extra), self.assertRaises(ValueError):
                capture.extract_sources(self.archive([extra]), self.root / str(index), self.snapshot)

    def test_archive_rejects_hash_mismatch(self):
        archive = self.archive()
        bad = {"files": dict(self.snapshot["files"])}
        bad["files"]["Cargo.toml"] = "0" * 64
        bad["sha256"] = capture.source_digest(bad["files"])
        with self.assertRaisesRegex(ValueError, "member hash mismatch"):
            capture.extract_sources(archive, self.root / "unpack", bad)

    def test_validation_rejects_changed_source_and_runtime(self):
        value = self.imported()
        source = Path(value["source"]["root"]) / "Cargo.toml"
        original = source.read_bytes()
        source.write_bytes(b"changed")
        with self.assertRaisesRegex(ValueError, "captured source changed"):
            capture.validate_capture(self.root / "capture")
        source.write_bytes(original)
        self.dependency.write_bytes(b"changed")
        with self.assertRaisesRegex(ValueError, "runtime artifact changed"):
            capture.validate_capture(self.root / "capture")

    def test_validation_rejects_added_source(self):
        value = self.imported()
        (Path(value["source"]["root"]) / "scripts/new.py").write_text("new")
        with self.assertRaisesRegex(ValueError, "inventory changed"):
            capture.validate_capture(self.root / "capture")

    def test_output_is_never_overwritten(self):
        self.imported()
        with self.assertRaises(FileExistsError):
            capture.import_capture(self.receipt(), self.root / "capture")

    def test_namespace_admission_is_stable_and_rejects_prior_unknown_use(self):
        with capture.namespace_lock(self.repo, self.snapshot) as first:
            pass
        with capture.namespace_lock(self.repo, self.snapshot) as second:
            self.assertEqual(first, second)
        marker = self.repo / "target" / ("." + capture.NAMESPACE + ".json")
        marker.unlink()
        fingerprint = self.repo / "target/release/.fingerprint/example/example.json"
        fingerprint.parent.mkdir(parents=True)
        fingerprint.write_text(json.dumps({"rustflags": capture.FLAGS}))
        with self.assertRaisesRegex(ValueError, "already used"):
            with capture.namespace_lock(self.repo, self.snapshot):
                pass

    def test_namespace_must_not_have_source_semantics(self):
        (self.repo / "Cargo.toml").write_text("# " + capture.NAMESPACE)
        with self.assertRaisesRegex(ValueError, "affects source semantics"):
            with capture.namespace_lock(self.repo, capture.qualification.snapshot_sources(self.repo)):
                pass

    def test_environment_discards_variable_build_overrides(self):
        with patch.dict("os.environ", {"RUSTFLAGS": "--cfg mutant", "CARGO_PROFILE_RELEASE_LTO": "fat",
                                       "RUSTC_WRAPPER": "instrument", "CARGO_ENCODED_RUSTFLAGS": "unsafe"}):
            environment = capture.build_environment(self.repo, Path("/tools/rustc"))
        self.assertNotIn("RUSTFLAGS", environment)
        self.assertNotIn("CARGO_PROFILE_RELEASE_LTO", environment)
        self.assertEqual(environment["RUSTC_WRAPPER"], "")
        self.assertEqual(environment["CARGO_ENCODED_RUSTFLAGS"], "\x1f".join(capture.FLAGS))

    def test_capture_disk_check_uses_only_declared_host_volume(self):
        host = self.root / "actual-host-volume"
        host.mkdir()
        disk = type("Disk", (), {"free": 100 << 30})()
        for selected, expected in ((None, [self.repo]), (host, [self.repo, host])):
            with self.subTest(selected=selected), \
                    patch.object(capture.shutil, "disk_usage", return_value=disk) as usage, \
                    patch.object(capture.subprocess, "check_output", side_effect=RuntimeError("stop before build")):
                with self.assertRaisesRegex(RuntimeError, "stop before build"):
                    capture.capture_variant(self.root / "capture", repo=self.repo, host_volume=selected)
                self.assertEqual([call.args[0] for call in usage.call_args_list], expected)

    def test_capture_keeps_mutable_cargo_artifact_paths_out_of_evidence(self):
        tool = self.root / "tools"
        tool.mkdir()
        for name in ("cargo", "rustc"):
            (tool / name).write_bytes(name.encode())
        artifact = self.repo / "target/release/deps/mutable-benchmark"
        artifact.parent.mkdir(parents=True)
        artifact.write_bytes(b"compiled binary")
        event = {"reason": "compiler-artifact", "target": {"name": capture.BENCH}, "executable": str(artifact),
                 "filenames": [str(artifact)], "features": [], "profile": {"opt_level": "3"}}

        def check_output(command, **kwargs):
            if command[:2] == ["rustup", "which"]:
                return str(tool / "cargo") + "\n"
            if command[-1] == "-Vv":
                return "release: 1.93.1\nhost: x86_64-unknown-linux-gnu\n"
            return "cargo 1.93.1\n"

        with patch.object(capture.subprocess, "check_output", side_effect=check_output), \
                patch.object(capture.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, json.dumps(event))), \
                patch.object(capture, "runtime_dependencies", return_value=[capture.identity(self.dependency)]), \
                patch.object(capture.shutil, "disk_usage", return_value=type("Disk", (), {"free": 100 << 30})()):
            value = capture.capture_variant(self.root / "capture", repo=self.repo)
        self.assertEqual(value["benchmark"]["sha256"], capture.digest(artifact))
        for path in (self.root / "capture").rglob("*"):
            if path.is_file():
                self.assertNotIn(str(artifact).encode(), path.read_bytes())
        artifact.write_bytes(b"later rebuild")
        self.assertEqual(capture.validate_capture(self.root / "capture"), value)


if __name__ == "__main__":
    unittest.main()
