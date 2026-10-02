import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import fetch_evidence as fetcher


class FetchEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name) / "consumer"
        self.source = Path(self.temporary.name) / "source"
        self.root.mkdir(); self.source.mkdir()
        self.git("init", "-q", "-b", "archive/pre-rewrite")
        self.payload = self.source / "docs/bench_data/sample"
        self.payload.mkdir(parents=True)
        (self.payload / "summary.json").write_bytes(b'{"ok": true}\n')
        (self.payload / "nested").mkdir()
        (self.payload / "nested/history.txt").write_bytes(b"original\n")
        (self.source / "outside.txt").write_bytes(b"not campaign evidence\n")
        self.commit()
        self.manifest = {"schema_version": 1, "campaign_id": "sample", "files": [
            {"path": path.relative_to(self.payload).as_posix(), "bytes": path.stat().st_size,
             "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
            for path in sorted(self.payload.rglob("*")) if path.is_file()]}
        self.entry = {"id": "sample", "date": None, "kind": "benchmark", "status": "diagnostic", "claims": [],
                      "availability": "archive", "source": {"repo": "zpconn/aerostore-archive", "ref": "archive/pre-rewrite",
                      "path": "docs/bench_data/sample", "commit": self.git("rev-parse", "HEAD").strip()},
                      "manifest": {"path": "evidence/manifests/sample.json"}}
        self.save()

    def git(self, *args):
        return subprocess.check_output(["git", "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", *args],
                                       cwd=self.source, stderr=subprocess.PIPE, text=True)

    def commit(self):
        self.git("add", ".")
        self.git("commit", "-qm", "fixture")

    def save(self):
        directory = self.root / "evidence/manifests"
        directory.mkdir(parents=True, exist_ok=True)
        raw = (json.dumps(self.manifest) + "\n").encode()
        (directory / "sample.json").write_bytes(raw)
        self.entry["manifest"]["sha256"] = hashlib.sha256(raw).hexdigest()
        self.entry["files"] = len(self.manifest["files"])
        self.entry["bytes"] = sum(record["bytes"] for record in self.manifest["files"])
        self.save_catalog()

    def save_catalog(self):
        (self.root / "evidence/catalog.json").write_text(json.dumps({"schema_version": 1, "campaigns": [self.entry]}))

    def fetch(self, **kwargs):
        # Tiny Git fixtures use ordinary CI's reserve; production keeps 30 GiB.
        kwargs.setdefault("reserve_bytes", 4 * fetcher.GIB)
        return fetcher.fetch(self.root, "sample", source_url=self.source.as_uri(), **kwargs)

    def changed_source(self):
        self.commit()
        self.entry["source"]["commit"] = self.git("rev-parse", "HEAD").strip()
        self.save_catalog()

    def test_local_partial_sparse_fetch_verifies_every_file(self):
        original_config = (self.source / ".git/config").read_bytes()
        unrelated_oid = self.git("rev-parse", "HEAD:outside.txt").strip()
        cleanup = fetcher.shutil.rmtree
        inspected = []
        def inspect_then_cleanup(clone):
            objects = subprocess.check_output(["git", "--no-replace-objects", "cat-file", "--batch-all-objects",
                                               "--batch-check=%(objectname)"], cwd=clone, text=True).splitlines()
            self.assertNotIn(unrelated_oid, objects, "unrelated blob was downloaded despite partial/sparse selection")
            inspected.append(True)
            cleanup(clone)
        with patch.object(fetcher.shutil, "rmtree", side_effect=inspect_then_cleanup):
            receipt, path = self.fetch()
        self.assertEqual(receipt["status"], "passed")
        self.assertTrue(receipt["local_fixture"])
        self.assertFalse(receipt["production_archive_qualified"])
        self.assertEqual(receipt["resolved_commit"], self.entry["source"]["commit"])
        self.assertEqual(json.loads(path.read_text()), receipt)
        destination = self.root / "runs/evidence/sample"
        self.assertEqual((destination / "summary.json").read_bytes(), (self.payload / "summary.json").read_bytes())
        self.assertFalse((destination / "outside.txt").exists())
        self.assertFalse((path.parent / "repository").exists())
        self.assertEqual(inspected, [True])
        self.assertEqual((self.source / ".git/config").read_bytes(), original_config)
        self.assertEqual(self.git("status", "--porcelain"), "")

    def test_explicit_fixture_ref_override_is_recorded(self):
        self.git("branch", "fixture/alternate")
        receipt, _ = self.fetch(source_ref="fixture/alternate")
        self.assertEqual(receipt["actual_ref"], "fixture/alternate")
        self.assertEqual(receipt["source"]["ref"], "archive/pre-rewrite")

    def test_commit_pin_is_not_bypassed_by_fixture_override(self):
        self.entry["source"]["commit"] = "0" * 40
        self.save_catalog()
        with self.assertRaisesRegex(ValueError, "commit differs"):
            self.fetch(source_ref="archive/pre-rewrite")

    def test_existing_output_is_never_overwritten(self):
        destination = self.root / "runs/evidence/sample"
        destination.mkdir(parents=True)
        (destination / "sentinel").write_text("preserve")
        with self.assertRaisesRegex(ValueError, "already exists"):
            self.fetch()
        self.assertEqual((destination / "sentinel").read_text(), "preserve")

    def test_manifest_paths_and_output_escape_are_rejected(self):
        for name in ("../escape", "/absolute", "nested/../escape", "nested//file", "C:/file", "a\\b", ".git/config"):
            with self.subTest(name=name):
                original = self.manifest["files"][0]["path"]
                self.manifest["files"][0]["path"] = name
                self.save()
                with self.assertRaises(ValueError): self.fetch()
                self.manifest["files"][0]["path"] = original
        self.save()
        with self.assertRaisesRegex(ValueError, "below runs/evidence"):
            self.fetch(output="other/new")

    def test_symlink_output_parent_is_rejected(self):
        (self.root / "runs").mkdir()
        (self.root / "runs/evidence").symlink_to(self.source, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, "symlink"):
            self.fetch()

    def test_modified_missing_and_extra_archive_files_are_rejected(self):
        for change in ("modified", "missing", "extra"):
            with self.subTest(change=change):
                if change == "modified":
                    (self.payload / "summary.json").write_bytes(b'{"ok":false}\n')
                elif change == "missing":
                    (self.payload / "summary.json").unlink()
                else:
                    (self.payload / "summary.json").write_bytes(b'{"ok": true}\n')
                    (self.payload / "extra.txt").write_text("unexpected")
                self.changed_source()
                with self.assertRaisesRegex(ValueError, "mismatch|missing or unexpected"):
                    self.fetch(output="runs/evidence/" + change)

    def test_tracked_symlink_is_rejected_before_payload_checkout(self):
        (self.payload / "summary.json").unlink()
        (self.payload / "summary.json").symlink_to("../../../../outside")
        self.changed_source()
        with self.assertRaisesRegex(ValueError, "symlink"):
            self.fetch()

    def test_payload_validator_rejects_unexpected_missing_and_symlink(self):
        files = {item["path"]: item for item in self.manifest["files"]}
        fetcher.verify_payload(self.payload, files)
        (self.payload / "extra").write_text("x")
        with self.assertRaisesRegex(ValueError, "unexpected"):
            fetcher.verify_payload(self.payload, files)
        (self.payload / "extra").unlink()
        (self.payload / "summary.json").unlink()
        with self.assertRaisesRegex(ValueError, "missing"):
            fetcher.verify_payload(self.payload, files)
        (self.payload / "summary.json").symlink_to(self.source / "outside.txt")
        with self.assertRaisesRegex(ValueError, "non-regular"):
            fetcher.verify_payload(self.payload, files)

    def test_manifest_hash_totals_and_duplicate_metadata_are_rejected(self):
        manifest_path = self.root / "evidence/manifests/sample.json"
        manifest_path.write_text("{}")
        with self.assertRaisesRegex(ValueError, "manifest SHA"):
            self.fetch()
        self.save()
        self.entry["bytes"] += 1; self.save_catalog()
        with self.assertRaisesRegex(ValueError, "totals"):
            self.fetch()
        self.save()
        catalog = self.root / "evidence/catalog.json"
        catalog.write_text('{"schema_version":1,"schema_version":1,"campaigns":[]}')
        with self.assertRaisesRegex(ValueError, "duplicate JSON"):
            self.fetch()

    def test_duplicate_manifest_paths_and_invalid_hash_are_rejected(self):
        self.manifest["files"].append(dict(self.manifest["files"][0]))
        self.save()
        with self.assertRaisesRegex(ValueError, "duplicate"):
            self.fetch()
        self.manifest["files"].pop()
        self.manifest["files"][0]["sha256"] = "bad"
        self.save()
        with self.assertRaisesRegex(ValueError, "invalid file"):
            self.fetch()

    def test_local_only_campaign_has_no_fetch_claim(self):
        self.entry = {"id": "sample", "availability": "local-only", "files": 20, "bytes": 30,
                      "note": "owner-local inventory; no public manifest"}
        self.save_catalog()
        with self.assertRaisesRegex(ValueError, "local-only"):
            self.fetch()

    def test_direct_files_selection_excludes_nested_campaigns_explicitly(self):
        self.entry["source"]["selection"] = "direct-files"
        self.manifest["files"] = [item for item in self.manifest["files"] if "/" not in item["path"]]
        self.save()
        receipt, _ = self.fetch()
        self.assertEqual(receipt["files"], 1)
        self.assertFalse((self.root / "runs/evidence/sample/nested").exists())

    def test_resource_admission_rejects_payload_and_low_space(self):
        with self.assertRaisesRegex(ValueError, "payload byte limit"):
            self.fetch(max_bytes=1)
        with patch.object(fetcher.shutil, "disk_usage", return_value=type("Space", (), {"free": 1})()):
            with self.assertRaisesRegex(ValueError, "reserve"):
                self.fetch()

    def test_malformed_metadata_types_are_rejected_before_git(self):
        catalog = self.root / "evidence/catalog.json"
        cases = [[], {"schema_version": True, "campaigns": []}, {"schema_version": 1, "campaigns": [None]},
                 {"schema_version": 1, "campaigns": [{"id": "sample", "availability": "archive", "source": []}]}]
        with patch.object(fetcher.Git, "run", side_effect=AssertionError("must not invoke Git")):
            for value in cases:
                with self.subTest(value=value):
                    catalog.write_text(json.dumps(value))
                    with self.assertRaises(ValueError): self.fetch()
        self.save()
        self.manifest["files"] = [None]
        raw = json.dumps(self.manifest).encode()
        (self.root / self.entry["manifest"]["path"]).write_bytes(raw)
        self.entry["manifest"]["sha256"] = hashlib.sha256(raw).hexdigest()
        self.save_catalog()
        with self.assertRaisesRegex(ValueError, "records must be objects"):
            self.fetch()


if __name__ == "__main__":
    unittest.main()
