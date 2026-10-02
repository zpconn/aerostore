import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

SPEC = importlib.util.spec_from_file_location("small_catalog", Path(__file__).with_name("check_catalog.py"))
catalog = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(catalog)


class CatalogTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.data = b'{"passed":true}\n'
        self.digest = hashlib.sha256(self.data).hexdigest()
        self.sha = "a" * 40
        self.write("evidence/README.md", b"# Evidence\n")
        self.write("evidence/example/summary.json", self.data)
        manifest = {"schema_version": 1, "campaign_id": "example", "files": [
            {"path": "summary.json", "bytes": len(self.data), "sha256": self.digest}]}
        self.json("evidence/manifests/example.json", manifest)
        self.entry = {"id": "example", "availability": "archive", "status": "diagnostic",
                      "source": {"repo": "example/archive", "ref": "archive/pre-rewrite",
                                 "path": "docs/bench_data/example", "commit": self.sha},
                      "manifest": {"path": "evidence/manifests/example.json", "sha256": hashlib.sha256(
                          (self.root / "evidence/manifests/example.json").read_bytes()).hexdigest()},
                      "files": 1, "bytes": len(self.data)}
        self.json("evidence/catalog.json", {"schema_version": 1, "archive_commit": self.sha, "campaigns": [self.entry]})
        self.summary = {"path": "evidence/example/summary.json", "source_path": "docs/bench_data/example/summary.json",
                        "archive_commit": self.sha, "bytes": len(self.data), "sha256": self.digest}
        self.json("evidence/summaries.json", {"schema_version": 1, "files": [self.summary]})

    def write(self, name, data):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)

    def json(self, name, data):
        self.write(name, json.dumps(data).encode())

    def test_valid_retained_copy_is_bound_to_archive(self):
        self.assertEqual(catalog.check(self.root)["retained_summaries_and_fixtures"], 1)

    def test_changed_summary_rejected(self):
        self.write(self.summary["path"], b'{"passed":false}\n')
        with self.assertRaisesRegex(ValueError, "differs from recorded bytes"):
            catalog.check(self.root)

    def test_rehashed_fabrication_still_rejected_by_archive_manifest(self):
        new = b'{"invented":true}\n'
        self.write(self.summary["path"], new)
        self.summary.update(bytes=len(new), sha256=hashlib.sha256(new).hexdigest())
        self.json("evidence/summaries.json", {"schema_version": 1, "files": [self.summary]})
        with self.assertRaisesRegex(ValueError, "differs from archive manifest"):
            catalog.check(self.root)

    def test_wrong_archive_revision_rejected(self):
        self.summary["archive_commit"] = "b" * 40
        self.json("evidence/summaries.json", {"schema_version": 1, "files": [self.summary]})
        with self.assertRaisesRegex(ValueError, "differs from archive manifest"):
            catalog.check(self.root)

    def test_unregistered_payload_rejected(self):
        self.write("evidence/unreviewed.bin", b"payload")
        with self.assertRaisesRegex(ValueError, "unexpected or missing"):
            catalog.check(self.root)

    def test_missing_or_changed_campaign_pin_is_rejected(self):
        for value in [None, "b" * 40]:
            with self.subTest(value=value):
                self.entry["source"]["commit"] = value
                if value is None:
                    del self.entry["source"]["commit"]
                self.json("evidence/catalog.json", {"schema_version": 1, "archive_commit": self.sha, "campaigns": [self.entry]})
                with self.assertRaisesRegex(ValueError, "campaign commit differs"):
                    catalog.check(self.root)

    def test_duplicate_summary_keys_are_rejected(self):
        self.write("evidence/summaries.json", b'{"schema_version":1,"schema_version":1,"files":[]}')
        with self.assertRaisesRegex(ValueError, "duplicate"):
            catalog.check(self.root)

    def test_symlink_cannot_substitute_for_summary(self):
        path = self.root / self.summary["path"]
        path.unlink()
        (self.root / "outside").write_bytes(self.data)
        path.symlink_to(self.root / "outside")
        with self.assertRaisesRegex(ValueError, "symlink"):
            catalog.check(self.root)


if __name__ == "__main__":
    unittest.main()
