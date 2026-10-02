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


    def add_history_map(self):
        rows = {"1" * 40: "1" * 40, "2" * 40: "b" * 40,
                "3" * 40: "0" * 40, self.sha: "c" * 40, "d" * 40: "e" * 40}
        raw = ("old" + " " * 38 + "new\n" + "".join(old + " " + new + "\n"
                                                    for old, new in sorted(rows.items()))).encode()
        self.write(catalog.HISTORY_MAP, raw)
        self.provenance = {
            "schema_version": 1,
            "map": {"path": catalog.HISTORY_MAP, "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest(),
                    "counts": {"total": 5, "unchanged": 1, "rewritten": 3, "removed": 1}},
            "source": {"original_tip": "d" * 40, "rewritten_tip": "e" * 40},
            "archive": {"repo": self.entry["source"]["repo"], "ref": self.entry["source"]["ref"],
                        "commit": self.sha, "original_public_branches": {"refs/heads/master": self.sha}},
            "classification": {"originally_public": ["1" * 40, "2" * 40, "3" * 40, self.sha],
                               "unpublished_preparation": ["d" * 40]},
        }
        self.json(catalog.HISTORY_PROVENANCE, self.provenance)
        return raw

    def save_changed_map(self, raw):
        self.write(catalog.HISTORY_MAP, raw)
        self.provenance["map"].update(bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest())
        self.json(catalog.HISTORY_PROVENANCE, self.provenance)

    def test_valid_history_map_has_exact_counts_and_no_remote_claim(self):
        self.add_history_map()
        result = catalog.check(self.root)["history_commit_map"]
        self.assertEqual(result["counts"], {"total": 5, "unchanged": 1, "rewritten": 3, "removed": 1})
        self.assertEqual(result["originally_public_commits_declared"], 4)
        self.assertEqual(result["unpublished_preparation_commits_declared"], 1)
        self.assertFalse(result["archive_reachability_verified"])

    def test_history_map_and_provenance_are_required_together(self):
        for missing in (catalog.HISTORY_MAP, catalog.HISTORY_PROVENANCE):
            with self.subTest(missing=missing):
                self.add_history_map()
                (self.root / missing).unlink()
                with self.assertRaisesRegex(ValueError, "supplied together"):
                    catalog.check(self.root)

    def test_history_map_bytes_and_digest_are_bound(self):
        raw = self.add_history_map()
        self.write(catalog.HISTORY_MAP, raw.replace(b"b" * 40, b"f" * 40))
        with self.assertRaisesRegex(ValueError, "SHA-256"):
            catalog.check(self.root)

    def test_history_map_rehashed_malformed_or_duplicate_rows_are_rejected(self):
        raw = self.add_history_map()
        bad = [raw.replace(b" " * 38, b" ", 1), raw[:-1], raw + raw.splitlines(keepends=True)[1],
               raw.replace(b"1" * 40, b"0" * 40, 1), raw.replace(b"b" * 40, b"B" * 40)]
        for data in bad:
            with self.subTest(data=data[:90]):
                self.save_changed_map(data)
                with self.assertRaisesRegex(ValueError, "history map|history old"):
                    catalog.check(self.root)

    def test_history_counts_are_recomputed_and_booleans_are_not_counts(self):
        for value in (6, True):
            with self.subTest(value=value):
                self.add_history_map()
                self.provenance["map"]["counts"]["total"] = value
                self.json(catalog.HISTORY_PROVENANCE, self.provenance)
                with self.assertRaisesRegex(ValueError, "counts"):
                    catalog.check(self.root)

    def test_history_classification_is_an_exhaustive_disjoint_partition(self):
        for failure in ("overlap", "omission", "unknown"):
            with self.subTest(failure=failure):
                self.add_history_map()
                public = self.provenance["classification"]["originally_public"]
                if failure == "overlap": public.append("d" * 40)
                elif failure == "omission": public.pop(0)
                else: public.append("f" * 40)
                self.json(catalog.HISTORY_PROVENANCE, self.provenance)
                with self.assertRaisesRegex(ValueError, "partition"):
                    catalog.check(self.root)

    def test_preparation_tip_cannot_be_claimed_as_public(self):
        self.add_history_map()
        self.provenance["classification"]["originally_public"].append("d" * 40)
        self.provenance["classification"]["unpublished_preparation"] = []
        self.json(catalog.HISTORY_PROVENANCE, self.provenance)
        with self.assertRaisesRegex(ValueError, "unpublished preparation"):
            catalog.check(self.root)

    def test_history_tip_and_archive_pin_must_match(self):
        for failure in ("mapping", "archive", "branch"):
            with self.subTest(failure=failure):
                self.add_history_map()
                if failure == "mapping": self.provenance["source"]["rewritten_tip"] = "f" * 40
                elif failure == "archive": self.provenance["archive"]["commit"] = "2" * 40
                else: self.provenance["archive"]["original_public_branches"]["refs/heads/master"] = "d" * 40
                self.json(catalog.HISTORY_PROVENANCE, self.provenance)
                with self.assertRaisesRegex(ValueError, "history source tip mapping|history archive commit|not classified public"):
                    catalog.check(self.root)

    def test_history_provenance_rejects_duplicate_and_unknown_keys(self):
        self.add_history_map()
        self.write(catalog.HISTORY_PROVENANCE, b'{"schema_version":1,"schema_version":1}')
        with self.assertRaisesRegex(ValueError, "duplicate"):
            catalog.check(self.root)
        self.provenance["unchecked_extension"] = {}
        self.json(catalog.HISTORY_PROVENANCE, self.provenance)
        with self.assertRaisesRegex(ValueError, "provenance fields"):
            catalog.check(self.root)

    def test_history_paths_are_exact_and_cannot_be_symlinks(self):
        self.add_history_map()
        path = self.root / catalog.HISTORY_MAP
        raw = path.read_bytes()
        path.unlink()
        self.write("outside-map", raw)
        path.symlink_to(self.root / "outside-map")
        with self.assertRaisesRegex(ValueError, "symlink"):
            catalog.check(self.root)

    def test_history_map_support_does_not_allow_extra_evidence_files(self):
        self.add_history_map()
        self.write("evidence/history-other-map.tsv", b"not allowed\n")
        with self.assertRaisesRegex(ValueError, "unexpected or missing"):
            catalog.check(self.root)


if __name__ == "__main__":
    unittest.main()
