#!/usr/bin/env python3
"""Fail-closed adapter tests; semantic mutants are checked by run.py."""
import unittest
import generate
import run


class NativeAdapterTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source = generate.SOURCE.read_text()
        cls.helper = generate.HELPER.read_text()

    def test_checked_artifact_is_current(self):
        self.assertEqual(generate.OUTPUT.read_text(), generate.render(self.source, self.helper))

    def test_comments_do_not_change_operations(self):
        source = self.source.replace("let mut keys = BTreeSet::new();", "// ordinary comment\n        let mut keys = BTreeSet::new();", 1)
        self.assertEqual(generate.render(source, self.helper), generate.render(self.source, self.helper))

    def test_native_duplicate_rejected(self):
        with self.assertRaises(ValueError):
            generate.render(self.source + "\n    fn index_lock_keys() {}", self.helper)

    def test_native_signature_change_rejected(self):
        with self.assertRaises(ValueError):
            generate.render(self.source.replace("fn index_read_conflict(&self, tx: &OccTransaction<T>)", "fn index_read_conflict(&self, tx: &OtherTransaction<T>)", 1), self.helper)

    def test_wrong_registry_predicate_rejected(self):
        with self.assertRaises(ValueError):
            generate.render(self.source.replace(".position(|bound| bound.index.header_offset() == read.index_offset)", ".position(|bound| true)", 1), self.helper)

    def test_changed_atomic_ordering_rejected(self):
        marker = "let stamp = self.shm.global_txid().fetch_add(1, Ordering::AcqRel);"
        with self.assertRaises(ValueError):
            generate.render(self.source.replace(marker, marker.replace("AcqRel", "Relaxed"), 1), self.helper)

    def test_wrong_key_iteration_rejected(self):
        marker = "for key in [change.before.as_ref(), change.after.as_ref()]"
        with self.assertRaises(ValueError):
            generate.render(self.source.replace(marker, "for key in [change.after.as_ref(), change.after.as_ref()]", 1), self.helper)

    def test_unknown_receiver_call_rejected(self):
        with self.assertRaises(ValueError):
            generate.render(self.source.replace("let mut keys = BTreeSet::new();", "self.surprise(); let mut keys = BTreeSet::new();", 1), self.helper)

    def test_proof_bypass_rejected(self):
        for bypass in ("assume(true);", "admit();", "unsafe {}", "#[cfg(test)] let hidden = 0;"):
            with self.subTest(bypass=bypass), self.assertRaises(ValueError):
                generate.render(self.source.replace("let mut keys = BTreeSet::new();", bypass + "let mut keys = BTreeSet::new();", 1), self.helper)

    def test_semantic_mutations_reach_verifier(self):
        for name, root, old, new in run.MUTATIONS:
            with self.subTest(name=name):
                if root == "stamp_precedes_snapshot":
                    out = generate.render(self.source, self.helper.replace(old, new, 1))
                else:
                    offset = self.source.index("    fn " + root + "(")
                    mutant = self.source[:offset] + self.source[offset:].replace(old, new, 1)
                    out = generate.render(mutant, self.helper)
                self.assertNotEqual(generate.render(self.source, self.helper), out)
                self.assertNotIn("< =", out)


if __name__ == "__main__":
    unittest.main()
