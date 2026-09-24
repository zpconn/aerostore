#!/usr/bin/env python3
import unittest
import generate


class CaptureAdapterTests(unittest.TestCase):
    def setUp(self):
        self.source = generate.SOURCE.read_text()

    def test_fresh_generated_source(self):
        self.assertEqual(generate.OUTPUT.read_text(), generate.render(self.source))

    def test_dependency_capture_is_unconditional(self):
        original = "        for bucket in &buckets {\n            let stamp"
        altered = self.source.replace(original, "        if tx.index_reads.is_empty() {\n" + original)
        with self.assertRaises(ValueError):
            generate.render(altered)

    def test_early_guard_release_rejected(self):
        original = "        let candidates = index.transactional_raw_lookup(predicate)?;"
        with self.assertRaisesRegex(ValueError, "raw lookup"):
            generate.render(self.source.replace(original, "        drop(guards);\n" + original, 1))

    def test_dependency_clear_after_capture_rejected(self):
        original = "        drop(guards);\n        #[cfg(test)]\n        INDEX_CANDIDATES_CAPTURED_HOOK"
        with self.assertRaisesRegex(ValueError, "materialization boundary"):
            generate.render(self.source.replace(original, original.replace("drop(guards);", "drop(guards); tx.index_reads.clear();"), 1))

    def test_unknown_call_preserved(self):
        original = "            let stamp = index.transactional_stamp(*bucket)?;"
        generated = generate.render(self.source.replace(original, original + " unknown_operation();", 1))
        self.assertIn("unknown_operation ( )", generated)

    def test_snapshot_mutation_reaches_proof(self):
        generated = generate.render(self.source.replace("!aerostore_verified::stamp_precedes_snapshot(stamp, tx.txid)", "false", 1))
        self.assertIn("if false", generated)

    def test_changed_signature_rejected(self):
        with self.assertRaisesRegex(ValueError, "signature"):
            generate.render(self.source.replace("pub fn index_lookup(", "pub fn index_lookup(ignored: usize,", 1))

    def test_proof_bypass_rejected(self):
        original = "            let stamp = index.transactional_stamp(*bucket)?;"
        for bypass in ["assume(false);", "proof {}", "let ghost hidden = 0;"]:
            with self.subTest(bypass=bypass), self.assertRaisesRegex(ValueError, "proof bypass"):
                generate.render(self.source.replace(original, bypass + original, 1))

    def test_scalar_helper_extracted(self):
        helper = generate.HELPER.read_text().replace("stamp < transaction_id", "stamp <= transaction_id")
        self.assertIn("stamp <= transaction_id", generate.render(self.source, helper))


if __name__ == "__main__":
    unittest.main()
