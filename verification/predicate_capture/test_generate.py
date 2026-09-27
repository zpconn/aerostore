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

    def test_frozen_prefix_and_new_premise_are_represented(self):
        generated = generate.render(self.source)
        self.assertIn("let prior_read_len = tx . index_reads . len ( ) ;", generated)
        self.assertIn("find_read_prefix ( & tx . index_reads , prior_read_len", generated)
        self.assertIn("let previous = if tx . index_reads . len ( ) == prior_read_len", generated)
        self.assertIn("find_read_prefix ( & tx . index_reads , tx . index_reads . len ( )", generated)
        self.assertIn("proof { assert(tx.index_reads.len() == prior_read_len); }", generated)
        self.assertIn("requires unique(old(tx).index_reads@), unique_buckets(buckets@)", generated)
        self.assertIn("bucket_kernels::canonical_buckets_sort(input, 4096)", generated)
        self.assertIn("bucket_kernels::canonical_buckets_bitmap(input, 4096)", generated)

    def test_shortened_prefix_reaches_proof(self):
        changed = self.source.replace("let prior_read_len = tx.index_reads.len();", "let prior_read_len = 0;", 1)
        self.assertIn("let prior_read_len = 0 ;", generate.render(changed))

    def test_missing_frozen_prefix_rejected(self):
        changed = self.source.replace("let prior_read_len = tx.index_reads.len();", "", 1)
        with self.assertRaisesRegex(ValueError, "frozen prior"):
            generate.render(changed)

    def test_growing_search_rejected(self):
        changed = self.source.replace(".index_reads[..prior_read_len]", ".index_reads", 1)
        with self.assertRaises(ValueError):
            generate.render(changed)

    def test_hybrid_guard_mutation_reaches_prefix_bound_proof(self):
        changed = self.source.replace("if tx.index_reads.len() == prior_read_len {", "if true {", 1)
        generated = generate.render(changed)
        self.assertIn("let previous = if true", generated)
        self.assertIn("assert(tx.index_reads.len() == prior_read_len)", generated)

    def test_hybrid_selection_cannot_be_hoisted_outside_the_loop(self):
        original = "let prior_read_len = tx.index_reads.len();"
        changed = self.source.replace(original,
            original + " let full_search = tx.index_reads.len() == prior_read_len;", 1)
        changed = changed.replace("if tx.index_reads.len() == prior_read_len {", "if full_search {", 1)
        with self.assertRaisesRegex(ValueError, "unconditional dependency capture"):
            generate.render(changed)

    def test_selector_canonicalization_cannot_be_bypassed(self):
        source = generate.SELECTOR.read_text()
        mutations = [
            ("buckets.sort_unstable();", ""),
            ("buckets.dedup();", ""),
            ("let mut buckets = buckets;", "return Ok(buckets); let mut buckets = buckets;"),
            ("canonical_buckets_sort(&buckets, INDEX_TX_BUCKETS)", "Ok(buckets)"),
            ("canonical_buckets_bitmap(&buckets, INDEX_TX_BUCKETS)", "Ok(buckets)"),
            ('feature = "verified-buckets-sort"', 'feature = "unreviewed-selector"'),
        ]
        for old, new in mutations:
            with self.subTest(old=old), self.assertRaisesRegex(ValueError, "canonicalization boundary"):
                generate.render(self.source, selector=source.replace(old, new))

    def test_selector_domain_is_fixed_and_not_satisfied_by_a_comment(self):
        source = generate.SELECTOR.read_text()
        old = "const INDEX_TX_BUCKETS: usize = 4096;"
        changed = source.replace(old, "// " + old + "\nconst INDEX_TX_BUCKETS: usize = 4095;", 1)
        with self.assertRaisesRegex(ValueError, "domain changed"):
            generate.render(self.source, selector=changed)

    def test_commented_selector_cannot_supply_the_canonicalizer(self):
        source = generate.SELECTOR.read_text()
        start = source.index("    pub(crate) fn transactional_bucket_ids(")
        end = source.index("    pub(crate) fn transactional_try_lock_bucket(", start)
        old_method = source[start:end]
        changed = source[:start] + "/* " + old_method + " */\n" + source[end:]
        with self.assertRaisesRegex(ValueError, "missing or duplicated"):
            generate.render(self.source, selector=changed)
        renamed = old_method.replace("transactional_bucket_ids(", "transactional_bucket_ids (")
        renamed = renamed.replace("buckets.dedup();", "")
        changed = source[:start] + "/* " + old_method + " */\n" + renamed + source[end:]
        with self.assertRaisesRegex(ValueError, "canonicalization boundary"):
            generate.render(self.source, selector=changed)


if __name__ == "__main__":
    unittest.main()
