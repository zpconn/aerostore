#!/usr/bin/env python3
import unittest
import generate
from run import mutate, MUTATIONS


class PostingAdapterTests(unittest.TestCase):
    def setUp(self):
        self.source = generate.SOURCE.read_text()

    def test_generated_source_is_fresh(self):
        self.assertEqual(generate.OUTPUT.read_text(), generate.render(self.source))

    def test_selected_signature_fails_closed(self):
        old = "fn prepare_index_destinations(&self, changes: &[IndexChange])"
        changed = self.source.replace(old, "fn prepare_index_destinations(&self, changes: &mut [IndexChange])", 1)
        with self.assertRaisesRegex(ValueError, "signature"):
            generate.render(changed)

    def test_unreviewed_test_hook_is_rejected(self):
        changed = self.source.replace("hook(inserted.len());", "hook(0);", 1)
        with self.assertRaisesRegex(ValueError, "test-hook"):
            generate.render(changed)

    def test_proof_bypass_is_rejected(self):
        changed = mutate(self.source, "prepare_index_destinations", "let mut inserted = Vec::new();", "assume(false); let mut inserted = Vec::new();")
        with self.assertRaisesRegex(ValueError, "bypass"):
            generate.render(changed)

    def test_unknown_native_operation_remains_visible(self):
        changed = mutate(self.source, "remove_index_sources", "for change in changes {", "unknown_operation(); for change in changes {")
        self.assertIn("unknown_operation ( )", generate.render(changed))

    def test_each_semantic_mutant_reaches_generated_code(self):
        original = generate.render(self.source)
        for name, method, old, new in MUTATIONS:
            with self.subTest(mutation=name):
                self.assertNotEqual(original, generate.render(mutate(self.source, method, old, new)))

    def test_changed_row_argument_is_not_repaired_by_adapter(self):
        changed = mutate(self.source, "prepare_index_destinations", "after.clone(), change.row_id", "after.clone(), 0")
        generated = generate.render(changed)
        self.assertIn("transactional_insert ( index , after . clone ( ) , 0 )", generated)


if __name__ == "__main__":
    unittest.main()
