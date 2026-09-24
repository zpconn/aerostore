#!/usr/bin/env python3
"""Adapter tests; semantic mutations are checked by the actual verifier in run.py."""
import unittest
import generate


class AdapterTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source = generate.SOURCE.read_text()

    def test_current_source_renders(self):
        result = generate.render(self.source)
        self.assertIn("pub fn commit_with_record_impl", result)
        self.assertNotIn("external_body", result)

    def test_added_unknown_operation_is_not_silently_removed(self):
        changed = self.source.replace("self.ensure_open(tx)?;", "self.ensure_open(tx)?; self.unmodeled_write();")
        self.assertIn("unmodeled_write", generate.render(changed))

    def test_new_configuration_cannot_hide_code(self):
        marker = "    fn commit_with_record_impl<"
        offset = self.source.index(marker)
        changed = self.source[:offset] + self.source[offset:].replace("self.ensure_open(tx)?;", "#[cfg(feature = \"unchecked\")] self.unmodeled_write(); self.ensure_open(tx)?;", 1)
        with self.assertRaises(ValueError):
            generate.render(changed)

    def test_durable_wrapper_cannot_select_ordinary_branch(self):
        changed = self.source.replace("self.commit_with_record_impl::<true, E, P, F>", "self.commit_with_record_impl::<false, E, P, F>")
        with self.assertRaisesRegex(ValueError, "policy binding"):
            generate.render(changed)

    def test_legacy_callback_wrapper_preserves_preparation_boundary(self):
        changed = self.source.replace(
            "self.commit_with_record_prepared::<E, _, F>(tx, |_| Ok(before_publish))",
            "self.commit_with_record_prepared::<E, _, F>(tx, |record| { before_publish(record)?; Ok(before_publish) })",
        )
        with self.assertRaisesRegex(ValueError, "policy binding"):
            generate.render(changed)

    def test_nested_comments_do_not_hide_statements(self):
        tokens = generate.tokenize("x(); /* a /* b */ c */ y();")
        self.assertEqual(tokens, ["x", "(", ")", ";", "y", "(", ")", ";"])

    def test_unclosed_comments_fail_closed(self):
        with self.assertRaises(ValueError):
            generate.tokenize("x(); /* never closed")


if __name__ == "__main__":
    unittest.main()
