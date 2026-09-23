#!/usr/bin/env python3
import unittest
import generate


class CompositionAdapterTests(unittest.TestCase):
    def test_fresh_artifact(self):
        self.assertEqual(generate.OUTPUT.read_text(), generate.render())

    def test_exact_checked_modules_embedded(self):
        result = generate.render()
        for name in ("predicate_capture", "predicate"):
            module = generate.component(name)
            self.assertEqual(result.count(module.OUTPUT.read_text()), 1)

    def test_actual_functions_called(self):
        template = generate.TEMPLATE.read_text()
        self.assertIn("capture::capture_dependencies(index, tx, buckets)", template)
        self.assertIn("predicate::index_read_conflict(later, &validated)", template)
        self.assertNotIn("assume(", template)
        self.assertNotIn("external_body", template)


if __name__ == "__main__":
    unittest.main()
