#!/usr/bin/env python3
import unittest
import generate


class DetachAdapterTests(unittest.TestCase):
    def setUp(self):
        self.source = generate.SOURCE.read_text()

    def test_fresh_generated_source(self):
        self.assertEqual(generate.OUTPUT.read_text(), generate.render(self.source))

    def test_marking_boundary_fails_closed(self):
        changed = self.source.replace("let height = node.height as usize;", "let height = 1;")
        with self.assertRaisesRegex(ValueError, "marking/height"):
            generate.render(changed)

    def test_changed_signature_fails_closed(self):
        begin = self.source.index("    fn unlink_node(")
        changed = self.source[:begin] + self.source[begin:].replace("node_offset: u32", "node_offset: u64", 1)
        with self.assertRaisesRegex(ValueError, "signature"):
            generate.render(changed)

    def test_changed_cas_ordering_fails_closed(self):
        begin = self.source.index("        let mut detached_all = true;", self.source.index("    fn unlink_node("))
        changed = self.source[:begin] + self.source[begin:].replace("AtomicOrdering::AcqRel", "AtomicOrdering::Relaxed", 1)
        with self.assertRaises(ValueError):
            generate.render(changed)

    def test_successor_write_cannot_disappear(self):
        before = "        self.retire_node(node_offset);"
        changed = self.source.replace(before, "        overwrite_target_successors();\n" + before, 1)
        with self.assertRaisesRegex(ValueError, "retirement boundary"):
            generate.render(changed)

    def test_changed_find_order_fails_closed(self):
        changed = self.source.replace("let _ = self.find(key, preds, succs)?;", "detached_all = true; let _ = self.find(key, preds, succs)?;", 1)
        with self.assertRaisesRegex(ValueError, "refresh"):
            generate.render(changed)

    def test_upper_lane_mutation_reaches_proof(self):
        begin = self.source.index("        let mut detached_all = true;", self.source.index("    fn unlink_node("))
        changed = self.source[:begin] + self.source[begin:].replace("for level in (0..height).rev()", "for level in (0..1).rev()", 1)
        self.assertIn("let mut level = 1;", generate.render(changed))

    def test_unknown_scan_operation_is_not_discarded(self):
        changed = self.source.replace("if succs[level] != node_offset {", "unknown_operation(); if succs[level] != node_offset {", 1)
        self.assertIn("unknown_operation ( )", generate.render(changed))


if __name__ == "__main__":
    unittest.main()
