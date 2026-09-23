#!/usr/bin/env python3
"""Fail-closed boundaries of the narrow production token diagnostic."""
import unittest

import check_production_equivalence as diagnostic


PROC_BASE = """
#[cfg(test)] thread_local! {
    static OTHER_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        std::cell::RefCell::new(None);
}
impl ProcArray {
    fn create_transaction_snapshot(&self) {
        let guard = self.lifecycle.lock();
        self.snapshot_locked();
    }
}
#[cfg(test)] mod tests { fn old_test() {} }
"""


def with_hook(source=PROC_BASE):
    return source.replace("thread_local! {", "thread_local! {\n" + diagnostic.HOOK_DECLARATION).replace(
        "let guard = self.lifecycle.lock();", diagnostic.HOOK_CALL + "\nlet guard = self.lifecycle.lock();")


class ProductionEquivalenceTests(unittest.TestCase):
    def project(self, text, name=diagnostic.PROC):
        return diagnostic.production_tokens(name, text)[0]

    def test_only_exact_reviewed_hook_and_test_module_disappear(self):
        edited = with_hook().replace("fn old_test() {}", "fn new_test() { assert!(true); }")
        self.assertEqual(self.project(PROC_BASE), self.project(edited))

    def test_executable_change_is_retained(self):
        edited = with_hook().replace("self.snapshot_locked()", "self.stale_snapshot()")
        self.assertNotEqual(self.project(PROC_BASE), self.project(edited))

    def test_unreviewed_test_modules_are_retained(self):
        source = "#[cfg(test)] mod transactional_publication_tests { fn original() {} }"
        name = "aerostore_core/src/occ_partitioned.rs"
        self.assertNotEqual(self.project(source, name), self.project(source.replace("original", "modified"), name))

    def test_missing_test_attribute_keeps_module(self):
        source = "#[cfg(test)] mod predicate_completion_tests { fn example() {} }"
        name = "aerostore_core/src/occ_partitioned.rs"
        self.assertNotEqual(self.project(source, name), self.project(source.replace("#[cfg(test)]", ""), name))

    def test_hook_without_test_attribute_rejected(self):
        source = with_hook().replace(diagnostic.HOOK_CALL, diagnostic.HOOK_CALL.replace("#[cfg(test)]", ""))
        with self.assertRaisesRegex(ValueError, "declaration/call mismatch"):
            self.project(source)

    def test_hook_relocated_after_acquisition_rejected(self):
        source = with_hook().replace(diagnostic.HOOK_CALL, "").replace(
            "let guard = self.lifecycle.lock();", "let guard = self.lifecycle.lock();" + diagnostic.HOOK_CALL)
        with self.assertRaisesRegex(ValueError, "pre-acquisition position"):
            self.project(source)

    def test_hook_declaration_outside_test_block_rejected(self):
        source = with_hook().replace(diagnostic.HOOK_DECLARATION, "") + diagnostic.HOOK_DECLARATION
        with self.assertRaisesRegex(ValueError, "uniquely test-only"):
            self.project(source)

    def test_literals_and_joint_operators_preserved(self):
        self.assertNotEqual(diagnostic.rust_tokens("x <= y"), diagnostic.rust_tokens("x < = y"))
        self.assertNotEqual(diagnostic.rust_tokens('"a /* literal */"'), diagnostic.rust_tokens('"a"'))
        self.assertEqual(diagnostic.rust_tokens("x/* outer /* inner */ */ + y"), ["x", "+", "y"])
        self.assertEqual(diagnostic.rust_tokens('r##"/* raw */"##'), ['r##"/* raw */"##'])


if __name__ == "__main__":
    unittest.main()
