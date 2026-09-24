import unittest
import run


class P1NativeCampaignTests(unittest.TestCase):
    def test_all_controls_mutate_only_native_implementation(self):
        original = (run.ROOT / run.OCC).read_text()
        controls = list(run.variants(original))
        self.assertEqual(len(controls), 7)
        self.assertEqual(len({control[0] for control in controls}), 7)
        test_marker = "#[cfg(test)]\nmod tests"
        # Every mutant must preserve the complete native test suffix.
        suffix = original[original.index(test_marker):]
        for name, path, changed, selection, assertion in controls:
            with self.subTest(name=name):
                self.assertEqual(path, run.OCC)
                self.assertNotEqual(changed, original)
                self.assertEqual(changed[changed.index(test_marker):], suffix)
                self.assertIn(selection[0], ("--test", "--lib"))
                self.assertTrue(assertion)

    def test_selected_cases_and_all_native_inputs_are_present(self):
        self.assertEqual(len(run.TESTS), 9)
        self.assertEqual(len({name for name, _ in run.TESTS}), 9)
        for _, selection in run.TESTS:
            self.assertEqual(len(selection), 3 if selection[0] == "--test" else 2)
        paths = run.native_paths()
        self.assertEqual(len(paths), len(set(paths)))
        for needed in (run.OCC, run.INTEGRATION, "aerostore_core/src/wal_writer.rs", "Cargo.lock"):
            self.assertIn(run.ROOT / needed, paths)

    def test_mutation_anchor_must_be_unique(self):
        for source in ("start absent end", "start x x end"):
            with self.assertRaises(RuntimeError):
                run.scoped(source, "start", "end", "x", "changed")

    def test_missing_tests_are_not_positive_evidence(self):
        self.assertFalse(run.outcome_passes(0, "test result: ok. 0 passed; 0 failed"))
        self.assertFalse(run.outcome_passes(0, "test result: ok. 2 passed; 0 failed"))
        self.assertTrue(run.outcome_passes(0, "test result: ok. 1 passed; 0 failed"))

    def test_wrong_kind_failures_are_not_negative_evidence(self):
        valid = "panicked at test.rs: required assertion\ntest result: FAILED. 0 passed; 1 failed"
        self.assertTrue(run.outcome_passes(101, valid, True, "required assertion"))
        self.assertFalse(run.outcome_passes(101, valid, True, "different assertion"))
        self.assertFalse(run.outcome_passes(-9, valid, True, "required assertion"))
        self.assertFalse(run.outcome_passes(101, "error[E0001]\n" + valid, True, "required assertion"))
        self.assertFalse(run.outcome_passes(101, "required assertion\ntest result: FAILED. 0 passed; 1 failed", True, "required assertion"))


if __name__ == "__main__":
    unittest.main()
