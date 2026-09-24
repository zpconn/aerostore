#!/usr/bin/env python3
import unittest
import generate
import run


class AdapterTests(unittest.TestCase):
    def test_current_output_is_fresh(self):
        self.assertEqual(generate.OUTPUT.read_text(), generate.render())

    def test_all_semantic_mutations_preserve_altered_expression(self):
        for mutation in run.MUTATIONS:
            with self.subTest(name=mutation[0]):
                result = run.mutation_artifact(mutation)
                self.assertNotEqual(result, generate.render())
                self.assertIn(mutation[2], run.ROOTS)

    def test_native_guard_lifetime_change_is_rejected(self):
        source = generate.SOURCE.read_text().replace("mutex: &'a ShmMutex,", "mutex: &'static ShmMutex,")
        with self.assertRaisesRegex(ValueError, "representation/lifetime"):
            generate.render(source)

    def test_public_native_guard_fields_are_rejected(self):
        source = generate.SOURCE.read_text().replace("    mutex: &'a ShmMutex,", "    pub mutex: &'a ShmMutex,")
        with self.assertRaisesRegex(ValueError, "representation/lifetime"):
            generate.render(source)

    def test_extra_native_constructor_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "constructor"):
            generate.render(generate.SOURCE.read_text() + "\n// ShmMutexGuard {\n")

    def test_guard_clone_and_copy_are_rejected(self):
        for trait in ("Clone", "Copy"):
            with self.assertRaisesRegex(ValueError, "duplicated"):
                generate.render(generate.SOURCE.read_text() + "\nimpl " + trait + " for ShmMutexGuard<'_> {}\n")

    def test_cas_receiver_change_is_rejected(self):
        source = generate.SOURCE.read_text().replace("self.state\n            .compare_exchange", "self.priority_waiters\n            .compare_exchange")
        with self.assertRaisesRegex(ValueError, "CAS receiver"):
            generate.render(source)

    def test_removing_explicit_drop_requires_raii_lowering_review(self):
        # Removing this call is not a semantic counterexample: implicit Rust Drop
        # would still run at return. The restricted adapter refuses that new form.
        with self.assertRaises(ValueError):
            generate.render(generate.SOURCE.read_text().replace("            drop(guard);\n", ""))

    def test_source_bound_interference_cuts_are_retained(self):
        result = generate.render()
        self.assertIn("environment(driver, Ghost(protected)", result)
        self.assertIn("Ghost ( with_local )", result)
        self.assertEqual(result.count("environment_for_borrows ( driver , held , Tracked ( & mut * authority ) )"), 2)
        self.assertIn("let ghost local_serial", result.replace("let ghost local_serial", "let ghost local_serial"))

    def test_wrappers_do_not_freeze_foreign_cells(self):
        contracts = generate.CONTRACTS.read_text()
        for method in ("try_lock", "transactional_try_lock_bucket", "acquire_index_bucket"):
            start = contracts.index("pub fn " + method + "<")
            stop = contracts.index("\n{", start)
            selected = contracts[start:stop]
            self.assertNotIn("cells(final(authority)) == cells(old(authority))", selected)
            self.assertIn("leases_authorized", selected)

    def test_physical_domain_is_required_without_conjuring_membership(self):
        contracts = generate.CONTRACTS.read_text()
        for method in ("transactional_try_lock_bucket", "acquire_index_bucket"):
            start = contracts.index("pub fn " + method + "<")
            stop = contracts.index("\n{", start)
            self.assertIn("physical_domain(driver, old(authority))", contracts[start:stop])
        self.assertIn("requires bucket_request_valid", contracts)
        for root in ("bucket_reply_has_witness", "physical_domain_has_live_witness", "domain_survives_interference"):
            self.assertIn(root, run.ROOTS)

    def test_type_controls_embed_current_checked_native_module(self):
        for name in run.TYPE_MUTATIONS:
            self.assertIn(generate.render(), run.type_artifact(name))

    def test_input_paths_are_unique_and_complete(self):
        paths = run.inputs()
        self.assertEqual(len(paths), len(set(paths)))
        for source in (generate.SOURCE, generate.INDEX, generate.OCC):
            self.assertIn(source, paths)


if __name__ == "__main__":
    unittest.main()
