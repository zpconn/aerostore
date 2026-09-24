import unittest
import generate
import run


class CompletionAdapterTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source = generate.SOURCE.read_text()

    def test_generated_join_is_fresh(self):
        self.assertEqual(generate.OUTPUT.read_text(), generate.render())

    def test_exact_components_are_embedded(self):
        rendered = generate.render()
        self.assertIn(generate.component("commit_data").render(), rendered)
        scenario = generate.component("lifecycle_scenario").render()
        for name in ("lifecycle", "predicate", "capture"):
            scenario = scenario.replace("mod " + name + " {", "pub mod " + name + " {", 1)
        self.assertIn(scenario, rendered)

    def test_registration_take_is_preserved_not_repaired(self):
        changed = self.source.replace("tx.registration.take()", "tx.registration", 1)
        self.assertNotEqual(generate.finish_body(changed), generate.finish_body(self.source))
        self.assertNotIn(". take ( )", generate.finish_body(changed))

    def test_native_finish_new_operation_is_rejected(self):
        changed = self.source.replace("self.shm.end_transaction(registration)?;",
                                      "self.poison_indexes(); self.shm.end_transaction(registration)?;", 1)
        with self.assertRaisesRegex(ValueError, "unsupported"):
            generate.finish_body(changed)

    def test_native_finish_proof_bypass_is_rejected(self):
        changed = self.source.replace("self.shm.end_transaction(registration)?;",
                                      "assume(false); self.shm.end_transaction(registration)?;", 1)
        with self.assertRaisesRegex(ValueError, "unsupported"):
            generate.finish_body(changed)

    def test_native_completion_poison_is_required(self):
        start = self.source.index("let finish = match self.finish_transaction(tx)")
        changed = self.source[:start] + self.source[start:].replace("self.poison_indexes();", "", 1)
        with self.assertRaises(ValueError):
            generate.check_completion_order(changed)

    def test_native_guard_release_cannot_move_before_stamp(self):
        start = self.source.index("let finish = match self.finish_transaction(tx)")
        suffix = self.source[start:].replace("drop(index_locks);", "", 1)
        changed = self.source[:start] + "drop(index_locks);\n" + suffix
        with self.assertRaises(ValueError):
            generate.check_completion_order(changed)

    def test_join_order_is_checked_beyond_state_postconditions(self):
        source = generate.TEMPLATE.read_text()
        first = "native_ordinary_data_segment(storage,plan,ordinary_plan)"
        last = "predicate::publish_index_stamps::<scenario::Bridge<L,I>,C>(publisher,&changes)"
        changed = source.replace(first, "ORDER_SENTINEL", 1).replace(last, first, 1).replace("ORDER_SENTINEL", last, 1)
        with self.assertRaisesRegex(ValueError, "join order"):
            generate.render(template=changed)

    def test_missing_join_and_marker_fail_closed(self):
        source = generate.TEMPLATE.read_text()
        with self.assertRaisesRegex(ValueError, "join operation"):
            generate.render(template=source.replace("finish_transaction(&mut publisher.lifecycle,token)", "Ok(())", 1))
        with self.assertRaisesRegex(ValueError, "marker"):
            generate.render(template=source.replace("/* NATIVE_FINISH */", "", 1))

    def test_all_semantic_mutations_change_exactly_the_selected_artifact(self):
        source = generate.render()
        for mutation in run.MUTATIONS:
            with self.subTest(mutation=mutation[0]):
                changed = run.mutation(source, mutation)
                self.assertNotEqual(source, changed)
                self.assertEqual(changed.count("pub fn publish_then_complete"), 1)


if __name__ == "__main__":
    unittest.main()
