import unittest
import generate
import run


class PlannedCommitAdapterTests(unittest.TestCase):
    def test_current_artifact_matches_live_source(self):
        self.assertEqual(generate.OUTPUT.read_text(), generate.render())

    def test_actual_components_are_embedded_without_rewriting(self):
        source = generate.SOURCE.read_text()
        rendered = generate.render()
        self.assertIn(generate.component("commit_completion").render(), rendered)
        for name in ("write_plan", "write_admission"):
            self.assertIn(generate.component(name).render_module(source), rendered)
        self.assertEqual(rendered.count("pub struct Image {"), 1)
        self.assertEqual(rendered.count("pub struct Storage<"), 1)

    def test_planning_cannot_move_after_validation(self):
        template = generate.TEMPLATE.read_text()
        first = "planning::index_changes(&storage.rows,&tx,&indices)"
        last = "admission::has_write_base_conflict(&storage.rows,&tx,&indices)"
        changed = template.replace(first, "ORDER_SENTINEL", 1).replace(last, first, 1).replace("ORDER_SENTINEL", last, 1)
        with self.assertRaisesRegex(ValueError, "order changed"):
            generate.render(changed)

    def test_validation_cannot_move_after_publication(self):
        template = generate.TEMPLATE.read_text()
        first = "admission::has_write_base_conflict(&storage.rows,&tx,&indices)"
        last = "publish_then_complete::<P,R,L,I,C>(storage,publisher,token,&plan,&ordinary_plan)"
        changed = template.replace(first, "ORDER_SENTINEL", 1).replace(last, first, 1).replace("ORDER_SENTINEL", last, 1)
        with self.assertRaisesRegex(ValueError, "order changed"):
            generate.render(changed)

    def test_missing_or_duplicate_native_call_is_rejected(self):
        template = generate.TEMPLATE.read_text()
        call = "planning::final_write_indices::<M>(&tx)"
        for changed in (template.replace(call, "Vec::new()", 1), template + call):
            with self.subTest(changed=changed[-20:]), self.assertRaisesRegex(ValueError, "missing/duplicate"):
                generate.render(changed)

    def test_all_controls_modify_the_selected_source(self):
        source = generate.render()
        for item in run.MUTATIONS:
            with self.subTest(name=item[0]):
                self.assertNotEqual(run.mutation(source, item), source)

    def test_comments_and_strings_cannot_supply_required_calls(self):
        template = generate.TEMPLATE.read_text()
        call = "planning::final_write_indices::<M>(&tx)"
        for decoy in ("/* " + call + " */", "// " + call + "\n", '"' + call + '"'):
            with self.subTest(decoy=decoy), self.assertRaisesRegex(ValueError, "missing/duplicate"):
                generate.render(template.replace(call, decoy + " Vec::<usize>::new()", 1))


if __name__ == "__main__":
    unittest.main()
