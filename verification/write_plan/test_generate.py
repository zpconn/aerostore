import unittest
import generate
import run

class PlanningAdapterTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):cls.source=generate.SOURCE.read_text()

    def test_generated_source_is_fresh(self):
        self.assertEqual(generate.OUTPUT.read_text(),generate.render())

    def test_native_final_loop_and_shared_modules_are_retained(self):
        generated=generate.render()
        self.assertIn(generate.data.render(),generated)
        self.assertIn('by_row . insert ( write . row_id , idx )',generated)
        self.assertIn('driver . resolve ( write . base_offset )',generated)
        self.assertIn('driver . resolve ( write . new_offset )',generated)

    def test_signature_change_fails_closed(self):
        changed=self.source.replace('fn final_write_indices(&self, tx: &OccTransaction<T>) -> Vec<usize>',
            'fn final_write_indices(&self, tx: &OccTransaction<T>, extra: bool) -> Vec<usize>',1)
        with self.assertRaisesRegex(ValueError,'signature'):generate.render_module(changed)

    def test_ordered_map_replacement_fails_closed(self):
        changed=self.source.replace('let mut by_row = BTreeMap::<usize, usize>::new();',
            'let mut by_row = HashMap::<usize, usize>::new();',1)
        with self.assertRaisesRegex(ValueError,'abstraction'):generate.render_module(changed)

    def test_native_wrong_insert_argument_is_preserved(self):
        changed=self.source.replace('by_row.insert(write.row_id, idx);','by_row.insert(write.row_id, 0);',1)
        self.assertIn('by_row . insert ( write . row_id , 0 )',generate.render_module(changed))

    def test_native_omitted_insert_is_preserved(self):
        changed=self.source.replace('by_row.insert(write.row_id, idx);','',1)
        self.assertNotIn('by_row . insert (',generate.render_module(changed))

    def test_wrong_pointer_projection_is_rejected(self):
        changed=self.source.replace('let before = &self.resolve_row_ptr(&write.base_ptr)?.value;',
            'let before = &self.resolve_row_ptr(&write.new_ptr)?.value;',1)
        with self.assertRaisesRegex(ValueError,'abstraction'):generate.render_module(changed)

    def test_wrong_key_projection_is_rejected(self):
        changed=self.source.replace('let before = (bound.key)(before);','let before = (bound.key)(after);',1)
        with self.assertRaisesRegex(ValueError,'abstraction'):generate.render_module(changed)

    def test_wrong_return_row_is_preserved(self):
        start=self.source.index('    fn index_changes(');end=self.source.index('    fn index_lock_keys(',start)
        part=self.source[start:end].replace('row_id: write.row_id,','row_id: 0,',1)
        self.assertIn('row_id : 0',generate.render_module(self.source[:start]+part+self.source[end:]))

    def test_proof_bypass_is_rejected(self):
        changed=self.source.replace('by_row.insert(write.row_id, idx);','assume(false); by_row.insert(write.row_id, idx);',1)
        with self.assertRaisesRegex(ValueError,'bypass'):generate.render_module(changed)

    def test_unknown_receiver_call_is_rejected(self):
        changed=self.source.replace('by_row.insert(write.row_id, idx);','self.poison_indexes(); by_row.insert(write.row_id, idx);',1)
        with self.assertRaisesRegex(ValueError,'receiver'):generate.render_module(changed)

    def test_semantic_controls_have_exact_single_anchors(self):
        source=generate.render()
        for item in run.MUTATIONS:
            with self.subTest(item=item[0]):self.assertNotEqual(run.mutation(source,item),source)

    def test_module_roots_are_scoped(self):
        self.assertTrue(all(root.startswith('planning::') for root in run.ROOTS))
        self.assertEqual(len(run.ROOTS),len(set(run.ROOTS)))
        self.assertTrue(all(item[1] in run.ROOTS for item in run.MUTATIONS))

if __name__=='__main__':unittest.main()
