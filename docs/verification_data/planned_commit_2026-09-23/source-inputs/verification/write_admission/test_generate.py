import unittest
import generate
import run


class AdmissionAdapterTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source=generate.SOURCE.read_text()

    def changed(self,old,new):
        start=self.source.index('    fn has_write_base_conflict(')
        end=self.source.index('    fn has_row_lock_conflict(',start)
        self.assertEqual(self.source[start:end].count(old),1)
        return self.source[:start]+self.source[start:end].replace(old,new)+self.source[end:]

    def test_fresh_and_same_existing_data_component(self):
        self.assertEqual(generate.OUTPUT.read_text(),generate.render())
        self.assertIn(generate.component('commit_data').render(),generate.render())

    def test_condition_change_is_translated_not_repaired(self):
        rendered=generate.render_module(self.changed('current_head != expected_head','current_head == expected_head'))
        self.assertIn('current_head == expected_head',rendered)
        self.assertNotEqual(rendered,generate.render_module())

    def test_fallible_accesses_and_empty_base_are_preserved(self):
        rendered=generate.render_module()
        self.assertIn('driver . slot_ref ( write . row_id ) ?',rendered)
        self.assertIn('driver . resolve ( write . base_offset ) ?',rendered)
        self.assertIn('if expected_head != 0',rendered)

    def test_changed_atomic_order_is_rejected(self):
        source=self.changed('slot.head.load(Ordering::Acquire)','slot.head.load(Ordering::Relaxed)')
        with self.assertRaises(ValueError):generate.render_module(source)

    def test_skipped_load_is_rejected(self):
        with self.assertRaises(ValueError):
            generate.render_module(self.changed('slot.head.load(Ordering::Acquire)','0'))

    def test_empty_pointer_constant_is_checked(self):
        changed=self.source.replace('const EMPTY_PTR: u32 = 0;','const EMPTY_PTR: u32 = 1;',1)
        self.assertNotEqual(changed,self.source)
        with self.assertRaisesRegex(ValueError,'empty-pointer'):
            generate.render_module(changed)

    def test_new_native_operation_is_rejected(self):
        with self.assertRaisesRegex(ValueError,'unadapted'):
            generate.render_module(self.changed('Ok(false)','self.poison_indexes(); Ok(false)'))

    def test_proof_bypass_is_rejected(self):
        with self.assertRaisesRegex(ValueError,'proof bypass'):
            generate.render_module(self.changed('Ok(false)','assume(false); Ok(false)'))

    def test_changed_signature_is_rejected(self):
        with self.assertRaisesRegex(ValueError,'signature'):
            generate.render_module(self.changed('Result<bool, Error>','Result<(), Error>'))

    def test_semantic_mutations_have_unique_anchors(self):
        source=generate.render()
        for mutation in run.MUTATIONS:
            with self.subTest(mutation=mutation[0]):
                changed=run.mutation(source,mutation)
                self.assertNotEqual(source,changed)


if __name__=='__main__':unittest.main()
