import unittest
import generate


class InitializationAdapterTests(unittest.TestCase):
    def test_current_artifact(self):
        self.assertEqual(generate.OUTPUT.read_text(),generate.render())

    def test_complete_constructor_field_set_required(self):
        source=generate.SOURCE.read_text()
        with self.assertRaisesRegex(ValueError,'field set'):
            generate.render_module(source.replace('            recycle_next: AtomicU32::new(EMPTY_PTR),','',1))

    def test_unknown_initializer_unsafe_statement_rejected(self):
        source=generate.SOURCE.read_text().replace('std::ptr::write(row_mut, OccRow::new(value, xmin, next));',
            'std::ptr::write(row_mut, OccRow::new(value, xmin, next)); mutate_other_row();',1)
        with self.assertRaisesRegex(ValueError,'one ptr::write'): generate.render_module(source)

    def test_wrong_pointer_ordering_rejected(self):
        source=generate.SOURCE.read_text();a=source.index('    fn initialize_row(')
        source=source[:a]+source[a:].replace('Ordering::Acquire','Ordering::Relaxed',1)
        with self.assertRaises(ValueError): generate.render_module(source)

    def test_empty_pointer_constant_not_assumed(self):
        source=generate.SOURCE.read_text().replace('const EMPTY_PTR: u32 = 0;','const EMPTY_PTR: u32 = 1;',1)
        with self.assertRaisesRegex(ValueError,'constant'): generate.render_module(source)

    def test_all_native_mutations_reach_translated_program(self):
        expected=generate.render_module()
        for mutation in generate.MUTATIONS:
            with self.subTest(mutation=mutation[0]):
                self.assertNotEqual(generate.render_module(generate.mutation_source(mutation)),expected)


if __name__=='__main__': unittest.main()
