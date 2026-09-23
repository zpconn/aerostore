import unittest
import generate
import run


class LookupAdapterTests(unittest.TestCase):
    def test_current_artifact(self):
        self.assertEqual(generate.render(),generate.OUTPUT.read_text())

    def test_unknown_signature_rejected(self):
        source=generate.SOURCE.read_text().replace('row_id: usize) -> Result<Option<T>, Error>',
            'row_id: u64) -> Result<Option<T>, Error>',1)
        with self.assertRaises(ValueError): generate.render(source)

    def test_changed_atomic_ordering_rejected(self):
        source=generate.SOURCE.read_text()
        begin=source.index('    fn is_visible(')
        source=source[:begin]+source[begin:].replace('Ordering::Acquire','Ordering::Relaxed',1)
        with self.assertRaises(ValueError): generate.render(source)

    def test_extra_native_code_not_discarded(self):
        for extra in ('assume(true);','unsafe {}','#[cfg(test)] let omit=0;'):
            source=generate.SOURCE.read_text().replace('if row.xmin == tx.txid {',extra+'if row.xmin == tx.txid {',1)
            with self.subTest(extra=extra),self.assertRaises(ValueError): generate.render(source)

    def test_duplicate_native_function_rejected(self):
        with self.assertRaises(ValueError): generate.render(generate.SOURCE.read_text()+'\nfn is_visible() {}')

    def test_chain_bound_change_rejected(self):
        with self.assertRaises(ValueError): generate.render(generate.SOURCE.read_text().replace('262_144','262_145',1))

    def test_guard_release_order_rejected(self):
        source=generate.SOURCE.read_text().replace('let candidates = index.transactional_raw_lookup(predicate)?;','drop(guards);\nlet candidates = index.transactional_raw_lookup(predicate)?;',1)
        with self.assertRaises(ValueError): generate.render(source)

    def test_commented_original_cannot_mask_changed_constant(self):
        source = generate.SOURCE.read_text()
        for original, changed in [('262_144', '262_145'), ('const EMPTY_PTR: u32 = 0;', 'const EMPTY_PTR: u32 = 1;')]:
            modified = source.replace(original, changed, 1)
            if original == '262_144':
                modified = '// const MAX_VISIBLE_CHAIN_STEPS: usize = 262_144;\n' + modified
            else:
                modified = '// ' + original + '\n' + modified
            with self.subTest(original=original), self.assertRaises(ValueError):
                generate.render(modified)

    def test_mutation_targets_exist_and_change_artifact(self):
        source=generate.render()
        for item in run.MUTATIONS:
            with self.subTest(name=item[0]): self.assertNotEqual(source,run.mutation(source,item))


if __name__=='__main__': unittest.main()
