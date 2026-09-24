import importlib.util
import unittest
from pathlib import Path
import generate

spec=importlib.util.spec_from_file_location('commit_segment_test',Path(__file__).with_name('commit_segment.py'))
segment=importlib.util.module_from_spec(spec);spec.loader.exec_module(segment)

class CommitDataAdapterTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):cls.source=generate.SOURCE.read_text()

    def test_current_generated_composition_is_fresh(self):
        self.assertEqual(generate.OUTPUT.read_text(),generate.render())

    def test_existing_native_modules_are_embedded_without_rewriting(self):
        generated=generate.render()
        self.assertIn(generate.component('postings').render(self.source),generated)
        self.assertIn(generate.component('row_publication').render_module(self.source),generated)
        self.assertEqual(generated.count('pub struct Image {'),1)

    def test_commit_generic_policy_binding_is_checked(self):
        with self.assertRaisesRegex(ValueError,'signature'):
            segment.render(self.source.replace('const WRITE_AHEAD: bool','const OTHER_FLAG: bool',1))

    def test_ordinary_policy_binding_is_checked(self):
        with self.assertRaisesRegex(ValueError,'policy'):
            segment.render(self.source.replace('commit_with_record_impl::<false, Error, _, _>',
                'commit_with_record_impl::<true, Error, _, _>',1))

    def test_omitted_prefix_effects_cannot_silently_change(self):
        with self.assertRaisesRegex(ValueError,'context'):
            segment.render(self.source.replace('self.ensure_open(tx)?;\n        let final_write_indices',
                'self.ensure_open(tx)?; self.poison_indexes();\n        let final_write_indices',1))

    def test_omitted_suffix_effects_cannot_silently_change(self):
        with self.assertRaisesRegex(ValueError,'context'):
            start=self.source.index('    fn commit_with_record_impl<')
            end=self.source.index('    fn prepare_before_publish<',start)
            body=self.source[start:end].replace('let _ = self.shm.flush_local_recycle_caches();',
                'self.poison_indexes(); let _ = self.shm.flush_local_recycle_caches();',1)
            segment.render(self.source[:start]+body+self.source[end:])

    def test_failure_cleanup_projection_is_exact(self):
        needle='tx.write_set.clear();\n                tx.read_set.clear();\n                tx.index_reads.clear();'
        self.assertEqual(self.source.count(needle),1)
        with self.assertRaisesRegex(ValueError,'abstraction'):
            segment.render(self.source.replace(needle,'tx.savepoints.clear();',1))

    def test_actual_prepare_remove_order_is_preserved(self):
        first='let inserted = self.prepare_index_destinations(&index_changes)?;'
        second='self.remove_index_sources(&index_changes)?;'
        changed=self.source.replace(first,'TEMPORARY_ORDER_MARKER',1).replace(second,first,1).replace('TEMPORARY_ORDER_MARKER',second,1)
        lowered=segment.render(changed)
        self.assertLess(lowered.index('remove_sources'),lowered.index('prepare_destinations'))

    def test_ordinary_wrong_pointer_load_is_rejected(self):
        begin=self.source.index('    fn publish_write_set(');end=self.source.index('    fn has_write_base_conflict(',begin)
        body=self.source[begin:end].replace('write.base_ptr.load(Ordering::Acquire)','write.new_ptr.load(Ordering::Acquire)',1)
        with self.assertRaisesRegex(ValueError,'abstraction'):
            generate.ordinary.render(self.source[:begin]+body+self.source[end:])

    def test_ordinary_return_provenance_is_not_canonicalized(self):
        begin=self.source.index('    fn publish_write_set(');end=self.source.index('    fn has_write_base_conflict(',begin)
        body=self.source[begin:end].replace('row_id: write.row_id,','row_id: 0,',1)
        self.assertIn('row_id : 0',generate.ordinary.render(self.source[:begin]+body+self.source[end:]))

    def test_no_native_proof_bypass(self):
        changed=self.source.replace('let inserted = self.prepare_index_destinations(&index_changes)?;',
            'assume(false); let inserted = self.prepare_index_destinations(&index_changes)?;',1)
        with self.assertRaisesRegex(ValueError,'bypass'):
            segment.render(changed)

if __name__=='__main__':unittest.main()
