#!/usr/bin/env python3
import unittest
import generate
import run


class RetentionAdapterTests(unittest.TestCase):
    def setUp(self):
        self.source=generate.SOURCE.read_text()

    def test_current_artifact_and_shared_lookup_are_exact(self):
        self.assertEqual(generate.OUTPUT.read_text(),generate.render())
        self.assertIn(generate.lookup.OUTPUT.read_text(),generate.render())

    def test_module_has_only_shared_lookup_types(self):
        module=generate.render_module()
        self.assertTrue(module.startswith('use crate::lookup;'))
        self.assertNotIn('pub struct Image',module)
        self.assertNotIn('pub struct Row ',module)

    def test_missing_partition_acquisition_rejected(self):
        start=self.source.index('pub(crate) fn vacuum_reclaim_before(')
        changed=self.source[:start]+self.source[start:].replace('let _lock = self.acquire_row_lock(row_id);','',1)
        with self.assertRaisesRegex(ValueError,'partition acquisition'):
            generate.render_module(changed)

    def test_changed_outer_scan_rejected(self):
        start=self.source.index('pub(crate) fn vacuum_reclaim_before(')
        changed=self.source[:start]+self.source[start:].replace('for row_id in 0..self.capacity()','for row_id in 1..self.capacity()',1)
        with self.assertRaisesRegex(ValueError,'outer scan'):
            generate.render_module(changed)

    def test_weakened_store_order_is_not_erased(self):
        changed=self.source.replace('prev_row.next.store(next_offset, Ordering::Release);','prev_row.next.store(next_offset, Ordering::Relaxed);',1)
        with self.assertRaises(ValueError): generate.render_module(changed)

    def test_native_threshold_mutation_survives_adaptation(self):
        changed=self.source.replace('xmax < global_xmin && !curr_row.is_locked','xmax <= global_xmin && !curr_row.is_locked',1)
        self.assertIn('xmax <= global_xmin',generate.render_module(changed))

    def test_unreviewed_extra_native_side_effect_rejected(self):
        changed=self.source.replace('let live_head_value = head_row.value;','self.unreviewed_mutation(); let live_head_value = head_row.value;',1)
        with self.assertRaisesRegex(ValueError,'unadapted native'):
            generate.render_module(changed)


    def test_unchecked_kernel_cannot_be_public(self):
        with self.assertRaisesRegex(ValueError,'crate-private'):
            generate.render_module(self.source.replace('pub(crate) fn vacuum_reclaim_before(', 'pub fn vacuum_reclaim_before(',1))

    def test_public_clamp_and_same_arena_are_source_bound(self):
        for old,new in [('requested_xmin.min(retained_xmin)','requested_xmin.max(retained_xmin)'),
                        ('crate::vacuum::compute_global_xmin(self.shm.as_ref())','crate::vacuum::compute_global_xmin(other.as_ref())'),
                        ('self.vacuum_reclaim_before(requested_xmin.min(retained_xmin))','self.vacuum_reclaim_once(requested_xmin)')]:
            with self.subTest(old=old),self.assertRaisesRegex(ValueError,'public vacuum dispatch'):
                generate.render_module(self.source.replace(old,new,1))

    def test_collector_uses_one_scan_and_internal_kernel(self):
        vacuum=generate.VACUUM_SOURCE.read_text()
        for old,new in [('.vacuum_reclaim_before(global_xmin)','.vacuum_reclaim_once(global_xmin)'),
            ('let global_xmin = compute_global_xmin(table.shared_arena().as_ref());',
             'let global_xmin = compute_global_xmin(table.shared_arena().as_ref()); compute_global_xmin(table.shared_arena().as_ref());'),
            ('compute_global_xmin(table.shared_arena().as_ref())','compute_global_xmin(other.shared_arena().as_ref())')]:
            with self.subTest(old=old),self.assertRaisesRegex(ValueError,'collector dispatch'):
                generate.render_horizon(self.source,vacuum.replace(old,new,1))

    def test_table_accessor_cannot_select_another_arena(self):
        start=self.source.index('pub fn shared_arena(')
        changed=self.source[:start]+self.source[start:].replace('&self.shm','&self.other_shm',1)
        with self.assertRaisesRegex(ValueError,'arena accessor'):
            generate.render_module(changed)

    def test_compute_global_xmin_uses_actual_retention_scan(self):
        with self.assertRaisesRegex(ValueError,'same-arena horizon'):
            generate.render_horizon(self.source,generate.VACUUM_SOURCE.read_text().replace(
                'shm.proc_array().oldest_snapshot_xmin(shm.global_txid())', 'shm.proc_array().oldest_snapshot_xmin(other.global_txid())',1))

    def test_semantic_controls_modify_exactly_the_selected_native_body(self):
        source=generate.render()
        for item in run.MUTATIONS:
            with self.subTest(name=item[0]):
                changed=run.mutation(source,item)
                self.assertNotEqual(source,changed)
                self.assertIn(generate.lookup.OUTPUT.read_text(),changed)


if __name__=='__main__': unittest.main()
