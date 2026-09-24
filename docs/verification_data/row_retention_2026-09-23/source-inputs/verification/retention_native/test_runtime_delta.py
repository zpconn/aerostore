#!/usr/bin/env python3
"""Narrow fail-closed controls for the reviewed runtime-delta audit."""
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('runtime_delta', Path(__file__).with_name('check_runtime_delta.py'))
delta = importlib.util.module_from_spec(spec)
spec.loader.exec_module(delta)
lex, _ = delta.load_lexical(delta.ROOT_DEFAULT)

class RuntimeDeltaTests(unittest.TestCase):
    def normalization(self, path, text):
        tokens, _ = lex.production_tokens(path, text)
        return delta.normalize_delta(path, tokens, lex)[0]

    def test_exact_wrapper_and_kernel(self):
        text = delta.PUBLIC_WRAPPER + '\n' + delta.KERNEL_SIGNATURE + ' Ok(Vec::new()) }'
        result = self.normalization(delta.OCC, text)
        self.assertEqual(result, lex.rust_tokens(delta.KERNEL_SIGNATURE.replace('pub(crate) fn vacuum_reclaim_before', 'pub fn vacuum_reclaim_once') + ' Ok(Vec::new()) }'))

    def test_wrong_clamp_rejected(self):
        for changed in [delta.PUBLIC_WRAPPER.replace('.min(', '.max('),
                        delta.PUBLIC_WRAPPER.replace('requested_xmin.min(retained_xmin)', 'requested_xmin'),
                        delta.PUBLIC_WRAPPER.replace('self.shm.as_ref()', 'other.shm.as_ref()'),
                        delta.PUBLIC_WRAPPER.replace('crate::vacuum::compute_global_xmin(self.shm.as_ref())', 'self.shm.global_txid()')]:
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                self.normalization(delta.OCC, changed + delta.KERNEL_SIGNATURE + ' Ok(Vec::new()) }')

    def test_public_kernel_rejected(self):
        with self.assertRaises(ValueError):
            self.normalization(delta.OCC, delta.PUBLIC_WRAPPER + delta.KERNEL_SIGNATURE.replace('pub(crate)', 'pub') + ' Ok(Vec::new()) }')

    def test_duplicate_wrapper_rejected(self):
        with self.assertRaises(ValueError):
            self.normalization(delta.OCC, delta.PUBLIC_WRAPPER * 2 + delta.KERNEL_SIGNATURE + ' Ok(Vec::new()) }')

    def test_exact_collector(self):
        self.assertEqual(self.normalization(delta.VACUUM, delta.COLLECTOR),
            lex.rust_tokens(delta.COLLECTOR.replace('.vacuum_reclaim_before(global_xmin)', '.vacuum_reclaim_once(global_xmin)')))

    def test_extra_collector_scan_rejected(self):
        changed = delta.COLLECTOR.replace('table.vacuum_reclaim_before', 'let extra = compute_global_xmin(table.shared_arena().as_ref()); table.vacuum_reclaim_before')
        with self.assertRaises(ValueError):
            self.normalization(delta.VACUUM, changed)

    def test_public_collector_call_rejected(self):
        with self.assertRaises(ValueError):
            self.normalization(delta.VACUUM, delta.COLLECTOR.replace('vacuum_reclaim_before', 'vacuum_reclaim_once'))

    def test_exact_test_only_regression(self):
        self.assertEqual(self.normalization(delta.REGRESSION_FILE, delta.REGRESSION), [])

    def test_changed_test_rejected(self):
        with self.assertRaises(ValueError):
            self.normalization(delta.REGRESSION_FILE, delta.REGRESSION.replace('#[test]', ''))
        with self.assertRaises(ValueError):
            self.normalization(delta.REGRESSION_FILE, delta.REGRESSION.replace('assert!(', 'assert!(true || ', 1))

    def test_unrelated_production_stays_visible(self):
        source = delta.PUBLIC_WRAPPER + delta.KERNEL_SIGNATURE + ' Ok(Vec::new()) }'
        original = self.normalization(delta.OCC, source)
        mutated = self.normalization(delta.OCC, source.replace('Ok(Vec::new())', 'Err(Error::SerializationFailure)'))
        self.assertNotEqual(original, mutated)

if __name__ == '__main__':
    unittest.main()
