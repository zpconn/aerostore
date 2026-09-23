#!/usr/bin/env python3
import unittest
import generate
import run


class AcquisitionAdapterTests(unittest.TestCase):
    def setUp(self):
        self.source = generate.SOURCE.read_text()

    def test_fresh_exact_component_and_suffix(self):
        self.assertEqual(generate.OUTPUT.read_text(), generate.render())
        self.assertIn(generate.lifecycle.OUTPUT.read_text(), generate.render())
        self.assertIn("D: LockedPrimitives", generate.render())

    def test_missing_first_lock_rejected(self):
        source = self.source.replace("let _lifecycle = self.lifecycle.lock();", "", 1)
        with self.assertRaisesRegex(ValueError, "first and unique"):
            generate.render(source)

    def test_delayed_first_lock_rejected(self):
        source = self.source.replace("let _lifecycle = self.lifecycle.lock();", "let x = 1; let _lifecycle = self.lifecycle.lock();", 1)
        with self.assertRaisesRegex(ValueError, "first and unique"):
            generate.render(source)

    def test_duplicate_lock_rejected(self):
        source = self.source.replace("let _lifecycle = self.lifecycle.lock();", "let _lifecycle = self.lifecycle.lock(); let _lifecycle = self.lifecycle.lock();", 1)
        with self.assertRaisesRegex(ValueError, "first and unique"):
            generate.render(source)

    def test_signature_change_rejected(self):
        source = self.source.replace("global_txid: &AtomicU64,", "global_txid: &mut AtomicU64,", 1)
        with self.assertRaisesRegex(ValueError, "signature"):
            generate.render(source)

    def test_bypass_syntax_rejected(self):
        source = self.source.replace("let _lifecycle = self.lifecycle.lock();", "assume(false); let _lifecycle = self.lifecycle.lock();", 1)
        with self.assertRaisesRegex(ValueError, "unsupported"):
            generate.render(source)

    def test_all_semantic_mutants_render(self):
        template = generate.TEMPLATE.read_text()
        for mutation in run.MUTATIONS:
            with self.subTest(mutation=mutation[0]):
                self.assertNotEqual(run.mutation_source(self.source, template, mutation), generate.OUTPUT.read_text())

    def test_arena_wrappers_remain_checked(self):
        shm = generate.lifecycle.SHM.read_text().replace('.next_txid', '.different_clock', 1)
        with self.assertRaisesRegex(ValueError, "routing"):
            generate.lifecycle.render(self.source, shm=shm)

    def test_locked_interface_has_no_acquisition_frame(self):
        template = generate.TEMPLATE.read_text()
        selected = template[template.index('pub trait LockedPrimitives'):template.index('pub open spec fn token_live')]
        self.assertNotIn('lock_lifecycle(', selected)
        self.assertIn('trace_valid(old(self).state(), final(self).state()', template)

    def test_snapshot_acquisition_hook_is_exact(self):
        source = self.source.replace('SNAPSHOT_ACQUIRING_HOOK.with', 'OTHER_HOOK.with', 1)
        with self.assertRaisesRegex(ValueError, 'test-hook changed'):
            generate.render(source)

    def test_code_between_snapshot_hook_and_lock_is_not_erased(self):
        start = self.source.index('    pub fn create_transaction_snapshot(')
        end = self.source.index('    /// Oldest pinned', start)
        body = self.source[start:end].replace('let _lifecycle = self.lifecycle.lock();',
            'let premature = global_txid.load(Ordering::Relaxed); let _lifecycle = self.lifecycle.lock();', 1)
        source = self.source[:start] + body + self.source[end:]
        with self.assertRaisesRegex(ValueError, 'first and unique'):
            generate.render(source)


if __name__ == "__main__":
    unittest.main()
