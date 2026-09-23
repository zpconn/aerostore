#!/usr/bin/env python3
import unittest
import generate
import run


class LifecycleAdapterTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source = generate.SOURCE.read_text()
        cls.publication = generate.PUBLICATION.read_text()
        cls.shm = generate.SHM.read_text()

    def test_generated_source_is_current(self):
        self.assertEqual(generate.OUTPUT.read_text(), generate.render(self.source, self.publication, self.shm))

    def test_signature_change_is_rejected(self):
        changed = self.source.replace("pub fn create_snapshot(&self, global_txid: &AtomicU64)", "pub fn create_snapshot(&mut self, global_txid: &AtomicU64)", 1)
        with self.assertRaisesRegex(ValueError, "signature"):
            generate.render(changed)

    def test_slot_count_change_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "slot count"):
            generate.render(self.source.replace("pub const PROCARRAY_SLOTS: usize = 256;", "pub const PROCARRAY_SLOTS: usize = 512;", 1))

    def test_registration_hook_change_is_rejected(self):
        changed = self.source.replace("REGISTRATION_RESERVED_HOOK.with(|hook|", "REGISTRATION_RESERVED_HOOK.with(|other|", 1)
        with self.assertRaisesRegex(ValueError, "test-hook"):
            generate.render(changed)

    def test_atomic_store_ordering_is_rejected(self):
        changed = run.mutate(self.source, "end_transaction", "slot.txid.store(EMPTY_SLOT, Ordering::Release)", "slot.txid.store(EMPTY_SLOT, Ordering::Relaxed)")
        with self.assertRaisesRegex(ValueError, "ordering"):
            generate.render(changed)

    def test_uninitialized_prefix_projection_is_exact(self):
        changed = run.mutate(self.source, "snapshot_locked", "in_flight[in_flight_len as usize].write(txid);", "in_flight[0].write(txid);")
        # An unknown buffer write must remain visible and fail Verus; it cannot
        # silently become a push into the correctly initialized prefix.
        rendered = generate.render(changed)
        self.assertIn("in_flight [ 0 ] . write ( txid )", rendered)

    def test_shared_arena_clock_routing_is_checked(self):
        changed = self.shm.replace(".next_txid\n", ".other_txid\n", 1)
        with self.assertRaisesRegex(ValueError, "routing"):
            generate.render(self.source, self.publication, changed)

    def test_shared_arena_registration_routing_is_checked(self):
        changed = self.shm.replace("self.proc_array().begin_transaction(self.global_txid())", "self.proc_array().begin_transaction(other.global_txid())", 1)
        with self.assertRaisesRegex(ValueError, "routing"):
            generate.render(self.source, self.publication, changed)

    def test_bypass_is_rejected(self):
        for bypass in ("assume(false);", "admit();", "let ghost proof_only = 0;", "unsafe {}"):
            with self.subTest(bypass=bypass), self.assertRaisesRegex(ValueError, "bypass"):
                changed = run.mutate(self.source, "end_transaction", "let _lifecycle = self.lifecycle.lock();", bypass + "let _lifecycle = self.lifecycle.lock();")
                generate.render(changed)

    def test_semantic_mutations_reach_verifier(self):
        original = generate.render(self.source, self.publication, self.shm)
        for name, root, old, new in run.MUTATIONS:
            with self.subTest(name=name):
                if root == "reserve_publication_clock":
                    rendered = generate.render(self.source, run.mutate(self.publication, root, old, new), self.shm)
                else:
                    rendered = generate.render(run.mutate(self.source, root, old, new), self.publication, self.shm)
                self.assertNotEqual(original, rendered)


if __name__ == "__main__": unittest.main()
