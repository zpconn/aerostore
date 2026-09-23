#!/usr/bin/env python3
import unittest
import generate
import run


class GuardAdapterTests(unittest.TestCase):
    def test_checked_artifact_current(self):
        self.assertEqual(generate.render(), generate.OUTPUT.read_text())

    def test_signature_change_rejected(self):
        source = generate.SOURCE.read_text().replace("keys: &[(usize, usize)],", "keys: &[usize],", 1)
        with self.assertRaises(ValueError):
            generate.render(source)

    def test_changed_mutex_selection_rejected(self):
        index = generate.INDEX.read_text().replace("Ok(bucket.lock.try_lock())", "Ok(header.registry_lock.try_lock())", 1)
        with self.assertRaises(ValueError):
            generate.render(index=index)

    def test_changed_atomic_ordering_rejected(self):
        index = generate.INDEX.read_text()
        pos = index.index("fn transactional_try_lock_bucket(")
        index = index[:pos] + index[pos:].replace("AtomicOrdering::Acquire", "AtomicOrdering::Relaxed", 1)
        with self.assertRaises(ValueError):
            generate.render(index=index)

    def test_extra_code_cannot_be_silently_discarded(self):
        for code in ["assume(true);", "unsafe {}", "#[cfg(test)] let omitted = 0;"]:
            source = generate.SOURCE.read_text().replace("let mut guards = Vec::with_capacity(keys.len());",
                                                       code + "let mut guards = Vec::with_capacity(keys.len());", 1)
            with self.subTest(code=code), self.assertRaises(ValueError):
                generate.render(source)

    def test_duplicate_native_method_rejected(self):
        with self.assertRaises(ValueError):
            generate.render(generate.SOURCE.read_text() + "\nfn acquire_index_locks() {}")

    def test_mutation_anchors_unique(self):
        source = generate.render()
        for name, root, old, new in run.MUTATIONS:
            with self.subTest(name=name):
                self.assertEqual(source.count(old), 1)
                self.assertNotEqual(source, source.replace(old, new))


if __name__ == "__main__":
    unittest.main()
