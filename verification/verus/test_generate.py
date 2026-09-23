import unittest

import generate


class AdapterBoundary(unittest.TestCase):
    def test_source_and_adapter_match(self):
        generated = generate.render(generate.SOURCE.read_text())
        self.assertEqual(generated, generate.OUTPUT.read_text())

    def test_rejects_executable_suffixes_and_weakened_contracts(self):
        source = generate.SOURCE.read_text()
        for annotation in [
            "/*@ proof {} output.clear(); @*/",
            "/*@ invariant true; output.clear(); @*/",
            "/*@ invariant true {} @*/",
            "/*@ ensures true, @*/",
            "/*@ let ghost x = output@; output.clear(); @*/",
            "/*@ proof { assume(false); } @*/",
            "/*@ proof { } /* comment */ { } @*/",
        ]:
            with self.subTest(annotation=annotation), self.assertRaises(ValueError):
                generate.render(source + annotation)

    def test_rejects_changed_proof_root(self):
        source = generate.SOURCE.read_text().replace("stamp: u64", "stamp: u32")
        with self.assertRaises(ValueError):
            generate.render(source)

    def test_rejects_disabled_roots_and_external_source(self):
        source = generate.SOURCE.read_text()
        for changed in [
            source.replace("pub fn canonical_buckets_bitmap", "#[cfg(test)]\npub fn canonical_buckets_bitmap"),
            source + "\nmod replacement;\n",
            source + '\ninclude!("replacement.rs");\n',
            source.replace("#[cfg(test)]\nmod tests;", ""),
        ]:
            with self.subTest(changed=changed[-100:]), self.assertRaises(ValueError):
                generate.render(changed)

    def test_a_comment_cannot_substitute_for_a_proof_root(self):
        source = generate.SOURCE.read_text()
        signature = next(iter(generate.INTERFACES))
        changed = source.replace("pub fn canonical_buckets_sort", "pub fn renamed_buckets_sort")
        with self.assertRaises(ValueError):
            generate.render("// " + signature + "\n" + changed)


if __name__ == "__main__":
    unittest.main()
