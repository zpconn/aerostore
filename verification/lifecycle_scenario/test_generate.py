#!/usr/bin/env python3
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import generate
import run


class AdapterTests(unittest.TestCase):
    def test_current_output_is_exact_and_fresh(self):
        self.assertEqual(generate.OUTPUT.read_text(), generate.render())

    def test_every_exact_current_native_component_is_embedded(self):
        generated = generate.render()
        for name in generate.COMPONENTS:
            component = generate.component(name)
            self.assertEqual(generated.count(component.OUTPUT.read_text()), 1)

    def test_missing_component_is_rejected(self):
        for marker in generate.COMPONENTS.values():
            with self.assertRaisesRegex(ValueError, "missing/duplicate"):
                generate.render(generate.TEMPLATE.read_text().replace("/* " + marker + " */", ""))

    def test_duplicate_component_is_rejected(self):
        for marker in generate.COMPONENTS.values():
            with self.assertRaisesRegex(ValueError, "missing/duplicate"):
                generate.render(generate.TEMPLATE.read_text() + "\n/* " + marker + " */")

    def test_stale_component_is_rejected(self):
        fake = SimpleNamespace(SOURCE=SimpleNamespace(read_text=lambda: "native"),
            OUTPUT=SimpleNamespace(read_text=lambda: "stale"), render=lambda _: "current")
        with patch.object(generate, "component", return_value=fake):
            with self.assertRaisesRegex(ValueError, "stale source-bound"):
                generate.render()

    def test_mutants_reach_output_without_restoring_original_semantics(self):
        template = generate.TEMPLATE.read_text()
        for mutation in run.MUTATIONS:
            with self.subTest(name=mutation[0]):
                mutated = run.mutate(template, *mutation)
                self.assertNotEqual(mutated, template)
                self.assertIn(mutation[3], generate.render(mutated))
                self.assertIn(mutation[1], run.ROOTS)

    def test_missing_or_ambiguous_mutant_anchor_is_rejected(self):
        mutation = run.MUTATIONS[0]
        for template in ("", generate.TEMPLATE.read_text() + mutation[2]):
            with self.assertRaisesRegex(ValueError, "absent or ambiguous"):
                run.mutate(template, *mutation)

    def test_receipt_inputs_include_all_executed_source_dependencies(self):
        names = [str(p.relative_to(generate.ROOT)) for p in run.inputs()]
        self.assertEqual(len(names), len(set(names)))
        for name in ("aerostore_core/src/shm.rs", "aerostore_core/src/procarray.rs",
                     "aerostore_core/src/occ_partitioned.rs", "aerostore_verified/src/lib.rs",
                     "verification/concurrent/generate.py"):
            self.assertIn(name, names)
        for component in generate.COMPONENTS:
            self.assertIn("verification/" + component + "/generate.py", names)


if __name__ == "__main__":
    unittest.main()
