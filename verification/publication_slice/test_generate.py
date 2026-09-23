#!/usr/bin/env python3
import unittest
from unittest.mock import patch
import generate
import run


class PublicationSliceAdapterTests(unittest.TestCase):
    def test_checked_artifact_current(self):
        self.assertEqual(generate.render(), generate.OUTPUT.read_text())

    def test_every_embedded_component_is_exact(self):
        output = generate.render()
        for name in generate.COMPONENTS:
            with self.subTest(name=name):
                self.assertIn(generate.component(name).OUTPUT.read_text(), output)

    def test_stale_component_is_rejected(self):
        native_component = generate.component
        def stale(name):
            module = native_component(name)
            if name == "guards":
                module.render = lambda *args: "stale guard proof"
            return module
        with patch.object(generate, "component", side_effect=stale), self.assertRaisesRegex(ValueError, "stale component"):
            generate.render()

    def test_mutation_anchors_unique(self):
        output = generate.render()
        for name, root, old, new in run.MUTATIONS:
            with self.subTest(name=name):
                self.assertEqual(output.count(old), 1)


if __name__ == "__main__":
    unittest.main()
