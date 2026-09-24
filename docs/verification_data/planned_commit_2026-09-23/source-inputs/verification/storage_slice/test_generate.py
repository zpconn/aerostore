import unittest
import generate
import run


class StorageCompositionTests(unittest.TestCase):
    def test_current_artifact(self):
        self.assertEqual(generate.OUTPUT.read_text(), generate.render())

    def test_components_share_exact_lookup_vocabulary(self):
        source = generate.render()
        self.assertEqual(source.count('pub struct Image {'), 1)
        self.assertIn(generate.component('lookup').render(), source)
        for name in ('row_publication', 'row_retention'):
            with self.subTest(component=name):
                self.assertIn(generate.component(name).render_module(), source)

    def test_every_mutation_has_one_reviewed_anchor(self):
        source = generate.render()
        for item in run.MUTATIONS:
            with self.subTest(mutation=item[0]):
                self.assertNotEqual(source, run.mutation(source, item))


if __name__ == '__main__':
    unittest.main()
