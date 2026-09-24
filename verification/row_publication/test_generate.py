import unittest
import generate
import run

class PublicationAdapterTests(unittest.TestCase):
    def test_current_artifact(self):
        self.assertEqual(generate.render(),generate.OUTPUT.read_text())
    def test_embeds_exact_lookup(self):
        self.assertIn(generate.lookup.render(),generate.render())
    def test_signature_drift(self):
        s=generate.SOURCE.read_text().replace('fn publish_prepared_write_set(&self, record:', 'fn publish_prepared_write_set(&mut self, record:',1)
        with self.assertRaises(ValueError):generate.render(s)
    def test_duplicate_native_method(self):
        with self.assertRaises(ValueError):generate.render(generate.SOURCE.read_text()+'\nfn publish_prepared_write_set() {}')
    def test_changed_atomic_ordering(self):
        s=generate.SOURCE.read_text();a=s.index('    fn publish_prepared_write_set(');b=s.index('    fn publish_write_set(',a)
        for ordering in ('AcqRel','Acquire','Release'):
            with self.subTest(ordering=ordering),self.assertRaises(ValueError):
                generate.render(s[:a]+s[a:b].replace('Ordering::'+ordering,'Ordering::Relaxed',1)+s[b:])
    def test_native_bypass_rejected(self):
        s=generate.SOURCE.read_text()
        for extra in ('assume(true);','unsafe {}','#[cfg(test)] let omit=0;'):
            with self.subTest(extra=extra),self.assertRaises(ValueError):
                generate.render(s.replace('for write in &record.writes {','for write in &record.writes {'+extra,1))
    def test_exact_hook_only(self):
        s=generate.SOURCE.read_text()
        with self.assertRaises(ValueError):generate.render(s.replace('hook(write.row_id, false);','hook(0, false);',1))
    def test_added_early_return_preserved(self):
        s=generate.SOURCE.read_text().replace('for write in &record.writes {','for write in &record.writes { return Err(Error::Storage);',1)
        self.assertIn('return Err ( Error :: Storage ) ;',generate.render_module(s))
    def test_mutation_anchors(self):
        s=generate.render()
        for item in run.MUTATIONS:
            with self.subTest(name=item[0]):self.assertNotEqual(s,run.mutation(s,item))
if __name__=='__main__':unittest.main()
