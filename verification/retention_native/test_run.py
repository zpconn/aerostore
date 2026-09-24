import unittest
import run


class RetentionNativeCampaignTests(unittest.TestCase):
    def test_all_mutants_change_only_their_selected_source(self):
        occ=(run.ROOT/run.OCC).read_text()
        proc=(run.ROOT/run.PROC).read_text()
        variants=list(run.variants(occ,proc))
        self.assertEqual(len(variants),8)
        self.assertEqual(len({variant[0] for variant in variants}),8)
        for name,path,changed,selection,assertion in variants:
            with self.subTest(name=name):
                self.assertIn(path,(run.OCC,run.PROC))
                self.assertNotEqual(changed,occ if path==run.OCC else proc)
                self.assertIn(selection[0],('--lib','--test'))
                self.assertTrue(assertion)

    def test_public_clamp_mutant_preserves_the_kernel(self):
        occ = (run.ROOT / run.OCC).read_text()
        proc = (run.ROOT / run.PROC).read_text()
        variant = next(v for v in run.variants(occ, proc) if v[0] == 'omit_public_horizon_clamp')
        marker = '    pub(crate) fn vacuum_reclaim_before('
        self.assertEqual(occ[occ.index(marker):], variant[2][variant[2].index(marker):])
        self.assertEqual(occ.replace('requested_xmin.min(retained_xmin)', 'requested_xmin', 1), variant[2])
        self.assertIn(('current_public_horizon', ['--test', 'occ_transactional_index', run.PUBLIC_HORIZON]), run.TESTS)

    def test_missing_mutation_anchor_rejected(self):
        with self.assertRaises(RuntimeError):
            run.scoped('start end','start','end','absent','replacement')

    def test_ambiguous_mutation_anchor_rejected(self):
        with self.assertRaises(RuntimeError):
            run.scoped('start x x end','start','end','x','replacement')


if __name__=='__main__': unittest.main()
