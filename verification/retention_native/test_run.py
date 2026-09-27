import unittest
from pathlib import Path
import io
import tarfile
import tempfile
import run


class RetentionNativeCampaignTests(unittest.TestCase):
    def test_nested_native_module_is_overlaid_and_bound_in_each_source_receipt(self):
        archive = io.BytesIO()
        with tarfile.open(fileobj=archive, mode='w') as tar:
            data = b'old committed parent module'
            info = tarfile.TarInfo(run.OCC)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
        native = {path: ('current ' + path).encode() for path in run.NATIVE_SOURCES}
        self.assertIn(run.CAPTURE_TESTS, native)
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary) / 'current'
            run.source_tree(directory, archive.getvalue(), native)
            expected = {path: run.digest_bytes(data) for path, data in native.items()}
            self.assertEqual(run.native_hashes(directory), expected)
            self.assertEqual((directory / run.CAPTURE_TESTS).read_bytes(), native[run.CAPTURE_TESTS])
            mutation = Path(temporary) / 'mutation'
            run.source_tree(mutation, archive.getvalue(), native, (run.OCC, 'mutated OCC'))
            expected[run.OCC] = run.digest_bytes(b'mutated OCC')
            self.assertEqual(run.native_hashes(mutation), expected)
            (mutation / run.CAPTURE_TESTS).write_bytes(b'stale child module')
            self.assertNotEqual(run.native_hashes(mutation)[run.CAPTURE_TESTS], expected[run.CAPTURE_TESTS])

    def test_missing_nested_module_overlay_is_rejected(self):
        native = {path: b'current' for path in run.NATIVE_SOURCES if path != run.CAPTURE_TESTS}
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaisesRegex(RuntimeError, 'required input set'):
                run.source_tree(Path(temporary) / 'current', b'', native)

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
