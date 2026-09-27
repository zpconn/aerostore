import importlib.util
import copy
import json
from pathlib import Path
import tempfile
import unittest
import check_default
import normalize


class RetryDiagnosticNormalizationTests(unittest.TestCase):
    def test_current_placement_and_pinned_default_tokens(self):
        result = check_default.check()
        self.assertTrue(result['passed'])
        self.assertEqual(sum(v['sites'] for v in result['files'].values()), 18)
        self.assertTrue({
            'verification/retry_diagnostics/check_default.py',
            'verification/retry_diagnostics/normalize.py',
            'verification/lookup_native/check_production_equivalence.py',
            'verification/retry_diagnostics/default_sources.json',
            'verification/retry_diagnostics/default_sources_4da551b.json',
            'verification/retry_diagnostics/occ_partitioned_94ad54b.rs',
            check_default.OCC, check_default.WAL,
        } <= result['input_sha256'].keys())

    def test_every_reviewed_statement_erases_only_the_guarded_call(self):
        for cause in normalize.ARGS:
            with self.subTest(cause=cause):
                statement = normalize.pattern(cause)
                self.assertEqual(normalize.normalize(['left', ';'] + statement + ['right', ';']), ['left', ';', 'right', ';'])
                trailing = statement[:-2] + [','] + statement[-2:]
                self.assertEqual(normalize.normalize(trailing), [])

    def test_wrong_feature_unconditional_call_and_arbitrary_arguments_fail(self):
        good = normalize.pattern('ReadRowLocked')
        mutants = [good[len(normalize.PREFIX):],
                   [t.replace('"retry-diagnostics"', '"unchecked"') for t in good],
                   [t.replace('record', 'mutate_database') for t in good],
                   [t.replace('row_id', 'unreviewed_row') for t in good],
                   [t.replace('ReadRowLocked', 'InventedCause') for t in good]]
        call_pos = good.index('row_id')
        mutants.append(good[:call_pos] + ['side_effect', '(', ')'] + good[call_pos+1:])
        mutants.append(normalize.PREFIX + ['{'] + good[len(normalize.PREFIX):] + ['}'])
        for mutant in mutants:
            with self.subTest(mutant=mutant):
                with self.assertRaises(ValueError):
                    normalize.normalize(mutant)

    def test_moving_or_duplicating_a_legal_marker_changes_site_receipt(self):
        path = check_default.ROOT / 'aerostore_core/src/occ_partitioned.rs'
        source = path.read_text()
        expected = check_default.describe(source)
        tokens = check_default.lexer.rust_tokens(source)
        needle = normalize.pattern('ReadRowLocked')
        # Source may have rustfmt's optional final comma.
        candidates = [needle, needle[:-2] + [','] + needle[-2:]]
        position, chosen = next((i, candidate) for candidate in candidates for i in range(len(tokens)) if tokens[i:i+len(candidate)] == candidate)
        duplicate = tokens[:position] + chosen + tokens[position:]
        moved = tokens[:position] + tokens[position+len(chosen):] + chosen
        for mutant in [duplicate, moved]:
            normalized, sites = normalize.erase(mutant)
            self.assertEqual(check_default.digest(normalized), expected['default_tokens_sha256'])
            self.assertNotEqual([(s['cause'],s['position']) for s in sites],[(s['cause'],s['position']) for s in expected['sites']])

    def test_a_real_native_mutation_cannot_be_hidden_by_diagnostics(self):
        path = check_default.ROOT / 'aerostore_core/src/occ_partitioned.rs'
        source = path.read_text()
        changed = source.replace('if previous.stamp != stamp {', 'if previous.stamp == stamp {', 1)
        self.assertNotEqual(check_default.describe(changed)['default_tokens_sha256'],check_default.describe(source)['default_tokens_sha256'])

    def test_preserved_baseline_rejects_the_semantic_capture_change(self):
        historical = json.loads(check_default.MANIFEST.with_name('default_sources_4da551b.json').read_text())
        self.assertIsNone(check_default.check_transition(historical))
        reference = check_default.MANIFEST.with_name('occ_partitioned_94ad54b.rs').read_text()
        self.assertEqual(check_default.describe(reference), historical['files'][check_default.OCC]['default_projection'])
        with self.assertRaisesRegex(ValueError, 'default native tokens'):
            check_default.check(manifest=historical)

    def test_current_baseline_cannot_omit_or_null_the_reviewed_transition(self):
        manifest = json.loads(check_default.MANIFEST.read_text())
        for missing in (True, False):
            changed = copy.deepcopy(manifest)
            if missing:
                del changed['reviewed_transition']
            else:
                changed['reviewed_transition'] = None
            with self.subTest(missing=missing), self.assertRaisesRegex(ValueError, 'transition is required'):
                check_default.check_transition(changed)

    def test_claiming_legacy_revision_cannot_accept_rewritten_expectations(self):
        manifest = json.loads(check_default.MANIFEST.read_text())
        del manifest['reviewed_transition']
        manifest['baseline_commit'] = check_default.LEGACY_REFERENCE
        with self.assertRaisesRegex(ValueError, 'exact preserved historical manifest'):
            check_default.check_transition(manifest)
        historical = json.loads(check_default.MANIFEST.with_name('default_sources_4da551b.json').read_text())
        historical['files'][check_default.OCC]['default_projection'] = manifest['files'][check_default.OCC]['default_projection']
        with self.assertRaisesRegex(ValueError, 'exact preserved historical manifest'):
            check_default.check_transition(historical)

    def test_unknown_baseline_revisions_and_transition_kinds_fail_closed(self):
        manifest = json.loads(check_default.MANIFEST.read_text())
        mutants = []
        for revision in (check_default.LEGACY_REFERENCE, 'unknown', None):
            changed = copy.deepcopy(manifest)
            changed['baseline_commit'] = revision
            mutants.append(changed)
        changed = copy.deepcopy(manifest)
        changed['reviewed_transition']['kind'] = 'unreviewed'
        mutants.append(changed)
        for changed in mutants:
            with self.subTest(baseline=changed['baseline_commit']), self.assertRaisesRegex(ValueError, 'unknown reviewed'):
                check_default.check_transition(changed)
        del mutants[-1]['reviewed_transition']
        mutants[-1]['baseline_commit'] = 'unknown'
        with self.assertRaisesRegex(ValueError, 'transition is required'):
            check_default.check_transition(mutants[-1])

    def test_reviewed_transition_preserves_wal_and_exact_reviewed_diagnostic_contexts(self):
        manifest = json.loads(check_default.MANIFEST.read_text())
        result = check_default.check_transition(manifest)
        self.assertTrue(result['normalizer_unchanged'])
        self.assertTrue(result['wal_projection_unchanged'])
        self.assertFalse(result['diagnostic_contexts_unchanged'])
        self.assertTrue(result['diagnostic_contexts_reviewed'])
        self.assertEqual(result['reviewed_diagnostic_context_changes'], [
            {'cause': 'LookupChangedCapturedStamp', 'field': 'before'},
            {'cause': 'LookupPostSnapshotStamp', 'field': 'after'},
        ])
        self.assertEqual(result['reference_commit'], '94ad54bef275dd4db0ce382569bc239e931ada87')
        historical = json.loads(check_default.MANIFEST.with_name('default_sources_4da551b.json').read_text())
        old = historical['files'][check_default.OCC]['default_projection']['sites']
        new = manifest['files'][check_default.OCC]['default_projection']['sites']
        self.assertEqual([b['position'] - a['position'] for a, b in zip(old, new)], [11] + [65] * 16)
        self.assertEqual(manifest['files'][check_default.WAL]['default_projection'], historical['files'][check_default.WAL]['default_projection'])

    def test_hybrid_context_review_rejects_other_window_changes_and_missing_sites(self):
        historical = json.loads(check_default.MANIFEST.with_name('default_sources_4da551b.json').read_text())
        manifest = json.loads(check_default.MANIFEST.read_text())
        old = historical['files'][check_default.OCC]['default_projection']['sites']
        new = manifest['files'][check_default.OCC]['default_projection']['sites']
        for index, field in ((0, 'before'), (0, 'after'), (1, 'before'), (1, 'after'), (2, 'before')):
            changed = copy.deepcopy(new)
            changed[index][field][0] = 'unreviewed'
            with self.subTest(index=index, field=field), self.assertRaisesRegex(ValueError, 'unreviewed diagnostic context'):
                check_default.check_diagnostic_contexts(old, changed)
        with self.assertRaisesRegex(ValueError, 'site inventory'):
            check_default.check_diagnostic_contexts(old, new[1:])

    def test_rewriting_the_expected_digest_cannot_accept_an_unreviewed_capture(self):
        manifest = json.loads(check_default.MANIFEST.read_text())
        source = (check_default.ROOT / check_default.OCC).read_text()
        mutations = [
            source.replace('let prior_read_len = tx.index_reads.len();', 'let prior_read_len = 0;', 1),
            source.replace('index_reads[..prior_read_len]', 'index_reads', 1),
            source.replace('index_reads[..prior_read_len]', 'index_reads[..prior_read_len - 1]', 1),
            source.replace('tx.index_reads.len() == prior_read_len', 'tx.index_reads.len() >= prior_read_len', 1),
            source.replace('tx.index_reads.len() == prior_read_len', 'tx.index_reads.len() != prior_read_len', 1),
            source.replace('if let Some(previous) = previous {', 'if let Some(previous) = None {', 1),
            source.replace('read.index_offset == index.header_offset()', 'read.index_offset != index.header_offset()', 1),
            source.replace('if previous.stamp != stamp {', 'if previous.stamp == stamp {', 1),
            source.replace('mod capture_prefix_tests;', 'mod different_tests;', 1),
        ]
        for changed in mutations:
            self.assertNotEqual(source, changed)
            mutant_manifest = copy.deepcopy(manifest)
            mutant_manifest['files'][check_default.OCC]['default_projection'] = check_default.describe(changed)
            with self.subTest(digest=mutant_manifest['files'][check_default.OCC]['default_projection']['default_tokens_sha256']):
                with self.assertRaisesRegex(ValueError, 'exact reviewed prior-prefix transition'):
                    check_default.check_transition(mutant_manifest)

    def test_changing_the_wal_expectation_cannot_hide_a_wal_change(self):
        manifest = json.loads(check_default.MANIFEST.read_text())
        manifest['files'][check_default.WAL]['default_projection']['default_tokens_sha256'] = '0' * 64
        with self.assertRaisesRegex(ValueError, 'WAL default projection'):
            check_default.check_transition(manifest)

    def test_archived_reference_and_original_manifest_are_hash_bound(self):
        manifest = json.loads(check_default.MANIFEST.read_text())
        names = ('occ_partitioned_94ad54b.rs', 'default_sources_4da551b.json')
        for name in names:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                for artifact in names:
                    (root / artifact).write_bytes(check_default.MANIFEST.with_name(artifact).read_bytes())
                with (root / name).open('ab') as output:
                    output.write(b'\n')
                with self.assertRaisesRegex(ValueError, 'reference source changed|historical diagnostic baseline changed'):
                    check_default.check_transition(manifest, artifact_root=root)

    def test_reviewed_transition_fails_closed_on_missing_or_duplicate_capture(self):
        reference = check_default.MANIFEST.with_name('occ_partitioned_94ad54b.rs').read_text()
        for source in ('', reference + reference):
            with self.assertRaisesRegex(ValueError, 'missing or ambiguous'):
                check_default.capture_transition_tokens(source)

    def test_optimization_tokens_are_not_diagnostic_erasure(self):
        source = (check_default.ROOT / check_default.OCC).read_text()
        normalized = normalize.normalize(check_default.lexer.rust_tokens(source))
        for fragment in ('let prior_read_len = tx.index_reads.len();', 'index_reads[..prior_read_len]'):
            wanted = check_default.lexer.rust_tokens(fragment)
            self.assertEqual(sum(normalized[i:i + len(wanted)] == wanted for i in range(len(normalized))), 1)


if __name__ == '__main__':
    unittest.main()
