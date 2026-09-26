import importlib.util
from pathlib import Path
import unittest
import check_default
import normalize


class RetryDiagnosticNormalizationTests(unittest.TestCase):
    def test_current_placement_and_pinned_default_tokens(self):
        result = check_default.check()
        self.assertTrue(result['passed'])
        self.assertEqual(sum(v['sites'] for v in result['files'].values()), 18)

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


if __name__ == '__main__':
    unittest.main()
