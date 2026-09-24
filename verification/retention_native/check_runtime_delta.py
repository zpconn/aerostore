#!/usr/bin/env python3
"""Fail-closed lexical audit of the reviewed public-vacuum correctness fix.

This deliberately does NOT assert production equivalence. Only the exact public
horizon clamp, internal kernel rename and collector-call rename may differ,
beyond separately reviewed test additions. Correctness requires the formal and
native regression campaigns; this checker only bounds the production delta.
"""
from pathlib import Path
import argparse
import hashlib
import importlib.util
import json
import subprocess

BASELINE = 'f3d8ec49a59d44c84bc550118751c237b714c5d1'
ROOT_DEFAULT = Path(__file__).resolve().parents[2]
OCC = 'aerostore_core/src/occ_partitioned.rs'
VACUUM = 'aerostore_core/src/vacuum.rs'
REGRESSION_FILE = 'aerostore_core/tests/occ_transactional_index.rs'
PUBLIC_WRAPPER = '''pub fn vacuum_reclaim_once(
    &self, requested_xmin: TxId,
) -> Result<Vec<VacuumReclaimedRow<T>>, Error> {
    let retained_xmin = crate::vacuum::compute_global_xmin(self.shm.as_ref());
    self.vacuum_reclaim_before(requested_xmin.min(retained_xmin))
}'''
KERNEL_SIGNATURE = '''pub(crate) fn vacuum_reclaim_before(
    &self, global_xmin: TxId,
) -> Result<Vec<VacuumReclaimedRow<T>>, Error> {'''
COLLECTOR = '''pub fn run_vacuum_pass<T>(table: &OccTable<T>) -> Result<Vec<VacuumReclaimedRow<T>>, VacuumError>
where T: Copy + Send + Sync + 'static,
{
    let global_xmin = compute_global_xmin(table.shared_arena().as_ref());
    table.vacuum_reclaim_before(global_xmin).map_err(VacuumError::Occ)
}'''
# Filled with the reviewed regression itself, rather than accepting arbitrary
# edits on the strength of a test-function name alone.
REGRESSION = '#[test]\nfn public_vacuum_clamps_caller_horizon_to_retained_snapshot() {\n    let (_arena, table, _index) = fixture(&[Row::live(42)]);\n    let mut reader = table.begin_transaction().unwrap();\n    let mut updated = Row::live(42);\n    updated.value = 99;\n    replace(&table, 0, updated);\n\n    // The reader has no recorded row yet. Losing its version here could make\n    // its first read report absence and even let that empty read commit.\n    let while_pinned = table.vacuum_reclaim_once(u64::MAX).unwrap();\n    let observed = table.read(&mut reader, 0).unwrap();\n    let reader_commit = table.commit(&mut reader);\n    assert!(\n        while_pinned.is_empty(),\n        "a caller must not advance the retained horizon"\n    );\n    assert_eq!(\n        observed,\n        Some(Row::live(42)),\n        "the first read must retain its snapshot row"\n    );\n    assert_eq!(\n        reader_commit,\n        Err(OccError::SerializationFailure),\n        "retained row validation must reject the later writer"\n    );\n\n    assert!(\n        table.vacuum_reclaim_once(0).unwrap().is_empty(),\n        "a conservative caller horizon must still delay reclamation"\n    );\n    let after_reader = table.vacuum_reclaim_once(u64::MAX).unwrap();\n    assert_eq!(\n        after_reader.len(),\n        1,\n        "ending the last reader must still permit reclamation"\n    );\n    assert_eq!(after_reader[0].reclaimed_value, Row::live(42));\n    let mut fresh = table.begin_transaction().unwrap();\n    assert_eq!(table.read(&mut fresh, 0).unwrap(), Some(updated));\n    table.commit(&mut fresh).unwrap();\n}'


def digest(data):
    return hashlib.sha256(data).hexdigest()


def require(condition, message):
    if not condition:
        raise ValueError(message)


def load_lexical(root):
    path = root / 'verification/lookup_native/check_production_equivalence.py'
    spec = importlib.util.spec_from_file_location('reviewed_token_projection', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.ROOT = root
    return module, path


def one(tokens, needle, lex, label):
    positions = lex.locations(tokens, lex.rust_tokens(needle))
    require(len(positions) == 1, 'missing, changed or duplicate reviewed ' + label)
    return positions[0], len(lex.rust_tokens(needle))


def normalize_delta(name, tokens, lex):
    """Return the old production projection, accepting only exact reviewed edits."""
    tokens = list(tokens)
    changes = []
    if name == OCC:
        at, length = one(tokens, PUBLIC_WRAPPER, lex, 'public clamp wrapper')
        require(len(lex.locations(tokens, ['fn', 'vacuum_reclaim_once', '('])) == 1,
                'public wrapper not unique')
        tokens[at:at+length] = []
        at, length = one(tokens, KERNEL_SIGNATURE, lex, 'crate-private vacuum kernel signature')
        require(len(lex.locations(tokens, ['fn', 'vacuum_reclaim_before', '('])) == 1,
                'internal kernel not unique')
        tokens[at:at+length] = lex.rust_tokens(KERNEL_SIGNATURE.replace(
            'pub(crate) fn vacuum_reclaim_before', 'pub fn vacuum_reclaim_once'))
        changes += ['exact public same-arena requested.min(retained) horizon clamp',
                    'exact pub(crate) kernel rename; kernel body compared to parent']
    elif name == VACUUM:
        at, length = one(tokens, COLLECTOR, lex, 'single-scan collector function')
        require(len(lex.locations(tokens, ['fn', 'run_vacuum_pass', '<'])) == 1,
                'collector not unique')
        tokens[at:at+length] = lex.rust_tokens(COLLECTOR.replace(
            '.vacuum_reclaim_before(global_xmin)', '.vacuum_reclaim_once(global_xmin)'))
        changes += ['exact collector call rename preserving its single horizon calculation']
    elif name == REGRESSION_FILE:
        at, length = one(tokens, REGRESSION, lex, 'public vacuum regression')
        # Ensure this is a top-level test item, not another function's substring.
        depth = tokens[:at].count('{') - tokens[:at].count('}')
        require(depth == 0, 'reviewed regression is not a top-level test')
        tokens[at:at+length] = []
        changes += ['exact public_vacuum_clamps_caller_horizon_to_retained_snapshot test']
    return tokens, changes


def audit(root, overlay=None):
    root = root.resolve()
    if overlay is not None:
        overlay = overlay.resolve()
    lex, lexical_path = load_lexical(root)
    own_before = digest(Path(__file__).read_bytes())
    lexical_before = digest(lexical_path.read_bytes())
    git = lambda *args: subprocess.check_output(['git', *args], cwd=root)
    baseline = git('rev-parse', '--verify', BASELINE + '^{commit}').decode().strip()
    baseline_files = sorted(name for name in git('ls-tree', '-r', '--name-only', baseline,
        '--', *lex.NATIVE_ROOTS).decode().splitlines() if lex.selected(name))
    current_files = lex.current_inventory()
    require(baseline_files == current_files and len(current_files) == 118,
            'reviewed Rust/Cargo inventory changed')
    if overlay:
        require({str(p.relative_to(overlay)) for p in overlay.rglob('*') if p.is_file()}
                == {OCC, VACUUM, REGRESSION_FILE}, 'unexpected proposed-file overlay inventory')
    report = {'schema': 1, 'passed': False, 'scope': 'exact reviewed runtime delta; lexical audit only',
        'baseline_commit': baseline, 'production_equivalent': False,
        'formal_semantic_equivalence_proved': False, 'runtime_correctness_proved_by_this_checker': False,
        'production_runtime_changed': True, 'reviewed_runtime_delta_verified': False,
        'other_production_tokens_equal': False, 'normal_collector_extra_horizon_scan': False,
        'script_sha256': own_before, 'lexical_checker_sha256': lexical_before,
        'overlay': str(overlay) if overlay else None, 'files': {}, 'production_differences': []}
    for name in baseline_files:
        path = overlay / name if overlay and (overlay / name).exists() else root / name
        require(path.is_file() and not path.is_symlink(), 'nonregular current input: ' + name)
        before, after = git('show', baseline + ':' + name), path.read_bytes()
        ignored_before, ignored_after, allowed = [], [], []
        if name.endswith('.rs'):
            old_tokens, ignored_before = lex.production_tokens(name, before.decode())
            new_tokens, ignored_after = lex.production_tokens(name, after.decode())
            normalized, allowed = normalize_delta(name, new_tokens, lex)
            encode = lambda tokens: json.dumps(tokens, ensure_ascii=False, separators=(',', ':')).encode()
            old, actual, projected = encode(old_tokens), encode(new_tokens), encode(normalized)
        else:
            old, actual, projected = before, after, after
        report['files'][name] = {
            'baseline_sha256': digest(before), 'current_sha256': digest(after),
            'baseline_production_token_sha256': digest(old),
            'current_production_token_sha256': digest(actual),
            'reviewed_delta_normalized_token_sha256': digest(projected),
            'baseline_exclusions': ignored_before, 'current_exclusions': ignored_after,
            'reviewed_changes': allowed}
        if old != actual and name != REGRESSION_FILE:
            report['production_differences'].append(name)
        require(old == projected, 'unreviewed Rust/Cargo token difference: ' + name)
    require(report['production_differences'] == [OCC, VACUUM], 'expected two reviewed production changes')
    report['checked_files'] = len(baseline_files)
    report['script_stable'] = digest(Path(__file__).read_bytes()) == own_before
    report['lexical_checker_stable'] = digest(lexical_path.read_bytes()) == lexical_before
    report['source_stable'] = lex.current_inventory() == current_files and all(
        digest((overlay / name if overlay and (overlay / name).exists() else root / name).read_bytes()) == item['current_sha256']
        for name, item in report['files'].items())
    require(report['source_stable'] and report['script_stable'] and report['lexical_checker_stable'],
            'checker/source changed during runtime delta audit')
    report.update(passed=True, reviewed_runtime_delta_verified=True, other_production_tokens_equal=True)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=ROOT_DEFAULT)
    parser.add_argument('--overlay', type=Path, help='pre-application review only: exact three-file proposed overlay')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    try:
        report = audit(args.root, args.overlay)
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        report = {'schema': 1, 'passed': False, 'production_equivalent': False,
                  'reviewed_runtime_delta_verified': False, 'error': str(error)}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({'passed': report['passed'], 'production_equivalent': False,
                     'checked_files': report.get('checked_files'), 'error': report.get('error'),
                     'receipt': str(args.output)}))
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
