#!/usr/bin/env python3
"""Archive a completed indexed-read pilot; no writes without --write and PASS.

Prepared outside the repository while the frozen final campaign was running.
Only retained text evidence is copied. Runtime builds/tool binaries are excluded.
"""
from pathlib import Path
from string import Template
from collections import Counter
import argparse
import difflib
import hashlib
import importlib.util
import io
import json
import os
import re
import shutil
import tarfile
import tempfile

ROOT_DEFAULT = Path('/home/zpconn/code/aerostore')
DESTINATION = 'docs/verification_data/indexed_lookup_2026-09-23'
SOURCE = 'target/verification-lookup'
NEW_CAMPAIGNS = {
    'lifecycle-interference': ('Lifecycle acquisition interference', 'Acquired native suffixes and legal metadata wait traces'),
    'guard-ownership': ('Opaque guard ownership', 'CAS, borrowing, consuming release and legal foreign interference'),
    'lookup': ('Native lookup and history', 'MVCC, read provenance, candidate coverage and materialization'),
    'indexed-slice': ('Indexed read/validation composition', 'Guarded capture through materialization and native conflict validation'),
}
ALLOWED = {'.json', '.log', '.rs', '.txt', '.patch', '.cfg', '.tla'}
PRUNE = {'target', 'cargo-target', 'build', 'incremental', 'states', '.git', '.lake', '__pycache__'}


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def digest_bytes(data):
    return hashlib.sha256(data).hexdigest()


def digest(path):
    return digest_bytes(path.read_bytes())


def load(path):
    return json.loads(path.read_text())


def manifest(root):
    return {str(p.relative_to(root)): digest(p) for p in sorted(root.rglob('*')) if p.is_file()}


def text_files(root):
    for directory, dirs, names in os.walk(root):
        dirs[:] = sorted(d for d in dirs if d not in PRUNE)
        for name in sorted(names):
            path = Path(directory) / name
            if path.suffix in ALLOWED:
                require(not path.is_symlink(), 'refusing evidence symlink: ' + str(path))
                path.read_text()  # Binary or invalid text fails before archive creation.
                yield path


def diff_trees(before, after):
    require(set(before) == set(after), 'diagnostic source path set differs')
    result = []
    changes = {}
    for name in sorted(before):
        if before[name] != after[name]:
            result.extend(difflib.unified_diff(before[name].decode().splitlines(keepends=True),
                after[name].decode().splitlines(keepends=True), 'a/' + name, 'b/' + name))
            changes[name] = {'before_sha256': digest_bytes(before[name]), 'after_sha256': digest_bytes(after[name])}
    return ''.join(result), changes


def read_tree(path):
    return {str(p.relative_to(path)): p.read_bytes() for p in sorted(path.rglob('*')) if p.is_file()}


def negative_names(receipt):
    return [c['name'] for c in receipt['checks'] if c.get('expected_failure')]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=ROOT_DEFAULT)
    parser.add_argument('--write', action='store_true', help='write the reviewed archive only after all validations pass')
    parser.add_argument('--readme-template', type=Path,
        default=Path(__file__).with_name('aerostore_lookup_evidence_README.template.md'))
    args = parser.parse_args()
    root = args.root.resolve()
    source = root / SOURCE
    final = source / 'final'
    destination = root / DESTINATION
    final_report_sha256 = digest(final / 'report.json')
    report = load(final / 'report.json')
    require(report.get('passed') is True and report.get('completed') is True
        and report.get('source_stable') is True, 'final pilot has not completed with stable-source PASS; no archive written')
    require(report['source_before'] == report['source_after'], 'pilot source fingerprints changed')
    require(report.get('profile') == 'pilot', 'unexpected final profile')
    require(all(c.get('passed') is True for c in report['checks']), 'failed/incomplete final component')
    for flag in ('full_P1_complete', 'whole_engine_verified', 'promotion_eligible'):
        require(report.get(flag) is False, 'unsupported final claim: ' + flag)
    for name, expected in report['source_after'].items():
        require(digest(root / name) == expected, 'current source differs from final pilot: ' + name)

    # Independently reuse the current strict native refinement receipt validator.
    spec = importlib.util.spec_from_file_location('archive_refinement_validator', root / 'scripts/check_refinement_evidence.py')
    validator = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(validator)
    campaigns = load(root / 'verification/refinement_campaigns.json')['campaigns']
    for name in campaigns:
        validator.validate_receipt(final / name / 'receipt.json', name, root)

    lean = load(final / 'lean.json')
    require(lean.get('passed') is True and lean.get('completed') is True
        and lean.get('kernel_recheck_passed') is True and lean.get('forged_theorem_rejected') is True,
        'incomplete Lean evidence')
    require(all(m.get('rejected') is True for m in lean['mutation_checks']), 'unrejected Lean mutant')
    tla = load(final / 'tla/report.json')
    require(tla.get('passed') is True and tla.get('completed') is True
        and tla.get('complete_campaign') is True and all(x.get('passed') is True for x in tla['results']),
        'incomplete TLA evidence')
    production = load(source / 'production-equivalence.json')
    require(production.get('passed') is True, 'production comparison did not pass')
    rejection = load(source / 'accepted-boundary-rejection.json')
    require(rejection.get('passed') is False and rejection.get('errors')
        and rejection.get('anchoring') == 'independent_git_baseline', 'missing independent boundary rejection')

    native_dir = root / 'target/verification-lookup-native/final'
    native = load(native_dir / 'receipt.json')
    require(native.get('passed') is True and native.get('source_stable') is True
        and native['input_sha256'] == native['final_input_sha256'], 'incomplete native diagnostic')
    require(native['parent_commit'] == production['baseline_commit'] == report['git_revision'], 'parent revisions disagree')
    for name, expected in native['input_sha256'].items():
        require(digest(root / name) == expected, 'native diagnostic input differs: ' + name)
    current = read_tree(native_dir / 'current/source')
    archive = (native_dir / 'parent-crates.tar').read_bytes()
    require(digest_bytes(archive) == native['parent_archive_sha256'], 'native parent archive changed')
    with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
        parent = {m.name: tar.extractfile(m).read() for m in tar.getmembers() if m.isfile()}
    fixture_patch, fixture_changes = diff_trees(parent, current)
    require(set(fixture_changes) == {'aerostore_core/src/occ_partitioned.rs', 'aerostore_core/src/procarray.rs'},
        'unexpected current native fixture differences')
    patches = {'current-fixture.patch': fixture_patch}
    native_manifest = {
        'parent_commit': native['parent_commit'], 'parent_archive_sha256': native['parent_archive_sha256'],
        'parent_archive_command': ['git', 'archive', native['parent_commit'], 'Cargo.toml', 'Cargo.lock',
            'aerostore_core', 'aerostore_verified', 'aerostore_macros', 'aerostore_tcl'],
        'current_fixture': {'patch': 'current-fixture.patch', 'changes': fixture_changes,
            'patch_sha256': digest_bytes(fixture_patch.encode())}, 'mutations': [],
        'scope': 'Optional native diagnostic, not a formal refinement or mandatory pilot component',
    }
    for check in native['checks']:
        require(check.get('passed') is True, 'failed native diagnostic selection: ' + check['name'])
        log = root / check['log']
        require(digest(log) == check['log_sha256'], 'native log changed: ' + check['name'])
        tree = read_tree(Path(check['cwd']))
        for name, expected in check['source_sha256'].items():
            require(digest_bytes(tree[name]) == expected, 'native selected input changed: ' + name)
        if check['expected_assertion_failure']:
            require(check['exit_code'] == 101 and check['required_assertion'] in log.read_text(),
                'native mutant lacks intended assertion failure')
            patch, changes = diff_trees(current, tree)
            require(changes, 'empty native mutation')
            filename = check['name'] + '.patch'
            patches[filename] = patch
            native_manifest['mutations'].append({'name': check['name'], 'patch': filename,
                'patch_sha256': digest_bytes(patch.encode()), 'changes': changes,
                'source_tree_sha256': digest_bytes(json.dumps({n: digest_bytes(b) for n,b in tree.items()}, sort_keys=True).encode())})
        else:
            require(tree == current and check['exit_code'] == 0, 'positive native selection differs')

    counts = {}
    rows = []
    for name, (label, scope) in NEW_CAMPAIGNS.items():
        receipt = load(final / name / 'receipt.json')
        counts[name] = {'roots': len(receipt['required_roots']), 'mutations': len(receipt['required_mutations'])}
        rows.append(f"| {label} | {counts[name]['roots']} | {counts[name]['mutations']} | {scope} |")
    previous = load(root / 'docs/verification_data/lifecycle_publication_2026-09-23/formal/concurrent/receipt.json')
    concurrent = load(final / 'concurrent/receipt.json')
    extra = sorted(set(negative_names(concurrent)) - set(negative_names(previous)))
    outcomes = Counter(x['outcome'] for x in tla['results'])
    require(set(outcomes).issubset({'completed_finite_search', 'intended_counterexample',
        'intended_liveness_counterexample', 'reachable_witness'}), 'unrecognized TLA outcome classification')
    core_check = next(c for c in report['checks'] if c['name'] == 'core-regressions')
    core_results = re.findall(r'test result: ok\. (\d+) passed; (\d+) failed;', (root / core_check['log']).read_text())
    require(core_results and all(int(failed) == 0 for _, failed in core_results), 'missing passing core test summary')
    summary = {
        'passed': True, 'scope': 'Conditional indexed-read/validation, wait interference and opaque ownership; native heap/history obligations open',
        'parent_commit': production['baseline_commit'], 'checks': len(report['checks']), 'input_files': len(report['source_after']),
        'new_campaigns': counts, 'new_verus_roots': sum(c['roots'] for c in counts.values()),
        'new_verus_semantic_mutations': sum(c['mutations'] for c in counts.values()),
        'additional_concurrent_mutations': extra, 'lean_roots': len(lean['required_roots']),
        'lean_semantic_controls': len(lean['mutation_checks']), 'tla_cases': len(tla['results']),
        'tla_outcomes': dict(outcomes), 'core_regression_tests': sum(int(passed) for passed, _ in core_results),
        'native_positive_selections': sum(not c['expected_assertion_failure'] for c in native['checks']),
        'native_mutants': len(native_manifest['mutations']), 'production_inputs_checked': production['checked_files'],
        'production_runtime_changed': False, 'full_P1_complete': False, 'whole_engine_verified': False,
        'native_heap_history_refinement_proved': False, 'promotion_eligible': False,
        'ownership_type_controls': sum(len(load(final / name / 'receipt.json').get('type_checks', [])) for name in NEW_CAMPAIGNS),
        'performance_claim': 'No new timing claim; accepted production implementation unchanged',
    }
    values = {
        'GATE_CHECKS': summary['checks'], 'INPUT_FILES': summary['input_files'], 'PARENT_COMMIT': summary['parent_commit'],
        'CAMPAIGN_ROWS': '\n'.join(rows), 'NEW_ROOTS': summary['new_verus_roots'], 'NEW_MUTANTS': summary['new_verus_semantic_mutations'],
        'EXTRA_CONCURRENT': len(extra), 'LEAN_ROOTS': summary['lean_roots'], 'LEAN_MUTANTS': summary['lean_semantic_controls'],
        'TLA_CASES': summary['tla_cases'], 'TLA_COMPLETE': outcomes['completed_finite_search'],
        'TLA_SAFETY': outcomes['intended_counterexample'], 'TLA_LIVENESS': outcomes['intended_liveness_counterexample'],
        'TLA_WITNESS': outcomes['reachable_witness'], 'CORE_TESTS': summary['core_regression_tests'],
        'NATIVE_CURRENT': summary['native_positive_selections'], 'NATIVE_MUTANTS': summary['native_mutants'],
        'PRODUCTION_FILES': summary['production_inputs_checked'], 'OWNERSHIP_TYPES': summary['ownership_type_controls'],
    }
    readme = Template(args.readme_template.read_text()).substitute(values)
    formal_files = list(text_files(final))
    require(not destination.exists(), 'archive already exists; refusing to overwrite it')
    if not args.write:
        print(json.dumps({'ready': True, 'would_archive_text_files': len(formal_files), 'summary': summary,
            'destination': str(destination), 'written': False}, indent=2))
        return 0

    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='.lookup-archive-', dir=destination.parent) as temporary:
        staging = Path(temporary) / 'archive'
        staging.mkdir()
        def copy(src, relative):
            dst = staging / relative
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(src, dst)
        def write(relative, content):
            dst = staging / relative
            dst.parent.mkdir(parents=True, exist_ok=True)
            dst.write_text(content)
        def write_json(relative, content):
            write(relative, json.dumps(content, indent=2, sort_keys=True) + '\n')
        for path in formal_files:
            copy(path, Path('formal') / path.relative_to(final))
        for name in ('accepted_baseline_checker.py', 'accepted-boundary-rejection.json', 'production-equivalence.json'):
            copy(source / name, name)
        copy(root / 'verification/lookup_native/check_production_equivalence.py', 'production_equivalence.py')
        copy(root / 'verification/frozen_boundary.json', 'proposed_boundary.json')
        for path in text_files(source / 'quality'):
            copy(path, Path('quality') / path.relative_to(source / 'quality'))
        copy(native_dir / 'receipt.json', 'native/receipt.json')
        copy(root / 'verification/lookup_native/run.py', 'native/diagnostic_runner.py')
        for check in native['checks']:
            copy(root / check['log'], 'native/' + Path(check['log']).name)
        for name, patch in patches.items():
            write('native/' + name, patch)
        write_json('native/current-source-manifest.json', {n: digest_bytes(b) for n,b in current.items()})
        write_json('native/mutation-manifest.json', native_manifest)
        write_json('summary.json', summary)
        write('README.md', readme)
        copy(Path(__file__), 'archive_evidence.py')
        copy(args.readme_template, 'aerostore_lookup_evidence_README.template.md')
        write_json('artifact_sha256.json', manifest(staging))
        require(digest(final / 'report.json') == final_report_sha256, 'final pilot receipt changed while archiving')
        for name, expected in report['source_after'].items():
            require(digest(root / name) == expected, 'source changed while archiving: ' + name)
        staging.rename(destination)
    print(json.dumps({'archived': str(destination), 'summary': summary}, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
