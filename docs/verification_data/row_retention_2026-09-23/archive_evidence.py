#!/usr/bin/env python3
"""Archive a completed native storage/retention pilot; no writes without --write and PASS.

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
DESTINATION = 'docs/verification_data/row_retention_2026-09-23'
SOURCE = 'target/verification-storage'
BASELINE = 'f3d8ec49a59d44c84bc550118751c237b714c5d1'
NEW_CAMPAIGNS = {
    'row-publication': ('Native row publication', 'Prepared publication field order, exact final image and interrupted prefix safety'),
    'row-retention': ('Native row retention', 'Vacuum splice/recycle loop, visible selection and retained traversal prefix'),
    'storage-slice': ('Publication/retention/reuse composition', 'Horizon, publication, vacuum, initialization and native read/validation over one declared row chain'),
}

ALLOWED = {'.json', '.log', '.rs', '.txt', '.patch', '.cfg', '.tla', '.py', '.lean'}
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
        dirs[:] = sorted(d for d in dirs if d not in PRUNE and not (d == 'source' and 'retention-native' in Path(directory).relative_to(root).parts))
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
    parser.add_argument('--bug-evidence', type=Path, default=Path('/tmp/aerostore-vacuum-api-iw1_el02'))
    parser.add_argument('--write', action='store_true', help='write the reviewed archive only after all validations pass')
    parser.add_argument('--readme-template', type=Path,
        default=Path(__file__).with_name('aerostore_storage_evidence_README.template.md'))
    args = parser.parse_args()
    root = args.root.resolve()
    source = root / SOURCE
    final = source / 'final-fixed'
    destination = root / DESTINATION
    final_report_sha256 = digest(final / 'report.json')
    report = load(final / 'report.json')
    require(report.get('passed') is True and report.get('completed') is True
        and report.get('source_stable') is True, 'final pilot has not completed with stable-source PASS; no archive written')
    require(report['source_before'] == report['source_after'], 'pilot source fingerprints changed')
    require(report.get('profile') == 'pilot', 'unexpected final profile')
    require(report.get('git_revision') == BASELINE, 'unexpected pilot parent commit')
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
    # The earlier equality receipt describes only the pre-fix implementation.
    pre_fix_production = load(source / 'production-equivalence.json')
    require(pre_fix_production.get('passed') is True and pre_fix_production.get('source_stable') is True
        and pre_fix_production.get('script_stable') is True and pre_fix_production.get('checked_files') == 118
        and pre_fix_production.get('production_differences') == [], 'pre-fix comparison incomplete or changed')
    production = load(source / 'runtime-delta.json')
    require(production.get('passed') is True and production.get('source_stable') is True
        and production.get('script_stable') is True and production.get('lexical_checker_stable') is True
        and production.get('checked_files') == 118 and production.get('overlay') is None
        and production.get('production_equivalent') is False
        and production.get('production_runtime_changed') is True
        and production.get('reviewed_runtime_delta_verified') is True
        and production.get('other_production_tokens_equal') is True
        and production.get('normal_collector_extra_horizon_scan') is False
        and production.get('production_differences') == [
            'aerostore_core/src/occ_partitioned.rs', 'aerostore_core/src/vacuum.rs'],
        'reviewed post-fix runtime delta is incomplete or overclaimed')
    for name, data in production['files'].items():
        require(digest(root / name) == data['current_sha256'], 'post-fix production input changed: ' + name)
    require(digest(root / 'verification/lookup_native/check_production_equivalence.py') == production['lexical_checker_sha256'] == pre_fix_production['script_sha256'],
        'lexical production checker changed')
    runtime_checker = root / 'verification/retention_native/check_runtime_delta.py'
    require(digest(runtime_checker) == production['script_sha256'], 'runtime delta checker changed')
    require(pre_fix_production['baseline_commit'] == production['baseline_commit'] == BASELINE,
        'pre/post-fix accepted parent differs')

    # Preserve the safe-public-API reproduction as historical evidence, including
    # its distinct pre-fix source hashes. The isolated patched regression and
    # missing-clamp negative control precede the canonical final-fixed pilot.
    bug_dir = args.bug_evidence.resolve()
    bug = load(bug_dir / 'before-receipt.json')
    historical_occ = source / 'final/retention-native/current/source/aerostore_core/src/occ_partitioned.rs'
    require(digest(historical_occ) == bug['source_sha256']['aerostore_core/src/occ_partitioned.rs'],
        'historical native OCC snapshot differs from bug reproduction')
    import subprocess
    for name, expected in bug['source_sha256'].items():
        if name != 'aerostore_core/src/occ_partitioned.rs':
            require(digest_bytes(subprocess.check_output(['git', 'show', BASELINE + ':' + name], cwd=root)) == expected,
                'historical bug input cannot be reconstructed from parent: ' + name)
    compiler_info = load(bug_dir / 'rustc-info.json')
    compiler_versions = [v['stdout'] for v in compiler_info['outputs'].values() if v['stdout'].startswith('rustc ')]
    require(compiler_versions == [(bug_dir / 'rustc.txt').read_text()] and
        'commit-hash: 01f6ddf7588f42ae2d7eb0a2f21d44e8e96674cf' in compiler_versions[0],
        'historical compiler build cache/version evidence differs')
    require(bug.get('confirmed') is True and bug.get('single_threaded') is True
        and bug.get('unsafe_client_code') is False and bug.get('no_row_reuse_after_vacuum') is True,
        'missing safe API bug reproduction')
    for filename, key in [('src/main.rs', 'reproduction_source_sha256'),
            ('Cargo.toml', 'reproduction_manifest_sha256'), ('Cargo.lock', 'reproduction_lock_sha256'),
            ('before.stdout', 'stdout_sha256')]:
        require(digest(bug_dir / filename) == bug[key], 'bug reproduction artifact changed: ' + filename)
    before_stdout = (bug_dir / 'before.stdout').read_text()
    require('raw=true correct_horizon=2 safe_reclaimed=0 raw_reclaimed=1 observed=None reader_commit=Ok(0)' in before_stdout,
        'reproduction does not show the reported missing-row successful commit')
    for name, expected in bug['source_sha256'].items():
        require(pre_fix_production['files'][name]['current_sha256'] == expected,
            'historical bug does not match pre-fix native source: ' + name)
    fix = load(bug_dir / 'fix-receipt.json')
    require(fix.get('passed') is True and [c['name'] for c in fix['checks']] ==
        ['patched-positive', 'omitted-clamp-negative', 'restored-positive'], 'isolated fix campaign incomplete')
    require(digest(bug_dir / 'proposed-fix.patch') == fix['proposed_patch_sha256'], 'reviewed fix patch changed')
    require(digest(bug_dir / 'fix_runner.py') == fix['runner_sha256'], 'isolated fix runner changed')
    fixed_source = {name: (bug_dir / 'proposed-files' / name).read_bytes() for name in
        ['aerostore_core/src/occ_partitioned.rs', 'aerostore_core/src/vacuum.rs',
         'aerostore_core/tests/occ_transactional_index.rs']}
    omitted = dict(fixed_source)
    needle = b'self.vacuum_reclaim_before(requested_xmin.min(retained_xmin))'
    require(omitted['aerostore_core/src/occ_partitioned.rs'].count(needle) == 1, 'fix mutation locator changed')
    omitted['aerostore_core/src/occ_partitioned.rs'] = omitted['aerostore_core/src/occ_partitioned.rs'].replace(
        needle, b'self.vacuum_reclaim_before(requested_xmin)')
    for check in fix['checks']:
        log = bug_dir / Path(check['log']).name
        require(digest(log) == check['log_sha256'], 'isolated fix log changed')
        negative = check['name'] == 'omitted-clamp-negative'
        expected = omitted if negative else fixed_source
        require(check['negative'] is negative and check['exit_code'] == (101 if negative else 0),
            'isolated fix outcome differs')
        require({n: digest_bytes(data) for n, data in expected.items()} == check['source_sha256'],
            'isolated fix source differs from reviewed patch/mutation')
        summaries = re.findall(r'test result: (ok|FAILED)\. (\d+) passed; (\d+) failed;', log.read_text())
        require(summaries == [('FAILED', '0', '1') if negative else ('ok', '1', '0')],
            'isolated fix must execute exactly the regression')
        require(not re.search(r'error\[E\d+\]', log.read_text()), 'isolated fix compiler failure')
        if negative:
            require('a caller must not advance the retained horizon' in log.read_text(),
                'missing-clamp mutant failed at the wrong assertion')
    current_fix_dir = source / 'public-api-integration'
    current_fix = load(current_fix_dir / 'receipt.json')
    require(current_fix.get('passed') is True and current_fix.get('completed') is True
        and current_fix.get('source_stable') is True
        and current_fix['input_sha256'] == current_fix['final_input_sha256']
        and len(current_fix['input_sha256']) == 118, 'current fixed native integration evidence incomplete')
    require(digest(current_fix_dir / 'runner.py') == current_fix['runner_sha256'],
        'current fixed native runner changed')
    for name, expected in current_fix['input_sha256'].items():
        require(digest(root / name) == expected, 'current fixed native integration source stale: ' + name)
    require('commit-hash: 01f6ddf7588f42ae2d7eb0a2f21d44e8e96674cf' in current_fix['rustc'],
        'current fixed native compiler differs')
    require([check['name'] for check in current_fix['checks']] == ['public-horizon', 'single-thread-recycling'],
        'current fixed native selections differ')
    for check in current_fix['checks']:
        log = root / check['log']
        require(check['passed'] is True and check['exit_code'] == 0 and digest(log) == check['log_sha256'],
            'current fixed native log/outcome differs')
        require(re.findall(r'test result: (ok|FAILED)\. (\d+) passed; (\d+) failed;', log.read_text()) == [('ok', '1', '0')],
            'current fixed native selection did not run exactly one passing test')
    require(load(current_fix_dir / 'source-review.json') == {
        'native_kernel_tokens_unchanged': True, 'collector_only_callee_renamed': True,
        'collector_global_horizon_computations': 1, 'horizon_computation_unchanged': True,
        'public_same_arena_clamp': True, 'kernel_crate_private': True}, 'native source review changed')
    tcl_dir = source / 'public-api-tcl'
    tcl = load(tcl_dir / 'receipt.json')
    require(tcl.get('passed') is True and tcl.get('completed') is True
        and tcl.get('source_stable') is True and tcl.get('exit_code') == 0
        and tcl['input_sha256'] == tcl['final_input_sha256']
        and len(tcl['input_sha256']) == 118, 'Tcl fixed native caller evidence incomplete')
    require(digest(tcl_dir / 'runner.py') == tcl['runner_sha256'], 'Tcl native runner changed')
    for name, expected in tcl['input_sha256'].items():
        require(digest(root / name) == expected, 'Tcl native source stale: ' + name)
    require('commit-hash: 01f6ddf7588f42ae2d7eb0a2f21d44e8e96674cf' in tcl['rustc'],
        'Tcl native compiler differs')
    tcl_log = root / tcl['log']
    require(digest(tcl_log) == tcl['log_sha256'] and len(tcl['test_names']) == 4 and
        re.findall(r'test result: (ok|FAILED)\. (\d+) passed; (\d+) failed;', tcl_log.read_text()) == [('ok', '4', '0')]
        and all('test ' + name + ' ... ok' in tcl_log.read_text() for name in tcl['test_names']),
        'Tcl fixed native log/result differs')
    rejection = load(source / 'accepted-boundary-rejection.json')
    require(rejection.get('passed') is False and rejection.get('errors')
        and rejection.get('anchoring') == 'independent_git_baseline', 'missing independent boundary rejection')

    require(rejection.get('baseline_ref') == BASELINE, 'trusted rejection uses a different baseline')
    import subprocess
    accepted_checker = source / 'accepted-boundary-checker.py'
    trusted_bytes = subprocess.check_output(['git', 'show', BASELINE + ':scripts/check_formal_coverage.py'], cwd=root)
    require(accepted_checker.read_bytes() == trusted_bytes, 'trusted checker differs from accepted parent')
    require((source / 'accepted-frozen-boundary.json').read_bytes() == subprocess.check_output(
        ['git', 'show', BASELINE + ':verification/frozen_boundary.json'], cwd=root), 'accepted frozen boundary differs')
    require((source / 'proposed_boundary.json').read_bytes() == (root / 'verification/frozen_boundary.json').read_bytes(),
        'proposed boundary differs from final current boundary')

    native_dir = final / 'retention-native'
    native = load(native_dir / 'receipt.json')
    require(native.get('passed') is True and native.get('source_stable') is True
        and native['input_sha256'] == native['final_input_sha256']
        and native.get('formal_refinement_proved') is False, 'incomplete native diagnostic')
    spec = importlib.util.spec_from_file_location('archive_native_validator', root / 'verification/retention_native/run.py')
    native_runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(native_runner)
    native_variants = list(native_runner.variants((root / native_runner.OCC).read_text(), (root / native_runner.PROC).read_text()))
    expected_native = [(name, selection, False, None, None) for name, selection in native_runner.TESTS]
    expected_native += [(name, selection, True, assertion, (path, changed.encode()))
        for name, path, changed, selection, assertion in native_variants]
    require([c['name'] for c in native['checks']] == [x[0] for x in expected_native], 'native selections omitted/reordered')
    for tool, expected in native['tool_sha256'].items():
        require(digest(Path(tool)) == expected, 'native compiler artifact changed: ' + tool)
    require('commit-hash: ' + native_runner.PINNED_RUST in native['rustc'], 'native compiler pin differs')
    require(native['parent_commit'] == production['baseline_commit'] == report['git_revision'], 'parent revisions disagree')
    for name, expected in native['input_sha256'].items():
        require(digest(root / name) == expected, 'native diagnostic input differs: ' + name)
    current = read_tree(native_dir / 'current/source')
    archive = (native_dir / 'parent-crates.tar').read_bytes()
    require(digest_bytes(archive) == native['parent_archive_sha256'], 'native parent archive changed')
    with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
        parent = {m.name: tar.extractfile(m).read() for m in tar.getmembers() if m.isfile()}
    fixture_patch, fixture_changes = diff_trees(parent, current)
    require(set(fixture_changes) == {'aerostore_core/src/occ_partitioned.rs', 'aerostore_core/src/vacuum.rs',
        'aerostore_core/tests/occ_transactional_index.rs'},
        'unexpected current native fixture differences')
    patches = {'current-fixture.patch': fixture_patch}
    native_manifest = {
        'parent_commit': native['parent_commit'], 'parent_archive_sha256': native['parent_archive_sha256'],
        'parent_archive_command': ['git', 'archive', native['parent_commit'], 'Cargo.toml', 'Cargo.lock',
            'aerostore_core', 'aerostore_verified', 'aerostore_macros', 'aerostore_tcl'],
        'current_fixture': {'patch': 'current-fixture.patch', 'changes': fixture_changes,
            'patch_sha256': digest_bytes(fixture_patch.encode())}, 'mutations': [],
        'scope': 'Mandatory pilot native regression campaign; deterministic tests, not formal refinement',
    }
    for check, expected_check in zip(native['checks'], expected_native):
        name, selection, is_negative, assertion, expected_change = expected_check
        expected_cwd = native_dir / (name if is_negative else 'current') / 'source'
        require(Path(check['cwd']) == expected_cwd, 'native workspace path differs: ' + name)
        command = check['command']
        require(len(command) >= 11 and command[1:] == ['test', '--offline', '--locked', '--target-dir',
            str(expected_cwd.parent / 'cargo-target'), '-p', 'aerostore_core', *selection, '--', '--nocapture'],
            'native test command differs: ' + name)
        require(check['expected_assertion_failure'] is is_negative and check['required_assertion'] == assertion,
            'native outcome classification differs: ' + name)
        require(check.get('passed') is True, 'failed native diagnostic selection: ' + check['name'])
        log = root / check['log']
        require(digest(log) == check['log_sha256'], 'native log changed: ' + check['name'])
        tree = read_tree(Path(check['cwd']))
        for name, expected in check['source_sha256'].items():
            require(digest_bytes(tree[name]) == expected, 'native selected input changed: ' + name)
        summaries = re.findall(r'test result: (ok|FAILED)\. (\d+) passed; (\d+) failed;', log.read_text())
        require(len(summaries) == 1 and summaries[0] == (('FAILED', '0', '1') if is_negative else ('ok', '1', '0')),
            'native diagnostic did not execute exactly its selected test: ' + name)
        require(not re.search(r'error\[E\d+\]', log.read_text()), 'native diagnostic includes a compiler error')
        if check['expected_assertion_failure']:
            expected_tree = dict(current)
            expected_tree[expected_change[0]] = expected_change[1]
            require(tree == expected_tree, 'native mutation source differs from declared variant: ' + name)
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
    previous = load(root / 'docs/verification_data/indexed_lookup_2026-09-23/formal/concurrent/receipt.json')
    concurrent = load(final / 'concurrent/receipt.json')
    extra = sorted(set(negative_names(concurrent)) - set(negative_names(previous)))
    outcomes = Counter(x['outcome'] for x in tla['results'])
    require(set(outcomes).issubset({'completed_finite_search', 'intended_counterexample',
        'intended_liveness_counterexample', 'reachable_witness'}), 'unrecognized TLA outcome classification')
    core_check = next(c for c in report['checks'] if c['name'] == 'core-regressions')
    core_results = re.findall(r'test result: ok\. (\d+) passed; (\d+) failed;', (root / core_check['log']).read_text())
    require(core_results and all(int(failed) == 0 for _, failed in core_results), 'missing passing core test summary')
    summary = {
        'passed': True, 'scope': 'Conditional native single-row publication/retention/reuse composition; allocator, weak-memory and arbitrary-history obligations open',
        'parent_commit': production['baseline_commit'], 'checks': len(report['checks']), 'input_files': len(report['source_after']),
        'new_campaigns': counts, 'new_verus_roots': sum(c['roots'] for c in counts.values()),
        'new_verus_semantic_mutations': sum(c['mutations'] for c in counts.values()),
        'additional_concurrent_mutations': extra, 'lean_roots': len(lean['required_roots']),
        'lean_semantic_controls': len(lean['mutation_checks']), 'tla_cases': len(tla['results']),
        'tla_outcomes': dict(outcomes), 'core_regression_tests': sum(int(passed) for passed, _ in core_results),
        'native_positive_selections': sum(not c['expected_assertion_failure'] for c in native['checks']),
        'native_mutants': len(native_manifest['mutations']), 'production_inputs_checked': production['checked_files'],
        'production_runtime_changed': True, 'production_equivalent': False,
        'reviewed_runtime_delta_verified': True, 'public_vacuum_bug_reproduced': True,
        'public_vacuum_horizon_clamped': True, 'additional_Tcl_caller_tests': 4, 'full_P1_complete': False, 'whole_engine_verified': False,
        'native_heap_history_refinement_proved': False, 'promotion_eligible': False,
        'arbitrary_native_history_refinement_proved': False, 'native_allocator_ownership_refinement_proved': False,
        'multirow_publication_refinement_proved': False, 'weak_memory_refinement_proved': False,
        'ownership_type_controls': sum(len(load(final / name / 'receipt.json').get('type_checks', [])) for name in NEW_CAMPAIGNS),
        'performance_claim': 'No new timing claim; public vacuum now validates its horizon, normal collector retains one scan and unchanged kernel',
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
    initializer = load(final / 'storage-slice/receipt.json')
    summary['initializer_semantic_controls'] = len([name for name in initializer['required_mutations'] if name.startswith('native_constructor_') or name.startswith('native_initialize_')])
    values['INITIALIZER_MUTANTS'] = summary['initializer_semantic_controls']
    values['SOURCE_BYTES'] = sum((root / name).stat().st_size for name in report['source_after'])
    readme = Template(args.readme_template.read_text()).substitute(values)
    formal_files = list(text_files(final))
    require(not destination.exists(), 'archive already exists; refusing to overwrite it')
    if not args.write:
        print(json.dumps({'ready': True, 'would_archive_text_files': len(formal_files), 'summary': summary,
            'destination': str(destination), 'written': False}, indent=2))
        return 0

    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='.storage-archive-', dir=destination.parent) as temporary:
        staging = Path(temporary) / 'archive'
        staging.mkdir()
        archive_paths = {}
        def copy(src, relative):
            dst = staging / relative
            dst.parent.mkdir(parents=True, exist_ok=True)
            require(not src.is_symlink(), 'refusing archived input symlink: ' + str(src))
            shutil.copyfile(src, dst)
            if src.is_relative_to(root):
                archive_paths[str(src.relative_to(root))] = str(relative)
        def write(relative, content):
            dst = staging / relative
            dst.parent.mkdir(parents=True, exist_ok=True)
            dst.write_text(content)
        def write_json(relative, content):
            write(relative, json.dumps(content, indent=2, sort_keys=True) + '\n')
        for path in formal_files:
            copy(path, Path('formal') / path.relative_to(final))
        for name in ('accepted-boundary-checker.py', 'accepted-frozen-boundary.json', 'accepted-boundary-rejection.json', 'runtime-delta.json'):
            copy(source / name, name)
        copy(source / 'production-equivalence.json', 'pre-fix/production-equivalence.json')
        copy(root / 'verification/lookup_native/check_production_equivalence.py', 'production_equivalence.py')
        copy(runtime_checker, 'runtime_delta.py')
        copy(root / 'verification/retention_native/test_runtime_delta.py', 'runtime_delta_tests.py')
        for filename in ['before-receipt.json', 'before.stdout', 'Cargo.toml', 'Cargo.lock',
                'src/main.rs', 'fix-receipt.json', 'fix_runner.py', 'proposed-fix.patch', 'rustc-info.json', 'rustc.txt',
                'patched-positive.log', 'omitted-clamp-negative.log', 'restored-positive.log']:
            copy(bug_dir / filename, Path('public-vacuum-bug') / filename)
        copy(historical_occ, 'public-vacuum-bug/pre-fix/aerostore_core/src/occ_partitioned.rs')
        write_json('public-vacuum-bug/pre-fix/source-manifest.json', bug['source_sha256'])
        for name, data in fixed_source.items():
            write('public-vacuum-bug/proposed-files/' + name, data.decode())
        omit_patch, _ = diff_trees(fixed_source, omitted)
        write('public-vacuum-bug/omitted-clamp.patch', omit_patch)
        write_json('public-vacuum-bug/scope.json', {
            'before_is_historical_pre_fix': True,
            'canonical_fixed_regression_is_in_final_pilot': True,
            'safe_public_API_missing_row_successful_commit_reproduced': True,
            'no_unsafe_client_code_or_reuse_needed': True,
            'pre_fix_production_receipt': '../pre-fix/production-equivalence.json',
            'post_fix_delta_receipt': '../runtime-delta.json',
            'binary_omission': 'Build products omitted; recorded source, commands, Cargo lock and logs retained',
            'reproduction_source_reconstruction': 'Use accepted parent and replace OCC with pre-fix/aerostore_core/src/occ_partitioned.rs; verify pre-fix/source-manifest.json',
            'compiler_evidence': 'rustc-info.json is original Cargo build-cache output; rustc.txt extracts its version; final pilot independently pins compiler',
            'reproduction_build_command': 'source target/verification-tools/environment.sh; CARGO_TARGET_DIR=/tmp/aerostore-vacuum-api-target cargo run --offline --release --manifest-path /tmp/aerostore-vacuum-api-iw1_el02/Cargo.toml',
            'reproduction_build_log': 'Original build output was not persisted; before.stdout is the recorded binary rerun output, not a reconstructed build log',
        })
        copy(source / 'proposed_boundary.json', 'proposed_boundary.json')
        for path in text_files(tcl_dir):
            copy(path, Path('public-vacuum-bug/tcl-compatibility') / path.relative_to(tcl_dir))
        for path in text_files(current_fix_dir):
            copy(path, Path('public-vacuum-bug/current-integration') / path.relative_to(current_fix_dir))
        for path in text_files(source / 'quality'):
            copy(path, Path('quality') / path.relative_to(source / 'quality'))
        copy(root / 'verification/retention_native/run.py', 'native/diagnostic_runner.py')
        for name, patch in patches.items():
            write('native/' + name, patch)
        write_json('native/current-source-manifest.json', {n: digest_bytes(b) for n,b in current.items()})
        write_json('native/mutation-manifest.json', native_manifest)
        for name, expected in report['source_after'].items():
            require(digest(root / name) == expected, 'source input changed before snapshot: ' + name)
            copy(root / name, Path('source-inputs') / name)
        write_json('source-input-sha256.json', report['source_after'])
        write_json('archive-path-map.json', archive_paths)
        write_json('summary.json', summary)
        write('README.md', readme)
        copy(Path(__file__), 'archive_evidence.py')
        copy(args.readme_template, 'aerostore_storage_evidence_README.template.md')
        write_json('artifact_sha256.json', manifest(staging))
        require(digest(final / 'report.json') == final_report_sha256, 'final pilot receipt changed while archiving')
        for name, expected in report['source_after'].items():
            require(digest(root / name) == expected, 'source changed while archiving: ' + name)
        staging.rename(destination)
    print(json.dumps({'archived': str(destination), 'summary': summary}, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
