#!/usr/bin/env python3
"""Archive the stable commit-completion pilot. Dry run by default; --write is explicit.

Source snapshots, raw text evidence and exact native mutation patches are retained.
Tool executables and build outputs are omitted; original receipts retain their hashes.
"""
from pathlib import Path
from collections import Counter
import argparse
import difflib
import gzip
import hashlib
import importlib.util
import io
import json
import os
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile

ROOT_DEFAULT = Path('/home/zpconn/code/aerostore')
BASELINE = '4619b8f465d71a263f531eac72a88ca536aa0bcb'
SOURCE = 'target/verification-p1-completion'
DESTINATION = 'docs/verification_data/commit_completion_2026-09-23'
ALLOWED = {'.json', '.log', '.rs', '.txt', '.patch', '.cfg', '.tla', '.py', '.lean'}
PRUNE = {'target', 'cargo-target', 'build', 'incremental', 'states', '.git', '.lake', '__pycache__'}
NATIVE = ('retention-native', 'p1-native')
NEW_CAMPAIGNS = ('commit-data', 'commit-completion')


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def hash_bytes(data):
    return hashlib.sha256(data).hexdigest()


def digest(path):
    return hash_bytes(path.read_bytes())


def load(path):
    return json.loads(path.read_text())


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    sys.modules[name] = result
    spec.loader.exec_module(result)
    return result


def manifest(root):
    return {str(p.relative_to(root)): digest(p) for p in sorted(root.rglob('*')) if p.is_file()}


def tree(root):
    return {str(p.relative_to(root)): p.read_bytes() for p in sorted(root.rglob('*')) if p.is_file()}


def text_files(root):
    for directory, dirs, names in os.walk(root):
        parts = Path(directory).relative_to(root).parts
        dirs[:] = sorted(d for d in dirs if d not in PRUNE and not (d == 'source' and any(n in parts for n in NATIVE)))
        for name in sorted(names):
            path = Path(directory) / name
            if path.suffix in ALLOWED:
                require(not path.is_symlink(), 'refusing evidence symlink: ' + str(path))
                path.read_text()
                yield path


def diff(before, after):
    require(set(before) == set(after), 'native source path set differs')
    lines, changes = [], {}
    for name in sorted(before):
        if before[name] != after[name]:
            lines.extend(difflib.unified_diff(before[name].decode().splitlines(keepends=True),
                after[name].decode().splitlines(keepends=True), 'a/' + name, 'b/' + name))
            changes[name] = {'before_sha256': hash_bytes(before[name]), 'after_sha256': hash_bytes(after[name])}
    return ''.join(lines), changes


def tar_gz(contents):
    output = io.BytesIO()
    with gzip.GzipFile(fileobj=output, mode='wb', filename='', mtime=0) as zipped:
        with tarfile.open(fileobj=zipped, mode='w') as tar:
            for name, data in sorted(contents.items()):
                info = tarfile.TarInfo(name)
                info.size = len(data)
                info.mode = 0o644
                tar.addfile(info, io.BytesIO(data))
    return output.getvalue()


def native_evidence(root, final, name, expected_tree=None):
    directory = final / name
    receipt = load(directory / 'receipt.json')
    runner = module('archive_' + name.replace('-', '_'), root / 'verification' / name.replace('-', '_') / 'run.py')
    require(receipt.get('passed') is True and receipt.get('source_stable') is True
            and receipt.get('formal_refinement_proved') is False
            and receipt['input_sha256'] == receipt['final_input_sha256'], 'incomplete native evidence: ' + name)
    require(receipt['parent_commit'] == BASELINE, 'native parent differs: ' + name)
    for filename, sha in receipt['input_sha256'].items():
        require(digest(root / filename) == sha, 'native source stale: ' + filename)
    for filename, sha in receipt['tool_sha256'].items():
        require(digest(Path(filename)) == sha, 'native compiler changed: ' + filename)
    require('commit-hash: ' + runner.PINNED_RUST in receipt['rustc'], 'unreviewed native compiler')
    current = tree(directory / 'current/source')
    if expected_tree is not None:
        require(current == expected_tree, 'native campaigns used different current source trees')
    parent_archive = (directory / 'parent-crates.tar').read_bytes()
    require(hash_bytes(parent_archive) == receipt['parent_archive_sha256'], 'native parent archive changed')
    with tarfile.open(fileobj=io.BytesIO(parent_archive)) as tar:
        parent = {m.name: tar.extractfile(m).read() for m in tar.getmembers() if m.isfile()}
    fixture_patch, fixture_changes = diff(parent, current)
    require(set(fixture_changes) == {'aerostore_core/tests/occ_transactional_index.rs'}, 'unexpected native fixture changes')
    controls = list(runner.variants((root / runner.OCC).read_text(), (root / runner.PROC).read_text())) if name == 'retention-native' else list(runner.variants((root / runner.OCC).read_text()))
    expected = [(n, selection, False, None, None) for n, selection in runner.TESTS]
    expected += [(n, selection, True, assertion, (path, changed.encode())) for n, path, changed, selection, assertion in controls]
    require([c['name'] for c in receipt['checks']] == [c[0] for c in expected], 'native selection omitted/reordered')
    patches = {}
    mutation_manifest = {'parent_commit': BASELINE, 'parent_archive_sha256': receipt['parent_archive_sha256'],
        'parent_archive_command': ['git', 'archive', BASELINE, 'Cargo.toml', 'Cargo.lock', *runner.CRATES],
        'current_fixture_patch': '../current-fixture.patch', 'current_fixture_changes': fixture_changes,
        'current_fixture_patch_sha256': hash_bytes(fixture_patch.encode()), 'current_source': '../current-source.tar.gz',
        'mutations': [], 'scope': 'Deterministic native tests; not formal refinement'}
    for check, (checkname, selection, negative, assertion, changed) in zip(receipt['checks'], expected):
        selected = directory / (checkname if negative else 'current') / 'source'
        require(Path(check['cwd']) == selected, 'wrong native workspace')
        command = check['command']
        tail = ['--', '--test-threads=1', '--nocapture'] if name == 'p1-native' else ['--', '--nocapture']
        require(command[1:] == ['test', '--offline', '--locked', '--target-dir', str(selected.parent / 'cargo-target'),
            '-p', 'aerostore_core', *selection, *tail], 'wrong native command')
        require(command[0] in receipt['tool_sha256'] and Path(command[0]).name == 'cargo', 'wrong cargo binary')
        require(check.get('passed') is True and check['expected_assertion_failure'] is negative
                and check['required_assertion'] == assertion, 'wrong native outcome classification')
        log = root / check['log']
        require(log == directory / (checkname + '.log') and digest(log) == check['log_sha256'], 'native log changed')
        contents = log.read_text()
        require(re.findall(r'test result: (ok|FAILED)\. (\d+) passed; (\d+) failed;', contents) ==
                [('FAILED', '0', '1') if negative else ('ok', '1', '0')], 'native selection did not run exactly one test')
        require(check['exit_code'] == (101 if negative else 0) and not re.search(r'error\[E\d+\]', contents), 'wrong-kind native failure')
        if negative:
            require('panicked at' in contents and assertion in contents, 'wrong native assertion')
        actual = tree(selected)
        expected_contents = dict(current)
        if changed:
            expected_contents[changed[0]] = changed[1]
        require(actual == expected_contents, 'native mutated source differs')
        for filename, sha in check['source_sha256'].items():
            require(hash_bytes(actual[filename]) == sha, 'native selected source digest changed')
        for filename, sha in check.get('binary_sha256', {}).items():
            require(digest(Path(filename)) == sha, 'native executable changed')
        if negative:
            patch, changes = diff(current, actual)
            require(changes, 'empty native mutation')
            patches[checkname + '.patch'] = patch
            mutation_manifest['mutations'].append({'name': checkname, 'patch': checkname + '.patch',
                'patch_sha256': hash_bytes(patch.encode()), 'changes': changes,
                'source_tree_sha256': hash_bytes(json.dumps({n: hash_bytes(b) for n, b in actual.items()}, sort_keys=True).encode())})
    return receipt, current, fixture_patch, mutation_manifest, patches


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=ROOT_DEFAULT)
    parser.add_argument('--write', action='store_true')
    parser.add_argument('--readme', type=Path, help='Reviewed README to include; optional, root may write it then refresh the artifact manifest')
    args = parser.parse_args()
    root = args.root.resolve()
    sys.path.insert(0, str(root / 'scripts'))
    gate = module('archive_formal_gate', root / 'scripts/verify_formal.py')
    source, destination = root / SOURCE, root / DESTINATION
    final = source / 'final'
    report_sha = digest(final / 'report.json')
    report = load(final / 'report.json')
    require(report.get('passed') is True and report.get('completed') is True and report.get('source_stable') is True,
        'final pilot has not completed with stable-source PASS; no archive written')
    require(report['source_before'] == report['source_after'] and report['profile'] == 'pilot'
        and report['git_revision'] == BASELINE, 'final pilot source/profile/parent mismatch')
    require(all(c.get('passed') is True for c in report['checks']), 'failed/incomplete component')
    for flag in ('full_P1_complete', 'whole_engine_verified', 'promotion_eligible'):
        require(report.get(flag) is False, 'unsupported final claim: ' + flag)
    require(gate.source_fingerprint(root) == report['source_after'], 'current source path set or hashes differ from final pilot')
    for filename, sha in report['source_after'].items():
        require(digest(root / filename) == sha, 'current source differs from final pilot: ' + filename)
    validator = module('archive_refinement_validator', root / 'scripts/check_refinement_evidence.py')
    campaigns = load(root / 'verification/refinement_campaigns.json')['campaigns']
    for name in campaigns:
        validator.validate_receipt(final / name / 'receipt.json', name, root)
    module('archive_p1_native_validator', root / 'scripts/check_p1_native_evidence.py').validate_receipt(final / 'p1-native/receipt.json', root)
    p0 = module('archive_p0_validator', root / 'scripts/check_p0_contracts.py').validate(root)
    require(load(final / 'p0-contracts.log') == p0 == report['p0_contract_audit'], 'P0 audit differs from source/pilot')
    require(report.get('p0_complete') is p0.get('p0_complete'), 'P0 status differs')
    lean = load(final / 'lean.json')
    require(lean.get('passed') is True and lean.get('completed') is True and lean.get('kernel_recheck_passed') is True
        and lean.get('forged_theorem_rejected') is True and all(m.get('rejected') is True for m in lean['mutation_checks']), 'incomplete Lean evidence')
    tla = load(final / 'tla/report.json')
    require(tla.get('passed') is True and tla.get('completed') is True and tla.get('complete_campaign') is True
        and all(c.get('passed') is True for c in tla['results']), 'incomplete TLA evidence')
    runtime = load(source / 'runtime-audit.json')
    runtime_runner = Path('/tmp/aerostore_p1_runtime_audit.py')
    require(runtime.get('passed') is True and runtime.get('source_stable') is True and runtime.get('checked_files') == 118
        and runtime.get('production_runtime_changed') is False and runtime.get('production_byte_identical') is True
        and runtime.get('formal_semantic_equivalence_proved') is False and runtime['baseline_commit'] == BASELINE
        and runtime['changed_inputs'] == ['aerostore_core/tests/occ_transactional_index.rs'], 'runtime audit scope/outcome differs')
    require(digest(runtime_runner) == runtime['runner_sha256'], 'runtime audit runner changed')
    for filename, item in runtime['files'].items():
        old = subprocess.check_output(['git', 'show', BASELINE + ':' + filename], cwd=root)
        require(hash_bytes(old) == item['baseline_sha256'] and digest(root / filename) == item['current_sha256']
            and item['byte_identical'] is (old == (root / filename).read_bytes()), 'runtime byte comparison differs: ' + filename)
    baseline_checker = source / 'baseline_check_formal_coverage.py'
    require(baseline_checker.read_bytes() == subprocess.check_output(['git', 'show', BASELINE + ':scripts/check_formal_coverage.py'], cwd=root), 'accepted checker differs')
    rejection = load(source / 'baseline-rejection.json')
    require(rejection.get('passed') is False and rejection.get('errors') and rejection['baseline_ref'] == BASELINE
        and rejection['anchoring'] == 'independent_git_baseline', 'missing independent baseline rejection')
    require(module('archive_accepted_coverage', baseline_checker).validate(root, BASELINE) == rejection, 'baseline rejection no longer matches')
    native_results, shared_current, fixture = {}, None, None
    for name in NATIVE:
        result = native_evidence(root, final, name, shared_current)
        native_results[name] = result
        shared_current, fixture = result[1], result[2]
    counts = {name: {'roots': len(load(final / name / 'receipt.json')['required_roots']),
        'semantic_mutations': len(load(final / name / 'receipt.json')['required_mutations'])} for name in NEW_CAMPAIGNS}
    core = next(c for c in report['checks'] if c['name'] == 'core-regressions')
    core_results = re.findall(r'test result: ok\. (\d+) passed; (\d+) failed;', (root / core['log']).read_text())
    require(core_results and all(n == '0' for _, n in core_results), 'missing core regression success')
    summary = {'passed': True, 'parent_commit': BASELINE, 'gate_checks': len(report['checks']),
        'input_files': len(report['source_after']), 'new_campaigns': counts,
        'lean_roots': len(lean['required_roots']), 'lean_semantic_controls': len(lean['mutation_checks']),
        'tla_cases': len(tla['results']), 'tla_outcomes': dict(Counter(c['outcome'] for c in tla['results'])),
        'core_regression_tests': sum(int(n) for n, _ in core_results),
        'native_campaigns': {n: {'positive_selections': sum(not c['expected_assertion_failure'] for c in r[0]['checks']),
            'semantic_controls': len(r[3]['mutations'])} for n, r in native_results.items()},
        'runtime_inputs_checked': runtime['checked_files'], 'production_byte_identical': True,
        'production_runtime_changed': False, 'formal_semantic_equivalence_proved': False,
        'p0_complete': report['p0_complete'], 'full_P1_complete': False, 'whole_engine_verified': False,
        'promotion_eligible': False, 'arbitrary_native_history_refinement_proved': False,
        'native_allocator_ownership_refinement_proved': False, 'weak_memory_refinement_proved': False,
        'scope': 'Conditional one-index/one-final-write ordinary publication, deregistration and shared-clock stamp completion; full transaction history join remains open',
        'performance_claim': 'No runtime change or new timing claim'}
    formal_files = list(text_files(final))
    require(not destination.exists(), 'archive exists; refusing overwrite')
    if not args.write:
        print(json.dumps({'ready': True, 'written': False, 'destination': str(destination),
            'text_artifacts': len(formal_files), 'summary': summary}, indent=2))
        return 0
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='.completion-archive-', dir=destination.parent) as temporary:
        staging = Path(temporary) / 'archive'
        staging.mkdir()
        paths = {}
        def write(relative, content):
            output = staging / relative
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_bytes(content if isinstance(content, bytes) else content.encode())
        def write_json(relative, value):
            write(relative, json.dumps(value, indent=2, sort_keys=True) + '\n')
        def copy(path, relative):
            require(not path.is_symlink(), 'refusing archived symlink')
            write(relative, path.read_bytes())
            if path.is_relative_to(root):
                paths[str(path.relative_to(root))] = str(relative)
        for path in formal_files:
            copy(path, Path('formal') / path.relative_to(final))
        for name in ('runtime-audit.json', 'baseline-rejection.json', 'baseline_check_formal_coverage.py'):
            copy(source / name, name)
        copy(runtime_runner, 'runtime_audit.py')
        copy(source / 'production-equivalence.json', 'diagnostics/legacy-production-equivalence.json')
        write_json('diagnostics/scope.json', {'canonical_runtime_receipt': '../runtime-audit.json',
            'legacy_production_equivalence_is_not_canonical': True,
            'reason': 'Historical checker excludes only older test bodies; new integration tests trigger its fixed token comparison'})
        write('accepted-frozen-boundary.json', subprocess.check_output(['git', 'show', BASELINE + ':verification/frozen_boundary.json'], cwd=root))
        copy(root / 'verification/frozen_boundary.json', 'reviewed-frozen-boundary.json')
        packed = tar_gz(shared_current)
        write('native/current-source.tar.gz', packed)
        write_json('native/current-source-manifest.json', {n: hash_bytes(b) for n, b in shared_current.items()})
        write('native/current-fixture.patch', fixture)
        for name, (_, _, _, mutation_manifest, patches) in native_results.items():
            mutation_manifest['current_source_tar_sha256'] = hash_bytes(packed)
            write_json('native/' + name + '/mutation-manifest.json', mutation_manifest)
            for filename, patch in patches.items():
                write('native/' + name + '/' + filename, patch)
        for name, sha in report['source_after'].items():
            require(digest(root / name) == sha, 'source changed before snapshot')
            copy(root / name, Path('source-inputs') / name)
        write_json('source-input-sha256.json', report['source_after'])
        write_json('archive-path-map.json', paths)
        write_json('summary.json', summary)
        if args.readme:
            copy(args.readme, 'README.md')
        copy(Path(__file__), 'archive_evidence.py')
        write_json('artifact_sha256.json', manifest(staging))
        require(digest(final / 'report.json') == report_sha, 'final report changed while archiving')
        for name, sha in report['source_after'].items():
            require(digest(root / name) == sha, 'source changed while archiving')
        require(gate.source_fingerprint(root) == report['source_after'], 'source path set or hashes changed while archiving')
        staging.rename(destination)
    print(json.dumps({'archived': str(destination), 'summary': summary,
        'artifacts': len(load(destination / 'artifact_sha256.json')),
        'README_pending': args.readme is None}, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
