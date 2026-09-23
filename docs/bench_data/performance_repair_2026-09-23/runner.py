#!/usr/bin/env python3
"""One predeclared diagnostic per isolated candidate; never performance acceptance."""
import argparse
import importlib.util
import json
from pathlib import Path
import shutil
import statistics
import subprocess
import tempfile
import time

ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location('performance', ROOT / 'scripts/compare_engine_performance.py')
PERF = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PERF)


def verify_inputs(plan, manifests):
    base, candidate = manifests['baseline'], manifests['candidate']
    for manifest in manifests.values():
        PERF.validate_captured_source(manifest)
    for key in ('rustc', 'rustc_sha256', 'fixture_sha256', 'build_configuration'):
        PERF.require(base[key] == candidate[key], 'capture configuration mismatch: ' + key)
    for name in (*PERF.BENCHES, 'wal_ring_benchmark'):
        for key in ('cargo_profile', 'features'):
            PERF.require(base['binaries'][name][key] == candidate['binaries'][name][key], 'artifact mismatch')
    changed = {name for name in base['source_sha256'].keys() | candidate['source_sha256'].keys()
               if base['source_sha256'].get(name) != candidate['source_sha256'].get(name)}
    PERF.require(changed == set(plan['changed_source_files']), 'unexpected candidate source changes')
    patch = HERE / plan['patch']
    PERF.require(PERF.digest(patch) == plan['patch_sha256'], 'candidate patch changed')
    with tempfile.TemporaryDirectory(prefix='aerostore-repair-transition-') as temporary:
        scratch = Path(temporary)
        reconstructed = dict(candidate['source_sha256'])
        for name in changed:
            rel = Path(name)
            PERF.require(not rel.is_absolute() and '..' not in rel.parts and
                         name.startswith('aerostore_core/') and rel.suffix == '.rs', 'unsupported patch path')
            location = Path(candidate['source']) / rel
            if location.exists():
                PERF.require(not location.is_symlink(), 'source symlink forbidden')
                destination = scratch / rel
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(location, destination)
        subprocess.run(['git', 'apply', '--reverse', '--whitespace=nowarn', str(patch)],
                       cwd=scratch, check=True, capture_output=True)
        for name in changed:
            location = scratch / name
            if location.exists():
                reconstructed[name] = PERF.digest(location)
            else:
                reconstructed.pop(name, None)
        PERF.require(reconstructed == base['source_sha256'], 'patch does not reconstruct committed control')
    for name, expected in plan['validation_evidence_sha256'].items():
        PERF.require(PERF.digest(HERE / name) == expected, 'validation artifact changed: ' + name)
    PERF.require(plan['validation_review']['completed'] is True and
                 plan['validation_review']['candidate_source_sha256'] == candidate['source_sha256'],
                 'reviewed candidate validation does not cover captured source')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--candidate', choices=('wal-reuse', 'remove-window', 'combined'), required=True)
    parser.add_argument('--quiet-window-note', required=True)
    args = parser.parse_args()
    policy_path = HERE / 'policy.json'
    plan_path = HERE / args.candidate / 'plan.json'
    plan = json.loads(plan_path.read_text())
    policy = json.loads(policy_path.read_text())
    PERF.require(plan['candidate'] == args.candidate and plan['policy_sha256'] == PERF.digest(policy_path),
                 'candidate does not bind predeclared policy')
    PERF.require(policy['pairs'] == 3 and policy['duration_seconds'] == 30 and
                 policy['minimum_median_throughput_ratio'] == 1.03 and
                 policy['minimum_individual_throughput_ratio'] == 1.0 and
                 policy['maximum_median_p99_ratio'] == 1.10 and
                 policy['maximum_individual_p99_ratio'] == 1.25 and
                 policy['maximum_within_variant_relative_range'] == 0.10,
                 'diagnostic policy changed')
    PERF.require(policy['order'] == [['baseline', 'candidate'], ['candidate', 'baseline'], ['baseline', 'candidate']],
                 'paired order changed')
    PERF.require(PERF.POLICY == policy['unchanged_regression_policy'], 'shared comparator policy changed')
    PERF.require(PERF.digest(PERF.__file__) == policy['comparison_library_sha256'], 'comparator changed')
    if args.candidate == 'combined':
        for component in ('wal-reuse', 'remove-window'):
            result_path = HERE / component / 'result.json'
            result = json.loads(result_path.read_text())
            PERF.require(result.get('completed') is True and result.get('source_stable') is True and
                         result.get('hypothesis_supported') is True and
                         plan['component_result_sha256'][component] == PERF.digest(result_path),
                         'combination requires independently supported retained components')
    output = HERE / args.candidate
    receipt = output / 'result.json'
    PERF.require(not receipt.exists(), 'retain every result: this diagnostic cannot be overwritten or retried')
    paths = {variant: ROOT / path for variant, path in plan['manifests'].items()}
    PERF.require(set(paths) == {'baseline', 'candidate'}, 'missing comparison capture')
    hashes = {variant: PERF.digest(path) for variant, path in paths.items()}
    PERF.require(hashes == plan['manifest_sha256'], 'capture manifest changed after predeclaration')
    manifests = {variant: PERF.validate_manifest(path) for variant, path in paths.items()}
    verify_inputs(plan, manifests)
    report = {'schema_version': 1, 'candidate': args.candidate, 'completed': False,
              'hypothesis_supported': False, 'performance_acceptance': False, 'speedup_claim': False,
              'scope': 'One fixed short diagnostic versus committed 9676fa9; full original-baseline campaign still required',
              'quiet_window_note': args.quiet_window_note, 'plan_sha256': PERF.digest(plan_path),
              'policy_sha256': PERF.digest(policy_path), 'runner_sha256': PERF.digest(__file__),
              'comparison_library_sha256': PERF.digest(PERF.__file__), 'manifest_sha256': hashes,
              'exact_patch_transition_verified': True,
              'started_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()), 'runs': []}
    PERF.write_json(receipt, report)
    workload = {'kind': 'churn', 'duration': 30, 'paired_timing': True}
    pairs = []
    try:
        for number, order in enumerate(policy['order'], 1):
            pair = {}
            for variant in order:
                print(f'{args.candidate} pair={number}/3 variant={variant}', flush=True)
                result = PERF.run_one(manifests[variant], variant, 'churn_128m_30s', workload, number, output)
                report['runs'].append(result)
                pair[variant] = result['metrics']
                PERF.write_json(receipt, report)
            pairs.append(pair)
            report['completed_pairs'] = len(pairs)
            report['comparison_so_far'] = PERF.compare_pairs(pairs, workload)
            PERF.write_json(receipt, report)
            print(json.dumps({'pair': number, 'comparison': report['comparison_so_far']}), flush=True)
        comparison = PERF.compare_pairs(pairs, workload)
        ratios = comparison['metrics']['throughput']['paired_ratios']
        policy_pass = comparison['status'] == 'pass'
        support = (statistics.median(ratios) >= 1.03 and min(ratios) >= 1.0 and policy_pass)
        for variant, path in paths.items():
            current = PERF.validate_manifest(path)
            PERF.validate_captured_source(current)
            PERF.require(PERF.digest(path) == hashes[variant], 'capture changed during diagnostic')
        verify_inputs(plan, manifests)
        for key, path in [('plan_sha256', plan_path), ('policy_sha256', policy_path),
                          ('runner_sha256', Path(__file__)), ('comparison_library_sha256', Path(PERF.__file__))]:
            PERF.require(report[key] == PERF.digest(path), 'diagnostic source or plan changed')
        failures = []
        if statistics.median(ratios) < 1.03:
            failures.append('median throughput gain below 3%')
        if min(ratios) < 1.0:
            failures.append('at least one throughput pair is slower')
        if not policy_pass:
            failures.append('tail/noise/resource comparison is not pass')
        report.update(completed=True, comparison=comparison, diagnostic_policy_pass=policy_pass,
                      hypothesis_supported=support, source_stable=True, failed_support_criteria=failures)
    except BaseException as error:
        report['error'] = f'{type(error).__name__}: {error}'
        report['interrupted'] = isinstance(error, KeyboardInterrupt)
    finally:
        try:
            for variant, path in paths.items():
                PERF.validate_captured_source(PERF.validate_manifest(path))
                PERF.require(PERF.digest(path) == hashes[variant], 'postflight capture changed')
            for key, path in [('plan_sha256', plan_path), ('policy_sha256', policy_path),
                              ('runner_sha256', Path(__file__)), ('comparison_library_sha256', Path(PERF.__file__))]:
                PERF.require(report[key] == PERF.digest(path), 'postflight diagnostic source changed')
            report['source_stable'] = True
        except Exception as error:
            report.update(completed=False, hypothesis_supported=False, source_stable=False,
                          postflight_error=str(error))
        report['finished_utc'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
        PERF.write_json(receipt, report)
    print(json.dumps({key: report[key] for key in ('completed', 'hypothesis_supported', 'performance_acceptance')}))
    return 0 if report['completed'] else 2


if __name__ == '__main__':
    raise SystemExit(main())
