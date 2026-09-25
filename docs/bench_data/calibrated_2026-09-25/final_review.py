"""Reconcile completed frozen-source validation receipts before archiving."""
import datetime
import importlib.util
import json
import re
from pathlib import Path

ROOT = Path('/home/zpconn/code/aerostore')
BASE = ROOT / 'target/calibrated-validation'
spec = importlib.util.spec_from_file_location('gate', ROOT / 'scripts/qualify_hyperfeed.py')
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)

def read(path):
    return json.loads((BASE / path).read_text())

build = read('build-provenance.json')
assert gate.snapshot_sources() == build['source_before'] == build['source_after']
assert gate.sha256(Path(build['binary'])) == build['binary_sha256']
campaigns = {}
for name in ['accelerated-full', 'accelerated-metrics', 'cadence-native', 'cadence-postgres']:
    campaign = read(name + '/campaign.json')
    assert campaign['completed'] and campaign['passed'] and campaign['source_stable']
    assert campaign['source_before'] == campaign['source_after'] == build['source_after']
    assert campaign['binary_before_sha256'] == campaign['binary_after_sha256'] == build['binary_sha256']
    assert not campaign['actual_hyperfeed_replacement_qualified']
    assert not campaign['architecture_promotion_eligible']
    assert not campaign['whole_engine_verified']
    rows = []
    for trial in campaign['trials']:
        assert trial['exit_code'] == 0 and trial['owned_processes_terminated']
        assessment = trial['assessment']
        for key in ['execution_valid', 'continuous_timing_passed', 'useful_work_passed',
                    'foreground_effect_coverage_passed', 'foreground_ordering_passed',
                    'correctness_companion_verified', 'diagnostic_performance_passed']:
            assert assessment[key], (name, key)
        for key in ['qualified_capacity_trial', 'calibrated_capacity_qualification_complete',
                    'global_maintenance_sweep_complete', 'population_turnover_tested']:
            assert not assessment[key], (name, key)
        full = name != 'accelerated-metrics'
        assert assessment['history_verified'] == full
        report = json.loads((Path(trial['directory']) / 'report.json').read_text())
        assert len(report['runs']) == 1
        run = report['runs'][0]
        assert run['passed'] and run['execution_completed']
        assert run['completed_messages'] == run['offered_messages']
        if full:
            assert run['oracle_status'] == 'Valid'
        counts = {k: {v: run['workload_classes'][k][v] for v in ['offered', 'completed', 'positive_effect_jobs']}
                  for k in ['foreground', 'projection', 'housekeeping']}
        for values in counts.values():
            assert len(set(values.values())) == 1
        expected = [19232, 2, 2] if name.startswith('cadence') else [320, 4, 2]
        assert [counts[k]['completed'] for k in counts] == expected
        assert assessment['representative_cadence_coverage_passed'] == name.startswith('cadence')
        rows.append({'engine': run['engine'], 'counts': counts,
                     'full_history_verified': full, 'continuous_timing_passed': True,
                     'foreground_order_passed': True, 'retries': run['retries'],
                     'report': str((Path(trial['directory']) / 'report.json').relative_to(BASE))})
    assert len(rows) == (1 if name.startswith('cadence') else 9)
    campaigns[name] = rows

tests = {}
for name, count in [('integration-final', 11), ('pause-tests', 2),
                    ('qualification-tests', 37), ('remote-tests', 8)]:
    log = (BASE / (name + '.log')).read_text()
    assert re.search(r'Ran ' + str(count) + r' tests\b', log)
    assert re.search(r'^OK\s*$', log, re.MULTILINE)
    tests[name] = {'passed': count, 'log': name + '.log'}
rust_log = (BASE / 'model-and-measurement-tests.log').read_text()
counts = [int(n) for n in re.findall(r'test result: ok\. (\d+) passed; 0 failed;', rust_log)]
assert counts == [27, 45, 6], counts
tests['rust_model_and_measurement'] = {'passed': counts, 'log': 'model-and-measurement-tests.log',
                                      'note': 'The 27-test calibrated target includes 15 imported existing model tests.'}
loopback = read('loopback/orchestration.json')
assert loopback['passed'] and loopback['server_resources_cleanly_drained']
assert not loopback['physical_hosts_independently_verified']
loop = read('loopback/client-report.json')['runs'][0]
assert loop['passed'] and loop['oracle_status'] == 'Valid' and loop['completed_messages'] == 196
assert loop['per_flight_order']['passed']
stress = read('preserved-stress/report.json')
assert stress['passed'] and len(stress['runs']) == 12
assert all(r['passed'] and r['oracle_status'] == 'Valid' for r in stress['runs'])
pilot = read('guardrails/report.json')
assert pilot['completed'] and pilot['passed'] and pilot['source_stable']
assert len(pilot['checks']) == 71 and all(c['passed'] for c in pilot['checks'])
assert pilot['source_before'] == pilot['source_after'] and len(pilot['source_after']) == 522
for path, expected in pilot['source_after'].items():
    assert gate.sha256(ROOT / path) == expected, path
for path, expected in build['source_after']['files'].items():
    assert pilot['source_after'][path] == expected, path
assert not pilot['promotion_eligible'] and not pilot['whole_engine_verified']
assert not pilot['full_P1_complete']
assert pilot['p0_complete'] and pilot['p0_contract_audit']['p0_complete']
assert pilot['anchoring'] == 'local_bootstrap_only'
for receipt in ['cadence-review.json', 'preservation-review.json', 'postgres-cleanup.json']:
    assert read(receipt)['passed'], receipt
assert gate.snapshot_sources() == build['source_after']

result = {
    'passed': True, 'completed_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
    'scope': 'Functional validation of a partially calibrated HyperFeed workload; no architecture promotion.',
    'binary_sha256': build['binary_sha256'], 'source_sha256': build['source_after']['sha256'],
    'source_stable': True, 'campaigns': campaigns, 'tests': tests,
    'loopback': {'passed': True, 'messages': 196, 'history_verified': True},
    'preserved_stress': {'passed': 12, 'histories_verified': 12},
    'guardrails': {'passed': 71, 'anchoring': pilot['anchoring'], 'report': 'guardrails/report.json',
                   'p0_contract_audit_complete': True, 'full_P1_complete': False,
                   'p0_scope': pilot['p0_contract_audit']['scope']},
    'negative_controls': ['Delayed maintenance retains original offered ticks and drains after admission.',
                          'A paused maintenance worker exceeding backlog fails, preserving uncompleted offers.',
                          'Worker message caps fail rather than silently truncating timer work.',
                          'Invalid/overflowing model clocks fail before mutation.'],
    'independent_reviews': ['cadence-review.json', 'preservation-review.json'],
    'postgres_cleanup': 'postgres-cleanup.json',
    'timing_scope': 'Runs overlapped other same-host validation; timing values are diagnostics, not controlled rankings.',
    'capacity_qualified': False, 'tenfold_speedup_established': False,
    'physical_mm_hf_measured': False, 'whole_engine_verified': False,
    'full_maintenance_sweeps': False, 'population_turnover': False,
    'worker_failure_contract_changed': False,
    'remaining': ['Complete maintenance jobs as bounded transaction batches.',
                  'Calibrate population, lifecycle turnover, message/source mix and offered-rate ranges.',
                  'Run isolated sustained matched-contract comparisons and saturation brackets.',
                  'Measure physical two-host behavior and close production worker-failure availability.']
}
(BASE / 'final-review.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps({'passed': True, 'campaign_trials': sum(map(len, campaigns.values())),
                  'formal_checks': len(pilot['checks']), 'tests': tests}))
