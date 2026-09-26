#!/usr/bin/env python3
"""Interleaved expiry eligibility and diagnostic-overhead experiment.

Default invocation only describes the plan. Root must supply --execute and an
isolation note after the final build and other validation processes have stopped.
Every cell uses the same feature-enabled binary. Full and metrics modes differ
only in evidence collection; metrics never verifies its own unrecorded history.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import signal
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
BASE = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / 'scripts'))
import qualify_hyperfeed as gate

SEEDS = (20260924, 20260925, 20260926)
RATES = (128, 512, 2048)
VARIANTS = (('all-active', 'off'), ('housekeeping', 'off'),
            ('housekeeping', 'on'), ('all-active', 'on'))


def now():
    return datetime.now(timezone.utc).isoformat()


def make_plan(native_engine, include_postgres):
    steps = []
    full_steps = {}
    for rate_index, rate in enumerate(RATES):
        for seed_index, seed in enumerate(SEEDS):
            block = rate_index * len(SEEDS) + seed_index
            rotation = block % len(VARIANTS)
            order = VARIANTS[rotation:] + VARIANTS[:rotation]
            if block % 2:
                order = tuple(reversed(order))
            full = [(native_engine, policy, diagnostics) for policy, diagnostics in order]
            if include_postgres:
                control = ('postgres', 'all-active', 'off')
                full = [control, *full] if block % 2 == 0 else [*full, control]
            for evidence, variants in [('full', full), ('metrics', list(reversed(full)))]:
                for engine, policy, diagnostics in variants:
                    key = engine, rate, seed, policy, diagnostics
                    step = {'index': len(steps), 'block': block, 'engine': engine,
                            'rate': rate, 'seed': seed, 'expiry_index_policy': policy,
                            'retry_diagnostics': diagnostics, 'evidence': evidence,
                            'purpose': 'diagnostic_overhead_control' if rate == 128 else 'retry_attribution_probe'}
                    step['label'] = f"{len(steps):03d}-{engine}-r{rate}-s{seed}-{policy}-diag-{diagnostics}-{evidence}"
                    if evidence == 'full':
                        full_steps[key] = step['index']
                    else:
                        step['full_companion_step'] = full_steps[key]
                    steps.append(step)
    return steps


def command_for(step, args, output, plan, timeout):
    command = [sys.executable, str(ROOT / 'scripts/qualify_hyperfeed.py'),
               '--binary', str(args.binary), '--output', str(output / step['label']),
               '--engines', step['engine'], '--workload', 'calibrated',
               '--maintenance-mode', 'sweep', '--projection-batch-size', '4',
               '--housekeeping-batch-size', '32', '--max-maintenance-batches', '4096',
               '--dispatch', 'signature-affinity', '--affinity-ttl-ms', '600', '--signature-pattern', 'mixed',
               '--expiry-index', step['expiry_index_policy'], '--retry-diagnostics', step['retry_diagnostics'],
               '--rates', str(step['rate']), '--seeds', str(step['seed']), '--workers', '4',
               '--families', '16', '--hot-percent', '0', '--seconds', '7',
               '--projection-interval-seconds', '1', '--housekeeping-interval-seconds', '2',
               '--max-backlog', '1000', '--max-messages', '100000', '--shm-mib', '256',
               '--pg-write-mode', 'buffered', '--rpc-delay-us', '0', '--slo-ms', '50',
               '--outcome-tolerance', '0', '--evidence', step['evidence'],
               '--timeout-seconds', str(timeout), '--pg-url-env', args.pg_url_env]
    if step['evidence'] == 'metrics':
        companion = plan[step['full_companion_step']]
        command += ['--correctness-report', str(output / companion['label'] / 'campaign.json')]
    return command


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, default=BASE / 'benchmark-final')
    parser.add_argument('--build-provenance', type=Path, default=BASE / 'build-provenance.json')
    parser.add_argument('--output', type=Path, default=BASE / 'controlled-campaign')
    parser.add_argument('--native-engine', choices=['aerostore', 'service-unix'], default='aerostore')
    parser.add_argument('--include-postgres', action='store_true')
    parser.add_argument('--pg-url-env', default='AEROSTORE_CONTENTION_PG_URL')
    parser.add_argument('--budget-seconds', type=float, default=900)
    parser.add_argument('--trial-timeout-seconds', type=float, default=45)
    parser.add_argument('--execute', action='store_true')
    parser.add_argument('--isolation-note', default='')
    args = parser.parse_args()
    plan = make_plan(args.native_engine, args.include_postgres)
    descriptor = {'scope': 'Controlled bounded synthetic diagnostic experiment, not a sustainable capacity, MMHF, retention plateau, or 10x claim.',
                  'sequential_only': True, 'same_binary_for_every_cell': True,
                  'diagnostic_overhead_scope': 'Runtime collection on/off within one feature-enabled binary; this does not measure compile-time feature overhead relative to an ordinary build.',
                  'native_variants': [dict(expiry_index_policy=p, retry_diagnostics=d) for p, d in VARIANTS],
                  'seeds': list(SEEDS), 'rates': list(RATES), 'seconds_per_trial': 7,
                  'foreground_workers': 4, 'families': 16, 'projection_interval_seconds': 1,
                  'housekeeping_interval_seconds': 2, 'dispatch': 'signature-affinity',
                  'affinity_ttl_ms': 600, 'signature_pattern': 'mixed',
                  'affinity_scope': 'Temporary signature affinity is architect-confirmed; 600ms sliding TTL and mixed alias pattern are explicit experimental parameters, not calibrated production settings.',
                  'planned_trials': len(plan),
                  'planned_admission_seconds': len(plan) * 7, 'budget_seconds': args.budget_seconds,
                  'ordering': 'Rotate/reverse the four variants by block; full companions precede metrics in reverse order. PostgreSQL control alternates block edges. No parallel benchmark trials.',
                  'postgres_scope': 'Unchanged prepared/buffered SERIALIZABLE control, native policy selector has no effect on its existing housekeeping partial index.' if args.include_postgres else 'Omitted by explicit plan.',
                  'metric_pair_scope': 'Compare on/off and eligibility pairs within the same engine, rate, seed, evidence and source/binary. Counterbalanced order across blocks reduces order bias; three seeds do not establish stable tail latency.',
                  'failed_companions': 'Run the predeclared metrics cell even if its full companion fails; exact companion acceptance remains false and no correctness-qualified performance claim may use that cell.',
                  'steps': plan}
    if not args.execute:
        print(json.dumps(descriptor, indent=2))
        return 0
    if not args.isolation_note.strip():
        parser.error('--execute requires --isolation-note recording how other benchmark/build/proof jobs were stopped')
    if not 60 <= args.budget_seconds <= 1000 or not 15 <= args.trial_timeout_seconds <= 60:
        parser.error('budget must be 60..1000 seconds and each trial timeout 15..60 seconds')
    args.binary = args.binary.resolve(strict=True)
    args.build_provenance = args.build_provenance.resolve(strict=True)
    args.output = args.output.resolve()
    if args.output.exists():
        parser.error('output already exists; select a fresh directory to preserve evidence')
    if args.include_postgres and not os.environ.get(args.pg_url_env):
        parser.error('PostgreSQL control requires the named URL environment variable')
    build = json.loads(args.build_provenance.read_text())
    source, binary_sha = gate.snapshot_sources(), gate.sha256(args.binary)
    if not (build['source_before'] == build['source_after'] == source and
            build['binary_sha256'] == binary_sha and build['exit_code'] == 0):
        parser.error('build provenance does not match current source and measured binary')
    if 'retry-diagnostics' not in build['cargo_artifact'].get('features', []):
        parser.error('one retry-diagnostics feature build is required for all on/off cells')
    args.output.mkdir(parents=True)
    receipt_path = args.output / 'execution.json'
    started = time.monotonic()
    deadline = started + args.budget_seconds
    receipt = {**descriptor, 'started_at': now(), 'completed': False, 'passed': False,
               'driver_pid': os.getpid(),
               'driver_start_ticks': Path('/proc/self/stat').read_text().rsplit(') ', 1)[1].split()[19],
               'driver_sha256': gate.sha256(Path(__file__)),
               'isolation_note': args.isolation_note,
               'isolation_scope': 'Operator/root assertion plus recorded host observations; not enforced CPU isolation or proof that background system work was absent.',
               'build_provenance': str(args.build_provenance), 'build_provenance_sha256': gate.sha256(args.build_provenance),
               'source_before': source, 'binary_before_sha256': binary_sha,
               'host_before': gate.host_info(), 'steps': [], 'unrun_steps': plan}
    gate.atomic_json(receipt_path, receipt)

    def interrupted(signum, frame):
        raise KeyboardInterrupt(f'received signal {signum}')

    old_term = signal.signal(signal.SIGTERM, interrupted)
    try:
        for step in plan:
            remaining = deadline - time.monotonic()
            if remaining < 30:
                receipt['budget_exhausted'] = True
                break
            if source != gate.snapshot_sources() or binary_sha != gate.sha256(args.binary):
                raise RuntimeError('source or binary changed; no further cells may run')
            # Reserve bounded cleanup time. A benchmark timeout is retained as
            # a failed cell, never retried until it happens to pass.
            inner_timeout = min(args.trial_timeout_seconds, remaining - 20)
            command = command_for(step, args, args.output, plan, inner_timeout)
            actual = {**step, 'command': command, 'started_at': now(),
                      'source_before_sha256': source['sha256'], 'binary_before_sha256': binary_sha}
            receipt['steps'].append(actual)
            receipt['unrun_steps'] = plan[step['index'] + 1:]
            gate.atomic_json(receipt_path, receipt)
            print('START', step['label'], flush=True)
            wrapper = gate.run_process(command, args.output / (step['label'] + '.log'), inner_timeout + 12)
            actual.update(wrapper_process=wrapper, finished_at=now())
            campaign_path = args.output / step['label'] / 'campaign.json'
            campaign = json.loads(campaign_path.read_text())
            if (not campaign.get('completed') or campaign.get('source_before') != source or
                    campaign.get('source_after') != source or campaign.get('source_stable') is not True or
                    campaign.get('binary_before_sha256') != binary_sha or
                    campaign.get('binary_after_sha256') != binary_sha or len(campaign.get('trials', [])) != 1):
                raise RuntimeError(f"{step['label']}: incomplete driver or inconsistent source/binary evidence")
            trial = campaign['trials'][0]
            if (trial['config']['engine'] != step['engine'] or trial['config']['arrival_rate'] != step['rate'] or
                    trial['config']['seed'] != step['seed'] or trial['config']['evidence'] != step['evidence'] or
                    trial['config']['expiry_index_policy'] != step['expiry_index_policy'] or
                    trial['config']['retry_diagnostics'] != (step['retry_diagnostics'] == 'on')):
                raise RuntimeError('driver reported a different experiment cell')
            actual.update(campaign_path=str(campaign_path), campaign_sha256=gate.sha256(campaign_path),
                          benchmark_exit_code=trial.get('exit_code'), timed_out=trial.get('timed_out'),
                          assessment=trial['assessment'],
                          source_after_sha256=gate.snapshot_sources()['sha256'], binary_after_sha256=gate.sha256(args.binary))
            if wrapper['timed_out'] or not wrapper['owned_processes_terminated']:
                raise RuntimeError('wrapper timed out or cannot confirm owned process termination')
            if actual['source_after_sha256'] != source['sha256'] or actual['binary_after_sha256'] != binary_sha:
                raise RuntimeError('source or binary changed during a trial')
            gate.atomic_json(receipt_path, receipt)
            print('DONE', step['label'], 'valid=', trial['assessment']['execution_valid'], flush=True)
        receipt['completed'] = len(receipt['steps']) == len(plan) and not receipt.get('budget_exhausted', False)
    except KeyboardInterrupt as error:
        receipt.update(interrupted=True, error=str(error))
    except Exception as error:
        receipt['error'] = type(error).__name__ + ': ' + str(error)
    finally:
        signal.signal(signal.SIGTERM, old_term)
        receipt.update(finished_at=now(), elapsed_seconds=time.monotonic() - started,
                       source_after=gate.snapshot_sources(), binary_after_sha256=gate.sha256(args.binary),
                       host_after=gate.host_info())
        receipt['source_stable'] = receipt['source_before'] == receipt['source_after'] and binary_sha == receipt['binary_after_sha256']
        receipt['passed'] = receipt['completed'] and receipt['source_stable'] and not receipt.get('error')
        receipt['all_trials_execution_valid'] = bool(receipt['steps']) and all(
            step.get('assessment', {}).get('execution_valid') is True for step in receipt['steps'])
        receipt['passed_scope'] = 'The complete predeclared experiment and evidence collection finished; individual failed benchmark cells are retained and separately reported.'
        gate.atomic_json(receipt_path, receipt)
    print(json.dumps({key: receipt[key] for key in ['completed', 'passed', 'all_trials_execution_valid', 'elapsed_seconds', 'source_stable']}), flush=True)
    return 0 if receipt['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
