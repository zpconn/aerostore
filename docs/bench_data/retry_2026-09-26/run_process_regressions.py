#!/usr/bin/env python3
"""Run functional process regressions; retain failures and source/binary bindings."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
BASE = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / 'scripts'))
import qualify_hyperfeed as gate


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--feature-binary', type=Path, default=BASE / 'benchmark-final')
    parser.add_argument('--default-binary', type=Path, default=BASE / 'benchmark-default')
    parser.add_argument('--output', type=Path, default=BASE / 'process-regressions')
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    binaries = {'feature': args.feature_binary.resolve(strict=True),
                'default': args.default_binary.resolve(strict=True)}
    source = gate.snapshot_sources()
    hashes = {kind: gate.sha256(path) for kind, path in binaries.items()}
    build = json.loads((BASE / 'build-provenance.json').read_text())
    assert source == build['source_before'] == build['source_after']
    assert hashes['feature'] == build['binary_sha256']
    specs = [
        ('retry_diagnostics_process', 'feature', 'AEROSTORE_RETRY_DIAGNOSTICS_TEST_OUTPUT', 4),
        ('contention_integration', 'default', 'AEROSTORE_CONTENTION_INTEGRATION_OUTPUT', 11),
        ('calibrated_worker_pause', 'default', 'AEROSTORE_CALIBRATED_PAUSE_OUTPUT', 2),
        ('affinity_dispatch', 'default', 'AEROSTORE_AFFINITY_TEST_OUTPUT', 2),
        ('maintenance_sweeps', 'default', 'AEROSTORE_MAINTENANCE_TEST_OUTPUT', 4),
    ]
    receipt = {'created_at': datetime.now(timezone.utc).isoformat(),
               'scope': 'Functional regression tests, deliberately including failures and process pauses. No performance/capacity comparison.',
               'source_before': source, 'binaries': {kind: str(path) for kind, path in binaries.items()},
               'binary_before_sha256': hashes, 'expected_tests': 23, 'suites': [], 'completed': False, 'passed': False}
    dest = args.output / 'receipt.json'
    save = lambda: dest.write_text(json.dumps(receipt, indent=2) + '\n')
    save()
    started = time.monotonic()
    try:
        for name, kind, output_name, expected in specs:
            assert gate.snapshot_sources() == source
            assert {kind: gate.sha256(path) for kind, path in binaries.items()} == hashes
            directory = args.output / name
            directory.mkdir()
            env = dict(os.environ, AEROSTORE_CONTENTION_BINARY=str(binaries[kind]),
                       AEROSTORE_CONTENTION_DEFAULT_BINARY=str(binaries['default']))
            env[output_name] = str(directory / 'cases')
            command = [sys.executable, '-m', 'unittest', 'discover', '-s', 'scripts', '-p', f'test_{name}.py', '-v']
            item = {'name': name, 'binary': kind, 'command': command, 'expected_tests': expected,
                    'environment': {key: value for key, value in env.items() if key.startswith('AEROSTORE_CONTENTION_') or key == output_name},
                    'started_at': datetime.now(timezone.utc).isoformat(),
                    'source_before_sha256': source['sha256'], 'binary_before_sha256': dict(hashes)}
            receipt['suites'].append(item)
            save()
            begin = time.monotonic()
            with (directory / 'unittest.log').open('w') as log:
                try:
                    result = subprocess.run(command, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT, timeout=360)
                    item.update(exit_code=result.returncode, timed_out=False)
                except subprocess.TimeoutExpired:
                    item.update(exit_code=None, timed_out=True)
            item.update(elapsed_seconds=time.monotonic() - begin,
                        source_after_sha256=gate.snapshot_sources()['sha256'],
                        binary_after_sha256={kind: gate.sha256(path) for kind, path in binaries.items()})
            output = (directory / 'unittest.log').read_text()
            item['expected_test_count_seen'] = f'Ran {expected} tests' in output
            item['skipped'] = 'skipped=' in output
            item['passed'] = (item['exit_code'] == 0 and not item['timed_out'] and item['expected_test_count_seen']
                              and not item['skipped'] and item['source_after_sha256'] == source['sha256']
                              and item['binary_after_sha256'] == hashes)
            save()
            print(json.dumps({key: item[key] for key in ['name','exit_code','passed','elapsed_seconds']}), flush=True)
            if item['source_after_sha256'] != source['sha256'] or item['binary_after_sha256'] != hashes:
                raise RuntimeError('source or binary changed during process suite')
        receipt['completed'] = True
    except Exception as error:
        receipt['error'] = repr(error)
    finally:
        receipt.update(source_after=gate.snapshot_sources(),
                       binary_after_sha256={kind: gate.sha256(path) for kind, path in binaries.items()},
                       elapsed_seconds=time.monotonic() - started)
        receipt['source_stable'] = receipt['source_before'] == receipt['source_after'] and receipt['binary_after_sha256'] == hashes
        receipt['passed'] = receipt['completed'] and receipt['source_stable'] and all(row['passed'] for row in receipt['suites'])
        save()
    print(json.dumps({key:receipt[key] for key in ['completed','passed','source_stable','elapsed_seconds']}),flush=True)
    return 0 if receipt['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
