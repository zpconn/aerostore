#!/usr/bin/env python3
"""Disposable systemd memory containment; this controller stays outside the unit.

The child waits for explicit release until actual cgroup values are read back.
No CPU quota or memory.high throttle is configured. All ordinary fork/setsid/
daemonized descendants inherit the cgroup. This is resource containment, not a
security sandbox against an intentionally escaping process.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import uuid

GIB = 1024 ** 3
CGROOT = Path('/sys/fs/cgroup')
CGFILES = ['memory.current', 'memory.peak', 'memory.swap.current', 'memory.swap.peak',
           'memory.events', 'memory.events.local', 'memory.stat', 'memory.pressure',
           'cgroup.events', 'cgroup.procs']
PROPERTIES = ['ActiveState', 'SubState', 'Result', 'ExecMainCode', 'ExecMainStatus',
              'ControlGroup', 'MainPID', 'MemoryCurrent', 'MemoryPeak',
              'MemorySwapCurrent', 'MemorySwapPeak', 'InvocationID', 'OOMPolicy']


def atomic_json(path, value):
    path = Path(path)
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')
    temporary.replace(path)


def read_text(path):
    return Path(path).read_text().strip()


def boot_id():
    return read_text('/proc/sys/kernel/random/boot_id')


def current_cgroup():
    rows = read_text('/proc/self/cgroup').splitlines()
    return next(row.split(':', 2)[2] for row in rows if row.startswith('0::'))


def key_values(text):
    return dict(line.split('=', 1) for line in text.splitlines() if '=' in line)


def mem_available():
    return int(next(line.split()[1] for line in Path('/proc/meminfo').read_text().splitlines()
                    if line.startswith('MemAvailable:'))) * 1024


def systemctl(*args, check=True):
    return subprocess.run(['systemctl', '--user', *args], check=check,
                          text=True, capture_output=True)


def unit_status(unit):
    return key_values(systemctl('show', unit, *['--property=' + p for p in PROPERTIES]).stdout)


def verify_controls(path, memory_max, swap_max):
    expected = {'memory.max': str(memory_max), 'memory.high': 'max',
                'memory.swap.max': str(swap_max), 'memory.oom.group': '1'}
    actual = {name: read_text(path / name) for name in expected}
    if actual != expected:
        raise RuntimeError(f'Actual cgroup controls {actual} do not match {expected}')
    return actual


def ancestor_controls(path):
    result = []
    while path.is_relative_to(CGROOT):
        row = {'path': str(path.relative_to(CGROOT))}
        for name in ['memory.max', 'memory.high', 'memory.swap.max', 'cpu.max', 'io.max']:
            p = path / name
            if p.exists():
                row[name] = read_text(p)
        result.append(row)
        if path == CGROOT:
            break
        path = path.parent
    return result


def child(config_path):
    config = json.loads(Path(config_path).read_text())
    output = Path(config['output'])
    atomic_json(output / 'child-ready.json', {'pid': os.getpid(), 'cgroup': current_cgroup(),
                                              'boot_id': boot_id(), 'time': time.time()})
    deadline = time.monotonic() + 120
    while not (output / 'release.json').exists():
        if time.monotonic() > deadline:
            raise RuntimeError('Controller did not release child after cgroup verification')
        time.sleep(0.05)
    release = json.loads((output / 'release.json').read_text())
    if release['boot_id'] != boot_id() or release['cgroup'] != current_cgroup():
        raise RuntimeError('Release identity does not match child')
    os.environ['AEROSTORE_MEMORY_ENVELOPE_READINESS'] = str(output / 'readiness.json')
    completed = subprocess.run(config['command'], cwd=config['cwd'], check=False)
    atomic_json(output / 'child-result.json', {'returncode': completed.returncode,
                                              'time': time.time(), 'boot_id': boot_id()})
    return completed.returncode if completed.returncode >= 0 else 128 - completed.returncode


def run(args):
    if not args.command:
        raise ValueError('A child command is required after --')
    command = args.command[1:] if args.command[0] == '--' else args.command
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    controller_boot = boot_id()
    token = uuid.uuid4().hex[:12]
    unit = 'aerostore-memory-' + token + '.service'
    accounting_slice = 'aerostoremem' + token + '.slice'
    config = {'output': str(output), 'command': command, 'cwd': str(Path.cwd()),
              'memory_max_bytes': args.memory_max, 'swap_max_bytes': args.swap_max,
              'reserve_bytes': args.reserve_bytes, 'timeout_seconds': args.timeout_seconds,
              'sample_seconds': args.sample_seconds, 'unit': unit, 'accounting_slice': accounting_slice}
    atomic_json(output / 'command.json', config)
    controller = {'pid': os.getpid(), 'cgroup': current_cgroup(), 'boot_id': controller_boot,
                  'mem_available_bytes': mem_available(), 'time': time.time()}
    atomic_json(output / 'controller.json', controller)
    status = {}
    readiness = None
    events = {}
    final_accounting = {}
    max_current = 0
    min_available = controller['mem_available_bytes']
    reason = None
    fds = {}
    launched = False
    cleanup_status = {}
    samples = 0
    returncode = 125
    started = time.monotonic()
    try:
        properties = {'MemoryAccounting': 'yes', 'MemoryMax': str(args.memory_max),
                      'MemoryHigh': 'infinity', 'MemorySwapMax': str(args.swap_max),
                      'OOMPolicy': 'kill', 'KillMode': 'control-group',
                      'RemainAfterExit': 'yes', 'TimeoutStopSec': '10s',
                      'RuntimeMaxSec': str(args.timeout_seconds) + 's',
                      'WorkingDirectory': str(Path.cwd()),
                      'StandardOutput': 'append:' + str(output / 'child.log'),
                      'StandardError': 'append:' + str(output / 'child.log')}
        launch = ['systemd-run', '--user', '--unit=' + unit, '--slice=' + accounting_slice]
        launch += ['--property=' + k + '=' + v for k, v in properties.items()]
        # Preserve explicit toolchain selection without shell evaluation. systemd
        # user units do not otherwise inherit this controller's PATH/environment.
        for key in ['PATH', 'RUSTUP_TOOLCHAIN', 'CARGO_HOME', 'RUSTUP_HOME']:
            if key in os.environ:
                launch += ['--setenv=' + key + '=' + os.environ[key]]
        launch += ['--', sys.executable, str(Path(__file__).resolve()), '--child',
                   str(output / 'command.json')]
        launch_result = subprocess.run(launch, check=False, text=True, capture_output=True)
        atomic_json(output / 'launch.json', {'command': launch, 'returncode': launch_result.returncode,
                                            'stdout': launch_result.stdout, 'stderr': launch_result.stderr})
        if launch_result.returncode:
            raise RuntimeError('systemd-run failed: ' + launch_result.stderr)
        launched = True
        deadline = time.monotonic() + 30
        while not (output / 'child-ready.json').exists():
            if time.monotonic() > deadline:
                raise RuntimeError('Child did not reach readiness barrier')
            time.sleep(0.05)
        child_ready = json.loads((output / 'child-ready.json').read_text())
        status = unit_status(unit)
        group = child_ready['cgroup']
        if group != status['ControlGroup'] or child_ready['boot_id'] != controller_boot:
            raise RuntimeError('Unit and child cgroup/boot identity mismatch')
        if controller['cgroup'] == group or controller['cgroup'].startswith(group + '/'):
            raise RuntimeError('Controller must stay outside benchmark cgroup')
        path = CGROOT / group.lstrip('/')
        controls = verify_controls(path, args.memory_max, args.swap_max)
        ancestors = ancestor_controls(path)
        accounting_status = unit_status(accounting_slice)
        accounting_path = CGROOT / accounting_status['ControlGroup'].lstrip('/')
        if path.parent != accounting_path:
            raise RuntimeError('Unique accounting slice is not the service parent')
        for row in ancestors:
            if row.get('cpu.max', 'max 100000').split()[0] != 'max':
                raise RuntimeError('Ancestor CPU quota is configured')
            maximum = row.get('memory.max', 'max')
            if maximum != 'max' and int(maximum) < args.memory_max:
                raise RuntimeError('Ancestor memory limit is below requested service limit')
        if any(row.get('memory.high', 'max') != 'max' for row in ancestors):
            raise RuntimeError('Ancestor memory.high throttle is configured')
        readiness = {'schema_version': 1, 'ready': True, 'unit': unit, 'cgroup': group,
                     'cgroup_path': str(path), 'boot_id': controller_boot,
                     'memory_max_bytes': args.memory_max, 'swap_max_bytes': args.swap_max,
                     'reserve_bytes': args.reserve_bytes, 'memory_high': 'max',
                     'memory_oom_group': 1, 'controls': controls, 'ancestors': ancestors,
                     'accounting_slice': accounting_slice,
                     'accounting_cgroup': accounting_status['ControlGroup'],
                     'accounting_cgroup_path': str(accounting_path),
                     'accounting_semantics': 'Hierarchical accounting in the unique parent slice retains counters after the limited service exits; no other service shares this slice.',
                     'controller': controller, 'child': child_ready, 'time': time.time(),
                     'helper_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
        atomic_json(output / 'readiness.json', readiness)
        for name in CGFILES:
            try:
                fds[name] = open(accounting_path / name)
            except FileNotFoundError:
                pass
        atomic_json(output / 'release.json', {'cgroup': group, 'boot_id': controller_boot})
        with (output / 'samples.jsonl').open('w', buffering=1) as stream:
            while True:
                sample = {'time': time.time(), 'elapsed_seconds': time.monotonic() - started,
                          'mem_available_bytes': mem_available(), 'cgroup': group,
                          'accounting_cgroup': accounting_status['ControlGroup']}
                min_available = min(min_available, sample['mem_available_bytes'])
                for name, fd in fds.items():
                    try:
                        fd.seek(0)
                        value = fd.read().strip()
                        sample[name] = value
                        if name.startswith('memory.events'):
                            events[name] = value
                        if name == 'memory.current':
                            max_current = max(max_current, int(value))
                    except (OSError, ValueError) as error:
                        sample[name + '_error'] = str(error)
                stream.write(json.dumps(sample, sort_keys=True) + '\n')
                samples += 1
                status = unit_status(unit)
                if status.get('ActiveState') in ('inactive', 'failed') or status.get('SubState') == 'exited':
                    reason = 'resource-oom' if status.get('Result') == 'oom-kill' else 'unit-completed'
                    break
                if sample['mem_available_bytes'] < args.reserve_bytes:
                    reason = 'host-available-memory-reserve'
                    systemctl('stop', unit)
                    status = unit_status(unit)
                    break
                if time.monotonic() - started > args.timeout_seconds + 30:
                    reason = 'controller-timeout'
                    systemctl('stop', unit)
                    status = unit_status(unit)
                    break
                time.sleep(args.sample_seconds)
        child_result_path = output / 'child-result.json'
        if child_result_path.exists():
            returncode = json.loads(child_result_path.read_text())['returncode']
        elif status.get('Result') == 'oom-kill':
            returncode = 137
        elif status.get('ExecMainStatus', '').isdigit() and int(status['ExecMainStatus']):
            returncode = 128 + int(status['ExecMainStatus']) if status.get('ExecMainCode') == '2' else int(status['ExecMainStatus'])
    except BaseException as error:
        reason = 'controller-exception: ' + repr(error)
        raise
    finally:
        # The unique parent slice survives service removal, preserving final
        # hierarchical event counters even after an immediate service OOM kill.
        for name, fd in fds.items():
            try:
                fd.seek(0)
                value = fd.read().strip()
                final_accounting[name] = value
                if name.startswith('memory.events'):
                    events[name] = value
            except OSError:
                pass
            fd.close()
        if launched:
            try:
                status = unit_status(unit)
                stopped = systemctl('stop', unit, check=False)
                cleanup_status['service_stop_returncode'] = stopped.returncode
                cleanup_status['service'] = unit_status(unit)
                systemctl('reset-failed', unit, check=False)
                stopped_slice = systemctl('stop', accounting_slice, check=False)
                cleanup_status['slice_stop_returncode'] = stopped_slice.returncode
                cleanup_status['slice'] = unit_status(accounting_slice)
                cleanup_status['owned_processes_terminated'] = (
                    not cleanup_status['service'].get('ControlGroup') and
                    not cleanup_status['slice'].get('ControlGroup'))
            except Exception as error:
                status['cleanup_error'] = repr(error)
        atomic_json(output / 'result.json', {'schema_version': 1, 'unit': unit, 'reason': reason,
                    'returncode': returncode, 'status_before_cleanup': status,
                    'memory_events': events, 'memory_events_scope': 'unique-accounting-slice',
                    'cleanup': cleanup_status, 'samples': samples, 'final_accounting': final_accounting,
                    'max_sampled_memory_current_bytes': max_current,
                    'minimum_mem_available_bytes': min_available,
                    'controller_boot_before': controller_boot, 'controller_boot_after': boot_id(),
                    'controller_survived': True, 'elapsed_seconds': time.monotonic() - started,
                    'finished_time': time.time(), 'ready': readiness is not None})
    if not cleanup_status.get('owned_processes_terminated', False):
        return 125
    return returncode if returncode >= 0 else 128 - returncode


def main():
    if len(sys.argv) == 3 and sys.argv[1] == '--child':
        return child(sys.argv[2])
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--memory-max', type=int, default=36 * GIB)
    parser.add_argument('--swap-max', type=int, default=4 * GIB)
    parser.add_argument('--reserve-bytes', type=int, default=4 * GIB)
    parser.add_argument('--timeout-seconds', type=int, default=3600)
    parser.add_argument('--sample-seconds', type=float, default=1.0)
    parser.add_argument('command', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.memory_max <= 0 or args.swap_max < 0 or args.reserve_bytes < 0 or args.sample_seconds <= 0:
        parser.error('Invalid nonpositive limit or sample interval')
    def stop_requested(signum, frame):
        raise KeyboardInterrupt(f'Controller received signal {signum}')
    signal.signal(signal.SIGTERM, stop_requested)
    return run(args)


if __name__ == '__main__':
    sys.exit(main())
