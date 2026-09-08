"""Count shared-mount syscalls inside the admission critical section.

Driver mode runs itself under ``strace`` and parses the trace; child mode
builds a private queue on the shared mount, admits M holders, and runs P
``claim`` passes, marking the start and end of ``PoolQueue._claim`` (the body
the admission flock guards) with ``stat`` calls on a path that cannot exist.
Every syscall between a pass's markers is classified by the path it names:
under the queue root (shared mount), under the box-state root (host-local),
or elsewhere. Counts and summed ``-T`` wall time are reported per pass.

The broker sample and the action contract are patched exactly as the unit
tests patch them, so the measured path is the claim code and its file I/O,
not a GPU broker. Holder telemetry is written to both the shared path and the
host-local path before each pass, so the same harness serves both the
generation that reads the mount and the generation that reads the host.
"""
import argparse
import contextlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time
import uuid

MARK = '/pb266b-marker'


def child(args):
    sys.path.insert(0, str(Path.cwd() / 'src'))
    os.environ['PRISMABUILD_BOX_STATE_ROOT'] = args.box_state
    from prismabuild import adaptive_cpu, adaptive_gpu, pool
    root = Path(args.queue_root)
    queue = pool.PoolQueue(root)
    cpus = args.holders + 8
    tiers = {'preferred': list(range(cpus)), 'fallback': []}
    capacity = {'cpu': cpus, 'mem_gb': cpus, 'gpu': 1}
    clock = [time.time()]
    adaptive_cpu.Controller.sample = lambda self: {
        'sampled_unix': time.time(), 'busy_cpus': 0., 'psi_some': 0.,
        'cpu_count': cpus, 'interval_s': 1.}
    adaptive_cpu.action_identity = lambda item: ('shape', False)
    gpu = args.gpu
    sample = {'schema': 'prismabuild.gpu_capacity.v1', 'sample_id': 's0',
              'sampled_unix': time.time(), 'complete': True, 'attributed': True,
              'devices': [{'uuid': 'GPU-1', 'power_w': 15., 'power_limit_w': None,
                           'power_reference_w': 140., 'power_reference_scope': 'soc_tdp',
                           'memory_domain': 'shared_system', 'limited': False}],
              'host_total_bytes': 128 * adaptive_gpu.GIB,
              'host_available_bytes': 100 * adaptive_gpu.GIB,
              'memory_pressure_some': 0., 'memory_pressure_full': 0.,
              'cpu_pressure_some': 0., 'foreign_processes': [], 'jobs': []}
    if gpu:
        adaptive_gpu.action_contract = lambda item, demand: (
            'shape', False, False, demand['mem_gb'] * adaptive_gpu.GIB)
        adaptive_gpu.Controller.sample = lambda self: dict(sample)
    local_dir = adaptive_cpu.local_state_base(queue.ledger().base) / 'telemetry'

    def publish(index):
        key = f'{index:064x}'
        queue.publish(action_key=key, cas_root=str(root / 'cas'),
                      checkout_root=str(root), worker_script='worker.py',
                      resources={'cpu': 1, 'mem_gb': 1, **({'gpu': 1} if gpu else {})},
                      needs_gpu=gpu)
        return key

    def telemetry():
        now = time.time()
        sample.update(sampled_unix=now, sample_id=uuid.uuid4().hex, jobs=[])
        for key in queue.ledger().held_keys():
            record = {'action_key': key, 'nonce': key[:32], 'scope_unit': key + '-scope',
                      'sampled_unix': now, 'cpu_seconds': .01 * (now - clock[0]),
                      'wall_seconds': now - clock[0] + 1., 'complete': True,
                      'memory_current_bytes': 100, 'memory_peak_bytes': 100}
            adaptive_cpu.write_json(queue.ledger().base / 'telemetry' / f'{key}.json', record)
            adaptive_cpu.write_json(local_dir / f'{key}.json', record)
            sample['jobs'].append({'action_key': key, 'nonce': record['nonce'],
                                   'scope_id': record['scope_unit'], 'complete': True})

    def claim():
        return queue.claim(capacity=capacity, cpu_tiers=tiers, adaptive_cpu=True, has_gpu=gpu)

    original = pool.PoolQueue._claim
    passes = []

    def marked(self, **kwargs):
        index = len(passes)
        with contextlib.suppress(OSError):
            os.stat(f'{MARK}/enter/{index}')
        started = time.perf_counter()
        try:
            return original(self, **kwargs)
        finally:
            passes.append(time.perf_counter() - started)
            with contextlib.suppress(OSError):
                os.stat(f'{MARK}/exit/{index}')

    # Holders: present before any marked pass, so every pass sees M of them.
    # CPU holders go through the real controller. GPU holders cannot: probe
    # admission needs a settled feedback window per holder, so eight of them
    # would take minutes and the count would depend on timing. They are
    # reserved directly with the metadata the controllers would have written.
    for index in range(args.holders):
        publish(index)
    admitted = 0
    if gpu:
        ledger = queue.ledger()
        tiers = ledger.configure_cpu_tiers(tiers)
        ledger.ensure_capacity(capacity)
        for index in range(args.holders):
            key = f'{index:064x}'
            now = time.time()
            cpu_meta = {'declared_cpu': 1, 'cost': 1., 'shape': 'shape', 'unbounded_cpu': False,
                        'measurement': False, 'admitted_unix': now, 'preferred_borrow': 0,
                        'sampled_unix': now, 'borrowing': False, 'borrowable_cpus': []}
            gpu_meta = {'declared_gpu': 1, 'exclusive': False, 'action_key': key,
                        'members_before': [], 'measurement': False, 'shape': 'shape',
                        'admitted_unix': now, 'probe': True, 'sample_id': 'seed',
                        'sampled_unix': now, 'gpu_memory_budget_bytes': adaptive_gpu.GIB,
                        'device_uuid': 'GPU-1', 'memory_domain': 'shared_system'}
            handle = ledger.begin_acquire(key, {'cpu': 1, 'mem_gb': 1, 'gpu': 1},
                                          adaptive=cpu_meta, cpu_tiers=tiers, adaptive_gpu=gpu_meta)
            assert handle, 'synthetic GPU holder could not reserve'
            ledger.commit_acquire(key, handle)
            os.rename(queue.item_path(pool.READY, key), queue.item_path(pool.CLAIMED, key))
            admitted += 1
        time.sleep(adaptive_gpu.SETTLE_S + .1)
    else:
        for _ in range(args.holders * 3):
            telemetry()
            time.sleep(0.05)
            if claim():
                admitted += 1
            if admitted == args.holders:
                break
    pool.PoolQueue._claim = marked
    results = []
    # Exactly one candidate in ready/ at every pass, so the ready scan does not
    # grow with refusals and the passes stay comparable across generations.
    key = publish(1000)
    for index in range(args.passes):
        telemetry()
        time.sleep(0.05)
        item = claim()
        results.append(bool(item))
        if item:
            queue.finish(key, status='executed', detail={})
            key = publish(1001 + index)
    json.dump({'holders_admitted': admitted, 'holders_present': len(queue.ledger().held_keys()),
               'pass_admitted': results, 'pass_wall_s': passes,
               'gpu': gpu}, open(args.child_out, 'w'))


PATH = re.compile(r'"((?:[^"\\]|\\.)*)"|<((?:[^>\\]|\\.)*)>')
TIME = re.compile(r'<(\d+\.\d+)>\s*$')
CALL = re.compile(r'^(\w+)\(')


def parse(trace, shared_root, local_root):
    passes = []
    current = None
    for line in Path(trace).read_text(errors='replace').splitlines():
        if f'{MARK}/enter/' in line:
            current = {'shared': [0, 0.], 'local': [0, 0.], 'other': [0, 0.], 'shared_calls': {}}
            continue
        if f'{MARK}/exit/' in line:
            if current is not None:
                passes.append(current)
            current = None
            continue
        if current is None:
            continue
        call = CALL.match(line)
        elapsed = TIME.search(line)
        if not call or not elapsed:
            continue
        names = [a or b for a, b in PATH.findall(line)]
        kind = 'other'
        if any(n.startswith(shared_root) for n in names):
            kind = 'shared'
        elif any(n.startswith(local_root) for n in names):
            kind = 'local'
        current[kind][0] += 1
        current[kind][1] += float(elapsed.group(1))
        if kind == 'shared':
            current['shared_calls'][call.group(1)] = current['shared_calls'].get(call.group(1), 0) + 1
    return passes


def driver(args):
    run = Path(args.run_dir) / f'run-{uuid.uuid4().hex[:8]}'
    run.mkdir(parents=True)
    queue_root = run / 'queue'
    box_state = Path(args.box_state_root) / run.name
    box_state.mkdir(parents=True)
    trace = run / 'trace.txt'
    child_out = run / 'child.json'
    command = ['strace', '-T', '-y', '-s', '4096', '-o', str(trace),
               '-e', 'trace=%file,%desc',
               sys.executable, __file__, '--child', '--queue-root', str(queue_root),
               '--box-state', str(box_state), '--holders', str(args.holders),
               '--passes', str(args.passes), '--child-out', str(child_out),
               *(['--gpu'] if args.gpu else [])]
    started = time.time()
    completed = subprocess.run(command, cwd=os.getcwd(), capture_output=True, text=True)
    report = {'command': command, 'returncode': completed.returncode,
              'stderr_tail': completed.stderr[-4000:], 'wall_s': time.time() - started,
              'queue_root': str(queue_root), 'box_state': str(box_state),
              'host': os.uname().nodename, 'cwd': os.getcwd(),
              'git_head': subprocess.run(['git', 'rev-parse', 'HEAD'], capture_output=True,
                                         text=True).stdout.strip()}
    if completed.returncode == 0:
        report['child'] = json.load(open(child_out))
        passes = parse(trace, str(queue_root), str(box_state))
        report['passes'] = passes
        n = len(passes)
        if n:
            for kind in ('shared', 'local', 'other'):
                counts = [p[kind][0] for p in passes]
                times = [p[kind][1] for p in passes]
                report[f'{kind}_calls_per_pass'] = {'mean': sum(counts) / n, 'min': min(counts),
                                                    'max': max(counts), 'total': sum(counts)}
                report[f'{kind}_seconds_per_pass'] = {'mean': sum(times) / n, 'max': max(times),
                                                      'total': sum(times)}
            merged = {}
            for p in passes:
                for name, count in p['shared_calls'].items():
                    merged[name] = merged.get(name, 0) + count
            report['shared_calls_by_syscall'] = dict(sorted(merged.items(), key=lambda x: -x[1]))
            report['section_wall_s_mean'] = sum(report['child']['pass_wall_s']) / n
    Path(args.out).write_text(json.dumps(report, indent=2))
    print(json.dumps({k: v for k, v in report.items() if k not in ('passes', 'command', 'stderr_tail')},
                     indent=2))
    if not args.keep:
        shutil.rmtree(queue_root, ignore_errors=True)
    return completed.returncode


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--child', action='store_true',
                        help='internal: run the traced workload instead of the strace driver')
    parser.add_argument('--queue-root', help='internal: private queue root for the traced child')
    parser.add_argument('--box-state', help='internal: private PRISMABUILD_BOX_STATE_ROOT for the child')
    parser.add_argument('--child-out', help='internal: where the child writes its own summary')
    parser.add_argument('--holders', type=int, default=8,
                        help='reservations admitted before the marked passes; every pass sees them')
    parser.add_argument('--passes', type=int, default=20,
                        help='marked claim passes to trace; each admits one item and finishes it')
    parser.add_argument('--gpu', action='store_true',
                        help='make every item a GPU action so both admission controllers run')
    parser.add_argument('--run-dir',
                        help='directory on the mount under test; a private queue root is created inside it')
    parser.add_argument('--box-state-root',
                        help='host-local directory for the private PRISMABUILD_BOX_STATE_ROOT')
    parser.add_argument('--out', help='JSON report path')
    parser.add_argument('--keep', action='store_true', help='keep the private queue root after the run')
    args = parser.parse_args()
    if args.child:
        return child(args)
    if not (args.run_dir and args.box_state_root and args.out):
        parser.error('--run-dir, --box-state-root and --out are required')
    return driver(args)


if __name__ == '__main__':
    raise SystemExit(main())
