"""Checkout-only #2062 Python-read experiment; no live commands or public CLI.

Instrumentation is process-global while running: invoke in an isolated test,
not concurrently with application threads. All patches are restored on exit.
"""
from collections import Counter
from contextlib import ExitStack
import importlib.util
import os
from pathlib import Path
import subprocess
from types import SimpleNamespace
from unittest.mock import patch

from prismabuild import gpu_capacity, gpu_memory
from prismabuild.digest_primitives import raw_sha256


_SOURCES = ('tools/fleet/resource_broker.py', 'src/prismabuild/gpu_memory.py',
            'src/prismabuild/gpu_capacity.py')


def run_synthetic(root, populations=(0, 8, 64), cycles=2):
    """Return actual fixture read attempts/results, split by workload and phase.

    root must be a fresh private directory. Time and vendor stdout are injected;
    filesystem reads, collector logic, monitor and admin dispatch remain real.
    """
    root = Path(root).resolve()
    if cycles < 1 or any(type(n) is not int or n < 0 for n in populations):
        raise ValueError('positive cycles and nonnegative PID populations required')
    root.mkdir(parents=True, exist_ok=False)
    checkout = Path(__file__).resolve().parents[2]
    sources = {name: raw_sha256((checkout / name).read_bytes()) for name in _SOURCES}
    spec = importlib.util.spec_from_file_location('_baseline_broker', Path(checkout, 'tools/fleet/resource_broker.py'))
    broker = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(broker)
    cases = []
    current = None

    def new_case(phase, scenario, population, active, iterations):
        nonlocal current
        current = dict(phase=phase, scenario=scenario, unrelated_pids=population,
                       active_scopes=active, iterations=iterations, reads=[], subprocesses=[])
        cases.append(current)
        return Counter(), Counter()

    def file_class(path):
        if 'installed' in path.parts:
            return 'installed_privileged'
        return {'stat': 'proc_identity', 'cgroup': 'proc_membership',
                'meminfo': 'host_memory', 'memory': 'memory_pressure',
                'cpu': 'cpu_pressure', 'memory.current': 'scope_memory',
                'memory.events': 'oom_hierarchical',
                'memory.events.local': 'oom_local',
                'cgroup.events': 'scope_population',
                'maintenance.json': 'maintenance_state'}.get(path.name, 'authority_state')

    def instrument(original):
        def measured(path, *args, **kwargs):
            try:
                relative = path.relative_to(root).as_posix()
            except ValueError:
                return original(path, *args, **kwargs)
            counts[relative, 'attempts'] += 1
            result = original(path, *args, **kwargs)
            counts[relative, 'reads'] += 1
            counts[relative, 'bytes'] += len(result.encode('utf-8') if isinstance(result, str) else result)
            return result
        return measured

    def finish():
        for name in sorted({key[0] for key in counts}):
            current['reads'].append(dict(path=name, file_class=file_class(Path(name)),
                                        **{field: counts[name, field] for field in ('attempts', 'reads', 'bytes')}))
        for (argv, outcome, size), calls in sorted(children.items()):
            current['subprocesses'].append(dict(argv=list(argv), calls=calls, outcome=outcome,
                                               stdout_bytes=size * calls, measurement_kind='stubbed_not_traced'))

    def vendor(argv, **kwargs):
        device_query = any(arg.startswith('--query-gpu=') for arg in argv)
        stdout = ''
        code = 1 if scenario == 'gpu_error' else 0
        if scenario == 'gpu_present':
            if device_query:
                values = {'uuid': 'GPU-fixture', 'name': 'NVIDIA A100',
                          'memory.total': '40960', 'memory.free': '40959', 'memory.used': '1'}
                stdout = ', '.join(values.get(field, '0' if field.startswith('clocks_event_reasons.') else '100')
                                   for field in gpu_capacity.FIELDS) + '\n'
            else:
                stdout = '42, GPU-fixture, 1\n'
        outcome = 'exit_' + str(code)
        children[tuple(argv), outcome, len(stdout.encode('utf-8'))] += 1
        return subprocess.CompletedProcess(argv, code, stdout, '')

    def put(path, text):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding='utf-8')

    with ExitStack() as stack:
        stack.enter_context(patch.object(Path, 'read_text', instrument(Path.read_text)))
        stack.enter_context(patch.object(Path, 'read_bytes', instrument(Path.read_bytes)))
        stack.enter_context(patch.object(subprocess, 'run', vendor))
        stack.enter_context(patch.object(subprocess, 'Popen', side_effect=AssertionError('unexpected process')))
        # Both modules share time; fix rather than advance it to remove load-dependent deadlines.
        stack.enter_context(patch('time.time', return_value=1700000000.0))
        stack.enter_context(patch('time.monotonic', return_value=100.0))
        # Disable hardware discovery fallback, not its parser or NVIDIA query.
        stack.enter_context(patch.object(gpu_capacity, 'ROCMINFO', str(root / 'absent-rocminfo')))
        for scenario in ('empty', 'no_gpu', 'gpu_error', 'gpu_present'):
            for population in populations:
                base = root / scenario / str(population)
                proc, cgroup = base / 'proc', base / 'cgroup'
                active = 0 if scenario == 'empty' else 1
                counts, children = new_case('setup', scenario, population, active, 1)
                put(proc / 'meminfo', 'MemTotal: 8388608 kB\nMemAvailable: 7340032 kB\n')
                psi = 'some avg10=0.00 avg60=0.00 avg300=0.00 total=0\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=0\n'
                put(proc / 'pressure/memory', psi)
                put(proc / 'pressure/cpu', psi)
                scope = 'prismabuild-job' + 'a' * 32 + '.slice'
                path = cgroup / 'prismabuild.slice' / scope
                for pid in ([42] if active else []) + list(range(1000, 1000 + population)):
                    put(proc / str(pid) / 'stat', f'{pid} (fixture) S ' + '0 ' * 18 + '123\n')
                    group = '/prismabuild.slice/' + scope if pid == 42 else '/unrelated'
                    put(proc / str(pid) / 'cgroup', '0::' + group + '\n')
                scopes = []
                if active:
                    put(path / 'memory.current', '1048576\n')
                    put(path / 'memory.events', 'oom 0\noom_kill 0\n')
                    put(path / 'memory.events.local', 'oom 0\noom_kill 0\n')
                    put(path / 'cgroup.events', 'populated 1\nfrozen 0\n')
                    scopes = [gpu_memory.Scope(scope, path, 1024**3, gpu_budget_bytes=1024**3)]
                backend = SimpleNamespace(path=lambda name: cgroup / 'prismabuild.slice' / name,
                                          healthy=lambda: True, inventory=lambda: {})
                authority = broker.Authority(base / 'state/scopes', os.geteuid(), backend,
                                             max_memory_bytes=8 * 1024**3)
                for name in ('broker', 'gpu_memory', 'gpu_capacity'):
                    installed = base / 'installed' / (name + '.py')
                    put(installed, '# synthetic installed ' + name + '\n')
                    authority.installation_paths[name] = installed
                    authority.installed_sha256[name] = raw_sha256(installed.read_bytes())
                finish()
                counts, children = new_case('collector', scenario, population, active, cycles)
                for _ in range(cycles):
                    gpu_memory.collect(scopes, proc_root=proc, cgroup_root=cgroup)
                finish()
                # Adapters supply existing root seams; they do not replace collection logic.
                def memory_collect(scopes, **options):
                    return gpu_memory.collect(scopes, proc_root=proc, cgroup_root=cgroup, **options)

                def capacity_collect(snapshot, records, **options):
                    return gpu_capacity.collect(snapshot, records, proc_root=proc, **options)

                memory_adapter = SimpleNamespace(Guard=gpu_memory.Guard, Scope=gpu_memory.Scope, collect=memory_collect)
                capacity_adapter = SimpleNamespace(devices=gpu_capacity.devices, collect=capacity_collect)
                if active:
                    info = path.stat()
                    authority.records[scope] = dict(action_key='a' * 64, nonce='b' * 32, token='c' * 64,
                        memory_max_bytes=1024**3, gpu_memory_max_bytes=1024**3,
                        cgroup_identity=[info.st_dev, info.st_ino], memory_oom_kill_baseline=0,
                        memory_oom_local_baseline=0)
                monitor = broker.ResourceMonitor(authority, memory_adapter, capacity_module=capacity_adapter)
                counts, children = new_case('monitor', scenario, population, active, cycles)
                try:
                    for _ in range(cycles):
                        monitor.poll_once()
                finally:
                    monitor.close()
                finish()
                # Request-only workload has no scopes: isolate installed-file rereads
                # from the backend's inventory and unrelated authority reconciliation.
                authority.records.clear()
                counts, children = new_case('request', scenario, population, 0, cycles)
                # Fixtures are intentionally user-owned, not a claimed privileged install.
                # Bypass only the ownership prerequisite, with exact fixture allowlisting.
                def fixture_trust(candidate):
                    if candidate not in authority.installation_paths.values():
                        raise ValueError('not an installed fixture')
                    return candidate

                with patch.object(broker, '_trusted_file', fixture_trust):
                    for _ in range(cycles):
                        result = authority.admin(0, {'op': 'maintenance_status'})
                        if not result['health']:
                            raise AssertionError(result['errors'])
                finish()
    return dict(measurement_kind='synthetic_python_file_reads', sources=sources, cases=cases)
