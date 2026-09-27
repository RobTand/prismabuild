"""#1210: real private token ledger, scripted kernel samples, no live stores."""
import time

import pytest

from prismabuild import adaptive_cpu, pool


def sample(cpus, busy):
    return {'sampled_unix': time.time(), 'cpu_count': len(cpus), 'interval_s': 2.,
            'busy_cpus': sum(busy.values()), 'psi_some': .63,
            'per_cpu_busy': {str(c): busy.get(c, 0.) for c in cpus}}


def test_23_small_actions_pass_busy_first_free_cpu(tmp_path, monkeypatch):
    """80 CPUs, 11 held, CPU11 at .17: later free preferred cores can run."""
    q = pool.PoolQueue(tmp_path / 'queue')
    tiers = {'preferred': list(range(40)), 'fallback': list(range(40, 80))}
    ledger = q.ledger()
    ledger.configure_cpu_tiers(tiers)
    ledger.ensure_capacity({'cpu': 80, 'mem_gb': 96})
    assert ledger.acquire('holder', {'cpu': 11})
    monkeypatch.setattr(adaptive_cpu, 'action_identity', lambda _: ('shape', False))
    monkeypatch.setattr(adaptive_cpu.Controller, 'sample',
                        lambda _: sample(range(80), {**dict.fromkeys(range(11), 1.), 11: .17}))
    allocations = []
    for i in range(23):
        key = f'{i + 1:064x}'
        q.publish(action_key=key, cas_root=str(tmp_path / 'cas'), checkout_root=str(tmp_path),
                  worker_script='worker.py', resources={'cpu': 1, 'mem_gb': 1})
        claim = q.claim(capacity={'cpu': 80, 'mem_gb': 96}, cpu_tiers=tiers, adaptive_cpu=True)
        assert claim is not None, f'one busy free CPU stranded action {i}'
        allocations += claim['cpu_allocation']['preferred']
        assert claim['cpu_allocation']['fallback'] == []
    assert allocations == list(range(12, 35))
    assert (ledger.free_dir / 'cpu-0011').exists()


def test_control_ticks_do_not_become_foreign_load(tmp_path, monkeypatch):
    """Only verified non-migrating task ticks are removed, not whole CPU load."""
    ledger = pool.ResourceLedger(tmp_path / 'ledger')
    controller = adaptive_cpu.Controller(ledger, {'preferred': [11, 12], 'fallback': []})
    hosts = iter([
        {'sampled_unix': 100., 'cpus': {'11': [20, 100], '12': [0, 100]}, 'psi_total': 0},
        {'sampled_unix': 102., 'cpus': {'11': [54, 300], '12': [0, 300]}, 'psi_total': 1260000},
    ])
    tasks = iter([{'7': {'start': 5, 'cpu': 11, 'migrations': 0, 'ticks': t}}
                  for t in (0, 0, 30, 30)])
    monkeypatch.setattr(adaptive_cpu, 'counters', lambda _: next(hosts))
    # The collector verifies identity; the sampler brackets host counters with it.
    monkeypatch.setattr(adaptive_cpu, 'control_plane_counters', lambda _: next(tasks), raising=False)
    assert controller.sample() == {}
    observed = controller.sample()
    assert observed['per_cpu_busy']['11'] == pytest.approx(.17)  # raw load retained
    assert observed['control_plane_busy']['11'] == pytest.approx(.15)
    assert observed['foreign_per_cpu_busy']['11'] == pytest.approx(.02)


def _proc(tmp_path, *, marked=True, script='worker_loop.py'):
    root = tmp_path / 'runtime'
    exe = root / 'tools' / 'fleet' / script
    exe.parent.mkdir(parents=True)
    exe.write_text('# fixture')
    proc = tmp_path / 'proc'
    pid = proc / '7'
    thread = pid / 'task' / '7'
    thread.mkdir(parents=True)
    (pid / 'cmdline').write_bytes(f'/usr/bin/python3\0{exe}\0'.encode())
    (pid / 'environ').write_bytes(b'PRISMABUILD_SUPERVISED_WORKER=fixture\0' if marked else b'')
    fields = ['0'] * 40
    fields[0], fields[11], fields[12], fields[19], fields[36] = 'S', '12', '3', '5', '11'
    for p in (pid, thread):
        (p / 'stat').write_text('7 (worker with ) spaces) ' + ' '.join(fields))
    (thread / 'sched').write_text('se.nr_migrations : 0\n')
    return proc, root


@pytest.mark.parametrize('marked,script,expected', [(True, 'worker_loop.py', True),
    (True, 'tier_loop.py', True), (True, 'prewarm_loop.py', True), (True, 'pbmetrics.py', True),
    (False, 'worker_loop.py', False), (True, 'workload.py', False)])
def test_control_identity_is_not_an_inherited_environment_exemption(tmp_path, marked, script, expected):
    from prismabuild.control_cpu import control_plane_counters
    proc, root = _proc(tmp_path, marked=marked, script=script)
    records = control_plane_counters({11, 12}, proc_root=proc, runtime_root=root, hostname='fixture')
    assert bool(records) is expected


@pytest.mark.parametrize('foreign,expected', [(.02, 11), (.08, 12), (float('nan'), None), (-.1, None)])
def test_control_accounting_preserves_remaining_foreign_work(tmp_path, monkeypatch, foreign, expected):
    q = pool.PoolQueue(tmp_path / 'queue')
    observed = sample([11, 12], {11: .17})
    observed['foreign_per_cpu_busy'] = {'11': foreign, '12': 0.}
    monkeypatch.setattr(adaptive_cpu, 'action_identity', lambda _: ('shape', False))
    monkeypatch.setattr(adaptive_cpu.Controller, 'sample', lambda _: observed)
    q.publish(action_key='a' * 64, cas_root=str(tmp_path / 'cas'), checkout_root=str(tmp_path),
              worker_script='worker.py', resources={'cpu': 1, 'mem_gb': 1})
    claim = q.claim(capacity={'cpu': 2, 'mem_gb': 2},
                    cpu_tiers={'preferred': [11, 12], 'fallback': []}, adaptive_cpu=True)
    if expected is None:
        assert claim is None
    else:
        assert claim and claim['cpu_allocation']['preferred'] == [expected]


@pytest.mark.parametrize('changed', [{'migrations': 1}, {'start': 6}, {'cpu': 12}, {'ticks': -1}, {'ticks': 1000}])
def test_unproven_control_interval_is_not_subtracted(tmp_path, monkeypatch, changed):
    from prismabuild.control_cpu import attributed_ticks
    previous = {'7': {'start': 5, 'cpu': 11, 'migrations': 0, 'ticks': 0}}
    current = {'7': {**previous['7'], 'ticks': 30, **changed}}
    assert attributed_ticks(previous, current, {'11': 34, '12': 0}) == {'11': 0, '12': 0}


def test_acquire_cannot_replace_a_lost_eligible_token_with_busy_cpu(tmp_path):
    ledger = pool.ResourceLedger(tmp_path / 'ledger')
    tiers = {'preferred': [11, 12], 'fallback': [13]}
    ledger.configure_cpu_tiers(tiers)
    ledger.ensure_capacity({'cpu': 3, 'mem_gb': 3})
    assert ledger.free_cpu_allocation(1, tiers, eligible_cpus=[12]) == [12]
    # A competing old loop took the proven token; never take CPU 11 or 13 instead.
    stolen = ledger.held_dir / 'other'
    stolen.mkdir()
    (ledger.free_dir / 'cpu-0001').rename(stolen / 'cpu-0001')
    assert ledger.begin_acquire('a' * 64, {'cpu': 1, 'mem_gb': 1}, cpu_tiers=tiers,
                                adaptive={'eligible_cpus': [12], 'borrowing': False}) is None
    assert ledger.available() == {'cpu': 2, 'mem_gb': 3}


@pytest.mark.parametrize('fault', ['wrong_host', 'duplicate_mark', 'outside_root', 'missing_sched', 'bad_stat'])
def test_control_collector_refuses_unproven_identity(tmp_path, fault):
    from prismabuild.control_cpu import control_plane_counters
    proc, root = _proc(tmp_path)
    if fault == 'wrong_host':
        (proc / '7' / 'environ').write_bytes(b'PRISMABUILD_SUPERVISED_WORKER=other\0')
    elif fault == 'duplicate_mark':
        (proc / '7' / 'environ').write_bytes(b'PRISMABUILD_SUPERVISED_WORKER=other\0PRISMABUILD_SUPERVISED_WORKER=fixture\0')
    elif fault == 'outside_root':
        root = tmp_path / 'unrelated'
    elif fault == 'missing_sched':
        (proc / '7' / 'task' / '7' / 'sched').unlink()
    else:
        (proc / '7' / 'stat').write_text('not a stat record')
    assert control_plane_counters({11, 12}, proc_root=proc, runtime_root=root, hostname='fixture') == {}


@pytest.mark.parametrize('busy,expected', [({8: .2}, [10]), ({8: .2, 10: .2}, [2]),
                                         ({8: .2, 10: .2, 2: .2, 4: .2}, None)])
def test_skip_busy_uses_ordinal_mapping_and_preferred_tier(tmp_path, monkeypatch, busy, expected):
    q = pool.PoolQueue(tmp_path / 'queue')
    tiers = {'preferred': [8, 10], 'fallback': [2, 4]}
    monkeypatch.setattr(adaptive_cpu, 'action_identity', lambda _: ('shape', False))
    monkeypatch.setattr(adaptive_cpu.Controller, 'sample', lambda _: sample([8, 10, 2, 4], busy))
    q.publish(action_key='a' * 64, cas_root=str(tmp_path / 'cas'), checkout_root=str(tmp_path),
              worker_script='worker.py', resources={'cpu': 1, 'mem_gb': 1})
    claim = q.claim(capacity={'cpu': 4, 'mem_gb': 1}, cpu_tiers=tiers, adaptive_cpu=True)
    if expected is None:
        assert claim is None
    else:
        assert claim
        assert claim['cpu_allocation']['preferred'] + claim['cpu_allocation']['fallback'] == expected


@pytest.mark.parametrize('measurement,need,busy', [(False, 1, {11: 1., 12: 1.}),
                                                 (True, 1, {11: .9}), (False, 2, {11: .9})])
def test_control_attribution_never_discounts_raw_saturation_or_isolation(tmp_path, monkeypatch, measurement, need, busy):
    q = pool.PoolQueue(tmp_path / 'queue')
    observed = sample([11, 12], busy)
    observed['foreign_per_cpu_busy'] = {'11': 0., '12': 0.}
    monkeypatch.setattr(adaptive_cpu, 'action_identity', lambda _: ('shape', measurement))
    monkeypatch.setattr(adaptive_cpu.Controller, 'sample', lambda _: observed)
    q.publish(action_key='a' * 64, cas_root=str(tmp_path / 'cas'), checkout_root=str(tmp_path),
              worker_script='worker.py', resources={'cpu': need, 'mem_gb': 1})
    assert q.claim(capacity={'cpu': 2, 'mem_gb': 1},
                   cpu_tiers={'preferred': [11, 12], 'fallback': []}, adaptive_cpu=True) is None


def test_control_interval_uses_inner_tick_bounds():
    from prismabuild.control_cpu import attributed_ticks
    old = {'7': {'start': 5, 'cpu': 11, 'migrations': 0, 'ticks': 10, 'end_ticks': 12}}
    now = {'7': {**old['7'], 'ticks': 20, 'end_ticks': 25}}
    assert attributed_ticks(old, now, {'11': 20}) == {'11': 8}
    assert attributed_ticks([], now, {'11': 20}) == {'11': 0}


@pytest.mark.parametrize('fault', ['pid_reused', 'exec'])
def test_control_identity_is_rechecked_after_thread_census(tmp_path, monkeypatch, fault):
    from pathlib import Path
    from prismabuild.control_cpu import control_plane_counters
    proc, root = _proc(tmp_path)
    real_read = Path.read_text

    def read(path, *args, **kwargs):
        value = real_read(path, *args, **kwargs)
        if path == proc / '7' / 'task' / '7' / 'sched':
            if fault == 'exec':
                (proc / '7' / 'cmdline').write_bytes(b'python3\0workload.py\0')
            else:
                stat = proc / '7' / 'stat'
                text = real_read(stat)
                close = text.rfind(')')
                fields = text[close + 1:].split()
                fields[19] = '6'
                stat.write_text(text[:close + 1] + ' ' + ' '.join(fields))
        return value

    monkeypatch.setattr(Path, 'read_text', read)
    assert control_plane_counters({11, 12}, proc_root=proc, runtime_root=root, hostname='fixture') == {}
