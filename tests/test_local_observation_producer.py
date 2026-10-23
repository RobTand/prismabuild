"""Local producer regressions; ordinary files and fake broker/GPU authority only."""
from contextlib import nullcontext
import ctypes
import fcntl
import os
import errno
import json
import threading
import time
from types import SimpleNamespace

from prismabuild import adaptive_cpu, resource_observation
from test_resource_broker import authority, monitored  # noqa: F401 (fixture dependencies)


def eventually(predicate, timeout=3.0):
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, 'producer did not reach expected state'
        time.sleep(.01)


def test_degraded_inotify_init_retries_and_restores_coverage(tmp_path, monkeypatch):
    path = tmp_path / 'sample.json'
    path.write_text('before')
    libc = ctypes.CDLL(None, use_errno=True)
    unavailable = threading.Event()
    unavailable.set()

    def init(flags):
        if unavailable.is_set():
            ctypes.set_errno(errno.EMFILE)
            return -1
        return libc.inotify_init1(flags)

    # Instance attribute, not a class method: ctypes-style callables must not
    # acquire an implicit self, and must accept argtypes/restype assignments.
    fake_libc = SimpleNamespace(inotify_init1=init,
                                inotify_add_watch=libc.inotify_add_watch,
                                inotify_rm_watch=libc.inotify_rm_watch)
    monkeypatch.setattr(resource_observation.ctypes, 'CDLL', lambda *a, **k: fake_libc)
    monkeypatch.setattr(resource_observation, '_RECOVERY_S', .05)
    source = resource_observation.LocalObservationSource(paths={'sample': path}, pidfds={})
    try:
        cursor = source.generation
        assert cursor > 0
        assert 'inotify' in source.diagnostics['errors']
        eventually(lambda: source.diagnostics['repairs'] >= 3)
        assert source.wait(cursor, 0) == (cursor, set())  # unchanged failure is quiet

        unavailable.clear()
        eventually(lambda: 'inotify' not in source.diagnostics['errors'])
        generation, labels = source.wait(cursor, 1)
        assert generation > cursor and labels == {'sample'}
        assert source.diagnostics['armed_pathnames'] > 0
        events = source.diagnostics['inotify_events']
        path.write_text('after recovery')
        eventually(lambda: source.diagnostics['inotify_events'] > events)
        generation_after, labels = source.wait(generation, 1)
        assert generation_after > generation and labels == {'sample'}
    finally:
        source.close()
    assert not source._thread.is_alive()


def test_broker_notification_enforces_while_gpu_host_sampler_is_blocked(monitored, monkeypatch):
    a, backend, rows, gpu, monitor = monitored
    entered, release, stopped = threading.Event(), threading.Event(), threading.Event()
    original_collect, original_stop = gpu.collect, backend.stop

    def blocked_collect(*args, **kwargs):
        entered.set()
        release.wait()
        return original_collect(*args, **kwargs)

    def stop(scope):
        original_stop(scope)
        stopped.set()

    monkeypatch.setattr(gpu, 'collect', blocked_collect)
    monkeypatch.setattr(backend, 'stop', stop)
    monitor.start()
    try:
        assert entered.wait(3), 'numeric sampler never entered fake GPU collection'
        _, record, path = rows[0]
        (path / 'memory.events.local').write_text('oom 1\n')
        assert stopped.wait(3), 'notification enforcement waited for GPU collection'
        assert not release.is_set()
        assert backend.stopped == [record['scope_id']]
        assert a.records[record['scope_id']]['stop_reason'] == 'memory_limit_oom'
        assert monitor.thread.is_alive()
    finally:
        release.set()
        monitor.close()
    assert not monitor.thread.is_alive()
    assert not monitor.observer_thread.is_alive()


def test_population_hint_rereads_scope_without_releasing(monitored):
    authority, backend, rows, gpu, monitor = monitored
    _, record, path = rows[0]
    scope = record['scope_id']
    backend.groups[scope]['populated'] = True
    (path / 'cgroup.events').write_text('populated 1\n')
    monitor.interval_s = 20
    monitor.start()
    try:
        assert monitor.scope_observations[scope]['empty'] is False
        backend.groups[scope]['populated'] = False
        (path / 'cgroup.events').write_text('populated 0\n')
        eventually(lambda: monitor.scope_observations[scope]['empty'], timeout=1.5)
        assert scope in authority.records
        assert scope in backend.groups
        assert backend.stopped == []
    finally:
        monitor.close()


def test_host_sampler_reads_only_snapshot_and_closes(tmp_path, monkeypatch):
    entered = threading.Event()
    closed = threading.Event()
    source = SimpleNamespace(close=closed.set)
    controller = SimpleNamespace(base=tmp_path)
    monkeypatch.setattr(adaptive_cpu, 'Controller', lambda *a: controller)
    monkeypatch.setattr(resource_observation, 'LocalObservationSource', lambda **k: source)

    def parked_producer(self):
        entered.set()
        self._stop.wait()

    # Isolate the reader from collection: there is deliberately no controller
    # sample method to call, and no proc census or admission gate in this test.
    monkeypatch.setattr(adaptive_cpu.HostSampler, '_run', parked_producer)
    sampler = adaptive_cpu.HostSampler(object(), {})
    try:
        assert entered.wait(3)
        assert sampler.sample() == {}
        observation = {'sampled_unix': 1, 'busy_cpus': 2.0}
        snapshot = tmp_path / 'cpu-sample.json'
        snapshot.write_text(json.dumps({'observation': observation}))
        assert sampler.sample() == observation  # retain the stale timestamp
        assert sampler.sample() == observation
        assert json.loads(snapshot.read_text()) == {'observation': observation}
    finally:
        sampler.close()
    sampler.close()  # idempotent shutdown
    assert closed.is_set()
    assert sampler._stop.is_set()
    assert not sampler._thread.is_alive()


def test_host_sampler_follower_never_collects_and_takes_over(tmp_path, monkeypatch):
    sampled = threading.Event()
    controller = SimpleNamespace(base=tmp_path, ledger=object(), sample=sampled.set)
    monkeypatch.setattr(adaptive_cpu, 'Controller', lambda *a: controller)
    monkeypatch.setattr(adaptive_cpu, 'AdmissionGate',
                        lambda *a: SimpleNamespace(locked=nullcontext))
    monkeypatch.setattr(resource_observation, 'LocalObservationSource',
                        lambda **k: SimpleNamespace(close=lambda: None))
    owner = os.open(tmp_path / 'sampler.lock', os.O_CREAT | os.O_RDWR, 0o600)
    fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
    sampler = adaptive_cpu.HostSampler(object(), {})
    try:
        assert not sampled.wait(.15), 'follower performed a host census'
        os.close(owner)
        owner = None
        assert sampled.wait(2), 'follower did not recover after owner exit'
    finally:
        if owner is not None:
            os.close(owner)
        sampler.close()


def test_capacity_producer_brackets_numeric_and_ram_policy_reads(monkeypatch):
    from prismabuild import box_capacity as bc

    events = []
    holdings = iter([{'mem_gb': 40, 'cpu': 2}, {'mem_gb': 16, 'cpu': 4}])

    def held():
        events.append('held')
        return next(holdings)

    def memory():
        events.append('memory')
        return 32

    def load():
        events.append('load')
        return 2.5

    def roof():
        events.append('roof')
        return (64, 'measured')

    monkeypatch.setattr(bc.time, 'time', lambda: 100.)
    monkeypatch.setattr(bc, 'mem_available_gb', memory)
    monkeypatch.setattr(bc, 'run_queue', load)
    sample = bc.collect_host_capacity(held, roof)

    assert events == ['held', 'memory', 'load', 'roof', 'held']
    assert sample == {
        'sampled_unix': 100., 'mem_gb': 32, 'load1': 2.5,
        'held': {'mem_gb': 16, 'cpu': 2},
        'held_before': {'mem_gb': 40, 'cpu': 2},
        'held_after': {'mem_gb': 16, 'cpu': 4},
        'mem_roof': (64, 'measured'), 'mem_roof_enabled': True,
    }


def test_capacity_consumer_intersects_current_holds_without_proc_reads(monkeypatch):
    from prismabuild import box_capacity as bc
    from test_box_capacity import DECLARED

    monkeypatch.setattr(bc.time, 'time', lambda: 100.)
    monkeypatch.setattr(bc, 'mem_available_gb', lambda: 32)
    monkeypatch.setattr(bc, 'run_queue', lambda: 2.5)
    holdings = iter([{'mem_gb': 40}, {'mem_gb': 16}])
    sample = bc.collect_host_capacity(lambda: next(holdings))

    def forbidden():
        raise AssertionError('consumer performed a numeric host read')

    monkeypatch.setattr(bc, 'mem_available_gb', forbidden)
    monkeypatch.setattr(bc, 'run_queue', forbidden)
    observer = bc.CapacityObserver(samples=1, host_sample=lambda: sample)
    current = {'mem_gb': 8}
    offer = observer.offer(DECLARED, lambda: current, gpu_sample=None)
    assert offer['mem_gb'] == 8 + 32 - bc.MEMORY_MARGIN_GB
    assert offer['cpu'] == DECLARED['cpu']
    assert observer.last.detail['load1'] == 2.5
    assert observer.last.detail['held_moved_during_read'] == {
        'before': {'mem_gb': 40}, 'after': {'mem_gb': 16}}

    # A later acquisition cannot be paired with old free bytes either.
    current['mem_gb'] = 48
    assert observer.offer(DECLARED, lambda: current, gpu_sample=None)['mem_gb'] == (
        16 + 32 - bc.MEMORY_MARGIN_GB)
    current.clear()
    assert observer.offer(DECLARED, lambda: current, gpu_sample=None)['mem_gb'] == (
        32 - bc.MEMORY_MARGIN_GB)
    assert sample['held'] == {'mem_gb': 16}


def test_capacity_stale_gap_clears_window_and_equal_recovery_keeps_timestamp(monkeypatch):
    from prismabuild import box_capacity as bc
    from test_box_capacity import DECLARED

    clock = [100.]
    monkeypatch.setattr(bc.time, 'time', lambda: clock[0])
    sample = {'sampled_unix': 100., 'mem_gb': 100, 'load1': 0., 'held': {}}
    observer = bc.CapacityObserver(host_sample=lambda: sample, sample_max_age_s=5.)
    assert observer.offer(DECLARED, {}, gpu_sample=None)['mem_gb'] == DECLARED['mem_gb']

    clock[0] = 106.
    assert observer.offer(DECLARED, {}, gpu_sample=None)['mem_gb'] == 0
    assert observer.last.detail['host_capacity_trusted'] is False
    assert observer.last.detail['host_capacity_sampled_unix'] == 100.
    assert observer.last.detail['host_capacity_error'] == 'missing_stale_or_invalid_sample'

    # Fresh evidence with the same original capacity recovers immediately.
    sample['sampled_unix'] = 106.
    assert observer.offer(DECLARED, {}, gpu_sample=None)['mem_gb'] == DECLARED['mem_gb']
    assert observer.last.detail['host_capacity_trusted'] is True
    clock[0] = 107.
    assert observer.offer(DECLARED, {}, gpu_sample=None)['mem_gb'] == DECLARED['mem_gb']
    assert observer.last.detail['observed_unix'] == 106.
    assert observer.last.detail['host_capacity_sampled_unix'] == 106.

    # A fresh low reading after another stale gap must not resurrect the high window.
    clock[0] = 112.
    assert observer.offer(DECLARED, {}, gpu_sample=None)['mem_gb'] == 0
    sample.update(sampled_unix=112., mem_gb=24)
    assert observer.offer(DECLARED, {}, gpu_sample=None)['mem_gb'] == 24 - bc.MEMORY_MARGIN_GB


def test_capacity_ram_roof_uses_policy_snapshot_not_consumer_callback(monkeypatch):
    from prismabuild import box_capacity as bc
    from test_box_capacity import DECLARED

    monkeypatch.setattr(bc.time, 'time', lambda: 100.)
    monkeypatch.setattr(bc, 'mem_available_gb', lambda: 100)
    monkeypatch.setattr(bc, 'run_queue', lambda: 0.)
    sample = bc.collect_host_capacity(lambda: {}, lambda: (48, 'measured'))
    # Snapshot serialization turns the producer's roof tuple into a list.
    sample = json.loads(json.dumps(sample))

    def forbidden():
        raise AssertionError('consumer re-read RAM policy or host instruments')

    monkeypatch.setattr(bc, 'mem_available_gb', forbidden)
    monkeypatch.setattr(bc, 'run_queue', forbidden)
    observer = bc.CapacityObserver(samples=1, host_sample=lambda: sample, mem_roof=forbidden)
    assert observer.offer(DECLARED, {}, gpu_sample=None)['mem_gb'] == 48
    assert observer.last.detail['mem_roof_gib'] == 48
    assert observer.last.detail['mem_roof_source'] == 'measured'

    sample['mem_roof'] = [None, 'fallback:policy_unreadable']
    assert observer.offer(DECLARED, {}, gpu_sample=None)['mem_gb'] == DECLARED['mem_gb']
    assert observer.last.detail['mem_roof_gib'] == DECLARED['mem_gb']
    assert observer.last.detail['mem_roof_source'] == 'fallback:policy_unreadable'

    # A producer without policy collection cannot satisfy a configured roof consumer.
    sample['mem_roof_enabled'] = False
    assert observer.offer(DECLARED, {}, gpu_sample=None)['mem_gb'] == 0
    assert observer.last.detail['host_capacity_trusted'] is False
