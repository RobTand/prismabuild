"""CPU bookkeeping must not put a remote writer under host admission."""
from pathlib import Path
import subprocess
import sys
import time

import pytest

from prismabuild import adaptive_cpu, pool
from prismabuild import adaptive_snapshot


@pytest.fixture(autouse=True)
def reap_publishers():
    yield
    # Tests use only temporary local destinations. Leave no asynchronous
    # diagnostic process writing a fixture after its test has completed.
    for child in adaptive_snapshot._children:
        assert child.wait(timeout=10) == 0
    adaptive_snapshot._children.clear()


def test_cpu_sample_does_not_write_shared_bookkeeping_under_admission(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / 'queue')
    controller = adaptive_cpu.Controller(queue.ledger(), {'preferred': [0], 'fallback': []})
    shared = queue.ledger().base / 'adaptive'
    original = adaptive_cpu.write_json

    def reject_remote(path, value):
        if Path(path).is_relative_to(shared):
            pytest.fail('remote adaptive bookkeeping write while holding admission')
        return original(path, value)

    monkeypatch.setattr(adaptive_cpu, 'write_json', reject_remote)
    monkeypatch.setattr(adaptive_cpu, 'counters', lambda cpus: {
        'sampled_unix': 10., 'cpus': {'0': [10, 100]}, 'psi_total': 0})
    with controller.locked():
        assert controller.sample() == {}


def test_cpu_intervals_are_shared_by_new_controllers_but_not_other_ledgers(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / 'queue')
    tiers = {'preferred': [0], 'fallback': []}
    samples = iter([
        {'sampled_unix': 10., 'cpus': {'0': [10, 100]}, 'psi_total': 0},
        {'sampled_unix': 12., 'cpus': {'0': [30, 200]}, 'psi_total': 200000},
        {'sampled_unix': 12., 'cpus': {'0': [30, 200]}, 'psi_total': 200000},
    ])
    monkeypatch.setattr(adaptive_cpu, 'counters', lambda cpus: next(samples))
    first = adaptive_cpu.Controller(queue.ledger(), tiers)
    borrow = {'borrowing': True, 'sampled_unix': 10.}
    with first.locked():
        assert first.sample() == {}
        first.admitted(borrow)
    second = adaptive_cpu.Controller(queue.ledger(), tiers)
    with second.locked():
        assert second.sample()['busy_cpus'] == pytest.approx(.2)
    assert adaptive_cpu.read_json(second.base / 'last-borrow.json') == {
        'sampled_unix': 10., 'borrow_id': borrow['borrow_id']}
    other = adaptive_cpu.Controller(pool.PoolQueue(tmp_path / 'other').ledger(), tiers)
    with other.locked():
        assert other.sample() == {}
    assert first.base == second.base != other.base
    assert first.base.is_relative_to(adaptive_cpu.BOX_STATE_ROOT)


def test_blocked_publisher_does_not_hold_admission_or_spawn_siblings(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / 'queue')
    tiers = {'preferred': [0], 'fallback': []}
    controller = adaptive_cpu.Controller(queue.ledger(), tiers)
    shared = queue.ledger().base / 'adaptive'
    real_popen = subprocess.Popen
    # Inject the blocked remote operation in the exec child, keeping the real
    # publication entry, inherited descriptor and cross-process flock protocol.
    script = f"""
import sys, time
from pathlib import Path
sys.path.insert(0, {str(Path(adaptive_snapshot.__file__).parent.parent)!r})
from prismabuild import adaptive_snapshot as snap
local, shared = Path(sys.argv[1]), Path(sys.argv[2])
original = snap._write
def write(path, value):
    if path.is_relative_to(shared):
        (local / 'blocked').write_text('remote write reached')
        while not (local / 'release').exists():
            time.sleep(.01)
    return original(path, value)
snap._write = write
raise SystemExit(snap.main(sys.argv[1:]))
"""

    def launch(argv, **kwargs):
        assert kwargs['close_fds'] and len(kwargs['pass_fds']) == 1
        return real_popen([argv[0], '-c', script, *argv[2:]], **kwargs)

    monkeypatch.setattr(adaptive_snapshot.subprocess, 'Popen', launch)
    monkeypatch.setattr(adaptive_snapshot, 'MIN_PUBLISH_INTERVAL_S', 0.)
    monkeypatch.setattr(adaptive_cpu, 'counters', lambda cpus: {
        'sampled_unix': 10., 'cpus': {'0': [10, 100]}, 'psi_total': 0})
    with controller.locked():
        controller.sample()
    child = adaptive_snapshot._children[-1]
    try:
        deadline = time.monotonic() + 10
        while not (controller.base / 'blocked').exists():
            assert child.poll() is None
            assert time.monotonic() < deadline, 'publisher did not reach remote write'
            time.sleep(.01)
        owner = adaptive_cpu.read_json(controller.base / 'publisher-owner.json')
        assert owner['pid'] == child.pid
        assert owner['start_ticks'] == adaptive_snapshot._start_ticks(child.pid)
        # Neither the child nor the parent retained the admission descriptor.
        # The real lock is acquired again while the publication child is alive.
        sibling = adaptive_cpu.Controller(queue.ledger(), tiers)
        with sibling.locked():
            sibling.admitted({'borrowing': True, 'sampled_unix': 12.})
        assert child.poll() is None
        assert adaptive_snapshot.publish(sibling.base, shared) is None
        assert adaptive_cpu.read_json(controller.base / 'publisher-owner.json') == owner
        # A new process (as after worker-loop replacement) also sees the same
        # occupied slot; it cannot accumulate another blocked writer.
        check = (
            f'import sys; sys.path.insert(0, {str(Path(adaptive_snapshot.__file__).parent.parent)!r}); '
            'from pathlib import Path; from prismabuild.adaptive_snapshot import publish; '
            f'assert publish(Path({str(sibling.base)!r}), Path({str(shared)!r})) is None'
        )
        probe = real_popen([sys.executable, '-c', check], close_fds=True)
        assert probe.wait(timeout=10) == 0
    finally:
        (controller.base / 'release').touch()
        assert child.wait(timeout=10) == 0
    copied = adaptive_cpu.read_json(shared / 'cpu-sample.json')
    assert copied['sampled_unix'] == 10.
    assert copied['_snapshot']['source'] == 'host-local'
    result = adaptive_cpu.read_json(controller.base / 'publisher-result.json')
    assert result['nonce'] == owner['nonce'] and result['status'] == 'published'


def test_snapshot_preserves_source_age_and_is_never_read_back(tmp_path, monkeypatch):
    from test_pbstatus import pbstatus
    queue = pool.PoolQueue(tmp_path / 'queue')
    controller = adaptive_cpu.Controller(queue.ledger(), {'preferred': [0], 'fallback': []})
    shared = queue.ledger().base / 'adaptive'
    adaptive_cpu.write_json(controller.base / 'cpu-sample.json', {
        'sampled_unix': 10., 'cpus': {'0': [10, 100]}, 'psi_total': 0})
    assert adaptive_snapshot.copy_snapshot(controller.base, shared) == ['cpu-sample.json']
    record = adaptive_cpu.read_json(shared / 'cpu-sample.json')
    assert record['_snapshot']['copied_unix'] > 10.
    assert pbstatus._admission_sample(record, now=20., max_age_s=5.)['state'] == 'stale'
    (controller.base / 'cpu-sample.json').unlink()
    monkeypatch.setattr(adaptive_cpu, 'counters', lambda cpus: {
        'sampled_unix': 11., 'cpus': {'0': [20, 200]}, 'psi_total': 0})
    with controller.locked():
        assert controller.sample() == {}


def test_publisher_launch_failure_does_not_override_claim_result(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / 'queue')
    tiers = {'preferred': [0], 'fallback': []}
    monkeypatch.setattr(adaptive_cpu, 'action_identity', lambda item: ('shape', False))
    monkeypatch.setattr(adaptive_cpu.Controller, 'sample', lambda self: {})
    monkeypatch.setattr(adaptive_snapshot.subprocess, 'Popen',
                        lambda *a, **kw: (_ for _ in ()).throw(OSError('no interpreter')))
    key = 'a' * 64
    queue.publish(action_key=key, cas_root=str(tmp_path / 'cas'),
                  checkout_root=str(tmp_path), worker_script='worker.py', resources={'cpu': 1})
    claimed = queue.claim(capacity={'cpu': 1}, cpu_tiers=tiers, adaptive_cpu=True)
    assert claimed['action_key'] == key
    assert queue.ledger().held_keys() == [key]


def test_owned_local_state_mode_is_repaired_and_symlink_is_refused(tmp_path):
    base = tmp_path / 'ledger'
    state = adaptive_cpu.local_state_base(base)
    state.chmod(0o770)
    assert adaptive_cpu.local_state_base(base) == state
    assert state.stat().st_mode & 0o777 == 0o700
    state.rmdir()
    elsewhere = tmp_path / 'elsewhere'
    elsewhere.mkdir()
    state.symlink_to(elsewhere, target_is_directory=True)
    with pytest.raises(RuntimeError, match='unsafe PrismaBuild adaptive CPU'):
        adaptive_cpu.local_state_base(base)


def test_shared_cpu_sample_is_not_an_authoritative_input(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / 'queue')
    controller = adaptive_cpu.Controller(queue.ledger(), {'preferred': [0], 'fallback': []})
    adaptive_cpu.write_json(queue.ledger().base / 'adaptive' / 'cpu-sample.json', {
        'sampled_unix': 10., 'cpus': {'0': [10, 100]}, 'psi_total': 0,
        'observation': {'busy_cpus': 0., 'sampled_unix': 10.}})
    monkeypatch.setattr(adaptive_cpu, 'counters', lambda cpus: {
        'sampled_unix': 10.5, 'cpus': {'0': [20, 200]}, 'psi_total': 0})
    # A cold local sampler needs its own interval. A retained shared copy may
    # belong to an earlier boot/generation and must not supply lending credit.
    with controller.locked():
        assert controller.sample() == {}


@pytest.mark.parametrize('refusal', ['rate_limit', 'busy', 'launch_failure'])
def test_snapshot_retries_unchanged_state_from_a_new_controller(tmp_path, monkeypatch, refusal):
    """A skipped copy must survive the controller that wrote its last update."""
    import fcntl
    import json
    import os

    queue = pool.PoolQueue(tmp_path / 'queue')
    tiers = {'preferred': [0], 'fallback': []}
    first = adaptive_cpu.Controller(queue.ledger(), tiers)
    shared = queue.ledger().base / 'adaptive' / 'gpu-state.json'
    with first.locked():
        first.write_state('gpu-state.json', {'sampled_unix': 10., 'sample_id': 'old'})
    for child in adaptive_snapshot._children:
        assert child.wait(timeout=10) == 0
    assert json.loads(shared.read_text())['sample_id'] == 'old'

    descriptor = None
    try:
        with monkeypatch.context() as patch:
            patch.setattr(adaptive_snapshot, 'MIN_PUBLISH_INTERVAL_S',
                          10000. if refusal == 'rate_limit' else 0.)
            if refusal == 'busy':
                descriptor = os.open(first.base / 'publish.lock', os.O_RDWR)
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            elif refusal == 'launch_failure':
                patch.setattr(adaptive_snapshot.subprocess, 'Popen',
                              lambda *a, **kw: (_ for _ in ()).throw(OSError('no interpreter')))
            with first.locked():
                first.write_state('gpu-state.json', {'sampled_unix': 11., 'sample_id': 'new'})
        assert json.loads(shared.read_text())['sample_id'] == 'old'
    finally:
        if descriptor is not None:
            os.close(descriptor)

    monkeypatch.setattr(adaptive_snapshot, 'MIN_PUBLISH_INTERVAL_S', 0.)
    # claim() creates a controller on every poll. No fresh CPU/GPU sample is
    # available on this pass, so it writes no admission state at all.
    replacement = adaptive_cpu.Controller(queue.ledger(), tiers)
    with replacement.locked():
        pass
    for child in adaptive_snapshot._children:
        assert child.wait(timeout=10) == 0
    assert json.loads(shared.read_text())['sample_id'] == 'new'

    # Once caught up, another unchanged pass must not rewrite the shared mount.
    before = shared.stat().st_mtime_ns
    with adaptive_cpu.Controller(queue.ledger(), tiers).locked():
        pass
    for child in adaptive_snapshot._children:
        assert child.wait(timeout=10) == 0
    assert shared.stat().st_mtime_ns == before


def test_failed_copy_retries_without_another_state_write(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / 'queue')
    tiers = {'preferred': [0], 'fallback': []}
    controller = adaptive_cpu.Controller(queue.ledger(), tiers)
    shared = queue.ledger().base / 'adaptive'
    shared.parent.mkdir(parents=True, exist_ok=True)
    shared.write_text('destination temporarily unavailable')
    with controller.locked():
        controller.write_state('gpu-state.json', {'sampled_unix': 11.})
    child = adaptive_snapshot._children.pop()
    assert child.wait(timeout=10) == 1
    result = adaptive_cpu.read_json(controller.base / 'publisher-result.json')
    assert result['status'] == 'failed'
    shared.unlink()
    monkeypatch.setattr(adaptive_snapshot, 'MIN_PUBLISH_INTERVAL_S', 0.)
    with adaptive_cpu.Controller(queue.ledger(), tiers).locked():
        pass
    for child in adaptive_snapshot._children:
        assert child.wait(timeout=10) == 0
    assert adaptive_cpu.read_json(shared / 'gpu-state.json')['sampled_unix'] == 11.


def test_update_during_a_copy_is_retried_after_that_publisher_exits(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / 'queue')
    tiers = {'preferred': [0], 'fallback': []}
    first = adaptive_cpu.Controller(queue.ledger(), tiers)
    shared = queue.ledger().base / 'adaptive'
    real_popen = subprocess.Popen
    script = f"""
import sys, time
from pathlib import Path
sys.path.insert(0, {str(Path(adaptive_snapshot.__file__).parent.parent)!r})
from prismabuild import adaptive_snapshot as snap
local, shared = Path(sys.argv[1]), Path(sys.argv[2])
original = snap._write
def write(path, value):
    if path == shared / 'gpu-state.json':
        (local / 'blocked').write_text('old GPU state already read')
        while not (local / 'release').exists():
            time.sleep(.01)
    return original(path, value)
snap._write = write
raise SystemExit(snap.main(sys.argv[1:]))
"""

    def launch(argv, **kwargs):
        return real_popen([argv[0], '-c', script, *argv[2:]], **kwargs)

    monkeypatch.setattr(adaptive_snapshot.subprocess, 'Popen', launch)
    monkeypatch.setattr(adaptive_snapshot, 'MIN_PUBLISH_INTERVAL_S', 0.)
    with first.locked():
        first.write_state('gpu-state.json', {'sampled_unix': 10.})
    child = adaptive_snapshot._children[-1]
    try:
        deadline = time.monotonic() + 10
        while not (first.base / 'blocked').exists():
            assert child.poll() is None
            assert time.monotonic() < deadline
            time.sleep(.01)
        with adaptive_cpu.Controller(queue.ledger(), tiers).locked():
            first.write_state('gpu-state.json', {'sampled_unix': 11.})
    finally:
        (first.base / 'release').touch()
        assert child.wait(timeout=10) == 0
    assert adaptive_cpu.read_json(shared / 'gpu-state.json')['sampled_unix'] == 10.
    with adaptive_cpu.Controller(queue.ledger(), tiers).locked():
        pass
    for child in adaptive_snapshot._children:
        assert child.wait(timeout=10) == 0
    assert adaptive_cpu.read_json(shared / 'gpu-state.json')['sampled_unix'] == 11.
