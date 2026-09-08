"""One slow loop must not take its whole box down with it.

Box admission is guarded by a host-local flock, and everything that runs
inside that lock is on the shared mount: the headroom read, the ``ready``
scan, the record rename, the lease write, the token renames.  So the holder's
time in the critical section is bounded by a filesystem another machine
controls, and while ``flock`` was called blocking, every other loop on the box
waited for it.

That is measured, not hypothetical.  On 2026-09-06 one client was slow to
return an NFS read delegation; the holder sat in ``__break_lease`` against a
45-second ``lease-break-time`` (``/proc/sys/fs/lease-break-time`` on the
server reads 45), and 15 of dl380g10's 16 loops were in
``locks_lock_inode_wait`` behind it.  For the duration the box announced
nothing at all -- its offer age climbed past ``OFFER_TIMEOUT_S`` -- so a box
that was merely waiting looked exactly like a box that had died.

Nothing here touches a real queue, a real box or the fleet's lock: every test
builds its own pool under ``tmp_path``, and the peer that holds admission is
this process, on a second file descriptor of the same lock file.  ``flock`` is
held per open file description, so a second ``os.open`` of one file contends
with the first exactly as a second process would.

That last property is also this file's one hazard, and the reason every call
into admission below runs through ``_bounded``.  A peer that is this same
process can hold the lock, but it can never *release* it while the main thread
is blocked waiting for it -- so under the regression these tests exist to
catch, a call made on the main thread does not fail, it hangs forever.  It did:
on 2026-09-06 a mutation arm that stripped ``LOCK_NB`` from
``adaptive_cpu.py`` ran for 23 minutes, holding a PrismaBuild reservation,
with ``/proc/locks`` showing one pid as both the holder and the blocked waiter
on the same inode.  The lease heartbeat comes from the worker loop, not the
action, so the queue read ``CLAIMED`` and healthy throughout.  A test that
hangs proves nothing and costs a worker; ``_bounded`` turns the same
regression into a named failure.
"""
from __future__ import annotations

import fcntl
import json
import os
from pathlib import Path
import sys
import threading
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from prismabuild import adaptive_cpu, pool

TIERS = {'preferred': [0], 'fallback': [1]}
CAPACITY = {'cpu': 2, 'mem_gb': 4}


@pytest.fixture
def rig(tmp_path, monkeypatch):
    """A pool with one claimable item and a host that is plainly idle."""

    monkeypatch.setattr(adaptive_cpu.Controller, 'sample', lambda self: {
        'sampled_unix': time.time(), 'busy_cpus': 0., 'psi_some': 0.,
        'cpu_count': 2, 'interval_s': 1.})
    queue = pool.PoolQueue(tmp_path / 'queue')
    queue.publish(action_key='a' * 64, cas_root=str(tmp_path / 'cas'),
                  checkout_root=str(tmp_path), worker_script='worker.py',
                  resources={'cpu': 1, 'mem_gb': 1})
    return queue


def _lock_path(queue):
    directory, digest = adaptive_cpu.box_state(queue.ledger().base)
    return Path(directory) / f'{digest}.lock'


@pytest.fixture
def peer(rig):
    """A second loop, holding box admission for the whole test."""

    descriptor = os.open(_lock_path(rig), os.O_CREAT | os.O_RDWR, 0o600)
    fcntl.flock(descriptor, fcntl.LOCK_EX)
    try:
        yield descriptor
    finally:
        os.close(descriptor)


def _bounded(call, what, seconds=30.):
    """Run *call* off the main thread, and fail by name if it does not return.

    Returns what *call* returned and re-raises what it raised, so a caller
    still reads as an ordinary call -- ``pytest.raises`` around this behaves
    exactly as it would around the call itself.

    The bound is not a timeout on slowness.  Correct code answers here in
    microseconds, because refusing is a syscall that cannot block; the bound
    is reached only when the acquisition became blocking again, and in that
    state it can never be satisfied at all.  So it separates a hang from a
    result, which is the one thing the main thread cannot do for itself.
    """

    outcome = {}

    def run():
        try:
            outcome['value'] = call()
        except BaseException as exc:      # re-raised below, on the main thread
            outcome['error'] = exc

    thread = threading.Thread(target=run, daemon=True)
    started = time.monotonic()
    thread.start()
    thread.join(timeout=seconds)
    assert not thread.is_alive(), (
        f'{what} is still inside the kernel after {seconds:.0f}s, waiting for '
        f'a lock this same process holds and cannot release while it waits. '
        f'Admission is blocking again: one slow loop takes its whole box down')
    if 'error' in outcome:
        raise outcome['error']
    assert time.monotonic() - started < seconds
    return outcome['value']


def _acquire_and_release(controller):
    """Take box admission and give it straight back."""

    with controller.locked():
        pass


def _attempt(controller):
    """Enter and leave box admission, failing loudly if it was granted.

    Every caller already holds the lock on another descriptor, so being let in
    is the exclusivity failure, not a pass.
    """

    with controller.locked():
        pytest.fail('two loops were inside box admission at once')


def test_a_claim_refuses_at_once_rather_than_waiting_for_the_holder(rig, peer):
    """The whole defect, in one assertion: this must come back.

    A blocking acquisition never returns while ``peer`` holds the lock, so the
    call being unfinished *is* the regression -- which is why it runs through
    ``_bounded`` rather than on the main thread.
    """

    item = _bounded(lambda: rig.claim(capacity=CAPACITY, cpu_tiers=TIERS,
                                      adaptive_cpu=True), 'claim')
    assert item is None, 'a claim that never held admission returned an item'


def test_claim_reports_the_enclosing_admission_gate(rig, peer, capsys, monkeypatch):
    def must_not_scan(**kwargs):
        pytest.fail('a refused admission reached candidate evaluation')

    monkeypatch.setattr(rig, '_claim', must_not_scan)
    assert _bounded(lambda: rig.claim(capacity=CAPACITY, cpu_tiers=TIERS,
                                      adaptive_cpu=True), 'claim') is None
    diagnostic = capsys.readouterr().err
    assert 'host admission lock busy' in diagnostic
    assert f'observed holder pid={os.getpid()}' in diagnostic
    assert 'candidate evaluation not reached' in diagnostic


def test_busy_diagnostic_is_bounded_across_polls_and_holder_changes(
        rig, peer, capsys, monkeypatch):
    clock = [100.0]
    monkeypatch.setattr(pool.time, 'monotonic', lambda: clock[0])

    def claim():
        return _bounded(lambda: rig.claim(capacity=CAPACITY, cpu_tiers=TIERS,
                                          adaptive_cpu=True), 'claim')

    assert claim() is None
    assert 'host admission lock busy' in capsys.readouterr().err
    clock[0] = 159.0
    monkeypatch.setattr(adaptive_cpu, '_holder_of', lambda fd: None)
    for _ in range(10):
        assert claim() is None
    assert capsys.readouterr().err == ''

    # Acquiring admission does not reset the log budget: repeated brief
    # contention must not print on every transition back to busy.
    fcntl.flock(peer, fcntl.LOCK_UN)
    monkeypatch.setattr(rig, '_claim', lambda **kwargs: None)
    assert claim() is None
    fcntl.flock(peer, fcntl.LOCK_EX)
    assert claim() is None
    assert capsys.readouterr().err == ''
    clock[0] = 160.0
    assert claim() is None
    assert 'observed holder pid=unknown' in capsys.readouterr().err


@pytest.mark.parametrize('error', [BrokenPipeError('closed pipe'), ValueError('closed log')])
def test_failed_busy_diagnostic_still_refuses_without_scanning(
        rig, peer, monkeypatch, error):
    def no_log(*args, **kwargs):
        raise error

    def must_not_scan(**kwargs):
        pytest.fail('a diagnostic failure reached candidate evaluation')

    monkeypatch.setattr(pool, 'print', no_log, raising=False)
    monkeypatch.setattr(rig, '_claim', must_not_scan)
    assert _bounded(lambda: rig.claim(capacity=CAPACITY, cpu_tiers=TIERS,
                                      adaptive_cpu=True), 'claim') is None


def test_nothing_is_claimed_or_reserved_by_a_refused_admission(rig, peer):
    """Declining must leave the queue exactly as it found it.

    The failure this rules out is worse than the one it replaces: a refusal
    that renamed the record or took a token would lose the item, and it would
    lose it silently, because nothing downstream is looking for work that was
    claimed by a loop which then declined to run it.
    """

    assert _bounded(lambda: rig.claim(capacity=CAPACITY, cpu_tiers=TIERS,
                                      adaptive_cpu=True), 'claim') is None
    assert not rig.ledger().held(), 'a refused admission held tokens'
    assert rig.ledger().available() == rig.ledger().capacity()
    assert [p.name for p in (rig.root / 'ready').iterdir()] == ['a' * 64 + '.json'], (
        'the item did not stay ready for the next loop')


def test_the_box_still_announces_while_a_peer_holds_admission(rig, peer):
    """Announcing is the path that must never be behind admission.

    This is why the box went dark rather than merely slow.  Loops blocked
    *inside* ``claim`` never reached the top of their poll again, so nothing
    refreshed ``workers/<host>.json`` and every offer aged out.  With the
    refusal in place the loop returns to its own cadence, and this is the
    thing it does there.
    """

    _bounded(lambda: rig.announce(host='testbox', tags=['x86', 'testbox'],
                                  has_gpu=False, capacity=CAPACITY,
                                  cpu_tiers=TIERS), 'announce')
    offers = {offer['host']: offer for offer in rig.offers()}
    assert 'testbox' in offers, 'the box could not say what it was'
    assert 0 <= time.time() - offers['testbox']['announced_unix'] < 30


def test_admission_is_still_exclusive(rig, peer):
    """The safety property the refusal must not have traded away.

    Refusing is only correct because it refuses.  A ``locked()`` that yielded
    anyway on contention would let two loops price the same headroom against
    each other and both admit, which is the failure the lock exists for.
    """

    controller = adaptive_cpu.Controller(rig.ledger(), TIERS)
    with pytest.raises(adaptive_cpu.AdmissionBusy):
        _bounded(lambda: _attempt(controller), 'locked()')


def test_admission_is_free_again_once_the_holder_leaves(rig):
    """A refusal must be about *now*, not a state the lock gets stuck in."""

    descriptor = os.open(_lock_path(rig), os.O_CREAT | os.O_RDWR, 0o600)
    fcntl.flock(descriptor, fcntl.LOCK_EX)
    controller = adaptive_cpu.Controller(rig.ledger(), TIERS)
    with pytest.raises(adaptive_cpu.AdmissionBusy):
        _bounded(lambda: _attempt(controller), 'locked()')
    os.close(descriptor)
    # Acquired, so the refusal above was the peer and not a latch.  Bounded
    # too: with nobody holding the lock this cannot block, and if it does the
    # lock is stuck rather than busy -- a different defect, reported not hung.
    _bounded(lambda: _acquire_and_release(controller), 'locked() when free')


def test_the_refusal_names_the_holder(rig, peer):
    """Who is in there, so an operator is not left guessing.

    ``/proc/locks`` is the only place that answers this, and reading it costs
    nothing on the shared mount -- which is the point, since the reason the
    question is being asked is that the shared mount is slow.
    """

    controller = adaptive_cpu.Controller(rig.ledger(), TIERS)
    with pytest.raises(adaptive_cpu.AdmissionBusy) as refusal:
        _bounded(lambda: _attempt(controller), 'locked()')
    assert refusal.value.holder == os.getpid(), (
        f'named pid {refusal.value.holder}, but this process holds the lock')
    assert str(os.getpid()) in str(refusal.value)


def test_an_unreadable_proc_locks_still_refuses(rig, peer, monkeypatch):
    """A diagnostic may not turn a refusal into a crash.

    ``holder`` is then ``None``, which reads as "unknown" and must not be
    reported anywhere as "nobody".
    """

    real_open = open

    def no_proc_locks(path, *args, **kwargs):
        if str(path) == '/proc/locks':
            raise OSError('no /proc/locks on this kernel')
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr(adaptive_cpu, 'open', no_proc_locks, raising=False)
    controller = adaptive_cpu.Controller(rig.ledger(), TIERS)
    with pytest.raises(adaptive_cpu.AdmissionBusy) as refusal:
        _bounded(lambda: _attempt(controller), 'locked()')
    assert refusal.value.holder is None
    assert 'another loop' in str(refusal.value)


def test_a_completion_is_not_learned_while_admission_is_busy(rig, peer, monkeypatch):
    """``record_completion`` declines too, and leaves the profile untouched.

    It runs on a scope owner that has just finished its action, so blocking
    there holds that process open on another loop's NFS latency.  Its own
    contract already covers the outcome -- failure to attribute produces no
    learned credit -- so the only thing that must be proven here is that
    declining is not the same as corrupting.
    """

    # ``action_identity`` reads the published request out of the CAS to name a
    # shape; this test is about the lock, so the shape is stubbed the way the
    # neighbouring adaptive-CPU tests stub it.
    monkeypatch.setattr(adaptive_cpu, 'action_identity',
                        lambda item: ('shape', False))
    ledger = rig.ledger()
    adaptive_cpu.write_json(ledger.base / 'cpu-map.json', TIERS)
    controller = adaptive_cpu.Controller(ledger, TIERS)
    before = {'shape': {'cpu': 1.0, 'samples': 7, 'completions': [],
                        'sampled_unix': 1.0, 'memory_peak_bytes': 5}}
    adaptive_cpu.write_json(controller.base / 'profiles.json', before)

    now = time.time()
    item = {'action_key': 'b' * 64, 'claimed_unix': now - 1}
    telemetry = {'action_key': 'b' * 64, 'sampled_unix': now, 'complete': True,
                 'cpu_seconds': 1.0, 'wall_seconds': 2.0,
                 'memory_peak_bytes': 10}

    learned = _bounded(
        lambda: adaptive_cpu.record_completion(ledger, item, telemetry),
        'record_completion')

    assert learned is False, 'a completion was learned without holding admission'
    assert adaptive_cpu.read_json(controller.base / 'profiles.json') == before, (
        'the profile was rewritten by a call that never held the lock')


def test_stalled_ready_scan_does_not_exclude_a_sibling_claim(rig, monkeypatch):
    entered = threading.Event()
    release = threading.Event()
    original = rig.ready_items
    outcome = {}

    def paused_scan():
        snapshot = original()
        entered.set()
        assert release.wait(30), 'test did not release the paused ready scan'
        return snapshot

    monkeypatch.setattr(rig, 'ready_items', paused_scan)

    def run():
        try:
            outcome['item'] = rig.claim(capacity=CAPACITY, cpu_tiers=TIERS,
                                        adaptive_cpu=True)
        except BaseException as exc:
            outcome['error'] = exc

    reader = threading.Thread(target=run, daemon=True)
    reader.start()
    try:
        assert entered.wait(30), 'claim did not reach candidate discovery'
        sibling = pool.PoolQueue(rig.root)
        winner = sibling.claim(capacity=CAPACITY, cpu_tiers=TIERS,
                               adaptive_cpu=True)
        assert winner is not None, 'a stalled ready scan held host admission'
        assert winner['action_key'] == 'a' * 64
    finally:
        release.set()
        reader.join(30)
    assert not reader.is_alive()
    assert 'error' not in outcome, outcome
    assert outcome['item'] is None, 'late discovery claimed a sibling-owned item'
    assert rig.ledger().held_keys() == ['a' * 64]


def test_busy_admission_refuses_before_ready_discovery(rig, peer, monkeypatch):
    def forbidden():
        pytest.fail('busy admission started a shared ready scan')

    monkeypatch.setattr(rig, 'ready_items', forbidden)
    assert rig.claim(capacity=CAPACITY, cpu_tiers=TIERS, adaptive_cpu=True) is None


@pytest.mark.parametrize('replacement', [
    {'resources': {'cpu': 2, 'mem_gb': 1}},
    {'tags': ['another-host']},
    {'needs_gpu': True},
])
def test_prefetched_replacement_requires_fresh_admission(rig, monkeypatch, replacement):
    original = rig.ready_items

    def replaced_scan():
        snapshot = original()
        values = dict(resources={'cpu': 1, 'mem_gb': 1})
        values.update(replacement)
        rig.publish(action_key='a' * 64, cas_root='/cas', checkout_root='/co',
                    worker_script='worker.py', **values)
        return snapshot

    monkeypatch.setattr(rig, 'ready_items', replaced_scan)
    assert rig.claim(capacity=CAPACITY, cpu_tiers=TIERS, adaptive_cpu=True) is None
    assert not rig.ledger().held()
    assert not rig.item_path(pool.CLAIMED, 'a' * 64).exists()
    assert not rig.lease_path('a' * 64).exists()
    ready = json.loads(rig.item_path(pool.READY, 'a' * 64).read_text())
    for name, value in replacement.items():
        assert ready[name] == value


def test_admission_is_reacquired_after_discovery(rig, monkeypatch):
    original = rig.ready_items
    descriptor = os.open(_lock_path(rig), os.O_CREAT | os.O_RDWR, 0o600)

    def peer_wins_after_scan():
        snapshot = original()
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return snapshot

    monkeypatch.setattr(rig, 'ready_items', peer_wins_after_scan)
    try:
        assert rig.claim(capacity=CAPACITY, cpu_tiers=TIERS, adaptive_cpu=True) is None
        assert not rig.ledger().held()
        assert rig.item_path(pool.READY, 'a' * 64).exists()
    finally:
        os.close(descriptor)


def test_empty_discovery_is_not_repeated_under_admission(rig, monkeypatch):
    calls = []

    def empty_scan():
        calls.append(True)
        assert len(calls) == 1, 'empty discovery was repeated under admission'
        return []

    monkeypatch.setattr(rig, 'ready_items', empty_scan)
    assert rig.claim(capacity=CAPACITY, cpu_tiers=TIERS, adaptive_cpu=True) is None
    assert calls == [True]
    assert rig.item_path(pool.READY, 'a' * 64).exists()
    assert not rig.ledger().held()
