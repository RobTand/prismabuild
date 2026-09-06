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


def test_a_claim_refuses_at_once_rather_than_waiting_for_the_holder(rig, peer):
    """The whole defect, in one assertion: this must come back.

    Run on a thread with a bounded join so that a regression reports as a
    failure with a name on it, rather than hanging the suite the way it hung
    the box.  A blocking acquisition never returns while ``peer`` holds the
    lock, so the thread being alive *is* the regression.
    """

    answer = {}

    def claim():
        answer['item'] = rig.claim(capacity=CAPACITY, cpu_tiers=TIERS,
                                   adaptive_cpu=True)

    loop = threading.Thread(target=claim, daemon=True)
    started = time.monotonic()
    loop.start()
    loop.join(timeout=30)
    assert not loop.is_alive(), (
        'claim is still inside the kernel waiting for a peer to finish; '
        'one slow loop is taking the box down with it')
    assert answer['item'] is None, (
        'a claim that never held admission returned an item')
    assert time.monotonic() - started < 30


def test_nothing_is_claimed_or_reserved_by_a_refused_admission(rig, peer):
    """Declining must leave the queue exactly as it found it.

    The failure this rules out is worse than the one it replaces: a refusal
    that renamed the record or took a token would lose the item, and it would
    lose it silently, because nothing downstream is looking for work that was
    claimed by a loop which then declined to run it.
    """

    assert rig.claim(capacity=CAPACITY, cpu_tiers=TIERS,
                     adaptive_cpu=True) is None
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

    rig.announce(host='testbox', tags=['x86', 'testbox'], has_gpu=False,
                 capacity=CAPACITY, cpu_tiers=TIERS)
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
        with controller.locked():
            pytest.fail('two loops were inside box admission at once')


def test_admission_is_free_again_once_the_holder_leaves(rig):
    """A refusal must be about *now*, not a state the lock gets stuck in."""

    descriptor = os.open(_lock_path(rig), os.O_CREAT | os.O_RDWR, 0o600)
    fcntl.flock(descriptor, fcntl.LOCK_EX)
    controller = adaptive_cpu.Controller(rig.ledger(), TIERS)
    with pytest.raises(adaptive_cpu.AdmissionBusy):
        with controller.locked():
            pass
    os.close(descriptor)
    with controller.locked():
        pass  # acquired; the refusal was the peer, not a latch


def test_the_refusal_names_the_holder(rig, peer):
    """Who is in there, so an operator is not left guessing.

    ``/proc/locks`` is the only place that answers this, and reading it costs
    nothing on the shared mount -- which is the point, since the reason the
    question is being asked is that the shared mount is slow.
    """

    controller = adaptive_cpu.Controller(rig.ledger(), TIERS)
    with pytest.raises(adaptive_cpu.AdmissionBusy) as refusal:
        with controller.locked():
            pass
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
        with controller.locked():
            pass
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

    learned = adaptive_cpu.record_completion(ledger, item, telemetry)

    assert learned is False, 'a completion was learned without holding admission'
    assert adaptive_cpu.read_json(controller.base / 'profiles.json') == before, (
        'the profile was rewritten by a call that never held the lock')
