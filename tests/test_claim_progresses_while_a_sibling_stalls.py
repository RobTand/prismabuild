"""A box must keep claiming while one of its loops is stuck on the mount.

``tests/test_admission_does_not_wait_on_a_peer.py`` proves the *refusal* is
non-blocking, and that is a different property.  Refusing fast keeps a loop
responsive; it does not get the queued item claimed.  Issue #351 is the second
half: ready > 0, claimed == 0, the box idle with free memory and a free GPU.

The cause is where the host admission lock ends.  ``ready_items()`` was moved
out of it, but ``_claim`` still ran inside it in full -- and everything
``_claim`` does after the admission decision is on the shared mount: the record
``rename`` (which IS the claim), the lease write, the token renames.  A stall
anywhere in that region keeps the host-wide lock for its whole duration, every
sibling loop on the box gets ``AdmissionBusy`` and answers ``None``, and the box
claims nothing at all while work sits in ``ready``.

So the tests here stall the *real* code path rather than the lock: the shared
call is monkeypatched on one ``PoolQueue`` instance, and the sibling is a
second ``PoolQueue`` over the same root, unpatched.  Holding the flock directly
is what the neighbouring file already covers, and it cannot show this defect --
under the fix the flock is simply not held there any more.

Nothing here touches a real queue, a real box or the fleet's lock: every test
builds its own pool under ``tmp_path``.  The stalled loop is a thread of this
same process, which is also this file's one hazard and the reason every call
into admission runs through ``_bounded``: the stalled thread cannot release
anything while the main thread is blocked, so a regression that made the
acquisition blocking again would hang the worker rather than fail it.
``_bounded`` turns that into a named failure.  It is copied from the
neighbouring file deliberately -- these two files must be able to fail
independently.

Issue #351.
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

#: Two preferred CPUs, so both items are seated on the preferred tier and
#: neither routes through ``_defer_fallback`` -- which would decline for a
#: reason that has nothing to do with the lock.
TIERS = {'preferred': [0, 1], 'fallback': [2, 3]}
CAPACITY = {'cpu': 4, 'mem_gb': 4}
KEYS = ('a' * 64, 'b' * 64)


@pytest.fixture
def rig(tmp_path, monkeypatch):
    """A pool with two claimable items and a host that is plainly idle."""

    monkeypatch.setattr(adaptive_cpu.Controller, 'sample', lambda self: {
        'sampled_unix': time.time(), 'busy_cpus': 0., 'psi_some': 0.,
        'cpu_count': 4, 'interval_s': 1.})
    queue = pool.PoolQueue(tmp_path / 'queue')
    for key in KEYS:
        queue.publish(action_key=key, cas_root=str(tmp_path / 'cas'),
                      checkout_root=str(tmp_path), worker_script='worker.py',
                      resources={'cpu': 1, 'mem_gb': 1})
    return queue


def _claim(queue):
    return queue.claim(capacity=CAPACITY, cpu_tiers=TIERS, adaptive_cpu=True)


def _lock_path(queue):
    directory, digest = adaptive_cpu.box_state(queue.ledger().base)
    return Path(directory) / f'{digest}.lock'


def _admission_is_free(queue):
    """Is box admission unheld right now?  Asked on its own descriptor.

    ``flock`` is held per open file description, so this contends with a
    holder in this same process exactly as another process would.
    """

    descriptor = os.open(_lock_path(queue), os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return False
    finally:
        os.close(descriptor)
    return True


def _bounded(call, what, seconds=20.):
    """Run *call* off the main thread, and fail by name if it does not return.

    Returns what *call* returned and re-raises what it raised, so a caller
    still reads as an ordinary call.  The bound is not a timeout on slowness:
    every call made through it either answers in microseconds or is waiting on
    a lock this same process holds and will not release, which it can never be
    granted.  So it separates a hang from a result, which is the one thing the
    main thread cannot do for itself.
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


def test_two_claims_in_a_row_are_both_admitted(rig):
    """The control.  Nothing is stalled, so both items must be claimable.

    Without this, a red result below could equally mean the rig only ever had
    one admission in it -- headroom, ``MAX_ACTIONS``, a measurement holder --
    and the stall would be blamed for a refusal it did not cause.
    """

    first = _claim(rig)
    second = _claim(pool.PoolQueue(rig.root))
    assert first is not None and second is not None, (
        f'the rig admits fewer than two items even unstalled: {first}, {second}')
    assert {first['action_key'], second['action_key']} == set(KEYS)
    assert sorted(rig.ledger().held_keys()) == sorted(KEYS)


def test_sibling_claims_while_existing_cpu_map_read_stalls(rig, monkeypatch):
    """The fixed topology is not changing headroom; reading it needs no gate."""
    rig.ledger().configure_cpu_tiers(TIERS)
    entered, release = threading.Event(), threading.Event()
    original = pool._read_json
    outcome = {}

    def stalled_read(path, **kwargs):
        if (threading.current_thread() is stalled
                and Path(path) == rig.ledger().base / 'cpu-map.json'):
            entered.set()
            assert release.wait(30), 'test did not release CPU map read'
        return original(path, **kwargs)

    def run():
        try:
            outcome['item'] = _claim(rig)
        except BaseException as exc:
            outcome['error'] = exc

    stalled = threading.Thread(target=run, daemon=True)
    monkeypatch.setattr(pool, '_read_json', stalled_read)
    stalled.start()
    try:
        assert entered.wait(30), 'claim did not read CPU map'
        winner = _bounded(lambda: _claim(pool.PoolQueue(rig.root)), 'sibling claim')
        assert winner is not None, 'CPU map read held admission and blocked sibling work'
    finally:
        release.set()
        stalled.join(30)
    assert not stalled.is_alive()
    assert 'error' not in outcome, outcome
    assert outcome['item'] is not None
    assert outcome['item']['action_key'] != winner['action_key']
    assert sorted(rig.ledger().held_keys()) == sorted(KEYS)


def test_existing_cpu_map_read_does_not_hold_admission(rig, monkeypatch):
    rig.ledger().configure_cpu_tiers(TIERS)
    original = pool._read_json
    reads = []

    def checked_read(path, **kwargs):
        if Path(path) == rig.ledger().base / 'cpu-map.json':
            reads.append(path)
            assert _admission_is_free(rig), 'existing CPU map read held host admission'
        return original(path, **kwargs)

    monkeypatch.setattr(pool, '_read_json', checked_read)
    assert _claim(rig) is not None
    assert reads, 'claim skipped topology validation'


def test_missing_cpu_map_initialization_stays_under_admission(rig, monkeypatch):
    original = pool.pb._atomic_publish
    initialized = []

    def checked_publish(path, *args, **kwargs):
        if Path(path) == rig.ledger().base / 'cpu-map.json':
            assert not _admission_is_free(rig), 'topology initialization lost exclusion'
            initialized.append(path)
        return original(path, *args, **kwargs)

    monkeypatch.setattr(pool.pb, '_atomic_publish', checked_publish)
    assert _claim(rig) is not None
    assert len(initialized) == 1
    assert json.loads(initialized[0].read_text()) == TIERS


def test_cpu_map_preparation_does_not_bypass_busy_admission(rig, monkeypatch):
    rig.ledger().configure_cpu_tiers(TIERS)
    original = pool._read_json
    descriptor = os.open(_lock_path(rig), os.O_CREAT | os.O_RDWR, 0o600)
    entered = []

    def lock_after_read(path, **kwargs):
        record = original(path, **kwargs)
        if Path(path) == rig.ledger().base / 'cpu-map.json' and not entered:
            entered.append(path)
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return record

    monkeypatch.setattr(pool, '_read_json', lock_after_read)
    try:
        assert _bounded(lambda: _claim(rig), 'claim after topology read') is None
    finally:
        os.close(descriptor)
    assert entered
    assert rig.ledger().held() == {}
    assert {r['action_key'] for r in rig.ready_items()} == set(KEYS)


def test_a_sibling_claims_the_other_item_while_one_loop_stalls(rig, monkeypatch):
    """The whole defect, in one assertion.

    The stall is inside the real shared-mount I/O of ``_claim`` -- the lease
    write, which is on the mount and runs after the rename that already made
    this loop the owner.  While it is in there the box must still be able to
    claim the *other* ready item.  Under the enclosing lock it cannot: the
    sibling is refused admission before it evaluates a single candidate, and
    the box goes to zero claims with work ready and capacity free.
    """

    entered = threading.Event()
    release = threading.Event()
    original = rig.write_lease
    outcome = {}

    def stalled_lease(action_key, **kwargs):
        entered.set()
        assert release.wait(30), 'the test never released the stalled lease write'
        return original(action_key, **kwargs)

    monkeypatch.setattr(rig, 'write_lease', stalled_lease)

    def run():
        try:
            outcome['item'] = _claim(rig)
        except BaseException as exc:
            outcome['error'] = exc

    stalled = threading.Thread(target=run, daemon=True)
    stalled.start()
    try:
        assert entered.wait(30), 'the first claim never reached the lease write'
        winner = _bounded(lambda: _claim(pool.PoolQueue(rig.root)),
                          'the sibling claim', seconds=10.)
        assert winner is not None, (
            'the box claimed nothing while one loop was stalled on the shared '
            'mount: host admission is still held across the record rename, the '
            'lease write and the token renames, so every sibling loop is '
            'refused and ready work is left unclaimed (#351)')
        assert winner['action_key'] in KEYS
    finally:
        release.set()
        stalled.join(30)

    assert not stalled.is_alive()
    assert 'error' not in outcome, outcome
    assert outcome['item'] is not None, 'the stalled loop lost its own claim'
    assert outcome['item']['action_key'] != winner['action_key'], (
        'both loops claimed the same item')
    assert sorted(rig.ledger().held_keys()) == sorted(KEYS)


def test_box_admission_is_not_held_across_the_claims_shared_io(rig, monkeypatch):
    """The same property, single-threaded and pointed at the exact calls.

    The threaded test above says the box still makes progress; this one says
    *why*, at the two shared-mount steps the stall was measured in -- the
    intent marker written immediately before the rename, and the lease write
    immediately after it.  A regression that re-widened the lock fails here
    without needing a second thread to be scheduled at all.
    """

    observed = {}
    write_lease = rig.write_lease
    write_intent = rig._write_claim_intent

    def probing_intent(action_key, *, owner):
        observed['intent'] = _admission_is_free(rig)
        return write_intent(action_key, owner=owner)

    def probing_lease(action_key, **kwargs):
        observed['lease'] = _admission_is_free(rig)
        return write_lease(action_key, **kwargs)

    monkeypatch.setattr(rig, '_write_claim_intent', probing_intent)
    monkeypatch.setattr(rig, 'write_lease', probing_lease)

    assert _claim(rig) is not None
    assert observed.get('intent') is True, (
        'the claim rename still runs under host admission')
    assert observed.get('lease') is True, (
        'the lease write still runs under host admission')


def test_a_lost_rename_returns_only_this_claimants_tokens(rig, monkeypatch):
    """Ownership safety, on the path the narrowed lock changed.

    Exactly one ``rename`` wins; the loser must hold nothing.  With the
    reservation now taken under the lock and released outside it, that release
    is on a different side of the critical section than the acquisition, which
    is precisely the kind of split that leaks.
    """

    write_intent = rig._write_claim_intent

    def peer_wins(action_key, *, owner):
        write_intent(action_key, owner=owner)
        # The record leaves ``ready`` between this claimant's reservation and
        # its rename, which is what losing the race looks like from here.
        rig.item_path(pool.READY, action_key).unlink()

    monkeypatch.setattr(rig, '_write_claim_intent', peer_wins)

    assert _bounded(lambda: _claim(rig), 'claim') is None
    assert not rig.ledger().held(), 'a lost rename kept its reservation'
    assert rig.ledger().available() == rig.ledger().capacity()


def test_an_exception_before_the_rename_returns_the_reservation(rig, monkeypatch):
    """No ending between the reservation and the rename may keep tokens.

    ``begin_acquire`` is all-or-nothing about its *own* endings, but once it
    has returned a handle the tokens are the caller's problem, and the caller
    had no handler at all: an ``ESTALE`` out of the intent marker, or anything
    else raised in that window, left the whole demand under
    ``held/claiming.<...>/`` for ``sweep_stale_acquisitions`` to find a
    ``LEASE_TIMEOUT_S`` later -- once per poll, for as long as the cause
    repeated.
    """

    def refuse(action_key, *, owner):
        raise OSError('stale file handle writing the intent marker')

    monkeypatch.setattr(rig, '_write_claim_intent', refuse)

    with pytest.raises(OSError):
        _bounded(lambda: _claim(rig), 'claim')
    assert not rig.ledger().held(), (
        'a claim that raised between the reservation and the rename kept its '
        'tokens; the caller holds the only handle that can return them')
    assert rig.ledger().available() == rig.ledger().capacity()
    assert sorted(p.name for p in rig.dir(pool.READY).iterdir()) == sorted(
        f'{key}.json' for key in KEYS), 'the items did not stay ready'


def test_a_busy_admission_lock_after_the_rename_cannot_undo_the_claim(
        rig, monkeypatch):
    """Past the rename the item is this loop's, and nothing may take it back.

    The narrowed lock is never reacquired after the rename -- the borrow
    record goes in with the decision that spends it -- and this holds that
    true.  A refusal escaping anywhere past the rename would leave ``claim``
    answering "nothing to run" for an action that is already renamed into
    ``claimed/``, holding tokens, and carrying a lease: work nobody would then
    execute, and which only the reaper would recover a ``LEASE_TIMEOUT_S``
    later.
    """

    held = []
    write_lease = rig.write_lease

    def lease_then_hold_admission(action_key, **kwargs):
        result = write_lease(action_key, **kwargs)
        descriptor = os.open(_lock_path(rig), os.O_CREAT | os.O_RDWR, 0o600)
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        held.append(descriptor)
        return result

    monkeypatch.setattr(rig, 'write_lease', lease_then_hold_admission)
    try:
        claimed = _bounded(lambda: _claim(rig), 'claim')
    finally:
        for descriptor in held:
            os.close(descriptor)

    assert held, 'the claim never reached the lease write'
    assert claimed is not None, (
        'a busy host-local lock after the rename lost a claim that was '
        'already made')
    assert rig.ledger().held_keys() == [claimed['action_key']]
    assert rig.item_path(pool.CLAIMED, claimed['action_key']).exists()


@pytest.mark.parametrize(('refusal', 'operation'), [
    ('cpu', 'record_pass'), ('gpu', 'record_pass'),
    ('tokens', 'record_pass'), ('tokens', 'withhold_age'),
])
def test_sibling_claims_while_refusal_accounting_stalls(rig, monkeypatch, refusal, operation):
    """An unfunded candidate's aging write must not hold host admission (#266)."""
    capacity = dict(CAPACITY, gpu=1)

    def claim(queue):
        return queue.claim(capacity=capacity, cpu_tiers=TIERS,
                           adaptive_cpu=True, has_gpu=True)

    if refusal == 'tokens':
        # A real committed holder leaves enough for B, but not A's memory.
        rig.publish(action_key='c' * 64, cas_root=str(rig.root / 'cas'),
                    checkout_root=str(rig.root), worker_script='worker.py',
                    resources={'cpu': 1, 'mem_gb': 1}, priority=10)
        assert claim(rig)['action_key'] == 'c' * 64
    if refusal in ('gpu', 'tokens'):
        path = rig.item_path(pool.READY, KEYS[0])
        item = json.loads(path.read_text())
        item['resources'].update({'gpu': 1} if refusal == 'gpu' else {'mem_gb': 4})
        path.write_text(json.dumps(item))
    if refusal == 'cpu':
        decision = adaptive_cpu.Controller.decision
        monkeypatch.setattr(adaptive_cpu.Controller, 'decision',
                            lambda self, item, demand, **kwargs: None if item['action_key'] == KEYS[0]
                            else decision(self, item, demand, **kwargs))
    if refusal == 'gpu':
        monkeypatch.setattr(pool.gpu_admission.Controller, 'decision',
                            lambda self, item, demand, **kwargs: None)

    entered, release = threading.Event(), threading.Event()
    account = getattr(rig, operation)
    if operation == 'withhold_age':
        monkeypatch.setattr(pool, 'STARVATION_FLOOR', 1)
    outcome = {}

    def stalled_pass(key):
        assert key == KEYS[0]
        entered.set()
        assert release.wait(30), 'test did not release refusal accounting'
        return account(key)

    monkeypatch.setattr(rig, operation, stalled_pass)

    def run():
        try:
            outcome['item'] = claim(rig)
        except BaseException as exc:
            outcome['error'] = exc

    stalled = threading.Thread(target=run, daemon=True)
    stalled.start()
    try:
        assert entered.wait(30), 'candidate did not reach refusal accounting'
        winner = _bounded(lambda: claim(pool.PoolQueue(rig.root)), 'sibling claim', 10.)
        assert winner is not None and winner['action_key'] == KEYS[1], (
            f'{refusal} refusal accounting held admission and prevented useful work')
    finally:
        release.set()
        stalled.join(30)
    assert not stalled.is_alive() and 'error' not in outcome, outcome
    assert outcome['item'] is None
    assert rig.item_path(pool.READY, KEYS[0]).exists()
    expected = [KEYS[1]] + (['c' * 64] if refusal == 'tokens' else [])
    assert sorted(rig.ledger().held_keys()) == sorted(expected)


@pytest.mark.parametrize('contended', [False, True])
def test_preemption_after_accounting_keeps_host_exclusion(rig, monkeypatch, contended):
    """The unlocked accounting interval grants no authority to stop a holder."""
    monkeypatch.setattr(pool.ResourceLedger, 'begin_acquire', lambda *a, **kw: None)
    record_pass = rig.record_pass
    descriptors, preemptions = [], []

    def account(key):
        assert _admission_is_free(rig)
        result = record_pass(key)
        if contended:
            fd = os.open(_lock_path(rig), os.O_RDWR)
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            descriptors.append(fd)
        return result

    def preempt(*args, **kwargs):
        assert not _admission_is_free(rig)
        preemptions.append(kwargs['action_key'])
        return None

    monkeypatch.setattr(rig, 'record_pass', account)
    monkeypatch.setattr(rig, '_preempt_background_holder', preempt)
    try:
        assert _bounded(lambda: _claim(rig), 'claim after refusal') is None
    finally:
        for fd in descriptors:
            os.close(fd)
    assert preemptions == ([] if contended else list(KEYS))
    assert rig.ledger().held() == {}
    assert rig.ledger().available() == rig.ledger().capacity()
    assert all(rig.item_path(pool.READY, key).exists() for key in KEYS)


@pytest.mark.parametrize(('gpu', 'read_number'), [(False, 1), (True, 2), (True, 3)])
def test_sibling_claims_while_action_request_read_stalls(rig, monkeypatch, gpu, read_number):
    """Sealed request reads need no host exclusion, unlike capacity decisions."""
    monkeypatch.setattr(pool.gpu_admission.Controller, 'sample', lambda self: {})
    capacity = dict(CAPACITY, gpu=1) if gpu else CAPACITY
    if gpu:
        path = rig.item_path(pool.READY, KEYS[0])
        item = json.loads(path.read_text())
        item['resources']['gpu'] = 1
        path.write_text(json.dumps(item))
    entered, release = threading.Event(), threading.Event()
    read_json = adaptive_cpu.read_json
    reads = []
    outcome = {}

    def paused_read(path):
        path = Path(path)
        if path.name == KEYS[0] + '.json' and 'requests' in path.parts:
            reads.append(path)
            if len(reads) == read_number:
                entered.set()
                assert release.wait(30), 'test did not release the action request read'
        return read_json(path)

    monkeypatch.setattr(adaptive_cpu, 'read_json', paused_read)

    def claim(queue):
        return queue.claim(capacity=capacity, cpu_tiers=TIERS,
                           adaptive_cpu=True, has_gpu=gpu)

    def run():
        try:
            outcome['item'] = claim(rig)
        except BaseException as exc:
            outcome['error'] = exc

    stalled = threading.Thread(target=run, daemon=True)
    stalled.start()
    try:
        assert entered.wait(30), 'candidate did not read its sealed request'
        winner = _bounded(lambda: claim(pool.PoolQueue(rig.root)), 'sibling claim', 10.)
        assert winner is not None and winner['action_key'] == KEYS[1], (
            'a stalled sealed request read held host admission and prevented useful work')
    finally:
        release.set()
        stalled.join(30)
    assert not stalled.is_alive() and 'error' not in outcome, outcome
    if gpu:
        # No trusted GPU sample: the original candidate still cannot reserve.
        assert outcome['item'] is None
        assert rig.item_path(pool.READY, KEYS[0]).exists()
        assert rig.ledger().held_keys() == [KEYS[1]]
    else:
        assert outcome['item']['action_key'] == KEYS[0]
        assert sorted(rig.ledger().held_keys()) == sorted(KEYS)


@pytest.mark.parametrize('change', ['busy', 'pressure'])
def test_action_request_preparation_does_not_grant_capacity(rig, monkeypatch, change):
    """A delayed immutable read cannot bypass the subsequent live host gates."""
    read_json = adaptive_cpu.read_json
    descriptors, prepared = [], []

    def changed_host(path):
        result = read_json(path)
        if Path(path).name == KEYS[0] + '.json' and 'requests' in Path(path).parts:
            assert _admission_is_free(rig)
            prepared.append(path)
            if change == 'busy':
                fd = os.open(_lock_path(rig), os.O_RDWR)
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                descriptors.append(fd)
            else:
                monkeypatch.setattr(adaptive_cpu.Controller, 'sample', lambda self: {
                    'sampled_unix': time.time(), 'busy_cpus': 4., 'psi_some': 0.,
                    'cpu_count': 4, 'interval_s': 1.})
        return result

    monkeypatch.setattr(adaptive_cpu, 'read_json', changed_host)
    try:
        assert _bounded(lambda: _claim(rig), 'claim after request preparation') is None
    finally:
        for fd in descriptors:
            os.close(fd)
    assert len(prepared) == 1, 'identity was re-read inside the decision'
    assert rig.ledger().held() == {}
    assert rig.ledger().available() == rig.ledger().capacity()
    assert all(rig.item_path(pool.READY, key).exists() for key in KEYS)


def test_empty_poll_cannot_block_a_sibling_on_capacity_refresh(rig, monkeypatch):
    """An old empty READY view must not hold admission ahead of new work."""
    # This loop saw an empty queue just before the two rig items arrived.
    # A sibling's subsequent discovery sees both real records.
    monkeypatch.setattr(rig, 'ready_items', lambda: [])
    ledger = rig.ledger()
    monkeypatch.setattr(rig, 'ledger', lambda: ledger)
    refresh = ledger.configure_cpu_tiers
    reached_or_done, release = threading.Event(), threading.Event()
    outcome = {}

    def stalled_refresh(tiers):
        reached_or_done.set()
        assert release.wait(30), 'test did not release capacity refresh'
        return refresh(tiers)

    monkeypatch.setattr(ledger, 'configure_cpu_tiers', stalled_refresh)

    def run():
        try:
            outcome['item'] = _claim(rig)
        except BaseException as exc:
            outcome['error'] = exc
        finally:
            reached_or_done.set()

    stalled = threading.Thread(target=run, daemon=True)
    stalled.start()
    try:
        assert reached_or_done.wait(30), 'empty poll neither returned nor refreshed'
        winner = _bounded(lambda: _claim(pool.PoolQueue(rig.root)), 'new-work sibling', 10.)
        assert winner is not None, (
            'an empty poll held admission during shared capacity refresh and '
            'blocked newly arrived work')
    finally:
        release.set()
        stalled.join(30)
    assert not stalled.is_alive() and 'error' not in outcome, outcome
    assert outcome['item'] is None
    assert rig.ledger().held_keys() == [winner['action_key']]


def test_empty_adaptive_poll_leaves_capacity_and_active_holders_untouched(rig, monkeypatch):
    """Empty discovery needs no ledger writes; the next candidate reconciles."""
    first = _claim(rig)
    assert first is not None
    ledger = rig.ledger()
    capacity, held = ledger.capacity(), ledger.held()
    # Seed old fallback pacing state: absence from READY retires that hint.
    rig._cpu_deferrals[('f' * 64, '1.0')] = time.monotonic()
    with monkeypatch.context() as patch:
        patch.setattr(rig, 'ready_items', lambda: [])
        assert rig.claim(capacity={'cpu': 1, 'mem_gb': 4}, cpu_tiers=TIERS,
                         adaptive_cpu=True) is None
    assert ledger.capacity() == capacity, 'empty discovery rewrote capacity'
    assert ledger.held() == held
    assert not rig._cpu_deferrals
    # The lower offer still applies when this loop actually sees a candidate.
    assert rig.claim(capacity={'cpu': 1, 'mem_gb': 4}, cpu_tiers=TIERS,
                     adaptive_cpu=True) is None
    assert ledger.capacity()['cpu'] == 1
    assert ledger.held() == held
