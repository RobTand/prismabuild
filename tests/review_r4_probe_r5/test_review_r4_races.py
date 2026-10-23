"""Opus review r4 probes: R3-B1 at the deadline edge and the non-blocking items.

The e2e fixture binds Core, the real broker Authority/ResourceMonitor, Pool and
every reader to one action, nonce and scope (adopted from review r3). A
failing test is a finding. CPU-only fakes; no GPU.
"""
import json
import os
import struct
import threading
import time
import warnings

import pytest

from prismabuild import census, core as pb, endings, pool
from test_gpu_review_r4 import (WORKER, _agreement, _broker, _finish_and_read, _lifetime, _route,  # noqa: F401
                                _run_core, e2e)
from test_resource_broker import authority, monitored  # noqa: F401

PERIODIC = 'r4-periodic-guard-poll'
INFLIGHT = {'used': lambda e: {e.scope: 'GPU-inflight'}, 'fail': lambda e: 'fail', 'zero': lambda e: {}}


class _WatchedLock:
    """monitor.sample_lock; hooks fire only on the final observation's timed join."""

    def __init__(self, lock, *, on_wait=None, on_timeout=None):
        self.lock, self.on_wait, self.on_timeout, self.joins = lock, on_wait, on_timeout, []

    def acquire(self, blocking=True, timeout=-1):
        if timeout == -1:
            return self.lock.acquire(blocking)
        if self.on_wait:
            self.on_wait(timeout)
        got = self.lock.acquire(timeout=timeout)
        self.joins.append((round(timeout, 3), got))
        if not got and self.on_timeout:
            self.on_timeout()
        return got

    def release(self):
        self.lock.release()

    def __enter__(self):
        self.lock.acquire()
        return self

    def __exit__(self, *exc):
        self.lock.release()


def _start_inflight(e, inflight):
    """A periodic census that starts while argv is alive and blocks until released."""
    entered, release = threading.Event(), threading.Event()

    def plan():
        if threading.current_thread().name == PERIODIC:
            entered.set()
            assert release.wait(10)
            return INFLIGHT[inflight](e)
        return {}

    e.alive['value'] = True
    e.gpu.plan = plan
    poller = threading.Thread(target=e.live.poll_once, name=PERIODIC, daemon=True)
    poller.start()
    assert entered.wait(5)
    e.alive['value'] = False                       # argv exits; the worker finalizes
    return poller, release


def _frozen(e):
    record = e.a.records[e.scope].get('gpu_record')
    return None if record is None else pb.gpu_execution_judgment(record)


# -- R3-B1: the join includes a sample that completes inside the window -------

@pytest.mark.parametrize('inflight', sorted(INFLIGHT))
def test_b1_sample_released_while_the_final_waits_is_included(e2e, inflight):
    _lifetime(e2e, ['zero'])
    poller, release = _start_inflight(e2e, inflight)

    def on_wait(timeout):
        # Released only after the final observation is blocked on the join.
        threading.Timer(min(0.05, timeout / 4), release.set).start()

    e2e.live.sample_lock = _WatchedLock(e2e.live.sample_lock, on_wait=on_wait)
    try:
        core = _run_core(e2e, 'deliver')
    finally:
        release.set()
        poller.join(10)
    frozen = _frozen(e2e)
    readers = _finish_and_read(e2e, core)
    expected = {'used': ('used', 'attributed_gpu_uuid'), 'fail': ('unobserved', 'sampler_error'),
                'zero': ('zero', 'successful_payload_sample')}[inflight]
    got = (core['outcome'], frozen and (frozen['gpu_use'], frozen['gpu_use_reason']))
    warnings.warn(f'R4-B1-JOIN {inflight}: joins={e2e.live.sample_lock.joins} core/frozen={got} '
                  f'pool={readers["dir"]}/{readers["pool"]}')
    assert got == (expected[0], expected) and _agreement(core, readers, expected[0])[0], (
        f'{inflight}: joins={e2e.live.sample_lock.joins} core/frozen={got} readers={readers}')


@pytest.mark.parametrize('repeat', range(4))
@pytest.mark.parametrize('inflight', sorted(INFLIGHT))
def test_b1_sample_retained_after_join_timeout_but_before_freeze(e2e, inflight, repeat):
    """Deadline edge: the join times out, then the in-flight retain wins the authority lock."""
    _lifetime(e2e, ['zero'])
    poller, release = _start_inflight(e2e, inflight)
    retained, order = threading.Event(), {}
    real_retain = e2e.live._retain_gpu_observation

    def retain(*args, **kwargs):
        try:
            return real_retain(*args, **kwargs)
        finally:
            if threading.current_thread().name == PERIODIC:
                retained.set()

    def on_timeout():
        release.set()
        order['retained_before_freeze'] = retained.wait(0.03)

    e2e.monkeypatch.setattr(e2e.live, '_retain_gpu_observation', retain)
    e2e.live.sample_lock = _WatchedLock(e2e.live.sample_lock, on_timeout=on_timeout)
    try:
        core = _run_core(e2e, 'deliver')
    finally:
        release.set()
        poller.join(10)
    frozen = _frozen(e2e)
    readers = _finish_and_read(e2e, core)
    state = frozen['gpu_use'] if frozen else 'unobserved'
    message = (f'{inflight}: order={order} joins={e2e.live.sample_lock.joins} core={core["outcome"]} '
               f'frozen={frozen and (frozen["gpu_use"], frozen["gpu_use_reason"])} readers={readers}')
    warnings.warn(f'R4-B1-EDGE {message}')
    assert core['outcome'] != 'zero' and readers['pool'] != 'zero', message
    assert _agreement(core, readers, core['outcome'])[0], message
    if inflight == 'used' and frozen is not None and order.get('retained_before_freeze'):
        assert state == 'used', message
    if frozen is not None and state == 'unobserved':
        assert frozen['gpu_use_reason'] == 'sampler_in_flight_at_freeze', message


def test_b1_deadline_expires_while_the_freeze_waits_for_a_slow_retain(e2e):
    """The in-flight retain holds the authority lock past the deadline: no commit, all unobserved."""
    _lifetime(e2e, ['zero'])
    poller, release = _start_inflight(e2e, 'used')
    in_retain, box = threading.Event(), {}
    real_retain = e2e.live._retain_gpu_observation

    def retain(*args, **kwargs):
        if threading.current_thread().name != PERIODIC:
            return real_retain(*args, **kwargs)
        with e2e.a.lock:
            in_retain.set()
            time.sleep(max(0.0, box['deadline'] - time.monotonic()) + 0.05)
            return real_retain(*args, **kwargs)

    def on_timeout():
        release.set()
        assert in_retain.wait(2)

    route = _route(e2e, 'deliver')

    def capture(action_, op, **fields):
        if op == 'gpu_observation':
            box['deadline'] = fields['deadline_monotonic']
        return route(action_, op, **fields)

    e2e.monkeypatch.setattr(e2e.live, '_retain_gpu_observation', retain)
    e2e.live.sample_lock = _WatchedLock(e2e.live.sample_lock, on_timeout=on_timeout)
    e2e.monkeypatch.setattr(pb, '_gpu_broker_request', capture)
    try:
        try:
            result = pb.run_local_action(e2e.action, cas_root=e2e.cas.root, checkout_root=e2e.checkout,
                                         resource_demand=e2e.queue.demand_of(e2e.item))
            core = {'outcome': result['receipt']['resource_telemetry']['gpu_use'], 'result': result, 'error': ''}
        except pb.LocalActionError as error:
            core = {'outcome': 'zero', 'error': str(error), 'result': None}
    finally:
        release.set()
        poller.join(10)
    readers = _finish_and_read(e2e, core)
    warnings.warn(f'R4-B1-EXPIRY core={core["outcome"]} pool={readers["dir"]}/{readers["pool"]} '
                  f'({readers["reason"]}) committed={e2e.a.records[e2e.scope].get("gpu_record") is not None}')
    assert e2e.a.records[e2e.scope].get('gpu_record') is None, 'broker committed after the deadline'
    assert core['outcome'] == 'unobserved' and _agreement(core, readers, 'unobserved')[0], (core, readers)


def test_na_stop_landing_during_the_join_is_skipped_not_zero(e2e):
    """N-a variant: the scope is stopped while the final waits for the in-flight sample."""
    _lifetime(e2e, ['zero'])
    poller, release = _start_inflight(e2e, 'zero')
    stop = {'op': 'stop', 'action_key': e2e.action['action_key'], 'nonce': e2e.nonce, 'token': e2e.token,
            'reason': 'action execution timeout'}

    def on_wait(timeout):
        e2e.a.handle(e2e.a.uid, os.getpid(), stop)
        release.set()

    e2e.live.sample_lock = _WatchedLock(e2e.live.sample_lock, on_wait=on_wait)
    try:
        e2e.a.handle(e2e.a.uid, WORKER, {'op': 'gpu_observation', 'action_key': e2e.action['action_key'],
                                         'nonce': e2e.nonce, 'deadline_monotonic': time.monotonic() + 5})
    finally:
        release.set()
        poller.join(10)
    frozen = _frozen(e2e)
    assert (frozen['gpu_use'], frozen['gpu_use_reason']) == ('unobserved', 'final_census_skipped'), frozen


# -- N-e: a record-less "used" projection establishes nothing -----------------

def test_ne_recordless_used_projection_is_unobserved_for_every_reader(e2e):
    _lifetime(e2e, ['used'])
    core = _run_core(e2e, 'deliver')
    readers = _finish_and_read(e2e, core)
    assert readers['pool'] == 'used'
    key = e2e.action['action_key']
    path = e2e.queue.records.item_path(pool.record_store.DONE, key)
    record = json.loads(path.read_text())
    telemetry = dict(record['detail']['resource_telemetry'])
    telemetry.pop('gpu_record')
    record['detail']['resource_telemetry'] = {**telemetry, 'gpu_use': 'used', 'gpu_use_reason': 'attributed_gpu_uuid'}
    path.write_text(json.dumps(record))
    entry = next(x for x in os.scandir(path.parent) if x.path == str(path))
    row = census.ending_row(entry, e2e.queue.root)
    adopted = endings.resolve_attempt(
        [{**e2e.queue.attempt_outcomes(record)[-1], 'detail': record['detail']}],
        invalid=pool.PoolContractError, validate_output_retry_stop=e2e.queue._validate_output_retry_stop)
    assert (row['gpu_use'], row['gpu_use_reason'], adopted['detail']['gpu_use']) == (
        'unobserved', 'final_observation_failed', 'unobserved'), (row['gpu_use'], adopted['detail']['gpu_use'])


# -- New: one malformed GPU record and the whole endings census -----------------

def test_census_reports_a_malformed_gpu_record_as_one_unreadable_row(e2e):
    _lifetime(e2e, ['used'])
    core = _run_core(e2e, 'deliver')
    _finish_and_read(e2e, core)
    key = e2e.action['action_key']
    path = e2e.queue.records.item_path(pool.record_store.DONE, key)
    record = json.loads(path.read_text())
    record['detail']['resource_telemetry']['gpu_record']['resource_telemetry']['gpu_evidence']['added_field'] = True
    path.write_text(json.dumps(record))
    try:
        rows = census.endings(e2e.queue.root)
    except Exception as exc:                                      # noqa: BLE001
        pytest.fail(f'census.endings raised {type(exc).__name__}: {str(exc)[:120]} for one record with an '
                    'unknown gpu_evidence field; the census contract files an unreadable row instead')
    assert any(r.get('unreadable') for r in rows if r.get('action_key') in (key, None)), rows


# -- New: a same-UID read lock in the publication lock's creation window -------

def test_a_foreign_read_lock_in_the_creation_window_does_not_erase_a_zero(e2e):
    """The broker creates <scope>.lock (0444, in a 0755 directory) and only then
    takes its nonblocking write lock. Any same-UID process that read-locks the
    file in between makes the commit fail, even for another attempt's scope."""
    _lifetime(e2e, ['zero', 'zero'])
    fc = _broker(e2e.a)['fcntl']
    real = fc.fcntl
    held = []

    def racing(fd, cmd, arg=0):
        if (cmd == fc.F_OFD_SETLK and not held and isinstance(arg, bytes)
                and struct.unpack('hhqqii', arg)[0] == fc.F_WRLCK):
            target = os.readlink(f'/proc/self/fd/{fd}')
            if target.endswith('.lock'):
                other = os.open(target, os.O_RDONLY)
                real(other, fc.F_OFD_SETLK, struct.pack('hhqqii', fc.F_RDLCK, os.SEEK_SET, 0, 0, 0, 0))
                held.append(other)
        return real(fd, cmd, arg)

    e2e.monkeypatch.setattr(fc, 'fcntl', racing)
    try:
        core = _run_core(e2e, 'deliver')
    finally:
        for descriptor in held:
            os.close(descriptor)
    readers = _finish_and_read(e2e, core)
    assert held, 'harness did not reach the creation window'
    assert core['outcome'] == 'unobserved' and _agreement(core, readers, 'unobserved')[0] and readers['receipt'], (
        f"a read lock taken between O_CREAT and the broker's write lock refused the commit: "
        f"Core={core['outcome']}, Pool filed {readers['dir']}/ gpu_use={readers['pool']} ({readers['reason']}), "
        f"receipt={readers['receipt']}")
