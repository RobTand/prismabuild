"""A GPU host leaves CPU-only work to a host without a GPU that can run it (#1262).

A GPU host's CPUs feed its GPU.  On 2026-09-28 both GB10s sat idle for 100
minutes while their GPU rows were refused on CPU behind CPU-only rows that a
host without a GPU could have run.  The rule: while a live, matching host
without a GPU fits a CPU-only row now and has not passed on it, a GPU host
does not claim it.  There is no timer; the wait ends on evidence, and CPU-only
work still overflows onto the GPU hosts when the CPU host cannot take it.
"""
import json
import socket
import time

import pytest

from prismabuild import pool

CAPACITY = {'cpu': 4, 'gpu': 1, 'mem_gb': 4}
CPU_CAPACITY = {'cpu': 4, 'mem_gb': 4}
TIERS = {'preferred': [0, 1, 2, 3], 'fallback': []}
CPU_HOST = 'cpuhost'


def _detail(*, stale=False, gpu=True):
    age = 10 if stale else 0
    detail = {'observed_unix': time.time() - age, 'load1': 0.}
    if gpu:
        # An idle GPU: the bounded cross-resource preference never fires.
        detail.update(gpu_power_fraction=.05, gpu_power_sampled_unix=time.time() - age)
    return detail


@pytest.fixture
def fleet(tmp_path):
    queue = pool.PoolQueue(tmp_path / 'queue')

    def announce(host, *, has_gpu, tags=(), stale=False, state=None):
        capacity = CAPACITY if has_gpu else CPU_CAPACITY
        ledger = queue.ledger(host)
        ledger.configure_cpu_tiers(TIERS)
        ledger.ensure_capacity(capacity)
        # A parked loop keeps its declared capacity and announces zero
        # observed capacity (#1204), as the worker loop does.
        observed = {k: 0 for k in capacity} if state == 'draining' else capacity
        queue.announce(host=host, tags=list(tags), has_gpu=has_gpu, capacity=capacity,
                       observed_capacity=observed, cpu_tiers=TIERS,
                       observed_detail=_detail(stale=stale, gpu=has_gpu), state=state)

    announce(socket.gethostname(), has_gpu=True, tags=['gb10'])
    announce(CPU_HOST, has_gpu=False, tags=['x86'])

    def publish(key, resources, tags=None):
        queue.publish(action_key=key, cas_root=str(tmp_path / 'cas'),
                      checkout_root=str(tmp_path), worker_script='worker.py',
                      resources=resources, **({'tags': tags} if tags else {}))
        return pool._read_json(queue.item_path(pool.READY, key))

    def claim(has_gpu=True):
        return queue.claim(tags=['gb10'], has_gpu=has_gpu, capacity=CAPACITY,
                           cpu_tiers=TIERS)

    return queue, announce, publish, claim


def _local_denial(queue, key):
    records = queue.latest_denials({key}).get(key) or []
    mine = [r for r in records if r.get('host') == socket.gethostname()]
    return mine[0] if mine else None


def _cpu_host_denies(queue, item, reason, *, filler=0):
    """Publish ``CPU_HOST``'s latest denial for ``item`` the way its loop does."""
    identity = f"{item['action_key']}:{repr(float(item['published_unix']))}"
    records = {f"{'f' * 63}{n % 10}:{n}.0": {
        'action_key': 'f' * 64, 'published_unix': float(n), 'host': CPU_HOST,
        'reason': 'placement_mismatch', 'denied_unix': time.time()} for n in range(filler)}
    if reason is not None:
        records[identity] = {'action_key': item['action_key'],
                             'published_unix': float(item['published_unix']),
                             'host': CPU_HOST, 'reason': reason, 'denied_unix': time.time()}
    path = queue.root / pool.RESERVATIONS / CPU_HOST / 'adaptive' / pool.CLAIM_DENIALS
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({'schema': pool.CLAIM_DENIALS_SCHEMA_V1, 'records': records}))


def test_a_cpu_only_row_is_left_to_a_host_without_a_gpu(fleet):
    """The failure: an idle GPU host claimed CPU work the CPU host could run."""
    queue, announce, publish, claim = fleet
    publish('a' * 64, {'cpu': 2, 'mem_gb': 2})
    assert claim() is None
    assert not queue.ledger().held_keys()
    denial = _local_denial(queue, 'a' * 64)
    assert denial and denial['reason'] == 'deferred_for_cpu_only_host'
    assert denial['evidence']['cpu_host']['host'] == CPU_HOST
    assert not queue.passes_path('a' * 64).exists()     # not a refusal: ages nothing


def test_the_wait_has_no_timer(fleet, monkeypatch):
    """Unlike the bounded preference, it does not give up after 20 seconds."""
    queue, announce, publish, claim = fleet
    publish('a' * 64, {'cpu': 2, 'mem_gb': 2})
    monkeypatch.setattr(pool.time, 'monotonic', lambda: 10.)
    assert claim() is None
    monkeypatch.setattr(pool.time, 'monotonic', lambda: 10. + 3600.)
    assert claim() is None


@pytest.mark.parametrize('reason', ['reservation_unavailable', 'adaptive_cpu_refused',
                                    'placement_mismatch'])
def test_once_the_cpu_host_passes_on_it_the_gpu_host_claims(fleet, reason):
    """Overflow: a CPU host that looked and did not take the row releases it."""
    queue, announce, publish, claim = fleet
    item = publish('a' * 64, {'cpu': 2, 'mem_gb': 2})
    assert claim() is None
    _cpu_host_denies(queue, item, reason)
    claimed = claim()
    assert claimed and claimed['action_key'] == 'a' * 64


def test_a_cpu_host_loop_evaluating_the_row_is_not_a_verdict(fleet):
    queue, announce, publish, claim = fleet
    item = publish('a' * 64, {'cpu': 2, 'mem_gb': 2})
    _cpu_host_denies(queue, item, 'transition_busy')
    assert claim() is None


def test_a_full_denial_file_falls_back_to_the_reason_ring(fleet):
    """The latest-denial file keeps its newest records only; the ring is per key."""
    queue, announce, publish, claim = fleet
    item = publish('a' * 64, {'cpu': 2, 'mem_gb': 2})
    _cpu_host_denies(queue, item, None, filler=pool.MAX_CLAIM_DENIALS)
    assert claim() is None                     # full, and the ring names nothing yet
    queue._record_denial_transition(item, host=CPU_HOST, reason='reservation_unavailable',
                                    decision_reason=None)
    claimed = claim()
    assert claimed and claimed['action_key'] == 'a' * 64


@pytest.mark.parametrize('why', ['stale', 'full', 'mismatch', 'gone', 'draining'])
def test_without_a_cpu_host_that_can_take_it_the_gpu_host_claims(fleet, why):
    queue, announce, publish, claim = fleet
    tags = None
    if why == 'stale':
        announce(CPU_HOST, has_gpu=False, tags=['x86'], stale=True)
    elif why == 'full':
        assert queue.ledger(CPU_HOST).acquire('b' * 64, {'cpu': 3})
    elif why == 'draining':
        announce(CPU_HOST, has_gpu=False, tags=['x86'], state='draining')
    elif why == 'mismatch':
        tags = ['gb10']                        # aarch64-only work: no CPU host matches
    else:
        (queue.root / 'workers' / f'{CPU_HOST}.json').unlink()
    publish('a' * 64, {'cpu': 2, 'mem_gb': 2}, tags=tags)
    claimed = claim()
    assert claimed and claimed['action_key'] == 'a' * 64


def test_a_host_without_a_gpu_never_yields(fleet):
    """No two hosts can wait on each other: a CPU host claims its own work."""
    queue, announce, publish, claim = fleet
    announce('othercpu', has_gpu=False, tags=['x86'])
    publish('a' * 64, {'cpu': 2, 'mem_gb': 2})
    claimed = claim(has_gpu=False)
    assert claimed and claimed['action_key'] == 'a' * 64


def test_a_gpu_row_is_never_left_to_a_cpu_host(fleet):
    queue, announce, publish, claim = fleet
    publish('a' * 64, {'cpu': 1, 'gpu': 1, 'mem_gb': 1})
    claimed = claim()
    assert claimed and claimed['action_key'] == 'a' * 64


def test_one_pass_never_leaves_a_cpu_host_more_than_it_fits(fleet):
    """Each yield charges this pass's view, so the overflow is placed at once."""
    queue, announce, publish, claim = fleet
    publish('a' * 64, {'cpu': 3, 'mem_gb': 1})
    publish('c' * 64, {'cpu': 3, 'mem_gb': 1})
    claimed = claim()
    assert claimed and claimed['action_key'] in {'a' * 64, 'c' * 64}
    left = ({'a' * 64, 'c' * 64} - {claimed['action_key']}).pop()
    assert _local_denial(queue, left)['reason'] == 'deferred_for_cpu_only_host'
