"""Authorized 1521 exception: continuous offer evidence, one bounded loan."""
from __future__ import annotations


import pytest

from test_gang_fence_backfill import waiting
from test_gang_reservation_1517 import CAPACITY, TIERS, gang_fleet  # noqa: F401
from test_measurement_drains_gpu_backfill import fleet  # noqa: F401
from prismabuild import _gang, pool


@pytest.fixture
def absent_partner(gang_fleet, fleet, monkeypatch):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    monkeypatch.setattr(_gang, "monotonic", lambda: clock[0], raising=False)
    group, keys, incumbent = waiting(gang_fleet)
    queue.announce(host="sparky", tags=["gb10", "sparky", _gang.TAG], has_gpu=True,
                   capacity=CAPACITY, cpu_tiers=TIERS)
    offer = pool._read_json(queue.root / pool.WORKERS / "sparky.json")
    clock[0] += pool.OFFER_TIMEOUT_S + 1
    def local(q=queue):
        monkeypatch.setattr(pool.socket, "gethostname", lambda: "sparklina")
        fleet[5](0.01)
        got = q.claim(capacity=CAPACITY, cpu_tiers=TIERS, adaptive_cpu=True,
                      has_gpu=True, tags=["gb10", "sparklina", _gang.TAG])
        return None if got is None else got["action_key"]
    def observe(seconds):
        assert local() is None
        for _ in range(int(seconds / 30)):
            clock[0] += 30
            assert local() is None
    def job(name="bounded", timeout=900, *, gpu=1, mem_gb=24):
        return publish(name, priority=0, retry_safe=False, max_attempts=1,
                       timeout_s=timeout, gpu=gpu, mem_gb=mem_gb, tags=["sparklina"])
    return queue, clock, group, keys, incumbent, offer, local, observe, job, finish


@pytest.mark.parametrize("timeout,allowed", [(900, True), (1800, True), (1860, False), (None, False)])
def test_only_declared_thirty_minute_or_shorter_jobs_receive_the_absence_loan(
        absent_partner, timeout, allowed):
    queue, clock, group, keys, incumbent, offer, local, observe, job, finish = absent_partner
    observe(600)
    key = job(timeout=timeout)
    got = local()
    if allowed:
        assert got == key, "sustained readable partner absence did not admit the bounded job"
        held = pool._read_json(queue.item_path(pool.CLAIMED, key))
        assert held["gang_bounded_backfill"][0]["execution_timeout_s"] == timeout
        second = job("second-bounded", timeout=900)
        assert local() is None, "two bounded absence loans overlapped on one host"
        assert queue.item_path(pool.READY, second).exists()
    else:
        assert got is None, "absence loan admitted an unbounded or over-thirty-minute job"
        assert queue.item_path(pool.READY, key).exists()


def test_a_transient_missing_offer_does_not_admit_a_bounded_job(absent_partner):
    queue, clock, group, keys, incumbent, offer, local, observe, job, finish = absent_partner
    (queue.root / pool.WORKERS / "sparky.json").unlink()
    observe(570)
    key = job()
    assert local() is None
    assert queue.item_path(pool.READY, key).exists()


def test_two_distant_observations_cannot_claim_continuous_absence(absent_partner):
    queue, clock, group, keys, incumbent, offer, local, observe, job, finish = absent_partner
    assert local() is None
    clock[0] += 601
    key = job()
    assert local() is None, "an unobserved gap earned absence credit"
    assert queue.item_path(pool.READY, key).exists()


@pytest.mark.parametrize("state,tags", [(None, ["wrong-tags"]), ("draining", ["sparky", _gang.TAG])])
def test_any_fresh_offer_resets_the_absence_window(absent_partner, state, tags):
    queue, clock, group, keys, incumbent, offer, local, observe, job, finish = absent_partner
    observe(570)
    queue.announce(host="sparky", tags=tags, has_gpu=True, capacity=CAPACITY,
                   cpu_tiers=TIERS, state=state)
    assert local() is None
    pool._write_json_atomic(queue.root / pool.WORKERS / "sparky.json", offer)
    observe(30)
    key = job()
    assert local() is None, "a fresh or draining host did not reset the evidence window"
    assert queue.item_path(pool.READY, key).exists()


def test_unreadable_offer_resets_and_never_grants_the_exception(absent_partner):
    queue, clock, group, keys, incumbent, offer, local, observe, job, finish = absent_partner
    observe(570)
    path = queue.root / pool.WORKERS / "sparky.json"
    path.write_text("{broken offer", encoding="utf-8")
    key = job()
    assert local() is None
    pool._write_json_atomic(path, offer)
    clock[0] += 30
    assert local() is None, "an unreadable interval counted as continuous absence"
    assert queue.item_path(pool.READY, key).exists()


def test_losing_the_durable_window_cannot_grant_an_immediate_loan(absent_partner):
    queue, clock, group, keys, incumbent, offer, local, observe, job, finish = absent_partner
    observe(600)
    path = _gang.state_dir(queue, group) / "absence-0-1.json"
    assert path.exists(), "absence evidence has no durable record"
    path.unlink()
    key = job()
    assert local() is None, "a lost observation window was reconstructed as already qualified"
    assert queue.item_path(pool.READY, key).exists()


def test_an_observer_restart_cannot_advance_an_old_window(absent_partner):
    queue, clock, group, keys, incumbent, offer, local, observe, job, finish = absent_partner
    observe(570)
    restarted = pool.PoolQueue(queue.root)
    clock[0] += 30
    key = job()
    assert local(restarted) is None, "a restarted observer credited an unobserved restart gap"
    assert queue.item_path(pool.READY, key).exists()


def test_partner_return_waits_for_one_bounded_job_and_resets_the_window(absent_partner, gang_fleet):
    queue, clock, group, keys, incumbent, offer, local, observe, job, finish = absent_partner
    observe(600)
    key = job()
    assert local() == key, "sustained absence never admitted its bounded loan"
    queue.announce(host="sparky", tags=["gb10", "sparky", _gang.TAG], has_gpu=True,
                   capacity=CAPACITY, cpu_tiers=TIERS)
    finish(incumbent, "sparky")
    assert local() is None
    assert queue.ledger("sparklina").held_keys() == [key]
    assert not queue.withdrawal_decisions(key), "non-restartable bounded work was preempted"
    finish(key, "sparklina")
    assert local() is None  # actually ready; peer must make its pass too
    assert gang_fleet[4]("sparky") == keys[1]
    assert local() == keys[0]
    window = pool._read_json(_gang.state_dir(queue, group) / "absence-0-1.json")
    assert window["covered_s"] == 0


def test_the_absence_loan_switch_restores_the_strict_fence(absent_partner, monkeypatch):
    queue, clock, group, keys, incumbent, offer, local, observe, job, finish = absent_partner
    monkeypatch.setenv("PRISMABUILD_GANG_ABSENCE_BACKFILL", "0")
    observe(600)
    key = job()
    assert local() is None
    assert queue.item_path(pool.READY, key).exists()



def test_a_partial_inventory_from_an_unrelated_bad_offer_resets_the_window(absent_partner):
    queue, clock, group, keys, incumbent, offer, local, observe, job, finish = absent_partner
    observe(570)
    path = queue.root / pool.WORKERS / "unrelated.json"
    pool._write_json_atomic(path, {"schema": "broken", "host": "unrelated",
                                  "announced_unix": clock[0], "tags": [], "capacity": {}})
    assert local() is None
    path.unlink()
    clock[0] += 30
    key = job()
    assert local() is None, "a partial inventory credited the absence window"
    assert queue.item_path(pool.READY, key).exists()


@pytest.mark.parametrize("failures,allowed", [(1, True), (2, False)])
def test_estale_counts_only_after_retry_to_a_successful_offer_read(
        absent_partner, monkeypatch, failures, allowed):
    import errno
    queue, clock, group, keys, incumbent, offer, local, observe, job, finish = absent_partner
    observe(570)
    clock[0] += 30
    original = pool._read_json
    remaining = [failures]
    def stale(path, *args, **kwargs):
        if str(path).endswith("workers/sparky.json") and remaining[0]:
            remaining[0] -= 1
            raise OSError(errno.ESTALE, "stale test offer")
        return original(path, *args, **kwargs)
    monkeypatch.setattr(pool, "_read_json", stale)
    key = job()
    got = local()
    assert (got == key) is allowed, "ESTALE retry success/unknown was misclassified"



def test_only_one_bounded_cpu_loan_can_occupy_a_fenced_host(absent_partner):
    queue, clock, group, keys, incumbent, offer, local, observe, job, finish = absent_partner
    observe(600)
    first = job("small-cpu-first", gpu=0, mem_gb=1)
    assert local() == first
    second = job("small-cpu-second", gpu=0, mem_gb=1)
    assert local() is None, "two otherwise-fitting bounded CPU loans overlapped"
    assert queue.item_path(pool.READY, second).exists()
    assert queue.ledger("sparklina").held_keys() == [first]

