"""#1498 follow-up: a large measurement batch must not livelock admission.

2026-10-05 ~03:10Z: 38-49 READY PACT measurement rows, six worker loops on
each Spark. Every claim's census took the transition lock of every READY
measurement key without blocking, while each loop held its own candidate's key
through its pass, so nearly every census met one busy key and both Sparks
denied every row ``measurement_census_unavailable: measurement transition
busy`` with 27 GPU jobs READY. Only elections on this host can fence this
host, so only those keys are locked; one another loop holds mid-transition is
a live fence for the pass, not a refusal.

The real queue, ledgers, census, controllers and transition locks of the
#1185/#1419 fixture. Other worker loops are threads that hold a candidate's
transition lock, exactly as a loop does through its pass.
"""
from __future__ import annotations

from contextlib import contextmanager
import threading

from test_measurement_reservation_backfill_1419 import (
    _bounded_measurement_wait, fleet as fleet_fixture,
)

from prismabuild import pool

fleet = fleet_fixture
CAPACITY = {"cpu": 20, "gpu": 1, "mem_gb": 120}
TIERS = {"preferred": list(range(20)), "fallback": []}
OTHER_LOOPS = 11  # twelve loops in all: two Sparks x six


@contextmanager
def _loops_holding(queue, keys):
    """Each key held by its own thread, as another worker loop's candidate."""
    taken = [threading.Event() for _ in keys]
    release = threading.Event()
    results: list[bool] = [False] * len(keys)

    def hold(index, key):
        with queue._transition_locked(key, blocking=False) as acquired:
            results[index] = bool(acquired)
            taken[index].set()
            release.wait(timeout=60)

    threads = [threading.Thread(target=hold, args=(index, key), daemon=True)
               for index, key in enumerate(keys)]
    for thread in threads:
        thread.start()
    for event in taken:
        assert event.wait(timeout=30)
    assert all(results), "a simulated loop could not take its candidate's lock"
    try:
        yield
    finally:
        release.set()
        for thread in threads:
            thread.join(timeout=30)


def _claim_only(queue, key, host="sparklina"):
    row = pool._read_json(queue.item_path(pool.READY, key))
    assert isinstance(row, dict)
    result = queue.claim(capacity=CAPACITY, cpu_tiers=TIERS, adaptive_cpu=True,
                         has_gpu=True, tags=["gb10", host], ready=[row])
    return None if result is None else result["action_key"]


def test_twelve_loops_and_forty_ready_measurements_still_admit(fleet):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    measurements = [publish(f"pact-quantum-{index:02d}", measurement=True, priority=0)
                    for index in range(40)]
    candidate = publish("cpu-proof", priority=0, timeout_s=600, cpu=1, gpu=0, mem_gb=1)
    tick(0.01)
    with _loops_holding(queue, measurements[:OTHER_LOOPS]):
        result = _claim_only(queue, candidate)
    assert result == candidate, denial(candidate)
    assert queue.ledger().held_keys() == [candidate]
    for key in measurements:
        assert queue.item_path(pool.READY, key).exists()


def test_a_busy_election_on_this_host_still_fences_lower_priority_work(fleet):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    incumbent, measurement, opportunity, snapshot = _bounded_measurement_wait(fleet)
    lower = publish("lower-refill", priority=-10, timeout_s=600, cpu=1, gpu=0, mem_gb=1)
    higher = publish("higher-work", priority=1, timeout_s=600, cpu=1, gpu=0, mem_gb=1)
    tick(0.01)
    with _loops_holding(queue, [measurement]):
        assert _claim_only(queue, lower) is None
        fenced = denial(lower)
        # Conservatively fenced by the elected measurement, not a census refusal.
        assert fenced["reason"] == "deferred_for_measurement_reservation", fenced
        assert fenced["evidence"]["withheld_for"] == measurement
        assert _claim_only(queue, higher) == higher, denial(higher)
    assert set(queue.ledger().held_keys()) == {incumbent, higher}
    assert queue.item_path(pool.READY, lower).exists()


def test_a_busy_election_for_another_host_does_not_block_this_one(fleet, monkeypatch):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    incumbent, measurement, opportunity, snapshot = _bounded_measurement_wait(fleet)
    other = publish("sparky-work", priority=-10, timeout_s=600, cpu=1, gpu=0, mem_gb=1)
    tick(0.01)
    monkeypatch.setattr(pool.socket, "gethostname", lambda: "sparky")
    with _loops_holding(queue, [measurement]):
        assert _claim_only(queue, other, host="sparky") == other, denial(other, "sparky")
    assert queue.ledger("sparky").held_keys() == [other]
    assert queue.ledger("sparklina").held_keys() == [incumbent]
