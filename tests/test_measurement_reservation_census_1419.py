"""Exact publication/lock/restart boundary for the UNKNOWN-first #1419 slice."""
from concurrent.futures import ThreadPoolExecutor
from contextvars import ContextVar
import os
import threading

import pytest

from prismabuild import _bounded_reader as reader
from prismabuild import _measurement_reservation as reservation, adaptive_cpu, pool
from test_measurement_reservation_backfill_1419 import (
    _bounded_measurement_wait, _observe_real_sharing_permission, fleet as fleet_fixture,
)

fleet = fleet_fixture


def _chosen(queue, key):
    record = pool._read_json(queue.passes_path(key))
    assert isinstance(record, dict)
    chosen = reservation.selection(record)
    assert chosen is not None
    return chosen


def test_stale_prefetch_cannot_omit_an_elected_measurement(fleet):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    holder, measurement, opportunity, snapshot = _bounded_measurement_wait(fleet)
    lower = publish("omitted-measurement", timeout_s=5)
    _observe_real_sharing_permission(fleet, lower)
    candidate = pool._read_json(queue.item_path(pool.READY, lower))
    result = queue.claim(capacity={"cpu": 20, "gpu": 1, "mem_gb": 120},
                         cpu_tiers={"preferred": list(range(20)), "fallback": []},
                         adaptive_cpu=True, has_gpu=True, tags=["gb10", "sparklina"],
                         ready=[candidate])
    assert result is None
    refusal = denial(lower)
    assert refusal["reason"] == "deferred_for_measurement_reservation"
    assert refusal["evidence"]["candidate_release_bound"] == "UNKNOWN"
    assert refusal["evidence"]["selection"] == _chosen(queue, measurement)
    assert queue.ledger().held_keys() == [holder]


def test_new_unlocked_measurement_at_refresh_restarts_without_tokens(fleet, monkeypatch):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    lower = publish("refresh-candidate")
    original = reservation.CensusReader.capture
    reads = []
    measurement = []
    def capture(self):
        result = original(self)
        reads.append(result)
        if len(reads) == 1:
            measurement.append(publish("arrives-before-H-refresh", measurement=True, priority=0))
        return result
    monkeypatch.setattr(reservation.CensusReader, "capture", capture)
    # The guarantee starts at canonical election, not at publication: a
    # measurement with no election yet fences nothing, so the refresh does not
    # deny the pass (locking every measurement key did, and livelocked
    # admission under a large measurement batch -- #1498 follow-up). An
    # election for this host is written under this host's H, so the refresh
    # would see it.
    assert claim() == lower, denial(lower)
    assert queue.ledger().held_keys() == [lower]
    assert queue.item_path(pool.READY, measurement[0]).exists()


def test_busy_measurement_key_is_nonblocking_even_with_stale_candidates(fleet):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    holder, measurement, opportunity, snapshot = _bounded_measurement_wait(fleet)
    lower = publish("busy-M-lower")
    candidate = pool._read_json(queue.item_path(pool.READY, lower))
    def candidate_only():
        return queue.claim(capacity={"cpu": 20, "gpu": 1, "mem_gb": 120},
                           cpu_tiers={"preferred": list(range(20)), "fallback": []},
                           adaptive_cpu=True, has_gpu=True, tags=["gb10", "sparklina"],
                           ready=[candidate])
    with queue._transition_locked(measurement), ThreadPoolExecutor(max_workers=1) as executor:
        assert executor.submit(candidate_only).result(timeout=5) is None
    # Still nonblocking and still no refill: the busy elected key is kept as a
    # live election for the pass (#1498 follow-up), not a census refusal.
    assert denial(lower)["reason"] == "deferred_for_measurement_reservation"
    assert denial(lower)["evidence"]["withheld_for"] == measurement
    assert queue.ledger().held_keys() == [holder]


@pytest.mark.parametrize("fault", ["missing", "corrupt", "cpu_unknown", "foreign"])
def test_elected_identity_or_evidence_loss_never_authorizes_refill(fleet, fault, monkeypatch):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    holder, measurement, opportunity, snapshot = _bounded_measurement_wait(fleet)
    chosen = _chosen(queue, measurement)
    assert chosen["opportunity_unix"] == opportunity
    lower = publish("unknown-after-election")
    tick(opportunity - clock[0] + 1)
    _observe_real_sharing_permission(fleet, lower)
    if fault == "missing":
        queue.item_path(pool.READY, measurement).unlink()
    elif fault == "corrupt":
        queue.item_path(pool.READY, measurement).write_text("{")
    elif fault == "cpu_unknown":
        monkeypatch.setattr(adaptive_cpu.Controller, "sample", lambda self: {})
    else:
        readings["foreign"]["2"] = 0.2
        readings["busy_cpus"] = 2.0
    assert claim(q=pool.PoolQueue(queue.root)) is None
    assert _chosen(queue, measurement) == chosen
    assert queue.ledger().held_keys() == [holder]
    assert queue.item_path(pool.READY, lower).exists()


def test_measurement_keys_and_H_cover_begin_but_not_commit(fleet, monkeypatch):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    holder, measurement, opportunity, snapshot = _bounded_measurement_wait(fleet)
    candidate = publish("higher-boundary-candidate", priority=1)
    _observe_real_sharing_permission(fleet, candidate)
    events = []
    def can_take_M():
        with queue._transition_locked(measurement, blocking=False) as acquired:
            return acquired
    begin = pool.ResourceLedger.begin_acquire
    commit = pool.ResourceLedger.commit_acquire
    def acquire(self, *args, **kwargs):
        with ThreadPoolExecutor(max_workers=1) as executor:
            events.append(("begin", executor.submit(can_take_M).result(timeout=5)))
        return begin(self, *args, **kwargs)
    def settle(self, *args, **kwargs):
        with ThreadPoolExecutor(max_workers=1) as executor:
            events.append(("commit", executor.submit(can_take_M).result(timeout=5)))
        return commit(self, *args, **kwargs)
    monkeypatch.setattr(pool.ResourceLedger, "begin_acquire", acquire)
    monkeypatch.setattr(pool.ResourceLedger, "commit_acquire", settle)
    assert claim() == candidate
    assert events == [("begin", False), ("commit", True)]
    assert _chosen(queue, measurement)["host"] == "sparklina"


def test_two_host_election_is_one_persisted_generation_choice(fleet, monkeypatch):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    first = publish("first-host-incumbent", timeout_s=1200)
    assert claim("sparklina") == first
    second = publish("second-host-incumbent", timeout_s=4200)
    assert claim("sparky") == second
    measurement = publish("two-host-election", measurement=True, priority=0)
    host_context = ContextVar("fixture-host", default="sparklina")
    monkeypatch.setattr(pool.socket, "gethostname", host_context.get)
    row = pool._read_json(queue.item_path(pool.READY, measurement))
    start = threading.Barrier(2)
    def elect_on(host):
        token = host_context.set(host)
        try:
            start.wait(timeout=5)
            return pool.PoolQueue(queue.root).claim(
                capacity={"cpu": 20, "gpu": 1, "mem_gb": 120},
                cpu_tiers={"preferred": list(range(20)), "fallback": []},
                adaptive_cpu=True, has_gpu=True, tags=["gb10", host], ready=[row])
        finally:
            host_context.reset(token)
    with ThreadPoolExecutor(max_workers=2) as executor:
        a, b = executor.submit(elect_on, "sparklina"), executor.submit(elect_on, "sparky")
        assert a.result(timeout=10) is None and b.result(timeout=10) is None
    selected = _chosen(queue, measurement)
    assert selected["host"] in ("sparklina", "sparky")
    assert selected["generation"] == queue.attempt_generation(row)
    queue.record_pass(measurement)
    assert _chosen(queue, measurement) == selected


def test_claim_defer_retry_preserves_one_election(fleet):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    holder, measurement, opportunity, snapshot = _bounded_measurement_wait(fleet)
    chosen = _chosen(queue, measurement)
    queue.finish(holder, status="executed")
    tick(11)
    assert claim() == measurement
    assert _chosen(queue, measurement) == chosen
    record = pool._read_json(queue.item_path(pool.CLAIMED, measurement))
    queue._defer_unstarted_claim(record)
    assert _chosen(queue, measurement) == chosen
    assert queue.item_path(pool.READY, measurement).exists()
    tick(11)
    assert claim(q=pool.PoolQueue(queue.root)) == measurement
    assert _chosen(queue, measurement) == chosen
    queue.finish(measurement, status="failed", detail={"returncode": 1})
    retry = pool._read_json(queue.item_path(pool.READY, measurement))
    assert isinstance(retry, dict) and retry["attempts"] == 1
    assert _chosen(queue, measurement) == chosen
    tick(11)
    assert claim(q=pool.PoolQueue(queue.root)) == measurement
    assert _chosen(queue, measurement) == chosen


@pytest.mark.parametrize("ending", ["withdrawal", "terminal", "successor"])
def test_locked_authoritative_retirement_allows_lower_progress(fleet, ending):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    holder, measurement, opportunity, snapshot = _bounded_measurement_wait(fleet)
    old = _chosen(queue, measurement)
    lower = publish("after-authoritative-retirement")
    if ending == "withdrawal":
        queue.withdraw(measurement, reason="test cancellation")
    elif ending == "terminal":
        queue.finish(holder, status="executed")
        tick(11)
        assert claim() == measurement
        queue.finish(measurement, status="executed")
    else:
        queue.item_path(pool.READY, measurement).unlink()
        tick(1)
        same = publish("priority-zero-measurement", measurement=True, priority=0,
                       cpu=8, gpu=1, mem_gb=48)
        assert same == measurement
        # The new generation has not elected a host; old metadata cannot
        # bind its successor. Observe lower with stale prefetch to isolate it.
        successor = pool._read_json(queue.item_path(pool.READY, same))
        assert isinstance(successor, dict)
        assert queue.attempt_generation(successor) != old["generation"]
    if queue.ledger().held_keys():
        _observe_real_sharing_permission(fleet, lower)
    else:
        tick(11)
    candidate = pool._read_json(queue.item_path(pool.READY, lower))
    result = queue.claim(capacity={"cpu": 20, "gpu": 1, "mem_gb": 120},
                         cpu_tiers={"preferred": list(range(20)), "fallback": []},
                         adaptive_cpu=True, has_gpu=True, tags=["gb10", "sparklina"], ready=[candidate])
    assert result is not None and result["action_key"] == lower


def test_census_cap_is_unknown_not_an_empty_election(fleet, monkeypatch):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    candidate = publish("capped")
    monkeypatch.setattr(reservation, "MAX_RECORDS", 0)
    assert claim() is None
    assert denial(candidate)["reason"] == "measurement_census_unavailable"
    assert not queue.ledger().held_keys()


def _legacy_claim(queue, row):
    return queue.claim(capacity={"cpu": 20, "gpu": 1, "mem_gb": 120},
                       cpu_tiers={"preferred": list(range(20)), "fallback": []},
                       adaptive_cpu=False, has_gpu=True, tags=["gb10", "sparklina"], ready=[row])


def test_mixed_capacity_claim_cannot_bypass_an_adaptive_election(fleet):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    holder, measurement, opportunity, snapshot = _bounded_measurement_wait(fleet)
    lower = publish("nonadaptive-no-refill", timeout_s=5)
    row = pool._read_json(queue.item_path(pool.READY, lower))
    assert _legacy_claim(pool.PoolQueue(queue.root), row) is None
    assert denial(lower)["reason"] == "deferred_for_measurement_reservation"
    assert queue.ledger().held_keys() == [holder]


def test_lock_only_gate_preserves_nonadaptive_decisions(fleet, monkeypatch):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    lower = publish("legacy-capacity-control")
    row = pool._read_json(queue.item_path(pool.READY, lower))
    def no_policy(*args, **kwargs):
        raise AssertionError("nonadaptive claim evaluated CPU policy")
    monkeypatch.setattr(adaptive_cpu.Controller, "decision", no_policy)
    assert _legacy_claim(queue, row)["action_key"] == lower
    assert queue.ledger().held_keys() == [lower]


def test_mixed_lock_contention_is_nonblocking_and_takes_no_tokens(fleet):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    lower = publish("legacy-contended-control")
    row = pool._read_json(queue.item_path(pool.READY, lower))
    controller = adaptive_cpu.Controller(queue.ledger(), {"preferred": list(range(20)), "fallback": []})
    with controller.locked():
        assert _legacy_claim(queue, row) is None
    assert not queue.ledger().held_keys()
    assert _legacy_claim(queue, row)["action_key"] == lower


def test_unavailable_host_identity_refuses_nonadaptive_capacity_claim(fleet, monkeypatch):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    lower = publish("legacy-unavailable-control")
    row = pool._read_json(queue.item_path(pool.READY, lower))
    def unknown(base):
        raise RuntimeError("unprovable host exclusion identity")
    monkeypatch.setattr(adaptive_cpu, "box_state", unknown)
    assert _legacy_claim(queue, row) is None
    assert not queue.ledger().held_keys()


def test_claimed_tombstone_ready_defer_gap_never_loses_election(fleet, monkeypatch):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    holder, measurement, opportunity, snapshot = _bounded_measurement_wait(fleet)
    queue.finish(holder, status="executed")
    tick(11)
    assert claim() == measurement
    chosen = _chosen(queue, measurement)
    row = pool._read_json(queue.item_path(pool.CLAIMED, measurement))
    assert isinstance(row, dict)
    entomb = queue._entomb_claim
    gap, proceed = threading.Event(), threading.Event()
    def paused(*args, **kwargs):
        result = entomb(*args, **kwargs)
        gap.set()
        assert proceed.wait(5)
        return result
    monkeypatch.setattr(queue, "_entomb_claim", paused)
    lower = publish("inside-real-defer-gap")
    candidate = pool._read_json(queue.item_path(pool.READY, lower))
    with ThreadPoolExecutor(max_workers=1) as executor:
        deferred = executor.submit(queue._defer_unstarted_claim, row)
        try:
            assert gap.wait(5)
            assert not queue.item_path(pool.READY, measurement).exists()
            assert not queue.item_path(pool.CLAIMED, measurement).exists()
            assert _legacy_claim(pool.PoolQueue(queue.root), candidate) is None
            assert _chosen(queue, measurement) == chosen
        finally:
            proceed.set()
        deferred.result(timeout=5)
    assert queue.item_path(pool.READY, measurement).exists()
    assert _legacy_claim(queue, candidate) is None
    assert _chosen(queue, measurement) == chosen


def test_persisted_live_reader_blocks_restart_before_another_fork(fleet, monkeypatch):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    observer = reservation.CensusReader(queue, queue.ledger())
    identity = reader._capture_ownership(os.getpid(), pool_identity=observer.pool_identity,
                                         section=reservation.READ_SECTION)
    pool._write_json_atomic(observer.directory / observer.name, identity.to_record())
    def no_fork():
        raise AssertionError("live retained identity spawned a second reader")
    monkeypatch.setattr(reader.os, "fork", no_fork)
    with pytest.raises(reservation.CensusUnavailable, match="unresolved"):
        reservation.CensusReader(pool.PoolQueue(queue.root), queue.ledger()).capture()


@pytest.mark.parametrize("field", ["generation", "host", "opportunity_unix"])
def test_corrupt_selector_never_disappears_into_legacy_attention(fleet, field):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    holder, measurement, opportunity, snapshot = _bounded_measurement_wait(fleet)
    sidecar = pool._read_json(queue.passes_path(measurement))
    assert isinstance(sidecar, dict)
    sidecar[reservation.FIELD][field] = None
    pool._write_json_atomic(queue.passes_path(measurement), sidecar)
    lower = publish("corrupt-selector-lower")
    candidate = pool._read_json(queue.item_path(pool.READY, lower))
    assert _legacy_claim(queue, candidate) is None
    assert denial(lower)["reason"] == "measurement_census_unavailable"
    assert queue.ledger().held_keys() == [holder]


def test_unmanaged_claim_is_refused_without_changing_elected_ownership(fleet):
    # Owner decision C: even stale candidate discovery cannot make a missing
    # admission grant executable. The measurement/holder remain untouched.
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    holder, measurement, opportunity, snapshot = _bounded_measurement_wait(fleet)
    lower = publish("unmanaged-bypass-witness")
    row = pool._read_json(queue.item_path(pool.READY, lower))
    result = queue.claim(has_gpu=True, tags=["gb10", "sparklina"], ready=[row])
    assert result is None
    assert denial(lower)["reason"] == "admission_capacity_required"
    assert queue.item_path(pool.READY, lower).exists()
    assert queue.ledger().held_keys() == [holder]
    assert _chosen(queue, measurement)["host"] == "sparklina"
