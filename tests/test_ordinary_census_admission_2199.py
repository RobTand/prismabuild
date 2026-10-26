"""Ordinary admission must not depend on an irrelevant census fence (#2199)."""
from __future__ import annotations

from contextlib import contextmanager
import fcntl
import multiprocessing
import os
import time

import pytest

from prismabuild import _measurement_reservation as reservation, adaptive_cpu, pool
from test_gang_reservation_1517 import gang_fleet  # noqa: F401
from test_measurement_drains_gpu_backfill import fleet  # noqa: F401


@contextmanager
def _fixture_census_fence(queue):
    census = reservation.CensusReader(queue, queue.ledger())
    descriptor = os.open(census.directory / (census.name + ".guard"),
                         os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield
    finally:
        os.close(descriptor)


def test_ordinary_admission_latency_under_a_contended_census_fence(fleet):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    key = publish("ordinary-contended", gpu=0)
    with _fixture_census_fence(queue):
        started = time.perf_counter()
        admitted = claim()
        elapsed = time.perf_counter() - started
    print(f"ordinary_contended_latency_s={elapsed:.6f} admitted={admitted == key}")
    assert admitted == key


@pytest.mark.parametrize("legacy", [True, False], ids=["before", "after"])
def test_paired_ordinary_admission_latency(fleet, monkeypatch, legacy):
    """Same action and fixture, alternating the original and updated call paths."""
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    if legacy:
        monkeypatch.setattr(reservation, "candidate_admission_census",
                            lambda stored, active, ledger, controller, item, refusal:
                            reservation.pass_admission_census(stored, active, ledger, controller))
    key = publish("paired-ordinary-contended", gpu=0)
    with _fixture_census_fence(queue):
        started = time.perf_counter()
        admitted = claim()
        elapsed = time.perf_counter() - started
    print(f"paired_contended arm={'before' if legacy else 'after'} "
          f"latency_s={elapsed:.6f} admitted={admitted == key}")
    if legacy:
        assert admitted is None
        assert denial(key)["reason"] == "measurement_census_unavailable"
    else:
        assert admitted == key


def _fixture_selection(queue, key, *, host="sparklina", priority=10):
    row = pool.record_store.value_or_raise(queue.records.item(pool.record_store.READY, key))
    chosen = {
        "schema": reservation.SCHEMA, "action_key": key,
        "generation": queue.records.attempt_generation(row), "host": host,
        "published_unix": row["published_unix"], "priority": priority,
        "epoch_unix": row["published_unix"], "opportunity_unix": row["published_unix"],
    }
    queue.records.replace_record_at(queue.records.passes_path(key), {
        "action_key": key, reservation.FIELD: chosen,
    })
    return chosen


@pytest.mark.parametrize("host,priority", [
    ("sparky", 10), ("sparklina", -20), ("sparklina", -10),
])
def test_an_election_outside_the_host_or_priority_band_needs_no_reader(
        fleet, host, priority):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    selected = publish("irrelevant-election", measurement=True, gpu=0, priority=priority)
    _fixture_selection(queue, selected, host=host, priority=priority)
    candidate = publish("outside-election-band", gpu=0)
    row = pool.record_store.value_or_raise(queue.records.item(pool.record_store.READY, candidate))
    with _fixture_census_fence(queue):
        result = queue.claim(capacity={"cpu": 20, "mem_gb": 120}, ready=[row])
    assert result is not None and result["action_key"] == candidate


@pytest.mark.parametrize("garbled", [False, True])
def test_a_live_or_garbled_measurement_election_never_grants_refill(
        fleet, monkeypatch, garbled):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    selected = publish("blocking-election", measurement=True, gpu=0, priority=10)
    _fixture_selection(queue, selected)
    if garbled:
        queue.records.passes_path(selected).write_text("{broken")
    candidate = publish("behind-election", gpu=0)
    row = pool.record_store.value_or_raise(queue.records.item(pool.record_store.READY, candidate))
    monkeypatch.setattr(reservation, "FENCE_WAIT_S", 0.02)
    with _fixture_census_fence(queue):
        result = queue.claim(capacity={"cpu": 20, "mem_gb": 120}, ready=[row])
    assert result is None
    assert denial(candidate)["reason"] == "measurement_census_unavailable"
    assert queue.ledger().held_keys() == []


@pytest.mark.parametrize("election_state", ["none", "live", "retired", "garbled"])
def test_independent_loops_admit_ordinary_rows_while_a_census_child_is_slow(
        fleet, monkeypatch, election_state):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    if election_state != "none":
        selected = publish("slow-election", measurement=True, gpu=0, priority=10)
        _fixture_selection(queue, selected)
        if election_state == "retired":
            queue.withdraw(selected)
        if election_state == "garbled":
            queue.records.passes_path(selected).write_text("{broken")
    candidates = [publish(f"loop-{i}", gpu=0, cpu=1) for i in range(4)]
    rows = [pool.record_store.value_or_raise(queue.records.item(pool.record_store.READY, key))
            for key in candidates]
    monkeypatch.setattr(reservation, "SHARED_CENSUS_MAX_AGE_S", 60)
    if election_state in {"live", "retired"}:
        with reservation.locked_census(
                queue, queue.ledger(), adaptive_cpu.AdmissionGate(queue.ledger())):
            pass
    context = multiprocessing.get_context("fork")
    entered, release = context.Event(), context.Event()
    original_capture = reservation._capture

    def slow_capture(active):
        entered.set()
        assert release.wait(30), "fixture census was never released"
        return original_capture(active)

    monkeypatch.setattr(reservation, "_capture", slow_capture)
    monkeypatch.setattr(reservation, "READ_BUDGET_S", 40)
    monkeypatch.setattr(reservation, "FENCE_WAIT_S", 0.02)

    def observe():
        try:
            reservation.CensusReader(queue, queue.ledger()).capture()
        except reservation.CensusUnavailable:
            if election_state != "garbled":
                raise

    observer = context.Process(target=observe)
    observer.start()
    try:
        assert entered.wait(30), "fixture census child did not start"
        for key, row in zip(candidates, rows):
            sibling = pool.PoolQueue(queue.root)
            result = sibling.claim(capacity={"cpu": 20, "mem_gb": 120}, ready=[row])
            if election_state in {"none", "retired"}:
                assert result is not None and result["action_key"] == key
            else:
                assert result is None
                expected = ("measurement_census_unavailable" if election_state == "garbled"
                            else "deferred_for_measurement_reservation")
                assert denial(key)["reason"] == expected
    finally:
        release.set()
        observer.join(45)
        if observer.is_alive():
            observer.kill()
            observer.join()
    assert observer.exitcode == 0


def test_a_complete_retirement_census_is_shared_across_independent_loops(fleet, monkeypatch):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    selected = publish("retired-election", measurement=True, gpu=0, priority=10)
    _fixture_selection(queue, selected)
    queue.withdraw(selected)
    candidate = publish("after-retirement", gpu=0)
    row = pool.record_store.value_or_raise(queue.records.item(pool.record_store.READY, candidate))
    with reservation.locked_census(
            queue, queue.ledger(), adaptive_cpu.AdmissionGate(queue.ledger())) as census:
        assert selected not in census["elections"]
    with _fixture_census_fence(queue):
        result = pool.PoolQueue(queue.root).claim(
            capacity={"cpu": 20, "mem_gb": 120}, ready=[row])
    assert result is not None and result["action_key"] == candidate


def test_a_new_election_invalidates_a_siblings_shared_census(fleet, monkeypatch):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    selected = publish("late-election", measurement=True, gpu=0, priority=10)
    candidate = publish("late-follower", gpu=0)
    row = pool.record_store.value_or_raise(queue.records.item(pool.record_store.READY, candidate))
    with reservation.locked_census(
            queue, queue.ledger(), adaptive_cpu.AdmissionGate(queue.ledger())):
        pass
    _fixture_selection(queue, selected)
    monkeypatch.setattr(reservation, "FENCE_WAIT_S", 0.02)
    with _fixture_census_fence(queue):
        result = pool.PoolQueue(queue.root).claim(
            capacity={"cpu": 20, "mem_gb": 120}, ready=[row])
    assert result is None
    assert denial(candidate)["reason"] == "measurement_census_unavailable"


def test_a_census_denial_does_not_spread_to_an_unfenced_priority_band(fleet, monkeypatch):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    selected = publish("band-election", measurement=True, gpu=0, priority=-20)
    _fixture_selection(queue, selected, priority=-20)
    fenced = publish("band-fenced", gpu=0, priority=0, measurement=True)
    unfenced = publish("band-unfenced", gpu=0, priority=-10)
    rows = [pool.record_store.value_or_raise(queue.records.item(pool.record_store.READY, key))
            for key in (fenced, unfenced)]
    monkeypatch.setattr(reservation, "FENCE_WAIT_S", 0.02)
    with _fixture_census_fence(queue):
        result = queue.claim(capacity={"cpu": 20, "mem_gb": 120}, ready=rows)
    assert result is not None and result["action_key"] == unfenced
    assert denial(fenced)["reason"] == "measurement_census_unavailable"


@pytest.mark.parametrize("priority", [0, 10])
def test_gang_elections_keep_lower_and_equal_priority_on_the_reader_path(
        gang_fleet, monkeypatch, priority):
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    group, keys = members("reader-gang", priority=10, mem_gb=8)
    assert gclaim("sparklina") is None
    candidate = publish("gang-follower", priority=priority, gpu=0)
    row = pool.record_store.value_or_raise(queue.records.item(pool.record_store.READY, candidate))
    monkeypatch.setattr(reservation, "SHARED_CENSUS_MAX_AGE_S", -1)
    monkeypatch.setattr(reservation, "FENCE_WAIT_S", 0.02)
    with _fixture_census_fence(queue):
        result = queue.claim(capacity={"cpu": 20, "mem_gb": 120, "gpu": 1},
                             has_gpu=True, tags=["gb10", "sparklina"], ready=[row])
    assert result is None
    assert denial(candidate, "sparklina")["reason"] == "measurement_census_unavailable"


def test_a_live_census_is_shared_without_another_reader_fence(fleet, monkeypatch):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    selected = publish("shared-live-election", measurement=True, gpu=0, priority=10)
    _fixture_selection(queue, selected)
    candidate = publish("shared-live-follower", gpu=0)
    row = pool.record_store.value_or_raise(queue.records.item(pool.record_store.READY, candidate))
    monkeypatch.setattr(reservation, "SHARED_CENSUS_MAX_AGE_S", 60)
    with reservation.locked_census(
            queue, queue.ledger(), adaptive_cpu.AdmissionGate(queue.ledger())) as census:
        assert selected in census["elections"]
    with _fixture_census_fence(queue):
        for _ in range(4):
            result = pool.PoolQueue(queue.root).claim(
                capacity={"cpu": 20, "mem_gb": 120}, ready=[row])
            assert result is None
            assert denial(candidate)["reason"] == "deferred_for_measurement_reservation"


@pytest.mark.parametrize("damage", ["expired", "garbled", "other_pool", "other_boot"])
def test_a_retirement_cache_must_be_verifiable_before_ordinary_admission(
        fleet, monkeypatch, damage):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    selected = publish("cache-retired", measurement=True, gpu=0, priority=10)
    _fixture_selection(queue, selected)
    queue.withdraw(selected)
    candidate = publish("cache-retired-follower", gpu=0)
    row = pool.record_store.value_or_raise(queue.records.item(pool.record_store.READY, candidate))
    with reservation.locked_census(
            queue, queue.ledger(), adaptive_cpu.AdmissionGate(queue.ledger())):
        pass
    cache = reservation.CensusReader(queue, queue.ledger()).shared_path
    saved = reservation._read(cache)
    if damage == "expired":
        saved["created"] -= 60
    elif damage == "other_pool":
        saved["pool"] += "-other"
    elif damage == "other_boot":
        saved["boot"] = "00000000-0000-0000-0000-000000000000"
    else:
        saved["census"] = {}
    pool.immutable_files.write_json_atomic(cache, saved)
    monkeypatch.setattr(reservation, "FENCE_WAIT_S", 0.02)
    with _fixture_census_fence(queue):
        result = queue.claim(capacity={"cpu": 20, "mem_gb": 120}, ready=[row])
    assert result is None, "a retired source entry alone cannot authorize admission"
    assert denial(candidate)["reason"] == "measurement_census_unavailable"


def test_mutation_skipping_election_sources_is_caught_at_the_reservation(fleet, monkeypatch):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    selected = publish("mutation-election", measurement=True, gpu=0, priority=10)
    _fixture_selection(queue, selected)
    candidate = publish("mutation-follower", gpu=0)
    row = pool.record_store.value_or_raise(queue.records.item(pool.record_store.READY, candidate))
    monkeypatch.setattr(reservation, "FENCE_WAIT_S", 0.02)

    def must_stay_fenced():
        with _fixture_census_fence(queue):
            result = queue.claim(capacity={"cpu": 20, "mem_gb": 120}, ready=[row])
        assert result is None, "a live election was undercut"
        assert queue.ledger().held_keys() == []

    must_stay_fenced()
    empty = {"measurements": {}, "elections": {}, "selections": {}, "opportunities": {},
             "keys": [], "gang_elections": {}, "movements": {}}
    monkeypatch.setattr(reservation, "_ordinary_election_census", lambda *args: empty)
    with pytest.raises(AssertionError, match="a live election was undercut"):
        must_stay_fenced()


def test_mutation_skipping_shared_generation_verification_is_caught(fleet, monkeypatch):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    selected = publish("generation-mutation-election", measurement=True, gpu=0, priority=10)
    candidate = publish("generation-mutation-follower", gpu=0)
    row = pool.record_store.value_or_raise(queue.records.item(pool.record_store.READY, candidate))
    monkeypatch.setattr(reservation, "SHARED_CENSUS_MAX_AGE_S", 60)
    with reservation.locked_census(
            queue, queue.ledger(), adaptive_cpu.AdmissionGate(queue.ledger())):
        pass
    saved = reservation.CensusReader(queue, queue.ledger()).shared_census()
    saved_generation = reservation._admission_source_generation(queue, saved["census"])
    _fixture_selection(queue, selected)
    monkeypatch.setattr(reservation, "FENCE_WAIT_S", 0.02)

    def must_reject_stale_census():
        with _fixture_census_fence(queue):
            result = queue.claim(capacity={"cpu": 20, "mem_gb": 120}, ready=[row])
        assert result is None, "stale census skipped a new election"

    must_reject_stale_census()
    monkeypatch.setattr(reservation, "_admission_source_generation", lambda *args: saved_generation)
    with pytest.raises(AssertionError, match="stale census skipped a new election"):
        must_reject_stale_census()


def test_a_vanished_sidecar_cannot_clear_a_pass_remembered_election(fleet, monkeypatch):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    selected = publish("remembered-election", measurement=True, gpu=0, priority=10)
    _fixture_selection(queue, selected)
    candidate = publish("remembered-follower", gpu=0)
    row = pool.record_store.value_or_raise(queue.records.item(pool.record_store.READY, candidate))
    gate = adaptive_cpu.AdmissionGate(queue.ledger())
    remembered = reservation.PassCensus(queue, queue.ledger(), gate)
    with reservation.locked_census(queue, queue.ledger(), gate) as census:
        remembered.store(census)
    queue.records.passes_path(selected).unlink()
    monkeypatch.setattr(reservation, "FENCE_WAIT_S", 0.02)
    with _fixture_census_fence(queue):
        with reservation.candidate_admission_census(
                remembered, queue, queue.ledger(), gate, row) as census:
            assert "unavailable" in census
    assert queue.ledger().held_keys() == []


@pytest.mark.parametrize("state", ["retired", "garbled"])
def test_gang_retirement_is_shared_but_garbled_authority_is_not(gang_fleet, monkeypatch, state):
    from prismabuild import _gang
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    group, keys = members("shared-gang", priority=10, mem_gb=8)
    assert gclaim("sparklina") is None
    if state == "retired":
        queue.withdraw(keys[0])
    else:
        (_gang.state_dir(queue, group) / "elect-0.json").write_text("{broken")
    candidates = [publish(f"shared-gang-follower-{i}", priority=0, gpu=0) for i in range(4)]
    rows = [pool.record_store.value_or_raise(queue.records.item(pool.record_store.READY, key))
            for key in candidates]
    monkeypatch.setattr(reservation, "SHARED_CENSUS_MAX_AGE_S", 60)
    monkeypatch.setattr(reservation, "FENCE_WAIT_S", 0.02)
    if state == "retired":
        with reservation.locked_census(
                queue, queue.ledger(), adaptive_cpu.AdmissionGate(queue.ledger())) as census:
            assert census["gang_elections"] == {}
    with _fixture_census_fence(queue):
        for key, row in zip(candidates, rows):
            result = pool.PoolQueue(queue.root).claim(
                capacity={"cpu": 20, "mem_gb": 120, "gpu": 1}, has_gpu=True,
                tags=["gb10", "sparklina"], ready=[row])
            if state == "retired":
                assert result is not None and result["action_key"] == key
            else:
                assert result is None
                assert denial(key, "sparklina")["reason"] == "measurement_census_unavailable"
