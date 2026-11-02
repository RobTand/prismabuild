"""#1515: an unopenable transition-lock file is a named claim denial.

The real queue, ledgers, census and denial files are used. Only the POSIX
lock open of one elected measurement's transition-lock file is made to fail.
"""
from concurrent.futures import ThreadPoolExecutor
import hashlib

import pytest

from test_measurement_drains_gpu_backfill import (
    cpu_sample as fleet_cpu_sample, gpu_sample as fleet_gpu_sample, fleet as fleet_fixture)
from test_measurement_reservation_backfill_1419 import _bounded_measurement_wait
from prismabuild import _measurement_reservation as reservation, adaptive_cpu, pool, posix_lock

fleet = fleet_fixture
TIERS = {"preferred": list(range(20)), "fallback": []}
CAPACITY = {"cpu": 20, "gpu": 1, "mem_gb": 120}


def _lock_name(key):
    return hashlib.sha256(str(key).encode()).hexdigest() + ".lock"


@pytest.fixture()
def broken(monkeypatch):
    """Make opening the named key's transition-lock file fail with OSError."""
    state = {"keys": set(), "opens": 0}
    original = posix_lock.held

    def held(path, *args, **kwargs):
        if path.parent.name == "transition-locks" and path.name in {
                _lock_name(key) for key in state["keys"]}:
            state["opens"] += 1
            raise OSError(24, "Too many open files", str(path))
        return original(path, *args, **kwargs)

    monkeypatch.setattr(posix_lock, "held", held)
    return state


def _claim_lower(fleet, row):
    queue, clock, readings, sample, *_ = fleet
    return queue.claim(capacity=CAPACITY, cpu_tiers=TIERS, adaptive_cpu=True,
                       has_gpu=True, tags=["gb10", "sparklina"], ready=[row],
                       cpu_sampler=lambda: fleet_cpu_sample(clock, readings),
                       gpu_sampler=lambda: fleet_gpu_sample(sample, readings))


def _setup(fleet, broken):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    holder, measurement, opportunity, snapshot = _bounded_measurement_wait(fleet)
    lower = publish("unopenable-lock-lower")
    row = pool.record_store.value_or_raise(queue.records.item(pool.record_store.READY, lower))
    broken["keys"].add(measurement)
    return holder, measurement, lower, row


def test_unopenable_transition_lock_is_a_named_census_denial(fleet, broken):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    holder, measurement, lower, row = _setup(fleet, broken)
    try:
        result = _claim_lower(fleet, row)
    except OSError as exc:
        pytest.fail(f"raw OSError escaped the claim path: {exc!r}")
    assert broken["opens"] >= 1
    assert result is None
    refusal = denial(lower)
    assert refusal["reason"] == "measurement_census_unavailable"
    assert "transition lock" in refusal["evidence"]["unavailable"]
    assert queue.records.item_path(pool.record_store.READY, lower).exists()


def test_serve_once_continues_after_transition_lock_denial(fleet, broken, monkeypatch):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    holder, measurement, lower, row = _setup(fleet, broken)
    original = queue.claim

    def claim_with_samplers(**kwargs):
        return original(cpu_sampler=lambda: fleet_cpu_sample(clock, readings),
                        gpu_sampler=lambda: fleet_gpu_sample(sample, readings), **kwargs)

    monkeypatch.setattr(queue, "claim", claim_with_samplers)
    monkeypatch.setattr(queue, "_sweep_due", lambda: False)

    def serve():
        return queue.serve_once(tags=["gb10", "sparklina"], has_gpu=True,
                                capacity=CAPACITY, cpu_tiers=TIERS,
                                adaptive_cpu=True, ready=[row])

    try:
        assert serve() is None
    except OSError as exc:
        pytest.fail(f"raw OSError escaped serve_once: {exc!r}")
    assert denial(lower)["reason"] == "measurement_census_unavailable"
    # The loop is not broken: with the file openable again the next pass
    # decides normally, by the measurement fence rather than the census refusal.
    broken["keys"].clear()
    assert serve() is None
    assert denial(lower)["reason"] == "deferred_for_measurement_reservation"
    assert queue.ledger().held_keys() == [holder]


def test_transition_lock_denial_takes_no_tokens_and_releases_locks(fleet, broken):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    holder, measurement, lower, row = _setup(fleet, broken)
    tokens = queue.ledger().holder_tokens(holder)
    before = pool.record_store.read_document(
        queue.records.item_path(pool.record_store.CLAIMED, holder))
    try:
        assert _claim_lower(fleet, row) is None
    except OSError as exc:
        pytest.fail(f"raw OSError escaped the claim path: {exc!r}")
    assert broken["opens"] >= 1
    assert queue.ledger().held_keys() == [holder]
    assert queue.ledger().holder_tokens(holder) == tokens
    assert pool.record_store.read_document(
        queue.records.item_path(pool.record_store.CLAIMED, holder)) == before
    assert not queue.ledger().holder_tokens(lower)

    def can_take_H():
        try:
            with adaptive_cpu.AdmissionGate(queue.ledger()).locked():
                return True
        except adaptive_cpu.AdmissionBusy:
            return False

    def can_take_lower():
        with queue.action_transition_lock(lower, blocking=False) as acquired:
            return acquired

    with ThreadPoolExecutor(max_workers=1) as executor:
        assert executor.submit(can_take_H).result(timeout=5)
        assert executor.submit(can_take_lower).result(timeout=5)


def _census_controller(fleet):
    queue, clock, readings, *_ = fleet
    return adaptive_cpu.Controller(
        queue.ledger(), TIERS, sampler=lambda: fleet_cpu_sample(clock, readings))


def test_claim_body_exception_keeps_its_own_type_and_busy_key_is_not_a_refusal(fleet, broken):
    queue = fleet[0]
    holder, measurement, lower, row = _setup(fleet, broken)
    broken["keys"].clear()
    controller = _census_controller(fleet)
    with pytest.raises(KeyError, match="claim body"):
        with reservation.admission_census(queue, queue.ledger(), controller) as census:
            assert "unavailable" not in census
            raise KeyError("claim body")
    # A key another loop holds mid-transition stays a live election.
    with queue.action_transition_lock(measurement), ThreadPoolExecutor(max_workers=1) as executor:
        def busy_census():
            with reservation.admission_census(queue, queue.ledger(), controller) as census:
                return "unavailable" in census, measurement in census["elections"]
        assert executor.submit(busy_census).result(timeout=5) == (False, True)
    # An openable lock yields the census; an unopenable one is unavailable.
    with reservation.admission_census(queue, queue.ledger(), controller) as census:
        assert "unavailable" not in census
    broken["keys"].add(measurement)
    with reservation.admission_census(queue, queue.ledger(), controller) as census:
        assert "transition lock" in census["unavailable"]
