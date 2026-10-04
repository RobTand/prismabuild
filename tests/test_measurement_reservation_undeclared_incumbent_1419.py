"""PB #1419: an incumbent with no declared timeout must not void the election.

``pbrun --timeout-s`` defaults to None, so most live holders declare no finite
payload opportunity. Before this fix ``elect`` refused to choose a host unless
every incumbent declared one, no reservation was written, and once the bounded
900 s attention window lapsed a continuous priority -10 stream refilled the
drained host for as long as it kept arriving (#1502 found zero elections
fleet-wide). Same real queue, ledgers and controllers as the #1419 suite; only
the clock and sampler observations are controlled.
"""
from __future__ import annotations

from test_measurement_drains_gpu_backfill import fleet as fleet_fixture
from test_measurement_reservation_backfill_1419 import (
    _assert_holder_unchanged, _holder_snapshot, _observe_real_sharing_permission)

from prismabuild import _measurement_reservation as reservation
from prismabuild import adaptive_gpu, pool

fleet = fleet_fixture


def test_undeclared_incumbent_still_elects_and_bounds_the_wait_by_its_lifetime(fleet):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    incumbent = publish("undeclared-incumbent", priority=-10, timeout_s=None,
                        cpu=2, gpu=1, mem_gb=8)
    assert claim() == incumbent
    assert queue.holder_bound(incumbent).get("requested_timeout_s") is None
    snapshot = _holder_snapshot(queue, incumbent)

    measurement = publish("priority-zero-measurement", measurement=True,
                          priority=0, cpu=8, gpu=1, mem_gb=48)
    assert claim() is None
    evidence = denial(measurement)["evidence"]
    assert evidence["decision"]["reason"] == "measurement_holder", evidence
    assert evidence["decision"]["measurement_pool_drain"]["foreign_clear"] is True
    assert evidence["withhold"]["why"] == "draining_for_measurement"

    # Outlive the bounded attention window, then let a continuous -10 stream
    # arrive with genuine GPU sharing permission while the incumbent runs.
    tick(pool.WITHHOLD_CEILING_S + 60)
    lower = [publish(f"refill-{index}", priority=-10, timeout_s=None) for index in range(3)]
    for index, candidate in enumerate(lower):
        if index:
            tick(adaptive_gpu.PSI_AVG10_S + 1)
        _observe_real_sharing_permission(fleet, candidate)
        before = _holder_snapshot(queue, incumbent)
        assert claim() is None, f"refill {index} replaced the drained host's holder"
        assert denial(candidate)["reason"] in (
            "deferred_for_measurement_reservation", "deferred_behind_withheld_row")
        assert denial(candidate)["evidence"]["withheld_for"] == measurement
        _assert_holder_unchanged(queue, incumbent, before)
    _assert_holder_unchanged(queue, incumbent, snapshot)
    assert queue.ledger().held_keys() == [incumbent]
    chosen = reservation.selection(pool._read_json(queue.passes_path(measurement)) or {})
    assert chosen is not None and chosen["host"] == "sparklina" and chosen["priority"] == 0
    assert chosen["opportunity_unix"] >= chosen["epoch_unix"]

    # The wait ends with the incumbent's own lifetime, not a later refill's.
    queue.finish(incumbent, status="executed")
    tick(adaptive_gpu.PSI_AVG10_S + 1)
    assert claim() == measurement, denial(measurement)
    assert queue.ledger().held_keys() == [measurement]
    for candidate in lower:
        assert queue.item_path(pool.READY, candidate).exists()


def test_undeclared_incumbent_election_leaves_the_other_matching_host_open(fleet):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    incumbent = publish("undeclared-incumbent", priority=-10, timeout_s=None)
    assert claim() == incumbent
    other = publish("other-host-incumbent", priority=-10, timeout_s=None)
    assert claim("sparky") == other
    measurement = publish("priority-zero-measurement", measurement=True,
                          priority=0, cpu=8, gpu=1, mem_gb=48)
    assert claim("sparklina") is None
    chosen = reservation.selection(pool._read_json(queue.passes_path(measurement)) or {})
    assert chosen is not None and chosen["host"] == "sparklina"
    candidate = publish("other-matching-host-backfill", priority=-10, timeout_s=None)
    assert claim("sparky") == candidate, "one election shut both matching GPUs"
    assert set(queue.ledger("sparky").held_keys()) == {other, candidate}
    assert queue.ledger("sparklina").held_keys() == [incumbent]
