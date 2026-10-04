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

import sys

from test_measurement_drains_gpu_backfill import fleet as fleet_fixture
from test_measurement_reservation_backfill_1419 import (
    _assert_holder_unchanged, _holder_snapshot, _observe_real_sharing_permission)

from prismabuild import _measurement_reservation as reservation
from prismabuild import adaptive_cpu, adaptive_gpu, core as pb, pool

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


def _export_of(tmp_path, queue, clock, producer, name, *, sealed_owner=True):
    """A producer's spool export as ``ProducedSpool._publish`` files it.

    Sealed with ``params.produced_spool.owner`` -- the link
    ``adaptive_cpu.dependent_owner`` reads -- and published with
    ``dependent_of``, the producer's priority and its host's tag. No export
    allowance: it needs free tokens, so it meets every gate a real one meets.
    """
    clock[0] += 0.001
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    checkout = tmp_path / "checkout"
    action = pb.seal_action({
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {"definition_id": "tests/produced-export", "definition_version": "v1",
                 "task_class": "generation", "determinism": "deterministic",
                 "artifact_family": "generic", "artifact_kind": "generic",
                 "argv": [sys.executable, "task.py"], "working_directory": ".",
                 "result_path": name},
        "inputs": [], "code_closure": pb.build_code_closure(checkout, ["task.py"]),
        "params": {"gpu_exclusive": False, "execution_timeout_s": None,
                   **({"produced_spool": {"manifest_sha256": "f" * 64, "owner": producer,
                                          "batch_id": name}} if sealed_owner else {})},
        "environment": {"variables": {}, "toolchain": {}},
        "execution_scope": {"portability": "portable", "platform_key": None,
                            "host_class": None},
    })
    cas.publish_action_request(action)
    key = action["action_key"]
    queue.publish(action_key=key, cas_root=str(cas.root), checkout_root=str(checkout),
                  worker_script="worker.py", resources={"cpu": 1, "mem_gb": 1},
                  needs_gpu=False, tags=["sparklina"], priority=-10,
                  max_attempts=3, retry_safe=True, dependent_of=producer)
    return key


def test_an_undeclared_producers_own_export_is_not_held_behind_the_election(fleet, tmp_path):
    """Review of #1504: a -10 producer with no declared deadline holds the
    host; its spool exports are pinned there at its priority. Holding them
    back behind the measurement's election would let the producer wait on its
    exports forever and the measurement on the producer. The export runs; an
    unrelated -10 refill still does not; the measurement claims after."""
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    producer = publish("undeclared-producer", priority=-10, timeout_s=None,
                       cpu=2, gpu=1, mem_gb=8)
    assert claim() == producer
    measurement = publish("priority-zero-measurement", measurement=True,
                          priority=0, cpu=8, gpu=1, mem_gb=48)
    assert claim() is None
    assert denial(measurement)["evidence"]["withhold"]["why"] == "draining_for_measurement"
    tick(pool.WITHHOLD_CEILING_S + 60)

    refill = publish("unrelated-refill", priority=-10, timeout_s=None, cpu=1, gpu=0, mem_gb=1)
    export = _export_of(tmp_path, queue, clock, producer, "export-0")
    assert adaptive_cpu.dependent_owner(pool._read_json(queue.item_path(pool.READY, export))) == producer
    assert claim() == export, denial(export)
    assert set(queue.ledger().held_keys()) == {producer, export}
    chosen = reservation.selection(pool._read_json(queue.passes_path(measurement)) or {})
    assert chosen is not None and chosen["host"] == "sparklina"

    assert claim() is None, "an unrelated refill took the elected host"
    assert denial(refill)["reason"] in (
        "deferred_for_measurement_reservation", "deferred_behind_withheld_row")
    assert denial(refill)["evidence"]["withheld_for"] == measurement
    assert queue.item_path(pool.READY, refill).exists()

    queue.finish(export, status="executed")
    queue.finish(producer, status="executed")
    tick(adaptive_gpu.PSI_AVG10_S + 1)
    assert claim() == measurement, denial(measurement)
    assert queue.ledger().held_keys() == [measurement]
    assert queue.item_path(pool.READY, refill).exists()


def test_a_dependent_of_hint_without_a_sealed_owner_stays_behind_the_election(fleet, tmp_path):
    """Only the sealed ``produced_spool.owner`` exempts a row; the row's own
    ``dependent_of`` hint is not authority (#1504 review)."""
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    producer = publish("undeclared-producer", priority=-10, timeout_s=None,
                       cpu=2, gpu=1, mem_gb=8)
    assert claim() == producer
    measurement = publish("priority-zero-measurement", measurement=True,
                          priority=0, cpu=8, gpu=1, mem_gb=48)
    assert claim() is None
    tick(pool.WITHHOLD_CEILING_S + 60)
    hinted = _export_of(tmp_path, queue, clock, producer, "hint-only", sealed_owner=False)
    item = pool._read_json(queue.item_path(pool.READY, hinted))
    assert item["dependent_of"] == producer
    assert adaptive_cpu.dependent_owner(item) is None
    assert claim() is None, "a dependent_of hint alone bypassed the election"
    assert denial(hinted)["evidence"]["withheld_for"] == measurement
    assert queue.ledger().held_keys() == [producer]
    assert reservation.selection(pool._read_json(queue.passes_path(measurement)) or {}) is not None


def _non_action_holder_keeps_the_bounded_episode(fleet, hold):
    """A mem_gb shortage whose only holder is not a claimed action elects
    nothing: no action lifetime bounds that wait, and fencing the host could
    deadlock against the holder's own lower-priority consumers. The bounded
    episode lapses as on main and lower-priority work runs."""
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    assert claim() is None  # an empty pass mints the host's capacity tokens
    holder = hold(queue)
    assert queue.ledger("sparklina").held_keys() == [holder]
    assert not queue.item_path(pool.CLAIMED, holder).exists()
    measurement = publish("priority-zero-measurement", measurement=True,
                          priority=0, cpu=8, gpu=1, mem_gb=48)
    assert claim() is None
    first = denial(measurement)["evidence"]["withhold"]
    assert first["why"] == "draining_for_measurement", first
    assert [entry["action_key"] for entry in first["holders"]] == [holder]
    assert reservation.selection(pool._read_json(queue.passes_path(measurement)) or {}) is None
    tick(pool.WITHHOLD_CEILING_S + 60)
    refill = publish("lower-priority-consumer", priority=-10, timeout_s=None,
                     cpu=1, gpu=0, mem_gb=1)
    assert claim() == refill, denial(refill)
    assert reservation.selection(pool._read_json(queue.passes_path(measurement)) or {}) is None
    lapsed = denial(measurement)["evidence"]["withhold"]
    assert lapsed["why"] == "measurement_drain_expired" and lapsed["withhold"] is False
    assert queue.item_path(pool.READY, measurement).exists()


def test_a_ram_fill_hold_alone_does_not_elect_the_host(fleet):
    def hold(queue):
        grant = "a" * 64
        assert queue.hold_tier_host_memory("sparklina", grant, 100) == ("taken", "")
        return pool.PoolQueue.RAM_HOST_MEMORY_PREFIX + grant
    _non_action_holder_keeps_the_bounded_episode(fleet, hold)


def test_an_unclaimed_raw_holder_does_not_elect_the_host(fleet):
    def hold(queue):
        raw = "e" * 64
        assert queue.ledger("sparklina").acquire(raw, {"mem_gb": 100})
        return raw
    _non_action_holder_keeps_the_bounded_episode(fleet, hold)
