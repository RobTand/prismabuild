"""The demand-based gang fence on real claims (#1579).

The two-host fixture of ``test_gang_reservation_1517``: real queue, ledgers,
census and controllers; only the clock and the sampler are controlled.  A gang
member is a whole-box row (cpu 2, gpu 1, mem 100 on a 20/1/120 host).
"""
from __future__ import annotations

import platform
import sys
from pathlib import Path

import pytest
from test_gang_reservation_1517 import HOSTS, _busy_both, gang_fleet  # noqa: F401
from test_measurement_drains_gpu_backfill import fleet  # noqa: F401

from prismabuild import _gang, _measurement_reservation as reservation, core as pb, pool

REASONS = ("deferred_for_gang_reservation",)
SRC = Path(__file__).resolve().parents[1]


def _seal_and_publish(queue, tmp_path, clock, name, *, returns_capacity=None, priority=-10,
                      cpu=1, gpu=0, mem_gb=1, tags=("sparky",)):
    """A sealed row whose params carry ``returns_capacity`` as given (``None``: absent)."""
    clock[0] += 0.001
    checkout = tmp_path / "checkout"
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    params = {"gpu_exclusive": False, "execution_timeout_s": 600}
    if returns_capacity is not None:
        params["returns_capacity"] = returns_capacity
    action = pb.seal_action({
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {"definition_id": "tests/demand-fence", "definition_version": "v1",
                 "task_class": "generation", "determinism": "deterministic",
                 "artifact_family": "generic", "artifact_kind": "generic",
                 "argv": [sys.executable, "task.py"], "working_directory": ".",
                 "result_path": name},
        "inputs": [], "code_closure": pb.build_code_closure(checkout, ["task.py"]),
        "params": params, "environment": {"variables": {}, "toolchain": {}},
        "execution_scope": {"portability": "portable", "platform_key": None, "host_class": None}})
    cas.publish_action_request(action)
    key = action["action_key"]
    resources = {"cpu": cpu, "mem_gb": mem_gb, **({"gpu": gpu} if gpu else {})}
    queue.publish(action_key=key, cas_root=str(cas.root), checkout_root=str(checkout),
                  worker_script="worker.py", resources=resources, needs_gpu=bool(gpu),
                  tags=list(tags), priority=priority, max_attempts=1, retry_safe=True)
    return key


def test_a_sealed_returns_capacity_reaches_the_row_and_only_true_is_accepted(gang_fleet, tmp_path):
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    key = _seal_and_publish(queue, tmp_path, clock, "egress", returns_capacity=True)
    assert pool._read_json(queue.item_path(pool.READY, key))["returns_capacity"] is True
    plain = _seal_and_publish(queue, tmp_path, clock, "plain")
    assert "returns_capacity" not in pool._read_json(queue.item_path(pool.READY, plain))
    for bad in (False, "yes", 1):
        with pytest.raises(pool.PoolContractError):
            _seal_and_publish(queue, tmp_path, clock, f"bad-{bad!r}", returns_capacity=bad)


def test_the_sealers_of_capacity_returning_rows_declare_it():
    """Inventory: every node whose job is to give capacity back seals the declaration.

    A stage egress and a RAM egress (``pbrun``), a produced-output egress, a
    produced-output spool export and a resident evict.  The movers and copies
    that take capacity do not.
    """
    def count(path):
        return (SRC / path).read_text().count("RETURNS_CAPACITY_PARAMS")
    assert count("tools/fleet/pbrun.py") == 2
    assert count("src/prismabuild/produced_output.py") == 1
    assert count("src/prismabuild/produced_spool.py") == 1
    assert count("src/prismabuild/local_resident.py") == 1


def _elect_both_and_wait(publish, gclaim, members, clock, *, name="res", **kw):
    incumbents = _busy_both(publish, gclaim)
    group, keys = members(name, priority=-10, **kw)
    for host in HOSTS:
        assert gclaim(host) is None
    return incumbents, group, keys


def test_a_waiting_gang_reserves_its_member_demand_and_admits_what_returns_capacity(
        gang_fleet, tmp_path):
    """Real claims: held by demand, never by type.

    Past the bound sparky admits a sealed egress (it returns capacity) while the
    GPU single and a small undeclared row are held, because the incumbent still
    holds what the member needs.  When the incumbent ends, the small row fits
    beside the member and is admitted; the GPU single never does.
    """
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    incumbents, group, (first, second) = _elect_both_and_wait(publish, gclaim, members, clock)
    clock[0] += reservation.GANG_RESERVE_AFTER_S + 1
    gpu = publish("late-gpu", priority=-10, timeout_s=None, cpu=1, gpu=1, mem_gb=8, tags=["sparky"])
    small = _seal_and_publish(queue, tmp_path, clock, "small", cpu=1, mem_gb=1)
    egress = _seal_and_publish(queue, tmp_path, clock, "egress", returns_capacity=True)
    assert gclaim("sparky") == egress, (denial(egress, "sparky"), denial(small, "sparky"))
    for key in (gpu, small):
        record = denial(key, "sparky")
        assert record["reason"] in REASONS, record
    finish(incumbents["sparky"], "sparky")
    assert gclaim("sparky") == small, denial(small, "sparky")
    assert denial(gpu, "sparky")["reason"] in REASONS


def test_a_young_gang_does_not_hold_equal_priority_work(gang_fleet, tmp_path):
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    incumbents, group, keys = _elect_both_and_wait(publish, gclaim, members, clock, name="young")
    clock[0] += reservation.GANG_RESERVE_AFTER_S - 60
    egress_like = _seal_and_publish(queue, tmp_path, clock, "young-small", cpu=1, mem_gb=1)
    assert gclaim("sparky") == egress_like


def test_the_reservation_wins_over_an_older_measurement_withhold_so_the_gang_can_elect(
        gang_fleet, tmp_path):
    """The morning's starvation: a waiting measurement single held the box.

    The measurement single is older than the gang and pinned to sparky, so its
    withhold holds back every row behind it -- including the gang member that
    would elect the host.  Past the bound the member is no longer held behind
    it, elects, and the reservation suspends the withhold for the host.  The
    measurement row stays READY.
    """
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    incumbents = _busy_both(publish, gclaim)
    measurement = publish("old-measurement", measurement=True, priority=-10, timeout_s=None,
                          cpu=2, gpu=1, mem_gb=8, tags=["sparky"])
    group, (first, second) = members("behind-measurement", priority=-10)
    assert gclaim("sparklina") is None
    assert gclaim("sparky") is None
    assert denial(second, "sparky")["reason"] == "deferred_behind_withheld_row", denial(second, "sparky")
    assert _gang.elections(queue, group, 2).get(1) is None
    clock[0] += reservation.GANG_RESERVE_AFTER_S + 1
    assert gclaim("sparky") is None
    assert denial(second, "sparky")["reason"] != "deferred_behind_withheld_row", denial(second, "sparky")
    assert _gang.elections(queue, group, 2).get(1) is not None
    assert queue.item_path(pool.READY, measurement).exists()


def test_a_measurement_single_does_not_elect_on_a_host_a_gang_reserves(gang_fleet, tmp_path):
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    incumbents, group, keys = _elect_both_and_wait(publish, gclaim, members, clock, name="no-elect")
    clock[0] += reservation.GANG_RESERVE_AFTER_S + 1
    measurement = publish("late-measurement", measurement=True, priority=-10, timeout_s=None,
                          cpu=2, gpu=1, mem_gb=8, tags=["sparky"])
    assert gclaim("sparky") is None
    from prismabuild import _measurement_reservation as mr
    census = mr.CensusReader(queue, queue.ledger("sparky")).capture()
    assert measurement not in census["elections"], "a host must not be reserved and withheld"
    assert queue.item_path(pool.READY, measurement).exists()


def test_a_measurement_class_gang_member_is_not_blocked_by_its_own_reservation(gang_fleet, tmp_path):
    """The gang whose member is itself a measurement starts within the bound.

    Sparky's member is a measurement-class row.  Past the bound a separate
    measurement single is neither elected nor allowed to withhold against the
    gang, and the member still elects and is admitted when its host is free.
    """
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    incumbents = _busy_both(publish, gclaim)
    group, (first, second) = members("measured-gang", priority=-10, measurement_member=1)
    for host in HOSTS:
        assert gclaim(host) is None
    clock[0] += reservation.GANG_RESERVE_AFTER_S + 1
    other = publish("other-measurement", measurement=True, priority=-10, timeout_s=None,
                    cpu=2, gpu=1, mem_gb=8, tags=["sparky"])
    for host in HOSTS:
        finish(incumbents[host], host)
    started = set()
    for _ in range(3):
        for host in HOSTS:
            claimed = gclaim(host)
            if claimed is not None:
                started.add(claimed)
    assert started == {first, second}, (started, denial(second, "sparky"), denial(other, "sparky"))
    assert queue.item_path(pool.READY, other).exists()
