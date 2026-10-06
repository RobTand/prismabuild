"""The demand-based gang fence on real claims (#1579).

The two-host fixture of ``test_gang_reservation_1517``: real queue, ledgers,
census and controllers; only the clock and the sampler are controlled.  A gang
member is a whole-box row (cpu 2 or 20, gpu 1, mem 100 on a 20/1/120 host).
The roles ``returns_capacity`` and ``serves_residency`` are assigned by
``publish`` from the sealed definition of a movement node and refused in any
submitted action, so these tests publish real sealed movement nodes.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest
from test_gang_reservation_1517 import HOSTS, _busy_both, gang_fleet  # noqa: F401
from test_measurement_drains_gpu_backfill import fleet  # noqa: F401

from prismabuild import _gang, _measurement_reservation as reservation, core as pb, pool

REASONS = ("deferred_for_gang_reservation",)
from test_gang_residency_members import GIB, MANIFEST, STAGE_KIND, TIER  # noqa: E402

RANGE = {"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER, "manifest_sha256": MANIFEST,
         "manifest_bytes": 6 * GIB, "range_start_bytes": 0, "range_end_bytes": 2 * GIB}
TOOLS = "/mnt/shared/prismabuild-fleet/repo/tools"


def _seal(queue, tmp_path, name, *, script, extra_params=None, extra_command=()):
    """Seal one action whose command runs ``script`` (``None``: an ordinary argv); its key."""
    checkout = tmp_path / "checkout"
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    command = ([sys.executable, f"{TOOLS}/{script}", "--pool-root", str(queue.root), *extra_command]
               if script else [sys.executable, "task.py"])
    params = {"gpu_exclusive": False, "execution_timeout_s": 600, "command": command,
              **(extra_params or {})}
    action = pb.seal_action({
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {"definition_id": "tests/demand-fence", "definition_version": "v1",
                 "task_class": "generation", "determinism": "stochastic",
                 "artifact_family": "generic", "artifact_kind": "generic",
                 "argv": [sys.executable, "task.py"], "working_directory": ".",
                 "result_path": name},
        "inputs": [], "code_closure": pb.build_code_closure(checkout, ["task.py"]),
        "params": params, "environment": {"variables": {}, "toolchain": {}},
        "execution_scope": {"portability": "portable", "platform_key": None, "host_class": None}})
    cas.publish_action_request(action)
    return action["action_key"], cas, checkout


def _enqueue(queue, clock, key, cas, checkout, *, resources, recompute=True, residency=None,
             priority=-10, tags=("sparky",)):
    clock[0] += 0.001
    queue.publish(action_key=key, cas_root=str(cas.root), checkout_root=str(checkout),
                  worker_script="worker.py", resources=dict(resources),
                  needs_gpu=bool(resources.get("gpu")), tags=list(tags), priority=priority,
                  max_attempts=1, retry_safe=True, recompute=recompute,
                  **({} if residency is None else {"residency": residency}))


def _publish_sealed(queue, tmp_path, clock, name, *, script, resources, recompute=True,
                    residency=None, priority=-10, tags=("sparky",), extra_params=None,
                    extra_command=()):
    key, cas, checkout = _seal(queue, tmp_path, name, script=script, extra_params=extra_params,
                               extra_command=extra_command)
    _enqueue(queue, clock, key, cas, checkout, resources=resources, recompute=recompute,
             residency=residency, priority=priority, tags=tags)
    return key


def _row(queue, key):
    return pool._read_json(queue.item_path(pool.READY, key))


@pytest.mark.parametrize("field", ["returns_capacity", "serves_residency"])
@pytest.mark.parametrize("value", [True, False, "yes", 1])
def test_publish_refuses_a_role_declared_by_a_submitted_action(gang_fleet, tmp_path, field, value):
    """A forged exemption: the roles are PrismaBuild's to assign, never an action's to claim."""
    queue, clock, *_ = gang_fleet
    with pytest.raises(pool.PoolContractError, match="assigned by PrismaBuild"):
        _publish_sealed(queue, tmp_path, clock, f"forged-{field}-{value!r}", script="stage_release.py",
                        resources={"cpu": 1, "mem_gb": 1}, extra_params={field: value})


def test_publish_assigns_the_roles_to_prismabuilds_own_movement_nodes_only(gang_fleet, tmp_path):
    queue, clock, *_ = gang_fleet
    small = {"cpu": 1, "mem_gb": 1}
    cases = [
        # (name, script, resources, recompute, residency, extra_command, expected role)
        ("release", "stage_release.py", small, True, None, (), "returns_capacity"),
        ("export", "produced_export.py", {**small, f"fill_mb_s@{TIER}": 50}, True, None, (),
         "returns_capacity"),
        ("evict", "local_resident.py", small, True, None, ("--operation", "evict"), "returns_capacity"),
        ("mover", "stage_move.py", {"cpu": 4, "mem_gb": 8, STAGE_KIND: 2}, True, RANGE, (),
         "serves_residency"),
        ("promotion", "ram_promote.py", {"cpu": 4, "mem_gb": 8, STAGE_KIND: 2}, True, RANGE, (),
         "serves_residency"),
        # looks like one and is not: each missing condition leaves the row an ordinary consumer
        ("no-recompute", "stage_release.py", small, False, None, (), None),
        ("big-release", "stage_release.py", {"cpu": 8, "mem_gb": 1}, True, None, (), None),
        ("big-memory", "stage_release.py", {"cpu": 1, "mem_gb": 64}, True, None, (), None),
        ("gpu-release", "stage_release.py", {**small, "gpu": 1}, True, None, (), None),
        ("foreign-kind", "stage_release.py", {**small, "scratch_gib": 1}, True, None, (), None),
        ("other-script", "evil.py", small, True, None, (), None),
        ("ordinary", None, small, True, None, (), None),
        ("copy", "local_resident.py", small, True, None, ("--operation", "copy"), None),
        ("mover-without-range", "stage_move.py", {"cpu": 4, "mem_gb": 8}, True, None, (), None),
        ("gpu-mover", "stage_move.py", {"cpu": 4, "mem_gb": 8, "gpu": 1}, True, RANGE, (), None),
    ]
    for name, script, resources, recompute, residency, extra, role in cases:
        key = _publish_sealed(queue, tmp_path, clock, name, script=script, resources=resources,
                              recompute=recompute, residency=residency, extra_command=extra)
        row = _row(queue, key)
        assert [field for field in ("returns_capacity", "serves_residency") if row.get(field) is True] == (
            [role] if role else []), (name, row)


def _wait_gang(publish, gclaim, members, clock, name, **kw):
    incumbents = _busy_both(publish, gclaim)
    group, keys = members(name, priority=-10, **kw)
    for host in HOSTS:
        assert gclaim(host) is None
    return incumbents, group, keys


def test_a_waiting_gang_reserves_its_member_demand_and_admits_what_returns_capacity(gang_fleet, tmp_path):
    """Real claims: held by demand, never by type.

    Past the bound sparky admits PrismaBuild's own release node while the GPU single and a small
    ordinary row are held, because the incumbent still holds what the member needs.  When the
    incumbent ends, the small row fits beside the member and is admitted; the GPU single never does.
    """
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    incumbents, group, (first, second) = _wait_gang(publish, gclaim, members, clock, "res")
    clock[0] += reservation.GANG_RESERVE_AFTER_S + 1
    gpu = publish("late-gpu", priority=-10, timeout_s=None, cpu=1, gpu=1, mem_gb=8, tags=["sparky"])
    small = _publish_sealed(queue, tmp_path, clock, "small", script=None, resources={"cpu": 1, "mem_gb": 1})
    release = _publish_sealed(queue, tmp_path, clock, "release", script="stage_release.py",
                              resources={"cpu": 1, "mem_gb": 1})
    assert gclaim("sparky") == release, (denial(release, "sparky"), denial(small, "sparky"))
    for key in (gpu, small):
        assert denial(key, "sparky")["reason"] in REASONS, denial(key, "sparky")
    finish(incumbents["sparky"], "sparky")
    assert gclaim("sparky") == small, denial(small, "sparky")
    assert denial(gpu, "sparky")["reason"] in REASONS


def test_a_young_gang_does_not_hold_equal_priority_work(gang_fleet, tmp_path):
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    _wait_gang(publish, gclaim, members, clock, "young")
    clock[0] += reservation.GANG_RESERVE_AFTER_S - 60
    small = _publish_sealed(queue, tmp_path, clock, "young-small", script=None, resources={"cpu": 1, "mem_gb": 1})
    assert gclaim("sparky") == small


def test_a_gang_member_that_takes_every_cpu_still_progresses_through_its_own_movers(
        gang_fleet, monkeypatch, tmp_path):
    """The review's hole, end to end.

    Member 1 takes all 20 CPUs on sparky and waits there, its gang past the bound.  Member 0's
    residency lead is a stage mover (4 CPUs, 8 GiB, tier tokens) that runs on sparky.  The
    reservation leaves no CPU slack, so an ordinary row of that demand is held; the sealed mover
    PrismaBuild publishes (recompute, its own script, its range) is admitted anyway, runs, and
    the gang starts whole.
    """
    from test_gang_residency_members import _compose_map, _consumer_block
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    stage = tmp_path / "stage"
    stage.mkdir()
    need = {"cpu": 4, "mem_gb": 8, STAGE_KIND: 2}
    mover, cas, checkout = _seal(queue, tmp_path, "mover", script="stage_move.py")
    incumbents = _busy_both(publish, gclaim)
    group, (first, second) = members("whole-cpu", priority=-10, member_cpu=20,
                                     residency=_consumer_block([mover]))
    queue.mint_tier_capacity(TIER, {"stage_gib": 8})
    for host in HOSTS:
        assert gclaim(host) is None
    assert _gang.elections(queue, group, 2)[1]["host"] == "sparky"
    clock[0] += reservation.GANG_RESERVE_AFTER_S + 1
    for host in HOSTS:
        finish(incumbents[host], host)

    # An ordinary row with the mover's demand is held: 4 CPUs beside the member's 20 do not fit.
    control = _publish_sealed(queue, tmp_path, clock, "ordinary-same-demand", script=None,
                              resources=need, residency=RANGE)
    assert gclaim("sparky") is None
    held = denial(control, "sparky")
    assert held["reason"] in REASONS, held
    assert held["evidence"]["gang_election"]["reservation"]["cpu"] == 4, held
    queue.withdraw(control, reason="control done", by="test")

    # The mover PrismaBuild publishes is admitted, runs, and releases the gang.
    _enqueue(queue, clock, mover, cas, checkout, resources=need, residency=RANGE)
    assert _row(queue, mover)["serves_residency"] is True
    assert gclaim("sparky") == mover, denial(mover, "sparky")
    queue.record_move(mover, {
        "consumer_action_key": first, "tier_id": TIER, "stage_root": str(stage),
        "manifest_sha256": MANIFEST, "range_start_bytes": 0, "range_end_bytes": 2 * GIB,
        "bytes_staged": 2 * GIB, "complete": True})
    queue.finish(mover, status="executed")
    _compose_map(monkeypatch, queue, first, [mover])
    started = set()
    for _ in range(3):
        for host in ("sparklina", "sparky"):
            claimed = gclaim(host)
            if claimed is not None:
                started.add(claimed)
    assert started == {first, second}, (started, denial(first, "sparklina"), denial(second, "sparky"))


def test_a_carried_measurement_withhold_does_not_hold_back_an_aged_gang_member(
        gang_fleet, monkeypatch, tmp_path):
    """The review's carried-withhold path: a measurement's carried episode is a measurement withhold.

    The older measurement single cannot be evaluated this pass (its residency lead record is
    unreadable) and carries its withhold.  Past the bound the gang member behind it still elects.
    """
    from test_gang_residency_members import _consumer_block, _hexkey
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    incumbents = _busy_both(publish, gclaim)
    measurement = publish("carried-measurement", measurement=True, priority=-10, timeout_s=None,
                          cpu=2, gpu=1, mem_gb=8, tags=["sparky"])
    group, (first, second) = members("behind-carried", priority=-10)
    real = queue.residency_verdict

    def verdict(item):
        if item.get("action_key") == measurement:
            raise OSError("ESTALE")
        return real(item)

    monkeypatch.setattr(queue, "residency_verdict", verdict)
    real_carry = queue._carried_withhold
    monkeypatch.setattr(pool.PoolQueue, "_carried_withhold", staticmethod(
        lambda records, item, *, host, now: {
            "reason": "adaptive_cpu_refused_withholding", "mode": "exclusive",
            "epoch_unix": now, "drain_until_unix": now + 3600.0}
        if item.get("action_key") == measurement else real_carry(records, item, host=host, now=now)))
    assert gclaim("sparklina") is None
    assert gclaim("sparky") is None
    assert denial(second, "sparky")["reason"] == "deferred_behind_withheld_row", denial(second, "sparky")
    clock[0] += reservation.GANG_RESERVE_AFTER_S + 1
    assert gclaim("sparky") is None
    assert denial(second, "sparky")["reason"] != "deferred_behind_withheld_row", denial(second, "sparky")
    assert _gang.elections(queue, group, 2).get(1) is not None
    assert queue.item_path(pool.READY, measurement).exists()


def test_the_reservation_wins_over_an_older_measurement_withhold_so_the_gang_can_elect(gang_fleet, tmp_path):
    """The morning's starvation: a waiting measurement single held the box."""
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


def test_a_strictly_higher_priority_measurement_keeps_its_place_ahead_of_an_aged_gang(gang_fleet, tmp_path):
    """Priority order, not a reservation exception: a priority-10 measurement goes first."""
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    incumbents = _busy_both(publish, gclaim)
    group, (first, second) = members("below-ship", priority=-10)
    for host in HOSTS:
        assert gclaim(host) is None
    elected = _gang.elections(queue, group, 2)
    assert {e["host"] for e in elected.values()} == set(HOSTS)
    clock[0] += reservation.GANG_RESERVE_AFTER_S + 1
    ship = publish("ship-window-measurement", measurement=True, priority=10, timeout_s=None,
                   cpu=2, gpu=1, mem_gb=8, tags=["sparky"])
    for _ in range(3):
        assert gclaim("sparky") is None
    census = reservation.CensusReader(queue, queue.ledger("sparky")).capture()
    assert ship in census["elections"], "the higher-priority measurement still elects on a reserved host"
    assert denial(second, "sparky")["reason"] == "deferred_for_measurement_reservation", denial(second, "sparky")


def test_a_measurement_single_of_the_gangs_priority_does_not_elect_on_a_reserved_host(gang_fleet, tmp_path):
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    incumbents, group, keys = _wait_gang(publish, gclaim, members, clock, "no-elect")
    clock[0] += reservation.GANG_RESERVE_AFTER_S + 1
    measurement = publish("late-measurement", measurement=True, priority=-10, timeout_s=None,
                          cpu=2, gpu=1, mem_gb=8, tags=["sparky"])
    assert gclaim("sparky") is None
    census = reservation.CensusReader(queue, queue.ledger("sparky")).capture()
    assert measurement not in census["elections"], "a host must not be reserved and withheld"
    assert queue.item_path(pool.READY, measurement).exists()


def test_a_measurement_class_gang_member_is_not_blocked_by_its_own_reservation(gang_fleet, tmp_path):
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
