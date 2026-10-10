"""Gang teardown proof, demand-fence authority, and class-scoped CPU placement (#1721).

Pure-unit cover for the three rules:

* Teardown needs a synchronized fresh proof under the leads' locks, with a
  reset on any failure (proved by ``test_gang_residency_members``; this file
  pins the fence-release half: a torn-down gang fences nothing).
* Equal-priority rows wait on a waiting gang's declared demand only past the
  bound and only on a host with live authority. Absent or stale authority
  keeps the old fence against strictly lower priority and nothing more.
* Class-scoped CPU rows (tags no host without a GPU offers) pass the
  READY-GPU guard only beside the eligible GPU row's own room, with adaptive
  headroom through the one shared projected-cost calculation at declared
  demand. Portable rows and hostname-pinned rows never qualify.
"""
from __future__ import annotations

import math
import types

import pytest

from prismabuild import _measurement_reservation as reservation
from prismabuild import adaptive_cpu, pool

FIRST = 1000.0
BOUND = reservation.GANG_RESERVE_AFTER_S
YOUNG = FIRST + BOUND - 1
OLD = FIRST + BOUND + 1
CAPACITY = {"cpu": 20, "gpu": 1, "mem_gb": 120}
MEMBER = {"cpu": 2, "gpu": 1, "mem_gb": 100}
GROUP = "g" * 32


def _census(priority=-10, demand=MEMBER, host="sparky", other=None, waiting=True):
    elections = {"g" * 63 + "1": {
        "group": GROUP, "index": 0, "action_key": "g" * 63 + "1", "host": host,
        "priority": priority, "rank": [-priority, FIRST, GROUP], "demand": demand,
        "member_state": "ready" if waiting else "claimed"}}
    if other is not None:
        elections.update(other)
    return {"gang_elections": elections}


def _row(**over):
    row = {"action_key": "a" * 64, "priority": -10, "published_unix": FIRST + 5,
           "needs_gpu": False, "resources": {"cpu": 1, "mem_gb": 1}}
    row.update(over)
    return row


def _blocked(row, *, now=OLD, held=None, capacity=CAPACITY, census=None, authority=True,
             **kw):
    census = _census(**kw) if census is None else census
    return reservation.gang_blocking(census, row, host="sparky", group=None, now=now,
                                     held={} if held is None else held, capacity=capacity,
                                     authority=authority)


# --- one projected-cost calculation -------------------------------------------------

def test_projected_cost_fits_beside_holders():
    assert adaptive_cpu.projected_cpu_fits(
        busy_cpus=2.0, pending_cpu_cost=1.0, active_cpu_cost=3.0,
        requested_cpu_cost=2.0, cpu_count=8)


def test_projected_cost_refuses_past_the_host():
    assert not adaptive_cpu.projected_cpu_fits(
        busy_cpus=6.0, pending_cpu_cost=0.0, active_cpu_cost=6.0,
        requested_cpu_cost=4.0, cpu_count=8)


def test_projected_cost_takes_the_busier_of_the_two_baselines():
    # Busy baseline above active: busy + pending decides.
    assert not adaptive_cpu.projected_cpu_fits(
        busy_cpus=7.0, pending_cpu_cost=1.0, active_cpu_cost=2.0,
        requested_cpu_cost=1.0, cpu_count=8)
    # Active baseline above busy: active decides.
    assert adaptive_cpu.projected_cpu_fits(
        busy_cpus=1.0, pending_cpu_cost=0.0, active_cpu_cost=5.0,
        requested_cpu_cost=2.0, cpu_count=8)


@pytest.mark.parametrize("kw", [
    {"busy_cpus": None, "pending_cpu_cost": 0.0, "active_cpu_cost": 0.0,
     "requested_cpu_cost": 1.0, "cpu_count": 8},
    {"busy_cpus": 0.0, "pending_cpu_cost": math.nan, "active_cpu_cost": 0.0,
     "requested_cpu_cost": 1.0, "cpu_count": 8},
    {"busy_cpus": 0.0, "pending_cpu_cost": 0.0, "active_cpu_cost": -1.0,
     "requested_cpu_cost": 1.0, "cpu_count": 8},
    {"busy_cpus": 0.0, "pending_cpu_cost": 0.0, "active_cpu_cost": 0.0,
     "requested_cpu_cost": 1.0, "cpu_count": 0},
    {"busy_cpus": 0.0, "pending_cpu_cost": 0.0, "active_cpu_cost": 0.0,
     "requested_cpu_cost": 1.0, "cpu_count": "8"},
])
def test_projected_cost_unknown_is_unknown_never_zero(kw):
    assert adaptive_cpu.projected_cpu_fits(**kw) is False


def test_class_headroom_shares_the_one_calculation_at_declared_demand():
    room = {"action_key": "b" * 64, "room": {"cpu": 4, "mem_gb": 48}}
    costs = {"active": 1.0, "pending": 0.0, "busy_cpus": 1.0}
    # Declared demand decides: a 1-CPU candidate beside a 4-CPU GPU row fits 8.
    assert pool.PoolQueue._class_scoped_projected_headroom(
        room, {"cpu": 1, "mem_gb": 2}, cpu_count=8, holder_costs=costs,
        candidate_demand={"cpu": 1, "mem_gb": 2}) is None
    # The same terms refuse a 6-CPU candidate: the shared rule says no.
    held = pool.PoolQueue._class_scoped_projected_headroom(
        room, {"cpu": 6, "mem_gb": 2}, cpu_count=8, holder_costs=costs,
        candidate_demand={"cpu": 6, "mem_gb": 2})
    assert held is not None and held["kept_for"] == "class_scoped_cpu_projected_cpu_cost"
    assert held["holder_cost"] == 6.0 and held["gpu_cost"] == 4.0
    # The token remainder never stands in for the declared cost.
    assert adaptive_cpu.projected_cpu_fits(
        busy_cpus=1.0, pending_cpu_cost=6.0, active_cpu_cost=7.0,
        requested_cpu_cost=4.0, cpu_count=8) is False


# --- demand-fence authority ---------------------------------------------------------

def test_a_young_gang_reserves_nothing():
    assert _blocked(_row(needs_gpu=True, resources={"cpu": 1, "gpu": 1, "mem_gb": 1}),
                    now=YOUNG) is None


def test_strictly_lower_priority_is_fenced_without_authority_or_wait():
    blocked = _blocked(_row(priority=-20), now=FIRST + 1, authority=False)
    assert blocked is not None and "reservation" not in blocked


def test_equal_priority_needs_authority_and_age():
    big = {"cpu": 8, "gpu": 1, "mem_gb": 64}
    assert _blocked(_row(resources=big), now=YOUNG, authority=True) is None
    assert _blocked(_row(resources=big), now=OLD, authority=False) is None
    assert _blocked(_row(resources=big), now=OLD, authority=None) is None
    assert _blocked(_row(resources=big), now=OLD, authority="yes") is None


def test_a_gpu_single_is_held_by_the_gpu_the_member_needs():
    held = _blocked(_row(needs_gpu=True, resources={"cpu": 1, "gpu": 1, "mem_gb": 8}))
    assert held is not None and held["reservation"] == {"gpu": 1}


def test_a_small_row_that_fits_beside_the_member_is_admitted():
    assert _blocked(_row(), held={"cpu": 2, "mem_gb": 10}) is None


def test_a_small_row_is_held_once_running_work_has_used_the_slack():
    held = _blocked(_row(), held={"cpu": 2, "mem_gb": 20})
    assert held is not None and held["reservation"] == {"mem_gb": 1}


def test_a_running_member_reserves_nothing():
    census = _census()
    for election in census["gang_elections"].values():
        election["member_state"] = "claimed"
    assert _blocked(_row(resources={"cpu": 1, "mem_gb": 1}), census=census) is None
    assert reservation.reservation_priority_on(
        census, host="sparky", now=OLD, authority=True) is None


def test_a_terminal_member_reserves_nothing_not_the_whole_host():
    census = _census(demand=None)
    for election in census["gang_elections"].values():
        election["member_state"] = "terminal"
    assert _blocked(_row(resources={"cpu": 1, "mem_gb": 1}), census=census) is None


def test_an_election_with_no_member_state_reserves_nothing():
    census = _census()
    del next(iter(census["gang_elections"].values()))["member_state"]
    assert _blocked(_row(), census=census) is None


def test_a_member_that_has_ended_still_fences_strictly_lower_priority():
    blocked = _blocked(_row(priority=-20), now=FIRST + 1, waiting=False)
    assert blocked is not None and "reservation" not in blocked


def test_higher_priority_and_own_members_are_never_held():
    big = {"cpu": 8, "gpu": 1, "mem_gb": 64}
    assert _blocked(_row(priority=0, resources=big)) is None
    assert reservation.gang_blocking(_census(), _row(resources=big), host="sparky",
                                     group=GROUP, now=OLD, held={}, capacity=CAPACITY,
                                     authority=True) is None
    assert reservation.gang_blocking(_census(), _row(resources=big), host="sparklina",
                                     group=None, now=OLD, held={}, capacity=CAPACITY,
                                     authority=True) is None


def test_two_gangs_of_one_priority_stay_ordered_by_rank():
    other = {"h" * 63 + "1": {
        "group": "h" * 32, "index": 0, "action_key": "h" * 63 + "1", "host": "sparky",
        "priority": -10, "rank": [10, FIRST - 1, "h" * 32], "demand": MEMBER,
        "member_state": "ready"}}
    census = _census(other=other)
    member = _row(gang={"group": "h" * 32, "size": 2, "index": 1})
    assert reservation.gang_blocking(census, member, host="sparky", group="h" * 32,
                                     now=OLD, held={}, capacity=CAPACITY,
                                     authority=True) is None


def test_an_unknown_member_demand_reserves_the_whole_host():
    for demand in (None, "x", {"cpu": "2"}, {"cpu": -1}):
        held = _blocked(_row(), demand=demand)
        assert held is not None and set(held["reservation"]) == {"cpu", "mem_gb"}, demand


def test_an_unreadable_host_ledger_fails_safe():
    for held, capacity in ((None, CAPACITY), ({}, None), ("x", CAPACITY)):
        blocked = reservation.gang_blocking(
            _census(), _row(), host="sparky", group=None, now=OLD, held=held,
            capacity=capacity, authority=True)
        assert blocked is not None and "unknown" in blocked["reservation"]


@pytest.mark.parametrize("first, now, authority, expected", [
    (FIRST, FIRST + BOUND + 1, True, True),
    (FIRST, FIRST + BOUND, True, False),
    (FIRST, FIRST + BOUND + 1, False, False),
    (FIRST, FIRST + BOUND + 1, None, False),
    (None, FIRST + BOUND + 1, True, False),
    (True, FIRST + BOUND + 1, True, False),
    ("1000", FIRST + BOUND + 1, True, False),
])
def test_a_gang_reserves_when_it_has_waited_and_the_host_holds_authority(
        first, now, authority, expected):
    assert reservation.reserves_after(first, now, authority=authority) is expected


def test_the_reservation_priority_is_the_gangs_and_only_past_the_bound_on_its_host():
    census = _census()
    assert reservation.reservation_priority_on(
        census, host="sparky", now=YOUNG, authority=True) is None
    assert reservation.reservation_priority_on(
        census, host="sparky", now=OLD, authority=True) == -10
    assert reservation.reservation_priority_on(
        census, host="sparklina", now=OLD, authority=True) is None
    assert reservation.reservation_priority_on(
        {}, host="sparky", now=OLD, authority=True) is None


def test_member_state_reads_the_census_generations():
    record = {"members": [{"action_key": "k" * 64, "published_unix": 42.0}]}
    assert reservation._elected_member_state(
        record, "k" * 64, ready_versions={("k" * 64, 42.0)},
        claimed_versions=set()) == "ready"
    assert reservation._elected_member_state(
        record, "k" * 64, ready_versions=set(),
        claimed_versions={("k" * 64, 42.0)}) == "claimed"
    assert reservation._elected_member_state(
        record, "k" * 64, ready_versions=set(),
        claimed_versions=set()) == "terminal"
    assert reservation._elected_member_state(
        {"members": [{"action_key": "k" * 64, "published_unix": "x"}]}, "k" * 64,
        ready_versions={("k" * 64, 42.0)}, claimed_versions=set()) == "terminal"


def test_member_demand_reads_the_matching_generation_only():
    rows = {"k" * 64: [{"published_unix": 41.0, "resources": {"cpu": 9, "mem_gb": 9}},
                       {"published_unix": 42.0, "resources": {"cpu": 2, "mem_gb": 3}}]}
    record = {"members": [{"action_key": "k" * 64, "published_unix": 42.0}]}
    assert reservation._member_demand(rows, record, "k" * 64) == {"cpu": 2, "mem_gb": 3}
    assert reservation._member_demand({}, record, "k" * 64) is None
    assert reservation._member_demand(
        {"k" * 64: [{"published_unix": 42.0, "resources": "x"}]},
        record, "k" * 64) is None


def test_reservation_needs_a_mature_copy_and_a_drained_host():
    # Authority (#1579, adopted by #1721): the host holds a mature protected
    # copy of its runtime, and no live movement row without a protected role
    # may run there. A young gang or a host without authority reserves nothing.
    old = 100.0
    now = old + reservation.GANG_RESERVE_AFTER_S + 1.0
    assert reservation.reserves_after(old, now, authority=True) is True
    assert reservation.reserves_after(old, now, authority=False) is False
    assert reservation.reserves_after(now - 1.0, now, authority=True) is False
    queue = types.SimpleNamespace(
        _placement_matches=lambda row, tags=None, has_gpu=False: True)
    assert reservation.movement_drained(
        {}, queue, tags=frozenset(), has_gpu=False, host="h") is False
    assert reservation.movement_drained(
        {"movements": {}}, queue, tags=frozenset(), has_gpu=False,
        host="h") is True
    assert reservation.movement_drained(
        {"movements": {"k" * 64: [{"claimed_host": "h"}]}}, queue,
        tags=frozenset(), has_gpu=False, host="h") is False
    assert reservation.movement_drained(
        {"movements": {"k" * 64: [{"claimed_host": "other"}]}}, queue,
        tags=frozenset(), has_gpu=False, host="h") is True


# --- class-scoped rows ---------------------------------------------------------------

def _offers():
    return [{"host": "dl380g10", "has_gpu": False, "tags": ["x86"]},
            {"host": "sparky", "has_gpu": True, "tags": ["gb10", "arm64"]},
            {"host": "sparklina", "has_gpu": True, "tags": ["gb10", "arm64"]}]


def test_portable_work_is_not_class_scoped():
    assert pool.PoolQueue._excluded_from_cpu_hosts(
        {"tags": ["x86"]}, _offers()) is False


def test_class_scoped_work_names_what_no_cpu_host_offers():
    assert pool.PoolQueue._excluded_from_cpu_hosts(
        {"tags": ["gb10"]}, _offers()) is True


def test_no_evidence_without_both_sides_on_file():
    assert pool.PoolQueue._excluded_from_cpu_hosts(
        {"tags": ["gb10"]},
        [offer for offer in _offers() if offer["has_gpu"]]) is False
    assert pool.PoolQueue._excluded_from_cpu_hosts(
        {"tags": ["gb10"]},
        [offer for offer in _offers() if not offer["has_gpu"]]) is False
    assert pool.PoolQueue._excluded_from_cpu_hosts({"tags": []}, _offers()) is False
    assert pool.PoolQueue._excluded_from_cpu_hosts({"tags": ["gb10"]}, []) is False


def test_a_stale_cpu_offer_keeps_portable_work_portable():
    stale = {"host": "dl380g10", "has_gpu": False, "tags": ["x86"],
             "announced_unix": FIRST - 10000.0}
    assert pool.PoolQueue._excluded_from_cpu_hosts(
        {"tags": ["x86"]}, [stale, * _offers()]) is False


def _candidate_self():
    return types.SimpleNamespace(
        demand_of=pool.PoolQueue.demand_of,
        _excluded_from_cpu_hosts=pool.PoolQueue._excluded_from_cpu_hosts)


def test_only_a_bounded_plain_cpu_row_is_a_candidate():
    myself = _candidate_self()
    assert pool.PoolQueue._class_scoped_candidate(
        myself, {"tags": ["gb10"], "resources": {"cpu": 2, "mem_gb": 8}},
        _offers()) is True
    assert pool.PoolQueue._class_scoped_candidate(
        myself, {"tags": ["x86"], "resources": {"cpu": 2, "mem_gb": 8}},
        _offers()) is False
    assert pool.PoolQueue._class_scoped_candidate(
        myself, {"tags": ["gb10"],
                 "gang": {"group": GROUP, "size": 2, "index": 0},
                 "resources": {"cpu": 2, "mem_gb": 8}}, _offers()) is False
    assert pool.PoolQueue._class_scoped_candidate(
        myself, {"tags": ["gb10"], "resources": {"mem_gb": 8}},
        _offers()) is False
    assert pool.PoolQueue._class_scoped_candidate(
        myself, {"tags": ["gb10"], "resources": {"cpu": 2, "mem_gb": 8, "gpu": 1}},
        _offers()) is False
    assert pool.PoolQueue._class_scoped_candidate(
        myself, {"tags": ["gb10"], "resources": {"cpu": 2, "mem_gb": 8, "stage_gib@t": 1}},
        _offers()) is False


class _Ledger:
    def __init__(self, available):
        self._available = dict(available)

    def available(self):
        return dict(self._available)


def test_beside_room_passes_when_the_gpu_room_still_fits():
    room = {"action_key": "b" * 64, "room": {"cpu": 4, "mem_gb": 48}}
    assert pool.PoolQueue._class_scoped_beside_room(
        room, ledger=_Ledger({"cpu": 8, "mem_gb": 64}),
        identity=(None, False), demand={"cpu": 2, "mem_gb": 8}) is None


def test_beside_room_holds_when_the_gpu_room_no_longer_fits():
    room = {"action_key": "b" * 64, "room": {"cpu": 4, "mem_gb": 48}}
    held = pool.PoolQueue._class_scoped_beside_room(
        room, ledger=_Ledger({"cpu": 5, "mem_gb": 64}),
        identity=(None, False), demand={"cpu": 2, "mem_gb": 8})
    assert held is not None and held["kept_for"] == "class_scoped_cpu_beside_ready_gpu"


def test_beside_room_holds_a_measurement_row_and_unknown_tokens():
    room = {"action_key": "b" * 64, "room": {"cpu": 4, "mem_gb": 48}}
    assert pool.PoolQueue._class_scoped_beside_room(
        room, ledger=_Ledger({"cpu": 8, "mem_gb": 64}),
        identity=("shape", True), demand={"cpu": 2, "mem_gb": 8}) == {
        "class_scoped": "measurement_row"}

    class _Broken:
        def available(self):
            raise OSError("torn mount")

    assert pool.PoolQueue._class_scoped_beside_room(
        room, ledger=_Broken(), identity=(None, False),
        demand={"cpu": 2, "mem_gb": 8}) == {"class_scoped": "free_tokens_unreadable"}


def test_beside_room_without_a_controller_needs_no_headroom():
    room = {"action_key": "b" * 64, "room": {"cpu": 4, "mem_gb": 48}}
    assert pool.PoolQueue._class_scoped_beside_room(
        room, ledger=_Ledger({"cpu": 8, "mem_gb": 64}),
        identity=(None, False), demand={"cpu": 2, "mem_gb": 8},
        cpu_count=None, holder_costs=None,
        candidate_demand={"cpu": 2, "mem_gb": 8}) is None


def test_beside_room_with_a_controller_keeps_projected_headroom():
    room = {"action_key": "b" * 64, "room": {"cpu": 4, "mem_gb": 48}}
    costs = {"active": 1.0, "pending": 0.0, "busy_cpus": 1.0}
    assert pool.PoolQueue._class_scoped_beside_room(
        room, ledger=_Ledger({"cpu": 8, "mem_gb": 64}),
        identity=(None, False), demand={"cpu": 1, "mem_gb": 2}, cpu_count=8,
        holder_costs=costs, candidate_demand={"cpu": 1, "mem_gb": 2}) is None
    held = pool.PoolQueue._class_scoped_beside_room(
        room, ledger=_Ledger({"cpu": 8, "mem_gb": 64}),
        identity=(None, False), demand={"cpu": 1, "mem_gb": 2}, cpu_count=8,
        holder_costs=costs, candidate_demand={"cpu": 6, "mem_gb": 2})
    assert held is not None and held["kept_for"] == "class_scoped_cpu_projected_cpu_cost"
    assert pool.PoolQueue._class_scoped_beside_room(
        room, ledger=_Ledger({"cpu": 8, "mem_gb": 64}),
        identity=(None, False), demand={"cpu": 1, "mem_gb": 2}, cpu_count=8,
        holder_costs=None, candidate_demand={"cpu": 1, "mem_gb": 2}) == {
        "class_scoped": "holder_costs_unreadable"}
