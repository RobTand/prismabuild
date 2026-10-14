"""Gang teardown proof, demand-fence authority, and class-scoped CPU placement (#1721).

Unit and integrated claim coverage for the three rules:

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

from contextlib import contextmanager
import json
import math
from queue import Queue
from threading import Event, Thread
import types

import pytest

from prismabuild import _measurement_reservation as reservation
from prismabuild import adaptive_cpu, adaptive_gpu, core as pb, pool

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


@pytest.fixture
def class_claim_host(tmp_path, monkeypatch):
    host = types.SimpleNamespace(
        queue=pool.PoolQueue(tmp_path / "queue"), root=tmp_path,
        now=10_000.0, busy=0.0,
        capacity={"cpu": 8, "gpu": 1, "mem_gb": 32},
        tiers={"preferred": list(range(8)), "fallback": []})
    monkeypatch.setattr(pool.socket, "gethostname", lambda: "sparky")
    monkeypatch.setattr(pool, "_now", lambda: host.now)
    monkeypatch.setattr(adaptive_cpu.time, "time", lambda: host.now)
    monkeypatch.setattr(adaptive_cpu.Controller, "sample", lambda self: {
        "sampled_unix": host.now, "cpu_count": 8, "interval_s": 1.0,
        "busy_cpus": host.busy, "psi_some": 0.0})
    monkeypatch.setattr(adaptive_gpu.Controller, "sample", lambda self: {
        "sampled_unix": host.now, "foreign_processes": []})
    host.queue.announce(host="sparky", tags=["gb10", "arm64"], has_gpu=True,
                        capacity=host.capacity, cpu_tiers=host.tiers)
    host.queue.announce(host="dl380g10", tags=["x86"], has_gpu=False,
                        capacity={"cpu": 8, "mem_gb": 32})
    return host


def _publish_class_action(host, name, demand, *, producer=False, slots=None, owner=None):
    checkout = host.root / name
    checkout.mkdir()
    (checkout / "task.py").write_text("print('fixture')\n")
    variables = {} if slots is None else {
        adaptive_cpu.SPOOL_ROOT_ENV: str(host.root / "spool"),
        adaptive_cpu.EXPORT_SLOTS_ENV: str(slots)}
    action = pb.seal_action({
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {"definition_id": f"tests/{name}", "definition_version": "v1",
                 "task_class": "generation", "determinism": "deterministic",
                 "artifact_family": "generic", "artifact_kind": "generic",
                 "argv": ["/bin/true"], "working_directory": ".",
                 "result_path": "result"},
        "inputs": [], "code_closure": pb.build_code_closure(checkout, ["task.py"]),
        "params": {} if owner is None else {"produced_spool": {"owner": owner}},
        "environment": {"variables": variables, "toolchain": {}},
        "execution_scope": {"portability": "portable", "platform_key": None,
                            "host_class": None}})
    cas = pb.PrismaBuildCAS(host.root / "cas")
    cas.publish_action_request(action)
    key = action["action_key"]
    host.now += 0.001
    host.queue.publish(action_key=key, cas_root=str(cas.root),
                       checkout_root=str(checkout), worker_script="worker.py",
                       resources=demand, tags=["gb10"],
                       needs_gpu=bool(demand.get("gpu")), dependent_of=owner)
    if producer:
        path = host.queue.item_path(pool.READY, key)
        row = json.loads(path.read_text())
        row["produced_output"] = {"schema": "prismabuild.produced_output_ref.v1",
                                  "template_id": "fixture", "template_sha256": "0" * 64}
        path.write_text(json.dumps(row))
    return key


def _claim_class_action(host, *, adaptive=True):
    return host.queue.claim(tags=["gb10", "arm64"], has_gpu=True,
                            capacity=host.capacity, cpu_tiers=host.tiers,
                            adaptive_cpu=adaptive)


def _class_metadata(host, key):
    return adaptive_cpu.read_json(
        host.queue.ledger().held_dir / key / adaptive_cpu.METADATA)


def _class_holder(host, *, slots=None):
    key = _publish_class_action(host, "holder", {"cpu": 1, "mem_gb": 1},
                                producer=slots is not None, slots=slots)
    claimed = _claim_class_action(host)
    assert claimed is not None and claimed["action_key"] == key
    admitted = _class_metadata(host, key)["admitted_unix"]
    host.now += 5.0
    current = {"action_key": key, "complete": True, "nonce": "holder",
               "sampled_unix": host.now, "wall_seconds": 5.0, "cpu_seconds": 4.0}
    base = adaptive_cpu.local_state_base(host.queue.ledger().base)
    adaptive_cpu.write_json(base / "telemetry" / f"{key}.json", current)
    adaptive_cpu.write_json(base / "jobs.json", {key: dict(
        current, sampled_unix=admitted, wall_seconds=0.0, cpu_seconds=0.0)})
    return key


def _class_denial(host, key):
    base = adaptive_cpu.local_state_base(host.queue.ledger().base)
    records = adaptive_cpu.read_json(base / pool.CLAIM_DENIALS)["records"]
    return next(record for record in records.values() if record["action_key"] == key)


@contextmanager
def _class_gpu_lock_busy(host, gpu):
    ready = Queue()
    release = Event()

    def hold():
        try:
            with host.queue._transition_locked(gpu) as acquired:
                ready.put(acquired)
                release.wait()
        except BaseException as exc:
            ready.put(exc)

    thread = Thread(target=hold)
    thread.start()
    try:
        acquired = ready.get()
        if isinstance(acquired, BaseException):
            raise acquired
        assert acquired
        yield
    finally:
        release.set()
        thread.join()


def _assert_class_deferred(host, candidate, gpu, *, cost, gpu_cost=4):
    queue = host.queue
    assert queue.item_path(pool.READY, candidate).exists()
    assert not queue.item_path(pool.CLAIMED, candidate).exists()
    assert queue.ledger().holder_tokens(candidate) == {}
    assert not (queue.ledger().held_dir / candidate).exists()
    assert queue.passes(candidate) == 0
    denial = _class_denial(host, candidate)
    assert denial["reason"] == "deferred_for_ready_gpu_row"
    evidence = denial["evidence"]
    assert evidence["kept_for"] == "class_scoped_cpu_projected_cpu_cost"
    assert evidence["gpu_row"] == gpu[:12]
    assert evidence["holder_cost"] == cost
    assert evidence["gpu_cost"] == gpu_cost
    assert evidence["host_busy_cpus"] == host.busy
    assert evidence["active_cpu_cost"] == 1.0 + cost
    assert evidence["pending_cpu_cost"] == cost
    assert evidence["decision"]["reason"] in {"admitted", "admitted_on_allowance"}


def test_claim_class_scoped_producer_allowance_preserves_gpu_headroom(class_claim_host):
    host = class_claim_host
    holder = _class_holder(host)
    candidate = _publish_class_action(host, "producer", {"cpu": 1, "mem_gb": 1},
                                      producer=True, slots=2)
    gpu = _publish_class_action(host, "gpu", {"cpu": 4, "gpu": 1, "mem_gb": 8})
    host.busy = 3.0
    before = host.queue.ledger().available()
    assert before["cpu"] >= 3 + 4 and before["mem_gb"] >= 3 + 8
    with _class_gpu_lock_busy(host, gpu):
        assert _claim_class_action(host) is None
    _assert_class_deferred(host, candidate, gpu, cost=3)
    assert host.queue.ledger().available() == before
    assert host.queue.ledger().held_keys() == [holder]


@pytest.mark.parametrize(("busy", "admitted"), [
    (1.0, True), (1.01, True), (1.02, False),
], ids=["equality", "tolerance-boundary", "beyond-tolerance"])
def test_claim_class_scoped_producer_full_allowance_fits(class_claim_host, busy, admitted):
    host = class_claim_host
    _class_holder(host)
    candidate = _publish_class_action(host, "producer", {"cpu": 1, "mem_gb": 1},
                                      producer=True, slots=2)
    gpu = _publish_class_action(host, "gpu", {"cpu": 4, "gpu": 1, "mem_gb": 8})
    host.busy = busy
    with _class_gpu_lock_busy(host, gpu):
        claimed = _claim_class_action(host)
    if not admitted:
        assert claimed is None
        _assert_class_deferred(host, candidate, gpu, cost=3)
        return
    assert claimed is not None and claimed["action_key"] == candidate
    assert host.queue.ledger().holder_tokens(candidate) == {"cpu": 3, "mem_gb": 3}
    meta = _class_metadata(host, candidate)
    assert meta["declared_cpu"] == 3
    assert meta["dependent_allowance"]["slots"] == 2
    assert len(meta["dependent_allowance"]["cpus"]) == 2
    assert meta["dependent_allowance"]["mem_gb"] == 2
    assert host.queue.passes(candidate) == 0
    assert host.queue.item_path(pool.READY, gpu).exists()


@pytest.mark.parametrize(("slots", "adaptive"), [
    (None, True), (8, True), (2, False),
], ids=["no-allowance", "capacity-omitted", "non-adaptive"])
def test_claim_class_scoped_producer_without_accepted_allowance(
        class_claim_host, slots, adaptive):
    host = class_claim_host
    _class_holder(host)
    candidate = _publish_class_action(host, "producer", {"cpu": 1, "mem_gb": 1},
                                      producer=True, slots=slots)
    gpu = _publish_class_action(host, "gpu", {"cpu": 4, "gpu": 1, "mem_gb": 8})
    host.busy = 3.0 if adaptive else 7.0
    with _class_gpu_lock_busy(host, gpu):
        claimed = _claim_class_action(host, adaptive=adaptive)
    assert claimed is not None and claimed["action_key"] == candidate
    assert host.queue.ledger().holder_tokens(candidate) == {"cpu": 1, "mem_gb": 1}
    meta = _class_metadata(host, candidate)
    assert meta.get("dependent_allowance") is None
    if adaptive:
        assert meta["declared_cpu"] == 1
    assert host.queue.passes(candidate) == 0
    assert host.queue.item_path(pool.READY, gpu).exists()


@pytest.mark.parametrize(("busy", "admitted"), [(2.0, True), (3.0, False)],
                         ids=["funded-fits", "funded-defers"])
def test_claim_class_scoped_funded_row_uses_full_demand(class_claim_host, busy, admitted):
    host = class_claim_host
    holder = _class_holder(host, slots=2)
    candidate = _publish_class_action(host, "export", {"cpu": 1, "mem_gb": 1}, owner=holder)
    gpu = _publish_class_action(host, "gpu", {"cpu": 5, "gpu": 1, "mem_gb": 8})
    host.busy = busy
    before = host.queue.ledger().available()
    assert before["cpu"] == 5
    with _class_gpu_lock_busy(host, gpu):
        claimed = _claim_class_action(host)
    assert host.queue.ledger().available() == before
    if not admitted:
        assert claimed is None
        _assert_class_deferred(host, candidate, gpu, cost=1, gpu_cost=5)
        return
    assert claimed is not None and claimed["action_key"] == candidate
    assert host.queue.ledger().holder_tokens(candidate) == {}
    meta = _class_metadata(host, candidate)
    assert meta["funded_by"] == holder
    assert meta["declared_cpu"] == 1
    assert meta["funded"]["mem_gb"] == 1
    assert len(meta["funded"]["cpus"]) == 1
    assert set(meta["funded"]["cpus"]) <= set(
        _class_metadata(host, holder)["dependent_allowance"]["cpus"])
    assert host.queue.passes(candidate) == 0
    assert host.queue.item_path(pool.READY, gpu).exists()
