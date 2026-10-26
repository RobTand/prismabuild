"""Class CPU prerequisites preserve READY-GPU room and adaptive headroom."""
from __future__ import annotations

from contextlib import contextmanager
import json
import math
import platform
from queue import Queue
from threading import Event, Thread
import types

import pytest

from prismabuild import adaptive_cpu, adaptive_gpu, core as pb, pool
from gpu_capacity_fixture import idle_gpu_sample

FIRST = 1000.0
GROUP = "g" * 32


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


def test_class_headroom_shares_the_one_calculation_at_future_holder_cost():
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
    assert held["holder_cost"] == 7.5 and held["gpu_cost"] == 4.0
    # The token remainder never stands in for the declared cost.
    assert adaptive_cpu.projected_cpu_fits(
        busy_cpus=1.0, pending_cpu_cost=6.0, active_cpu_cost=7.0,
        requested_cpu_cost=4.0, cpu_count=8) is False


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
    host.queue.announce(host="sparky", tags=["gb10", "arm64", "sparky"], has_gpu=True,
                        capacity=host.capacity, cpu_tiers=host.tiers)
    host.queue.announce(host="dl380g10", tags=["x86"], has_gpu=False,
                        capacity={"cpu": 8, "mem_gb": 32})
    return host


def _publish_class_action(host, name, demand, *, producer=False, slots=None, owner=None,
                          tags=None, task_class="generation"):
    checkout = host.root / name
    checkout.mkdir()
    (checkout / "task.py").write_text("print('fixture')\n")
    variables = {} if slots is None else {
        adaptive_cpu.SPOOL_ROOT_ENV: str(host.root / "spool"),
        adaptive_cpu.EXPORT_SLOTS_ENV: str(slots)}
    action = pb.seal_action({
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {"definition_id": f"tests/{name}", "definition_version": "v1",
                 "task_class": task_class, "determinism": "deterministic",
                 "artifact_family": "generic", "artifact_kind": "generic",
                 "argv": ["/bin/true"], "working_directory": ".",
                 "result_path": "result"},
        "inputs": [], "code_closure": pb.build_code_closure(checkout, ["task.py"]),
        "params": {} if owner is None else {"produced_spool": {"owner": owner}},
        "environment": {"variables": variables, "toolchain": {
            **pb.executable_toolchain_contract("/bin/true"),
            "system": platform.system(), "machine": platform.machine(),
            "libc": "-".join(platform.libc_ver()),
        } if task_class == "measurement" else {}},
        "execution_scope": {"portability": ("host_class_keyed" if task_class == "measurement"
                                              else "portable"), "platform_key": None,
                            "host_class": "gb10" if task_class == "measurement" else None}})
    cas = pb.PrismaBuildCAS(host.root / "cas")
    cas.publish_action_request(action)
    key = action["action_key"]
    host.now += 0.001
    host.queue.publish(action_key=key, cas_root=str(cas.root),
                       checkout_root=str(checkout), worker_script="worker.py",
                       resources=demand, tags=["gb10"] if tags is None else tags,
                       needs_gpu=bool(demand.get("gpu")), dependent_of=owner)
    if producer:
        path = host.queue.records.item_path(pool.READY, key)
        row = json.loads(path.read_text())
        row["produced_output"] = {"schema": "prismabuild.produced_output_ref.v1",
                                  "template_id": "fixture", "template_sha256": "0" * 64}
        path.write_text(json.dumps(row))
    return key


def _claim_class_action(host, *, adaptive=True):
    return host.queue.claim(tags=["gb10", "arm64", "sparky"], has_gpu=True,
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
            with host.queue.action_transition_lock(gpu) as acquired:
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
    assert queue.records.item_path(pool.READY, candidate).exists()
    assert not queue.records.item_path(pool.CLAIMED, candidate).exists()
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
    _assert_class_deferred(host, candidate, gpu, cost=3.75)
    assert host.queue.ledger().available() == before
    assert host.queue.ledger().held_keys() == [holder]


@pytest.mark.parametrize(("busy", "admitted"), [
    (1.25, True), (1.26, True), (1.27, False),
], ids=["equality", "tolerance-boundary", "beyond-tolerance"])
def test_claim_class_scoped_producer_full_allowance_fits(class_claim_host, busy, admitted):
    host = class_claim_host
    _class_holder(host)
    candidate = _publish_class_action(host, "producer", {"cpu": 1, "mem_gb": 1},
                                      producer=True, slots=2)
    gpu = _publish_class_action(host, "gpu", {"cpu": 3, "gpu": 1, "mem_gb": 8})
    host.busy = busy
    with _class_gpu_lock_busy(host, gpu):
        claimed = _claim_class_action(host)
    if not admitted:
        assert claimed is None
        _assert_class_deferred(host, candidate, gpu, cost=3.75, gpu_cost=3)
        return
    assert claimed is not None and claimed["action_key"] == candidate
    assert host.queue.ledger().holder_tokens(candidate) == {"cpu": 3, "mem_gb": 3}
    meta = _class_metadata(host, candidate)
    assert meta["declared_cpu"] == 3
    assert meta["dependent_allowance"]["slots"] == 2
    assert len(meta["dependent_allowance"]["cpus"]) == 2
    assert meta["dependent_allowance"]["mem_gb"] == 2
    assert host.queue.passes(candidate) == 0
    assert host.queue.records.item_path(pool.READY, gpu).exists()


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
    host.busy = 2.75 if adaptive else 7.0
    with _class_gpu_lock_busy(host, gpu):
        claimed = _claim_class_action(host, adaptive=adaptive)
    assert claimed is not None and claimed["action_key"] == candidate
    assert host.queue.ledger().holder_tokens(candidate) == {"cpu": 1, "mem_gb": 1}
    meta = _class_metadata(host, candidate)
    assert meta.get("dependent_allowance") is None
    if adaptive:
        assert meta["declared_cpu"] == 1
    assert host.queue.passes(candidate) == 0
    assert host.queue.records.item_path(pool.READY, gpu).exists()


@pytest.mark.parametrize(("busy", "admitted"), [(1.75, True), (3.0, False)],
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
        _assert_class_deferred(host, candidate, gpu, cost=1.25, gpu_cost=5)
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
    assert host.queue.records.item_path(pool.READY, gpu).exists()


@pytest.mark.parametrize(("tags", "admitted"), [
    (["gb10", "arm64"], True),
    ([], False),
    (["sparky"], True),
], ids=["arm64-prerequisite-1589", "portable-stays-protected", "explicit-host"])
def test_ready_gpu_class_prerequisite_1589(class_claim_host, tags, admitted):
    host = class_claim_host
    candidate = _publish_class_action(host, "prerequisite", {"cpu": 2, "mem_gb": 8},
                                      tags=tags)
    gpu = _publish_class_action(host, "gpu", {"cpu": 4, "gpu": 1, "mem_gb": 16})
    with _class_gpu_lock_busy(host, gpu):
        claimed = _claim_class_action(host)
    if admitted:
        assert claimed is not None and claimed["action_key"] == candidate
        assert host.queue.ledger().holder_tokens(candidate) == {"cpu": 2, "mem_gb": 8}
        assert not claimed.get("cpu_guest")
        assert host.queue.ledger().available()["cpu"] >= 4
        assert host.queue.ledger().available()["mem_gb"] >= 16
    else:
        assert claimed is None
        assert _class_denial(host, candidate)["reason"] == "deferred_for_ready_gpu"
        assert host.queue.ledger().holder_tokens(candidate) == {}
    assert host.queue.records.item_path(pool.READY, gpu).exists()
    assert host.queue.passes(candidate) == 0


@pytest.mark.parametrize("adaptive", [True, False])
@pytest.mark.parametrize("measured", ["candidate", "gpu"])
def test_class_measurements_do_not_bypass_ready_gpu(class_claim_host, adaptive, measured):
    host = class_claim_host
    candidate = _publish_class_action(
        host, "candidate", {"cpu": 2, "mem_gb": 8},
        task_class="measurement" if measured == "candidate" else "generation")
    gpu = _publish_class_action(
        host, "gpu", {"cpu": 4, "gpu": 1, "mem_gb": 16},
        task_class="measurement" if measured == "gpu" else "generation")
    with _class_gpu_lock_busy(host, gpu):
        assert _claim_class_action(host, adaptive=adaptive) is None
    assert _class_denial(host, candidate)["reason"] == "deferred_for_ready_gpu"
    assert host.queue.ledger().holder_tokens(candidate) == {}
    assert host.queue.passes(candidate) == 0


@pytest.mark.parametrize("missing", ["candidate", "gpu"])
def test_unreadable_sealed_contract_keeps_ready_gpu_protection(class_claim_host, missing):
    host = class_claim_host
    candidate = _publish_class_action(host, "candidate", {"cpu": 2, "mem_gb": 8})
    gpu = _publish_class_action(host, "gpu", {"cpu": 4, "gpu": 1, "mem_gb": 16})
    key = candidate if missing == "candidate" else gpu
    (host.root / "cas" / "requests" / key[:2] / f"{key}.json").unlink()

    with _class_gpu_lock_busy(host, gpu):
        assert _claim_class_action(host) is None

    assert _class_denial(host, candidate)["reason"] == "deferred_for_ready_gpu"
    assert host.queue.ledger().holder_tokens(candidate) == {}


def test_unknown_gpu_presence_is_not_class_evidence():
    assert not pool.PoolQueue._excluded_from_cpu_hosts(
        {"tags": ["gb10"]}, [{"tags": ["x86"]}, _offers()[1]])
    assert not pool.PoolQueue._excluded_from_cpu_hosts(
        {"tags": ["gb10"]}, [_offers()[0], {"tags": ["gb10"]}])


@pytest.mark.parametrize("value", [True, math.inf, -math.inf, 10 ** 1000])
def test_projected_cost_invalid_or_overflowing_number_refuses(value):
    assert not adaptive_cpu.projected_cpu_fits(
        busy_cpus=value, pending_cpu_cost=0, active_cpu_cost=0,
        requested_cpu_cost=1, cpu_count=8)


@pytest.mark.parametrize("capability", [contract.tag for contract in pool.contracts.CONTRACTS])
def test_automatic_capability_tags_do_not_establish_class_exclusion(capability):
    offers = [_offers()[0], dict(_offers()[1], tags=["gb10", "arm64", capability])]
    row = {"tags": [capability], "resources": {"cpu": 2, "mem_gb": 8}}
    assert not pool.PoolQueue._excluded_from_cpu_hosts(row, offers)
    assert not pool.PoolQueue._class_scoped_candidate(_candidate_self(), row, offers)


def test_original_1589_spool_row_is_class_scoped():
    row = {"tags": ["container-image-v1", "gb10"],
           "resources": {"cpu": 1, "mem_gb": 4, "spool_gb": 2}}
    assert pool.PoolQueue._class_scoped_candidate(_candidate_self(), row, _offers())


@pytest.mark.parametrize("adaptive", [False, True])
@pytest.mark.parametrize(("capacity", "gpu_spool", "admitted"), [
    (6, 2, True), (6, 3, False), (3, 0, False),
], ids=["exact-room", "preserve-gpu-spool", "own-spool-exhausted"])
def test_class_spool_contention_keeps_gpu_room(class_claim_host, monkeypatch, adaptive,
                                                capacity, gpu_spool, admitted):
    host = class_claim_host
    monkeypatch.setattr(adaptive_gpu.Controller, "sample", lambda self: idle_gpu_sample())
    host.capacity["spool_gb"] = capacity
    holder = _publish_class_action(host, "holder", {"cpu": 1, "mem_gb": 1, "spool_gb": 2})
    assert _claim_class_action(host, adaptive=adaptive)["action_key"] == holder
    candidate = _publish_class_action(host, "spool-prerequisite", {
        "cpu": 1, "mem_gb": 4, "spool_gb": 2}, tags=["gb10", "arm64"])
    gpu = _publish_class_action(host, "gpu", {
        "cpu": 4, "gpu": 1, "mem_gb": 16, "spool_gb": gpu_spool})
    with _class_gpu_lock_busy(host, gpu):
        claimed = _claim_class_action(host, adaptive=adaptive)
    assert bool(claimed) is admitted
    if admitted:
        assert claimed["action_key"] == candidate
        assert not claimed.get("cpu_guest")
        assert host.queue.ledger().holder_tokens(candidate)["spool_gb"] == 2
    else:
        assert host.queue.records.item_path(pool.READY, candidate).exists()
        assert host.queue.ledger().holder_tokens(candidate) == {}
    assert _claim_class_action(host, adaptive=adaptive)["action_key"] == gpu
    held = host.queue.ledger().held()
    assert held["spool_gb"] == 2 + (2 if admitted else 0) + gpu_spool
    assert held["spool_gb"] <= capacity
