"""GB10 unified memory charges mem_gb once; caps are subsets (#1661)."""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src")]
from prismabuild import adaptive_cpu, adaptive_gpu, box_capacity, pool  # noqa: E402

sys.path.insert(0, str(ROOT / "tools" / "fleet"))
import pbrun  # noqa: E402
import pbcampaign  # noqa: E402
from prismabuild import decomposition as dc  # noqa: E402

GIB = 1024**3
CAPACITY = {"cpu": 20, "gpu": 1, "mem_gb": 104}
TIERS = {"preferred": list(range(20)), "fallback": []}
SMALL = {"cpu": 2, "gpu": 1, "mem_gb": 6}
GPU_CAP_GB = 98


def _sealed(tmp_path: Path, name: str, params: dict | None = None):
    checkout = tmp_path / f"checkout-{name}"
    checkout.mkdir(exist_ok=True)
    (checkout / "task.py").write_text("print('fixture')\n")
    from prismabuild import core as pb
    action = pb.seal_action({
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {"definition_id": f"tests/{name}", "definition_version": "v1",
                 "task_class": "generation", "determinism": "deterministic",
                 "artifact_family": "generic", "artifact_kind": "generic",
                 "argv": [sys.executable, "task.py"], "working_directory": ".",
                 "result_path": "result"},
        "inputs": [], "code_closure": pb.build_code_closure(checkout, ["task.py"]),
        "params": dict(params or {}),
        "environment": {"variables": {}, "toolchain": {}},
        "execution_scope": {"portability": "portable", "platform_key": None,
                            "host_class": None},
    })
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    cas.publish_action_request(action)
    return action["action_key"], str(cas.root), str(checkout)


def _publish(q: pool.PoolQueue, key: str, resources: dict) -> None:
    q.publish(action_key=key, cas_root="/cas", checkout_root="/co",
              worker_script="/w.py", resources=resources, needs_gpu=True,
              priority=-10, tags=["gb10"])


def _publish_cpu(q: pool.PoolQueue, key: str, resources: dict) -> None:
    q.publish(action_key=key, cas_root="/cas", checkout_root="/co",
              worker_script="/w.py", resources=resources,
              priority=-10, tags=["gb10"])


def _denial(q: pool.PoolQueue, key: str) -> dict:
    path = adaptive_cpu.local_state_base(q.ledger().base) / pool.CLAIM_DENIALS
    records = adaptive_cpu.read_json(path).get("records", {})
    return next(v for v in records.values() if v["action_key"] == key)


@pytest.fixture()
def real_gpu_contract():
    return adaptive_gpu.action_contract


@pytest.fixture()
def rig(tmp_path: Path, monkeypatch, real_gpu_contract):
    import time as _time
    now = [4_000_000.0]
    monkeypatch.setattr(pool, "_now", lambda: now[0])
    monkeypatch.setattr(_time, "time", lambda: now[0])
    monkeypatch.setattr(adaptive_cpu.time, "time", lambda: now[0])
    monkeypatch.setattr(adaptive_cpu, "action_identity", lambda item: ("shape", False))
    sample = {
        "schema": "prismabuild.gpu_capacity.v1", "sample_id": "100",
        "sampled_unix": now[0], "complete": True, "attributed": True,
        "devices": [{"uuid": "GPU-1", "name": "NVIDIA GB10", "power_w": 15.0,
                     "power_limit_w": None, "power_reference_w": 140.0,
                     "power_reference_scope": "soc_tdp",
                     "memory_domain": "shared_system", "limited": False}],
        "host_total_bytes": 128 * GIB, "host_available_bytes": 120 * GIB,
        "memory_pressure_some": 0.0, "memory_pressure_full": 0.0,
        "cpu_pressure_some": 0.0, "foreign_processes": [], "jobs": []}
    monkeypatch.setattr(adaptive_gpu.Controller, "sample", lambda self: dict(sample))
    monkeypatch.setattr(adaptive_gpu, "action_contract",
                        lambda item, demand: (
                            "shape", False, False,
                            min(demand["mem_gb"], GPU_CAP_GB) * GIB))
    monkeypatch.setattr(adaptive_cpu.Controller, "sample", lambda self: {
        "sampled_unix": now[0], "busy_cpus": 0.0, "psi_some": 0.0,
        "cpu_count": 20, "interval_s": 1.0})
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    args = dict(capacity=CAPACITY, tags=["gb10"], cpu_tiers=TIERS,
                adaptive_cpu=True, has_gpu=True)
    return queue, now, sample, args


def _tick(rig, seconds: float = 2.0) -> None:
    queue, now, sample, _ = rig
    now[0] += seconds
    sample.update(sampled_unix=now[0], sample_id=str(now[0]))
    sample["jobs"] = []
    for key in queue.ledger().held_keys():
        record = {"action_key": key, "nonce": key + "-attempt",
                  "scope_unit": key + "-scope", "sampled_unix": now[0],
                  "cpu_seconds": 0.01 * (now[0] - 4_000_000.0),
                  "wall_seconds": now[0] - 4_000_000.0, "complete": True}
        adaptive_cpu.write_json(
            adaptive_cpu.local_telemetry_path(queue.ledger().base, key), record)
        sample["jobs"].append({"action_key": key, "nonce": record["nonce"],
                               "scope_id": record["scope_unit"], "complete": True})


@pytest.mark.parametrize("mem_gb,cap_gb", [(104, 100), (80, 64)])
def test_real_shapes_fit_an_empty_unified_offer(
        rig, tmp_path, monkeypatch, mem_gb, cap_gb):
    """104/100 and 80/64 start on an empty 104 GiB offer; caps add nothing."""
    from prismabuild import adaptive_gpu as _gpu
    monkeypatch.setattr(_gpu, "action_contract",
                        lambda item, demand: ("shape", False, False, cap_gb * GIB))
    queue, now, sample, args = rig
    key, _, _ = _sealed(tmp_path, f"shape-{mem_gb}-{cap_gb}")
    _publish(queue, key, {"cpu": 2, "gpu": 1, "mem_gb": mem_gb})
    got = queue.claim(**args)
    assert got is not None and got["action_key"] == key
    assert got["gpu_admission"]["gpu_memory_budget_bytes"] == cap_gb * GIB
    assert queue.ledger().held()["mem_gb"] == mem_gb


def test_second_large_action_waits_while_first_holds_mem(rig, tmp_path):
    """The 10-08 repro, subset form: 102 GiB held refuses the next 6 GiB."""
    queue, now, sample, args = rig
    first, _, _ = _sealed(tmp_path, "first")
    _publish(queue, first, {"cpu": 2, "gpu": 1, "mem_gb": 102})
    assert queue.claim(**args)["action_key"] == first
    second, _, _ = _sealed(tmp_path, "second")
    _publish(queue, second, SMALL)
    _tick(rig)
    assert queue.claim(**args) is None
    denial = _denial(queue, second)
    assert denial["reason"] == "adaptive_gpu_refused"
    decision = denial["evidence"]["decision"]
    assert decision["reason"] == "unified_gpu_memory_budget"
    assert decision["requested_mem_gb"] == SMALL["mem_gb"]
    assert decision["requested_budget_gib"] == SMALL["mem_gb"]
    assert decision["held_gpu_cap_total_gib"] == GPU_CAP_GB
    assert decision["held_mem_gb"] == 102.0
    assert decision["held_ram_mem_gb"] == 0.0
    assert decision["committed_gib"] == 102.0
    assert decision["candidate_charge_gib"] == SMALL["mem_gb"]
    assert decision["external_gpu_gib"] == 0
    assert decision["mem_offer_gib"] == CAPACITY["mem_gb"]
    assert decision["held_gpu_caps"] == [
        {"action_key": first, "gpu_cap_gib": GPU_CAP_GB,
         "mem_gb": 102.0, "charge_gib": 102.0}]
    queue.finish(first, status="executed", detail={})
    _tick(rig)
    assert queue.claim(**args)["action_key"] == second


def test_small_jobs_share_the_unified_offer(rig, tmp_path):
    """Two small GPU rows fit the 104 GiB offer at once, caps included."""
    queue, now, sample, args = rig
    first, _, _ = _sealed(tmp_path, "small-first")
    _publish(queue, first, SMALL)
    assert queue.claim(**args)["action_key"] == first
    second, _, _ = _sealed(tmp_path, "small-second")
    _publish(queue, second, SMALL)
    _tick(rig)
    got = queue.claim(**args)
    assert got is not None and got["action_key"] == second


def test_discrete_host_ignores_the_unified_gate(rig, tmp_path, monkeypatch):
    """A discrete device sums VRAM budgets; no unified refusal fires."""
    from prismabuild import adaptive_gpu as _gpu
    monkeypatch.setattr(_gpu, "action_contract",
                        lambda item, demand: ("shape", False, False, 60 * GIB))
    queue, now, sample, args = rig
    sample["devices"][0].update(
        memory_domain="discrete", memory_total_bytes=100 * GIB,
        memory_free_bytes=100 * GIB, memory_used_bytes=0)
    first, _, _ = _sealed(tmp_path, "discrete-first")
    _publish(queue, first, {"cpu": 2, "gpu": 1, "mem_gb": 60})
    assert queue.claim(**args)["action_key"] == first
    second, _, _ = _sealed(tmp_path, "discrete-second")
    _publish(queue, second, {"cpu": 2, "gpu": 1, "mem_gb": 30})
    _tick(rig)
    assert queue.claim(**args) is None
    decision = _denial(queue, second)["evidence"]["decision"]
    assert decision["reason"] == "gpu_memory_budget"
    assert decision["held_budget_bytes"] == 60 * GIB
    sample["devices"][0].update(
        memory_total_bytes=200 * GIB, memory_free_bytes=200 * GIB,
        memory_used_bytes=0)
    _tick(rig)
    got = queue.claim(**args)
    assert got is not None and got["action_key"] == second
    assert got["gpu_admission"]["memory_domain"] == "discrete"

def test_external_bytes_count_only_foreign_shared_system():
    """Attributed and discrete bytes never count; unknown flags, never hides."""
    devices = [{"uuid": "GPU-1", "memory_domain": "shared_system"}]
    foreign = [{"gpu_uuid": "GPU-1", "used_bytes": 30 * GIB}]
    total, unknown = adaptive_gpu.external_unified_gpu_bytes(devices, foreign)
    assert (total, unknown) == (30 * GIB, False)
    total, unknown = adaptive_gpu.external_unified_gpu_bytes(devices, [])
    assert (total, unknown) == (0, False)
    discrete = [{"uuid": "GPU-9", "memory_domain": "discrete"}]
    total, unknown = adaptive_gpu.external_unified_gpu_bytes(discrete, [
        {"gpu_uuid": "GPU-9", "used_bytes": 50 * GIB}])
    assert (total, unknown) == (0, False)
    total, unknown = adaptive_gpu.external_unified_gpu_bytes(
        devices, [{"gpu_uuid": "GPU-1", "used_bytes": None}])
    assert total == 0 and unknown is True
    total, unknown = adaptive_gpu.external_unified_gpu_bytes(
        devices, [{"gpu_uuid": "GPU-?", "used_bytes": 4 * GIB}])
    assert (total, unknown) == (4 * GIB, False)
    mixed = [devices[0], discrete[0]]
    total, unknown = adaptive_gpu.external_unified_gpu_bytes(mixed, [
        {"gpu_uuid": "GPU-1", "used_bytes": 4 * GIB},
        {"gpu_uuid": "GPU-9", "used_bytes": 50 * GIB},
        {"gpu_uuid": "GPU-?", "used_bytes": 8 * GIB}])
    assert (total, unknown) == (4 * GIB, False)


def test_node_offer_subtracts_external_unified_bytes():
    """The published offer falls by foreign GPU GiB, never by owned bytes."""
    assert box_capacity.MEMORY_MARGIN_GB == 8
    now = 5_000_000.0

    def sample_with(devices, foreign, jobs):
        return {
            "schema": "prismabuild.gpu_capacity.v1", "sample_id": "s",
            "sampled_unix": now, "complete": True, "attributed": True,
            "devices": devices, "host_total_bytes": 128 * GIB,
            "host_available_bytes": 120 * GIB,
            "memory_pressure_some": 0.0, "memory_pressure_full": 0.0,
            "cpu_pressure_some": 0.0, "cpu_pressure_full": 0.0,
            "foreign_processes": foreign, "jobs": jobs}

    shared = [{"uuid": "GPU-1", "memory_domain": "shared_system"}]
    foreign = [{"pid": 9, "gpu_uuid": "GPU-1", "used_bytes": 30 * GIB}]
    seen = box_capacity.observe(
        {"cpu": 8, "gpu": 1, "mem_gb": 104}, {}, gpu_sample=sample_with(
            shared, foreign, []),
        mem_gb=120, load1=0, now=now)
    assert seen.capacity["mem_gb"] == 82
    assert seen.detail["external_unified_gpu_gib"] == 30
    assert seen.detail["mem_available_less_external_gib"] == 90
    assert seen.detail["mem_available_gb"] == 120

    clean = box_capacity.observe(
        {"cpu": 8, "gpu": 1, "mem_gb": 104}, {}, gpu_sample=sample_with(
            shared, [], []),
        mem_gb=120, load1=0, now=now)
    assert clean.capacity["mem_gb"] == 104
    assert "external_unified_gpu_gib" not in clean.detail

    owned = box_capacity.observe(
        {"cpu": 8, "gpu": 1, "mem_gb": 104}, {"mem_gb": 60}, gpu_sample=sample_with(
            shared, [], [{"action_key": "k", "gpu_reported_bytes": 60 * GIB}]),
        mem_gb=120, load1=0, now=now)
    assert owned.capacity["mem_gb"] == 104

    discrete = [{"uuid": "GPU-9", "memory_domain": "discrete",
                 "memory_total_bytes": 200 * GIB, "memory_free_bytes": 150 * GIB,
                 "memory_used_bytes": 50 * GIB}]
    quiet = box_capacity.observe(
        {"cpu": 8, "gpu": 1, "mem_gb": 104}, {}, gpu_sample=sample_with(
            discrete, [{"pid": 9, "gpu_uuid": "GPU-9", "used_bytes": 50 * GIB}],
            []),
        mem_gb=120, load1=0, now=now)
    assert quiet.capacity["mem_gb"] == 104
    assert "external_unified_gpu_gib" not in quiet.detail

    margined = box_capacity.observe(
        {"cpu": 8, "gpu": 1, "mem_gb": 104}, {}, gpu_sample=sample_with(
            shared, [], []),
        mem_gb=100, load1=0, now=now)
    assert margined.capacity["mem_gb"] == 92


def test_external_gpu_bytes_refuse_a_cpu_only_candidate(rig, tmp_path, monkeypatch):
    """30 GiB of foreign GPU memory refuses a 32 GiB CPU row beside 60 held."""
    from prismabuild import adaptive_gpu as _gpu
    queue, now, sample, args = rig
    monkeypatch.setattr(_gpu, "trusted_sample", lambda *a, **k: sample)
    first, _, _ = _sealed(tmp_path, "gpu-holder")
    _publish(queue, first, {"cpu": 2, "gpu": 1, "mem_gb": 60})
    assert queue.claim(**args)["action_key"] == first
    cpu_key, _, _ = _sealed(tmp_path, "cpu-after-foreign")
    _publish_cpu(queue, cpu_key, {"cpu": 2, "mem_gb": 32})
    _tick(rig)
    sample["foreign_processes"] = [{
        "pid": 999, "start_ticks": 1, "cgroup": "/user.slice",
        "gpu_uuid": "GPU-1", "used_bytes": 30 * GIB}]
    assert queue.claim(**args) is None
    cpu_denial = _denial(queue, cpu_key)["evidence"]["decision"]
    assert cpu_denial["reason"] == "unified_gpu_memory_budget"
    assert cpu_denial["committed_gib"] == 60.0
    assert cpu_denial["candidate_charge_gib"] == 32.0
    assert cpu_denial["external_gpu_gib"] == 30
    assert cpu_denial["external_gpu_bytes"] == 30 * GIB
    assert cpu_denial["mem_offer_gib"] == CAPACITY["mem_gb"]
    sample["foreign_processes"] = []
    _tick(rig)
    got = queue.claim(**args)
    assert got is not None and got["action_key"] == cpu_key


def test_foreign_gpu_processes_keep_the_gpu_congestion_refusal(rig, tmp_path):
    """Foreign processes refuse GPU rows as congested, before any budget gate."""
    queue, now, sample, args = rig
    first, _, _ = _sealed(tmp_path, "congested-holder")
    _publish(queue, first, {"cpu": 2, "gpu": 1, "mem_gb": 60})
    assert queue.claim(**args)["action_key"] == first
    gpu_key, _, _ = _sealed(tmp_path, "gpu-after-foreign")
    _publish(queue, gpu_key, SMALL)
    _tick(rig)
    sample["foreign_processes"] = [{
        "pid": 999, "start_ticks": 1, "cgroup": "/user.slice",
        "gpu_uuid": "GPU-1", "used_bytes": 30 * GIB}]
    assert queue.claim(**args) is None
    gpu_denial = _denial(queue, gpu_key)["evidence"]["decision"]
    assert gpu_denial["reason"] == "host_or_device_congested"


def test_absent_cap_defaults_to_mem_gb(rig, tmp_path, real_gpu_contract):
    """No declared cap seals a budget equal to the mem_gb demand."""
    key, cas_root, _ = _sealed(tmp_path, "default-cap")
    item = {"action_key": key, "cas_root": cas_root}
    shape, measurement, exclusive, budget = real_gpu_contract(
        item, {"cpu": 2, "gpu": 1, "mem_gb": 80})
    assert (shape, measurement, exclusive, budget) == ("shape", False, True, 80 * GIB)
    explicit, _, _ = _sealed(
        tmp_path, "explicit-cap",
        {"gpu_exclusive": False, "gpu_memory_gb": 64})
    shape, measurement, exclusive, budget = real_gpu_contract(
        {"action_key": explicit, "cas_root": cas_root},
        {"cpu": 2, "gpu": 1, "mem_gb": 80})
    assert (shape, measurement, exclusive, budget) == ("shape", False, False, 64 * GIB)


def test_pbrun_refuses_a_cap_above_mem_gb_on_unified_placement():
    with pytest.raises(ValueError, match="exceeds mem_gb"):
        pbrun.require_gpu_memory_scope(
            gpu_memory_gb=100, gpu=True, transport="pool", mem_gb=80,
            tags=["gb10"])
    with pytest.raises(ValueError, match="exceeds mem_gb"):
        pbrun.require_gpu_memory_scope(
            gpu_memory_gb=100, gpu=True, transport="pool", mem_gb=80,
            host_class="gb10")
    pbrun.require_gpu_memory_scope(
        gpu_memory_gb=100, gpu=True, transport="pool", mem_gb=104,
        tags=["gb10"])
    pbrun.require_gpu_memory_scope(
        gpu_memory_gb=None, gpu=True, transport="pool", mem_gb=80,
        tags=["gb10"])
    assert pbrun.default_host_mem_gb(gpu=True) == 16
    assert pbrun.default_host_mem_gb(gpu=False) == 4


def test_discrete_submission_allows_a_cap_above_host_ram():
    """A 2 GiB discrete action may budget 8 GiB of separate VRAM."""
    pbrun.require_gpu_memory_scope(
        gpu_memory_gb=8, gpu=True, transport="pool", mem_gb=2)
    pbrun.require_gpu_memory_scope(
        gpu_memory_gb=8, gpu=True, transport="pool", mem_gb=2,
        tags=["x86"])
    assert not adaptive_gpu.unified_placement(["x86"], None)
    assert not adaptive_gpu.unified_placement([], None)
    assert adaptive_gpu.unified_placement(["gb10"], None)
    assert adaptive_gpu.unified_placement([], "gb10")


def test_discrete_scope_creation_allows_a_cap_above_host_ram(
        tmp_path, monkeypatch):
    """Scope creation keeps no subset rule; admission owns it per device."""
    from prismabuild import core as pb
    from prismabuild import resource_scope
    checkout = tmp_path / "checkout-discrete-scope"
    checkout.mkdir(exist_ok=True)
    (checkout / "task.py").write_text("print('fixture')\n")
    demand = {"cpu": 1, "gpu": 1, "mem_gb": 2}
    action = pb.seal_action({
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {"definition_id": "tests/discrete-scope",
                 "definition_version": "v1",
                 "task_class": "generation", "determinism": "deterministic",
                 "artifact_family": "generic", "artifact_kind": "generic",
                 "argv": [sys.executable, "task.py"], "working_directory": ".",
                 "result_path": "result"},
        "inputs": [], "code_closure": pb.build_code_closure(checkout, ["task.py"]),
        "params": {"demand": demand, "gpu_memory_gb": 8},
        "environment": {"variables": {}, "toolchain": {}},
        "execution_scope": {"portability": "portable", "platform_key": None,
                            "host_class": None},
    })
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    cas.publish_action_request(action)
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    queue.publish(action_key=action["action_key"], cas_root=str(cas.root),
                  checkout_root=str(checkout), worker_script="/w.py",
                  resources=demand, needs_gpu=True, priority=-10,
                  tags=["x86"])
    item = queue.claim(capacity={"cpu": 4, "gpu": 1, "mem_gb": 8},
                       tags=["x86"], has_gpu=True)
    assert item is not None and item["action_key"] == action["action_key"]
    from prismabuild.produced_output import _broker_scope_id
    def fake_broker(scope, op, **extra):
        assert op == "create"
        unit = _broker_scope_id(scope.action_key, scope.nonce)
        return {"ok": True, "scope_id": unit, "token": "b" * 64,
                "cgroup_path": "/sys/fs/cgroup/prismabuild.slice/" + unit,
                "gpu_memory_max_bytes": scope.gpu_memory_max_bytes}
    monkeypatch.setattr(resource_scope.ResourceScope, "_request", fake_broker)
    scope = queue._start_resource_scope(item)
    assert scope.gpu_memory_max_bytes == 8 * GIB
    assert scope.memory_max_bytes == 2 * GIB


def test_unified_admission_refuses_a_cap_above_mem_gb(rig, tmp_path, monkeypatch):
    """A 60 GiB action with a 98 GiB cap never starts on shared_system."""
    from prismabuild import adaptive_gpu as _gpu
    monkeypatch.setattr(_gpu, "action_contract",
                        lambda item, demand: ("shape", False, False, 98 * GIB))
    queue, now, sample, args = rig
    key, _, _ = _sealed(tmp_path, "cap-above-mem")
    _publish(queue, key, {"cpu": 2, "gpu": 1, "mem_gb": 60})
    assert queue.claim(**args) is None
    decision = _denial(queue, key)["evidence"]["decision"]
    assert decision["reason"] == "unified_gpu_cap_exceeds_mem"
    assert decision["requested_budget_gib"] == 98.0
    assert decision["requested_mem_gb"] == 60


def test_discrete_admission_allows_a_cap_above_host_ram(rig, tmp_path, monkeypatch):
    """The same 2/8 shape starts on a discrete device with roomy VRAM."""
    from prismabuild import adaptive_gpu as _gpu
    monkeypatch.setattr(_gpu, "action_contract",
                        lambda item, demand: ("shape", False, False, 8 * GIB))
    queue, now, sample, args = rig
    sample["devices"][0].update(
        memory_domain="discrete", memory_total_bytes=100 * GIB,
        memory_free_bytes=100 * GIB, memory_used_bytes=0)
    key, _, _ = _sealed(tmp_path, "discrete-cap-above-mem")
    _publish(queue, key, {"cpu": 2, "gpu": 1, "mem_gb": 2})
    got = queue.claim(**args)
    assert got is not None and got["action_key"] == key
    assert got["gpu_admission"]["gpu_memory_budget_bytes"] == 8 * GIB


def test_campaign_preflight_refuses_a_cap_above_mem_gb_on_gb10(tmp_path):
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps([{
        "argv": ["python", "task.py"],
        "demand": {"gpu": 1, "mem_gb": 80}, "gpu_memory_gb": 100,
        "tags": ["gb10"]}]))
    with pytest.raises(pbcampaign.ManifestError, match="exceeds mem_gb"):
        pbcampaign.load_manifest(str(manifest), transport="pool")
    manifest.write_text(json.dumps([{
        "argv": ["python", "task.py"],
        "demand": {"gpu": 1, "mem_gb": 80}, "gpu_memory_gb": 64,
        "tags": ["gb10"]}]))
    rows = pbcampaign.load_manifest(str(manifest), transport="pool")
    assert rows[0]["gpu_memory_gb"] == 64


def test_campaign_preflight_allows_a_cap_above_mem_gb_off_gb10(tmp_path):
    """Discrete campaign rows keep independent VRAM budgets."""
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps([{
        "argv": ["python", "task.py"],
        "demand": {"gpu": 1, "mem_gb": 2}, "gpu_memory_gb": 8,
        "tags": ["x86"]}]))
    rows = pbcampaign.load_manifest(str(manifest), transport="pool")
    assert rows[0]["gpu_memory_gb"] == 8
    manifest.write_text(json.dumps([{
        "argv": ["python", "task.py"],
        "demand": {"gpu": 1, "mem_gb": 2}, "gpu_memory_gb": 8}]))
    rows = pbcampaign.load_manifest(str(manifest), transport="pool")
    assert rows[0]["gpu_memory_gb"] == 8


def test_decomposition_refuses_a_cap_above_mem_gb_on_gb10():
    common = {
        "argv": ["python", "collect.py", dc.TASK_BATCH_PLACEHOLDER],
        "cwd": "/checkout",
        "demand": {"gpu": 1, "cpu": 1, "mem_gb": 80},
        "gpu_memory_gb": 100,
        "data_manifest": None,
        "env": {"OMP_NUM_THREADS": "1"},
        "tags": ["gb10"],
    }
    from prismabuild import core as pb
    with pytest.raises(pb.ActionContractError, match="exceeds demand"):
        dc.validate_common_spec(common)
    assert dc.validate_common_spec({**common, "gpu_memory_gb": 64})["gpu_memory_gb"] == 64


def test_decomposition_allows_a_cap_above_mem_gb_off_gb10():
    """Discrete decomposed rows keep independent VRAM budgets."""
    common = {
        "argv": ["python", "collect.py", dc.TASK_BATCH_PLACEHOLDER],
        "cwd": "/checkout",
        "demand": {"gpu": 1, "cpu": 1, "mem_gb": 2},
        "gpu_memory_gb": 8,
        "data_manifest": None,
        "env": {"OMP_NUM_THREADS": "1"},
    }
    assert dc.validate_common_spec(common)["gpu_memory_gb"] == 8
    assert dc.validate_common_spec({**common, "tags": ["x86"]})["gpu_memory_gb"] == 8


def test_adjusted_offer_admits_without_a_second_external_charge(
        rig, tmp_path, monkeypatch):
    """An 82 GiB offer with a 30 GiB baseline still admits 60 GiB once."""
    from prismabuild import adaptive_gpu as _gpu
    queue, now, sample, args = rig
    monkeypatch.setattr(_gpu, "trusted_sample", lambda *a, **k: sample)
    observer = box_capacity.CapacityObserver(samples=1)
    observed_sample = {
        "schema": "prismabuild.gpu_capacity.v1", "sample_id": "obs",
        "sampled_unix": now[0], "complete": True, "attributed": True,
        "devices": [{"uuid": "GPU-1", "memory_domain": "shared_system"}],
        "host_total_bytes": 128 * GIB, "host_available_bytes": 120 * GIB,
        "memory_pressure_some": 0.0, "memory_pressure_full": 0.0,
        "cpu_pressure_some": 0.0, "cpu_pressure_full": 0.0,
        "foreign_processes": [{"pid": 9, "gpu_uuid": "GPU-1",
                               "used_bytes": 30 * GIB}],
        "jobs": []}
    offered = observer.offer(
        {"cpu": 20, "gpu": 1, "mem_gb": 104}, {}, gpu_sample=observed_sample,
        mem_gb=120, load1=0)
    assert offered["mem_gb"] == 82
    assert observer.last_offer_external_gib == 30
    sample["foreign_processes"] = [{
        "pid": 999, "start_ticks": 1, "cgroup": "/user.slice",
        "gpu_uuid": "GPU-1", "used_bytes": 30 * GIB}]

    def gate():
        return queue.claim(capacity=offered, tags=["gb10"], cpu_tiers=TIERS,
                           adaptive_cpu=True, has_gpu=True,
                           observed_external_gib=observer.last_offer_external_gib)
    first, _, _ = _sealed(tmp_path, "adjusted-first")
    _publish_cpu(queue, first, {"cpu": 2, "mem_gb": 60})
    assert gate()["action_key"] == first
    # Fresh growth beyond the baseline still charges once: beside 60 held
    # and 10 new foreign GiB, 12 GiB fits the 82 GiB offer exactly.
    sample["foreign_processes"] = [{
        "pid": 999, "start_ticks": 1, "cgroup": "/user.slice",
        "gpu_uuid": "GPU-1", "used_bytes": 40 * GIB}]
    _tick(rig)
    sample["foreign_processes"] = [{
        "pid": 999, "start_ticks": 1, "cgroup": "/user.slice",
        "gpu_uuid": "GPU-1", "used_bytes": 40 * GIB}]
    cpu_fit, _, _ = _sealed(tmp_path, "adjusted-cpu-fit")
    _publish_cpu(queue, cpu_fit, {"cpu": 2, "mem_gb": 12})
    assert gate()["action_key"] == cpu_fit
    cpu_big, _, _ = _sealed(tmp_path, "adjusted-cpu-big")
    _publish_cpu(queue, cpu_big, {"cpu": 2, "mem_gb": 24})
    _tick(rig)
    sample["foreign_processes"] = [{
        "pid": 999, "start_ticks": 1, "cgroup": "/user.slice",
        "gpu_uuid": "GPU-1", "used_bytes": 40 * GIB}]
    assert gate() is None
    decision = _denial(queue, cpu_big)["evidence"]["decision"]
    assert decision["reason"] == "unified_gpu_memory_budget"
    assert decision["external_gpu_gib"] == 40
    assert decision["external_baseline_gib"] == 30
    assert decision["external_charged_gib"] == 10


def test_host_without_gpu_keeps_plain_token_admission(rig, tmp_path):
    """No GPU controller runs, so no unified charge can refuse."""
    queue, now, sample, args = rig
    first, _, _ = _sealed(tmp_path, "nogpu-first")
    _publish_cpu(queue, first, {"cpu": 2, "mem_gb": 60})
    plain = dict(args, has_gpu=False)
    assert queue.claim(**plain)["action_key"] == first
    cpu_key, _, _ = _sealed(tmp_path, "nogpu-second")
    _publish_cpu(queue, cpu_key, {"cpu": 2, "mem_gb": 40})
    _tick(rig)
    got = queue.claim(**plain)
    assert got is not None and got["action_key"] == cpu_key
    assert "gpu_admission" not in got


def test_release_frees_the_unified_charge(rig, tmp_path):
    """The release path is the existing finish path; charges leave with it."""
    queue, now, sample, args = rig
    first, _, _ = _sealed(tmp_path, "rel-first")
    _publish(queue, first, {"cpu": 2, "gpu": 1, "mem_gb": 100})
    assert queue.claim(**args)["action_key"] == first
    queue.finish(first, status="executed", detail={})
    assert queue.ledger().held_keys() == []
    second, _, _ = _sealed(tmp_path, "rel-second")
    _publish(queue, second, SMALL)
    _tick(rig)
    assert queue.claim(**args)["action_key"] == second


def test_starting_reservation_counts_once(rig, tmp_path):
    """An acquiring holder charges mem once; its cap is reported, not added."""
    from prismabuild import adaptive_gpu as _gpu
    queue, now, sample, args = rig
    first, _, _ = _sealed(tmp_path, "starting-first")
    _publish(queue, first, SMALL)
    assert queue.claim(**args)["action_key"] == first
    ledger = queue.ledger()
    handle = ledger.begin_acquire(
        "f" * 64, {"cpu": 2, "gpu": 1, "mem_gb": 96}, adaptive_gpu={
            "action_key": "f" * 64, "admitted_unix": now[0], "probe": True,
            "gpu_memory_budget_bytes": 90 * GIB,
            "memory_domain": "shared_system"})
    assert handle is not None
    assert handle.startswith(pool.ACQUIRING_PREFIX)
    try:
        controller = _gpu.Controller(ledger)
        controller._sample = dict(sample)
        item = adaptive_cpu.read_json(
            queue.item_path(pool.CLAIMED, first))
        assert controller.decision(
            item, SMALL,
            contract=("shape", False, False, SMALL["mem_gb"] * GIB)) is None
        decision = controller.last_decision
        assert decision["reason"] == "unified_gpu_memory_budget"
        assert decision["held_gpu_cap_total_gib"] == SMALL["mem_gb"] + 90.0
        assert decision["held_mem_gb"] == SMALL["mem_gb"] + 96.0
        assert decision["held_ram_mem_gb"] == 0.0
        assert decision["committed_gib"] == SMALL["mem_gb"] + 96.0
        assert decision["candidate_charge_gib"] == SMALL["mem_gb"]
        assert {entry["action_key"] for entry in decision["held_gpu_caps"]} == {
            first, handle}
    finally:
        ledger.abandon_acquire(handle)


def test_oversize_mem_refuses_on_an_empty_host(rig, tmp_path):
    """A 120 GiB demand cannot start on a 104 GiB offer, even first."""
    from prismabuild import adaptive_gpu as _gpu
    queue, now, sample, args = rig
    first, _, _ = _sealed(tmp_path, "oversize-first")
    _publish(queue, first, {"cpu": 2, "gpu": 1, "mem_gb": 120})
    queue.ledger().ensure_capacity(CAPACITY)
    # Plenty of live headroom, so only the 104 GiB roof can refuse.
    sample.update(host_total_bytes=256 * GIB, host_available_bytes=200 * GIB)
    controller = _gpu.Controller(queue.ledger())
    controller._sample = dict(sample)
    item = adaptive_cpu.read_json(queue.item_path(pool.READY, first))
    assert controller.decision(
        item, {"cpu": 2, "gpu": 1, "mem_gb": 120},
        contract=("shape", False, False, SMALL["mem_gb"] * GIB)) is None
    decision = controller.last_decision
    assert decision["reason"] == "unified_gpu_memory_budget"
    assert decision["held_gpu_caps"] == []
    assert decision["held_gpu_cap_total_gib"] == 0.0
    assert decision["held_mem_gb"] == 0.0
    assert decision["candidate_charge_gib"] == 120.0
    assert decision["mem_offer_gib"] == CAPACITY["mem_gb"]


def test_cpu_holder_memory_counts_once_toward_the_offer(rig, tmp_path):
    """100 GiB of CPU-held memory leaves no room for the next 6 GiB."""
    queue, now, sample, args = rig
    cpu_key, _, _ = _sealed(tmp_path, "cpu-holder")
    _publish_cpu(queue, cpu_key, {"cpu": 2, "mem_gb": 100})
    assert queue.claim(**args)["action_key"] == cpu_key
    gpu_key, _, _ = _sealed(tmp_path, "gpu-after-cpu")
    _publish(queue, gpu_key, SMALL)
    _tick(rig)
    assert queue.claim(**args) is None
    denial = _denial(queue, gpu_key)
    assert denial["reason"] == "adaptive_gpu_refused"
    decision = denial["evidence"]["decision"]
    assert decision["reason"] == "unified_gpu_memory_budget"
    assert decision["held_gpu_caps"] == [
        {"action_key": cpu_key, "gpu_cap_gib": 0.0, "mem_gb": 100.0,
         "charge_gib": 100.0}]
    assert decision["held_gpu_cap_total_gib"] == 0.0
    assert decision["held_mem_gb"] == 100.0
    assert decision["committed_gib"] == 100.0
    assert decision["candidate_charge_gib"] == SMALL["mem_gb"]


def test_ram_fill_memory_counts_once_toward_the_offer(rig, tmp_path):
    """40 GiB of RAM-fill memory leaves no room for the next 70 GiB."""
    queue, now, sample, args = rig
    queue.ledger().ensure_capacity(CAPACITY)
    state, detail = queue.hold_tier_host_memory(
        queue.ledger().host, "a" * 64, 40)
    assert (state, detail) == ("taken", "")
    gpu_key, _, _ = _sealed(tmp_path, "gpu-after-ram-fill")
    _publish(queue, gpu_key, {"cpu": 2, "gpu": 1, "mem_gb": 70})
    _tick(rig)
    assert queue.claim(**args) is None
    denial = _denial(queue, gpu_key)
    assert denial["reason"] == "adaptive_gpu_refused"
    decision = denial["evidence"]["decision"]
    assert decision["reason"] == "unified_gpu_memory_budget"
    assert decision["held_ram_mem_gb"] == 40.0
    assert decision["held_mem_gb"] == 40.0
    assert decision["committed_gib"] == 40.0
    assert decision["candidate_charge_gib"] == 70.0
    queue.release_tier_host_memory(queue.ledger().host, "a" * 64)
    _tick(rig)
    assert queue.claim(**args)["action_key"] == gpu_key


def test_hold_then_release_admits_beside_small_caps(rig, tmp_path):
    """A 6 GiB CPU row claims beside small GPU rows on the 104 GiB offer."""
    queue, now, sample, args = rig
    first, _, _ = _sealed(tmp_path, "small-gpu-first")
    _publish(queue, first, SMALL)
    assert queue.claim(**args)["action_key"] == first
    second, _, _ = _sealed(tmp_path, "small-gpu-second")
    _publish(queue, second, SMALL)
    _tick(rig)
    assert queue.claim(**args)["action_key"] == second
    cpu_key, _, _ = _sealed(tmp_path, "small-cpu-third")
    _publish_cpu(queue, cpu_key, {"cpu": 2, "mem_gb": 6})
    _tick(rig)
    got = queue.claim(**args)
    assert got is not None and got["action_key"] == cpu_key


@pytest.mark.parametrize("fault", [
    "absent", "stale", "incomplete", "unattributed", "domain_unknown",
])
@pytest.mark.parametrize("starting", [False, True])
def test_held_charge_survives_gpu_telemetry_loss(
        rig, tmp_path, fault, starting):
    """Held mem still refuses CPU work when the broker loses GPU evidence."""
    queue, now, sample, args = rig
    key, _, _ = _sealed(tmp_path, "telemetry-holder")
    if starting:
        ledger = queue.ledger()
        ledger.configure_cpu_tiers(TIERS)
        ledger.ensure_capacity(CAPACITY)
        handle = ledger.begin_acquire(
            key, {"cpu": 2, "gpu": 1, "mem_gb": 100}, adaptive_gpu={
                "action_key": key, "admitted_unix": now[0], "probe": False,
                "gpu_memory_budget_bytes": GPU_CAP_GB * GIB,
                "memory_domain": "shared_system"})
        assert handle is not None
    else:
        _publish(queue, key, {"cpu": 2, "gpu": 1, "mem_gb": 100})
        assert queue.claim(**args)["action_key"] == key
        handle = key
    cpu_key, _, _ = _sealed(tmp_path, "telemetry-candidate")
    _publish_cpu(queue, cpu_key, {"cpu": 2, "mem_gb": 6})
    _tick(rig)
    if fault == "absent":
        sample.clear()
    elif fault == "stale":
        sample["sampled_unix"] -= adaptive_gpu.MAX_SAMPLE_AGE_S + 1
    elif fault == "incomplete":
        sample["complete"] = False
    elif fault == "unattributed":
        sample["attributed"] = False
    else:
        sample["devices"][0]["memory_domain"] = "unknown"
    assert queue.claim(**args) is None
    decision = _denial(queue, cpu_key)["evidence"]["decision"]
    assert decision["reason"] == "unified_gpu_memory_budget"
    assert decision["held_gpu_cap_total_gib"] == GPU_CAP_GB
    assert decision["committed_gib"] == 100.0
    assert decision["candidate_charge_gib"] == 6.0
    assert decision["held_gpu_caps"][0]["action_key"] == handle
    if starting:
        queue.ledger().abandon_acquire(handle)
    else:
        queue.finish(key, status="executed", detail={})
    # CPU work needs no GPU evidence after the last charge releases.
    assert queue.claim(**args)["action_key"] == cpu_key


@pytest.mark.parametrize("explicit_cap", [False, True])
def test_gpu_producer_charges_export_allowance_once(
        rig, tmp_path, monkeypatch, real_gpu_contract, explicit_cap):
    """The producer and its exports charge mem once beside the sealed cap."""
    import test_prepaid_writer_integration as fx
    from prismabuild import core, produced_output, produced_spool

    queue, now, sample, args = rig
    monkeypatch.setattr(adaptive_gpu, "action_contract", real_gpu_contract)
    holder, _, _ = _sealed(tmp_path, "producer-cpu-holder")
    _publish_cpu(queue, holder, {"cpu": 1, "mem_gb": 4})
    assert queue.claim(**args)["action_key"] == holder
    _tick(rig)
    template = fx._template(str(tmp_path / "canonical"))
    cas_root = tmp_path / "cas"
    initial = fx._producer_request(tmp_path, cas_root, template)
    cas, request = produced_output._read_producer_request(cas_root, initial)
    request.pop("action_key")
    host_mem = 100
    cap = 96 if explicit_cap else host_mem
    demand = {"cpu": 2, "gpu": 1, "mem_gb": host_mem}
    request["params"].update(demand=demand, gpu_exclusive=False)
    if explicit_cap:
        request["params"]["gpu_memory_gb"] = cap
    request["environment"]["variables"].update({
        produced_spool.ROOT_ENV: str(tmp_path / "spool"),
        adaptive_cpu.EXPORT_SLOTS_ENV: "1"})
    action = core.seal_action(request)
    cas.publish_action_request(action)
    key = action["action_key"]
    queue.mint_tier_capacity(fx.TIER, {fx.KIND: 4})
    queue.publish(action_key=key, cas_root=str(cas.root),
                  checkout_root=str(tmp_path / "mover-checkout"),
                  worker_script="/w.py", needs_gpu=True, tags=["gb10"],
                  resources={**demand, **produced_output.owner_demand_terms(template)},
                  produced_output_template=template)
    assert queue.claim(**args) is None
    decision = _denial(queue, key)["evidence"]["decision"]
    assert decision["reason"] == "unified_gpu_memory_budget"
    assert decision["requested_budget_gib"] == cap
    assert decision["requested_mem_gb"] == host_mem + 1
    assert decision["candidate_charge_gib"] == host_mem + 1
    assert decision["committed_gib"] == 4
    assert queue.ledger().held_keys() == [holder]
    queue.finish(holder, status="executed", detail={})
    _tick(rig)
    claimed = queue.claim(**args)
    assert claimed is not None and claimed["action_key"] == key
    assert claimed["gpu_admission"]["gpu_memory_budget_bytes"] == cap * GIB
    assert queue.ledger().holder_tokens(key)["mem_gb"] == host_mem + 1
    meta = adaptive_cpu.read_json(
        queue.ledger().held_dir / key / adaptive_cpu.METADATA)
    assert meta["dependent_allowance"]["mem_gb"] == 1


def test_cpu_claimants_cannot_spend_the_same_unified_headroom(
        rig, tmp_path, monkeypatch):
    """The host admission lock serializes CPU claims against held memory."""
    from concurrent.futures import ThreadPoolExecutor

    queue, now, sample, args = rig
    key, _, _ = _sealed(tmp_path, "concurrent-holder")
    _publish(queue, key, {"cpu": 2, "gpu": 1, "mem_gb": 96})
    assert queue.claim(**args)["action_key"] == key
    for index in range(4):
        candidate, _, _ = _sealed(tmp_path, f"concurrent-cpu-{index}")
        _publish_cpu(queue, candidate, {"cpu": 1, "mem_gb": 6})
    _tick(rig)
    sample.clear()
    with ThreadPoolExecutor(max_workers=4) as threads:
        claims = list(threads.map(lambda _: queue.claim(**args), range(4)))
    assert sum(claim is not None for claim in claims) == 1
    assert queue.ledger().held()["mem_gb"] == 102
    assert queue.claim(**args) is None


def test_held_charge_holds_during_the_memory_observation_window(rig, tmp_path):
    """A full windowed offer cannot hide the declared mem charge."""
    from prismabuild import box_capacity

    queue, now, sample, args = rig
    observer = box_capacity.CapacityObserver(samples=3)
    assert observer.offer(CAPACITY, held={}, gpu_sample=sample,
                          mem_gb=120, load1=0)["mem_gb"] == 104
    key, _, _ = _sealed(tmp_path, "window-holder")
    _publish(queue, key, {"cpu": 2, "gpu": 1, "mem_gb": 100})
    assert queue.claim(**args)["action_key"] == key
    candidate, _, _ = _sealed(tmp_path, "window-cpu")
    _publish_cpu(queue, candidate, {"cpu": 2, "mem_gb": 6})
    for _ in range(3):
        _tick(rig)
        offered = observer.offer(CAPACITY, held=queue.ledger().held,
                                 gpu_sample=sample, mem_gb=30, load1=0)
        assert offered["mem_gb"] == 104
        assert queue.claim(**dict(args, capacity=offered)) is None
        decision = _denial(queue, candidate)["evidence"]["decision"]
        assert decision["reason"] == "unified_gpu_memory_budget"
        assert decision["committed_gib"] == 100.0
        assert decision["candidate_charge_gib"] == 6.0


@pytest.mark.parametrize("gpu_candidate", [False, True], ids=["cpu", "gpu"])
def test_memory_refusal_preserves_background_preemption(rig, tmp_path, gpu_candidate):
    """A memory refusal stops restartable background work but keeps its charge."""
    queue, now, sample, args = rig
    # CPU work explicitly targets this synthetic host, so the ready-GPU
    # placement rule cannot mask the priority and memory-release assertions.
    host = pool.socket.gethostname()
    args = dict(args, tags=["gb10", host])
    holder, _, _ = _sealed(tmp_path, "preemptible-holder")
    queue.publish(
        action_key=holder, cas_root=tmp_path / "cas", checkout_root=tmp_path,
        worker_script="/w.py", resources={"cpu": 2, "gpu": 1, "mem_gb": 100},
        needs_gpu=True, tags=["gb10"], priority=-10, retry_safe=True, max_attempts=3)
    first = queue.claim(**args)
    assert first is not None and first["action_key"] == holder
    candidate, _, _ = _sealed(tmp_path, "foreground")
    demand = dict(SMALL) if gpu_candidate else {"cpu": 2, "mem_gb": 6}
    queue.publish(
        action_key=candidate, cas_root=tmp_path / "cas", checkout_root=tmp_path,
        worker_script="/w.py", resources=demand, needs_gpu=gpu_candidate,
        tags=["gb10"] if gpu_candidate else [host], priority=0)
    _tick(rig)
    assert queue.claim(**args) is None
    decisions = [value for _, value in queue.withdrawal_decisions(holder)]
    assert len(decisions) == 1
    assert decisions[0]["preempted_by"] == candidate
    assert queue.ledger().held()["mem_gb"] == 100
    decision = _denial(queue, candidate)["evidence"]["decision"]
    assert decision["reason"] == "unified_gpu_memory_budget"
    assert decision["held_gpu_cap_total_gib"] == 98
    _tick(rig)
    assert queue.claim(**args) is None
    assert len(queue.withdrawal_decisions(holder)) == 1
    assert queue.ledger().held()["mem_gb"] == 100
    queue.finish(holder, status="withdrawn", detail={}, claim_snapshot=first)
    _tick(rig)
    assert queue.claim(**args)["action_key"] == candidate


def test_external_growth_prevents_a_useless_background_stop(rig, tmp_path, monkeypatch):
    """Do not stop a holder when external GPU memory still prevents admission."""
    queue, now, sample, args = rig
    monkeypatch.setattr(adaptive_gpu, "trusted_sample", lambda: sample)
    holder, _, _ = _sealed(tmp_path, "external-preemption-holder")
    queue.publish(
        action_key=holder, cas_root=tmp_path / "cas", checkout_root=tmp_path,
        worker_script="/w.py", resources={"cpu": 2, "gpu": 1, "mem_gb": 100},
        needs_gpu=True, tags=["gb10"], priority=-10, retry_safe=True, max_attempts=3)
    assert queue.claim(**args)["action_key"] == holder
    candidate, _, _ = _sealed(tmp_path, "external-preemption-candidate")
    queue.publish(
        action_key=candidate, cas_root=tmp_path / "cas", checkout_root=tmp_path,
        worker_script="/w.py", resources={"cpu": 2, "mem_gb": 6},
        tags=["gb10"], priority=0)
    _tick(rig)
    sample["foreign_processes"] = [
        {"gpu_uuid": "GPU-1", "used_bytes": 100 * GIB}]
    assert queue.claim(**args) is None
    assert queue.withdrawal_decisions(holder) == []
    assert queue.ledger().held()["mem_gb"] == 100
    decision = _denial(queue, candidate)["evidence"]["decision"]
    assert decision["external_charged_gib"] == 100
    assert decision["reason"] == "unified_gpu_memory_budget"
