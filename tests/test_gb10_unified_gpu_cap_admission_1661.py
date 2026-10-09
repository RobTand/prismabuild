"""GB10 unified memory admits GPU caps from the shared pool (#1661)."""
from __future__ import annotations

import hashlib
import json
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src")]
from prismabuild import adaptive_cpu, adaptive_gpu, pool  # noqa: E402

GIB = 1024**3
CAPACITY = {"cpu": 20, "gpu": 1, "mem_gb": 104}
TIERS = {"preferred": list(range(20)), "fallback": []}
FIRST = {"cpu": 2, "gpu": 1, "mem_gb": 6}
SECOND = {"cpu": 2, "gpu": 1, "mem_gb": 6}
GPU_CAP_GB = 98


def _sealed(tmp_path: Path, name: str):
    checkout = tmp_path / f"checkout-{name}"
    checkout.mkdir()
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
        "params": {},
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


def _denial(q: pool.PoolQueue, key: str) -> dict:
    path = adaptive_cpu.local_state_base(q.ledger().base) / pool.CLAIM_DENIALS
    records = adaptive_cpu.read_json(path).get("records", {})
    return next(v for v in records.values() if v["action_key"] == key)


@pytest.fixture()
def rig(tmp_path: Path, monkeypatch):
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
                        lambda item, demand: ("shape", False, False, GPU_CAP_GB * GIB))
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


def test_second_large_action_waits_while_first_holds_unified_gpu_cap(rig, tmp_path):
    """The 10-08 repro: 98 GiB held GPU cap refuses the next 98 GiB cap."""
    queue, now, sample, args = rig
    first, cas_root, checkout = _sealed(tmp_path, "first")
    _publish(queue, first, FIRST)
    assert queue.claim(**args)["action_key"] == first
    second, _, _ = _sealed(tmp_path, "second")
    _publish(queue, second, SECOND)
    _tick(rig)
    assert queue.claim(**args) is None
    denial = _denial(queue, second)
    assert denial["reason"] == "adaptive_gpu_refused"
    decision = denial["evidence"]["decision"]
    assert decision["reason"] == "unified_gpu_memory_budget"
    assert decision["requested_budget_gib"] == GPU_CAP_GB
    assert decision["requested_mem_gb"] == SECOND["mem_gb"]
    assert decision["held_gpu_cap_total_gib"] == GPU_CAP_GB
    assert decision["held_mem_gb"] == FIRST["mem_gb"]
    assert decision["held_ram_mem_gb"] == 0.0
    assert decision["committed_gib"] == FIRST["mem_gb"] + GPU_CAP_GB
    assert decision["candidate_charge_gib"] == SECOND["mem_gb"] + GPU_CAP_GB
    assert decision["mem_offer_gib"] == CAPACITY["mem_gb"]
    assert decision["held_gpu_caps"] == [
        {"action_key": first, "gpu_cap_gib": GPU_CAP_GB,
         "mem_gb": float(FIRST["mem_gb"]),
         "charge_gib": FIRST["mem_gb"] + GPU_CAP_GB}]
    queue.finish(first, status="executed", detail={})
    _tick(rig)
    assert queue.claim(**args)["action_key"] == second

def test_small_caps_share_the_unified_offer(rig, tmp_path, monkeypatch):
    """Two 2 GiB caps fit the 104 GiB offer at once."""
    from prismabuild import adaptive_gpu as _gpu
    monkeypatch.setattr(_gpu, "action_contract",
                        lambda item, demand: ("shape", False, False, 2 * GIB))
    queue, now, sample, args = rig
    first, _, _ = _sealed(tmp_path, "small-first")
    _publish(queue, first, FIRST)
    assert queue.claim(**args)["action_key"] == first
    second, _, _ = _sealed(tmp_path, "small-second")
    _publish(queue, second, SECOND)
    _tick(rig)
    got = queue.claim(**args)
    assert got is not None and got["action_key"] == second


def test_discrete_host_ignores_unified_gpu_caps(rig, tmp_path, monkeypatch):
    """A discrete device keeps its VRAM budget; no unified refusal."""
    queue, now, sample, args = rig
    sample["devices"][0].update(
        memory_domain="discrete", memory_total_bytes=200 * GIB,
        memory_free_bytes=200 * GIB, memory_used_bytes=0)
    first, _, _ = _sealed(tmp_path, "discrete-first")
    _publish(queue, first, FIRST)
    assert queue.claim(**args)["action_key"] == first
    second, _, _ = _sealed(tmp_path, "discrete-second")
    _publish(queue, second, SECOND)
    _tick(rig)
    got = queue.claim(**args)
    assert got is not None and got["action_key"] == second
    assert got["gpu_admission"]["memory_domain"] == "discrete"


def test_cpu_only_candidate_charges_held_unified_gpu_caps(rig, tmp_path):
    """A CPU-only row refuses beside held GPU caps that fill the offer."""
    queue, now, sample, args = rig
    first, _, _ = _sealed(tmp_path, "gpu-first")
    _publish(queue, first, FIRST)
    assert queue.claim(**args)["action_key"] == first
    cpu_key, _, _ = _sealed(tmp_path, "cpu-second")
    queue.publish(action_key=cpu_key, cas_root="/cas", checkout_root="/co",
                  worker_script="/w.py", resources={"cpu": 2, "mem_gb": 6},
                  priority=-10, tags=["gb10"])
    _tick(rig)
    assert queue.claim(**args) is None
    denial = _denial(queue, cpu_key)
    assert denial["reason"] == "adaptive_cpu_refused"
    decision = denial["evidence"]["decision"]
    assert decision["reason"] == "unified_gpu_memory_budget"
    assert decision["requested_budget_gib"] == 0.0
    assert decision["requested_mem_gb"] == 6
    assert decision["held_gpu_cap_total_gib"] == GPU_CAP_GB
    assert decision["committed_gib"] == FIRST["mem_gb"] + GPU_CAP_GB
    assert decision["candidate_charge_gib"] == 6
    assert decision["mem_offer_gib"] == CAPACITY["mem_gb"]


def test_release_frees_the_unified_gpu_cap(rig, tmp_path):
    """The release path is the existing finish path; caps leave with it."""
    queue, now, sample, args = rig
    first, _, _ = _sealed(tmp_path, "rel-first")
    _publish(queue, first, FIRST)
    assert queue.claim(**args)["action_key"] == first
    queue.finish(first, status="executed", detail={})
    assert queue.ledger().held_keys() == []
    second, _, _ = _sealed(tmp_path, "rel-second")
    _publish(queue, second, SECOND)
    _tick(rig)
    assert queue.claim(**args)["action_key"] == second


def test_starting_reservation_charges_the_unified_gpu_cap(rig, tmp_path):
    """A private acquiring dir holds GPU metadata and blocks the next cap."""
    from prismabuild import adaptive_gpu as _gpu
    queue, now, sample, args = rig
    first, _, _ = _sealed(tmp_path, "starting-first")
    _publish(queue, first, FIRST)
    assert queue.claim(**args)["action_key"] == first
    ledger = queue.ledger()
    handle = ledger.begin_acquire(
        "f" * 64, SECOND, adaptive_gpu={
            "action_key": "f" * 64, "admitted_unix": now[0], "probe": True,
            "gpu_memory_budget_bytes": GPU_CAP_GB * GIB,
            "memory_domain": "shared_system"})
    assert handle is not None
    assert handle.startswith(pool.ACQUIRING_PREFIX)
    try:
        controller = _gpu.Controller(ledger)
        controller._sample = dict(sample)
        item = adaptive_cpu.read_json(
            queue.item_path(pool.CLAIMED, first))
        assert controller.decision(
            item, SECOND,
            contract=("shape", False, False, GPU_CAP_GB * GIB)) is None
        decision = controller.last_decision
        assert decision["reason"] == "unified_gpu_memory_budget"
        assert decision["held_gpu_cap_total_gib"] == 2 * GPU_CAP_GB
        assert decision["held_mem_gb"] == FIRST["mem_gb"] + SECOND["mem_gb"]
        assert decision["held_ram_mem_gb"] == 0.0
        assert decision["committed_gib"] == (
            FIRST["mem_gb"] + SECOND["mem_gb"] + 2 * GPU_CAP_GB)
        assert decision["candidate_charge_gib"] == SECOND["mem_gb"] + GPU_CAP_GB
        assert {entry["action_key"] for entry in decision["held_gpu_caps"]} == {
            first, handle}
    finally:
        ledger.abandon_acquire(handle)


def test_host_without_gpu_keeps_plain_token_admission(rig, tmp_path):
    """No GPU controller runs, so no unified charge can refuse."""
    queue, now, sample, args = rig
    first, _, _ = _sealed(tmp_path, "nogpu-first")
    queue.publish(action_key=first, cas_root="/cas", checkout_root="/co",
                  worker_script="/w.py", resources={"cpu": 2, "mem_gb": 60},
                  priority=-10, tags=["gb10"])
    plain = dict(args, has_gpu=False)
    assert queue.claim(**plain)["action_key"] == first
    cpu_key, _, _ = _sealed(tmp_path, "nogpu-second")
    queue.publish(action_key=cpu_key, cas_root="/cas", checkout_root="/co",
                  worker_script="/w.py", resources={"cpu": 2, "mem_gb": 40},
                  priority=-10, tags=["gb10"])
    _tick(rig)
    got = queue.claim(**plain)
    assert got is not None and got["action_key"] == cpu_key
    assert "gpu_admission" not in got


def test_first_cap_above_offer_refuses_on_an_empty_host(rig, tmp_path):
    """A 120 GiB cap cannot start on a 104 GiB offer, even first."""
    from prismabuild import adaptive_gpu as _gpu
    queue, now, sample, args = rig
    first, _, _ = _sealed(tmp_path, "oversize-first")
    _publish(queue, first, FIRST)
    queue.ledger().ensure_capacity(CAPACITY)
    controller = _gpu.Controller(queue.ledger())
    controller._sample = dict(sample)
    item = adaptive_cpu.read_json(queue.item_path(pool.READY, first))
    assert controller.decision(
        item, FIRST, contract=("shape", False, False, 120 * GIB)) is None
    decision = controller.last_decision
    assert decision["reason"] == "unified_gpu_memory_budget"
    assert decision["held_gpu_caps"] == []
    assert decision["held_gpu_cap_total_gib"] == 0.0
    assert decision["held_mem_gb"] == 0.0
    assert decision["candidate_charge_gib"] == FIRST["mem_gb"] + 120.0
    assert decision["mem_offer_gib"] == CAPACITY["mem_gb"]


def test_cpu_holder_memory_counts_toward_the_unified_charge(rig, tmp_path):
    """60 GiB of CPU-held memory leaves no room for a 98 GiB GPU cap."""
    queue, now, sample, args = rig
    cpu_key, _, _ = _sealed(tmp_path, "cpu-holder")
    queue.publish(action_key=cpu_key, cas_root="/cas", checkout_root="/co",
                  worker_script="/w.py", resources={"cpu": 2, "mem_gb": 60},
                  priority=-10, tags=["gb10"])
    assert queue.claim(**args)["action_key"] == cpu_key
    gpu_key, _, _ = _sealed(tmp_path, "gpu-after-cpu")
    _publish(queue, gpu_key, SECOND)
    _tick(rig)
    assert queue.claim(**args) is None
    denial = _denial(queue, gpu_key)
    assert denial["reason"] == "adaptive_gpu_refused"
    decision = denial["evidence"]["decision"]
    assert decision["reason"] == "unified_gpu_memory_budget"
    assert decision["held_gpu_caps"] == [
        {"action_key": cpu_key, "gpu_cap_gib": 0.0, "mem_gb": 60.0,
         "charge_gib": 60.0}]
    assert decision["held_gpu_cap_total_gib"] == 0.0
    assert decision["held_mem_gb"] == 60.0
    assert decision["committed_gib"] == 60.0
    assert decision["candidate_charge_gib"] == SECOND["mem_gb"] + GPU_CAP_GB


def test_candidate_mem_and_cap_charge_additively(rig, tmp_path, monkeypatch):
    """A 40 GiB mem demand with a 2 GiB cap charges 42, not 40."""
    from prismabuild import adaptive_gpu as _gpu
    monkeypatch.setattr(_gpu, "action_contract",
                        lambda item, demand: ("shape", False, False, 2 * GIB))
    queue, now, sample, args = rig
    first, _, _ = _sealed(tmp_path, "mem-heavy-first")
    _publish(queue, first, FIRST)
    assert queue.claim(**args)["action_key"] == first
    heavy, _, _ = _sealed(tmp_path, "mem-heavy-second")
    queue.publish(action_key=heavy, cas_root="/cas", checkout_root="/co",
                  worker_script="/w.py",
                  resources={"cpu": 2, "gpu": 1, "mem_gb": 40},
                  priority=-10, tags=["gb10"], needs_gpu=True)
    _tick(rig)
    got = queue.claim(**args)
    assert got is not None and got["action_key"] == heavy
    third, _, _ = _sealed(tmp_path, "mem-heavy-third")
    queue.publish(action_key=third, cas_root="/cas", checkout_root="/co",
                  worker_script="/w.py",
                  resources={"cpu": 2, "gpu": 1, "mem_gb": 60},
                  priority=-10, tags=["gb10"], needs_gpu=True)
    _tick(rig)
    assert queue.claim(**args) is None
    decision = _denial(queue, third)["evidence"]["decision"]
    assert decision["reason"] == "unified_gpu_memory_budget"
    assert decision["committed_gib"] == FIRST["mem_gb"] + 40 + 2 * 2
    assert decision["candidate_charge_gib"] == 60.0 + 2


def test_ram_fill_memory_counts_toward_the_unified_charge(rig, tmp_path):
    """40 GiB of RAM-fill memory leaves no room for a 98 GiB GPU cap."""
    queue, now, sample, args = rig
    queue.ledger().ensure_capacity(CAPACITY)
    state, detail = queue.hold_tier_host_memory(
        queue.ledger().host, "a" * 64, 40)
    assert (state, detail) == ("taken", "")
    gpu_key, _, _ = _sealed(tmp_path, "gpu-after-ram-fill")
    _publish(queue, gpu_key, SECOND)
    _tick(rig)
    assert queue.claim(**args) is None
    denial = _denial(queue, gpu_key)
    assert denial["reason"] == "adaptive_gpu_refused"
    decision = denial["evidence"]["decision"]
    assert decision["reason"] == "unified_gpu_memory_budget"
    assert decision["held_ram_mem_gb"] == 40.0
    assert decision["held_mem_gb"] == 40.0
    assert decision["committed_gib"] == 40.0
    assert decision["candidate_charge_gib"] == SECOND["mem_gb"] + GPU_CAP_GB
    queue.release_tier_host_memory(queue.ledger().host, "a" * 64)
    _tick(rig)
    assert queue.claim(**args)["action_key"] == gpu_key


def test_small_cpu_only_candidate_fits_beside_small_unified_caps(
        rig, tmp_path, monkeypatch):
    """A 6 GiB CPU row claims beside two 2 GiB caps on the 104 GiB offer."""
    from prismabuild import adaptive_gpu as _gpu
    monkeypatch.setattr(_gpu, "action_contract",
                        lambda item, demand: ("shape", False, False, 2 * GIB))
    queue, now, sample, args = rig
    first, _, _ = _sealed(tmp_path, "small-gpu-first")
    _publish(queue, first, FIRST)
    assert queue.claim(**args)["action_key"] == first
    second, _, _ = _sealed(tmp_path, "small-gpu-second")
    _publish(queue, second, SECOND)
    _tick(rig)
    assert queue.claim(**args)["action_key"] == second
    cpu_key, _, _ = _sealed(tmp_path, "small-cpu-third")
    queue.publish(action_key=cpu_key, cas_root="/cas", checkout_root="/co",
                  worker_script="/w.py", resources={"cpu": 2, "mem_gb": 6},
                  priority=-10, tags=["gb10"])
    _tick(rig)
    got = queue.claim(**args)
    assert got is not None and got["action_key"] == cpu_key
