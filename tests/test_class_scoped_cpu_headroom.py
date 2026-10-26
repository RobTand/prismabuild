"""Independent admission-boundary and READY-GPU continuity probes."""
import json

import pytest

from prismabuild import adaptive_cpu, adaptive_gpu, pool
from gpu_capacity_fixture import idle_gpu_sample
from test_class_scoped_cpu_admission import (
    class_claim_host,
    _claim_class_action,
    _class_denial,
    _class_gpu_lock_busy,
    _class_metadata,
    _publish_class_action,
)


@pytest.mark.parametrize("adaptive", [False, True])
@pytest.mark.parametrize("cpu", [1, 2, 4, 5])
@pytest.mark.parametrize("mem", [8, 16, 17])
def test_bounded_class_claim_preserves_exact_cpu_and_memory_room(
    class_claim_host, monkeypatch, adaptive, cpu, mem,
):
    host = class_claim_host
    monkeypatch.setattr(adaptive_gpu.Controller, "sample", lambda self: idle_gpu_sample())
    cpu_key = _publish_class_action(host, "candidate", {"cpu": cpu, "mem_gb": mem})
    gpu_key = _publish_class_action(host, "gpu", {"cpu": 4, "gpu": 1, "mem_gb": 16})
    candidate_cost = cpu * 1.25 if adaptive else cpu
    fits = candidate_cost + 4 <= 8 and mem + 16 <= 32
    with _class_gpu_lock_busy(host, gpu_key):
        claimed = _claim_class_action(host, adaptive=adaptive)
    assert bool(claimed) is fits
    if fits:
        assert claimed["action_key"] == cpu_key
        assert not claimed.get("cpu_guest")
        assert host.queue.ledger().held() == {"cpu": cpu, "mem_gb": mem}
    else:
        assert host.queue.ledger().held() == {}
    claimed = _claim_class_action(host, adaptive=adaptive)
    assert claimed is not None and claimed["action_key"] == gpu_key
    held = host.queue.ledger().held()
    assert held["cpu"] <= 8 and held["mem_gb"] <= 32 and held["gpu"] == 1


def _set_priority(host, key, priority):
    path = host.queue.records.item_path(pool.READY, key)
    row = json.loads(path.read_text())
    row["priority"] = priority
    path.write_text(json.dumps(row))


def _fully_busy_two_cpu_holder(host, key):
    admitted = _class_metadata(host, key)["admitted_unix"]
    host.now += 2.0
    host.busy = 2.0
    current = {"action_key": key, "complete": True, "nonce": key,
               "sampled_unix": host.now, "wall_seconds": 2.0, "cpu_seconds": 4.0}
    base = adaptive_cpu.local_state_base(host.queue.ledger().base)
    adaptive_cpu.write_json(base / "telemetry" / f"{key}.json", current)
    adaptive_cpu.write_json(base / "jobs.json", {key: dict(
        current, sampled_unix=admitted, wall_seconds=0.0, cpu_seconds=0.0)})


def test_waiting_gpu_runs_within_one_class_cpu_row_duration(class_claim_host, monkeypatch):
    host = class_claim_host
    monkeypatch.setattr(adaptive_gpu.Controller, "sample", lambda self: idle_gpu_sample())
    first = _publish_class_action(host, "first", {"cpu": 2, "mem_gb": 2})
    gpu = _publish_class_action(host, "gpu", {"cpu": 6, "gpu": 1, "mem_gb": 16})
    with _class_gpu_lock_busy(host, gpu):
        claimed = _claim_class_action(host)
    if claimed is None:
        assert _claim_class_action(host)["action_key"] == gpu
        return
    assert claimed["action_key"] == first
    assert not claimed.get("cpu_guest")
    current = first
    for index in range(2):
        _fully_busy_two_cpu_holder(host, current)
        assert _claim_class_action(host) is None
        denied = _class_denial(host, gpu)
        assert denied["evidence"]["decision"]["reason"] == "projected_cpu_cost"
        assert denied["evidence"]["decision"]["active_cpu_cost"] == 2.5
        assert denied["evidence"]["drain_resolves"] is True
        refill = _publish_class_action(host, f"refill-{index}", {"cpu": 2, "mem_gb": 2})
        _set_priority(host, refill, 20)
        assert _claim_class_action(host) is None
        host.queue.finish(current, status="executed")
        host.now += 0.01
        host.busy = 0.0
        claimed = _claim_class_action(host)
        assert claimed is not None
        if claimed["action_key"] == gpu:
            return
        assert claimed["action_key"] == refill
        assert not claimed.get("cpu_guest")
        current = refill
    pytest.fail("GPU stayed READY across two completed class CPU rows and their replacements")


def test_complete_tag_conjunction_identifies_class_scope():
    offers = [
        {"host": "cpu-arm", "has_gpu": False, "tags": ["arm64"]},
        {"host": "cpu-fast", "has_gpu": False, "tags": ["fast"]},
        {"host": "gpu", "has_gpu": True, "tags": ["arm64", "fast"]},
    ]
    row = {"tags": ["arm64", "fast"], "resources": {"cpu": 1, "mem_gb": 1}}
    assert pool.PoolQueue._excluded_from_cpu_hosts(row, offers)
    assert not pool.PoolQueue._excluded_from_cpu_hosts(
        row, [*offers, {"host": "cpu-both", "has_gpu": False,
                       "tags": ["arm64", "fast"]}])
    assert not pool.PoolQueue._excluded_from_cpu_hosts(
        row, [*offers[:2], {"host": "gpu-arm", "has_gpu": True, "tags": ["arm64"]},
              {"host": "gpu-fast", "has_gpu": True, "tags": ["fast"]}])


@pytest.mark.parametrize("funded", [False, True])
def test_class_headroom_reuses_one_holder_pricing(class_claim_host, monkeypatch, funded):
    from test_class_scoped_cpu_admission import _class_holder

    host = class_claim_host
    holder = _class_holder(host, slots=2 if funded else None)
    candidate = _publish_class_action(
        host, "candidate", {"cpu": 1, "mem_gb": 1}, owner=holder if funded else None)
    gpu = _publish_class_action(host, "gpu", {"cpu": 4, "gpu": 1, "mem_gb": 16})
    telemetry_reads = []
    read = adaptive_cpu.read_json

    def count_read(path):
        if path.name == f"{holder}.json" and path.parent.name == "telemetry":
            telemetry_reads.append(path)
        return read(path)

    monkeypatch.setattr(adaptive_cpu, "read_json", count_read)
    with _class_gpu_lock_busy(host, gpu):
        claimed = _claim_class_action(host)
    assert claimed is not None and claimed["action_key"] == candidate
    assert len(telemetry_reads) == 1
