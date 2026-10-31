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


def _pending_declared_holder(host, declared_cpu):
    """Model pending demand separately from the one physical CPU token."""
    holder = _publish_class_action(host, "pending", {"cpu": 1, "mem_gb": 1})
    assert _claim_class_action(host)["action_key"] == holder
    metadata = _class_metadata(host, holder)
    metadata["declared_cpu"] = declared_cpu
    adaptive_cpu.write_json(
        host.queue.ledger().held_dir / holder / adaptive_cpu.METADATA, metadata)
    assert host.queue.ledger().holder_tokens(holder) == {"cpu": 1, "mem_gb": 1}
    return holder


@pytest.mark.parametrize(("declared_cpu", "admitted"), [(1, True), (3, False)])
def test_cpu_admission_counts_pending_declared_demand(
    class_claim_host, monkeypatch, declared_cpu, admitted,
):
    host = class_claim_host
    monkeypatch.setattr(adaptive_gpu.Controller, "sample", lambda self: idle_gpu_sample())
    holder = _pending_declared_holder(host, declared_cpu)
    gpu = _publish_class_action(host, "gpu", {"cpu": 6, "gpu": 1, "mem_gb": 16})
    claimed = _claim_class_action(host)
    if admitted:
        assert claimed is not None and claimed["action_key"] == gpu
        assert host.queue.ledger().held() == {"cpu": 7, "gpu": 1, "mem_gb": 17}
    else:
        assert claimed is None
        decision = _class_denial(host, gpu)["evidence"]["decision"]
        assert decision["reason"] == "projected_cpu_cost"
        assert decision["active_cpu_cost"] == 3.0
        assert decision["pending_cpu_cost"] == 3.0
        assert decision["requested_cpu_cost"] == 6.0
        assert host.queue.ledger().held_keys() == [holder]
        assert host.queue.records.item_path(pool.READY, gpu).exists()


@pytest.mark.parametrize("declared_cpu", [1, 3])
@pytest.mark.parametrize(("tags", "scope"), [
    (["gb10", "arm64"], "class"),
    (["sparky"], "hostname"),
    ([], "portable"),
])
def test_ready_gpu_exemption_counts_pending_declared_demand(
    class_claim_host, monkeypatch, declared_cpu, tags, scope,
):
    host = class_claim_host
    monkeypatch.setattr(adaptive_gpu.Controller, "sample", lambda self: idle_gpu_sample())
    holder = _pending_declared_holder(host, declared_cpu)
    candidate = _publish_class_action(
        host, "candidate", {"cpu": 1, "mem_gb": 1}, tags=tags)
    gpu = _publish_class_action(host, "gpu", {"cpu": 4, "gpu": 1, "mem_gb": 16})
    with _class_gpu_lock_busy(host, gpu):
        claimed = _claim_class_action(host)
    admitted = scope == "hostname" or (scope == "class" and declared_cpu == 1)
    if admitted:
        assert claimed is not None and claimed["action_key"] == candidate
        assert not claimed.get("cpu_guest")
        assert host.queue.ledger().holder_tokens(candidate) == {"cpu": 1, "mem_gb": 1}
    else:
        assert claimed is None
        denial = _class_denial(host, candidate)
        if scope == "portable":
            assert denial["reason"] == "deferred_for_ready_gpu"
        else:
            assert denial["reason"] == "deferred_for_ready_gpu_row"
            evidence = denial["evidence"]
            assert evidence["kept_for"] == "class_scoped_cpu_projected_cpu_cost"
            assert evidence["active_cpu_cost"] == 4.25
            assert evidence["pending_cpu_cost"] == 4.25
            assert evidence["holder_cost"] == 1.25
            assert evidence["gpu_cost"] == 4.0
        assert host.queue.ledger().holder_tokens(candidate) == {}
        assert host.queue.ledger().held_keys() == [holder]
    assert host.queue.records.item_path(pool.READY, gpu).exists()
    assert host.queue.passes(candidate) == 0


@pytest.mark.parametrize("exemption", [False, True], ids=["admission", "exemption"])
def test_admission_and_exemption_obey_shared_projection(
    class_claim_host, monkeypatch, exemption,
):
    host = class_claim_host
    monkeypatch.setattr(adaptive_gpu.Controller, "sample", lambda self: idle_gpu_sample())
    holder = _pending_declared_holder(host, 1)
    if exemption:
        candidate = _publish_class_action(host, "candidate", {"cpu": 1, "mem_gb": 1})
    gpu = _publish_class_action(host, "gpu", {"cpu": 4, "gpu": 1, "mem_gb": 16})
    projection = adaptive_cpu.projected_cpu_fits

    def deny_gpu_projection(**costs):
        return costs["requested_cpu_cost"] != 4.0 and projection(**costs)

    monkeypatch.setattr(adaptive_cpu, "projected_cpu_fits", deny_gpu_projection)
    if exemption:
        with _class_gpu_lock_busy(host, gpu):
            assert _claim_class_action(host) is None
        denial = _class_denial(host, candidate)
        assert denial["reason"] == "deferred_for_ready_gpu_row"
        assert denial["evidence"]["kept_for"] == "class_scoped_cpu_projected_cpu_cost"
        assert denial["evidence"]["decision"]["reason"] == "admitted"
    else:
        assert _claim_class_action(host) is None
        decision = _class_denial(host, gpu)["evidence"]["decision"]
        assert decision["reason"] == "projected_cpu_cost"
        assert decision["requested_cpu_cost"] == 4.0
    assert host.queue.ledger().held_keys() == [holder]
    assert host.queue.records.item_path(pool.READY, gpu).exists()
