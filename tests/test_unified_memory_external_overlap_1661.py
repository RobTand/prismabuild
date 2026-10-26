"""GB10 foreign CUDA bytes are already inside host memory usage."""
import pytest

from prismabuild import box_capacity, gpu_sample_facts
from test_measurement_drains_gpu_backfill import fleet
from test_unified_memory_evidence_2088 import unified_sample

CAPACITY = {"cpu": 20, "gpu": 1, "mem_gb": 104}


def test_foreign_cpu_memory_bounds_an_unresident_pb_hold():
    seen = box_capacity.observe(
        CAPACITY, {"mem_gb": 40}, gpu_sample=unified_sample(available=68),
        mem_gb=68, load1=0, now=100.0)

    assert seen.capacity["mem_gb"] == 60


@pytest.mark.parametrize("available,admitted,ceiling", [(96, True, 88), (64, False, 56)])
def test_foreign_cpu_and_gpu_usage_share_one_physical_ceiling(
        fleet, available, admitted, ceiling):
    queue, _clock, _readings, sample, publish, _tick, *_rest, denial = fleet
    ledger = queue.ledger()
    ledger.ensure_capacity(CAPACITY)
    assert ledger.acquire("a" * 64, {"mem_gb": 40})
    sample.update(host_total_bytes=128 << 30, host_available_bytes=available << 30,
                  cpu_pressure_full=0.0, jobs=[])
    sample["foreign_processes"] = [
        {"pid": 9101, "gpu_uuid": "GPU-1", "used_bytes": 32 << 30}]
    key = publish("foreign-external-union", gpu=0, mem_gb=40, tags=["gb10"])

    claimed = queue.claim(
        capacity=CAPACITY, has_gpu=True, tags=["gb10", "sparklina"],
        gpu_sampler=lambda: sample)

    if admitted:
        assert claimed is not None and claimed["action_key"] == key
        assert ledger.held()["mem_gb"] == 80
    else:
        assert claimed is None
        assert ledger.held()["mem_gb"] == 40
        decision = denial(key)["evidence"]["decision"]
        assert decision["reason"] == "unified_gpu_memory_budget"
        assert decision["mem_ceiling_gib"] == ceiling


@pytest.mark.parametrize("fault", ["missing", "partial", "unattributed", "stale"])
def test_unclassifiable_sample_cannot_reuse_clear_evidence_in_a_claim(fleet, fault):
    queue, clock, _readings, sample, publish, _tick, *_rest, denial = fleet
    sample["cpu_pressure_full"] = 0.0
    seed = gpu_sample_facts.unified_memory_evidence(sample, now=clock.now, max_age_s=5)
    assert seed is not None and not seed.unknown
    key = publish("lost-unified-evidence", gpu=0, mem_gb=4, tags=["gb10"])
    if fault == "partial":
        sample["complete"] = False
    elif fault == "unattributed":
        sample["attributed"] = False
    elif fault == "stale":
        clock.now += box_capacity.GPU_SAMPLE_MAX_AGE_S + 1

    claimed = queue.claim(
        capacity=CAPACITY, has_gpu=True, tags=["gb10", "sparklina"],
        gpu_sampler=lambda: None if fault == "missing" else sample,
        unified_memory=seed)

    assert claimed is None
    assert queue.ledger().held_keys() == []
    assert denial(key)["reason"] == "unified_gpu_memory_unknown"


def test_resident_bytes_above_the_reservation_are_not_credited():
    job_a = {"complete": True, "host_bytes": 30 << 30, "budget_bytes": 40 << 30,
             "memory_domain": "shared_system", "gpu_lower_bound_bytes": 40 << 30}
    seen = box_capacity.observe(
        {"cpu": 8, "gpu": 1, "mem_gb": 120}, {"mem_gb": 80},
        gpu_sample=unified_sample(jobs=[job_a], available=54),
        mem_gb=54, load1=0, now=100.0)

    assert seen.capacity["mem_gb"] <= 86
    assert seen.unified_memory.resident_bytes == 40 << 30


def test_overrunning_resident_scope_denies_a_new_pool_commitment(fleet):
    queue, _clock, _readings, sample, publish, _tick, *_rest, denial = fleet
    capacity = {"cpu": 20, "gpu": 1, "mem_gb": 120}
    ledger = queue.ledger()
    ledger.ensure_capacity(capacity)
    assert ledger.acquire("a" * 64, {"mem_gb": 40})
    assert ledger.acquire("c" * 64, {"mem_gb": 40})
    sample.update(host_total_bytes=128 << 30, host_available_bytes=54 << 30,
                  cpu_pressure_full=0.0, jobs=[{
                      "complete": True, "host_bytes": 30 << 30,
                      "budget_bytes": 40 << 30, "memory_domain": "shared_system",
                      "gpu_lower_bound_bytes": 40 << 30}])
    key = publish("overrunning-resident-scope", gpu=0, mem_gb=8, tags=["gb10"])

    claimed = queue.claim(
        capacity=capacity, has_gpu=True, tags=["gb10", "sparklina"],
        gpu_sampler=lambda: sample)

    assert claimed is None
    assert ledger.held()["mem_gb"] == 80
    decision = denial(key)["evidence"]["decision"]
    assert decision["reason"] == "unified_gpu_memory_budget"
    assert decision["mem_ceiling_gib"] == 86
    assert decision["pb_resident_bytes"] == 40 << 30
