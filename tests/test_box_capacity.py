"""Capacity arithmetic from host memory and trusted GPU telemetry."""
from __future__ import annotations

import math
from pathlib import Path
import sys
import time
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import box_capacity as bc  # noqa: E402

GIB = 1024**3
DECLARED = {"cpu": 10, "gpu": 1, "mem_gb": 72}


def gpu_sample(*, foreign=(), jobs=(), age=0.0, domain="shared_system", devices=1):
    now = time.time()
    return {
        "schema": bc.GPU_CAPACITY_SCHEMA,
        "sample_id": "a" * 32,
        "sampled_unix": now - age,
        "published_unix": now,
        "complete": True,
        "attributed": True,
        "devices": [{
            "uuid": f"GPU-{index}",
            "memory_domain": domain,
            "memory_total_bytes": 120 * GIB,
            "memory_free_bytes": 80 * GIB,
            "memory_used_bytes": 40 * GIB,
        } for index in range(devices)],
        "host_total_bytes": 120 * GIB,
        "host_available_bytes": 80 * GIB,
        "memory_pressure_some": 0.0,
        "memory_pressure_full": 0.0,
        "cpu_pressure_some": 0.0,
        "cpu_pressure_full": 0.0,
        "foreign_processes": list(foreign),
        "jobs": list(jobs),
    }


def test_fresh_exact_gpu_evidence_reports_the_physical_device():
    seen = bc.observe(DECLARED, {}, gpu_sample=gpu_sample(), mem_gb=100, load1=0)

    assert seen.capacity == DECLARED
    assert seen.foreign == {}
    assert seen.detail["gpu_capacity_trusted"] is True
    assert seen.detail["physical_gpus"] == 1
    assert seen.detail["gpu_memory_domains"] == ["shared_system"]


def test_a_pool_job_with_many_processes_is_not_foreign():
    jobs = [{"action_key": "b" * 64, "nonce": "c" * 32,
             "complete": True, "processes": [10, 11, 12, 13]}]
    seen = bc.observe(DECLARED, {"gpu": 1}, gpu_sample=gpu_sample(jobs=jobs),
                      mem_gb=100, load1=0)

    assert seen.capacity["gpu"] == 1
    assert seen.foreign == {}
    assert seen.detail["gpu_attributed_jobs"] == 1


def test_foreign_gpu_inventory_closes_capacity_without_process_count_arithmetic():
    seen = bc.observe(DECLARED, {"gpu": 1},
                      gpu_sample=gpu_sample(foreign=[{"pid": 1}, {"pid": 2}]),
                      mem_gb=100, load1=0)

    assert seen.capacity["gpu"] == 0
    assert seen.foreign == {"gpu": 1}
    assert seen.detail["foreign_gpu_processes"] == 2


@pytest.mark.parametrize("mutate", [
    lambda sample: None,
    lambda sample: sample.update(complete=False),
    lambda sample: sample.update(attributed=False),
    lambda sample: sample.update(sampled_unix=time.time() - 6),
    lambda sample: sample["devices"][0].update(memory_domain="unknown"),
    lambda sample: sample["devices"][0].update(memory_free_bytes=121 * GIB),
    lambda sample: sample.update(foreign_processes=None),
])
def test_unknown_gpu_evidence_fails_closed(mutate):
    sample = gpu_sample()
    result = mutate(sample)
    if result is None and mutate.__code__.co_consts == (None,):
        # The first case represents a missing snapshot.
        sample = None
    seen = bc.observe(DECLARED, {}, gpu_sample=sample, mem_gb=100, load1=0)

    assert seen.capacity["gpu"] == 0
    assert seen.detail["gpu_capacity_trusted"] is False
    assert "gpu_capacity_error" in seen.detail


def test_physical_count_is_zero_for_untrusted_evidence_and_exact_when_trusted():
    assert bc.physical_gpu_count(None) == 0
    assert bc.physical_gpu_count(gpu_sample(devices=1)) == 1
    assert bc.physical_gpu_count(gpu_sample(devices=2)) == 2


def test_discrete_vram_does_not_reduce_the_separate_host_memory_offer():
    seen = bc.observe(DECLARED, {}, gpu_sample=gpu_sample(domain="discrete"),
                      mem_gb=60, load1=0)

    assert seen.capacity["gpu"] == 1
    assert seen.capacity["mem_gb"] == 52
    assert seen.detail["gpu_memory_free_bytes"] == 80 * GIB
    assert seen.detail["mem_available_gb"] == 60


def test_memory_is_bounded_by_physical_free_memory_minus_margin():
    seen = bc.observe({"mem_gb": 80}, {}, mem_gb=20, load1=0)
    assert seen.capacity == {"mem_gb": 12}
    assert seen.foreign == {"mem_gb": 68}


def test_memory_held_by_the_pool_is_not_charged_twice():
    seen = bc.observe({"mem_gb": 80}, {"mem_gb": 30}, mem_gb=20, load1=0)
    assert seen.capacity == {"mem_gb": 42}
    assert seen.foreign == {"mem_gb": 38}


def test_offer_never_exceeds_declaration():
    seen = bc.observe({"mem_gb": 40}, {}, mem_gb=900, load1=0)
    assert seen.capacity == {"mem_gb": 40}


def test_run_queue_is_recorded_but_not_charged():
    seen = bc.observe({"cpu": 80}, {"cpu": 24}, mem_gb=100, load1=371)
    assert seen.capacity["cpu"] == 80
    assert seen.foreign == {}
    assert seen.detail["load1"] == 371.0


def test_unknown_resource_passes_through():
    assert bc.observe({"nvme_gb": 400}, {}, mem_gb=100, load1=0).capacity == {
        "nvme_gb": 400}


def test_gpu_decision_is_immediate_while_memory_fall_remains_smoothed():
    observer = bc.CapacityObserver(samples=3)
    idle = gpu_sample()
    busy = gpu_sample(foreign=[{"pid": 9}])

    first = observer.offer(DECLARED, {}, gpu_sample=idle, mem_gb=100, load1=0)
    second = observer.offer(DECLARED, {}, gpu_sample=busy, mem_gb=20, load1=0)

    assert first["gpu"] == 1
    assert second["gpu"] == 0
    assert second["mem_gb"] == 72
    assert observer.offer(DECLARED, {}, gpu_sample=idle, mem_gb=20,
                          load1=0)["gpu"] == 1
    assert observer.offer(DECLARED, {}, gpu_sample=idle, mem_gb=20,
                          load1=0)["mem_gb"] == 12


def test_rejoin_caps_memory_but_does_not_override_current_gpu_safety():
    observer = bc.CapacityObserver(samples=3)
    observer.rejoin({"cpu": 10, "gpu": 1, "mem_gb": 40})
    offer = observer.offer(DECLARED, {}, gpu_sample=None, mem_gb=100, load1=0)

    assert offer["gpu"] == 0
    assert offer["mem_gb"] == 40


def test_invalid_sample_count_is_refused():
    with pytest.raises(ValueError):
        bc.CapacityObserver(samples=0)


def test_memavailable_reader_uses_kernel_field_and_gib_scale(tmp_path, monkeypatch):
    meminfo = tmp_path / "meminfo"
    meminfo.write_text(
        "MemTotal: 131072000 kB\nMemFree: 1048576 kB\n"
        "MemAvailable: 104857600 kB\nCached: 2097152 kB\n"
    )
    monkeypatch.setattr(bc, "MEMINFO", meminfo)
    assert bc.mem_available_gb() == 100


def test_trusted_reader_delegates_to_adaptive_gpu(monkeypatch):
    sample = gpu_sample()
    monkeypatch.setitem(sys.modules, "prismabuild.adaptive_gpu",
                        SimpleNamespace(trusted_sample=lambda: sample))
    assert bc.trusted_gpu_sample() is sample


def test_nonfinite_numeric_fields_fail_closed():
    sample = gpu_sample()
    sample["memory_pressure_some"] = math.inf
    assert bc.physical_gpu_count(sample) == 0
