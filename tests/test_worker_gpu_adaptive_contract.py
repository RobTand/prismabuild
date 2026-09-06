"""Worker-side contracts for physical and adaptive GPU capacity."""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys
import time
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import box_capacity  # noqa: E402


WORKER = Path(__file__).resolve().parents[1] / "tools/fleet/worker_loop.py"


def _sample(*, foreign=(), domain="shared_system", age=0.0):
    now = time.time()
    return {
        "schema": "prismabuild.gpu_capacity.v1",
        "host": "testbox",
        "sample_id": "a" * 32,
        "sampled_unix": now - age,
        "published_unix": now,
        "complete": True,
        "attributed": True,
        "devices": [{
            "uuid": "GPU-physical-0",
            "memory_domain": domain,
            "memory_total_bytes": 120 * 1024**3,
            "memory_free_bytes": 80 * 1024**3,
            "memory_used_bytes": 40 * 1024**3,
            "power_w": 18.0,
            "power_limit_w": None,
            "power_reference_w": 140.0,
            "power_reference_scope": "soc_tdp",
            "limited": False,
        }],
        "host_total_bytes": 120 * 1024**3,
        "host_available_bytes": 80 * 1024**3,
        "memory_pressure_some": 0.0,
        "memory_pressure_full": 0.0,
        "cpu_pressure_some": 0.0,
        "cpu_pressure_full": 0.0,
        "foreign_processes": list(foreign),
        "jobs": [],
    }


def test_exact_attribution_does_not_charge_a_multiprocess_pool_job_twice():
    sample = _sample()
    sample["jobs"] = [{
        "action_key": "b" * 64,
        "nonce": "c" * 32,
        "complete": True,
        "processes": [101, 102, 103, 104],
    }]

    seen = box_capacity.observe(
        {"gpu": 1, "mem_gb": 72}, {"gpu": 1, "mem_gb": 16},
        gpu_sample=sample, mem_gb=80, load1=0.0,
    )

    assert seen.capacity["gpu"] == 1
    assert seen.foreign == {}
    assert seen.detail["gpu_attributed_jobs"] == 1


def test_any_exact_foreign_gpu_work_closes_the_single_physical_device():
    seen = box_capacity.observe(
        {"gpu": 1}, {}, gpu_sample=_sample(foreign=[{"pid": 901}, {"pid": 902}]),
        mem_gb=100, load1=0.0,
    )

    assert seen.capacity["gpu"] == 0
    assert seen.foreign == {"gpu": 1}
    assert seen.detail["foreign_gpu_processes"] == 2


def test_missing_incomplete_stale_and_unknown_domain_gpu_evidence_fail_closed():
    bad = []
    bad.append(None)
    incomplete = _sample()
    incomplete["complete"] = False
    bad.append(incomplete)
    bad.append(_sample(age=6.0))
    bad.append(_sample(domain="unknown"))

    for sample in bad:
        seen = box_capacity.observe(
            {"gpu": 1}, {}, gpu_sample=sample, mem_gb=100, load1=0.0,
        )
        assert seen.capacity["gpu"] == 0
        assert seen.detail["gpu_capacity_trusted"] is False


def test_discrete_vram_is_recorded_separately_from_host_memory_capacity():
    seen = box_capacity.observe(
        {"gpu": 1, "mem_gb": 72}, {}, gpu_sample=_sample(domain="discrete"),
        mem_gb=60, load1=0.0,
    )

    assert seen.capacity == {"gpu": 1, "mem_gb": 52}
    assert seen.detail["gpu_memory_domains"] == ["discrete"]
    assert seen.detail["gpu_memory_free_bytes"] == 80 * 1024**3
    assert seen.detail["mem_available_gb"] == 60


def test_live_gb10_shapes_use_one_autodetected_physical_gpu_policy():
    config = json.loads(
        (Path(__file__).resolve().parents[1] / "tools/fleet/fleet_boxes.json").read_text()
    )["boxes"]

    for host in ("sparky", "gx10-6b77"):
        args = config[host]["args"]
        assert "--gpu" in args
        assert "--gpu-slots" not in args
    assert "--gpu" not in config["dl380g10"]["args"]
    assert "--gpu-slots" not in config["dl380g10"]["args"]


class _Ledger:
    def capacity(self):
        return {}

    def held(self):
        return {}

    def retire_free_capacity(self, capacity):
        return dict(capacity)

    def available(self):
        return {}


class _Queue:
    def __init__(self, *, ready):
        self.ready = ready
        self.census_calls = 0
        self.ledger_value = _Ledger()
        self.announcements = []

    def ledger(self):
        return self.ledger_value

    def announce(self, **value):
        self.announcements.append(value)

    def serve_once(self, **_kwargs):
        return None

    def placement_census(self):
        self.census_calls += 1
        return {
            "ready": self.ready, "offers": 1, "known": True,
            "unplaceable": 0, "one_box": self.ready, "one_box_by_path": 0,
            "wide": 0, "unreadable": 0, "pinned_to": {"testbox": self.ready},
        }


def _worker():
    spec = importlib.util.spec_from_file_location("adaptive_gpu_worker", WORKER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _run_worker(argv, *, ready, sample=None):
    worker = _worker()
    queue = _Queue(ready=ready)
    sleeps = []
    with mock.patch.object(worker.pool, "PoolQueue", return_value=queue), \
         mock.patch.object(worker.socket, "gethostname", return_value="testbox"), \
         mock.patch.object(worker.cpu_topology, "pin_to_preferred", return_value=None), \
         mock.patch.object(worker.cpu_topology, "inherited_tiers", return_value=None), \
         mock.patch.object(worker, "inherited_cpus", return_value=4), \
         mock.patch.object(worker, "published_commit", return_value="deadbeef"), \
         mock.patch.object(worker, "loaded_runtime_commit", return_value="deadbeef"), \
         mock.patch.object(worker, "maintenance_requested", return_value=False), \
         mock.patch.object(worker.box_capacity, "trusted_gpu_sample", return_value=sample,
                           create=True), \
         mock.patch.object(worker.time, "sleep", side_effect=lambda seconds: sleeps.append(seconds)), \
         mock.patch.object(sys, "argv", ["worker_loop.py", *argv]):
        assert worker.main() == 0
    return queue, sleeps


def test_worker_default_has_no_implicit_four_gpu_capacity():
    queue, _ = _run_worker(
        ["--once", "--assume-idle", "--all-cores"], ready=0,
    )

    assert queue.announcements[-1]["has_gpu"] is False
    assert queue.announcements[-1]["capacity"]["gpu"] == 0


def test_worker_autodetects_one_physical_gpu_from_the_trusted_snapshot():
    queue, _ = _run_worker(
        ["--once", "--gpu", "--all-cores"], ready=0, sample=_sample(),
    )

    assert queue.announcements[-1]["has_gpu"] is True
    assert queue.announcements[-1]["capacity"]["gpu"] == 1
    assert queue.announcements[-1]["observed_capacity"]["gpu"] == 1


def test_worker_rechecks_a_nonempty_queue_at_one_second_cadence():
    queue, sleeps = _run_worker(
        ["--assume-idle", "--all-cores", "--max-idle", "2", "--poll-s", "15"],
        ready=1,
    )

    assert sleeps == [1.0]
    assert queue.census_calls == 2


def test_worker_backs_off_to_configured_cadence_when_queue_is_empty():
    queue, sleeps = _run_worker(
        ["--assume-idle", "--all-cores", "--max-idle", "2", "--poll-s", "15"],
        ready=0,
    )

    assert sleeps == [15.0]
    assert queue.census_calls == 2
