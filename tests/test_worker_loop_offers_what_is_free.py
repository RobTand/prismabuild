"""A real worker loop publishes the trusted physical GPU remainder."""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import socket
import sys
import time
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import box_capacity, pool  # noqa: E402

WORKER = Path(__file__).resolve().parents[1] / "tools/fleet/worker_loop.py"
GIB = 1024**3
BASE = ["--class", "gb10", "--gpu", "--mem-gb", "72",
        "--cpu-slots", "10", "--all-cores", "--poll-s", "0"]


def sample(*, foreign=(), jobs=(), complete=True):
    return {
        "schema": box_capacity.GPU_CAPACITY_SCHEMA,
        "sample_id": "d" * 32,
        "sampled_unix": time.time(),
        "published_unix": time.time(),
        "complete": complete,
        "attributed": True,
        "devices": [{
            "uuid": "GPU-one", "memory_domain": "shared_system",
            "memory_total_bytes": 120 * GIB,
            "memory_free_bytes": 100 * GIB,
            "memory_used_bytes": 20 * GIB,
        }],
        "host_total_bytes": 120 * GIB,
        "host_available_bytes": 100 * GIB,
        "memory_pressure_some": 0.0, "memory_pressure_full": 0.0,
        "cpu_pressure_some": 0.0, "cpu_pressure_full": 0.0,
        "foreign_processes": list(foreign), "jobs": list(jobs),
    }


def _module():
    spec = importlib.util.spec_from_file_location("worker_offer_test", WORKER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _run(tmp_path: Path, argv, *, gpu_sample, mem_gb=100):
    worker = _module()
    with mock.patch.object(worker, "SH", tmp_path), \
         mock.patch.object(worker.cpu_topology, "pin_to_preferred", return_value=None), \
         mock.patch.object(worker.cpu_topology, "inherited_tiers", return_value=None), \
         mock.patch.object(worker, "loaded_runtime_commit", return_value="deadbeef"), \
         mock.patch.object(worker, "published_commit", return_value="deadbeef"), \
         mock.patch.object(worker.box_capacity, "trusted_gpu_sample", return_value=gpu_sample), \
         mock.patch.object(worker.box_capacity, "mem_available_gb", return_value=mem_gb), \
         mock.patch.object(worker.box_capacity, "run_queue", return_value=0.0), \
         mock.patch.object(sys, "argv", ["worker_loop.py", *argv]):
        assert worker.main() == 0
    return pool.PoolQueue(tmp_path / "pb-queue")


def _offer(queue, host):
    return json.loads((queue.root / "workers" / f"{host}.json").read_text())


def test_fresh_snapshot_announces_one_physical_gpu_on_a_gb10(tmp_path):
    host = socket.gethostname()
    queue = _run(tmp_path, [*BASE, "--once"], gpu_sample=sample())
    offer = _offer(queue, host)

    assert offer["has_gpu"] is True
    assert offer["capacity"]["gpu"] == 1
    assert offer["observed_capacity"]["gpu"] == 1
    assert queue.ledger(host).capacity()["gpu"] == 1


def test_foreign_gpu_work_immediately_retires_the_free_physical_token(tmp_path):
    host = socket.gethostname()
    queue = _run(tmp_path, [*BASE, "--once"],
                 gpu_sample=sample(foreign=[{"pid": 794915}, {"pid": 805673}]))
    offer = _offer(queue, host)

    assert offer["capacity"]["gpu"] == 1
    assert offer["observed_capacity"]["gpu"] == 0
    assert offer["foreign"] == {"gpu": 1}
    assert offer["observed_detail"]["foreign_gpu_processes"] == 2
    assert queue.ledger(host).capacity().get("gpu", 0) == 0


def test_pool_job_process_count_never_becomes_foreign_gpu_demand(tmp_path):
    host = socket.gethostname()
    jobs = [{"action_key": "a" * 64, "nonce": "b" * 32,
             "complete": True, "processes": list(range(20))}]
    queue = _run(tmp_path, [*BASE, "--once"], gpu_sample=sample(jobs=jobs))

    assert _offer(queue, host)["observed_capacity"]["gpu"] == 1
    assert _offer(queue, host)["foreign"] == {}


def test_missing_gpu_snapshot_is_capability_without_free_capacity(tmp_path):
    host = socket.gethostname()
    queue = _run(tmp_path, [*BASE, "--once"], gpu_sample=None)
    offer = _offer(queue, host)

    assert offer["has_gpu"] is True
    assert offer["capacity"]["gpu"] == 1
    assert offer["observed_capacity"]["gpu"] == 0
    assert queue.ledger(host).capacity().get("gpu", 0) == 0


def test_busy_gpu_box_remains_a_queueable_capability(tmp_path):
    queue = _run(tmp_path, [*BASE, "--once"],
                 gpu_sample=sample(foreign=[{"pid": 9}]))

    # The worker's physical declaration remains one while observed admission
    # is zero. A submitted action waits instead of being called impossible.
    assert queue.placeable({"tags": [], "resources": {"gpu": 1}}) is True


def test_host_memory_budget_remains_separate_and_hard(tmp_path):
    host = socket.gethostname()
    queue = _run(tmp_path, [*BASE, "--once"], gpu_sample=sample(), mem_gb=20)

    assert _offer(queue, host)["observed_capacity"]["mem_gb"] == 72
    # One reading cannot lower the smoothed host-memory offer. Three sustained
    # readings are covered directly by CapacityObserver tests.


def test_legacy_positive_gpu_slots_normalize_to_one_physical_token(tmp_path):
    host = socket.gethostname()
    legacy = ["--class", "gb10", "--gpu-slots", "3", "--mem-gb", "72",
              "--cpu-slots", "10", "--all-cores", "--poll-s", "0",
              "--once", "--assume-idle"]
    queue = _run(tmp_path, legacy, gpu_sample=None)

    assert _offer(queue, host)["capacity"]["gpu"] == 1
    assert queue.ledger(host).capacity()["gpu"] == 1
