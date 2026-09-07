"""How many loops a box is running, recorded where a reader can find it.

Every ``worker_loop`` on a box writes the same ``workers/<host>.json``, so the
offer said *that the box was offering* and never *how many loops were*.  The
count had to be recovered by hand on 2026-09-06, one ``ps`` per box, because
no series carried it (#254).

The load-bearing property here is that the number is a **census** and not a
running total.  A count each loop contributed to could only grow, and a record
that outlives what it describes is the bug (#244), not the fix.
"""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import socket
import sys
import time
from unittest import mock

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))

from prismabuild import box_capacity, pool  # noqa: E402
import pbmetrics  # noqa: E402
import pbstatus  # noqa: E402

WORKER = REPOSITORY / "tools/fleet/worker_loop.py"
GIB = 1024**3
BASE = ["--class", "gb10", "--gpu", "--mem-gb", "72",
        "--cpu-slots", "10", "--all-cores", "--poll-s", "0", "--once"]
LOOP = "/mnt/shared/prismabuild-fleet/runtime-generations/a98c/tools/worker_loop.py"


def _proc(root: Path, pid: int, argv: list[str]) -> None:
    """One process in a fake ``/proc``, exactly as the kernel presents one."""

    entry = root / str(pid)
    entry.mkdir(parents=True)
    (entry / "cmdline").write_bytes(b"\0".join(a.encode() for a in argv) + b"\0")


def _sample() -> dict:
    return {
        "schema": box_capacity.GPU_CAPACITY_SCHEMA,
        "sample_id": "d" * 32,
        "sampled_unix": time.time(),
        "published_unix": time.time(),
        "complete": True,
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
        "foreign_processes": [], "jobs": [],
    }


def _run(tmp_path: Path, *, loops) -> pool.PoolQueue:
    """One real poll of a real worker loop, with the census answering ``loops``."""

    spec = importlib.util.spec_from_file_location("worker_loop_count_test", WORKER)
    worker = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(worker)
    census = (mock.Mock(side_effect=OSError) if loops is None else
              mock.Mock(return_value=tuple((100 + i, (LOOP,)) for i in range(loops))))
    with mock.patch.object(worker, "SH", tmp_path), \
         mock.patch.object(worker.cpu_topology, "pin_to_preferred", return_value=None), \
         mock.patch.object(worker.cpu_topology, "inherited_tiers", return_value=None), \
         mock.patch.object(worker, "loaded_runtime_commit", return_value="deadbeef"), \
         mock.patch.object(worker, "published_commit", return_value="deadbeef"), \
         mock.patch.object(worker.box_capacity, "worker_loops", census), \
         mock.patch.object(worker.box_capacity, "trusted_gpu_sample", return_value=_sample()), \
         mock.patch.object(worker.box_capacity, "mem_available_gb", return_value=100), \
         mock.patch.object(worker.box_capacity, "run_queue", return_value=0.0), \
         mock.patch.object(sys, "argv", ["worker_loop.py", *BASE]):
        assert worker.main() == 0
    return pool.PoolQueue(tmp_path / "pb-queue")


def _offer(queue: pool.PoolQueue, host: str) -> dict:
    return json.loads((queue.root / "workers" / f"{host}.json").read_text())


# --- the census itself ------------------------------------------------------

def test_the_census_counts_the_loops_and_only_the_loops(tmp_path):
    _proc(tmp_path, 100, ["/usr/bin/python3", LOOP, "--class", "gb10"])
    _proc(tmp_path, 101, ["/usr/bin/python3.12", LOOP, "--class", "gb10"])
    # Three near misses, each of which a laxer rule would count.
    _proc(tmp_path, 102, ["/usr/bin/python3", "/opt/other/worker_loop.pyc"])
    _proc(tmp_path, 103, ["/bin/bash", LOOP])
    _proc(tmp_path, 104, ["/usr/bin/python3"])

    assert [pid for pid, _ in box_capacity.worker_loops(proc=tmp_path)] == [100, 101]


def test_a_loop_that_exits_is_gone_from_the_next_reading(tmp_path):
    for pid in (100, 101, 102):
        _proc(tmp_path, pid, ["/usr/bin/python3", LOOP])
    assert len(box_capacity.worker_loops(proc=tmp_path)) == 3

    # The whole point of a census: nothing has to notice the exit for the
    # count to fall.  An accumulated count could only ever have gone up.
    for path in (tmp_path / "101").iterdir():
        path.unlink()
    (tmp_path / "101").rmdir()

    assert len(box_capacity.worker_loops(proc=tmp_path)) == 2


def test_a_process_that_exits_mid_walk_is_skipped_not_raised(tmp_path):
    _proc(tmp_path, 100, ["/usr/bin/python3", LOOP])
    _proc(tmp_path, 101, ["/usr/bin/python3", LOOP])
    (tmp_path / "101" / "cmdline").unlink()          # gone between the two reads

    assert [pid for pid, _ in box_capacity.worker_loops(proc=tmp_path)] == [100]


def test_the_census_and_the_activation_gate_share_one_rule():
    """The publish gate and the offer must not disagree about what a loop is."""

    spec = importlib.util.spec_from_file_location(
        "census_shares_the_rule", REPOSITORY / "tools/fleet/runtime_process_census.py")
    census = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(census)

    assert census.box_capacity.worker_loops is box_capacity.worker_loops


# --- the offer --------------------------------------------------------------

def test_a_real_worker_announces_the_count_it_censused(tmp_path):
    queue = _run(tmp_path, loops=4)

    assert _offer(queue, socket.gethostname())["loops"] == 4


def test_an_unreadable_proc_announces_no_count_rather_than_zero(tmp_path):
    queue = _run(tmp_path, loops=None)
    offer = _offer(queue, socket.gethostname())

    # Zero loops is a claim the box cannot make about itself -- something is
    # running to write the record -- so the field is absent instead.
    assert "loops" not in offer
    assert offer["capacity"]["gpu"] == 1          # the rest of the offer stands


def test_announce_omits_the_field_when_nobody_counted(tmp_path):
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.announce(host="sparky", tags=["sparky"], has_gpu=False)

    assert "loops" not in _offer(queue, "sparky")


# --- the readers ------------------------------------------------------------

def _queue_with_offers(tmp_path: Path) -> Path:
    root = tmp_path / "pb-queue"
    for name in ("ready", "claimed", "done", "failed", "withdrawn", "workers"):
        (root / name).mkdir(parents=True)
    for host, extra in (("sparky", {"loops": 5}), ("dl380", {})):
        record = {
            "schema": pool.POOL_OFFER_SCHEMA_V1, "host": host,
            "announced_unix": time.time(), "tags": [host], "has_gpu": False,
            "capacity": {"cpu": 8, "gpu": 0, "mem_gb": 32},
            "observed_capacity": {"cpu": 8, "gpu": 0, "mem_gb": 32},
            "foreign": {}, "observed_detail": {}, **extra,
        }
        path = root / "workers" / f"{host}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(record), encoding="utf-8")
    return root


def test_status_carries_the_count_and_marks_an_uncounted_box_unknown(tmp_path):
    root = _queue_with_offers(tmp_path)
    nodes = {node["node"]: node for node in pbstatus.read_pool(root)["nodes"]}

    assert nodes["sparky"]["loops"] == 5
    assert nodes["dl380"]["loops"] is None
    lines = pbstatus.pool_node_lines(list(nodes.values()))
    assert "LOOPS" in lines[0]
    assert [line for line in lines if line.startswith("dl380")][0].count(pbstatus.ABSENT)


def test_metrics_export_the_count_without_inventing_one(tmp_path):
    root = _queue_with_offers(tmp_path)
    text = pbmetrics.collect_metrics(root)

    assert 'prismabuild_worker_loops{host="sparky"} 5' in text
    # An offer from a generation that predates the field is a missing series,
    # not a zero one, and above all not a failed scrape.
    assert 'prismabuild_worker_loops{host="dl380"}' not in text
    assert "prismabuild_collection_success 1" in text
