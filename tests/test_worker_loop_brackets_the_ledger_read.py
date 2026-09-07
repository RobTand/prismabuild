"""A sibling loop's release must not be counted as both a token and free memory.

``observe`` adds the pool's held ``mem_gb`` tokens to ``MemAvailable``, because
a running action's bytes are missing from the instrument *and* reserved in the
ledger, and a clamp that ignored the reservation would charge the box twice for
its own work.  That addition is only honest while the two readings describe the
same instant.

They did not.  ``worker_loop`` read ``ledger().held()`` at the call boundary and
``observe`` read ``/proc/meminfo`` later, and every ending proves the payload
stopped *before* it returns the tokens -- ``cleanup_action_containers``
terminates, cleans up containers, samples, releases the scope, and only then
does the caller ``ledger.release``.  So there is a seconds-wide window in which
a **sibling** loop's memory is already free and its tokens are still held, and a
reading that straddles it counts those bytes twice.  Boxes here run 3-16 loops
against one ledger.

``rejoin`` does not cover it: it caps *this* observer after *this* loop's
action, and a loop is single-threaded, so its own release can never land inside
its own reading.
"""
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
SIBLING = "b" * 64

DECLARED_MEM_GB = 72
SIBLING_MEM_GB = 40
# What ``MemAvailable`` reads *after* the sibling's release: 22 GB that was free
# before it, plus the 40 GB it just gave back.
FREE_AFTER_RELEASE_GB = 62
HONEST = FREE_AFTER_RELEASE_GB - box_capacity.MEMORY_MARGIN_GB          # 54


def _gpu_sample():
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
        "host_available_bytes": FREE_AFTER_RELEASE_GB * GIB,
        "memory_pressure_some": 0.0, "memory_pressure_full": 0.0,
        "cpu_pressure_some": 0.0, "cpu_pressure_full": 0.0,
        "foreign_processes": [], "jobs": [],
    }


def _module():
    spec = importlib.util.spec_from_file_location("worker_bracket_test", WORKER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_a_sibling_release_inside_the_reading_is_not_counted_twice(tmp_path):
    host = socket.gethostname()
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    ledger = queue.ledger(host)
    ledger.ensure_capacity({"gpu": 1, "mem_gb": DECLARED_MEM_GB, "cpu": 10})
    assert ledger.acquire(SIBLING, {"mem_gb": SIBLING_MEM_GB})
    assert queue.ledger(host).held().get("mem_gb") == SIBLING_MEM_GB

    # The instrument *is* the race: the sibling finishes -- payload stopped,
    # tokens back -- while this loop is between its two readings.  Everything
    # after this line reads a box the sibling has already left.
    def meminfo():
        queue.ledger(host).release(SIBLING)
        return FREE_AFTER_RELEASE_GB

    worker = _module()
    argv = ["worker_loop.py", "--class", "gb10", "--gpu",
            "--mem-gb", str(DECLARED_MEM_GB), "--cpu-slots", "10",
            "--all-cores", "--poll-s", "0", "--observe-samples", "1", "--once"]
    with mock.patch.object(worker, "SH", tmp_path), \
         mock.patch.object(worker.cpu_topology, "pin_to_preferred", return_value=None), \
         mock.patch.object(worker.cpu_topology, "inherited_tiers", return_value=None), \
         mock.patch.object(worker, "loaded_runtime_commit", return_value="deadbeef"), \
         mock.patch.object(worker, "published_commit", return_value="deadbeef"), \
         mock.patch.object(worker.box_capacity, "trusted_gpu_sample",
                           return_value=_gpu_sample()), \
         mock.patch.object(worker.box_capacity, "mem_available_gb",
                           side_effect=meminfo), \
         mock.patch.object(worker.box_capacity, "run_queue", return_value=0.0), \
         mock.patch.object(sys, "argv", argv):
        assert worker.main() == 0

    offer = json.loads((queue.root / "workers" / f"{host}.json").read_text())
    # 54, not 72: the sibling's 40 GB is either a reservation this box still
    # holds or memory it has back, never both.  Before the bracket this read
    # ``40 + (62 - 8)``, saturated at the declaration, and the window's maximum
    # kept that offer standing for the polls in which this loop claims again.
    assert offer["observed_capacity"]["mem_gb"] == HONEST
    assert offer["observed_detail"]["held_moved_during_read"] == {
        "before": {"mem_gb": SIBLING_MEM_GB}, "after": {}}
    # The declaration is untouched -- observing a box is how an offer falls,
    # not how the configuration changes -- and the ledger is retired to what
    # was actually offered.
    assert offer["capacity"]["mem_gb"] == DECLARED_MEM_GB
    assert queue.ledger(host).capacity()["mem_gb"] == HONEST


def test_a_ledger_that_does_not_move_reads_the_same_either_way(tmp_path):
    """The bracket must cost nothing when nothing races it.

    Pinned because the fix's whole risk is on this side: taking the reading
    twice and keeping the minimum is only safe if a quiet box gives the same
    answer twice.  ``held`` is a filesystem scan, so "twice" is not free of
    meaning -- it is two scans, and this is what says they agree.
    """

    calls: list[int] = []

    def held():
        calls.append(1)
        return {"mem_gb": SIBLING_MEM_GB}

    with mock.patch.object(
            box_capacity, "mem_available_gb",
            # 22: the sibling is resident, so its 40 GB is missing from here.
            return_value=FREE_AFTER_RELEASE_GB - SIBLING_MEM_GB):
        seen = box_capacity.observe({"mem_gb": DECLARED_MEM_GB}, held)
    assert len(calls) == 2
    assert "held_moved_during_read" not in seen.detail
    # 40 held + (22 - 8) free = 54, the same box as above seen a moment earlier.
    assert seen.capacity["mem_gb"] == HONEST
