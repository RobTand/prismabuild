"""The whole chain, driven the way the fleet drives it, on a real cgroup.

``tests/test_pool_memory_cap_binds.py`` proves the wrapper binds.  This proves
the *loop* uses it: a worker started the way ``supervise.py`` starts one, an
item published the way ``dispatch_tessera_shards.py`` publishes one, and a
payload that takes more than it declared.  What is being tested is the join --
that the declaration the ledger admitted is the figure the cgroup enforced, and
that the record left behind says so.

Skipped where the box cannot cap, which is the same condition the loop itself
degrades on.
"""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import socket
import sys
from unittest import mock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import pool  # noqa: E402

WORKER_LOOP = Path(__file__).resolve().parents[1] / "tools" / "fleet" / "worker_loop.py"

_SUPPORTED, _WHY = pool.memory_capping_supported()
pytestmark = pytest.mark.skipif(
    not _SUPPORTED, reason=f"this box cannot start a capped user unit: {_WHY}"
)

#: Faults in far more than any declaration below, in steps, so a cap that binds
#: stops it partway and a cap that does not lets it finish and say so.
GREEDY = '''
import sys
took = []
for i in range(48):
    block = bytearray(64 * 1024 * 1024)
    for j in range(0, len(block), 4096):
        block[j] = 1
    took.append(block)
    print("took", (i + 1) * 64, "MiB", flush=True)
print("FINISHED WITHOUT A KILL", flush=True)
'''


def _worker_loop():
    spec = importlib.util.spec_from_file_location("wl_cap_under_test", WORKER_LOOP)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _publish_greedy(tmp_path: Path, mem_gb: int) -> tuple[pool.PoolQueue, str]:
    script = tmp_path / "greedy_worker.py"
    script.write_text(GREEDY, encoding="utf-8")
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    key = "c" * 64
    queue.publish(
        action_key=key,
        cas_root=str(tmp_path / "cas"),
        checkout_root=str(tmp_path),
        worker_script=str(script),
        resources={"cpu": 1, "gpu": 0, "mem_gb": mem_gb},
        max_attempts=1,
    )
    return queue, key


def _run_one(tmp_path: Path, mem_gb: int):
    wl = _worker_loop()
    with mock.patch.object(wl, "SH", tmp_path), \
         mock.patch.object(wl.cpu_topology, "pin_to_preferred", return_value=None), \
         mock.patch.object(wl, "published_commit", return_value="deadbeef"), \
         mock.patch.object(sys, "argv", [
             "worker_loop.py", "--once", "--gpu-slots", "0",
             "--mem-gb", str(mem_gb), "--class", "test", "--all-cores"]):
        assert wl.main() == 0


def _outcome(queue: pool.PoolQueue, key: str) -> dict:
    for state in ("done", "failed"):
        path = queue.item_path(state, key)
        if path.exists():
            record = json.loads(path.read_text(encoding="utf-8"))
            record["_state"] = state
            return record
    raise AssertionError("the loop filed no outcome")


def test_the_loop_holds_an_action_to_the_figure_it_declared(tmp_path: Path) -> None:
    """One gigabyte declared, three taken, and the offender is the casualty."""

    queue, key = _publish_greedy(tmp_path, mem_gb=1)
    _run_one(tmp_path, mem_gb=8)
    record = _outcome(queue, key)
    detail = record["detail"]

    assert record["_state"] == "failed"
    assert detail["capped"] is True, "the loop must use the wrapper, not just own it"
    assert detail["cap_scope"] == "host"
    assert detail["declared_mem_gb"] == 1
    assert detail["oom_killed"] is True
    assert detail["returncode"] == -9
    assert detail["memory_peak_bytes"] == 1 << 30, "the cap, to the byte"
    # The payload's own account of how far it got, through ``--pipe``.
    assert "took 64 MiB" in detail["stdout"]
    assert "FINISHED WITHOUT A KILL" not in detail["stdout"]
    assert "declared 1 GB and exceeded it" in detail["stderr"]


def test_the_same_payload_under_a_sufficient_declaration_completes(
    tmp_path: Path,
) -> None:
    """Two treatments are not a control: the cap must be what changed.

    Same payload, same loop, same private root -- only the declared figure
    differs.  Without this the first test is equally consistent with a wrapper
    that kills everything.
    """

    queue, key = _publish_greedy(tmp_path, mem_gb=8)
    _run_one(tmp_path, mem_gb=8)
    record = _outcome(queue, key)
    detail = record["detail"]

    assert record["_state"] == "done"
    assert detail["capped"] is True and detail["declared_mem_gb"] == 8
    assert not detail.get("oom_killed")
    assert detail["returncode"] == 0
    assert "FINISHED WITHOUT A KILL" in detail["stdout"]


def test_the_worker_publishes_what_its_cap_charges(tmp_path: Path) -> None:
    """A submitter can see which boxes hold a declaration, and to what extent.

    ``"host"``, never a bare "yes": the same loop leaves a GPU action's device
    allocations uncharged, and a submitter reading "enforced" would size a
    declaration against a limit that is not there.
    """

    queue, _key = _publish_greedy(tmp_path, mem_gb=8)
    _run_one(tmp_path, mem_gb=8)
    offer = [o for o in queue.offers() if o["host"] == socket.gethostname()][0]
    assert offer["mem_cap_scope"] == "host"
