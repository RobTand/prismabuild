"""The cap against a real kernel, on whatever box is running the suite.

Everything in ``test_pool_memory_cap.py`` is about the argv and the bookkeeping
and runs anywhere.  This file asks the only question those cannot: does the
limit actually bind?  It needs a real transient user unit, so it skips whole
where the box cannot start one -- which is the same capability the worker
probes, exercised through the same function.

Scope: what these tests establish is that the cap binds a **host** allocation
-- ``bytearray`` faulted in page by page -- on the box running the suite.
Whether a GB10's cgroup also charges memory taken through the CUDA allocator
is a separate question with a separate measurement; see
``docs/memory_enforcement_2026-09-04.md``.  Nothing here answers it, and
nothing here should be read as answering it.
"""
from __future__ import annotations

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import pool  # noqa: E402

KEY_A = "a" * 64

_SUPPORTED, _WHY = pool.memory_capping_supported()
pytestmark = pytest.mark.skipif(
    not _SUPPORTED, reason=f"this box cannot start a capped user unit: {_WHY}"
)


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    q = pool.PoolQueue(tmp_path / "pb-queue")
    q.ensure_layout()
    return q


def _publish(q: pool.PoolQueue, stub: Path, mem_gb: int) -> None:
    q.publish(
        action_key=KEY_A, cas_root="/cas", checkout_root="/co",
        worker_script=str(stub), resources={"mem_gb": mem_gb}, max_attempts=1,
    )


def test_an_action_that_exceeds_its_own_declaration_is_killed(
    queue, tmp_path
) -> None:
    stub = tmp_path / "greedy_worker.py"
    stub.write_text(
        "held = []\n"
        "for _ in range(32):\n"
        "    block = bytearray(128 * 1024 * 1024)\n"
        "    block[::4096] = b'\\x01' * len(block[::4096])\n"
        "    held.append(block)\n"
        "print('SURVIVED')\n"
    )
    _publish(queue, stub, mem_gb=1)
    outcome = queue.execute(queue.claim())
    assert outcome["capped"] is True
    assert outcome["oom_killed"] is True, outcome
    assert outcome["status"] == "failed"
    assert outcome["returncode"] == -9
    assert "SURVIVED" not in (outcome["stdout"] or "")


def test_an_action_inside_its_declaration_finishes_normally(
    queue, tmp_path
) -> None:
    """The cap must bound the offender and nothing else, including this."""

    stub = tmp_path / "modest_worker.py"
    stub.write_text(
        "block = bytearray(64 * 1024 * 1024)\n"
        "block[::4096] = b'\\x01' * len(block[::4096])\n"
        "print('SURVIVED')\n"
    )
    _publish(queue, stub, mem_gb=1)
    outcome = queue.execute(queue.claim())
    assert outcome["capped"] is True
    assert outcome["status"] == "executed" and outcome["returncode"] == 0
    assert outcome["oom_killed"] is False
    assert "SURVIVED" in outcome["stdout"]


def test_stdout_and_stderr_still_reach_the_outcome_record(queue, tmp_path) -> None:
    """``--pipe`` rather than a journal file, because the record is made of these.

    The worker loop prints the last six stderr lines of a failure; a wrapper
    that sent them to the journal would leave the fleet's only error surface
    empty.
    """

    stub = tmp_path / "chatty_worker.py"
    stub.write_text("import sys\nprint('to-out')\nprint('to-err', file=sys.stderr)\n")
    _publish(queue, stub, mem_gb=1)
    outcome = queue.execute(queue.claim())
    assert "to-out" in outcome["stdout"] and "to-err" in outcome["stderr"]


def test_the_timeout_still_bounds_a_wedged_capped_action(queue, tmp_path) -> None:
    """Killing ``systemd-run`` does not stop the service it started.

    Under ``--pipe`` the service holds the pipe this call is about to read, so
    a plain kill would leave the timeout bounding nothing and block until the
    work ended by itself.  The unit is stopped; the launcher then exits.
    """

    stub = tmp_path / "hang_worker.py"
    stub.write_text("import time; time.sleep(300)\n")
    _publish(queue, stub, mem_gb=1)
    item = queue.claim()
    started = pool._now()
    outcome = queue.execute(item, heartbeat_s=0.2, timeout_s=1.0)
    assert outcome["status"] == "timeout"
    assert outcome["capped"] is True
    assert pool._now() - started < 60.0, "the timeout did not bound the child"


def test_a_fat_grandchild_is_still_the_actions_own_kill(queue, tmp_path) -> None:
    """The shape the fleet actually runs: the payload is not the main process.

    ``worker_argv`` makes ``prismabuild_worker.py run-local`` the unit's main
    process and the real work its child, so the kernel's victim is a
    grandchild.  Under the default ``OOMPolicy=stop`` that reaches the caller
    as SIGTERM -- systemd stopping a unit whose child died -- and "terminated"
    is not what happened.  ``OOMPolicy=kill`` takes the tree together, so the
    status is SIGKILL and the action, not one process of it, is what failed.
    """

    fat = tmp_path / "fat_child.py"
    fat.write_text(
        "held = []\n"
        "for _ in range(32):\n"
        "    block = bytearray(128 * 1024 * 1024)\n"
        "    block[::4096] = b'\\x01' * len(block[::4096])\n"
        "    held.append(block)\n"
        "print('SURVIVED')\n"
    )
    stub = tmp_path / "parent_worker.py"
    stub.write_text(
        "import subprocess, sys\n"
        f"print('parent alive', flush=True)\n"
        f"sys.exit(subprocess.run([sys.executable, {str(fat)!r}]).returncode)\n"
    )
    _publish(queue, stub, mem_gb=1)
    outcome = queue.execute(queue.claim())
    assert outcome["oom_killed"] is True, outcome
    assert outcome["returncode"] == -9, "SIGTERM here would mean the tree survived"
    assert "parent alive" in (outcome["stdout"] or "")
    assert "SURVIVED" not in (outcome["stdout"] or "")


def test_a_cap_reclaims_page_cache_rather_than_killing_a_reader(
    queue, tmp_path
) -> None:
    """A cap is a ceiling on *anonymous* memory and a throttle on file I/O.

    Measured 2026-09-04: a 1 GiB cap reading a cold 3 GiB file charged
    ``file`` 1068630016, hit ``memory.events max`` 8211 times, killed nothing,
    and finished.  So a streaming action under a small declaration is not
    stopped -- it re-reads, which on an NFS input is the "loud kill becomes a
    slow box" failure arriving by the other door.  Asserted here so that if a
    future kernel starts killing these instead, the change is caught by a test
    rather than by a shard campaign.
    """

    payload = tmp_path / "cold.bin"
    with payload.open("wb") as fh:
        fh.write(b"\0" * (1536 * 1024 * 1024))
    stub = tmp_path / "reader_worker.py"
    stub.write_text(
        "import os\n"
        f"fd = os.open({str(payload)!r}, os.O_RDONLY)\n"
        "os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)\n"
        "read = 0\n"
        "while True:\n"
        "    chunk = os.read(fd, 8 << 20)\n"
        "    if not chunk:\n"
        "        break\n"
        "    read += len(chunk)\n"
        "print('READ', read)\n"
    )
    _publish(queue, stub, mem_gb=1)
    outcome = queue.execute(queue.claim())
    assert outcome["status"] == "executed", outcome
    assert outcome["oom_killed"] is False
    assert "READ 1610612736" in outcome["stdout"]
