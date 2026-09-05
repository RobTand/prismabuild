"""A loop that holds a claim is busy, even before it has a child.

``supervise._is_idle`` read childlessness alone: a loop that has claimed an
action but has not yet started the launcher has no child, so it read as idle
and was SIGTERMed by ``cycle_stale`` or by the declared-shape sweep. The claim
then sat in ``claimed/`` until ``reap_stale`` timed the lease out
``LEASE_TIMEOUT_S`` later, and the action ran again from the start.

The window is not microseconds wide. ``PoolQueue.claim`` returns as soon as
the rename lands, and ``execute`` then materializes a sealed checkout, which
means unpacking a bundle of up to 512 MiB out of the CAS, before it reaches
``Popen``.

The fact the supervisor was missing already exists: ``claim`` writes the
lease, carrying the claiming loop's own host and pid, before it returns, and
``finish`` unlinks it. Asking the queue who holds a claim on this box is
therefore exact, where the process tree is a guess.
"""

from __future__ import annotations

import json
from pathlib import Path
import socket
import subprocess
import sys

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))

from prismabuild import pool  # noqa: E402
import supervise  # noqa: E402

KEY = "e" * 64


@pytest.fixture()
def childless(tmp_path: Path):
    """A live process of our own with no children, killed at teardown."""

    process = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(30)"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    try:
        yield process
    finally:
        process.kill()
        process.wait()


def _queue(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> pool.PoolQueue:
    """A queue at the root the supervisor asks about, which is its mirror's."""

    monkeypatch.setattr(supervise, "MIRROR", tmp_path / "fleet")
    queue = pool.PoolQueue(tmp_path / "fleet" / "pb-queue")
    queue.ensure_layout()
    return queue


def _hold_a_claim(queue: pool.PoolQueue, pid: int, *, host: str) -> None:
    """The lease ``claim`` writes, for a loop that has not spawned anything."""

    queue.item_path(pool.CLAIMED, KEY).write_text(
        json.dumps({"action_key": KEY, "claimed_host": host}), encoding="utf-8"
    )
    queue.lease_path(KEY).write_text(json.dumps({
        "schema": pool.POOL_LEASE_SCHEMA_V1,
        "action_key": KEY,
        "owner": f"{host}:{pid}:abcdef12",
        "host": host,
        "pid": pid,
        "child_pid": None,
        "heartbeat_unix": 0.0,
    }), encoding="utf-8")


def test_a_loop_holding_a_claim_is_not_idle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, childless: subprocess.Popen
) -> None:
    """The window between ``claim`` and ``Popen``.

    main: the loop has no child and a lease naming it, and reads as busy.
    branch: the same loop with no lease reads as idle, so the fix does not
    simply refuse to stop anything.
    """

    queue = _queue(tmp_path, monkeypatch)
    assert supervise._is_idle(childless.pid) is True

    _hold_a_claim(queue, childless.pid, host=socket.gethostname())
    assert supervise._is_idle(childless.pid) is False


def test_a_claim_held_on_another_box_does_not_hold_this_loop(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, childless: subprocess.Popen
) -> None:
    """A pid is only a name until it is qualified by a host.

    branch: the queue is shared, so another box's lease can carry a pid this
    box also hands out. Reading it as this loop's claim would leave a drained
    box unable to cycle.
    """

    queue = _queue(tmp_path, monkeypatch)
    _hold_a_claim(queue, childless.pid, host="some-other-box")
    assert supervise._is_idle(childless.pid) is True


def test_a_queue_that_is_not_there_leaves_the_process_tree_deciding(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, childless: subprocess.Popen
) -> None:
    """A box with no queue root still cycles.

    branch: the pull queue retires at cutover, and an absent queue is the
    answer "nobody holds a claim here", not a reason to stop cycling loops.
    """

    monkeypatch.setattr(supervise, "MIRROR", tmp_path / "no-such-mirror")
    assert supervise._is_idle(childless.pid) is True
