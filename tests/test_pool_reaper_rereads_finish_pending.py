"""The reaper acts from the read it judged, not the one it guarded on.

``reap_stale`` reads ``claimed/<key>.json`` twice on the path that concludes a
claim: once for the ``finish_pending`` guard, and again for the record every
destructive step below hangs off -- container cleanup, the superseded filing,
the attempt archive, the requeue.  Only the first read was checked.

``finish`` publishes ``finish_pending`` by atomically *replacing* that file, so
a replace landing between the two reads is an ordinary claim to the guard and a
pending finish to nothing.  A completed action whose payload has already
returned is then reaped as ``lease_lost``, and a reaper on a foreign box runs
the container cleanup that the guard's owner-host rule exists to keep it away
from.

The window is real rather than theoretical: the guard deliberately precedes the
lease check, because a payload awaiting container cleanup has stopped
heartbeating, so an expired lease is that state's normal condition.  The lease
check between the two reads does not close it.  Issue #215.
"""
from __future__ import annotations

import json
from pathlib import Path
import socket
import sys
from unittest import mock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import pool  # noqa: E402

KEY = "b" * 64


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    return pool.PoolQueue(tmp_path / "queue")


def _publish(q: pool.PoolQueue, **kw: object) -> None:
    q.publish(
        action_key=KEY, cas_root=q.root / "cas", checkout_root=q.root / "co",
        worker_script=q.root / "worker.py", resources={"cpu": 1}, **kw,
    )


def _finish_pending_lands_during(q: pool.PoolQueue, *, host: str) -> dict:
    """Replace the claim with a pending finish between the two reads.

    ``lease_age`` is called after the guard's read and before the read the loop
    acts from, which is exactly the window ``finish``'s atomic replace lands
    in.  Patching it here reproduces that ordering deterministically instead of
    racing for it.
    """

    path = q.item_path(pool.CLAIMED, KEY)
    real = pool.PoolQueue.lease_age
    landed: dict[str, object] = {}

    def lease_age(self, action_key):          # type: ignore[no-untyped-def]
        age = real(self, action_key)
        if action_key == KEY and "replaced" not in landed:
            landed["replaced"] = True
            record = json.loads(path.read_text())
            record["claimed_host"] = host
            record["finish_pending"] = {
                "status": "executed",
                "detail": {"returncode": 0, "stdout": "", "stderr": ""},
            }
            pool._write_json_atomic(path, record)
        return age

    with mock.patch.object(pool.PoolQueue, "lease_age", lease_age):
        landed["reaped"] = q.reap_stale(timeout_s=-1)
    return landed


def test_a_finish_that_lands_inside_the_window_is_not_reaped(
    queue: pool.PoolQueue,
) -> None:
    """A foreign reaper leaves a pending finish alone, whenever it arrives."""

    _publish(queue, max_attempts=4, retry_safe=True)
    assert queue.claim(owner="attempt-1", capacity={"cpu": 1}) is not None
    path = queue.item_path(pool.CLAIMED, KEY)

    landed = _finish_pending_lands_during(queue, host="a-foreign-box")

    assert landed["reaped"] == []
    # The completed action's own result is still the live record.
    assert path.exists(), "a completed action's claim was reaped as lease_lost"
    assert json.loads(path.read_text()).get("finish_pending") is not None
    # Nothing was concluded on its behalf.
    assert list(queue.dir(pool.FAILED).glob("*.json")) == []
    assert list(queue.dir(pool.READY).glob("*.json")) == []
    assert list((queue.root / pool.ATTEMPTS).rglob("*.json")) == []


def test_the_owner_host_still_concludes_its_own_pending_finish(
    queue: pool.PoolQueue,
) -> None:
    """The guard's live path is unchanged: the owner completes the finish."""

    _publish(queue, max_attempts=4, retry_safe=True)
    assert queue.claim(owner="attempt-1", capacity={"cpu": 1}) is not None

    landed = _finish_pending_lands_during(queue, host=socket.gethostname())

    # Not this cycle -- the guard read before the replace -- but the record is
    # intact, so the next cycle's guard reads it and files the saved outcome.
    assert queue.item_path(pool.CLAIMED, KEY).exists()
    assert landed["reaped"] == []
    assert queue.reap_stale(timeout_s=-1) == []
    done = list(queue.dir(pool.DONE).glob("*.json"))
    assert [p.name for p in done] == [f"{KEY}.json"]
    assert json.loads(done[0].read_text())["status"] == "executed"


def test_an_ordinary_stale_claim_is_still_reaped(queue: pool.PoolQueue) -> None:
    """Nothing else changes: a claim with no pending finish still concludes."""

    _publish(queue, max_attempts=4, retry_safe=True)
    assert queue.claim(owner="attempt-1", capacity={"cpu": 1}) is not None

    assert queue.reap_stale(timeout_s=-1) == [KEY]

    assert not queue.item_path(pool.CLAIMED, KEY).exists()
    assert queue.item_path(pool.READY, KEY).exists()
