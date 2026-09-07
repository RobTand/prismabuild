"""What a preemption looks like to the people and tools waiting on it.

#364 stops an admitted background holder and re-publishes it.  That requeue is
necessarily a NEW generation -- the cancellation it revives is generation-scoped
and would otherwise cover its own retry -- and both readers of the queue are
generation-aware:

* ``pbrun`` waits on the generation it submitted, so without this it reads the
  cancellation, exits 143, and reports work the queue is in the middle of
  running again as decided against.  "Retried, not lost" would then be true of
  the queue and false of everyone waiting on it.
* ``pbstatus`` builds fixed rows and drops fields it does not name, so
  ``preempted_by`` sitting in the record is not the same thing as an operator
  being able to see it -- which is what the issue asks for.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys
import uuid

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import pool  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import pbrun  # noqa: E402
import pbstatus  # noqa: E402

FOREGROUND = uuid.uuid4().hex + uuid.uuid4().hex
BACKGROUND = uuid.uuid4().hex + uuid.uuid4().hex


@pytest.fixture()
def preempted(tmp_path: Path):
    """A queue where a foreground denial has just preempted the holder."""

    q = pool.PoolQueue(tmp_path / "pb-queue")
    q.ensure_layout()
    q.ledger().ensure_capacity({"gpu": 1})

    q.publish(action_key=BACKGROUND, cas_root="/cas", checkout_root="/co",
              worker_script="/w.py", priority=-10, resources={"gpu": 1})
    holder = q.claim(capacity={"gpu": 1})
    assert holder is not None and holder["action_key"] == BACKGROUND

    q.publish(action_key=FOREGROUND, cas_root="/cas", checkout_root="/co",
              worker_script="/w.py", priority=0, resources={"gpu": 1})
    assert q.claim(capacity={"gpu": 1}) is None

    stopped = float(holder["published_unix"])
    requeued = json.loads(q.item_path(pool.READY, BACKGROUND).read_text())
    assert float(requeued["published_unix"]) != stopped
    return q, holder, stopped


def test_a_waiter_follows_the_preemption_to_the_generation_it_requeued(
    preempted,
) -> None:
    """The stopped generation is not an ending while its retry is queued."""

    q, holder, stopped = preempted

    # Nothing has ended: the retry is waiting to run.
    assert pbrun.landed_outcome(q, BACKGROUND, wait_s=0.0,
                                generation=stopped) is None

    # The holder concludes the stop; the foreground item it yielded to runs.
    q.finish(BACKGROUND, status="withdrawn", detail={"returncode": -15},
             claim_snapshot=holder)
    first = q.claim(capacity={"gpu": 1})
    assert first is not None and first["action_key"] == FOREGROUND
    q.finish(FOREGROUND, status="executed", detail={"returncode": 0},
             claim_snapshot=first)

    # And then the retry runs and succeeds.  A waiter that named the stopped
    # generation is told about that ending, not about the stop.
    retry = q.claim(capacity={"gpu": 1})
    assert retry is not None and retry["action_key"] == BACKGROUND
    q.finish(BACKGROUND, status="executed", detail={"returncode": 0},
             claim_snapshot=retry)

    landed = pbrun.landed_outcome(q, BACKGROUND, wait_s=0.0,
                                  generation=stopped)
    assert landed is not None
    assert landed[1]["status"] == "executed"
    assert float(landed[1]["published_unix"]) == float(retry["published_unix"])


def test_an_operator_withdrawal_still_ends_the_wait(tmp_path: Path) -> None:
    """Only a preemption is followed.  A decision is still a decision."""

    q = pool.PoolQueue(tmp_path / "pb-queue")
    q.ensure_layout()
    q.publish(action_key=BACKGROUND, cas_root="/cas", checkout_root="/co",
              worker_script="/w.py", priority=-10)
    generation = float(json.loads(
        q.item_path(pool.READY, BACKGROUND).read_text())["published_unix"])
    q.withdraw(BACKGROUND, reason="an operator changed their mind",
               by="an operator")

    landed = pbrun.landed_outcome(q, BACKGROUND, wait_s=0.0,
                                  generation=generation)
    assert landed is not None
    assert landed[1]["status"] == "withdrawn"
    assert landed[1].get("preempted_by") is None


def test_pbstatus_names_the_preemption_on_the_ending_and_on_the_requeue(
    preempted,
) -> None:
    """The cost is readable in the tables, not only in the record."""

    q, _, _ = preempted

    endings = pbstatus.read_endings(q.root)
    withdrawn = [row for row in endings if row["status"] == "withdrawn"]
    assert len(withdrawn) == 1
    assert withdrawn[0]["preempted_by"] == FOREGROUND
    assert f"preempted by {FOREGROUND[:12]}" in "\n".join(
        pbstatus.ending_lines(endings))

    pool_state = pbstatus.read_pool(q.root)
    rows = [row for row in pool_state["jobs"]
            if row["action_key"] == BACKGROUND and row["state"] == "READY"]
    assert len(rows) == 1
    assert rows[0]["preempted_by"] == FOREGROUND
    assert f"preemption by {FOREGROUND[:12]}" in str(rows[0]["reason"])
