"""A withdrawal an operator can read, even after the action was retried.

``withdraw`` copies the live record into ``withdrawn/``. A record that a
requeue has already touched carries ``attempt_history``, and every reader of a
terminal record adopts the immutable attempt whenever that key is present.
The adopted attempt's disposition is ``requeued``, the directory is
``withdrawn``, and ``outcome_summary`` refuses the disagreement, so
``pbrun.await_outcome`` and ``pbwait`` raised ``PoolContractError`` instead of
reporting that somebody had cancelled the work.

A withdrawal is an operator verb, not an attempt. The links are kept under a
name of their own so the evidence survives without being read as this record's
outcome.
"""

from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

from prismabuild import pool  # noqa: E402
import pbrun  # noqa: E402
import pbstatus  # noqa: E402
import pbwait  # noqa: E402


KEY = "a" * 64


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    value = pool.PoolQueue(tmp_path / "queue")
    value.ensure_layout()
    value.publish(
        action_key=KEY,
        cas_root=str(tmp_path / "cas"),
        checkout_root=str(tmp_path / "checkout"),
        worker_script=str(tmp_path / "worker.py"),
        max_attempts=3,
        retry_safe=True,
    )
    return value


@pytest.fixture()
def withdrawn(queue: pool.PoolQueue) -> pool.PoolQueue:
    """One failed attempt, a requeue, a second claim, then an operator's verb."""

    assert queue.claim() is not None
    requeued = queue.finish(
        KEY,
        status="failed",
        detail={"returncode": 1, "stdout": "", "stderr": "boom\n"},
    )
    assert requeued == queue.item_path(pool.READY, KEY)
    ready = json.loads(requeued.read_text(encoding="utf-8"))
    assert "attempt_history" in ready, "the requeue is what puts the links here"
    assert queue.claim() is not None
    queue.withdraw(KEY, reason="changed my mind", by="rob", signal_child=False)
    return queue


def test_a_withdrawal_after_a_retry_is_readable(
    withdrawn: pool.PoolQueue, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The three readers of an ending all report the cancellation.

    main: ``outcome_summary`` returns ``withdrawn`` rather than refusing the
    record.
    branch: ``pbrun`` exits 143 and ``pbwait`` renders a row.
    """

    path = withdrawn.item_path(pool.WITHDRAWN, KEY)
    record = json.loads(path.read_text(encoding="utf-8"))

    summary = pbrun.outcome_summary(withdrawn, path, record)
    assert summary["status"] == "withdrawn"
    assert summary["withdrawn_by"] == "rob"
    assert summary["adopted"] is None

    monkeypatch.setattr(pbrun, "POLL_S", 0.001, raising=False)
    assert pbrun.await_outcome(withdrawn, KEY, wait_s=1.0) == pbrun.WITHDRAWN_EXIT
    assert pbwait._from_record(withdrawn, path, record)["status"] == "withdrawn"


def test_the_withdrawn_record_keeps_the_attempts_it_inherited(
    withdrawn: pool.PoolQueue,
) -> None:
    """Dropped from the adoption, not from the record.

    main: the links live under ``attempt_history_before_withdrawal`` and still
    resolve to the immutable outcome the failed attempt published.
    branch: the queue's own two readers of a withdrawal are unaffected.
    """

    path = withdrawn.item_path(pool.WITHDRAWN, KEY)
    record = json.loads(path.read_text(encoding="utf-8"))

    assert "attempt_history" not in record
    assert "attempt_history_missing_before" not in record
    inherited = record["attempt_history_before_withdrawal"]
    assert [link["attempt"] for link in inherited] == [1]
    outcomes = withdrawn.attempt_outcomes(
        {**record, "attempt_history": inherited, "attempts": 1}
    )
    assert outcomes[0]["status"] == "failed"
    assert outcomes[0]["stderr"] == "boom\n"

    assert withdrawn.withdrawal_covers(record, action_key=KEY) is not None
    endings = pbstatus.read_endings(withdrawn.root, limit=10)
    assert [row["status"] for row in endings if row["action_key"] == KEY] == [
        "withdrawn"
    ]
