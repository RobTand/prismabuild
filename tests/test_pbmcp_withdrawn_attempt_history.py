"""A withdrawn action's preserved execution is readable, and never adopted.

``pool.withdraw`` moves the links a requeue wrote to
``attempt_history_before_withdrawal`` so no reader of the terminal record
adopts an inherited attempt as the withdrawal's ending.  ``pbmcp`` read only
``attempt_history``, so the real attempt behind the withdrawal -- the DL380
preflight failure in #790, exit 1 after 0.76s -- vanished from ``pb_action``
and the action read as though it had never run.

The tools must surface the preserved attempts and their logs while keeping
``withdrawn`` as the current state, must not adopt the preserved execution as
this record's ending, and must say when retained evidence is missing or
corrupt instead of answering an empty list.
"""

from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))

from prismabuild import pool  # noqa: E402
import pbmcp  # noqa: E402

KEY = "9" * 64
DONE_KEY = "7" * 64
STDOUT = "preflight refused\n"
STDERR = "ActionContractError: live checkout identity differs\n"


def _session(queue: pool.PoolQueue, base: Path) -> pbmcp.Session:
    return pbmcp.Session(queue_root=queue.root, cas_root=base / "cas",
                         repo_link=base / "repo")


def _publish(queue: pool.PoolQueue, base: Path, key: str, **extra: object) -> None:
    queue.publish(
        action_key=key,
        cas_root=str(base / "cas"),
        checkout_root=str(base / "checkout"),
        worker_script=str(base / "worker.py"),
        max_attempts=3,
        retry_safe=True,
        **extra,
    )


@pytest.fixture()
def withdrawn(tmp_path: Path) -> tuple[pool.PoolQueue, pbmcp.Session]:
    """One failed attempt, a requeue, then an operator's verb on the ready row.

    Withdrawing from ``ready`` is the live #790 shape
    (``withdrawn_from: ready``): a claimed row keeps its live record until the
    holder stops, so a withdrawal from ``claimed`` still reads as claimed.
    """

    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    _publish(queue, tmp_path, KEY)
    assert queue.claim() is not None
    requeued = queue.finish(
        KEY, status="failed",
        detail={"returncode": 1, "elapsed_s": 0.759, "status": "failed",
                "stdout": STDOUT, "stderr": STDERR})
    assert requeued == queue.item_path(pool.READY, KEY)
    queue.withdraw(KEY, reason="producer timed out", by="rob",
                   signal_child=False)
    return queue, _session(queue, tmp_path)


@pytest.fixture()
def done(tmp_path: Path) -> tuple[pool.PoolQueue, pbmcp.Session]:
    """One action that finished, so the ordinary adoption path stays covered."""

    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    _publish(queue, tmp_path, DONE_KEY)
    assert queue.claim() is not None
    queue.finish(DONE_KEY, status="executed",
                 detail={"returncode": 0, "elapsed_s": 1.5, "status": "executed",
                         "stdout": "ok\n", "stderr": ""})
    return queue, _session(queue, tmp_path)


def _record(queue: pool.PoolQueue, key: str = KEY) -> dict:
    return json.loads(
        queue.item_path(pool.WITHDRAWN, key).read_text(encoding="utf-8"))


def _rewrite(queue: pool.PoolQueue, record: dict, key: str = KEY) -> None:
    queue.item_path(pool.WITHDRAWN, key).write_text(
        json.dumps(record), encoding="utf-8")


def test_pb_action_surfaces_the_execution_a_withdrawal_preserved(
    withdrawn: tuple[pool.PoolQueue, pbmcp.Session],
) -> None:
    """The defect: ``attempts_detail`` was empty and the action read never run.

    main: the preserved links under ``attempt_history_before_withdrawal`` are
    read exactly as a record's own links are, so the failed attempt and its
    returncode are visible again.
    """

    _queue, session = withdrawn
    body = session.call("pb_action", {"key_prefix": KEY[:12], "tail_lines": 2})

    assert body["complete"] is True
    assert body["state"] == "withdrawn"
    assert body["outcome"]["status"] == "withdrawn"
    assert body["attempts_detail"], (
        "the preserved attempt is the evidence #790 was missing")
    attempt = body["attempts_detail"][0]
    assert attempt["attempt"] == 1
    assert attempt["status"] == "failed"
    assert attempt["disposition"] == "requeued"
    assert attempt["detail"]["returncode"] == 1
    history = body["attempts_history"]
    assert history["source"] == "attempt_history_before_withdrawal"
    assert history["before_withdrawal"] is True
    assert history["missing_before"] == 0
    assert history["recorded_attempts"] == 1
    assert history["unretained_attempts"] == []
    assert history["problems"] == []


def test_the_withdrawal_does_not_adopt_the_preserved_execution(
    withdrawn: tuple[pool.PoolQueue, pbmcp.Session],
) -> None:
    """Historical execution is evidence, not this record's ending.

    main: ``adopted_attempt`` stays null and the preserved detail is reported
    under a name that says where it came from, so no reader concludes the
    withdrawn action completed because an earlier attempt ran.
    """

    _queue, session = withdrawn
    body = session.call("pb_action", {"key_prefix": KEY[:12], "tail_lines": 2})

    assert body["adopted_attempt"] is None
    assert body["preserved_attempt"] == 1
    assert body["outcome"]["returncode"] is None
    preserved = body["outcome_before_withdrawal"]
    assert preserved["returncode"] == 1
    assert preserved["elapsed_s"] == 0.759
    assert preserved["status"] == "failed"
    assert preserved["preserved_by"] == "withdrawal"

    tail = body["log_tail"]
    assert tail["attempt"] == 1
    assert tail["before_withdrawal"] is True
    assert tail["stream"] == "stdout"
    assert tail["lines"] == [STDOUT.rstrip("\n")]


def test_pb_log_reads_the_preserved_attempts_log(
    withdrawn: tuple[pool.PoolQueue, pbmcp.Session],
) -> None:
    _queue, session = withdrawn
    body = session.call("pb_log", {"key_prefix": KEY[:12], "stream": "stderr"})

    assert body["state"] == "withdrawn"
    assert body["log"]["present"] is True
    assert body["log"]["attempt"] == 1
    assert body["log"]["before_withdrawal"] is True
    assert body["log"]["lines"] == [STDERR.rstrip("\n")]


def test_missing_retained_history_is_named_not_answered_empty(
    withdrawn: tuple[pool.PoolQueue, pbmcp.Session],
) -> None:
    """A record that cannot link its attempts says which ones are unretained."""

    queue, session = withdrawn
    record = _record(queue)
    record.pop("attempt_history_before_withdrawal")
    record["attempts"] = 2
    _rewrite(queue, record)

    body = session.call("pb_action", {"key_prefix": KEY[:12]})
    assert body["attempts_detail"] == []
    history = body["attempts_history"]
    assert history["source"] is None
    assert history["before_withdrawal"] is True
    assert history["recorded_attempts"] == 2
    assert history["unretained_attempts"] == [1, 2]
    assert history["unretained_attempt_count"] == 2
    assert history["unretained_attempts_truncated"] is False
    assert history["problems"], (
        "a record whose links do not cover its own attempt count is corrupt "
        "or incomplete evidence and must say so")


def test_corrupt_retained_history_is_named(
    withdrawn: tuple[pool.PoolQueue, pbmcp.Session],
) -> None:
    queue, session = withdrawn
    record = _record(queue)
    record["attempt_history_before_withdrawal"] = {"attempt": 1}
    _rewrite(queue, record)

    body = session.call("pb_action", {"key_prefix": KEY[:12]})
    assert body["attempts_detail"] == []
    problems = body["attempts_history"]["problems"]
    assert any(
        problem.get("field") == "attempt_history_before_withdrawal"
        for problem in problems), problems
    assert body["adopted_attempt"] is None


def test_a_preserved_link_that_resolves_nowhere_is_reported(
    withdrawn: tuple[pool.PoolQueue, pbmcp.Session],
) -> None:
    """The canonical-path and read checks still run over preserved links."""

    queue, session = withdrawn
    record = _record(queue)
    outcome = queue.root / record["attempt_history_before_withdrawal"][0]["outcome"]
    outcome.unlink()

    body = session.call("pb_action", {"key_prefix": KEY[:12]})
    assert body["attempts_detail"][0]["attempt"] == 1
    assert body["attempts_detail"][0]["unreadable"] == "unreadable outcome"
    assert body["attempts_history"]["problems"] == []


def test_generation_identity_is_still_checked_for_preserved_links(
    withdrawn: tuple[pool.PoolQueue, pbmcp.Session],
) -> None:
    """A record whose generation cannot resolve fails closed, per link."""

    queue, session = withdrawn
    record = _record(queue)
    record["published_unix"] = None
    _rewrite(queue, record)

    body = session.call("pb_action", {"key_prefix": KEY[:12]})
    assert body["state"] == "withdrawn"
    assert body["attempts_detail"] == [{
        "attempt": 1, "unreadable": "attempt history requires published_unix"}]


@pytest.mark.parametrize("field,value,reason", [
    ("published_unix", 1700000000.0, "outcome belongs to another generation"),
    ("attempt", 2, "outcome names another attempt number"),
    ("schema", "not.an.attempt", "outcome is not an attempt record"),
    ("action_key", "1" * 64, "outcome belongs to another action"),
])
def test_an_outcome_that_is_not_this_attempt_is_reported(
    withdrawn: tuple[pool.PoolQueue, pbmcp.Session],
    field: str, value: object, reason: str,
) -> None:
    """A canonical pathname is not identity: the body is checked before use."""

    queue, session = withdrawn
    record = _record(queue)
    outcome = queue.root / record["attempt_history_before_withdrawal"][0]["outcome"]
    attempt = json.loads(outcome.read_text(encoding="utf-8"))
    attempt[field] = value
    outcome.chmod(0o600)
    outcome.write_text(json.dumps(attempt), encoding="utf-8")

    body = session.call("pb_action", {"key_prefix": KEY[:12]})
    assert body["state"] == "withdrawn"
    assert body["attempts_detail"][0]["attempt"] == 1
    assert body["attempts_detail"][0]["unreadable"] == reason
    assert body["adopted_attempt"] is None
    assert body["preserved_attempt"] == 1
    assert body.get("log_tail", {}).get("present") is not True


def test_a_corrupt_log_link_is_not_read_at_all(
    withdrawn: tuple[pool.PoolQueue, pbmcp.Session],
) -> None:
    """The log link is checked before the file is opened, not after."""

    queue, session = withdrawn
    record = _record(queue)
    outcome = queue.root / record["attempt_history_before_withdrawal"][0]["outcome"]
    attempt = json.loads(outcome.read_text(encoding="utf-8"))
    attempt["logs"]["stdout"]["path"] = "/etc/hostname"
    outcome.chmod(0o600)
    outcome.write_text(json.dumps(attempt), encoding="utf-8")

    body = session.call("pb_log", {"key_prefix": KEY[:12]})
    assert body["state"] == "withdrawn"
    assert body["log"]["present"] is False
    assert body["log"]["canonical"] != "/etc/hostname"
    assert "canonical path" in body["log"]["reason"]


def test_a_done_actions_own_history_is_still_adopted(
    done: tuple[pool.PoolQueue, pbmcp.Session],
) -> None:
    """The ordinary path is unchanged: its own link is its adopted ending."""

    _queue, session = done
    body = session.call("pb_action", {"key_prefix": DONE_KEY[:12],
                                      "tail_lines": 2})

    assert body["state"] == "done"
    assert body["adopted_attempt"] == 1
    assert body["preserved_attempt"] is None
    assert body["attempts_history"]["source"] == "attempt_history"
    assert body["attempts_history"]["before_withdrawal"] is False
    assert body["outcome_before_withdrawal"] is None
    assert body["attempts_detail"][0]["status"] == "executed"
    assert body["log_tail"]["before_withdrawal"] is False
    assert body["log_tail"]["lines"] == ["ok"]
