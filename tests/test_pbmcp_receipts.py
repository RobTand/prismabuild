"""``pb_receipts``: one verdict per action key, for PR validation evidence.

An agent that submitted a batch of test shards through PrismaBuild holds a
handful of keys and wants one answer per key: did it end, with what return
code, and is there a CAS receipt.  ``pb_action`` answers that for one key at
the cost of the sealed submission, every attempt and a log tail; this answers
it for a set, with the ending and the receipt only.

What it deliberately does not answer is pass counts: per-shard
passed/failed/skipped totals live in the ``pbtest --json`` the caller wrote,
not on the queue record, and saying so in the payload beats a caller
mistaking "returncode 0" for "every case passed".
"""

from __future__ import annotations

from pathlib import Path
import sys
import time

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))
sys.path.insert(0, str(REPOSITORY / "tests"))

from prismabuild import pool  # noqa: E402
import pbmcp  # noqa: E402
import pbmcp_fixture as fx  # noqa: E402

FAILED_KEY = "f" * 64


@pytest.fixture()
def fleet(tmp_path: Path) -> fx.Fleet:
    built = fx.build(tmp_path)
    queue = built.queue
    queue.publish(action_key=FAILED_KEY, tags=["oops"], priority=0,
                  resources={"cpu": 1, "mem_gb": 1}, max_attempts=1,
                  cas_root=built.cas_root,
                  checkout_root=str(built.checkout),
                  worker_script=str(built.base / "worker.py"))
    queue.claim(tags=["oops"], capacity={"cpu": 8, "mem_gb": 16})
    queue.finish(FAILED_KEY, status="failed",
                 detail={"returncode": 3, "elapsed_s": 0.5,
                         "stdout": "1 failed, 9 passed\n",
                         "stderr": ""})
    return built


@pytest.fixture()
def session(fleet: fx.Fleet) -> pbmcp.Session:
    return pbmcp.Session(queue_root=fleet.queue_root, cas_root=fleet.cas_root,
                         repo_link=fleet.repo_link)


def test_each_key_gets_a_state_a_verdict_and_a_receipt(
    session: pbmcp.Session,
) -> None:
    body = session.call("pb_receipts", {"keys": [
        fx.DONE_KEY[:12], FAILED_KEY[:12], fx.READY_KEY[:12]]})
    assert body["complete"] is True, (body["timed_out"], body["unavailable"])
    rows = {row["action_key"]: row for row in body["receipts"]}

    done = rows[fx.DONE_KEY]
    assert done["found"] is True and done["state"] == "done"
    assert done["verdict"] == "green"
    assert done["returncode"] == 0
    assert done["receipt"]["present"] is True
    assert done["receipt"]["result"]["bytes"] == len(fx.PAYLOAD)

    failed = rows[FAILED_KEY]
    assert failed["found"] is True and failed["state"] == "failed"
    assert failed["verdict"] == "failed"
    assert failed["returncode"] == 3
    assert failed["receipt"]["present"] is False

    ready = rows[fx.READY_KEY]
    assert ready["found"] is True and ready["state"] == "ready"
    assert ready["verdict"] == "pending"
    assert ready["returncode"] is None
    assert ready["receipt"]["present"] is False

    assert body["summary"] == {"green": 1, "failed": 1, "pending": 1,
                               "withdrawn": 0, "unknown": 0, "errors": 0}


def test_an_unknown_prefix_is_an_entry_not_an_error(
    session: pbmcp.Session,
) -> None:
    body = session.call("pb_receipts", {"keys": [fx.DONE_KEY[:12], "b" * 12]})
    assert body["complete"] is True
    rows = {row["key_prefix"]: row for row in body["receipts"]}
    assert rows[fx.DONE_KEY[:12]]["found"] is True
    missing = rows["b" * 12]
    assert missing["found"] is False
    assert missing["action_key"] is None
    assert "no action starts with" in missing["error"]
    assert body["summary"]["errors"] == 1


def test_an_ambiguous_prefix_names_its_candidates(
    session: pbmcp.Session, fleet: fx.Fleet,
) -> None:
    fleet.queue.publish(action_key=fx.TWIN_KEY, cas_root=fleet.cas_root,
                        checkout_root=str(fleet.checkout),
                        worker_script=str(fleet.base / "worker.py"))
    body = session.call("pb_receipts", {"keys": [fx.DONE_KEY[:12]]})
    assert body["complete"] is True
    (row,) = body["receipts"]
    assert row["found"] is False
    assert set(row["candidates"]) == {fx.DONE_KEY, fx.TWIN_KEY}
    assert body["summary"]["errors"] == 1


def test_a_key_that_does_not_answer_is_unread_not_absent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``null``, never an empty verdict, when the mount does not answer."""

    fleet = fx.build(tmp_path)

    def never(*_args, **_kwargs):
        time.sleep(600)

    monkeypatch.setattr(pbmcp, "_resolve_prefix", never)
    session = pbmcp.Session(queue_root=fleet.queue_root,
                            cas_root=fleet.cas_root,
                            repo_link=fleet.repo_link, deadline_s=1.0)
    body = session.call("pb_receipts", {"keys": [fx.DONE_KEY[:12]]})
    assert body["complete"] is False
    (row,) = body["receipts"]
    assert row["found"] is None
    assert body["summary"]["unknown"] == 1


def test_keys_are_required_and_bounded(session: pbmcp.Session) -> None:
    with pytest.raises(pbmcp.InvalidArguments):
        session.call("pb_receipts", {})
    with pytest.raises(pbmcp.InvalidArguments):
        session.call("pb_receipts", {"keys": fx.DONE_KEY[:12]})
    with pytest.raises(pbmcp.ToolError):
        session.call("pb_receipts", {"keys": []})
    with pytest.raises(pbmcp.ToolError) as raised:
        session.call("pb_receipts", {"keys": ["a" * 12] * 51})
    assert "at most 50" in str(raised.value)
