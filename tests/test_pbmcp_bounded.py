"""A shared mount that does not answer costs ``pbmcp`` its deadline, and no more.

The failure this exists to prevent is the one #350 recorded: fifteen
``pbstatus`` processes wedged in uninterruptible sleep on one box, each of
them a status command that went to look at the queue and never came back. An
MCP server is worse placed than a command to survive that -- it is long-lived,
an agent calls it repeatedly, and a hung tool call hangs the agent -- so every
read of the mount goes through ``pbstatus.bounded``, which runs it in a child
this process can abandon.

Both halves are checked. A section that does not answer must be *named* in
the response rather than silently missing, because "the queue is empty" and
"the queue did not answer" are the two states an agent must never confuse.
And the server must still be answering afterwards: a timeout that leaves the
process useless has moved the hang rather than removed it.
"""

from __future__ import annotations

import errno
import json
import os
from pathlib import Path
import subprocess
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
import pbstatus  # noqa: E402

DEADLINE_S = 1.0
TOOL = REPOSITORY / "tools" / "fleet" / "pbmcp.py"


def test_a_census_that_never_returns_is_reported_not_awaited(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    fleet = fx.build(tmp_path)

    def never(_queue_root):
        time.sleep(600)

    monkeypatch.setattr(pbmcp.pbstatus, "read_pool", never)
    session = pbmcp.Session(queue_root=fleet.queue_root,
                            cas_root=fleet.cas_root, repo_link=fleet.repo_link,
                            deadline_s=DEADLINE_S)
    started = time.monotonic()
    body = session.call("pb_status")
    elapsed = time.monotonic() - started

    assert body["complete"] is False
    assert "pool" in body["timed_out"]
    assert elapsed < DEADLINE_S * 5, (
        f"the call spent {elapsed:.1f}s on a {DEADLINE_S}s deadline")
    # The census is reported as absent rather than as an empty node list that
    # would read like a fleet with nothing running on it.
    assert body["nodes"] is None


def test_a_timed_out_section_is_null_not_an_empty_collection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One convention across every tool: ``null`` means "not read".

    An empty list is an answer -- no endings, no matching actions, no such
    action -- and a section that never came back has not answered.  Handing
    back ``[]`` or ``{}`` for a read that timed out makes the two
    indistinguishable to anything but a reader that also checks
    ``timed_out``, and the whole point of the envelope is that the payload
    does not quietly contradict it.
    """

    fleet = fx.build(tmp_path)

    def never(*_args, **_kwargs):
        time.sleep(600)

    for name in ("read_endings",):
        monkeypatch.setattr(pbmcp.pbstatus, name, never)
    for name in ("_reservations", "_scan_actions", "_records_for"):
        monkeypatch.setattr(pbmcp, name, never)
    session = pbmcp.Session(queue_root=fleet.queue_root,
                            cas_root=fleet.cas_root, repo_link=fleet.repo_link,
                            deadline_s=DEADLINE_S)

    status = session.call("pb_status")
    assert "endings" in status["timed_out"] and status["endings"] is None
    assert "reservations" in status["timed_out"] and status["reservations"] is None

    listing = session.call("pb_actions")
    assert "actions" in listing["timed_out"]
    assert listing["actions"] is None
    assert listing["scanned"] is None
    assert listing["truncated"] is None
    assert listing["returned"] is None

    action = session.call("pb_action", {"key_prefix": fx.DONE_KEY[:12]})
    assert "records" in action["timed_out"]
    assert action["found"] is None, "a read that never happened found nothing"
    assert action["states"] is None

    log = session.call("pb_log", {"key_prefix": fx.DONE_KEY[:12]})
    assert "records" in log["timed_out"]
    assert log["found"] is None
    assert log["log"] is None


def test_a_record_read_that_blocks_on_the_mount_is_bounded(
    tmp_path: Path,
) -> None:
    """The real shape, without a stub: a read that the kernel will not finish.

    A FIFO where an attempt outcome should be is a file whose ``read`` blocks
    until somebody writes to it, which is the closest thing a test can build
    to a hard mount that has stopped answering. No thread or alarm can
    interrupt it in process; only the abandonable child can.
    """

    fleet = fx.build(tmp_path)
    record = fleet.record(pool.DONE, fx.DONE_KEY)
    outcome = fleet.queue.attempt_path(record, 1)
    outcome.chmod(0o644)
    outcome.unlink()
    os.mkfifo(outcome, 0o444)

    session = pbmcp.Session(queue_root=fleet.queue_root,
                            cas_root=fleet.cas_root, repo_link=fleet.repo_link,
                            deadline_s=DEADLINE_S)
    started = time.monotonic()
    body = session.call("pb_action", {"key_prefix": fx.DONE_KEY[:12]})
    elapsed = time.monotonic() - started

    assert body["complete"] is False
    assert "attempts" in body["timed_out"]
    assert elapsed < DEADLINE_S * 5, f"spent {elapsed:.1f}s"
    # What could be read is still reported: the ending is on the mutable
    # record and did not need the attempt that would not answer.
    assert body["outcome"]["status"] == "executed"


def test_the_server_still_answers_after_a_section_timed_out(
    tmp_path: Path,
) -> None:
    """A bounded read must end a call, never a session."""

    fleet = fx.build(tmp_path)
    record = fleet.record(pool.DONE, fx.DONE_KEY)
    outcome = fleet.queue.attempt_path(record, 1)
    outcome.chmod(0o644)
    outcome.unlink()
    os.mkfifo(outcome, 0o444)

    process = subprocess.Popen(
        [sys.executable, str(TOOL),
         "--queue-root", str(fleet.queue_root),
         "--cas-root", str(fleet.cas_root),
         "--repo-link", str(fleet.repo_link),
         "--deadline-s", str(DEADLINE_S)],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, bufsize=1)

    def rpc(method, params=None, ident=1):
        assert process.stdin is not None and process.stdout is not None
        process.stdin.write(json.dumps(
            {"jsonrpc": "2.0", "id": ident, "method": method,
             "params": params or {}}) + "\n")
        process.stdin.flush()
        return json.loads(process.stdout.readline())

    try:
        rpc("initialize", {"protocolVersion": "2024-11-05"})
        response = rpc("tools/call", {"name": "pb_action",
                                      "arguments": {"key_prefix": fx.DONE_KEY[:12]}},
                       ident=2)
        body = json.loads(response["result"]["content"][0]["text"])
        assert body["complete"] is False and "attempts" in body["timed_out"]
        assert rpc("ping", ident=3)["result"] == {}
        healthy = json.loads(rpc(
            "tools/call", {"name": "pb_runtime"}, ident=4
        )["result"]["content"][0]["text"])
        assert healthy["complete"] is True
    finally:
        assert process.stdin is not None
        process.stdin.close()
        try:
            process.wait(timeout=30)
        except subprocess.TimeoutExpired:  # pragma: no cover - a hung server
            process.kill()
            process.wait(timeout=10)


def test_a_zero_deadline_asks_for_the_unbounded_read_and_says_so(
    tmp_path: Path,
) -> None:
    """``--deadline-s 0`` is the pre-#358 behaviour, and stays available."""

    fleet = fx.build(tmp_path)
    session = pbmcp.Session(queue_root=fleet.queue_root,
                            cas_root=fleet.cas_root, repo_link=fleet.repo_link,
                            deadline_s=0)
    body = session.call("pb_status")
    assert body["deadline_s"] == 0
    assert body["complete"] is True
    assert pbstatus.Deadline(0).bounded is False


def test_the_generation_link_is_read_under_the_deadline_too(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Staleness that cannot be read is ``None``, never ``False``.

    ``False`` would tell an agent it is on the published generation at
    exactly the moment nothing can say so, which is the confident wrong
    answer this whole design refuses to give.
    """

    fleet = fx.build(tmp_path)

    def never(_link):
        time.sleep(600)

    session = pbmcp.Session(queue_root=fleet.queue_root,
                            cas_root=fleet.cas_root, repo_link=fleet.repo_link,
                            deadline_s=DEADLINE_S)
    monkeypatch.setattr(pbmcp, "_link_target", never)
    body = session.call("pb_runtime")
    assert body["generation"] is None
    assert body["generation_stale"] is None
    assert "repo-link" in body["timed_out"]
    assert body["complete"] is False


def test_a_stale_handle_is_not_reported_as_an_absent_action(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ESTALE during a prefix scan must not become "no action starts with".

    The precedent is #208: an NFS burst turned two tidied-away offer files
    into ``ESTALE`` on a whole directory read, and a reader that treated the
    error as absence killed a submission that had queued nothing. ``pbmcp``
    reads the same mount from a longer-lived process, so the same burst would
    have it answer "there is no such action" with ``complete: true`` beside
    it -- a confident wrong verdict about somebody's running job.
    """

    fleet = fx.build(tmp_path)
    session = pbmcp.Session(queue_root=fleet.queue_root,
                            cas_root=fleet.cas_root, repo_link=fleet.repo_link,
                            deadline_s=DEADLINE_S)
    real_scandir = os.scandir

    def stale(path, *args, **kwargs):
        if str(path).startswith(str(fleet.queue_root)):
            raise OSError(errno.ESTALE, "Stale file handle", str(path))
        return real_scandir(path, *args, **kwargs)

    monkeypatch.setattr(pbmcp.os, "scandir", stale)
    with pytest.raises(pbmcp.ToolError) as raised:
        session.call("pb_action", {"key_prefix": fx.DONE_KEY[:12]})
    assert "did not answer" in str(raised.value), (
        "an unreadable queue is not an empty one")
    assert raised.value.detail["unavailable"], "name the section that failed"

    body = session.call("pb_actions", {})
    assert body["complete"] is False
    assert body["actions"] is None


def test_a_stale_handle_on_the_record_is_not_a_missing_action(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The same rule one level down: a record that cannot be read is not absent."""

    fleet = fx.build(tmp_path)
    session = pbmcp.Session(queue_root=fleet.queue_root,
                            cas_root=fleet.cas_root, repo_link=fleet.repo_link,
                            deadline_s=DEADLINE_S)

    def stale(path, **kwargs):
        raise OSError(errno.ESTALE, "Stale file handle", str(path))

    monkeypatch.setattr(pbmcp.pool, "_read_json", stale)
    body = session.call("pb_action", {"key_prefix": fx.DONE_KEY[:12]})

    assert body["complete"] is False
    assert body["found"] is None, (
        "found: false is the queue saying the key is not there; a stale "
        "handle says nothing of the kind")
    assert body["states"] is None
