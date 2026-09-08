"""A long-lived MCP session must not accumulate readers after a mount stall."""

import os
from pathlib import Path
import signal
import sys
import time

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))
sys.path.insert(0, str(REPOSITORY / "tests"))

import pbmcp
import pbmcp_fixture as fx


@pytest.fixture
def retained(monkeypatch):
    """Keep real owned reader children alive at cleanup, without wedging NFS.

    Only the stop/reap boundary is replaced. The real bounded fork, deadline
    and fd isolation still run; finally always disposes of these exact children.
    """
    children = []

    def retain(pid, section, started, abandoned):
        children.append(pid)
        abandoned.append({"pid": pid, "section": section,
                          "starttime_ticks": pbmcp.pbstatus._starttime_ticks(pid),
                          "since_unix": time.time()})

    monkeypatch.setattr(pbmcp.pbstatus, "_stop_reader", retain)
    yield children
    for pid in children:
        # A nonblocking wait proves this is still our child before signalling.
        try:
            done, _ = os.waitpid(pid, os.WNOHANG)
        except ChildProcessError:
            continue
        if not done:
            os.kill(pid, signal.SIGKILL)
            os.waitpid(pid, 0)


def stall(*_args, **_kwargs):
    time.sleep(600)


@pytest.mark.parametrize("during_startup", [False, True])
def test_retained_reader_blocks_new_shared_reads_across_calls(
    tmp_path, monkeypatch, retained, during_startup,
):
    fleet = fx.build(tmp_path)
    if during_startup:
        monkeypatch.setattr(pbmcp, "_link_target", stall)
    session = pbmcp.Session(queue_root=fleet.queue_root, cas_root=fleet.cas_root,
                            repo_link=fleet.repo_link, deadline_s=0.1)
    if not during_startup:
        monkeypatch.setattr(pbmcp.pbstatus, "read_pool", stall)
        first = session.call("pb_status")
        assert first["complete"] is False
    assert len(retained) == 1
    for _ in range(3):
        body = session.call("pb_status")
        assert len(retained) == 1, "each poll launched another retained reader"
        assert body["complete"] is False
        assert body["nodes"] is None and body["generation"] is None
        assert body["abandoned_readers"][0]["pid"] == retained[0]
        assert any(item["type"] == "ReaderStillRunning"
                   for item in body["unavailable"])
    assert pbmcp.Server(session).handle({"id": 1, "method": "ping"})["result"] == {}


def test_late_exit_is_reaped_and_reading_recovers(tmp_path, monkeypatch, retained):
    fleet = fx.build(tmp_path)
    session = pbmcp.Session(queue_root=fleet.queue_root, cas_root=fleet.cas_root,
                            repo_link=fleet.repo_link, deadline_s=0.1)
    original = pbmcp.pbstatus.read_pool
    monkeypatch.setattr(pbmcp.pbstatus, "read_pool", stall)
    body = session.call("pb_status")
    assert body["abandoned_readers"][0]["pid"] == retained[0]
    pid = retained[0]
    os.kill(pid, signal.SIGKILL)
    # Observe exit without consuming the wait status; the session must reap it.
    limit = time.monotonic() + 5
    while os.waitid(os.P_PID, pid, os.WEXITED | os.WNOHANG | os.WNOWAIT) is None:
        assert time.monotonic() < limit, "fixture child did not exit"
        time.sleep(0.01)
    monkeypatch.setattr(pbmcp.pbstatus, "read_pool", original)
    session.deadline_s = 5
    healthy = session.call("pb_runtime")
    assert healthy["complete"] is True
    assert healthy["abandoned_readers"] == []
    with pytest.raises(ChildProcessError):
        os.waitpid(pid, os.WNOHANG)
