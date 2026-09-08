"""A stalled initial runtime-link read must not prevent the MCP handshake."""

from pathlib import Path
import os
import sys
import time

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))
sys.path.insert(0, str(REPOSITORY / "tests"))

import pbmcp
import pbmcp_fixture as fx


@pytest.mark.parametrize("failure", ["timeout", "error"])
def test_failed_startup_read_does_not_hang_or_invent_generation(
    tmp_path, monkeypatch, failure,
):
    fleet = fx.build(tmp_path)
    parent = os.getpid()
    original = pbmcp._link_target

    def unavailable(path):
        # Fail the baseline immediately instead of hanging the test parent.
        assert os.getpid() != parent, "startup shared read ran in session parent"
        if failure == "timeout":
            time.sleep(600)
        raise RuntimeError("startup link unavailable")

    monkeypatch.setattr(pbmcp, "_link_target", unavailable)
    started = time.monotonic()
    session = pbmcp.Session(queue_root=fleet.queue_root, cas_root=fleet.cas_root,
                            repo_link=fleet.repo_link, deadline_s=0.1)
    assert time.monotonic() - started < 2
    server = pbmcp.Server(session)
    assert "result" in server.handle({"id": 1, "method": "initialize"})
    assert server.handle({"id": 2, "method": "ping"})["result"] == {}

    # Recovery cannot recover which generation the failed startup observed.
    monkeypatch.setattr(pbmcp, "_link_target", original)
    session.deadline_s = 5
    for _ in range(2):
        body = session.call("pb_runtime")
        assert body["generation"] == fx.GENERATION_A
        assert body["generation_stale"] is None
        assert body["started_from_generation"] is None
        assert session.startup_generation is None
        assert body["complete"] is False
        if failure == "timeout":
            assert body["timed_out"] == ["startup-repo-link"]
        else:
            assert body["unavailable"] == [{"section": "startup-repo-link",
                                            "type": "RuntimeError",
                                            "error": "startup link unavailable"}]


def test_healthy_startup_read_keeps_generation_change_detection(tmp_path):
    fleet = fx.build(tmp_path)
    session = pbmcp.Session(queue_root=fleet.queue_root, cas_root=fleet.cas_root,
                            repo_link=fleet.repo_link)
    fx.point_at(fleet, fx.GENERATION_B)
    body = session.call("pb_runtime")
    assert body["complete"] is True
    assert body["generation_stale"] is True
    assert session.startup_generation == fx.GENERATION_A
