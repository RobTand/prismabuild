"""A long-lived ``pbmcp`` says when the fleet has moved out from under it.

The server is launched from ``/mnt/shared/prismabuild-fleet/repo/tools/fleet``
so that a new session always starts on the published generation.  A session
outlives publications, though, and after one the process is running code the
fleet has replaced -- the same drift that makes a stale ``pbstatus`` describe
a queue whose record shapes have changed.  Nothing can be done about it from
inside the process, so it is stamped on every response instead and the agent
decides.

Two facts, and they are different: ``generation`` and ``generation_stale``
are about the *fleet's* link moving since this process started, and
``started_from_generation`` is about whether this process was launched out of
that generation at all -- which it is not when a developer runs it from a
checkout.
"""

from __future__ import annotations

from pathlib import Path
import sys

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))
sys.path.insert(0, str(REPOSITORY / "tests"))

import pbmcp  # noqa: E402
import pbmcp_fixture as fx  # noqa: E402


@pytest.fixture()
def fleet(tmp_path: Path) -> fx.Fleet:
    return fx.build(tmp_path)


def test_a_session_names_the_generation_it_started_on(fleet: fx.Fleet) -> None:
    session = pbmcp.Session(queue_root=fleet.queue_root,
                            cas_root=fleet.cas_root, repo_link=fleet.repo_link)
    body = session.call("pb_runtime")
    assert body["generation"] == fx.GENERATION_A
    assert body["generation_stale"] is False


def test_every_response_carries_the_stamp_not_just_pb_runtime(
    fleet: fx.Fleet,
) -> None:
    session = pbmcp.Session(queue_root=fleet.queue_root,
                            cas_root=fleet.cas_root, repo_link=fleet.repo_link)
    for tool, arguments in (("pb_status", {}),
                            ("pb_actions", {"limit": 5}),
                            ("pb_action", {"key_prefix": fx.DONE_KEY[:12]})):
        body = session.call(tool, arguments)
        assert body["generation"] == fx.GENERATION_A, tool
        assert body["generation_stale"] is False, tool


def test_a_publication_under_a_live_session_is_stamped_stale(
    fleet: fx.Fleet,
) -> None:
    session = pbmcp.Session(queue_root=fleet.queue_root,
                            cas_root=fleet.cas_root, repo_link=fleet.repo_link)
    assert session.call("pb_runtime")["generation_stale"] is False

    fx.point_at(fleet, fx.GENERATION_B)

    body = session.call("pb_runtime")
    assert body["generation"] == fx.GENERATION_B
    assert body["generation_stale"] is True
    assert body["manifest"]["generation"] == fx.GENERATION_B
    assert session.call("pb_status")["generation_stale"] is True, (
        "the stamp belongs on every response, because the response an agent "
        "acts on is rarely pb_runtime")


def test_a_missing_link_is_unknown_rather_than_current(fleet: fx.Fleet) -> None:
    fleet.repo_link.unlink()
    session = pbmcp.Session(queue_root=fleet.queue_root,
                            cas_root=fleet.cas_root, repo_link=fleet.repo_link)
    body = session.call("pb_runtime")
    assert body["generation"] is None
    assert body["generation_stale"] is None
    assert body["manifest"] is None


def test_a_server_run_from_a_checkout_does_not_claim_the_generation(
    fleet: fx.Fleet,
) -> None:
    session = pbmcp.Session(queue_root=fleet.queue_root,
                            cas_root=fleet.cas_root, repo_link=fleet.repo_link)
    assert session.call("pb_runtime")["started_from_generation"] is False
    assert pbmcp.RUNTIME_ROOT == REPOSITORY
