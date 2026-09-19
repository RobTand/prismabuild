"""``pb_cursors`` answers where each consumer's read cursor stands.

The cursor is the phase the consumer's progress record last vouched for, and
the frontier is what the tier ledgers say is staged; both live in records
the fleet already files, and #660 asks for exactly that join as a tool.  The
gap arithmetic is ``pbstatus``'s own -- the same census ``pb_starvation``
serves -- so the two tools cannot disagree about where a reader is, and the
tests here hold them to that.
"""
from __future__ import annotations

import errno
import os
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
import pbstatus  # noqa: E402

DEADLINE_S = 1.0


@pytest.fixture()
def fleet(tmp_path: Path) -> fx.Fleet:
    return fx.build_starved(tmp_path)


@pytest.fixture()
def session(fleet: fx.Fleet) -> pbmcp.Session:
    return pbmcp.Session(queue_root=fleet.queue_root, cas_root=fleet.cas_root,
                         repo_link=fleet.repo_link)


@pytest.fixture()
def plain(tmp_path: Path) -> fx.Fleet:
    return fx.build(tmp_path)


def test_one_row_per_filed_plan_with_the_accepted_phase_and_when(
    session: pbmcp.Session, fleet: fx.Fleet,
) -> None:
    body = session.call("pb_cursors")
    assert body["complete"] is True and body["timed_out"] == []
    assert len(body["cursors"]) == 1
    row = body["cursors"][0]
    assert row["consumer_action_key"] == fx.CONSUMER_KEY
    assert row["consumer_action_key_prefix"] == fx.CONSUMER_KEY[:12]
    assert row["valid"] is True
    assert row["state"] == "claimed"
    assert row["tier_id"] == fx.STAGE_TIER
    assert row["ram_tier_id"] == fx.RAM_TIER
    assert row["phase_count"] == 2
    assert row["accepted_phase"] == "phase-0000"
    assert row["accepted_index"] == 0
    assert row["accepted_units_completed"] == 3
    reported = getattr(fleet, "consumer_reported_unix")
    assert row["accepted_reported_unix"] == pytest.approx(reported, abs=5)
    assert row["accepted_age_s"] == pytest.approx(800.0, abs=10)


def test_the_gap_names_what_is_staged_ahead_of_the_cursor(
    session: pbmcp.Session,
) -> None:
    body = session.call("pb_cursors")
    gap = body["cursors"][0]["cursor_gap"]
    assert gap["accepted_phase"] == "phase-0000"
    assert gap["live_byte_cursor"] == pbstatus.NOT_OBSERVABLE
    assert gap["stage"]["remaining_phases"] == 2
    assert gap["stage"]["staged_phases"] == 1
    assert gap["stage"]["unstaged_phases"] == ["phase-0001"]
    assert gap["stage"]["unstaged_bytes"] == 2 * fx.STARVED_GIB
    assert gap["ram"]["remaining_phases"] == 1
    assert gap["ram"]["staged_phases"] == 1
    assert gap["ram"]["unstaged_phases"] == []


def test_the_gap_agrees_with_the_starvation_census_it_reuses(
    session: pbmcp.Session,
) -> None:
    """One source of truth: the same plan, the same arithmetic, one answer."""

    cursors = session.call("pb_cursors")["cursors"][0]
    plans = session.call("pb_starvation")["residency_plans"]
    plan = next(row for row in plans
                if row["consumer_action_key_prefix"] == fx.CONSUMER_KEY[:12])
    assert cursors["cursor_gap"] == plan["cursor_gap"]
    assert cursors["accepted_phase"] == plan["accepted_phase"]
    assert cursors["accepted_index"] == plan["accepted_index"]


def test_a_prefix_keeps_only_its_consumer(session: pbmcp.Session) -> None:
    body = session.call("pb_cursors", {"key_prefix": fx.CONSUMER_KEY[:12]})
    assert [row["consumer_action_key"] for row in body["cursors"]] == [
        fx.CONSUMER_KEY]
    assert body["returned"] == 1
    nobody = session.call("pb_cursors", {"key_prefix": "f" * 12})
    assert nobody["complete"] is True
    assert nobody["cursors"] == [], (
        "no plan under that prefix is an answer, not a failed read")


def test_a_prefix_that_cannot_name_one_is_refused_before_the_mount(
    session: pbmcp.Session,
) -> None:
    with pytest.raises(pbmcp.ToolError) as raised:
        session.call("pb_cursors", {"key_prefix": "not-hex"})
    assert "hexadecimal" in str(raised.value)


def test_a_fleet_where_nobody_was_staged_has_no_cursors_to_report(
    plain: fx.Fleet,
) -> None:
    session = pbmcp.Session(queue_root=plain.queue_root,
                            cas_root=plain.cas_root,
                            repo_link=plain.repo_link)
    body = session.call("pb_cursors")
    assert body["complete"] is True
    assert body["cursors"] == []
    assert "residency plan" in body["population"], (
        "say what the population is, so an empty answer is not mistaken for "
        "a fleet that cannot read")


def test_an_unreadable_plan_is_a_row_that_says_so(
    session: pbmcp.Session, fleet: fx.Fleet,
) -> None:
    path = fleet.queue_root / pool.RESIDENCY_PLANS / f"{fx.CONSUMER_KEY}.json"
    path.chmod(0o644)
    path.write_text("{ not json")
    body = session.call("pb_cursors")
    assert body["complete"] is True
    row = body["cursors"][0]
    assert row["consumer_action_key"] == fx.CONSUMER_KEY
    assert row["valid"] is False
    assert row["note"]
    assert body["unreadable"], "the refusal is named, not swallowed"
    assert not body["notes"] or all(body["notes"])


def test_a_stale_handle_is_not_a_queue_with_no_plans(
    fleet: fx.Fleet, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The #208 lesson, one directory up from where pb_actions learned it."""

    session = pbmcp.Session(queue_root=fleet.queue_root,
                            cas_root=fleet.cas_root,
                            repo_link=fleet.repo_link)
    real_scandir = os.scandir

    def stale(path, *args, **kwargs):
        if str(path).startswith(str(fleet.queue_root)):
            raise OSError(errno.ESTALE, "Stale file handle", str(path))
        return real_scandir(path, *args, **kwargs)

    monkeypatch.setattr(pbmcp.os, "scandir", stale)
    body = session.call("pb_cursors")
    assert body["complete"] is False
    assert body["cursors"] is None, (
        "a mount that did not answer has not said there are no plans")
    assert body["unavailable"]


def test_a_census_that_never_returns_is_null_not_an_empty_list(
    fleet: fx.Fleet, monkeypatch: pytest.MonkeyPatch,
) -> None:
    def never(*_args, **_kwargs):
        time.sleep(600)

    monkeypatch.setattr(pbmcp, "_cursor_census", never)
    bounded = pbmcp.Session(queue_root=fleet.queue_root,
                            cas_root=fleet.cas_root,
                            repo_link=fleet.repo_link,
                            deadline_s=DEADLINE_S)
    body = bounded.call("pb_cursors")
    assert "cursors" in body["timed_out"]
    assert body["cursors"] is None
    assert body["returned"] is None
