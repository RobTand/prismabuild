"""``pb_starvation`` serves the one-command starvation census over MCP.

The census itself is ``pbstatus --starvation``'s (#661); this is #660's
frontend contract for it, so the tool reuses that reader whole rather than
growing a second one that could disagree about who is waiting.  What is
under test here is the serving: that every section of the blob arrives, that
the census's own completeness survives the trip under a name of its own (the
envelope's ``complete`` already means "the mount answered"), and that a
census which never came back is ``null`` rather than a quiet fleet.
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


def test_waiting_claims_carry_the_quiet_arithmetic_and_the_child(
    session: pbmcp.Session,
) -> None:
    body = session.call("pb_starvation")
    assert body["complete"] is True and body["timed_out"] == []
    assert body["starvation_schema"] == pbstatus.STARVATION_SCHEMA_V1
    assert body["census_complete"] is True
    assert len(body["waiting_claims"]) == 1
    claim = body["waiting_claims"][0]
    assert claim["action_key_prefix"] == fx.CONSUMER_KEY[:12]
    assert claim["node"] == "fixture-box"
    assert claim["accepted_phase"] == "phase-0000"
    assert claim["quiet_s"] == pytest.approx(800.0)
    assert claim["grace_s"] == pytest.approx(900.0)
    assert claim["quiet_fraction"] == pytest.approx(800.0 / 900.0)
    assert claim["child"]["silent_s"] == pytest.approx(700.0)
    assert claim["child"]["cpu_seconds"] == pytest.approx(12.0)
    assert claim["waiting_on_data"] is True
    assert claim["waiting_rule"], "a boolean without its inputs is an opinion"
    assert claim["cpu_growth"] == pbstatus.NOT_OBSERVABLE


def test_residency_plans_say_what_blocks_the_promotion(
    session: pbmcp.Session,
) -> None:
    body = session.call("pb_starvation")
    assert len(body["residency_plans"]) == 1
    plan = body["residency_plans"][0]
    assert plan["valid"] is True and plan["state"] == "claimed"
    assert plan["tier_id"] == fx.STAGE_TIER
    assert plan["ram_tier_id"] == fx.RAM_TIER
    first, second = plan["phases"]
    assert first["stage"]["staged"] is True
    assert first["ram"]["staged"] is True and first["ram"]["fragment"] is True
    assert second["stage"]["published"] is True
    assert second["stage"]["staged"] is False
    gap = plan["cursor_gap"]
    assert gap["stage"]["unstaged_phases"] == ["phase-0001"]
    assert gap["stage"]["unstaged_bytes"] == 2 * fx.STARVED_GIB


def test_tiers_carry_the_announcement_and_the_ledger_beside_it(
    session: pbmcp.Session,
) -> None:
    body = session.call("pb_starvation")
    tier = next(row for row in body["tiers"]
                if row["tier_id"] == fx.STAGE_TIER)
    assert tier["announced"] is True
    assert tier["tier_kind"] == "stage"
    assert tier["ledger_capacity"]["stage_gib"] == 64
    assert tier["ledger_available"]["stage_gib"] == 62
    assert tier["ledger_held"]["stage_gib"] == 2
    ram = next(row for row in body["tiers"] if row["tier_id"] == fx.RAM_TIER)
    assert ram["epoch"] == fx.RAM_EPOCH and ram["window_gib"] == 4


def test_denial_top_separates_the_reasons_that_block_movers(
    session: pbmcp.Session,
) -> None:
    body = session.call("pb_starvation")
    host = body["denial_top"][0]
    assert host["host"] == "fixture-box"
    assert host["denials"] == 2
    assert host["mover_blocked"] == 1
    assert host["mover_blocked_reasons"] == [
        {"reason": "tier_reservation_unavailable", "count": 1}]


def test_starved_ready_items_travel_with_the_answer(
    session: pbmcp.Session, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The items a box will not withhold for arrive as pbstatus lists them (#924)."""

    assert session.call("pb_starvation")["starved"] == []
    reader = pbmcp.pbstatus.read_starvation
    row = {"action_key_prefix": "aaaaaaaaaaaa", "host": "fixture-box",
           "reason": "reservation_unavailable_starved",
           "why": "holder_does_not_drain_soon",
           "holders": [{"action_key": "683cb3caa5ea", "bound": "unbounded"}]}
    monkeypatch.setattr(pbmcp.pbstatus, "read_starvation",
                        lambda *args, **kwargs: {**reader(*args, **kwargs),
                                                 "starved": [row]})
    assert session.call("pb_starvation")["starved"] == [row]


def test_the_gap_list_travels_with_the_answer(session: pbmcp.Session) -> None:
    """What no record carries is a deliverable, not an error."""

    body = session.call("pb_starvation")
    fields = {entry["field"] for entry in body["not_observable"]}
    assert {"waiting_claims[].cpu_growth",
            "residency_plans[].cursor_gap.live_byte_cursor"} <= fields
    assert all(entry["would_need"] for entry in body["not_observable"])


def test_a_census_that_never_returns_is_reported_not_awaited(
    fleet: fx.Fleet, monkeypatch: pytest.MonkeyPatch,
) -> None:
    def never(*_args, **_kwargs):
        time.sleep(600)

    monkeypatch.setattr(pbmcp.pbstatus, "read_starvation", never)
    bounded = pbmcp.Session(queue_root=fleet.queue_root,
                            cas_root=fleet.cas_root,
                            repo_link=fleet.repo_link,
                            deadline_s=DEADLINE_S)
    body = bounded.call("pb_starvation")
    assert body["complete"] is False
    assert "starvation" in body["timed_out"]
    # null rather than empty collections: a census that did not answer has
    # not said "nobody is waiting".
    assert body["waiting_claims"] is None
    assert body["residency_plans"] is None
    assert body["not_observable"] is None
    assert body["census_complete"] is None


def test_a_fleet_that_never_staged_anybody_still_answers_completely(
    plain: fx.Fleet,
) -> None:
    session = pbmcp.Session(queue_root=plain.queue_root,
                            cas_root=plain.cas_root,
                            repo_link=plain.repo_link)
    body = session.call("pb_starvation")
    assert body["complete"] is True
    assert body["census_complete"] is True
    assert body["waiting_claims"] == []
    assert body["residency_plans"] == []
    assert body["not_observable"], (
        "the gap list is static: it says what is not observable even when "
        "everything is quiet")


def test_the_census_leaves_the_queue_byte_identical(
    fleet: fx.Fleet,
) -> None:
    """The starvation reader reuses pbstatus's own, and this says so in the
    terms an operator would check it in: same files, same bytes, same times."""

    import os

    def listing() -> set[tuple[str, int, int]]:
        found = set()
        for directory, _sub, files in os.walk(fleet.queue_root):
            for name in files:
                path = Path(directory) / name
                info = path.stat()
                found.add((str(path.relative_to(fleet.queue_root)),
                           info.st_size, info.st_mtime_ns))
        return found

    before = listing()
    session = pbmcp.Session(queue_root=fleet.queue_root,
                            cas_root=fleet.cas_root,
                            repo_link=fleet.repo_link)
    assert session.call("pb_starvation")["complete"] is True
    assert session.call("pb_cursors")["complete"] is True
    assert listing() == before
