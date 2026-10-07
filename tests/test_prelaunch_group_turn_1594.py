"""The prelaunch turn for multi-tier units (#1594 R3').

Tests first: one durable ordering point, no overtaking, ranked picks,
abandoned turns and unknown evidence. The integrator runs them.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import pool  # noqa: E402
from prismabuild import prelaunch_group as pg  # noqa: E402

G1 = "11" * 32
G2 = "22" * 32
G3 = "33" * 32


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    """A bare queue; the turn reads files, not ledgers."""
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    return queue


def _ticket(unit: str, priority: int, when: float, **tiers: int) -> dict:
    """A well-formed ticket body for one unit."""
    return {"unit": unit, "priority": priority, "published_unix": when,
            "tier_demands": dict(tiers)}


def test_two_racing_creators_produce_one_winner(queue) -> None:
    """The no-clobber link linearizes the epoch."""
    first = pg.take_turn(queue, [_ticket(G1, 1, 10.0, tA=4, tB=2),
                                 _ticket(G2, 1, 11.0, tA=4, tB=2)])
    assert (first.status, first.epoch, first.unit, first.won) == ("created", 0, G1, True)
    second = pg.take_turn(queue, [_ticket(G1, 1, 10.0, tA=4, tB=2),
                                  _ticket(G2, 1, 11.0, tA=4, tB=2)])
    assert (second.status, second.epoch, second.unit, second.won) == ("standing", 0, G1, False)


def test_rank_is_priority_then_time_then_unit(queue) -> None:
    """Higher priority wins; time then unit break the tie."""
    outcome = pg.take_turn(queue, [_ticket(G2, 5, 12.0, tA=4, tB=2),
                                   _ticket(G1, 9, 30.0, tA=4, tB=2),
                                   _ticket(G3, 9, 20.0, tA=4, tB=2)])
    assert outcome.unit == G3
    assert outcome.epoch == 0 and outcome.won is True


def test_higher_priority_arriving_later_waits(queue) -> None:
    """An open turn never yields to a newcomer, however ranked."""
    first = pg.take_turn(queue, [_ticket(G1, 1, 10.0, tA=4, tB=2)])
    assert first.unit == G1
    assert pg.turn_ticket(queue, G2, 99, 20.0, {"tA": 4, "tB": 2}) is True
    waiting = pg.take_turn(queue, [_ticket(G2, 99, 20.0, tA=4, tB=2),
                                   _ticket(G1, 1, 10.0, tA=4, tB=2)])
    assert (waiting.status, waiting.unit, waiting.won) == ("standing", G1, False)


def test_turn_with_holdings_is_not_overtaken(queue) -> None:
    """Holdings pin the next pick even when a better rank exists."""
    queue.mint_tier_capacity("prismabuild-stage:dl380g10", {"stage_gib": 8})
    tier = "prismabuild-stage:dl380g10"
    holder = pg.holder_name(G2, tier, ["phase-a"])
    assert pg.file_intent(queue, G2, holder, tier, 2, []) is True
    first = pg.take_turn(queue, [_ticket(G2, 100, 10.0, tA=4, tB=2)])
    assert first.unit == G2
    begun = pg.reconcile(queue, tier, G2, holder, 2, [], writer_is_me=True)
    assert begun.state == "acquiring"
    assert pg.turn_ticket(queue, G3, 99, 20.0, {"tA": 4, "tB": 2}) is True
    waiting = pg.take_turn(queue, [_ticket(G3, 99, 20.0, tA=4, tB=2),
                                   _ticket(G2, 100, 10.0, tA=4, tB=2)])
    assert (waiting.status, waiting.unit) == ("standing", G2)
    assert pg.finish_turn(queue, 0, "complete") is True
    nxt = pg.take_turn(queue, [_ticket(G3, 99, 20.0, tA=4, tB=2),
                               _ticket(G2, 100, 10.0, tA=4, tB=2)])
    assert (nxt.status, nxt.epoch, nxt.unit, nxt.won) == ("created", 1, G3, True)


def test_highest_rank_with_holdings_is_skipped_next_turn(queue) -> None:
    """A fully reserved holder never blocks the next epoch behind itself."""
    queue.mint_tier_capacity("prismabuild-stage:dl380g10", {"stage_gib": 8})
    tier = "prismabuild-stage:dl380g10"
    holder = pg.holder_name(G2, tier, ["phase-a"])
    assert pg.file_intent(queue, G2, holder, tier, 2, []) is True
    assert pg.take_turn(queue, [_ticket(G2, 100, 10.0, tA=4, tB=2)]).unit == G2
    assert pg.reconcile(queue, tier, G2, holder, 2, [], writer_is_me=True).state == "acquiring"
    assert pg.reconcile(queue, tier, G2, holder, 2, [], writer_is_me=True).state == "acquiring"
    assert pg.reconcile(queue, tier, G2, holder, 2, [], writer_is_me=True).state == "reserved"
    assert pg.finish_turn(queue, 0, "complete") is True
    nxt = pg.take_turn(queue, [_ticket(G2, 100, 10.0, tA=4, tB=2),
                               _ticket(G3, 99, 20.0, tA=4, tB=2)])
    assert nxt.unit == G3 and nxt.epoch == 1


def test_abandoned_turn_releases_groups_and_moves_on(queue) -> None:
    """Abandon files done; the released room serves the next turn."""
    queue.mint_tier_capacity("prismabuild-stage:dl380g10", {"stage_gib": 8})
    tier = "prismabuild-stage:dl380g10"
    ledger = queue.tier_ledger(tier)
    holder = pg.holder_name(G1, tier, ["phase-a"])
    assert pg.file_intent(queue, G1, holder, tier, 2, []) is True
    assert pg.take_turn(queue, [_ticket(G1, 1, 10.0, tA=4, tB=2)]).unit == G1
    assert pg.reconcile(queue, tier, G1, holder, 2, [], writer_is_me=True).state == "acquiring"
    assert pg.reconcile(queue, tier, G1, holder, 2, [], writer_is_me=True).state == "acquiring"
    assert pg.reconcile(queue, tier, G1, holder, 2, [], writer_is_me=True).state == "reserved"
    assert pg.finish_turn(queue, 0, "abandoned") is True
    assert pg.release_unit(queue, tier, G1, holder, [], terminal=True) == ["prelaunch-group-released"]
    assert ledger.available().get("stage_gib") == 8
    nxt = pg.take_turn(queue, [_ticket(G1, 1, 10.0, tA=4, tB=2),
                               _ticket(G2, 9, 20.0, tA=4, tB=2)])
    assert (nxt.epoch, nxt.unit, nxt.won) == (1, G2, True)


def test_unreadable_ticket_reserves_nothing(queue) -> None:
    """Garbage tickets never win; garbage alone opens no turn."""
    outcome = pg.take_turn(queue, ["not-a-ticket", {"unit": G1}])
    assert (outcome.status, outcome.epoch, outcome.unit) == ("no-candidate", None, None)
    assert pg.current_turn(queue)["status"] == "none"


def test_torn_turn_file_reads_unknown_and_blocks(queue) -> None:
    """A turn the reader cannot parse authorizes no reservation."""
    assert pg.take_turn(queue, [_ticket(G1, 1, 10.0, tA=4, tB=2)]).unit == G1
    (queue.root / "prelaunch-turn" / "turn-0.json").write_text("{torn", encoding="utf-8")
    assert pg.current_turn(queue)["status"] == "unknown"
    outcome = pg.take_turn(queue, [_ticket(G2, 9, 20.0, tA=4, tB=2)])
    assert outcome.status == "unknown" and outcome.won is False


def test_reserved_receipts_and_done_gate_epochs(queue) -> None:
    """Reserved markers accumulate; only a done epoch opens the next."""
    assert pg.take_turn(queue, [_ticket(G1, 1, 10.0, tA=4, tB=2)]).unit == G1
    assert pg.record_reserved(queue, 0, "tier-a") is True
    assert pg.record_reserved(queue, 0, "tier-b") is True
    assert pg.record_reserved(queue, 0, "tier-a") is True
    current = pg.current_turn(queue)
    assert current["status"] == "open"
    assert sorted(current["reserved"]) == ["tier-a", "tier-b"]
    blocked = pg.take_turn(queue, [_ticket(G2, 9, 20.0, tA=4, tB=2)])
    assert (blocked.status, blocked.unit) == ("standing", G1)
    assert pg.finish_turn(queue, 0, "complete") is True
    assert pg.finish_turn(queue, 0, "complete") is True
    done = pg.current_turn(queue)
    assert done["status"] == "done" and done["reason"] == "complete"
    nxt = pg.take_turn(queue, [_ticket(G2, 9, 20.0, tA=4, tB=2)])
    assert (nxt.epoch, nxt.unit) == (1, G2)


def test_ticket_is_immutable(queue) -> None:
    """A second filing with new terms refuses; the first stands."""
    assert pg.turn_ticket(queue, G1, 1, 10.0, {"tA": 4}) is True
    assert pg.turn_ticket(queue, G1, 1, 10.0, {"tA": 4}) is True
    with pytest.raises(pool.PoolContractError):
        pg.turn_ticket(queue, G1, 9, 10.0, {"tA": 4})
    body = json.loads((queue.root / "prelaunch-turn" / "tickets" / f"{G1}.json").read_text())
    assert body["priority"] == 1


def test_no_turn_without_tickets(queue) -> None:
    """An empty ranking opens nothing."""
    assert pg.take_turn(queue, []) == pg.TakeTurn(None, None, False, "no-candidate")
    assert pg.current_turn(queue)["status"] == "none"
