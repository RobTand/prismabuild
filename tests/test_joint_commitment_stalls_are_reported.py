"""Joint-commitment stalls and over-committed stage tiers are reported (#930).

#907 refuses a newcomer whose read footprint does not fit beside what the
stage has already promised.  The refusal printed one ``window-gated`` event
with the totals, and nothing said which holders or windows made up the gap,
whether any of them could be evicted, or that a tier was promised more than
it has.  A wait nobody reports is a defect under the fleet's closed-loop
observability rule.

The tier loop now files, per stage tier and per cycle, what its commitment
census saw: every holder with the reason it is or is not evictable, every
window's growth, and every newcomer it refused, with the terms of the gap.
``pbstatus --starvation`` reads that record.  An over-committed tier is said
on every cycle while it lasts.

The numbers are those of ``test_admission_charges_refill_horizons_jointly.py``:
a claimed reader's footprint is 12 GiB and a newcomer's 14 GiB.

Everything runs on ``tmp_path`` queues and stage roots (#628).
"""
from __future__ import annotations

import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from prismabuild import pool, window_credit  # noqa: E402
import pbstatus  # noqa: E402
import tier_loop  # noqa: E402
from test_a_resident_range_is_adopted_rather_than_recopied import (  # noqa: E402
    TIER, _hexkey)
from test_a_consumer_stages_only_to_its_refill_horizon import (  # noqa: E402
    _fixture_queue)
from test_admission_charges_refill_horizons_jointly import (  # noqa: E402
    NEWCOMER_FOOTPRINT, READER_FOOTPRINT, Consumer, _reader, _refused, _tiers)

STATIC = _hexkey("static")


def _record(queue: pool.PoolQueue) -> dict[str, object]:
    record = queue.tier_commitment(TIER)
    assert isinstance(record, dict), record
    assert record["tier_id"] == TIER
    return record


def _starvation_tier(queue: pool.PoolQueue) -> dict[str, object]:
    blob = pbstatus.read_starvation(queue.root)
    tiers = [tier for tier in blob["tiers"] if tier["tier_id"] == TIER]
    assert len(tiers) == 1, blob["tiers"]
    return tiers[0]


def _term(terms, **match) -> dict[str, object]:
    found = [term for term in terms
             if all(term.get(field) == value for field, value in match.items())]
    assert len(found) == 1, (match, terms)
    return found[0]


# ------------------------------------------------ a newcomer behind a holder


def test_a_newcomer_behind_a_holder_nothing_can_evict_names_it(
        tmp_path: Path) -> None:
    """The acceptance fixture: the waiting newcomer and the holder, by name.

    10 GiB held by a holder with no receipt and no live owner (the live
    ``6fbc96301c6c`` shape), a reader at its 12 GiB footprint and a newcomer
    whose footprint is 14 GiB, on a 30 GiB stage: 10 + 12 + 14 = 36, a gap
    of 6 GiB.  Neither the holder nor the reader's in-horizon legs can be
    evicted.
    """

    queue, stage = _fixture_queue(tmp_path, 30)
    assert queue.tier_ledger(TIER).acquire(STATIC, {"stage_gib": 10})
    reader = _reader(queue, stage)
    newcomer = Consumer(queue, stage, "n")

    events = tier_loop.residency_window(queue, tiers=_tiers(stage))

    terms = _refused(events, newcomer.key)
    assert terms["shortfall_gib"] == 10 + READER_FOOTPRINT + NEWCOMER_FOOTPRINT - 30
    static = _term(terms["terms"], holder=STATIC)
    assert static == {"basis": "receipt-less", "holder": STATIC, "gib": 10,
                      "holders": 1, "evictable": False}

    tier = _starvation_tier(queue)
    assert tier["commitment_age_s"] is not None
    commitment = tier["commitment"]
    waiting = _term(commitment["waiting"], consumer=newcomer.key)
    assert waiting["reason"] == window_credit.REASON_COMMITMENT
    assert waiting["footprint_gib"] == NEWCOMER_FOOTPRINT
    assert waiting["committed_gib"] == 10 + READER_FOOTPRINT
    assert waiting["capacity_gib"] == 30
    assert _term(waiting["terms"], holder=STATIC)["evictable"] is False
    legs = _term(waiting["terms"], consumer=reader.key, basis="in-horizon-leg")
    assert legs["gib"] == READER_FOOTPRINT and legs["evictable"] is False

    blob = pbstatus.read_starvation(queue.root)
    named = [entry for entry in blob["joint_commitment_waits"]
             if entry["consumer"] == newcomer.key]
    assert len(named) == 1
    assert named[0]["tier_id"] == TIER
    assert _term(named[0]["terms"], holder=STATIC)["gib"] == 10


def test_a_newcomer_behind_a_waiting_window_is_named_with_it(
        tmp_path: Path) -> None:
    """The transitive wait: behind a higher-priority newcomer that waits.

    The lower-priority newcomer is not refused by the commitment itself; it
    waits behind one that is.  Both are named, and the second says on whom
    it waits and why.
    """

    queue, stage = _fixture_queue(tmp_path, 30)
    assert queue.tier_ledger(TIER).acquire(STATIC, {"stage_gib": 10})
    _reader(queue, stage)
    first = Consumer(queue, stage, "n")
    item = queue.item_path(pool.READY, first.key)
    body = json.loads(item.read_text())
    body["priority"] = 5
    item.write_text(json.dumps(body))
    second = Consumer(queue, stage, "m")

    tier_loop.residency_window(queue, tiers=_tiers(stage))

    waiting = _record(queue)["waiting"]
    assert _term(waiting, consumer=first.key)["reason"] == (
        window_credit.REASON_COMMITMENT)
    behind = _term(waiting, consumer=second.key)
    assert behind["reason"] == "higher-priority-window-waiting"
    assert behind["waiting_on"] == {
        "consumer": first.key, "reason": window_credit.REASON_COMMITMENT}


# ---------------------------------------------------- an over-committed tier


def _over_committed(events) -> list[dict[str, object]]:
    return [event for event in events
            if event.get("event") == "tier-over-committed"]


def test_an_over_committed_tier_is_reported_every_cycle_while_it_lasts(
        tmp_path: Path) -> None:
    """A reader promised 12 GiB beside 10 GiB nothing evicts, on 20 GiB.

    The reader holds its 2 GiB lead and has 10 GiB still to grow into: 22 of
    20 promised, with no newcomer asking.  Said on each cycle, with the
    terms, until the holder goes.
    """

    queue, stage = _fixture_queue(tmp_path, 20)
    assert queue.tier_ledger(TIER).acquire(STATIC, {"stage_gib": 10})
    reader = _reader(queue, stage, landed=(0,))

    for _cycle in range(2):
        events = tier_loop.residency_window(queue, tiers=_tiers(stage))
        [event] = _over_committed(events)
        assert event["tier_id"] == TIER
        assert event["committed_gib"] == 10 + READER_FOOTPRINT
        assert event["capacity_gib"] == 20
        assert event["over_committed_gib"] == 10 + READER_FOOTPRINT - 20
        assert _term(event["terms"], holder=STATIC)["evictable"] is False
        growth = _term(event["terms"], consumer=reader.key, basis="window-growth")
        assert growth["evictable"] is False

    commitment = _starvation_tier(queue)["commitment"]
    assert commitment["over_committed_gib"] == 10 + READER_FOOTPRINT - 20
    assert commitment["waiting"] == []

    queue.tier_ledger(TIER).release(STATIC)
    events = tier_loop.residency_window(queue, tiers=_tiers(stage))
    assert _over_committed(events) == []
    assert _record(queue)["over_committed_gib"] == 0


def test_a_quiet_tier_files_its_record_and_says_nothing(tmp_path: Path) -> None:
    """No wait and no over-commitment: a fresh record, and no event."""

    queue, stage = _fixture_queue(tmp_path, 30)
    _reader(queue, stage)

    events = tier_loop.residency_window(queue, tiers=_tiers(stage))

    assert _over_committed(events) == []
    record = _record(queue)
    assert record["over_committed_gib"] == 0 and record["waiting"] == []
    assert record["committed_gib"] == READER_FOOTPRINT


# ------------------------------------------------------- every token, once


def test_every_held_token_is_classified_once(tmp_path: Path) -> None:
    """The per-holder detail sums to the census's own terms.

    One of each shape: a reader's passed legs and in-horizon legs, an orphan
    whose receipt names a consumer that is gone, and a receipt-less holder.
    """

    queue, stage = _fixture_queue(tmp_path, 40)
    assert queue.tier_ledger(TIER).acquire(STATIC, {"stage_gib": 3})
    reader = Consumer(queue, stage, "a", read_mb_s=None)
    reader.land(0, 1, 2, 3)
    reader.land(7, consumer=_hexkey("gone"))
    reader.claim("phase-2")
    tiers = _tiers(stage)
    unknown: list[dict[str, object]] = []
    consumers = tier_loop._planned_consumers(queue, tiers, unknown=unknown)

    census = tier_loop._commitment_census(queue, tiers, consumers=consumers,
                                          unknown=unknown)[TIER]

    holders = census["holders"]
    assert sum(entry["gib"] for entry in holders) == census["held_gib"]
    assert (sum(entry["gib"] for entry in holders if entry["evictable"])
            == census["evictable_gib"])
    bases = {entry["key"]: (entry["basis"], entry["evictable"])
             for entry in holders}
    assert bases[STATIC] == ("receipt-less", False)
    assert bases[reader.mover(0)] == ("passed-leg", True)
    assert bases[reader.mover(2)] == ("in-horizon-leg", False)
    assert bases[_hexkey("donora7")] == ("orphan", True)
    assert census["committed_gib"] == (
        census["held_gib"] - census["evictable_gib"] + census["queued_gib"]
        + census["unheld_output_gib"]
        + sum(window["growth_gib"] for window in census["windows"].values()
              if not window["newcomer"]))
