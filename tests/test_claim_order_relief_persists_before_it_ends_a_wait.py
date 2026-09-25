"""A claim-order relief ends a wait only once it persisted, and never stays exempt unbounded (#1037).

#1022 ranks the claimed consumers of an over-committed stage tier and makes
the head's room from what is ranked after it.  Its deadlock-freedom argument
(I5, ``docs/design.md``) left three relief outcomes uncovered:

* ``short`` in which every candidate declined -- a pinned copy, the head's
  own copy, a copy still being copied.  Nothing is evicted, the head does
  not publish, and ``stuck_victim`` wanted ``futile``: nobody was ended and
  the head and the consumers behind it stayed exempt with no bound.
* ``refused`` and ``unknown``.  Round 3 of #1022 made them end every
  standing's exemption (``CLAIM_ORDER_RELIEF_ENDS_WAIT``), but the tier loop
  stamped either on a single failed read, so a consumer whose rung checked
  during that one cycle was ended by a one-cycle fault.

After the fix the record carries the relief with its age
(``relief_since_unix``), what the pass evicted (``relief_evicted_gib``) and
what this cycle observed (``relief_observed``); a ``refused`` or ``unknown``
is stamped as the relief only once a second consecutive record observed
one, and a ``short`` in which everything declined is futile for the stuck
rule once it has lasted the judged wait's own evidence window.

Everything runs on ``tmp_path`` queues and stage roots (#628).
"""
from __future__ import annotations

from pathlib import Path
import sys
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from prismabuild import pool, reader_lease, window_credit  # noqa: E402
import tier_loop  # noqa: E402
from test_a_consumer_stages_only_to_its_refill_horizon import _cycle  # noqa: E402
from test_a_resident_range_is_adopted_rather_than_recopied import TIER  # noqa: E402
from test_claimed_consumers_drain_a_tier_in_admission_order import (  # noqa: E402
    AHEAD, GRACE_S, LAST, PHASES, TWO_HOLD, _ahead_running, _file_stuck_order,
    _fixture, _key, _manifest, _mover)
from test_a_staged_wait_is_not_no_progress import (  # noqa: E402
    _judge, _verdict_fixture)


def _order(queue: pool.PoolQueue) -> dict[str, object]:
    record = queue.tier_commitment(TIER) or {}
    order = record.get("claim_order")
    assert isinstance(order, dict), record
    return order


# ------------------------------------------------ short, every candidate declined


def _pin_everything_held(queue: pool.PoolQueue, count: int) -> None:
    """Every landed range on the tier is pinned by its reader: every relief
    candidate declines whole (``stage_release.evict`` with ``whole``)."""

    for n in range(count):
        for name in TWO_HOLD:
            phase = PHASES[name]
            pinned = reader_lease.acquire(
                queue, consumer_action_key=_key(n),
                attempt={"nonce": f"n{n}{name}", "scope_id": f"s{n}{name}"},
                tier_id=TIER, epoch="",
                span={"start_bytes": int(phase["start_bytes"]),
                      "end_bytes": int(phase["end_bytes"])},
                holder={"host": "test-host", "pid": 4242},
                acquire_token=f"t{n}{name}",
                covers=[{"mover_action_key": _mover(n, name),
                         "manifest_sha256": _manifest(n)}])
            assert pinned.get("pin_id"), pinned


def test_a_short_relief_in_which_everything_declined_carries_its_age(
        tmp_path: Path) -> None:
    """The acceptance fixture's two R12s on a shrunk tier, with every range
    they hold pinned: the head's room can only come from those ranges, and
    every one declines.  Two cycles.  The record says ``short`` with nothing
    evicted, and the age runs from the first cycle rather than restarting,
    so the stuck rule names a consumer once the state has lasted a window
    and not before."""

    queue, stage, _plans, shrunk = _fixture(tmp_path, 2, TWO_HOLD)
    _pin_everything_held(queue, 2)

    _cycle(queue, stage, gib=shrunk)
    first = _order(queue)
    _cycle(queue, stage, gib=shrunk)
    second = _order(queue)

    assert (first.get("relief"), second.get("relief")) == ("short", "short"), second
    assert second.get("relief_evicted_gib") == 0, second
    since = second.get("relief_since_unix")
    assert isinstance(since, float), second
    assert since == first.get("relief_since_unix"), (first, second)
    # Nobody is published, nobody is granted: a stuck order once it lasts.
    assert window_credit.stuck_victim(
        second, now=since + GRACE_S - 1.0, stall_bound_s=GRACE_S) is None
    assert window_credit.stuck_victim(
        second, now=since + GRACE_S, stall_bound_s=GRACE_S) == _key(1), second


def _file_short(queue: pool.PoolQueue, key: str, *, rank: int, age_s: float,
                evicted_gib: int = 0) -> None:
    """``_file_stuck_order`` with a ``short`` relief ``age_s`` old."""

    _file_stuck_order(queue, key, rank=rank)
    record = queue.tier_commitment(TIER)
    assert record is not None
    order = dict(record["claim_order"])                    # type: ignore[arg-type]
    order.update({"relief": "short", "relief_observed": "short",
                  "relief_evicted_gib": evicted_gib,
                  "relief_since_unix": time.time() - age_s})
    queue.file_tier_commitment({**record, "claim_order": order})


def _single_head(queue: pool.PoolQueue, key: str, *, age_s: float) -> None:
    """The head alone in the rank, blocked, every candidate declining."""

    queue.file_tier_commitment({
        "tier_id": TIER, "capacity_gib": 565, "committed_gib": 726,
        "over_committed_gib": 161,
        "claim_order": {"head": key, "relief": "short",
                        "relief_observed": "short", "relief_evicted_gib": 0,
                        "relief_since_unix": time.time() - age_s,
                        "entries": [{"consumer": key, "rank": 0,
                                     "standing": window_credit.CLAIM_HEAD,
                                     "ahead": None, "need_gib": 22,
                                     "blocked": True}]}})


def test_a_head_alone_whose_relief_every_candidate_declines_is_ended_past_its_window(
        tmp_path: Path) -> None:
    """The issue's red: the head is the one ranked consumer, blocked, and
    every relief candidate has declined for longer than its evidence window.
    Before the fix its standing kept it exempt with no bound; now the stuck
    rule names it, and the verdict names the relief and its age."""

    queue, item, _row, progress_path = _verdict_fixture(tmp_path, over_committed_gib=161)
    key = str(item["action_key"])
    _single_head(queue, key, age_s=GRACE_S + 5.0)

    verdict = _judge(queue, item, progress_path, prior=None, window_s=GRACE_S)

    assert verdict["exempt"] is False, verdict
    claim = verdict.get("claim_order") or {}
    assert (claim.get("relief"), claim.get("relief_evicted_gib"),
            claim.get("stuck_victim")) == ("short", 0, key), claim
    assert claim.get("relief_age_s", 0.0) >= GRACE_S, claim


@pytest.mark.parametrize("age_s,evicted_gib", [
    # Declining for less than the window: a pin its reader drops clears it.
    (GRACE_S - 30.0, 0),
    # Short, but the pass evicted something: room is being made.
    (GRACE_S + 30.0, 22),
], ids=["young", "evicting"])
def test_a_short_relief_that_is_young_or_evicting_keeps_the_head_exempt(
        tmp_path: Path, age_s: float, evicted_gib: int) -> None:
    queue, item, _row, progress_path = _verdict_fixture(tmp_path, over_committed_gib=161)
    key = str(item["action_key"])
    _single_head(queue, key, age_s=age_s)
    if evicted_gib:
        record = queue.tier_commitment(TIER)
        assert record is not None
        record["claim_order"]["relief_evicted_gib"] = evicted_gib  # type: ignore[index]
        queue.file_tier_commitment(record)

    verdict = _judge(queue, item, progress_path, prior=None, window_s=GRACE_S)

    assert verdict["exempt"] is True, verdict
    assert "stuck_victim" not in (verdict.get("claim_order") or {}), verdict


@pytest.mark.parametrize("rank,exempt", [(0, True), (1, True), (2, False)],
                         ids=["head", "middle", "lowest"])
def test_a_stalled_short_relief_ends_only_the_lowest_ranked_consumer(
        tmp_path: Path, rank: int, exempt: bool) -> None:
    """At most one consumer a cycle, as for ``futile`` (I4): three blocked,
    nobody granted, every candidate declining past the window.  Only the
    lowest-ranked is ended; the head and the middle keep their answers."""

    queue, item, _row, progress_path = _verdict_fixture(tmp_path, over_committed_gib=161)
    key = str(item["action_key"])
    _file_short(queue, key, rank=rank, age_s=GRACE_S + 5.0)
    _ahead_running(queue, quiet_s=60.0, accepted_count=3)

    verdict = _judge(queue, item, progress_path, prior=None, window_s=GRACE_S)

    victim = key if rank == 2 else LAST
    claim = verdict.get("claim_order") or {}
    assert verdict["exempt"] is exempt, verdict
    assert claim.get("stuck_victim") == victim, verdict
    assert claim.get("relief") == "short", claim


# ------------------------------------------------ refused / unknown: one cycle is carried


def _refuse(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        tier_loop.stage_release, "stage_root_refusal",
        lambda _queue, _root: "stage_root_marker_unreadable: [Errno 5] EIO")


def _ledger_unreadable_in_relief(monkeypatch: pytest.MonkeyPatch) -> None:
    """``ledger.available()`` raises inside the relief pass only."""

    original = pool.ResourceLedger.available

    def available(self, *args, **kwargs):
        if sys._getframe(1).f_code.co_name == "evict_beyond_horizon":
            raise OSError(5, "EIO")
        return original(self, *args, **kwargs)

    monkeypatch.setattr(pool.ResourceLedger, "available", available)


FAULTS = {"refused": _refuse, "unknown": _ledger_unreadable_in_relief}


@pytest.mark.parametrize("fault", sorted(FAULTS))
def test_a_one_cycle_relief_fault_ends_no_wait_and_a_persistent_one_does(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fault: str) -> None:
    """Two R12s on a tier too small for either's range: relief is futile and
    the head is exempt by its standing.  One cycle's relief read then fails.  Before the fix the record said
    ``refused`` or ``unknown`` and the head's wait ended on that single
    fault; now the record carries ``futile`` for that cycle and says what it
    observed.  A second failing cycle ends the wait, with the reason named."""

    queue, stage, _plans, _shrunk = _fixture(tmp_path, 2, ())
    _cycle(queue, stage, gib=30)
    healthy = _order(queue)
    head = str(healthy.get("head"))
    assert healthy.get("relief") == "futile", healthy
    _value, before = queue._tier_commitment_standing(TIER, head, now=time.time())
    assert before is not None and before["exempt"] is True, before

    with monkeypatch.context() as patch:
        FAULTS[fault](patch)
        _cycle(queue, stage, gib=30)
        once = _order(queue)
        _value, after_one = queue._tier_commitment_standing(TIER, head, now=time.time())

        assert after_one is not None and after_one["exempt"] is True, after_one
        assert once.get("relief_observed") == fault, once
        assert once.get("relief") == "futile", once
        assert (after_one.get("relief_observed"), after_one.get("relief_carried")) \
            == (fault, True), after_one

        _cycle(queue, stage, gib=30)
        twice = _order(queue)
        _value, after_two = queue._tier_commitment_standing(TIER, head, now=time.time())

    assert twice.get("relief") == fault, twice
    assert twice.get("relief_since_unix") == once.get("relief_observed_since_unix"), twice
    assert after_two is not None and after_two["exempt"] is False, after_two
    assert f"relief {fault}" in str(after_two.get("reason")), after_two
    if fault == "refused":
        assert "stage_root_marker_unreadable" in str(after_two.get("reason")), after_two


def test_the_head_waits_on_the_observed_relief_while_one_is_carried(
        tmp_path: Path) -> None:
    """A carried relief says what the rungs read, never whether the head's
    room exists: the head publishes only on what this cycle's pass saw."""

    order: dict[str, object] = {"head": AHEAD, "entries": []}
    tier_loop._stamp_relief(order, "unknown",
                            previous={"head": AHEAD, "relief": "evicted",
                                      "relief_observed": "evicted",
                                      "relief_since_unix": 100.0,
                                      "relief_observed_since_unix": 100.0,
                                      "relief_evicted_gib": 22},
                            now=200.0)

    assert (order["relief"], order["relief_observed"], order["relief_carried"],
            order["relief_since_unix"]) == ("evicted", "unknown", True, 100.0), order
    assert tier_loop._relief_observed(order) not in tier_loop.RELIEF_MADE_ROOM


def test_alternating_faults_persist_rather_than_carry_for_ever() -> None:
    """``refused`` then ``unknown``: two failed reads in a row, whichever each
    was, persist."""

    previous: dict[str, object] = {"head": AHEAD, "entries": []}
    tier_loop._stamp_relief(previous, "futile", now=100.0)
    tier_loop._stamp_relief(first := {"head": AHEAD, "entries": []}, "refused",
                            previous=previous, now=105.0, refusal="x")
    tier_loop._stamp_relief(second := {"head": AHEAD, "entries": []}, "unknown",
                            previous=first, now=110.0)

    assert first["relief"] == "futile", first
    assert (second["relief"], second["relief_since_unix"]) == ("unknown", 105.0), second


def test_a_fault_between_declining_cycles_does_not_restart_the_stall() -> None:
    """``short`` with nothing evicted, one failed read, ``short`` again: the
    fault cycle carries the stall, so an alternation of faults and declines
    cannot keep the age from growing."""

    first: dict[str, object] = {"head": AHEAD, "entries": []}
    tier_loop._stamp_relief(first, "short", now=100.0, evicted_gib=0)
    fault: dict[str, object] = {"head": AHEAD, "entries": []}
    tier_loop._stamp_relief(fault, "unknown", previous=first, now=105.0)
    again: dict[str, object] = {"head": AHEAD, "entries": []}
    tier_loop._stamp_relief(again, "short", previous=fault, now=110.0, evicted_gib=0)
    freed: dict[str, object] = {"head": AHEAD, "entries": []}
    tier_loop._stamp_relief(freed, "short", previous=again, now=115.0, evicted_gib=22)

    assert (fault["relief"], fault["relief_evicted_gib"]) == ("short", 0), fault
    assert again["relief_since_unix"] == 100.0, again
    assert freed["relief_since_unix"] == 115.0, freed
