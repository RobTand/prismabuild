"""A lost claim is credited to the box that won it, not to one that raced it.

``claim_intent_host`` recovers a holder from a marker written *before* the
rename that decides ownership, so it names a **claimant**, not the winner.
``_write_claim_intent`` writes by rename and therefore replaces whatever is
there, which produces this order:

1. ``W`` writes intent naming ``W`` and wins the rename.
2. ``L`` writes intent naming ``L``, overwriting ``W``'s, then loses and moves on.
3. ``W`` dies before it rewrites the claimed record.

Both markers pass the generation check, so the recovery answered ``L``: the
terminal record named a box that never had the work, and the release moved
nothing because ``L`` holds nothing under that key -- leaving ``W``'s tokens
committed, which is the leak #261 exists to close (#272).

The fix is to stop using a proxy where the fact itself is on disk. A
claimant's tokens are taken into a private ``held/<handle>``, exactly because
the owner is undecided while they are taken; ``commit_acquire`` is what moves
them to ``held/<action_key>`` and is called by the winner of the rename and by
nobody else.  So ``reservations/<host>/held/<key>/`` is the rename's own
effect, and a loser cannot produce one.

Issue #272.
"""
from __future__ import annotations

import json
from pathlib import Path
import socket
import sys
from unittest import mock

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))

from prismabuild import pool  # noqa: E402

KEY = "f" * 64
WON_BY = f"won-{socket.gethostname()}"
LOST_BY = f"lost-{socket.gethostname()}"
DEMAND = {"cpu": 1}


class _ClaimantDied(RuntimeError):
    """The winner, between the rename and the record rewrite."""


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    q = pool.PoolQueue(tmp_path / "queue")
    q.publish(
        action_key=KEY, cas_root=q.root / "cas", checkout_root=q.root / "co",
        worker_script=q.root / "worker.py", resources=DEMAND,
        max_attempts=1, retry_safe=True,
    )
    return q


def _win_but_die_before_naming_itself(q: pool.PoolQueue) -> None:
    claimed_path = q.item_path(pool.CLAIMED, KEY)
    real = pool._write_json_atomic

    def dies(path, payload):
        if Path(path) == claimed_path and "claimed_host" in payload:
            raise _ClaimantDied(path)
        return real(path, payload)

    with mock.patch.object(pool.socket, "gethostname", lambda: WON_BY):
        with mock.patch.object(pool, "_write_json_atomic", dies):
            with pytest.raises(_ClaimantDied):
                q.claim(owner=f"{WON_BY}:1:aaaa0001", capacity=DEMAND)

    assert q.ledger(WON_BY).held() == DEMAND, "the winner committed nothing"
    assert not q.item_path(pool.READY, KEY).exists(), "the rename did not happen"


def _a_second_box_races_and_loses(q: pool.PoolQueue, stale: list) -> None:
    """``LOST_BY`` claims from a ready listing taken before the winner's rename.

    That listing is the race.  Both boxes scan ``ready`` and one of them then
    renames; the other is already past its scan and walks the item it saw, so
    it writes its intent and only discovers the loss at its own rename.  A
    loser that re-listed first would never reach the marker at all, and a
    test that wrote the marker by hand would assert the writing rather than
    the order ``_claim`` writes in.

    Simulated by handing ``_claim`` the stale listing, which is exactly what
    the shared mount hands a second box.
    """

    with mock.patch.object(pool.socket, "gethostname", lambda: LOST_BY):
        with mock.patch.object(pool.PoolQueue, "ready_items", lambda self: stale):
            assert q.claim(owner=f"{LOST_BY}:1:bbbb0002", capacity=DEMAND) is None
    assert q.ledger(LOST_BY).held() == {}, "the loser kept tokens"


def _the_race(q: pool.PoolQueue) -> None:
    """The whole interleaving, in the order the two boxes produce it."""

    stale = q.ready_items()          # both boxes have listed ``ready``
    _win_but_die_before_naming_itself(q)
    _a_second_box_races_and_loses(q, stale)


def _record(q: pool.PoolQueue) -> dict:
    return json.loads(q.item_path(pool.CLAIMED, KEY).read_text())


def _reap_on_a_third_box(q: pool.PoolQueue) -> list[str]:
    later = pool._now() + pool.LEASE_TIMEOUT_S
    with mock.patch.object(pool, "_now", lambda: later):
        return q.reap_stale(timeout_s=-1)


def test_the_ledger_names_the_winner_and_only_the_winner(queue) -> None:
    _the_race(queue)

    assert queue.claim_reservation_hosts(KEY) == [WON_BY]
    assert queue.resolve_claim_holder(KEY, _record(queue)) == WON_BY


def test_the_losers_marker_does_not_become_the_answer(queue) -> None:
    """The regression itself: the proxy could name the box that lost."""

    _the_race(queue)

    assert queue.resolve_claim_holder(KEY, _record(queue)) != LOST_BY


def test_a_loser_removes_the_marker_it_wrote(queue) -> None:
    stale = queue.ready_items()
    _win_but_die_before_naming_itself(queue)
    assert queue.claim_intent_host(KEY, _record(queue)) == WON_BY

    _a_second_box_races_and_loses(queue, stale)

    # Either the winner's marker survives or none does.  What must never
    # survive is the loser's.
    assert queue.claim_intent_host(KEY, _record(queue)) in (WON_BY, None)


def test_a_discard_only_ever_removes_its_own_marker(queue) -> None:
    """The ``owner`` check is the whole safety of the discard.

    A claimant that unlinked whatever it found would delete the winner's
    marker as readily as its own -- the very confusion the discard exists to
    stop -- so the check is asserted directly rather than through a race whose
    outcome the ledger would rescue anyway.
    """

    _win_but_die_before_naming_itself(queue)
    assert queue.claim_intent_host(KEY, _record(queue)) == WON_BY

    queue._discard_claim_intent(KEY, owner=f"{LOST_BY}:1:bbbb0002")

    assert queue.claim_intent_host(KEY, _record(queue)) == WON_BY, (
        "a claimant discarded a marker that names another box")

    queue._discard_claim_intent(KEY, owner=f"{WON_BY}:1:aaaa0001")

    assert queue.claim_intent_host(KEY, _record(queue)) is None, (
        "the owner check refused the marker's own owner")


def test_a_stale_losing_marker_does_not_outrank_the_ledger(queue) -> None:
    """The residual race, and the reason the ledger is read first.

    A loser removes its own marker, but it can die between losing the rename
    and doing so -- and the compare-and-delete is a read then an unlink on a
    shared mount, so it has a window of its own.  Either way the marker left
    behind names the box that lost, this generation, passing every check
    ``claim_intent_host`` makes.

    The reservation cannot be wrong in the same way: it is the effect of the
    rename rather than a claimant's statement before it.  So the order between
    the two is what decides the answer here, and this pins it.
    """

    _the_race(queue)
    with mock.patch.object(pool.socket, "gethostname", lambda: LOST_BY):
        queue._write_claim_intent(KEY, owner=f"{LOST_BY}:1:bbbb0002")
    assert queue.claim_intent_host(KEY, _record(queue)) == LOST_BY, (
        "the marker does not name the loser; this test proves nothing")

    assert queue.resolve_claim_holder(KEY, _record(queue)) == WON_BY
    assert _reap_on_a_third_box(queue) == [KEY]
    assert queue.ledger(WON_BY).held() == {}
    assert queue.ledger(WON_BY).available() == DEMAND


def test_the_winners_tokens_come_back_to_the_winner(queue) -> None:
    """End to end: the reaper credits the box the rename actually chose."""

    _the_race(queue)

    assert _reap_on_a_third_box(queue) == [KEY]

    assert queue.ledger(WON_BY).held() == {}
    assert queue.ledger(WON_BY).available() == DEMAND
    # The loser minted its own token when it tried to claim and handed it back
    # to its own free pool, which is correct and unrelated: what must not have
    # happened is the winner's reservation being released against this ledger.
    assert queue.ledger(LOST_BY).held() == {}
    assert queue.ledger(LOST_BY).available() == DEMAND


def test_the_superseded_record_names_the_winner(queue) -> None:
    _the_race(queue)

    _reap_on_a_third_box(queue)

    named = {
        json.loads(path.read_text()).get("claimed_host")
        for path in sorted(queue.superseded_dir().glob("*.json"))
    }
    assert named == {WON_BY}, f"superseded filings name {named}"


def test_two_ledgers_holding_one_action_is_refused_not_guessed(queue) -> None:
    """A contradiction of ``commit_acquire``'s single-winner rule.

    Fabricated, because the ledger's own invariant is what stops it arising --
    but a resolution that picked one anyway would be choosing at random which
    box to debit, and would do it silently.
    """

    _win_but_die_before_naming_itself(queue)
    (queue.root / pool.RESERVATIONS / LOST_BY / "held" / KEY).mkdir(parents=True)

    assert queue.claim_reservation_hosts(KEY) == sorted([WON_BY, LOST_BY])
    assert queue.resolve_claim_holder(KEY, _record(queue)) is None


def test_a_zero_token_claim_still_falls_back_to_the_marker(queue, tmp_path) -> None:
    """The ledger is silent when there is nothing to hold; the proxy is not.

    That is the one case the marker still decides, and it is also the case
    where naming the wrong box cannot misplace a reservation -- there is none.
    """

    q = pool.PoolQueue(tmp_path / "empty-demand")
    q.publish(
        action_key=KEY, cas_root=q.root / "cas", checkout_root=q.root / "co",
        worker_script=q.root / "worker.py", resources={},
        max_attempts=1, retry_safe=True,
    )
    claimed_path = q.item_path(pool.CLAIMED, KEY)
    real = pool._write_json_atomic

    def dies(path, payload):
        if Path(path) == claimed_path and "claimed_host" in payload:
            raise _ClaimantDied(path)
        return real(path, payload)

    with mock.patch.object(pool.socket, "gethostname", lambda: WON_BY):
        with mock.patch.object(pool, "_write_json_atomic", dies):
            with pytest.raises(_ClaimantDied):
                q.claim(owner=f"{WON_BY}:1:aaaa0001", capacity={})

    record = json.loads(claimed_path.read_text())
    assert q.claim_reservation_hosts(KEY) == []
    assert q.resolve_claim_holder(KEY, record) == WON_BY
