"""``withdraw`` must not conclude a claim it cannot name.

Same window as #261, different verb.  ``claim`` commits its reservation when
it wins the ``rename`` and only *then* rewrites the claimed record with
``claimed_host``; the lease is written after that.  A claimant interrupted in
between leaves a claim that names no box while holding real capacity under its
own ledger.

``reap_stale`` learned to recover that name from the intent marker (#227,
#261).  ``withdraw`` resolves the holder from the same two fields and was left
alone, because it is a different verb and wanted its own change (#271).

What the miss costs here is worse than in the reaper, and this file measures
it rather than assuming it.  With ``host`` unresolved, ``withdraw`` reaches
its "nothing left to stop" branch: it releases against the *operator's* ledger
-- which moves nothing, there being no ``held/<key>`` there -- and then
unlinks the claimed record and the lease.  The holder's tokens stay committed
under a key that no longer names anything in the queue, so the reaper has
nothing left to conclude and no automatic path returns them.

Issue #271.
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

KEY = "e" * 64
#: Derived, never a literal: the suite runs on the fleet, and a hardcoded name
#: is the local host on one of the boxes that runs it -- where holder and
#: operator collapse and the test can no longer tell them apart.
HELD_BY = f"not-{socket.gethostname()}"
DEMAND = {"cpu": 1}


class _ClaimantDied(RuntimeError):
    """The claiming process, between the rename and the record rewrite."""


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    q = pool.PoolQueue(tmp_path / "queue")
    q.publish(
        action_key=KEY, cas_root=q.root / "cas", checkout_root=q.root / "co",
        worker_script=q.root / "worker.py", resources=DEMAND,
        max_attempts=1, retry_safe=True,
    )
    return q


def _lose_the_record_rewrite(q: pool.PoolQueue) -> None:
    """Claim on ``HELD_BY`` and die before the claimed record is rewritten.

    Injected into the driver rather than hand-written onto the record: the
    window is defined by the *order* ``claim`` writes in, and a record edited
    into shape afterwards would assert the edit instead of the order.
    """

    claimed_path = q.item_path(pool.CLAIMED, KEY)
    real = pool._write_json_atomic

    def dies_before_naming_itself(path, payload):
        if Path(path) == claimed_path and "claimed_host" in payload:
            raise _ClaimantDied(path)
        return real(path, payload)

    with mock.patch.object(pool.socket, "gethostname", lambda: HELD_BY):
        with mock.patch.object(pool, "_write_json_atomic",
                               dies_before_naming_itself):
            with pytest.raises(_ClaimantDied):
                q.claim(owner=f"{HELD_BY}:1:abcd1234", capacity=DEMAND)

    # The window as the claimant left it.  If these stop holding, the window
    # has moved and everything below is about something else.
    record = json.loads(claimed_path.read_text())
    assert "claimed_host" not in record, "claim named its box after all"
    assert not q.lease_path(KEY).exists(), "a lease means the rewrite landed"
    assert q.ledger(HELD_BY).held() == DEMAND
    assert q.claim_intent_host(KEY, record) == HELD_BY


def _withdraw_here(q: pool.PoolQueue) -> dict:
    return q.withdraw(KEY, by="operator", reason="test")


def test_withdraw_names_the_box_that_holds_the_claim(queue) -> None:
    _lose_the_record_rewrite(queue)

    result = _withdraw_here(queue)

    assert result["host"] == HELD_BY, (
        "the operator was told a holder of None for a claim the marker names")
    assert result["stop_pending"]["holder_host"] == HELD_BY


def test_withdraw_leaves_a_live_holders_claim_for_its_own_box(queue) -> None:
    """A box this one cannot signal keeps its claim, its lease and its tokens.

    This is the branch the missing name skipped.  Unresolved, ``host`` is
    neither "another box" nor a local process, so the verb concluded that
    nothing was left to stop -- and deleted the claim of a holder that may be
    seconds from running the action.
    """

    _lose_the_record_rewrite(queue)

    result = _withdraw_here(queue)

    assert result["released"] == 0
    assert queue.item_path(pool.CLAIMED, KEY).exists(), (
        "the claim of a box this one cannot signal was concluded from here")
    assert queue.ledger(HELD_BY).held() == DEMAND, (
        "the holder's tokens were dropped by an operator on another box")
    claim = json.loads(queue.item_path(pool.CLAIMED, KEY).read_text())
    assert claim["stop_pending"]["holder_host"] == HELD_BY
    # Durable regardless: the operator's decision is filed before any of this.
    assert queue.item_path(pool.WITHDRAWN, KEY).exists()


def test_the_reaper_then_returns_the_tokens_to_that_same_box(queue) -> None:
    """The end of the path: withdraw defers, the reaper concludes, one box.

    ``reap_stale``'s withdrawal branch resolves the holder the same way since
    #261, so deferring here is not deferring forever -- it hands the claim to
    the one mechanism that can conclude it against the box that owns it.
    """

    _lose_the_record_rewrite(queue)
    _withdraw_here(queue)

    later = pool._now() + pool.LEASE_TIMEOUT_S
    with mock.patch.object(pool, "_now", lambda: later):
        queue.reap_stale(timeout_s=-1)

    assert not queue.item_path(pool.CLAIMED, KEY).exists()
    assert queue.ledger(HELD_BY).held() == {}
    assert queue.ledger(HELD_BY).available() == DEMAND


def test_an_unknown_holder_is_still_not_invented(queue) -> None:
    """No marker, no holder.  The fallback stays the honest one."""

    _lose_the_record_rewrite(queue)
    queue.item_path(pool.INTENT, KEY).unlink()

    result = _withdraw_here(queue)

    assert result["host"] is None
    assert queue.ledger().capacity() == {}, "the operator's box invented a holder"


def test_a_ready_item_never_borrows_a_live_claimants_intent(queue) -> None:
    """The marker is written before the rename, so ``ready`` has one too.

    A claimant sitting between its intent and a rename it has not yet won
    leaves this exact state: the item in ``ready``, the marker naming the
    claimant, and that claimant's tokens already acquired.  Recovering a
    holder from it would release tokens out from under a claim that is about
    to succeed, so the recovery is for a CLAIMED record only.
    """

    with mock.patch.object(pool.socket, "gethostname", lambda: HELD_BY):
        queue._write_claim_intent(KEY, owner=f"{HELD_BY}:1:abcd1234")
    ready = json.loads(queue.item_path(pool.READY, KEY).read_text())
    assert queue.claim_intent_host(KEY, ready) == HELD_BY, (
        "the marker does not name the claimant; this test proves nothing")

    result = _withdraw_here(queue)

    assert result["state"] == pool.READY
    assert result["host"] is None, (
        "a ready item borrowed the name of a claimant that holds nothing here")
