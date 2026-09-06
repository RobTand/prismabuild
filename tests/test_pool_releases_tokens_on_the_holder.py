"""A lost claim's tokens go back to the box that committed them.

``claim`` commits its reservation the moment it wins the ``rename`` -- the
tokens stop belonging to the claimant and start belonging to the action -- and
only *then* rewrites the claimed record with ``claimed_host``.  A claimant that
dies between those two writes leaves a claim that names no box, holding real
capacity under its own box's ledger.

``reap_stale`` releases by action key against ``self.ledger(holder)``, and
``holder`` defaulted to the local hostname whenever the record named nobody.
The reaper is usually not the claimant, so the release named the reaper's
ledger.

What that costs is the opposite of what issue #261 assumed.  ``release`` is
``_empty_into_free(held/<key>)`` and returns 0 when that directory is absent,
so the reaper's free pool does **not** grow by capacity it never had -- it does
not change at all.  The holder's tokens are simply never returned: a
reservation outliving its holder, which is the starvation shape ``reap_stale``
names in its own comment one line above the release.  Both halves are asserted
here, because the wrong half is the one the issue records.

The recovery already existed.  #227 added ``claim_intent_host`` for exactly
this window -- the intent marker is written before the rename and does name the
box -- and stamped it on the record.  This makes the same answer reach the
release, so the ledger the reaper debits and the hostname the record carries
are one box.

Issue #261.
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

KEY = "d" * 64
#: A claiming box that is never the reaping box.  Derived rather than named:
#: the suite runs on the fleet, and a literal hostname is the local host on one
#: of the boxes that runs it -- where holder and reaper collapse into one name
#: and the test stops being able to tell them apart.
HELD_BY = f"not-{socket.gethostname()}"
DEMAND = {"cpu": 1}


class _ClaimantDied(RuntimeError):
    """The claiming process, between the rename and the record rewrite."""


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    return pool.PoolQueue(tmp_path / "queue")


def _publish(q: pool.PoolQueue) -> None:
    q.publish(
        action_key=KEY, cas_root=q.root / "cas", checkout_root=q.root / "co",
        worker_script=q.root / "worker.py", resources=DEMAND,
        max_attempts=1, retry_safe=True,
    )


def _lose_the_record_rewrite(q: pool.PoolQueue) -> None:
    """Claim on ``HELD_BY`` and die before the claimed record is rewritten.

    Injected into the driver rather than hand-written onto the record: the
    point of the window is the *order* ``claim`` writes in, and a record edited
    into shape afterwards would assert the edit rather than the order.  The
    ``claimed`` file left behind here is byte-identical to the ready record,
    which is exactly what ``claim`` leaves when it is interrupted there.
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

    # The window, as the claimant left it.  If any of these stop holding, the
    # window has moved and the assertions below are about something else.
    record = json.loads(claimed_path.read_text())
    assert "claimed_host" not in record, "claim named its box after all"
    assert not q.lease_path(KEY).exists(), "a lease means the rewrite landed"
    assert q.ledger(HELD_BY).held() == DEMAND, (
        "the claimant committed no tokens; there is nothing to misplace")
    assert q.claim_intent_host(KEY, record) == HELD_BY


def _reap_here(q: pool.PoolQueue) -> list[str]:
    """Run the reaper on this box, one lease timeout later.

    The clock moves, not the records: the grace for a claim whose lease has not
    arrived is measured against ``claimed_unix`` and, without one, against the
    intent marker, and rewriting either by hand would put them in an order
    ``claim`` never produces.
    """

    later = pool._now() + pool.LEASE_TIMEOUT_S
    with mock.patch.object(pool, "_now", lambda: later):
        return q.reap_stale(timeout_s=-1)


def _same_generation_record(q: pool.PoolQueue) -> dict:
    published = json.loads(q.item_path(pool.CLAIMED, KEY).read_text())
    return {"action_key": KEY, "published_unix": published["published_unix"]}


def test_the_holders_tokens_come_back_to_the_holder(
    queue: pool.PoolQueue,
) -> None:
    """The release names the box that committed the tokens."""

    _publish(queue)
    _lose_the_record_rewrite(queue)

    assert _reap_here(queue) == [KEY]

    assert queue.ledger(HELD_BY).held() == {}, (
        "a reservation outlived its holder: the reaper released against its "
        "own ledger and the claiming box still holds the tokens")
    assert queue.ledger(HELD_BY).available() == DEMAND


def test_the_reaper_gains_no_capacity_it_never_had(
    queue: pool.PoolQueue,
) -> None:
    """The correction to #261: the reaper's pool does not grow.  It cannot.

    ``release`` moves tokens out of ``held/<key>`` and there is no such
    directory on the reaper, so the misdirected call was a no-op rather than a
    credit.  Asserted on both branches so the ledger cannot be "fixed" by
    minting the reaper the capacity the issue believed it had gained.
    """

    _publish(queue)
    _lose_the_record_rewrite(queue)
    before = queue.ledger().capacity()

    _reap_here(queue)

    assert queue.ledger().capacity() == before == {}
    assert queue.ledger().held() == {}


def test_an_unknown_holder_releases_nowhere_it_can_name(
    queue: pool.PoolQueue,
) -> None:
    """No marker, no holder, no invention.

    Without the intent marker the window leaves nothing that names a box, and
    the honest answer is the one the queue already gives: fall back to the
    local ledger, where the release finds nothing and moves nothing.

    Those tokens stay stranded, and no automatic path returns them.
    ``sweep_stale_acquisitions`` frees tokens a claimant took and *never*
    committed -- private ``begin_acquire`` directories -- and these are
    committed under ``held/<key>``, the exact shape its own docstring says
    ``reap_stale``'s release by key cannot see.  ``reclaim_terminal_reservation``
    refuses too: it wants a single terminal ``done`` record whose
    ``finished_host`` equals the holder, and a reaped claim either has no
    terminal at all or names the reaper there.  So an operator returns them by
    hand.  That is the cost of the missing marker, and it is not a reason to
    guess a holder here.
    """

    _publish(queue)
    _lose_the_record_rewrite(queue)
    queue.item_path(pool.INTENT, KEY).unlink()

    _reap_here(queue)

    assert queue.ledger().capacity() == {}, "the reaper invented a holder"
    assert queue.ledger(HELD_BY).held() == DEMAND, (
        "tokens moved against a ledger nothing named")


def test_a_claim_already_filed_elsewhere_still_credits_its_holder(
    queue: pool.PoolQueue,
) -> None:
    """The terminal-claim branch releases against the same resolved holder."""

    _publish(queue)
    _lose_the_record_rewrite(queue)
    filed = _same_generation_record(queue)
    filed["status"] = "executed"
    pool._write_json_atomic(queue.item_path(pool.DONE, KEY), filed)

    _reap_here(queue)

    assert not queue.item_path(pool.CLAIMED, KEY).exists()
    assert queue.ledger(HELD_BY).held() == {}
    assert queue.ledger(HELD_BY).available() == DEMAND


def test_a_withdrawn_claim_still_credits_its_holder(
    queue: pool.PoolQueue,
) -> None:
    """The withdrawal branch releases against the same resolved holder."""

    _publish(queue)
    _lose_the_record_rewrite(queue)
    marker = _same_generation_record(queue)
    marker["withdrawn_unix"] = pool._now()
    pool._write_json_atomic(queue.item_path(pool.WITHDRAWN, KEY), marker)

    _reap_here(queue)

    assert not queue.item_path(pool.CLAIMED, KEY).exists()
    assert queue.ledger(HELD_BY).held() == {}
    assert queue.ledger(HELD_BY).available() == DEMAND


def test_the_record_and_the_ledger_name_the_same_box(
    queue: pool.PoolQueue,
) -> None:
    """One resolution, so the two answers cannot disagree.

    #227 stamped the recovered hostname on the record.  Reading it separately
    for the release is how the record came to say ``HELD_BY`` while the tokens
    were released somewhere else, and one holder resolved once is what stops
    that being expressible.
    """

    _publish(queue)
    _lose_the_record_rewrite(queue)

    _reap_here(queue)

    requeued = json.loads(queue.item_path(pool.READY, KEY).read_text())
    superseded = [
        json.loads(path.read_text())
        for path in sorted(queue.superseded_dir().glob("*.json"))
    ]
    named = {record.get("claimed_host") for record in superseded}
    assert named == {HELD_BY}, f"superseded filings name {named}"
    # The requeue strips the holder on its way back to ``ready`` -- it is a new
    # request, and naming a box would be a placement it does not carry.
    assert "claimed_host" not in requeued
    assert queue.ledger(HELD_BY).available() == DEMAND
