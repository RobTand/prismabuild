"""A lost claim is reported against the box that held it, not the one that reaped it.

``reap_stale`` sets ``finished_host`` to its own hostname -- correctly, since
the reaper is who filed the record -- and ``pbrun`` rendered that field as the
place the action failed.  So a claim ``dl380g10`` held and stopped refreshing
was reported as having failed ``on sparky``, and two people spent a day on
sparky for it.

The bias is not cosmetic.  Sparky legitimately runs most of the fleet's work
and therefore reaps most of it, so misattributed failures accumulate on the box
that already looks busiest and the fleet's failure profile acquires a lean
toward whichever box reaps.  That corrupts the evidence used to decide which
box is unhealthy.

Two halves, because rendering the right field is only half of it:

*   ``pbrun`` says which box held the claim and which box concluded it.
*   ``reap_stale`` fills ``claimed_host`` from the claim-intent marker when the
    claim was lost before the claiming box rewrote the record.  A claim must
    not be able to be lost more anonymously than it was taken.

Issue #227.
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
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))

from prismabuild import pool  # noqa: E402

import pbrun  # noqa: E402

KEY = "c" * 64
#: A claiming box that is never the reaping box. Derived rather than named:
#: the suite runs on the fleet, and a literal ``dl380g10`` is the local host
#: on one of the boxes that runs it -- where holder and reaper collapse into
#: one name and the test stops being able to tell them apart.
HELD_BY = f"not-{socket.gethostname()}"


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    return pool.PoolQueue(tmp_path / "queue")


def _lose_a_claim(q: pool.PoolQueue, *, forget_claimed_host: bool = False,
                  forget_intent: bool = False,
                  never_leased: bool = False) -> tuple[Path, dict]:
    """Claim on one box, reap on this one, and return the terminal record.

    By default the claim keeps its lease and the lease goes stale, which is the
    ordinary shape of the loss this issue is about: a box took the work, ran
    it, and stopped refreshing.  ``never_leased`` builds the other shape --
    a lease that never arrived at all -- and stamps the withdrawal that a
    cancelled claim carries, because an un-withdrawn claim with no lease and no
    published attempt is not a lost claim at all: nothing ever ran under it,
    and #222 releases it back to ``ready/`` rather than filing an ending for
    it.  Asking for a terminal record from that shape would be asking the
    reaper to charge an attempt nobody made.
    """

    q.publish(
        action_key=KEY, cas_root=q.root / "cas", checkout_root=q.root / "co",
        worker_script=q.root / "worker.py", resources={"cpu": 1},
        max_attempts=1, retry_safe=True,
    )
    with mock.patch.object(pool.socket, "gethostname", lambda: HELD_BY):
        assert q.claim(owner=f"{HELD_BY}:1:abc", capacity={"cpu": 1}) is not None
    path = q.item_path(pool.CLAIMED, KEY)
    if never_leased:
        # The lease is what says a payload was launched; this one has none.
        q.lease_path(KEY).unlink()
        # ``withdraw`` closes the retry by writing these two onto the live
        # claim.  Written directly here: the subject is what ``pbrun`` renders
        # from the record, not how the record came to be stamped.
        record = json.loads(path.read_text())
        record["max_attempts"] = 1
        record["withdrawn_unix"] = pool._now()
        pool._write_json_atomic(path, record)
    intent_path = q.item_path(pool.INTENT, KEY)
    if forget_claimed_host:
        # The shape of a claim lost between the rename and the record rewrite:
        # the claiming box never got to name itself in the record.
        record = json.loads(path.read_text())
        record.pop("claimed_host", None)
        record.pop("claimed_by", None)
        pool._write_json_atomic(path, record)
    if forget_intent:
        intent_path.unlink(missing_ok=True)

    # Age the claim by moving the reaper's clock, not the records: the grace
    # for a lease that has not arrived yet is measured against ``claimed_unix``
    # and, without one, against the intent marker, and rewriting either by hand
    # would put them in an order ``claim`` never produces.
    later = pool._now() + pool.LEASE_TIMEOUT_S
    with mock.patch.object(pool, "_now", lambda: later):
        # The reaper reports every claim it moved, terminal or requeued.
        assert q.reap_stale(timeout_s=-1) == [KEY]
    failed = q.item_path(pool.FAILED, KEY)
    assert failed.exists()
    record = json.loads(failed.read_text())
    assert record["status"] == "lease_lost_max_attempts"
    return failed, record


def test_the_headline_names_the_holder_and_the_reaper(
    queue: pool.PoolQueue,
) -> None:
    """The box that held the claim leads; the reaper is named as the reaper."""

    path, record = _lose_a_claim(queue)
    summary = pbrun.outcome_summary(queue, path, record)

    assert summary["claimed_host"] == HELD_BY
    assert summary["finished_host"] == socket.gethostname()
    headline = pbrun.outcome_headline(summary)
    assert f"held by {HELD_BY}" in headline
    assert f"reaped by {socket.gethostname()}" in headline
    # And never the shape that sent people to the wrong box.
    assert f"on {socket.gethostname()}" not in headline


def test_a_claim_lost_before_its_record_still_names_its_box(
    queue: pool.PoolQueue,
) -> None:
    """The intent marker names the claimant, and it precedes the rename."""

    path, record = _lose_a_claim(queue, forget_claimed_host=True)

    assert record["claimed_host"] == HELD_BY, (
        "a claim was lost more anonymously than it was taken"
    )
    summary = pbrun.outcome_summary(queue, path, record)
    assert f"held by {HELD_BY}" in pbrun.outcome_headline(summary)


def test_an_unknown_holder_is_said_to_be_unknown(queue: pool.PoolQueue) -> None:
    """``held by (not recorded)`` sends nobody anywhere; ``on sparky`` did."""

    path, record = _lose_a_claim(
        queue, forget_claimed_host=True, forget_intent=True)

    summary = pbrun.outcome_summary(queue, path, record)
    headline = pbrun.outcome_headline(summary)
    assert "held by (not recorded)" in headline
    assert f"reaped by {socket.gethostname()}" in headline


def test_a_claim_lost_before_any_lease_says_so(queue: pool.PoolQueue) -> None:
    """``lease_age_s: null`` is not "the lease was old"; it is "there was none"."""

    path, record = _lose_a_claim(queue, never_leased=True)
    summary = pbrun.outcome_summary(queue, path, record)

    assert summary["detail"]["lease_age_s"] is None
    headline = pbrun.outcome_headline(summary)
    assert "no lease was ever written" in headline
    assert "None" not in headline


def test_a_stale_lease_reports_its_age(queue: pool.PoolQueue) -> None:
    """The ordinary lost-claim shape: a lease that stopped being refreshed."""

    summary = {
        "status": "lease_lost_max_attempts",
        "claimed_host": "dl380g10",
        "finished_host": "sparky",
        "detail": {"lease_age_s": 312.52},
    }
    headline = pbrun.outcome_headline(summary)
    assert headline == (
        "lease_lost_max_attempts -- held by dl380g10, reaped by sparky, "
        "lease 312.5s stale"
    )


def test_an_ordinary_outcome_is_rendered_as_before(queue: pool.PoolQueue) -> None:
    """Nothing a worker itself filed changes shape."""

    summary = {
        "status": "executed",
        "claimed_host": "sparky",
        "finished_host": "sparky",
        "detail": {"elapsed_s": 12.4, "returncode": 0},
    }
    assert pbrun.outcome_headline(summary) == "executed on sparky in 12s"
