"""A box that cannot prove a payload stopped keeps the claim, and says so.

``cleanup_action_containers`` is the gate that proves both direct and Docker
payloads stopped before tokens return.  Its outer handler caught
``(OSError, ValueError, KeyError, TypeError)``, and everything it wraps reaches
the resource broker over a socket, Docker, and the shared mount.

Anything else escaped the method.  All four callers -- ``finish``,
``reap_stale``, the lease sweep and ``withdraw`` -- index ``["complete"]`` on a
dict they then never receive, so the payload had run, the claim was never
concluded, the lease stopped being renewed, and the reaper filed
``lease_lost_max_attempts``.  That is fail-OPEN in the way that costs the work.
#286 fixed the same shape one handler in, on the best-effort learning hook, and
left this one to be decided rather than widened to match its neighbour.

The decision, and both halves matter:

*   **Fail closed.** ``complete: False`` is the honest answer -- cleanup could
    not be proved -- and the claim and its tokens are retained.  It is
    deliberately *not* a decision to release capacity for a payload nobody has
    shown to have stopped: a GPU an action still holds must not be handed to
    somebody else because the box gave up asking.
*   **Stay visible.** What the old crash bought was a signal -- it ran up
    ``MAX_CONSECUTIVE_ERRORS`` and took the box out of service.  Swallowing the
    raise without replacing that would trade a lost action for a claim pinned
    forever with nothing saying so.  So the retry is counted and its first
    failure dated, and ``pbstatus`` and ``pbmetrics`` report both.

Relying on the crash was relying on the wrong instrument anyway: that counter
is written for bad *items*, and it had already misfired once -- a 0770
admission directory made every box run it up while announcing full capacity
(#281).

No bound is applied to the retries here.  Concluding such a claim means
releasing tokens for an unproven payload, which is a fleet policy decision
about hardware and not a defect fix.  What this makes possible is deciding it
on evidence instead of on nothing.

Issue #288.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))

from prismabuild import pool, resource_scope  # noqa: E402

import pbmetrics  # noqa: E402
import pbstatus  # noqa: E402

from test_pool_resource_scope import _process, scoped  # noqa: E402,F401


#: The two classes the old tuple did not name, for opposite reasons.  A bare
#: ``RuntimeError`` is how this subsystem states an honest refusal -- both
#: ``box_state`` on a directory this uid does not own and ``Controller.locked``
#: on a lock file that is not a private regular file raise one, and that is
#: what escaped on two boxes on 2026-09-06.  ``ZeroDivisionError`` stands in
#: for the other half of the question: a plain code bug nobody anticipated.
UNANTICIPATED = [RuntimeError("admission directory is not private"),
                 ZeroDivisionError("a bug, not a refusal")]


def _fails_cleanup(monkeypatch, exc):
    """Make proving the payload stopped raise ``exc``."""

    def boom(_scope, *_a, **_kw):
        raise exc

    monkeypatch.setattr(resource_scope.ResourceScope, "terminate_owned", boom)


def _claimed_record(queue, key):
    return json.loads(queue.item_path(pool.CLAIMED, key).read_text())


# --------------------------------------------------------------------------
# Fail closed
# --------------------------------------------------------------------------

@pytest.mark.parametrize("exc", UNANTICIPATED, ids=lambda e: type(e).__name__)
def test_an_unanticipated_failure_is_answered_not_raised(scoped, monkeypatch, exc):
    queue, item, calls = scoped
    _process(monkeypatch, queue, item, calls)
    queue.execute(item, containment=True)
    _fails_cleanup(monkeypatch, exc)

    result = queue.cleanup_action_containers(item)

    assert result["complete"] is False
    assert result["used"] is True
    assert type(exc).__name__ in result["error"]


def test_the_claim_and_its_tokens_survive_an_unprovable_cleanup(scoped, monkeypatch):
    # The half that is not about tidiness: the payload may still be running.
    queue, item, calls = scoped
    _process(monkeypatch, queue, item, calls)
    queue.execute(item, containment=True)
    _fails_cleanup(monkeypatch, RuntimeError("broker unreachable"))
    key = item["action_key"]

    queue.finish(key, status="done", detail={"status": "done", "returncode": 0})

    assert queue.item_path(pool.CLAIMED, key).exists()
    assert queue.ledger().held() != {}


def test_a_keyboard_interrupt_still_stops_the_process(scoped, monkeypatch):
    # ``Exception``, never ``BaseException``.  An operator's Ctrl-C must not be
    # answered with a dict.
    queue, item, calls = scoped
    _process(monkeypatch, queue, item, calls)
    queue.execute(item, containment=True)
    _fails_cleanup(monkeypatch, KeyboardInterrupt())

    with pytest.raises(KeyboardInterrupt):
        queue.cleanup_action_containers(item)


# --------------------------------------------------------------------------
# Stay visible
# --------------------------------------------------------------------------

def test_each_retry_is_counted_and_the_first_failure_is_dated(scoped, monkeypatch):
    queue, item, calls = scoped
    _process(monkeypatch, queue, item, calls)
    queue.execute(item, containment=True)
    _fails_cleanup(monkeypatch, RuntimeError("broker unreachable"))
    key = item["action_key"]

    queue.finish(key, status="done", detail={"status": "done", "returncode": 0})
    first = _claimed_record(queue, key)

    assert first["container_cleanup_attempts"] == 1
    assert first["container_cleanup_first_failed_unix"] <= pool._now()


def test_the_reapers_own_retries_are_counted_and_dated(scoped, monkeypatch):
    """The loop that would otherwise be silent, reached where it actually is.

    ``reap_stale`` skips any record carrying ``finish_pending``, so a claim
    that has been through ``finish`` never reaches the reaper's cleanup branch
    at all -- driving this through ``finish`` first tests a different site and
    calls it this one.
    """

    queue, item, calls = scoped
    _process(monkeypatch, queue, item, calls)
    queue.execute(item, containment=True)
    _fails_cleanup(monkeypatch, RuntimeError("broker unreachable"))
    key = item["action_key"]

    queue.reap_stale(timeout_s=-1)
    first = _claimed_record(queue, key)
    assert first["container_cleanup_attempts"] == 1
    began = first["container_cleanup_first_failed_unix"]

    queue.reap_stale(timeout_s=-1)
    second = _claimed_record(queue, key)

    assert second["container_cleanup_attempts"] == 2
    # The date is of the FIRST failure, not the latest: the age of the problem
    # is the number an operator acts on, and the last-retry stamp
    # (``container_cleanup_checked_unix``) is always recent by construction.
    assert second["container_cleanup_first_failed_unix"] == began
    assert second["container_cleanup_checked_unix"] >= began


def test_withdrawing_an_unprovable_claim_counts_its_attempt_too(scoped, monkeypatch):
    # Three sites retain a claim on unproven cleanup, and a count only two of
    # them increment measures nothing.
    queue, item, calls = scoped
    _process(monkeypatch, queue, item, calls)
    queue.execute(item, containment=True)
    _fails_cleanup(monkeypatch, RuntimeError("broker unreachable"))
    key = item["action_key"]

    queue.withdraw(key, by="an operator", reason="testing")

    assert _claimed_record(queue, key)["container_cleanup_attempts"] == 1


def test_the_count_dies_with_the_claim_it_describes(scoped, monkeypatch):
    # A requeued item is claimed by nobody and holds no tokens, so a retry
    # count from a previous claim would be somebody else's number.
    queue, item, calls = scoped
    _process(monkeypatch, queue, item, calls)
    queue.execute(item, containment=True)
    _fails_cleanup(monkeypatch, RuntimeError("broker unreachable"))
    key = item["action_key"]
    queue.finish(key, status="done", detail={"status": "done", "returncode": 0})
    assert _claimed_record(queue, key)["container_cleanup_attempts"] == 1

    assert "container_cleanup_attempts" in pool.PoolQueue._CLAIM_SCOPED_FIELDS
    assert "container_cleanup_first_failed_unix" in pool.PoolQueue._CLAIM_SCOPED_FIELDS


# --------------------------------------------------------------------------
# What the operator and the alert see
# --------------------------------------------------------------------------

def _pinned_status(scoped_fixture, monkeypatch):
    queue, item, calls = scoped_fixture
    _process(monkeypatch, queue, item, calls)
    queue.execute(item, containment=True)
    _fails_cleanup(monkeypatch, RuntimeError("broker unreachable"))
    key = item["action_key"]
    queue.finish(key, status="done", detail={"status": "done", "returncode": 0})
    queue.announce(host=item.get("claimed_host") or "sparky", tags=["cpu"],
                   has_gpu=False, capacity={"gpu": 0, "mem_gb": 60, "cpu": 8})
    return queue, key


def test_pbstatus_says_how_hard_and_for_how_long(scoped, monkeypatch):
    queue, key = _pinned_status(scoped, monkeypatch)

    rows = {job["action_key"]: job for job in pbstatus.read_pool(queue.root)["jobs"]}
    row = rows[key]

    assert row["cleanup_pending"] is True
    assert row["cleanup_attempts"] == 1
    assert row["cleanup_pending_s"] is not None
    # A pending cleanup already said "reservation retained".  What it could not
    # say is whether that had been true for three seconds or for six hours.
    assert "1 attempt" in row["reason"]
    assert "over" in row["reason"]


def test_metrics_raise_the_hand_of_a_loop_that_cannot_finish(scoped, monkeypatch):
    queue, _key = _pinned_status(scoped, monkeypatch)

    text = pbmetrics.collect_metrics(queue.root)

    pinned = [line for line in text.splitlines()
               if line.startswith("prismabuild_cleanup_pending_claims{")]
    assert len(pinned) == 1 and pinned[0].endswith(" 1"), pinned
    aged = [line for line in text.splitlines()
            if line.startswith("prismabuild_cleanup_pending_oldest_seconds{")]
    assert len(aged) == 1, aged


def test_a_claim_pinned_before_the_stamp_existed_reports_no_age(scoped, monkeypatch):
    # The count and the age are separate readings on purpose.  A claim pinned
    # by a generation that predates the first-failure stamp is still pinned --
    # it must be counted -- but its age is unknown, and emitting 0 would say
    # "just started" about a cleanup that may have been stuck for hours.
    queue, key = _pinned_status(scoped, monkeypatch)
    path = queue.item_path(pool.CLAIMED, key)
    record = json.loads(path.read_text())
    del record["container_cleanup_first_failed_unix"]
    path.write_text(json.dumps(record))

    text = pbmetrics.collect_metrics(queue.root)

    assert [line for line in text.splitlines()
            if line.startswith("prismabuild_cleanup_pending_claims{")][0].endswith(" 1")
    assert "prismabuild_cleanup_pending_oldest_seconds{" not in text


def test_a_quiet_fleet_reports_zero_pinned_claims_rather_than_nothing(tmp_path):
    # Zero is a real reading here, unlike the age beside it: "nothing is
    # stuck" is exactly what an alert needs to be told, and a missing series
    # would make a dead exporter look like a healthy fleet.
    queue = pool.PoolQueue(tmp_path / "queue")
    for name in ("ready", "claimed", "done", "failed", "withdrawn", "workers"):
        (queue.root / name).mkdir(parents=True, exist_ok=True)
    queue.announce(host="sparky", tags=["cpu"], has_gpu=False,
                   capacity={"gpu": 0, "mem_gb": 60, "cpu": 8})

    text = pbmetrics.collect_metrics(queue.root)

    assert 'prismabuild_cleanup_pending_claims{host="sparky"} 0' in text
    # Absent, not zero: there is no oldest failure, and 0 would read as one
    # that just started.
    assert "prismabuild_cleanup_pending_oldest_seconds{" not in text
