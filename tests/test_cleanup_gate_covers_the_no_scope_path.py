"""The fail-closed cleanup gate must cover the path that has no resource scope.

#288 made ``cleanup_action_containers`` answer ``complete: False`` instead of
raising, so a box that cannot prove a payload stopped keeps the claim and its
tokens and says so.  The handler it added wraps the resource-scope branch.  Two
paths reach the caller without passing through it:

*   the **no-scope** early return -- a legacy or uncontained claim, and the
    recover-create-missing case -- which goes straight to
    ``_cleanup_action_containers``, where ``marker.exists()`` sat outside every
    ``try``.  ``Path.exists`` re-raises an errno outside
    ``ENOENT/ENOTDIR/EBADF/ELOOP``, and ESTALE on an NFS handle is outside it,
    so a stale marker handle escaped the gate entirely; and
*   ``_recover_resource_scope_creation``, still guarded by an enumerated tuple
    ``(OSError, ValueError, KeyError, TypeError)`` -- the third list of the
    errors somebody thought of on a path that reaches the broker socket, the
    shared mount and JSON.  A bare ``RuntimeError`` is how this subsystem
    states an honest refusal (#281, #286), and it is in neither list.

The consequence is the same in both, and it is the expensive direction: all
four callers -- ``finish``, ``reap_stale``, the lease sweep and ``withdraw`` --
index ``["complete"]`` on a dict they then never receive.  The payload has run,
the claim is never concluded, the lease stops being renewed, and the reaper
files ``lease_lost_max_attempts``.

Issue #302.
"""
from __future__ import annotations

import errno
import json
from pathlib import Path
import sys

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
from prismabuild import pool  # noqa: E402

KEY = "e" * 64
OWNER = "a" * 64
STALE = OSError(errno.ESTALE, "stale NFS marker handle")


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    q = pool.PoolQueue(tmp_path / "queue")
    q.publish(
        action_key=KEY, cas_root=q.root / "cas", checkout_root=q.root / "co",
        worker_script=q.root / "worker.py", resources={"cpu": 1},
        max_attempts=2, retry_safe=True,
    )
    return q


def _uncontained_claim_with_a_container(queue: pool.PoolQueue) -> dict:
    """A claim that entered the Docker shim and has no resource scope."""

    item = queue.claim(capacity={"cpu": 1})
    assert item is not None and item.get("resource_scope") is None
    marker = queue.container_marker(OWNER)
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text("")
    record = json.loads(queue.item_path(pool.CLAIMED, KEY).read_text())
    record["container_owner"] = OWNER
    pool._write_json_atomic(queue.item_path(pool.CLAIMED, KEY), record)
    return record


def _marker_stat_raises(monkeypatch, queue: pool.PoolQueue, exc: BaseException) -> None:
    marker = queue.container_marker(OWNER)
    real = Path.exists

    def guarded(self, **kwargs):
        if self == marker:
            raise exc
        return real(self, **kwargs)

    monkeypatch.setattr(Path, "exists", guarded)


def test_a_stale_marker_handle_is_answered_not_raised(queue, monkeypatch):
    record = _uncontained_claim_with_a_container(queue)
    _marker_stat_raises(monkeypatch, queue, STALE)

    result = queue.cleanup_action_containers(record)

    assert result["complete"] is False
    assert result["used"] is True
    assert "stale NFS marker handle" in result["error"]


def test_the_result_the_claim_and_the_tokens_all_survive_it(queue, monkeypatch):
    """The half that is not about tidiness: the container may still be running.

    ``finish`` is the caller that has something to lose -- the payload's real
    outcome.  It must be retained as ``finish_pending`` rather than discarded
    with the raise, and the reservation must stay held, because releasing it
    hands a box's capacity to somebody else for a payload nobody has shown to
    have stopped.
    """

    record = _uncontained_claim_with_a_container(queue)
    _marker_stat_raises(monkeypatch, queue, STALE)
    detail = {"status": "executed", "returncode": 0, "stdout": "the real result"}

    queue.finish(KEY, status="executed", detail=detail, claim_snapshot=record)

    pending = json.loads(queue.item_path(pool.CLAIMED, KEY).read_text())
    assert pending["finish_pending"]["status"] == "executed"
    assert pending["finish_pending"]["detail"] == detail
    assert queue.ledger().held() == {"cpu": 1}
    assert not queue.item_path(pool.DONE, KEY).exists()
    # Visible, not merely retained: a claim that can never be cleaned up has to
    # be countable, or fail-closed just trades a lost action for a silent one.
    assert pending["container_cleanup_attempts"] == 1
    assert pending["container_cleanup_first_failed_unix"] <= pool._now()


def test_a_keyboard_interrupt_still_stops_the_process(queue, monkeypatch):
    # ``Exception``, never ``BaseException``.  An operator's Ctrl-C must not be
    # answered with a dict on this path either.
    record = _uncontained_claim_with_a_container(queue)
    _marker_stat_raises(monkeypatch, queue, KeyboardInterrupt())

    with pytest.raises(KeyboardInterrupt):
        queue.cleanup_action_containers(record)


def test_an_honest_refusal_from_scope_recovery_is_answered_too(queue, monkeypatch):
    """The adjacent branch, audited in the same repair.

    ``_recover_resource_scope_creation`` runs for a claim whose scope was
    intended but never recorded.  ``box_state`` and ``Controller.locked`` both
    refuse with a bare ``RuntimeError``, which the enumerated tuple did not
    name -- the same omission that cost two boxes their claims on 2026-09-06.
    """

    record = _uncontained_claim_with_a_container(queue)
    record["resource_scope_intent"] = {"nonce": "c" * 32}
    monkeypatch.setattr(
        pool.PoolQueue, "_recover_resource_scope_creation",
        lambda self, rec: (_ for _ in ()).throw(
            RuntimeError("admission directory is not private")))

    result = queue.cleanup_action_containers(record)

    assert result["complete"] is False
    assert "RuntimeError" in result["error"]
    assert "admission directory is not private" in result["error"]


#: The two classes the enumerated tuple did not name, for opposite reasons --
#: the same pair ``test_cleanup_gate_fails_closed_and_visibly`` uses on the
#: resource-scope path.  A bare ``RuntimeError`` is how this subsystem states
#: an honest refusal; ``ZeroDivisionError`` stands in for a plain code bug
#: nobody anticipated.
UNANTICIPATED = [RuntimeError("admission directory is not private"),
                 ZeroDivisionError("a bug, not a refusal")]


@pytest.mark.parametrize("exc", UNANTICIPATED, ids=lambda e: type(e).__name__)
def test_an_unanticipated_failure_while_asking_docker_is_answered_too(
    queue, monkeypatch, exc
):
    """The terminal handler on this path, held to the decision #288 made.

    Extending the ``try`` upward is only half the repair: the region it now
    covers is the part that PROVES the payload stopped, and it was still
    guarded by a list of the errors somebody thought of.  Neither of these is
    on it, and both reach a caller that indexes ``["complete"]``.
    """

    record = _uncontained_claim_with_a_container(queue)
    monkeypatch.setattr(
        pool, "_docker_owned_container_ids",
        lambda owner: (_ for _ in ()).throw(exc))

    result = queue.cleanup_action_containers(record)

    assert result["complete"] is False
    assert result["used"] is True
    assert type(exc).__name__ in result["error"]


def test_the_reapers_own_retry_is_counted_on_this_path_too(queue, monkeypatch):
    """The other caller named in the acceptance, reached where it actually is.

    ``finish`` and ``reap_stale`` reach this gate by different routes, and
    driving this fixture through ``finish`` first would not then exercise the
    reaper's.  ``finish`` leaves a ``finish_pending`` record, and ``reap_stale``
    handles those in an earlier branch of its own -- retrying the saved outcome
    on the owner host, and leaving it alone on any other, because only the owner
    can prove its scope is empty -- so such a record never reaches the lease-age
    path this test is about.  The claim here carries no pending finish for
    exactly that reason.
    """

    record = _uncontained_claim_with_a_container(queue)
    _marker_stat_raises(monkeypatch, queue, STALE)

    assert queue.reap_stale(timeout_s=-1) == []

    pending = json.loads(queue.item_path(pool.CLAIMED, KEY).read_text())
    assert pending["container_cleanup_attempts"] == 1
    assert queue.ledger().held() == {"cpu": 1}
    assert not queue.item_path(pool.READY, KEY).exists()
