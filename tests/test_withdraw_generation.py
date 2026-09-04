"""A withdrawal cancels the run it was filed against, not the key for all time.

An action key is a content hash.  ``pbrun``'s own
``result_and_stamp_names`` says so out loud -- *"the same command at the same
commit still fingerprints identically, so a repeat submission can still be
answered from the CAS"* -- which makes re-submitting a key the normal way to
ask for the same work again, not an attempt to defeat somebody's cancellation.

The first cut of ``withdraw`` treated the marker as a permanent blacklist on
the key.  ``publish`` never consulted ``withdrawn/``; ``claim``'s scan guard
unlinked ANY ready record whose key was in it, with no age or generation
check; nothing pruned ``withdrawn/`` and there was no un-withdraw.  So
re-running a withdrawn command from an unchanged tree wrote ``ready/<key>``,
the first worker to poll deleted it, and ``pbrun`` matched the OLD withdrawal
on its first poll and exited 143 quoting a stranger's reason for a run the
caller had just submitted.  Nothing was filed anywhere.  The only remedy was
``rm withdrawn/<key>.json`` on the live queue -- the exact class of live-queue
hand edit this whole issue exists to remove.

The generation is ``published_unix``: ``publish`` stamps a fresh one, and
every requeue -- ``finish``'s and ``reap_stale``'s -- carries the original
forward.  So the losing half of a withdrawal race and a fresh submission are
distinguishable, and this file is where that distinction is pinned.
"""

from __future__ import annotations

import json
from pathlib import Path
import sys
import uuid

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import pool  # noqa: E402

KEY_A = uuid.uuid4().hex + uuid.uuid4().hex


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    q = pool.PoolQueue(tmp_path / "pb-queue")
    q.ensure_layout()
    return q


def _publish(q: pool.PoolQueue, key: str, **kw: object) -> None:
    q.publish(
        action_key=key,
        cas_root=kw.pop("cas_root", "/cas"),
        checkout_root=kw.pop("checkout_root", "/co"),
        worker_script=kw.pop("worker_script", "/w.py"),
        **kw,
    )


def _old_bytes(q: pool.PoolQueue, monkeypatch) -> pool.PoolQueue:
    """The same queue as a worker running pre-withdraw bytes sees it.

    A worker loop holds the module it imported at start until the runtime
    rolls, so during the transition part of the fleet is running ``main``'s
    ``pool.py``.  The ONLY behavioural difference between that ``finish`` and
    this branch's is that it does not consult the withdrawal marker -- the
    lost-race branch, the retry branch and the release are identical text --
    so blinding the marker lookup is a faithful model of it, not a fake.
    """

    old = pool.PoolQueue(q.root)
    monkeypatch.setattr(old, "withdrawn_keys", lambda: frozenset())
    return old


def _superseded(q: pool.PoolQueue, key: str) -> list[dict]:
    directory = q.dir(pool.WITHDRAWN) / "superseded"
    if not directory.is_dir():
        return []
    return [
        json.loads(path.read_text())
        for path in sorted(directory.glob(f"{key}.*.json"))
    ]


# -- the blocker: a key is a content hash, not a blacklist --------------------


def test_resubmitting_a_withdrawn_command_runs_it(queue: pool.PoolQueue) -> None:
    """The whole defect, end to end: submit, withdraw, submit the same thing.

    Before the fix this claim returned ``None`` and the fresh ready record was
    unlinked by the scan guard, with nothing filed anywhere.
    """

    _publish(queue, KEY_A)
    queue.withdraw(KEY_A, reason="four suites, one box", by="rob@sparky")

    _publish(queue, KEY_A)                    # the same command, the same tree
    item = queue.claim()
    assert item is not None, "a re-submission is a new request, not the old one"
    assert item["action_key"] == KEY_A
    assert queue.item_path(pool.CLAIMED, KEY_A).exists()

    # And it can conclude: the outcome is not swallowed by the stale marker.
    queue.finish(KEY_A, status="executed", detail={"returncode": 0})
    assert queue.item_path(pool.DONE, KEY_A).exists()


def test_the_marker_is_retired_by_the_resubmission_not_deleted(
    queue: pool.PoolQueue
) -> None:
    """The operator's decision is kept, out of the guard's way.

    ``pbrun`` polls ``withdrawn/<key>.json`` by ``readdir`` and would otherwise
    answer a brand new submission with the previous run's ``withdrawn_by`` and
    reason, at exit 143, on its first poll.
    """

    _publish(queue, KEY_A)
    queue.withdraw(KEY_A, reason="four suites, one box", by="rob@sparky")
    assert queue.item_path(pool.WITHDRAWN, KEY_A).exists()

    _publish(queue, KEY_A)
    assert not queue.item_path(pool.WITHDRAWN, KEY_A).exists(), (
        "the live marker must not answer for a run submitted after it")
    kept = _superseded(queue, KEY_A)
    assert len(kept) == 1 and kept[0]["reason"] == "four suites, one box"
    assert kept[0]["withdrawn_by"] == "rob@sparky"
    assert kept[0]["superseded_unix"] >= kept[0]["withdrawn_unix"]
    # The new item says out loud that it revived a cancelled key.
    item = json.loads(queue.item_path(pool.READY, KEY_A).read_text())
    assert item["supersedes_withdrawal"]["withdrawn_by"] == "rob@sparky"


def test_a_withdrawal_still_stops_the_generation_it_was_filed_against(
    queue: pool.PoolQueue
) -> None:
    """Scoping the guard must not reopen the race the verb exists to close."""

    _publish(queue, KEY_A)
    queue.claim()
    queue.withdraw(KEY_A)
    # No re-submission: the marker is live and the key stays unclaimable.
    assert queue.claim() is None
    assert queue.item_path(pool.WITHDRAWN, KEY_A).exists()


def test_the_losing_half_of_the_race_is_dropped_and_recorded(
    queue: pool.PoolQueue, monkeypatch
) -> None:
    """A requeue of the withdrawn generation is stopped -- and filed, not lost.

    This is the state the race leaves: a pre-publish worker that read its
    claimed record before the withdrawal removed it, and requeued afterwards.
    The record is reproduced through the real ``finish`` retry branch, so it
    carries the real ``published_unix`` (the withdrawn generation) and the real
    ``requeued_unix``.
    """

    _publish(queue, KEY_A, max_attempts=3)
    claimed = queue.claim()
    generation = claimed["published_unix"]
    # What the old worker still had open when the withdrawal landed.
    held = json.loads(queue.item_path(pool.CLAIMED, KEY_A).read_text())

    queue.withdraw(KEY_A)
    queue.item_path(pool.CLAIMED, KEY_A).write_text(json.dumps(held))
    _old_bytes(queue, monkeypatch).finish(KEY_A, status="failed", detail={})

    stranded = queue.item_path(pool.READY, KEY_A)
    assert stranded.exists(), "the retry branch really did write one back"
    assert json.loads(stranded.read_text())["published_unix"] == generation

    assert queue.claim() is None, "the cancelled action must not run again"
    assert not stranded.exists(), "and must not sit at the head of ready"
    dropped = [r for r in _superseded(queue, KEY_A) if r.get("status") == "dropped"]
    assert dropped, "a dropped record is filed, never silently unlinked"
    assert dropped[0]["published_unix"] == generation
    assert "requeued_unix" in dropped[0]


def test_a_fresh_submission_survives_a_marker_that_is_still_live(
    queue: pool.PoolQueue
) -> None:
    """The generation test, isolated from ``publish``'s retirement of the marker.

    Both belts are wanted: ``publish`` retires the marker, and the guard reads
    generations rather than keys.  Either alone would leave a window -- a
    ready record written by anything that is not this ``publish`` (a requeue
    that outlived its withdrawal, a fleet mid-roll) has to be judged on what
    generation it belongs to, not on its name.
    """

    _publish(queue, KEY_A)
    queue.withdraw(KEY_A)
    marker = json.loads(queue.item_path(pool.WITHDRAWN, KEY_A).read_text())

    _publish(queue, KEY_A)
    # Put the withdrawal back, as if the retirement had lost its own race.
    queue.item_path(pool.WITHDRAWN, KEY_A).write_text(json.dumps(marker))

    item = queue.claim()
    assert item is not None, "a newer generation is not what was withdrawn"
    assert item["published_unix"] > marker["published_unix"]


def test_the_generation_test_does_not_lean_on_the_clock(
    queue: pool.PoolQueue
) -> None:
    """A different generation is a different request, whichever way it runs.

    Only ``publish`` and the two requeue branches ever write to ``ready``, and
    the requeues copy ``published_unix`` through unchanged, so equality is the
    whole test.  Ordering would have made a cancelled action re-runnable, or a
    fresh submission eatable, on nothing worse than NTP stepping a clock
    backwards between two submissions.
    """

    _publish(queue, KEY_A)
    queue.withdraw(KEY_A)
    marker = json.loads(queue.item_path(pool.WITHDRAWN, KEY_A).read_text())

    _publish(queue, KEY_A)
    queue.item_path(pool.WITHDRAWN, KEY_A).write_text(json.dumps(marker))
    # The clock stepped back between the withdrawal and the re-submission.
    ready = queue.item_path(pool.READY, KEY_A)
    item = json.loads(ready.read_text())
    item["published_unix"] = marker["published_unix"] - 1.0
    ready.write_text(json.dumps(item))

    assert queue.claim() is not None, (
        "an earlier stamp is still a different request, not the withdrawn one")


def test_withdrawing_again_cancels_the_run_that_is_live_now(
    queue: pool.PoolQueue
) -> None:
    """A second withdrawal is a fresh decision when the marker is stale.

    ``withdraw`` is idempotent, and the idempotent branch keeps the first
    decision's record verbatim.  That is right while the marker still names
    the live record, and wrong the moment it does not: the operator who
    withdraws the claimed run, sees the submission that was queued behind it
    still queued, and runs the verb again was answered ``already_withdrawn``
    with generation two left untouched -- told the action was cancelled while
    it went on to run.  Claimed rather than queued it is worse: the cleanup at
    the end of the verb unlinks the claim, drops the lease and returns the
    tokens while no marker covers the box actually running the child.
    """

    _publish(queue, KEY_A)                       # generation one
    assert queue.claim() is not None
    _publish(queue, KEY_A)                       # generation two, queued behind
    second = json.loads(
        queue.item_path(pool.READY, KEY_A).read_text())["published_unix"]

    queue.withdraw(KEY_A, by="rob", signal_child=False)     # names generation one
    again = queue.withdraw(KEY_A, by="rob", signal_child=False)

    assert again["status"] == "withdrawn", "a stale marker is not this decision"
    assert not queue.item_path(pool.READY, KEY_A).exists()
    marker = json.loads(queue.item_path(pool.WITHDRAWN, KEY_A).read_text())
    assert marker["published_unix"] == second
    # The first decision is kept, not overwritten: it is still the record of
    # who cancelled generation one and why.
    assert [one for one in _superseded(queue, KEY_A)
            if one.get("status") == "withdrawn"]
