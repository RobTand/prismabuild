"""``refuse_if_live``: a publisher that must not duplicate says so.

``PoolQueue.publish`` writes ``ready/<key>.json`` whatever state the queue is
already carrying the key in, and that stays the default -- a fresh generation
over a live key is how an operator asks for the same work again, and `_claim`
treats it as uncovered by the old cancellation on purpose.

For a publisher that did not mean to duplicate, the same overwrite costs twice:

* #812, before the claim.  Identical submissions seal one content-addressed
  key, so three ``pbtest`` shards started at the same moment published the same
  row three times.  Each client read back a different ``published_unix`` and
  waited pinned to it; the worker ran the surviving row once and filed one
  terminal; every client holding a superseded generation polled an empty queue
  until its three-hour budget ran out.
* #810, after the claim.  An automatic republisher whose read of the row was
  stale published a key that was live in ``claimed``.  With ``recompute`` the
  duplicate is not answered from the receipt, so the action really ran again --
  four times per egress key in the 2026-09-21 Stage A cycle.

``refuse_if_live`` is the declaration that closes both, in the shape
``refuse_withdrawn`` already uses.  It is exact rather than advisory because
every transition on a key takes that key's transition lock, which ``publish``
already holds.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys
import uuid

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))

from prismabuild import pool  # noqa: E402
from prismabuild import produced_output  # noqa: E402
from prismabuild import storage_tiers  # noqa: E402
import pbrun  # noqa: E402

from test_pbrun_detach import _checkout, _queue, _run_pbrun  # noqa: E402,F401


KEY = uuid.uuid4().hex + uuid.uuid4().hex


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


def _row(q: pool.PoolQueue, state: str, key: str) -> dict:
    return json.loads(q.item_path(state, key).read_text(encoding="utf-8"))


# --------------------------------------------------------------------------
# The default is unchanged
# --------------------------------------------------------------------------

def test_an_ordinary_publication_still_replaces_a_live_key(
    queue: pool.PoolQueue,
) -> None:
    """A fresh generation over a live key is a designed operation, not a bug.

    ``_requeue_arguments`` depends on it, a resubmission while a stop is in
    flight depends on it, and ``publish`` has no opinion about it unless a
    publisher declares one.
    """

    _publish(queue, KEY, resources={"cpu": 1})
    first = _row(queue, pool.READY, KEY)["published_unix"]
    _publish(queue, KEY, resources={"cpu": 1})
    assert _row(queue, pool.READY, KEY)["published_unix"] != first

    assert queue.claim(capacity={"cpu": 4}) is not None
    _publish(queue, KEY, resources={"cpu": 1})
    assert queue.item_path(pool.READY, KEY).exists()


# --------------------------------------------------------------------------
# #810: an automatic republisher whose look at the row was stale
# --------------------------------------------------------------------------

def test_a_declared_publisher_is_refused_while_the_key_is_claimed(
    queue: pool.PoolQueue,
) -> None:
    """The stale caller is refused, and the live claim is left exactly as it was."""

    _publish(queue, KEY, resources={"cpu": 1})
    assert queue.claim(capacity={"cpu": 4}) is not None
    before = _row(queue, pool.CLAIMED, KEY)

    with pytest.raises(pool.PoolContractError) as refusal:
        _publish(queue, KEY, resources={"cpu": 1}, recompute=True,
                 refuse_if_live=True)

    # The behavioural claim first: nothing was queued behind the running run.
    assert not queue.item_path(pool.READY, KEY).exists()
    assert _row(queue, pool.CLAIMED, KEY) == before

    # And the refusal is typed, and says which generation to wait on.
    assert isinstance(refusal.value, pool.ActionAlreadyLiveError)
    assert refusal.value.state == pool.CLAIMED
    assert refusal.value.generation == before["published_unix"]


def test_a_stale_mover_republication_does_not_queue_a_second_run(
    queue: pool.PoolQueue, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#810 through the caller that hit it: a by-name read that answers absent.

    ``_publish_output_mover_row`` looks at the row before it publishes.  On NFS
    that look can answer ``absent`` for a key another box has already claimed,
    which is what republished each egress of the 2026-09-21 Stage A cycle three
    times.  The queue's own check is inside the transition lock, so the stale
    look cannot get past it, and the caller gets the answer its own look would
    have given.
    """

    _publish(queue, KEY, resources={"cpu": 1})
    assert queue.claim(capacity={"cpu": 4}) is not None
    claimed = _row(queue, pool.CLAIMED, KEY)

    monkeypatch.setattr(produced_output, "_mover_live_state",
                        lambda *args, **kwargs: "absent")
    answer = produced_output._publish_output_mover_row(
        queue, mover_key=KEY, cas_root=str(tmp_path / "cas"),
        launch={"worker_script": str(tmp_path / "w.py"), "priority": 0,
                "addressing": {"checkout_root": str(tmp_path / "co")}},
        host="sparky", tier="sparky",
        kind=storage_tiers.capacity_kind_of("sparky"), gib=1,
        manifest_digest="0" * 64, total=1,
        retry_policy={"max_attempts": 1, "retry_safe": True})

    assert not queue.item_path(pool.READY, KEY).exists()
    assert _row(queue, pool.CLAIMED, KEY) == claimed
    assert answer == {"ok": True, "published": False, "state": pool.CLAIMED}


def test_the_key_is_publishable_again_once_the_claim_has_concluded(
    queue: pool.PoolQueue,
) -> None:
    """The refusal is about liveness, not about the key."""

    _publish(queue, KEY, resources={"cpu": 1})
    assert queue.claim(capacity={"cpu": 4}) is not None
    queue.finish(KEY, status="executed")

    _publish(queue, KEY, resources={"cpu": 1}, recompute=True,
             refuse_if_live=True)
    assert queue.item_path(pool.READY, KEY).exists()


def test_a_live_cancellation_is_still_superseded_by_a_resubmission(
    queue: pool.PoolQueue,
) -> None:
    """A marker means the submission is the replacement it asked for."""

    _publish(queue, KEY, resources={"cpu": 1})
    assert queue.claim(capacity={"cpu": 4}) is not None
    queue.withdraw(KEY, signal_child=False)
    assert queue.live_withdrawal(KEY) is not None

    _publish(queue, KEY, resources={"cpu": 1}, refuse_if_live=True)
    assert queue.item_path(pool.READY, KEY).exists()
    assert queue.live_withdrawal(KEY) is None


def test_a_handoff_that_names_its_claim_is_not_judged_as_a_duplicate(
    queue: pool.PoolQueue,
) -> None:
    """``preempted_claim`` reaches the handoff gate rather than this one.

    The preemption requeue and the resign handoff replace a live claim on
    purpose.  Neither passes ``refuse_if_live``, so neither meets this check at
    all; this asserts the stronger property that even when a publication does
    pass it, naming a claim routes it to the handoff rules that already
    existed.  Here it fails them, because no withdrawal revives it.  The
    handoffs' own end-to-end behaviour is covered by
    ``test_pool_preempts_a_background_holder`` and
    ``test_fleet_membership_busy_resign``.
    """

    _publish(queue, KEY, resources={"cpu": 1})
    claimed = queue.claim(capacity={"cpu": 4})
    assert claimed is not None

    with pytest.raises(pool.PoolContractError) as refusal:
        _publish(queue, KEY, resources={"cpu": 1}, refuse_if_live=True,
                 preempted_claim=dict(claimed))

    assert not isinstance(refusal.value, pool.ActionAlreadyLiveError)
    assert "preemption handoff changed before requeue" in str(refusal.value)


# --------------------------------------------------------------------------
# #812: a second submission of a key that is still in ``ready``
# --------------------------------------------------------------------------

def test_a_duplicate_submission_keeps_the_first_generation(
    queue: pool.PoolQueue,
) -> None:
    """The row the first waiter is pinned to survives, so its ending reaches it."""

    _publish(queue, KEY, resources={"cpu": 1}, refuse_if_live=True)
    first = _row(queue, pool.READY, KEY)["published_unix"]

    with pytest.raises(pool.PoolContractError) as refusal:
        _publish(queue, KEY, resources={"cpu": 1}, refuse_if_live=True)

    assert _row(queue, pool.READY, KEY)["published_unix"] == first
    assert isinstance(refusal.value, pool.ActionAlreadyLiveError)
    assert refusal.value.state == pool.READY
    assert refusal.value.generation == first

    # What the first client was waiting for: the one run's ending, carrying the
    # generation it was given at submission time.
    assert queue.claim(capacity={"cpu": 4}) is not None
    queue.finish(KEY, status="executed")

    landed, _ = pbrun.outcome_poll(queue, KEY, first)
    assert landed is not None, (
        "the duplicate restamped the row, so the first waiter's generation "
        "no longer matches any terminal record")


def test_pbrun_attaches_a_duplicate_pool_submission_instead_of_restamping(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys,
) -> None:
    """The waiting pool path, end to end: two ``pbrun``s, one generation.

    Nothing drains this queue, so neither call reaches an ending; their exit
    codes are not the claim here.  What matters is the row they leave behind:
    the second call attaches to the first submission's generation, so the first
    client's wait still has an ending to find.
    """

    work = _checkout(tmp_path)
    queue = _queue(tmp_path)
    unfinished = {pbrun.GAVE_UP_EXIT, pbrun.RECORD_WRITE_FAILED_EXIT}

    assert _run_pbrun(tmp_path, monkeypatch, work) in unfinished
    capsys.readouterr()
    keys = [path.stem for path in queue.dir(pool.READY).glob("*.json")]
    assert len(keys) == 1
    key = keys[0]
    first = _row(queue, pool.READY, key)["published_unix"]

    assert _run_pbrun(tmp_path, monkeypatch, work) in unfinished
    captured = capsys.readouterr()

    assert _row(queue, pool.READY, key)["published_unix"] == first
    assert "attaching to that run" in captured.err
    assert len(list(queue.dir(pool.READY).glob("*.json"))) == 1
