"""A foreground denial takes the box back from an admitted background holder.

#363 put the priority band ahead of aging, so a ``--priority -10`` item is
never *considered* while foreground work is ready.  What that left untouched is
an item already admitted: its reservation stands until it finishes or hits its
own ``--timeout-s``, so "does not displace real work" held in the queue and not
at the box (#364).

The mechanism here is the withdrawal ladder the remote-withdrawal tests
already cover, and it is asynchronous on every path: ``withdraw`` returns
``released: 0`` and the holder's own ``finish`` returns the tokens.  So these
tests drive the stop explicitly, exactly as those tests do, rather than
sleeping on a worker that does not exist in a unit test.
"""

from __future__ import annotations

import json
from pathlib import Path
import sys
import uuid

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import pool  # noqa: E402

FOREGROUND = uuid.uuid4().hex + uuid.uuid4().hex
BACKGROUND = uuid.uuid4().hex + uuid.uuid4().hex
OTHER = uuid.uuid4().hex + uuid.uuid4().hex


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


def _decisions(q: pool.PoolQueue, key: str) -> list[dict]:
    return [decision for _, decision in q.withdrawal_decisions(key)]


def test_a_foreground_denial_preempts_the_background_holder_and_requeues_it(
    queue: pool.PoolQueue,
) -> None:
    """The whole of #364, on one token.

    The background holder is stopped through the ladder, its generation is
    cancelled, and the same action is re-published at its own priority so it
    runs again later.  Release is asynchronous -- ``withdraw`` frees nothing --
    so the foreground item is claimed on the pass after the holder's ``finish``,
    not on the pass that preempted it.
    """

    capacity = {"gpu": 1}
    queue.ledger().ensure_capacity(capacity)

    _publish(queue, BACKGROUND, priority=-10, resources={"gpu": 1},
             max_attempts=3, retry_safe=True)
    holder = queue.claim(capacity=capacity)
    assert holder is not None and holder["action_key"] == BACKGROUND
    assert queue.ledger().held() == {"gpu": 1}
    # The aging sidecar is a free-standing file, and a claim clears it.  Seed it
    # again so the requeue has something to keep: preemption must not be a way
    # to lose the denials an item has already accrued.
    queue.record_pass(BACKGROUND)
    assert queue.passes(BACKGROUND) == 1

    _publish(queue, FOREGROUND, priority=0, resources={"gpu": 1})

    # The pass that denies the foreground item preempts the holder and returns
    # nothing: the token is not free until the holder stops.
    assert queue.claim(capacity=capacity) is None
    assert queue.ledger().held() == {"gpu": 1}

    decisions = _decisions(queue, BACKGROUND)
    assert len(decisions) == 1
    assert decisions[0]["status"] == "withdrawn"
    assert decisions[0]["preempted_by"] == FOREGROUND

    # Requeued at its own priority, with its passes, as a fresh generation the
    # cancellation does not cover.
    requeued = json.loads(queue.item_path(pool.READY, BACKGROUND).read_text())
    assert requeued["priority"] == -10
    assert requeued["preempted_by"] == FOREGROUND
    assert requeued["resources"] == {"gpu": 1}
    assert requeued["max_attempts"] == 3
    assert requeued["retry_safe"] is True
    assert requeued["published_unix"] != holder["published_unix"]
    assert queue.withdrawal_covers(requeued, action_key=BACKGROUND) is None
    assert queue.passes(BACKGROUND) == 1

    # The holder's claim still owns the token, so nothing may run yet.
    assert queue.item_path(pool.CLAIMED, BACKGROUND).exists()
    assert queue.claim(capacity=capacity) is None

    # The holder concludes the withdrawal and the token comes back.
    queue.finish(BACKGROUND, status="withdrawn", detail={"returncode": -15},
                 claim_snapshot=holder)
    assert queue.ledger().available() == {"gpu": 1}

    taken = queue.claim(capacity=capacity)
    assert taken is not None and taken["action_key"] == FOREGROUND

    queue.finish(FOREGROUND, status="executed", claim_snapshot=taken)
    again = queue.claim(capacity=capacity)
    assert again is not None and again["action_key"] == BACKGROUND


def test_a_background_denial_never_preempts_a_background_holder(
    queue: pool.PoolQueue,
) -> None:
    """Intra-band fairness is aging's job, not preemption's (#364)."""

    capacity = {"gpu": 1}
    queue.ledger().ensure_capacity(capacity)

    _publish(queue, BACKGROUND, priority=-10, resources={"gpu": 1})
    holder = queue.claim(capacity=capacity)
    assert holder is not None and holder["action_key"] == BACKGROUND

    _publish(queue, OTHER, priority=-10, resources={"gpu": 1})
    assert queue.claim(capacity=capacity) is None

    assert _decisions(queue, BACKGROUND) == []
    assert queue.item_path(pool.CLAIMED, BACKGROUND).exists()
    assert queue.item_path(pool.READY, OTHER).exists()
    assert queue.ledger().held() == {"gpu": 1}


def test_a_foreground_holder_is_never_preempted_by_foreground_work(
    queue: pool.PoolQueue,
) -> None:
    """Only the background band yields.  A campaign action is not fair game."""

    capacity = {"gpu": 1}
    queue.ledger().ensure_capacity(capacity)

    _publish(queue, OTHER, priority=0, resources={"gpu": 1})
    holder = queue.claim(capacity=capacity)
    assert holder is not None and holder["action_key"] == OTHER

    _publish(queue, FOREGROUND, priority=5, resources={"gpu": 1})
    assert queue.claim(capacity=capacity) is None

    assert _decisions(queue, OTHER) == []
    assert queue.item_path(pool.CLAIMED, OTHER).exists()
    assert queue.ledger().held() == {"gpu": 1}


def test_a_holder_whose_release_would_still_not_fit_is_left_alone(
    queue: pool.PoolQueue,
) -> None:
    """The threshold is the fit, not the priority ordering.

    Stopping work that does not unblock the denied item is pure loss: the
    foreground item waits exactly as long and the background one has to start
    over.  So the test is whether *this* holder's tokens close the gap.
    """

    capacity = {"gpu": 2}
    ledger = queue.ledger()
    ledger.ensure_capacity(capacity)
    # A second token is held by work the pool did not queue here, so releasing
    # the background holder leaves one free against a demand for two.
    assert ledger.acquire("0" * 64, {"gpu": 1}) is True

    _publish(queue, BACKGROUND, priority=-10, resources={"gpu": 1})
    holder = queue.claim(capacity=capacity)
    assert holder is not None and holder["action_key"] == BACKGROUND

    _publish(queue, FOREGROUND, priority=0, resources={"gpu": 2})
    assert queue.claim(capacity=capacity) is None

    assert _decisions(queue, BACKGROUND) == []
    assert queue.item_path(pool.CLAIMED, BACKGROUND).exists()


def test_preemption_never_cascades_while_a_release_is_already_in_flight(
    queue: pool.PoolQueue,
) -> None:
    """One holder per denial, and only while nothing is already coming back.

    A second pass runs before the first holder has stopped.  Counting the
    tokens a withdrawn holder is about to return is what keeps that pass from
    cancelling a second action for capacity it has already been promised.
    """

    capacity = {"gpu": 2}
    queue.ledger().ensure_capacity(capacity)

    _publish(queue, BACKGROUND, priority=-10, resources={"gpu": 1})
    first = queue.claim(capacity=capacity)
    assert first is not None and first["action_key"] == BACKGROUND
    _publish(queue, OTHER, priority=-10, resources={"gpu": 1})
    second = queue.claim(capacity=capacity)
    assert second is not None and second["action_key"] == OTHER

    _publish(queue, FOREGROUND, priority=0, resources={"gpu": 1})
    assert queue.claim(capacity=capacity) is None
    assert queue.claim(capacity=capacity) is None
    assert queue.claim(capacity=capacity) is None

    preempted = [key for key in (BACKGROUND, OTHER) if _decisions(queue, key)]
    assert len(preempted) == 1


def test_a_holder_that_finished_first_is_not_requeued(
    queue: pool.PoolQueue, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A preemption that raced a natural ending publishes nothing.

    The claim is read before the holder's key is locked, so the holder can
    conclude in between.  ``withdraw`` then reports ``already_finished`` and
    stops nothing -- and re-publishing on that answer would put work that just
    succeeded back on the queue as a fresh generation, which no terminal-claim
    guard catches because the guard is generation-scoped.
    """

    capacity = {"gpu": 1}
    queue.ledger().ensure_capacity(capacity)

    _publish(queue, BACKGROUND, priority=-10, resources={"gpu": 1})
    holder = queue.claim(capacity=capacity)
    assert holder is not None and holder["action_key"] == BACKGROUND

    _publish(queue, FOREGROUND, priority=0, resources={"gpu": 1})

    original = pool.PoolQueue.withdraw

    def finish_then_withdraw(self, key, *args, **kwargs):
        if key == BACKGROUND and queue.item_path(pool.CLAIMED, key).exists():
            queue.finish(BACKGROUND, status="executed",
                         detail={"returncode": 0}, claim_snapshot=holder)
        return original(self, key, *args, **kwargs)

    monkeypatch.setattr(pool.PoolQueue, "withdraw", finish_then_withdraw)

    queue.claim(capacity=capacity)

    assert queue.item_path(pool.DONE, BACKGROUND).exists()
    assert not queue.item_path(pool.READY, BACKGROUND).exists()
    assert _decisions(queue, BACKGROUND) == []


def test_a_contradictory_holder_does_not_end_the_claim_pass(
    queue: pool.PoolQueue, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A holder nobody can conclude is skipped, not raised through ``claim``.

    ``withdraw`` refuses on contradictory committed reservations
    (``AmbiguousClaimHolder``, a ``PoolContractError``).  That is a fact about
    somebody else's action; letting it out of ``claim`` would end a pass -- and
    with it every other item's chance of being admitted -- over bookkeeping the
    denied item has nothing to do with.
    """

    capacity = {"gpu": 1}
    queue.ledger().ensure_capacity(capacity)

    _publish(queue, BACKGROUND, priority=-10, resources={"gpu": 1})
    holder = queue.claim(capacity=capacity)
    assert holder is not None and holder["action_key"] == BACKGROUND

    _publish(queue, FOREGROUND, priority=0, resources={"gpu": 1})

    def refuse(self, key, *args, **kwargs):
        raise pool.AmbiguousClaimHolder(f"two hosts claim {key}")

    monkeypatch.setattr(pool.PoolQueue, "withdraw", refuse)

    assert queue.claim(capacity=capacity) is None
    assert not queue.item_path(pool.READY, BACKGROUND).exists()
    assert queue.item_path(pool.CLAIMED, BACKGROUND).exists()
    assert queue.ledger().held() == {"gpu": 1}
