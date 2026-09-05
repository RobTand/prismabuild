"""A losing claimant returns its own tokens and not the winner's.

Admission runs before the ready-to-claimed rename, so at the moment tokens are
taken it is not yet decided which contender owns the action. Filing them under
``held/<action_key>`` gave every contender for one key the same rollback
target, and the loser's release handed back the winner's reservation. A third
action was then admitted on capacity the winner was already executing against.
"""

from __future__ import annotations

from pathlib import Path
import sys
from unittest import mock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import pool  # noqa: E402

KEY_A = "a" * 64
KEY_B = "b" * 64


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    q = pool.PoolQueue(tmp_path / "pb-queue")
    q.ensure_layout()
    return q


def _publish(q: pool.PoolQueue, key: str, **kw: object) -> None:
    q.publish(
        action_key=key,
        cas_root=q.root / "cas",
        checkout_root=q.root / "co",
        worker_script=q.root / "worker.py",
        resources={"cpu": 1},
        **kw,
    )


def test_losing_same_key_claimant_keeps_the_winners_tokens(
    queue: pool.PoolQueue,
) -> None:
    """The issue #60 interleaving, asserting the reservation that must survive."""

    _publish(queue, KEY_A)
    original_intent = queue._write_claim_intent
    observed: dict[str, object] = {}

    def intent(key: str, *, owner: str) -> None:
        if owner == "winner":
            # The winner holds the only CPU token and has not renamed ready
            # yet. A second worker reads the same ready record, fails
            # admission, and rolls its own admission back.
            observed["held_before_contender"] = queue.ledger().held()
            observed["contender"] = queue.claim(
                owner="loser", capacity={"cpu": 1})
            observed["held_after_contender"] = queue.ledger().held()
        return original_intent(key, owner=owner)

    with mock.patch.object(queue, "_write_claim_intent", intent):
        winner = queue.claim(owner="winner", capacity={"cpu": 1})

    assert winner is not None and winner["claimed_by"] == "winner"
    assert observed["contender"] is None
    # The contender's rollback must not reach the winner's tokens.
    assert observed["held_before_contender"] == {"cpu": 1}
    assert observed["held_after_contender"] == {"cpu": 1}
    assert queue.ledger().held() == {"cpu": 1}
    assert queue.ledger().held_keys() == [KEY_A]
    assert queue.ledger().available() == {}

    # And a second action is refused on a box whose only token is spoken for.
    _publish(queue, KEY_B)
    other = queue.claim(owner="other-action", capacity={"cpu": 1})
    assert other is None
    assert len(list(queue.dir(pool.CLAIMED).glob("*.json"))) == 1


def test_a_crashed_claimants_private_tokens_are_swept(
    queue: pool.PoolQueue,
) -> None:
    """Tokens taken but never committed are recovered, and only when stale."""

    ledger = queue.ledger()
    ledger.ensure_capacity({"cpu": 1})
    handle = ledger.begin_acquire(KEY_A, {"cpu": 1})
    assert handle is not None
    # In flight, the tokens are not free and not attributable to an action.
    assert ledger.available() == {}
    assert ledger.held() == {"cpu": 1}
    assert ledger.held_keys() == []

    # A claimant still inside its own window is left alone, and nothing else
    # in the queue can recover these: they are filed under the claimant, so no
    # claimed record names them.
    assert queue.sweep_stale_acquisitions() == []
    assert queue.reap_stale(timeout_s=-1) == []
    assert ledger.held() == {"cpu": 1}

    swept = queue.sweep_stale_acquisitions(grace_s=-1.0)
    assert swept == [f"{ledger.host}/{handle}"]
    assert ledger.available() == {"cpu": 1}
    assert ledger.held() == {}
    assert not (ledger.held_dir / handle).exists()


def _restamp(ledger: pool.ResourceLedger, handle: str, *, unix: float,
             pid: int | None = None) -> str:
    """Rewrite an acquisition's stamped clock, and optionally its pid."""

    parts = handle.split(".")
    parts[1] = str(int(unix * 1_000_000))
    if pid is not None:
        parts[4] = str(pid)
    renamed = ".".join(parts)
    (ledger.held_dir / handle).rename(ledger.held_dir / renamed)
    return renamed


def test_reap_stale_sweeps_an_abandoned_acquisition(
    queue: pool.PoolQueue,
) -> None:
    """The recovery is wired to the reaper, at the lease timeout."""

    ledger = queue.ledger()
    ledger.ensure_capacity({"cpu": 1})
    handle = ledger.begin_acquire(KEY_A, {"cpu": 1})
    assert handle is not None

    # Older than the heartbeat but inside the lease timeout, with a live pid:
    # this sweep has no heartbeat behind it, so it waits.
    handle = _restamp(ledger, handle, unix=pool._now() - 2 * pool.HEARTBEAT_S)
    assert queue.reap_stale() == []
    assert ledger.held() == {"cpu": 1}

    # Past the lease timeout it goes back, from the reaper's own call.
    handle = _restamp(ledger, handle, unix=pool._now() - pool.LEASE_TIMEOUT_S - 1)
    assert queue.reap_stale() == []
    assert ledger.held() == {}
    assert ledger.available() == {"cpu": 1}


def test_a_dead_claimant_on_this_host_is_swept_at_the_heartbeat(
    queue: pool.PoolQueue,
) -> None:
    """A stamped pid that is gone is proof; there is nothing to wait for."""

    ledger = queue.ledger()
    ledger.ensure_capacity({"cpu": 1})
    handle = ledger.begin_acquire(KEY_A, {"cpu": 1})
    assert handle is not None
    # A pid that cannot exist, on this host, older than the heartbeat.
    handle = _restamp(
        ledger, handle,
        unix=pool._now() - 2 * pool.HEARTBEAT_S,
        pid=2 ** 31 - 1,
    )
    assert queue.reap_stale() == []
    assert ledger.held() == {}
    assert ledger.available() == {"cpu": 1}


def test_a_swept_claimant_does_not_run_unreserved(
    queue: pool.PoolQueue,
) -> None:
    """A commit that lost its tokens puts the item back rather than run it."""

    _publish(queue, KEY_A)
    original_commit = pool.ResourceLedger.commit_acquire

    def commit(self: pool.ResourceLedger, action_key: str, handle: str) -> int:
        # The sweep fires between this claimant's acquire and its commit.
        queue.sweep_stale_acquisitions(grace_s=-1.0)
        return original_commit(self, action_key, handle)

    with mock.patch.object(pool.ResourceLedger, "commit_acquire", commit):
        claimed = queue.claim(owner="swept", capacity={"cpu": 1})

    assert claimed is None
    assert queue.item_path(pool.READY, KEY_A).exists()
    assert not queue.item_path(pool.CLAIMED, KEY_A).exists()
    assert queue.ledger().held() == {}
    assert queue.ledger().available() == {"cpu": 1}
    # The item is intact, so the next poll claims it normally.
    again = queue.claim(owner="next", capacity={"cpu": 1})
    assert again is not None and again["action_key"] == KEY_A
    assert queue.ledger().held() == {"cpu": 1}
