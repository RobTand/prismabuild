"""An older claim's retry cannot replace a newer publication of the same key."""
from __future__ import annotations

import errno
import json
from pathlib import Path

import pytest

from prismabuild import pool
import pbrun


KEY = "f" * 64
DEMAND = {"cpu": 1, "mem_gb": 1}
ENDINGS = ("finish_retry", "reap_lease_lost", "reap_unstarted", "defer_unstarted")


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    return pool.PoolQueue(tmp_path / "queue")


@pytest.fixture()
def clock(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    now = [1_800_000_000.0]
    monkeypatch.setattr(pool, "_now", lambda: now[0])
    return now


def _publish(queue: pool.PoolQueue, *, priority: int = 0) -> None:
    queue.publish(
        action_key=KEY,
        cas_root=queue.root / "cas",
        checkout_root=queue.root / "checkout",
        worker_script=queue.root / "worker.py",
        resources=DEMAND,
        max_attempts=2,
        retry_safe=True,
        priority=priority,
    )


def _end_claim(
    queue: pool.PoolQueue, claim: dict, ending: str, clock: list[float],
) -> None:
    if ending == "finish_retry":
        queue.finish(KEY, status="failed", detail={"returncode": 17},
                     claim_snapshot=claim)
    elif ending == "defer_unstarted":
        queue._defer_unstarted_claim(claim)
    else:
        if ending == "reap_unstarted":
            # Reproduce the lost write between the claim rename and its lease.
            queue.lease_path(KEY).unlink()
        clock[0] += max(pool.LEASE_TIMEOUT_S, pool.HEARTBEAT_S) + 1
        queue.reap_stale()


@pytest.mark.parametrize("ending", ENDINGS)
def test_requeue_preserves_newer_ready_publication(
    queue: pool.PoolQueue, clock: list[float], ending: str,
) -> None:
    _publish(queue)
    claim = queue.claim(owner="first-claim", capacity=DEMAND)
    assert claim is not None
    assert queue.ledger().held() == DEMAND

    clock[0] += 1
    _publish(queue, priority=7)
    ready = queue.item_path(pool.READY, KEY)
    newer_bytes = ready.read_bytes()
    newer = json.loads(newer_bytes)
    assert newer["published_unix"] > claim["published_unix"]

    _end_claim(queue, claim, ending, clock)

    assert ready.read_bytes() == newer_bytes, (
        f"{ending} displaced the newer ready publication with its older claim"
    )
    assert not queue.item_path(pool.CLAIMED, KEY).exists()
    assert not queue.lease_path(KEY).exists()
    assert queue.ledger().held() == {}
    filed = [json.loads(path.read_bytes())
             for path in queue.superseded_dir().glob(f"{KEY}.*.json")]
    retired = [record for record in filed if record.get("status") == "dropped"]
    assert len(retired) == 1
    assert retired[0]["published_unix"] == claim["published_unix"]
    charged = ending in {"finish_retry", "reap_lease_lost"}
    assert retired[0]["attempts"] == int(charged)
    old_outcomes = queue.attempt_outcomes(retired[0])
    assert len(old_outcomes) == int(charged)
    if ending == "defer_unstarted":
        assert retired[0]["reason"] == (
            "a newer publication already owns ready after maintenance deferral"
        )

    successor = queue.claim(owner="new-publication", capacity=DEMAND)
    assert successor is not None
    assert successor["published_unix"] == newer["published_unix"]
    assert successor["attempts"] == 0
    terminal = json.loads(queue.finish(
        KEY, status="executed", claim_snapshot=successor,
    ).read_bytes())
    assert terminal["published_unix"] == newer["published_unix"]
    assert terminal["attempts"] == 1
    assert [outcome["status"] for outcome in queue.attempt_outcomes(terminal)] == ["executed"]
    assert queue.attempt_outcomes(retired[0]) == old_outcomes
    assert not queue.attempt_path(claim, 2).exists()
    assert queue.ledger().held() == {}
    landed, watched = pbrun.outcome_poll(queue, KEY, newer["published_unix"])
    assert landed is not None and landed[1]["status"] == "executed"
    assert watched == newer["published_unix"]
    superseded, watched = pbrun.outcome_poll(queue, KEY, claim["published_unix"])
    assert isinstance(superseded, pool.PublicationSupersession)
    assert watched == claim["published_unix"]
    assert superseded.successor_published_unix == newer["published_unix"]


@pytest.mark.parametrize("ending", ENDINGS)
def test_requeue_preserves_attempt_budget_and_releases_capacity(
    queue: pool.PoolQueue, clock: list[float], ending: str,
) -> None:
    _publish(queue)
    claim = queue.claim(owner="first-claim", capacity=DEMAND)
    assert claim is not None
    assert claim["attempts"] == 0
    assert queue.ledger().held() == DEMAND

    _end_claim(queue, claim, ending, clock)

    ready = json.loads(queue.item_path(pool.READY, KEY).read_bytes())
    charged = ending in {"finish_retry", "reap_lease_lost"}
    assert ready["published_unix"] == claim["published_unix"]
    assert ready["attempts"] == int(charged)
    assert ready["max_attempts"] == 2
    assert ready["retry_safe"] is True
    assert not queue.item_path(pool.CLAIMED, KEY).exists()
    assert not queue.lease_path(KEY).exists()
    assert queue.ledger().held() == {}
    outcomes = queue.attempt_outcomes(ready)
    if charged:
        assert len(outcomes) == 1
        assert outcomes[0]["attempt"] == 1
        assert outcomes[0]["status"] == (
            "failed" if ending == "finish_retry" else "lease_lost"
        )
        assert outcomes[0]["disposition"] == "requeued"
    else:
        assert outcomes == []
        assert not queue.attempt_path(claim, 1).exists()

    clock[0] += 1
    successor = queue.claim(owner="first-claim", capacity=DEMAND)
    assert successor is not None
    assert successor["published_unix"] == claim["published_unix"]
    assert successor["attempts"] == int(charged)
    assert not pool._same_claim(successor, claim)
    assert queue.ledger().held() == DEMAND


@pytest.mark.parametrize("ending", ENDINGS)
@pytest.mark.parametrize("failure", ("link", "collision_filing"))
def test_requeue_publication_error_retains_original_claim_for_recovery(
    queue: pool.PoolQueue, clock: list[float], ending: str, failure: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _publish(queue)
    claim = queue.claim(owner="first-claim", capacity=DEMAND)
    assert claim is not None
    original_bytes = queue.item_path(pool.CLAIMED, KEY).read_bytes()
    ready = queue.item_path(pool.READY, KEY)
    if failure == "collision_filing":
        clock[0] += 1
        _publish(queue, priority=7)
        newer_bytes = ready.read_bytes()

    link = pool.os.link
    file_superseded = queue._file_superseded

    def failing_link(source, destination, *args, **kwargs):
        if destination == ready:
            raise OSError(errno.EIO, "requeue link unavailable")
        return link(source, destination, *args, **kwargs)

    def failing_filing(record, **stamps):
        if stamps["kind"] in {"retry-superseded", "unstarted-requeue", "maintenance-deferred"}:
            raise OSError(errno.EIO, "requeue collision filing unavailable")
        return file_superseded(record, **stamps)

    with monkeypatch.context() as patch:
        if failure == "link":
            patch.setattr(pool.os, "link", failing_link)
        else:
            patch.setattr(queue, "_file_superseded", failing_filing)
        if ending.startswith("reap_"):
            _end_claim(queue, claim, ending, clock)
        else:
            with pytest.raises(OSError, match="unavailable"):
                _end_claim(queue, claim, ending, clock)

    retained = [
        *queue.dir(pool.CLAIMED).glob(f"*{pool.TOMBSTONE_SUFFIX}"),
        *queue.dir(pool.CLAIMED).glob(f"{KEY}.json"),
    ]
    assert len(retained) == 1
    assert retained[0].read_bytes() == original_bytes
    assert not queue.lease_path(KEY).exists()
    assert queue.ledger().held() == {}
    if failure == "collision_filing":
        assert ready.read_bytes() == newer_bytes
    else:
        assert not ready.exists()

    clock[0] += max(pool.LEASE_TIMEOUT_S, pool.HEARTBEAT_S) + 1
    # A retained tombstone restores the claim on one poll, then concludes it.
    queue.reap_stale()
    queue.reap_stale()

    assert not queue.item_path(pool.CLAIMED, KEY).exists()
    assert not list(queue.dir(pool.CLAIMED).glob(f"*{pool.TOMBSTONE_SUFFIX}"))
    assert queue.ledger().held() == {}
    charged = ending in {"finish_retry", "reap_lease_lost"}
    assert queue.attempt_path(claim, 1).exists() == charged
    assert not queue.attempt_path(claim, 2).exists()
    if failure == "collision_filing":
        assert ready.read_bytes() == newer_bytes
    else:
        requeued = json.loads(ready.read_bytes())
        assert requeued["published_unix"] == claim["published_unix"]
        assert requeued["attempts"] == int(charged)
        outcomes = queue.attempt_outcomes(requeued)
        assert len(outcomes) == int(charged)
        if charged:
            assert outcomes[0]["status"] == (
                "failed" if ending == "finish_retry" else "lease_lost"
            )
