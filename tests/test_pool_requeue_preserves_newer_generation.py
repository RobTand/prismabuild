"""An older claim's retry cannot replace a newer publication of the same key."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from prismabuild import pool


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
