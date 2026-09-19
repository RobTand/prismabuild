"""Prewarm sidecars are a cache: pruned when the queue is done, watched by generation.

Two contracts, one file.  Retention (#596): a receipt for a key nobody queues
any more is garbage -- the claim-time reference already resolved whatever the
terminal record keeps -- but only the queue may say the key needs nothing
more, so every deletion rule demands positive evidence and keeps the receipt
on anything ambiguous.  Liveness (#571): a warm selected for one generation
must stop when that generation leaves the queue, where the generation is the
``published_unix`` the queue itself stamps, and must never read a successor
generation as its own.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys
import time
import uuid

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import pool  # noqa: E402


def _key() -> str:
    return uuid.uuid4().hex + uuid.uuid4().hex


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    q = pool.PoolQueue(tmp_path / "pb-queue")
    q.ensure_layout()
    return q


def _publish(queue: pool.PoolQueue, key: str) -> dict:
    queue.publish(action_key=key, cas_root="/cas", checkout_root="/co",
                  worker_script="/w.py")
    return pool._read_json(queue.item_path(pool.READY, key))


def _receipt(queue: pool.PoolQueue, key: str, **overrides) -> None:
    record = {
        "host": "dl380g10", "manifest_sha256": "a" * 64,
        "manifest_bytes": 8, "bytes_warmed": 8,
        "entry_count": 1, "entries_warmed": 1, "status": "complete",
        "seconds": 0.1, "mb_per_s": 80.0,
    }
    record.update(overrides)
    queue.record_prewarm(key, record)


def _receipt_path(queue: pool.PoolQueue, key: str) -> Path:
    return queue.root / pool.PREWARM / f"{key}.json"


# -- retention ------------------------------------------------------------


def test_a_live_key_always_keeps_its_receipt(queue: pool.PoolQueue) -> None:
    key = _key()
    _publish(queue, key)
    _receipt(queue, key)
    verdict = queue.prune_prewarm_receipt(key, live=True)
    assert verdict == {"action_key": key, "pruned": False, "reason": "live"}
    assert _receipt_path(queue, key).is_file()


def test_a_terminal_key_is_pruned(queue: pool.PoolQueue) -> None:
    key = _key()
    _publish(queue, key)
    _receipt(queue, key)
    claimed = queue.claim()
    queue.finish(key, status="executed", detail={"status": "executed"},
                 claim_snapshot=claimed)
    verdict = queue.prune_prewarm_receipt(key, live=False)
    assert verdict == {"action_key": key, "pruned": True, "reason": "terminal"}
    assert not _receipt_path(queue, key).exists()


def test_a_withdrawn_key_keeps_an_unreleased_stage_band(
        queue: pool.PoolQueue) -> None:
    key = _key()
    _publish(queue, key)
    _receipt(queue, key, stage={"staged_through_bytes": 8})
    queue.withdraw(key, reason="test")
    verdict = queue.prune_prewarm_receipt(key, live=False)
    assert verdict == {"action_key": key, "pruned": False,
                       "reason": "stage band pending"}
    assert _receipt_path(queue, key).is_file()


def test_a_withdrawn_key_is_pruned_once_its_band_is_released(
        queue: pool.PoolQueue) -> None:
    key = _key()
    _publish(queue, key)
    _receipt(queue, key, stage={"staged_through_bytes": 8, "swept": True})
    queue.withdraw(key, reason="test")
    verdict = queue.prune_prewarm_receipt(key, live=False)
    assert verdict == {"action_key": key, "pruned": True,
                       "reason": "withdrawn"}
    assert not _receipt_path(queue, key).exists()


def test_an_unknown_state_is_kept_not_deleted(queue: pool.PoolQueue) -> None:
    """No terminal, no withdrawal, no age: keep, however odd the absence."""

    key = _key()
    _receipt(queue, key)
    verdict = queue.prune_prewarm_receipt(key, live=False)
    assert verdict == {"action_key": key, "pruned": False,
                       "reason": "unknown state"}
    assert _receipt_path(queue, key).is_file()


def test_a_swept_receipt_without_a_queue_event_ages_out(
        queue: pool.PoolQueue) -> None:
    """The safety valve: swept, queue-absent and older than retention."""

    key = _key()
    _receipt(queue, key, stage={"staged_through_bytes": 8, "swept": True})
    fresh = queue.prune_prewarm_receipt(key, live=False, now=time.time())
    assert fresh["pruned"] is False
    old = queue.prune_prewarm_receipt(
        key, live=False, now=time.time() + pool.PREWARM_RECEIPT_RETENTION_S + 1)
    assert old == {"action_key": key, "pruned": True, "reason": "stale"}
    assert not _receipt_path(queue, key).exists()


def test_pruning_a_missing_receipt_is_a_non_action(
        queue: pool.PoolQueue) -> None:
    key = _key()
    assert queue.prune_prewarm_receipt(key, live=False) == {
        "action_key": key, "pruned": False, "reason": "absent"}


def test_the_sweep_pass_prunes_only_non_live_receipts(
        queue: pool.PoolQueue) -> None:
    live_key = _key()
    _publish(queue, live_key)
    _receipt(queue, live_key)
    dead_key = _key()
    _publish(queue, dead_key)
    _receipt(queue, dead_key)
    claimed = queue.claim()
    if claimed["action_key"] == live_key:
        queue.finish(live_key, status="executed",
                     detail={"status": "executed"}, claim_snapshot=claimed)
        live_key, dead_key = dead_key, live_key
    else:
        queue.finish(dead_key, status="executed",
                     detail={"status": "executed"}, claim_snapshot=claimed)

    rows = queue.sweep_prewarm_receipts({live_key})
    by_key = {row["action_key"]: row for row in rows}
    assert by_key[dead_key]["pruned"] is True
    assert _receipt_path(queue, live_key).is_file()
    assert not _receipt_path(queue, dead_key).exists()


# -- generation-scoped liveness --------------------------------------------


def test_a_claimed_generation_is_live(queue: pool.PoolQueue) -> None:
    key = _key()
    item = _publish(queue, key)
    queue.claim()
    assert queue.selection_live(
        key, published_unix=item["published_unix"]) == "live"


def test_a_ready_generation_is_live(queue: pool.PoolQueue) -> None:
    key = _key()
    item = _publish(queue, key)
    assert queue.selection_live(
        key, published_unix=item["published_unix"]) == "live"


def test_a_finished_generation_is_terminal(queue: pool.PoolQueue) -> None:
    key = _key()
    item = _publish(queue, key)
    claimed = queue.claim()
    queue.finish(key, status="executed", detail={"status": "executed"},
                 claim_snapshot=claimed)
    assert queue.selection_live(
        key, published_unix=item["published_unix"]) == "terminal"


def test_a_successor_generation_reads_as_superseded_not_live(
        queue: pool.PoolQueue) -> None:
    """The same key under a new stamp is a different request (#571)."""

    key = _key()
    item = _publish(queue, key)
    live_claim = queue.claim()
    assert live_claim is not None
    # A successor generation holds the claim now: the old stamp is gone.
    successor = dict(live_claim)
    successor["published_unix"] = float(item["published_unix"]) + 1000.0
    queue.item_path(pool.CLAIMED, key).write_text(json.dumps(successor))
    assert queue.selection_live(
        key, published_unix=item["published_unix"]) == "superseded"


def test_a_withdrawn_generation_is_withdrawn(queue: pool.PoolQueue) -> None:
    key = _key()
    item = _publish(queue, key)
    queue.withdraw(key, reason="test")
    assert queue.selection_live(
        key, published_unix=item["published_unix"]) == "withdrawn"


def test_a_key_nowhere_is_unknown_not_terminal(
        queue: pool.PoolQueue) -> None:
    """Absence everywhere is a requeue in flight, not a verdict (#571)."""

    key = _key()
    item = _publish(queue, key)
    (queue.item_path(pool.READY, key)).unlink()
    assert queue.selection_live(
        key, published_unix=item["published_unix"]) == "unknown"


def test_an_unscoped_watch_is_unknown(queue: pool.PoolQueue) -> None:
    key = _key()
    _publish(queue, key)
    assert queue.selection_live(key, published_unix=None) == "unknown"


def test_an_unreadable_record_is_unknown_not_terminal(
        queue: pool.PoolQueue, monkeypatch) -> None:
    """Unavailable evidence never cancels a warm (#571)."""

    key = _key()
    item = _publish(queue, key)

    def failing(path, **kwargs):
        raise OSError("stale handle")

    monkeypatch.setattr(pool, "_read_json", failing)
    assert queue.selection_live(
        key, published_unix=item["published_unix"]) == "unknown"
