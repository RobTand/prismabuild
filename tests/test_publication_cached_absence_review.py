"""A verified READY successor must outrank a cached negative stat during recovery."""
from __future__ import annotations

import errno
from pathlib import Path

import pytest

from prismabuild import immutable_files
from prismabuild import core as pb, pool

from test_pool_requeue_preserves_newer_generation import (
    KEY, _end_claim, clock, queue,
)
from test_publication_successor_review import _collision


def test_ready_successor_read_prevents_restoring_old_claim_after_cached_absence(
    queue: pool.PoolQueue, clock: list[float], monkeypatch: pytest.MonkeyPatch,
):
    original, ready, successor = _collision(queue, clock)
    ready_bytes = ready.read_bytes()
    address = queue.publication_ending_path(original)
    atomic_publish = immutable_files.publish_immutable

    def unavailable(path, raw, *args, **kwargs):
        if Path(path) == address:
            raise OSError(errno.EIO, "decision write unavailable")
        return atomic_publish(path, raw, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(immutable_files, "publish_immutable", unavailable)
        with pytest.raises(OSError, match="decision write unavailable"):
            _end_claim(queue, original, "finish_retry", clock)
    marks = list(queue.dir(pool.CLAIMED).glob(f"{KEY}.*{pool.TOMBSTONE_SUFFIX}"))
    assert len(marks) == 1
    attempt = queue.attempt_path(original, 1)
    attempt_bytes = attempt.read_bytes()
    clock[0] += max(pool.LEASE_TIMEOUT_S, pool.HEARTBEAT_S) + 1
    exists = Path.exists

    def cached_absence(path):
        return False if path == ready else exists(path)

    with monkeypatch.context() as patch:
        patch.setattr(Path, "exists", cached_absence)
        queue.sweep_finish_tombstones()

    assert ready.read_bytes() == ready_bytes
    ending = queue.publication_ending(KEY, generation=original["published_unix"])
    assert isinstance(ending, pool.PublicationSupersession)
    assert ending.successor_published_unix == successor["published_unix"]
    assert not queue.item_path(pool.CLAIMED, KEY).exists()
    assert not list(queue.dir(pool.CLAIMED).glob(f"{KEY}.*{pool.TOMBSTONE_SUFFIX}"))
    assert attempt.read_bytes() == attempt_bytes
    assert not queue.attempt_path(original, 2).exists()
