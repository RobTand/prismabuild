"""The queue owns exact immutable publication decisions and their recovery."""
from __future__ import annotations

import errno
import json
from pathlib import Path

import pytest

from prismabuild import immutable_files
from prismabuild import core as pb, pool
from test_pool_requeue_preserves_newer_generation import (
    DEMAND, ENDINGS, KEY, _end_claim, _publish, clock, queue,
)


def _collision(queue, clock):
    _publish(queue)
    original = queue.claim(owner="original", capacity=DEMAND)
    assert original is not None
    clock[0] += 1
    _publish(queue)
    successor = json.loads(queue.records.item_path(pool.record_store.READY, KEY).read_bytes())
    return original, successor


@pytest.mark.parametrize("ending", ENDINGS)
def test_queue_retains_typed_exact_successor_after_it_finishes(queue, clock, ending):
    original, successor = _collision(queue, clock)
    _end_claim(queue, original, ending, clock)

    decision = queue.publication_ending(KEY, generation=original["published_unix"])

    assert isinstance(decision, pool.PublicationSupersession)
    assert decision.action_key == KEY
    assert decision.published_unix == original["published_unix"]
    assert decision.successor_published_unix == successor["published_unix"]
    assert decision.record == {
        "schema": "prismabuild.publication_superseded.v1",
        "status": "superseded", "action_key": KEY,
        "published_unix": original["published_unix"], "successor": successor,
    }
    assert decision.path == (queue.root / "publication-endings" / KEY
                             / f"{queue.records.attempt_generation(original)}.json")
    assert decision.path.stat().st_mode & 0o222 == 0
    raw = decision.path.read_bytes()
    retry = queue.claim(owner="successor", capacity=DEMAND)
    assert retry is not None
    queue.finish(KEY, status="executed", claim_snapshot=retry)
    assert queue.publication_ending(KEY, generation=original["published_unix"]) == decision
    assert decision.path.read_bytes() == raw
    assert queue.publication_ending(KEY, generation=successor["published_unix"]) is None


@pytest.mark.parametrize("persisted", [False, True])
def test_interrupted_collision_keeps_first_persisted_successor(queue, clock, monkeypatch, persisted):
    original, first = _collision(queue, clock)
    address = queue.records.publication_ending_path(original)
    publish = immutable_files.publish_immutable

    def unavailable(path, raw, *args, **kwargs):
        if path == address:
            if persisted:
                publish(path, raw, *args, **kwargs)
            raise OSError(errno.EIO, "decision write uncertain")
        return publish(path, raw, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(immutable_files, "publish_immutable", unavailable)
        with pytest.raises(OSError, match="decision write uncertain"):
            _end_claim(queue, original, "finish_retry", clock)
    attempt = queue.records.attempt_path(original, 1)
    attempt_bytes = attempt.read_bytes()
    clock[0] += 1
    _publish(queue)
    later = json.loads(queue.records.item_path(pool.record_store.READY, KEY).read_bytes())
    clock[0] += pool.LEASE_TIMEOUT_S + pool.HEARTBEAT_S + 1
    queue.sweep_finish_tombstones()
    decision = queue.publication_ending(KEY, generation=original["published_unix"])
    assert decision is not None
    expected = first if persisted else later
    assert decision.successor_published_unix == expected["published_unix"]
    assert decision.record["successor"] == expected
    assert attempt.read_bytes() == attempt_bytes
    assert not queue.records.attempt_path(original, 2).exists()
    assert not queue.records.item_path(pool.record_store.CLAIMED, KEY).exists()


def test_public_decoder_validates_evidence_without_queue_reads(queue, clock, monkeypatch):
    original, successor = _collision(queue, clock)
    _end_claim(queue, original, "finish_retry", clock)
    decision = queue.publication_ending(KEY, generation=original["published_unix"])
    assert decision is not None

    def unexpected_read(*args, **kwargs):
        pytest.fail("decoding an observed publication ending performed I/O")

    monkeypatch.setattr(pb.immutable_files, '_read_file', unexpected_read)
    assert queue.decode_publication_supersession(decision.path, decision.record) == decision
    changed = {**decision.record, "successor": {**successor, "action_key": "0" * 64}}
    with pytest.raises(pool.PoolContractError):
        queue.decode_publication_supersession(decision.path, changed)
    with pytest.raises(pool.PoolContractError):
        queue.decode_publication_supersession(Path("/wrong-address.json"), decision.record)
