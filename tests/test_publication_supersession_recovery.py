"""Retained claims and immutable decisions survive interrupted supersession."""
from __future__ import annotations

import errno
import json
import os
from pathlib import Path

import pytest

from prismabuild import immutable_files
from prismabuild import core as pb, pool
import pbrun

from test_pool_requeue_preserves_newer_generation import (
    DEMAND, KEY, _end_claim, _publish, clock, queue,
)
from test_publication_supersession import _assert_superseded, _decision_path, _ready


def _collision(queue: pool.PoolQueue, clock: list[float]):
    _publish(queue)
    original = queue.claim(owner="original", capacity=DEMAND)
    assert original is not None
    clock[0] += 1
    _publish(queue)
    return original, _ready(queue)["published_unix"]


def _recover(queue: pool.PoolQueue, clock: list[float]) -> None:
    clock[0] += max(pool.LEASE_TIMEOUT_S, pool.HEARTBEAT_S) + 1
    queue.reap_stale()
    queue.reap_stale()


@pytest.mark.parametrize("boundary", [
    "ready_collision", "dropped_filing", "tombstone_unlink",
])
def test_death_converges_without_losing_original_or_successor_authority(
    queue: pool.PoolQueue, clock: list[float], boundary: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original, successor_generation = _collision(queue, clock)
    generation = original["published_unix"]
    ready = queue.records.item_path(pool.record_store.READY, KEY)
    ready_bytes = ready.read_bytes()
    pid = os.fork()
    if pid == 0:
        try:
            if boundary == "ready_collision":
                real_link = pool.os.link

                def stop_at_collision(source, destination, *args, **kwargs):
                    try:
                        return real_link(source, destination, *args, **kwargs)
                    except FileExistsError:
                        if Path(destination) == ready:
                            os._exit(91)
                        raise

                monkeypatch.setattr(pool.os, "link", stop_at_collision)
            elif boundary == "dropped_filing":
                real_filing = queue._file_superseded

                def stop_at_filing(record, **stamps):
                    if stamps["kind"] == "retry-superseded":
                        os._exit(91)
                    return real_filing(record, **stamps)

                monkeypatch.setattr(queue, "_file_superseded", stop_at_filing)
            else:
                real_unlink = Path.unlink

                def stop_at_unlink(path, *args, **kwargs):
                    if path.name.startswith(f"{KEY}.") and path.name.endswith(
                            pool.record_store.TOMBSTONE_SUFFIX):
                        os._exit(91)
                    return real_unlink(path, *args, **kwargs)

                monkeypatch.setattr(Path, "unlink", stop_at_unlink)
            _end_claim(queue, original, "finish_retry", clock)
        except BaseException:
            os._exit(99)
        os._exit(98)
    _, status = os.waitpid(pid, 0)
    assert os.waitstatus_to_exitcode(status) == 91
    tombstones = list(queue.records.dir(pool.record_store.CLAIMED).glob(f"{KEY}.*{pool.record_store.TOMBSTONE_SUFFIX}"))
    assert len(tombstones) == 1
    assert json.loads(tombstones[0].read_bytes())["published_unix"] == generation
    attempt = queue.records.attempt_path(original, 1)
    attempt_bytes = attempt.read_bytes()
    if boundary == "ready_collision":
        assert pbrun.outcome_poll(queue, KEY, generation) == (None, generation)
    else:
        _assert_superseded(queue, generation, successor_generation)

    _recover(queue, clock)
    _recover(queue, clock)

    _assert_superseded(queue, generation, successor_generation)
    assert ready.read_bytes() == ready_bytes
    assert not list(queue.records.dir(pool.record_store.CLAIMED).glob(f"{KEY}.*{pool.record_store.TOMBSTONE_SUFFIX}"))
    assert attempt.read_bytes() == attempt_bytes
    assert not queue.records.attempt_path(original, 2).exists()
    assert queue.ledger().held() == {}


@pytest.mark.parametrize("effect", ["before_write", "persist_then_error"])
def test_uncertain_write_retains_authority_and_first_persisted_successor_wins(
    queue: pool.PoolQueue, clock: list[float], effect: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original, first_successor = _collision(queue, clock)
    generation = original["published_unix"]
    decision = _decision_path(queue, generation)
    real_publish = immutable_files.publish_immutable

    def uncertain_publish(path, raw, *args, **kwargs):
        if Path(path) == decision:
            if effect == "persist_then_error":
                real_publish(path, raw, *args, **kwargs)
            raise OSError(errno.EIO, "uncertain publication decision write")
        return real_publish(path, raw, *args, **kwargs)

    refused = False
    with monkeypatch.context() as patch:
        patch.setattr(immutable_files, "publish_immutable", uncertain_publish)
        try:
            _end_claim(queue, original, "finish_retry", clock)
        except OSError as exc:
            assert exc.errno == errno.EIO
            refused = True
    assert refused, "the immutable publication decision was never attempted"
    retained = [*queue.records.dir(pool.record_store.CLAIMED).glob(f"{KEY}.json"),
                *queue.records.dir(pool.record_store.CLAIMED).glob(f"{KEY}.*{pool.record_store.TOMBSTONE_SUFFIX}")]
    assert len(retained) == 1
    assert json.loads(retained[0].read_bytes())["published_unix"] == generation
    attempt = queue.records.attempt_path(original, 1)
    attempt_bytes = attempt.read_bytes()
    if effect == "persist_then_error":
        _assert_superseded(queue, generation, first_successor)
        first_decision_bytes = decision.read_bytes()
    else:
        assert not decision.exists()
        assert pbrun.outcome_poll(queue, KEY, generation) == (None, generation)

    clock[0] += 1
    _publish(queue)
    later_successor = _ready(queue)["published_unix"]
    assert later_successor == first_successor + 1
    later_bytes = queue.records.item_path(pool.record_store.READY, KEY).read_bytes()
    _recover(queue, clock)
    _recover(queue, clock)

    expected = first_successor if effect == "persist_then_error" else later_successor
    _assert_superseded(queue, generation, expected)
    if effect == "persist_then_error":
        assert decision.read_bytes() == first_decision_bytes
    assert queue.records.item_path(pool.record_store.READY, KEY).read_bytes() == later_bytes
    assert attempt.read_bytes() == attempt_bytes
    assert not queue.records.attempt_path(original, 2).exists()


@pytest.mark.parametrize("damage", ["malformed", "wrong_original", "wrong_successor"])
def test_corrupt_exact_decision_is_unavailable_instead_of_an_execution_verdict(
    queue: pool.PoolQueue, clock: list[float], damage: str,
) -> None:
    original, successor = _collision(queue, clock)
    generation = original["published_unix"]
    _end_claim(queue, original, "finish_retry", clock)
    decision = _decision_path(queue, generation)
    assert decision.is_file(), "no exact publication ending was retained"
    record = json.loads(decision.read_bytes())
    assert record["schema"] == "prismabuild.publication_superseded.v1"
    assert record["status"] == "superseded"
    assert record["published_unix"] == generation
    assert record["successor"]["published_unix"] == successor
    if damage == "malformed":
        raw = b"{unfinished"
    else:
        if damage == "wrong_original":
            record["published_unix"] = generation - 1
        else:
            record["successor"]["action_key"] = "0" * 64
        raw = json.dumps(record).encode()
    decision.chmod(0o600)
    decision.write_bytes(raw)
    decision.chmod(0o444)

    assert pbrun.await_outcome(queue, KEY, wait_s=0, generation=generation) == 74
