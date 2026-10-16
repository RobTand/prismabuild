"""#1221: one latest-only diagnostic commit per claim pass, never per row."""
from __future__ import annotations

import json
import os
import socket
from pathlib import Path

import pytest

from prismabuild import adaptive_cpu, pool
from test_claim_denials import local_records, publish


def _writes(monkeypatch):
    writes = []
    original = adaptive_cpu.write_json

    def record(path, value):
        if Path(path).name == pool.CLAIM_DENIALS:
            writes.append(json.loads(json.dumps(value)))
        return original(path, value)

    monkeypatch.setattr(adaptive_cpu, "write_json", record)
    return writes


@pytest.mark.parametrize("placement_mismatch", [False, True])
def test_one_diagnostic_write_per_pass_not_per_candidate(tmp_path, monkeypatch, placement_mismatch):
    queue = pool.PoolQueue(tmp_path / "queue")
    for index in range(20):
        publish(queue, f"{index:064x}", resources={"cpu": 2},
                tags=["foreign"] if placement_mismatch else [])
    writes = _writes(monkeypatch)
    assert queue.claim(capacity={"cpu": 1}) is None
    assert len(writes) == 1, f"one pass rewrote the full map {len(writes)} times"
    assert len(writes[0]["records"]) == 20
    if not placement_mismatch:
        assert str(os.getpid()) in writes[0]["claim_passes"]


def test_record_pass_reads_its_existing_sidecar_once(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    key = "f" * 64
    assert queue.record_pass(key) == 1
    path = queue.passes_path(key)
    first = json.loads(path.read_text())["first_unix"]
    reads = []
    original = pool._read_json

    def record(candidate):
        if candidate == path:
            reads.append(candidate)
        return original(candidate)

    monkeypatch.setattr(pool, "_read_json", record)
    assert queue.record_pass(key) == 2
    assert len(reads) == 1, f"one increment reread the sidecar {len(reads)} times"
    after = json.loads(path.read_text())
    assert after["passes"] == 2
    assert after["first_unix"] == first


def test_summaries_only_flush_preserves_existing_records(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    queue.record_denial({"action_key": "a" * 64, "published_unix": 1.0}, "placement_mismatch")
    before = local_records(queue)
    assert before
    queue._write_claim_diagnostics({}, {"999": {
        "host": socket.gethostname(), "pid": 999, "passed_unix": 1.0}})
    path = adaptive_cpu.local_state_base(queue.ledger().base) / pool.CLAIM_DENIALS
    document = json.loads(path.read_text())
    assert document["records"] == before
    assert "999" in document["claim_passes"]


def test_overflowing_generation_is_dropped_not_raised(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    writes = _writes(monkeypatch)
    queue.record_denial({"action_key": "a" * 64, "published_unix": 10 ** 400},
                        "placement_mismatch")
    assert pool._CLAIM_DIAGNOSTICS.get() is None
    assert not writes or all("a" * 64 not in (row.get("action_key") or "")
                             for row in writes[-1]["records"].values())


def test_exception_flushes_and_resets_context(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / "queue")
    key = "a" * 64
    publish(queue, key, resources={"cpu": 2})
    writes = _writes(monkeypatch)
    original = queue.record_denial

    def fail(item, reason, evidence=None):
        original(item, reason, evidence)
        assert not writes
        raise LookupError("original pass failure")

    monkeypatch.setattr(queue, "record_denial", fail)
    with pytest.raises(LookupError, match="original pass failure"):
        queue.claim(capacity={"cpu": 1})
    assert len(writes) == 1
    assert pool._CLAIM_DIAGNOSTICS.get() is None
    assert queue.item_path(pool.READY, key).is_file()
    assert queue.ledger().held_keys() == []
    monkeypatch.setattr(queue, "record_denial", original)
    queue.record_denial(pool.read_queue_record(queue.item_path(pool.READY, key)), "placement_mismatch")
    assert len(writes) == 2


def test_failed_flush_does_not_mask_original_exception(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / "queue")
    key = "a" * 64
    publish(queue, key, resources={"cpu": 2})
    original = queue.record_denial

    def broken_write(*args):
        raise OSError("diagnostic disk full")

    def fail(item, reason, evidence=None):
        original(item, reason, evidence)
        raise LookupError("original pass failure")

    monkeypatch.setattr(adaptive_cpu, "write_json", broken_write)
    monkeypatch.setattr(queue, "record_denial", fail)
    with pytest.raises(LookupError, match="original pass failure"):
        queue.claim(capacity={"cpu": 1})
    assert pool._CLAIM_DIAGNOSTICS.get() is None
    assert queue.item_path(pool.READY, key).is_file()
    assert queue.ledger().held_keys() == []


def test_reason_ring_is_written_before_latest_document_flush(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / "queue")
    key = "a" * 64
    publish(queue, key, resources={"cpu": 2})
    writes = _writes(monkeypatch)
    original = queue.record_denial

    def observe(item, reason, evidence=None):
        original(item, reason, evidence)
        assert queue.denial_transitions_path(key).is_file()
        assert not writes

    monkeypatch.setattr(queue, "record_denial", observe)
    assert queue.claim(capacity={"cpu": 1}) is None
    assert len(writes) == 1


def test_other_queue_does_not_join_current_batch(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / "one")
    other = pool.PoolQueue(tmp_path / "two")
    publish(queue, "a" * 64, resources={"cpu": 2})
    other.ensure_layout()
    original = queue.record_denial

    def observe(item, reason, evidence=None):
        original(item, reason, evidence)
        other.record_denial(item, reason, evidence)
        assert local_records(other)
        assert not local_records(queue)

    monkeypatch.setattr(queue, "record_denial", observe)
    assert queue.claim(capacity={"cpu": 1}) is None
    assert local_records(queue)


def test_interleaved_passes_merge_and_preserve_newer_peer_verdict(tmp_path, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    import threading

    queue = pool.PoolQueue(tmp_path / "queue")
    clock = threading.local()
    clock.value = 1.0
    monkeypatch.setattr(pool, "_now", lambda: clock.value)
    same, unrelated = "a" * 64, "b" * 64
    for key in (same, unrelated):
        publish(queue, key, resources={"cpu": 1}, tags=["foreign"])
    first_buffered, second_flushed = threading.Event(), threading.Event()
    original = queue.record_denial

    def observe(item, reason, evidence=None):
        newer = clock.value == 200.0
        if item["action_key"] == same:
            if newer:
                assert first_buffered.wait(5)
                original(item, reason, {"version": "new"})
            else:
                original(item, reason, {"version": "old"})
                first_buffered.set()
                assert second_flushed.wait(5)
        else:
            original(item, reason, evidence)

    def claim(newer):
        clock.value = 200.0 if newer else 100.0
        try:
            assert queue.claim(tags=["local"], owner="diagnostic-observer") is None
        finally:
            if newer:
                second_flushed.set()

    monkeypatch.setattr(queue, "record_denial", observe)
    with ThreadPoolExecutor(max_workers=2) as executor:
        old = executor.submit(claim, False)
        new = executor.submit(claim, True)
        old.result(timeout=10)
        new.result(timeout=10)
    records = list(local_records(queue).values())
    assert len(records) == 2
    assert next(row for row in records if row["action_key"] == same)["evidence"] == {"version": "new"}
    assert all(queue.item_path(pool.READY, key).is_file() for key in (same, unrelated))


def test_pending_diagnostics_are_bounded(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / "queue")
    for index in range(pool.MAX_CLAIM_DENIALS + 20):
        publish(queue, f"{index:064x}", resources={"cpu": 1}, tags=["foreign"])
    original = queue.record_denial

    def observe(item, reason, evidence=None):
        original(item, reason, evidence)
        batch = pool._CLAIM_DIAGNOSTICS.get()
        assert batch is not None
        assert len(batch.denials) <= pool.MAX_CLAIM_DENIALS

    monkeypatch.setattr(queue, "record_denial", observe)
    assert queue.claim(tags=["local"]) is None
    assert len(local_records(queue)) == pool.MAX_CLAIM_DENIALS


def test_diagnostics_lock_contention_drops_batch_not_claim(tmp_path):
    import fcntl

    queue = pool.PoolQueue(tmp_path / "queue")
    publish(queue, "a" * 64, resources={"cpu": 2})
    base = adaptive_cpu.local_state_base(queue.ledger().base)
    with (base / "claim-denials.lock").open("a") as held:
        fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert queue.claim(capacity={"cpu": 1}) is None
        assert not local_records(queue)
        assert queue.denial_transitions_path("a" * 64).is_file()
    assert queue.claim(capacity={"cpu": 1}) is None
    assert local_records(queue)
