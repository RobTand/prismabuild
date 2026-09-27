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

    def scan(*, holds, **kwargs):
        with queue._transition_locked("a" * 64):
            queue.record_denial({"action_key": "a" * 64,
                                 "published_unix": 10 ** 400}, "placement_mismatch")

    monkeypatch.setattr(queue, "_claim_pass", scan)
    queue._claim()  # float(10**400) overflows; dropped, not raised
    assert pool._CLAIM_DIAGNOSTICS.get() is None
    assert not writes or all("a" * 64 not in (row.get("action_key") or "")
                             for row in writes[-1]["records"].values())


def test_exception_flushes_and_resets_context(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    item = {"action_key": "a" * 64, "published_unix": 1.0}
    writes = _writes(monkeypatch)

    def fail(*, holds, **kwargs):
        queue.record_denial(item, "placement_mismatch")
        assert not writes
        raise LookupError("original pass failure")

    monkeypatch.setattr(queue, "_claim_pass", fail)
    with pytest.raises(LookupError, match="original pass failure"):
        queue._claim()
    assert len(writes) == 1
    assert pool._CLAIM_DIAGNOSTICS.get() is None
    queue.record_denial(item, "placement_mismatch")
    assert len(writes) == 2  # standalone use still commits immediately


def test_failed_flush_does_not_mask_original_exception(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()

    def broken_write(*args):
        raise OSError("diagnostic disk full")

    def fail(*, holds, **kwargs):
        queue.record_denial({"action_key": "a" * 64, "published_unix": 1.0},
                            "placement_mismatch")
        raise LookupError("original pass failure")

    monkeypatch.setattr(adaptive_cpu, "write_json", broken_write)
    monkeypatch.setattr(queue, "_claim_pass", fail)
    with pytest.raises(LookupError, match="original pass failure"):
        queue._claim()
    assert pool._CLAIM_DIAGNOSTICS.get() is None


def test_reason_ring_is_written_before_latest_document_flush(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    item = {"action_key": "a" * 64, "published_unix": 1.0}
    writes = _writes(monkeypatch)

    def scan(*, holds, **kwargs):
        with queue._transition_locked(item["action_key"]) as acquired:
            assert acquired
            queue.record_denial(item, "reservation_unavailable")
            assert queue.denial_transitions_path(item["action_key"]).is_file()
            assert not writes

    monkeypatch.setattr(queue, "_claim_pass", scan)
    queue._claim()
    assert len(writes) == 1


def test_other_queue_does_not_join_current_batch(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / "one")
    other = pool.PoolQueue(tmp_path / "two")
    queue.ensure_layout()
    other.ensure_layout()
    item = {"action_key": "a" * 64, "published_unix": 1.0}

    def scan(*, holds, **kwargs):
        queue.record_denial(item, "placement_mismatch")
        other.record_denial(item, "placement_mismatch")
        assert local_records(other)
        assert not local_records(queue)

    monkeypatch.setattr(queue, "_claim_pass", scan)
    queue._claim()
    assert local_records(queue)


def test_interleaved_passes_merge_and_preserve_newer_peer_verdict(tmp_path, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    import threading

    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    clock = threading.local()
    first_buffered, second_flushed = threading.Event(), threading.Event()
    same = {"action_key": "a" * 64, "published_unix": 1.0}
    unrelated = {"action_key": "b" * 64, "published_unix": 1.0}
    monkeypatch.setattr(pool, "_now", lambda: clock.value)

    def scan(*, holds, newer, **kwargs):
        clock.value = 200.0 if newer else 100.0
        if newer:
            assert first_buffered.wait(5)
            queue.record_denial(same, "placement_mismatch", {"version": "new"})
            queue.record_denial(unrelated, "placement_mismatch")
        else:
            queue.record_denial(same, "placement_mismatch", {"version": "old"})
            first_buffered.set()
            assert second_flushed.wait(5)

    def newer_pass():
        try:
            queue._claim(newer=True)
        finally:
            second_flushed.set()

    monkeypatch.setattr(queue, "_claim_pass", scan)
    with ThreadPoolExecutor(max_workers=2) as executor:
        old = executor.submit(queue._claim, newer=False)
        new = executor.submit(newer_pass)
        old.result(timeout=10)
        new.result(timeout=10)
    records = list(local_records(queue).values())
    assert len(records) == 2
    assert next(row for row in records if row["action_key"] == same["action_key"])["evidence"] == {"version": "new"}


def test_pending_diagnostics_are_bounded(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()

    def scan(*, holds, **kwargs):
        for index in range(pool.MAX_CLAIM_DENIALS + 20):
            queue.record_denial({"action_key": f"{index:064x}", "published_unix": 1.0},
                                "placement_mismatch")
            batch = pool._CLAIM_DIAGNOSTICS.get()
            assert batch is not None
            assert len(batch.denials) <= pool.MAX_CLAIM_DENIALS

    monkeypatch.setattr(queue, "_claim_pass", scan)
    queue._claim()
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
