"""One unparseable ``ready/*.json`` must not stop the queue on every box.

``_read_json`` refuses a record it cannot parse.  That is right for a caller
addressing one key: the operator asked about that action and should be told
its bytes are broken.  It is wrong for the scans that walk the whole
directory, because there a single truncated file made ``claim``,
``ready_items``, ``reap_stale`` and ``quarantine_orphans`` raise on every box
at once -- and one of the four is the sweep that would have cleared it.

The queue already has the verb for a ready record no consumer can address.
These tests pin that a malformed one takes the same route: the healthy items
keep flowing, the bytes are kept, and the defect is countable in ``failed/``
rather than invisible.
"""

from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import pool  # noqa: E402

KEY_GOOD = "a" * 64
KEY_TRUNCATED = "b" * 64
KEY_INVALID = "c" * 64
KEY_EMPTY = "d" * 64


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


def _foreign_writes_a_broken_record(q: pool.PoolQueue) -> None:
    """What a foreign writer leaves behind: a half-written and a non-JSON file."""

    q.item_path(pool.READY, KEY_TRUNCATED).write_bytes(
        b'{"schema": "prismaquant.prismabuild.pool_item.v1", "action_key": "'
    )
    q.item_path(pool.READY, KEY_INVALID).write_bytes(b"not json at all\n")


def test_ready_items_serves_the_healthy_items(queue: pool.PoolQueue) -> None:
    _publish(queue, KEY_GOOD)
    _foreign_writes_a_broken_record(queue)
    assert [r["action_key"] for r in queue.ready_items()] == [KEY_GOOD]


def test_claim_serves_the_healthy_items(queue: pool.PoolQueue) -> None:
    _publish(queue, KEY_GOOD)
    _foreign_writes_a_broken_record(queue)
    claimed = queue.claim()
    assert claimed is not None and claimed["action_key"] == KEY_GOOD


def test_reap_stale_still_runs(queue: pool.PoolQueue) -> None:
    _publish(queue, KEY_GOOD)
    _foreign_writes_a_broken_record(queue)
    assert queue.reap_stale(timeout_s=-1.0) == []


def test_quarantine_orphans_files_the_unreadable_records(
    queue: pool.PoolQueue,
) -> None:
    _publish(queue, KEY_GOOD)
    _foreign_writes_a_broken_record(queue)
    assert queue.quarantine_orphans() == [KEY_TRUNCATED, KEY_INVALID]
    # The healthy item is untouched and still claimable.
    assert queue.item_path(pool.READY, KEY_GOOD).exists()
    # The broken ones have left ``ready`` for good.
    for key in (KEY_TRUNCATED, KEY_INVALID):
        assert not queue.item_path(pool.READY, key).exists()


def test_an_unreadable_record_is_countable_where_an_operator_looks(
    queue: pool.PoolQueue,
) -> None:
    """``failed/`` is what ``pbstatus`` and ``pbwait`` read.  Not silence."""

    _foreign_writes_a_broken_record(queue)
    queue.quarantine_orphans()
    filed = json.loads(
        queue.item_path(pool.FAILED, KEY_INVALID).read_text(encoding="utf-8")
    )
    assert filed["schema"] == pool.POOL_OUTCOME_SCHEMA_V1
    assert filed["action_key"] == KEY_INVALID
    assert filed["status"] == "unreadable_record"
    assert "not valid JSON" in filed["detail"]["parse_error"]


def test_the_bytes_are_kept_so_the_writer_can_be_found(
    queue: pool.PoolQueue,
) -> None:
    """A record removed with nothing kept is a record nobody can diagnose."""

    _foreign_writes_a_broken_record(queue)
    queue.quarantine_orphans()
    kept = sorted(queue.superseded_dir().glob(f"{KEY_INVALID}.*.unreadable.json"))
    assert len(kept) == 1
    evidence = json.loads(kept[0].read_text(encoding="utf-8"))
    assert evidence["raw_head"] == "not json at all\n"
    assert evidence["state"] == pool.READY
    assert (queue.root / evidence["raw_path"]).read_bytes() == b"not json at all\n"


def test_an_empty_ready_record_is_filed_too(queue: pool.PoolQueue) -> None:
    """Zero bytes is the extreme of truncated, and a permanent ready resident."""

    queue.item_path(pool.READY, KEY_EMPTY).write_bytes(b"")
    assert queue.quarantine_orphans() == [KEY_EMPTY]
    assert not queue.item_path(pool.READY, KEY_EMPTY).exists()


def test_a_filed_outcome_is_never_replaced_by_the_quarantine(
    queue: pool.PoolQueue,
) -> None:
    """A corrupt ready file says nothing about an ending already filed."""

    outcome = {
        "schema": pool.POOL_OUTCOME_SCHEMA_V1,
        "action_key": KEY_INVALID,
        "status": "executed",
        "finished_unix": 1.0,
    }
    queue.item_path(pool.DONE, KEY_INVALID).write_text(
        json.dumps(outcome), encoding="utf-8"
    )
    _foreign_writes_a_broken_record(queue)
    queue.quarantine_orphans()
    assert json.loads(
        queue.item_path(pool.DONE, KEY_INVALID).read_text(encoding="utf-8")
    ) == outcome
    assert not queue.item_path(pool.FAILED, KEY_INVALID).exists()
    # Kept as evidence even when it may not be filed as an ending.
    assert list(queue.superseded_dir().glob(f"{KEY_INVALID}.*.unreadable.json"))


def test_invalid_utf8_does_not_stop_consumers(queue: pool.PoolQueue) -> None:
    _publish(queue, KEY_GOOD)
    queue.item_path(pool.READY, KEY_INVALID).write_bytes(b'\xff\xfe\xfa')
    assert [item["action_key"] for item in queue.ready_items()] == [KEY_GOOD]
    assert queue.quarantine_orphans() == [KEY_INVALID]
    assert queue.claim()["action_key"] == KEY_GOOD


def test_repair_between_scan_and_quarantine_survives(
    queue: pool.PoolQueue, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _foreign_writes_a_broken_record(queue)
    real_file = queue._file_unreadable

    def repair_then_file(path: Path, *, reason: str) -> str | None:
        _publish(queue, path.stem)
        return real_file(path, reason=reason)

    monkeypatch.setattr(queue, "_file_unreadable", repair_then_file)
    assert queue.quarantine_orphans() == []
    assert {item["action_key"] for item in queue.ready_items()} == {
        KEY_INVALID, KEY_TRUNCATED,
    }
    assert not list(queue.dir(pool.FAILED).glob("*.json"))


def test_publication_during_quarantine_is_not_unlinked(
    queue: pool.PoolQueue, monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue.item_path(pool.READY, KEY_INVALID).write_bytes(b"not json")
    real_file = queue._file_superseded

    def publish_then_file(*args: object, **kwargs: object) -> object:
        _publish(queue, KEY_INVALID)
        return real_file(*args, **kwargs)

    monkeypatch.setattr(queue, "_file_superseded", publish_then_file)
    assert queue.quarantine_orphans() == [KEY_INVALID]
    assert queue.item_path(pool.READY, KEY_INVALID).exists()
    assert queue.claim()["action_key"] == KEY_INVALID


def test_concurrent_failed_outcome_wins_over_quarantine(
    queue: pool.PoolQueue, monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue.item_path(pool.READY, KEY_INVALID).write_bytes(b"not json")
    destination = queue.item_path(pool.FAILED, KEY_INVALID)
    outcome = {"action_key": KEY_INVALID, "status": "worker_failed",
               "published_unix": 1.0}
    real_write = pool._write_json_atomic
    real_publish = pool.pb._atomic_publish

    def race_write(path: Path, payload: object) -> object:
        if path == destination:
            real_write(destination, outcome)
        return real_write(path, payload)

    def race_publish(path: Path, payload: bytes) -> bool:
        if path == destination:
            real_write(destination, outcome)
        return real_publish(path, payload)

    monkeypatch.setattr(pool, "_write_json_atomic", race_write)
    monkeypatch.setattr(pool.pb, "_atomic_publish", race_publish)
    assert queue.quarantine_orphans() == [KEY_INVALID]
    assert json.loads(destination.read_text()) == outcome
