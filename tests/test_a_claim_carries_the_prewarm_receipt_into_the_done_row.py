"""What was made resident has to reach the record that outlives the sidecar.

The prewarm sidecar is queue bookkeeping and a later campaign may prune it.
The question a reader asks months later -- "was this row's data resident when
it ran?" -- has to be answerable from the done row alone, or the measurement
that justified prewarming in the first place cannot be repeated against the
fleet's own history.

The claim carries a reference, never a copy (#596): the receipt keeps growing
after the claim as later windows extend it, and a copy frozen at claim time
describes a counter that no longer exists.  The copy into the terminal record
happens at finish, where the reference is resolved and its key and digest
verified -- so a receipt rewritten for a same-key successor is never mistaken
for this generation's window, and a reference that no longer resolves leaves
no dangling pointer in an immutable record.
"""
from __future__ import annotations

from pathlib import Path
import sys
import uuid

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import pool  # noqa: E402

KEY = uuid.uuid4().hex + uuid.uuid4().hex
OTHER = uuid.uuid4().hex + uuid.uuid4().hex


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    q = pool.PoolQueue(tmp_path / "pb-queue")
    q.ensure_layout()
    return q


def _publish(q: pool.PoolQueue, key: str) -> None:
    q.publish(action_key=key, cas_root="/cas", checkout_root="/co",
              worker_script="/w.py")


def _warm(q: pool.PoolQueue, key: str) -> None:
    q.record_prewarm(key, {
        "host": "dl380g10", "manifest_sha256": "a" * 64,
        "manifest_bytes": 63786036448, "bytes_warmed": 63786036448,
        "entry_count": 868, "entries_warmed": 868, "status": "complete",
        "seconds": 125.76, "mb_per_s": 391.9,
    })


def test_the_reference_reaches_the_claim_and_resolves_into_the_done_row(
        queue: pool.PoolQueue) -> None:
    _publish(queue, KEY)
    _warm(queue, KEY)

    claimed = queue.claim()
    assert claimed is not None
    assert claimed["prewarm"] == {
        pool.PREWARM_RECEIPT_REF: KEY, "manifest_sha256": "a" * 64}

    queue.finish(KEY, status="executed", detail={"status": "executed"},
                 claim_snapshot=claimed)
    done = pool._read_json(queue.item_path(pool.DONE, KEY))
    assert done["detail"]["prewarm"]["bytes_warmed"] == 63786036448
    assert done["detail"]["prewarm"]["status"] == "complete"
    assert done["detail"]["status"] == "executed"


def test_a_receipt_pruned_before_finish_leaves_no_dangling_pointer(
        queue: pool.PoolQueue) -> None:
    """The sidecar is a cache: pruning it before finish must not poison the row."""

    _publish(queue, KEY)
    _warm(queue, KEY)
    claimed = queue.claim()
    assert claimed is not None
    (queue.root / pool.PREWARM / f"{KEY}.json").unlink()

    queue.finish(KEY, status="executed", detail={"status": "executed"},
                 claim_snapshot=claimed)
    done = pool._read_json(queue.item_path(pool.DONE, KEY))
    assert "prewarm" not in done["detail"]


def test_a_same_key_successors_receipt_is_not_this_generations_window(
        queue: pool.PoolQueue) -> None:
    """A receipt rewritten under the same key must fail the digest check."""

    _publish(queue, KEY)
    _warm(queue, KEY)
    claimed = queue.claim()
    assert claimed is not None
    queue.record_prewarm(KEY, {
        "host": "dl380g10", "manifest_sha256": "b" * 64,
        "manifest_bytes": 1, "bytes_warmed": 1,
        "entry_count": 1, "entries_warmed": 1, "status": "complete",
        "seconds": 0.1, "mb_per_s": 10.0,
    })

    queue.finish(KEY, status="executed", detail={"status": "executed"},
                 claim_snapshot=claimed)
    done = pool._read_json(queue.item_path(pool.DONE, KEY))
    assert "prewarm" not in done["detail"]


def test_a_legacy_full_copy_claim_still_files_a_complete_row(
        queue: pool.PoolQueue) -> None:
    """Claims filed before the reference resolve by copying, as they always did."""

    _publish(queue, KEY)
    claimed = queue.claim()
    assert claimed is not None
    # A legacy claim carries the full receipt on the on-disk claimed record
    # itself -- filed before the reference -- so the legacy block goes there,
    # where ``finish`` reads it, not on the in-memory snapshot.
    path = queue.item_path(pool.CLAIMED, KEY)
    on_disk = pool._read_json(path)
    assert on_disk is not None
    on_disk["prewarm"] = {
        "host": "dl380g10", "manifest_sha256": "a" * 64,
        "bytes_warmed": 63786036448, "status": "complete",
    }
    pool._write_json_atomic(path, on_disk)

    queue.finish(KEY, status="executed", detail={"status": "executed"},
                 claim_snapshot=claimed)
    done = pool._read_json(queue.item_path(pool.DONE, KEY))
    assert done["detail"]["prewarm"]["bytes_warmed"] == 63786036448


def test_an_action_nobody_warmed_carries_no_prewarm_key(
        queue: pool.PoolQueue) -> None:
    """Absent is the normal case and must not become an empty stub.

    Most of the fleet has no storage host and no manifests.  A ``prewarm: {}``
    on every done row would read as "warmed nothing" rather than "nobody
    looked", and those are different facts.
    """

    _publish(queue, OTHER)
    claimed = queue.claim()
    assert claimed is not None
    assert "prewarm" not in claimed

    queue.finish(OTHER, status="executed", detail={"status": "executed"},
                 claim_snapshot=claimed)
    done = pool._read_json(queue.item_path(pool.DONE, OTHER))
    assert "prewarm" not in done["detail"]


def test_a_workers_own_measurement_outranks_the_loops_prediction(
        queue: pool.PoolQueue) -> None:
    _publish(queue, KEY)
    _warm(queue, KEY)
    claimed = queue.claim()
    queue.finish(KEY, status="executed",
                 detail={"status": "executed",
                         "prewarm": {"measured_by": "the worker"}},
                 claim_snapshot=claimed)
    done = pool._read_json(queue.item_path(pool.DONE, KEY))
    assert done["detail"]["prewarm"] == {"measured_by": "the worker"}
