"""What was made resident has to reach the record that outlives the sidecar.

The prewarm sidecar is queue bookkeeping and a later campaign may prune it.
The question a reader asks months later -- "was this row's data resident when
it ran?" -- has to be answerable from the done row alone, or the measurement
that justified prewarming in the first place cannot be repeated against the
fleet's own history.

The copy happens at claim, where the claimed record is being written anyway,
so it costs no extra write and cannot race: past the rename the prewarm loop
has already stopped looking at this key, because it only ever reads ``ready``.
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


def test_the_receipt_reaches_the_claim_and_then_the_done_row(
        queue: pool.PoolQueue) -> None:
    _publish(queue, KEY)
    _warm(queue, KEY)

    claimed = queue.claim()
    assert claimed is not None
    assert claimed["prewarm"]["bytes_warmed"] == 63786036448
    assert claimed["prewarm"]["schema"] == pool.POOL_PREWARM_SCHEMA_V1

    queue.finish(KEY, status="succeeded", detail={"status": "succeeded"},
                 claim_snapshot=claimed)
    done = pool._read_json(queue.item_path(pool.DONE, KEY))
    assert done["detail"]["prewarm"]["bytes_warmed"] == 63786036448
    assert done["detail"]["prewarm"]["status"] == "complete"
    assert done["detail"]["status"] == "succeeded"


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

    queue.finish(OTHER, status="succeeded", detail={"status": "succeeded"},
                 claim_snapshot=claimed)
    done = pool._read_json(queue.item_path(pool.DONE, OTHER))
    assert "prewarm" not in done["detail"]


def test_a_workers_own_measurement_outranks_the_loops_prediction(
        queue: pool.PoolQueue) -> None:
    _publish(queue, KEY)
    _warm(queue, KEY)
    claimed = queue.claim()
    queue.finish(KEY, status="succeeded",
                 detail={"status": "succeeded",
                         "prewarm": {"measured_by": "the worker"}},
                 claim_snapshot=claimed)
    done = pool._read_json(queue.item_path(pool.DONE, KEY))
    assert done["detail"]["prewarm"] == {"measured_by": "the worker"}
