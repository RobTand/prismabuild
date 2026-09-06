"""What a claim record is, and what it must not pick up on the way.

``claim`` turns a ready item into a claimed one by copying it and stamping the
claimant on the copy.  Two things leaked through that copy.  The aging counter
is a sidecar the claim itself deletes, so freezing it into the record makes
every attempt, done and failed record carry a number that no longer describes
anything.
"""

from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import pool  # noqa: E402

KEY_A = "a" * 64


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


def test_the_claim_does_not_carry_the_aging_counter(
    queue: pool.PoolQueue,
) -> None:
    """``passes`` belongs to the sidecar the same claim deletes."""

    _publish(queue, KEY_A)
    queue.record_pass(KEY_A)
    queue.record_pass(KEY_A)
    assert queue.passes(KEY_A) == 2
    # The scan stamps it on the item it returns, because the ready ordering
    # reads it there.  That is the whole of its life.
    assert queue.ready_items()[0]["passes"] == 2

    claimed = queue.claim()
    assert claimed is not None
    assert "passes" not in claimed
    on_disk = json.loads(
        queue.item_path(pool.CLAIMED, KEY_A).read_text(encoding="utf-8")
    )
    assert "passes" not in on_disk
    # And the counter it would have described is gone, which is the point.
    assert not queue.passes_path(KEY_A).exists()


def test_the_outcome_does_not_carry_the_aging_counter(
    queue: pool.PoolQueue,
) -> None:
    """The claim is what every terminal record is built from."""

    _publish(queue, KEY_A)
    queue.record_pass(KEY_A)
    claimed = queue.claim()
    assert claimed is not None
    filed = queue.finish(KEY_A, status="executed", claim_snapshot=claimed)
    assert "passes" not in json.loads(filed.read_text(encoding="utf-8"))
