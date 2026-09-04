"""The rename-to-rewrite window inside ``claim()`` is not a dead claimant (#36)."""

from __future__ import annotations

import json
import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import pool  # noqa: E402

KEY = "c" * 64


def _publish(queue: pool.PoolQueue, tmp_path: Path) -> None:
    queue.publish(
        action_key=KEY,
        cas_root=tmp_path / "cas",
        checkout_root=tmp_path / "co",
        worker_script=tmp_path / "w.py",
    )


def _enter_claim_window(queue: pool.PoolQueue) -> Path:
    """Reproduce ``claim()`` up to its rename: intent written, record not yet."""

    queue._write_claim_intent(KEY, owner="worker-1")
    claimed = queue.item_path(pool.CLAIMED, KEY)
    os.rename(queue.item_path(pool.READY, KEY), claimed)
    record = json.loads(claimed.read_text())
    assert "claimed_unix" not in record
    assert queue.lease_age(KEY) is None
    return claimed


def test_a_claim_between_rename_and_rewrite_is_not_reaped(tmp_path: Path) -> None:
    """main: reap leaves the fresh claim alone.  Branch (pre-fix): it requeues
    it, a second worker runs the same action while the first is live, and the
    retry's refusal is what the client reads (the live #36 incident)."""

    queue = pool.PoolQueue(tmp_path / "q")
    queue.ensure_layout()
    _publish(queue, tmp_path)
    claimed = _enter_claim_window(queue)

    assert queue.reap_stale() == []

    assert claimed.exists()
    assert not queue.item_path(pool.READY, KEY).exists()
    assert queue.claim(tags=(), has_gpu=False, owner="worker-2") is None


def test_a_claimant_dead_inside_the_window_is_still_reaped(tmp_path: Path) -> None:
    queue = pool.PoolQueue(tmp_path / "q")
    queue.ensure_layout()
    _publish(queue, tmp_path)
    _enter_claim_window(queue)
    intent = queue.item_path(pool.INTENT, KEY)
    record = json.loads(intent.read_text())
    record["intent_unix"] = record["intent_unix"] - (pool.HEARTBEAT_S + 60.0)
    intent.write_text(json.dumps(record))

    assert queue.reap_stale() == [KEY]

    assert queue.item_path(pool.READY, KEY).exists()
    assert queue.claim(tags=(), has_gpu=False, owner="worker-2") is not None


def test_a_claimed_record_with_no_clock_at_all_is_still_reaped(tmp_path: Path) -> None:
    """No lease, no ``claimed_unix``, no intent: nothing vouches for it."""

    queue = pool.PoolQueue(tmp_path / "q")
    queue.ensure_layout()
    _publish(queue, tmp_path)
    _enter_claim_window(queue)
    queue.item_path(pool.INTENT, KEY).unlink()

    assert queue.reap_stale() == [KEY]
