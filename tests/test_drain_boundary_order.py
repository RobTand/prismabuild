"""A claim pass must not take the boundary a busy row waited for (#1217).

At a GPU quantum drain boundary on 2026-09-27 a priority -9 row whose
transition lock another loop held was skipped without a live withhold carry
(its drains_soon episode had outlived ``WITHHOLD_CEILING_S``), and the same
pass claimed the older priority -10 row behind it.  The ordering contract
(#362 bands, #924 verdicts) says a row the pass cannot evaluate still keeps
its place: the pass holds the box for it instead of claiming past it.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "tools/fleet")]
from prismabuild import adaptive_cpu, pool  # noqa: E402

KEY_H, KEY_A, KEY_B = "0" * 64, "a" * 64, "b" * 64
CAPACITY = {"cpu": 20, "gpu": 1, "mem_gb": 104}


def publish(queue, key, **kwargs):
    queue.publish(action_key=key, cas_root="/cas", checkout_root="/co", worker_script="/w.py", **kwargs)


def local_records(queue):
    path = adaptive_cpu.local_state_base(queue.ledger().base) / pool.CLAIM_DENIALS
    return adaptive_cpu.read_json(path).get("records", {})


def _record_for(queue, key):
    return next(value for value in local_records(queue).values() if value["action_key"] == key)


class _unheld:
    def __enter__(self):
        return False

    def __exit__(self, *args):
        return False


def _age_the_withhold_episode(queue, key, seconds):
    """Move the row's filed withholding denial back in time, past the ceiling.

    The carry reads the episode start from the record (``denied_unix`` minus
    its recorded age), so rewinding ``denied_unix`` expires the episode exactly
    as the real incident's 3326-second drain did, without touching the row.
    """
    path = adaptive_cpu.local_state_base(queue.ledger().base) / pool.CLAIM_DENIALS
    document = json.loads(path.read_text())
    slot = next(name for name, value in document["records"].items()
                if value["action_key"] == key)
    document["records"][slot]["denied_unix"] -= seconds
    path.write_text(json.dumps(document))


def test_a_busy_row_keeps_the_drain_boundary_when_its_episode_expired(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / "queue")
    ledger = queue.ledger()
    ledger.ensure_capacity(CAPACITY)
    assert ledger.acquire(KEY_H, {"cpu": 6, "gpu": 1, "mem_gb": 69})
    publish(queue, KEY_A, priority=-9, resources={"cpu": 6, "gpu": 1, "mem_gb": 64})
    publish(queue, KEY_B, priority=-10, resources={"cpu": 6, "gpu": 1, "mem_gb": 69})
    for _ in range(pool.STARVATION_FLOOR - 1):
        queue.record_pass(KEY_A)

    # While the quantum holder holds its tokens, the head row withholds.
    assert queue.claim(capacity=CAPACITY) is None
    assert _record_for(queue, KEY_A)["reason"] == "reservation_unavailable_withholding"

    # The drain outlives the carry ceiling, then the holder finishes.
    _age_the_withhold_episode(queue, KEY_A, pool.WITHHOLD_CEILING_S + 1.0)
    ledger.release(KEY_H)

    original = queue._transition_locked

    def contested(key, **kwargs):
        return original(key, blocking=False) if key != KEY_A else _unheld()

    monkeypatch.setattr(queue, "_transition_locked", contested)
    # Another loop is deciding the head row at the boundary: this pass must
    # not claim the lower-priority row behind it while that decision pends.
    assert queue.claim(capacity=CAPACITY) is None
    busy = _record_for(queue, KEY_A)
    assert busy["reason"] == "transition_busy"
    assert busy["evidence"]["gpu_room_kept"] == {"cpu": 6, "gpu": 1, "mem_gb": 64}
    assert busy["evidence"]["gpu_room_binds_all"] is True
    deferred = _record_for(queue, KEY_B)
    assert deferred["reason"] == "deferred_for_ready_gpu_row"
    assert deferred["evidence"]["gpu_row"] == KEY_A[:12]
    monkeypatch.setattr(queue, "_transition_locked", original)
    # Once the sibling is done, the head row takes its boundary.
    assert queue.claim(capacity=CAPACITY)["action_key"] == KEY_A


def test_a_busy_row_with_no_history_still_keeps_its_place(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / "queue")
    ledger = queue.ledger()
    ledger.ensure_capacity(CAPACITY)
    publish(queue, KEY_A, priority=-9, resources={"cpu": 6, "gpu": 1, "mem_gb": 64})
    publish(queue, KEY_B, priority=-10, resources={"cpu": 6, "gpu": 1, "mem_gb": 69})

    original = queue._transition_locked

    def contested(key, **kwargs):
        return original(key, blocking=False) if key != KEY_A else _unheld()

    monkeypatch.setattr(queue, "_transition_locked", contested)
    assert queue.claim(capacity=CAPACITY) is None
    assert _record_for(queue, KEY_B)["reason"] == "deferred_for_ready_gpu_row"
    monkeypatch.setattr(queue, "_transition_locked", original)
    assert queue.claim(capacity=CAPACITY)["action_key"] == KEY_A
