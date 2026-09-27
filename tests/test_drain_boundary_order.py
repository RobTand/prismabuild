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
        return _unheld() if key == KEY_A else original(key, **kwargs)

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


def test_the_room_survives_a_holder_that_releases_mid_pass(tmp_path, monkeypatch):
    """#1230 review: the pass started while the holder still held the GPU.

    ``gpu_first`` is read once, at the pass's start, and is empty while the
    quantum holder holds the GPU token.  The holder then releases MID-PASS --
    inside the head row's contested lock call, exactly the incident's 300 ms
    boundary -- and the head row's room must still be computed and bind every
    row: without it the pass reads the fresh tokens and claims the priority
    -10 row past the busy -9 row, the #1217 inversion.
    """
    queue = pool.PoolQueue(tmp_path / "queue")
    ledger = queue.ledger()
    ledger.ensure_capacity(CAPACITY)
    assert ledger.acquire(KEY_H, {"cpu": 6, "gpu": 1, "mem_gb": 69})
    publish(queue, KEY_A, priority=-9, resources={"cpu": 6, "gpu": 1, "mem_gb": 64})
    publish(queue, KEY_B, priority=-10, resources={"cpu": 6, "gpu": 1, "mem_gb": 69})
    for _ in range(pool.STARVATION_FLOOR - 1):
        queue.record_pass(KEY_A)

    original = queue._transition_locked

    def contested(key, **kwargs):
        if key == KEY_A:
            # The quantum holder finishes while the sibling decides the row.
            ledger.release(KEY_H)
            return _unheld()
        return original(key, **kwargs)

    monkeypatch.setattr(queue, "_transition_locked", contested)
    claimed = queue.claim(capacity=CAPACITY)
    assert claimed is None, (
        f"claimed {claimed and claimed['action_key'][:4]} past the busy head row "
        "whose holder released mid-pass")
    busy = _record_for(queue, KEY_A)
    assert busy["reason"] == "transition_busy"
    assert busy["evidence"]["gpu_room_kept"] == {"cpu": 6, "gpu": 1, "mem_gb": 64}
    assert busy["evidence"]["gpu_room_binds_all"] is True
    deferred = _record_for(queue, KEY_B)
    assert deferred["reason"] == "deferred_for_ready_gpu_row"
    assert deferred["evidence"]["gpu_row"] == KEY_A[:12]
    monkeypatch.setattr(queue, "_transition_locked", original)
    assert queue.claim(capacity=CAPACITY)["action_key"] == KEY_A


def test_a_malformed_busy_row_keeps_no_room_and_ends_no_pass(tmp_path, monkeypatch):
    """A busy row outside ``gpu_first`` has its demand read on the busy path.

    The loop holding it will refuse it as malformed; this pass must neither
    keep a room for it nor end on its contract error -- every row behind it
    is still decided.
    """
    queue = pool.PoolQueue(tmp_path / "queue")
    ledger = queue.ledger()
    ledger.ensure_capacity(CAPACITY)
    assert ledger.acquire(KEY_H, {"cpu": 6, "gpu": 1, "mem_gb": 69})
    publish(queue, KEY_A, priority=-9, resources={"cpu": 6, "gpu": 1, "mem_gb": 64})
    publish(queue, KEY_B, priority=-10, resources={"cpu": 6, "gpu": 1, "mem_gb": 69})
    demand_of = queue.demand_of

    def malformed(item):
        if item.get("action_key") == KEY_A:
            raise pool.PoolContractError("pool item resources must be an object")
        return demand_of(item)

    original = queue._transition_locked

    def contested(key, **kwargs):
        if key == KEY_A:
            ledger.release(KEY_H)
            return _unheld()
        return original(key, **kwargs)

    monkeypatch.setattr(queue, "demand_of", malformed)
    monkeypatch.setattr(queue, "_transition_locked", contested)
    claimed = queue.claim(capacity=CAPACITY)
    assert claimed is not None and claimed["action_key"] == KEY_B
    assert "gpu_room_kept" not in _record_for(queue, KEY_A)["evidence"]


def test_a_cpu_row_fills_beside_a_binds_all_room(tmp_path, monkeypatch):
    """#1230 review: a CPU row that fits beside the kept room still runs.

    The binds-all room binds every row, but binding is not blocking: a CPU
    row whose demand fits the free tokens beside the room is admitted, and
    only a row the room leaves no room for is deferred (#1169 beside #1217).
    """
    queue = pool.PoolQueue(tmp_path / "queue")
    ledger = queue.ledger()
    ledger.ensure_capacity(CAPACITY)
    publish(queue, KEY_A, priority=-9, resources={"cpu": 6, "gpu": 1, "mem_gb": 64})
    publish(queue, "c" * 64, priority=-10, resources={"cpu": 2, "mem_gb": 8})
    publish(queue, "d" * 64, priority=-11, resources={"cpu": 2, "mem_gb": 80})

    original = queue._transition_locked

    def contested(key, **kwargs):
        return _unheld() if key == KEY_A else original(key, **kwargs)

    monkeypatch.setattr(queue, "_transition_locked", contested)
    # The CPU row that fits beside the room is admitted.
    assert queue.claim(capacity=CAPACITY)["action_key"] == "c" * 64
    # The row the room leaves no memory for is deferred, not merely short.
    assert queue.claim(capacity=CAPACITY) is None
    deferred = _record_for(queue, "d" * 64)
    assert deferred["reason"] == "deferred_for_ready_gpu_row", deferred
    assert deferred["evidence"]["gpu_row"] == KEY_A[:12]


class _SampledGPU:
    """A gpu controller stand-in whose cached sample _gpu_sample_for reads."""

    def __init__(self, sample):
        self._sample = sample


def test_the_kept_room_reads_the_gpu_sample_branch(tmp_path):
    """#1230 review: on a box that samples its GPU the room reads the sample.

    ``_ready_gpu_row_room`` keeps the room only on a clean, fresh GPU sample
    with no foreign process on the device; a stale sample, or one naming a
    foreign process, answers None.  The drain-boundary tests run without a
    controller, so this is the branch they cannot reach.
    """
    import time

    queue = pool.PoolQueue(tmp_path / "queue")
    ledger = queue.ledger()
    ledger.ensure_capacity(CAPACITY)
    publish(queue, KEY_A, priority=-9, resources={"cpu": 6, "gpu": 1, "mem_gb": 64})
    item = json.loads(queue.item_path(pool.READY, KEY_A).read_text())

    def room_for(controller):
        return queue._ready_gpu_row_room(
            item, ledger=ledger, total=CAPACITY, controller=None,
            gpu_controller=controller, observed_images=None)

    clean = _SampledGPU({"foreign_processes": [],
                         "sampled_unix": time.time()})
    assert room_for(clean) == {
        "action_key": KEY_A, "room": {"cpu": 6, "gpu": 1, "mem_gb": 64}}
    stale = _SampledGPU({"foreign_processes": [],
                         "sampled_unix": time.time() - 3600.0})
    assert room_for(stale) is None
    foreign = _SampledGPU({"foreign_processes": ["vllm serve"],
                           "sampled_unix": time.time()})
    assert room_for(foreign) is None


def test_a_busy_row_with_no_history_still_keeps_its_place(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / "queue")
    ledger = queue.ledger()
    ledger.ensure_capacity(CAPACITY)
    publish(queue, KEY_A, priority=-9, resources={"cpu": 6, "gpu": 1, "mem_gb": 64})
    publish(queue, KEY_B, priority=-10, resources={"cpu": 6, "gpu": 1, "mem_gb": 69})

    original = queue._transition_locked

    def contested(key, **kwargs):
        return _unheld() if key == KEY_A else original(key, **kwargs)

    monkeypatch.setattr(queue, "_transition_locked", contested)
    assert queue.claim(capacity=CAPACITY) is None
    assert _record_for(queue, KEY_B)["reason"] == "deferred_for_ready_gpu_row"
    monkeypatch.setattr(queue, "_transition_locked", original)
    assert queue.claim(capacity=CAPACITY)["action_key"] == KEY_A
