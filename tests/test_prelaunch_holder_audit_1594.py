"""The orphan sweep keeps prelaunch group holders quietly (#1594).

A prelaunch holder (`prelaunch-<unit16>-<tier12>-<phase12>`) reserves room
for a declared phase. It is not an action key, so it files no move receipt
and no fragment names it. Before the fix the sweep misread it every pass:
`stage-holder-unresolved` without pressure, and
`stage-receiptless-holder-retained` under pressure. The prelaunch pass owns
its release, so the sweep must keep it quietly and never evict it.

These drive the real `stage_release.sweep` over a real queue with a minted
stage tier: a group holder kept quietly with and without pressure, and a
real orphan mover still evicted exactly as before.
"""
from __future__ import annotations

import time
import uuid
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))

from prismabuild import pool  # noqa: E402
from prismabuild import prelaunch_group  # noqa: E402
import stage_release  # noqa: E402

TIER = "prismabuild-stage:testbox"
KIND = "stage_gib"
CAPACITY_GIB = 10
HOLDER_GIB = 3


def _key() -> str:
    return uuid.uuid4().hex + uuid.uuid4().hex


def _fleet(tmp_path: Path) -> tuple[pool.PoolQueue, Path]:
    """A real queue with a registered stage root and minted tier room."""
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    stage = tmp_path / "stage"
    stage.mkdir()
    assert stage_release.register_stage_root(
        queue, tier_id=TIER, stage_root=stage) == "registered"
    queue.mint_tier_capacity(TIER, {KIND: CAPACITY_GIB})
    stage_release.reset_holder_reports()
    return queue, stage


def _holder() -> str:
    """One deterministic group holder for a live-looking unit."""
    return prelaunch_group.holder_name("c" * 64, TIER, ["phase-0"])


def _acquire(queue: pool.PoolQueue, holder: str, gib: int) -> None:
    assert queue.tier_ledger(TIER).acquire(holder, {KIND: gib})


def _names(receipts: list[dict[str, object]]) -> list[str]:
    return [str(entry.get("action_key")) for entry in receipts]


def _fail_consumer(queue: pool.PoolQueue) -> str:
    """An ended consumer, so the orphan mover below is nobody's."""
    key = _key()
    queue.publish(
        action_key=key, cas_root="/cas", checkout_root="/co",
        worker_script="/w.py", resources={"cpu": 1}, max_attempts=1)
    claimed = queue.claim(capacity={"cpu": 4})
    assert claimed is not None and claimed["action_key"] == key
    queue.finish(key, status="failed", detail={"returncode": 1})
    return key


def test_the_predicate_names_only_group_holders() -> None:
    """`_is_prelaunch_holder` reads the shared prefix, nothing else."""
    assert prelaunch_group.HOLDER_PREFIX == "prelaunch-"
    assert stage_release._is_prelaunch_holder(_holder())
    assert not stage_release._is_prelaunch_holder("a" * 64)
    assert not stage_release._is_prelaunch_holder("advance-" + "b" * 16 + "-x")


def test_sweep_keeps_a_prelaunch_holder_quiet_without_pressure(
    tmp_path: Path,
) -> None:
    """No pressure: the holder stays held and files no report."""
    queue, stage = _fleet(tmp_path)
    holder = _holder()
    _acquire(queue, holder, HOLDER_GIB)
    receipts = stage_release.sweep(queue, stage_roots={TIER: str(stage)})
    assert receipts == [], receipts
    assert queue.tier_ledger(TIER).holder_tokens(holder) == {KIND: HOLDER_GIB}


def test_sweep_keeps_a_prelaunch_holder_quiet_under_pressure(
    tmp_path: Path,
) -> None:
    """Under pressure: no receipt-less event and no eviction call."""
    queue, stage = _fleet(tmp_path)
    holder = _holder()
    _acquire(queue, holder, HOLDER_GIB)
    receipts = stage_release.sweep(
        queue, stage_roots={TIER: str(stage)},
        pressure={TIER: CAPACITY_GIB - 1})
    assert holder not in _names(receipts), receipts
    assert not [entry for entry in receipts
                if entry.get("event")
                in ("stage-receiptless-holder-retained",
                    "stage-holder-unresolved")], receipts
    assert queue.tier_ledger(TIER).holder_tokens(holder) == {KIND: HOLDER_GIB}


def test_sweep_still_evicts_a_real_orphan_mover(tmp_path: Path) -> None:
    """A receipt-backed orphan still leaves; the holder still stays."""
    queue, stage = _fleet(tmp_path)
    holder = _holder()
    _acquire(queue, holder, HOLDER_GIB)
    mover = _key()
    _acquire(queue, mover, 2)
    consumer = _fail_consumer(queue)
    queue.record_move(mover, {
        "consumer_action_key": consumer, "unix": time.time()})
    receipts = stage_release.sweep(
        queue, stage_roots={TIER: str(stage)},
        pressure={TIER: CAPACITY_GIB - 1})
    orphan = [entry for entry in receipts
              if entry.get("action_key") == mover]
    assert orphan, f"the orphan mover was not evicted: {receipts}"
    assert all(entry.get("reason") == "orphan-sweep" for entry in orphan)
    assert mover not in queue.tier_ledger(TIER).held_keys()
    assert holder not in _names(receipts), receipts
    assert queue.tier_ledger(TIER).holder_tokens(holder) == {KIND: HOLDER_GIB}
