"""R3 repro: positive publication and exact-obligation defects (must fail pre-fix)."""
from __future__ import annotations
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import pool, residency_plan, storage_tiers  # noqa: E402
import tier_loop  # noqa: E402

TIER = "prismabuild-stage:dl380g10"
STAGE_KIND = f"stage_gib@{TIER}"
GIB = storage_tiers.GIB
SPAN = 1 << 20

def _hexkey(seed: str) -> str:
    return (seed.encode().hex() * 64)[:64]

def _queue(tmp_path: Path, gib: int) -> pool.PoolQueue:
    root = tmp_path / "pb-queue"
    q = pool.PoolQueue(root)
    (root / "cas").mkdir(parents=True, exist_ok=True)
    (root / "co").mkdir(parents=True, exist_ok=True)
    (root / "worker.py").write_text("x")
    q.mint_tier_capacity(TIER, {f"stage_gib": gib})
    return q

def test_repro_unknown_ready_preserved(tmp_path: Path) -> None:
    q = _queue(tmp_path, 3)
    tiers = {TIER: {"tier_id": TIER}}
    ready_dir = q.dir(pool.READY)
    ready_dir.mkdir(parents=True, exist_ok=True)
    (q.dir(pool.CLAIMED)).mkdir(parents=True, exist_ok=True)
    ready_dir.chmod(0o000)
    try:
        out = tier_loop._protect_tier_advances(
            q, tiers, mover_role="mover_row",
            tier_of=lambda plan: plan.get("tier_id"),
            state_of=tier_loop._mover_state)
    finally:
        ready_dir.chmod(0o755)
    assert out.get("unknown_ready") is True, f"got {out.get('unknown_ready')}"

def test_repro_terminal_torn_proof_preserved(tmp_path: Path) -> None:
    q = _queue(tmp_path, 3)
    ledger = q.tier_ledger(TIER)
    mover = _hexkey("r3-torn-mover")
    assert ledger.acquire(mover, {"stage_gib": 1}) is True
    names = sorted(p.name for p in (ledger.held_dir / mover).glob("*-*"))
    rec = {"schema": pool.TIER_FUNDING_SCHEMA_V1, "tier_id": TIER,
           "consumer_action_key": _hexkey("r3-consumer"),
           "plan_sha256": "8" * 64, "mover_action_key": mover,
           "range_start_bytes": 0, "range_end_bytes": SPAN,
           "kind": "stage_gib", "tokens": names,
           "generation": "b" * 32, "state": "reserved",
           "unix": 1.0, "published_unix": 1.0}
    q._write_funding_locked(rec, expect_generation=None)
    assert q.advance_funding_state(
        mover, TIER, expect="reserved", advance_to="transferring",
        generation="b" * 32) is True
    done = q.item_path(pool.DONE, mover)
    done.parent.mkdir(parents=True, exist_ok=True)
    done.write_text("{torn-json")
    tier_loop._settle_terminal_fence(
        q, ledger, tier_id=TIER, mover_role="mover_row",
        consumer=None, mover=mover)
    assert int(ledger.holder_tokens(mover).get("stage_gib", 0)) == 1
    assert q.read_funding(mover, TIER) is not None
    assert q.read_funding(mover, TIER)["state"] == "transferring"
    # Same through the real window caller (funding scan): defers loudly,
    # frees nothing, closes nothing.
    events = tier_loop.residency_window(q, tiers={TIER: {"tier_id": TIER}})
    assert int(ledger.holder_tokens(mover).get("stage_gib", 0)) == 1
    assert q.read_funding(mover, TIER)["state"] == "transferring"
    assert [e for e in events
            if e.get("event") == "advance-deferred-unknown-evidence"], \
        "torn terminal proof must defer loudly"
