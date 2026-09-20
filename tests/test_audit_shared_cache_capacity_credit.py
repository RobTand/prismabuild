"""Audit R2: shared staged bytes vs writable-capacity credits, actual lifecycle.

Drives the REAL production path at tiny scale, no hand-written fragments,
no hand-filed move receipts, no direct ledger holds for movers A/B, no
hand-minted supply totals:

  publish mover row (residency range + sealed demand) -> queue.claim (real
  admission: tier tokens move under the mover key) -> stage_move.move (real
  copy, real fragment, real receipt) -> record_move -> finish(executed)
  (residency_pin_holds keeps the tokens) -> ... -> stage_release.evict.

A and B stage the SAME extent (forward/reverse shape): one staged file,
two fragments, two complete receipts, two separate 1-token holdings.
Newcomer C is admitted or refused through the REAL queue.claim. The tier
supply is computed by the REAL mint caller (tier_loop helper over
landed_and_in_flight + mint_tier_capacity). The ONE modelled number,
labelled everywhere, is storage discovery's `available` (W_MODEL): the
fixture tmpfs has ample space, so writable is stood in by a fixed count.
Scale: 1 token == 1 GiB in production; here demands are 1 token per MiB
range via the same ceil function, so the ledger arithmetic is identical
and only the byte scale is small. No fake OOM, no multi-GiB I/O, no
benchmark claim.

Runs under pbtest at priority -10; never executed locally.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import pool, residency_map, storage_tiers  # noqa: E402
import prismabuild.core as pb  # noqa: E402
import stage_move  # noqa: E402
import stage_release  # noqa: E402
import tier_loop  # noqa: E402

TIER = "prismabuild-stage:dl380g10"
KIND = "stage_gib"
CONSUMER_A = "a" * 64
CONSUMER_B = "b" * 64
MOVER_A = "1" * 64
MOVER_B = "2" * 64
MOVER_C = "c" * 64
MIB = 1 << 20
SHARED_BYTES = MIB
W_MODEL_TIGHT = 0  # MODELLED discovery `available`: stage physically tight
HOST_CAP = {"cpu": 8, "mem_gb": 16}


def _manifest_bytes(pool_dir: Path) -> tuple[dict, Path]:
    pool_dir.mkdir(parents=True, exist_ok=True)
    shared = hashlib.sha256(b"audit-shared-extent").digest() * (MIB // 32)
    other = hashlib.sha256(b"audit-other-extent").digest() * (MIB // 32)
    (pool_dir / "shared.bin").write_bytes(shared)
    (pool_dir / "other.bin").write_bytes(other)
    entries = [
        {"path": str(pool_dir / "shared.bin"), "offset": 0, "bytes": MIB,
         "sha256": hashlib.sha256(shared).hexdigest()},
        {"path": str(pool_dir / "other.bin"), "offset": 0, "bytes": MIB,
         "sha256": hashlib.sha256(other).hexdigest()},
    ]
    manifest = {
        "schema": pb.DATA_MANIFEST_SCHEMA_V1,
        "produced_by": {"tool": "audit-r2"},
        "mount_prefix": str(pool_dir),
        "entries": entries, "entry_count": 2, "total_bytes": 2 * MIB,
        "annotations": {"phases": [
            {"name": "shared", "cumulative_bytes": MIB},
            {"name": "other", "cumulative_bytes": 2 * MIB}]},
    }
    manifest_path = pool_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest))
    return manifest, manifest_path


def _fleet(tmp_path: Path) -> pool.PoolQueue:
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    queue.mint_tier_capacity(TIER, {KIND: 8})  # standing supply admits A, B
    (tmp_path / "stage").mkdir(parents=True, exist_ok=True)
    assert stage_release.register_stage_root(
        queue, tier_id=TIER, stage_root=tmp_path / "stage") == "registered"
    return queue


def _publish_mover(queue: pool.PoolQueue, key: str, start: int, end: int,
                   manifest_sha: str, total: int, demand: int | None = None) -> None:
    # Demand may exceed the range floor (chunked ranges ceil separately);
    # the gate refuses only below-floor declarations.
    tokens = demand if demand is not None else storage_tiers.stage_tokens_for_bytes(end - start)
    queue.publish(
        action_key=key, cas_root=str(queue.root / "cas"),
        checkout_root=str(queue.root / "co"),
        worker_script=str(queue.root / "worker.py"),
        resources={"cpu": 1, "mem_gb": 1, f"{KIND}@{TIER}": tokens},
        residency={"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                   "manifest_sha256": manifest_sha, "manifest_bytes": total,
                   "range_start_bytes": start, "range_end_bytes": end},
        max_attempts=1, retry_safe=False)


def _claim(queue: pool.PoolQueue):
    return queue.claim(owner="worker:1:abcd0001", capacity=dict(HOST_CAP),
                       tags=["dl380g10"])


def _move_args(tmp_path: Path, queue: pool.PoolQueue, manifest_path: Path,
               mover: str, consumer: str, start: int, end: int):
    return stage_move.build_parser().parse_args([
        "--pool-root", str(queue.root),
        "--cas-root", str(queue.root / "cas"),
        "--action-key", mover,
        "--consumer-action-key", consumer,
        "--tier-id", TIER,
        "--stage-root", str(tmp_path / "stage"),
        "--manifest-sha256", "0" * 64,
        "--range-start-bytes", str(start),
        "--range-end-bytes", str(end),
        "--manifest", str(manifest_path),
        "--residency-root", str(queue.root / pool.RESIDENCY),
        "--block", str(1 << 16),
        "--readers", "2", "--max-readers", "2", "--unpaced",
    ])


def _run_mover(tmp_path: Path, queue: pool.PoolQueue, manifest_path: Path,
               mover: str, consumer: str, start: int, end: int):
    """The actual mover lifecycle: claim -> copy -> file receipt -> finish."""
    claimed = _claim(queue)
    assert claimed is not None and claimed["action_key"] == mover
    receipt = stage_move.move(_move_args(
        tmp_path, queue, manifest_path, mover, consumer, start, end))
    assert receipt["complete"] is True
    queue.record_move(mover, receipt)
    queue.finish(mover, status="executed")
    return receipt


def _supply(queue: pool.PoolQueue, writable: int) -> dict[str, int]:
    """The REAL mint caller computes supply: writable + landed -> ledger.

    Post-fix this is tier_loop.mint_stage_supply (snapshot and mint under
    one lock). Pre-fix fallback is the old caller sequence (real
    landed_and_in_flight snapshot + mint_tier_capacity): identical
    arithmetic in a single-threaded fixture, so the baseline RED isolates
    the old egress behavior, not the mint.
    """
    helper = getattr(tier_loop, "mint_stage_supply", None)
    if helper is not None:
        out = helper(queue, tier_id=TIER, kind=KIND, writable_tokens=writable)
        return {"landed": int(out["landed"]), "in_flight": int(out["in_flight"]),
                "supply": int(out["supply"])}
    landed, in_flight = tier_loop.landed_and_in_flight(queue, TIER, KIND)
    queue.mint_tier_capacity(TIER, {KIND: writable + landed})
    return {"landed": landed, "in_flight": in_flight,
            "supply": writable + landed}


def _ledger_numbers(queue: pool.PoolQueue) -> dict[str, int]:
    ledger = queue.tier_ledger(TIER)
    return {"capacity": int(ledger.capacity().get(KIND, 0)),
            "held": sum(int(ledger.holder_tokens(k).get(KIND, 0))
                        for k in ledger.held_keys()),
            "free": int(ledger.available().get(KIND, 0))}


def test_shared_egress_leaves_no_stealable_phantom(tmp_path: Path) -> None:
    """A and B land the same extent; A's shared egress must decharge, and a
    concurrent C claim before any new cycle must be refused."""
    snap: dict[str, object] = {"W_MODEL": W_MODEL_TIGHT,
                               "model": "discovery `available` only; all else real"}
    queue = _fleet(tmp_path)
    manifest, manifest_path = _manifest_bytes(tmp_path / "pool")
    manifest_sha = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    total = 2 * MIB

    _publish_mover(queue, MOVER_A, 0, MIB, manifest_sha, total)
    _publish_mover(queue, MOVER_B, 0, MIB, manifest_sha, total)
    _run_mover(tmp_path, queue, manifest_path, MOVER_A, CONSUMER_A, 0, MIB)
    _run_mover(tmp_path, queue, manifest_path, MOVER_B, CONSUMER_B, 0, MIB)
    ledger = queue.tier_ledger(TIER)
    assert ledger.holder_tokens(MOVER_A).get(KIND) == 1
    assert ledger.holder_tokens(MOVER_B).get(KIND) == 1
    staged = [p for p in (tmp_path / "stage").rglob("*")
              if p.is_file() and p.name != stage_release.STAGE_ROOT_MARKER]
    assert len(staged) == 1, "one extent staged once, two owners"
    snap["staged_files"] = len(staged)

    # The real cycle math: supply = writable + landed.
    minted = _supply(queue, W_MODEL_TIGHT)
    assert minted["landed"] == 2, minted
    assert minted["supply"] == W_MODEL_TIGHT + 2, minted
    numbers = _ledger_numbers(queue)
    assert (numbers["capacity"], numbers["held"], numbers["free"]) == (2, 2, 0)
    snap["after_mint"] = numbers
    print("AUDIT minted " + json.dumps(snap, sort_keys=True))

    # A's egress: bytes stay (shared), so no token may return as free.
    first = stage_release.evict(queue, MOVER_A, consumer_action_key=CONSUMER_A,
                                stage_root=str(tmp_path / "stage"))
    assert first["complete"] is True
    assert first["entries_shared"] == 1 and first["entries_deleted"] == 0
    assert staged[0].exists(), "B still vouches: bytes stay"
    snap["tokens_released"] = first["tokens_released"]
    snap["tokens_decharged"] = first.get("tokens_decharged", "MISSING")
    numbers = _ledger_numbers(queue)
    snap["after_a_egress"] = numbers
    print("AUDIT a-egress " + json.dumps(snap, sort_keys=True))

    # Concurrent C, BEFORE any new cycle: real admission verdict.
    _publish_mover(queue, MOVER_C, MIB, 2 * MIB, manifest_sha, total)
    c_claim = _claim(queue)
    snap["c_claim_pre_cycle"] = "admitted" if c_claim is not None else "refused"
    snap["phantom"] = numbers["free"] - W_MODEL_TIGHT
    print("AUDIT c-claim " + json.dumps(snap, sort_keys=True))

    violations = []
    if first["tokens_released"] != 0:
        violations.append("shared egress freed credits: tokens_released=%r"
                          % (first["tokens_released"],))
    if first.get("tokens_decharged") != 1:
        violations.append("duplicate ownership not decharged: %r"
                          % (first.get("tokens_decharged"),))
    if c_claim is not None:
        violations.append("PHANTOM OVERADMISSION: C claimed %r pre-cycle"
                          % (c_claim["action_key"][:12],))
    assert not violations, "RED: " + "; ".join(violations) + " :: " + json.dumps(
        snap, sort_keys=True)

    # Final owner deletes: useful capacity returns, next cycle is stable.
    second = stage_release.evict(queue, MOVER_B, consumer_action_key=CONSUMER_B,
                                 stage_root=str(tmp_path / "stage"))
    assert second["entries_deleted"] == 1 and not staged[0].exists()
    assert second["tokens_released"] == 1
    minted2 = _supply(queue, W_MODEL_TIGHT + 1)  # file gone: 1 token freed
    assert minted2["supply"] == 1, minted2
    numbers = _ledger_numbers(queue)
    assert numbers["free"] == 1, numbers
    c_claim2 = _claim(queue)
    assert c_claim2 is not None and c_claim2["action_key"] == MOVER_C
    snap["final"] = numbers
    print("AUDIT final " + json.dumps(snap, sort_keys=True))


def test_mixed_shared_and_deleted_splits_freed_from_decharged(tmp_path: Path) -> None:
    """One mover's fragment half-shared, half-exclusive: deleted bytes free
    their tokens, shared bytes decharge, nothing else moves."""
    queue = _fleet(tmp_path)
    manifest, manifest_path = _manifest_bytes(tmp_path / "pool")
    manifest_sha = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    total = 2 * MIB

    # A stages both extents as two chunks (2 tokens); B shares the first (1).
    _publish_mover(queue, MOVER_A, 0, 2 * MIB, manifest_sha, total, demand=2)
    _publish_mover(queue, MOVER_B, 0, MIB, manifest_sha, total)
    _run_mover(tmp_path, queue, manifest_path, MOVER_A, CONSUMER_A, 0, 2 * MIB)
    _run_mover(tmp_path, queue, manifest_path, MOVER_B, CONSUMER_B, 0, MIB)
    ledger = queue.tier_ledger(TIER)
    assert ledger.holder_tokens(MOVER_A).get(KIND) == 2
    _supply(queue, W_MODEL_TIGHT)

    first = stage_release.evict(queue, MOVER_A, consumer_action_key=CONSUMER_A,
                                stage_root=str(tmp_path / "stage"))
    assert first["complete"] is True
    assert first["entries_shared"] == 1 and first["entries_deleted"] == 1
    assert first["tokens_released"] == 1, first
    assert first.get("tokens_decharged") == 1, first
    remaining = sorted(p.name for p in (tmp_path / "stage").rglob("*")
                       if p.is_file() and p.name != stage_release.STAGE_ROOT_MARKER)
    assert remaining == ["shared.bin"], remaining


def test_interrupted_decharge_recovers_conservatively(tmp_path: Path) -> None:
    """A crash between file handling and accounting leaves everything held;
    the retry settles exactly once with no free inflation."""
    queue = _fleet(tmp_path)
    manifest, manifest_path = _manifest_bytes(tmp_path / "pool")
    manifest_sha = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    total = 2 * MIB
    _publish_mover(queue, MOVER_A, 0, MIB, manifest_sha, total)
    _publish_mover(queue, MOVER_B, 0, MIB, manifest_sha, total)
    _run_mover(tmp_path, queue, manifest_path, MOVER_A, CONSUMER_A, 0, MIB)
    _run_mover(tmp_path, queue, manifest_path, MOVER_B, CONSUMER_B, 0, MIB)
    _supply(queue, W_MODEL_TIGHT)
    ledger = queue.tier_ledger(TIER)

    real_settle = pool.PoolQueue.release_tier_holder_for_egress
    calls = {"n": 0}

    def _crash_once(self, *args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("simulated crash before settle")
        return real_settle(self, *args, **kwargs)

    pool.PoolQueue.release_tier_holder_for_egress = _crash_once  # type: ignore[method-assign]
    try:
        try:
            stage_release.evict(queue, MOVER_A, consumer_action_key=CONSUMER_A,
                                stage_root=str(tmp_path / "stage"))
        except RuntimeError:
            pass
        else:
            raise AssertionError("expected the simulated crash")
    finally:
        pool.PoolQueue.release_tier_holder_for_egress = real_settle  # type: ignore[method-assign]
    # Conservative: nothing freed or destroyed; fragment still vouches.
    assert ledger.holder_tokens(MOVER_A).get(KIND) == 1
    assert ledger.available().get(KIND, 0) == 0

    second = stage_release.evict(queue, MOVER_A, consumer_action_key=CONSUMER_A,
                                 stage_root=str(tmp_path / "stage"))
    assert second["complete"] is True
    assert second.get("tokens_decharged") == 1
    assert second["tokens_released"] == 0
    numbers = _ledger_numbers(queue)
    assert (numbers["capacity"], numbers["held"], numbers["free"]) == (1, 1, 0)


def test_retire_held_is_idempotent_and_remintable(tmp_path: Path) -> None:
    """Ledger unit: destroying held tokens counts actuals, retries no-op,
    and honest regrowth re-mints (no permanent leak)."""
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    ledger = queue.tier_ledger(TIER)
    queue.mint_tier_capacity(TIER, {KIND: 4})
    assert ledger.acquire(MOVER_A, {KIND: 3}) is True
    destroyed = ledger.retire_held(MOVER_A, {KIND: 2})
    assert destroyed == {KIND: 2}
    assert ledger.holder_tokens(MOVER_A).get(KIND) == 1
    assert ledger.capacity().get(KIND) == 2
    # Capped at the remainder, then a true no-op once nothing is held.
    assert ledger.retire_held(MOVER_A, {KIND: 2}) == {KIND: 1}
    assert ledger.holder_tokens(MOVER_A).get(KIND, 0) == 0
    assert ledger.retire_held(MOVER_A, {KIND: 1}) == {}
    queue.mint_tier_capacity(TIER, {KIND: 6})
    assert ledger.capacity().get(KIND) == 6
    assert ledger.available().get(KIND) == 6
