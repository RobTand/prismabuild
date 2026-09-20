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

import errno
import hashlib
import json
import os
import socket
import threading
import time
from pathlib import Path
import sys

import pytest

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
                   manifest_sha: str, total: int, demand: int | None = None,
                   fill: int | None = None) -> None:
    # Demand may exceed the range floor (chunked ranges ceil separately);
    # the gate refuses only below-floor declarations.  A sealed fill demand
    # exercises the real probe rule (oldest ready demand prices the tier).
    tokens = demand if demand is not None else storage_tiers.stage_tokens_for_bytes(end - start)
    resources = {"cpu": 1, "mem_gb": 1, f"{KIND}@{TIER}": tokens}
    if fill is not None:
        resources[f"{storage_tiers.FILL_KIND}@{TIER}"] = fill
    queue.publish(
        action_key=key, cas_root=str(queue.root / "cas"),
        checkout_root=str(queue.root / "co"),
        worker_script=str(queue.root / "worker.py"),
        resources=resources,
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
    _complete_move(tmp_path, queue, manifest_path, mover, consumer, start, end)
    return claimed


def _complete_move(tmp_path: Path, queue: pool.PoolQueue, manifest_path: Path,
                   mover: str, consumer: str, start: int, end: int):
    """Copy, file the receipt and finish an already-claimed mover."""
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


def test_retire_held_is_idempotent_and_reclaimable(tmp_path: Path) -> None:
    """Ledger unit: destroying held tokens counts actuals, retries converge,
    and a destroyed name never reappears except through dead-set reclaim
    inside an honestly backed wanted bound (no transient excess)."""
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
    dead = sorted(path.name for path in (ledger.minted_dir / "dead").iterdir())
    assert len(dead) == 3, dead
    # Honest regrowth to 6 reclaims exactly the dead slots inside the
    # wanted bound: capacity returns whole, free exact, then stable.
    result = queue.mint_tier_capacity(TIER, {KIND: 6})
    assert result["reclaimed"] == {KIND: 3}, result
    assert ledger.capacity().get(KIND) == 6
    assert ledger.available().get(KIND) == 6
    result = queue.mint_tier_capacity(TIER, {KIND: 6})
    assert result["reclaimed"] == {}, result
    assert ledger.capacity().get(KIND) == 6


def test_reclaim_converges_across_an_interleaved_rename(tmp_path: Path) -> None:
    """A token renamed into a reclaiming slot mid-apply cannot duplicate:
    the guard already passed, but the ordinary ensure still saves it --
    adoption re-marks the found token and the fill loop skips the marked
    name, so no second copy is ever created. Capacity stays exact."""
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    ledger = queue.tier_ledger(TIER)
    queue.mint_tier_capacity(TIER, {KIND: 4})
    assert ledger.acquire(MOVER_A, {KIND: 2}) is True
    assert ledger.retire_held(MOVER_A, {KIND: 1}) == {KIND: 1}
    assert ledger.holder_tokens(MOVER_A).get(KIND) == 1
    dead_dir = ledger.minted_dir / "dead"
    (victim,) = sorted(path.name for path in dead_dir.iterdir())
    spare = tmp_path / "spare-token"
    spare.write_bytes(b"x")
    (ledger.held_dir / MOVER_B).mkdir(parents=True, exist_ok=True)
    real_unlink = Path.unlink

    def _rename_in(path_self, *args, **kwargs):
        if str(path_self) == str(dead_dir / victim):
            # A rename landing between the guard and the unlink: the slot
            # is live again before anything is reaped.
            os.rename(str(spare), str(ledger.held_dir / MOVER_B / victim))
        return real_unlink(path_self, *args, **kwargs)

    monkey = pytest.MonkeyPatch()
    monkey.setattr(Path, "unlink", _rename_in)
    try:
        result = queue.mint_tier_capacity(TIER, {KIND: 4})
    finally:
        monkey.undo()
    # No duplicate: the slot is held once, marked once, dead record gone.
    assert ledger.holder_tokens(MOVER_B).get(KIND) == 1
    assert (ledger.minted_dir / victim).exists()
    assert not (dead_dir / victim).exists()
    assert result["reclaimed"] == {KIND: 1}, result
    assert ledger.capacity().get(KIND) == 4
    copies = 0
    for path in pool._scan(ledger.free_dir):
        copies += path.name == victim
    for holder in pool._scan(ledger.held_dir):
        if holder.is_dir():
            for path in pool._glob(holder, "*-*"):
                copies += path.name == victim
    assert copies == 1, "exactly one live token carries the name"
    result = queue.mint_tier_capacity(TIER, {KIND: 4})
    assert ledger.capacity().get(KIND) == 4


# -- R3: the actual cycle, fractional buckets, failed decharge -------------

def _shield_consumer(queue: pool.PoolQueue) -> None:
    """A live consumer naming A and B as leads, so the cycle's orphan sweep
    spares their pinned ranges (production shielding, not a mint input)."""
    queue.publish(
        action_key="e" * 64, cas_root=str(queue.root / "cas"),
        checkout_root=str(queue.root / "co"),
        worker_script=str(queue.root / "worker.py"),
        resources={"cpu": 1, "mem_gb": 1},
        residency={"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                   "manifest_sha256": "f" * 64, "manifest_bytes": 1 << 40,
                   "leads": [MOVER_A, MOVER_B]})


def _tiny_discover(stage: Path, capacity_bytes: int, fill: int):
    def discover(*, host, source_pool, fill_records, now, ram_policy,
                 worker_mem_gb):
        return {TIER: {
            "tier_id": TIER, "tier": "stage", "host": socket.gethostname(),
            "pool": "tank/stage", "dataset": "tank/stage",
            "mountpoint": str(stage),
            "capacity_bytes": capacity_bytes,
            "capacity_source": storage_tiers.WRITABLE_CAPACITY_SOURCE,
            "primarycache": "all",
            storage_tiers.FILL_RECORD_FIELD: fill,
        }}
    return discover


def _run_cycle(queue: pool.PoolQueue, stage: Path, **kw) -> list[dict]:
    return tier_loop.cycle(
        queue, host=socket.gethostname(), source_pool="tank",
        receipts=tier_loop.ReceiptCache(),
        discover=_tiny_discover(stage, **kw))


def test_actual_cycle_mints_once_and_never_wipes_rates(tmp_path: Path) -> None:
    """The ACTUAL tier_loop.cycle applies exactly one ledger write per tier:
    no early partial-kind mint, no stale second write, rate kinds intact."""
    queue = _fleet(tmp_path)
    manifest, manifest_path = _manifest_bytes(tmp_path / "pool")
    manifest_sha = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    total = 2 * MIB
    _publish_mover(queue, MOVER_A, 0, MIB, manifest_sha, total)
    _publish_mover(queue, MOVER_B, 0, MIB, manifest_sha, total)
    _run_mover(tmp_path, queue, manifest_path, MOVER_A, CONSUMER_A, 0, MIB)
    _run_mover(tmp_path, queue, manifest_path, MOVER_B, CONSUMER_B, 0, MIB)
    _shield_consumer(queue)
    # A ready row with sealed fill demand: the real probe rule (not the
    # record's advisory fill field) prices the tier's rate kind.
    _publish_mover(queue, MOVER_C, MIB, 2 * MIB, manifest_sha, total, fill=7)
    ledger = queue.tier_ledger(TIER)

    applies: list[dict] = []
    real_apply = pool.PoolQueue._apply_tier_capacity

    def counting(self, tier_id, tier_ledger, wanted):
        if tier_id == TIER:
            applies.append(dict(wanted))
        return real_apply(self, tier_id, tier_ledger, wanted)

    monkey = pytest.MonkeyPatch()
    monkey.setattr(pool.PoolQueue, "_apply_tier_capacity", counting)
    try:
        announced = _run_cycle(queue, tmp_path / "stage",
                               capacity_bytes=1, fill=50)
    finally:
        monkey.undo()
    assert len(applies) == 1, applies
    assert set(applies[0]) == {KIND, storage_tiers.FILL_KIND}, applies
    assert applies[0][KIND] == 0 + 2, applies  # writable + landed, once
    assert applies[0][storage_tiers.FILL_KIND] == 7, applies  # real probe
    (record,) = [r for r in announced if r["tier_id"] == TIER]
    assert record["landed_gib"] == 2 and record["in_flight_gib"] == 0
    assert record["capacity_basis"] == "zfs available + landed"
    assert ledger.capacity().get(KIND) == 2
    assert ledger.capacity().get(storage_tiers.FILL_KIND) == 7
    assert ledger.available().get(KIND, 0) == 0
    assert ledger.holder_tokens(MOVER_A).get(KIND) == 1
    assert ledger.holder_tokens(MOVER_B).get(KIND) == 1


def test_actual_cycle_on_refused_root_keeps_without_churn(tmp_path: Path) -> None:
    """An inadmissible tier gets no momentary supply: one keep-mint, rate
    and occupancy holds intact, never transiently zeroed."""
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    stage = tmp_path / "stage"
    stage.mkdir(parents=True, exist_ok=True)
    queue.mint_tier_capacity(TIER, {KIND: 3, storage_tiers.FILL_KIND: 50})
    ledger = queue.tier_ledger(TIER)
    assert ledger.acquire("d" * 64, {KIND: 1}) is True
    # A live (ready) consumer naming the holder, so the cycle's orphan
    # sweep cannot mistake the pinned key for an orphan.
    queue.publish(
        action_key="e" * 64, cas_root=str(queue.root / "cas"),
        checkout_root=str(queue.root / "co"),
        worker_script=str(queue.root / "worker.py"),
        resources={"cpu": 1, "mem_gb": 1},
        residency={"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                   "manifest_sha256": "f" * 64, "manifest_bytes": 1 << 40,
                   "leads": ["d" * 64]})
    other = pool.PoolQueue(tmp_path / "other-queue")
    other.ensure_layout()
    assert stage_release.register_stage_root(
        other, tier_id=TIER, stage_root=stage) == "registered"
    # A ready row with sealed fill demand, so the keep-mint must carry the
    # rate kind through the refusal path as well.
    queue.publish(
        action_key=MOVER_C, cas_root=str(queue.root / "cas"),
        checkout_root=str(queue.root / "co"),
        worker_script=str(queue.root / "worker.py"),
        resources={"cpu": 1, "mem_gb": 1, f"{KIND}@{TIER}": 1,
                   f"{storage_tiers.FILL_KIND}@{TIER}": 7},
        residency={"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                   "manifest_sha256": "f" * 64, "manifest_bytes": 1 << 40,
                   "range_start_bytes": 0, "range_end_bytes": MIB},
        max_attempts=1, retry_safe=False)

    applies: list[dict] = []
    real_apply = pool.PoolQueue._apply_tier_capacity

    def counting(self, tier_id, tier_ledger, wanted):
        if tier_id == TIER:
            applies.append(dict(wanted))
        return real_apply(self, tier_id, tier_ledger, wanted)

    monkey = pytest.MonkeyPatch()
    monkey.setattr(pool.PoolQueue, "_apply_tier_capacity", counting)
    try:
        announced = tier_loop.cycle(
            queue, host=socket.gethostname(), source_pool="tank",
            receipts=tier_loop.ReceiptCache(),
            discover=_tiny_discover(stage, capacity_bytes=1, fill=50))
    finally:
        monkey.undo()
    (record,) = [r for r in announced if r["tier_id"] == TIER]
    assert record.get("stage_root_admits") is False
    assert len(applies) == 1, applies
    assert ledger.holder_tokens("d" * 64).get(KIND) == 1
    assert ledger.capacity().get(storage_tiers.FILL_KIND) == 7
    assert ledger.capacity().get(KIND) == 1  # keep: held stays, free retired


def _big_bytes(pool_dir: Path, name: str, size: int) -> bytes:
    pool_dir.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256(f"audit-{name}".encode()).digest()
    path = pool_dir / name
    with open(path, "wb") as stream:
        for _ in range(size // (1 << 20)):
            stream.write(digest * ((1 << 20) // 32))
    return path.read_bytes()


def test_fractional_mixed_bucket_decharges_without_freeing(tmp_path: Path) -> None:
    """One token backs 0.6 GiB deleted + 0.4 GiB shared with zero writable:
    freeing the ceil would hand a whole token for 0.6 GiB of new room, so
    nothing returns and the duplicate decharges (600 + 400 MiB, real GiB
    ceil math, labelled)."""
    pool_dir = tmp_path / "pool"
    excl = _big_bytes(pool_dir, "excl.bin", 600 * (1 << 20))
    shared_blob = _big_bytes(pool_dir, "frac.bin", 400 * (1 << 20))
    entries = [
        {"path": str(pool_dir / "frac.bin"), "offset": 0,
         "bytes": len(shared_blob),
         "sha256": hashlib.sha256(shared_blob).hexdigest()},
        {"path": str(pool_dir / "excl.bin"), "offset": 0, "bytes": len(excl),
         "sha256": hashlib.sha256(excl).hexdigest()},
    ]
    total = sum(e["bytes"] for e in entries)
    manifest = {
        "schema": pb.DATA_MANIFEST_SCHEMA_V1,
        "produced_by": {"tool": "audit-r3-fractional"},
        "mount_prefix": str(pool_dir),
        "entries": entries, "entry_count": 2, "total_bytes": total,
        "annotations": {"phases": [
            {"name": "shared", "cumulative_bytes": len(shared_blob)},
            {"name": "all", "cumulative_bytes": total}]},
    }
    manifest_path = pool_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest))
    manifest_sha = hashlib.sha256(manifest_path.read_bytes()).hexdigest()

    queue = _fleet(tmp_path)
    # A holds exactly the 1 GiB floor for the whole range; B shares 0.4.
    _publish_mover(queue, MOVER_A, 0, total, manifest_sha, total)
    _publish_mover(queue, MOVER_B, 0, len(shared_blob), manifest_sha, total)
    _run_mover(tmp_path, queue, manifest_path, MOVER_A, CONSUMER_A, 0, total)
    _run_mover(tmp_path, queue, manifest_path, MOVER_B, CONSUMER_B,
               0, len(shared_blob))
    ledger = queue.tier_ledger(TIER)
    assert ledger.holder_tokens(MOVER_A).get(KIND) == 1
    _supply(queue, W_MODEL_TIGHT)

    first = stage_release.evict(queue, MOVER_A, consumer_action_key=CONSUMER_A,
                                stage_root=str(tmp_path / "stage"))
    assert first["complete"] is True
    assert first["entries_shared"] == 1 and first["entries_deleted"] == 1
    assert first["tokens_released"] == 0, first
    assert first.get("tokens_decharged") == 1, first
    assert (tmp_path / "stage" / "frac.bin").exists()
    assert not (tmp_path / "stage" / "excl.bin").exists()
    numbers = _ledger_numbers(queue)
    assert numbers["free"] == 0, numbers
    _publish_mover(queue, MOVER_C, 0, MIB, manifest_sha, total)
    assert _claim(queue) is None


def test_decharge_failure_retains_and_surfaces_until_retry(tmp_path: Path) -> None:
    """An unlink failure mid-decharge keeps the duplicate held, reports
    incomplete, refuses a claimant, and converges on retry."""
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
    held_dir = str(ledger.held_dir / MOVER_A)

    real_unlink = os.unlink
    calls = {"n": 0}

    def _fail_once(path, *args, **kwargs):
        if (str(path).startswith(held_dir + "/")
                and str(path).split("/")[-1].startswith(KIND) and calls["n"] == 0):
            calls["n"] += 1
            raise OSError(errno.EIO, "injected decharge failure")
        return real_unlink(path, *args, **kwargs)

    monkey = pytest.MonkeyPatch()
    monkey.setattr("os.unlink", _fail_once)
    try:
        failed = stage_release.evict(
            queue, MOVER_A, consumer_action_key=CONSUMER_A,
            stage_root=str(tmp_path / "stage"))
    finally:
        monkey.undo()
    assert failed["complete"] is False
    assert any("decharge-incomplete" in e for e in failed["errors"]), failed
    assert failed.get("tokens_decharged", 0) == 0
    assert ledger.holder_tokens(MOVER_A).get(KIND) == 1
    assert ledger.available().get(KIND, 0) == 0
    _publish_mover(queue, MOVER_C, MIB, 2 * MIB, manifest_sha, total)
    assert _claim(queue) is None, "failed decharge must not free anything"

    retry = stage_release.evict(queue, MOVER_A, consumer_action_key=CONSUMER_A,
                                stage_root=str(tmp_path / "stage"))
    assert retry["complete"] is True
    assert retry.get("tokens_decharged") == 1
    assert retry["tokens_released"] == 0
    numbers = _ledger_numbers(queue)
    assert (numbers["capacity"], numbers["held"], numbers["free"]) == (1, 1, 0)


def test_concurrent_egress_and_mint_never_shows_phantom(tmp_path: Path) -> None:
    """Egress decharge racing cycle mints: sampled free never exceeds the
    modelled physical free, and the end state is exact."""
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
    _publish_mover(queue, MOVER_C, MIB, 2 * MIB, manifest_sha, total)

    samples: list[int] = []
    stop = threading.Event()

    def _mint_loop() -> None:
        for _ in range(5):
            tier_loop.mint_stage_supply(
                queue, tier_id=TIER, kind=KIND, writable_tokens=W_MODEL_TIGHT)

    def _egress() -> None:
        stage_release.evict(queue, MOVER_A, consumer_action_key=CONSUMER_A,
                            stage_root=str(tmp_path / "stage"))

    mint_thread = threading.Thread(target=_mint_loop)
    egress_thread = threading.Thread(target=_egress)
    mint_thread.start()
    egress_thread.start()
    while mint_thread.is_alive() or egress_thread.is_alive():
        samples.append(int(ledger.available().get(KIND, 0)))
        time.sleep(0.001)
    mint_thread.join()
    egress_thread.join()
    samples.append(int(ledger.available().get(KIND, 0)))
    assert samples, "must have observed the race window"
    assert max(samples) == 0, samples
    assert _claim(queue) is None
    numbers = _ledger_numbers(queue)
    assert (numbers["capacity"], numbers["held"], numbers["free"]) == (1, 1, 0)


# -- R4: mixed-time mint + dead-marker regrowth (minimal REDs) ----------------

def test_mixed_time_mint_does_not_create_free(tmp_path: Path) -> None:
    """A copy+complete landing between discovery and the mint must not mint
    free: the actual cycle runs with discovery W=1 while A's bytes land
    before the mint section, and C must still be refused.  The lock-time
    re-sample is doubled by the modelled disk (same shape as the
    production `stage_dataset` reader); everything else is the production
    path, including the mint lock that orders A's filing against the
    re-sample."""
    disk = {"writable": 1}  # the modelled disk
    queue = _fleet(tmp_path)
    manifest, manifest_path = _manifest_bytes(tmp_path / "pool")
    manifest_sha = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    total = 2 * MIB
    queue.mint_tier_capacity(TIER, {KIND: 1})
    _publish_mover(queue, MOVER_A, 0, MIB, manifest_sha, total)
    _shield_consumer(queue)
    claimed = _claim(queue)
    assert claimed is not None and claimed["action_key"] == MOVER_A
    assert _ledger_numbers(queue)["free"] == 0

    real_apply = pool.PoolQueue._apply_tier_capacity
    real_helper = tier_loop.mint_stage_supply
    applied: list[dict] = []

    def completing_helper(*args, **kwargs):
        if not applied:
            # A lands between discovery and the mint section, for real --
            # before any lock is taken, so no copy runs under a global lock.
            _complete_move(tmp_path, queue, manifest_path,
                           MOVER_A, CONSUMER_A, 0, MIB)
            disk["writable"] = 0
            applied.append("completed")
        return real_helper(*args, **kwargs)

    def recording_apply(self, tier_id, tier_ledger, wanted):
        applied.append(dict(wanted))
        return real_apply(self, tier_id, tier_ledger, wanted)

    def fake_stage_dataset(pool_name, **kwargs):
        return {"dataset": pool_name, "available_bytes": disk["writable"] * storage_tiers.GIB,
                "mountpoint": str(tmp_path / "stage"), "primarycache": "all"}

    monkey = pytest.MonkeyPatch()
    monkey.setattr(tier_loop, "mint_stage_supply", completing_helper)
    monkey.setattr(pool.PoolQueue, "_apply_tier_capacity", recording_apply)
    monkey.setattr(storage_tiers, "stage_dataset", fake_stage_dataset)
    try:
        announced = tier_loop.cycle(
            queue, host=socket.gethostname(), source_pool="tank",
            receipts=tier_loop.ReceiptCache(),
            discover=_tiny_discover(
                tmp_path / "stage", capacity_bytes=disk["writable"] * storage_tiers.GIB,
                fill=0))
    finally:
        monkey.undo()
    wanteds = [entry for entry in applied if isinstance(entry, dict)]
    assert len(wanteds) == 1, wanteds
    # Mechanism, not just verdict: the mint must pair the re-sampled
    # writable (0, after the copy) with the newcomer's landed (1).
    assert wanteds[0][KIND] == 1, wanteds
    numbers = _ledger_numbers(queue)
    snap = {"applied": wanteds, "ledger": numbers, "disk": dict(disk)}
    print("AUDIT mixed-time " + json.dumps(snap, sort_keys=True))
    _publish_mover(queue, MOVER_C, MIB, 2 * MIB, manifest_sha, total)
    assert _claim(queue) is None, (
        "MIXED-TIME OVERMINT: " + json.dumps(snap, sort_keys=True))
    assert numbers["free"] == 0, snap


def test_fresh_mint_regrows_usable_capacity(tmp_path: Path) -> None:
    """Decharged names must not ratchet usable capacity down: after A/B
    shared, A decharge and B delete, a fresh mint on honestly regrown
    writable readmits a newcomer -- repeatedly, without shrinkage."""
    queue = _fleet(tmp_path)
    manifest, manifest_path = _manifest_bytes(tmp_path / "pool")
    manifest_sha = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    total = 2 * MIB
    _publish_mover(queue, MOVER_A, 0, MIB, manifest_sha, total)
    _publish_mover(queue, MOVER_B, 0, MIB, manifest_sha, total)
    _run_mover(tmp_path, queue, manifest_path, MOVER_A, CONSUMER_A, 0, MIB)
    _run_mover(tmp_path, queue, manifest_path, MOVER_B, CONSUMER_B, 0, MIB)
    _supply(queue, 0)
    first = stage_release.evict(queue, MOVER_A, consumer_action_key=CONSUMER_A,
                                stage_root=str(tmp_path / "stage"))
    assert first.get("tokens_decharged") == 1
    second = stage_release.evict(queue, MOVER_B, consumer_action_key=CONSUMER_B,
                                 stage_root=str(tmp_path / "stage"))
    assert second["entries_deleted"] == 1 and second["tokens_released"] == 1

    # The file is gone: writable honestly holds 1 token again.
    _supply(queue, 1)
    _publish_mover(queue, MOVER_C, MIB, 2 * MIB, manifest_sha, total)
    c_claim = _claim(queue)
    snap = {"ledger": _ledger_numbers(queue)}
    print("AUDIT regrow " + json.dumps(snap, sort_keys=True))
    assert c_claim is not None and c_claim["action_key"] == MOVER_C, (
        "DEAD-MARKER LEAKAGE: " + json.dumps(snap, sort_keys=True))

    # A second wave on the same ledger must find the same room, not less.
    _publish_mover(queue, "d" * 64, MIB, 2 * MIB, manifest_sha, total)
    _supply(queue, 2)
    d_claim = _claim(queue)
    snap["round2"] = _ledger_numbers(queue)
    assert d_claim is not None, (
        "DEAD-MARKER LEAKAGE (round 2): " + json.dumps(snap, sort_keys=True))
    _supply(queue, 2)
    assert _ledger_numbers(queue) == snap["round2"], snap
