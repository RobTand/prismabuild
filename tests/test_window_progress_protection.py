"""Window progress policy on the accepted funded-claim primitive (#745).

Real production paths end to end with tiny physical fixtures: 1 MiB ranges
(1 token each), real ``stage_move.move`` copies from a local corpus, real
``ram_promote.promote`` with a provisioned epoch, real ``evict`` deletions.
No hand-filed byte receipts for the integration result; no fleet defaults.
Component scope is labeled where it applies (the device copy itself,
``stage_move`` against real pools, is covered by the mover's own suites;
what is proved here is the window policy around it: gating, fencing,
settling, cancellation bounds, and pin charge).

* wedge-becomes-progress (C=3): the second window is gated typed-transient
  at admission, the admitted window runs to durable completion with real
  bytes, and the stalled window is admitted into the freed room and
  completes -- digest-verified at every landing;
* parallel-fit (C=6): both windows run concurrently, all four ranges land;
* fence (C=3): a held fence blocks a 2-token stealer that fits the empty
  tier, admits a 1-token one, and releases exactly once through the owner
  path;
* cancellation: withdrawing a consumer frees its grant fence boundedly
  while staged pins stay charged;
* pin: a landed pin survives window/settle cycles until the egress;
* ram leg (SSD+RAM): a promotion publishes behind its landed stage source,
  promotes real bytes under epoch, and stalls typed when the joint gate
  refuses it.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

from prismabuild import pool, residency_map, residency_plan, storage_tiers  # noqa: E402
from prismabuild import core as pb  # noqa: E402
from prismabuild import window_credit  # noqa: E402
import stage_release  # noqa: E402
import stage_move  # noqa: E402
import tier_loop  # noqa: E402

TIER = "prismabuild-stage:dl380g10"
RAM_TIER = "ram:dl380g10"
STAGE_KIND = f"stage_gib@{TIER}"
RAM_KIND = f"ram_gib@{RAM_TIER}"
GIB = storage_tiers.GIB
SPAN = 1 << 20

CONSUMER_A = "a" * 64
CONSUMER_B = "b" * 64


def _hexkey(seed: str) -> str:
    return (seed.encode().hex() * 64)[:64]


def _row(key: str, resources: dict[str, int], queue: pool.PoolQueue) -> dict[str, object]:
    return {"action_key": key, "cas_root": str(queue.root / "cas"),
            "checkout_root": str(queue.root / "co"),
            "worker_script": str(queue.root / "worker.py"),
            "tags": ["dl380g10"], "resources": resources}


def _corpus(tmp_path: Path, tag: str) -> tuple[Path, dict[str, bytes]]:
    """Two 1 MiB source files with distinct bytes, under one mount.

    Payload names carry the tag: several of these fixtures share one stage
    root, and the accepted shared-publication contract (#752) refuses a
    second mover whose bytes diverge at a name the first one published --
    distinct names keep these windows independent, which is what the
    fixtures mean to exercise.
    """
    mount = tmp_path / f"mnt-{tag}"
    payloads = {
        f"{tag}-p0.bin": hashlib.sha256(f"{tag}-p0".encode()).digest() * (SPAN // 32),
        f"{tag}-p1.bin": hashlib.sha256(f"{tag}-p1".encode()).digest() * (SPAN // 32),
    }
    for name, payload in payloads.items():
        path = mount / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
    return mount, payloads


def _manifest(mount: Path, payloads: dict[str, bytes]) -> dict[str, object]:
    entries = [{"path": str(mount / name), "offset": 0, "bytes": len(payload),
                "sha256": hashlib.sha256(payload).hexdigest()}
               for name, payload in sorted(payloads.items())]
    return {
        "schema": pb.DATA_MANIFEST_SCHEMA_V1,
        "produced_by": {"tool": "window-policy-fixture"},
        "mount_prefix": str(mount),
        "entries": entries,
        "entry_count": len(entries),
        "total_bytes": sum(int(entry["bytes"]) for entry in entries),
        "annotations": {},
    }


def _plan(queue: pool.PoolQueue, consumer: str, digest: str, *,
          tag: str) -> dict[str, object]:
    built = []
    for ordinal in range(2):
        start, end = ordinal * SPAN, (ordinal + 1) * SPAN
        built.append({
            "name": f"phase-{ordinal}",
            "start_bytes": start, "end_bytes": end, "stage_gib": 1,
            "mover_row": {
                **_row(_hexkey(f"wpp-{tag}-{ordinal}"),
                       {STAGE_KIND: 1, "mem_gb": 1}, queue),
                "residency": {
                    "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                    "manifest_sha256": digest,
                    "manifest_bytes": 2 * SPAN,
                    "range_start_bytes": start, "range_end_bytes": end},
            },
            "egress_row": _row(_hexkey(f"wppe-{tag}-{ordinal}"),
                               {"mem_gb": 1}, queue),
        })
    return residency_plan.build_plan(
        consumer_action_key=consumer, tier_id=TIER, stage_root="/stage/prewarm",
        manifest_sha256=digest, manifest_bytes=2 * SPAN, phases=built)


def _queue(tmp_path: Path, *, stage_gib: int, ram_gib: int = 0) -> pool.PoolQueue:
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    queue.ledger().ensure_capacity({"cpu": 4, "mem_gb": 8})
    queue.mint_tier_capacity(TIER, {"stage_gib": stage_gib})
    stage = tmp_path / "stage"
    stage.mkdir(exist_ok=True)
    assert stage_release.register_stage_root(
        queue, tier_id=TIER, stage_root=str(stage)) == "registered"
    if ram_gib:
        queue.mint_tier_capacity(RAM_TIER, {"ram_gib": ram_gib})
    return queue


def _tiers(tmp_path: Path) -> dict[str, dict[str, object]]:
    return {TIER: {"tier_id": TIER, "tier": "stage",
                   "mountpoint": str(tmp_path / "stage")}}


def _publish_consumer(queue: pool.PoolQueue, plan: dict[str, object],
                      consumer: str) -> None:
    residency_plan.freeze(queue, plan)
    queue.publish(
        action_key=consumer, cas_root=queue.root / "cas",
        checkout_root=queue.root / "co", worker_script=queue.root / "worker.py",
        resources={"cpu": 1, "mem_gb": 1},
        residency={"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                   "manifest_sha256": str(plan["manifest_sha256"]),
                   "manifest_bytes": 2 * SPAN,
                   "leads": residency_plan.leads_for(plan)})


def _mover_key(plan: dict[str, object], ordinal: int) -> str:
    phases = plan["phases"]
    assert isinstance(phases, list)
    return str(phases[ordinal]["mover_row"]["action_key"])  # type: ignore[index]


def _claim_exact(queue: pool.PoolQueue, key: str, *, owner: str) -> dict[str, object]:
    items = queue.ready_items()
    items.sort(key=lambda item: (0 if str(item.get("action_key")) == key else 1))
    got = queue.claim(tags=["dl380g10"], owner=owner, ready=items)
    assert got is not None and got["action_key"] == key
    return got


def _move_args(queue: pool.PoolQueue, tmp_path: Path, manifest_path: Path,
               digest: str, mover: str, consumer: str, start: int, end: int):
    return stage_move.build_parser().parse_args([
        "--pool-root", str(queue.root),
        "--cas-root", str(tmp_path / "cas"),
        "--action-key", mover,
        "--consumer-action-key", consumer,
        "--tier-id", TIER,
        "--stage-root", str(tmp_path / "stage"),
        "--manifest-sha256", digest,
        "--range-start-bytes", str(start),
        "--range-end-bytes", str(end),
        "--manifest", str(manifest_path),
        "--residency-root", str(queue.root / pool.RESIDENCY),
        "--block", str(1 << 16),
        "--readers", "2", "--max-readers", "2", "--unpaced",
    ])


def _await_funded(queue: pool.PoolQueue, mover: str, *, cycles: int = 4) -> None:
    for _ in range(cycles):
        record = queue.read_funding(mover, TIER)
        if record is not None and record.get("state") == "transferring":
            names = record.get("tokens")
            held = {path.name for path in
                    (queue.tier_ledger(TIER).held_dir / mover).glob("*-*")}
            if isinstance(names, list) and names and all(
                    str(name) in held for name in names):
                return
    raise AssertionError(f"no transferred fence for {mover[:12]}")


def _staged_name(file_name: str) -> str:
    """The stage's name for one whole ``SPAN``-byte source file."""

    return stage_move.stage_relative(f"/m/{file_name}", 0, SPAN,
                                     mount_prefix="/m")

def _land(queue: pool.PoolQueue, tmp_path: Path, manifest_path: Path,
          digest: str, mover: str, consumer: str, start: int, end: int,
          expect: bytes, *, owner: str) -> None:
    """Claim one mover, copy its real bytes, file the receipt, finish pinned."""
    _claim_exact(queue, mover, owner=owner)
    receipt = stage_move.move(
        _move_args(queue, tmp_path, manifest_path, digest, mover, consumer,
                   start, end))
    assert receipt["complete"] is True, receipt
    assert int(receipt["bytes_staged"]) == end - start
    assert receipt.get("refusal") is None
    queue.record_move(mover, receipt)
    queue.finish(mover, status="executed")
    assert queue.item_path(pool.DONE, mover).exists()
    assert int(queue.tier_ledger(TIER).holder_tokens(mover).get("stage_gib", 0)) == 1
    entries = sorted(json.loads(manifest_path.read_text())["entries"],
                     key=lambda entry: entry["path"])
    staged_name = Path(str(entries[start // SPAN]["path"])).name
    staged = tmp_path / "stage" / _staged_name(staged_name)
    assert staged.read_bytes() == expect, f"staged bytes differ for {mover[:12]}"


def _ready_only(queue: pool.PoolQueue, key: str) -> list[dict[str, object]]:
    return [item for item in queue.ready_items()
            if str(item.get("action_key")) == key]


def _stealer_row(queue: pool.PoolQueue, key: str, gib: int) -> None:
    span = gib * SPAN
    queue.publish(
        action_key=key, cas_root=queue.root / "cas",
        checkout_root=queue.root / "co", worker_script=queue.root / "worker.py",
        tags=["dl380g10"], resources={STAGE_KIND: gib},
        residency={"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                   "manifest_sha256": "7" * 64, "manifest_bytes": span,
                   "range_start_bytes": 0, "range_end_bytes": span})


def _setup_two_consumers(tmp_path: Path, *, stage_gib: int):
    """Two frozen/published consumers with per-phase manifests; returns ctx."""
    queue = _queue(tmp_path, stage_gib=stage_gib)
    ctx: dict[str, object] = {"queue": queue}
    for consumer, tag, digest in ((CONSUMER_A, "aa", "8" * 64),
                                  (CONSUMER_B, "bb", "9" * 64)):
        mount, payloads = _corpus(tmp_path, tag)
        manifest = _manifest(mount, payloads)
        manifest_path = tmp_path / f"manifest-{tag}.json"
        manifest_path.write_text(json.dumps(manifest))
        plan = _plan(queue, consumer, digest, tag=tag)
        _publish_consumer(queue, plan, consumer)
        ctx[tag] = {"plan": plan, "mount": mount, "payloads": payloads,
                    "manifest_path": manifest_path, "digest": digest,
                    "movers": [_mover_key(plan, 0), _mover_key(plan, 1)]}
    return ctx


def _gated(events: list[dict[str, object]]) -> dict[str, dict[str, object]]:
    return {str(e["consumer"]): e for e in events
            if e.get("event") == "window-gated"}


def _published(events: list[dict[str, object]]) -> set[str]:
    return {str(e["action_key"]) for e in events
            if e.get("event") == "mover-published"}


def test_wedge_becomes_durable_progress(tmp_path: Path) -> None:
    """The formerly wedging pair: gated typed-transient, both complete real."""
    ctx = _setup_two_consumers(tmp_path, stage_gib=3)
    queue = ctx["queue"]
    tiers = _tiers(tmp_path)

    first = tier_loop.residency_window(queue, tiers=tiers)
    assert _published(first) == set(ctx["aa"]["movers"]) or \
        _published(first) == set(ctx["bb"]["movers"])
    first_consumer = (CONSUMER_A
                      if ctx["aa"]["movers"][0] in _published(first)
                      else CONSUMER_B)
    gated_consumer = (CONSUMER_B
                      if first_consumer == CONSUMER_A else CONSUMER_A)
    gate = _gated(first)
    assert set(gate) == {gated_consumer}
    # Both gates refuse (1+1 held against 3 beside a 1+1 minimum; 2 committed
    # beside a 2 GiB footprint), and since #907 the commitment names it:
    # no eviction admits the second window while the first is running.
    assert gate[gated_consumer]["reason"] == "joint-commitment-stall"
    assert gate[gated_consumer]["permanent"] is False
    P, Q = first_consumer, gated_consumer
    ptag = "aa" if P == CONSUMER_A else "bb"
    qtag = "bb" if P == CONSUMER_A else "aa"
    p0, p1 = ctx[ptag]["movers"]
    q0, q1 = ctx[qtag]["movers"]

    # The admitted window runs to durable completion with real bytes.
    _land(queue, tmp_path, ctx[ptag]["manifest_path"], ctx[ptag]["digest"],
          p0, P, 0, SPAN, ctx[ptag]["payloads"][f"{ptag}-p0.bin"], owner="w-pp")
    for _ in range(4):
        tier_loop.residency_window(queue, tiers=tiers)
        record = queue.read_funding(p1, TIER)
        if record is not None and record.get("state") == "transferring":
            break
    # The protected next claims with no new money: free is untouched.
    ledger = queue.tier_ledger(TIER)
    _await_funded(queue, p1)
    free_before = ledger.available().get("stage_gib")
    _claim_exact(queue, p1, owner="w-pp")
    assert ledger.available().get("stage_gib") == free_before
    receipt = stage_move.move(
        _move_args(queue, tmp_path, ctx[ptag]["manifest_path"],
                   ctx[ptag]["digest"], p1, P, SPAN, 2 * SPAN))
    assert receipt["complete"] is True
    queue.record_move(p1, receipt)
    queue.finish(p1, status="executed")
    assert (tmp_path / "stage"
            / _staged_name(f"{ptag}-p1.bin")).read_bytes() == \
        ctx[ptag]["payloads"][f"{ptag}-p1.bin"]
    assert tier_loop.compose_map(queue, P) is not None
    _claim_exact(queue, P, owner="w-pp")
    queue.finish(P, status="executed")

    # The stalled window is admitted into the freed room and completes.
    stage_release.evict(queue, p0, consumer_action_key=P,
                        stage_root=str(tmp_path / "stage"))
    assert not (tmp_path / "stage" / _staged_name(f"{ptag}-p0.bin")).exists()
    admitted = False
    for _ in range(4):
        events = tier_loop.residency_window(queue, tiers=tiers)
        if q0 in _published(events):
            admitted = True
            break
    assert admitted, "stalled window never admitted after egress"
    assert Q not in _gated(tier_loop.residency_window(queue, tiers=tiers))
    _land(queue, tmp_path, ctx[qtag]["manifest_path"], ctx[qtag]["digest"],
          q0, Q, 0, SPAN, ctx[qtag]["payloads"][f"{qtag}-p0.bin"], owner="w-pp")
    for _ in range(4):
        tier_loop.residency_window(queue, tiers=tiers)
        record = queue.read_funding(q1, TIER)
        if record is not None and record.get("state") == "transferring":
            break
    _await_funded(queue, q1)
    _claim_exact(queue, q1, owner="w-pp")
    receipt = stage_move.move(
        _move_args(queue, tmp_path, ctx[qtag]["manifest_path"],
                   ctx[qtag]["digest"], q1, Q, SPAN, 2 * SPAN))
    assert receipt["complete"] is True
    queue.record_move(q1, receipt)
    queue.finish(q1, status="executed")
    assert tier_loop.compose_map(queue, Q) is not None
    _claim_exact(queue, Q, owner="w-pp")
    queue.finish(Q, status="executed")

    stage_release.evict(queue, p1, consumer_action_key=P,
                        stage_root=str(tmp_path / "stage"))
    stage_release.evict(queue, q0, consumer_action_key=Q,
                        stage_root=str(tmp_path / "stage"))
    stage_release.evict(queue, q1, consumer_action_key=Q,
                        stage_root=str(tmp_path / "stage"))
    # Per-consumer payload names (#752 shared-publication contract), so no
    # staged entry is shared across windows: every egress releases plainly.
    # Exact conservation: three minted markers, three free, nothing held,
    # nothing decharged, nothing phantom.
    minted = queue.tier_ledger(TIER)
    assert minted.available().get("stage_gib") == 3
    assert minted.capacity().get("stage_gib") == 3
    assert minted.held().get("stage_gib", 0) == 0
    assert not (minted.minted_dir / "dead").exists()


def test_parallel_fit_runs_both(tmp_path: Path) -> None:
    """Six tokens fit two 1+1 windows: all four ranges land concurrently."""
    ctx = _setup_two_consumers(tmp_path, stage_gib=6)
    queue = ctx["queue"]
    tiers = _tiers(tmp_path)

    events = tier_loop.residency_window(queue, tiers=tiers)
    movers = {ctx["aa"]["movers"][0], ctx["aa"]["movers"][1],
              ctx["bb"]["movers"][0], ctx["bb"]["movers"][1]}
    assert _published(events) == movers
    assert _gated(events) == {}

    for mover, consumer, tag, ordinal in (
            (ctx["aa"]["movers"][0], CONSUMER_A, "aa", 0),
            (ctx["aa"]["movers"][1], CONSUMER_A, "aa", 1),
            (ctx["bb"]["movers"][0], CONSUMER_B, "bb", 0),
            (ctx["bb"]["movers"][1], CONSUMER_B, "bb", 1)):
        _land(queue, tmp_path, ctx[tag]["manifest_path"], ctx[tag]["digest"],
              mover, consumer, ordinal * SPAN, (ordinal + 1) * SPAN,
              ctx[tag]["payloads"][f"{tag}-p{ordinal}.bin"], owner="w-pp")
    for consumer in (CONSUMER_A, CONSUMER_B):
        assert tier_loop.compose_map(queue, consumer) is not None
        _claim_exact(queue, consumer, owner="w-pp")
        queue.finish(consumer, status="executed")
    assert queue.tier_ledger(TIER).available().get("stage_gib") == 2


def test_fence_blocks_stealer_and_releases_once(tmp_path: Path) -> None:
    """Exact-fit fence: a 2-token stealer waits, a 1-token one runs."""
    ctx = _setup_two_consumers(tmp_path, stage_gib=3)
    queue = ctx["queue"]
    tiers = _tiers(tmp_path)
    ledger = queue.tier_ledger(TIER)

    first = tier_loop.residency_window(queue, tiers=tiers)
    first_consumer = (CONSUMER_A
                      if ctx["aa"]["movers"][0] in _published(first)
                      else CONSUMER_B)
    assert _published(first) == set(ctx["aa"]["movers"]) or \
        _published(first) == set(ctx["bb"]["movers"])
    ptag = "aa" if first_consumer == CONSUMER_A else "bb"
    P = first_consumer
    p0, p1 = ctx[ptag]["movers"]
    _land(queue, tmp_path, ctx[ptag]["manifest_path"], ctx[ptag]["digest"],
          p0, P, 0, SPAN, ctx[ptag]["payloads"][f"{ptag}-p0.bin"], owner="w-fence")
    for _ in range(4):
        tier_loop.residency_window(queue, tiers=tiers)
        record = queue.read_funding(p1, TIER)
        if record is not None and record.get("state") == "transferring":
            break
    _await_funded(queue, p1)
    # Fence for the protected next is held: free is 1 of 3.
    assert ledger.available().get("stage_gib") == 1

    big = _hexkey("fence-big")
    _stealer_row(queue, big, 2)
    assert queue.claim(tags=["dl380g10"], owner="w-fence",
                       ready=_ready_only(queue, big)) is None
    assert queue.item_path(pool.READY, big).exists()
    small = _hexkey("fence-small")
    _stealer_row(queue, small, 1)
    got = queue.claim(tags=["dl380g10"], owner="w-fence",
                      ready=_ready_only(queue, small))
    assert got is not None and got["action_key"] == small
    queue.finish(small, status="executed")

    # The protected next still claims with no new money after the probe.
    _claim_exact(queue, p1, owner="w-fence")
    assert ledger.available().get("stage_gib") == 1
    queue.record_move(p1, stage_move.move(
        _move_args(queue, tmp_path, ctx[ptag]["manifest_path"],
                   ctx[ptag]["digest"], p1, P, SPAN, 2 * SPAN)))
    queue.finish(p1, status="executed")

    # Owner-path release returns the room exactly once; the waiter proceeds.
    assert tier_loop.compose_map(queue, P) is not None
    _claim_exact(queue, P, owner="w-fence")
    queue.finish(P, status="executed")
    stage_release.evict(queue, p0, consumer_action_key=P,
                        stage_root=str(tmp_path / "stage"))
    assert ledger.available().get("stage_gib") == 2
    stage_release.evict(queue, p1, consumer_action_key=P,
                        stage_root=str(tmp_path / "stage"))
    assert ledger.available().get("stage_gib") == 3
    got = queue.claim(tags=["dl380g10"], owner="w-fence")
    assert got is not None and got["action_key"] == big
    queue.finish(big, status="executed")
    assert ledger.available().get("stage_gib") == 3


def test_cancellation_frees_fence_keeps_pin(tmp_path: Path) -> None:
    """Withdrawing a consumer releases its grant boundedly; pins stay."""
    ctx = _setup_two_consumers(tmp_path, stage_gib=3)
    queue = ctx["queue"]
    tiers = _tiers(tmp_path)
    ledger = queue.tier_ledger(TIER)
    p0, p1 = ctx["aa"]["movers"]

    tier_loop.residency_window(queue, tiers=tiers)
    _land(queue, tmp_path, ctx["aa"]["manifest_path"], ctx["aa"]["digest"],
          p0, CONSUMER_A, 0, SPAN, ctx["aa"]["payloads"]["aa-p0.bin"],
          owner="w-cancel")
    for _ in range(4):
        tier_loop.residency_window(queue, tiers=tiers)
        record = queue.read_funding(p1, TIER)
        if record is not None and record.get("state") == "transferring":
            break
    _await_funded(queue, p1)
    assert ledger.available().get("stage_gib") == 1

    queue.withdraw(CONSUMER_A, reason="operator-cancel-test")
    tier_loop.withdraw_dead_consumer_movers(queue)
    events = tier_loop.residency_window(queue, tiers=tiers)
    # The mover-held fence came home exactly once through the terminal
    # branch; the staged pin is untouched, the queued mover withdrawn. B is
    # admitted in the same window and fences its own advance (#832), so one
    # free token is B's legitimate reservation, not A's leaked fence.
    assert ledger.available().get("stage_gib") == 1
    assert int(ledger.holder_tokens(p0).get("stage_gib", 0)) == 1
    assert int(ledger.holder_tokens(p1).get("stage_gib", 0)) == 0
    assert not queue.item_path(pool.READY, p1).exists()
    record = queue.read_funding(p1, TIER)
    assert record is not None and record["state"] == "released"
    assert sum(int(e.get("released_gib", 0)) for e in events
               if e.get("event") == "advance-released") == 1
    assert ledger.holder_tokens(window_credit.grant_key(
        CONSUMER_B, TIER, "mover_row", "phase-1", None)) == {"stage_gib": 1}


def test_pin_survives_window_cycles_until_egress(tmp_path: Path) -> None:
    """Window/settle cycles never erode a landed pin."""
    ctx = _setup_two_consumers(tmp_path, stage_gib=3)
    queue = ctx["queue"]
    tiers = _tiers(tmp_path)
    ledger = queue.tier_ledger(TIER)
    p0 = ctx["aa"]["movers"][0]

    tier_loop.residency_window(queue, tiers=tiers)
    _land(queue, tmp_path, ctx["aa"]["manifest_path"], ctx["aa"]["digest"],
          p0, CONSUMER_A, 0, SPAN, ctx["aa"]["payloads"]["aa-p0.bin"],
          owner="w-pin")
    p1 = ctx["aa"]["movers"][1]
    for _ in range(3):
        tier_loop.residency_window(queue, tiers=tiers)
        # Pin intact and the next advance fenced beside it: exact split.
        assert int(ledger.holder_tokens(p0).get("stage_gib", 0)) == 1
        record = queue.read_funding(p1, TIER)
        assert record is not None and record.get("state") in (
            "reserved", "transferring")
        assert ledger.available().get("stage_gib") == 1
    stage_release.evict(queue, p0, consumer_action_key=CONSUMER_A,
                        stage_root=str(tmp_path / "stage"))
    assert ledger.available().get("stage_gib") == 2


def _ram_setup(tmp_path: Path, *, stage_gib: int, ram_gib: int):
    """Queue with SSD+RAM tiers, epoch provisioned, one dual-leg consumer."""
    queue = _queue(tmp_path, stage_gib=stage_gib, ram_gib=ram_gib)
    ram = tmp_path / "ram"
    ram.mkdir(exist_ok=True)
    epoch = storage_tiers.ensure_ram_epoch(ram, host="dl380g10")
    assert epoch is not None
    queue.announce_tier({
        "schema": storage_tiers.TIER_RECORD_SCHEMA_V1, "tier": "ram",
        "tier_id": RAM_TIER, "host": "dl380g10",
        "mountpoint": str(ram), "epoch": str(epoch["epoch"]),
        "capacity_bytes": ram_gib * GIB,
    })
    mount, payloads = _corpus(tmp_path, "ram")
    manifest = _manifest(mount, payloads)
    digest = "6" * 64
    manifest_path = tmp_path / "manifest-ram.json"
    manifest_path.write_text(json.dumps(manifest))
    built = [{
        "name": "phase-0",
        "start_bytes": 0, "end_bytes": SPAN, "stage_gib": 1,
        "mover_row": {
            **_row(_hexkey("wpp-ram-0"), {STAGE_KIND: 1, "mem_gb": 1}, queue),
            "residency": {
                "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                "manifest_sha256": digest, "manifest_bytes": 2 * SPAN,
                "range_start_bytes": 0, "range_end_bytes": SPAN},
        },
        "egress_row": _row(_hexkey("wpp-ram-e0"), {"mem_gb": 1}, queue),
        "ram_mover_row": {
            **_row(_hexkey("wpp-ramp-0"), {RAM_KIND: 1, "mem_gb": 1}, queue),
            "residency": {
                "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": RAM_TIER,
                "manifest_sha256": digest, "manifest_bytes": 2 * SPAN,
                "range_start_bytes": 0, "range_end_bytes": SPAN},
        },
        "ram_egress_row": _row(_hexkey("wpp-ramre-0"), {"mem_gb": 1}, queue),
    }]
    plan = residency_plan.build_plan(
        # The root the movers below actually copy to: a plan that named a
        # different stage than its own mover writes would vouch for bytes
        # nobody can match to it, which is what readiness now checks (#759).
        consumer_action_key=CONSUMER_A, tier_id=TIER,
        stage_root=str(tmp_path / "stage"),
        manifest_sha256=digest, manifest_bytes=2 * SPAN, phases=built,
        ram_tier_id=RAM_TIER)
    _publish_consumer(queue, plan, CONSUMER_A)
    m0 = str(built[0]["mover_row"]["action_key"])
    r0 = str(built[0]["ram_mover_row"]["action_key"])
    return {"queue": queue, "plan": plan, "mount": mount,
            "payloads": payloads, "manifest_path": manifest_path,
            "digest": digest, "m0": m0, "r0": r0, "ram": ram}


def _ram_tiers(tmp_path: Path) -> dict[str, dict[str, object]]:
    tiers = _tiers(tmp_path)
    ram = tmp_path / "ram"
    record = None
    announced = storage_tiers.read_ram_epoch(ram)
    return {**tiers, RAM_TIER: {
        "tier_id": RAM_TIER, "tier": "ram", "mountpoint": str(ram),
        "epoch": str(announced["epoch"]) if announced else ""}}


def _promote_args(queue: pool.PoolQueue, tmp_path: Path, manifest_path: Path,
                  digest: str, mover: str, consumer: str, start: int, end: int):
    import ram_promote
    return ram_promote.build_parser().parse_args([
        "--pool-root", str(queue.root),
        "--action-key", mover,
        "--consumer-action-key", consumer,
        "--tier-id", RAM_TIER,
        "--ram-root", str(tmp_path / "ram"),
        "--source-stage-root", str(tmp_path / "stage"),
        "--manifest-sha256", digest,
        "--range-start-bytes", str(start),
        "--range-end-bytes", str(end),
        "--manifest", str(manifest_path),
        "--residency-root", str(queue.root / pool.RESIDENCY),
    ])


def test_ram_leg_promotes_behind_landed_stage(tmp_path: Path) -> None:
    """SSD lands, RAM promotes real bytes under epoch; joint gate stalls next."""
    import ram_promote
    ctx = _ram_setup(tmp_path, stage_gib=3, ram_gib=1)
    queue = ctx["queue"]
    tiers = _ram_tiers(tmp_path)
    ram_ledger = queue.tier_ledger(RAM_TIER)

    tier_loop.residency_window(queue, tiers=tiers)
    _land(queue, tmp_path, ctx["manifest_path"], ctx["digest"], ctx["m0"],
          CONSUMER_A, 0, SPAN, ctx["payloads"]["ram-p0.bin"], owner="w-ram")
    events = tier_loop.ram_residency_window(queue, tiers=tiers)
    assert queue.item_path(pool.READY, ctx["r0"]).exists(), events

    _claim_exact(queue, ctx["r0"], owner="w-ram")
    receipt = ram_promote.promote(
        _promote_args(queue, tmp_path, ctx["manifest_path"], ctx["digest"],
                      ctx["r0"], CONSUMER_A, 0, SPAN))
    assert receipt["complete"] is True, receipt
    assert int(receipt["bytes_staged"]) == SPAN
    queue.record_move(ctx["r0"], receipt)
    queue.finish(ctx["r0"], status="executed")
    assert (tmp_path / "ram" / _staged_name("ram-p0.bin")).read_bytes() == \
        ctx["payloads"]["ram-p0.bin"]
    assert int(ram_ledger.holder_tokens(ctx["r0"]).get("ram_gib", 0)) == 1

    # A second promotion does not fit beside the pinned one: typed stall.
    # Land B's stage first (real bytes), then the ram joint gate refuses.
    mount_b, payloads_b = _corpus(tmp_path, "ramb")
    manifest_b = _manifest(mount_b, payloads_b)
    manifest_b_path = tmp_path / "manifest-ramb.json"
    manifest_b_path.write_text(json.dumps(manifest_b))
    digest_b = "5" * 64
    plan_b = residency_plan.build_plan(
        consumer_action_key=CONSUMER_B, tier_id=TIER,
        stage_root="/stage/prewarm", manifest_sha256=digest_b,
        manifest_bytes=2 * SPAN, phases=[{
            "name": "phase-0", "start_bytes": 0, "end_bytes": SPAN,
            "stage_gib": 1,
            "mover_row": {
                **_row(_hexkey("wpp-ram-b0"), {STAGE_KIND: 1, "mem_gb": 1},
                       queue),
                "residency": {
                    "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                    "manifest_sha256": digest_b, "manifest_bytes": 2 * SPAN,
                    "range_start_bytes": 0, "range_end_bytes": SPAN},
            },
            "egress_row": _row(_hexkey("wpp-ram-be0"), {"mem_gb": 1}, queue),
            "ram_mover_row": {
                **_row(_hexkey("wpp-ramp-b0"), {RAM_KIND: 1, "mem_gb": 1},
                       queue),
                "residency": {
                    "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": RAM_TIER,
                    "manifest_sha256": digest_b, "manifest_bytes": 2 * SPAN,
                    "range_start_bytes": 0, "range_end_bytes": SPAN},
            },
            "ram_egress_row": _row(_hexkey("wpp-ramre-b0"),
                                   {"mem_gb": 1}, queue),
        }], ram_tier_id=RAM_TIER)
    residency_plan.freeze(queue, plan_b)
    queue.publish(action_key=CONSUMER_B, cas_root=queue.root / "cas",
                  checkout_root=queue.root / "co",
                  worker_script=queue.root / "worker.py",
                  resources={"cpu": 1, "mem_gb": 1},
                  residency={"schema": pool.RESIDENCY_SCHEMA_V1,
                             "tier_id": TIER, "manifest_sha256": digest_b,
                             "manifest_bytes": 2 * SPAN,
                             "leads": residency_plan.leads_for(plan_b)})
    n0 = _hexkey("wpp-ram-b0")
    r1 = _hexkey("wpp-ramp-b0")
    stage_events = tier_loop.residency_window(queue, tiers=tiers)
    assert queue.item_path(pool.READY, n0).exists(), [
        e for e in stage_events if e.get("event") in (
            "mover-published", "window-gated")]
    _claim_exact(queue, n0, owner="w-ram")
    n0_receipt = stage_move.move(_move_args(
        queue, tmp_path, manifest_b_path, digest_b, n0, CONSUMER_B, 0, SPAN))
    assert n0_receipt["complete"] is True
    queue.record_move(n0, n0_receipt)
    queue.finish(n0, status="executed")
    assert (tmp_path / "stage" / _staged_name("ramb-p0.bin")).read_bytes() \
        == payloads_b["ramb-p0.bin"]

    ram_events = tier_loop.ram_residency_window(queue, tiers=tiers)
    stalled = {str(e["consumer"]): e for e in ram_events
               if e.get("event") == "ram-window-gated"}
    assert CONSUMER_B in stalled, [e for e in ram_events
                                   if "gated" in str(e.get("event"))]
    assert stalled[CONSUMER_B]["permanent"] is False
    assert not queue.item_path(pool.READY, r1).exists()
    assert int(ram_ledger.holder_tokens(ctx["r0"]).get("ram_gib", 0)) == 1


def test_gate_permanent_joint_oversize(tmp_path: Path) -> None:
    """Pure gate: unmeetable minima are permanent; encumbered ones wait."""
    # Nothing held, queued, owed, or protected: cur+next can never fit.
    over = window_credit.gate_newcomer(
        held_gib=0, ready_gib=0, output_gib=0, capacity_gib=5,
        cur_min_gib=3, next_min_gib=3, existing_min_next_gib=0)
    assert over == {"admit": False, "reason": "joint-fit-oversize",
                    "permanent": True,
                    "output_note": window_credit.OUTPUT_UNENFORCED_NOTE}
    # Same shape with a live obligation elsewhere: transient, it may free.
    stall = window_credit.gate_newcomer(
        held_gib=2, ready_gib=0, output_gib=0, capacity_gib=5,
        cur_min_gib=3, next_min_gib=3, existing_min_next_gib=0)
    assert stall["admit"] is False and stall["permanent"] is False
    assert stall["reason"] == "joint-fit-stall"
    # Feasible minima still admit; finals need no future credit.
    assert window_credit.gate_newcomer(
        held_gib=0, ready_gib=0, output_gib=0, capacity_gib=5,
        cur_min_gib=2, next_min_gib=2,
        existing_min_next_gib=0)["admit"] is True
    assert window_credit.gate_newcomer(
        held_gib=0, ready_gib=0, output_gib=0, capacity_gib=5,
        cur_min_gib=5, next_min_gib=None,
        existing_min_next_gib=0)["admit"] is True
    assert window_credit.gate_newcomer(
        held_gib=0, ready_gib=0, output_gib=0, capacity_gib=None,
        cur_min_gib=1, next_min_gib=1,
        existing_min_next_gib=0)["reason"] == "advance-deferred-unknown-evidence"


def test_unknown_ready_defers_all_publication(tmp_path: Path) -> None:
    """An unreadable ready scan publishes nothing and says unknown."""
    ctx = _setup_two_consumers(tmp_path, stage_gib=3)
    queue = ctx["queue"]
    tiers = _tiers(tmp_path)
    ready_dir = queue.dir(pool.READY)
    ready_dir.chmod(0o000)
    try:
        events = tier_loop.residency_window(queue, tiers=tiers)
    finally:
        ready_dir.chmod(0o755)
    assert [e for e in events if e.get("event") == "mover-published"] == []
    assert [e for e in events if e.get("event") == "window-gated"] == []
    unknown = [e for e in events
               if e.get("event") == "advance-deferred-unknown-evidence"]
    assert unknown, "ready outage must be named, not silent"
    assert queue.tier_ledger(TIER).available().get("stage_gib") == 3


def test_unknown_plan_defers_only_its_consumer(tmp_path: Path) -> None:
    """A torn plan stops its consumer; the healthy window still publishes."""
    ctx = _setup_two_consumers(tmp_path, stage_gib=3)
    queue = ctx["queue"]
    tiers = _tiers(tmp_path)
    plan_path = queue.residency_plan_path(CONSUMER_B)
    original = plan_path.read_bytes()
    plan_path.chmod(0o644)
    plan_path.write_text("{torn", encoding="utf-8")
    try:
        events = tier_loop.residency_window(queue, tiers=tiers)
    finally:
        plan_path.write_bytes(original)
    unknown = [e for e in events
               if e.get("event") == "advance-deferred-unknown-evidence"
               and e.get("consumer") == CONSUMER_B]
    assert unknown, "torn plan must defer its consumer loudly"
    # Nobody publishes for a consumer the census cannot see.
    assert CONSUMER_B not in {c for c, _p in
                              [(e.get("consumer"), e.get("phase"))
                               for e in events
                               if e.get("event") == "mover-published"]}
    # The healthy consumer is unaffected.
    assert CONSUMER_A in {e.get("consumer") for e in events
                          if e.get("event") == "mover-published"}


def test_unknown_ledger_defers_tier_keeps_obligations(tmp_path: Path) -> None:
    """An unreadable tier ledger publishes nothing and frees nothing."""
    ctx = _setup_two_consumers(tmp_path, stage_gib=3)
    queue = ctx["queue"]
    tiers = _tiers(tmp_path)
    tier_dir = queue.tier_ledger(TIER).base
    tier_dir.chmod(0o000)
    try:
        events = tier_loop.residency_window(queue, tiers=tiers)
    finally:
        tier_dir.chmod(0o755)
    assert [e for e in events if e.get("event") == "mover-published"] == []
    assert [e for e in events
            if e.get("event") == "advance-deferred-unknown-evidence"], \
        "ledger outage must be named"
    assert [e for e in events if e.get("event") == "advance-released"] == []


def test_crash_prefix_split_transfer_completes(tmp_path: Path) -> None:
    """Tokens split grant/mover by a crash land whole via the normal tick."""
    import os as _os
    queue = _queue(tmp_path, stage_gib=3)
    ledger = queue.tier_ledger(TIER)
    mover = _hexkey("split-mover")
    consumer = _hexkey("split-consumer")
    plan = _plan(queue, consumer, "8" * 64, tag="split")
    row = dict(_row(mover, {STAGE_KIND: 1, "mem_gb": 1}, queue),
               residency={
                   "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                   "manifest_sha256": "8" * 64, "manifest_bytes": 2 * SPAN,
                   "range_start_bytes": 0, "range_end_bytes": SPAN})
    queue.publish(action_key=mover, cas_root=row["cas_root"],
                  checkout_root=row["checkout_root"],
                  worker_script=row["worker_script"], tags=["dl380g10"],
                  resources=row["resources"], residency=row["residency"])
    live = pool.read_queue_record(queue.item_path(pool.READY, mover))
    grant = window_credit.grant_key(consumer, TIER, "mover_row", "phase-0")
    # Two-token fence taken directly: the record below is filed by hand to
    # stage the exact crash residue (split tokens, unmarked record).
    assert ledger.acquire(grant, {"stage_gib": 2}) is True
    names = sorted(path.name for path in (ledger.held_dir / grant).glob("*-*"))
    assert len(names) == 2
    # Crash residue: one token moved, the record still reserved.
    (ledger.held_dir / mover).mkdir(parents=True, exist_ok=True)
    _os.rename(str(ledger.held_dir / grant / names[0]),
               str(ledger.held_dir / mover / names[0]))
    record = {"schema": pool.TIER_FUNDING_SCHEMA_V1, "tier_id": TIER,
              "consumer_action_key": consumer,
              "plan_sha256": residency_plan.plan_sha256(plan),
              "mover_action_key": mover, "range_start_bytes": 0,
              "range_end_bytes": SPAN, "kind": "stage_gib", "tokens": names,
              "generation": "a" * 32, "state": "reserved",
              "unix": 1.0, "published_unix": float(live["published_unix"])}
    queue._write_funding_locked(record, expect_generation=None)
    protection = {"protected": {(consumer, TIER): {
        "grant": grant, "mover": mover, "need_gib": 2, "phase": "phase-0",
        "tier_id": TIER, "kind": "stage_gib", "leg": "mover_row"}}}
    events = tier_loop._settle_protected(queue, protection)
    # The remainder completed without touching free: 2 under the mover.
    assert int(ledger.holder_tokens(mover).get("stage_gib", 0)) == 2
    assert int(ledger.holder_tokens(grant).get("stage_gib", 0)) == 0
    assert ledger.available().get("stage_gib") == 1
    assert queue.read_funding(mover, TIER)["state"] == "transferring"
    assert [e for e in events if e.get("event") == "advance-handed-off"]


def test_republish_recovery_through_tick(tmp_path: Path) -> None:
    """A republished row re-fences fresh; the stale fence never double-holds."""
    queue = _queue(tmp_path, stage_gib=3)
    ledger = queue.tier_ledger(TIER)
    consumer = _hexkey("repub-consumer")
    plan = _plan(queue, consumer, "8" * 64, tag="repub")
    residency_plan.freeze(queue, plan)
    phases = plan["phases"]
    assert isinstance(phases, list)
    mover_row = dict(phases[0]["mover_row"])
    mover = str(mover_row["action_key"])
    queue.publish(action_key=mover, cas_root=mover_row["cas_root"],
                  checkout_root=mover_row["checkout_root"],
                  worker_script=mover_row["worker_script"], tags=["dl380g10"],
                  resources=mover_row["resources"],
                  residency=mover_row["residency"])
    live = pool.read_queue_record(queue.item_path(pool.READY, mover))
    grant = window_credit.grant_key(consumer, TIER, "mover_row", "phase-0")
    fields = {"consumer_action_key": consumer,
              "plan_sha256": residency_plan.plan_sha256(plan),
              "mover_action_key": mover, "range_start_bytes": 0,
              "range_end_bytes": SPAN, "kind": "stage_gib",
              "published_unix": float(live["published_unix"])}
    assert queue.reserve_fence(TIER, grant, fields, 1) is True
    old = queue.read_funding(mover, TIER)
    old_gen = str(old["generation"])
    assert queue.transfer_fence(TIER, grant, mover) == 1
    assert queue.advance_funding_state(
        mover, TIER, expect="reserved", advance_to="transferring",
        generation=old_gen) is True
    # Unpublish without terminal (row withdrawn by hand): fence stranded.
    queue.item_path(pool.READY, mover).unlink()
    assert int(ledger.holder_tokens(mover).get("stage_gib", 0)) == 1
    # Republish the same key: a new publication never inherits old credit.
    # The stale fence is reclaimed exactly and a fresh one takes its place.
    queue.publish(action_key=mover, cas_root=mover_row["cas_root"],
                  checkout_root=mover_row["checkout_root"],
                  worker_script=mover_row["worker_script"], tags=["dl380g10"],
                  resources=mover_row["resources"],
                  residency=mover_row["residency"])
    live2 = pool.read_queue_record(queue.item_path(pool.READY, mover))
    assert float(live2["published_unix"]) != float(live["published_unix"])
    fields["published_unix"] = float(live2["published_unix"])
    assert queue.reserve_fence(TIER, grant, fields, 1) is True
    new = queue.read_funding(mover, TIER)
    assert new is not None and new["state"] == "reserved"
    assert str(new["generation"]) != old_gen
    # No double hold: exactly the live fence is held, free is exact.
    assert ledger.available().get("stage_gib") == 2
    assert queue.transfer_fence(TIER, grant, mover) == 1
    assert queue.advance_funding_state(
        mover, TIER, expect="reserved", advance_to="transferring",
        generation=str(new["generation"])) is True
    got = queue.claim(tags=["dl380g10"], owner="w-repub",
                      ready=_ready_only(queue, mover))
    assert got is not None and got["action_key"] == mover
    assert int(ledger.holder_tokens(mover).get("stage_gib", 0)) == 1
    queue.finish(mover, status="executed")
    assert ledger.available().get("stage_gib") == 3


def test_published_nexts_leave_no_unfunded_window(tmp_path: Path) -> None:
    """Post-cycle invariant: every wanted next is funded or stealer-visible.

    After any window cycle, each published unclaimed mover the window still
    wants holds a transferring fence for its exact demand -- or the cycle
    left the room visibly free, in which case a stealer may fairly take it
    and the window re-fences next cycle.  What must never happen is a
    published wanted next with neither fence nor free room behind it.
    """
    ctx = _setup_two_consumers(tmp_path, stage_gib=3)
    queue = ctx["queue"]
    tiers = _tiers(tmp_path)
    ledger = queue.tier_ledger(TIER)
    for _ in range(3):
        tier_loop.residency_window(queue, tiers=tiers)
        for tag in ("aa", "bb"):
            for ordinal in (0, 1):
                mover = ctx[tag]["movers"][ordinal]
                if not queue.item_path(pool.READY, mover).exists():
                    continue
                if queue.read_funding(mover, TIER) is not None:
                    record = queue.read_funding(mover, TIER)
                    assert record is not None
                    assert record.get("state") == "transferring"
                    names = record.get("tokens")
                    held = {path.name for path in
                            (ledger.held_dir / mover).glob("*-*")}
                    assert isinstance(names, list) and names and all(
                        str(name) in held for name in names)
                else:
                    # Unfunded and exposed: the room must actually be there.
                    assert ledger.available().get("stage_gib") >= 1, (
                        f"{mover[:12]} unfunded with no free room behind it")


def test_competing_claim_race_stays_exact(tmp_path: Path) -> None:
    """A stealer racing the fence cycle by thread still ends exact."""
    import threading
    ctx = _setup_two_consumers(tmp_path, stage_gib=3)
    queue = ctx["queue"]
    tiers = _tiers(tmp_path)
    ledger = queue.tier_ledger(TIER)
    p0 = ctx["aa"]["movers"][0]
    p1 = ctx["aa"]["movers"][1]

    tier_loop.residency_window(queue, tiers=tiers)
    _land(queue, tmp_path, ctx["aa"]["manifest_path"], ctx["aa"]["digest"],
          p0, CONSUMER_A, 0, SPAN, ctx["aa"]["payloads"]["aa-p0.bin"],
          owner="w-race")
    stealer = _hexkey("race-stealer")
    span2 = 2 * SPAN
    queue.publish(
        action_key=stealer, cas_root=queue.root / "cas",
        checkout_root=queue.root / "co", worker_script=queue.root / "worker.py",
        tags=["dl380g10"], resources={STAGE_KIND: 2},
        residency={"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                   "manifest_sha256": "7" * 64, "manifest_bytes": span2,
                   "range_start_bytes": 0, "range_end_bytes": span2})
    outcome: dict[str, object] = {}

    def steal() -> None:
        try:
            got = queue.claim(tags=["dl380g10"], owner="w-race-steal",
                              ready=_ready_only(queue, stealer))
        except Exception as exc:  # never fail the test on transport noise
            outcome["error"] = repr(exc)
        else:
            outcome["claim"] = got["action_key"] if got else None

    racer = threading.Thread(target=steal)
    racer.start()
    for _ in range(4):
        tier_loop.residency_window(queue, tiers=tiers)
    racer.join(60)
    assert not racer.is_alive()
    assert "error" not in outcome, outcome.get("error")
    # Either order ends exact: stealer denied and the window fenced, or the
    # stealer won the free room first and the window re-fences after it.
    if outcome.get("claim") == stealer:
        queue.finish(stealer, status="executed")
    else:
        assert outcome.get("claim") is None
        assert queue.item_path(pool.READY, stealer).exists()
    for _ in range(6):
        tier_loop.residency_window(queue, tiers=tiers)
        record = queue.read_funding(p1, TIER)
        if record is not None and record.get("state") == "transferring":
            break
    _await_funded(queue, p1)
    before = ledger.available().get("stage_gib")
    _claim_exact(queue, p1, owner="w-race")
    assert ledger.available().get("stage_gib") == before
    queue.record_move(p1, stage_move.move(
        _move_args(queue, tmp_path, ctx["aa"]["manifest_path"],
                   ctx["aa"]["digest"], p1, CONSUMER_A, SPAN, 2 * SPAN)))
    queue.finish(p1, status="executed")
    # Pins plus fences add up; nothing leaked, nothing doubled.
    total_held = sum(
        int(tokens.get("stage_gib", 0))
        for tokens in (ledger.holder_tokens(holder)
                       for holder in ledger.held_keys()))
    assert total_held + ledger.available().get("stage_gib") == 3


def test_cycle_drives_window_with_real_moves(tmp_path: Path) -> None:
    """The production tick (simulated discovery only) advances a window."""
    ctx = _setup_two_consumers(tmp_path, stage_gib=3)
    queue = ctx["queue"]
    ledger = queue.tier_ledger(TIER)

    def discover(*, host, source_pool, fill_records, now, ram_policy,
                 worker_mem_gb):
        assert host and source_pool is not None
        return {TIER: {"tier_id": TIER, "tier": "stage", "host": host,
                       "capacity_bytes": 3 * GIB}}

    receipts = tier_loop.ReceiptCache()
    announced = tier_loop.cycle(
        queue, host="testbox", source_pool="testpool", receipts=receipts,
        discover=discover)
    assert {str(record["tier_id"]) for record in announced} == {TIER}
    ready = {str(item["action_key"]) for item in queue.ready_items()
             if str(item["action_key"]) in (
                 ctx["aa"]["movers"] + ctx["bb"]["movers"])}
    assert ready == set(ctx["aa"]["movers"]) or ready == set(ctx["bb"]["movers"])
    winner = CONSUMER_A if ctx["aa"]["movers"][0] in ready else CONSUMER_B
    wtag = "aa" if winner == CONSUMER_A else "bb"
    w0, w1 = ctx[wtag]["movers"]
    # The blind advance take is already held: free 2 of 3, no publish gap.
    assert queue.tier_ledger(TIER).available().get("stage_gib") == 2
    record = queue.read_funding(w1, TIER)
    assert record is not None and record.get("state") in (
        "reserved", "transferring")

    _land(queue, tmp_path, ctx[wtag]["manifest_path"], ctx[wtag]["digest"],
          w0, winner, 0, SPAN, ctx[wtag]["payloads"][f"{wtag}-p0.bin"],
          owner="w-cycle")
    tier_loop.cycle(queue, host="testbox", source_pool="testpool",
                    receipts=receipts, discover=discover)
    for _ in range(4):
        tier_loop.cycle(queue, host="testbox", source_pool="testpool",
                        receipts=receipts, discover=discover)
        record = queue.read_funding(w1, TIER)
        if record is not None and record.get("state") == "transferring":
            break
    _await_funded(queue, w1)
    _claim_exact(queue, w1, owner="w-cycle")
    receipt = stage_move.move(
        _move_args(queue, tmp_path, ctx[wtag]["manifest_path"],
                   ctx[wtag]["digest"], w1, winner, SPAN, 2 * SPAN))
    assert receipt["complete"] is True
    queue.record_move(w1, receipt)
    queue.finish(w1, status="executed")
    assert int(ledger.holder_tokens(w1).get("stage_gib", 0)) == 1
    assert ledger.available().get("stage_gib") == 1


def test_two_fitting_windows_exact_each_once(tmp_path: Path) -> None:
    """C=6: two cur+next windows admit with each token counted once.

    After every actual take and every publish prefix the ledger adds up:
    blind takes move planned nexts to held (no double), both windows
    publish only with their advances retained, and a 5-token stealer is
    denied while the 2-token advance room stays held.  One window then
    runs to completion with real bytes while the other's fence stays
    exact.
    """
    ctx = _setup_two_consumers(tmp_path, stage_gib=6)
    queue = ctx["queue"]
    tiers = _tiers(tmp_path)
    ledger = queue.tier_ledger(TIER)
    a0, a1 = ctx["aa"]["movers"]
    b0, b1 = ctx["bb"]["movers"]

    first = tier_loop.residency_window(queue, tiers=tiers)
    # Both windows publish; nothing unfunded, gated, or unknown for A/B.
    assert {a0, b0} <= _published(first)
    assert [e for e in first if e.get("event") == "window-unfunded"] == []
    assert [e for e in first if e.get("event") == "window-unknown"] == []
    assert _gated(first) == {}
    # Exact after the takes: blind-held advances plus free add to capacity.
    grant_a = window_credit.grant_key(CONSUMER_A, TIER, "mover_row", "phase-1")
    grant_b = window_credit.grant_key(CONSUMER_B, TIER, "mover_row", "phase-1")
    total_held = sum(
        int(tokens.get("stage_gib", 0))
        for tokens in (ledger.holder_tokens(h) for h in ledger.held_keys()))
    assert total_held + ledger.available().get("stage_gib") == 6
    assert int(ledger.holder_tokens(grant_a).get("stage_gib", 0)) + \
        int(ledger.holder_tokens(a1).get("stage_gib", 0)) == 1
    assert int(ledger.holder_tokens(grant_b).get("stage_gib", 0)) + \
        int(ledger.holder_tokens(b1).get("stage_gib", 0)) == 1

    # Competing claim: a 5-token stealer cannot take advance room; the
    # published prefix stays funded.
    stealer5 = _hexkey("r3-steal-5")
    _stealer_row(queue, stealer5, 5)
    got = queue.claim(tags=["dl380g10"], owner="w-steal5",
                      ready=_ready_only(queue, stealer5))
    assert got is None
    assert queue.item_path(pool.READY, stealer5).exists()
    total_held = sum(
        int(tokens.get("stage_gib", 0))
        for tokens in (ledger.holder_tokens(h) for h in ledger.held_keys()))
    assert total_held + ledger.available().get("stage_gib") == 6
    # Stealer withdraws: its queued demand must not pollute later windows.
    queue.item_path(pool.READY, stealer5).unlink()

    # One window runs with real bytes; the other's advance stays exact.
    _land(queue, tmp_path, ctx["aa"]["manifest_path"], ctx["aa"]["digest"],
          a0, CONSUMER_A, 0, SPAN, ctx["aa"]["payloads"]["aa-p0.bin"],
          owner="w-tight")
    for _ in range(6):
        tier_loop.residency_window(queue, tiers=tiers)
        record = queue.read_funding(a1, TIER)
        if record is not None and record.get("state") == "transferring":
            break
    _await_funded(queue, a1)
    # A1's advance retained (transferred), B's still held (grant or mover);
    # total exact.
    assert int(ledger.holder_tokens(a1).get("stage_gib", 0)) == 1
    assert int(ledger.holder_tokens(grant_b).get("stage_gib", 0)) + \
        int(ledger.holder_tokens(b1).get("stage_gib", 0)) == 1
    total_held = sum(
        int(tokens.get("stage_gib", 0))
        for tokens in (ledger.holder_tokens(h) for h in ledger.held_keys()))
    assert total_held + ledger.available().get("stage_gib") == 6
    free_before = ledger.available().get("stage_gib")
    _claim_exact(queue, a1, owner="w-tight")
    assert ledger.available().get("stage_gib") == free_before
    receipt = stage_move.move(
        _move_args(queue, tmp_path, ctx["aa"]["manifest_path"],
                   ctx["aa"]["digest"], a1, CONSUMER_A, SPAN, 2 * SPAN))
    assert receipt["complete"] is True
    queue.record_move(a1, receipt)
    queue.finish(a1, status="executed")
    # Pins plus B's fence add up; nothing leaked, nothing doubled.
    total_held = sum(
        int(tokens.get("stage_gib", 0))
        for tokens in (ledger.holder_tokens(h) for h in ledger.held_keys()))
    assert total_held + ledger.available().get("stage_gib") == 6
    assert int(ledger.holder_tokens(grant_b).get("stage_gib", 0)) + \
        int(ledger.holder_tokens(b1).get("stage_gib", 0)) == 1
