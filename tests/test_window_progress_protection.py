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
    """Two 1 MiB source files with distinct bytes, under one mount."""
    mount = tmp_path / f"mnt-{tag}"
    payloads = {
        "p0.bin": hashlib.sha256(f"{tag}-p0".encode()).digest() * (SPAN // 32),
        "p1.bin": hashlib.sha256(f"{tag}-p1".encode()).digest() * (SPAN // 32),
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
    staged = tmp_path / "stage" / f"p{start // SPAN}.bin"
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
    assert gate[gated_consumer]["reason"] == "joint-fit-stall"
    assert gate[gated_consumer]["permanent"] is False
    P, Q = first_consumer, gated_consumer
    ptag = "aa" if P == CONSUMER_A else "bb"
    qtag = "bb" if P == CONSUMER_A else "aa"
    p0, p1 = ctx[ptag]["movers"]
    q0, q1 = ctx[qtag]["movers"]

    # The admitted window runs to durable completion with real bytes.
    _land(queue, tmp_path, ctx[ptag]["manifest_path"], ctx[ptag]["digest"],
          p0, P, 0, SPAN, ctx[ptag]["payloads"]["p0.bin"], owner="w-pp")
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
    assert (tmp_path / "stage" / "p1.bin").read_bytes() == \
        ctx[ptag]["payloads"]["p1.bin"]
    assert tier_loop.compose_map(queue, P) is not None
    _claim_exact(queue, P, owner="w-pp")
    queue.finish(P, status="executed")

    # The stalled window is admitted into the freed room and completes.
    stage_release.evict(queue, p0, consumer_action_key=P,
                        stage_root=str(tmp_path / "stage"))
    assert not (tmp_path / "stage" / "p0.bin").exists()
    admitted = False
    for _ in range(4):
        events = tier_loop.residency_window(queue, tiers=tiers)
        if q0 in _published(events):
            admitted = True
            break
    assert admitted, "stalled window never admitted after egress"
    assert Q not in _gated(tier_loop.residency_window(queue, tiers=tiers))
    _land(queue, tmp_path, ctx[qtag]["manifest_path"], ctx[qtag]["digest"],
          q0, Q, 0, SPAN, ctx[qtag]["payloads"]["p0.bin"], owner="w-pp")
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
    assert ledger.available().get("stage_gib") == 3


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
              ctx[tag]["payloads"][f"p{ordinal}.bin"], owner="w-pp")
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
          p0, P, 0, SPAN, ctx[ptag]["payloads"]["p0.bin"], owner="w-fence")
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
          p0, CONSUMER_A, 0, SPAN, ctx["aa"]["payloads"]["p0.bin"],
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
    # branch; the staged pin is untouched, the queued mover withdrawn.
    assert ledger.available().get("stage_gib") == 2
    assert int(ledger.holder_tokens(p0).get("stage_gib", 0)) == 1
    assert int(ledger.holder_tokens(p1).get("stage_gib", 0)) == 0
    assert not queue.item_path(pool.READY, p1).exists()
    record = queue.read_funding(p1, TIER)
    assert record is not None and record["state"] == "released"
    assert sum(int(e.get("released_gib", 0)) for e in events
               if e.get("event") == "advance-released") == 1


def test_pin_survives_window_cycles_until_egress(tmp_path: Path) -> None:
    """Window/settle cycles never erode a landed pin."""
    ctx = _setup_two_consumers(tmp_path, stage_gib=3)
    queue = ctx["queue"]
    tiers = _tiers(tmp_path)
    ledger = queue.tier_ledger(TIER)
    p0 = ctx["aa"]["movers"][0]

    tier_loop.residency_window(queue, tiers=tiers)
    _land(queue, tmp_path, ctx["aa"]["manifest_path"], ctx["aa"]["digest"],
          p0, CONSUMER_A, 0, SPAN, ctx["aa"]["payloads"]["p0.bin"],
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
        consumer_action_key=CONSUMER_A, tier_id=TIER, stage_root="/stage/prewarm",
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
    return {**tiers, RAM_TIER: {"tier_id": RAM_TIER, "tier": "ram",
                                "mountpoint": str(ram)}}


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
          CONSUMER_A, 0, SPAN, ctx["payloads"]["p0.bin"], owner="w-ram")
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
    assert (tmp_path / "ram" / "p0.bin").read_bytes() == \
        ctx["payloads"]["p0.bin"]
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
    assert (tmp_path / "stage" / "p0.bin").read_bytes() == payloads_b["p0.bin"]

    ram_events = tier_loop.ram_residency_window(queue, tiers=tiers)
    stalled = {str(e["consumer"]): e for e in ram_events
               if e.get("event") == "ram-window-gated"}
    assert CONSUMER_B in stalled, [e for e in ram_events
                                   if "gated" in str(e.get("event"))]
    assert stalled[CONSUMER_B]["permanent"] is False
    assert not queue.item_path(pool.READY, r1).exists()
    assert int(ram_ledger.holder_tokens(ctx["r0"]).get("ram_gib", 0)) == 1
