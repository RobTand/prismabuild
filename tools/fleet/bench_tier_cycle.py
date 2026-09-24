#!/usr/bin/env python3
"""What one tier-loop cycle costs on a campaign-sized queue, and where (#992).

#944 recorded the tier loop on dl380g10 at about 99% of one core.  The audit
(WS-DA finding 6) named the reads each 5 s cycle repeats whether or not
anything changed: every ``done/``, ``failed/`` and ``withdrawn/`` name, every
residency namespace and fragment, every movement receipt, and every ``ready/``
and ``claimed/`` record, some of them under the stage ownership lock.

This is the hermetic reproduction, shaped like ``bench_stage_adopt.py`` and
reusing its live-shaped residency forest: a synthetic queue and stage root
with the live queue's widths (measured 2026-09-23: 28,943 done, 7,142
failed, 1,877 withdrawn, 6,434 movement receipts, 429 residency namespaces
plus 1,957 produced-output directories), a few live consumers with frozen
plans, landed ranges and a mover in flight, and the real ``tier_loop.cycle``
run against a stub discovery.

The cycles run in a child process, optionally under ``py-spy record`` (not
``--idle``: the loop is single-threaded and its cost is CPU).  The first
cycle is cold: every cache is empty.  The rest are the steady state the live
loop spends its time in.  Each runs through its own wrapper function
(``cold_cycle`` or ``steady_cycle``), so the profile separates them, and each
sample is attributed to the step of ``cycle`` it was spent in -- the callee
frame directly below ``cycle`` -- and to the innermost filesystem or parse
operation it was doing.  The same analysis reads a before and an after tree.

``--dead-owner-pairs`` adds the #1056 shape: pairs of dead owners (a failed
consumer's executed, charged mover with a fragment and a material sidecar)
whose fragments name the same staged files, one ``.pbrange`` parent
directory per entry, as an adoption leaves them once both consumers have
died.  ``--no-range-claims`` adds claimed exports that demand only the
tier's fill rate and seal no range, which taint every ownership census on
the tier while they are claimed (the live ``produced_export.py`` shape).
Each cycle row then carries the stage-lock hold its sweep receipts
recorded.

Run it through PrismaBuild on a GB10, never on the tier host, and never
against the live queue::

    pbrun.py --cwd <checkout> --tag sparky --cpus 2 --demand mem_gb=8 \\
        --priority -10 -- python3 tools/fleet/bench_tier_cycle.py \\
        --work <scratch dir> --out <results dir> --py-spy <path to py-spy>
"""
from __future__ import annotations

import argparse
import collections
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parents[1] / "src"))

from prismabuild import pool, reader_lease, residency_map  # noqa: E402
from prismabuild import residency_plan, storage_tiers  # noqa: E402
import bench_stage_adopt  # noqa: E402
import stage_release  # noqa: E402

TIER = bench_stage_adopt.TIER
HOST = "dl380g10"
STAGE_KIND = f"stage_gib@{TIER}"
GIB = storage_tiers.GIB
PHASE_GIB = 2
MANIFEST = "9" * 64
DIGEST = "a" * 64


def _key(label: str) -> str:
    return hashlib.sha256(f"bench-992:{label}".encode()).hexdigest()


# ---- the queue --------------------------------------------------------------

def _row(queue: pool.PoolQueue, key: str,
         resources: dict[str, int]) -> dict[str, object]:
    return {"action_key": key, "cas_root": str(queue.root / "cas"),
            "checkout_root": str(queue.root / "co"),
            "worker_script": str(queue.root / "worker.py"),
            "tags": [HOST], "resources": resources}


def _plan(queue: pool.PoolQueue, consumer: str, stage: Path, *,
          phases: int) -> dict[str, object]:
    built = []
    for ordinal in range(phases):
        start, end = ordinal * PHASE_GIB * GIB, (ordinal + 1) * PHASE_GIB * GIB
        built.append({
            "name": f"phase-{ordinal}",
            "start_bytes": start, "end_bytes": end, "stage_gib": PHASE_GIB,
            "mover_row": {
                **_row(queue, _key(f"{consumer}:mover{ordinal}"),
                       {STAGE_KIND: PHASE_GIB, "cpu": 1, "mem_gb": 1}),
                "residency": {
                    "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                    "manifest_sha256": MANIFEST, "manifest_bytes": 1 << 34,
                    "range_start_bytes": start, "range_end_bytes": end},
            },
            "egress_row": _row(queue, _key(f"{consumer}:egress{ordinal}"),
                               {"mem_gb": 1}),
        })
    return residency_plan.build_plan(
        consumer_action_key=consumer, tier_id=TIER, stage_root=str(stage),
        manifest_sha256=MANIFEST, manifest_bytes=1 << 34, phases=built)


def _land(queue: pool.PoolQueue, stage: Path, *, consumer: str, mover: str,
          ordinal: int, files: int) -> None:
    """A finished mover's state: tokens, files, fragment, sidecar, receipt."""

    start, end = ordinal * PHASE_GIB * GIB, (ordinal + 1) * PHASE_GIB * GIB
    share, remainder = divmod(end - start, files)
    entries: dict[str, object] = {}
    for index in range(files):
        size = share + (remainder if index == files - 1 else 0)
        path = stage / consumer[:8] / f"phase-{ordinal}" / f"part-{index}.bin"
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "wb") as stream:
            stream.truncate(size)
        entries[residency_map.residency_map_key(
            f"/pool/{consumer[:8]}/phase-{ordinal}/part-{index}.bin", 0)] = {
                "stage_path": str(path), "bytes": size, "offset": 0,
                "sha256": DIGEST}
    assert queue.tier_ledger(TIER).acquire(mover, {"stage_gib": PHASE_GIB})
    root = queue.residency_fragment_root()
    residency_map.write_fragment(root, {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": consumer, "mover_action_key": mover,
        "tier_id": TIER, "stage_root": str(stage), "manifest_sha256": MANIFEST,
        "entries": entries})
    reader_lease.write_material(
        root, consumer_action_key=consumer, mover_action_key=mover,
        tier_id=TIER, stage_root=str(stage), manifest_sha256=MANIFEST,
        generation="a" * 32,
        entries={key: {**dict(mention),  # type: ignore[dict-item]
                       "file_id": reader_lease.stat_identity(
                           str(mention["stage_path"]))}  # type: ignore[index]
                 for key, mention in entries.items()})
    queue.record_move(mover, {
        "consumer_action_key": consumer, "tier_id": TIER,
        "stage_root": str(stage), "manifest_sha256": MANIFEST,
        "range_start_bytes": start, "range_end_bytes": end,
        "range_bytes": end - start, "bytes_staged": end - start,
        "entries_declared": files, "entries_staged": files,
        "complete": True, "seconds": 20.0, "unix": time.time()})


def _terminal(queue: pool.PoolQueue, state: str, count: int) -> None:
    directory = queue.dir(state)
    status = {"done": "executed", "failed": "failed",
              "withdrawn": "withdrawn"}[state]
    for index in range(count):
        key = _key(f"{state}{index}")
        (directory / f"{key}.json").write_text(json.dumps(
            {"schema": pool.POOL_OUTCOME_SCHEMA_V1, "action_key": key,
             "status": status, "published_unix": 1000.0}) + "\n")


def _receipts(queue: pool.PoolQueue, count: int) -> None:
    """Terminal movers' receipts: the fill history every cycle folds."""

    directory = queue.root / pool.MOVERS
    directory.mkdir(parents=True, exist_ok=True)
    now = time.time()
    for index in range(count):
        key = _key(f"receipt{index}")
        seconds = 20.0 + (index % 7)
        body = {
            "schema": pool.POOL_MOVE_SCHEMA_V1, "action_key": key,
            "consumer_action_key": _key(f"receipt-consumer{index // 8}"),
            "tier_id": TIER, "stage_root": "/stage/prewarm",
            "manifest_sha256": MANIFEST, "range_start_bytes": 0,
            "range_end_bytes": PHASE_GIB * GIB, "range_bytes": PHASE_GIB * GIB,
            "bytes_staged": PHASE_GIB * GIB, "entries_declared": 64,
            "entries_staged": 64, "complete": True, "seconds": seconds,
            "unix": now - 86400.0 + index,
            "pool_read_bytes": PHASE_GIB * GIB,
            "mean_pool_read_mb_s": 900.0 + (index % 50),
            "fill_mb_s": 1000, "peak_rss_bytes": 1 << 28, "cpu_seconds": 30.0,
        }
        (directory / f"{key}.json").write_text(json.dumps(body) + "\n")


def _publish_consumer(queue: pool.PoolQueue, consumer: str,
                      plan: dict[str, object]) -> None:
    residency_plan.freeze(queue, plan)
    queue.publish(
        action_key=consumer, cas_root=queue.root / "cas",
        checkout_root=queue.root / "co", worker_script=queue.root / "worker.py",
        tags=[HOST], resources={"cpu": 1, "mem_gb": 1},
        residency={"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                   "manifest_sha256": MANIFEST, "manifest_bytes": 1 << 34,
                   "leads": residency_plan.leads_for(plan)})


def _claim(queue: pool.PoolQueue, key: str) -> None:
    source = queue.item_path(pool.READY, key)
    item = json.loads(source.read_text())
    source.unlink()
    item.update({"action_key": key, "claimed_unix": time.time(),
                 "claimed_by": "bench", "claimed_host": HOST})
    queue.item_path(pool.CLAIMED, key).write_text(json.dumps(item))
    queue.write_lease(key, owner="bench", claim_snapshot=item)


def _dead_action(queue: pool.PoolQueue, key: str, status: str) -> None:
    """One real claim and terminal transition: an attempt-backed outcome."""

    queue.publish(action_key=key, cas_root=str(queue.root / "cas"),
                  checkout_root=str(queue.root / "co"),
                  worker_script=str(queue.root / "worker.py"),
                  resources={"cpu": 1}, max_attempts=1)
    claimed = queue.claim(capacity={"cpu": 4})
    assert claimed is not None and claimed["action_key"] == key, claimed
    queue.finish(key, status=status,
                 detail={"returncode": 0 if status == "executed" else 1})


def build_dead_owners(queue: pool.PoolQueue, stage: Path, *, pairs: int,
                      entries: int) -> dict[str, int]:
    """Co-owned dead owners: the #1056 live shape.

    Each pair shares ``entries`` staged files, each in its own ``.pbrange``
    directory (one parent per entry since #889).  Both owners are a failed
    consumer's executed DONE mover with a move receipt, one stage token, a
    fragment and a material sidecar dating the files, so each is the
    other's co-owner and neither prunes anything.  Built before any other
    ready row, so each claim takes the row just published.
    """

    counts: collections.Counter = collections.Counter()
    if pairs <= 0:
        return {}
    root = queue.residency_fragment_root()
    queue.mint_tier_capacity(TIER, {"stage_gib": 2 * pairs})
    for pair in range(pairs):
        mentions: dict[str, dict[str, object]] = {}
        for index in range(entries):
            directory = (stage / "models" / f"dead-{pair:03d}"
                         / f"model-{index:05d}-of-{entries:05d}"
                           ".safetensors.pbrange")
            directory.mkdir(parents=True, exist_ok=True)
            path = directory / f"0-{4096}"
            with open(path, "wb") as stream:
                stream.truncate(4096)
            key = residency_map.residency_map_key(
                f"/pool/models/dead-{pair:03d}/model-{index:05d}", 0)
            mentions[key] = {"stage_path": str(path), "bytes": 4096,
                             "sha256": DIGEST,
                             "file_id": reader_lease.stat_identity(str(path))}
        fragment_entries = {
            key: {"stage_path": mention["stage_path"], "bytes": 4096,
                  "sha256": DIGEST, "offset": 0}
            for key, mention in mentions.items()}
        for side in ("a", "b"):
            consumer = _key(f"dead{pair}{side}:consumer")
            mover = _key(f"dead{pair}{side}:mover")
            _dead_action(queue, consumer, "failed")
            _dead_action(queue, mover, "executed")
            reader_lease.write_material(
                root, consumer_action_key=consumer, mover_action_key=mover,
                tier_id=TIER, stage_root=str(stage),
                manifest_sha256=MANIFEST, generation="b" * 32,
                entries=mentions)
            residency_map.write_fragment(root, {
                "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
                "consumer_action_key": consumer, "mover_action_key": mover,
                "tier_id": TIER, "stage_root": str(stage),
                "manifest_sha256": MANIFEST, "entries": fragment_entries})
            queue.record_move(mover, {
                "consumer_action_key": consumer, "tier_id": TIER,
                "stage_root": str(stage), "manifest_sha256": MANIFEST,
                "range_start_bytes": 0, "range_end_bytes": 4096 * entries,
                "range_bytes": 4096 * entries,
                "bytes_staged": 4096 * entries,
                "entries_declared": entries, "entries_staged": entries,
                "complete": True, "seconds": 20.0,
                "unix": time.time() - 3600.0})
            assert queue.tier_ledger(TIER).acquire(mover, {"stage_gib": 1})
            counts["dead_owners"] += 1
        counts["dead_owner_entries"] += entries
    return dict(counts)


def build_no_range_claims(queue: pool.PoolQueue, count: int) -> dict[str, int]:
    """Claimed exports that demand only the tier's fill rate (#1056).

    The live ``produced_export.py`` rows: a ``fill_mb_s_pool_side@<tier>``
    demand makes the claim census read them as movement on the tier, and
    their sealed command carries no range, so every ownership census taints
    ("mover seals no range") while one is claimed.
    """

    for index in range(count):
        key = _key(f"no-range{index}")
        queue.publish(**_row(queue, key, {"cpu": 1, "mem_gb": 1}))
        _claim(queue, key)
        claimed = queue.item_path(pool.CLAIMED, key)
        item = json.loads(claimed.read_text())
        item["resources"] = {"cpu": 1, "mem_gb": 1,
                             f"fill_mb_s_pool_side@{TIER}": 510}
        claimed.write_text(json.dumps(item))
        request = queue.root / "cas" / "requests" / key[:2] / f"{key}.json"
        request.parent.mkdir(parents=True, exist_ok=True)
        request.write_text(json.dumps({
            "action_key": key, "inputs": [],
            "params": {"command": [
                "/usr/bin/python3", "tools/fleet/produced_export.py",
                "--pace-mb-s", "510", "--pace-tier", TIER]}}))
    return {"no_range_claims": count} if count else {}


def build_queue(queue: pool.PoolQueue, stage: Path, args) -> dict[str, int]:
    counts = collections.Counter()
    counts.update(bench_stage_adopt.build_forest(
        queue, stage, empty_dirs=args.empty_dirs, small_dirs=args.small_dirs,
        big_fragments=[int(x) for x in args.big_fragments.split(",") if x],
        produced_fragment_dirs=args.produced_fragment_dirs))
    # Shapes built before #1056 name no dead owners and no no-range claims.
    counts.update(build_dead_owners(
        queue, stage, pairs=getattr(args, "dead_owner_pairs", 0),
        entries=getattr(args, "dead_entries", 1680)))
    for state, count in ((pool.DONE, args.done), (pool.FAILED, args.failed),
                         (pool.WITHDRAWN, args.withdrawn)):
        _terminal(queue, state, count)
        counts[state] = count
    _receipts(queue, args.receipts)
    counts["receipts"] = args.receipts
    passes = queue.root / pool.PASSES
    passes.mkdir(parents=True, exist_ok=True)
    for index in range(args.passes):
        (passes / f"{_key(f'passes{index}')}.json").write_text(
            json.dumps({"passes": 1 + index % 5}) + "\n")
    counts["passes"] = args.passes
    queue.mint_tier_capacity(TIER, {"stage_gib": 700})
    for index in range(args.live_consumers):
        consumer = _key(f"live{index}")
        plan = _plan(queue, consumer, stage, phases=args.phases)
        _publish_consumer(queue, consumer, plan)
        movers = residency_plan.mover_keys(plan)
        _land(queue, stage, consumer=consumer, mover=movers[0], ordinal=0,
              files=args.files_per_range)
        if index % 2 == 0:
            _claim(queue, consumer)
        if index == 0 and len(movers) > 1:
            # One copy in flight, as on a live campaign.
            queue.publish(**plan["phases"][1]["mover_row"], recompute=True)
            counts["movers_in_flight"] += 1
        counts["live_consumers"] += 1
    for index in range(args.ready_noise):
        queue.publish(**_row(queue, _key(f"ready{index}"),
                             {"cpu": 1, "mem_gb": 1}))
    for index in range(args.claimed_noise):
        key = _key(f"claimed{index}")
        queue.publish(**_row(queue, key, {"cpu": 1, "mem_gb": 1}))
        _claim(queue, key)
    counts["ready_noise"] = args.ready_noise
    counts["claimed_noise"] = args.claimed_noise
    counts.update(build_no_range_claims(
        queue, getattr(args, "no_range_claims", 0)))
    return dict(counts)


# ---- the cycles -------------------------------------------------------------

def _tier_record(stage: Path) -> dict[str, object]:
    return {"schema": storage_tiers.TIER_RECORD_SCHEMA_V1, "tier_id": TIER,
            "host": HOST, "tier": "stage", "mountpoint": str(stage),
            "capacity_bytes": 700 * GIB}


def run_cycles(args) -> int:
    """The child: cycles against the built queue, timed one by one."""

    import tier_loop

    work = Path(args.work)
    queue = pool.PoolQueue(work / "pb-queue")
    stage = work / "stage"
    receipts = tier_loop.ReceiptCache()

    def discover(**_kwargs):
        return {TIER: _tier_record(stage)}

    # What each cycle's sweep held the stage ownership lock for, summed over
    # the receipts it returned (#1056): every egress and stale-mention prune
    # records its own hold.
    swept: dict[str, float] = {}
    real_sweep = stage_release.sweep

    def counted_sweep(*args, **kwargs):
        out = real_sweep(*args, **kwargs)
        for entry in out:
            held = entry.get("lock_held_s")
            if isinstance(held, (int, float)):
                swept["lock_held_s"] = swept.get("lock_held_s", 0.0) + held
                swept["holds"] = swept.get("holds", 0) + 1
            if entry.get("event") == stage_release.STALE_MENTION_EVENT:
                swept["stale_receipts"] = swept.get("stale_receipts", 0) + 1
        return out

    stage_release.sweep = counted_sweep

    def one() -> dict[str, object]:
        swept.clear()
        unix = time.time()
        started = time.perf_counter()
        cpu = time.process_time()
        tier_loop.cycle(queue, host=HOST, source_pool="storage_pool",
                        receipts=receipts, discover=discover)
        row: dict[str, object] = {
            "wall_s": round(time.perf_counter() - started, 4),
            "cpu_s": round(time.process_time() - cpu, 4),
            "started_unix": round(unix, 3), "ended_unix": round(time.time(), 3),
            "stage_lock_held_s": round(swept.get("lock_held_s", 0.0), 6),
            "stage_lock_holds": int(swept.get("holds", 0)),
            "stale_mention_receipts": int(swept.get("stale_receipts", 0))}
        recorded = getattr(tier_loop, "LAST_CYCLE", None)
        if isinstance(recorded, dict):
            row["recorded"] = recorded
        return row

    def cold_cycle() -> dict[str, object]:
        return one()

    def steady_cycle() -> dict[str, object]:
        return one()

    rows = [dict(cold_cycle(), kind="cold")]
    for _ in range(args.cycles - 1):
        rows.append(dict(steady_cycle(), kind="steady"))
    Path(args.cycles_out).write_text(json.dumps(rows, indent=1) + "\n")
    return 0


# ---- attributing samples ----------------------------------------------------

_OPS = (
    ("os.listdir", "listdir"), ("os.scandir", "scandir"), ("scandir", "scandir"),
    (".glob(", "glob"), ("json.load", "json_parse"), ("json.loads", "json_parse"),
    ("_read_json", "read_json"), ("os.stat(", "stat"), ("os.lstat(", "stat"),
    (".stat()", "stat"), (".exists()", "stat"), ("os.open(", "open"),
    ("open(", "open"), ("os.walk", "walk"), ("os.replace", "rename"),
)


def classify(frames: list[tuple[str, str, int]]) -> tuple[str, str, str]:
    """``(wrapper, step, operation)`` for one sample.

    ``wrapper`` is ``cold_cycle`` or ``steady_cycle``; ``step`` is the
    callee of ``tier_loop.cycle`` the sample is inside (``cycle`` itself when
    none), or of ``tier_loop._cycle`` on a tree where ``cycle`` wraps the
    body (#992); ``operation`` is the innermost filesystem or parse call on the
    stack, read off the source line of the innermost PrismaBuild frame that
    names one.
    """

    wrapper = "setup"
    step = "outside_cycle"
    for index, (function, file, _number) in enumerate(frames):
        base = os.path.basename(file)
        if base == "bench_tier_cycle.py" and function in ("cold_cycle",
                                                          "steady_cycle"):
            wrapper = function
        if base == "tier_loop.py" and function in ("cycle", "_cycle"):
            step = function
            if index + 1 < len(frames):
                step = frames[index + 1][0]
    operation = "cpu"
    for function, file, number in reversed(frames):
        base = os.path.basename(file)
        if base in ("decoder.py", "__init__.py") and function in (
                "decode", "raw_decode", "load", "loads"):
            operation = "json_parse"
            break
        if base not in ("tier_loop.py", "stage_release.py", "pool.py",
                        "residency_plan.py", "residency_map.py",
                        "reader_lease.py", "storage_tiers.py",
                        "produced_output.py", "posix_lock.py"):
            continue
        text = bench_stage_adopt._line(file, number)
        for needle, name in _OPS:
            if needle in text:
                operation = f"{name}({base}:{function})"
                break
        else:
            continue
        break
    return wrapper, step, operation


def analyze(profile: Path, rate: int) -> dict[str, object]:
    if not profile.exists():
        return {"missing": str(profile)}
    by_step: dict[str, collections.Counter] = collections.defaultdict(
        collections.Counter)
    by_op: dict[str, collections.Counter] = collections.defaultdict(
        collections.Counter)
    for raw in profile.read_text().splitlines():
        stack, _, count = raw.rpartition(" ")
        try:
            samples = int(count)
        except ValueError:
            continue
        frames = bench_stage_adopt._frames(stack)
        wrapper, step, operation = classify(frames)
        by_step[wrapper][step] += samples
        by_op[wrapper][f"{step} :: {operation}"] += samples
    out: dict[str, object] = {}
    for wrapper in sorted(by_step):
        total = sum(by_step[wrapper].values())
        out[wrapper] = {
            "seconds": round(total / rate, 3),
            "by_step": {name: round(count / rate, 3)
                        for name, count in by_step[wrapper].most_common(25)},
            "by_operation": {name: round(count / rate, 3)
                             for name, count in by_op[wrapper].most_common(30)},
        }
    return out


# ---- main --------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--work", required=True,
                        help="scratch directory for the synthetic queue and "
                             "stage; emptied first; never /tmp")
    parser.add_argument("--out", default="",
                        help="where the profile and the summary go")
    parser.add_argument("--py-spy", default="",
                        help="py-spy executable; empty runs unprofiled")
    parser.add_argument("--rate", type=int, default=250,
                        help="py-spy samples per second")
    parser.add_argument("--cycles", type=int, default=11,
                        help="cycles to run: one cold, the rest steady")
    parser.add_argument("--done", type=int, default=28943,
                        help="done/ records (the live queue held 28,943 on "
                             "2026-09-23)")
    parser.add_argument("--failed", type=int, default=7142,
                        help="failed/ records (7,142 live)")
    parser.add_argument("--withdrawn", type=int, default=1877,
                        help="withdrawn/ records (1,877 live)")
    parser.add_argument("--receipts", type=int, default=6434,
                        help="movement receipts (6,434 live)")
    parser.add_argument("--passes", type=int, default=1324,
                        help="passes/ sidecars (1,324 live)")
    parser.add_argument("--empty-dirs", type=int, default=404,
                        help="empty consumer directories in the residency "
                             "forest")
    parser.add_argument("--small-dirs", type=int, default=30,
                        help="consumer directories holding one to nine "
                             "small noise fragments each")
    parser.add_argument("--big-fragments", default="90000,60000,50000,40000",
                        help="entries in each large noise fragment, "
                             "comma-separated")
    parser.add_argument("--produced-fragment-dirs", type=int, default=1957,
                        help="subdirectories of the produced-output fragment "
                             "directory")
    parser.add_argument("--live-consumers", type=int, default=6,
                        help="live consumers with frozen plans and one landed "
                             "range each; even-numbered ones are claimed, and "
                             "the first has a mover in flight when --phases is "
                             "above 1")
    parser.add_argument("--phases", type=int, default=3,
                        help="phases in each live consumer's plan")
    parser.add_argument("--files-per-range", type=int, default=64,
                        help="files in each landed range")
    parser.add_argument("--ready-noise", type=int, default=30,
                        help="unrelated ready rows")
    parser.add_argument("--claimed-noise", type=int, default=28,
                        help="unrelated claimed rows")
    parser.add_argument("--dead-owner-pairs", type=int, default=0,
                        help="pairs of co-owned dead owners (#1056; the live "
                             "tier loop carried 35 to 47 such owners on "
                             "2026-09-24)")
    parser.add_argument("--dead-entries", type=int, default=1680,
                        help="entries in each dead owner's fragment, one "
                             "parent directory each")
    parser.add_argument("--no-range-claims", type=int, default=0,
                        help="claimed fill-rate-only exports that seal no "
                             "range (#1056)")
    parser.add_argument("--run-cycles", action="store_true",
                        help=argparse.SUPPRESS)
    parser.add_argument("--cycles-out", default="", help=argparse.SUPPRESS)
    args = parser.parse_args(argv)

    if args.run_cycles:
        return run_cycles(args)

    work = Path(args.work).resolve()
    out = Path(args.out or (work.parent / (work.name + "-out"))).resolve()
    for guarded in (work, out):
        if str(guarded) == "/tmp" or str(guarded).startswith("/tmp/"):
            raise SystemExit(f"refusing {guarded}: never /tmp")
    if work.exists():
        shutil.rmtree(work)
    work.mkdir(parents=True)
    out.mkdir(parents=True, exist_ok=True)

    queue = pool.PoolQueue(work / "pb-queue")
    queue.ensure_layout()
    stage = work / "stage"
    stage.mkdir()
    assert stage_release.register_stage_root(
        queue, tier_id=TIER, stage_root=stage) == "registered"
    built = time.monotonic()
    shape = build_queue(queue, stage, args)
    setup_s = time.monotonic() - built

    cycles_out = out / "cycles.json"
    argv_child = [sys.executable, str(Path(__file__).resolve()),
                  "--run-cycles", "--work", str(work),
                  "--cycles", str(args.cycles),
                  "--cycles-out", str(cycles_out)]
    profile = out / "tier-cycle.pyspy.txt"
    if args.py_spy:
        argv_child = [args.py_spy, "record", "--nonblocking",
                      "--rate", str(args.rate), "--format", "raw",
                      "--output", str(profile), "--"] + argv_child
    log = out / "cycles.log"
    with open(log, "w") as stream:
        code = subprocess.call(argv_child, stdout=stream,
                               stderr=subprocess.STDOUT)
    rows = json.loads(cycles_out.read_text()) if cycles_out.exists() else []
    steady = sorted(float(row["wall_s"]) for row in rows
                    if row.get("kind") == "steady")
    summary: dict[str, object] = {
        "checkout": str(HERE.parents[1]), "host": os.uname().nodename,
        "python": sys.version.split()[0], "shape": shape,
        "setup_s": round(setup_s, 2), "returncode": code,
        "cycles": rows,
        "cold_wall_s": rows[0]["wall_s"] if rows else None,
        "steady_median_wall_s": (steady[len(steady) // 2] if steady else None),
        "steady_max_wall_s": (steady[-1] if steady else None),
        "started_unix": rows[0].get("started_unix") if rows else None,
        "ended_unix": rows[-1].get("ended_unix") if rows else None,
        "profile": analyze(profile, args.rate) if args.py_spy else None,
    }
    (out / "summary.json").write_text(json.dumps(summary, indent=1) + "\n")
    print(json.dumps(summary, indent=1))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
