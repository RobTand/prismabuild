#!/usr/bin/env python3
"""What a whole-range egress costs the publishers beside it, and where (#988).

The audit (WS-DA finding 1) found ``stage_release.evict`` holding the stage
root's ownership lock across four fleet-wide censuses, a per-entry judgement
and a per-entry unlink.  Every mover's publication, every reader's pin and
every new mover's start gate on that root waits for the whole range, and the
tier loop evicts whole ranges of 1,680 to 161,572 entries.

This is the hermetic reproduction, shaped like ``bench_stage_adopt.py`` and
reusing its live-shaped residency forest: a synthetic queue and stage root, a
landed range of ``--entries`` one-KiB entries with its fragment, material
sidecar and tier token, and ``--publishers`` real publishers
(``stage_move._StagedPublisher.publish``), each its own process publishing
``--per-publisher`` fresh entries.  The egress is ``evict(whole=True)`` on
the landed range, in its own process, which is what the tier loop's
beyond-horizon eviction runs.  The publishers start at the egress's first
grant of the lock, so each of them meets it.

Every process runs under ``py-spy record --idle --threads`` so a thread
waiting on the lock is sampled where it waits.  The analysis attributes each
sample to the cost it is spending (a census, the per-entry judgement, an
unlink, a lock wait, a rename) and, for the egress, to whether the stage
ownership lock was held at the time, read off the frames that run inside it.
The same analysis reads a before and an after tree.

Checks, not just timings: the egress receipt must be complete, every landed
file must be gone and every publication must have landed.

Run it through PrismaBuild on a GB10, never on the tier host, and never
against the live stage or queue::

    pbrun.py --cwd <checkout> --tag sparky --cpus 6 --demand mem_gb=8 \\
        --priority -10 -- python3 tools/fleet/bench_stage_egress.py \\
        --work <scratch dir> --out <results dir> --py-spy <path to py-spy>
"""
from __future__ import annotations

import argparse
import collections
from contextlib import contextmanager
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parents[1] / "src"))

from prismabuild import pool, reader_lease, residency_map, storage_tiers  # noqa: E402
import bench_stage_adopt  # noqa: E402
import stage_move  # noqa: E402
import stage_release  # noqa: E402

TIER = bench_stage_adopt.TIER
STAGE_KIND = f"stage_gib@{TIER}"
ENTRY_BYTES = 1024
DIGEST = "a" * 64


def _key(label: str) -> str:
    return hashlib.sha256(f"bench-988:{label}".encode()).hexdigest()


LANDED_CONSUMER = _key("landed-consumer")
LANDED_MOVER = _key("landed-mover")
LANDED_MANIFEST = _key("landed-manifest")


# ---- the fixture -------------------------------------------------------------

def land_range(queue: pool.PoolQueue, stage: Path, entries_wanted: int) -> None:
    """A complete mover's range: files, fragment, material, and its tier token."""

    root = queue.root / pool.RESIDENCY
    entries: dict[str, dict[str, object]] = {}
    material: dict[str, dict[str, object]] = {}
    for number in range(entries_wanted):
        declared = f"/pool/landed/part-{number}.bin"
        key = residency_map.residency_map_key(declared, 0)
        path = stage / "landed" / f"d{number // 500:04d}" / f"part-{number}.bin"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"\0" * ENTRY_BYTES)
        entries[key] = {"stage_path": str(path), "bytes": ENTRY_BYTES,
                        "offset": 0, "sha256": DIGEST}
        material[key] = {"stage_path": str(path), "bytes": ENTRY_BYTES,
                         "sha256": DIGEST,
                         "file_id": reader_lease.stat_identity(str(path))}
    residency_map.write_fragment(root, {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": LANDED_CONSUMER,
        "mover_action_key": LANDED_MOVER, "tier_id": TIER,
        "stage_root": str(stage), "manifest_sha256": LANDED_MANIFEST,
        "entries": entries})
    reader_lease.write_material(
        root, consumer_action_key=LANDED_CONSUMER,
        mover_action_key=LANDED_MOVER, tier_id=TIER, stage_root=str(stage),
        manifest_sha256=LANDED_MANIFEST,
        generation=reader_lease.mint_generation(), entries=material)
    queue.publish(action_key=LANDED_MOVER, cas_root=queue.root / "cas",
                  checkout_root=queue.root / "co",
                  worker_script=queue.root / "worker.py",
                  resources={"cpu": 1, "mem_gb": 1, STAGE_KIND: 1},
                  residency={"schema": pool.RESIDENCY_SCHEMA_V1,
                             "tier_id": TIER,
                             "manifest_sha256": LANDED_MANIFEST,
                             "manifest_bytes": 1 << 40,
                             "range_start_bytes": 0,
                             "range_end_bytes": storage_tiers.GIB},
                  max_attempts=1, retry_safe=False)
    claimed = queue.claim(capacity={"cpu": 4, "mem_gb": 8}, tags=["dl380g10"])
    if claimed is None or claimed["action_key"] != LANDED_MOVER:
        raise SystemExit("the landed mover could not be claimed")
    queue.record_move(LANDED_MOVER, {
        "consumer_action_key": LANDED_CONSUMER, "tier_id": TIER,
        "stage_root": str(stage), "manifest_sha256": LANDED_MANIFEST,
        "range_start_bytes": 0, "range_end_bytes": storage_tiers.GIB,
        "bytes_staged": storage_tiers.GIB, "complete": True})
    queue.finish(LANDED_MOVER, status="executed")


def plan_publisher(stage: Path, index: int, count: int) -> list[list[str]]:
    """One publisher's verified temporaries and their fresh destinations."""

    pairs = []
    temps = stage / f"publisher-{index}" / "temps"
    temps.mkdir(parents=True, exist_ok=True)
    for number in range(count):
        destination = (stage / f"publisher-{index}" / f"d{number // 500:04d}"
                       / f"part-{number}.bin")
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = temps / f".part-{number}.bin.partial"
        temporary.write_bytes(b"\1" * ENTRY_BYTES)
        pairs.append([str(temporary), str(destination)])
    return pairs


# ---- the roles ---------------------------------------------------------------

def run_publisher(args) -> int:
    """Publish one plan once the go marker exists; file every entry's latency."""

    queue = pool.PoolQueue(Path(args.pool_root))
    stage = Path(args.stage_root)
    publisher = stage_move._StagedPublisher(
        queue=queue, stage_root=stage,
        residency_root=queue.root / pool.RESIDENCY,
        mover_action_key=_key(f"publisher-{args.index}"),
        manifest_sha256=_key(f"publisher-manifest-{args.index}"), tier_id=TIER,
        cas_root=queue.root.parent / "cas",
        consumer_action_key=_key(f"publisher-consumer-{args.index}"))
    publisher.begin_material()
    pairs = json.loads(Path(args.plan).read_text())
    go = Path(args.go)
    waited = time.monotonic()
    while not go.exists():
        if time.monotonic() - waited > 600:
            raise SystemExit("the egress never took the lock")
        time.sleep(0.001)
    samples = []
    for temporary, destination in pairs:
        entry = {"bytes": ENTRY_BYTES, "sha256": DIGEST, "offset": 0,
                 "path": destination}
        asked = time.monotonic()
        publisher.publish(entry, Path(destination), Path(temporary), DIGEST)
        samples.append([asked, time.monotonic()])
    Path(args.samples).write_text(json.dumps({
        "samples": samples, "phase_timings": publisher.clock.report()}))
    return 0


def run_egress(args) -> int:
    """``evict(whole=True)`` on the landed range, the lock measured from outside."""

    queue = pool.PoolQueue(Path(args.pool_root))
    go = Path(args.go)
    holds: list[list[float]] = []
    original = queue.stage_ownership_lock

    @contextmanager
    def measured(stage_root, *, blocking: bool = True):
        asked = time.monotonic()
        with original(stage_root, blocking=blocking) as got:
            granted = time.monotonic()
            if not go.exists():
                go.write_text("go\n")
            try:
                yield got
            finally:
                holds.append([asked, granted, time.monotonic()])

    queue.stage_ownership_lock = measured  # type: ignore[method-assign]
    began = time.monotonic()
    began_unix = time.time()
    try:
        receipt = stage_release.evict(
            queue, LANDED_MOVER, consumer_action_key=LANDED_CONSUMER,
            stage_root=args.stage_root, reason="beyond-horizon", whole=True)
    finally:
        if not go.exists():
            go.write_text("go\n")
    ended = time.monotonic()
    Path(args.samples).write_text(json.dumps({
        "receipt": receipt, "holds": holds, "began": began, "ended": ended,
        "began_unix": began_unix, "ended_unix": time.time()}))
    return 0


# ---- attributing samples -----------------------------------------------------

#: Frames that run with the stage ownership lock held, by function name.
#: ``_evict_owned`` is the one hold before #988; the ``_held_`` helpers are
#: the holds after it.
HELD_FRAMES = ("_evict_owned",)
HELD_PREFIX = "_held_"


def classify_egress(frames: list[tuple[str, str, int]]) -> str:
    """The cost one egress sample is spending, read off its innermost frame."""

    for function, file, number in reversed(frames):
        base = os.path.basename(file)
        text = bench_stage_adopt._line(file, number)
        if base == "posix_lock.py":
            if "fcntl.lockf" in text and "LOCK_UN" not in text:
                return "lock_wait"
            continue
        if base == "decoder.py" or function in ("loads", "load"):
            continue
        if base == "reader_lease.py" and function == "live_for":
            return "census:pins"
        if base != "stage_release.py":
            continue
        if "os.unlink" in text:
            return "unlink"
        if function == "_prune_empty":
            return "prune_empty_dirs"
        if function in ("_claimed_paths_attributed", "_mover_claim_paths",
                        "_claims_census"):
            return "census:claims"
        if function in ("_claimed_source_paths", "_promotion_source_paths"):
            return "census:promotion_sources"
        if function in ("_fragment_census", "_census_level",
                        "_census_fragment_directory", "_read_fragment",
                        "_fragment_owners"):
            return "census:fragments"
        if function == "_entry_fences":
            return "fence"
        if function in ("judge", "_relative_under") or ".resolve()" in text:
            return "judge"
        if function in ("_evict_locked",) and "json.load" in text:
            return "own_fragment_parse"
        return f"stage_release:{function}"
    return "other"


def classify_publisher(frames: list[tuple[str, str, int]]) -> str:
    for function, file, number in reversed(frames):
        base = os.path.basename(file)
        text = bench_stage_adopt._line(file, number)
        if base == "posix_lock.py":
            if "fcntl.lockf" in text and "LOCK_UN" not in text:
                return "lock_wait"
            if "mutex.acquire" in text:
                return "lock_wait:thread_rlock"
            continue
        if base == "bench_stage_egress.py" and "time.sleep" in text:
            return "idle:waiting_for_go"
        if base != "stage_move.py":
            continue
        if "os.replace" in text:
            return "publish:rename"
        if "os.lstat(" in text or "os.stat(" in text:
            return "publish:stat"
        return f"stage_move:{function}"
    return "other"


def analyze(profile: Path, rate: int, classify, *, egress: bool
            ) -> dict[str, object]:
    by_cost = collections.Counter()
    held = collections.Counter()
    held_by_cost = collections.Counter()
    if not profile.exists():
        return {"missing": str(profile)}
    for raw in profile.read_text().splitlines():
        stack, _, count = raw.rpartition(" ")
        try:
            samples = int(count)
        except ValueError:
            continue
        frames = bench_stage_adopt._frames(stack)
        category = classify(frames)
        by_cost[category] += samples
        if egress:
            inside = any(function in HELD_FRAMES
                         or function.startswith(HELD_PREFIX)
                         for function, _file, _line in frames)
            held["held" if inside else "not_held"] += samples
            if inside:
                held_by_cost[category] += samples
    out: dict[str, object] = {
        "seconds_by_cost": {name: round(count / rate, 3)
                            for name, count in by_cost.most_common()}}
    if egress:
        out["seconds_by_lock"] = {name: round(count / rate, 3)
                                  for name, count in held.most_common()}
        out["seconds_held_by_cost"] = {
            name: round(count / rate, 3)
            for name, count in held_by_cost.most_common()}
    return out


def _p(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(fraction * len(ordered)) - 1)]


# ---- orchestration -------------------------------------------------------------

def run_bench(args) -> int:
    work = Path(args.work).resolve()
    out = Path(args.out).resolve()
    for guarded in (work, out):
        if str(guarded) == "/tmp" or str(guarded).startswith("/tmp/"):
            raise SystemExit(f"refusing {guarded}: never /tmp")
    if work.exists():
        shutil.rmtree(work)
    work.mkdir(parents=True)
    out.mkdir(parents=True, exist_ok=True)

    queue = pool.PoolQueue(work / "pb-queue")
    queue.ensure_layout()
    queue.mint_tier_capacity(TIER, {"stage_gib": 8})
    stage = work / "stage"
    stage.mkdir()
    stage = stage.resolve()
    assert stage_release.register_stage_root(
        queue, tier_id=TIER, stage_root=stage) == "registered"
    built = time.monotonic()
    forest = bench_stage_adopt.build_forest(
        queue, stage, empty_dirs=args.empty_dirs, small_dirs=args.small_dirs,
        big_fragments=[int(x) for x in args.big_fragments.split(",") if x],
        produced_fragment_dirs=args.produced_fragment_dirs)
    land_range(queue, stage, args.entries)
    plans = []
    for index in range(args.publishers):
        plan = work / f"publisher-{index}.plan.json"
        plan.write_text(json.dumps(plan_publisher(stage, index,
                                                  args.per_publisher)))
        plans.append(plan)
    setup_s = time.monotonic() - built

    go = work / "go"
    procs = []

    def launch(label: str, role_args: list[str]) -> None:
        argv = [sys.executable, str(Path(__file__).resolve()), *role_args]
        profile = out / f"{label}.pyspy.txt"
        if args.py_spy:
            argv = [args.py_spy, "record", "--idle", "--threads",
                    "--nonblocking", "--rate", str(args.rate),
                    "--format", "raw", "--output", str(profile), "--"] + argv
        log = open(out / f"{label}.log", "w")
        procs.append((label, profile, log,
                      subprocess.Popen(argv, stdout=log,
                                       stderr=subprocess.STDOUT)))

    common = ["--pool-root", str(queue.root), "--stage-root", str(stage),
              "--go", str(go)]
    for index, plan in enumerate(plans):
        launch(f"publisher-{index}",
               ["--role", "publisher", "--index", str(index), "--plan",
                str(plan), "--samples", str(out / f"publisher-{index}.json"),
                *common])
    # Let the publishers import and park on the go marker before the egress
    # starts, so the first grant finds them ready.
    time.sleep(args.settle_s)
    launch("egress", ["--role", "egress", "--samples",
                      str(out / "egress.json"), *common])
    codes = {}
    for label, _profile, log, proc in procs:
        codes[label] = proc.wait()
        log.close()

    checks: list[str] = []
    for label, code in codes.items():
        if code != 0:
            checks.append(f"{label} exited {code}")
    egress = json.loads((out / "egress.json").read_text())
    receipt = egress["receipt"]
    if not receipt.get("complete"):
        checks.append(f"egress incomplete: {receipt.get('errors')}")
    if receipt.get("entries_deleted") != args.entries:
        checks.append(f"egress deleted {receipt.get('entries_deleted')} of "
                      f"{args.entries}")
    if any((stage / "landed").rglob("*.bin")):
        checks.append("landed files survived the egress")
    holds = egress["holds"]
    held_from = min((granted for _a, granted, _r in holds), default=0.0)
    held_until = max((released for _a, _g, released in holds), default=0.0)
    publishers = []
    overlapping_all: list[float] = []
    for index in range(args.publishers):
        body = json.loads((out / f"publisher-{index}.json").read_text())
        samples = body["samples"]
        landed = sum(1 for _ in (stage / f"publisher-{index}").rglob("part-*.bin"))
        if landed != args.per_publisher:
            checks.append(f"publisher-{index} landed {landed} of "
                          f"{args.per_publisher}")
        latencies = [end - start for start, end in samples]
        overlapping = [end - start for start, end in samples
                       if end >= held_from and start <= held_until]
        overlapping_all.extend(overlapping)
        publishers.append({
            "index": index,
            "p50_s": _p(latencies, 0.50), "p99_s": _p(latencies, 0.99),
            "max_s": max(latencies) if latencies else None,
            "overlapping": len(overlapping),
            "overlapping_p99_s": _p(overlapping, 0.99),
            "overlapping_max_s": max(overlapping) if overlapping else None,
            "phase_timings": body.get("phase_timings"),
            "profile": analyze(out / f"publisher-{index}.pyspy.txt",
                               args.rate, classify_publisher, egress=False),
        })
    summary = {
        "checkout": str(HERE.parents[1]), "host": os.uname().nodename,
        "python": sys.version.split()[0], "entries": args.entries,
        "publishers": args.publishers, "per_publisher": args.per_publisher,
        "forest": forest, "setup_s": round(setup_s, 2),
        "profiled": bool(args.py_spy),
        "egress": {
            "wall_s": round(egress["ended"] - egress["began"], 4),
            "began_unix": egress["began_unix"],
            "ended_unix": egress["ended_unix"],
            "holds": len(holds),
            "longest_hold_s": max((r - g for _a, g, r in holds), default=None),
            "total_held_s": sum(r - g for _a, g, r in holds),
            "total_wait_s": sum(g - a for a, g, _r in holds),
            "receipt": {name: receipt.get(name) for name in (
                "complete", "entries_deleted", "tokens_released",
                "lock_wait_s", "lock_held_s", "entries_judged", "census_s",
                "census_validate_s", "unlink_s", "prune_s")},
            "profile": analyze(out / "egress.pyspy.txt", args.rate,
                               classify_egress, egress=True),
        },
        "publications_overlapping": len(overlapping_all),
        "overlapping_p99_s": _p(overlapping_all, 0.99),
        "overlapping_max_s": max(overlapping_all) if overlapping_all else None,
        "publisher_detail": publishers,
        "checks_failed": checks,
    }
    (out / "summary.json").write_text(json.dumps(summary, indent=1) + "\n")
    print(json.dumps(summary, indent=1))
    return 1 if checks else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--role", choices=("bench", "publisher", "egress"),
                        default="bench",
                        help="bench orchestrates; the other two are the "
                             "processes it starts")
    parser.add_argument("--work", help="scratch directory for the synthetic "
                                       "queue and stage; emptied first; "
                                       "never /tmp")
    parser.add_argument("--out", help="where receipts, profiles and the "
                                      "summary go")
    parser.add_argument("--entries", type=int, default=20_000,
                        help="entries in the landed range the egress evicts")
    parser.add_argument("--publishers", type=int, default=3,
                        help="concurrent publisher processes")
    parser.add_argument("--per-publisher", type=int, default=2_000,
                        help="fresh entries each publisher publishes")
    parser.add_argument("--settle-s", type=float, default=2.0,
                        help="seconds between starting the publishers and "
                             "the egress, so they are parked on the go marker")
    parser.add_argument("--py-spy", default="",
                        help="py-spy executable; empty runs unprofiled")
    parser.add_argument("--rate", type=int, default=200,
                        help="py-spy samples per second")
    parser.add_argument("--empty-dirs", type=int, default=404,
                        help="empty consumer directories in the residency "
                             "forest (bench_stage_adopt's live shape)")
    parser.add_argument("--small-dirs", type=int, default=30,
                        help="consumer directories holding one to nine small "
                             "noise fragments each")
    parser.add_argument("--big-fragments", default="90000,60000,50000,40000",
                        help="entries in each large noise fragment")
    parser.add_argument("--produced-fragment-dirs", type=int, default=1957,
                        help="subdirectories of the produced-output fragment "
                             "directory")
    # Role plumbing.
    parser.add_argument("--pool-root", help=argparse.SUPPRESS)
    parser.add_argument("--stage-root", help=argparse.SUPPRESS)
    parser.add_argument("--go", help=argparse.SUPPRESS)
    parser.add_argument("--samples", help=argparse.SUPPRESS)
    parser.add_argument("--plan", help=argparse.SUPPRESS)
    parser.add_argument("--index", type=int, default=0, help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    if args.role == "publisher":
        return run_publisher(args)
    if args.role == "egress":
        return run_egress(args)
    if not args.work or not args.out:
        parser.error("--work and --out are required")
    return run_bench(args)


if __name__ == "__main__":
    raise SystemExit(main())
