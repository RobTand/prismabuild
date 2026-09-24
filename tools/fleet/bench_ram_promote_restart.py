#!/usr/bin/env python3
"""What a restarted RAM promotion costs to take back its own copies (#1081).

On 2026-09-24, after a tier-role restart, ``ram_promote`` refilled the RAM
tier at about 9 MB/s with sixteen threads parked in the publication poll.
A promotion files its records only at the end of its range, so one killed
before then leaves correct copies that no record names; its retry runs under
the same action key and meets every one of them.  This is the hermetic
reproduction on scratch: a synthetic queue, stage root and RAM root under
``--work``, the range staged by the real stage mover, promoted once by the
real promoter, its records deleted -- exactly what the kill leaves -- and the
promotion run again as a real process.

Two scenarios, each a real ``ram_promote.py`` process:

* ``fresh`` -- the first promotion of a freshly staged range (the copy path,
  the reference rate);
* ``restart`` -- the same action key again, over the copies ``fresh`` left
  with their records removed.

The publication grace is the tool's own (``_PUBLISH_GRACE_S``, 30 s), never
shortened: the restart's cost on the tree under test is the claim.  Each
process runs under ``py-spy record --idle --threads`` when ``--py-spy`` is
given, and the samples of its copy workers are attributed to the cost they
sit in by :mod:`bench_stage_adopt`'s classifier, with the content proof's
read and hash named separately.  The restart is checked, not just timed:
the receipt must be complete, and the result counts how many destinations
kept the inode ``fresh`` published (adopted) against how many were replaced.

The scratch is local disk, not a tmpfs, so absolute rates are this box's disk
and page cache rather than the RAM tier's; the grace-bound wait the bench
exists to show does not depend on either.

Run it through PrismaBuild on a GB10, never on the tier host, and never
against the live stage, RAM tier or queue::

    pbrun.py --tag sparky --cpus 2 --demand mem_gb=12 --priority -10 -- \\
        python3 tools/fleet/bench_ram_promote_restart.py \\
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
import socket
import subprocess
import sys
import time

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parents[1] / "src"))

from prismabuild import pool, reader_lease, residency_map  # noqa: E402
from prismabuild import storage_tiers  # noqa: E402
import bench_stage_adopt  # noqa: E402
import stage_move  # noqa: E402

STAGE_TIER = "prismabuild-stage:bench"
RAM_TIER = "ram:bench"
MIB = 1 << 20


def _key(label: str) -> str:
    return hashlib.sha256(f"bench-1081:{label}".encode()).hexdigest()


CONSUMER = _key("consumer")
STAGE_MOVER = _key("stage-mover")
RAM_MOVER = _key("ram-mover")


def make_range(work: Path, entries: int, entry_bytes: int) -> dict[str, object]:
    """Real source files and the manifest that names them."""

    origin = work / "origin"
    origin.mkdir(parents=True)
    rows = []
    for index in range(entries):
        payload = os.urandom(entry_bytes)
        path = origin / f"shard-{index:05d}.bin"
        path.write_bytes(payload)
        rows.append({"path": str(path), "offset": 0, "bytes": entry_bytes,
                     "sha256": hashlib.sha256(payload).hexdigest()})
    body = {
        "schema": "prismaquant.prismabuild.data_manifest.v1",
        "produced_by": {"tool": "bench-ram-promote-restart"},
        "mount_prefix": str(origin),
        "entries": rows,
        "entry_count": entries,
        "total_bytes": entries * entry_bytes,
        "annotations": {},
    }
    path = work / "manifest.json"
    path.write_text(json.dumps(body))
    return {"path": path, "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "bytes": entries * entry_bytes, "body": body}


def stage(work: Path, queue: pool.PoolQueue, window: dict[str, object],
          workers: int) -> dict[str, object]:
    """Stage the range with the real stage mover (setup, not measured)."""

    args = stage_move.build_parser().parse_args([
        "--pool-root", str(queue.root), "--cas-root", str(work / "cas"),
        "--action-key", STAGE_MOVER, "--consumer-action-key", CONSUMER,
        "--tier-id", STAGE_TIER, "--stage-root", str(work / "stage"),
        "--manifest-sha256", str(window["sha256"]),
        "--range-start-bytes", "0", "--range-end-bytes", str(window["bytes"]),
        "--manifest", str(window["path"]),
        "--residency-root", str(queue.root / pool.RESIDENCY),
        "--readers", str(workers), "--max-readers", str(workers),
        "--warm-after-copy", "never", "--unpaced",
    ])
    receipt = stage_move.move(args)
    if receipt.get("complete") is not True:
        raise SystemExit(f"staging the range failed: {receipt}")
    return receipt


def promote_argv(work: Path, queue: pool.PoolQueue, window: dict[str, object],
                 workers: int, receipt: Path) -> list[str]:
    return [sys.executable, str(HERE / "ram_promote.py"),
            "--pool-root", str(queue.root), "--cas-root", str(work / "cas"),
            "--action-key", RAM_MOVER, "--consumer-action-key", CONSUMER,
            "--tier-id", RAM_TIER, "--ram-root", str(work / "ram"),
            "--source-stage-root", str(work / "stage"),
            "--manifest-sha256", str(window["sha256"]),
            "--range-start-bytes", "0", "--range-end-bytes", str(window["bytes"]),
            "--manifest", str(window["path"]),
            "--residency-root", str(queue.root / pool.RESIDENCY),
            "--readers", str(workers), "--max-readers", str(workers),
            "--receipt", str(receipt)]


def ram_paths(work: Path, window: dict[str, object]) -> list[Path]:
    body = window["body"]
    return [work / "ram" / stage_move.stage_relative(
                str(entry["path"]), 0, int(entry["bytes"]),
                mount_prefix=str(body["mount_prefix"]))
            for entry in body["entries"]]


def identities(paths: list[Path]) -> list[dict[str, int] | None]:
    return [reader_lease.stat_identity(str(path)) for path in paths]


def classify(frames: list[tuple[str, str, int]]) -> str:
    """:func:`bench_stage_adopt.classify`, with the content proof named."""

    for function, file, number in reversed(frames):
        if os.path.basename(file) != "stage_move.py":
            continue
        if function == "_content_proof":
            text = bench_stage_adopt._line(file, number)
            if "os.readv" in text:
                return "content_proof:read"
            if "hasher" in text:
                return "content_proof:hash"
            return "content_proof:other"
        break
    return bench_stage_adopt.classify(frames)


def analyze(profile: Path, rate: int) -> dict[str, object]:
    """Copy-worker thread seconds by cost, from one py-spy raw profile."""

    worker = collections.Counter()
    main = collections.Counter()
    if not profile.exists():
        return {"missing": str(profile)}
    for raw in profile.read_text(errors="replace").splitlines():
        stack, _, count = raw.rpartition(" ")
        try:
            samples = int(count)
        except ValueError:
            continue
        frames = bench_stage_adopt._frames(stack)
        category = classify(frames)
        is_worker = any(function == "worker"
                        and os.path.basename(file) == "stage_move.py"
                        for function, file, _ in frames)
        (worker if is_worker else main)[category] += samples
    total = sum(worker.values()) or 1
    return {
        "worker_thread_seconds": round(sum(worker.values()) / rate, 2),
        "worker_by_cost": {name: {"thread_s": round(count / rate, 2),
                                  "share": round(count / total, 4)}
                           for name, count in worker.most_common()},
        "main_thread_by_cost": {name: round(count / rate, 2)
                                for name, count in main.most_common(8)},
    }


def run_promotion(args, work: Path, out: Path, queue: pool.PoolQueue,
                  window: dict[str, object], scenario: str) -> dict[str, object]:
    """One real promotion process, profiled when asked; its receipt and cost."""

    receipt_path = out / f"{scenario}.receipt.json"
    profile = out / f"{scenario}.pyspy.txt"
    argv = promote_argv(work, queue, window, args.workers, receipt_path)
    if args.py_spy:
        argv = [args.py_spy, "record", "--idle", "--threads", "--nonblocking",
                "--rate", str(args.rate), "--format", "raw",
                "--output", str(profile), "--"] + argv
    started_unix = time.time()
    started = time.monotonic()
    with open(out / f"{scenario}.log", "w") as log:
        code = subprocess.run(argv, stdout=log, stderr=subprocess.STDOUT,
                              check=False).returncode
    wall = time.monotonic() - started
    receipt = (json.loads(receipt_path.read_text())
               if receipt_path.exists() else {})
    return {
        "scenario": scenario,
        "returncode": code,
        "started_unix": round(started_unix, 3),
        "ended_unix": round(time.time(), 3),
        "wall_s": round(wall, 3),
        "resident_mb_per_s": round(int(window["bytes"]) / 1e6 / wall, 1),
        "complete": receipt.get("complete"),
        "entries_staged": receipt.get("entries_staged"),
        "receipt_seconds": receipt.get("seconds"),
        "receipt_mb_per_s_file_side": receipt.get("mb_per_s_file_side"),
        "proc_io": receipt.get("proc_io"),
        "cpu_seconds": receipt.get("cpu_seconds"),
        "phase_timings": receipt.get("phase_timings"),
        "errors": receipt.get("errors"),
        "profile": (analyze(profile, args.rate) if args.py_spy else None),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--work", required=True,
                        help="scratch directory the bench creates and fills; "
                             "must not exist, and is removed at the end "
                             "unless --keep")
    parser.add_argument("--out", required=True,
                        help="directory for receipts, logs, profiles and "
                             "result.json")
    parser.add_argument("--entries", type=int, default=32,
                        help="entries in the promoted range")
    parser.add_argument("--entry-mib", type=int, default=16,
                        help="size of each entry in MiB (the live RAM "
                             "promotion's entries were 16 MiB)")
    parser.add_argument("--workers", type=int, default=16,
                        help="promotion copy width (ram_promote's default "
                             "--max-readers is 16)")
    parser.add_argument("--py-spy", default="",
                        help="py-spy executable; empty runs unprofiled")
    parser.add_argument("--rate", type=int, default=100,
                        help="py-spy samples per second")
    parser.add_argument("--keep", action="store_true",
                        help="keep the scratch directory after the run")
    args = parser.parse_args(argv)

    work = Path(args.work)
    out = Path(args.out)
    if work.exists():
        raise SystemExit(f"--work {work} exists; the bench owns a fresh one")
    work.mkdir(parents=True)
    out.mkdir(parents=True, exist_ok=True)
    try:
        setup_started = time.monotonic()
        queue = pool.PoolQueue(work / "pb-queue")
        queue.ensure_layout()
        window = make_range(work, args.entries, args.entry_mib * MIB)
        stage(work, queue, window, args.workers)
        (work / "ram").mkdir()
        if storage_tiers.ensure_ram_epoch(
                work / "ram", host=socket.gethostname()) is None:
            raise SystemExit("could not stamp the scratch RAM epoch")
        setup_s = time.monotonic() - setup_started

        fresh = run_promotion(args, work, out, queue, window, "fresh")
        if fresh["complete"] is not True:
            raise SystemExit(f"the fresh promotion failed: {fresh}")
        paths = ram_paths(work, window)
        published = identities(paths)

        # What a kill before the end-of-range records leaves.
        residence = queue.root / pool.RESIDENCY
        residency_map.fragment_path(residence, CONSUMER, RAM_MOVER).unlink()
        reader_lease.material_path(residence, CONSUMER, RAM_MOVER).unlink()

        restart = run_promotion(args, work, out, queue, window, "restart")
        after = identities(paths)
        restart["destinations_adopted"] = sum(
            1 for old, new in zip(published, after)
            if old is not None and reader_lease.file_id_matches(old, new))
        restart["destinations_replaced"] = sum(
            1 for old, new in zip(published, after)
            if old is not None and new is not None
            and not reader_lease.file_id_matches(old, new))

        grace = getattr(stage_move, "_PUBLISH_GRACE_S", None)
        result = {
            "schema": "prismabuild.bench.ram_promote_restart.v1",
            "host": socket.gethostname(),
            "entries": args.entries,
            "entry_bytes": args.entry_mib * MIB,
            "range_bytes": int(window["bytes"]),
            "workers": args.workers,
            "publish_grace_s": grace,
            "setup_s": round(setup_s, 2),
            "profiled": bool(args.py_spy),
            "scenarios": [fresh, restart],
        }
        (out / "result.json").write_text(json.dumps(result, indent=1))
        for row in (fresh, restart):
            print(f"{row['scenario']:>8}: {row['wall_s']:8.2f} s wall, "
                  f"{row['resident_mb_per_s']:8.1f} MB/s resident, "
                  f"complete={row['complete']}, "
                  f"outcomes={(row['phase_timings'] or {}).get('outcomes')}")
        print(f" restart: adopted {restart['destinations_adopted']}, "
              f"replaced {restart['destinations_replaced']} of {args.entries}")
        return 0 if restart["complete"] is True else 1
    finally:
        if not args.keep:
            shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
