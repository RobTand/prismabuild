#!/usr/bin/env python3
"""What the stage reconcile's census costs at the live stage shape (#1073).

On generation ``02b27a8804d3`` the tier loop spent 80.3% of its samples in
``stage_release._unattributed_candidates``, and 48.1% on one line of it: the
containment check ``stage_resolved not in path.resolve().parents``, run on
every regular file the walk finds.  The live ``/stage/prewarm`` held 54,400
files in 53,168 directories, and 38,422 of them were unowned but marked by
the prewarm stage, so they are only counted, never deleted.

This is the hermetic reproduction.  It builds a synthetic stage of that shape
(never the live one): one ``.pbrange`` directory per staged object, as an
adoption leaves them, with the marked files carrying the prewarm stage's
``user.pbstage.source`` attribute, the attributed files named by one
fragment in a synthetic queue, and a handful of unmarked orphans.  It then
runs the real ``stage_release.reconcile`` against it, one cold pass and the
rest steady, in a child process, optionally under ``py-spy record
--idle``.  The orphans are recreated before every pass, outside the timed
call, so each pass deletes the same set.

Each pass is recorded with its receipt's ``census_s``, ``lock_held_s``,
``unowned_left`` and ``entries_deleted``.  The profile is summarized by the
innermost ``stage_release.py`` line each sample was on, so a before and an
after tree can be compared line by line.

Run it through PrismaBuild on a GB10, never on the tier host, and never
against the live stage::

    pbrun.py --cwd <checkout> --tag gb10 --cpus 2 --demand mem_gb=4 \\
        --priority -10 -- python3 tools/fleet/bench_stage_reconcile.py \\
        --work <scratch dir> --out <results dir> --py-spy <path to py-spy>
"""
from __future__ import annotations

import argparse
import collections
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

from prismabuild import pool, residency_map  # noqa: E402
import bench_stage_adopt  # noqa: E402
import prewarm_loop  # noqa: E402
import stage_release  # noqa: E402

TIER = "prismabuild-stage:dl380g10"
CONSUMER = "c" * 64
MOVER = "4" * 64
MANIFEST = "a" * 64
DIGEST = "b" * 64
FILE_BYTES = 4096


def _orphans(stage: Path, count: int) -> list[Path]:
    """Unmarked, unattributed files: the only ones a pass deletes."""

    made = []
    for index in range(count):
        directory = stage / "orphans" / f"orphan-{index:03d}.pbrange"
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f"0-{FILE_BYTES}"
        with open(path, "wb") as stream:
            stream.truncate(FILE_BYTES)
        made.append(path)
    return made


def build_stage(queue: pool.PoolQueue, stage: Path, *, files: int,
                directories: int, marked: int, orphans: int,
                ) -> dict[str, int]:
    """The live shape: ``files`` files in about ``directories`` directories.

    Files are laid out one per ``.pbrange`` directory, and the surplus of
    files over directories share a directory in pairs.  The first ``marked``
    carry the prewarm mark; the rest, less the orphans, are attributed by one
    fragment.
    """

    counts: collections.Counter = collections.Counter()
    body = files - orphans
    shared = max(0, files - directories)   # files that share a directory
    entries: dict[str, object] = {}
    index = 0
    slot = 0
    while index < body:
        per_dir = 2 if slot < shared and index + 1 < body else 1
        directory = (stage / "models" / f"m-{slot // 1000:03d}"
                     / f"model-{slot:05d}.safetensors.pbrange")
        directory.mkdir(parents=True, exist_ok=True)
        counts["directories"] += 1
        for part in range(per_dir):
            path = directory / f"{part * FILE_BYTES}-{(part + 1) * FILE_BYTES}"
            with open(path, "wb") as stream:
                stream.truncate(FILE_BYTES)
            if index < marked:
                os.setxattr(path, prewarm_loop.STAGE_SOURCE_XATTR,
                            f"/mnt/shared/m/{index}@0".encode())
                counts["marked"] += 1
            else:
                entries[residency_map.residency_map_key(
                    f"/pool/m/{index}", 0)] = {
                        "stage_path": str(path), "bytes": FILE_BYTES,
                        "sha256": DIGEST, "offset": 0}
                counts["attributed"] += 1
            index += 1
        slot += 1
    residency_map.write_fragment(queue.root / pool.RESIDENCY, {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": CONSUMER, "mover_action_key": MOVER,
        "tier_id": TIER, "stage_root": str(stage),
        "manifest_sha256": MANIFEST, "entries": entries})
    counts["orphans"] = len(_orphans(stage, orphans))
    counts["directories"] += orphans
    counts["files"] = counts["marked"] + counts["attributed"] + orphans
    return dict(counts)


# ---- the passes -------------------------------------------------------------

def run_passes(args) -> int:
    """The child: reconcile passes against the built stage, each timed."""

    work = Path(args.work)
    queue = pool.PoolQueue(work / "pb-queue")
    stage = work / "stage"
    index = stage_release.CensusIndex()

    def one() -> dict[str, object]:
        _orphans(stage, args.orphans)
        started = time.perf_counter()
        cpu = time.process_time()
        receipt = stage_release.reconcile(
            queue, tier_id=TIER, stage_root=str(stage), wanted={MOVER},
            index=index)
        row: dict[str, object] = {
            "wall_s": round(time.perf_counter() - started, 4),
            "cpu_s": round(time.process_time() - cpu, 4),
        }
        for field in ("census_s", "lock_held_s", "unowned_left",
                      "entries_deleted", "entries_judged", "complete",
                      "skipped"):
            if field in receipt:
                row[field] = receipt[field]
        return row

    def cold_pass() -> dict[str, object]:
        return one()

    def steady_pass() -> dict[str, object]:
        return one()

    rows = [dict(cold_pass(), kind="cold")]
    for _ in range(args.passes - 1):
        rows.append(dict(steady_pass(), kind="steady"))
    Path(args.passes_out).write_text(json.dumps(rows, indent=1) + "\n")
    return 0


# ---- attributing samples ----------------------------------------------------

def analyze(profile: Path, rate: int) -> dict[str, object]:
    """Steady-pass samples by frame and by innermost ``stage_release`` line."""

    if not profile.exists():
        return {"missing": str(profile)}
    total = 0
    by_frame: collections.Counter = collections.Counter()
    by_line: collections.Counter = collections.Counter()
    for raw in profile.read_text(errors="replace").splitlines():
        stack, _, count = raw.rpartition(" ")
        try:
            samples = int(count)
        except ValueError:
            continue
        frames = bench_stage_adopt._frames(stack)
        if not any(function == "steady_pass" for function, _f, _n in frames):
            continue
        total += samples
        for function in {function for function, _f, _n in frames}:
            by_frame[function] += samples
        for function, file, number in reversed(frames):
            if os.path.basename(file) == "stage_release.py":
                by_line[f"stage_release.py:{number} ({function})"] += samples
                break

    def share(count: int) -> float:
        return round(100.0 * count / total, 1) if total else 0.0

    frames_of_interest = ("reconcile", "_unattributed_candidates", "walk",
                          "resolve", "_marked_by_the_prewarm_stage",
                          "_held_reconcile", "_attributed_census")
    return {
        "steady_samples": total, "steady_seconds": round(total / rate, 3),
        "frame_share_pct": {name: share(by_frame[name])
                            for name in frames_of_interest},
        "line_share_pct": {name: share(count)
                           for name, count in by_line.most_common(15)},
    }


# ---- main -------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--work", required=True,
                        help="scratch directory for the synthetic queue and "
                             "stage; emptied first; never /tmp; its "
                             "filesystem must carry user extended attributes")
    parser.add_argument("--out", default="",
                        help="where the profile and the summary go")
    parser.add_argument("--py-spy", default="",
                        help="py-spy executable; empty runs unprofiled")
    parser.add_argument("--blocking", action="store_true",
                        help="sample with py-spy's default blocking mode: "
                             "every sample is read from a paused child, so "
                             "the shares lose nothing but the pass times "
                             "grow")
    parser.add_argument("--rate", type=int, default=100,
                        help="py-spy samples per second")
    parser.add_argument("--passes", type=int, default=6,
                        help="reconcile passes to run: one cold, the rest "
                             "steady")
    parser.add_argument("--files", type=int, default=54400,
                        help="files on the stage (the live /stage/prewarm "
                             "held 54,400 on 2026-09-24)")
    parser.add_argument("--directories", type=int, default=53168,
                        help="directories holding them (53,168 live)")
    parser.add_argument("--marked", type=int, default=38422,
                        help="files unowned but marked by the prewarm stage "
                             "(38,422 live)")
    parser.add_argument("--orphans", type=int, default=8,
                        help="unmarked, unattributed files, recreated before "
                             "every pass")
    parser.add_argument("--run-passes", action="store_true",
                        help=argparse.SUPPRESS)
    parser.add_argument("--passes-out", default="", help=argparse.SUPPRESS)
    args = parser.parse_args(argv)

    if args.run_passes:
        return run_passes(args)

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
    shape = build_stage(queue, stage, files=args.files,
                        directories=args.directories, marked=args.marked,
                        orphans=args.orphans)
    setup_s = time.monotonic() - built

    passes_out = out / "passes.json"
    argv_child = [sys.executable, str(Path(__file__).resolve()),
                  "--run-passes", "--work", str(work),
                  "--passes", str(args.passes),
                  "--orphans", str(args.orphans),
                  "--passes-out", str(passes_out)]
    profile = out / "reconcile.pyspy.txt"
    if args.py_spy:
        # ``--idle`` keeps the samples taken inside a system call: the pass
        # is single-threaded, and its lstat, getxattr and directory reads
        # are its cost, not idle time.
        # ``--blocking`` pauses the child for each sample: slower passes,
        # but no sample is lost to a stack that changed while it was read.
        pace = [] if args.blocking else ["--nonblocking"]
        argv_child = [args.py_spy, "record", *pace, "--idle",
                      "--rate", str(args.rate), "--format", "raw",
                      "--output", str(profile), "--"] + argv_child
    log = out / "passes.log"
    started_unix = time.time()
    with open(log, "w") as stream:
        code = subprocess.call(argv_child, stdout=stream,
                               stderr=subprocess.STDOUT)
    ended_unix = time.time()
    rows = json.loads(passes_out.read_text()) if passes_out.exists() else []
    steady = sorted(float(row["census_s"]) for row in rows
                    if row.get("kind") == "steady" and "census_s" in row)
    summary: dict[str, object] = {
        "checkout": str(HERE.parents[1]), "host": os.uname().nodename,
        "python": sys.version.split()[0], "shape": shape,
        "setup_s": round(setup_s, 2), "returncode": code,
        "passes": rows,
        "steady_median_census_s": (steady[len(steady) // 2]
                                   if steady else None),
        "steady_max_census_s": steady[-1] if steady else None,
        "started_unix": round(started_unix, 3),
        "ended_unix": round(ended_unix, 3),
        "profile": analyze(profile, args.rate) if args.py_spy else None,
    }
    (out / "summary.json").write_text(json.dumps(summary, indent=1) + "\n")
    print(json.dumps(summary, indent=1))
    shutil.rmtree(work, ignore_errors=True)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
