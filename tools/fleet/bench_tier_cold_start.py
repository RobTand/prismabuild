#!/usr/bin/env python3
"""What a freshly started tier loop's first cycle costs over a cold receipt set (#1153).

On 2026-09-25 the publish of ``9ea9dc9ac4cd`` re-executed the tier loop on
dl380g10.  The new process started with an empty ``ReceiptCache`` and read
all 12,295 prewarm and movement receipts cold, serially, at about 4 a
second on the loaded HDD pool, before its first announcement.  Nothing
re-announced the stage record meanwhile, and a Stage B row died in its
staged wait 112 s later.

This harness builds a scratch queue with ``--receipts`` movement receipts
(``bench_tier_cycle``'s generator: the live schema, one tier), runs one
cycle as the replaced loop before they are filed, so the tier record on disk
is that loop's last announcement, then starts a fresh loop the way the role
does and times its first cycle.  Every receipt's read sleeps ``--read-ms``
first, standing in for the cold seek (the parse is where the tier loop opens
the file; the ``stat`` before it is not charged).  It reports:

* ``first_cycle_s`` and ``receipts_s``: the fresh loop's first cycle and its
  ``receipts`` step, wall time;
* ``first_write_s``: from the start to the first tier-record write;
* ``oldest_age_s``: the oldest the tier record got, read off the file every
  50 ms the way a reader reads it, against ``L`` = ``pool.OFFER_TIMEOUT_S``;
* ``profile``: the top functions of a cProfile of the start and the first
  cycle, by own time (``tottime``).  On Python 3.12 and later cProfile
  records every thread, the reader threads and the age poller included, so
  cumulative times overlap across threads and can exceed the wall time; own
  time says where each thread spent it;
* ``warm_receipts_s`` and ``warm_serial_receipts_s``: the ``receipts`` step
  of later cycles, each after one new receipt is filed, so the movement
  directory is listed and every entry stat-ed again but only the new one
  parsed.  The cycles alternate between the configured readers and one
  reader, ``--warm-cycles`` of each, in the same process on the same queue.
  This is the steady state, where handing entries to readers is overhead
  rather than overlap.

The harness names which tree it measured (``fix_present``), so the same file
reads a before tree and an after tree.  Run it through PrismaBuild on a
GB10, once per checkout, never on the tier host and never against the live
stage or queue::

    pbrun.py --cwd <checkout> --tag gb10 --cpus 2 --demand mem_gb=2 \\
        --priority -10 -- python3 tools/fleet/bench_tier_cold_start.py \\
        --work <scratch dir>
"""
from __future__ import annotations

import argparse
import cProfile
import io
import json
import os
from pathlib import Path
import pstats
import shutil
import sys
import tempfile
import threading
import time

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parents[1] / "src"))

from prismabuild import pool  # noqa: E402
import bench_tier_cycle  # noqa: E402
import stage_release  # noqa: E402
import tier_loop  # noqa: E402

TIER = bench_tier_cycle.TIER
HOST = bench_tier_cycle.HOST
#: Real mountpoints a scratch root must never sit under.
FORBIDDEN_ROOTS = ("/stage", "/ram", "/mnt/shared", "/tmp")
#: The live role's ``--interval-s``.
INTERVAL_S = 5.0


def _start(queue: pool.PoolQueue):
    """The role's start path; before #1153, what ``_serve`` built."""

    start = getattr(tier_loop, "_start", None)
    if start is None:
        return tier_loop.ReceiptCache(), tier_loop.Liveness(interval_s=INTERVAL_S)
    return start(queue, host=HOST, interval_s=INTERVAL_S)


def _discover(stage: Path):
    record = bench_tier_cycle._tier_record(stage)
    return lambda **_kwargs: {TIER: dict(record)}


class _Ages(threading.Thread):
    """The tier record's age, read off the file every 50 ms."""

    def __init__(self, queue: pool.PoolQueue) -> None:
        super().__init__(daemon=True)
        self.path = queue.tier_record_path(TIER)
        self.halt = threading.Event()
        self.oldest = 0.0
        self.dead_polls = 0
        self.polls = 0

    def run(self) -> None:
        while not self.halt.is_set():
            try:
                stamp = float(json.loads(self.path.read_text())["announced_unix"])
            except (OSError, ValueError, KeyError, TypeError):
                stamp = None
            if stamp is not None:
                age = time.time() - stamp
                self.oldest = max(self.oldest, age)
                self.polls += 1
                if age > pool.OFFER_TIMEOUT_S:
                    self.dead_polls += 1
            self.halt.wait(0.05)


def run(args) -> dict[str, object]:
    work = Path(args.work).resolve()
    for root in FORBIDDEN_ROOTS:
        if work == Path(root) or Path(root) in work.parents:
            raise SystemExit(f"refusing a scratch root under {root}: {work}")
    work.mkdir(parents=True, exist_ok=True)
    scratch = Path(tempfile.mkdtemp(prefix="bench-1153-", dir=work))
    try:
        queue = pool.PoolQueue(scratch / "pb-queue")
        queue.ensure_layout()
        stage = scratch / "stage"
        stage.mkdir()
        queue.mint_tier_capacity(TIER, {"stage_gib": 700})
        stage_release.register_stage_root(queue, tier_id=TIER, stage_root=stage)
        discover = _discover(stage)

        # The replaced loop's last cycle: its record is what is on disk.
        tier_loop.cycle(queue, host=HOST, source_pool="storage_pool",
                        receipts=tier_loop.ReceiptCache(), discover=discover)
        bench_tier_cycle._receipts(queue, args.receipts)
        # Past the coarse tick, so every receipt's version is keepable.
        time.sleep(0.05)

        if args.readers is not None:
            tier_loop.RECEIPT_READERS = int(args.readers)
        readers = int(getattr(tier_loop, "RECEIPT_READERS", 1))
        delay = args.read_ms / 1000.0
        receipt_dirs = {str(queue.root / pool.MOVERS),
                        str(queue.root / pool.PREWARM)}
        real_read = pool._read_json
        reads = [0]
        lock = threading.Lock()

        def slow_read(path, *a, **kw):
            if str(Path(path).parent) in receipt_dirs:
                with lock:
                    reads[0] += 1
                time.sleep(delay)
            return real_read(path, *a, **kw)

        pool._read_json = slow_read
        writes: list[float] = []
        real_announce = queue.announce_tier

        def announce(record, **kw):
            path = real_announce(record, **kw)
            writes.append(time.monotonic())
            return path

        queue.announce_tier = announce          # type: ignore[method-assign]

        ages = _Ages(queue)
        ages.start()
        profiler = cProfile.Profile()
        started = time.monotonic()
        profiler.enable()
        receipts, liveness = _start(queue)
        tier_loop.cycle(queue, host=HOST, source_pool="storage_pool",
                        receipts=receipts, discover=discover, liveness=liveness)
        profiler.disable()
        finished = time.monotonic()
        ages.halt.set()
        ages.join(5.0)

        text = io.StringIO()
        stats = pstats.Stats(profiler, stream=text)
        stats.sort_stats("tottime").print_stats(args.profile_top)
        if args.profile_out:
            stats.dump_stats(args.profile_out)
        top = []
        for (filename, line, name), (cc, nc, tt, ct, _callers) in sorted(
                stats.stats.items(), key=lambda item: -item[1][2])[:args.profile_top]:
            top.append({"function": f"{Path(filename).name}:{line}:{name}",
                        "calls": nc, "tottime_s": round(tt, 3),
                        "cumtime_s": round(ct, 3)})
        phases = tier_loop.LAST_CYCLE.get("phases", {})
        first_line = tier_loop.LAST_CYCLE
        # Paired in one process, alternating, so the box's load is shared:
        # the configured readers, then one reader, per pair.
        warm: list[float] = []
        warm_serial: list[float] = []
        for index in range(2 * args.warm_cycles):
            serial = index % 2 == 1
            tier_loop.RECEIPT_READERS = 1 if serial else readers
            directory = queue.root / pool.MOVERS
            (directory / f"warm-{index:04d}.json").write_text(json.dumps(
                {"schema": "pb.bench-1153-noise.v1", "unix": time.time()}))
            time.sleep(0.05)
            tier_loop.cycle(queue, host=HOST, source_pool="storage_pool",
                            receipts=receipts, discover=discover,
                            liveness=liveness)
            (warm_serial if serial else warm).append(round(float(
                tier_loop.LAST_CYCLE.get("phases", {}).get("receipts", 0.0)), 4))
        tier_loop.RECEIPT_READERS = readers
        pool._read_json = real_read
        return {
            "bench": "tier-cold-start-1153",
            "fix_present": callable(getattr(tier_loop, "_start", None)),
            "readers": readers, "receipts": args.receipts,
            "read_ms": args.read_ms, "reads_charged": reads[0],
            "first_cycle_s": round(finished - started, 3),
            "receipts_s": round(float(phases.get("receipts", 0.0)), 3),
            "first_write_s": (round(writes[0] - started, 3) if writes else None),
            "tier_writes": len(writes),
            "oldest_age_s": round(ages.oldest, 3),
            "age_polls": ages.polls, "dead_polls": ages.dead_polls,
            "bound_s": pool.OFFER_TIMEOUT_S,
            "liveness": first_line.get("liveness"),
            "warm_receipts_s": warm,
            "warm_serial_receipts_s": warm_serial,
            "profile": top,
            "cpu_count": os.cpu_count(),
        }
    finally:
        if not args.keep:
            shutil.rmtree(scratch, ignore_errors=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--work", required=True,
                        help="scratch directory; never under /stage, /ram, "
                             "/mnt/shared or /tmp")
    parser.add_argument("--receipts", type=int, default=12000,
                        help="cold movement receipts (12,295 live)")
    parser.add_argument("--read-ms", type=float, default=20.0,
                        help="injected latency of each receipt read")
    parser.add_argument("--readers", type=int, default=None,
                        help="override tier_loop.RECEIPT_READERS")
    parser.add_argument("--warm-cycles", type=int, default=5,
                        help="later cycles timed after one new receipt each")
    parser.add_argument("--profile-top", type=int, default=15)
    parser.add_argument("--profile-out", default=None,
                        help="write the raw cProfile stats here")
    parser.add_argument("--keep", action="store_true")
    args = parser.parse_args(argv)
    result = run(args)
    print("bench-1153 " + json.dumps(result, sort_keys=True, default=str),
          flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
