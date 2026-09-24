#!/usr/bin/env python3
"""What the tier cycle reads with R13's dead instance on the queue (#1053).

`bench_tier_cycle.py` builds a queue with the live queue's widths (#992) but
no produced-output scopes, so the origin-retirement tick has nothing to
walk.  This builds the same queue, installs the dead R13 Stage A instance
from `tests/fixtures/r13_1053_dead_instance.json.gz` (436 committed batches,
28 outstanding prewrites, their 27,904 committed and 1,792 prewritten origin
files, under a synthetic prefix; `tests/r13_1053_replay.py`), and runs the
real `tier_loop.cycle`: one cold cycle, then steady ones.

It counts the filesystem calls each cycle makes -- ``stat``/``lstat``,
``open``, ``listdir``/``scandir`` and the rename/unlink calls -- in total and
inside the two steps #1053 changes, ``stage_release.sweep`` and
``produced_output.origin_retirement_tick``, beside the cycle's own
``LAST_CYCLE`` record (its per-step seconds and its kept-record counters).
The same script runs against the tree before the change and after it; the
difference is the change's cost.  ``--writer`` creates one file in the
directory every R13 path is in before each steady cycle, as the relaunch
writing its own batches there does: a cycle that remembers a decision by
that directory's version decides again.

Run it through PrismaBuild, never on the tier host, and never against the
live queue::

    pbrun.py --cwd <checkout> --tag sparky --cpus 2 --demand mem_gb=8 \\
        --priority -10 -- python3 tools/fleet/bench_tier_cycle_r13.py \\
        --work <scratch dir> --out <results dir>
"""
from __future__ import annotations

import argparse
import builtins
import collections
import io
import json
import os
from pathlib import Path
import shutil
import socket
import sys
import time

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parents[1] / "src"))
sys.path.insert(0, str(HERE.parents[1] / "tests"))

from prismabuild import pool  # noqa: E402
import prismabuild.produced_output as po  # noqa: E402
import bench_tier_cycle  # noqa: E402
import stage_release  # noqa: E402

#: The calls counted, by the name each is reported under.
_COUNTED = {
    "stat": [(os, "stat")], "lstat": [(os, "lstat")],
    "open": [(builtins, "open"), (io, "open"), (os, "open")],
    "listdir": [(os, "listdir")], "scandir": [(os, "scandir")],
    "rename": [(os, "rename"), (os, "replace")], "unlink": [(os, "unlink")],
}


class Counter:
    """Filesystem calls, counted globally and inside named steps."""

    def __init__(self) -> None:
        self.total: collections.Counter = collections.Counter()
        self.steps: dict[str, collections.Counter] = collections.defaultdict(
            collections.Counter)
        self._active: list[str] = []
        self._saved: list[tuple[object, str, object]] = []

    def install(self) -> None:
        for name, targets in _COUNTED.items():
            for module, attribute in targets:
                original = getattr(module, attribute)
                self._saved.append((module, attribute, original))
                setattr(module, attribute, self._wrap(name, original))

    def _wrap(self, name: str, original):
        def counted(*args, **kwargs):
            self.total[name] += 1
            for step in self._active:
                self.steps[step][name] += 1
            return original(*args, **kwargs)
        return counted

    def step(self, name: str, function):
        def stepped(*args, **kwargs):
            self._active.append(name)
            try:
                return function(*args, **kwargs)
            finally:
                self._active.pop()
        return stepped

    def snapshot(self) -> dict[str, object]:
        return {"total": dict(self.total),
                "steps": {name: dict(counts)
                          for name, counts in self.steps.items()}}

    def reset(self) -> None:
        self.total.clear()
        self.steps.clear()


def _delta_events(events: list[dict]) -> dict[str, int]:
    return dict(collections.Counter(str(event.get("event")) for event in events))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--work", required=True,
                        help="scratch directory; emptied first; never /tmp")
    parser.add_argument("--out", required=True,
                        help="where summary.json goes")
    parser.add_argument("--cycles", type=int, default=6,
                        help="cycles to run: one cold, the rest steady")
    parser.add_argument("--writer", action="store_true",
                        help="before each cycle after the first, create one "
                             "file in the directory R13's paths are in, as a "
                             "relaunch writing there does")
    args = parser.parse_args(argv)
    work = Path(args.work).resolve()
    out = Path(args.out).resolve()
    for guarded in (work, out):
        if str(guarded) == "/tmp" or str(guarded).startswith("/tmp/"):
            raise SystemExit(f"refusing {guarded}: never /tmp")
    if work.exists():
        shutil.rmtree(work)
    work.mkdir(parents=True)
    out.mkdir(parents=True, exist_ok=True)

    # The tier record must name this box, as the live one names the tier
    # host, so the in-process egress is the one taken.
    bench_tier_cycle.HOST = socket.gethostname()
    host = bench_tier_cycle.HOST
    queue = pool.PoolQueue(work / "pb-queue")
    queue.ensure_layout()
    stage = work / "stage"
    stage.mkdir()
    assert stage_release.register_stage_root(
        queue, tier_id=bench_tier_cycle.TIER, stage_root=stage) == "registered"
    shape_args = argparse.Namespace(
        empty_dirs=404, small_dirs=30, big_fragments="90000,60000,50000,40000",
        produced_fragment_dirs=1957, done=28943, failed=7142, withdrawn=1877,
        receipts=6434, passes=1324, live_consumers=6, phases=3,
        files_per_range=64, ready_noise=30, claimed_noise=28)
    built = time.monotonic()
    shape = bench_tier_cycle.build_queue(queue, stage, shape_args)
    import r13_1053_replay

    replay = r13_1053_replay.install(queue, prefix=work / "origin" / "adjoint",
                                     stage=stage)
    entries = Path(next(iter(replay.prewrite_paths.values()))[0]).parent
    shape["r13_batches"] = len(replay.paths)
    shape["r13_prewrites"] = len(replay.prewrites)
    setup_s = time.monotonic() - built

    import tier_loop

    counter = Counter()
    events: list[dict] = []
    tick = po.origin_retirement_tick

    def recorded_tick(*a, **k):
        found = tick(*a, **k)
        events.extend(found)
        return found

    po.origin_retirement_tick = counter.step("origin_retirement_tick",
                                             recorded_tick)
    stage_release.sweep = counter.step("stage_release.sweep",
                                       stage_release.sweep)
    receipts = tier_loop.ReceiptCache()

    def discover(**_kwargs):
        record = bench_tier_cycle._tier_record(stage)
        record["host"] = host
        return {bench_tier_cycle.TIER: record}

    counter.install()
    rows = []
    for index in range(args.cycles):
        if args.writer and index:
            (entries / f"bench-writer-{index}.pt").write_bytes(b"w")
        counter.reset()
        events.clear()
        started = time.perf_counter()
        cpu = time.process_time()
        tier_loop.cycle(queue, host=host, source_pool="storage_pool",
                        receipts=receipts, discover=discover)
        row = {"kind": "cold" if index == 0 else "steady",
               "wall_s": round(time.perf_counter() - started, 4),
               "cpu_s": round(time.process_time() - cpu, 4),
               "calls": counter.snapshot(),
               "tick_events": _delta_events(events)}
        recorded = getattr(tier_loop, "LAST_CYCLE", None)
        if isinstance(recorded, dict):
            row["last_cycle"] = {key: recorded.get(key)
                                 for key in ("cycle_seconds", "completed",
                                             "phases", "reads")
                                 if key in recorded}
        rows.append(row)

    retired = sum(1 for batch_id in replay.unretired
                  if po._batch_stage_retired(replay.entry(queue.root, batch_id)))
    summary = {
        "checkout": str(HERE.parents[1]), "host": os.uname().nodename,
        "python": sys.version.split()[0], "shape": shape,
        "setup_s": round(setup_s, 2), "writer": args.writer, "cycles": rows,
        "r13_unretired_after": len(replay.unretired) - retired,
    }
    (out / "summary.json").write_text(json.dumps(summary, indent=1) + "\n")
    print(json.dumps({"cycles": [{key: row[key] for key in
                                  ("kind", "wall_s", "cpu_s", "calls",
                                   "tick_events")} for row in rows],
                      "r13_unretired_after": summary["r13_unretired_after"]},
                     indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
