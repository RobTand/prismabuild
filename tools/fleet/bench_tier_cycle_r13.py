#!/usr/bin/env python3
"""What the tier cycle reads with R13's dead instance on the queue (#1053, #1072).

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

**The first cycle after a restart (#1072).**  The live tier loop's first
cycle after the 09-24 publish of ``02b27a8804d3`` took 222.7 s: 137
dead-producer batch retirements across fourteen dead Stage A instances
(129.7 s) and a cold stale-mention census of 50 owners (87.9 s).
``--instances 14`` files fourteen variants of the dead instance (each its
own owner, attempt, template and movers; ten unretired batches each, so 140
retirements), and ``--dead-owner-pairs 25`` adds #1056's co-owned dead
owners (50 of them) through `bench_tier_cycle.build_dead_owners`.  Only the
first variant gets origin files: retirement never reads an origin file.
Before the first cycle the tier is announced once, as the loop that was
replaced left it, and a sampler thread reads the tier record's
``announced_unix`` every ``--age-sample-s`` for the whole run: each cycle row
carries the oldest age a reader could have seen during it.

``--py-spy`` runs the cycles in a child process under ``py-spy record``
(not ``--idle``: the loop is single-threaded and its cost is CPU or a
blocking write).  Each sample is attributed to its wrapper (``cold_cycle``
or ``steady_cycle``), to the step of ``cycle`` it was spent in, and to the
innermost PrismaBuild frame, with the call on that frame's line
(``json.dump``, ``os.fsync``, ...) and the stdlib codec below it.

Run it through PrismaBuild, never on the tier host, and never against the
live queue::

    pbrun.py --cwd <checkout> --tag sparky --cpus 2 --demand mem_gb=8 \\
        --priority -10 -- python3 tools/fleet/bench_tier_cycle_r13.py \\
        --work <scratch dir> --out <results dir> --instances 14 \\
        --dead-owner-pairs 25 --py-spy <path to py-spy>
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
import subprocess
import sys
import threading
import time

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parents[1] / "src"))
sys.path.insert(0, str(HERE.parents[1] / "tests"))

from prismabuild import pool  # noqa: E402
import prismabuild.produced_output as po  # noqa: E402
import bench_stage_adopt  # noqa: E402
import bench_tier_cycle  # noqa: E402
import stage_release  # noqa: E402

#: The calls counted, by the name each is reported under.
_COUNTED = {
    "stat": [(os, "stat")], "lstat": [(os, "lstat")],
    "open": [(builtins, "open"), (io, "open"), (os, "open")],
    "listdir": [(os, "listdir")], "scandir": [(os, "scandir")],
    "rename": [(os, "rename"), (os, "replace")], "unlink": [(os, "unlink")],
    "fsync": [(os, "fsync")],
    # The whole-document reads and writes of an instance's commitments, the
    # retirement's cost that does not depend on the box (#1072).
    "commitments_read": [(po, "_read_commitments")],
    "commitments_write": [(po, "_write_commitments")],
}

#: The sampler's own reads, taken before the counter wraps anything, so the
#: thread's reads are never counted as the cycle's.
_RAW_OPEN = builtins.open


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
            if threading.current_thread() is threading.main_thread():
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


class AgeSampler:
    """The tier record's age as a reader would compute it, sampled on a thread.

    ``time.time() - announced_unix`` read off the file every ``interval_s``,
    the same arithmetic as PQ's ``tier_record_age``.  ``mark`` starts a new
    window; ``oldest`` is the largest age seen since it.
    """

    def __init__(self, path: Path, interval_s: float) -> None:
        self.path = path
        self.interval_s = interval_s
        self._lock = threading.Lock()
        self._oldest: float | None = None
        self.samples = 0
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True,
                                        name="tier-age-sampler")

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=5)

    def mark(self) -> float | None:
        with self._lock:
            oldest, self._oldest = self._oldest, None
        return oldest

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                with _RAW_OPEN(self.path, "rb") as stream:
                    announced = json.loads(stream.read()).get("announced_unix")
            except (OSError, ValueError):
                announced = None
            if isinstance(announced, (int, float)):
                age = time.time() - float(announced)
                with self._lock:
                    self.samples += 1
                    if self._oldest is None or age > self._oldest:
                        self._oldest = age
            self._stop.wait(self.interval_s)


def _delta_events(events: list[dict]) -> dict[str, int]:
    return dict(collections.Counter(str(event.get("event")) for event in events))


def _event_samples(events: list[dict]) -> dict[str, dict]:
    """One example of each ``event/reason``, its long lists cut to three."""

    samples: dict[str, dict] = {}
    for event in events:
        key = f"{event.get('event')}/{event.get('reason', '')}"
        if key not in samples:
            samples[key] = {name: (value[:3] if isinstance(value, list)
                                   else value)
                            for name, value in event.items()}
    return samples


def _unretired(queue: pool.PoolQueue, scopes: list[str]) -> int:
    """Staged batches still unretired, over every installed instance."""

    count = 0
    for scope in scopes:
        batches = po._read_commitments(Path(scope) / "commitments.json")["batches"]
        count += sum(1 for entry in batches.values()
                     if isinstance(entry, dict) and not entry.get("origin_only")
                     and not po._batch_stage_retired(entry))
    return count


def _discover_record(stage: Path, host: str) -> dict[str, object]:
    record = bench_tier_cycle._tier_record(stage)
    record["host"] = host
    return record


# ---- the cycles (the child when profiled) ----------------------------------

def run_cycles(args) -> int:
    work = Path(args.work).resolve()
    setup = json.loads((work / "setup.json").read_text())
    bench_tier_cycle.HOST = setup["host"]
    host = setup["host"]
    queue = pool.PoolQueue(work / "pb-queue")
    stage = work / "stage"
    entries = Path(setup["writer_dir"])

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
    liveness = (tier_loop.Liveness(interval_s=args.interval_s)
                if callable(getattr(tier_loop, "Liveness", None)) else None)

    def discover(**_kwargs):
        return {bench_tier_cycle.TIER: _discover_record(stage, host)}

    sampler = AgeSampler(queue.tier_record_path(bench_tier_cycle.TIER),
                         args.age_sample_s)
    sampler.start()
    counter.install()
    rows = []

    def one(index: int) -> dict[str, object]:
        if args.writer and index:
            (entries / f"bench-writer-{index}.pt").write_bytes(b"w")
        counter.reset()
        events.clear()
        sampler.mark()
        started = time.perf_counter()
        unix = time.time()
        cpu = time.process_time()
        kwargs = {} if liveness is None else {"liveness": liveness}
        tier_loop.cycle(queue, host=host, source_pool="storage_pool",
                        receipts=receipts, discover=discover, **kwargs)
        wall = time.perf_counter() - started
        tick_events = _delta_events(events)
        retired = int(tick_events.get(po.DEAD_PRODUCER_BATCH_RETIRED_EVENT, 0))
        row: dict[str, object] = {
            "kind": "cold" if index == 0 else "steady",
            "wall_s": round(wall, 4),
            "cpu_s": round(time.process_time() - cpu, 4),
            "started_unix": round(unix, 3), "ended_unix": round(time.time(), 3),
            "oldest_tier_age_s": None,
            "retired": retired,
            "calls": counter.snapshot(),
            "tick_events": tick_events,
            "tick_event_samples": _event_samples(events)}
        recorded = getattr(tier_loop, "LAST_CYCLE", None)
        if isinstance(recorded, dict):
            row["last_cycle"] = {key: recorded.get(key)
                                 for key in ("cycle_seconds", "completed",
                                             "phases", "reads", "liveness")
                                 if key in recorded}
            tick_s = (recorded.get("phases") or {}).get("origin_retirement_tick")
            if retired and isinstance(tick_s, (int, float)):
                row["tick_s_per_retirement"] = round(float(tick_s) / retired, 4)
        oldest = sampler.mark()
        row["oldest_tier_age_s"] = None if oldest is None else round(oldest, 3)
        return row

    def cold_cycle(index: int) -> dict[str, object]:
        return one(index)

    def steady_cycle(index: int) -> dict[str, object]:
        return one(index)

    for index in range(args.cycles):
        rows.append(cold_cycle(index) if index == 0 else steady_cycle(index))
    sampler.stop()
    out = {"rows": rows, "age_samples": sampler.samples,
           "r13_unretired_after": _unretired(queue, setup["scopes"])}
    Path(args.cycles_out).write_text(json.dumps(out, indent=1) + "\n")
    return 0


# ---- attributing samples ----------------------------------------------------

#: The calls a frame's source line may carry, by the name each is reported as.
_CALLS = (
    ("json.dumps(", "json.dumps"), ("json.dump(", "json.dump"),
    ("json.loads(", "json.loads"), ("json.load(", "json.load"),
    ("os.fsync(", "os.fsync"), ("os.replace(", "os.replace"),
    ("read_text(", "read_text"), ("os.scandir(", "scandir"),
    ("os.listdir(", "listdir"), ("os.lstat(", "lstat"), ("os.stat(", "stat"),
    ("os.unlink(", "unlink"), ("unlink(", "unlink"), (".write(", "write"),
)
BENCH_FILE = os.path.basename(__file__)


def _own(file: str) -> bool:
    """Is ``file`` PrismaBuild's own source, as py-spy prints it shortened?

    py-spy prints ``tier_loop.py`` or ``prismabuild/pool.py``, never the
    full path, so a frame is ours when the name resolves to a file in this
    checkout's ``tools/fleet`` or ``src`` (`bench_stage_adopt._resolve`).
    This bench's own frames are the wrappers around the steps, not an
    operation.
    """

    if os.path.basename(file) == BENCH_FILE:
        return False
    resolved = bench_stage_adopt._resolve(file)
    return resolved is not None and ROOT in resolved.parents


def classify(frames: list[tuple[str, str, int]]) -> tuple[str, str, str]:
    """``(wrapper, step, operation)`` for one sample.

    ``operation`` is the innermost PrismaBuild frame, ``file:function``, with
    the call on its line and the stdlib codec below it when there is one.
    """

    wrapper = "setup"
    step = "outside_cycle"
    for index, (function, file, _number) in enumerate(frames):
        base = os.path.basename(file)
        if base == BENCH_FILE and function in ("cold_cycle", "steady_cycle"):
            wrapper = function
        if base == "tier_loop.py" and function in ("cycle", "_cycle"):
            step = function
            # The first frame below `_cycle` that is not this bench's own
            # counting wrapper names the step.
            for below, file_below, _line_below in frames[index + 1:]:
                if os.path.basename(file_below) != BENCH_FILE:
                    step = below
                    break
    codec = ""
    operation = "other"
    for function, file, number in reversed(frames):
        base = os.path.basename(file)
        if not codec and base in ("encoder.py", "decoder.py") and "json" in file:
            codec = "json-encode" if base == "encoder.py" else "json-decode"
        if not _own(file):
            continue
        text = bench_stage_adopt._line(file, number)
        call = next((name for needle, name in _CALLS if needle in text), "")
        operation = f"{base}:{function}"
        if call:
            operation += f" {call}"
        if codec:
            operation += f" [{codec}]"
        break
    return wrapper, step, operation


def analyze(profile: Path, rate: int) -> dict[str, object]:
    if not profile.exists():
        return {"missing": str(profile)}
    by_step: dict[str, collections.Counter] = collections.defaultdict(
        collections.Counter)
    by_op: dict[str, collections.Counter] = collections.defaultdict(
        collections.Counter)
    for raw in profile.read_text(errors="replace").splitlines():
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
            "samples": total,
            "seconds": round(total / rate, 3),
            "by_step": {name: {"s": round(count / rate, 3),
                               "share": round(count / total, 4)}
                        for name, count in by_step[wrapper].most_common(12)},
            "top_frames": {name: {"s": round(count / rate, 3),
                                  "share": round(count / total, 4)}
                           for name, count in by_op[wrapper].most_common(20)},
            # Each step's own frames, as shares of that step's samples: the
            # retirement's are what #1072's before/after compares.
            "step_frames": {
                step: {name.partition(" :: ")[2]: {
                    "s": round(count / rate, 3),
                    "share": round(count / by_step[wrapper][step], 4)}
                    for name, count in collections.Counter({
                        name: count for name, count in by_op[wrapper].items()
                        if name.startswith(f"{step} :: ")}).most_common(10)}
                for step, _count in by_step[wrapper].most_common(4)},
        }
    return out


# ---- main --------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--work", default="",
                        help="scratch directory; emptied first; never /tmp")
    parser.add_argument("--out", default="",
                        help="where summary.json, the cycles and the profile go")
    parser.add_argument("--cycles", type=int, default=6,
                        help="cycles to run: one cold, the rest steady")
    parser.add_argument("--writer", action="store_true",
                        help="before each cycle after the first, create one "
                             "file in the directory R13's paths are in, as a "
                             "relaunch writing there does")
    parser.add_argument("--instances", type=int, default=1,
                        help="dead instances to file, each a variant of R13's "
                             "with ten unretired batches (the live first cycle "
                             "of 2026-09-24 met fourteen)")
    parser.add_argument("--dead-owner-pairs", type=int, default=0,
                        help="pairs of co-owned dead owners (#1056); the live "
                             "first cycle censused 50 owners")
    parser.add_argument("--dead-entries", type=int, default=1680,
                        help="entries in each dead owner's fragment")
    parser.add_argument("--py-spy", default="",
                        help="py-spy executable; empty runs unprofiled")
    parser.add_argument("--rate", type=int, default=100,
                        help="py-spy samples per second")
    parser.add_argument("--age-sample-s", type=float, default=0.5,
                        help="seconds between reads of the tier record's age")
    parser.add_argument("--interval-s", type=float, default=5.0,
                        help="the serving loop's --interval-s, which a loop "
                             "that budgets its cycle is told (the live tier "
                             "loop runs --interval-s 5)")
    parser.add_argument("--run-cycles", action="store_true",
                        help=argparse.SUPPRESS)
    parser.add_argument("--cycles-out", default="", help=argparse.SUPPRESS)
    parser.add_argument("--analyze", default="",
                        help="attribute the samples of a recorded raw py-spy "
                             "profile, print the result and exit; run it from "
                             "the checkout that recorded the profile, whose "
                             "line numbers the samples carry")
    args = parser.parse_args(argv)
    if args.analyze:
        print(json.dumps(analyze(Path(args.analyze), args.rate), indent=1,
                         sort_keys=True))
        return 0
    if not args.out or (not args.run_cycles and not args.work):
        parser.error("--work and --out are required")
    if args.run_cycles:
        return run_cycles(args)

    work = Path(args.work).resolve()
    out = Path(args.out).resolve()
    for guarded in (work, out):
        if str(guarded) == "/tmp" or str(guarded).startswith("/tmp/"):
            raise SystemExit(f"refusing {guarded}: never /tmp")
    if args.py_spy and not os.access(args.py_spy, os.X_OK):
        raise SystemExit(f"refusing: py-spy {args.py_spy!r} is not executable")
    if work.exists():
        shutil.rmtree(work)
    work.mkdir(parents=True)
    out.mkdir(parents=True, exist_ok=True)

    # The tier record must name this box, as the live one names the tier
    # host, so the in-process egress is the one taken.
    host = socket.gethostname()
    bench_tier_cycle.HOST = host
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
        files_per_range=64, ready_noise=30, claimed_noise=28,
        dead_owner_pairs=args.dead_owner_pairs,
        dead_entries=args.dead_entries)
    built = time.monotonic()
    shape = bench_tier_cycle.build_queue(queue, stage, shape_args)
    import r13_1053_replay

    scopes: list[str] = []
    writer_dir = ""
    unretired = 0
    for variant in range(max(1, args.instances)):
        replay = r13_1053_replay.install(
            queue, prefix=work / "origin" / f"adjoint-v{variant}", stage=stage,
            origin_files=variant == 0, variant=variant)
        scopes.append(str(replay.scope))
        unretired += len(replay.unretired)
        if variant == 0:
            writer_dir = str(Path(next(iter(
                replay.prewrite_paths.values()))[0]).parent)
    shape["r13_instances"] = len(scopes)
    shape["r13_unretired"] = unretired
    setup_s = time.monotonic() - built
    (work / "setup.json").write_text(json.dumps({
        "host": host, "scopes": scopes, "writer_dir": writer_dir}))
    # The loop that was replaced announced the tier once; its record is what
    # a reader ages until the new loop's first announcement.
    queue.announce_tier(_discover_record(stage, host))

    cycles_out = out / "cycles.json"
    argv_child = [sys.executable, str(Path(__file__).resolve()),
                  "--run-cycles", "--work", str(work), "--out", str(out),
                  "--cycles", str(args.cycles),
                  "--age-sample-s", str(args.age_sample_s),
                  "--interval-s", str(args.interval_s),
                  "--cycles-out", str(cycles_out)]
    if args.writer:
        argv_child.append("--writer")
    profile = out / "tier-cycle-r13.pyspy.txt"
    if args.py_spy:
        argv_child = [args.py_spy, "record", "--nonblocking",
                      "--rate", str(args.rate), "--format", "raw",
                      "--output", str(profile), "--"] + argv_child
    log = out / "cycles.log"
    with open(log, "w") as stream:
        code = subprocess.call(argv_child, stdout=stream,
                               stderr=subprocess.STDOUT)
    child = json.loads(cycles_out.read_text()) if cycles_out.exists() else {}
    rows = list(child.get("rows") or [])
    summary = {
        "checkout": str(HERE.parents[1]), "host": os.uname().nodename,
        "python": sys.version.split()[0], "shape": shape,
        "setup_s": round(setup_s, 2), "writer": args.writer,
        "returncode": code, "cycles": rows,
        "age_samples": child.get("age_samples"),
        "r13_unretired_after": child.get("r13_unretired_after"),
        "started_unix": rows[0].get("started_unix") if rows else None,
        "ended_unix": rows[-1].get("ended_unix") if rows else None,
        "profile": analyze(profile, args.rate) if args.py_spy else None,
    }
    if code == 0 and not rows:
        # py-spy exits 0 whatever its child did: a child that ran no cycle
        # is a failed bench, and its log says why.
        code = 1
        summary["returncode"] = code
        summary["child_log_tail"] = log.read_text(errors="replace")[-2000:]
    (out / "summary.json").write_text(json.dumps(summary, indent=1) + "\n")
    print(json.dumps({
        "returncode": code, "shape": shape,
        "child_log_tail": summary.get("child_log_tail"),
        "cycles": [{key: row.get(key) for key in
                    ("kind", "wall_s", "cpu_s", "retired",
                     "tick_s_per_retirement", "oldest_tier_age_s",
                     "tick_events")} for row in rows],
        "phases_cold": (rows[0].get("last_cycle") or {}).get("phases")
        if rows else None,
        "r13_unretired_after": summary["r13_unretired_after"],
        "profile": summary["profile"]}, indent=1))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
