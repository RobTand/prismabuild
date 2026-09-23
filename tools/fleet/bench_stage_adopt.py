#!/usr/bin/env python3
"""What one staged entry costs a stage mover to adopt or copy, and where (#981).

On 2026-09-23 a resubmitted Stage A measurement's movers adopted bytes their
predecessor had already staged, and adoption ran at about 100 ms per entry --
nine times slower than the original copy -- with every mover thread on the
host parked on the stage ownership lock.  This is the hermetic reproduction:
a synthetic queue, stage root and residency forest shaped like the live one
on dl380g10, and the real ``stage_move.py`` run as real processes, so the
lock that serializes them is the real inter-process ``fcntl`` lock and the
per-process ``RLock`` in front of it.

Five scenarios, in order, each on its own windows of small entries (the cost
under study follows entries, not bytes):

* ``copy1`` -- one mover copies a fresh window (the copy path);
* ``adopt1`` -- one mover of another consumer adopts that window (#901);
* ``copy3`` -- three movers copy three fresh windows at once;
* ``adopt3`` -- three movers adopt those three windows at once;
* ``mixed`` -- one mover copies a fresh window while two adopt, which is the
  shape the copy mover's idle tail was measured under.

Every mover runs under ``py-spy record --idle --threads`` so a thread
waiting on a lock is sampled where it waits.  The analysis attributes each
worker-thread sample to the innermost frame that names a cost -- a lock
wait, a directory scan, a stat, a parse, the copy's read, write, hash or
fsync, the publication poll's sleep -- by reading the source line the sample
sits on, so the same analysis reads a before and an after tree.

Every adopt scenario is checked, not just timed: each destination must keep
the inode its predecessor published (adopted, never replaced), and every
receipt must be complete.

Run it through PrismaBuild on a GB10, never on the tier host, and never
against the live stage or queue::

    pbrun.py --cwd <checkout> --tag sparky --cpus 8 --demand mem_gb=8 \\
        --priority -10 -- python3 tools/fleet/bench_stage_adopt.py \\
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

from prismabuild import core as pb  # noqa: E402
from prismabuild import pool, reader_lease, residency_map  # noqa: E402
import stage_move  # noqa: E402
import stage_release  # noqa: E402

TIER = "prismabuild-stage:dl380g10"
ENTRY_BYTES = 4096


def _key(label: str) -> str:
    return hashlib.sha256(f"bench-981:{label}".encode()).hexdigest()


# ---- the live-shaped residency forest --------------------------------------

def build_forest(queue: pool.PoolQueue, stage: Path, *, empty_dirs: int,
                 small_dirs: int, big_fragments: list[int],
                 produced_fragment_dirs: int) -> dict[str, int]:
    """Noise shaped like dl380g10's residency root on 2026-09-23.

    Measured there: 434 directories, 404 of them empty consumer directories;
    about 100 fragment files, median 1.6 KB, the largest 23.6 MB; a
    ``produced-output-fragments`` directory of 1,957 subdirectories and three
    smaller produced-output directories; ``leases`` and ``material``; and a
    ``<consumer>.map.json`` beside most consumer directories.  None of it
    names a destination the scenarios stage, which is the live shape: most
    of the forest is irrelevant to any one lookup and is scanned for it
    anyway.
    """

    root = queue.residency_fragment_root()
    root.mkdir(parents=True, exist_ok=True)
    counts = collections.Counter()
    for index in range(empty_dirs):
        consumer = _key(f"empty{index}")
        (root / consumer).mkdir(exist_ok=True)
        (root / f"{consumer}.map.json").write_text("{}\n")
        counts["empty_dirs"] += 1

    def noise_fragment(consumer: str, mover: str, entries: int, tag: str) -> None:
        named = {}
        for number in range(entries):
            declared = f"/noise/{tag}/part-{number}.bin"
            named[residency_map.residency_map_key(declared, 0)] = {
                "stage_path": str(stage / "noise" / tag / f"part-{number}.bin"),
                "bytes": ENTRY_BYTES, "offset": 0, "sha256": "e" * 64}
        residency_map.write_fragment(root, {
            "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
            "consumer_action_key": consumer, "mover_action_key": mover,
            "tier_id": TIER, "stage_root": str(stage),
            "manifest_sha256": "f" * 64, "entries": named})
        counts["fragments"] += 1
        counts["fragment_entries"] += entries

    for index in range(small_dirs):
        consumer = _key(f"small{index}")
        (root / f"{consumer}.map.json").write_text("{}\n")
        for mover_index in range(1 + index % 9):
            noise_fragment(consumer, _key(f"small{index}-{mover_index}"), 5,
                           f"s{index}-{mover_index}")
    for index, entries in enumerate(big_fragments):
        consumer = _key(f"big{index}")
        noise_fragment(consumer, _key(f"big{index}-mover"), entries, f"b{index}")
    produced = root / "produced-output-fragments"
    for index in range(produced_fragment_dirs):
        (produced / _key(f"pof{index}")).mkdir(parents=True, exist_ok=True)
    for name, many in (("produced-output-batches", 27),
                       ("produced-output-scopes", 35)):
        for index in range(many):
            (root / name / _key(f"{name}{index}")).mkdir(parents=True,
                                                          exist_ok=True)
    templates = root / "produced-output-templates"
    templates.mkdir(exist_ok=True)
    for index in range(35):
        (templates / f"template-{index}.json").write_text(json.dumps(
            {"schema": "prismaquant.prismabuild.produced_output_template.v1",
             "index": index}) + "\n")
    leases = root / "leases"
    leases.mkdir(exist_ok=True)
    for index in range(342):
        (leases / f"{_key(f'lease{index}')}.json").write_text("{}\n")
    return dict(counts)


# ---- windows ----------------------------------------------------------------

def make_window(work: Path, name: str, entries: int,
                ranges_per_file: int) -> dict[str, object]:
    """One manifest of ``entries`` small ranges over real source files."""

    mount = work / "sources"
    base = mount / name
    base.mkdir(parents=True, exist_ok=True)
    listed = []
    files = (entries + ranges_per_file - 1) // ranges_per_file
    made = 0
    for number in range(files):
        source = base / f"shard-{number:05d}.bin"
        ranges = min(ranges_per_file, entries - made)
        payload = hashlib.sha256(f"{name}:{number}".encode()).digest() \
            * (ENTRY_BYTES * ranges // 32 + 1)
        payload = payload[:ENTRY_BYTES * ranges]
        source.write_bytes(payload)
        for part in range(ranges):
            chunk = payload[part * ENTRY_BYTES:(part + 1) * ENTRY_BYTES]
            listed.append({"path": str(source), "offset": part * ENTRY_BYTES,
                           "bytes": ENTRY_BYTES,
                           "sha256": hashlib.sha256(chunk).hexdigest()})
        made += ranges
    body = {"schema": pb.DATA_MANIFEST_SCHEMA_V1, "produced_by": {},
            "annotations": {}, "mount_prefix": str(mount),
            "entries": listed, "entry_count": len(listed),
            "total_bytes": len(listed) * ENTRY_BYTES}
    raw = pb._canonical_file_bytes(pb.validate_data_manifest(body))
    path = work / f"manifest-{name}.json"
    path.write_bytes(raw)
    return {"name": name, "path": path, "sha256": hashlib.sha256(raw).hexdigest(),
            "entries": listed, "mount": mount, "bytes": len(listed) * ENTRY_BYTES}


def destinations(stage: Path, window: dict[str, object]) -> list[Path]:
    return [stage / stage_move.stage_relative(
        str(entry["path"]), int(entry["offset"]), int(entry["bytes"]),
        mount_prefix=str(window["mount"]))
        for entry in window["entries"]]  # type: ignore[union-attr]


def inodes(paths: list[Path]) -> list[int | None]:
    out = []
    for path in paths:
        try:
            out.append(os.stat(path).st_ino)
        except OSError:
            out.append(None)
    return out


# ---- running movers ---------------------------------------------------------

def mover_argv(args, queue: pool.PoolQueue, stage: Path, window, mover: str,
               consumer: str, receipt: Path) -> list[str]:
    return [sys.executable, str(HERE / "stage_move.py"),
            "--pool-root", str(queue.root), "--action-key", mover,
            "--consumer-action-key", consumer, "--tier-id", TIER,
            "--stage-root", str(stage), "--manifest", str(window["path"]),
            "--manifest-sha256", str(window["sha256"]),
            "--range-start-bytes", "0",
            "--range-end-bytes", str(window["bytes"]),
            "--readers", str(args.workers), "--max-readers", str(args.workers),
            "--warm-after-copy", "never", "--unpaced",
            "--receipt", str(receipt)] + (
                ["--progress-interval-s", str(args.progress_interval_s)]
                if args.progress_interval_s is not None else [])


def mover_env(args, out: Path, scenario: str, label: str) -> dict[str, str] | None:
    """The progress channel a worker would give the mover (#1010), or ``None``.

    With ``--progress-interval-s`` each mover reports into its own file under
    ``out``, as a sealed mover does into ``claimed/<key>.progress``.
    """

    if args.progress_interval_s is None:
        return None
    from prismabuild import progress as pb_progress
    from prismabuild import movement_actions
    env = dict(os.environ)
    env[pb_progress.ACTION_PROGRESS_PATH_ENV] = str(
        out / f"{scenario}-{label}.progress")
    env[pb_progress.ACTION_PROGRESS_TOKEN_ENV] = _key(f"{scenario}-{label}")[:32]
    env[pb_progress.ACTION_PROGRESS_PHASES_ENV] = json.dumps(
        list(movement_actions.MOVER_PROGRESS_PHASES))
    return env


def run_movers(args, queue, stage, out: Path, scenario: str,
               specs: list[tuple[str, dict, str, str]]) -> list[dict[str, object]]:
    """Start every mover of one scenario at once; wait for all of them."""

    procs = []
    started = time.monotonic()
    for label, window, mover, consumer in specs:
        receipt = out / f"{scenario}-{label}.receipt.json"
        argv = mover_argv(args, queue, stage, window, mover, consumer, receipt)
        profile = out / f"{scenario}-{label}.pyspy.txt"
        if args.py_spy:
            argv = [args.py_spy, "record", "--idle", "--threads",
                    "--nonblocking", "--rate", str(args.rate),
                    "--format", "raw", "--output", str(profile), "--"] + argv
        log = open(out / f"{scenario}-{label}.log", "w")
        procs.append((label, window, receipt, profile, log,
                      subprocess.Popen(argv, stdout=log, stderr=subprocess.STDOUT,
                                       env=mover_env(args, out, scenario, label))))
    results = []
    for label, window, receipt, profile, log, proc in procs:
        code = proc.wait()
        log.close()
        wall = time.monotonic() - started
        body = json.loads(receipt.read_text()) if receipt.exists() else {}
        results.append({"label": label, "window": window["name"],
                        "returncode": code, "wall_s_since_start": round(wall, 3),
                        "receipt": body, "profile": str(profile)})
    return results


# ---- attributing samples ----------------------------------------------------

_SOURCES: dict[str, list[str]] = {}


def _resolve(path: str) -> Path | None:
    """The source file a py-spy frame names, which it prints shortened."""

    for candidate in (Path(path), HERE / path, HERE.parents[1] / "src" / path):
        if candidate.is_absolute() and candidate.is_file():
            return candidate
    return None


def _line(path: str, number: int) -> str:
    lines = _SOURCES.get(path)
    if lines is None:
        resolved = _resolve(path)
        try:
            lines = resolved.read_text().splitlines() if resolved else []
        except OSError:
            lines = []
        _SOURCES[path] = lines
    if 1 <= number <= len(lines):
        return lines[number - 1]
    return ""


def _frames(stack: str) -> list[tuple[str, str, int]]:
    """``(function, file, line)`` root first; py-spy's raw frame spelling."""

    out = []
    for token in stack.split(";"):
        token = token.strip()
        if not token.endswith(")") or " (" not in token:
            continue
        function, _, where = token.rpartition(" (")
        where = where[:-1]
        file, _, number = where.rpartition(":")
        try:
            out.append((function, file, int(number)))
        except ValueError:
            continue
    return out


def classify(frames: list[tuple[str, str, int]]) -> str:
    """The cost one sample is spending, read off the innermost naming frame."""

    for function, file, number in reversed(frames):
        text = _line(file, number)
        base = os.path.basename(file)
        if base == "posix_lock.py":
            if "mutex.acquire" in text:
                return "lock_wait:thread_rlock"
            if "fcntl.lockf" in text and "LOCK_UN" not in text:
                return "lock_wait:fcntl"
            continue
        if base in ("queue.py",) and function == "get":
            return "idle:work_queue"
        if base != "stage_move.py":
            if base == "decoder.py" or function in ("loads", "load"):
                return "parse:json"
            continue
        if "_lookup_lock" in text:
            return "lock_wait:lookup_lock"
        if "time.sleep" in text:
            return "publish:poll_sleep"
        if "os.scandir" in text:
            return f"proof:scandir({function})"
        if "os.fsync" in text:
            return "copy:fsync"
        if "os.readv" in text:
            return "copy:read"
        if "sink.write" in text:
            return "copy:write"
        if "digest.update" in text or "hexdigest" in text:
            return "copy:hash"
        if "os.replace" in text:
            return "publish:rename"
        if "os.stat(" in text or "os.lstat(" in text or "stat_identity(" in text:
            return f"stat({function})"
        if function in ("_fragment_record", "_material_record",
                        "_read_metadata", "_metadata_version"):
            return f"proof:{function}"
        if function in ("_proof_search", "_proof_candidate",
                        "_prove", "_prove_window", "_proof_index"):
            return f"proof:{function}"
        if function in ("publish",) and "write_fragment" in text:
            return "fragment_publication"
        if function in ("write_fragment", "write_material"):
            return "fragment_publication"
        if function == "take":
            return "idle:dispatch"
        if function == "worker" and "on_entry" in text:
            return "fragment_publication"
        return f"stage_move:{function}"
    return "other"


def analyze(profile: Path, rate: int) -> dict[str, object]:
    """Worker-thread seconds by cost, from one py-spy raw profile."""

    worker = collections.Counter()
    main = collections.Counter()
    if not profile.exists():
        return {"missing": str(profile)}
    # py-spy can write a frame name that is not UTF-8; one bad byte must not
    # cost the scenario its summary.
    for raw in profile.read_text(errors="replace").splitlines():
        stack, _, count = raw.rpartition(" ")
        try:
            samples = int(count)
        except ValueError:
            continue
        frames = _frames(stack)
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


# ---- the scenarios ----------------------------------------------------------

def summarize(scenario: str, results: list[dict[str, object]], entries: int,
              rate: int) -> dict[str, object]:
    movers = []
    for result in results:
        receipt = result["receipt"]
        seconds = float(receipt.get("seconds") or 0.0)  # type: ignore[union-attr]
        movers.append({
            "label": result["label"], "window": result["window"],
            "returncode": result["returncode"],
            "complete": receipt.get("complete"),  # type: ignore[union-attr]
            "entries_staged": receipt.get("entries_staged"),  # type: ignore[union-attr]
            "errors": receipt.get("errors"),  # type: ignore[union-attr]
            "seconds": seconds,
            "entries_per_s": round(entries / seconds, 1) if seconds else None,
            "ms_per_entry": round(1000 * seconds / entries, 2) if entries else None,
            "phase_timings": receipt.get("phase_timings"),  # type: ignore[union-attr]
            "progress_report": receipt.get("progress_report"),  # type: ignore[union-attr]
            "proc_io": {k: v for k, v in (receipt.get("proc_io") or {}).items()  # type: ignore[union-attr]
                        if k in ("read_bytes", "write_bytes", "rchar", "wchar")},
            "cpu_seconds": receipt.get("cpu_seconds"),  # type: ignore[union-attr]
            "profile": analyze(Path(str(result["profile"])), rate),
        })
    span = max((float(r["wall_s_since_start"]) for r in results), default=0.0)
    total = entries * len(results)
    return {"scenario": scenario, "movers": movers,
            "host_entries_per_s": round(total / span, 1) if span else None,
            "span_s": round(span, 3)}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--work", required=True,
                        help="scratch directory for the synthetic queue, stage "
                             "and sources; emptied first; never /tmp")
    parser.add_argument("--out", required=True,
                        help="where receipts, profiles and the summary go")
    parser.add_argument("--entries", type=int, default=1500,
                        help="entries per mover window")
    parser.add_argument("--ranges-per-file", type=int, default=10,
                        help="4 KiB ranges cut from each synthetic source file")
    parser.add_argument("--workers", type=int, default=16,
                        help="copy workers per mover (the tool's --max-readers)")
    parser.add_argument("--py-spy", default="",
                        help="py-spy executable; empty runs unprofiled")
    parser.add_argument("--rate", type=int, default=100,
                        help="py-spy samples per second")
    parser.add_argument("--scenarios", default="copy1,adopt1,copy3,adopt3,mixed",
                        help="comma-separated scenarios to run, in order")
    parser.add_argument("--empty-dirs", type=int, default=404,
                        help="empty consumer directories in the residency "
                             "forest")
    parser.add_argument("--small-dirs", type=int, default=30,
                        help="consumer directories holding one to nine "
                             "small noise fragments each")
    parser.add_argument("--big-fragments", default="90000,60000,50000,40000",
                        help="entries in each large noise fragment")
    parser.add_argument("--produced-fragment-dirs", type=int, default=1957,
                        help="subdirectories of the produced-output fragment "
                             "directory")
    parser.add_argument("--progress-interval-s", type=float, default=None,
                        help="give every mover a progress channel and report "
                             "at this interval (#1010); unset gives none, as "
                             "an unmeasured mover has")
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

    queue = pool.PoolQueue(work / "pb-queue")
    queue.ensure_layout()
    stage = work / "stage"
    stage.mkdir()
    assert stage_release.register_stage_root(
        queue, tier_id=TIER, stage_root=stage) == "registered"
    built = time.monotonic()
    forest = build_forest(
        queue, stage, empty_dirs=args.empty_dirs, small_dirs=args.small_dirs,
        big_fragments=[int(x) for x in args.big_fragments.split(",") if x],
        produced_fragment_dirs=args.produced_fragment_dirs)
    windows = {name: make_window(work, name, args.entries, args.ranges_per_file)
               for name in ("w0", "w1", "w2", "w3", "w4")}
    setup_s = time.monotonic() - built

    wanted = [s for s in args.scenarios.split(",") if s]
    summary: dict[str, object] = {
        "checkout": str(HERE.parents[1]), "host": os.uname().nodename,
        "python": sys.version.split()[0], "entries_per_window": args.entries,
        "workers_per_mover": args.workers, "forest": forest,
        "setup_s": round(setup_s, 2), "profiled": bool(args.py_spy),
        "progress_interval_s": args.progress_interval_s,
        "scenarios": []}
    published: dict[str, list[int | None]] = {}
    checks: list[str] = []

    def adopt_check(scenario: str, window_names: list[str]) -> None:
        for name in window_names:
            now = inodes(destinations(stage, windows[name]))
            if now != published[name]:
                changed = sum(1 for a, b in zip(now, published[name]) if a != b)
                checks.append(f"{scenario}: {changed} destinations of {name} "
                              f"changed inode; adoption must not replace")

    def copy_note(name: str) -> None:
        published[name] = inodes(destinations(stage, windows[name]))
        if None in published[name]:
            checks.append(f"copy of {name} left "
                          f"{published[name].count(None)} destinations absent")

    plan = {
        "copy1": [("m0", "w0", "copy1-p0", "copy1-c0")],
        "adopt1": [("m0", "w0", "adopt1-a0", "adopt1-c0")],
        "copy3": [(f"m{i}", f"w{i}", f"copy3-p{i}", f"copy3-c{i}")
                  for i in (1, 2, 3)],
        "adopt3": [(f"m{i}", f"w{i}", f"adopt3-a{i}", f"adopt3-c{i}")
                   for i in (1, 2, 3)],
        "mixed": [("copy", "w4", "mixed-p4", "mixed-c4"),
                  ("adopt1", "w1", "mixed-a1", "mixed-e1"),
                  ("adopt2", "w2", "mixed-a2", "mixed-e2")],
    }
    for scenario in wanted:
        specs = [(label, windows[window], _key(mover), _key(consumer))
                 for label, window, mover, consumer in plan[scenario]]
        results = run_movers(args, queue, stage, out, scenario, specs)
        if scenario.startswith("copy"):
            for _label, window, _m, _c in plan[scenario]:
                copy_note(window)
        elif scenario.startswith("adopt"):
            adopt_check(scenario, [w for _l, w, _m, _c in plan[scenario]])
        else:
            copy_note("w4")
            adopt_check(scenario, ["w1", "w2"])
        for result in results:
            receipt = result["receipt"]
            if result["returncode"] != 0 or not receipt.get("complete"):  # type: ignore[union-attr]
                checks.append(f"{scenario}/{result['label']}: rc="
                              f"{result['returncode']} complete="
                              f"{receipt.get('complete')} errors="  # type: ignore[union-attr]
                              f"{(receipt.get('errors') or [])[:2]}")  # type: ignore[union-attr]
        summary["scenarios"].append(  # type: ignore[union-attr]
            summarize(scenario, results, args.entries, args.rate))
    summary["checks_failed"] = checks
    (out / "summary.json").write_text(json.dumps(summary, indent=1) + "\n")
    print(json.dumps(summary, indent=1))
    return 1 if checks else 0


if __name__ == "__main__":
    raise SystemExit(main())
