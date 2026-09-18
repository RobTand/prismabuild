#!/usr/bin/env python3
"""What one cycle's stage release costs, before and after #589's F6.

The prewarm loop's release path runs for every window of every cycle against
a 10 s poll, on the file server, and nothing measured it.  This is the
standalone timing that says what it cost and what it costs now, over the real
GLM-5.3 prepare manifest (469,008 entries) rather than a fixture.

Three arms, in one process over one parsed manifest, interleaved:
``original`` is the release path exactly as it stood before the review --
one band, one other row asked; ``before`` is the work the fixed loop does
(F5 added a scan of the releasing row's own band) with the pre-#589
arithmetic, so it isolates F6; ``after`` is the loop as it stands.  The
pre-#589 functions are copied verbatim below so the arms differ in nothing
else.  The run also asserts the arms name the same objects, because a cheaper
release that released something else would not be a speedup.

Run it through PrismaBuild on the box that pays for it::

    pbrun.py --cwd <checkout> --tag dl380g10 --cpus 1 --demand mem_gb=6 -- \\
        python3 tools/fleet/bench_prewarm_stage_release.py --manifest <path>
"""
from __future__ import annotations

import argparse
import cProfile
import gzip
import json
import os
from pathlib import Path
import pstats
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parent))
import prewarm_loop  # noqa: E402


# ---- the arithmetic as it stood before #589 (copied verbatim) -------------

def old_stage_object_key(entry, mount_prefix):
    path = str(entry.get("path", ""))
    prefix = str(mount_prefix or "")
    if not path or not prefix:
        return None
    try:
        relative = os.path.relpath(os.path.normpath(path),
                                   os.path.normpath(prefix))
    except ValueError:
        return None
    if relative.startswith("..") or os.path.isabs(relative) or relative == ".":
        return None
    if any(part == ".." for part in relative.split(os.sep)):
        return None
    offset = int(entry.get("offset", 0) or 0)
    size = int(entry.get("bytes", 0) or 0)
    return f"{relative}{prewarm_loop.STAGE_OBJECT_MARK}{offset}+{size}"


def old_stage_keys_between(entries, mount_prefix, start, end):
    keys = []
    position = 0
    for entry in entries:
        position += int(entry.get("bytes", 0) or 0)
        if position > end:
            break
        if position <= start:
            continue
        key = old_stage_object_key(entry, mount_prefix)
        if key:
            keys.append(key)
    return keys


def old_stage_keys_wanted(entries, mount_prefix, consumed, candidates):
    wanted = set()
    position = 0
    for entry in entries:
        position += int(entry.get("bytes", 0) or 0)
        if position <= consumed:
            continue
        key = old_stage_object_key(entry, mount_prefix)
        if key in candidates:
            wanted.add(key)
    return wanted


# ---- the two scenarios ----------------------------------------------------

def original_advanced(entries, prefix, start, end):
    """The release path exactly as it stood before this PR's review.

    One band, then one other row asked what it still wants.  The releasing
    row was not asked about its own band -- that scan is what F5 added, and
    it is why this arm is cheaper than ``before_advanced`` while doing less.
    """

    candidates = old_stage_keys_between(entries, prefix, start, end)
    pending = set(candidates)
    for _ in range(1):
        if not pending:
            break
        pending -= old_stage_keys_wanted(entries, prefix, 0, pending)
    return {str(key) for key in pending}, len(candidates)


def before_advanced(entries, prefix, start, end):
    """The same work the fixed loop does, with the old arithmetic.

    F5's self-scan included, so this arm and ``after_advanced`` do the same
    thing and differ only in how an object is named and compared.  469,007 of
    469,008 entries are shared across three of these manifests, so "the other
    row wants all of it" is the measured shape, not a corner.
    """

    candidates = old_stage_keys_between(entries, prefix, start, end)
    pending = set(candidates)
    pending -= old_stage_keys_wanted(entries, prefix, end, pending)
    for _ in range(1):
        if not pending:
            break
        pending -= old_stage_keys_wanted(entries, prefix, 0, pending)
    return {str(key) for key in pending}, len(candidates)


def after_advanced(entries, prefix, start, end):
    head = prewarm_loop.stage_prefix_head(prefix)
    candidates = prewarm_loop.stage_ids_between(entries, head, start, end)
    pending = set(candidates)
    pending -= prewarm_loop.stage_ids_wanted(entries, head, end, pending)
    for _ in range(1):
        if not pending:
            break
        pending -= prewarm_loop.stage_ids_wanted(entries, head, 0, pending)
    return {prewarm_loop.stage_object_name(i) for i in pending}, len(candidates)


def before_unchanged(entries, prefix, frontier):
    """A frontier that did not move: the common case, every cycle, per row."""

    return old_stage_keys_between(entries, prefix, frontier, frontier)


def after_unchanged(entries, prefix, frontier):
    head = prewarm_loop.stage_prefix_head(prefix)
    return prewarm_loop.stage_ids_between(entries, head, frontier, frontier)


def timed(call, *args):
    started = time.perf_counter()
    result = call(*args)
    return time.perf_counter() - started, result


def peak_rss_mb() -> float:
    try:
        for line in open("/proc/self/status"):
            if line.startswith("VmHWM:"):
                return int(line.split()[1]) / 1024
    except OSError:
        pass
    return -1.0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True,
                        help="a real data manifest, .json or .json.gz")
    parser.add_argument("--repeats", type=int, default=3,
                        help="how many times each arm runs; the arms are "
                             "interleaved and the best of each is reported, "
                             "so a box that was busy for one repeat does not "
                             "become the result")
    parser.add_argument("--profile", action="store_true",
                        help="also run each arm once under cProfile")
    args = parser.parse_args(argv)

    started = time.perf_counter()
    opener = gzip.open if args.manifest.endswith(".gz") else open
    with opener(args.manifest, "rt") as handle:
        manifest = json.load(handle)
    entries = manifest["entries"]
    prefix = str(manifest.get("mount_prefix", ""))
    total = int(manifest.get("total_bytes") or sum(
        int(e.get("bytes", 0) or 0) for e in entries))
    print(f"manifest {args.manifest}")
    print(f"  entries {len(entries)}  total_bytes {total}  "
          f"mount_prefix {prefix}  parsed in "
          f"{time.perf_counter() - started:.2f} s")

    frontier = (total * 2) // 3
    original_keys, original_candidates = original_advanced(
        entries, prefix, 0, frontier)
    before_keys, candidates = before_advanced(entries, prefix, 0, frontier)
    after_keys, after_candidates = after_advanced(entries, prefix, 0, frontier)
    assert before_keys == after_keys, "the arms name different objects"
    print(f"  band (0, {frontier}]  candidates original={original_candidates} "
          f"before={candidates} after={after_candidates}  "
          f"released={len(before_keys)}  objects identical: "
          f"{before_keys == after_keys}  "
          f"original names the same set: {original_keys == after_keys}")

    rows = []
    for repeat in range(args.repeats):
        rows.append(("advanced original",
                     timed(original_advanced, entries, prefix, 0, frontier)[0]))
        rows.append(("advanced before",
                     timed(before_advanced, entries, prefix, 0, frontier)[0]))
        rows.append(("advanced after",
                     timed(after_advanced, entries, prefix, 0, frontier)[0]))
        rows.append(("unchanged before",
                     timed(before_unchanged, entries, prefix, frontier)[0]))
        rows.append(("unchanged after",
                     timed(after_unchanged, entries, prefix, frontier)[0]))
        print(f"  repeat {repeat}: " + "  ".join(
            f"{name}={seconds:.3f}s" for name, seconds in rows[-5:]))

    print("\nbest of each arm, seconds:")
    for name in ("advanced original", "advanced before", "advanced after",
                 "unchanged before", "unchanged after"):
        best = min(seconds for label, seconds in rows if label == name)
        print(f"  {name:<18} {best:.3f}")

    if args.profile:
        for name, call, call_args in (
                ("original", original_advanced,
                 (entries, prefix, 0, frontier)),
                ("before", before_advanced, (entries, prefix, 0, frontier)),
                ("after", after_advanced, (entries, prefix, 0, frontier))):
            profiler = cProfile.Profile()
            profiler.enable()
            call(*call_args)
            profiler.disable()
            print(f"\n--- cProfile, advanced frontier, {name} ---")
            pstats.Stats(profiler, stream=sys.stdout).sort_stats(
                "tottime").print_stats(8)

    print(f"\npeak RSS {peak_rss_mb():.0f} MiB")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
