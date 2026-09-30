#!/usr/bin/env python3
"""Summarize live-worker sampling coverage without inventing a timing delta.

Read-only analysis of an existing py-spy speedscope capture. This does not
attach to, signal, or change a worker, and never enables request caching.
"""
from __future__ import annotations

import argparse
from collections.abc import Mapping
import json
import math
from pathlib import Path, PurePosixPath
from typing import Any


FUNCTIONS = ("_claim_pass", "held", "_withhold_verdict", "holder_bound", "_declared_run_bound")


def summarize_profile(profile: Mapping[str, Any], *, pid: int,
                      generation_root: str) -> dict[str, Any]:
    """Count inclusive samples in one PID and the exact published pool module.

    Child processes and same-named functions in other trees do not count.
    A zero count establishes only sampling absence, never zero runtime cost.
    """
    if type(pid) is not int or pid <= 0:
        raise ValueError("profile pid must be a positive integer")
    root = PurePosixPath(generation_root)
    if not root.is_absolute() or ".." in root.parts:
        raise ValueError("profile generation_root must be an absolute normalized path")
    shared = profile.get("shared")
    frames = shared.get("frames") if isinstance(shared, Mapping) else None
    profiles = profile.get("profiles")
    if not isinstance(frames, list) or not isinstance(profiles, list):
        raise ValueError("profile requires shared.frames and profiles arrays")
    expected_file = str(root / "src/prismabuild/pool.py")
    indices: dict[int, str] = {}
    for i, frame in enumerate(frames):
        if not isinstance(frame, Mapping):
            raise ValueError("profile frame must be an object")
        name = frame.get("name")
        if frame.get("file") == expected_file and name in FUNCTIONS:
            indices[i] = str(name)
    selected = [p for p in profiles if isinstance(p, Mapping)
                and isinstance(p.get("name"), str)
                and p["name"].startswith(f"Process {pid} Thread ")]
    if not selected:
        raise ValueError(f"profile contains no thread for process {pid}")
    counts = {name: {"samples": 0, "inclusive_seconds": 0.0} for name in FUNCTIONS}
    total = 0
    for thread in selected:
        if thread.get("type") != "sampled" or thread.get("unit") != "seconds":
            raise ValueError("profile must contain sampled threads weighted in seconds")
        samples, weights = thread.get("samples"), thread.get("weights")
        if not isinstance(samples, list) or not isinstance(weights, list) or len(samples) != len(weights):
            raise ValueError("profile samples and weights must be equally sized arrays")
        for stack, weight in zip(samples, weights):
            if not isinstance(stack, list) or any(type(i) is not int or i < 0 or i >= len(frames) for i in stack):
                raise ValueError("profile stack contains an invalid frame index")
            if type(weight) not in (int, float) or not math.isfinite(weight) or weight < 0:
                raise ValueError("profile weight must be a finite nonnegative number")
            # Recursion/duplicate frames are inclusive once per sample, not a
            # multiplier. The seconds are sampled occupancy, not call latency.
            for name in {indices[i] for i in stack if i in indices}:
                counts[name]["samples"] += 1
                counts[name]["inclusive_seconds"] += float(weight)
            total += 1
    if not total:
        raise ValueError("profile contains no samples for the requested process")
    for values in counts.values():
        values["inclusive_seconds"] = round(values["inclusive_seconds"], 9)
    sampled = bool(counts["holder_bound"]["samples"] and counts["_declared_run_bound"]["samples"])
    return {
        "schema": "prismabuild.diag940.profile_coverage.v1",
        "pid": pid, "generation_root": str(root), "pool_module": expected_file,
        "sample_count": total, "thread_count": len(selected), "functions": counts,
        "holder_read_path_sampled": sampled,
        "status": "holder_read_path_sampled" if sampled else "holder_read_path_not_sampled",
        "absence_proves_zero_cost": False, "performance_delta": None,
        "limits": ["inclusive sampled occupancy is not per-call latency",
                   "holder eligibility and before/after workload matching require separate evidence",
                   "this report does not authorize a cache or a speedup claim"],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", required=True, type=Path)
    parser.add_argument("--pid", required=True, type=int)
    parser.add_argument("--generation-root", required=True)
    args = parser.parse_args()
    data = json.loads(args.profile.read_text())
    if not isinstance(data, Mapping):
        raise ValueError("profile document must be an object")
    print(json.dumps(summarize_profile(data, pid=args.pid,
                                      generation_root=args.generation_root), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
