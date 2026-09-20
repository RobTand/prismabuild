"""Metadata-only cost of one _inflight_partials call against directory width.

No payload, no hashing, no queue: empty files and one sibling census.  Prints
JSON so a before/after pair is comparable.
"""

import json
import os
import statistics
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "tools", "fleet"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

import stage_move  # noqa: E402


class _Stub:
    """Only what _inflight_partials reads off self."""

    mover = "a" * 64


def measure(width: int, calls: int) -> dict:
    stub = _Stub()
    with tempfile.TemporaryDirectory(dir=os.environ.get("BENCH_TMP") or None) as td:
        parent = Path(td)
        for i in range(width):
            (parent / f"entry-{i:06d}.bin").write_bytes(b"")
        destination = parent / "entry-000000.bin"
        # One real sibling partial from another owner, so the match path runs.
        (parent / f".{destination.name}.{'b' * 16}.partial").write_bytes(b"")
        fn = stage_move._StagedPublisher._inflight_partials
        fn(stub, destination)  # warm the dentry cache
        samples = []
        for _ in range(calls):
            t0 = time.perf_counter()
            got = fn(stub, destination)
            samples.append(time.perf_counter() - t0)
        assert got == [f".{destination.name}.{'b' * 16}.partial"], got
        return {
            "width": width,
            "calls": calls,
            "mean_ms": 1000 * statistics.mean(samples),
            "median_ms": 1000 * statistics.median(samples),
            "min_ms": 1000 * min(samples),
            "per_dirent_us": 1e6 * statistics.mean(samples) / max(width, 1),
        }


if __name__ == "__main__":
    out = [measure(w, c) for w, c in ((1000, 20), (10000, 10), (36439, 10))]
    print(json.dumps({"results": out}, indent=2))
