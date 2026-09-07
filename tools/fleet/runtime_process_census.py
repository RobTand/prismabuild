#!/usr/bin/env python3
"""Refuse unless every live PrismaBuild loop started after an activation."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import socket
import sys

sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
from runtime_paths import generation_root  # noqa: E402

# The matching rule -- what counts as a live PrismaBuild loop -- is shared with
# the offer this box publishes (``pool.announce``'s ``loops``).  This gate and
# that field must not be able to disagree about the same box, so there is one
# implementation and both read it.
sys.path.insert(0, str(generation_root(__file__) / "src"))
from prismabuild import box_capacity  # noqa: E402


def _boot_unix() -> int:
    for line in Path("/proc/stat").read_text(encoding="utf-8").splitlines():
        if line.startswith("btime "):
            return int(line.split()[1])
    raise RuntimeError("/proc/stat has no btime record")


def _process_start_unix(pid: int, *, boot_unix: int, ticks_per_s: int) -> float:
    raw = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
    # comm (field 2) is parenthesized and may itself contain spaces or `)`.
    fields_from_state = raw[raw.rfind(")") + 2 :].split()
    start_ticks = int(fields_from_state[19])  # field 22 overall
    return boot_unix + start_ticks / ticks_per_s


def _worker_loops() -> list[dict[str, object]]:
    boot_unix = _boot_unix()
    ticks_per_s = int(os.sysconf("SC_CLK_TCK"))
    loops: list[dict[str, object]] = []
    for pid, argv in box_capacity.worker_loops():
        try:
            started = _process_start_unix(
                pid, boot_unix=boot_unix, ticks_per_s=ticks_per_s
            )
        except (OSError, ValueError, IndexError):
            continue  # exited between the census and its start-time read
        loops.append({"pid": pid, "started_unix": started, "argv": list(argv)})
    return loops


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", required=True,
                        help="the box this census is meant to describe; "
                             "exits 2 if it is not the box running it")
    parser.add_argument("--activation-unix", required=True, type=float,
                        help="when the runtime was activated, as a Unix "
                             "timestamp; a loop that started at or before it "
                             "is stale and exits 4")
    parser.add_argument("--expected-loops", required=True, type=int,
                        help="how many worker loops this box should be "
                             "running; any other count exits 3")
    args = parser.parse_args()

    actual_host = socket.gethostname()
    loops = _worker_loops()
    stale = [
        int(loop["pid"])
        for loop in loops
        if float(loop["started_unix"]) <= args.activation_unix
    ]
    result = {
        "schema": "prismabuild.runtime_process_census.v1",
        "host": actual_host,
        "activation_unix": args.activation_unix,
        "expected_loops": args.expected_loops,
        "observed_loops": len(loops),
        "stale_pids": stale,
        "loops": loops,
    }
    print(json.dumps(result, sort_keys=True))
    if actual_host != args.host:
        return 2
    if len(loops) != args.expected_loops:
        return 3
    if stale:
        return 4
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
