#!/usr/bin/env python3
"""A deterministic action that commits work steadily, for #480's demonstration.

Small on purpose: the behaviour under test is *whether an advancing action is
allowed to finish*, and a multi-hour GLM pricing row proves that no better
than ninety seconds of arithmetic does.  It writes a durable, monotone record
of committed units to its own result path, and reports each commitment through
the PrismaBuild progress contract when it was admitted under one.

    python tests/progress_demo.py --seconds 180 --result committed.json

Run without the contract it is the "before": a run that is demonstrably
working and is killed anyway when its sealed deadline passes.  Run with
``--progress-phase`` it is the "after": the same work, ended by stopping
rather than by elapsing.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import progress as pb_progress  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--seconds", type=float, default=180.0,
                    help="how long to keep committing units")
    ap.add_argument("--interval-s", type=float, default=1.0,
                    help="seconds of work per committed unit")
    ap.add_argument("--result", default="committed.json",
                    help="where the durable record of committed units is kept")
    ap.add_argument("--phase", default="run",
                    help="which declared phase these units belong to")
    ap.add_argument("--stall-after", type=float, default=None,
                    help="stop committing this many seconds in and keep "
                         "running, printing all the while: the control that "
                         "must still be terminated")
    args = ap.parse_args(argv)

    result = Path(args.result)
    committed = 0
    deadline = time.monotonic() + args.seconds
    reported = False
    while time.monotonic() < deadline:
        time.sleep(args.interval_s)
        stalled = (args.stall_after is not None
                   and time.monotonic() > (deadline - args.seconds) + args.stall_after)
        if stalled:
            # Alive, printing, burning a core, committing nothing.  None of
            # that is progress and none of it may buy another second.
            print(f"[demo] still here at {time.monotonic():.1f}", flush=True)
            continue
        committed += 1
        # Durable first, reported second: a counter that ran ahead of the work
        # it stands for would keep a broken action alive, which is the whole
        # thing the contract is trying not to do.
        tmp = result.with_name(result.name + ".tmp")
        tmp.write_text(json.dumps({"committed_units": committed,
                                   "unix": time.time()}, sort_keys=True) + "\n",
                       encoding="utf-8")
        os.replace(tmp, result)
        reported = pb_progress.report_action_progress(
            args.phase, committed, unit="demo-units") or reported
        print(f"[demo] committed {committed}", flush=True)
    print(f"[demo] done: {committed} units, reported={reported}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
