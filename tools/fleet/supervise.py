#!/usr/bin/env python3
"""Keep this box's worker loops alive.

Nothing started the fleet's worker loops but a person at a keyboard, and
nothing restarted them.  A loop exits on ``--max-idle`` by design, and it
exits on an unhandled error by accident, and the counts were visibly
draining: sparky 5 -> 4, sparklina 3 -> 2, dl380g10 15 -> 14 over one
evening, every survivor reparented to init.  A fleet that only loses workers
reaches zero, and the failure at zero is indistinguishable from a busy queue:
items sit in ``ready``, counted and reported as pending, with nobody left to
claim them.  That is the same silence the placement bug had, arrived at from
the other end.

So: one supervisor per box, holding an exclusive advisory claim on a
box-local file so a second invocation is a no-op rather than a doubling; the
target count and the loop arguments read from ``fleet_boxes.json`` in the
checkout, so the shape of the fleet is a versioned file rather than three
command lines nobody wrote down; and a respawn whenever a loop is missing,
whether it left by design or by crash.

The claim is deliberately box-local.  It says "a supervisor owns THIS box",
which is a fact about one machine's processes; on shared storage it would let
one box's supervisor silence another's.  It is not a scheduling primitive and
admits no work -- every action still goes through the pool's ledger, which is
the thing that can actually balance two boxes.

``--ensure`` is the form a periodic job uses: become the supervisor if there
isn't one, exit quietly if there is.  Nothing supervises the supervisor, so
that idempotence is what makes a five-minute timer a sufficient answer.
"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time

MIRROR = Path("/mnt/shared/prismabuild-fleet")
LOOP = MIRROR / "repo" / "tools" / "worker_loop.py"
CONFIG = Path(__file__).resolve().parent / "fleet_boxes.json"
CLAIM = Path("/home/rob/tmp/prismabuild-supervisor.claim")
LOG_DIR = Path("/home/rob/tmp")


def _config(host: str) -> dict:
    """Read this box's declared shape, from the checkout or the published copy."""

    for path in (CONFIG, MIRROR / "repo" / "tools" / "fleet_boxes.json"):
        try:
            boxes = json.loads(path.read_text())["boxes"]
        except (OSError, ValueError, KeyError):
            continue
        if host in boxes:
            return boxes[host]
    raise SystemExit(
        f"no fleet_boxes.json entry for {host}; refusing to guess what this "
        f"box offers -- add it to tools/fleet/fleet_boxes.json and publish"
    )


def _live_loops() -> list[int]:
    proc = subprocess.run(["pgrep", "-f", "worker_loop.py"],
                          capture_output=True, text=True, check=False)
    mine = os.getpid()
    return [int(p) for p in proc.stdout.split() if int(p) != mine]


def _spawn(args: list[str], index: int) -> int:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    handle = (LOG_DIR / f"pb-worker-{index}.log").open("a", buffering=1)
    handle.write(f"\n=== spawned {time.strftime('%F %T')} ===\n")
    proc = subprocess.Popen(
        [sys.executable, str(LOOP), *args],
        cwd=str(MIRROR), stdout=handle, stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    return proc.pid


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--ensure", action="store_true",
                    help="exit 0 if a supervisor already owns this box")
    ap.add_argument("--loops", type=int, default=0,
                    help="override the configured target count")
    ap.add_argument("--interval-s", type=float, default=30.0)
    ap.add_argument("--once", action="store_true",
                    help="top the box up and exit, without supervising")
    args = ap.parse_args()

    host = socket.gethostname()
    config = _config(host)
    target = args.loops or int(config.get("loops", 1))
    loop_args = [str(a) for a in config.get("args", [])]

    CLAIM.parent.mkdir(parents=True, exist_ok=True)
    handle = CLAIM.open("w")
    try:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        if args.ensure:
            return 0                       # somebody else already owns this box
        raise SystemExit(f"another supervisor owns {CLAIM}")
    handle.write(f"{os.getpid()}\n")
    handle.flush()

    print(f"[{host}] supervising {target} loops: {' '.join(loop_args)}",
          flush=True)
    while True:
        live = _live_loops()
        missing = max(0, target - len(live))
        for offset in range(missing):
            index = len(live) + offset
            pid = _spawn(loop_args, index)
            print(f"[{host}] spawned loop {index} pid {pid} "
                  f"({index + 1} of {target})", flush=True)
            time.sleep(0.5)                # stagger the first queue poll
        if args.once:
            return 0
        time.sleep(args.interval_s)


if __name__ == "__main__":
    raise SystemExit(main())
