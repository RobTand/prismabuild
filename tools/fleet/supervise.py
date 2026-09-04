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
import signal
import socket
import subprocess
import sys
import time

MIRROR = Path("/mnt/shared/prismabuild-fleet")
CONFIG = Path(__file__).resolve().parent / "fleet_boxes.json"
CLAIM = Path("/home/rob/tmp/prismabuild-supervisor.claim")
LOG_DIR = Path("/home/rob/tmp")


def _current_root() -> Path:
    return MIRROR / "repo"


def _config(host: str) -> dict:
    """Read this box's declared shape, from the checkout or the published copy."""

    # A supervisor intentionally outlives a generation.  Prefer the current
    # live generation; CONFIG is only the checkout/bootstrapping fallback.
    for path in (_current_root() / "tools" / "fleet_boxes.json", CONFIG):
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


def declared_shape(host: str, override_loops: int,
                   previous: tuple[int, list[str]] | None = None
                   ) -> tuple[int, list[str]]:
    """This box's target count and loop arguments, re-read every tick.

    The docstring above promises that the shape of the fleet is a versioned
    file rather than three command lines nobody wrote down, and reading it
    once at startup quietly broke that promise: a supervisor started before a
    publish kept its old arguments for its whole life, so an offer widened in
    the file reached the queue only if somebody remembered to restart the
    supervisor too.  That is the same failure as a worker holding stale
    modules -- ``cycle_stale`` exists for exactly that -- arriving one level
    up, where nothing was watching.  It cost a box two of three GPU slots for
    as long as nobody noticed.

    A bad read keeps the previous shape instead of raising.  Runtime
    publication is atomic now, but a transient NFS read failure or damaged
    generation must still not take the box's loops with it the next time one
    goes idle.  The first read has no previous to fall back on and still
    refuses, because guessing what a box offers is the one thing this must
    never do.
    """

    try:
        config = _config(host)
    except (SystemExit, OSError, ValueError):
        if previous is None:
            raise
        return previous
    return (override_loops or int(config.get("loops", 1)),
            [str(a) for a in config.get("args", [])])


def _live_loops() -> list[int]:
    """The worker loops actually running, not the processes that mention one.

    ``pgrep -f worker_loop.py`` matches any command line containing that
    string, and plenty do: the ssh invocation that starts a supervisor, an
    agent grepping for loops, this file being edited.  Over-counting is the
    dangerous direction -- it makes a drained box look full and suppresses
    exactly the respawn this exists for -- so a candidate is confirmed by
    reading its own argv and requiring the loop script to be an argument to
    an interpreter, which is what a running loop is and a mention of one
    never is.
    """

    proc = subprocess.run(["pgrep", "-f", "worker_loop.py"],
                          capture_output=True, text=True, check=False)
    mine = os.getpid()
    confirmed: list[int] = []
    for token in proc.stdout.split():
        pid = int(token)
        if pid == mine:
            continue
        try:
            argv = Path(f"/proc/{pid}/cmdline").read_bytes().split(b"\0")
        except OSError:
            continue                      # exited between pgrep and here
        args = [part.decode("utf-8", "replace") for part in argv if part]
        if len(args) < 2 or "python" not in args[0].rsplit("/", 1)[-1]:
            continue
        if any(arg.endswith("worker_loop.py") for arg in args[1:2]):
            confirmed.append(pid)
    return confirmed


def loop_args_of(pid: int) -> list[str] | None:
    """The arguments this loop is actually running with, or None if unreadable.

    Comparing the file against the previous *read* of the file is not enough:
    a supervisor restarted after a publish reads the new shape first thing,
    sees no change on any later tick, and leaves loops running the old one
    forever -- which is exactly how three loops kept announcing two GPU slots
    while their supervisor's own banner said three.  The authority is the
    file and the question is what the processes carry, so ask the processes.
    """

    try:
        raw = Path(f"/proc/{pid}/cmdline").read_bytes().split(b"\0")
    except OSError:
        return None
    args = [part.decode("utf-8", "replace") for part in raw if part]
    if len(args) < 2:
        return None
    return args[2:]                       # past the interpreter and the script


def _is_idle(pid: int) -> bool:
    """True when this loop holds no action: no child process of its own.

    A loop that is executing an action has spawned the worker launcher, so
    childlessness is the one externally visible fact that distinguishes "safe
    to stop" from "stopping this drops somebody's claim".  Reading
    ``/proc/<pid>/task/*/children`` asks the kernel rather than guessing from
    a log line.
    """

    try:
        tasks = sorted(Path(f"/proc/{pid}/task").iterdir())
    except OSError:
        return False                      # gone, or not ours to judge
    for task in tasks:
        try:
            if (task / "children").read_text().split():
                return False
        except OSError:
            continue
    return True


def cycle_stale(published: str) -> list[int]:
    """Stop idle loops running bytes other than the published ones.

    A loop holds the modules it imported at start for its whole life, so a fix
    published under a running fleet reaches none of it -- and because the
    supervisor counts a stale-byte loop as a healthy one, the target is met
    and the fix is never loaded.  Loops carrying the reload check exit on
    their own; this is for the generation that predates it.

    Only idle loops are stopped, and only with SIGTERM: a loop mid-action
    keeps its claim and cycles when it next goes idle.  Nothing here kills
    work.
    """

    if not published:
        return []
    return _stop_idle_loops()


def _stop_idle_loops(pids: list[int] | None = None) -> list[int]:
    """SIGTERM every loop holding no action, and report which.

    The one rule both reasons to cycle a loop share -- stale bytes and a
    stale declared shape.  Only idle loops, only SIGTERM: a loop mid-action
    keeps its claim and cycles when it next goes idle.
    """

    stopped: list[int] = []
    for pid in (_live_loops() if pids is None else pids):
        if not _is_idle(pid):
            continue
        try:
            os.kill(pid, signal.SIGTERM)
        except OSError:
            continue
        stopped.append(pid)
    return stopped


def _spawn(args: list[str], index: int) -> int:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    handle = (LOG_DIR / f"pb-worker-{index}.log").open("a", buffering=1)
    handle.write(f"\n=== spawned {time.strftime('%F %T')} ===\n")
    # Resolve the live generation once before spawning.  The child then loads
    # its script and every sibling import through one immutable absolute root,
    # even if a later publish moves the live ``repo`` symlink.
    loop = (_current_root() / "tools" / "worker_loop.py").resolve(strict=True)
    proc = subprocess.Popen(
        [sys.executable, str(loop), *args],
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
    ap.add_argument("--cycle-stale", action="store_true",
                    help="SIGTERM idle loops so they respawn on the published "
                         "runtime; a loop mid-action is left alone")
    args = ap.parse_args()

    host = socket.gethostname()
    target, loop_args = declared_shape(host, args.loops)

    if args.cycle_stale and args.once:
        # A one-shot cycle does not need to own the box: it stops only idle
        # loops, the running supervisor's next tick tops the count back up,
        # and taking the claim here would make the maintenance action fail
        # precisely when a supervisor is present -- which is always.
        published = ""
        try:
            published = str(json.loads(
                (_current_root() / "RUNTIME_VERSION.json").read_text()
            ).get("commit") or "")
        except (OSError, ValueError):
            pass
        stopped = cycle_stale(published)
        print(f"[{host}] cycled {len(stopped)} idle loop(s) onto "
              f"{published[:12] or '(unknown)'}: {stopped}", flush=True)
        return 0

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
    if args.cycle_stale:
        published = ""
        try:
            published = str(json.loads(
                (_current_root() / "RUNTIME_VERSION.json").read_text()
            ).get("commit") or "")
        except (OSError, ValueError):
            pass
        stopped = cycle_stale(published)
        print(f"[{host}] cycled {len(stopped)} idle loop(s) onto "
              f"{published[:12] or '(unknown)'}: {stopped}", flush=True)
    while True:
        target, loop_args = declared_shape(
            host, args.loops, (target, loop_args))

        live = _live_loops()
        # The file is the authority, so a loop running other arguments is
        # stale in the same sense a loop running other bytes is.  Stop the
        # idle ones and let the top-up below respawn them on the declared
        # shape; a loop mid-action keeps its claim and cycles when it next
        # goes idle, which is why this never kills work.
        wrong = [pid for pid in live
                 if loop_args_of(pid) not in (None, loop_args)]
        if wrong:
            stopped = _stop_idle_loops(wrong)
            live = [pid for pid in live if pid not in stopped]
            print(f"[{host}] {len(wrong)} loop(s) carry a shape the file no "
                  f"longer declares; stopped the {len(stopped)} idle one(s) "
                  f"{stopped} onto: {' '.join(loop_args)}", flush=True)
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
