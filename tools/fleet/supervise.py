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
PROC = Path("/proc")

#: The mark this supervisor sets on every loop it launches, carrying the box
#: it launched the loop for.
#:
#: A basename is not ownership.  ``pgrep -f worker_loop.py`` matches any
#: command line containing that string, and the confirmation step only asked
#: whether an interpreter was running a script whose name ended that way -- so
#: an unrelated ``/another-project/worker_loop.py`` under the same user was
#: counted toward this box's fleet capacity, and on the first tick where its
#: arguments did not match ``fleet_boxes.json`` and it happened to have no
#: child process, it was SIGTERMed.  A long-running Python workload is busy
#: and childless at the same time all the time.
#:
#: The mark is read from ``/proc/<pid>/environ`` rather than believed from a
#: log or a pid file, and it is only half the answer: it says this supervisor
#: launched the process, and the resolved script path says the bytes came from
#: a runtime generation this fleet published.  Either alone is guessable.
OWNERSHIP_ENV = "PRISMABUILD_SUPERVISED_WORKER"

#: The script a fleet worker loop runs, checked after the path resolves rather
#: than as a suffix of whatever ``pgrep`` matched.
LOOP_SCRIPT = "worker_loop.py"


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
        # The second Spark was renamed from gx10-6b77 to sparklina.  Its
        # declared alias names the same machine, not a second capacity offer.
        # Keep one shape so the old and new hostname cannot drift apart.
        aliases = [shape for shape in boxes.values()
                   if shape.get("_alias") == host]
        if len(aliases) > 1:
            raise SystemExit(f"ambiguous fleet hostname alias {host}: {path}")
        if aliases:
            return aliases[0]
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


def _proven_roots() -> list[Path]:
    """Every runtime tree a fleet worker loop may legitimately have started in.

    The live generation, and the published generations behind it.  A loop
    holds the bytes it imported for its whole life and publication never
    deletes a generation, so an old immutable generation may still be running
    an action; refusing to recognize it would make the supervisor spawn a
    replacement beside work in flight.  A tree that is neither is not this
    fleet's, whatever the script inside it is called.

    The generation store is filtered by the rule ``publish_runtime
    ._activate_existing`` already applies: a direct child, not a dot-name,
    carrying a ``RUNTIME_VERSION.json``.  A ``.<name>.staging`` survivor of an
    interrupted publish carries a receipt and passes every other check, and
    its bytes were never sealed or probed.
    """

    roots: list[Path] = []

    def add(candidate: Path) -> None:
        try:
            resolved = candidate.resolve(strict=True)
        except OSError:
            return
        if resolved not in roots:
            roots.append(resolved)

    add(_current_root())
    try:
        children = sorted((MIRROR / "runtime-generations").iterdir())
    except OSError:
        children = []
    for child in children:
        if child.name.startswith("."):
            continue
        if not (child / "RUNTIME_VERSION.json").is_file():
            continue
        add(child)
    return roots


def _proc_field(pid: int, name: str, proc_root: Path) -> bytes | None:
    try:
        return (proc_root / str(pid) / name).read_bytes()
    except OSError:
        return None                       # exited, or not ours to read


def _script_of(pid: int, argv: list[str], proc_root: Path) -> Path | None:
    """The loop script this process is running, resolved absolutely.

    dl380g10's loops carry a relative ``repo/tools/worker_loop.py``, so the
    path is resolved against the process's own working directory rather than
    the supervisor's -- reading ``/proc/<pid>/cwd`` asks the kernel instead of
    assuming the two agree.
    """

    script = Path(argv[1])
    if not script.is_absolute():
        try:
            script = (proc_root / str(pid) / "cwd").resolve(strict=True) / script
        except OSError:
            return None
    try:
        return script.resolve(strict=True)
    except OSError:
        return None


def _is_fleet_loop(pid: int, roots: list[Path],
                   proc_root: Path | None = None) -> bool:
    """True when this pid is a worker loop this box's supervisor launched.

    Three facts, none of them a name: an interpreter is running the script as
    an argument, the script resolves inside a proven runtime root, and the
    process carries this supervisor's ownership mark for this box.  A process
    that fails any of them is neither counted toward the box's target nor
    signalled, which is the whole of issue #87.
    """

    proc_root = PROC if proc_root is None else proc_root
    raw = _proc_field(pid, "cmdline", proc_root)
    if raw is None:
        return False
    argv = [part.decode("utf-8", "replace") for part in raw.split(b"\0") if part]
    if len(argv) < 2 or "python" not in argv[0].rsplit("/", 1)[-1]:
        return False
    script = _script_of(pid, argv, proc_root)
    if script is None or script.name != LOOP_SCRIPT:
        return False
    if not any(script.is_relative_to(root) for root in roots):
        return False
    environ = _proc_field(pid, "environ", proc_root)
    if environ is None:
        return False
    for entry in environ.split(b"\0"):
        name, sep, value = entry.partition(b"=")
        if sep and name.decode("utf-8", "replace") == OWNERSHIP_ENV:
            return value.decode("utf-8", "replace") == socket.gethostname()
    return False


def _live_loops(proc_root: Path | None = None) -> list[int]:
    """The worker loops this supervisor owns, not the processes that mention one.

    ``pgrep`` is a candidate generator and nothing more.  Over-counting is the
    dangerous direction for the respawn this exists for -- it makes a drained
    box look full -- and over-claiming is the dangerous direction for the
    SIGTERM: both are decided by ``_is_fleet_loop`` rather than by the string
    ``pgrep`` matched on.
    """

    proc = subprocess.run(["pgrep", "-f", LOOP_SCRIPT],
                          capture_output=True, text=True, check=False)
    mine = os.getpid()
    roots = _proven_roots()
    confirmed: list[int] = []
    for token in proc.stdout.split():
        pid = int(token)
        if pid == mine:
            continue
        if _is_fleet_loop(pid, roots, proc_root):
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


def _stop_idle_loops(pids: list[int] | None = None,
                     proc_root: Path | None = None) -> list[int]:
    """SIGTERM every loop holding no action, and report which.

    The one rule both reasons to cycle a loop share -- stale bytes and a
    stale declared shape.  Only idle loops, only SIGTERM: a loop mid-action
    keeps its claim and cycles when it next goes idle.
    """

    stopped: list[int] = []
    roots = _proven_roots()
    for pid in (_live_loops(proc_root) if pids is None else pids):
        # Proved again here, at the line that actually sends the signal.  The
        # caller has always passed pids that came from ``_live_loops``, but a
        # future caller that does not must not be able to turn this into the
        # basename kill it used to be.
        if not _is_fleet_loop(pid, roots, proc_root):
            continue
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
        # The mark that makes this loop attributable to this supervisor.  It
        # is what ``_is_fleet_loop`` reads back out of ``/proc``, and a loop
        # spawned without it is not counted and never signalled.
        env={**os.environ, OWNERSHIP_ENV: socket.gethostname()},
    )
    return proc.pid


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--ensure", action="store_true",
                    help="exit 0 if a supervisor already owns this box")
    ap.add_argument("--loops", type=int, default=0,
                    help="override the configured target count")
    ap.add_argument("--interval-s", type=float, default=30.0,
                    help="seconds between supervision cycles; ignored with "
                         "--once")
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
