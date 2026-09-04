"""Pull actions off the shared queue until it drains.  Runs on any box.

``serve_once`` deliberately runs one item and exits; a real stage needs a loop
around it.  Two defaults from the smoke-test worker would be wrong here:

* ``timeout_s=120`` kills a shard encode at two minutes.  A shard takes
  minutes, so the timeout is hours and exists only to bound a wedged run.
* The queue is polled rather than exited on first miss, because another box
  may still be publishing.  ``--max-idle`` bounds the wait.

Concurrency is admitted by the pool's resource ledger, not by loop count: each
loop declares what the *box* has and each action declares what it needs, so a
box cannot be oversubscribed by starting more loops than it can feed.  The
capacity below is measured, not guessed -- one exporter holds ~8 GB resident
and four concurrent ones took a GB10 from 116 GB free to 55 GB, so 16 GB per
action is the honest figure and 96 GB leaves the box its working headroom.

``serve_once`` returning ``None`` can now mean "denied admission" as well as
"queue empty", including the deliberate case where a starved item is
withholding this host.  Both are back-pressure, so the loop polls rather than
exiting on the first miss; ``--max-idle`` still bounds the wait.

Three things are the *worker's* to declare, not the action's, because they
are properties of the box rather than of the work:

* **The interpreter running the pool worker.**  ``--python`` names what
  launches ``worker.py run-local`` on this box, which is not the same thing as
  the interpreter an *action* runs under: ``run_local_action`` executes each
  action in a **closed** environment built from the variables the action itself
  declares, so an action's interpreter is sealed into its command and therefore
  into its action key.  Making that portable would mean changing the executed
  contract ``slurm.py`` pins, so it is deliberately not done here.  A cross-arch
  action instead names an interpreter that exists on its target and carries the
  matching class tag; the two must agree, and the submitter owns both.
* **The tags.**  ``--class`` replaces a hardcoded ``gb10``: a box that offers
  a class it is not will be sent work it cannot run.
* **The cores.**  Both box shapes punish the obvious affinity (GB10
  interleaves fast and slow cores; the Xeon is 2-way SMT), so the loop pins
  itself to the preferred set and every action inherits it.  See
  ``prismabuild.cpu_topology``.
"""
import argparse
import json
import socket
import sys
import time
from pathlib import Path

SH = Path("/mnt/shared/prismabuild-fleet")
sys.path.insert(0, str(SH / "repo" / "src"))
from prismabuild import cpu_topology, pool  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--timeout-s", type=float, default=7200.0)
    ap.add_argument("--poll-s", type=float, default=10.0)
    ap.add_argument("--max-idle", type=int, default=6,
                    help="consecutive empty polls before exiting")
    ap.add_argument("--gpu-slots", type=int, default=4,
                    help="concurrent GPU actions this box admits; 0 = no GPU")
    ap.add_argument("--class", dest="klass", default="gb10",
                    help="hardware class this box offers, e.g. gb10 or x86")
    ap.add_argument("--python", default="/usr/bin/python3",
                    help="interpreter that launches the pool worker on this box")
    ap.add_argument("--all-cores", action="store_true",
                    help="do not pin to the preferred cores (debug)")
    ap.add_argument("--mem-gb", type=int, default=96,
                    help="memory this box offers the queue, of ~121 GB total")
    ap.add_argument("--tag", action="append", default=[],
                    help="extra placement tag this box offers")
    ap.add_argument("--honest-memory", action="store_true",
                    help="clamp the memory offer to what the box actually has free")
    args = ap.parse_args()
    capacity = {"gpu": args.gpu_slots, "mem_gb": args.mem_gb}
    pinned = None if args.all_cores else cpu_topology.pin_to_preferred()
    if args.honest_memory:
        # The declared figure is what this box offers when the pool is the only
        # thing on it.  While work the pool did not schedule is running, the
        # honest offer is lower, and a ledger advertising the high-water mark
        # is a promise the box cannot keep.
        try:
            with open("/proc/meminfo", encoding="utf-8") as handle:
                fields = dict(
                    (line.split(":", 1)[0], int(line.split()[1]))
                    for line in handle if ":" in line
                )
            free_gb = fields.get("MemAvailable", 0) // (1024 * 1024)
            # Leave the box a working margin rather than offering the last byte.
            capacity["mem_gb"] = max(0, min(args.mem_gb, free_gb - 8))
        except OSError:
            pass

    queue = pool.PoolQueue(SH / "pb-queue")
    if args.honest_memory:
        queue.ledger().retire_free_capacity({"mem_gb": capacity["mem_gb"]})
    host = socket.gethostname()
    # A box offers its own hostname as well as its class.  Item tags must be a
    # subset of the worker's, so without this an action pinned to one box --
    # which is every action whose checkout is a box-local worktree rather than
    # shared storage -- matches no worker and never runs.
    offered = [args.klass, host, *args.tag]
    if args.gpu_slots <= 0:
        # A box with no GPU must say so, or an action demanding gpu=1 matches
        # it on tags and then fails at run time instead of waiting for a box
        # that can serve it.
        offered.append("cpu")
    if pinned is not None:
        print(f"[{host}] pinned to {cpu_topology.as_range(pinned)} "
              f"({len(pinned)} of {len(cpu_topology.classify()[0]) + len(cpu_topology.classify()[1])} cpus)",
              flush=True)
    idle = 0
    served = 0
    while True:
        outcome = queue.serve_once(
            tags=offered, has_gpu=args.gpu_slots > 0, python=args.python,
            timeout_s=args.timeout_s, capacity=capacity,
        )
        if outcome is None:
            idle += 1
            if args.once or idle >= args.max_idle:
                free = queue.ledger().available()
                print(f"[{host}] nothing admissible ({idle} idle polls); "
                      f"served {served}; free {free}", flush=True)
                return 0
            time.sleep(args.poll_s)
            continue
        idle = 0
        served += 1
        record = {k: outcome.get(k) for k in
                  ("action_key", "status", "returncode", "elapsed_s")}
        print(f"[{host}] {json.dumps(record)}", flush=True)
        tail = str(outcome.get("stderr") or "").strip().splitlines()[-6:]
        if outcome.get("status") != "executed" and tail:
            for line in tail:
                print(f"[{host}]   ! {line}", flush=True)
        if args.once:
            return 0


if __name__ == "__main__":
    raise SystemExit(main())
