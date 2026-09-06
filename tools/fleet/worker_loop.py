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

**What the box offers is observed, not only declared.**  The ledger is exact
about what the pool scheduled and was blind to everything else, so a box six
hours into an out-of-pool encode campaign reported every token free
(sparklina, 2026-09-04 01:06: four GPU processes, ``ledger free 51 held 0``).
Every poll now subtracts what else is running -- the GPU's compute apps and
``MemAvailable``, each minus what the pool's own held tokens account for -- and
retires free tokens to the remainder, so a busy box looks busy whoever made it
busy.  Cores are read and recorded but deliberately not charged: a ``cpu``
token is a slot and the run queue counts threads, so clamping on it charges the
box for its own multithreaded actions.  ``prismabuild.box_capacity`` holds the
arithmetic and the reasons for both.

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
  itself to the preferred set by default. With ``--all-cores`` it retains
  the inherited mask and the ledger assigns preferred CPUs first, fallback
  CPUs for overflow. Each action inherits only its reserved CPUs. See
  ``prismabuild.cpu_topology``.
"""
import argparse
import json
import os
import socket
import signal
import sys
import time
from pathlib import Path

SH = Path("/mnt/shared/prismabuild-fleet")
sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
from runtime_paths import generation_root  # noqa: E402

RUNTIME_ROOT = generation_root(__file__)
sys.path.insert(0, str(RUNTIME_ROOT / "src"))
from prismabuild import box_capacity, cpu_topology, pool  # noqa: E402

#: Consecutive ``serve_once`` failures before the loop gives up and lets the
#: supervisor replace it.  Survive the items; do not survive a broken box.
MAX_CONSECUTIVE_ERRORS = 5

#: The receipt beside these imported bytes proves what this process loaded.
GENERATION_VERSION = RUNTIME_ROOT / "RUNTIME_VERSION.json"
#: The stable name crosses the generation boundary on every idle poll, which
#: is how a loop notices that the publisher activated a successor.
RUNTIME_VERSION = SH / "repo" / "RUNTIME_VERSION.json"


def _commit_at(path: Path) -> str:
    try:
        return str(json.loads(path.read_text()).get("commit") or "")
    except (OSError, ValueError):
        return ""


def published_commit() -> str:
    """The commit at the live generation boundary, or "" if unknown."""

    return _commit_at(RUNTIME_VERSION)


def loaded_runtime_commit() -> str:
    """The commit beside the immutable source tree this loop imported."""

    return _commit_at(GENERATION_VERSION)


def inherited_cpus() -> int:
    """How many CPUs this process may actually run on.

    ``--all-cores`` turns the topology pin off, and the automatic capacity then
    fell back to ``os.cpu_count()``, which is the machine's total rather than
    this process's affinity.  An outer ``taskset`` or cpuset confines the loop
    and every action it launches, so a confined worker advertised the whole
    box: the checked-in dl380g10 configuration passes ``--all-cores``, and 80
    cpu tokens against a two-CPU affinity is the same promise the box cannot
    keep that the capacity drift was.  The topology pinning path already
    preserved an outer restriction; turning the pin off must not throw it
    away.

    ``os.cpu_count()`` remains the last resort, for a platform with no
    affinity call at all.
    """

    try:
        return len(os.sched_getaffinity(0)) or 1
    except (AttributeError, OSError):
        return os.cpu_count() or 1


def census_line(queue) -> str:
    """The fleet's width, or why it is missing -- and never an exception.

    ``3711b29`` bought this loop the contract that one bad item must not take
    the worker with it, and it bought it by wrapping ``serve_once``.  A
    diagnostic added beside that handler is outside it: ``placement_census``
    reads EVERY ready item, including the ones tagged for other boxes that
    ``claim`` skips at ``_placement_matches`` before ``demand_of`` is ever
    reached, so one corrupted or out-of-band write became a raw traceback and
    an immediate exit on a box that was otherwise fine -- once per supervisor
    cycle, against an item that is still there.

    The census now counts an unreadable item instead of raising, which fixes
    the case that was found.  This exists for the ones that are not: a line
    of telemetry has no business deciding whether this box keeps working.
    """

    try:
        return pool.describe_placement_census(queue.placement_census())
    except Exception as exc:                                     # noqa: BLE001
        return f"fleet width unavailable ({type(exc).__name__}: {exc})"


def main():
    """Drain an in-flight action on SIGTERM, then exit before the next poll.

    The supervisor's idle snapshot can race with claim acquisition. A signal
    therefore requests shutdown instead of interrupting materialization or
    execution and leaving a lease for the reaper. Action cancellation remains
    the queue withdrawal protocol.
    """
    stopping = False

    def request_stop(signum, frame):
        nonlocal stopping
        stopping = True

    previous = signal.signal(signal.SIGTERM, request_stop)
    try:
        return _run_loop(lambda: stopping)
    finally:
        signal.signal(signal.SIGTERM, previous)


def _run_loop(stop_requested):
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true",
                    help="serve at most one action and exit, whether or not "
                         "the queue had anything (debug)")
    ap.add_argument("--timeout-s", type=float, default=7200.0,
                    help="how long a single action may run before this loop "
                         "kills it")
    ap.add_argument("--poll-s", type=float, default=10.0,
                    help="seconds to sleep between queue polls")
    ap.add_argument("--max-idle", type=int, default=6,
                    help="consecutive empty polls before exiting")
    ap.add_argument("--gpu-slots", type=int, default=4,
                    help="concurrent GPU actions this box admits; 0 = no GPU")
    ap.add_argument("--class", dest="klass", default="gb10",
                    help="hardware class this box offers, e.g. gb10 or x86")
    ap.add_argument("--python", default="/usr/bin/python3",
                    help="interpreter that launches the pool worker on this box")
    ap.add_argument("--all-cores", action="store_true",
                    help="retain all inherited CPUs; reserve preferred cores before fallback")
    ap.add_argument("--mem-gb", type=int, default=96,
                    help="memory this box offers the queue, of ~121 GB total")
    ap.add_argument("--tag", action="append", default=[],
                    help="extra placement tag this box offers")
    ap.add_argument("--assume-idle", action="store_true",
                    help="offer the declared numbers without looking at what "
                         "else is running on this box (debug)")
    ap.add_argument("--observe-samples", type=int,
                    default=box_capacity.DEFAULT_SAMPLES,
                    help="consecutive observations that must agree before the "
                         "offer falls; 1 lets a single reading decide")
    ap.add_argument("--cpu-slots", type=int, default=0,
                    help="cores this box offers the queue; 0 = the cores this "
                         "loop is actually pinned to")
    args = ap.parse_args()
    pinned = None if args.all_cores else cpu_topology.pin_to_preferred()
    # Cores are a resource, and until now they were the only one the ledger
    # could not see.  A ``pytest -n 24`` action declaring ``mem_gb=4`` was
    # admitted five times over on one 80-core box on 2026-09-04: load average
    # 371, four concurrent full suites, every one of them slower for it.
    # Memory was never the binding constraint and memory was all the ledger
    # knew about.
    #
    # The offer is what this loop is *pinned to*, not what the box has: on a
    # GB10 that is ten of twenty cores, and offering twenty would be the same
    # promise-the-box-cannot-keep the capacity drift was.  With ``--all-cores``
    # there is no pin, and the offer is then the inherited affinity rather than
    # the machine count; ``--cpu-slots`` can cap either path but cannot
    # exceed its inherited affinity.  See ``inherited_cpus``.
    cores = args.cpu_slots
    if cores <= 0:
        cores = len(pinned) if pinned else inherited_cpus()
    cpu_tiers = cpu_topology.inherited_tiers()
    if cpu_tiers is not None:
        if cores > sum(map(len, cpu_tiers.values())):
            ap.error("--cpu-slots exceeds inherited CPU affinity")
        # An explicit capacity cap retains the best CPUs first.
        ordered = cpu_tiers["preferred"] + cpu_tiers["fallback"]
        enabled = set(ordered[:cores])
        cpu_tiers = {kind: [c for c in values if c in enabled]
                     for kind, values in cpu_tiers.items()}
    # What this box offers when the pool is the only thing on it.  What it can
    # offer *now* is that minus whatever else is running, read at every poll.
    declared = {"gpu": args.gpu_slots, "mem_gb": args.mem_gb, "cpu": cores}

    queue = pool.PoolQueue(SH / "pb-queue")
    # Read once, not per poll: it seeds the observer's window so a loop that
    # starts while the box is busy inherits the standing verdict instead of
    # re-minting, for the length of its window, every token the other loops on
    # this box retired.  A loop exits on ``--max-idle`` and the supervisor
    # replaces it, so that window would come round every half hour.
    # Constructing the queue is inert, but even seeding the observer reads its
    # ledger.  Defer that read until after this generation has fenced itself
    # against the currently published one below.
    observer = None
    observer_initialized = False
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
    errors = 0
    announced: dict[str, int] | None = None
    loaded_commit = loaded_runtime_commit()
    print(f"[{host}] runtime {loaded_commit[:12] or '(unversioned)'}", flush=True)
    while True:
        if stop_requested():
            print(f"[{host}] shutdown requested; current action drained", flush=True)
            return 0
        # Fence stale imported bytes before the first queue read or mutation,
        # and again at the top of every later poll.  In particular, an old
        # orphan reaper must never classify a record emitted by a successor's
        # pbrun schema merely because the successor was activated while this
        # loop was between actions.
        #
        # This is defense in depth, not a rollout handshake: publication can
        # still move after this read and before serve_once.  Cross-generation
        # queue-schema rollout therefore remains an expand/converge/contract
        # operation; this local fence only closes the much larger window in
        # which a loop that already observes the successor keeps touching the
        # queue before exiting.
        current = published_commit()
        if current and current != loaded_commit:
            print(f"[{host}] runtime moved "
                  f"{loaded_commit[:12] or '(unversioned)'} -> "
                  f"{current[:12]}; exiting so the supervisor reloads it",
                  flush=True)
            return 0
        if not observer_initialized:
            observer = None if args.assume_idle else box_capacity.CapacityObserver(
                samples=args.observe_samples,
                ledger_total=queue.ledger().capacity(),
            )
            observer_initialized = True
        # Every kind, every poll -- not just memory, and not just under a flag.
        #
        # Two things make the ledger disagree with the box, and one call
        # answers both.  ``ensure_capacity`` is increase-only by design and
        # runs on every claim attempt, so a ledger remembers the LARGEST
        # capacity any worker ever declared for this host while the offer file
        # is last-writer-wins and falls: measured 2026-09-04, sparklina offered
        # ``gpu: 1`` with FOUR free gpu tokens and three worker processes able
        # to claim against them.  And the ledger is exact about what the pool
        # scheduled and blind to everything else: at 01:06 the same box read
        # every token free while four out-of-pool GPU processes held the card.
        # Admission is by token, so either shape admits work the box cannot
        # run, which is how sparklina went down on 2026-09-03.
        #
        # So the offer is the declaration minus what else is running, and the
        # ledger is retired to it.  ``box_capacity`` explains the attribution
        # (held tokens, not a process tree) and the asymmetry (falls slowly,
        # recovers at once).  Retiring is safe to do bluntly: it deletes FREE
        # tokens only, so a running action never loses the reservation it is
        # executing under, and the total falls the rest of the way as holders
        # finish.  Nothing here is a ratchet: ``ensure_capacity`` re-mints
        # inside ``claim`` on the next poll once the foreign work is gone.
        capacity = dict(declared) if observer is None else observer.offer(
            declared, queue.ledger().held())
        queue.ledger().retire_free_capacity(capacity)
        if capacity != announced:
            seen = observer.last if observer is not None else None
            # ``foreign`` and ``detail``, named as the announce record names
            # them, so a line in a log and a field in ``workers/<host>.json``
            # can be read against each other.
            print(f"[{host}] offer {capacity}"
                  + (f" (declared {declared}; foreign {seen.foreign}; "
                     f"{seen.detail})" if seen is not None and seen.foreign
                     else ""), flush=True)
            announced = dict(capacity)
        # Say what this box offers before asking what it may run.  The queue
        # otherwise knows only what has been *asked for*, which makes an item
        # no box can run look exactly like an item whose box is busy.
        #
        # ``capacity`` stays the declaration: it is what ``placeable`` reads,
        # and the question there is whether any box could EVER run the item.
        # A box occupied by someone else's encode is a slow submission, and
        # publishing the live figure there would make ``pbrun`` refuse it.
        queue.announce(
            host=host, tags=offered, has_gpu=args.gpu_slots > 0,
            capacity=declared, observed_capacity=capacity,
            foreign=(observer.last.foreign if observer is not None
                     and observer.last is not None else None),
            observed_detail=(observer.last.detail if observer is not None
                             and observer.last is not None else None),
            runtime_commit=loaded_commit, cpu_tiers=cpu_tiers,
        )
        # One bad item must not take the worker with it.  ``serve_once``
        # re-raises whatever ``execute`` raised, and this loop had no handler,
        # so a single unexecutable queue record -- a payload-less stub raising
        # KeyError, an OSError on spawn -- killed the process, and did it once
        # per attempt.  A worker's job is to survive the items it is given.
        #
        # ``Exception``, not ``BaseException``: KeyboardInterrupt and
        # SystemExit must still stop the loop.  And the count is consecutive,
        # so a box whose every poll raises stops rather than spinning: that
        # shape is the box being broken, not the items.
        if stop_requested():
            return 0
        try:
            outcome = queue.serve_once(
                tags=offered, has_gpu=args.gpu_slots > 0, python=args.python,
                timeout_s=args.timeout_s, capacity=capacity, cpu_tiers=cpu_tiers,
                adaptive_cpu=not args.assume_idle, containment=True,
            )
        except Exception as exc:                                 # noqa: BLE001
            # The raise may have come two hours into an action, so this loop
            # has been away from the box for as long as a returning one has.
            # It may equally have come from ``reap_stale`` before anything was
            # claimed, and the two are not distinguishable from here without
            # threading state out of ``serve_once``.  Emptying the window on
            # both is the conservative reading of the pair.
            if observer is not None:
                observer.rejoin(queue.ledger().capacity())
            errors += 1
            print(f"[{host}] serve_once raised ({errors} in a row): "
                  f"{type(exc).__name__}: {exc}", flush=True)
            if errors >= MAX_CONSECUTIVE_ERRORS:
                print(f"[{host}] {errors} consecutive failures; the box, not "
                      f"the items -- exiting for the supervisor to replace",
                      flush=True)
                return 1
            if stop_requested():
                return 0
            time.sleep(args.poll_s)
            continue
        errors = 0
        if outcome is not None and observer is not None:
            # Back from an action, having taken no reading of the box for as
            # long as it ran.  The window's samples are from before it and the
            # offer is a maximum, so they would go on deciding it for
            # ``--observe-samples`` - 1 polls -- the polls in which this loop
            # claims again -- and ``ensure_capacity`` would re-mint against
            # them every free token a sibling loop had retired meanwhile.  The
            # ledger is read here, after the action's tokens are released, and
            # caps the first reading back rather than seeding it: part of that
            # total is the token this loop has just let go of.
            observer.rejoin(queue.ledger().capacity())
        if outcome is None:
            idle += 1
            # Entering an idle streak is the moment this box starts paying for
            # the fleet's WIDTH, so say how much of the waiting queue no other
            # box could take.  Box-local worktrees pin an action to one box,
            # and the pin is invisible from the queue's own counts: ten items
            # in ``ready`` look identical whether they are queued behind one
            # busy box or spread across three.  Live on 2026-09-04, with two
            # boxes idle: "ready 10, 10 on exactly one box (sparky 10)".
            #
            # Printed at the START of the streak, not on every poll: the
            # interesting event is the transition, and ``--max-idle`` is 500
            # on this fleet, so an exit-only line would appear about twice a
            # day per loop.
            if idle == 1:
                print(f"[{host}] idle; {census_line(queue)}", flush=True)
            if args.once or idle >= args.max_idle:
                free = queue.ledger().available()
                print(f"[{host}] nothing admissible ({idle} idle polls); "
                      f"served {served}; free {free}; {census_line(queue)}",
                      flush=True)
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
