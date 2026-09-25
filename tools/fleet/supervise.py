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
floor count and loop arguments read from ``fleet_boxes.json`` in the checkout,
so the shape of the fleet is a versioned file rather than three command lines
nobody wrote down; and elastic housekeeping width driven by the claims this
box is actually holding, which is the one signal about its own load the
supervisor can attribute.  ``--loops`` remains a fixed-count operator
override.

A long-lived supervisor also follows the immutable runtime generation at a
cycle boundary.  It validates the new generation and its own loaded bytes,
then preserves its exclusive local claim across ``exec``.  Worker loops are
not children to recycle during that handoff: active work finishes under the
generation that claimed it, and idle loops upgrade independently.

The claim is deliberately box-local.  It says "a supervisor owns THIS box",
which is a fact about one machine's processes; on shared storage it would let
one box's supervisor silence another's.  It is not a scheduling primitive and
admits no work -- every action still goes through the pool's ledger, which is
the thing that can actually balance two boxes.

``--ensure`` is the form a periodic job uses: become the supervisor if there
isn't one, exit quietly if there is.  Nothing supervises the supervisor, so
that idempotence is what makes a five-minute timer a sufficient answer.
And ``--ensure`` now ensures more than presence: a role loop running an argv
the current ``fleet_boxes.json`` no longer declares, or an executable from a
generation the fleet has retired, is stopped when idle and respawned from
the active generation, with the restart stamped in the queue's
``generation-drift`` record namespace beside the workers' own claim
refusals.

A role must not depend on its launcher being single.  On 2026-09-19 a
duplicate supervisor started its own storage reader beside the primary's --
two readers of one ready list, double-published movers and contradictory
fill measurements compounding the movers wedge (#709).  So each role's
process takes its own host-local singleton flock (``worker_loop``'s
``take_role_singleton``, under a private per-uid namespace stable across
generations), this supervisor probes that lock before spawning and reports a
held one instead of racing it, and a role the kernel reports stopped -- alive
in every census and serving nothing, indistinguishable from a quiet loop in
the process table -- is named with pid, role and state in this supervisor's
own log on the transition.  Nothing unproven is inspected or signalled, and
a stopped role keeps its lock until an operator continues or terminates it.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import select
import signal
import socket
import stat
import subprocess
import sys
import time
from typing import Collection, TextIO

sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
from runtime_paths import generation_root  # noqa: E402
import fleet_roster  # noqa: E402
# The generation readers, and the drift-record writer beside them, live in
# the worker loop module -- the same object the role loops import as
# ``runtime_gate`` -- so the supervisor, the workers and the roles all stamp
# refusals through one code path rather than three near-identical ones.
import worker_loop as runtime_gate  # noqa: E402
RUNTIME_ROOT = generation_root(__file__)
sys.path.insert(0, str(RUNTIME_ROOT / "src"))
from prismabuild import pool  # noqa: E402

MIRROR = Path("/mnt/shared/prismabuild-fleet")
CONFIG = Path(__file__).resolve().parent / "fleet_boxes.json"
CLAIM = Path("/home/rob/tmp/prismabuild-supervisor.claim")
LOG_DIR = Path("/home/rob/tmp")
PROC = Path("/proc")
SYSTEMD_UNIT = Path.home() / ".config/systemd/user/prismabuild-supervisor.service"
SYSTEMD_EXEC = ("ExecStart=/usr/bin/python3 "
                "/mnt/shared/prismabuild-fleet/repo/tools/supervise.py --ensure --systemd")

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

#: Role name to the helper script that serves it.  A role is an auxiliary
#: single-instance loop a box declares in ``fleet_boxes.json``, deliberately
#: outside the worker sizing law: it claims nothing, holds no lease and earns
#: no capacity, so counting it as a poller would make a box look busier than
#: it is.  ``storage`` is the file server, which makes the next actions'
#: declared bytes resident before anybody claims them (issue #487).
ROLE_SCRIPTS = {"storage": "prewarm_loop.py", "tiers": "tier_loop.py",
                # The Prometheus exporter (#1020): one per fleet, on the box
                # whose own filesystem holds the queue, where an unchanged
                # queue costs a scrape one ``lstat`` per directory.
                "metrics": "pbmetrics.py"}

#: A role child that exits ``ROLE_SINGLETON_HELD_EXIT`` refused to run: its
#: singleton lock or, for ``metrics``, its port is held by something this
#: supervisor's lock probe cannot see (a unit whose ``PrivateTmp`` hides its
#: lock, #1046).  Nothing the supervisor observes tells it when that holder
#: goes away, so it retries -- but not once a tick.  The first retry waits
#: ``BUSY_INTERVAL_S``, each consecutive refusal doubles the wait, and the
#: wait stops growing at this ceiling: one refusal record per five minutes
#: instead of one per 5 s tick (720 an hour), and a role whose holder is
#: stopped is back within five minutes without an operator touching the
#: supervisor.  Held in memory only: a re-exec'd supervisor forgets it and
#: pays at most one refusal to learn it again.
ROLE_REFUSAL_BACKOFF_MAX_S = 300.0

#: pid -> role for every role child this process image spawned, so a reaped
#: exit status can be attributed to a role and never to a worker loop.
_ROLE_CHILDREN: dict[int, str] = {}
#: role -> the standing refusal: ``count`` consecutive refusals, the last
#: ``pid`` and raw ``status``, ``retry_at`` on ``_monotonic``'s clock, and
#: whether the supervisor has ``announced`` it.
_ROLE_REFUSALS: dict[str, dict] = {}


def _monotonic() -> float:
    """The backoff clock; one name a test can repoint."""

    return time.monotonic()


#: The single-letter scheduler states ``/proc/<pid>/stat`` reports, named for
#: the supervisor's health evidence.  ``T`` is what SIGSTOP or SIGTSTP leaves
#: and ``t`` what a tracer leaves; a process in either is alive and serving
#: nothing, which the process table otherwise presents exactly like a quiet
#: loop (issue #709).  ``Z`` is a zombie -- dead, awaiting reap -- and is
#: deliberately not an alarm.
PROC_STATE_NAMES = {
    "R": "running", "S": "sleeping", "D": "uninterruptible-wait",
    "T": "stopped", "t": "tracing-stop", "X": "dead", "Z": "zombie",
    "I": "idle", "P": "parked",
}
#: The states a SIGSTOPped role is in.
STOPPED_STATES = frozenset({"T", "t"})
#: The state names a health line alarms on: the stopped states above, an
#: unreadable state, and the two dead states.  Only a state outside this set
#: clears an earlier alarm, so a zombie or an unrecognized letter can never
#: be reported as a return to health.
ALARM_STATE_NAMES = frozenset({"stopped", "tracing-stop", "unknown",
                               "zombie", "dead"})

# Worker loops are queue pollers, not CPU reservations.  Keep a couple ready
# to claim without a process-start round trip, while the queue's admission
# controller remains the sole authority over whether an action may run.
#
# The reserve is also what absorbs the gap between two actions.  A loop that
# has just served one does not sleep: only ``worker_loop``'s idle and error
# branches wait ``--poll-s``, and the served branch falls straight back into
# ``serve_once``.  So a working loop is unclaimed for one claim round trip,
# not one poll interval, and two spares cover more of those than a 5 s
# supervision tick can land in.  That is why the sizing law below needs no
# smoothing window: there is no long transient to smooth.
IDLE_RESERVE = 2
BUSY_INTERVAL_S = 5.0
LOOPS_PER_VISIBLE_CPU = 4
BYTES_PER_HOUSEKEEPING_LOOP = 256 * 1024 * 1024
INHERITED_CLAIM_FD_ENV = "PRISMABUILD_SUPERVISOR_CLAIM_FD"
RUNTIME_VERSION_SCHEMA = "prismaquant.prismabuild.runtime_version.v1"
# A large inherited zombie backlog must not starve the census or re-exec.
MAX_REAPS_PER_TICK = 256


@dataclass(frozen=True)
class PublishedGeneration:
    root: Path
    name: str
    commit: str
    supervisor: Path


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _published_generation(
    root: Path | None = None, *, entrypoint: Path | None = None,
) -> PublishedGeneration | None:
    """Return one fully validated published generation, or ``None``.

    A receipt-shaped directory is insufficient.  The root must be a direct,
    non-staging child of this fleet's generation store, its receipt must name
    that generation, and the supervisor bytes must match the sealed manifest.
    Transient or damaged publication state leaves the current supervisor in
    charge and is retried on the next boundary.
    """

    try:
        store = (MIRROR / "runtime-generations").resolve(strict=True)
        candidate = (_current_root() if root is None else root).resolve(strict=True)
        if (candidate.parent != store or candidate.name.startswith(".")
                or candidate.name in ("", ".", "..")):
            return None
        receipt = json.loads(
            (candidate / "RUNTIME_VERSION.json").read_text(encoding="utf-8"))
        if not isinstance(receipt, dict):
            return None
        commit = receipt.get("commit")
        files = receipt.get("files")
        if (receipt.get("schema") != RUNTIME_VERSION_SCHEMA
                or receipt.get("generation") != candidate.name
                or not isinstance(commit, str)
                or re.fullmatch(r"[0-9a-f]{40}", commit) is None
                or not isinstance(files, dict)):
            return None
        supervisor = (
            candidate / "tools" / "supervise.py" if entrypoint is None
            else entrypoint
        ).resolve(strict=True)
        relative = supervisor.relative_to(candidate).as_posix()
        if relative not in ("tools/supervise.py", "tools/fleet/supervise.py"):
            return None
        expected = files.get(relative)
        if (not isinstance(expected, str)
                or re.fullmatch(r"[0-9a-f]{64}", expected) is None
                or _sha256(supervisor) != expected):
            return None
        return PublishedGeneration(candidate, candidate.name, commit, supervisor)
    except (OSError, ValueError, TypeError):
        return None


def _loaded_published_generation() -> PublishedGeneration | None:
    """The receipt identity of the exact supervisor bytes now executing."""

    return _published_generation(RUNTIME_ROOT, entrypoint=Path(__file__))


def _inherited_claim_handle() -> TextIO | None:
    """Validate and adopt the flock description preserved across ``exec``."""

    raw = os.environ.pop(INHERITED_CLAIM_FD_ENV, None)
    if raw is None:
        return None
    if re.fullmatch(r"[0-9]+", raw) is None:
        raise SystemExit("invalid inherited PrismaBuild supervisor claim fd")
    try:
        descriptor = int(raw)
    except (ValueError, OverflowError) as exc:
        raise SystemExit(
            "invalid inherited PrismaBuild supervisor claim fd") from exc
    if descriptor <= 2:
        raise SystemExit("invalid inherited PrismaBuild supervisor claim fd")
    try:
        held = os.fstat(descriptor)
        named = CLAIM.stat()
        if (not stat.S_ISREG(held.st_mode) or held.st_uid != os.getuid()
                or (held.st_dev, held.st_ino) != (named.st_dev, named.st_ino)):
            raise OSError("descriptor does not name the supervisor claim")
        # This succeeds only when this open-file description owns the lock (or
        # can acquire an otherwise unowned one).  The fresh description below
        # must then be excluded, proving the lock survived the exec boundary.
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        contender = CLAIM.open("r+")
        try:
            try:
                fcntl.flock(contender, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                pass
            else:
                raise OSError("inherited claim does not exclude a contender")
        finally:
            contender.close()
        os.set_inheritable(descriptor, False)
        return os.fdopen(descriptor, "r+", encoding="utf-8", closefd=True)
    except (OSError, OverflowError) as exc:
        try:
            os.close(descriptor)
        except OSError:
            pass
        raise SystemExit(
            f"invalid inherited PrismaBuild supervisor claim fd: {exc}") from exc


def _claim_handle(*, ensure: bool) -> TextIO | None:
    """Adopt an exec-preserved lock or acquire the box-local supervisor lock."""

    inherited = _inherited_claim_handle()
    if inherited is not None:
        return inherited
    CLAIM.parent.mkdir(parents=True, exist_ok=True)
    handle = CLAIM.open("a+", encoding="utf-8")
    try:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        handle.close()
        if ensure:
            return None
        raise SystemExit(f"another supervisor owns {CLAIM}")
    handle.seek(0)
    handle.truncate()
    handle.write(f"{os.getpid()}\n")
    handle.flush()
    return handle


def _reexec_if_published(
    loaded: PublishedGeneration | None, claim_handle: TextIO,
) -> bool:
    """Exec a different trusted live generation at a cycle boundary.

    Worker loops are separate owned processes and are deliberately untouched.
    They finish active jobs under their immutable generation and perform their
    own idle upgrade; the replacement supervisor merely adopts their census.
    """

    if loaded is None:
        return False
    try:
        live_root = _current_root().resolve(strict=True)
    except OSError:
        return False
    if live_root == loaded.root:
        return False
    replacement = _published_generation(live_root)
    if replacement is None:
        return False
    descriptor = claim_handle.fileno()
    argv = [sys.executable, str(replacement.supervisor), *sys.argv[1:]]
    environment = {**os.environ, INHERITED_CLAIM_FD_ENV: str(descriptor)}
    print(f"[{socket.gethostname()}] supervisor generation {loaded.name} -> "
          f"{replacement.name}; preserving pid {os.getpid()} and worker loops",
          flush=True)
    os.set_inheritable(descriptor, True)
    try:
        os.execve(sys.executable, argv, environment)
    except OSError as exc:
        os.set_inheritable(descriptor, False)
        print(f"[{socket.gethostname()}] supervisor re-exec refused: {exc}",
              flush=True)
        return False
    # Test doubles can return even though a real successful exec never does.
    os.set_inheritable(descriptor, False)
    return True


def _current_root() -> Path:
    return MIRROR / "repo"


def _queue_root() -> Path:
    """The pull queue this box's loops serve, spelled as they spell it.

    Read from ``MIRROR`` at call time rather than bound at import, for the
    same reason ``_current_root`` is: it is the one name a test can repoint to
    keep the suite off the live store.
    """

    return MIRROR / "pb-queue"


def _claim_holders() -> frozenset[int] | None:
    """The pids on this box the queue says hold a claim, or ``None`` if unknown.

    ``None`` is not the empty set.  A queue root that is not there answers the
    question -- nobody holds a claim on a box with no queue, which is where the
    fleet ends up after the cutover -- while a listing that fails for any other
    reason answers nothing, and the caller must not read silence as idleness.
    """

    try:
        return frozenset(pool.PoolQueue(_queue_root()).claim_holder_pids())
    except (OSError, pool.PoolContractError):
        return None


def _roster_entry(host: str) -> tuple[dict, dict] | None:
    """This box's raw roster entry and the roster file it came from.

    The roster document travels with the entry because a box's declaration
    can depend on a fleet-wide value beside ``boxes`` -- the local-disk free
    floor that ``--spool-gb`` is checked against -- and both must come from
    the same file, never one from the live generation and one from the
    checkout.  ``None`` is the same "no answer" ``_box_entry`` documents.
    """

    # A supervisor intentionally outlives a generation.  Prefer the current
    # live generation; CONFIG is only the checkout/bootstrapping fallback.
    for path in (_current_root() / "tools" / "fleet_boxes.json", CONFIG):
        try:
            document = json.loads(path.read_text())
            boxes = document["boxes"]
        except (OSError, ValueError, KeyError, TypeError):
            continue
        if not isinstance(boxes, dict):
            continue
        if host in boxes and isinstance(boxes[host], dict):
            return boxes[host], document
        # The second Spark was renamed from gx10-6b77 to sparklina.  Its
        # declared alias names the same machine, not a second capacity offer.
        # Keep one shape so the old and new hostname cannot drift apart.
        aliases = [(name, shape) for name, shape in boxes.items()
                   if isinstance(shape, dict) and shape.get("_alias") == host]
        if len(aliases) > 1:
            raise SystemExit(f"ambiguous fleet hostname alias {host}: {path}")
        if aliases:
            return aliases[0][1], document
    return None


def _box_entry(host: str) -> dict | None:
    """This box's raw roster entry, or ``None`` when no file names it.

    ``None`` covers both "no readable roster" (a transient NFS failure or a
    damaged generation, which must not take the box's loops with it) and "no
    entry" (which ``_config`` still refuses at startup, where there is no
    previous shape to keep).  Alias handling matches ``_config``: the roster
    key and its ``_alias`` name the same machine.
    """

    found = _roster_entry(host)
    return None if found is None else found[0]


def box_presence(host: str) -> tuple[str, dict[str, object]] | None:
    """This box's declared presence, or ``None`` when it cannot be read.

    ``None`` is "no answer", not "active": callers keep their previous
    behaviour on it.  A malformed presence refuses rather than answering,
    because an ambiguous presence is not an active box (#606).
    """

    entry = _box_entry(host)
    if entry is None:
        return None
    try:
        return fleet_roster.box_status(host, entry)
    except fleet_roster.RosterPresenceError as exc:
        raise SystemExit(f"cannot establish this box's roster presence: {exc}")


def _config(host: str) -> dict:
    """Read this box's declared shape, from the checkout or the published copy."""

    entry = _box_entry(host)
    if entry is None:
        raise SystemExit(
            f"no fleet_boxes.json entry for {host}; refusing to guess what this "
            f"box offers -- add it to tools/fleet/fleet_boxes.json and publish"
        )
    try:
        status, detail = fleet_roster.box_status(host, entry)
    except fleet_roster.RosterPresenceError as exc:
        raise SystemExit(f"cannot establish this box's roster presence: {exc}")
    if status in fleet_roster.ABSENT:
        raise SystemExit(
            f"fleet_boxes.json declares {host} {status} "
            f"({detail['reason']}) by {detail['by']}; refusing to offer "
            f"loops or roles -- un-declare the absence and publish to resume"
        )
    return entry


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
    args = [str(a) for a in config.get("args", [])]
    if SPOOL_FLAG in args or any(a.startswith(SPOOL_FLAG + "=") for a in args):
        # Only a box that declares a spool budget reads anything more; every
        # other box's arguments are the file's, byte for byte (#910).
        found = _roster_entry(host)
        document = found[1] if found is not None else {}
        args = spool_declaration(host, config, document, args)
    return (override_loops or int(config.get("loops", 1)), args)


#: The worker loop flag that declares this box's local disk budget, in GiB
#: (#747), and the roster fields it is derived from (#910, #911).  In the
#: roster its value is ``auto`` -- the whole measured room -- or a whole
#: number of GiB that caps the measured room.  The supervisor always passes
#: the worker an integer: the roster is its input, not a command line.
SPOOL_FLAG = "--spool-gb"
SPOOL_AUTO = "auto"
#: The host-ledger kind the flag declares.  The produced-spool window and
#: declared bounded local scratch both reserve it.
SPOOL_KIND = "spool_gb"
#: Per box: the directory whose filesystem the budget is carved from.  The
#: roots themselves are the actions' sealed variables, not something a box
#: knows, so the roster names the disk rather than the supervisor guessing it.
LOCAL_DISK_FIELD = "local_disk"
#: Fleet-wide, beside ``boxes``: the share of a local filesystem, in whole
#: percent of its size, that the fleet keeps free.  It is Rob's disk-headroom
#: rule, stated in the file rather than assumed by the code.
LOCAL_DISK_FLOOR_FIELD = "local_disk_free_floor_percent"
GIB = 1 << 30
#: One settled verdict per distinct declaration for the life of this
#: process, so the disk is measured at supervisor start -- and again after a
#: publish, which re-execs -- rather than every tick.  Other writers move free
#: space continuously, and a per-tick measurement would change the loops'
#: arguments, and so cycle every idle loop, with every GiB they wrote.  A
#: measurement a running holder blocks is not settled; see ``_spool_budget``.
_SPOOL_VERDICTS: dict[tuple, list[str]] = {}
#: The last line printed for a declaration whose measurement is waiting on a
#: holder, so a wait that repeats every tick is logged once.
_SPOOL_WAITING: dict[tuple, str] = {}


def local_disk_room(path: str, floor_percent: int, *,
                    statvfs=None) -> dict[str, int]:
    """What the filesystem under ``path`` can still give, above the floor.

    ``free_bytes`` is what an unprivileged writer can allocate (``f_bavail``,
    which already withholds root's reserve) and ``floor_bytes`` is
    ``floor_percent`` of the filesystem's size, rounded up.  ``room_bytes``
    is the first minus the second and may be negative.
    """

    # Bound at call time, not definition time, so a repointed ``os.statvfs``
    # is the one read.
    stat_result = (os.statvfs if statvfs is None else statvfs)(path)
    size = int(stat_result.f_blocks) * int(stat_result.f_frsize)
    free = int(stat_result.f_bavail) * int(stat_result.f_frsize)
    floor = -(-size * floor_percent // 100)
    return {"size_bytes": size, "free_bytes": free, "floor_bytes": floor,
            "room_bytes": free - floor}


def _spool_gb_of(args: list[str]) -> int | None:
    """The one ``--spool-gb`` value ``args`` declares, or ``ValueError``.

    ``None`` is ``auto``: no ceiling on the measured room.
    """

    if any(a.startswith(SPOOL_FLAG + "=") for a in args):
        raise ValueError(f"spell {SPOOL_FLAG} as two arguments, flag and value")
    positions = [i for i, a in enumerate(args) if a == SPOOL_FLAG]
    if len(positions) != 1:
        raise ValueError(f"{SPOOL_FLAG} appears {len(positions)} times")
    try:
        value = args[positions[0] + 1]
    except IndexError:
        raise ValueError(f"{SPOOL_FLAG} has no value") from None
    if value == SPOOL_AUTO:
        return None
    if not value.isascii() or not value.isdigit():
        raise ValueError(f"{SPOOL_FLAG} {value!r} is neither {SPOOL_AUTO!r} "
                         "nor a whole number of GiB")
    return int(value)


def _without_spool(args: list[str]) -> list[str]:
    """``args`` with every ``--spool-gb`` declaration removed."""

    out: list[str] = []
    skip = False
    for arg in args:
        if skip:
            skip = False
            continue
        if arg == SPOOL_FLAG:
            skip = True
            continue
        if arg.startswith(SPOOL_FLAG + "="):
            continue
        out.append(arg)
    return out


def _with_spool(args: list[str], gib: int) -> list[str]:
    """``args`` with the one ``--spool-gb`` value replaced by ``gib``."""

    out = list(args)
    out[out.index(SPOOL_FLAG) + 1] = str(gib)
    return out


def _held_spool(ledger) -> int:
    """``spool_gb`` tokens running actions hold in ``ledger``, error-visibly.

    A ledger nobody has created holds nothing.  Any other listing failure
    raises: "cannot see a holder" must not read as "no holder", because the
    caller would then measure a disk whose written bytes it cannot account
    for.
    """

    try:
        holders = pool._scan_visible(ledger.held_dir)
    except FileNotFoundError:
        return 0
    return sum(1 for holder in holders
               for name in pool.held_names_visible(ledger, holder.name)
               if name.rsplit("-", 1)[0] == SPOOL_KIND)


def _spool_budget(host: str, entry: dict, document: dict, args: list[str], *,
                  statvfs=None) -> tuple:
    """Measure this box's ``--spool-gb`` from its disk: a verdict and a value.

    ``("offer", gib, detail)`` and ``("refuse", reason)`` are settled; either
    is kept for the life of the process.  ``("wait", reason)`` is not: a
    running action holds ``spool_gb`` here, and the measurement cannot be
    taken until none does.

    The measured budget is ``f_bavail`` on ``local_disk`` minus the fleet
    floor, in whole GiB, capped at a numeric declaration.  It is taken only
    while the host ledger shows no ``spool_gb`` held, read before and after
    ``statvfs``.  With nothing held, every byte on the disk is either
    unreserved or belongs to no reservation, so the room is exactly what the
    ledger may promise.  With a holder, the room is short by what the holder
    has already written and cannot be told from anything else on the disk:
    counting it as used charges the holder twice, and adding the holder's
    whole reservation back, as the ``mem_gb`` offer does, credits the part it
    has not yet written, which the next holder could then reserve too.
    """

    try:
        ceiling = _spool_gb_of(args)
    except ValueError as exc:
        return ("refuse", str(exc))
    if ceiling == 0:
        return ("offer", 0, "")
    declared = SPOOL_AUTO if ceiling is None else str(ceiling)
    path = entry.get(LOCAL_DISK_FIELD)
    if not isinstance(path, str) or not path.startswith("/"):
        return ("refuse", f"{host} declares {SPOOL_FLAG} {declared} but no "
                f"absolute {LOCAL_DISK_FIELD!r} path naming the filesystem it "
                "is on")
    floor = document.get(LOCAL_DISK_FLOOR_FIELD) if isinstance(document, dict) else None
    if type(floor) is not int or not 0 <= floor < 100:
        return ("refuse", f"the roster states no {LOCAL_DISK_FLOOR_FIELD!r} as a "
                f"whole percent from 0 to 99 (found {floor!r}), so there is no "
                f"floor to measure {SPOOL_FLAG} {declared} against")
    try:
        from prismabuild import produced_spool

        produced_spool._local_disk(Path(path))
    except Exception as exc:  # noqa: BLE001 - any failure refuses the declaration
        return ("refuse", f"{LOCAL_DISK_FIELD} {path} is not a usable local "
                f"disk: {exc}")
    ledger = pool.PoolQueue(_queue_root()).ledger(host)

    def holders_wait() -> tuple[str, str] | None:
        try:
            held = _held_spool(ledger)
        except (OSError, pool.PoolContractError) as exc:
            return ("wait", f"cannot read the host ledger at {ledger.base}: {exc}")
        if held:
            return ("wait", f"running actions hold {held} GiB of {SPOOL_KIND} "
                    "here, and the bytes they have written cannot be told "
                    "from other use of the disk")
        return None

    # Held is read on both sides of ``statvfs``: a holder that claimed and
    # wrote in between would otherwise be measured as if it were not there.
    waiting = holders_wait()
    if waiting is not None:
        return waiting
    try:
        room = local_disk_room(path, floor, statvfs=statvfs)
    except OSError as exc:
        return ("refuse", f"cannot read free space on {LOCAL_DISK_FIELD} "
                f"{path}: {exc}")
    waiting = holders_wait()
    if waiting is not None:
        return waiting
    gib = max(0, room["room_bytes"]) // GIB
    if ceiling is not None:
        gib = min(ceiling, gib)
    detail = (f"{room['free_bytes']} B free - {room['floor_bytes']} B "
              f"({floor}%) floor = {room['room_bytes']} B on {path}")
    if gib == 0:
        return ("refuse", f"{SPOOL_FLAG} {declared} measures 0 GiB: {detail}")
    return ("offer", gib, detail)


def spool_declaration(host: str, entry: dict, document: dict,
                      args: list[str], *, statvfs=None) -> list[str]:
    """The loop arguments with ``--spool-gb`` measured, or dropped with a reason.

    A settled measurement is kept once per distinct declaration in this
    process (see ``_SPOOL_VERDICTS``).  A refusal drops the flag rather than
    exiting: this supervisor runs under ``Restart=always``, so an exit would
    take every loop on the box with it, where a refused budget should cost the
    box only its disk kind.  Actions that need it then record
    ``never_fits_capacity`` on this box and are claimed where it fits.

    While a running action holds ``spool_gb`` here, nothing is settled and the
    loops offer the ledger's current total, capped at a numeric declaration:
    the last measured value, which neither grows nor shrinks the ledger.  The
    measurement is retried every tick until the box holds none.
    """

    key = (host, tuple(args), entry.get(LOCAL_DISK_FIELD),
           document.get(LOCAL_DISK_FLOOR_FIELD) if isinstance(document, dict) else None)
    if key in _SPOOL_VERDICTS:
        return list(_SPOOL_VERDICTS[key])
    verdict = _spool_budget(host, entry, document, args, statvfs=statvfs)
    if verdict[0] == "offer":
        gib = int(verdict[1])
        _SPOOL_VERDICTS[key] = _with_spool(args, gib)
        _SPOOL_WAITING.pop(key, None)
        if gib:
            declared = args[args.index(SPOOL_FLAG) + 1]
            print(f"[{host}] {SPOOL_FLAG} {declared} measured {gib} GiB: "
                  f"{verdict[2]}", flush=True)
        return list(_SPOOL_VERDICTS[key])
    if verdict[0] == "refuse":
        _SPOOL_VERDICTS[key] = _without_spool(args)
        _SPOOL_WAITING.pop(key, None)
        print(f"[{host}] {SPOOL_FLAG} declaration refused, so this box offers "
              f"no {SPOOL_KIND}: {verdict[1]}", flush=True)
        return list(_SPOOL_VERDICTS[key])
    try:
        total = pool.PoolQueue(_queue_root()).ledger(host).capacity().get(SPOOL_KIND, 0)
    except (OSError, pool.PoolContractError):
        total = 0
    ceiling = _spool_gb_of(args)
    if ceiling is not None:
        total = min(ceiling, total)
    waiting = _with_spool(args, total) if total else _without_spool(args)
    offering = f"the ledger's current {total} GiB" if total else "none"
    line = (f"[{host}] {SPOOL_FLAG} measurement waits: {verdict[1]}; offering "
            f"{offering} until it can be taken")
    if _SPOOL_WAITING.get(key) != line:
        _SPOOL_WAITING[key] = line
        print(line, flush=True)
    return waiting


def declared_roles(host: str) -> list[tuple[str, list[str]]]:
    """The auxiliary roles this box declares, with each one's arguments.

    ``roles`` is a mapping beside ``loops``/``args`` in ``fleet_boxes.json``,
    so a role is versioned and published exactly like the loop shape.  An
    unreadable config is not a reason to tear a role down -- the caller simply
    spawns nothing new this tick -- and a role name with no script is refused
    rather than guessed at, for the same reason an unknown hostname is.
    """

    try:
        config = _config(host)
    except (SystemExit, OSError, ValueError):
        return []
    declared = config.get("roles") or {}
    if not isinstance(declared, dict):
        raise SystemExit(f"fleet_boxes.json roles for {host} must be an object")
    out: list[tuple[str, list[str]]] = []
    for name in sorted(declared):
        if name not in ROLE_SCRIPTS:
            raise SystemExit(
                f"{host} declares role {name!r}, which names no script; "
                f"known roles: {sorted(ROLE_SCRIPTS)}")
        out.append((name, [str(a) for a in (declared[name] or [])]))
    return out


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


def _proc_state(pid: int, proc_root: Path | None = None) -> str | None:
    """The scheduler state ``/proc/<pid>/stat`` reports, or ``None``.

    The state is field 3, and the parse is the one the generation census
    already keeps: ``comm`` (field 2) is parenthesized and may itself contain
    spaces or ``)``, so the state is the first field after the LAST ``)``.
    ``None`` means the file could not be read -- exited, or not ours to read
    -- and is reported as unknown; missing evidence is never stopped.

    The stopped states are what this exists for: ``T`` (job control, what
    SIGSTOP leaves) and ``t`` (tracing).  A ``Z`` zombie is dead and awaiting
    reap, which ``_reap_children`` handles and which is not a health signal.
    """

    raw = _proc_field(pid, "stat", PROC if proc_root is None else proc_root)
    if raw is None:
        return None
    close = raw.rfind(b")")
    if close < 0:
        return None
    fields = raw[close + 1:].split()
    if not fields:
        return None
    state = fields[0].decode("ascii", "replace")
    return state if len(state) == 1 else None


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
                   proc_root: Path | None = None,
                   script_name: str = LOOP_SCRIPT) -> bool:
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
    if script is None or script.name != script_name:
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


def _has_children(pid: int) -> bool | None:
    """Whether ``pid`` has a child, or ``None`` when its state is unreadable."""

    try:
        tasks = sorted(Path(f"/proc/{pid}/task").iterdir())
    except OSError:
        return None
    saw_task = False
    for task in tasks:
        try:
            children = (task / "children").read_text().split()
        except OSError:
            continue
        saw_task = True
        if children:
            return True
    return False if saw_task else None


def _is_idle(pid: int, holders: Collection[int] | None = None) -> bool:
    """True when this loop holds no action: no claim, and no child of its own.

    Childlessness alone was wrong in one direction, and the direction that
    costs work.  ``PoolQueue.claim`` returns as soon as its rename lands, and
    ``execute`` then materializes a sealed checkout -- a bundle of up to
    512 MiB out of the CAS -- before it reaches ``Popen``.  For all of that a
    loop holds an action and has no child, so it read as idle, was SIGTERMed,
    and left its claim in ``claimed/`` until ``reap_stale`` timed the lease out
    ``LEASE_TIMEOUT_S`` later.  The action then ran again from the start.

    So ask the queue first.  The loop already recorded the claim: ``claim``
    writes its own pid into the lease before it returns and ``finish`` unlinks
    it, so a lease naming this box and this pid IS the claim, where the process
    tree is an inference about it.  When that answer cannot be had at all the
    loop is treated as busy: a cycle deferred to the next tick costs a tick,
    and a claim dropped costs the action.

    The child check stays, and stays second.  It is what covers a loop running
    an action under a generation whose lease this one cannot parse, and it
    needs no shared filesystem.  Reading ``/proc/<pid>/task/*/children`` asks
    the kernel rather than guessing from a log line.
    """

    if holders is None:
        holders = _claim_holders()
    if holders is None or pid in holders:
        return False
    return _has_children(pid) is False


def _ready_backlog() -> bool | None:
    """Whether the queue has a ready record, or ``None`` when unknown.

    This is one directory scan per supervision cycle.  The loops parse and
    order records when they claim; the supervisor only needs the pressure
    signal and must not duplicate that NFS work.
    """

    ready = _queue_root() / pool.READY
    try:
        with os.scandir(ready) as entries:
            return any(entry.name.endswith(".json") for entry in entries)
    except FileNotFoundError:
        return False
    except OSError:
        return None


def _visible_memory_bytes() -> int:
    """Best-effort physical memory visible to this supervisor."""

    try:
        return int(os.sysconf("SC_PHYS_PAGES")) * int(
            os.sysconf("SC_PAGE_SIZE"))
    except (OSError, ValueError):
        return 0


def housekeeping_ceiling(floor: int) -> int:
    """Bound cheap queue pollers from visible CPU and memory.

    This is a process-count safety ceiling, not an action concurrency limit,
    and since the sizing law became evidence-driven it is not the operative
    bound either: reaching it now requires that many loops to be genuinely
    holding claims, which admission decides.  It was operative while growth
    was a fraction of it -- ``LOOPS_PER_VISIBLE_CPU = 4`` made it 80 on a
    20-CPU Spark and a growth batch 20 -- and these pollers are metadata-bound
    on a shared network filesystem rather than CPU-bound, so the number was
    never derived from the medium it actually loads.  Deriving it needs a
    measurement of that medium; until there is one, a different unmeasured
    constant would not be an improvement.
    """

    try:
        cpus = len(os.sched_getaffinity(0))
    except (AttributeError, OSError):
        cpus = os.cpu_count() or 1
    by_cpu = max(1, cpus) * LOOPS_PER_VISIBLE_CPU
    memory = _visible_memory_bytes()
    by_memory = memory // BYTES_PER_HOUSEKEEPING_LOOP if memory else by_cpu
    return max(floor, min(by_cpu, max(1, by_memory)))


def _next_log_index() -> int:
    """Choose a log slot no running or earlier loop can already be appending."""

    highest = -1
    try:
        entries = list(LOG_DIR.iterdir())
    except OSError:
        return 0
    for path in entries:
        name = path.name
        if not (name.startswith("pb-worker-") and name.endswith(".log")):
            continue
        try:
            highest = max(highest, int(name[10:-4]))
        except ValueError:
            continue
    return highest + 1


def cycle_stale(published: str) -> list[int]:
    """Stop idle loops running bytes other than the published ones.

    A loop holds the modules it imported at start for its whole life, so a fix
    published under a running fleet reaches none of it -- and because the
    supervisor counts a stale-byte loop as a healthy one, the target is met
    and the fix is never loaded.  Loops carrying the reload check exit on
    their own; this is for the generation that predates it.

    **Roles too, and that is the whole of #615's first defect.**  This walked
    ``_live_loops()``, which is worker loops only, so the single-instance
    ``tiers`` and ``storage`` roles were reachable by no route at all: the
    installed unit carries no ``--cycle-stale``, and ``tier_loop.py`` carried
    no reload check either, so one kept serving bytes two publishes old while
    every worker on the box had moved.

    A role is cycled on a narrower rule than a worker, because it is a
    singleton rather than one of a pool: only when the generation it is
    running *differs* from the published one.  A worker respawns in a poll
    interval and the box has others; stopping a live-generation storage role
    costs a cycle of the service nothing else provides.

    Only idle loops are stopped, and only with SIGTERM: a loop mid-action
    keeps its claim and cycles when it next goes idle.  Nothing here kills
    work.
    """

    if not published:
        return []
    holders = _claim_holders()
    if holders is None:
        return []                         # NFS silence never licenses a signal
    stopped = _stop_idle_loops(holders=holders)
    stopped.extend(_stop_stale_roles(published, holders=holders))
    return stopped


def _loop_commit(pid: int, roots: list[Path],
                 proc_root: Path | None = None) -> str:
    """The published commit of the generation this process is running, or "".

    Resolved from the script the process actually loaded rather than from
    anything it announces: the generation root is the directory its imports
    are pinned to for the rest of its life.  An unreadable or unrecognized
    answer is "", and "" never licenses a signal -- missing evidence is not
    staleness.
    """

    proc_root = PROC if proc_root is None else proc_root
    raw = _proc_field(pid, "cmdline", proc_root)
    if raw is None:
        return ""
    argv = [part.decode("utf-8", "replace") for part in raw.split(b"\0") if part]
    if len(argv) < 2:
        return ""
    script = _script_of(pid, argv, proc_root)
    if script is None:
        return ""
    for root in roots:
        if not script.is_relative_to(root):
            continue
        try:
            receipt = json.loads((root / "RUNTIME_VERSION.json").read_text())
        except (OSError, ValueError):
            return ""
        commit = receipt.get("commit") if isinstance(receipt, dict) else None
        return str(commit) if isinstance(commit, str) else ""
    return ""


def _stop_stale_roles(published: str, proc_root: Path | None = None,
                      holders: Collection[int] | None = None) -> list[int]:
    """SIGTERM each idle role loop whose generation is not the published one."""

    stopped: list[int] = []
    roots = _proven_roots()
    receipt = _published_receipt()
    for role, script in sorted(ROLE_SCRIPTS.items()):
        for pid in _live_role_loops(script, proc_root):
            commit = _loop_commit(pid, roots, proc_root)
            if not commit or commit == published:
                continue
            was = _stop_idle_loops(
                [pid], proc_root, holders, script_name=script)
            stopped.extend(was)
            if was:
                argv, script_path = _role_process(pid, proc_root)
                _record_role_restart(socket.gethostname(), role, {
                    "pid": pid, "argv": argv, "script": script_path,
                    "reason": "generation"}, receipt)
    return stopped


def _published_receipt() -> dict:
    """The active generation's receipt, or ``{}`` when it cannot be read."""

    try:
        value = json.loads(
            (_current_root() / "RUNTIME_VERSION.json").read_text())
    except (OSError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


def _role_process(pid: int, proc_root: Path | None = None,
                  ) -> tuple[list[str] | None, Path | None]:
    """This role pid's argv past the script, and the script it runs.

    Both from one ``cmdline`` read.  ``None`` for either means ``/proc`` did
    not answer, and missing evidence is never staleness -- the rule
    ``_loop_commit`` already keeps.
    """

    proc_root = PROC if proc_root is None else proc_root
    raw = _proc_field(pid, "cmdline", proc_root)
    if raw is None:
        return None, None
    argv = [part.decode("utf-8", "replace") for part in raw.split(b"\0") if part]
    if len(argv) < 2:
        return None, None
    return argv[2:], _script_of(pid, argv, proc_root)


def _stale_role_loops(role_args: list[str], script_name: str,
                      proc_root: Path | None = None,
                      pids: list[int] | None = None) -> list[dict]:
    """Live role loops the current declaration no longer names as-is.

    Two ways to be stale, and the 2026-09-19 incident was both at once.  A
    role whose argv is not the one the current ``fleet_boxes.json`` declares
    -- the ``prewarm_loop --readers 1`` that outlived #703's declaration of
    ``--readers 4`` for hours, because ``--ensure`` checked presence and
    presence alone.  Or a role whose executable resolves outside the ACTIVE
    generation: fleet-proven bytes, retired by a publish, kept serving
    because only the operator's ``--cycle-stale`` verb reached roles and
    the installed unit does not pass it.

    Each entry carries the evidence the drift record needs: the pid, the
    argv the process is actually running, the script it loaded, and which
    of the two rules fired.  An unreadable argv or script, or an
    unresolvable live root, keeps the role off this list.  ``pids`` may be
    the caller's already-taken census of this role, so one tick does not
    re-prove the same processes three times.
    """

    try:
        active = _current_root().resolve(strict=True)
    except OSError:
        active = None
    stale: list[dict] = []
    for pid in (_live_role_loops(script_name, proc_root)
                if pids is None else pids):
        argv, script = _role_process(pid, proc_root)
        reason = None
        if argv is not None and argv != role_args:
            reason = "argv"
        elif (active is not None and script is not None
              and not script.is_relative_to(active)):
            reason = "generation"
        if reason is not None:
            stale.append({"pid": pid, "argv": argv, "script": script,
                          "reason": reason})
    return stale


def _record_role_restart(host: str, role: str, entry: dict,
                         published: dict, *,
                         role_args: list[str] | None = None) -> None:
    """Stamp one drift record for a role restart this supervisor performed.

    The record goes where the worker's claim refusals go -- the queue's
    ``generation-drift/`` namespace, written by the one shared helper -- so
    a reader asking "what drifted tonight, and what did the fleet do about
    it" finds both halves in one place.  The role's own generation is named
    by the tree its script resolved into, the fleet's by the live receipt.
    """

    loaded_commit = ""
    loaded_generation = ""
    script = entry.get("script")
    if script is not None:
        for root in _proven_roots():
            if not script.is_relative_to(root):
                continue
            try:
                value = json.loads(
                    (root / "RUNTIME_VERSION.json").read_text())
                if isinstance(value, dict):
                    loaded_commit = str(value.get("commit") or "")
                    loaded_generation = str(value.get("generation") or "")
            except (OSError, ValueError):
                pass
            break
    detail = {"event": "role-restart", "role": role,
              "pid": entry["pid"], "reason": entry.get("reason"),
              "running_argv": entry.get("argv"),
              "script": str(entry.get("script") or "")}
    if role_args is not None:
        detail["declared_argv"] = role_args
    runtime_gate.record_generation_drift(
        {"loaded_commit": loaded_commit,
         "loaded_generation": loaded_generation,
         "published_commit": str(published.get("commit") or ""),
         "published_generation": str(published.get("generation") or "")},
        actor="supervise", queue_root=_queue_root(), detail=detail)


def _stop_idle_loops(pids: list[int] | None = None,
                     proc_root: Path | None = None,
                     holders: Collection[int] | None = None,
                     script_name: str = LOOP_SCRIPT) -> list[int]:
    """SIGTERM every loop holding no action, and report which.

    The one rule every reason to cycle a loop shares -- stale bytes, a stale
    declared shape, a stale role.  Only idle loops, only SIGTERM: a loop
    mid-action keeps its claim and cycles when it next goes idle.

    ``script_name`` is what the ownership proof below is made against, and it
    has to be passed for a role: the default is the worker script, so a role
    pid handed to this function used to be dropped silently by the very check
    that exists to stop a basename kill (#615).
    """

    stopped: list[int] = []
    roots = _proven_roots()
    if holders is None:
        holders = _claim_holders()
    if holders is None:
        return []                         # NFS silence never licenses a signal
    for pid in (_live_loops(proc_root) if pids is None else pids):
        # Proved again here, at the line that actually sends the signal.  The
        # caller has always passed pids that came from ``_live_loops``, but a
        # future caller that does not must not be able to turn this into the
        # basename kill it used to be.
        if not _is_fleet_loop(pid, roots, proc_root, script_name):
            continue
        if not _is_idle(pid, holders):
            continue
        try:
            os.kill(pid, signal.SIGTERM)
        except OSError:
            continue
        stopped.append(pid)
    return stopped


def _reap_children() -> int:
    """Collect exited direct children, including those inherited across exec.

    Popen's private registry disappears at exec; the kernel's child ownership
    does not. No asynchronous status consumer runs in this single-threaded
    supervisor: synchronous subprocess calls finish between tick boundaries,
    and spawned worker handles are discarded. Never ignore SIGCHLD, whose
    inherited disposition would hide worker/action failure statuses.
    """
    reaped = 0
    for _ in range(MAX_REAPS_PER_TICK):
        try:
            pid, _status = os.waitpid(-1, os.WNOHANG)
        except ChildProcessError:
            break
        except InterruptedError:
            continue
        if pid == 0:
            break
        reaped += 1
        _note_role_exit(pid, _status)
    return reaped


def _note_role_exit(pid: int, status: int) -> None:
    """Record a reaped role child's refusal, so its respawn is backed off.

    Only a pid this process image spawned as a role is read, and only the
    refusal status backs off: any other exit -- a crash, a signal, a clean
    return -- ends the refusal streak and keeps the prompt respawn every
    other role exit gets.
    """

    role = _ROLE_CHILDREN.pop(pid, None)
    if role is None:
        return
    if (os.WIFEXITED(status) and os.WEXITSTATUS(status)
            == runtime_gate.ROLE_SINGLETON_HELD_EXIT):
        previous = _ROLE_REFUSALS.get(role)
        count = 1 if previous is None else int(previous["count"]) + 1
        delay = min(BUSY_INTERVAL_S * 2 ** (count - 1),
                    ROLE_REFUSAL_BACKOFF_MAX_S)
        _ROLE_REFUSALS[role] = {"count": count, "pid": pid, "status": status,
                                "retry_at": _monotonic() + delay,
                                "announced": False}
    else:
        _ROLE_REFUSALS.pop(role, None)


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


def _live_role_loops(script_name: str,
                     proc_root: Path | None = None,
                     roots: list[Path] | None = None) -> list[int]:
    """This box's live children for one role script.

    Its own census rather than ``_live_loops`` with an argument, because a
    role loop is not a worker loop in any sense the sizing law cares about:
    it claims nothing, and counting one as a poller would make the box look
    busier than it is.  The three facts proved are the same three
    ``_is_fleet_loop`` proves.  ``roots`` may be the proven trees the caller
    already listed, so one tick's health census and role pass share one
    generation-store walk instead of re-listing it per role per caller.
    """

    proc = subprocess.run(["pgrep", "-f", script_name],
                          capture_output=True, text=True, check=False)
    mine = os.getpid()
    if roots is None:
        roots = _proven_roots()
    return [pid for pid in (int(t) for t in proc.stdout.split())
            if pid != mine and _is_fleet_loop(pid, roots, proc_root, script_name)]


def role_health(roles: list[tuple[str, list[str]]] | None = None,
                proc_root: Path | None = None,
                census: dict[str, list[int]] | None = None) -> list[dict]:
    """Every owned role loop and the scheduler state the kernel reports.

    The census is the same proof every restart path makes -- interpreter,
    role script, a runtime generation this fleet published, and this box's
    ownership mark, re-proven on every tick rather than cached -- so a role
    named here is one this fleet launched, and nothing unrelated or foreign
    is inspected, counted or signalled.  ``roles`` may be the declaration the
    caller already read; ``None`` reads it here for a direct caller.  ``census``
    may be the caller's already-taken per-role pid census, so the tick's
    health evidence and its role pass share one generation-store walk.

    A process that exits between the census and the state read carries
    ``state=None``: missing evidence is "unknown", never "stopped".  A zombie
    (``Z``) is dead and awaiting reap, which is ``_reap_children``'s job and
    not a health alarm.
    """

    if roles is None:
        roles = declared_roles(socket.gethostname())
    health: list[dict] = []
    for role, _role_args in roles:
        script = ROLE_SCRIPTS[role]
        pids = (census.get(role) if census is not None
                else _live_role_loops(script, proc_root))
        for pid in pids or ():
            state = _proc_state(pid, proc_root)
            health.append({
                "pid": pid,
                "role": role,
                "script": script,
                "state": state,
                "state_name": PROC_STATE_NAMES.get(state or "", "unknown"),
                "stopped": None if state is None else state in STOPPED_STATES,
            })
    return health


def role_health_lines(host: str, health: list[dict],
                      seen: dict[int, str]) -> list[str]:
    """Lines naming each owned role whose reported health state changed.

    A role that enters a stopped state (``T``/``t``, what SIGSTOP leaves) or
    whose state cannot be read is named with its pid, role and state the
    first time it is seen that way, and once per later change; the first
    healthy tick after that reports the state cleared.  A steady state is not
    reprinted, so the log is evidence of transitions rather than a heartbeat
    flood.  ``seen`` is the caller's memory of the last state name per pid and
    is pruned of pids that left the census, so it cannot grow with uptime.
    """

    lines: list[str] = []
    live: set[int] = set()
    for entry in sorted(health, key=lambda item: (str(item["role"]),
                                                  int(item["pid"]))):
        pid = int(entry["pid"])
        name = str(entry["state_name"])
        live.add(pid)
        previous = seen.get(pid)
        if name in ALARM_STATE_NAMES:
            if previous == name:
                continue
            if entry["state"] is None:
                lines.append(
                    f"[{host}] role {entry['role']} pid {pid} state "
                    f"unreadable; it is not reported as stopped or healthy")
            elif name in ("stopped", "tracing-stop"):
                lines.append(
                    f"[{host}] role {entry['role']} pid {pid} state "
                    f"{entry['state']} ({name}); it is alive but serves "
                    f"nothing until continued")
            else:
                lines.append(
                    f"[{host}] role {entry['role']} pid {pid} state "
                    f"{entry['state']} ({name}); it is not serving and "
                    f"awaits reap or exit")
        elif previous in ALARM_STATE_NAMES:
            lines.append(
                f"[{host}] role {entry['role']} pid {pid} state "
                f"{entry['state']} ({name}); the earlier {previous} state "
                f"cleared")
    for stale_pid in [pid for pid in seen if pid not in live]:
        del seen[stale_pid]
    for entry in health:
        seen[int(entry["pid"])] = str(entry["state_name"])
    return lines


def _stopped_pending_note(pending: dict[int, int],
                          targets: list[tuple[int, str]]) -> str:
    """Name the pending loops the kernel reports stopped, for the stop line.

    A SIGTERM sent to a ``T``/``t`` process is queued, not delivered, until
    the process is continued, so a silent shutdown wait is exactly the shape
    an operator's SIGSTOP leaves.  Roles are named where the target list
    knows them; a state that cannot be read is left out rather than guessed
    at.  Nothing here signals anything.
    """

    scripts = {pid: script for pid, script in targets}
    named = []
    for pid in pending.values():
        state = _proc_state(pid)
        if state is None or state not in STOPPED_STATES:
            continue
        script = scripts.get(pid, LOOP_SCRIPT)
        role = next((name for name, candidate in ROLE_SCRIPTS.items()
                     if candidate == script), script)
        named.append(f"{role} pid {pid} state {state} "
                     f"({PROC_STATE_NAMES.get(state, state)})")
    if not named:
        return ""
    return ("; stopped and will not exit until continued: "
            + ", ".join(named))


def _spawn_role(role: str, args: list[str]) -> int:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    handle = (LOG_DIR / f"pb-role-{role}.log").open("a", buffering=1)
    handle.write(f"\n=== spawned {time.strftime('%F %T')} ===\n")
    script = (_current_root() / "tools" / ROLE_SCRIPTS[role]).resolve(strict=True)
    proc = subprocess.Popen(
        [sys.executable, str(script), *args],
        cwd=str(MIRROR), stdout=handle, stderr=subprocess.STDOUT,
        start_new_session=True,
        env={**os.environ, OWNERSHIP_ENV: socket.gethostname()},
    )
    return proc.pid


def ensure_roles(host: str, stop_requested=lambda: False, *,
                 holders: Collection[int] | None = None,
                 declared: list[tuple[str, list[str]]] | None = None,
                 census: dict[str, list[int]] | None = None,
                 ) -> list[tuple[str, int]]:
    """Keep exactly one live child per declared role, **as declared**.

    One, not a target: a role loop is a single reader of one queue, and a
    second copy of it would read the same ready list and warm the same bytes
    twice.  A box that declares no role does nothing here, which is every box
    but the file server.

    Presence was, for years, the whole of this function -- and presence is
    exactly what the 2026-09-19 incident survived on.  A supervisor that
    re-execs itself into a new generation leaves its role children running,
    and a role that is present but running an argv the current
    ``fleet_boxes.json`` no longer declares, or an executable from a
    generation the fleet has retired, went on serving the old shape for
    hours: the ``prewarm_loop --readers 1`` that outlived #703.  So each
    declared role's live loops are checked against the declaration first,
    and a stale one is stopped when idle and respawned here from the active
    generation, with the restart stamped in the queue's
    ``generation-drift`` namespace.  The idle rule is every other restart
    path's: only SIGTERM, only a role holding no work, so a role mid-cycle
    finishes it and cycles on a later tick.

    Spawning is also gated on the role's own host-local singleton lock
    (#709), which the role's process holds: this census only sees roles its
    ownership proof recognizes, and a duplicate supervisor's child, a
    hand-started loop or a stale generation's can hold the lock without
    appearing here.  A held lock is reported and nothing is started over it;
    the child proves the same lock again at startup.  ``declared`` is the
    declaration the caller already read and ``census`` its already-taken
    per-role pid census, so one tick reads and proves each role once.

    A signalled role is not gone until the census says so.  SIGTERM is a
    request -- the process may be finishing a cycle, and an old generation's
    role, started before this guard existed, holds no singleton lock that
    would stop a replacement serving beside it -- so a tick that stopped
    anything re-censuses, and any survivor keeps the role counted as live.
    The replacement starts on the tick after it is gone: at most one interval
    later, and never as a second reader.
    """

    started: list[tuple[str, int]] = []
    published = _published_receipt()
    for role, role_args in (declared_roles(host) if declared is None
                            else declared):
        if stop_requested():
            break
        script_name = ROLE_SCRIPTS[role]
        known = census.get(role) if census is not None else None
        stopped: list[int] = []
        stale = _stale_role_loops(role_args, script_name, pids=known)
        if stale:
            stopped = _stop_idle_loops(
                [entry["pid"] for entry in stale], holders=holders,
                script_name=script_name)
            for entry in stale:
                if entry["pid"] not in stopped:
                    continue      # busy: it cycles when it next goes idle
                _record_role_restart(host, role, entry, published,
                                     role_args=role_args)
                print(f"[{host}] role {role} pid {entry['pid']} stale by "
                      f"{entry['reason']} ({' '.join(entry['argv'] or [])}); "
                      f"stopped for restart on "
                      f"{str(published.get('commit') or '')[:12] or '(unknown)'}",
                      flush=True)
        # A signalled role has not exited until the kernel's census says so:
        # the request may still be queued behind a cycle, a stopped process,
        # or an old generation that holds no singleton lock.  Any survivor
        # keeps the role counted as live, so a replacement cannot start
        # beside a predecessor that is still reading the same queue.
        if stopped or known is None:
            live = _live_role_loops(script_name)
        else:
            live = known
        if live:
            if _ROLE_REFUSALS.pop(role, None) is not None:
                print(f"[{host}] role {role} refusal cleared: pid "
                      f"{', '.join(str(pid) for pid in live)} is serving",
                      flush=True)
            continue
        if stop_requested():
            break
        # A role that exited refusing (#1046) is not spawned again until its
        # backoff elapses; the refusal is named once, with the next attempt.
        refusal = _ROLE_REFUSALS.get(role)
        if refusal is not None:
            wait = float(refusal["retry_at"]) - _monotonic()
            if wait > 0:
                if not refusal["announced"]:
                    refusal["announced"] = True
                    print(f"[{host}] role {role} refused: pid "
                          f"{refusal['pid']} exit "
                          f"{runtime_gate.ROLE_SINGLETON_HELD_EXIT} "
                          f"({refusal['count']} consecutive; see "
                          f"{LOG_DIR / f'pb-role-{role}.log'}); next attempt "
                          f"in {wait:.0f}s", flush=True)
                continue
        # No owned role is running.  The lock, not this census, is what keeps
        # two readers off the queue, so probe it before spawning: the refusal
        # belongs in the supervisor's own log rather than a role log nobody
        # watches, and a held lock must not be raced once per tick.  Nothing
        # unproven is ever signalled; an unreadable lock fails closed.
        try:
            held, holder = runtime_gate.role_singleton_holder(script_name)
        except (OSError, RuntimeError) as exc:
            print(f"[{host}] role {role} singleton lock unusable: {exc}; "
                  f"starting nothing", flush=True)
            continue
        if held:
            print(f"[{host}] role {role} not started: another instance holds "
                  f"{runtime_gate.role_lock_path(script_name)}"
                  + (f" (pid {holder})" if holder is not None
                     else " (holder pid unreadable)"), flush=True)
            continue
        try:
            pid = _spawn_role(role, role_args)
            _ROLE_CHILDREN[pid] = role
            started.append((role, pid))
        except (OSError, FileNotFoundError) as exc:
            # A generation published before this role existed has no script.
            # That is a missing capability, not a reason to stop supervising
            # the loops that do exist.
            print(f"[{host}] role {role} not startable: {exc}", flush=True)
    return started


def _systemd_managed() -> bool:
    """The installed unit owns startup, including while deliberately stopped.

    Do not ask whether the unit is active: that would let the next cron tick
    undo an operator's stop. Older installed units retain their old contract
    until the installer replaces them with the explicit --systemd invocation.
    """
    try:
        return SYSTEMD_EXEC in SYSTEMD_UNIT.read_text().splitlines()
    except FileNotFoundError:
        return False


def _shutdown_workers() -> None:
    """Request cooperative exit and retain the supervisor claim until exit.

    Signal worker PIDs only, never their sessions or payloads. Worker loops
    finish their current action and its cleanup before honoring SIGTERM.
    pidfds bind the ownership check, signal and wait to the same process even
    when a PID is reused. Auxiliary roles hold no action and receive TERM too.
    A blocked or externally stopped process keeps shutdown pending; there is
    no timeout that turns missing evidence into a stopped worker.
    """
    roots = _proven_roots()
    targets = [(pid, LOOP_SCRIPT) for pid in _live_loops()]
    targets.extend((pid, script) for script in ROLE_SCRIPTS.values()
                   for pid in _live_role_loops(script))
    pending: dict[int, int] = {}
    poller = select.poll()
    try:
        for pid, script in targets:
            try:
                fd = os.pidfd_open(pid)
            except ProcessLookupError:
                continue
            try:
                if not _is_fleet_loop(pid, roots, script_name=script):
                    continue
                signal.pidfd_send_signal(fd, signal.SIGTERM)
                pending[fd] = pid
                poller.register(fd, select.POLLIN)
                fd = -1
            except ProcessLookupError:
                pass
            finally:
                if fd >= 0:
                    os.close(fd)
        print(f"[{socket.gethostname()}] shutdown requested; waiting for "
              f"owned workers/roles {list(pending.values())}"
              + _stopped_pending_note(pending, targets), flush=True)
        while pending:
            for fd, _events in poller.poll(1000):
                poller.unregister(fd)
                os.close(fd)
                del pending[fd]
            _reap_children()
        _reap_children()
        print(f"[{socket.gethostname()}] shutdown complete; owned workers and "
              "roles exited", flush=True)
    finally:
        for fd in pending:
            os.close(fd)


def _wait_for_shutdown() -> None:
    last_error = None
    while True:
        try:
            _shutdown_workers()
            return
        except OSError as exc:
            # A failed ownership handle or signal is not worker exit. Keep
            # the claim and the service deactivating, then retry the census.
            error = f"{type(exc).__name__}: {exc}"
            if error != last_error:
                print(f"shutdown pending: {error}", flush=True)
                last_error = error
            time.sleep(1)


def main() -> int:
    stopping = False

    def request_stop(_signum, _frame):
        nonlocal stopping
        stopping = True

    previous = signal.signal(signal.SIGTERM, request_stop)
    try:
        return _run_supervisor(lambda: stopping)
    finally:
        signal.signal(signal.SIGTERM, previous)


def _run_supervisor(stop_requested) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--ensure", action="store_true",
                    help="exit 0 if a supervisor already owns this box")
    ap.add_argument("--systemd", action="store_true",
                    help="installed systemd unit owns this invocation")
    ap.add_argument("--loops", type=int, default=0,
                    help="fixed loop count (disables elastic scaling)")
    ap.add_argument("--interval-s", type=float, default=30.0,
                    help="seconds between supervision cycles; ignored with "
                         "--once")
    ap.add_argument("--once", action="store_true",
                    help="top the box up and exit, without supervising")
    ap.add_argument("--cycle-stale", action="store_true",
                    help="SIGTERM idle loops so they respawn on the published "
                         "runtime; a loop mid-action is left alone")
    args = ap.parse_args()

    if (not args.systemd and _systemd_managed()
            and not (args.cycle_stale and args.once)):
        print("supervisor is managed by systemd; use systemctl --user "
              "start/stop prismabuild-supervisor.service", flush=True)
        return 0 if args.ensure else 1

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

    loaded_generation = _loaded_published_generation()
    handle = _claim_handle(ensure=args.ensure)
    if handle is None:
        return 0                           # somebody else already owns this box

    print(f"[{host}] supervising {target} loops: {' '.join(loop_args)}",
          flush=True)
    fixed_target = args.loops > 0
    next_log_index = _next_log_index()
    draining_announced = False
    # The last health state named per role pid, so a steady stopped role is
    # reported once rather than every tick (#709).
    role_health_seen: dict[int, str] = {}
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
        if stop_requested():
            _wait_for_shutdown()
            return 0
        if not args.systemd and _systemd_managed():
            print("installed systemd unit now owns startup; handing over "
                  "supervision without stopping workers", flush=True)
            return 0
        reaped = _reap_children()
        if reaped:
            print(f"[{host}] reaped {reaped} exited child process(es)", flush=True)
        if not args.once and _reexec_if_published(loaded_generation, handle):
            return 0                       # reached only under an exec test double
        target, loop_args = declared_shape(
            host, args.loops, (target, loop_args))
        if stop_requested():
            continue
        # A box the roster declares absent offers nothing (#606).  Startup
        # already refused it in ``_config``; a supervisor that was running
        # when the declaration published drains instead: no new loops, no new
        # roles, and the idle reserve goes with them, so its offers expire
        # and placement stops considering it.  Loops mid-action are never
        # killed -- they finish, go idle, and are stopped on a later tick.
        # Roles already running (the file server's) are left to the operator's
        # stop: ``ensure_roles`` only spawns, and tearing down a tier loop
        # from here would strand the ledgers it mints.
        draining = False
        try:
            presence = box_presence(host)
        except SystemExit as exc:
            # A presence that cannot be established is not an active box: an
            # ambiguous declaration fails closed as absent (#606).  Fall
            # through to the normal sleep rather than spinning on the error.
            presence, presence_error = None, str(exc)
        else:
            presence_error = None
        if presence_error is not None:
            draining = True
            if not draining_announced:
                print(f"[{host}] {presence_error}; starting nothing",
                      flush=True)
                draining_announced = True
            target = 0
        elif presence is not None and presence[0] in fleet_roster.ABSENT:
            draining = True
            status, detail = presence
            if not draining_announced:
                print(f"[{host}] roster declares this box {status} "
                      f"({detail['reason']}) by {detail['by']}; draining "
                      f"loops to zero and starting nothing", flush=True)
                draining_announced = True
            target = 0
        else:
            draining_announced = False

        live = _live_loops()
        # One authoritative claim census per cycle.  Computed before the role
        # pass so role freshness shares it, and reused for stale cycling, load
        # feedback and scale-down below -- still one NFS rescan per pid, not
        # one per caller.
        holders = _claim_holders()
        # One declaration read and one proven census per tick serve both the
        # health evidence and the role pass; health is reported before the
        # pass so a stopped role's line precedes any decision about it.  A
        # box that declares no role pays none of this.
        roles = declared_roles(host)
        census: dict[str, list[int]] = {}
        if roles:
            roots = _proven_roots()
            census = {role: _live_role_loops(ROLE_SCRIPTS[role], roots=roots)
                      for role, _role_args in roles}
        for line in role_health_lines(host, role_health(roles, census=census),
                                      role_health_seen):
            print(line, flush=True)
        for role, pid in ensure_roles(host, stop_requested, holders=holders,
                                      declared=roles, census=census):
            print(f"[{host}] spawned role {role} pid {pid}", flush=True)
        # The file is the authority, so a loop running other arguments is
        # stale in the same sense a loop running other bytes is.  Stop the
        # idle ones and let the top-up below respawn them on the declared
        # shape; a loop mid-action keeps its claim and cycles when it next
        # goes idle, which is why this never kills work.
        wrong = [pid for pid in live
                 if loop_args_of(pid) not in (None, loop_args)]
        if wrong:
            stopped = _stop_idle_loops(wrong, holders=holders)
            live = [pid for pid in live if pid not in stopped]
            print(f"[{host}] {len(wrong)} loop(s) carry a shape the file no "
                  f"longer declares; stopped the {len(stopped)} idle one(s) "
                  f"{stopped} onto: {' '.join(loop_args)}", flush=True)
        backlog = _ready_backlog()
        busy = 0
        idle: list[int] = []
        unknown = 0
        if holders is None:
            unknown = len(live)
        else:
            for pid in live:
                if pid in holders:
                    busy += 1
                    continue
                children = _has_children(pid)
                if children is True:
                    busy += 1
                elif children is False:
                    idle.append(pid)
                else:
                    unknown += 1

        # The sizing law, and the one thing to keep in mind reading it: the
        # backlog is not in it.  A ready record says work is waiting; it does
        # not say why, and the supervisor cannot tell "no poller is free to
        # take it" from "no poller can take it" -- the second is what a slow
        # shared mount, a stuck client or a resource refusal all look like
        # from here.  Sizing on it made the response unbounded in exactly the
        # direction that hurts: growth was a fraction of the ceiling per busy
        # tick, shrink required the backlog to empty, and a backlog that is
        # not draining is at once the thing that forbids shrink and the thing
        # that authorises twenty more readers of the metadata path that is
        # not draining it.  Sparky reached twelve loops that way on
        # 2026-09-06 and had no path back down (issue #231).
        #
        # So size on the one signal that is attributable: a claim this box is
        # holding.  ``busy`` is loops with a lease or a running child, and
        # every poller above ``busy + IDLE_RESERVE`` is a reader the evidence
        # does not pay for.  Growth is then self-limiting without a batch --
        # each new claim earns the spare that lets the next one be taken
        # without a process start -- and shrink needs no permission from the
        # queue, because an idle poller is idle whether or not work is
        # waiting.  ``backlog`` survives only below, choosing the tick rate.
        desired = target
        if (not fixed_target or draining) and not args.once:
            if holders is None:
                # Silence never licenses a signal, and it does not license a
                # spawn either: hold the count and ask again next tick.
                desired = max(target, len(live))
            else:
                # A draining box keeps no idle reserve: every idle poller is
                # excess, so its offers expire and placement stops seeing it.
                reserve = 0 if draining else IDLE_RESERVE
                desired = min(housekeeping_ceiling(target),
                              max(target, busy + unknown + reserve))
                excess = max(0, len(live) - desired)
                if excess and idle:
                    stopped = _stop_idle_loops(
                        idle[-excess:], holders=holders)
                    live = [pid for pid in live if pid not in stopped]
                    print(f"[{host}] {busy} claim(s) held; sized to "
                          f"{desired} and stopped {len(stopped)} proven-idle "
                          f"loop(s) {stopped}", flush=True)

        missing = max(0, desired - len(live))
        for offset in range(missing):
            if stop_requested():
                break
            index = next_log_index
            next_log_index += 1
            pid = _spawn(loop_args, index)
            print(f"[{host}] spawned loop {index} pid {pid} "
                  f"({len(live) + offset + 1} of {desired})", flush=True)
        if stop_requested():
            continue
        if missing:
            # One bounded delay lets a large batch spread its first polls
            # without charging half a second for every housekeeping process.
            time.sleep(min(0.5, 0.02 * missing))
        if args.once:
            return 0
        active = busy > 0 or backlog is True
        time.sleep(min(args.interval_s, BUSY_INTERVAL_S)
                   if active else args.interval_s)


if __name__ == "__main__":
    raise SystemExit(main())
