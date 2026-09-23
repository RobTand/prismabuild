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

**What the box offers is observed, not only declared.** Host memory comes from
``MemAvailable`` with the pool's held reservation added back once. GPU state
comes from the broker's single root-owned, attributed snapshot; worker loops
never launch their own device query. Missing, stale or incomplete evidence
offers no GPU capacity, and a multi-process pool job remains one attributed
job rather than being mistaken for foreign demand. Cores are read and recorded
but deliberately not clamped: a ``cpu`` token is a slot and the run queue
counts threads, so host load is handled by adaptive CPU admission instead.

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

**Offer publication is advisory and bounded.**  The offer is what a box says
it can take, not what it has taken: it reserves nothing, claims nothing and
starts nothing.  So the loop publishes it through the same abandonable-child
machinery the status reader already uses (``pbstatus.bounded``), with a fixed
five-second budget (``OFFER_PUBLISH_TIMEOUT_S``), and a publication that does
not complete skips this poll's admission rather than serving work behind an
advertisement nobody can read.  This is the boundary issue #16 measures: a hard
NFS mount need not return from an ``open``/``fsync``/``rename`` at any
deadline, even after a signal, and this loop used to run that write in its own
process -- one stall took the loop's whole poll cadence with it.

The publisher child takes a host-local, per-uid, nonblocking ``flock`` under a
private local path (never the shared mount) before its first shared operation
and holds it until the process ends.  That lock is what keeps one box's loops
from piling up writers: while a retained or sibling writer holds it, another
publisher is refused and reported, not queued.  The lock releases only when its owning process exits; never remove
the lock file while a writer may survive.  A timed-out publication's outcome is unknown rather than absent, so
the loop says so; the previous offer is left to expire on its own, and a child
the reader could not reap is retained by ``(pid, starttime)``.  The boundary
covers the loop's own poll, not the persistence of the write: an older
generation's loop publishes without this lock, so two generations can still
interleave one offer file, a publisher that lands after its deadline keeps its
original ``announced_unix``, and the claim, lease and token mutations that
follow a claim decision remain synchronous and unbounded elsewhere.

**A claim is taken only under the active generation.**  The poll-top fence
above guards the poll, but publication and discovery sit between it and the
claim, and a publisher can activate a successor inside exactly that window.
So the loop re-reads the one tiny ``RUNTIME_VERSION.json`` through the live
``repo`` name immediately before ``serve_once`` and compares it with the
generation its own bytes came from: on mismatch it refuses the claim, stamps
one immutable ``generation-drift/`` record under the queue root naming both
generations, its pid, host and a timestamp, and exits for the supervisor to
respawn.  An action already executing is atomic to the loop and finishes
under the generation that claimed it; rolling publishes converge at these
claim boundaries and are never interrupted mid-action.
"""
import argparse
from contextlib import contextmanager
import errno
import fcntl
import hashlib
import json
import os
import socket
import signal
import stat
import sys
import time
from collections import namedtuple
from pathlib import Path

SH = Path("/mnt/shared/prismabuild-fleet")
sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
from runtime_paths import generation_root  # noqa: E402

RUNTIME_ROOT = generation_root(__file__)
sys.path.insert(0, str(RUNTIME_ROOT / "src"))
from prismabuild import (adaptive_cpu, adaptive_gpu as gpu_admission,  # noqa: E402
                         box_capacity, container_images,
                         core as pb, cpu_topology, pool)
from pbstatus import Deadline, bounded  # noqa: E402

#: The safety ceiling a worker loop enforces on one action unless told
#: otherwise.  Named because submitters derive from it: a shard that sets no
#: ``--timeout-s`` of its own still runs under this, and ``pbtest.py`` sizes
#: its per-test bound against it (#600).  A box is free to announce a lower
#: ceiling; ``pbrun.timeout_ceiling_notice`` is what says so at submission.
DEFAULT_EXECUTION_CEILING_S = 7200.0

#: Consecutive ``serve_once`` failures before the loop gives up and lets the
#: supervisor replace it.  Survive the items; do not survive a broken box.
MAX_CONSECUTIVE_ERRORS = 5

# Reconsider work promptly while the queue is nonempty. The configured poll
# remains the empty-queue backoff, so idle workers do not turn one shared NFS
# directory into a per-second fleet-wide scan.
PRESSURE_POLL_S = 1.0

#: The receipt beside these imported bytes proves what this process loaded.
GENERATION_VERSION = RUNTIME_ROOT / "RUNTIME_VERSION.json"
#: The stable name crosses the generation boundary on every idle poll, which
#: is how a loop notices that the publisher activated a successor.
RUNTIME_VERSION = SH / "repo" / "RUNTIME_VERSION.json"
#: The path is read at import so a qualification harness can point a loop
#: child at a private gate.  ``/run/prismabuild`` is ``root:root 0755`` and
#: nothing running as uid 1000 can write under it, so a test that could not
#: move this path could not exercise the drain at all.
MAINTENANCE_GATE = Path(os.environ.get("PRISMABUILD_MAINTENANCE_GATE")
                        or "/run/prismabuild/maintenance.json")

#: Where a parked loop records that it parked.  ``/run`` is tmpfs cleared only
#: at boot, and no loop survives a boot either, so a missing marker here is
#: never a stale absence.  That is exactly the inference a drain proof has to
#: make, and it is the reason ``adaptive_cpu.box_state()`` cannot serve:
#: ``BOX_STATE_ROOT`` defaults under ``/tmp``, which is cleared on some of
#: these hosts, and that module requires a missing file there to be read as
#: "no information" rather than as a fact.
#:
#: The directory is created and handed to this uid by the root upgrade agent.
#: The loop never creates it: under the real gate it could not, and a loop
#: that silently made its own would be recording into a place no reader looks.
PARKED_ROOT = MAINTENANCE_GATE.parent / "rollout" / "parked"

#: What may appear in a marker name.  ``changed_unix`` arrives from a JSON
#: document this process does not write, so it is spelled into the filename
#: rather than trusted as one.
_KEY_SAFE = frozenset("0123456789abcdefghijklmnopqrstuvwxyz"
                      "ABCDEFGHIJKLMNOPQRSTUVWXYZ._")

#: How long one worker-loop offer publication may spend on the shared mount
#: before the loop gives up on this poll.  An offer is advisory: the box is not
#: claiming, holding or reserving anything by writing it, so refusing to serve
#: for one poll is the safe reading of a publication that did not complete
#: (issue #16).  The bound exists because a hard NFS mount need not return from
#: an ``open``/``fsync``/``rename`` at any deadline, even after a signal; only a
#: separate process can be abandoned, so the loop publishes through the
#: existing isolated reader (``pbstatus.bounded``) and stops waiting at the
#: deadline rather than joining a child that may still be blocked in the kernel.
OFFER_PUBLISH_TIMEOUT_S = 5.0

#: How often, inside that budget, a publisher refused by the host-local
#: publication lock is retried.  Sibling loops on one box announce at the same
#: cadence, so a single nonblocking refusal would otherwise let one sibling's
#: publication consume another's whole poll and reduce a box to one offer per
#: cycle per lock holder.  The retry is bounded by the same budget, so a lock
#: that is genuinely stuck is still reported, not waited on.
OFFER_PUBLISH_RETRY_S = 0.05

#: How long one worker-loop queue discovery may spend on the shared mount
#: before the loop gives up on this poll.  Discovery is the pure read-only
#: half of the claim scan -- the ready records and their ``passes/`` sidecars
#: -- so unlike the claim itself it can run in a disposable child: nothing it
#: does needs ownership, and a result that arrives after the deadline is
#: merely stale, never a double claim (issue #16).  The bound exists for the
#: same reason the publication bound does: a hard NFS mount need not return
#: from a read at any deadline, even after a signal, so the loop discovers
#: through the same isolated reader (``pbstatus.bounded``) and stops waiting
#: at the deadline rather than joining a child that may still be blocked in
#: the kernel.  The budget is larger than the publication one because one scan
#: reads one record per queued item rather than writing one file; it stays
#: well under the 120 s offer lifetime so a box that cannot finish a scan
#: lets its offer expire instead of refreshing it on no evidence.
DISCOVERY_TIMEOUT_S = 30.0

#: Where the host-local publication lock lives.  Hardcoded to a private local
#: tmpfs directory, never the shared mount and never a caller-influenced path:
#: the lock's whole job is to keep one box's writers from piling up while the
#: shared mount is what is stalled, so a lock kept there would be the defect it
#: is meant to prevent.  ``/tmp`` is local to every host in this fleet and this
#: path is private per uid.  A test may point this constant at its own private
#: directory.
PUBLICATION_LOCK_ROOT = Path(f"/tmp/prismabuild-offer-publish-{os.getuid()}")


def read_maintenance_gate() -> dict | None:
    """The drain in force on this box, or ``None`` when there is none.

    Split out of :func:`maintenance_requested` so a caller can read the
    drain's ``changed_unix`` without restating the rules below:

    * a missing gate means admission has not been initialized after boot;
    * a gate that cannot be read or parsed means draining, because the other
      reading admits work on a parse error;
    * a value that is not an object means draining, for the same reason;
    * ``draining`` anything other than ``False`` means draining.

    An open gate returns ``None`` rather than the parsed object, so a caller
    testing truthiness cannot mistake ``{"draining": false}`` for a stop.
    """

    try:
        value = json.loads(MAINTENANCE_GATE.read_text())
    except FileNotFoundError:
        return {"draining": True, "reason": "maintenance gate not initialized"}
    except (OSError, ValueError):
        return {"draining": True}
    if not isinstance(value, dict):
        return {"draining": True}
    return None if value.get("draining") is False else value


def maintenance_requested() -> bool:
    return read_maintenance_gate() is not None


def _proc_starttime(pid: int) -> str:
    """Field 22 of ``/proc/<pid>/stat``, which makes a marker survive pid reuse.

    ``comm`` is field 2, is parenthesised, and may itself contain spaces and
    parentheses, so the fields are counted from the last ``)`` rather than by
    splitting the line.
    """

    try:
        line = Path(f"/proc/{pid}/stat").read_text()
    except OSError:
        return "unknown"
    _, _, rest = line.rpartition(")")
    fields = rest.split()
    return fields[19] if len(fields) > 19 else "unknown"


def _gate_key(gate: dict) -> str:
    """This drain's identity, as the broker already stamps it.

    ``maintenance_begin`` stamps a new drain with a fresh ``changed_unix``
    and preserves it while that drain remains held, so the field names
    this drain and not the last one, and a reader matching on it reads a marker
    left by an earlier drain as what it is. ``reason`` is descriptive text,
    not the identity of a drain.

    A gate with no identity gets a key no reader matches, which leaves the host
    reading as not drained.
    """

    value = gate.get("changed_unix") if isinstance(gate, dict) else None
    if value is None:
        return "unknown"
    return "".join(c if c in _KEY_SAFE else "_" for c in str(value))


def post_park_marker(gate: dict) -> Path | None:
    """Record that this process is parked on ``gate``, once, and return the path.

    Best effort by contract, and the direction of the failure is the point: a
    loop that cannot write the marker parks anyway, the host then reads as not
    drained, and whatever is waiting on the drain keeps waiting.  The opposite
    arrangement would let a box that failed to record itself pass for stopped.

    Reposting on every poll is idempotent: the name is fixed by the
    pid and the drain, and the create is exclusive.
    """

    marker = PARKED_ROOT / (f"{os.getpid()}-{_proc_starttime(os.getpid())}"
                            f"-{_gate_key(gate)}")
    try:
        os.close(os.open(marker, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644))
    except FileExistsError:
        return marker
    except OSError:
        return None
    return marker


def _runtime_receipt(path: Path) -> dict:
    """The parsed receipt at ``path``, or ``{}`` when it cannot be read.

    One read serves both identities a receipt carries -- the commit and the
    generation name -- so a claim-boundary handshake costs one file open,
    not one per compared field.  ``{}`` is "unknown", never "absent": the
    callers' existing rule stands, and a receipt that cannot be read does
    not license a refusal any more than it licenses a claim.
    """

    try:
        value = json.loads(path.read_text())
    except (OSError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


def _commit_at(path: Path) -> str:
    return str(_runtime_receipt(path).get("commit") or "")


def _generation_at(path: Path) -> str:
    return str(_runtime_receipt(path).get("generation") or "")


def published_commit() -> str:
    """The commit at the live generation boundary, or "" if unknown."""

    return _commit_at(RUNTIME_VERSION)


#: Where a loop that refuses drifted work files the refusal.  A directory
#: under the queue's own root, spelled like the queue's other record
#: namespaces (``workers/``, ``attempts/``, ``prewarm/``), so a reader of
#: the queue finds it where the queue keeps everything else.
GENERATION_DRIFT_DIR = "generation-drift"
#: The one schema every record in that namespace carries.
GENERATION_DRIFT_SCHEMA = "prismaquant.prismabuild.generation_drift.v1"


def generation_drift(*, loaded_commit: str,
                     loaded_generation: str) -> dict | None:
    """The active generation's disagreement with this process's own bytes.

    Reads ``RUNTIME_VERSION.json`` through the live ``repo`` name -- the
    same existing reader path every reload check uses, not a second one --
    and compares it with the generation this process imported, which the
    caller derived from ``__file__``'s immutable root at startup.  The
    comparison is the reload fence's, unchanged: a published commit that
    differs from the loaded one, or a published generation name that
    differs while both are known.  ``None`` means the fleet and this
    process agree (or the receipt is unreadable, which was never a move).

    The reads go through ``published_commit`` and ``_generation_at`` rather
    than an inlined ``json.loads`` for the same reason those helpers exist:
    they are the one seam the fleet's own convergence tests patch, and a
    second reading path beside them would be the drift this module refuses.

    The return value is the drift half of a ``generation-drift`` record, so
    the refusal, the stamp and any later forensic read all name the same
    identities from one comparison.
    """

    current_commit = published_commit()
    current_generation = _generation_at(RUNTIME_VERSION)
    moved = bool((current_commit and current_commit != loaded_commit) or (
        loaded_generation and current_generation
        and loaded_generation != current_generation))
    if not moved:
        return None
    return {
        "loaded_commit": loaded_commit,
        "loaded_generation": loaded_generation,
        "published_commit": current_commit,
        "published_generation": current_generation,
    }


def record_generation_drift(drift: dict, *, actor: str,
                            queue_root, detail: dict | None = None) -> Path | None:
    """Stamp one immutable ``generation-drift/`` record, or ``None``.

    The loud half of the handshake.  On 2026-09-19 the fleet's active
    generation sat on a four-day-old tree for hours while workers and roles
    kept executing, and the drift was found by forensic inspection because
    nothing anywhere had written a word: loops printed to logs that scroll
    away, and no record named both generations.  This is that record.  It
    follows the queue's own conventions -- canonical JSON, published by
    atomic first-writer link under the queue root, mode 0444 so not even
    its writer can quietly edit it -- and it is written once per incident,
    because every caller stamps on the way out the door (a worker exits;
    a supervisor restarts a role), not per poll.

    Best effort in the same direction as the park marker: a loop that
    cannot write the stamp still refuses, because the refusal is the
    safety and the stamp is the evidence.
    """

    record = {
        "schema": GENERATION_DRIFT_SCHEMA,
        "actor": actor,
        "host": socket.gethostname(),
        "pid": os.getpid(),
        "unix": round(time.time(), 3),
        "loaded_commit": str(drift.get("loaded_commit") or ""),
        "loaded_generation": str(drift.get("loaded_generation") or ""),
        "published_commit": str(drift.get("published_commit") or ""),
        "published_generation": str(drift.get("published_generation") or ""),
        "detail": dict(detail or {}),
    }
    raw = pb._canonical_bytes(record) + b"\n"
    digest = hashlib.sha256(raw).hexdigest()[:8]
    path = (Path(queue_root) / GENERATION_DRIFT_DIR
            / (f"{int(record['unix'] * 1000):013d}-{record['host']}"
               f"-{record['pid']}-{digest}.json"))
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        pb._atomic_publish(path, raw)
    except OSError:
        return None
    return path


def _refuse_moved_runtime(drift: dict, *, host: str, boundary: str) -> None:
    """Stamp the drift, say it loudly, and leave the exit to the caller."""

    stamped = record_generation_drift(
        drift, actor="worker_loop", queue_root=SH / "pb-queue")
    print(f"[{host}] runtime moved "
          f"{drift['loaded_commit'][:12] or '(unversioned)'} -> "
          f"{drift['published_commit'][:12]}"
          + (f"; generation {drift['loaded_generation']} -> "
             f"{drift['published_generation']}"
             if drift["loaded_generation"]
             and drift["published_generation"] != drift["loaded_generation"]
             else "")
          + f"; drift stamped "
          f"{stamped.name if stamped is not None else 'UNWRITABLE'}"
          + f"; refusing the claim at {boundary}"
          + "; exiting so the supervisor reloads it",
          flush=True)


def publication_lock_path() -> Path:
    """This uid's host-local writer lock, as a path only.

    Created and taken in the publisher child, never the loop: a descriptor
    this process opened would be shared with every fork it makes, and a
    ``serve_once`` child would then hold a publication lock it knows nothing
    about for its whole run.
    """

    return (Path(PUBLICATION_LOCK_ROOT)
            / f"prismabuild-offer-{os.getuid()}.lock")


class PublicationLockHeld(RuntimeError):
    """Another writer on this box holds the publication lock."""


def _open_publication_lock(path: Path) -> int:
    """Open, verify, and nonblocking-flock this uid's publication lock.

    The containing directory is as load-bearing as the file: a writable or
    symlinked parent lets anyone replace the lock inode the checks below
    validated.  So the parent is opened once as a real private directory
    (``O_NOFOLLOW`` refuses a symlink, ``O_DIRECTORY`` a file, st_uid and mode
    must be this uid's 0700) and the lock is opened through that descriptor
    with ``dir_fd``, which pins the directory the checks describe.  The file
    itself is then checked as a regular, this-uid, non-group/world-writable
    file with a single link.

    ``flock(LOCK_EX | LOCK_NB)`` is the exclusion.  ``EAGAIN``/``EWOULDBLOCK``
    alone mean contention and raise ``PublicationLockHeld`` (retryable); any
    other ``flock`` failure raises ``RuntimeError`` and fails the publication.
    The returned descriptor is deliberately never closed by the publisher: the
    child holds the lock across the write and until ``os._exit`` drops it, so
    a writer the parent cannot reap still fences its siblings.
    """

    directory = path.parent
    try:
        dir_fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY
                         | os.O_NOFOLLOW | os.O_CLOEXEC)
    except FileNotFoundError:
        try:
            os.mkdir(directory, 0o700)
        except FileExistsError:
            pass
        dir_fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY
                         | os.O_NOFOLLOW | os.O_CLOEXEC)
    except OSError as exc:
        raise RuntimeError(
            f"publication lock directory cannot be opened: {exc}") from exc
    descriptor = None
    try:
        info = os.fstat(dir_fd)
        if info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise RuntimeError(
                "publication lock directory is not this uid's private 0700 "
                "directory")
        try:
            descriptor = os.open(
                path.name, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW
                | os.O_NONBLOCK | os.O_CLOEXEC, 0o600, dir_fd=dir_fd)
        except OSError as exc:
            raise RuntimeError(
                f"publication lock cannot be opened: {exc}") from exc
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode):
            raise RuntimeError("publication lock is not a regular file")
        if info.st_uid != os.getuid():
            raise RuntimeError("publication lock is not owned by this uid")
        if info.st_mode & 0o077:
            raise RuntimeError("publication lock is group- or world-writable")
        if info.st_nlink != 1:
            raise RuntimeError("publication lock has more than one link")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            if exc.errno in (errno.EAGAIN, errno.EWOULDBLOCK):
                raise PublicationLockHeld(
                    f"publication lock is held: {exc}") from exc
            raise RuntimeError(
                f"publication lock could not be taken: {exc}") from exc
    except BaseException:
        if descriptor is not None:
            os.close(descriptor)
        raise
    finally:
        os.close(dir_fd)
    return descriptor


#: Where a service role's host-local singleton lock lives.
#:
#: A role is a box singleton -- one storage reader, one tier minter -- and the
#: queue has no lock that makes two of them safe: two readers warm the same
#: bytes twice and publish contradictory receipts, which is the compounding
#: movers wedge of 2026-09-19 (#709).  The supervisor's own host-wide claim
#: already refuses a second supervisor, but that claim belongs to the
#: supervisor, not to the role: role entrypoints previously had no per-role
#: lock, so a direct invocation, a legacy loop or a stale generation's
#: supervisor could serve a second role beside the running one.  So the lock
#: is taken here, by the process that serves.  The root is
#: host-local and private per uid, the same discipline the admission lock
#: keeps, and ``/tmp`` being cleared on some hosts only means a missing lock
#: file is "no information" and is re-created -- the same accepted lapse the
#: admission lock documents.  Nothing on the shared mount: on shared storage
#: one box's role would silence another's.
ROLE_LOCK_ROOT = Path(f"/tmp/prismabuild-roles-{os.getuid()}")

#: Exit code a service role returns when another instance on this box already
#: holds its singleton lock.  Not 0 (it served nothing), not 75 (nothing was
#: deferred -- this instance must not run at all); stable so the supervisor's
#: respawn logging and an operator can name it.
ROLE_SINGLETON_HELD_EXIT = 3


class RoleLockHeld(RuntimeError):
    """Another process on this box already serves this role.

    Raised instead of racing.  ``holder`` is the pid the kernel names for the
    lock when that could be read, and ``None`` when it could not; a missing
    holder is "unknown", never "nobody".
    """

    def __init__(self, role: str, holder: int | None = None):
        self.role = role
        self.holder = holder
        super().__init__(
            f"the {role} role singleton is held by pid {holder} on this box"
            if holder is not None else
            f"the {role} role singleton is held by another process on this box")


class RoleLockUnavailable(RuntimeError):
    """This role's singleton lock cannot be trusted on this box.

    An unsafe directory or lock file, or a failure other than contention: the
    caller must refuse rather than serve without the exclusion, and it must
    not confuse this with an error out of the work the lock protects.
    """


def role_lock_path(script: str | Path) -> Path:
    """This role's singleton lock, named by the script and not by generation.

    One role, one path, whatever tree the caller's bytes were loaded from:
    that is what lets a replacement generation and a stale one contend rather
    than each serve.  The path is a path only -- ``take_role_singleton`` owns
    creating and locking it -- and it is never unlinked, because a lock file
    removed under a live holder lets the next starter lock a second inode.
    """

    return Path(ROLE_LOCK_ROOT) / f"{Path(script).stem}.lock"


def _role_lock_directory() -> Path:
    """The private per-uid directory the role locks live in.

    The admission lock's discipline: host-local, private to this uid,
    owner-repairable when the mode is too permissive, refused when the object
    is not ours at all.  Refusing rather than proceeding matters here for the
    same reason it does there: a lock nobody can trust is not a lock.
    """

    directory = Path(ROLE_LOCK_ROOT)
    directory.mkdir(parents=True, mode=0o700, exist_ok=True)
    info = directory.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid():
        raise RuntimeError("unsafe PrismaBuild role lock directory")
    if info.st_mode & 0o077:
        directory.chmod(0o700)
        info = directory.lstat()
        if info.st_mode & 0o077:
            raise RuntimeError("unsafe PrismaBuild role lock directory")
    return directory


def take_role_singleton(script: str | Path) -> int:
    """Take this role's host-local singleton lock for the life of the caller.

    Returns an open descriptor holding the flock; the exclusion lives on that
    open file description, so it is released by the final close (the role's
    exit, or the caller's own close) and never by unlinking or an explicit
    unlock.  ``RoleLockHeld`` means another process on this box already serves
    the role; ``RoleLockUnavailable`` means the lock could not be trusted at
    all.  Either way the caller refuses rather than races: fail closed,
    because the lock is the safety.

    Prefer :func:`role_singleton`, which guarantees the final close on every
    exit; raw descriptors are for callers that must probe without holding
    (``role_singleton_holder``).
    """

    role = Path(script).stem
    try:
        directory = _role_lock_directory()
        descriptor = os.open(directory / f"{role}.lock",
                             os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW
                             | os.O_CLOEXEC, 0o600)
    except (OSError, RuntimeError) as exc:
        raise RoleLockUnavailable(
            f"cannot open the {role} role singleton lock: {exc}") from exc
    try:
        info = os.fstat(descriptor)
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                or info.st_nlink != 1):
            raise RoleLockUnavailable("unsafe PrismaBuild role lock file")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            # The holder pid is a diagnostic, and the admission lock's own
            # holder rule is the one place that parse lives (#264, #276),
            # including the btrfs inode/device ambiguity; a helper that
            # cannot answer still leaves this a refusal.
            raise RoleLockHeld(role, adaptive_cpu._holder_of(descriptor)) \
                from None
        except OSError as exc:
            raise RoleLockUnavailable(
                f"the {role} role singleton lock could not be taken: {exc}"
            ) from exc
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor


@contextmanager
def role_singleton(script: str | Path):
    """Hold this role's singleton for the block, or raise ``RoleLockHeld``.

    The descriptor's final close -- the release, and the only release -- is
    guaranteed on every exit from the block, including a ``return`` or an
    exception, so a one-shot invocation that takes the lock cannot leak it
    into a hosting interpreter and a repeated ``main`` call still contends
    honestly.  Nothing here unlinks the lock file or unlocks a shared
    description.
    """

    descriptor = take_role_singleton(script)
    try:
        yield descriptor
    finally:
        os.close(descriptor)


#: The host-local lock and cadence stamp of the spool retirement tick (#1001),
#: beside the role locks.  One loop per box runs a tick; the stamp's mtime is
#: when the last one started.
SPOOL_RETIRE_LOCK = "produced-spool-retire.lock"
SPOOL_RETIRE_STAMP = "produced-spool-retire.stamp"


def spool_retirement(queue, *, host: str, cas_root: Path,
                     now: float | None = None) -> dict | None:
    """Run this box's spool retirement tick when it is due; ``None`` otherwise.

    Due once per ``produced_spool.RETIRE_INTERVAL_S`` per box, whichever loop
    reaches it first: the tick takes a host-local ``flock`` non-blocking, and
    a loop that finds it held, or the stamp younger than the interval, does
    nothing.  The stamp is touched before the tick runs, so a tick that
    raises is retried an interval later, not on every poll of every loop.
    A tick that raises replaces ``<host>.tick.json`` with its failure
    (``produced_spool.record_failed_tick``) before the exception propagates.
    """

    from prismabuild import produced_spool

    now = time.time() if now is None else float(now)
    directory = _role_lock_directory()
    descriptor = os.open(directory / SPOOL_RETIRE_LOCK,
                         os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return None
        stamp = directory / SPOOL_RETIRE_STAMP
        try:
            if now - stamp.stat().st_mtime < produced_spool.RETIRE_INTERVAL_S:
                return None
        except FileNotFoundError:
            pass
        stamp.touch()
        os.utime(stamp, (now, now))
        try:
            return produced_spool.retirement_tick(queue, cas_root, host=host)
        except Exception as exc:
            # A tick that raised is recorded where every tick is, not only
            # on the loop's stdout; the caller still sees the exception.
            try:
                produced_spool.record_failed_tick(queue, host, exc)
            except Exception as record_exc:                      # noqa: BLE001
                print(f"[{host}] spool retirement failure not recorded: "
                      f"{type(record_exc).__name__}: {record_exc}", flush=True)
            raise
    finally:
        os.close(descriptor)


def role_singleton_holder(script: str | Path) -> tuple[bool, int | None]:
    """Whether this role's singleton lock is held, and by which pid if known.

    The lock is tried, not assumed: a free lock is taken and released here --
    the caller's own start still races nothing, because its child takes the
    lock for real -- and a held one is discovered by ``flock``'s refusal and
    named from /proc/locks, never from a pid file.  ``(True, None)`` is a held
    lock whose holder could not be read: "held, holder unknown", never free.
    """

    try:
        descriptor = take_role_singleton(script)
    except RoleLockHeld as held:
        return True, held.holder
    os.close(descriptor)
    return False, None


def _proc_starttime_or_none(pid: int) -> str | None:
    """The pid's starttime, or ``None`` when ``/proc`` cannot answer.

    ``_proc_starttime`` spells an unreadable ``/proc`` as ``"unknown"``, which
    is right for a marker filename but wrong for an ownership decision: a
    launch must not read "I could not ask" as "the writer is gone".
    """

    observed = _proc_starttime(pid)
    return None if observed == "unknown" else observed


def _publisher_child(announce, *, budget_s: float, retry_s: float) -> dict:
    """Child-side publisher: lock, announce, return -- inside ``bounded``.

    ``pbstatus.bounded`` calls this *after* it has isolated this child's file
    descriptors, so the only descriptors here are the reply pipe and what this
    function opens.  Contention is retried locally under the same budget --
    one child, not one fork per retry -- so a busy sibling costs a delay
    rather than this loop's whole poll.
    """

    deadline = time.monotonic() + budget_s
    while True:
        try:
            # Held until ``os._exit``: intentionally never closed.
            _open_publication_lock(publication_lock_path())
            break
        except PublicationLockHeld:
            if time.monotonic() + retry_s >= deadline:
                raise
            time.sleep(retry_s)
    announce()
    return {"status": "published"}


#: One offer publication's outcome.  ``status`` is the only field the loop
#: branches on: ``published`` is the one value that admits work, and every
#: other value means this poll does not reach ``serve_once``.  ``unavailable``
#: is deliberately not "nothing was published": a timed-out publisher may have
#: landed its rename before the deadline, so the outcome is unknown.
PublicationResult = namedtuple("PublicationResult", (
    "status",       # "published" | "unavailable" | "busy" | "failed"
    "elapsed_s",
    "retained",     # (pid, starttime) of a writer this loop could not reap
    "error",        # a named failure reason, or ""
))


def _retained_publisher(abandoned: list) -> tuple[int, str] | None:
    """The first publisher identity this loop cannot prove has exited.

    ``pbstatus.bounded`` appends to ``abandoned`` only a child it could not
    reap within ``KILL_GRACE_S``, and records ``(pid, starttime_ticks)`` for
    it.  A child ``waitpid`` says has exited is reaped here (never a join) and
    dropped.  An identity whose starttime cannot be read is kept: fail closed,
    because the alternative hands the lock to a second writer.
    """

    live: list = []
    retained = None
    for child in abandoned:
        pid = child.get("pid")
        if not isinstance(pid, int) or pid <= 0:
            continue
        try:
            reaped, _status = os.waitpid(pid, os.WNOHANG)
        except ChildProcessError:
            continue
        except OSError:
            pass
        else:
            if reaped == pid:
                continue
        ticks = child.get("starttime_ticks")
        if ticks is None:
            identity = (pid, "unknown")
        else:
            observed = _proc_starttime_or_none(pid)
            if observed is not None and observed != str(ticks):
                continue          # the pid was reused; the child is gone
            identity = (pid, str(ticks))
        live.append(child)
        if retained is None:
            retained = identity
    abandoned[:] = live
    return retained


def publish_offer(announce, *, budget_s: float,
                  retry_s: float, abandoned: list) -> PublicationResult:
    """Publish one advisory offer through an abandonable child under a deadline.

    The child, its file-descriptor isolation, the deadline, the ``SIGKILL``
    and the reaping are all ``pbstatus.bounded``'s, which is also what records
    the exact ``(pid, starttime)`` of a child it could not reap.  This
    function contributes only what is specific to an advisory *write*: the
    host-local lock the child takes, the mapping from ``bounded``'s envelope
    to a publication verdict, and the retained-writer fence.

    Every non-``published`` outcome is a skip, never a serve: the offer
    reserves nothing, so refusing to admit for one poll costs nothing that a
    stale or unreadable advertisement would not cost anyway.
    """

    started = time.monotonic()
    retained = _retained_publisher(abandoned)
    if retained is not None:
        return PublicationResult(
            "busy", round(time.monotonic() - started, 3), retained,
            "an earlier publisher is still unreaped")
    result = bounded("worker-offer-publication",
                     lambda: _publisher_child(
                         announce, budget_s=budget_s, retry_s=retry_s),
                     deadline=Deadline(budget_s), abandoned=abandoned,
                     cap_s=budget_s)
    elapsed = round(time.monotonic() - started, 3)

    # A child the helper could not reap after SIGKILL may still hold the lock;
    # it is dropped only when ``waitpid`` or its starttime proves it is gone.
    retained = _retained_publisher(abandoned)
    status = result.get("status")
    if status == "ok":
        if retained is not None or result.get("value") != {
                "status": "published"}:
            return PublicationResult(
                "unavailable", elapsed, retained,
                "publisher replied without a confirmed publication")
        return PublicationResult("published", elapsed, None, "")
    if status == "timed_out":
        # The write may have landed before the deadline: unknown, not absent.
        # The previous offer is left to expire on its own.
        return PublicationResult(
            "unavailable", elapsed, retained,
            "publication timed out; the write may have landed")
    if status == "error":
        error = result.get("error") or ""
        if result.get("type") == "PublicationLockHeld":
            return PublicationResult("busy", elapsed, retained, str(error))
        return PublicationResult(
            "failed", elapsed, retained,
            f"{result.get('type', 'Exception')}: {error}")
    return PublicationResult(
        "failed", elapsed, retained,
        f"publication returned an unknown status: {status!r}")


#: One queue discovery's outcome.  ``status`` is the only field the loop
#: branches on: ``ready`` carries the snapshot ``serve_once`` must consume,
#: and every other value means this poll reaches neither publication nor
#: admission.  ``unavailable`` is deliberately not "the queue is empty": a
#: scan that did not finish says nothing about what is queued, so the loop
#: must not serve, must not advertise, and must not count the poll as idle.
DiscoveryResult = namedtuple("DiscoveryResult", (
    "status",       # "ready" | "unavailable" | "busy" | "failed"
    "snapshot",     # list of ready records, or None
    "retained",     # (pid, starttime) of a reader this loop could not reap
    "error",        # a named failure reason, or ""
))


def _retained_reader(abandoned: list) -> tuple[int, str] | None:
    """The first discovery reader identity this loop cannot prove has exited.

    Discovery readers never take a lock -- the scan runs outside admission and
    the child inherits nothing it must release (``pbstatus.bounded`` closes
    every unrelated descriptor before the scan) -- so unlike a retained
    publisher this fences nothing but accumulation: one poll must not abandon
    a second reader while the first is still unreaped, or a long stall parks
    one D-state process per poll per loop.  The identity check is the
    publisher's: ``waitpid`` reaps the exited, a reused pid is recognised by
    its starttime, and an unreadable ``/proc`` keeps the entry retained.
    """

    return _retained_publisher(abandoned)


def discover_ready_snapshot(queue, *, budget_s: float,
                            abandoned: list,
                            placement: tuple | None = None) -> DiscoveryResult:
    """Read the claim scan's candidate list through an abandonable child.

    The child runs ``queue.ready_items`` -- ready records plus their
    ``passes/`` sidecars -- and the parent stops waiting at ``budget_s``.
    ``placement`` is this loop's ``(tags, has_gpu)``: given, the child reads
    the sidecar only of a record this loop could place (#993), because the
    claim skips every other record whatever its aging count.  A
    scan that completes is passed to ``serve_once`` as its ``ready`` snapshot;
    anything else skips the poll: the loop returns to its generation and
    maintenance checks after the normal poll delay and never publishes an
    offer or calls ``serve_once`` without evidence it can read the queue
    (issue #16).  The snapshot is advisory -- an intervening claim wins at the
    rename -- so a scan that finished just before the deadline is still safe
    to serve from.
    """

    started = time.monotonic()
    retained = _retained_reader(abandoned)
    if retained is not None:
        return DiscoveryResult(
            "busy", None, retained,
            "an earlier discovery reader is still unreaped")
    if placement is None:
        read = queue.ready_items
    else:
        def read():  # type: ignore[no-untyped-def]
            with pool.ready_placement(*placement):
                return queue.ready_items()
    result = bounded("queue-discovery", read,
                     deadline=Deadline(budget_s), abandoned=abandoned,
                     cap_s=budget_s)
    elapsed = round(time.monotonic() - started, 3)

    retained = _retained_reader(abandoned)
    status = result.get("status")
    if status == "ok":
        snapshot = result.get("value")
        if retained is not None or not isinstance(snapshot, list):
            return DiscoveryResult(
                "unavailable", None, retained,
                "discovery replied without a candidate list")
        return DiscoveryResult("ready", snapshot, None, "")
    if status == "timed_out":
        # Pure read: nothing was claimed, renamed or reserved, so there is
        # nothing unknown to reconcile -- the poll is simply skipped.
        return DiscoveryResult(
            "unavailable", None, retained,
            f"discovery timed out after {elapsed:g}s; the queue was not read")
    if status == "error":
        return DiscoveryResult(
            "failed", None, retained,
            f"{result.get('type', 'Exception')}: {result.get('error') or ''}")
    return DiscoveryResult(
        "failed", None, retained,
        f"discovery returned an unknown status: {status!r}")


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


def idle_census(
    queue,
) -> tuple[dict[str, object] | None, float, str | None]:
    """Read queue pressure once and choose the next idle polling delay."""

    try:
        census = queue.placement_census()
    except Exception as exc:                                    # noqa: BLE001
        # An unreadable queue is not evidence of ready work. Back off instead
        # of amplifying an NFS fault at one request per second per loop.
        return None, 0.0, (f"fleet width unavailable "
                           f"({type(exc).__name__}: {exc})")
    return (census,
            PRESSURE_POLL_S if int(census.get("ready", 0)) > 0 else 0.0,
            None)


def describe_census(
    census: dict[str, object] | None, error: str | None = None,
) -> str:
    if error is not None:
        return error
    if census is None:
        return "fleet width unavailable"
    try:
        return pool.describe_placement_census(census)
    except Exception as exc:                                    # noqa: BLE001
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


def build_parser() -> argparse.ArgumentParser:
    """The worker loop's arguments, without reading them."""

    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true",
                    help="serve at most one action and exit, whether or not "
                         "the queue had anything (debug)")
    ap.add_argument("--timeout-s", type=float, default=DEFAULT_EXECUTION_CEILING_S,
                    help="how long a single action may run before this loop "
                         "kills it")
    ap.add_argument("--poll-s", type=float, default=10.0,
                    help="seconds to sleep between queue polls")
    ap.add_argument("--max-idle", type=int, default=6,
                    help="consecutive empty polls before exiting")
    gpu = ap.add_mutually_exclusive_group()
    gpu.add_argument(
        "--gpu", action="store_true",
        help="detect physical GPU capacity from trusted broker telemetry")
    gpu.add_argument(
        "--gpu-slots", type=int, default=0,
        help="legacy capability flag; 0 means CPU-only and any positive "
             "value is conservatively one physical GPU")
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
                    help="offer declared CPU and host memory without observing "
                         "them (debug); GPU evidence remains mandatory")
    ap.add_argument("--observe-samples", type=int,
                    default=box_capacity.DEFAULT_SAMPLES,
                    help="consecutive observations that must agree before the "
                         "offer falls; 1 lets a single reading decide")
    ap.add_argument("--cpu-slots", type=int, default=0,
                    help="cores this box offers the queue; 0 = the cores this "
                         "loop is actually pinned to")
    ap.add_argument("--spool-gb", type=int, default=0,
                    help="local disk this box offers produced-output spool "
                         "windows and declared bounded local scratch, in GiB; "
                         "0 declares none (#747, #911)")
    return ap


def validate_args(ap: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    """Refuse argument values no box can declare."""

    if args.gpu_slots < 0:
        ap.error("--gpu-slots cannot be negative")
    if args.spool_gb < 0:
        ap.error("--spool-gb cannot be negative")


def declared_host_capacity(args: argparse.Namespace, *, cores: int) -> dict[str, int]:
    """The stable host kinds this loop declares: memory, cores, and spool.

    ``spool_gb`` is the local disk budget produced-output producers reserve
    their spool windows against at claim (#747), and that actions declaring
    bounded local scratch reserve their pairs against (#911).  It is declared only when
    ``--spool-gb`` is positive, so a box started without the flag offers
    exactly what it offered before the kind existed.
    """

    declared = {"mem_gb": args.mem_gb, "cpu": cores}
    if args.spool_gb > 0:
        declared["spool_gb"] = args.spool_gb
    return declared


def _run_loop(stop_requested):
    ap = build_parser()
    args = ap.parse_args()
    validate_args(ap, args)
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
    # Memory and CPU are stable box bounds. GPU capacity is a physical device
    # count from the root-published snapshot, refreshed on every idle pass.
    # Positive legacy slot values are normalized to one device during rollout;
    # they no longer encode a hand-tuned concurrency ceiling.
    base_declared = declared_host_capacity(args, cores=cores)
    gpu_capable = args.gpu or args.gpu_slots > 0
    # Capability and current admission are distinct. ``--gpu`` says this host
    # has a GPU so submissions remain queueable through a telemetry outage;
    # the observed offer below is still zero until trusted evidence returns.
    # A fresh snapshot replaces this one-device bootstrap with its exact
    # physical count, which also supports later multi-device workers without
    # a per-host concurrency knob.
    known_physical_gpus = 1 if gpu_capable else 0

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
    #: This box's bounded local Docker inventory, shared by its loops through
    #: a host-local record: one listing per TTL for the box, not one per loop
    #: per poll.  It is refreshed independently of the ready queue so the
    #: first image-pinned submission can be placed, and a failure leaves
    #: ``None`` (unknown), never an empty set (#714).
    inventory = container_images.InventoryCache()
    def offered_tags(name: str) -> list[str]:
        """What this box offers, for the name it currently has.

        A box offers its own hostname as well as its class.  Item tags must be
        a subset of the worker's, so without the hostname an action pinned to
        one box -- which is every action whose checkout is a box-local worktree
        rather than shared storage -- matches no worker and never runs.

        The progress tags ride the same subset rule as capabilities rather than
        a place, exactly as ``cpu`` does below.  The watchdog and helper have
        separate tags: a previous v1 loop can still run an already-sealed
        watchdog action, but cannot claim a new action that depends on its
        helper environment.

        Built from a name passed in rather than read here, because the name can
        change while this loop runs; see the re-read at the top of the poll.
        """

        tags = [args.klass, name, *args.tag]
        if not gpu_capable:
            # A box with no GPU must say so, or an action demanding gpu=1
            # matches it on tags and then fails at run time instead of waiting
            # for a box that can serve it.
            tags.append("cpu")
        tags.extend((pb.PROGRESS_TAG, pb.PROGRESS_HELPER_TAG, pb.PROGRESS_CYCLE_TAG,
                     pb.POOL_CONTENTION_TAG))
        # This loop's code performs the declared-image claim check, so it may
        # take image-pinned work.  A loop from before the check does not offer
        # the tag and therefore cannot claim that work and die inside it
        # (#714).  Docker being absent does not remove the capability: the
        # check then finds presence unknown and refuses, leaving the item
        # ready for a box that can see it.
        tags.append(pb.CONTAINER_IMAGE_TAG)
        return tags

    host = socket.gethostname()
    offered = offered_tags(host)
    if pinned is not None:
        print(f"[{host}] pinned to {cpu_topology.as_range(pinned)} "
              f"({len(pinned)} of {len(cpu_topology.classify()[0]) + len(cpu_topology.classify()[1])} cpus)",
              flush=True)
    idle = 0
    served = 0
    errors = 0
    announced: dict[str, int] | None = None
    #: Publishers ``pbstatus.bounded`` could not reap after SIGKILL, as
    #: ``(pid, starttime)`` identities.  While one survives, this loop does not
    #: launch another writer: the survivor is what holds the publication lock.
    abandoned_publishers: list = []
    #: Discovery readers ``pbstatus.bounded`` could not reap, as
    #: ``(pid, starttime)`` identities.  While one survives, this loop does not
    #: launch another reader: the survivor holds no lock, but a stalled mount
    #: must not accumulate one D-state process per poll per loop.
    abandoned_discoveries: list = []
    loaded_commit = loaded_runtime_commit()
    loaded_generation = _generation_at(GENERATION_VERSION)
    print(f"[{host}] runtime {loaded_commit[:12] or '(unversioned)'}",
          flush=True)
    print(f"[{host}] offer publication bounded to {OFFER_PUBLISH_TIMEOUT_S:g}s "
          f"per poll (issue #16)", flush=True)
    print(f"[{host}] queue discovery bounded to {DISCOVERY_TIMEOUT_S:g}s "
          f"per poll (issue #16)", flush=True)
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
        # The name is re-read every poll, not read once at startup.  A box can
        # be renamed under a running loop, and a loop that cached the name it
        # started with kept announcing the old one for the rest of its life:
        # after sparklina was renamed, twenty loops went on offering
        # `gx10-6b77`, which showed as a second live node whose admission was
        # permanently `unavailable` because nothing published counters under
        # that name.  Every action that matched only that name would have
        # starved.  The offer follows the box.
        #
        # The old name's `workers/<name>.json` is left to expire on its own
        # rather than removed here: this loop is one of many on the box and
        # cannot know whether a sibling still answers to the old name.
        renamed_to = socket.gethostname()
        if renamed_to != host:
            print(f"[{host}] renamed to {renamed_to}; announcing under the new "
                  f"name from this poll on", flush=True)
            host = renamed_to
            offered = offered_tags(host)
        # Fence stale imported bytes before the first queue read or mutation,
        # and again at the top of every later poll.  In particular, an old
        # orphan reaper must never classify a record emitted by a successor's
        # pbrun schema merely because the successor was activated while this
        # loop was between actions.  The first pass through this check is the
        # loop's startup check, and it runs both directions: a loop started
        # from a generation older than the fleet's and one resurrected from a
        # generation newer than the fleet retreated to both refuse here.
        #
        # This is defense in depth around the claim-time handshake below, not
        # a replacement for it: publication can still move after this read
        # and before serve_once, which is exactly the window the handshake
        # closes.  Cross-generation queue-schema rollout therefore remains an
        # expand/converge/contract operation; this local fence only closes
        # the much larger window in which a loop that already observes the
        # successor keeps touching the queue before exiting.
        #
        # The name is re-read every poll, not read once at startup.  A box can
        # be renamed under a running loop, and a loop that cached the name it
        # started with kept announcing the old one for the rest of its life:
        # after sparklina was renamed, twenty loops went on offering
        # `gx10-6b77`, which showed as a second live node whose admission was
        # permanently `unavailable` because nothing published counters under
        # that name.  Every action that matched only that name would have
        # starved.  The offer follows the box.
        #
        # The old name's `workers/<name>.json` is left to expire on its own
        # rather than removed here: this loop is one of many on the box and
        # cannot know whether a sibling still answers to the old name.
        drift = generation_drift(loaded_commit=loaded_commit,
                                 loaded_generation=loaded_generation)
        if drift is not None:
            _refuse_moved_runtime(drift, host=host, boundary="poll top")
            return 0
        gate = read_maintenance_gate()
        if gate is not None:
            post_park_marker(gate)
            print(f"[{host}] resource broker draining for maintenance; admission paused", flush=True)
            # Drain-path membership reconciliation (authoritative for
            # resigned workers): a resign CLI that died after withdrawing
            # leaves owed rows no CLAIMED scan can see, and a resigned
            # worker never reaches the open-gate hook below — the
            # ``continue`` here skips it. Settle through the existing
            # retry path without claiming: publish matured successors,
            # adopt exact ones, preserve foreign rows. Bounded (one
            # withdrawn/ glob when empty) and exception-isolated; any
            # failure only skips this poll's settlement.
            try:
                from fleet_membership import reconcile_membership
                reconciled = reconcile_membership(queue, host)
                settled = (reconciled.get("published", [])
                           + reconciled.get("adopted", []))
                if settled:
                    print(f"[{host}] membership reconciled in drain: {settled}",
                          flush=True)
                retained = reconciled.get("retained", [])
                if retained:
                    print(f"[{host}] membership retained in drain: {retained}",
                          flush=True)
            except Exception as exc:                             # noqa: BLE001
                print(f"[{host}] membership reconciliation skipped in drain: "
                      f"{type(exc).__name__}: {exc}", flush=True)
            if args.once:
                return 0
            time.sleep(args.poll_s)
            continue
        if not observer_initialized:
            observer = (None if args.assume_idle and not gpu_capable else
                        box_capacity.CapacityObserver(
                            samples=args.observe_samples,
                            ledger_total=queue.ledger().capacity(),
                        ))
            observer_initialized = True
        gpu_sample = box_capacity.trusted_gpu_sample() if gpu_capable else None
        if args.gpu:
            detected = box_capacity.physical_gpu_count(gpu_sample)
            if detected > 0:
                known_physical_gpus = detected
        declared = {"gpu": known_physical_gpus, **base_declared}

        # The announcement records the stable physical capability while the
        # ledger receives only the capacity justified at this claim boundary.
        # That distinction keeps temporarily blocked GPU work placeable without
        # permitting a claim from missing or stale telemetry. Retiring deletes
        # free tokens only, so it never removes an active action's reservation;
        # a later fresh observation can restore capacity on a subsequent claim.
        # The placement power fraction this offer publishes is a GPU-only
        # reading over a GPU-only reference (#806).  Admission's host-local
        # record is where the measured half of that reference lives, so the
        # offer reads the same one rather than keeping a second.
        observe_overrides = {
            "gpu_sample": gpu_sample,
            "gpu_state": gpu_admission.host_local_power_state(
                getattr(queue.ledger(), "base", None)),
        }
        if args.assume_idle:
            # This diagnostic may bypass noisy CPU and host-memory readings,
            # but it is not an escape hatch from the trusted GPU boundary.
            observe_overrides.update(mem_gb=None, load1=None)
        # The bound method, not its result: ``observe`` reads /proc between the
        # ledger and the clamp that adds the two together, and reads this on
        # both sides of that instrument so a sibling loop's release cannot be
        # counted as both a token this box still holds and memory it has back.
        # There are 3-16 of these loops per box against one ledger.
        held = queue.ledger().held
        capacity = dict(declared) if observer is None else observer.offer(
            declared, held, **observe_overrides)
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
        # How many loops this box is running.  Censused here rather than
        # accumulated across siblings, and re-censused every poll, so a loop
        # that exits is absent from the next record without anything having to
        # notice it left.  ``None`` on an unreadable ``/proc`` is deliberate:
        # the field is a diagnostic, and a wrong count is worse than none.
        try:
            loops = len(box_capacity.worker_loops())
        except OSError:
            loops = None
        # Which client addresses this box's NFS reads arrive from, so the
        # storage host's pacer can tell an action it is warming for from a
        # client it must protect (#580).  ``None`` on any failure, and the
        # helper cannot raise: this is the offer path of every loop.
        addresses = box_capacity.ipv4_addresses()

        # The claim scan's pure-read half runs first, in an abandonable child:
        # a box that cannot read the queue must neither offer nor admit, and
        # its offer must expire on its own rather than be refreshed on no
        # evidence (issue #16).  The snapshot is advisory -- an intervening
        # claim wins at the rename -- so serving from it is safe, and skipping
        # on any other outcome is fail-closed: an unreadable queue is not an
        # empty one.  This poll is not counted idle and not counted as a
        # failure either way: a wedged mount is environmental, and neither the
        # idle exit nor the error exit would repair it -- both would only churn
        # loops while the mount is down and slow the recovery after it.
        discovery = discover_ready_snapshot(
            queue, budget_s=DISCOVERY_TIMEOUT_S,
            abandoned=abandoned_discoveries,
            placement=(tuple(offered), gpu_capable))
        if discovery.status != "ready":
            identity = (f" retained reader pid={discovery.retained[0]}"
                        f" starttime={discovery.retained[1]}"
                        if discovery.retained else "")
            print(f"[{host}] queue discovery {discovery.status} "
                  f"({discovery.error or 'no reason'}"
                  f"{identity}); skipping publication and admission this poll",
                  flush=True)
            if args.once:
                return 1
            time.sleep(args.poll_s)
            continue

        # The offer's inventory: the shared record, refreshed at most once per
        # TTL for the box.  It says what this box can positively show right
        # now, and an item that declares images is not placeable here without
        # it.
        observed_images = inventory.get()

        def announce_offer(queue=queue, host=host, tags=offered,
                           has_gpu=gpu_capable, declared=declared,
                           capacity=capacity, observer=observer, loops=loops,
                           runtime_commit=loaded_commit, cpu_tiers=cpu_tiers,
                           timeout_s=args.timeout_s, addresses=addresses,
                           observed_images=observed_images):
            """The exact advisory record this poll offers the queue.

            A closure, not a kwargs dict, so the publisher's child runs the
            ordinary :meth:`PoolQueue.announce` with the same arguments the
            loop always passed -- one publication path, not two.  Every value
            is bound at definition, so the record a publisher writes is the
            one this poll computed.
            """

            queue.announce(
                host=host, tags=tags, has_gpu=has_gpu,
                capacity=declared, observed_capacity=capacity,
                foreign=(observer.last.foreign if observer is not None
                         and observer.last is not None else None),
                observed_detail=(observer.last.detail if observer is not None
                                 and observer.last is not None else None),
                runtime_commit=runtime_commit, cpu_tiers=cpu_tiers, loops=loops,
                # The ceiling this loop will actually kill an action at.  It is a
                # CLI default nobody outside the loop could see, and
                # ``_execution_timeout`` applies it as a silent ``min`` -- so a
                # submitter asking for 13000 s was given 7200 s and told nothing,
                # and the #275 campaign died at 7200 s believing it had 13000
                # (#293).  Announcing it is what lets pbrun say so at submit.
                timeout_ceiling_s=timeout_s,
                # And what this loop's code can do with a progress-declaring
                # action.  Announced beside the ceiling because they are two halves
                # of one answer: the ceiling is what this box would cut a run at,
                # and this is whether it would count committed work first (#480).
                progress_contracts=[pb.PROGRESS_RECORD_SCHEMA_V1],
                addresses=addresses,
                # Which local image references this box can positively show.
                # Present-and-empty says it looked and has none; absent says
                # it could not look.  An image-pinned item reads both as
                # not-here at claim and as not-placeable here at dispatch.
                observed_images=observed_images,
            )

        publication = publish_offer(
            announce_offer, budget_s=OFFER_PUBLISH_TIMEOUT_S,
            retry_s=OFFER_PUBLISH_RETRY_S, abandoned=abandoned_publishers)
        if publication.status != "published":
            # An offer is advisory: it reserves nothing, claims nothing and
            # starts nothing.  So a publication that did not complete -- busy,
            # failed, or past a deadline that may still have landed the write
            # -- skips this poll's admission instead of admitting work from an
            # advertisement nobody can read.  The loop stays alive, returns to
            # the generation and maintenance checks above on its normal
            # cadence, and the previous offer is left to expire rather than
            # unlinked, exactly as a box that stopped offering must.  The
            # write may already have landed; this reports that the outcome is
            # unknown, never that nothing was published.
            identity = (f" retained writer pid={publication.retained[0]}"
                        f" starttime={publication.retained[1]}"
                        if publication.retained else "")
            print(f"[{host}] offer publication {publication.status} after "
                  f"{publication.elapsed_s:g}s ({publication.error or 'no reason'}"
                  f"; result unknown{identity}); "
                  f"skipping admission this poll", flush=True)
            idle += 1
            if args.once:
                return 1
            time.sleep(args.poll_s)
            continue

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
        # Membership handoff reconciliation, through the existing retry
        # path: a resign CLI that died after withdrawing leaves owed rows
        # no CLAIMED scan can see. The loops themselves settle them here —
        # publish matured successors, adopt exact ones, preserve foreign
        # rows — so settlement does not depend on the requester surviving.
        # Bounded and exception-isolated: an empty withdrawn/ directory
        # costs one glob, and any failure only skips this poll.
        try:
            from fleet_membership import reconcile_membership
            reconciled = reconcile_membership(queue, host)
            settled = (reconciled.get("published", [])
                       + reconciled.get("adopted", []))
            if settled:
                print(f"[{host}] membership reconciled: {settled}", flush=True)
        except Exception as exc:                                 # noqa: BLE001
            print(f"[{host}] membership reconciliation skipped this poll: "
                  f"{type(exc).__name__}: {exc}", flush=True)
        # The spool retirement tick (#1001): a producer's host spool outlives
        # a producer that was killed or withdrawn, and only this box can
        # reach it.  Due once per interval per box, exception-isolated, and
        # outside every pool lock.
        try:
            retired = spool_retirement(queue, host=host, cas_root=SH / "cas")
            if retired is not None and (retired["retired"] or retired["errors"]):
                print(f"[{host}] spool retirement: {retired['retired']} namespaces, "
                      f"{retired['bytes']} bytes, {len(retired['kept'])} kept, "
                      f"errors {retired['errors'][:3]} ({retired['seconds']}s)",
                      flush=True)
        except Exception as exc:                                 # noqa: BLE001
            print(f"[{host}] spool retirement skipped this poll: "
                  f"{type(exc).__name__}: {exc}", flush=True)

        # The claim-time handshake.  Everything above -- offer publication,
        # queue discovery -- may have taken seconds, and a publisher can
        # activate a successor generation inside exactly that window, so the
        # poll-top fence alone let a worker claim an action the fleet had
        # already moved off of: agreement at the top of the poll, execution
        # of bytes the fleet retired, silence in between.  That was the
        # 2026-09-19 incident's worker half.  So the generation identity is
        # re-read here, immediately before the claim, through the same
        # existing reader -- one file, the tiny receipt -- and on mismatch
        # the claim is refused, one drift record is stamped, and the loop
        # exits for the supervisor to respawn from the active generation.
        # A worker never executes an action sealed for a fleet it is not.
        #
        # The boundary is the claim, deliberately: an action already
        # executing under ``serve_once`` is atomic to this loop and finishes
        # under the generation that claimed it, which is also what
        # publish_runtime's rolling convergence depends on -- loops cycle at
        # their own boundaries while a publish converges, and nothing here
        # fights that by interrupting work in flight.
        drift = generation_drift(loaded_commit=loaded_commit,
                                 loaded_generation=loaded_generation)
        if drift is not None:
            _refuse_moved_runtime(drift, host=host, boundary="claim boundary")
            return 0
        # The claim's own evidence, taken after publication so it is not the
        # offer's TTL that answers a claim.  While image-pinned work is
        # waiting, the shared record is re-probed at CLAIM_FRESHNESS_S, once
        # for the box, in this poll and outside every pool lock.  A claim can
        # still race an image removal between this observation and the
        # container start; the probe bounds how stale the box's evidence is,
        # never how immutable the image is.
        claim_images = (
            inventory.get(max_age_s=container_images.CLAIM_FRESHNESS_S)
            if container_images.required_from_items(discovery.snapshot)
            else None)
        try:
            outcome = queue.serve_once(
                tags=offered, has_gpu=gpu_capable, python=args.python,
                timeout_s=args.timeout_s, capacity=capacity, cpu_tiers=cpu_tiers,
                adaptive_cpu=not args.assume_idle, containment=True,
                ready=discovery.snapshot, observed_images=claim_images,
                # Re-checked under the per-key transition lock just before
                # the claim rename, so a resign drain that began after this
                # poll's gate check still refuses the claim deterministically.
                admission_open=lambda: read_maintenance_gate() is None,
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
            census, pressure_delay, census_error = idle_census(queue)
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
                print(f"[{host}] idle; {describe_census(census, census_error)}", flush=True)
            if args.once or idle >= args.max_idle:
                free = queue.ledger().available()
                print(f"[{host}] nothing admissible ({idle} idle polls); "
                      f"served {served}; free {free}; "
                      f"{describe_census(census, census_error)}",
                      flush=True)
                return 0
            time.sleep(min(args.poll_s, pressure_delay)
                       if pressure_delay > 0 else args.poll_s)
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
