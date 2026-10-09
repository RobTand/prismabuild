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
import math
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
                         core as pb, cpu_topology, dependency_digest,
                         filesystem_floor, local_dependencies, local_scratch, pool,
                         publication_canary, storage_tiers)
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


#: The longest owner and reason this loop copies out of the maintenance gate.
#: The gate is root-written data this process does not control and every field
#: in it is optional; the broker already truncates owner at 200 and reason at
#: 1000, and a reader of the offer should never have to parse an unbounded
#: string out of a JSON record.
_DRAIN_OWNER_LIMIT = 200
_DRAIN_REASON_LIMIT = 1000


def drain_offer_fields(gate: dict) -> dict:
    """The offer fields that keep a parked loop visible in the census (#1204).

    ``read_maintenance_gate`` returns a gate for every state that means
    "closed" -- including a missing or unparsable file, where it synthesizes
    the reason -- so ``state`` is always ``draining`` here.  ``owner``,
    ``reason`` and ``changed_unix`` are copied only when they have the shape a
    reader can use, and are bounded; a missing or malformed field is omitted
    rather than rendered, and the record still says the box is draining.
    ``changed_unix`` names one drain, which is what lets a reader match this
    offer to the gate that produced it; ``reason`` is descriptive text and is
    deliberately not identity.
    """

    fields = {"state": "draining"}
    owner = gate.get("owner")
    if isinstance(owner, str) and owner:
        fields["drain_owner"] = owner[:_DRAIN_OWNER_LIMIT]
    reason = gate.get("reason")
    if isinstance(reason, str) and reason:
        fields["drain_reason"] = reason[:_DRAIN_REASON_LIMIT]
    stamp = gate.get("changed_unix")
    if type(stamp) in (int, float):
        try:
            stamp = float(stamp)
        except OverflowError:
            stamp = None
        if stamp is not None and math.isfinite(stamp):
            fields["drain_changed_unix"] = stamp
    return fields


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


def interpreter_lookup(items) -> tuple[list[str], list[str]]:
    """The paths READY items name, split into this box's ``(present, absent)``.

    The lookup design (#1263): no directory scan and no configured roots --
    the offer answers exactly the paths the queue asks about, each with one
    stat.  Present is positive evidence only, and absent is an ANSWER, not a
    guess (#1266 review): the submission-time verdict refuses a path only
    when every capable eligible offer named it absent, which is what makes a
    first submission -- a path no offer was asked about yet -- publish with a
    notice instead of deadlocking the tool on its own first use.
    """

    paths = sorted({
        str(item.get("interpreter"))
        for item in items
        if isinstance(item, dict)
        and isinstance(item.get("interpreter"), str)})
    present, absent = [], []
    for path in paths:
        if os.path.isfile(path) and os.access(path, os.X_OK):
            present.append(path)
        else:
            absent.append(path)
    return present, absent


def local_dependency_lookup(items, *, python: str) -> dict[str, str]:
    """Extend the same offer lookup with command/path questions, not a registry."""
    import shutil
    requirements = {python: "executable"}
    shell = shutil.which("bash", path=os.defpath)
    if shell:
        requirements[str(Path(shell).resolve())] = "executable"
    for item in items:
        if not isinstance(item, dict):
            continue
        interpreter = item.get("interpreter")
        if isinstance(interpreter, str):
            requirements[interpreter] = "executable"
        for field in ("dependency_queries", "local_dependencies"):
            try:
                queries = local_dependencies.normalize(item.get(field, {}))
            except ValueError:
                continue  # A malformed foreign row cannot vouch for any path.
            for path, kind in queries.items():
                if kind == "executable" or path not in requirements:
                    requirements[path] = kind
    return local_dependencies.observe(requirements)
def dependency_lookup(items) -> tuple[list[str], list[str]]:
    """The requirement paths READY items name, as this box's ``(present, absent)``.

    The interpreter lookup's shape (#1263), one stat per distinct path per
    poll (#1495): the offer answers exactly what the queue asks about, never
    scans, and never hashes here -- the claim gate reads the bytes.
    """

    paths = sorted({
        str(entry.get("path"))
        for item in items if isinstance(item, dict)
        for entry in (item.get("requires_files") or ())
        if isinstance(entry, dict) and isinstance(entry.get("path"), str)})
    present, absent = [], []
    for path in paths:
        (present if os.path.isfile(path) else absent).append(path)
    return present, absent


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


def main(argv=None, *, on_outcome=None):
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
        if argv is None and on_outcome is None:
            return _run_loop(lambda: stopping)
        return _run_loop(lambda: stopping, argv=argv, on_outcome=on_outcome)
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
    ap.add_argument("--mem-gb-ram-tier-roof", action="store_true",
                    help="offer the RAM tier policy's measured roof "
                         "(MemTotal - max(arc c_max, arc_floor) - reserve) "
                         "instead of --mem-gb, reread every poll; unreadable "
                         "inputs keep --mem-gb (#1222)")
    ap.add_argument("--tag", action="append", default=[],
                    help="extra placement tag this box offers")
    ap.add_argument("--gang-admission", action="store_true",
                    default=os.environ.get("PRISMABUILD_GANG_ADMISSION") == "1",
                    help="offer the gang-v1 capability and admit gang members "
                         "(#1517; default off, or PRISMABUILD_GANG_ADMISSION=1)")
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
    ap.add_argument("--disk-metadata-capacity", type=int, choices=(0, 1), default=0,
                    help="cooperative same-host disk_metadata admission token; "
                         "0 disables (default), 1 serializes actions declaring "
                         "disk_metadata=1 on this host's shared ledger, not "
                         "ordinary work, tier egress or external I/O (#1008)")
    ap.add_argument("--class-image-config", default=None,
                    help="explicit generation-relative v1 class image declaration; "
                         "refuses declared container work only on drift or unknown evidence")
    ap.add_argument("--local-scratch-profile-config", default=None,
                    help="explicit v1 configured executed CAS scratch-profile references; "
                         "absent reads no profile input; never benchmarks during polling")
    ap.add_argument("--spool-gb", type=int, default=0,
                    help="local disk this box offers produced-output spool "
                         "windows and declared bounded local scratch, in GiB; "
                         "0 declares none (#747, #911).  A positive value is "
                         "the supervisor's first measurement; each poll "
                         "declares its live one in its place (#1190)")
    return ap



def image_observation(inventory, policy, *, max_age_s=None):
    """Read one shared cache view for the references and optional class proof."""
    if policy is None:
        references = inventory.get() if max_age_s is None else inventory.get(max_age_s=max_age_s)
        return references, None, None
    snapshot = inventory.snapshot(max_age_s=max_age_s)
    references = (frozenset(snapshot["entries"])
                  if snapshot is not None and snapshot["entries"] is not None else None)
    verdict = policy.evaluate(snapshot, now=time.time(),
                              max_age_s=container_images.INVENTORY_TTL_S
                              if max_age_s is None else max_age_s)
    return references, snapshot, verdict


def validate_args(ap: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    """Refuse argument values no box can declare."""

    if args.gpu_slots < 0:
        ap.error("--gpu-slots cannot be negative")
    if args.spool_gb < 0:
        ap.error("--spool-gb cannot be negative")
    if (type(args.disk_metadata_capacity) is not int
            or args.disk_metadata_capacity not in (0, 1)):
        ap.error("--disk-metadata-capacity must be 0 or 1")


def declared_host_capacity(args: argparse.Namespace, *, cores: int) -> dict[str, int]:
    """The stable host kinds: memory, cores, cooperative metadata, and spool.

    ``disk_metadata`` is configured, not measured disk headroom. All loops on
    the host share one ledger; the token serializes only actions declaring it.
    Explicit zero lets the normal poll retire stale free tokens on opt-out,
    without revoking a running holder's reservation.

    ``spool_gb`` is the local disk budget produced-output producers reserve
    their spool windows against at claim (#747), and that actions declaring
    bounded local scratch reserve their pairs against (#911). It is declared
    only when ``--spool-gb`` is positive; a box started without that flag
    declares no spool budget.
    """

    declared = {"mem_gb": args.mem_gb, "cpu": cores,
                "disk_metadata": args.disk_metadata_capacity}
    if args.spool_gb > 0:
        declared["spool_gb"] = args.spool_gb
    return declared


def _ram_tier_mem_roof() -> tuple[int | None, str]:
    """The RAM tier policy's measured roof for this box, with its source.

    The policy file sits beside this loop like it sits beside the tier
    loop, published with the runtime; the ARC and ``MemTotal`` are read
    per call so the observer re-measures the roof every poll.  The
    answer is a ``(roof, source)`` pair the observer stamps into
    ``observed_detail`` every poll (#1245 review B2): a measured roof
    with ``"measured"``, and any unreadable input as ``None`` with its
    fallback reason, so the worker record always says which roof is in
    force and a silent fall back to the declared ``--mem-gb`` is
    observable (#1222).
    """

    here = Path(__file__).resolve().parent
    for candidate in (here / storage_tiers.RAM_POLICY_FILE,
                      here.parent / storage_tiers.RAM_POLICY_FILE):
        roof = box_capacity.ram_policy_mem_roof(
            policy_path=candidate, arcstats_path=storage_tiers.ARCSTATS,
            meminfo_path="/proc/meminfo")
        if roof is not None:
            return roof, "measured"
        if not candidate.exists():
            continue
        # The policy is here but its inputs will not read: name it.
        return None, "fallback:roof_unreadable"
    return None, "fallback:policy_missing"


def live_host_capacity(declared: dict[str, int], ledger) -> dict[str, int]:
    """``declared`` with its ``spool_gb`` replaced by the box's live offer.

    The loop's ``--spool-gb`` is the supervisor's first measurement of the
    disk, and it does not change while the loop runs.  The supervisor measures
    again on every tick while nothing holds ``spool_gb`` here and records the
    value in the host-local offer file, which this reads on every poll (#1190).
    A zero there is a measurement and is declared as one, so free tokens are
    retired as the disk fills.

    A missing or unreadable file is no information, not a zero.  The loop
    then declares its argument capped at the ledger's current total: the file
    may have been removed, or never written, while a holder runs, and
    declaring the uncapped argument could mint back room the disk no longer
    has.  A
    loop started without ``--spool-gb`` declares no ``spool_gb`` and reads
    nothing.
    """

    kind = local_scratch.KIND
    if kind not in declared:
        return dict(declared)
    live = local_scratch.read_spool_offer(ledger.base)
    if live is None:
        try:
            live = min(int(declared[kind]),
                       int(ledger.capacity().get(kind, 0)))
        except (OSError, pool.PoolContractError):
            live = 0
    return {**declared, kind: live}


def observed_claim_capacity(queue, *, args, base_declared, observer,
                            gpu_sample, known_physical_gpus):
    """One producer for fresh claim capacity, including bracketed held reads."""
    if args.gpu:
        detected = box_capacity.physical_gpu_count(gpu_sample)
        if detected > 0:
            known_physical_gpus = detected
    declared = {"gpu": known_physical_gpus,
                **live_host_capacity(base_declared, queue.ledger())}
    overrides = {
        "gpu_sample": gpu_sample,
        "gpu_state": gpu_admission.host_local_power_state(
            getattr(queue.ledger(), "base", None)),
    }
    if args.assume_idle:
        overrides.update(mem_gb=None, load1=None)
    held = queue.ledger().held
    capacity = (dict(declared) if observer is None else
                observer.offer(declared, held, **overrides))
    return declared, capacity, known_physical_gpus


def private_claim_parameters(queue):
    """Executable private tool callers use observed host admission, not grants.

    A separate queue root does not prove an outer reservation. No submissions
    or synthetic scope adoption occur here; resource requests still come from
    the caller and may be refused on the actual host.
    """
    args = build_parser().parse_args([])
    tiers = cpu_topology.inherited_tiers()
    if tiers is None:
        raise pool.PoolContractError("private claim requires known inherited CPU topology")
    base = declared_host_capacity(args, cores=sum(map(len, tiers.values())))
    observer = box_capacity.CapacityObserver(
        samples=args.observe_samples, ledger_total=queue.ledger().capacity())
    _declared, capacity, _gpus = observed_claim_capacity(
        queue, args=args, base_declared=base, observer=observer,
        gpu_sample=None, known_physical_gpus=0)
    return {"capacity": capacity, "cpu_tiers": tiers, "adaptive_cpu": True,
            "observed_external_gib": observer.last_offer_external_gib}


def stable_host_capacity(live: dict[str, int],
                         started: dict[str, int]) -> dict[str, int]:
    """What the offer record declares for ``placeable``: ``live``, held up.

    ``placeable`` asks whether this box could ever run an item, and a disk
    another writer has filled for now answers that no better than a box
    running someone else's encode (see the announce below).  So the record
    declares the larger of the loop's starting measurement and the live one:
    a freed disk raises it at once, and a filling one does not refuse at
    submission what the box offered when the loop started.  The claim still
    uses ``live``.
    """

    kind = local_scratch.KIND
    if kind not in live or kind not in started:
        return dict(live)
    return {**live, kind: max(int(live[kind]), int(started[kind]))}


def _pid_provably_dead(pid: int) -> bool:
    """True only when this box's kernel says ``pid`` does not exist.

    ``EPERM`` means the process exists, and anything else is unknown; both read
    as not provably dead.
    """
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    except OSError:
        return False
    return False


def sweep_own_dead_offer_tmp(workers_dir: Path, host: str, *,
                             now: float | None = None,
                             min_age_s: float | None = None) -> list[str]:
    """Remove this box's own offer temporary files whose writer is gone (#1040).

    ``_write_json_atomic`` names its temporary ``.<name>.<pid>.<uuid>.tmp`` and
    removes it only on an exception, so a writer killed between the create and
    the rename leaves the file for good.  Only files named for *this* host are
    considered: a pid names a process on one box, and another box's files are
    not ours to judge.  A file goes only when both hold: the pid is provably dead
    here, and the file is older than an offer's life, so a live writer in
    another pid namespace on this box (a container sharing the mount) is never
    swept mid-write.  Returns the names removed.  Any error is a skip: the sweep
    is housekeeping and never blocks the loop.
    """
    import re
    now = time.time() if now is None else now
    min_age_s = pool.OFFER_TIMEOUT_S if min_age_s is None else min_age_s
    pattern = re.compile(
        r"\." + re.escape(host) + r"\.json\.(\d+)\.[0-9a-f]{32}\.tmp")
    removed: list[str] = []
    try:
        entries = sorted(os.scandir(workers_dir), key=lambda e: e.name)
    except OSError:
        return removed
    for entry in entries:
        match = pattern.fullmatch(entry.name)
        if match is None:
            continue
        try:
            info = entry.stat(follow_symlinks=False)
            if not stat.S_ISREG(info.st_mode):
                continue
            if now - info.st_mtime <= min_age_s:
                continue
            if not _pid_provably_dead(int(match.group(1))):
                continue
            os.unlink(entry.path)
            removed.append(entry.name)
        except OSError:
            continue
    return removed


def scratch_observed_detail(observer, profile_inputs):
    """The actual offer merges qualified configured profiles with host samples."""
    detail = dict(observer.last.detail if observer is not None and observer.last is not None else {})
    if profile_inputs is not None:
        detail.update(profile_inputs.observe())
    return detail


def _run_loop(stop_requested, *, argv=None, on_outcome=None):
    ap = build_parser()
    args = ap.parse_args(argv)
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
    profile_inputs = (local_scratch.ProfileInputs(
        args.local_scratch_profile_config, source_root=RUNTIME_ROOT,
        checkout_root=pool.LOCAL_CHECKOUT_ROOT, producer_python=args.python)
        if args.local_scratch_profile_config is not None else None)
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
    class_policy = (container_images.ClassImagePolicy.from_file(
        args.class_image_config, source_root=RUNTIME_ROOT, klass=args.klass)
        if args.class_image_config is not None else None)
    # Immutable loaded generation, never the moving repo symlink. Old loops
    # cannot claim a successor publisher's privileged action.
    loaded_generation = _generation_at(GENERATION_VERSION)
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
                     pb.POOL_CONTENTION_TAG, pb.EGRESS_PROGRESS_TAG))
        # This loop's code performs the declared-image claim check, so it may
        # take image-pinned work.  A loop from before the check does not offer
        # the tag and therefore cannot claim that work and die inside it
        # (#714).  Docker being absent does not remove the capability: the
        # check then finds presence unknown and refuses, leaving the item
        # ready for a box that can see it.
        tags.append(pb.CONTAINER_IMAGE_TAG)
        # Same fence for a named interpreter (#1263): this loop's claim checks
        # the item's absolute path on this box before it spends an attempt,
        # and its offer answers for the paths READY items name.  A loop from
        # before the field offers neither, so an interpreter-naming item waits
        # for a box that can run it instead of dying with 127 there.
        tags.append(pb.INTERPRETER_TAG)
        tags.append(local_dependencies.TAG)
        # And for digest-pinned dependencies (#1495): this loop's claim
        # hashes the row's required bytes on this box before it spends an
        # attempt, and its offer answers for the paths READY items name.  A
        # loop from before the contract offers neither, so a row whose
        # dependencies it would silently ignore waits for a box that can
        # read the requirement instead of dying inside it.
        tags.append(dependency_digest.DEPENDENCY_DIGEST_TAG)
        # Code capability only; configured executed profiles and current root
        # observations separately decide admission. Old workers cannot ignore
        # opted-in sealed traffic during a rolling publication.
        tags.append(local_scratch.IO_CAPABILITY)
        # Versioned lifetime actions cannot run on declaration-only workers.
        tags.append(local_scratch.SCRATCH_LIFETIME_TAG)
        if args.gang_admission:
            # Default off (#1517): without this no box offers the tag, so no
            # gang member is ever claimed and no gang code runs in a pass.
            from prismabuild import _gang
            tags.append(_gang.TAG)
        if loaded_generation:
            tags.extend((publication_canary.CAPABILITY,
                         f"runtime-generation:{loaded_generation}"))
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
    #: The declared capability the last open-gate poll published, as
    #: ``placeable`` reads it, carried across polls so a drain can go on
    #: declaring what this box could run (#1204).  It starts at the configured
    #: bootstrap and is replaced by every open poll's stable declaration.  The
    #: drain branch must not shrink it -- a supervisor spool refresh can raise
    #: the live figure above the startup base, and a submission that fits the
    #: box must not become unplaceable because the gate closed afterwards --
    #: and must not re-read the ledger to recompute it.
    last_declared: dict[str, int] = {"gpu": known_physical_gpus, **base_declared}
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
    print(f"[{host}] runtime {loaded_commit[:12] or '(unversioned)'}",
          flush=True)
    print(f"[{host}] offer publication bounded to {OFFER_PUBLISH_TIMEOUT_S:g}s "
          f"per poll (issue #16)", flush=True)
    print(f"[{host}] queue discovery bounded to {DISCOVERY_TIMEOUT_S:g}s "
          f"per poll (issue #16)", flush=True)
    # The dead-offer sweep (#1040) reads and removes files in the queue's
    # ``workers/`` directory, so it runs after the first stale-generation
    # fence below, never before: a loop that is about to exit for a newer
    # runtime must not touch the queue first.
    swept_dead_offers = False
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
        if not swept_dead_offers:
            swept_dead_offers = True
            swept = sweep_own_dead_offer_tmp(queue.root / "workers", host)
            if swept:
                print(f"[{host}] swept {len(swept)} dead offer temporary "
                      f"file(s) (issue #1040)", flush=True)
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
            # Retry this box's own saved finishes under the closed gate
            # (#1403).  A saved outcome claims nothing, so it is safe while
            # admission is paused, and the queue applies the owner-host rule
            # itself: only the claiming box may retry, foreign and non-pending
            # rows are left alone, and ordinary expired leases are not
            # requeued.  This is the maintenance branch's only reach into the
            # reaper's retry path -- ``serve_once`` is never called here, which
            # is what left a drained owner holding its scope and its tokens
            # until an operator ran the reaper by hand.  The retry runs on the
            # queue's host-local heartbeat throttle, the same deliberately
            # unlocked ``_sweep_due`` marker ``serve_once`` uses: two loops may
            # race a redundant pass; no strict at-most-one pass is promised.
            # It is exception-isolated like the membership reconciliation
            # above: a failure only skips this poll's retry and the parked
            # offer below still publishes.
            try:
                retried = queue.retry_own_pending_finishes()
                if retried:
                    print(f"[{host}] saved finishes concluded in drain: "
                          f"{retried}", flush=True)
            except Exception as exc:                             # noqa: BLE001
                print(f"[{host}] saved-finish retry skipped in drain: "
                      f"{type(exc).__name__}: {exc}", flush=True)
            # Keep the parked box visible (#1204).  A loop that waits without
            # announcing leaves its last offer to expire, and then a
            # deliberately drained box and a dead worker read identically:
            # same state, same reason, only a root-owned gate file on the box
            # tells them apart.  So each drain poll republishes an offer that
            # says the box is draining and names the gate's holder, reason and
            # change time.  Staleness means the loop stopped again, which is
            # what a reader needs it to mean.
            #
            # The record keeps the declared capability (tags, host, the last
            # open poll's declared capacity, image inventory) because that
            # answers "could any box ever run this", and dropping it would
            # turn a submission that is merely waiting on the drain into a
            # refusal.
            # The live figure is zero on every declared kind, so the two
            # bounded placement preferences -- the only admission readers of
            # another box's offer -- cannot wait for a box that will not
            # claim, and no reader sees admittable capacity that is not there.
            # Publication goes through the ordinary bounded writer: it is the
            # one queue write that reserves nothing, and a poll that cannot
            # publish leaves the old offer to expire rather than admitting on
            # an advertisement nobody can read.
            try:
                loops = len(box_capacity.worker_loops())
            except OSError:
                loops = None
            # Both probes are diagnostics and individually bounded; a failure
            # in either reads as unknown and must not stop the parked offer,
            # which is the one thing this poll exists to publish.
            try:
                addresses = box_capacity.ipv4_addresses()
            except Exception:                                    # noqa: BLE001
                addresses = None
            class_verdict = None
            try:
                observed_images, image_snapshot, class_verdict = image_observation(inventory, class_policy)
            except Exception:                                    # noqa: BLE001
                observed_images = None
                if class_policy is not None:
                    class_verdict = class_policy.evaluate(None, now=time.time())
            drain_fields = drain_offer_fields(gate)
            # What the last open poll declared, not a fresh ledger read: the
            # spool figure ``stable_host_capacity`` holds up survives the gate
            # closing, and this capacity still comes from that declaration --
            # the drain branch's own saved-finish retry (#1403) reads
            # ``claimed/``, but it changes nothing this offer declares.  The
            # declared capability is what ``placeable`` answers from, so
            # preserving it keeps a waiting submission queueable; the zero live
            # figure below is what stops any admission reading this box as
            # admittable.
            drain_capacity = dict(last_declared)

            def announce_drain(queue=queue, host=host, tags=offered,
                               has_gpu=gpu_capable, capacity=drain_capacity,
                               runtime_commit=loaded_commit, cpu_tiers=cpu_tiers,
                               timeout_s=args.timeout_s, loops=loops,
                               addresses=addresses, observed_images=observed_images,
                               class_verdict=class_verdict, fields=drain_fields):
                """The exact parked record this poll offers the queue.

                A closure, not a kwargs dict, so the publisher's child runs
                the ordinary :meth:`PoolQueue.announce` with the fields this
                poll computed -- one publication path, like the open-gate
                offer the loop publishes when the gate reopens.
                """

                queue.announce(
                    host=host, tags=tags, has_gpu=has_gpu,
                    capacity=capacity,
                    observed_capacity={kind: 0 for kind in capacity},
                    runtime_commit=runtime_commit, cpu_tiers=cpu_tiers,
                    loops=loops, timeout_ceiling_s=timeout_s,
                    progress_contracts=[pb.PROGRESS_RECORD_SCHEMA_V1],
                    addresses=addresses, observed_images=observed_images,
                    **({"container_class_verdict": class_verdict} if class_verdict is not None else {}),
                    **fields)

            publication = publish_offer(
                announce_drain, budget_s=OFFER_PUBLISH_TIMEOUT_S,
                retry_s=OFFER_PUBLISH_RETRY_S, abandoned=abandoned_publishers)
            if publication.status != "published":
                identity = (f" retained writer pid={publication.retained[0]}"
                            f" starttime={publication.retained[1]}"
                            if publication.retained else "")
                print(f"[{host}] drain offer publication {publication.status} "
                      f"after {publication.elapsed_s:g}s "
                      f"({publication.error or 'no reason'}; result unknown"
                      f"{identity}); the previous offer is left to expire",
                      flush=True)
            if args.once:
                return 0
            time.sleep(args.poll_s)
            continue
        if not observer_initialized:
            observer = (None if args.assume_idle and not gpu_capable else
                        box_capacity.CapacityObserver(
                            samples=args.observe_samples,
                            ledger_total=queue.ledger().capacity(),
                            mem_roof=(_ram_tier_mem_roof
                                      if args.mem_gb_ram_tier_roof else None),
                        ))
            observer_initialized = True
        gpu_sample = box_capacity.trusted_gpu_sample() if gpu_capable else None
        declared, capacity, known_physical_gpus = observed_claim_capacity(
            queue, args=args, base_declared=base_declared, observer=observer,
            gpu_sample=gpu_sample, known_physical_gpus=known_physical_gpus)
        placeable_capacity = stable_host_capacity(declared, base_declared)
        # The declaration a drain will carry forward if the gate closes
        # before the next poll (#1204).
        last_declared = placeable_capacity

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
        # The shared producer passes the bound held method so observation
        # brackets a sibling release; a snapshot cannot be counted twice.
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
        observed_images, _image_snapshot, class_verdict = image_observation(inventory, class_policy)
        # The interpreter answers (#1263): exactly the paths this poll's ready
        # snapshot asks about, statted on this box.  One bounded set per poll;
        # a snapshot that could not be read publishes nothing, which is the
        # fail-closed answer rather than a guess.
        offered_interpreters, absent_interpreters = interpreter_lookup(
            discovery.snapshot or [])
        dependency_answers = local_dependency_lookup(discovery.snapshot or [], python=args.python)
        # And the digest-contract answers (#1495): the requirement paths this
        # poll's ready snapshot asks about, statted on this box.
        offered_dependencies, absent_dependencies = dependency_lookup(
            discovery.snapshot or [])

        # Immutable producer/source verification is cached; small CAS proof
        # inputs and independent current device identity refresh outside the
        # admission lock. The publisher receives this poll's captured detail.
        observed_detail = scratch_observed_detail(observer, profile_inputs)

        def announce_offer(queue=queue, host=host, tags=offered,
                           has_gpu=gpu_capable, declared=placeable_capacity,
                           capacity=capacity, observer=observer, loops=loops,
                           runtime_commit=loaded_commit, cpu_tiers=cpu_tiers,
                           timeout_s=args.timeout_s, addresses=addresses,
                           observed_images=observed_images, class_verdict=class_verdict,
                           interpreters=offered_interpreters,
                           interpreters_absent=absent_interpreters,
                           dependency_answers=dependency_answers,
                           dependency_files=offered_dependencies,
                           dependency_files_absent=absent_dependencies,
                           observed_detail=observed_detail):
            # (``interpreters`` binds the poll's lookup; the announce call
            # below receives it under that closure-local name.)
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
                observed_detail=observed_detail,
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
                **({"container_class_verdict": class_verdict} if class_verdict is not None else {}),
                # Which named interpreters this box can positively run
                # (#1263): the paths this poll's ready items ask about, statted
                # here.  Absent says it could not answer, and an item naming
                # an interpreter reads that as unknown, never capable.
                interpreters=interpreters,
                interpreters_absent=interpreters_absent,
                local_dependency_answers=dependency_answers,
                dependency_files=dependency_files,
                dependency_files_absent=dependency_files_absent,
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
        # The used-filesystem floor (#1483).  The tick refreshes the bindings
        # this box owns and reaps dead growth holders, in every mode, so a
        # binding is freshly sampled before anyone enforces it.  The check
        # then covers what this box writes without an allowance of its own
        # -- the queue, the CAS, its checkouts and logs -- and under
        # ``enforce`` a filesystem below its floor skips this poll's
        # admission, like an unpublished offer: nothing is claimed, nothing
        # fails.  Under ``off`` (the default) the check reads only the mode;
        # the tick still refreshes any binding this box owns, so ``status``
        # is real before anyone enforces.  Before the claim-time handshake,
        # so a refresh never widens its window, and exception-isolated like
        # the ticks above.
        floor_open = True
        try:
            floor_root = getattr(queue, "root", None)
            if floor_root is not None:
                filesystem_floor.loop_tick(floor_root, label=f"worker_loop {host}")
                floor_open = filesystem_floor.host_admission(
                    floor_root, [floor_root, SH / "cas", pool.LOCAL_CHECKOUT_ROOT],
                    label=f"worker_loop {host}")
        except Exception as exc:                                 # noqa: BLE001
            print(f"[{host}] filesystem floor skipped this poll: "
                  f"{type(exc).__name__}: {exc}", flush=True)
        if not floor_open:
            print(f"[{host}] used filesystem below its floor; skipping admission "
                  f"this poll", flush=True)
            idle += 1
            if args.once:
                return 1
            time.sleep(args.poll_s)
            continue

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
        claim_images = claim_snapshot = None
        if container_images.required_from_items(discovery.snapshot):
            claim_images, claim_snapshot, _class_verdict = image_observation(
                inventory, class_policy, max_age_s=container_images.CLAIM_FRESHNESS_S)
        try:
            outcome = queue.serve_once(
                tags=offered, has_gpu=gpu_capable, python=args.python,
                timeout_s=args.timeout_s, capacity=capacity, cpu_tiers=cpu_tiers,
                adaptive_cpu=not args.assume_idle, containment=True,
                ready=discovery.snapshot, observed_images=claim_images,
                observed_external_gib=(
                    observer.last_offer_external_gib if observer is not None else 0),
                **({"container_class_policy": class_policy,
                    "container_inventory": claim_snapshot} if class_policy is not None else {}),
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
        if on_outcome is not None:
            # Structured result seam for the one-shot adapter; never recover
            # admission/outcome evidence by parsing the diagnostic stream.
            on_outcome(outcome)
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
