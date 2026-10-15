"""Shared role operations; serving loops own their completion policies.

Entrypoints prepend their generation's src directory before importing this
module, so its source root and immutable receipt name the tool's loaded tree.
Marker bytes belong to park_proof; patch that owner to observe marker naming,
because imported helper bindings here do not replace its internal calls.
"""
from contextlib import contextmanager
import fcntl
import os
from pathlib import Path
import signal
import socket
import stat
import threading
import time

from . import adaptive_cpu, core as pb, generation_receipt, park_proof
from .park_proof import drain_offer_fields
from .digest_primitives import raw_sha256

GENERATION_VERSION = Path(__file__).resolve().parents[2] / "RUNTIME_VERSION.json"
RUNTIME_VERSION = Path("/mnt/shared/prismabuild-fleet/repo/RUNTIME_VERSION.json")
MAINTENANCE_GATE = Path(os.environ.get("PRISMABUILD_MAINTENANCE_GATE")
                        or "/run/prismabuild/maintenance.json")
PARKED_ROOT = MAINTENANCE_GATE.parent / "rollout" / "parked"
GENERATION_DRIFT_DIR = "generation-drift"
GENERATION_DRIFT_SCHEMA = "prismaquant.prismabuild.generation_drift.v1"


@contextmanager
def stop_requests(*signals: int):
    """Request cooperative stop and restore the caller's signal handlers."""
    stop = threading.Event()
    previous = {}
    try:
        for signum in signals:
            previous[signum] = signal.signal(signum, lambda *_: stop.set())
        yield stop
    finally:
        for signum, handler in reversed(tuple(previous.items())):
            signal.signal(signum, handler)


ROLE_LOCK_ROOT = Path(f"/tmp/prismabuild-roles-{os.getuid()}")
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



def role_lock_directory() -> Path:
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
        directory = role_lock_directory()
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



def read_maintenance_gate() -> dict | None:
    return park_proof.read_gate(MAINTENANCE_GATE)


def post_park_marker(gate: dict) -> Path | None:
    return park_proof.write_park_marker(PARKED_ROOT, gate)



def maintenance_requested() -> bool:
    return read_maintenance_gate() is not None


def loaded_runtime() -> generation_receipt.ReceiptObservation:
    return generation_receipt.observe(GENERATION_VERSION)


def published_runtime() -> generation_receipt.ReceiptObservation:
    return generation_receipt.observe(RUNTIME_VERSION)


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
    digest = raw_sha256(raw)[:8]
    path = (Path(queue_root) / GENERATION_DRIFT_DIR
            / (f"{int(record['unix'] * 1000):013d}-{record['host']}"
               f"-{record['pid']}-{digest}.json"))
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        pb._atomic_publish(path, raw)
    except OSError:
        return None
    return path
