#!/usr/bin/env python3
"""Report, and on request remove, the per-execution litter in a store.

Claims, worker lock files, empty result staging namespaces and killed ingest
payloads accumulate under the shared CAS. This command surveys them without
writing, and removes candidates only during explicitly acknowledged maintenance.

Before --apply --quiescent-store, stop new submissions and drain/stop every
producer sharing this CAS, on every host. Inspect the dry-run candidates and
verify that each candidate claim's checkout is absent on every host where it
could reside; retain claims needed to repair persistent checkouts. Keep the
store quiescent until the sweep finishes. This acknowledgement is an operator
assertion, not an automatically acquired distributed maintenance lock.

A missing local checkout, local /proc descriptors, an available flock, and file
age cannot prove that a remote execution is dead. An age threshold is only a
retention preference. In particular an unlocked worker lock cannot safely be
unlinked while producers may open it: a waiter could acquire an orphan inode.
Local probes add protection against mistakes, but never authorize online GC.

Requests and receipts are records and are never removed. Nonempty local-result
staging namespaces remain available for core.repair_local_result. --cas-root
is required and --apply defaults off.

Two kinds live under the pool queue rather than the CAS, and are surveyed only
when ``--queue-root`` names it (#995).  A *transition lock* is
``transition-locks/<sha256(key)>.lock``; one is a candidate when its key has
a ``done``, ``failed`` or ``withdrawn`` record older than the lease timeout
and no ``ready`` or ``claimed`` entry.  A *residency namespace* is
``residency/<consumer>/`` with its map ``<consumer>.map.json``; one is a
candidate under the same rule when the directory is also empty or gone.  A
*landing record* is ``residency/<consumer>.landing.json`` (#989); one is a
candidate under the same rule alone.  None needs a quiescent store.  A lock
file is removed only through ``posix_lock.retire``, whose protocol keeps
mutual exclusion against a holder racing the sweep, provided every lock
taker runs the post-lock check (``--all-lock-takers-verify`` acknowledges
that).  A namespace or landing record is removed under its consumer's
transition lock after re-reading that the consumer was not queued again,
and a namespace's ``rmdir`` fails on a fragment that landed after the
survey.  An applied queue sweep writes a JSON receipt under
``<queue>/gc-receipts/``.

Canary run namespaces (``pb-canary/<run-id>/``) are a separate root with
their own kind: the canary driver seals one per run, its receipts are CAS
records this tool never touches, and the 24 MiB of staged chunks plus
artifacts would otherwise accumulate with no owner. The root is surveyed
only when ``--canary-root`` names it — the same explicit-root rule as
``--cas-root`` — and a namespace becomes a candidate only when its run
record is a valid canary record naming the namespace, every member is a
recognizable regular file or directory owned by this user and held open by
nobody, and it is either sealed (``canary-result.json``, the driver's last
write) or older than the age backstop. Anything else about a namespace —
an unreadable record, a foreign schema, a symlink, an open descriptor —
keeps it whole.

"""

from __future__ import annotations

import argparse
import errno
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import socket
import stat
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
from runtime_paths import generation_root  # noqa: E402
RUNTIME_ROOT = generation_root(__file__)
sys.path.insert(0, str(RUNTIME_ROOT / "src"))
from collections.abc import Iterable, Mapping  # noqa: E402
from prismabuild import core as pb  # noqa: E402
from prismabuild import pool, posix_lock  # noqa: E402

#: The litter classes, in the order the report prints them.
KIND_CLAIM = "claim"
KIND_LOCK = "lock"
KIND_NAMESPACE = "staging namespace"
KIND_INGEST = "aborted staging copy"
KIND_PRIVATE_INGEST = "private ingest staging"
KIND_CANARY = "canary namespace"
KINDS = (KIND_CLAIM, KIND_LOCK, KIND_NAMESPACE, KIND_INGEST,
         KIND_PRIVATE_INGEST, KIND_CANARY)
#: The two queue-root kinds (#995), surveyed only under ``--queue-root``.
KIND_TRANSITION_LOCK = "transition lock"
KIND_RESIDENCY_NAMESPACE = "residency namespace"
KIND_LANDING_RECORD = "landing record"
#: In removal order: the residency kinds take their consumer's transition
#: lock, so they go before the lock files are retired.
QUEUE_KINDS = (KIND_RESIDENCY_NAMESPACE, KIND_LANDING_RECORD, KIND_TRANSITION_LOCK)
#: ``residency_map.map_path`` and ``residency_map.landing_path``.
MAP_SUFFIX = ".map.json"
LANDING_SUFFIX = ".landing.json"
#: ``pool.PoolQueue._transition_locked`` and ``residency_fragment_root``.
TRANSITION_LOCK_SUBPATH = ("transition-locks",)
RESIDENCY_SUBPATH = (pool.RESIDENCY,)
GC_RECEIPTS_SUBPATH = ("gc-receipts",)
GC_RECEIPT_SCHEMA = "prismabuild.pb_gc.receipt.v1"
PRIVATE_STAGING_PREFIX = "ingest."
PRIVATE_STAGING_OWNER = ".owner.lock"

#: The canary kind's root and records, spelled the way the driver that
#: writes them spells it (``tools/fleet/pbcanary.py``): every run lives
#: under ``<canary-root>/<run-id>/`` with a ``run.json`` registration
#: record stamped once at creation and a ``canary-result.json`` seal
#: written exactly once at the end of every driver path. The schema
#: literal is pinned here rather than imported so a driver change is a
#: failing test (``test_pb_gc`` mints namespaces through the driver's own
#: ``build_run_record``) rather than a sweeper that quietly stops
#: sweeping.
CANARY_RUN_SCHEMA = "prismabuild.pbcanary.run.v1"
CANARY_RUN_RECORD = "run.json"
CANARY_SEAL_RECORD = "canary-result.json"

#: Where each class lives, spelled the way the code that writes it spells it:
#: ``core._local_result_claim_path``, ``core._local_output_lock``,
#: ``core.PrismaBuildCAS.publish_result`` and ``core._copy_to_staging``.
#: ``test_pb_gc`` mints one of each through those functions and asserts this
#: tool finds them, so a layout change is a failing test rather than a sweeper
#: that quietly stops sweeping.
CLAIM_SUBPATH = ("local-results", "v1")
LOCK_SUBPATH = (".worker-locks",)
STAGING_SUBPATH = (".staging",)
NAMESPACE_SUBPATH = (".staging", "local-results")
STAGING_PREFIX = ".payload."
STAGING_SUFFIX = ".tmp"

#: Generous enough that an ordinary campaign action is never near it, small
#: enough to be useful on a store nobody has swept. It is a backstop, not the
#: safety argument; see the module docstring.
DEFAULT_MIN_AGE_HOURS = 24.0

_HEX = frozenset("0123456789abcdef")


class SweepError(Exception):
    """The store cannot be surveyed at all."""


# --------------------------------------------------------------------------
# Reading the store
# --------------------------------------------------------------------------


def _entries(directory: Path) -> list[os.DirEntry[str]]:
    """The directory's entries, empty when it is not there.

    Never creates anything. A store that has never staged a result has no
    ``.staging`` at all, and a sweeper that made one would be writing to the
    store it was asked to report on.
    """

    try:
        # Validate every component, including fixed CAS layout directories.
        # A final-entry nofollow check alone misses a redirected .staging.
        for component in reversed((directory, *directory.parents)):
            if stat.S_ISLNK(os.lstat(component).st_mode):
                raise SweepError(f"refusing symlinked store directory: {component}")
        with os.scandir(directory) as scan:
            return sorted(scan, key=lambda entry: entry.name)
    except FileNotFoundError:
        return []
    except NotADirectoryError:
        return []
    except OSError as exc:
        raise SweepError(f"cannot list {directory}: {exc}") from exc


def _is_digest(name: str) -> bool:
    return len(name) == 64 and set(name) <= _HEX


def _path_state(path: Path) -> str:
    """``present``, ``absent`` or ``undecidable`` for one checkout root.

    ``lstat`` rather than ``exists``: a dangling symlink where a checkout root
    used to be is still something on the filesystem, and this refuses to
    reason about it. Anything but a clean "no such file" is undecidable and
    keeps the claim.
    """

    try:
        os.lstat(path)
    except (FileNotFoundError, NotADirectoryError):
        return "absent"
    except (OSError, ValueError):
        return "undecidable"
    return "present"


def open_inodes(*, skip_pid: int) -> tuple[set[tuple[int, int]], int]:
    """Every ``(st_dev, st_ino)`` a process on this box holds open.

    ``core._process_group_has_live_members`` reads ``/proc`` for the same
    reason: the process table is the only thing that answers "is anything
    still using this" without asking the thing itself.

    Returns the inode set and the number of *this user's* processes that could
    not be inspected, so the report can say how complete the answer is. Only
    this user's are counted: a process owned by somebody else never shows its
    descriptors, and the store is one user's own, which
    ``core._reap_local_result_staging`` already relies on when it refuses a
    staging file with a foreign owner. Counting the other users' processes
    would print the caveat on every run of every box and mean nothing.
    """

    held: set[tuple[int, int]] = set()
    unreadable = 0
    euid = os.geteuid()
    try:
        listing = os.listdir("/proc")
    except OSError:
        return held, -1
    for entry in listing:
        if not entry.isdigit() or int(entry) == skip_pid:
            continue
        directory = f"/proc/{entry}/fd"
        try:
            names = os.listdir(directory)
        except (FileNotFoundError, ProcessLookupError):
            continue  # the process exited while this was reading
        except OSError:
            try:
                if os.stat(f"/proc/{entry}").st_uid == euid:
                    unreadable += 1
            except OSError:
                pass
            continue
        for name in names:
            try:
                info = os.stat(f"{directory}/{name}")
            except OSError:
                continue
            held.add((info.st_dev, info.st_ino))
    return held, unreadable


def output_lock_name(claim: Mapping[str, object]) -> str:
    """The ``.worker-locks`` file name one claim's execution would take.

    This mirrors ``core._local_output_lock``: the identity is the normalized
    physical output path and nothing else. ``core._validate_execution_paths``
    resolves the working directory before joining the result path, so this
    resolves it too. The agreement is pinned by a test that takes the real
    lock and compares the file it created against this name.
    """

    root = Path(str(claim["checkout_root"]))
    working = str(claim["working_directory"])
    cwd = root if working == "." else root / working
    output = Path(os.path.realpath(cwd)) / str(claim["result_path"])
    identity = hashlib.sha256(
        os.path.normpath(str(output)).encode("utf-8")
    ).hexdigest()
    return f"{identity}.lock"


def _count_json(directory: Path) -> int:
    """How many ``*.json`` files live under ``directory``, at any depth."""

    total = 0
    for entry in _entries(directory):
        if entry.is_dir(follow_symlinks=False):
            total += _count_json(Path(entry.path))
        elif entry.name.endswith(".json"):
            total += 1
    return total


# --------------------------------------------------------------------------
# The survey
# --------------------------------------------------------------------------


def _candidate(kind: str, path: Path, info: os.stat_result, why: str) -> dict:
    return {
        "kind": kind,
        "path": path,
        "bytes": info.st_size if stat.S_ISREG(info.st_mode) else 0,
        "age_s": max(0.0, time.time() - info.st_mtime),
        "identity": (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns),
        "why": why,
    }


def _retain(kind: str, path: Path, why: str) -> dict:
    return {"kind": kind, "path": path, "why": why}


def _survey_claims(cas_root: Path, *, min_age_s: float) -> dict:
    """Classify every claim, and collect what live claims protect.

    The protected sets are built from every live claim before any lock or
    namespace is judged, because a claim protects a lock whose own file says
    nothing about which action owns it.
    """

    root = cas_root.joinpath(*CLAIM_SUBPATH)
    remove: list[dict] = []
    keep: list[dict] = []
    protected_locks: set[str] = set()
    protected_namespaces: set[str] = set()
    scanned = 0
    for shard in _entries(root):
        if not shard.is_dir(follow_symlinks=False):
            keep.append(_retain(KIND_CLAIM, Path(shard.path), "unexpected entry"))
            continue
        for entry in _entries(Path(shard.path)):
            path = Path(entry.path)
            if not entry.name.endswith(".json"):
                keep.append(_retain(KIND_CLAIM, path, "unexpected entry"))
                continue
            scanned += 1
            try:
                info = os.lstat(path)
            except OSError as exc:
                keep.append(_retain(KIND_CLAIM, path, f"cannot inspect: {exc}"))
                continue
            if not stat.S_ISREG(info.st_mode):
                keep.append(_retain(KIND_CLAIM, path, "not a regular file"))
                continue
            try:
                claim = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError) as exc:
                keep.append(_retain(KIND_CLAIM, path, f"unreadable: {exc}"))
                continue
            if (
                not isinstance(claim, Mapping)
                or claim.get("schema") != pb.LOCAL_RESULT_CLAIM_SCHEMA_V1
                or not isinstance(claim.get("checkout_root"), str)
                or not isinstance(claim.get("working_directory"), str)
                or not isinstance(claim.get("result_path"), str)
            ):
                keep.append(_retain(KIND_CLAIM, path, "not a local result claim"))
                continue
            state = _path_state(Path(str(claim["checkout_root"])))
            if state != "absent":
                # A live or unreadable root keeps the claim, and keeps
                # everything the claim owns: the lock on its output path and
                # the staging namespace named by its digest.
                protected_locks.add(output_lock_name(claim))
                protected_namespaces.add(path.stem)
                keep.append(_retain(
                    KIND_CLAIM, path,
                    "checkout root is present" if state == "present"
                    else "checkout root could not be inspected",
                ))
                continue
            age = max(0.0, time.time() - info.st_mtime)
            if age < min_age_s:
                protected_locks.add(output_lock_name(claim))
                protected_namespaces.add(path.stem)
                keep.append(_retain(
                    KIND_CLAIM, path, "younger than the age backstop"))
                continue
            remove.append(_candidate(
                KIND_CLAIM, path, info,
                f"checkout root is gone: {claim['checkout_root']}",
            ))
    return {
        "scanned": scanned,
        "remove": remove,
        "keep": keep,
        "protected_locks": protected_locks,
        "protected_namespaces": protected_namespaces,
    }


def _probe_lock(path: Path) -> str:
    """``""`` when nothing holds this lock, else why it is held.

    Opened read-write but never created or written. Linux NFS implements
    ``flock`` with byte-range locks, requiring a writable descriptor for an
    exclusive lock. Taking the lock non-blocking preserves active owners.
    """

    flags = os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK | getattr(os, "O_CLOEXEC", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        return f"cannot open: {exc}"
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            return "not a regular ownership lock"
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return "held by a live local action"
        except OSError as exc:
            return f"cannot probe ownership lock: {exc}"
        fcntl.flock(descriptor, fcntl.LOCK_UN)
    finally:
        os.close(descriptor)
    return ""


def _survey_locks(
    cas_root: Path,
    *,
    min_age_s: float,
    protected: set[str],
    held_inodes: set[tuple[int, int]],
) -> dict:
    root = cas_root.joinpath(*LOCK_SUBPATH)
    remove: list[dict] = []
    keep: list[dict] = []
    scanned = 0
    for entry in _entries(root):
        path = Path(entry.path)
        if not (entry.name.endswith(".lock") and _is_digest(entry.name[:-5])):
            keep.append(_retain(KIND_LOCK, path, "unexpected entry"))
            continue
        scanned += 1
        if entry.name in protected:
            keep.append(_retain(
                KIND_LOCK, path, "output path of a live local result claim"))
            continue
        try:
            info = os.lstat(path)
        except OSError as exc:
            keep.append(_retain(KIND_LOCK, path, f"cannot inspect: {exc}"))
            continue
        if not stat.S_ISREG(info.st_mode):
            keep.append(_retain(KIND_LOCK, path, "not a regular file"))
            continue
        # The flock is asked first because it is the specific answer: a
        # process inside ``_local_output_lock`` holds it for the whole action.
        # The open-inode reading is the broader one and catches a producer
        # between its ``os.open`` and its ``flock``, which holds no lock yet.
        held = _probe_lock(path)
        if held:
            keep.append(_retain(KIND_LOCK, path, held))
            continue
        if (info.st_dev, info.st_ino) in held_inodes:
            keep.append(_retain(KIND_LOCK, path, "open in another process"))
            continue
        # An ``O_CREAT`` open of an existing lock does not move its mtime, so
        # this is creation age: how long ago the first action to declare that
        # output path took the lock.
        if max(0.0, time.time() - info.st_mtime) < min_age_s:
            keep.append(_retain(KIND_LOCK, path, "younger than the age backstop"))
            continue
        remove.append(_candidate(
            KIND_LOCK, path, info, "no live claim, no holder"))
    return {"scanned": scanned, "remove": remove, "keep": keep}


def _survey_namespaces(
    cas_root: Path, *, min_age_s: float, protected: set[str]
) -> tuple[dict, int]:
    root = cas_root.joinpath(*NAMESPACE_SUBPATH)
    remove: list[dict] = []
    keep: list[dict] = []
    scanned = 0
    occupied = 0
    for entry in _entries(root):
        path = Path(entry.path)
        if not (entry.is_dir(follow_symlinks=False) and _is_digest(entry.name)):
            keep.append(_retain(KIND_NAMESPACE, path, "unexpected entry"))
            continue
        scanned += 1
        if entry.name in protected:
            keep.append(_retain(
                KIND_NAMESPACE, path, "named by a live local result claim"))
            continue
        contents = _entries(path)
        if contents:
            # A staged payload under a dead claim is a killed publication.
            # ``core.repair_local_result`` is what removes one, under the
            # output lock and with the ownership checks this tool does not
            # have, so it is reported and left alone.
            occupied += 1
            keep.append(_retain(
                KIND_NAMESPACE, path,
                f"holds {len(contents)} staged files; clear it with "
                "core.repair_local_result",
            ))
            continue
        try:
            info = os.lstat(path)
        except OSError as exc:
            keep.append(_retain(KIND_NAMESPACE, path, f"cannot inspect: {exc}"))
            continue
        if max(0.0, time.time() - info.st_mtime) < min_age_s:
            keep.append(_retain(
                KIND_NAMESPACE, path, "younger than the age backstop"))
            continue
        remove.append(_candidate(
            KIND_NAMESPACE, path, info, "empty, and no live claim carries it"))
    return {"scanned": scanned, "remove": remove, "keep": keep}, occupied


def _survey_ingests(
    cas_root: Path, *, min_age_s: float, held_inodes: set[tuple[int, int]]
) -> dict:
    root = cas_root.joinpath(*STAGING_SUBPATH)
    remove: list[dict] = []
    keep: list[dict] = []
    scanned = 0
    for entry in _entries(root):
        path = Path(entry.path)
        if (entry.name == NAMESPACE_SUBPATH[-1]
                or entry.name.startswith(PRIVATE_STAGING_PREFIX)):
            continue  # the namespace container, surveyed above
        if not (entry.name.startswith(STAGING_PREFIX)
                and entry.name.endswith(STAGING_SUFFIX)):
            keep.append(_retain(KIND_INGEST, path, "unexpected entry"))
            continue
        scanned += 1
        try:
            info = os.lstat(path)
        except OSError as exc:
            keep.append(_retain(KIND_INGEST, path, f"cannot inspect: {exc}"))
            continue
        if not stat.S_ISREG(info.st_mode):
            keep.append(_retain(KIND_INGEST, path, "not a regular file"))
            continue
        if (info.st_dev, info.st_ino) in held_inodes:
            keep.append(_retain(KIND_INGEST, path, "open in another process"))
            continue
        if max(0.0, time.time() - info.st_mtime) < min_age_s:
            keep.append(_retain(KIND_INGEST, path, "younger than the age backstop"))
            continue
        # ``_copy_to_staging`` chmods to 0o444 only once the whole copy landed,
        # so the mode says which half died: 0o600 never finished copying,
        # 0o444 copied and never published.
        finished = "copied, never published" if info.st_mode & 0o444 == 0o444 \
            else "copy never finished"
        remove.append(_candidate(
            KIND_INGEST, path, info, f"nothing holds it open; {finished}"))
    return {"scanned": scanned, "remove": remove, "keep": keep}


def _survey_private_ingests(
    cas_root: Path, *, min_age_s: float, held_inodes: set[tuple[int, int]],
) -> dict:
    remove, keep = [], []
    scanned = 0
    for entry in _entries(cas_root / ".staging"):
        if not entry.name.startswith(PRIVATE_STAGING_PREFIX):
            continue
        scanned += 1
        path = Path(entry.path)
        if not entry.is_dir(follow_symlinks=False):
            keep.append(_retain(KIND_PRIVATE_INGEST, path, "not a real directory"))
            continue
        try:
            info = path.lstat()
        except OSError as exc:
            keep.append(_retain(KIND_PRIVATE_INGEST, path, f"cannot inspect: {exc}"))
            continue
        members = {}
        reason = ""
        for member in _entries(path):
            try:
                member_info = member.stat(follow_symlinks=False)
            except OSError:
                reason = "ingest contents changed while surveying"
                break
            if (not stat.S_ISREG(member_info.st_mode)
                    or member_info.st_uid != os.geteuid()
                    or not (member.name == PRIVATE_STAGING_OWNER
                            or (member.name.startswith(STAGING_PREFIX)
                                and member.name.endswith(STAGING_SUFFIX)))):
                reason = "unrecognized or foreign-owned ingest contents"
                break
            if (member_info.st_dev, member_info.st_ino) in held_inodes:
                reason = "open in another process"
                break
            members[member.name] = _identity(member_info)
        if not reason and PRIVATE_STAGING_OWNER not in members:
            reason = "missing ingest ownership lock"
        if not reason:
            reason = _probe_lock(path / PRIVATE_STAGING_OWNER)
        if not reason and time.time() - info.st_mtime < min_age_s:
            reason = "younger than the age backstop"
        if reason:
            keep.append(_retain(KIND_PRIVATE_INGEST, path, reason))
            continue
        row = _candidate(KIND_PRIVATE_INGEST, path, info,
                         "private ingest ownership lock is available")
        row["members"] = members
        row["bytes"] = sum(identity[2] for identity in members.values())
        remove.append(row)
    return {"scanned": scanned, "remove": remove, "keep": keep}


def _canary_members(
    namespace: Path, held_inodes: set[tuple[int, int]],
) -> tuple[dict, str, int]:
    """Walk one canary namespace; return ``(members, refusal, bytes)``.

    Refuses anything the canary driver does not write: a symlink, a
    non-regular file, a foreign-owned member, or a member another
    process on this box holds open. ``members`` maps slash-relative
    names to identities, mirroring the private-ingest candidate rows;
    directory identities are advisory only, because a directory's size
    and mtime legitimately change as its children are removed.
    """

    members: dict[str, tuple] = {}
    total = 0
    stack: list[tuple[str, Path]] = [("", namespace)]
    while stack:
        relative, directory = stack.pop()
        for member in _entries(directory):
            name = f"{relative}/{member.name}" if relative else member.name
            try:
                info = member.stat(follow_symlinks=False)
            except OSError as exc:
                return {}, f"namespace changed while surveying: {exc}", 0
            if not (stat.S_ISREG(info.st_mode) or stat.S_ISDIR(info.st_mode)):
                return {}, f"unrecognized member: {name} is not a regular file or directory", 0
            if info.st_uid != os.geteuid():
                return {}, f"unrecognized member: {name} is not this user's", 0
            if stat.S_ISREG(info.st_mode):
                if (info.st_dev, info.st_ino) in held_inodes:
                    return {}, "open in another process", 0
                total += info.st_size
            members[name] = _identity(info)
            if stat.S_ISDIR(info.st_mode):
                stack.append((name, Path(member.path)))
    return members, "", total


def _canary_sealed(namespace: Path) -> tuple[bool | None, str]:
    """``(True, "")`` sealed, ``(False, "")`` not, ``(None, why)`` undecidable.

    The seal is the driver's own terminal write: the existence of a
    readable ``canary-result.json`` means every driver path finished.
    Anything undecidable about the file keeps the namespace.
    """

    seal = namespace / CANARY_SEAL_RECORD
    try:
        info = os.lstat(seal)
    except FileNotFoundError:
        return False, ""
    except OSError as exc:
        return None, f"cannot inspect the seal record: {exc}"
    if not stat.S_ISREG(info.st_mode):
        return None, "seal record is not a regular file"
    try:
        record = json.loads(seal.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        return None, f"unreadable seal record: {exc}"
    if not isinstance(record, Mapping):
        return None, "seal record is not a JSON object"
    return True, ""


def _survey_canary(
    canary_root: Path, *, min_age_s: float,
    held_inodes: set[tuple[int, int]],
) -> dict:
    """Classify every canary run namespace under one root.

    A namespace is removable only when its registration record is a
    valid canary record naming the namespace, every member is
    recognizable, and it is sealed or older than the age backstop. The
    chunks and artifacts inside are per-run droppings; the receipts the
    run refers to are CAS records and stay.
    """

    remove: list[dict] = []
    keep: list[dict] = []
    scanned = 0
    for entry in _entries(canary_root):
        path = Path(entry.path)
        if not entry.is_dir(follow_symlinks=False):
            keep.append(_retain(KIND_CANARY, path, "unexpected entry"))
            continue
        scanned += 1
        try:
            info = os.lstat(path)
        except OSError as exc:
            keep.append(_retain(KIND_CANARY, path, f"cannot inspect: {exc}"))
            continue
        try:
            record = json.loads(
                (path / CANARY_RUN_RECORD).read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            keep.append(_retain(
                KIND_CANARY, path, f"unreadable run record: {exc}"))
            continue
        if (not isinstance(record, Mapping)
                or record.get("schema") != CANARY_RUN_SCHEMA
                or not isinstance(record.get("run_id"), str)):
            keep.append(_retain(KIND_CANARY, path, "not a canary run record"))
            continue
        if record["run_id"] != entry.name:
            keep.append(_retain(
                KIND_CANARY, path, "run record does not name this namespace"))
            continue
        members, why, total = _canary_members(path, held_inodes)
        if why:
            keep.append(_retain(KIND_CANARY, path, why))
            continue
        if max(0.0, time.time() - info.st_mtime) < min_age_s:
            keep.append(_retain(
                KIND_CANARY, path, "younger than the age backstop"))
            continue
        sealed, seal_why = _canary_sealed(path)
        if sealed is None:
            keep.append(_retain(KIND_CANARY, path, seal_why))
            continue
        row = _candidate(
            KIND_CANARY, path, info,
            "sealed canary run; its CAS receipts are records and stay" if sealed
            else "no seal and older than the age backstop: a driver run "
                 "that died mid-run",
        )
        row["bytes"] = total
        row["members"] = members
        remove.append(row)
    return {"scanned": scanned, "remove": remove, "keep": keep}


def _remove_private_ingest(row: Mapping[str, object]) -> str:
    path = Path(str(row["path"]))
    parent_fd = directory_fd = owner_fd = None
    try:
        parent_fd = pb._open_directory_nofollow(path.parent, where="GC parent")
        directory_fd = os.open(path.name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                               dir_fd=parent_fd)
        if _identity(os.fstat(directory_fd)) != row["identity"]:
            return "replaced or changed during the sweep"
        owner_fd = os.open(PRIVATE_STAGING_OWNER, os.O_RDWR | os.O_NOFOLLOW
                           | os.O_NONBLOCK, dir_fd=directory_fd)
        fcntl.flock(owner_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        members = row["members"]
        if set(os.listdir(directory_fd)) != set(members):
            return "ingest contents changed during the sweep"
        for name, expected in members.items():
            observed = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
            if _identity(observed) != expected:
                return "ingest contents replaced during the sweep"
        if _identity(os.fstat(owner_fd)) != members[PRIVATE_STAGING_OWNER]:
            return "ingest ownership lock replaced during the sweep"
        for name in members:
            if name != PRIVATE_STAGING_OWNER:
                os.unlink(name, dir_fd=directory_fd)
        os.unlink(PRIVATE_STAGING_OWNER, dir_fd=directory_fd)
        # NFS may rename an open unlinked owner to .nfs*. Closing the last
        # descriptor completes that deletion before the directory is removed.
        # All payloads are gone and the caller maintains fleet quiescence.
        os.close(owner_fd)
        owner_fd = None
        os.rmdir(path.name, dir_fd=parent_fd)
    except (OSError, pb.PrismaBuildError) as exc:
        return f"cannot remove ingest: {exc}"
    finally:
        for descriptor in (owner_fd, directory_fd, parent_fd):
            if descriptor is not None:
                os.close(descriptor)
    return ""


def survey(
    cas_root: Path,
    *,
    min_age_s: float = DEFAULT_MIN_AGE_HOURS * 3600.0,
    held_inodes: set[tuple[int, int]] | None = None,
    unreadable_processes: int = 0,
    canary_root: Path | None = None,
) -> dict:
    """Classify every per-execution dropping in one store.

    Reads only. The caller supplies the ``/proc`` answer so the sweep can take
    a fresh one immediately before it removes anything. ``canary_root``
    optionally adds the canary run namespaces under a second explicitly
    named root (issue #690); when it is None the kind surveys nothing.
    """

    if not math.isfinite(min_age_s) or min_age_s < 0:
        raise SweepError("minimum age must be finite and nonnegative")
    cas_root = Path(cas_root)
    if not cas_root.is_dir():
        raise SweepError(f"not a CAS root: {cas_root}")
    canary_root = Path(canary_root) if canary_root is not None else None
    if canary_root is not None and not canary_root.is_dir():
        raise SweepError(f"not a canary root: {canary_root}")
    if held_inodes is None:
        held_inodes, unreadable_processes = open_inodes(skip_pid=os.getpid())
    claims = _survey_claims(cas_root, min_age_s=min_age_s)
    locks = _survey_locks(
        cas_root, min_age_s=min_age_s,
        protected=claims["protected_locks"], held_inodes=held_inodes,
    )
    namespaces, occupied = _survey_namespaces(
        cas_root, min_age_s=min_age_s, protected=claims["protected_namespaces"]
    )
    ingests = _survey_ingests(
        cas_root, min_age_s=min_age_s, held_inodes=held_inodes)
    sections = {
        KIND_CLAIM: claims,
        KIND_LOCK: locks,
        KIND_NAMESPACE: namespaces,
        KIND_INGEST: ingests,
        KIND_PRIVATE_INGEST: _survey_private_ingests(
            cas_root, min_age_s=min_age_s, held_inodes=held_inodes),
        KIND_CANARY: _survey_canary(
            canary_root, min_age_s=min_age_s, held_inodes=held_inodes
        ) if canary_root is not None
        else {"scanned": 0, "remove": [], "keep": []},
    }
    requests = _count_json(cas_root / "requests")
    receipts = _count_json(cas_root / "actions")
    return {
        "cas_root": cas_root,
        "canary_root": canary_root,
        "sections": sections,
        "remove": [row for kind in KINDS for row in sections[kind]["remove"]],
        "keep": [row for kind in KINDS for row in sections[kind]["keep"]],
        "diagnostics": {
            "requests": requests,
            "receipts": receipts,
            "requests_without_a_receipt": max(0, requests - receipts),
            "occupied_staging_namespaces": occupied,
            "unreadable_processes": unreadable_processes,
            "canary_root": str(canary_root) if canary_root is not None else None,
        },
    }


# --------------------------------------------------------------------------
# The sweep
# --------------------------------------------------------------------------


def _remove_lock(path: Path, identity: tuple) -> str:
    """Unlink one lock file while holding its own exclusive lock.

    The lock is taken first so a producer already inside ``_local_output_lock``
    keeps the file, and the inode is re-identified through the directory
    descriptor so what is unlinked is what was locked.
    """

    try:
        directory_fd = pb._open_directory_nofollow(path.parent, where="GC parent")
    except (OSError, pb.PrismaBuildError) as exc:
        return f"cannot open {path.parent}: {exc}"
    try:
        flags = os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK | getattr(os, "O_CLOEXEC", 0)
        try:
            descriptor = os.open(path.name, flags, dir_fd=directory_fd)
        except FileNotFoundError:
            return "already gone"
        except OSError as exc:
            return f"cannot open: {exc}"
        try:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return "taken by a live local action during the sweep"
            except OSError as exc:
                return f"cannot acquire ownership lock: {exc}"
            locked = os.fstat(descriptor)
            if _identity(locked) != identity:
                return "replaced or changed during the sweep"
            named = os.stat(path.name, dir_fd=directory_fd, follow_symlinks=False)
            if (locked.st_dev, locked.st_ino) != (named.st_dev, named.st_ino):
                return "replaced during the sweep"
            os.unlink(path.name, dir_fd=directory_fd)
        except OSError as exc:
            return f"cannot remove: {exc}"
        finally:
            os.close(descriptor)
    finally:
        os.close(directory_fd)
    return ""


def _identity(info: os.stat_result) -> tuple:
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns


def _remove_canary(row: Mapping[str, object]) -> str:
    """Remove one canary namespace, deepest member first, by identity.

    The namespace identity and the complete member map are re-verified
    against the plan before anything is unlinked, and each member is
    re-identified as it is removed. Directory identities compare by
    ``(dev, ino)`` only: a directory's size and mtime legitimately move
    while its children are being removed.
    """

    path = Path(str(row["path"]))
    try:
        if _identity(path.lstat()) != row["identity"]:
            return "replaced or changed during the sweep"
        observed, why, _total = _canary_members(path, set())
        if why or observed != row["members"]:
            return "namespace changed during the sweep"
        deepest_first = sorted(
            observed, key=lambda name: (-name.count("/"), name))
        for name in deepest_first:
            member = path / name
            info = member.lstat()
            identity = observed[name]
            if (info.st_dev, info.st_ino) != (identity[0], identity[1]):
                return "namespace changed during the sweep"
            if stat.S_ISDIR(info.st_mode):
                os.rmdir(member)
            else:
                if _identity(info) != identity:
                    return "namespace changed during the sweep"
                os.unlink(member)
        os.rmdir(path)
    except FileNotFoundError:
        return "already gone"
    except OSError as exc:
        return f"cannot remove canary namespace: {exc}"
    return ""


def _remove(row: Mapping[str, object]) -> str:
    """Remove the inspected inode through a descriptor anchored below the CAS."""

    path = Path(str(row["path"]))
    kind = str(row["kind"])
    if kind == KIND_LOCK:
        return _remove_lock(path, row["identity"])
    if kind == KIND_PRIVATE_INGEST:
        return _remove_private_ingest(row)
    if kind == KIND_CANARY:
        return _remove_canary(row)
    try:
        directory_fd = pb._open_directory_nofollow(path.parent, where="GC parent")
    except (OSError, pb.PrismaBuildError) as exc:
        return f"cannot open parent: {exc}"
    try:
        info = os.stat(path.name, dir_fd=directory_fd, follow_symlinks=False)
        if _identity(info) != row["identity"]:
            return "replaced or changed during the sweep"
        if kind == KIND_NAMESPACE:
            os.rmdir(path.name, dir_fd=directory_fd)
        else:
            os.unlink(path.name, dir_fd=directory_fd)
    except FileNotFoundError:
        return "already gone"
    except OSError as exc:
        return f"cannot remove: {exc}"
    finally:
        os.close(directory_fd)
    return ""


def sweep(
    plan: Mapping[str, object], *, min_age_s: float, quiescent_store: bool = False,
) -> dict:
    """Remove candidates during an operator-established fleet maintenance window.

    Re-surveying adds a local race check; it cannot establish fleet quiescence.
    The caller must stop producers and verify remote checkout absence first.
    """

    if not quiescent_store:
        raise SweepError("removal requires an acknowledged quiescent store")
    held, unreadable = open_inodes(skip_pid=os.getpid())
    recheck = survey(
        Path(str(plan["cas_root"])), min_age_s=min_age_s,
        held_inodes=held, unreadable_processes=unreadable,
        canary_root=plan.get("canary_root"),
    )
    still_dead = {str(row["path"]): row["identity"] for row in recheck["remove"]}
    removed: list[dict] = []
    skipped: list[tuple[dict, str]] = []
    for row in plan["remove"]:  # type: ignore[index]
        if still_dead.get(str(row["path"])) != row["identity"]:
            skipped.append((dict(row), "became live or changed between the plan and the sweep"))
            continue
        why = _remove(row)
        if why:
            skipped.append((dict(row), why))
            continue
        removed.append(dict(row))
    return {"removed": removed, "skipped": skipped}


# --------------------------------------------------------------------------
# The queue kinds (#995)
# --------------------------------------------------------------------------


def transition_lock_name(action_key: str) -> str:
    """The ``transition-locks`` file one key's transitions take.

    Mirrors ``pool.PoolQueue._transition_locked``; ``test_pb_gc_queue``
    takes the real lock and compares the file it created with this name.
    """

    return hashlib.sha256(str(action_key).encode()).hexdigest() + ".lock"


def _live_key(name: str) -> str | None:
    """The key a ``ready``/``claimed`` entry keeps live, or ``None``.

    Any entry whose name starts with a key -- the row, its lease, a
    tombstone or a late-finish file -- makes that key live: a transition may
    be under way.  The survey and the re-check under the key's lock both
    decide liveness with this one rule.
    """

    return name[:64] if _is_digest(name[:64]) else None


def _key_live_now(queue_root: Path, key: str) -> bool:
    """Whether ``key`` is live, re-read now by :func:`_live_key`'s rule."""

    return any(_live_key(entry.name) == key
               for state in (pool.READY, pool.CLAIMED)
               for entry in _entries(queue_root / state))


def queue_key_states(queue_root: Path) -> tuple[set[str], dict[str, float]]:
    """Keys with a live entry, and each terminal key's newest record mtime.

    One listing per state directory; :func:`_live_key` decides which key an
    entry keeps live.
    """

    live: set[str] = set()
    for state in (pool.READY, pool.CLAIMED):
        for entry in _entries(queue_root / state):
            key = _live_key(entry.name)
            if key is not None:
                live.add(key)
    terminal: dict[str, float] = {}
    for state in (pool.DONE, pool.FAILED, pool.WITHDRAWN):
        for entry in _entries(queue_root / state):
            key = entry.name[:-5]
            if not (entry.name.endswith(".json") and _is_digest(key)):
                continue
            try:
                mtime = entry.stat(follow_symlinks=False).st_mtime
            except OSError:
                continue
            terminal[key] = max(terminal.get(key, 0.0), mtime)
    return live, terminal


def _survey_transition_locks(queue_root: Path, *, live: set[str],
                             terminal: Mapping[str, float], now: float,
                             grace_s: float) -> dict:
    root = queue_root.joinpath(*TRANSITION_LOCK_SUBPATH)
    by_name = {transition_lock_name(key): key for key in terminal}
    live_names = {transition_lock_name(key) for key in live}
    remove: list[dict] = []
    keep: list[dict] = []
    scanned = 0
    for entry in _entries(root):
        path = Path(entry.path)
        if not (entry.name.endswith(".lock") and _is_digest(entry.name[:-5])):
            # Includes an NFS ``.nfs*`` silly-rename of a lock being retired.
            keep.append(_retain(KIND_TRANSITION_LOCK, path, "unexpected entry"))
            continue
        scanned += 1
        if entry.name in live_names:
            keep.append(_retain(KIND_TRANSITION_LOCK, path,
                                "its key is ready or claimed"))
            continue
        key = by_name.get(entry.name)
        if key is None:
            keep.append(_retain(KIND_TRANSITION_LOCK, path,
                                "no terminal record names its key"))
            continue
        age = max(0.0, now - terminal[key])
        if age < grace_s:
            keep.append(_retain(KIND_TRANSITION_LOCK, path,
                                "terminal for less than the lease timeout"))
            continue
        remove.append({"kind": KIND_TRANSITION_LOCK, "path": path, "key": key,
                       "bytes": 0, "age_s": age,
                       "why": "its key is terminal and nothing is queued for it"})
    return {"scanned": scanned, "remove": remove, "keep": keep}


def _residency_entries(root: Path) -> dict[str, dict[str, Path]]:
    """Each consumer's residency state under ``residency/``, by kind.

    A consumer's namespace is its fragment directory ``<consumer>/`` and the
    composed map ``<consumer>.map.json`` beside it
    (``residency_map.map_path``); its landing record is
    ``<consumer>.landing.json`` (``residency_map.landing_path``, #989).  The
    produced-output subtrees under the same root are none of these.
    """

    found: dict[str, dict[str, Path]] = {}
    for entry in _entries(root):
        name = entry.name
        if _is_digest(name) and entry.is_dir(follow_symlinks=False):
            found.setdefault(name, {})["directory"] = Path(entry.path)
            continue
        for part, suffix in (("map", MAP_SUFFIX), ("landing", LANDING_SUFFIX)):
            key = name[:-len(suffix)]
            if (name.endswith(suffix) and _is_digest(key)
                    and entry.is_file(follow_symlinks=False)):
                found.setdefault(key, {})[part] = Path(entry.path)
    return found


def _terminal_why(key: str, *, live: set[str], terminal: Mapping[str, float],
                  now: float, grace_s: float, subject: str) -> tuple[str, float]:
    """Why ``key``'s state is kept (``""`` when it is not), and its age."""

    if key in live:
        return f"{subject} is ready or claimed", 0.0
    if key not in terminal:
        return f"no terminal record names {subject}", 0.0
    age = max(0.0, now - terminal[key])
    if age < grace_s:
        return "terminal for less than the lease timeout", age
    return "", age


def _survey_residency_namespaces(queue_root: Path, *, live: set[str],
                                 terminal: Mapping[str, float], now: float,
                                 grace_s: float) -> tuple[dict, dict]:
    """The residency-namespace and landing-record sections, from one listing.

    A namespace is a candidate when its consumer is terminal as a lock is
    and its fragment directory is empty or gone; its map goes with it,
    because a map composed from no stage fragment is what the tier loop's
    ``compose_map`` itself unlinks for a consumer that is not running.  A
    landing record is a candidate under the terminal rule alone, fragments
    or not: ``publish_landing_expectations`` keeps one only for a claimed
    consumer, and the plan reaper removes it only for a plan it reaps, so a
    finished consumer that was never superseded kept its record forever.
    """

    root = queue_root.joinpath(*RESIDENCY_SUBPATH)
    namespaces = {"scanned": 0, "remove": [], "keep": []}
    landings = {"scanned": 0, "remove": [], "keep": []}
    for key, parts in sorted(_residency_entries(root).items()):
        path = root / key
        if "landing" in parts:
            landings["scanned"] += 1
            why, age = _terminal_why(key, live=live, terminal=terminal, now=now,
                                     grace_s=grace_s, subject="its consumer")
            if why:
                landings["keep"].append(_retain(KIND_LANDING_RECORD, parts["landing"], why))
            else:
                landings["remove"].append({
                    "kind": KIND_LANDING_RECORD, "path": parts["landing"], "key": key,
                    "bytes": 0, "age_s": age,
                    "why": "its consumer is terminal and nothing is queued for it"})
        if not ({"directory", "map"} & set(parts)):
            continue
        namespaces["scanned"] += 1
        why, age = _terminal_why(key, live=live, terminal=terminal, now=now,
                                 grace_s=grace_s, subject="its consumer")
        if why:
            namespaces["keep"].append(_retain(KIND_RESIDENCY_NAMESPACE, path, why))
            continue
        if "directory" in parts:
            try:
                contents = _entries(parts["directory"])
            except SweepError as exc:
                namespaces["keep"].append(_retain(KIND_RESIDENCY_NAMESPACE, path, str(exc)))
                continue
            if contents:
                namespaces["keep"].append(_retain(KIND_RESIDENCY_NAMESPACE, path,
                                                  "holds fragments"))
                continue
        namespaces["remove"].append({
            "kind": KIND_RESIDENCY_NAMESPACE, "path": path, "key": key,
            "directory": parts.get("directory"), "map": parts.get("map"),
            "bytes": 0, "age_s": age,
            "why": "empty, and its consumer is terminal"})
    return namespaces, landings


def survey_queue(queue_root: Path, *, now: float | None = None,
                 grace_s: float = pool.LEASE_TIMEOUT_S) -> dict:
    """Classify the queue-root kinds; reads only.

    ``grace_s`` defaults to ``pool.LEASE_TIMEOUT_S``: the bound after which
    the pool itself reads a silent claim as dead, so a key terminal for
    longer has no attempt of its own left that could still be finishing.
    It selects candidates; the removal's safety does not rest on it.
    """

    queue_root = Path(queue_root)
    if not (queue_root / pool.READY).is_dir():
        raise SweepError(f"not a pool queue: {queue_root}")
    started = time.monotonic()
    now = time.time() if now is None else float(now)
    live, terminal = queue_key_states(queue_root)
    namespaces, landings = _survey_residency_namespaces(
        queue_root, live=live, terminal=terminal, now=now, grace_s=grace_s)
    sections = {
        KIND_RESIDENCY_NAMESPACE: namespaces,
        KIND_LANDING_RECORD: landings,
        KIND_TRANSITION_LOCK: _survey_transition_locks(
            queue_root, live=live, terminal=terminal, now=now, grace_s=grace_s),
    }
    return {
        "queue_root": queue_root,
        "sections": sections,
        "remove": [row for kind in QUEUE_KINDS for row in sections[kind]["remove"]],
        "keep": [row for kind in QUEUE_KINDS for row in sections[kind]["keep"]],
        "grace_s": grace_s,
        "live_keys": len(live),
        "terminal_keys": len(terminal),
        "survey_s": round(time.monotonic() - started, 3),
    }


def _remove_residency_row(queue: pool.PoolQueue, row: Mapping[str, object]) -> str:
    """Remove one consumer's namespace or landing record, or say why not.

    Under the consumer's transition lock, taken without waiting, and after
    re-reading that the consumer is neither ``ready`` nor ``claimed``: a
    resubmission publishes the same key under that lock, so a consumer queued
    again after the survey is seen here and keeps its state
    (``tier_loop._sweep_dead_consumer``'s rule).  A fragment that lands after
    the survey makes the ``rmdir`` fail, and the map is then kept with it.
    """

    key = str(row["key"])
    with queue._transition_locked(key, blocking=False) as acquired:
        if not acquired:
            return "its consumer's transition lock is held"
        # The survey's rule, re-read under the lock: a lease, tombstone or
        # late-finish file keeps the key live here too, not only its row.
        if _key_live_now(Path(queue.root), key):
            return "its consumer was queued again"
        if row["kind"] == KIND_LANDING_RECORD:
            Path(str(row["path"])).unlink(missing_ok=True)
            return ""
        directory = row.get("directory")
        if directory is not None:
            try:
                os.rmdir(Path(str(directory)))
            except FileNotFoundError:
                pass
            except OSError as exc:
                return "a fragment landed after the survey" if exc.errno in (
                    errno.ENOTEMPTY, errno.EEXIST) else f"cannot remove: {exc}"
        if row.get("map") is not None:
            Path(str(row["map"])).unlink(missing_ok=True)
    return ""


def _remove_queue_row(queue: pool.PoolQueue, row: Mapping[str, object]) -> str:
    if row["kind"] == KIND_TRANSITION_LOCK:
        return posix_lock.retire(Path(str(row["path"])))
    return _remove_residency_row(queue, row)


def sweep_queue(plan: Mapping[str, object], *, lock_takers_verify: bool = False) -> dict:
    """Remove the queue-root candidates; online, under the lock protocol.

    ``lock_takers_verify`` is the operator's statement that every process
    that takes a transition lock runs ``posix_lock.held``'s post-lock check
    (the module's ordering rule).  Without it nothing is removed.
    """

    if not lock_takers_verify:
        raise SweepError("removal requires --all-lock-takers-verify: every "
                         "lock taker must run posix_lock's post-lock check")
    started = time.monotonic()
    queue = pool.PoolQueue(Path(str(plan["queue_root"])))
    removed: list[dict] = []
    skipped: list[tuple[dict, str]] = []
    # ``plan["remove"]`` is in ``QUEUE_KINDS`` order: the residency rows take
    # their consumer's transition lock, so they run before the lock files are
    # retired, not after (taking a retired lock would create its file again).
    for row in plan["remove"]:  # type: ignore[index]
        why = _remove_queue_row(queue, row)
        if why:
            skipped.append((dict(row), why))
        else:
            removed.append(dict(row))
    # A residency row whose consumer had no lock file at the survey created
    # one by taking it.  A removed row's key met the lock rule under that
    # lock (the rows share the rule), so its lock file is retired too rather
    # than left for the next run.
    planned = {str(row["key"]) for row in plan["remove"]  # type: ignore[index]
               if row["kind"] == KIND_TRANSITION_LOCK}
    locks = Path(str(plan["queue_root"])).joinpath(*TRANSITION_LOCK_SUBPATH)
    own = 0
    for key in sorted({str(row["key"]) for row in removed
                       if row["kind"] != KIND_TRANSITION_LOCK} - planned):
        if not posix_lock.retire(locks / transition_lock_name(key)):
            own += 1
    return {"removed": removed, "skipped": skipped, "own_locks_retired": own,
            "sweep_s": round(time.monotonic() - started, 3)}


def queue_receipt(plan: Mapping[str, object], outcome: Mapping[str, object] | None,
                  *, host: str | None = None) -> dict:
    """The gc receipt: per kind, what was scanned, removed, skipped and kept."""

    kinds: dict[str, dict] = {}
    removed = list((outcome or {}).get("removed", []))
    skipped = list((outcome or {}).get("skipped", []))
    for kind in QUEUE_KINDS:
        section = plan["sections"][kind]  # type: ignore[index]
        kept: dict[str, int] = {}
        for row in section["keep"]:
            kept[str(row["why"])] = kept.get(str(row["why"]), 0) + 1
        skipped_reasons: dict[str, int] = {}
        for row, why in skipped:
            if row["kind"] == kind:
                skipped_reasons[why] = skipped_reasons.get(why, 0) + 1
        kinds[kind] = {
            "scanned": section["scanned"],
            "candidates": len(section["remove"]),
            "removed": sum(1 for row in removed if row["kind"] == kind),
            "skipped": skipped_reasons,
            "kept": kept,
        }
    return {
        "schema": GC_RECEIPT_SCHEMA,
        "queue_root": str(plan["queue_root"]),
        "host": host or socket.gethostname(),
        "pid": os.getpid(),
        "unix": round(time.time(), 3),
        "applied": outcome is not None,
        "grace_s": plan["grace_s"],
        "live_keys": plan["live_keys"],
        "terminal_keys": plan["terminal_keys"],
        "survey_s": plan["survey_s"],
        "sweep_s": (outcome or {}).get("sweep_s"),
        # Lock files the residency rows created by taking their consumers'
        # locks, retired in the same run; not among any kind's candidates.
        "own_locks_retired": (outcome or {}).get("own_locks_retired", 0),
        "kinds": kinds,
    }


def write_queue_receipt(queue_root: Path, receipt: Mapping[str, object],
                        path: Path | None = None) -> Path:
    """File the receipt, by rename, under ``<queue>/gc-receipts/`` by default."""

    if path is None:
        stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime(float(receipt["unix"])))
        path = (Path(queue_root).joinpath(*GC_RECEIPTS_SUBPATH)
                / f"{stamp}-{receipt['host']}-{receipt['pid']}.json")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(receipt, sort_keys=True, indent=1) + "\n")
    os.replace(temporary, path)
    return path


def report_queue(plan: Mapping[str, object], *, list_paths: bool = True) -> list[str]:
    lines = [f"pb_gc: queue {plan['queue_root']} ({plan['terminal_keys']} terminal "
             f"keys, {plan['live_keys']} live; grace {plan['grace_s']:g}s)"]
    for kind in QUEUE_KINDS:
        section = plan["sections"][kind]  # type: ignore[index]
        lines.append(f"\n{kind}: {section['scanned']} scanned, "
                     f"{len(section['remove'])} to remove")
        if list_paths:
            for row in section["remove"]:
                lines.append(f"  {row['path']}  {format_age(float(row['age_s']))}"
                             f"  {row['why']}")
        for why, count, examples in _group(section["keep"]):
            lines.append(f"  kept {count}: {why}")
            for example in examples if list_paths else []:
                lines.append(f"    {example}")
    return lines


# --------------------------------------------------------------------------
# The report
# --------------------------------------------------------------------------


def format_bytes(count: float) -> str:
    for unit in ("B", "KiB", "MiB", "GiB"):
        if count < 1024 or unit == "GiB":
            return f"{count:.0f} {unit}" if unit == "B" else f"{count:.1f} {unit}"
        count /= 1024
    return f"{count:.1f} GiB"


def format_age(seconds: float) -> str:
    if seconds < 3600:
        return f"{seconds / 60:.0f}m"
    if seconds < 86400:
        return f"{seconds / 3600:.1f}h"
    return f"{seconds / 86400:.1f}d"


def _group(rows: Iterable[Mapping[str, object]]) -> list[tuple[str, int, list[str]]]:
    grouped: dict[str, list[str]] = {}
    for row in rows:
        grouped.setdefault(str(row["why"]), []).append(str(row["path"]))
    return [(why, len(paths), paths[:3]) for why, paths in sorted(grouped.items())]


def report(plan: Mapping[str, object], *, list_paths: bool = True) -> list[str]:
    """The dry-run screen, as lines."""

    sections = plan["sections"]  # type: ignore[index]
    lines = [f"pb_gc: {plan['cas_root']}",
             "Candidates require fleet-wide quiescence and remote checkout "
             "verification before removal; local probes cannot prove abandonment."]
    canary_root = plan.get("canary_root")  # type: ignore[union-attr]
    if canary_root is not None:
        lines.append(f"pb_gc: canary root {canary_root}")
    total_bytes = 0.0
    for kind in KINDS:
        section = sections[kind]  # type: ignore[index]
        rows = section["remove"]
        size = sum(float(row["bytes"]) for row in rows)
        total_bytes += size
        if kind == KIND_CANARY and canary_root is None:
            lines.append(f"\n{kind}: not surveyed (pass --canary-root)")
            continue
        lines.append(
            f"\n{kind}: {section['scanned']} scanned, {len(rows)} to remove"
            + (f", {format_bytes(size)}" if size else "")
        )
        if list_paths:
            for row in rows:
                lines.append(
                    f"  {row['path']}  {format_bytes(float(row['bytes']))}"
                    f"  {format_age(float(row['age_s']))}  {row['why']}"
                )
        for why, count, examples in _group(section["keep"]):
            lines.append(f"  kept {count}: {why}")
            for example in examples if list_paths else []:
                lines.append(f"    {example}")
    lines.append(
        f"\ntotal: {len(plan['remove'])} entries, {format_bytes(total_bytes)}")
    diagnostics = plan["diagnostics"]  # type: ignore[index]
    lines.append("\ndiagnostics (records, never removed):")
    lines.append(
        f"  {diagnostics['requests']} requests, {diagnostics['receipts']} "
        f"receipts, {diagnostics['requests_without_a_receipt']} requests with "
        "no receipt"
    )
    if diagnostics["occupied_staging_namespaces"]:
        lines.append(
            f"  {diagnostics['occupied_staging_namespaces']} staging "
            "namespaces still hold a payload; retained for ownership-aware repair"
        )
    if diagnostics["unreadable_processes"]:
        lines.append(
            f"  {diagnostics['unreadable_processes']} of this user's "
            "processes hide their descriptors, so the open-file answer is "
            "incomplete"
        )
    return lines


class _Parser(argparse.ArgumentParser):
    """Refuses a command line that names no root at all."""

    def parse_args(self, args=None, namespace=None):  # type: ignore[override]
        parsed = super().parse_args(args, namespace)
        if parsed.cas_root is None and parsed.queue_root is None:
            self.error("name the store to sweep: --cas-root, --queue-root, or both")
        return parsed


def build_parser() -> argparse.ArgumentParser:
    """This tool's flags.

    A root is required on purpose. Every other fleet tool defaults its
    roots to the live store, which is right for a tool that reports and wrong
    for one that removes: a default here would mean an operator who typed no
    root swept the fleet's own CAS.  ``--cas-root`` and ``--queue-root`` each
    add their own kinds; at least one must be typed.
    """

    ap = _Parser(description=__doc__.splitlines()[0])
    ap.add_argument("--cas-root", default=None,
                    help="the store to sweep; deliberately has no default, "
                         "because this tool removes files")
    ap.add_argument("--queue-root", default=None,
                    help="also survey the pool queue's transition locks and "
                         "residency namespaces (#995); no default either")
    ap.add_argument("--canary-root", default=None,
                    help="also survey the pbcanary run namespaces at this "
                         "root (each holds a run's staged chunks and "
                         "artifacts); removable under the same --apply "
                         "rules, and deliberately has no default either")
    ap.add_argument("--apply", action="store_true",
                    help="actually remove; the default only reports")
    ap.add_argument("--quiescent-store", action="store_true",
                    help="acknowledge all producers on all hosts are paused, "
                         "and candidate checkout roots are absent on every "
                         "host; required with --apply for the CAS kinds")
    ap.add_argument("--all-lock-takers-verify", action="store_true",
                    help="acknowledge that every process taking a transition "
                         "lock runs posix_lock's post-lock check (a runtime "
                         "carrying #995 on every box and role); required "
                         "with --apply for the queue kinds")
    ap.add_argument("--receipt", default=None,
                    help="where to write the queue sweep's JSON receipt "
                         "(default <queue>/gc-receipts/<utc>-<host>-<pid>.json)")
    ap.add_argument("--min-age-hours", type=float, default=DEFAULT_MIN_AGE_HOURS,
                    help="minimum retention age in hours for the CAS kinds; "
                         "never proof of remote abandonment "
                         f"(default {DEFAULT_MIN_AGE_HOURS:g})")
    ap.add_argument("--summary", action="store_true",
                    help="counts and totals only, without a line per entry")
    return ap


def _main_queue(args: argparse.Namespace) -> int:
    """Survey, and on ``--apply`` sweep, the queue kinds; always file a receipt."""

    try:
        plan = survey_queue(Path(args.queue_root))
    except SweepError as exc:
        print(f"pb_gc: {exc}", file=sys.stderr)
        return 2
    print("\n".join(report_queue(plan, list_paths=not args.summary)))
    outcome = None
    if args.apply:
        try:
            outcome = sweep_queue(plan, lock_takers_verify=args.all_lock_takers_verify)
        except SweepError as exc:
            print(f"pb_gc: {exc}", file=sys.stderr)
            return 2
        print(f"\nremoved {len(outcome['removed'])} queue entries")
        for row, why in outcome["skipped"]:
            print(f"  left {row['path']}: {why}")
    else:
        print("\nnothing removed from the queue; re-run with --apply")
    receipt = queue_receipt(plan, outcome)
    try:
        written = write_queue_receipt(Path(args.queue_root), receipt,
                                      Path(args.receipt) if args.receipt else None)
    except OSError as exc:
        print(f"pb_gc: cannot write the gc receipt: {exc}", file=sys.stderr)
        return 2
    print(f"receipt: {written}")
    return 0


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if not math.isfinite(args.min_age_hours) or args.min_age_hours < 0:
        print("pb_gc: --min-age-hours must be finite and nonnegative", file=sys.stderr)
        return 2
    if args.cas_root is not None and args.apply and not args.quiescent_store:
        print("pb_gc: --apply requires --quiescent-store; pause all producers "
              "on all hosts and verify candidate checkout roots first", file=sys.stderr)
        return 2
    if args.queue_root is not None and args.apply and not args.all_lock_takers_verify:
        print("pb_gc: --apply on --queue-root requires --all-lock-takers-verify; "
              "every lock taker must run the runtime carrying #995 first",
              file=sys.stderr)
        return 2
    status = 0
    if args.cas_root is not None:
        status = _main_cas(args)
        if status == 2:
            return status
    if args.queue_root is not None:
        queue_status = _main_queue(args)
        if queue_status == 2:
            return queue_status
    return status


def _main_cas(args: argparse.Namespace) -> int:
    min_age_s = args.min_age_hours * 3600.0
    try:
        plan = survey(Path(args.cas_root), min_age_s=min_age_s,
                      canary_root=Path(args.canary_root) if args.canary_root
                      else None)
    except SweepError as exc:
        print(f"pb_gc: {exc}", file=sys.stderr)
        return 2
    print("\n".join(report(plan, list_paths=not args.summary)))
    if not args.apply:
        print("\nnothing removed; re-run with --apply")
        return 0
    try:
        outcome = sweep(plan, min_age_s=min_age_s,
                        quiescent_store=args.quiescent_store)
    except SweepError as exc:
        print(f"pb_gc: {exc}", file=sys.stderr)
        return 2
    freed = sum(float(row["bytes"]) for row in outcome["removed"])
    print(f"\nremoved {len(outcome['removed'])} entries, {format_bytes(freed)}")
    for row, why in outcome["skipped"]:
        print(f"  left {row['path']}: {why}")
    return 1 if outcome["skipped"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
