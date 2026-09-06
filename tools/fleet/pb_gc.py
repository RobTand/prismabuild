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

"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import stat
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
from runtime_paths import generation_root  # noqa: E402
RUNTIME_ROOT = generation_root(__file__)
sys.path.insert(0, str(RUNTIME_ROOT / "src"))
from collections.abc import Iterable, Mapping  # noqa: E402
from prismabuild import core as pb  # noqa: E402

#: The litter classes, in the order the report prints them.
KIND_CLAIM = "claim"
KIND_LOCK = "lock"
KIND_NAMESPACE = "staging namespace"
KIND_INGEST = "aborted staging copy"
KIND_PRIVATE_INGEST = "private ingest staging"
KINDS = (KIND_CLAIM, KIND_LOCK, KIND_NAMESPACE, KIND_INGEST, KIND_PRIVATE_INGEST)
PRIVATE_STAGING_PREFIX = "ingest."
PRIVATE_STAGING_OWNER = ".owner.lock"

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
) -> dict:
    """Classify every per-execution dropping in one store.

    Reads only. The caller supplies the ``/proc`` answer so the sweep can take
    a fresh one immediately before it removes anything.
    """

    if not math.isfinite(min_age_s) or min_age_s < 0:
        raise SweepError("minimum age must be finite and nonnegative")
    cas_root = Path(cas_root)
    if not cas_root.is_dir():
        raise SweepError(f"not a CAS root: {cas_root}")
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
    }
    requests = _count_json(cas_root / "requests")
    receipts = _count_json(cas_root / "actions")
    return {
        "cas_root": cas_root,
        "sections": sections,
        "remove": [row for kind in KINDS for row in sections[kind]["remove"]],
        "keep": [row for kind in KINDS for row in sections[kind]["keep"]],
        "diagnostics": {
            "requests": requests,
            "receipts": receipts,
            "requests_without_a_receipt": max(0, requests - receipts),
            "occupied_staging_namespaces": occupied,
            "unreadable_processes": unreadable_processes,
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


def _remove(row: Mapping[str, object]) -> str:
    """Remove the inspected inode through a descriptor anchored below the CAS."""

    path = Path(str(row["path"]))
    kind = str(row["kind"])
    if kind == KIND_LOCK:
        return _remove_lock(path, row["identity"])
    if kind == KIND_PRIVATE_INGEST:
        return _remove_private_ingest(row)
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
    total_bytes = 0.0
    for kind in KINDS:
        section = sections[kind]  # type: ignore[index]
        rows = section["remove"]
        size = sum(float(row["bytes"]) for row in rows)
        total_bytes += size
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


def build_parser() -> argparse.ArgumentParser:
    """This tool's flags.

    ``--cas-root`` is required on purpose. Every other fleet tool defaults its
    roots to the live store, which is right for a tool that reports and wrong
    for one that removes: a default here would mean an operator who typed no
    root swept the fleet's own CAS.
    """

    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--cas-root", required=True,
                    help="the store to sweep; required, and deliberately has "
                         "no default, because this tool removes files")
    ap.add_argument("--apply", action="store_true",
                    help="actually remove; the default only reports")
    ap.add_argument("--quiescent-store", action="store_true",
                    help="acknowledge all producers on all hosts are paused, "
                         "and candidate checkout roots are absent on every "
                         "host; required with --apply")
    ap.add_argument("--min-age-hours", type=float, default=DEFAULT_MIN_AGE_HOURS,
                    help="minimum retention age in hours; never proof of "
                         f"remote abandonment (default {DEFAULT_MIN_AGE_HOURS:g})")
    ap.add_argument("--summary", action="store_true",
                    help="counts and totals only, without a line per entry")
    return ap


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if not math.isfinite(args.min_age_hours) or args.min_age_hours < 0:
        print("pb_gc: --min-age-hours must be finite and nonnegative", file=sys.stderr)
        return 2
    if args.apply and not args.quiescent_store:
        print("pb_gc: --apply requires --quiescent-store; pause all producers "
              "on all hosts and verify candidate checkout roots first", file=sys.stderr)
        return 2
    min_age_s = args.min_age_hours * 3600.0
    try:
        plan = survey(Path(args.cas_root), min_age_s=min_age_s)
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
