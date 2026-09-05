#!/usr/bin/env python3
"""Report, and on request remove, the per-execution litter in a store.

Every local action leaves four immutable droppings behind, and nothing in the
system has ever removed any of them.  On the live store on 2026-09-05 that was
1785 claims, 1745 ``.worker-locks`` entries and 942 empty staging namespace
directories, all of them dead.

Where each comes from:

*   A **claim** at ``local-results/v1/<shard>/<digest>.json`` records exclusive
    ownership of one action's declared result path.  Its digest covers the
    ``checkout_root`` the action ran in (``core._local_result_claim_body``),
    and ``materialize._execution_checkout`` mints a fresh ``mkdtemp`` root for
    every execution of a sealed snapshot.  So a campaign that runs one action a
    thousand times files a thousand distinct claims.
*   A **lock** at ``.worker-locks/<digest>.lock`` serializes writers of one
    declared output path (``core._local_output_lock``).  The identity is the
    physical output path, which lives inside that same per-execution root, so
    it too is unique per run.
*   A **staging namespace** at ``.staging/local-results/<claim digest>/`` is
    where a claimed result is copied before publication.  The payload is
    unlinked when the publication finishes; the directory is not.
What makes removal safe is structural, not a guess about age.  A claim exists
to authorize ``core.repair_local_result`` of a leftover result *under its own
checkout root*.  ``materialize._cleanup_execution_checkout`` removes that root
in a ``finally``, unconditionally.  Once the root is gone there is no path the
claim could ever authorize again, so the claim is dead by construction.  A
claim whose root is still there is the persistent-checkout case, where repair
is real work, and this tool never touches one.  Every other class hangs off
that same fact: a lock is swept only when no live claim names its output path,
no process holds its ``flock``, and no process holds its inode open; a staging
namespace only when it is empty and no live claim carries its digest.

``--min-age-hours`` is a backstop on top of that rule and never a substitute
for it.  It exists for one blind spot: checkout roots are box-local with the
same spelling on every box (``materialize.LOCAL_CHECKOUT_ROOT``), so a root
that is absent here can be a live execution over there.  ``pbrun`` has no
default deadline, so no age is provably safe; raise the backstop past the
longest action a campaign runs.

Two honest limits, recorded rather than papered over:

*   Winning a lock file's ``flock`` and then unlinking it leaves a window in
    which a producer that opened the same path acquires the lock on an orphan
    inode while the next producer creates a fresh one.  The ``/proc`` scan
    narrows that window and does not close it.  It costs duplicate work rather
    than corruption, the same exposure the store already carries because
    ``flock`` is not cross-box, and for a per-execution checkout the output
    path is never reused at all.
*   The ``/proc`` scan sees only processes this user can inspect.  The count it
    could not read is reported, because an incomplete answer must not read as
    an empty one.

Requests and receipts are records, not litter.  The gap between them is
reported as a diagnostic and nothing here will remove either.

``--cas-root`` is required and has no default.  This tool removes files, and a
default would let it be pointed at the live fleet store by omission.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
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

#: The four litter classes, in the order the report prints them.
KIND_CLAIM = "claim"
KIND_LOCK = "lock"
KIND_NAMESPACE = "staging namespace"
KINDS = (KIND_CLAIM, KIND_LOCK, KIND_NAMESPACE)

#: Where each class lives, spelled the way the code that writes it spells it:
#: ``core._local_result_claim_path``, ``core._local_output_lock`` and
#: ``core.PrismaBuildCAS.publish_result``.
#: ``test_pb_gc`` mints one of each through those functions and asserts this
#: tool finds them, so a layout change is a failing test rather than a sweeper
#: that quietly stops sweeping.
CLAIM_SUBPATH = ("local-results", "v1")
LOCK_SUBPATH = (".worker-locks",)
NAMESPACE_SUBPATH = (".staging", "local-results")

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
    except OSError:
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

    Opened read-only and never created: ``flock`` does not care about the
    access mode, and a sweeper that created a lock file would be inventing the
    thing it came to remove. The lock is taken non-blocking and released at
    once, so a producer that arrives during the probe waits microseconds.
    """

    flags = os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        return f"cannot open: {exc}"
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            return "held by a live local action"
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
    sections = {
        KIND_CLAIM: claims,
        KIND_LOCK: locks,
        KIND_NAMESPACE: namespaces,
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


def _remove_lock(path: Path) -> str:
    """Unlink one lock file while holding its own exclusive lock.

    The lock is taken first so a producer already inside ``_local_output_lock``
    keeps the file, and the inode is re-identified through the directory
    descriptor so what is unlinked is what was locked.
    """

    try:
        directory_fd = os.open(
            path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    except OSError as exc:
        return f"cannot open {path.parent}: {exc}"
    try:
        flags = os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
        try:
            descriptor = os.open(path.name, flags, dir_fd=directory_fd)
        except FileNotFoundError:
            return "already gone"
        except OSError as exc:
            return f"cannot open: {exc}"
        try:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                return "taken by a live local action during the sweep"
            locked = os.fstat(descriptor)
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


def _remove(row: Mapping[str, object]) -> str:
    """Remove one candidate. Returns ``""`` or why it was left alone."""

    path = Path(str(row["path"]))
    kind = str(row["kind"])
    if kind == KIND_LOCK:
        return _remove_lock(path)
    try:
        if kind == KIND_NAMESPACE:
            os.rmdir(path)
        else:
            os.unlink(path)
    except FileNotFoundError:
        return "already gone"
    except OSError as exc:
        return f"cannot remove: {exc}"
    return ""


def sweep(plan: Mapping[str, object], *, min_age_s: float) -> dict:
    """Remove the plan's candidates, re-proving each one first.

    The whole survey is re-run against a fresh ``/proc`` reading and only the
    paths both surveys condemn are removed. A candidate that became live in
    between is skipped and reported, which is what makes "never removes
    something in flight" a property rather than a hope.
    """

    held, unreadable = open_inodes(skip_pid=os.getpid())
    recheck = survey(
        Path(str(plan["cas_root"])), min_age_s=min_age_s,
        held_inodes=held, unreadable_processes=unreadable,
    )
    still_dead = {str(row["path"]) for row in recheck["remove"]}
    removed: list[dict] = []
    skipped: list[tuple[dict, str]] = []
    for row in plan["remove"]:  # type: ignore[index]
        if str(row["path"]) not in still_dead:
            skipped.append((dict(row), "became live between the plan and the sweep"))
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
    lines = [f"pb_gc: {plan['cas_root']}"]
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
            "namespaces still hold a payload; each is a killed publication"
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
    ap.add_argument("--min-age-hours", type=float, default=DEFAULT_MIN_AGE_HOURS,
                    help="backstop on top of the structural rule, for the "
                         "cross-box blind spot: a checkout root absent here "
                         "can be a live execution on another box (default "
                         f"{DEFAULT_MIN_AGE_HOURS:g}). Raise it past the "
                         "longest action a campaign runs")
    ap.add_argument("--summary", action="store_true",
                    help="counts and totals only, without a line per entry")
    return ap


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    min_age_s = max(0.0, args.min_age_hours) * 3600.0
    try:
        plan = survey(Path(args.cas_root), min_age_s=min_age_s)
    except SweepError as exc:
        print(f"pb_gc: {exc}", file=sys.stderr)
        return 2
    print("\n".join(report(plan, list_paths=not args.summary)))
    if not args.apply:
        print("\nnothing removed; re-run with --apply")
        return 0
    outcome = sweep(plan, min_age_s=min_age_s)
    freed = sum(float(row["bytes"]) for row in outcome["removed"])
    print(f"\nremoved {len(outcome['removed'])} entries, {format_bytes(freed)}")
    for row, why in outcome["skipped"]:
        print(f"  left {row['path']}: {why}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
