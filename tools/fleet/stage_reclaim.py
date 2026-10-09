#!/usr/bin/env python3
"""Reclaim source-mark-only stage copies with digest-proven originals (#1636).

A staged range lives at ``<rel>.pbrange/<offset>-<size>``, so the copy
names its own original: ``<mount>/<rel>`` at extent ``[offset, offset+size)``.
No receipt names it. Routine ``reconcile`` leaves every such copy for
the life of the fleet.

This verb moves only copies whose whole bytes hash equal to the
original extent, into a quarantine area outside the stage tier. It
deletes nothing. The tier mint exposes the freed room on its next
cycle. ``restore`` copies a run back with its marks.
"""

from __future__ import annotations

from contextlib import contextmanager
import argparse
import json
import os
from pathlib import Path
import shlex
import socket
import stat as statmod
import sys
import tempfile
import time

sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
from runtime_paths import generation_root  # noqa: E402

sys.path.insert(0, str(generation_root(__file__) / "src"))

from prismabuild import core as pb, pool, resident_sets  # noqa: E402

import prewarm_loop  # noqa: E402
import stage_release  # noqa: E402
from stage_move import RANGE_SUFFIX, STAGE_MOVER_XATTR, stage_relative  # noqa: E402

#: The event each reclaim receipt publishes.
RECLAIM_EVENT = "stage-source-mark-reclaimed"
#: ``manifest.json`` inside each quarantine run directory.
MANIFEST_NAME = "manifest.json"
#: The newest manifest layout this verb reads and writes.
MANIFEST_SCHEMA_V1 = "prismabuild.stage-source-mark-reclaim.v1"
#: Chunk size for digest streams.
_CHUNK = 1 << 20


def _refusal(stage: Path, refusal: str, **fields: object) -> dict[str, object]:
    receipt: dict[str, object] = {
        "schema": pool.POOL_EGRESS_SCHEMA_V1,
        "event": RECLAIM_EVENT,
        "action_key": "",
        "consumer_action_key": "",
        "tier_id": "",
        "stage_root": str(stage),
        "reason": "reclaim-source-marks",
        "applied": False,
        "complete": False,
        "skipped": refusal,
        "errors": [refusal],
        "host": socket.gethostname(),
        "unix": time.time(),
    }
    receipt.update(fields)
    return receipt


def parse_staged_name(stage_rel: str) -> tuple[str, int, int] | None:
    """Split ``<rel>.pbrange/<offset>-<size>`` into its parts, or None."""

    marker = RANGE_SUFFIX + "/"
    # From the right: a source path may itself hold ``.pbrange/<a>-<b>``,
    # and the range is always the last component (stage_move.stage_relative
    # is injective on it).
    head, sep, tail = stage_rel.rpartition(marker)
    if not sep or not head or not tail:
        return None
    offset_s, dash, size_s = tail.partition("-")
    if not dash or not offset_s or not size_s:
        return None
    if (not offset_s.isdigit() or not size_s.isdigit()
            or "-" in size_s or "/" in size_s):
        return None
    try:
        offset, size = int(offset_s), int(size_s)
    except ValueError:
        return None
    if size <= 0:
        return None
    return head, offset, size


def original_for(stage: Path, path: Path, *, mount_prefix: str,
                 stage_resolved: Path) -> tuple[str, int, int] | None:
    """Derive one staged copy's original path and extent, or None.

    The relative name under the stage root must parse as a staged
    range, and the joined original must resolve outside the stage.
    """

    try:
        stage_rel = str(path.relative_to(stage))
    except ValueError:
        return None
    parsed = parse_staged_name(stage_rel)
    if parsed is None:
        return None
    rel, offset, size = parsed
    prefix = os.path.normpath(mount_prefix)
    if not prefix or prefix == ".":
        return None
    original = os.path.normpath(os.path.join(prefix, rel))
    try:
        if stage_resolved in Path(original).resolve().parents:
            return None
    except OSError:
        return None
    if original == str(stage_resolved) or original.startswith(
            str(stage_resolved) + os.sep):
        return None
    return original, offset, size


@contextmanager
def _open_regular(path: str | Path):
    """Open the descriptor to read, without symlink or access-time changes."""
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
                 | os.O_NOATIME)
    try:
        if not statmod.S_ISREG(os.fstat(fd).st_mode):
            raise OSError(f"{path}: not a regular file")
        handle = os.fdopen(fd, "rb")
    except BaseException:
        os.close(fd)
        raise
    with handle:
        yield handle


def extent_digest(path: str | Path, *, offset: int = 0,
                  length: int | None = None) -> tuple[str, int] | None:
    """Hash an exact extent from a stable regular-file descriptor."""
    digest = pb.new_sha256()
    hashed = 0
    try:
        with _open_regular(path) as handle:
            before = _stat_fence(os.fstat(handle.fileno()))
            extent = before["size"] - offset if length is None else length
            if offset < 0 or extent < 0 or offset + extent > before["size"]:
                return None
            handle.seek(offset)
            while hashed < extent:
                chunk = handle.read(min(_CHUNK, extent - hashed))
                if not chunk:
                    return None
                digest.update(chunk)
                hashed += len(chunk)
            if (_stat_fence(os.fstat(handle.fileno())) != before
                    or _stat_fence(os.lstat(path)) != before):
                return None
    except OSError:
        return None
    return digest.hexdigest(), hashed


def _stat_fence(info: os.stat_result) -> dict[str, int]:
    """The stat fields that fence one file's hashed bytes."""

    return {"dev": int(info.st_dev), "ino": int(info.st_ino),
            "mode": int(info.st_mode), "size": int(info.st_size),
            "mtime_ns": int(info.st_mtime_ns), "ctime_ns": int(info.st_ctime_ns)}


def prove_pair(stage_path: Path, original: str, offset: int, size: int,
               ) -> tuple[str | None, str, dict[str, object]]:
    """Hash a staged copy and its original extent; return proof or refusal.

    Returns ``(sha256, status, detail)``. ``status`` is ``"paired"`` with
    the shared digest, or one refusal reason. Each side's identity is
    fenced across its read, as ``stage_move._content_proof`` fences an
    adoption: the digest describes the fenced incarnation only.
    ``detail`` carries both identities and the exact bytes hashed.
    """

    detail: dict[str, object] = {"bytes_hashed": 0}
    try:
        link = os.lstat(stage_path)
    except OSError as exc:
        return None, "stage_unreadable", {"error": str(exc)}
    if not statmod.S_ISREG(link.st_mode):
        return None, "not_a_regular_file", dict(detail)
    if int(link.st_size) != size:
        detail["copy_bytes"] = int(link.st_size)
        return None, "copy_size_differs", detail
    try:
        origin_link = os.lstat(original)
    except FileNotFoundError:
        return None, "original_missing", dict(detail)
    except OSError as exc:
        return None, "original_unreadable", {"error": str(exc)}
    if not statmod.S_ISREG(origin_link.st_mode):
        return None, "original_not_regular", dict(detail)
    if int(origin_link.st_size) < offset + size:
        detail["original_bytes"] = int(origin_link.st_size)
        return None, "original_short", detail
    try:
        stage_before = _stat_fence(os.stat(stage_path))
    except OSError as exc:
        return None, "stage_unreadable", {"error": str(exc)}
    stage_got = extent_digest(stage_path, length=size)
    if stage_got is None:
        return None, "stage_unreadable", dict(detail)
    stage_hex, stage_n = stage_got
    detail["bytes_hashed"] = int(detail["bytes_hashed"]) + stage_n
    try:
        stage_after = _stat_fence(os.stat(stage_path))
    except OSError as exc:
        return None, "stage_unreadable", {"error": str(exc)}
    if stage_after != stage_before:
        return None, "copy_changed", dict(detail)
    if stage_n != size:
        detail["copy_bytes"] = stage_n
        return None, "copy_changed", detail
    try:
        origin_before = _stat_fence(os.stat(original))
    except FileNotFoundError:
        return None, "original_missing", dict(detail)
    except OSError as exc:
        return None, "original_unreadable", {"error": str(exc)}
    origin_got = extent_digest(original, offset=offset, length=size)
    if origin_got is None:
        return None, "original_unreadable", dict(detail)
    origin_hex, origin_n = origin_got
    detail["bytes_hashed"] = int(detail["bytes_hashed"]) + origin_n
    if origin_n != size:
        return None, "original_short", dict(detail)
    # The source may move under the read: the digest counts only when
    # the extent's incarnation is unchanged across it.
    try:
        origin_after = _stat_fence(os.stat(original))
    except OSError:
        return None, "original_unreadable", dict(detail)
    if origin_after != origin_before:
        return None, "original_changed", dict(detail)
    if stage_hex != origin_hex:
        detail["copy_sha256"] = stage_hex
        detail["original_sha256"] = origin_hex
        return None, "digest_differs", detail
    detail["sha256"] = stage_hex
    detail["stage_identity"] = stage_after
    detail["original_identity"] = origin_after
    return stage_hex, "paired", detail


def _source_mark_only(path: Path) -> bool | None:
    """Whether a file carries only the prewarm source mark, or None."""

    try:
        os.getxattr(path, STAGE_MOVER_XATTR, follow_symlinks=False)
    except OSError as exc:
        if exc.errno not in stage_release._XATTR_ABSENT:
            return None
    else:
        return False
    try:
        os.getxattr(path, prewarm_loop.STAGE_SOURCE_XATTR, follow_symlinks=False)
    except OSError as exc:
        if exc.errno in stage_release._XATTR_ABSENT:
            return False
        return None
    return True


def _walk_source_marks(stage: Path, attributed: set[str]
                       ) -> tuple[list[Path], list[Path], int, list[str]]:
    """The walk's source-mark-only copies and its attributed skips, sorted.

    Attributed skips are returned, not dropped, so the dry run lists
    every copy it saw. Prewarm temporaries are never copies: the loop
    reaps its own, and a live one belongs to a copy in flight.
    """

    found: list[Path] = []
    skipped: list[Path] = []
    walk_bytes = 0
    walk_errors: list[str] = []
    stack = [stage]
    while stack:
        directory = stack.pop()
        directory_fd = None
        try:
            directory_fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY
                                   | os.O_NOFOLLOW | os.O_NOATIME)
            with os.scandir(directory_fd) as scan:
                entries = sorted(scan, key=lambda entry: entry.name)
        except OSError as exc:
            walk_errors.append(f"{directory}: {exc}")
            continue
        finally:
            if directory_fd is not None:
                os.close(directory_fd)
        for entry in entries:
            if entry.name == stage_release.STAGE_ROOT_MARKER:
                continue
            entry_path = directory / entry.name
            try:
                info = os.lstat(entry_path)
                if statmod.S_ISLNK(info.st_mode):
                    continue
                if statmod.S_ISDIR(info.st_mode):
                    stack.append(entry_path)
                    continue
            except OSError as exc:
                walk_errors.append(f"{entry.path}: {exc}")
                continue
            if prewarm_loop._STAGE_TEMPORARY.search(entry.name):
                continue
            kind = _source_mark_only(entry_path)
            if kind is None:
                walk_errors.append(f"{entry_path}: marks unreadable")
                continue
            if kind is not True:
                continue
            normalized = os.path.normpath(str(entry_path))
            if normalized in attributed:
                skipped.append(entry_path)
                continue
            try:
                info = os.lstat(entry_path)
            except OSError as exc:
                walk_errors.append(f"{entry.path}: {exc}")
                continue
            if not statmod.S_ISREG(info.st_mode):
                continue
            found.append(entry_path)
            walk_bytes += int(info.st_size)
    found.sort()
    skipped.sort()
    return found, skipped, walk_bytes, walk_errors


def _quarantine_dest(quarantine_root: Path, run_id: str,
                     stage_rel: str) -> Path:
    return quarantine_root / run_id / Path(stage_rel)


def _mkdir_durable(path: Path) -> None:
    """Commit each new directory before a file can depend on it."""
    if os.path.lexists(path):
        if not statmod.S_ISDIR(os.lstat(path).st_mode):
            raise OSError(f"{path}: directory is not a plain directory")
        return
    _mkdir_durable(path.parent)
    path.mkdir()
    resident_sets.fsync_directory(path.parent)


def _copy_to_quarantine(source: Path, dest: Path, size: int, *,
                        metadata=None, expected_digest: str | None = None
                        ) -> str | None:
    """Commit a verified copy without replacing an existing destination."""
    temporary = None
    try:
        _mkdir_durable(dest.parent)
        fd, name = tempfile.mkstemp(prefix=f".{dest.name}.", dir=dest.parent)
        temporary = Path(name)
        digest = pb.new_sha256()
        with os.fdopen(fd, "wb") as writer, _open_regular(source) as reader:
            before = _stat_fence(os.fstat(reader.fileno()))
            if before["size"] != size:
                return None
            remaining = size
            while remaining:
                chunk = reader.read(min(_CHUNK, remaining))
                if not chunk:
                    return None
                writer.write(chunk)
                digest.update(chunk)
                remaining -= len(chunk)
            if (_stat_fence(os.fstat(reader.fileno())) != before
                    or _stat_fence(os.lstat(source)) != before):
                return None
            writer.flush()
            os.fsync(writer.fileno())
        check = extent_digest(temporary)
        if check != (digest.hexdigest(), size):
            return None
        if expected_digest is not None and check[0] != expected_digest:
            return None
        if metadata is not None:
            xattrs, times = metadata
            _apply_xattrs_and_times(temporary, xattrs, times)
            if not _metadata_matches(temporary, xattrs, times):
                return None
        os.link(temporary, dest, follow_symlinks=False)
        resident_sets.fsync_directory(dest.parent)
        return digest.hexdigest()
    except (OSError, ValueError):
        return None
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def _xattrs_and_times(path: Path
                      ) -> tuple[dict[str, str], dict[str, int]] | None:
    """One file's marks and times, or None when the marks won't answer.

    A manifest without the source mark restores an unmarked file, and
    ``reconcile`` deletes unmarked files. So unreadable marks refuse
    the file, as unreadable times do.
    """

    xattrs: dict[str, str] = {}
    try:
        names = os.listxattr(path, follow_symlinks=False)
    except OSError:
        return None
    for name in names:
        try:
            raw = os.getxattr(path, name, follow_symlinks=False)
        except OSError:
            return None
        xattrs[str(name)] = raw.hex()
    try:
        info = os.stat(path, follow_symlinks=False)
        times = {"mtime_ns": int(info.st_mtime_ns),
                 "atime_ns": int(info.st_atime_ns),
                 "mode": statmod.S_IMODE(info.st_mode)}
    except OSError:
        return None
    return xattrs, times


def _apply_xattrs_and_times(path: Path, xattrs: dict[str, str],
                            times: dict[str, int]) -> None:
    if "mode" in times:
        os.chmod(path, times["mode"], follow_symlinks=False)
    for name, hexed in xattrs.items():
        os.setxattr(path, name, bytes.fromhex(hexed), follow_symlinks=False)
    os.utime(path, ns=(int(times["atime_ns"]), int(times["mtime_ns"])),
             follow_symlinks=False)
    with _open_regular(path) as handle:
        os.fsync(handle.fileno())


def _metadata_matches(path: Path, xattrs: dict[str, str],
                      times: dict[str, int], *, check_atime: bool = True) -> bool:
    captured = _xattrs_and_times(path)
    if captured is None or captured[0] != xattrs:
        return False
    fields = ("mtime_ns", "atime_ns", "mode") if check_atime else ("mtime_ns", "mode")
    return all(captured[1].get(key) == times[key] for key in fields if key in times)


def _safe_tree_path(path: Path, root: Path) -> None:
    """Keep each path component on the approved device without symlinks."""
    root = root.resolve(strict=True)
    relative = path.absolute().relative_to(root)
    device = root.stat().st_dev
    current = root
    for part in relative.parts:
        current /= part
        if not os.path.lexists(current):
            break
        info = os.lstat(current)
        if statmod.S_ISLNK(info.st_mode) or info.st_dev != device:
            raise ValueError(f"{current}: unsafe path component")


def write_manifest(run_dir: Path, entries: list[dict[str, object]]) -> Path:
    manifest = {
        "schema": MANIFEST_SCHEMA_V1,
        "run_id": run_dir.name,
        "unix": time.time(),
        "host": socket.gethostname(),
        "entries": entries,
        "restore_command": entries[0].get("restore_command") if entries else None,
    }
    path = run_dir / MANIFEST_NAME
    temporary = run_dir / f".{MANIFEST_NAME}.{os.getpid()}.tmp"
    with open(temporary, "w") as stream:
        json.dump(manifest, stream, indent=1, sort_keys=True)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    directory_fd = os.open(run_dir, os.O_DIRECTORY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)
    return path


def read_manifest(run_dir: Path) -> dict[str, object]:
    with open(run_dir / MANIFEST_NAME) as stream:
        manifest = json.load(stream)
    if (not isinstance(manifest, dict)
            or manifest.get("schema") != MANIFEST_SCHEMA_V1
            or not isinstance(manifest.get("entries"), list)):
        raise ValueError(f"{run_dir}: not a reclaim manifest")
    return manifest


#: One JSON object per moved file, appended and fsynced as each move
#: completes, so a crash before the final manifest still names every
#: moved file for ``restore_run``.
JOURNAL_NAME = "journal.jsonl"


def _append_journal(run_dir: Path, entry: dict[str, object]) -> None:
    """Append one moved file to the run's journal, durably."""

    path = run_dir / JOURNAL_NAME
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND | os.O_NOFOLLOW,
                 0o600)
    with os.fdopen(fd, "w") as stream:
        stream.write(pb.sorted_json(entry) + "\n")
        stream.flush()
        os.fsync(stream.fileno())
    resident_sets.fsync_directory(run_dir)


def _read_journal(run_dir: Path) -> list[dict[str, object]]:
    """The journal's entries, oldest first; an absent journal reads empty."""

    path = run_dir / JOURNAL_NAME
    try:
        with _open_regular(path) as stream:
            raw = stream.read().decode("utf-8")
    except FileNotFoundError:
        return []
    entries: list[dict[str, object]] = []
    for line in raw.splitlines(keepends=True):
        if not line.endswith("\n"):
            break  # An interrupted append cannot authorize an unlink.
        entry = json.loads(line)
        if not isinstance(entry, dict):
            raise ValueError("journal entry is not an object")
        entries.append(entry)
    return entries


#: The memo of proven digests a dry run writes and an apply reuses.
MEMO_SCHEMA_V1 = "prismabuild.stage-source-mark-reclaim-memo.v1"
MEMO_NAME = "memo.json"


def write_memo_file(path: Path, pairs: list[dict[str, object]]) -> Path:
    """File the proven digests, fenced by both files' identities."""

    memo = {
        "schema": MEMO_SCHEMA_V1,
        "run_id": path.parent.name,
        "unix": time.time(),
        "host": socket.gethostname(),
        "entries": {
            str(one["stage_rel"]): {
                "sha256": one["sha256"],
                "stage_identity": one["stage_identity"],
                "original_identity": one["original_identity"],
                "original_path": one["original_path"],
                "offset": one["offset"],
                "size": one["size"],
            } for one in pairs},
    }
    temporary = path.parent / f".{path.name}.{os.getpid()}.tmp"
    with open(temporary, "w") as stream:
        json.dump(memo, stream, indent=1, sort_keys=True)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    return path


def write_memo(run_dir: Path, pairs: list[dict[str, object]]) -> Path:
    """File the proven digests under one run directory."""

    return write_memo_file(run_dir / MEMO_NAME, pairs)


def read_memo(path: str | Path) -> dict[str, dict[str, object]]:
    """The memo's entries by staged relative path, or empty when absent."""

    try:
        with open(path) as stream:
            memo = json.load(stream)
    except FileNotFoundError:
        return {}
    if (not isinstance(memo, dict)
            or memo.get("schema") != MEMO_SCHEMA_V1
            or not isinstance(memo.get("entries"), dict)):
        raise ValueError(f"{path}: not a reclaim memo")
    return {str(rel): dict(entry)
            for rel, entry in memo["entries"].items()
            if isinstance(entry, dict)}


def _memo_hit(memo: dict[str, dict[str, object]], stage: Path,
              stage_rel: str, original: str, offset: int, size: int,
              ) -> tuple[str | None, dict[str, object]]:
    """A memo entry's digest when both files still fence it, else None."""

    entry = memo.get(stage_rel)
    if entry is None:
        return None, {}
    if (entry.get("original_path") != original
            or entry.get("offset") != offset
            or entry.get("size") != size):
        return None, {}
    try:
        stage_identity = _stat_fence(os.lstat(stage / stage_rel))
    except OSError:
        return None, {}
    if stage_identity != entry.get("stage_identity"):
        return None, {}
    try:
        origin_identity = _stat_fence(os.lstat(original))
    except OSError:
        return None, {}
    if origin_identity != entry.get("original_identity"):
        return None, {}
    sha = entry.get("sha256")
    if not isinstance(sha, str) or len(sha) != 64:
        return None, {}
    return sha, {"stage_identity": stage_identity,
                 "original_identity": origin_identity, "memo": True}


def _valid_run_id(run_id: str) -> bool:
    """Whether ``run_id`` names one directory, never a path escape."""

    return (bool(run_id) and "/" not in run_id and run_id not in (".", "..")
            and "\\" not in run_id and "\x00" not in run_id)


def _reference_census(queue: pool.PoolQueue, *, stage: Path, tier_id: str,
                      cas_root: str | Path | None,
                      residency_root: str | Path | None) -> set[str]:
    """Resolve receipts, plans, consumers and orphan leases under the lock.

    Fragments and reader pins have their existing strict census. This census
    adds records that can name bytes without a surviving fragment.
    """
    named: set[str] = set()
    roots = (stage, stage.resolve(strict=True))
    layouts: dict[tuple[str, str], tuple[str, list[dict[str, object]]]] = {}

    def direct(value):
        if isinstance(value, dict):
            for item in value.values():
                direct(item)
        elif isinstance(value, list):
            for item in value:
                direct(item)
        elif isinstance(value, str) and value.startswith("/"):
            path = Path(os.path.normpath(value))
            if any(root in path.parents for root in roots):
                named.add(str(path))

    def manifests(value, own_cas):
        if isinstance(value, list):
            for item in value:
                manifests(item, own_cas)
            return
        if not isinstance(value, dict):
            return
        digest = value.get("manifest_sha256")
        declaration = value.get("data_manifest")
        if isinstance(declaration, dict):
            input_ref = declaration.get("input")
            if isinstance(input_ref, dict):
                digest = input_ref.get("sha256")
        if digest is not None:
            if (not isinstance(digest, str) or len(digest) != 64
                    or any(char not in "0123456789abcdef" for char in digest)):
                raise ValueError("reference names an invalid manifest digest")
            key = (own_cas, digest)
            if key not in layouts:
                blob = pb.PrismaBuildCAS(Path(own_cas)).blob_path(digest)
                proof = extent_digest(blob)
                if proof is None or proof[0] != digest:
                    raise ValueError(f"reference manifest {digest} is unreadable")
                layout = stage_release._cached_manifest_layout(own_cas, digest)
                if layout is None:
                    raise ValueError(f"reference manifest {digest} is invalid")
                layouts[key] = layout
            prefix, entries = layouts[key]
            start = value.get("range_start_bytes", 0)
            end = value.get("range_end_bytes",
                            sum(int(entry["bytes"]) for entry in entries))
            if (stage_release._bounded_int(start) is None
                    or stage_release._bounded_int(end) is None or end < start):
                raise ValueError("reference names an invalid extent")
            for entry in prewarm_loop.entries_between(entries, start, end):
                relative = stage_relative(
                    str(entry["path"]), int(entry["offset"]), int(entry["bytes"]),
                    mount_prefix=prefix)
                named.update(str(root / relative) for root in roots)
        for item in value.values():
            if isinstance(item, (dict, list)):
                manifests(item, own_cas)

    def record(path):
        value = pool._read_json_bounded(path, max_bytes=128 << 20)
        if not isinstance(value, dict) or not isinstance(value.get("schema"), str):
            raise ValueError(f"{path}: reference record is unreadable")
        direct(value)
        own_cas = str(cas_root or value.get("cas_root") or queue.root.parent / "cas")
        manifests(value, own_cas)
        return value, own_cas

    def tree(root):
        try:
            with os.scandir(root) as entries:
                paths = sorted((Path(entry.path) for entry in entries))
        except FileNotFoundError:
            return
        for path in paths:
            if path.name.startswith("."):
                continue
            mode = os.lstat(path).st_mode
            if statmod.S_ISDIR(mode):
                yield from tree(path)
            elif path.suffix in (".json", ".lease"):
                if not statmod.S_ISREG(mode):
                    raise ValueError(f"{path}: reference is not a regular file")
                yield path

    for root in (queue.root / pool.MOVERS, queue.root / pool.MOVERS_RETIRED,
                 queue.root / pool.RESIDENCY_PLANS):
        for path in tree(root):
            record(path)
    for path in tree(Path(residency_root or queue.root / pool.RESIDENCY)):
        value = pool._read_json_bounded(path, max_bytes=128 << 20)
        if not isinstance(value, dict) or not isinstance(value.get("schema"), str):
            raise ValueError(f"{path}: reference record is unreadable")
        direct(value)
    for state in (pool.READY, pool.CLAIMED):
        for path in tree(queue.dir(state)):
            value, own_cas = record(path)
            key = value.get("action_key")
            if not isinstance(key, str):
                raise ValueError(f"{path}: reference names no action")
            request, why = stage_release._validated_sealed_request(own_cas, key)
            if request is None:
                raise ValueError(f"{path}: {why}")
            direct(request)
            manifests(request.get("params"), own_cas)
    ledger = queue.tier_ledger(tier_id)
    for key in ledger.held_keys():
        if not ledger.holder_tokens(key).get("stage_gib", 0):
            continue
        receipt = pool._read_json_bounded(queue.move_path(key), max_bytes=128 << 20)
        if isinstance(receipt, dict):
            direct(receipt)
            manifests(receipt, str(cas_root or queue.root.parent / "cas"))
            if receipt.get("manifest_sha256") or receipt.get("entries"):
                continue
        raise ValueError(f"{key}: held stage scope is unreadable")
    return named


def _proof_current(pair: dict[str, object], stage: Path) -> None:
    """Refuse changed copies, changed originals, or source path substitution."""
    copy = Path(str(pair["stage_path"]))
    original = Path(str(pair["original_path"]))
    _safe_tree_path(copy, stage)
    for path, field in ((copy, "stage_identity"), (original, "original_identity")):
        info = os.lstat(path)
        if not statmod.S_ISREG(info.st_mode) or _stat_fence(info) != pair[field]:
            raise ValueError(f"{path}: identity changed")
    resolved = original.resolve(strict=True)
    if resolved == stage or stage in resolved.parents:
        raise ValueError(f"{original}: original resolves into the stage")
    if stage not in copy.resolve(strict=True).parents:
        raise ValueError(f"{copy}: copy resolves outside the stage")


def reclaim_source_marks(queue: pool.PoolQueue, *, tier_id: str,
                         stage_root: str, mount_prefix: str,
                         quarantine_root: str | None = None,
                         run_id: str | None = None,
                         apply: bool = False,
                         max_read_gib: float = 64.0,
                         memo_path: str | Path | None = None,
                         memo_out: str | Path | None = None,
                         cas_root: str | Path | None = None,
                         residency_root: str | Path | None = None,
                         ) -> dict[str, object]:
    """Move digest-proven source-mark-only copies to quarantine.

    The dry run (``apply=False``) lists every source-mark-only copy
    with its original, extent, digest or refusal reason. It changes
    nothing. Apply copies each proven pair to quarantine, checks
    the digest there, journals the move, then removes the stage
    copy. Nothing is deleted: a crash between the copy and the
    removal leaves the bytes in both places, and the journal names
    every moved file for ``restore_run``.

    The proofs run outside the stage ownership lock; the lock
    covers the fresh ownership censuses, the identity re-checks
    and the moves only (#988). ``memo_path`` names a dry run's
    memo: an apply reuses a digest only while both files still
    fence the identities it was proven under, and re-hashes else.
    ``memo_out`` writes this run's memo beside a dry run's receipt,
    so one hashing serves both passes.
    """

    stage = Path(stage_root)
    receipt: dict[str, object] = {
        "schema": pool.POOL_EGRESS_SCHEMA_V1,
        "event": RECLAIM_EVENT,
        "action_key": "",
        "consumer_action_key": "",
        "tier_id": tier_id,
        "stage_root": str(stage),
        "mount_prefix": mount_prefix,
        "reason": "reclaim-source-marks",
        "applied": bool(apply),
        "run_id": run_id or "",
        "quarantine_root": quarantine_root or "",
        "complete": True,
        "errors": [],
        "host": socket.gethostname(),
        "unix": time.time(),
    }
    if not mount_prefix or not str(mount_prefix).startswith("/"):
        return _refusal(stage, "mount_prefix must be an absolute path",
                        tier_id=tier_id)
    if apply and not quarantine_root:
        return _refusal(stage, "apply needs a quarantine root",
                        tier_id=tier_id)
    if run_id is not None and not _valid_run_id(run_id):
        return _refusal(stage, f"run_id {run_id!r} is not one directory",
                        tier_id=tier_id)
    try:
        memo = read_memo(memo_path) if memo_path is not None else {}
    except (OSError, ValueError) as exc:
        return _refusal(stage, f"memo unreadable: {exc}", tier_id=tier_id)
    refusal = stage_release.stage_root_refusal(queue, stage)
    if refusal is not None:
        receipt["event"] = stage_release.STAGE_ROOT_REFUSED_EVENT
        receipt["skipped"] = refusal
        receipt["complete"] = False
        receipt["errors"] = [refusal]
        return receipt
    try:
        stage_resolved = stage.resolve(strict=True)
    except OSError as exc:
        return _refusal(stage, f"stage_root_unreadable: {exc}",
                        tier_id=tier_id)
    if memo_out is not None:
        target = Path(memo_out).resolve()
        if target == stage_resolved or stage_resolved in target.parents:
            return _refusal(stage, "memo output must be outside the stage", tier_id=tier_id)
    quarantine: Path | None = None
    if quarantine_root is not None:
        quarantine = Path(quarantine_root)
        if apply:
            try:
                quarantine_resolved = quarantine.resolve(strict=True)
            except OSError as exc:
                return _refusal(stage,
                                f"quarantine_root_unreadable: {exc}",
                                tier_id=tier_id)
            if (quarantine_resolved == stage_resolved
                    or stage_resolved in quarantine_resolved.parents
                    or quarantine_resolved in stage_resolved.parents):
                return _refusal(
                    stage, "quarantine root must be outside the stage",
                    tier_id=tier_id)
            try:
                if not statmod.S_ISDIR(os.lstat(quarantine_resolved).st_mode):
                    return _refusal(stage, "quarantine root is not a directory",
                                    tier_id=tier_id)
                if os.stat(quarantine_resolved).st_dev == os.stat(
                        stage_resolved).st_dev:
                    return _refusal(
                        stage, "quarantine must be on another device",
                        tier_id=tier_id)
                quarantine = quarantine_resolved
            except OSError as exc:
                return _refusal(stage, f"quarantine_unstatable: {exc}",
                                tier_id=tier_id)
    active = run_id or time.strftime("%Y%m%dT%H%M%S", time.gmtime())
    if not _valid_run_id(active):
        return _refusal(stage, f"run_id {active!r} is not one directory",
                        tier_id=tier_id)
    own_cas = (str(cas_root) if cas_root is not None
               else str(queue.root.parent / "cas"))
    census_memo = stage_release._CensusMemo()
    census_started = time.perf_counter()
    hinted: set[str] = set()
    try:
        hinted, _hint_taint = stage_release._attributed_census(
            queue, wanted=None, residency_root=residency_root,
            memo=census_memo)
        hint_pins, _hint_pin_taint = stage_release.reader_lease.live_for(
            queue, None, residency_root=residency_root,
            memo=census_memo.pins)
        hinted |= set(hint_pins)
        stage_release._claimed_paths(queue, tier_id, own_cas,
                                     memo=census_memo)
        stage_release._claimed_source_paths(queue, stage, own_cas,
                                            memo=census_memo)
    except (OSError, ValueError, pool.PoolContractError):
        pass
    census_s = time.perf_counter() - census_started
    # The walk and the proofs run without the lock. The walk names
    # candidates; the proofs hash each copy against its original.
    # Nothing decided here is acted on: under the lock every pair is
    # judged again against fresh ownership, and moved only while
    # both files still fence the proven identities.
    found, walk_skipped, walk_bytes, walk_errors = _walk_source_marks(stage, set())
    try:
        read_budget = int(float(max_read_gib) * (1024 ** 3))
    except (TypeError, ValueError, OverflowError):
        return _refusal(stage, f"max_read_gib {max_read_gib!r} is not a size",
                        tier_id=tier_id)
    if read_budget < 0:
        return _refusal(stage, "max_read_gib must not be negative", tier_id=tier_id)
    proven: dict[str, dict[str, object]] = {}
    refused_early: dict[str, tuple[str, dict[str, object]]] = {}
    read_used = memo_hits = proof_read_reserved = apply_read_reserved = 0
    hash_started = time.perf_counter()
    for path in found:
        try:
            stage_rel = str(path.relative_to(stage))
        except ValueError:
            continue
        if stage_rel.startswith("produced-output/"):
            refused_early[stage_rel] = ("produced_output_lane", {})
            continue
        derived = original_for(stage, path, mount_prefix=mount_prefix,
                               stage_resolved=stage_resolved)
        if derived is None:
            refused_early[stage_rel] = ("unpaired_name", {})
            continue
        original, offset, size = derived
        sha, hit_detail = (_memo_hit(memo, stage, stage_rel, original, offset, size)
                           if apply else (None, {}))
        proof_read = 0 if sha is not None else 2 * size
        move_read = 3 * size if apply else 0
        if proof_read_reserved + apply_read_reserved + proof_read + move_read > read_budget:
            refused_early[stage_rel] = (
                "read_budget_exceeded",
                {"original_path": original, "offset": offset, "size": size})
            continue
        proof_read_reserved += proof_read
        if sha is not None:
            memo_hits += 1
            apply_read_reserved += move_read
            proven[stage_rel] = {
                "stage_path": str(path), "stage_rel": stage_rel,
                "status": "paired", "original_path": original,
                "offset": offset, "size": size, "sha256": sha,
                "stage_identity": hit_detail["stage_identity"],
                "original_identity": hit_detail["original_identity"],
                "memo": True,
            }
            continue
        sha, status, detail = prove_pair(path, original, offset, size)
        read_used += int(detail.get("bytes_hashed", 0))
        if status != "paired" or sha is None:
            extra: dict[str, object] = {"original_path": original,
                                        "offset": offset, "size": size}
            for key, value in detail.items():
                if key != "bytes_hashed":
                    extra[key] = value
            refused_early[stage_rel] = (status, extra)
            continue
        apply_read_reserved += move_read
        proven[stage_rel] = {
            "stage_path": str(path), "stage_rel": stage_rel,
            "status": "paired", "original_path": original,
            "offset": offset, "size": size, "sha256": sha,
            "stage_identity": detail["stage_identity"],
            "original_identity": detail["original_identity"],
        }
    hash_s = time.perf_counter() - hash_started
    run_dir: Path | None = None
    if apply and quarantine is not None:
        run_dir = quarantine / active
        try:
            run_dir.mkdir()
            resident_sets.fsync_directory(quarantine)
        except OSError as exc:
            return _refusal(stage, f"quarantine run dir refused: {exc}",
                            tier_id=tier_id)
        try:
            volume = os.statvfs(run_dir)
            room = volume.f_bavail * volume.f_frsize
        except OSError as exc:
            return _refusal(stage, f"quarantine capacity unreadable: {exc}",
                            tier_id=tier_id)
        need = sum(int(one["size"]) for one in proven.values())
        if room < need:
            return _refusal(
                stage, f"quarantine holds {room} bytes, needs {need}",
                tier_id=tier_id)

    pruned: list[Path] = []

    def _held_reclaim() -> dict[str, object]:
        parse_counts = census_memo.counts()
        validate_started = time.perf_counter()
        try:
            in_flight = stage_release.movers_in_flight(queue, tier_id=tier_id)
        except (OSError, pool.PoolContractError, ValueError) as exc:
            receipt["skipped"] = "queue_census_unreadable"
            receipt["complete"] = False
            receipt["errors"] = [str(exc)]
            return receipt
        if in_flight:
            receipt["skipped"] = "movers_in_flight"
            receipt["movers_in_flight"] = sorted(in_flight)
            receipt["complete"] = False
            receipt["errors"] = ["movers_in_flight"]
            return receipt
        attributed, attribution_taint = stage_release._attributed_census(
            queue, wanted=None, residency_root=residency_root,
            memo=census_memo)
        if attribution_taint:
            receipt["skipped"] = "attribution_unreadable"
            receipt["complete"] = False
            receipt["errors"] = [
                f"ownership uncertain: {item}"
                for item in attribution_taint[
                    :stage_release.ATTRIBUTION_TAINT_LIMIT]]
            return receipt
        pin_owners, pin_taint = stage_release.reader_lease.live_for(
            queue, None, residency_root=residency_root,
            memo=census_memo.pins)
        if pin_taint:
            receipt["skipped"] = "pin_census_unreadable"
            receipt["complete"] = False
            receipt["errors"] = [f"ownership uncertain: {item}"
                                 for item in pin_taint]
            return receipt
        attributed |= set(pin_owners)
        claimed, claim_taint = stage_release._claimed_paths(
            queue, tier_id, own_cas, memo=census_memo)
        if claim_taint:
            receipt["skipped"] = "claim_census_unreadable"
            receipt["complete"] = False
            receipt["errors"] = [f"ownership uncertain: {item}"
                                 for item in claim_taint]
            return receipt
        for relative in claimed:
            attributed.add(os.path.normpath(os.path.join(str(stage),
                                                         str(relative))))
        handoffs, handoff_taint = stage_release._claimed_source_paths(
            queue, stage, own_cas, memo=census_memo)
        if handoff_taint:
            receipt["skipped"] = "promotion_census_unreadable"
            receipt["complete"] = False
            receipt["errors"] = [f"ownership uncertain: {item}"
                                 for item in handoff_taint]
            return receipt
        attributed |= {os.path.normpath(str(one)) for one in handoffs}
        try:
            # Read, not used per file: the ledger names action keys,
            # never paths, so it cannot attribute a copy. The read
            # still fences the pass: an unreadable ledger is unknown
            # ownership, and the pass refuses on it.
            queue.tier_ledger(tier_id).held_keys()
        except (OSError, pool.PoolContractError) as exc:
            receipt["skipped"] = f"ledger_unreadable: {exc}"
            receipt["complete"] = False
            receipt["errors"] = [f"ledger_unreadable: {exc}"]
            return receipt
        try:
            attributed |= _reference_census(
                queue, stage=stage, tier_id=tier_id, cas_root=cas_root,
                residency_root=residency_root)
        except (OSError, pool.PoolContractError, pb.PrismaBuildError,
                ValueError, TypeError) as exc:
            receipt["skipped"] = "reference_census_unreadable"
            receipt["complete"] = False
            receipt["errors"] = [str(exc)]
            return receipt
        receipt["census_validate_s"] = round(
            time.perf_counter() - validate_started, 6)
        receipt.update(stage_release._locked_parse_record(
            census_memo, parse_counts))

        def _named(stage_rel: str) -> bool:
            """Whether fresh ownership names one staged relative path."""

            for form in (os.path.normpath(os.path.join(str(stage),
                                                       stage_rel)),
                         os.path.normpath(os.path.join(str(stage_resolved),
                                                       stage_rel))):
                if form in attributed or any(
                        str(parent) in attributed for parent in Path(form).parents):
                    return True
            return False

        rows: list[dict[str, object]] = []
        refused: dict[str, int] = {}

        def _refuse_row(stage_rel: str, why: str,
                        **extra: object) -> None:
            refused[why] = refused.get(why, 0) + 1
            row: dict[str, object] = {
                "stage_path": str(stage / stage_rel),
                "stage_rel": stage_rel,
                "status": why,
            }
            row.update(extra)
            rows.append(row)

        for skipped in walk_skipped:
            try:
                rel = str(skipped.relative_to(stage))
            except ValueError:
                continue
            if _named(rel):
                _refuse_row(rel, "attributed")
        pairs: list[dict[str, object]] = []
        for stage_rel in sorted(set(proven) | set(refused_early)):
            if stage_rel in refused_early:
                why, extra = refused_early[stage_rel]
                _refuse_row(stage_rel, why, **extra)
                continue
            pair = proven[stage_rel]
            if _named(stage_rel):
                _refuse_row(stage_rel, "attributed",
                            original_path=pair["original_path"],
                            offset=pair["offset"], size=pair["size"],
                            sha256=pair["sha256"])
                continue
            try:
                _proof_current(pair, stage_resolved)
            except (OSError, ValueError) as exc:
                _refuse_row(stage_rel, "proof_changed", error=str(exc))
                continue
            proof_receipt = {
                "schema": "prismabuild.stage-source-mark-proof.v1",
                **{key: pair[key] for key in (
                    "stage_path", "original_path", "offset", "size", "sha256",
                    "stage_identity", "original_identity")},
            }
            pair["proof_receipt"] = proof_receipt
            pair["proof_receipt_sha256"] = pb.canonical_sha256(proof_receipt)
            rows.append(dict(pair))
            pairs.append(pair)
        receipt["entries_scanned"] = len(found) + len(walk_skipped)
        receipt["walk_bytes"] = walk_bytes
        receipt["bytes_hashed"] = read_used
        receipt["read_budget_bytes"] = read_budget
        receipt["apply_read_reserved_bytes"] = apply_read_reserved
        receipt["proof_read_reserved_bytes"] = proof_read_reserved
        receipt["estimated_read_bytes"] = 2 * walk_bytes
        receipt["hash_s"] = round(hash_s, 6)
        receipt["memo_hits"] = memo_hits
        receipt["entries"] = rows
        receipt["entries_paired"] = len(pairs)
        receipt["bytes_paired"] = sum(int(one["size"]) for one in pairs)
        receipt["entries_refused"] = sum(refused.values())
        receipt["refused_reasons"] = dict(sorted(refused.items()))
        receipt["walk_errors"] = list(walk_errors)
        if walk_errors:
            receipt["complete"] = False
            receipt["errors"] = list(walk_errors)
            if apply:
                return receipt
        if not apply:
            if memo_out is not None:
                try:
                    memo_target = Path(memo_out)
                    memo_target.parent.mkdir(parents=True, exist_ok=True)
                    write_memo_file(memo_target,
                                    [dict(one) for one in pairs])
                    receipt["memo"] = str(memo_target)
                except OSError as exc:
                    receipt["complete"] = False
                    receipt["errors"] = [f"memo_out refused: {exc}"]
            return receipt
        if quarantine is None or run_dir is None:
            receipt["skipped"] = "apply needs a quarantine root"
            receipt["complete"] = False
            receipt["errors"] = ["apply needs a quarantine root"]
            return receipt
        try:
            journaled = _read_journal(run_dir)
        except (OSError, ValueError) as exc:
            receipt["skipped"] = f"journal unreadable: {exc}"
            receipt["complete"] = False
            receipt["errors"] = [str(receipt["skipped"])]
            return receipt
        if journaled:
            receipt["skipped"] = (
                f"run {active} already journaled "
                f"{len(journaled)} move(s); use a fresh run_id")
            receipt["complete"] = False
            receipt["errors"] = [str(receipt["skipped"])]
            return receipt
        receipt["restore_command"] = shlex.join([
            sys.executable, str(Path(__file__).resolve()),
            "--pool-root", str(queue.root), "--tier-id", tier_id,
            "--stage-root", str(stage), "--mount-prefix", mount_prefix,
            "--quarantine-root", str(quarantine), "--restore", active])
        manifest_entries: list[dict[str, object]] = []
        moved = moved_bytes = already_gone = 0
        errors: list[str] = []
        for pair in pairs:
            rel = str(pair["stage_rel"])
            stage_path = stage / rel
            original = str(pair["original_path"])
            size = int(pair["size"])
            sha = str(pair["sha256"])
            try:
                _proof_current(pair, stage_resolved)
            except (OSError, ValueError) as exc:
                errors.append(f"{rel}: {exc}")
                continue
            if _named(rel):
                errors.append(f"{rel}: attributed")
                continue
            if _source_mark_only(stage_path) is not True:
                errors.append(f"{rel}: mark changed")
                continue
            try:
                if stage_resolved not in stage_path.resolve().parents:
                    errors.append(f"{rel}: outside the owned stage")
                    continue
            except OSError as exc:
                errors.append(f"{rel}: {exc}")
                continue
            dest = _quarantine_dest(quarantine, active, rel)
            try:
                _mkdir_durable(dest.parent)
                _safe_tree_path(dest, quarantine)
            except (OSError, ValueError) as exc:
                errors.append(f"{rel}: {exc}")
                continue
            captured = _xattrs_and_times(stage_path)
            if captured is None:
                errors.append(f"{rel}: metadata unreadable")
                continue
            xattrs, times = captured
            got = _copy_to_quarantine(stage_path, dest, size)
            if got != sha:
                errors.append(f"{rel}: quarantine copy failed")
                continue
            try:
                _apply_xattrs_and_times(dest, xattrs, times)
                if not _metadata_matches(dest, xattrs, times):
                    raise ValueError("quarantine metadata differs")
                _safe_tree_path(dest, quarantine)
                quarantine_identity = _stat_fence(os.lstat(dest))
            except (OSError, ValueError) as exc:
                errors.append(f"{rel}: {exc}")
                continue
            entry = {
                "stage_path": str(stage_path),
                "stage_rel": rel,
                "original_path": original,
                "offset": int(pair["offset"]),
                "size": size,
                "sha256": sha,
                "xattrs": xattrs,
                "times": times,
                "quarantine_path": str(dest),
                "quarantine_identity": quarantine_identity,
                "proof_receipt": pair["proof_receipt"],
                "proof_receipt_sha256": pair["proof_receipt_sha256"],
                "restore_command": receipt["restore_command"],
            }
            # The journal is write-ahead: it names the move before the
            # unlink performs it. A journaled file still on stage is
            # benign -- a later restore skips it as identical -- while
            # a moved file no record names would be unmapped.
            try:
                _append_journal(run_dir, entry)
            except OSError as exc:
                errors.append(f"{rel}: journal refused: {exc}")
                break
            try:
                _proof_current(pair, stage_resolved)
                origin_check = extent_digest(
                    original, offset=int(pair["offset"]), length=size)
                if origin_check != (sha, size):
                    raise ValueError("original digest changed")
                _proof_current(pair, stage_resolved)
                fresh = _reference_census(
                    queue, stage=stage, tier_id=tier_id, cas_root=cas_root,
                    residency_root=residency_root)
                attributed.update(fresh)
                if (_named(rel) or stage_release.movers_in_flight(
                        queue, tier_id=tier_id)):
                    raise ValueError("reference changed")
                _safe_tree_path(dest, quarantine)
                if _stat_fence(os.lstat(dest)) != quarantine_identity:
                    raise ValueError("quarantine copy changed")
                if _source_mark_only(stage_path) is not True:
                    raise ValueError("stage mark changed")
                _proof_current(pair, stage_resolved)
                os.unlink(stage_path)
                resident_sets.fsync_directory(stage_path.parent)
            except (OSError, ValueError, pool.PoolContractError,
                    pb.PrismaBuildError) as exc:
                errors.append(f"{rel}: {exc}")
                continue
            manifest_entries.append(entry)
            pruned.append(stage_path.parent)
            moved += 1
            moved_bytes += size
        try:
            write_manifest(run_dir, manifest_entries)
            write_memo(run_dir, pairs)
        except OSError as exc:
            errors.append(f"manifest refused: {exc}")
        receipt["entries_moved"] = moved
        receipt["bytes_moved"] = moved_bytes
        receipt["entries_already_gone"] = already_gone
        receipt["manifest"] = str(run_dir / MANIFEST_NAME)
        receipt["manifest_entries"] = len(manifest_entries)
        receipt["memo"] = str(run_dir / MEMO_NAME)
        receipt["errors"] = list(walk_errors) + errors
        receipt["complete"] = not errors and not walk_errors
        if errors or walk_errors:
            receipt["skipped"] = "apply_partial"
        return receipt

    asked = time.perf_counter()
    with stage_release._stage_ownership(
            queue, stage, role="reclaim-source-marks") as granted:
        result = _held_reclaim()
    released = time.perf_counter()
    if "entries" not in result:
        result["entries"] = [
            {**proven.get(rel, refused_early.get(rel, ("", {}))[1]),
             "stage_path": str(stage / rel), "stage_rel": rel,
             "status": result.get("skipped", "pass_refused")}
            for rel in sorted(set(proven) | set(refused_early))]
        result["entries_scanned"] = len(found)
        result["entries_paired"] = 0
        result["entries_refused"] = len(result["entries"])
        result["bytes_hashed"] = read_used
        result["estimated_read_bytes"] = 2 * walk_bytes
        result["read_budget_bytes"] = read_budget
    prune_started = time.perf_counter()
    for parent in sorted(set(pruned), key=lambda one: len(one.parts),
                         reverse=True):
        stage_release._prune_empty(parent, stage)
    result.update(stage_release._hold_record(
        lock_wait_s=granted - asked, lock_held_s=released - granted,
        census_s=census_s, prune_s=time.perf_counter() - prune_started))
    return result


def restore_run(queue: pool.PoolQueue, *, stage_root: str,
                quarantine_root: str, run_id: str) -> dict[str, object]:
    """Copy one quarantine run back onto the stage, refusing overwrites.

    Reads the final manifest, or the journal when a crash stopped
    the apply before it. Holds the stage ownership lock for the
    copies, as any writer under the stage does.
    """

    stage = Path(stage_root)
    receipt: dict[str, object] = {
        "schema": pool.POOL_EGRESS_SCHEMA_V1,
        "event": "stage-source-mark-restored",
        "action_key": "",
        "consumer_action_key": "",
        "tier_id": "",
        "stage_root": str(stage),
        "run_id": run_id,
        "reason": "restore-source-marks",
        "complete": True,
        "errors": [],
        "host": socket.gethostname(),
        "unix": time.time(),
    }
    if not _valid_run_id(run_id):
        receipt["complete"] = False
        receipt["errors"] = [f"run_id {run_id!r} is not one directory"]
        return receipt
    run_dir = Path(quarantine_root) / run_id
    try:
        journaled = _read_journal(run_dir)
    except (OSError, ValueError) as exc:
        receipt["complete"] = False
        receipt["errors"] = [f"journal unreadable: {exc}"]
        return receipt
    try:
        manifest = read_manifest(run_dir)
        entries: list[object] = list(manifest["entries"])
        receipt["manifest"] = str(run_dir / MANIFEST_NAME)
    except FileNotFoundError:
        entries = list(journaled)
        receipt["manifest"] = str(run_dir / JOURNAL_NAME)
    except (OSError, ValueError) as exc:
        receipt["complete"] = False
        receipt["errors"] = [str(exc)]
        return receipt
    combined: dict[str, dict[str, object]] = {}
    for entry in [*entries, *journaled]:
        if not isinstance(entry, dict) or not isinstance(entry.get("stage_rel"), str):
            receipt["complete"] = False
            receipt["errors"] = ["restore record names no stage path"]
            return receipt
        rel = entry["stage_rel"]
        if rel in combined and combined[rel] != entry:
            receipt["complete"] = False
            receipt["errors"] = [f"{rel}: restore records conflict"]
            return receipt
        combined[rel] = entry
    entries = list(combined.values())
    refusal = stage_release.stage_root_refusal(queue, stage)
    if refusal is not None:
        receipt["event"] = stage_release.STAGE_ROOT_REFUSED_EVENT
        receipt["complete"] = False
        receipt["errors"] = [refusal]
        return receipt
    try:
        stage_resolved = stage.resolve(strict=True)
    except OSError as exc:
        receipt["complete"] = False
        receipt["errors"] = [f"stage_root_unreadable: {exc}"]
        return receipt
    try:
        run_resolved = run_dir.resolve()
    except OSError as exc:
        receipt["complete"] = False
        receipt["errors"] = [f"quarantine run unreadable: {exc}"]
        return receipt

    def _held_restore() -> None:
        restored = restored_bytes = skipped = 0
        errors: list[str] = []
        for raw in entries:
            if not isinstance(raw, dict):
                errors.append("manifest entry is not an object")
                continue
            try:
                stage_rel = str(raw["stage_rel"])
                quarantine_path = str(raw["quarantine_path"])
                size = int(raw["size"])
                sha = str(raw["sha256"])
                xattrs = {str(name): str(hexed)
                          for name, hexed in dict(
                              raw.get("xattrs") or {}).items()}
                times = {str(name): int(value)
                         for name, value in dict(
                             raw.get("times") or {}).items()}
            except (KeyError, TypeError, ValueError) as exc:
                errors.append(f"manifest entry unreadable: {exc}")
                continue
            if (not stage_rel or stage_rel == "." or os.path.isabs(stage_rel)
                    or ".." in Path(stage_rel).parts or size <= 0
                    or "mtime_ns" not in times or "atime_ns" not in times
                    or prewarm_loop.STAGE_SOURCE_XATTR not in xattrs):
                errors.append(f"{stage_rel}: manifest names an unsafe path")
                continue
            try:
                quarantined = Path(quarantine_path).resolve()
            except OSError as exc:
                errors.append(f"{stage_rel}: {exc}")
                continue
            if (quarantined != run_resolved
                    and run_resolved not in quarantined.parents):
                errors.append(
                    f"{stage_rel}: quarantine path outside the run")
                continue
            dest = stage / stage_rel
            try:
                _safe_tree_path(dest, stage_resolved)
                _safe_tree_path(Path(quarantine_path), run_resolved)
                resolved_parent = dest.parent.resolve()
                if (resolved_parent != stage_resolved
                        and stage_resolved not in resolved_parent.parents):
                    errors.append(f"{stage_rel}: outside the stage root")
                    continue
            except (OSError, ValueError) as exc:
                errors.append(f"{stage_rel}: {exc}")
                continue
            if dest.exists() or os.path.lexists(dest):
                # A second restore is a no-op when the file already
                # proves identical; a conflicting file refuses.
                check = extent_digest(dest)
                if (check is not None and check == (sha, size)
                        and _metadata_matches(dest, xattrs, times, check_atime=False)):
                    skipped += 1
                    continue
                errors.append(f"{stage_rel}: destination exists")
                continue
            check = extent_digest(quarantine_path)
            if check is None or check[1] != size or check[0] != sha:
                errors.append(f"{stage_rel}: quarantine copy unreadable")
                continue
            got = _copy_to_quarantine(
                Path(quarantine_path), dest, size, metadata=(xattrs, times),
                expected_digest=sha)
            if got != sha:
                errors.append(f"{stage_rel}: restore copy failed")
                continue
            restored += 1
            restored_bytes += size
        receipt["entries_restored"] = restored
        receipt["bytes_restored"] = restored_bytes
        receipt["entries_skipped_identical"] = skipped
        receipt["manifest_entries"] = len(entries)
        receipt["errors"] = errors
        receipt["complete"] = not errors

    asked = time.perf_counter()
    with stage_release._stage_ownership(
            queue, stage, role="restore-source-marks") as granted:
        _held_restore()
    released = time.perf_counter()
    result_hold = stage_release._hold_record(
        lock_wait_s=granted - asked, lock_held_s=released - granted,
        census_s=0.0)
    receipt.update(result_hold)
    return receipt


def _emit_receipt(receipt: dict[str, object], path: str | None) -> int:
    """File the receipt where asked, print it, and return the exit status."""

    if path:
        with open(path, "w") as stream:
            json.dump(receipt, stream, indent=1, sort_keys=True)
            stream.write("\n")
    print(pb.sorted_json(receipt))
    return 0 if receipt["complete"] else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="reclaim digest-proven source-mark-only stage copies "
                    "to quarantine (#1636)")
    parser.add_argument("--pool-root", required=True,
                        help="the pull queue root: its ledgers, claims and "
                             "movers are read, and an --apply or --restore "
                             "receipt is filed under it")
    parser.add_argument("--action-key", default=None,
                        help="this node's own action key, which files an "
                             "--apply or --restore receipt in the queue; "
                             f"defaults to {pb.ACTION_KEY_ENV}, which the "
                             "launcher sets. A dry run files nothing")
    parser.add_argument("--tier-id", required=True,
                        help="the stage tier whose ledger and held scopes "
                             "this pass checks")
    parser.add_argument("--stage-root", required=True,
                        help="the staging dataset's mountpoint; nothing "
                             "outside it is ever moved")
    parser.add_argument("--mount-prefix", required=True,
                        help="the source mount staged names resolve "
                             "against")
    parser.add_argument("--quarantine-root", default=None,
                        help="required with --apply: outside the stage, "
                             "on another device; must already exist, and "
                             "must hold the batch")
    parser.add_argument("--run-id", default=None,
                        help="one directory name under the quarantine "
                             "root; default a UTC timestamp")
    parser.add_argument("--apply", action="store_true",
                        help="copy proven pairs to quarantine and remove "
                             "the stage copies; omit for a dry run")
    parser.add_argument("--max-read-gib", type=float, default=64.0,
                        help="most payload GiB one pass may read for "
                             "proofs and, with --apply, copies; a pair "
                             "past it is refused as read_budget_exceeded "
                             "(default %(default)s)")
    parser.add_argument("--memo", default=None,
                        help="a dry run's memo: reuse a digest only "
                             "while both files still fence its identities")
    parser.add_argument("--memo-out", default=None,
                        help="write this run's memo here, so one hashing "
                             "serves the dry run and the apply")
    parser.add_argument("--cas-root", default=None,
                        help="where sealed requests are read "
                             "(default <pool-root>/../cas)")
    parser.add_argument("--residency-root", default=None,
                        help="where residency-map fragments are filed "
                             "(default <pool-root>/residency)")
    parser.add_argument("--receipt", default=None,
                        help="also write the receipt here, outside the "
                             "stage")
    parser.add_argument("--restore", default=None,
                        help="restore a quarantine run instead of "
                             "reclaiming")
    args = parser.parse_args(argv)
    declared_key = args.action_key or os.environ.get(pb.ACTION_KEY_ENV)
    args.action_key = (stage_release.own_action_key(declared_key)
                       if declared_key else None)
    for output in (args.receipt, args.memo_out):
        if output is not None:
            resolved = Path(output).resolve()
            stage = Path(args.stage_root).resolve()
            if resolved == stage or stage in resolved.parents:
                parser.error("receipt and memo outputs must be outside the stage")
    queue = pool.PoolQueue(Path(args.pool_root))
    if args.restore is not None:
        if args.quarantine_root is None:
            parser.error("--restore needs --quarantine-root")
        receipt = restore_run(queue, stage_root=args.stage_root,
                              quarantine_root=args.quarantine_root,
                              run_id=args.restore)
        if args.action_key:
            queue.record_move(args.action_key, receipt)
        return _emit_receipt(receipt, args.receipt)
    receipt = reclaim_source_marks(
        queue, tier_id=args.tier_id, stage_root=args.stage_root,
        mount_prefix=args.mount_prefix,
        quarantine_root=args.quarantine_root, run_id=args.run_id,
        apply=args.apply, max_read_gib=args.max_read_gib,
        memo_path=args.memo, memo_out=args.memo_out,
        cas_root=args.cas_root, residency_root=args.residency_root)
    if args.apply and args.action_key:
        queue.record_move(args.action_key, receipt)
    return _emit_receipt(receipt, args.receipt)


if __name__ == "__main__":
    raise SystemExit(main())
