#!/usr/bin/env python3
"""Reclaim source-mark-only stage copies with digest-proven originals (#1636).

A staged range lives at ``<rel>.pbrange/<offset>-<size>``, so the copy
names its own original: ``<mount>/<rel>`` at extent ``[offset, size)``.
No receipt names it. Routine ``reconcile`` leaves every such copy for
the life of the fleet.

This verb moves only copies whose whole bytes hash equal to the
original extent, into a quarantine area outside the stage tier. It
deletes nothing. The tier mint exposes the freed room on its next
cycle. ``restore`` copies a run back with its marks.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shlex
import socket
import stat as statmod
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
from runtime_paths import generation_root  # noqa: E402

sys.path.insert(0, str(generation_root(__file__) / "src"))

from prismabuild import pool  # noqa: E402

import prewarm_loop  # noqa: E402
import stage_release  # noqa: E402
from stage_move import RANGE_SUFFIX, STAGE_MOVER_XATTR  # noqa: E402

#: The event each reclaim receipt publishes.
RECLAIM_EVENT = "stage-source-mark-reclaimed"
#: ``manifest.json`` inside each quarantine run directory.
MANIFEST_NAME = "manifest.json"
#: The newest manifest layout this verb reads and writes.
MANIFEST_SCHEMA_V1 = "prismabuild.stage-source-mark-reclaim.v1"
#: Chunk size for digest streams.
_CHUNK = 1 << 20


def _refuse(stage: Path, refusal: str, **fields: object) -> dict[str, object]:
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


def digest_file(path: str | Path, *, offset: int = 0,
                length: int | None = None) -> tuple[str, int] | None:
    """SHA-256 of a file range, and the bytes hashed, or None."""

    digest = hashlib.sha256()
    hashed = 0
    try:
        with open(path, "rb") as handle:
            if offset:
                handle.seek(offset)
            while True:
                want = _CHUNK if length is None else min(
                    _CHUNK, length - hashed)
                if want <= 0:
                    break
                chunk = handle.read(want)
                if not chunk:
                    break
                digest.update(chunk)
                hashed += len(chunk)
    except OSError:
        return None
    return digest.hexdigest(), hashed


def _file_identity(info: os.stat_result) -> dict[str, int]:
    """The stat fields that fence one file's hashed bytes."""

    return {"ino": int(info.st_ino), "size": int(info.st_size),
            "mtime_ns": int(info.st_mtime_ns),
            "ctime_ns": int(getattr(info, "st_ctime_ns", 0))}


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
        stage_before = _file_identity(os.stat(stage_path))
    except OSError as exc:
        return None, "stage_unreadable", {"error": str(exc)}
    stage_got = digest_file(stage_path)
    if stage_got is None:
        return None, "stage_unreadable", dict(detail)
    stage_hex, stage_n = stage_got
    detail["bytes_hashed"] = int(detail["bytes_hashed"]) + stage_n
    try:
        stage_after = _file_identity(os.stat(stage_path))
    except OSError as exc:
        return None, "stage_unreadable", {"error": str(exc)}
    if stage_after != stage_before:
        return None, "copy_changed", dict(detail)
    if stage_n != size:
        detail["copy_bytes"] = stage_n
        return None, "copy_changed", detail
    try:
        origin_before = _file_identity(os.stat(original))
    except FileNotFoundError:
        return None, "original_missing", dict(detail)
    except OSError as exc:
        return None, "original_unreadable", {"error": str(exc)}
    origin_got = digest_file(original, offset=offset, length=size)
    if origin_got is None:
        return None, "original_unreadable", dict(detail)
    origin_hex, origin_n = origin_got
    detail["bytes_hashed"] = int(detail["bytes_hashed"]) + origin_n
    if origin_n != size:
        return None, "original_short", dict(detail)
    # The source may move under the read: the digest counts only when
    # the extent's incarnation is unchanged across it.
    try:
        origin_after = _file_identity(os.stat(original))
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
        os.getxattr(path, STAGE_MOVER_XATTR)
    except OSError as exc:
        if exc.errno not in stage_release._XATTR_ABSENT:
            return None
    else:
        return False
    try:
        os.getxattr(path, prewarm_loop.STAGE_SOURCE_XATTR)
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
        try:
            entries = sorted(os.scandir(directory),
                             key=lambda entry: entry.name)
        except OSError as exc:
            walk_errors.append(f"{directory}: {exc}")
            continue
        for entry in entries:
            if entry.name == stage_release.STAGE_ROOT_MARKER:
                continue
            entry_path = Path(entry.path)
            try:
                if entry.is_symlink():
                    continue
                if entry.is_dir():
                    stack.append(entry_path)
                    continue
            except OSError as exc:
                walk_errors.append(f"{entry.path}: {exc}")
                continue
            if prewarm_loop._STAGE_TEMPORARY.search(entry.name):
                continue
            normalized = os.path.normpath(str(entry_path))
            if normalized in attributed:
                skipped.append(entry_path)
                continue
            kind = _source_mark_only(entry_path)
            if kind is not True:
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


def _copy_to_quarantine(source: Path, dest: Path, size: int) -> str | None:
    """Copy a file to quarantine, fsync it, and check its digest.

    The bytes land on a temporary beside the destination and are
    renamed into place only after they verify, so a crash never
    leaves a partial file wearing a moved file's name.
    """

    try:
        dest.parent.mkdir(parents=True, exist_ok=True)
    except OSError:
        return None
    temporary = dest.parent / f".{dest.name}.{os.getpid()}.tmp"
    digest = hashlib.sha256()
    try:
        with open(source, "rb") as reader, open(temporary, "wb") as writer:
            while True:
                chunk = reader.read(_CHUNK)
                if not chunk:
                    break
                writer.write(chunk)
                digest.update(chunk)
            writer.flush()
            os.fsync(writer.fileno())
    except OSError:
        try:
            temporary.unlink()
        except OSError:
            pass
        return None
    check = digest_file(temporary)
    if check is None or check[1] != size or check[0] != digest.hexdigest():
        try:
            temporary.unlink()
        except OSError:
            pass
        return None
    try:
        os.replace(temporary, dest)
    except OSError:
        try:
            temporary.unlink()
        except OSError:
            pass
        return None
    return digest.hexdigest()


def _xattrs_and_times(path: Path
                      ) -> tuple[dict[str, str], dict[str, int]] | None:
    """One file's marks and times, or None when the marks won't answer.

    A manifest without the source mark restores an unmarked file, and
    ``reconcile`` deletes unmarked files. So unreadable marks refuse
    the file; only the times may fall back to empty.
    """

    xattrs: dict[str, str] = {}
    try:
        names = os.listxattr(path)
    except OSError:
        return None
    for name in names:
        try:
            raw = os.getxattr(path, name)
        except OSError:
            return None
        xattrs[str(name)] = raw.hex()
    try:
        info = os.stat(path, follow_symlinks=False)
        times = {"mtime_ns": int(info.st_mtime_ns),
                 "atime_ns": int(info.st_atime_ns)}
    except OSError:
        times = {}
    return xattrs, times


def _apply_xattrs_and_times(path: Path, xattrs: dict[str, str],
                            times: dict[str, int]) -> None:
    for name, hexed in xattrs.items():
        try:
            os.setxattr(path, name, bytes.fromhex(hexed))
        except OSError:
            continue
    if times:
        try:
            os.utime(path, ns=(int(times.get("atime_ns", 0)),
                               int(times.get("mtime_ns", 0))))
        except (OSError, ValueError):
            pass


def _marks_restored(path: Path, xattrs: dict[str, str]) -> bool:
    """Whether ``path`` carries the manifest's marks after a restore."""

    for name, hexed in xattrs.items():
        try:
            raw = os.getxattr(path, name)
        except OSError:
            return False
        if raw != bytes.fromhex(hexed):
            return False
    return True


def write_manifest(run_dir: Path, entries: list[dict[str, object]]) -> Path:
    manifest = {
        "schema": MANIFEST_SCHEMA_V1,
        "run_id": run_dir.name,
        "unix": time.time(),
        "host": socket.gethostname(),
        "entries": entries,
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
    with open(path, "a") as stream:
        stream.write(json.dumps(entry, sort_keys=True) + "\n")
        stream.flush()
        os.fsync(stream.fileno())


def _read_journal(run_dir: Path) -> list[dict[str, object]]:
    """The journal's entries, oldest first; an absent journal reads empty."""

    path = run_dir / JOURNAL_NAME
    try:
        raw = path.read_text()
    except FileNotFoundError:
        return []
    entries: list[dict[str, object]] = []
    for line in raw.splitlines():
        if not line.strip():
            continue
        entry = json.loads(line)
        if isinstance(entry, dict):
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
        stage_identity = _file_identity(os.stat(stage / stage_rel))
    except OSError:
        return None, {}
    if stage_identity != entry.get("stage_identity"):
        return None, {}
    try:
        origin_identity = _file_identity(os.stat(original))
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
        return _refuse(stage, "mount_prefix must be an absolute path",
                       tier_id=tier_id)
    if apply and not quarantine_root:
        return _refuse(stage, "apply needs a quarantine root",
                       tier_id=tier_id)
    if run_id is not None and not _valid_run_id(run_id):
        return _refuse(stage, f"run_id {run_id!r} is not one directory",
                       tier_id=tier_id)
    try:
        memo = read_memo(memo_path) if memo_path is not None else {}
    except (OSError, ValueError) as exc:
        return _refuse(stage, f"memo unreadable: {exc}", tier_id=tier_id)
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
        return _refuse(stage, f"stage_root_unreadable: {exc}",
                       tier_id=tier_id)
    quarantine: Path | None = None
    if quarantine_root is not None:
        quarantine = Path(quarantine_root)
        if apply:
            try:
                quarantine_resolved = quarantine.resolve()
            except OSError as exc:
                return _refuse(stage,
                               f"quarantine_root_unreadable: {exc}",
                               tier_id=tier_id)
            if (quarantine_resolved == stage_resolved
                    or stage_resolved in quarantine_resolved.parents
                    or quarantine_resolved in stage_resolved.parents):
                return _refuse(
                    stage, "quarantine root must be outside the stage",
                    tier_id=tier_id)
            try:
                if os.stat(quarantine_resolved).st_dev == os.stat(
                        stage_resolved).st_dev:
                    return _refuse(
                        stage, "quarantine must be on another device",
                        tier_id=tier_id)
            except OSError as exc:
                return _refuse(stage, f"quarantine_unstatable: {exc}",
                               tier_id=tier_id)
    active = run_id or time.strftime("%Y%m%dT%H%M%S", time.gmtime())
    if not _valid_run_id(active):
        return _refuse(stage, f"run_id {active!r} is not one directory",
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
    except (OSError, ValueError):
        pass
    census_s = time.perf_counter() - census_started
    # The walk and the proofs run without the lock. The walk names
    # candidates; the proofs hash each copy against its original.
    # Nothing decided here is acted on: under the lock every pair is
    # judged again against fresh ownership, and moved only while
    # both files still fence the proven identities.
    found, walk_skipped, walk_bytes, walk_errors = _walk_source_marks(
        stage, hinted)
    try:
        read_budget = int(float(max_read_gib) * (1024 ** 3))
    except (TypeError, ValueError, OverflowError):
        return _refuse(stage, f"max_read_gib {max_read_gib!r} is not a size",
                       tier_id=tier_id)
    proven: dict[str, dict[str, object]] = {}
    refused_early: dict[str, tuple[str, dict[str, object]]] = {}
    read_used = memo_hits = 0
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
        if read_used + 2 * size > read_budget:
            refused_early[stage_rel] = (
                "read_budget_exceeded",
                {"original_path": original, "offset": offset,
                 "size": size})
            continue
        sha, hit_detail = _memo_hit(memo, stage, stage_rel, original,
                                    offset, size)
        if sha is not None:
            memo_hits += 1
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
            run_dir.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            return _refuse(stage, f"quarantine run dir refused: {exc}",
                           tier_id=tier_id)
        try:
            volume = os.statvfs(run_dir)
            room = volume.f_bavail * volume.f_frsize
        except OSError:
            room = -1
        need = sum(int(one["size"]) for one in proven.values())
        if room >= 0 and room < need:
            return _refuse(
                stage, f"quarantine holds {room} bytes, needs {need}",
                tier_id=tier_id)

    pruned: list[Path] = []

    def _held_reclaim() -> dict[str, object]:
        parse_counts = census_memo.counts()
        validate_started = time.perf_counter()
        in_flight = stage_release.movers_in_flight(queue, tier_id=tier_id)
        if in_flight:
            receipt["skipped"] = "movers_in_flight"
            receipt["movers_in_flight"] = sorted(in_flight)
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
                if form in attributed:
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
            rows.append(dict(pair))
            pairs.append(pair)
        receipt["entries_scanned"] = len(found) + len(walk_skipped)
        receipt["walk_bytes"] = walk_bytes
        receipt["bytes_hashed"] = read_used
        receipt["read_budget_bytes"] = read_budget
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
                link = os.lstat(stage_path)
                if (not statmod.S_ISREG(link.st_mode)
                        or int(link.st_size) != size):
                    errors.append(f"{rel}: changed")
                    continue
                live = _file_identity(os.stat(stage_path))
            except FileNotFoundError:
                already_gone += 1
                continue
            except OSError as exc:
                errors.append(f"{rel}: {exc}")
                continue
            if live != pair["stage_identity"]:
                errors.append(f"{rel}: changed")
                continue
            try:
                if _file_identity(os.stat(original)) != pair[
                        "original_identity"]:
                    errors.append(f"{rel}: original changed")
                    continue
            except OSError as exc:
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
            got = _copy_to_quarantine(stage_path, dest, size)
            if got != sha:
                errors.append(f"{rel}: quarantine copy failed")
                continue
            captured = _xattrs_and_times(stage_path)
            if captured is None:
                errors.append(f"{rel}: marks unreadable")
                try:
                    dest.unlink()
                except OSError:
                    pass
                continue
            xattrs, times = captured
            _apply_xattrs_and_times(dest, xattrs, times)
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
            }
            # The journal is write-ahead: it names the move before the
            # unlink performs it. A journaled file still on stage is
            # benign -- a later restore skips it as identical -- while
            # a moved file no record names would be unmapped.
            try:
                _append_journal(run_dir, entry)
            except OSError as exc:
                errors.append(f"{rel}: journal refused: {exc}")
                try:
                    dest.unlink()
                except OSError:
                    pass
                continue
            try:
                os.unlink(stage_path)
            except FileNotFoundError:
                already_gone += 1
                manifest_entries.append(entry)
                pruned.append(stage_path.parent)
                continue
            except OSError as exc:
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
        receipt["restore_command"] = (
            f"stage_reclaim.py --pool-root {shlex.quote(str(queue.root))} "
            f"--tier-id {shlex.quote(tier_id)} "
            f"--stage-root {shlex.quote(str(stage))} "
            f"--mount-prefix {shlex.quote(str(mount_prefix))} "
            f"--quarantine-root {shlex.quote(str(quarantine))} "
            f"--restore {shlex.quote(active)}")
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
    if not entries and journaled:
        entries = list(journaled)
        receipt["manifest"] = str(run_dir / JOURNAL_NAME)
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
            if (os.path.isabs(stage_rel)
                    or ".." in Path(stage_rel).parts):
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
                resolved_parent = dest.parent.resolve()
                if (resolved_parent != stage_resolved
                        and stage_resolved not in resolved_parent.parents):
                    errors.append(f"{stage_rel}: outside the stage root")
                    continue
            except OSError as exc:
                errors.append(f"{stage_rel}: {exc}")
                continue
            if dest.exists() or os.path.lexists(dest):
                # A second restore is a no-op when the file already
                # proves identical; a conflicting file refuses.
                check = digest_file(dest)
                if (check is not None and check[1] == size
                        and check[0] == sha):
                    skipped += 1
                    continue
                errors.append(f"{stage_rel}: destination exists")
                continue
            check = digest_file(quarantine_path)
            if check is None or check[1] != size or check[0] != sha:
                errors.append(f"{stage_rel}: quarantine copy unreadable")
                continue
            got = _copy_to_quarantine(Path(quarantine_path), dest, size)
            if got != sha:
                errors.append(f"{stage_rel}: restore copy failed")
                continue
            _apply_xattrs_and_times(dest, xattrs, times)
            if not _marks_restored(dest, xattrs):
                errors.append(f"{stage_rel}: marks unrestorable")
                try:
                    dest.unlink()
                except OSError:
                    pass
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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="reclaim digest-proven source-mark-only stage copies "
                    "to quarantine (#1636)")
    parser.add_argument("--pool-root", required=True)
    parser.add_argument("--action-key", default=None)
    parser.add_argument("--tier-id", required=True)
    parser.add_argument("--stage-root", required=True)
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
    parser.add_argument("--max-read-gib", type=float, default=64.0)
    parser.add_argument("--memo", default=None,
                        help="a dry run's memo: reuse a digest only "
                             "while both files still fence its identities")
    parser.add_argument("--memo-out", default=None,
                        help="write this run's memo here, so one hashing "
                             "serves the dry run and the apply")
    parser.add_argument("--cas-root", default=None)
    parser.add_argument("--residency-root", default=None)
    parser.add_argument("--receipt", default=None)
    parser.add_argument("--restore", default=None,
                        help="restore a quarantine run instead of "
                             "reclaiming")
    args = parser.parse_args(argv)
    args.action_key = stage_release.own_action_key(args.action_key)
    queue = pool.PoolQueue(Path(args.pool_root))
    if args.restore is not None:
        if args.quarantine_root is None:
            parser.error("--restore needs --quarantine-root")
        receipt = restore_run(queue, stage_root=args.stage_root,
                              quarantine_root=args.quarantine_root,
                              run_id=args.restore)
        queue.record_move(args.action_key, receipt)
        if args.receipt:
            with open(args.receipt, "w") as stream:
                json.dump(receipt, stream, indent=1, sort_keys=True)
                stream.write("\n")
        print(json.dumps(receipt, indent=1, sort_keys=True, default=str))
        return 0 if receipt["complete"] else 1
    receipt = reclaim_source_marks(
        queue, tier_id=args.tier_id, stage_root=args.stage_root,
        mount_prefix=args.mount_prefix,
        quarantine_root=args.quarantine_root, run_id=args.run_id,
        apply=args.apply, max_read_gib=args.max_read_gib,
        memo_path=args.memo, memo_out=args.memo_out,
        cas_root=args.cas_root, residency_root=args.residency_root)
    queue.record_move(args.action_key, receipt)
    if args.receipt:
        with open(args.receipt, "w") as stream:
            json.dump(receipt, stream, indent=1, sort_keys=True)
            stream.write("\n")
    print(json.dumps(receipt, indent=1, sort_keys=True, default=str))
    return 0 if receipt["complete"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
