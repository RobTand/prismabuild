#!/usr/bin/env python3
"""Copy one byte range of a data manifest's read order onto a stage tier (#583).

This is the movement node: an ordinary PB action, placed by tag on the box
that serves the pool, whose whole job is to make a declared range of a
consumer's read order resident somewhere the consumer can reach faster.  It
publishes three things and nothing else --- the staged files, a residency-map
fragment naming the ones it verified, and a receipt saying what the pool
actually delivered.

**Why a copy and not a warm.**  ``prewarm_loop.py`` reads the bytes and throws
them away, which is the right shape for the ARC: the ARC is keyed by on-pool
block pointer, so reading a block warms the path the consumer will open.  A
stage tier is a different device.  Nothing about reading a file makes a copy of
it, so this tool has a sink where the prewarm loop has none.  Everything else
it shares, by import rather than by copy: the mount map, the pacer that
attributes the read to the pool's own members, the admission gate that makes
the depth mean something, and the manifest window helpers.  ``prewarm_loop`` is
imported and never edited.

**What makes a staged byte range visible.**  The copy is written beside its
final name and renamed into place, so a partial file is never visible under the
name the consumer reads.  The stage dataset runs ``sync=disabled``, which means
a crash can lose whole files that were renamed but not yet on stable storage;
the residency map is what turns that from a correctness problem into a
performance one, because an entry is written only after its rename and the
consumer verifies the digest before it trusts a staged read.  Read order is
preserved for the same reason it is in the prewarm loop: a partial range is
then a useful prefix rather than a random subset.

**Streaming, with a bounded window.**  Entries are copied in manifest read
order by a small pool of workers, so the first bytes land while the last are
still being read, and a finished entry is forgotten as soon as its fragment
entry is written.  Nothing accumulates the range in memory; the only buffer is
one block per worker.

**What it refuses.**  A range that stages more bytes than it declared is
``residency_overran_reservation``: its tokens bound what the tier can hold, so
a mover that exceeds them has broken the accounting that keeps the stage from
overfilling.  It exits non-zero, having published no fragment, so the gate
refuses its consumer rather than admitting it onto bytes nobody reserved.
"""

from __future__ import annotations

import argparse
import array
import bisect
from contextlib import ExitStack, contextmanager
import errno
from collections.abc import Mapping
import hashlib
import json
import os
from pathlib import Path
import queue as queuelib
import resource
import socket
import stat as statmod
import struct
import sys
import threading
import time

sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
from runtime_paths import generation_root  # noqa: E402

sys.path.insert(0, str(generation_root(__file__) / "src"))

import prewarm_loop  # noqa: E402
from prismabuild import core as pb  # noqa: E402
from prismabuild import movement_actions  # noqa: E402
from prismabuild import pool  # noqa: E402
from prismabuild import progress as pb_progress  # noqa: E402
from prismabuild import reader_lease  # noqa: E402
from prismabuild import residency_plan  # noqa: E402
from prismabuild import residency_map  # noqa: E402
from prismabuild import storage_tiers  # noqa: E402

#: Every staged object lives under this suffix, named by the exact range it
#: holds, so the name of one range can never be the name of another.
RANGE_SUFFIX = ".pbrange"

#: Seconds between republications of the residency fragment while a move runs.
#: See :func:`move`; a per-entry publish costs an NFS round trip per file and
#: was measured at 75 KB/s against 466 MB/s.
FRAGMENT_PUBLISH_S = 5.0


def stage_relative(path: str, offset: int, size: int, *, mount_prefix: str,
                   namespace: str | None = None,
                   named_once: bool = False) -> str:
    """Where one manifest entry's bytes live under the stage root.

    **Staged input.**  Every entry is named by the exact range it holds:
    ``<rel>.pbrange/<offset>-<size>``.  Two entries share a staged name
    exactly when they name the same bytes of the same source, which is
    exactly when sharing one staged object is correct, and the staged
    object's length is always the range's length, so the consumer reads it
    from position 0.  ``named_once`` is ignored for staged input.

    **Produced output** (``namespace`` given) keeps the name its producer
    declared: ``produced-output/<namespace>/<rel>`` for a path its manifest
    names once from offset zero (``named_once``), and a range name otherwise.
    The namespace is the producing action's own digest, so no other
    publisher's read can derive a name inside it and the declared name
    cannot collide across consumers.

    Staged input deliberately has no bare-name branch.  Its name is a pure
    function of the sealed manifest entry (``path``, ``offset``, ``bytes``),
    and a manifest entry carries no file size, so "this entry covers the
    whole file" is not derivable from what the name may depend on.  The
    former rule -- a path the manifest names exactly once, from offset zero,
    keeps its bare name -- took "named once from zero" for "whole": a routing
    capture that read each safetensors shard's 45 KB header and a consumer
    that read the whole 5.37 GB shard derived one name for two different
    objects, and whichever published second refused after the grace, for as
    long as the first publication stood (GLM Stage A R11, 2026-09-22: 74
    shard names across 40 read phases).  :func:`pre_range_stage_relative`
    keeps the former spelling for the retention censuses only.

    The staged-input derivation is injective: the last component is
    ``<offset>-<size>``, digits only, so the relative path and the range are
    both recoverable from the name, and a declared path that itself ends in
    ``.pbrange/<a>-<b>`` gets a suffix of its own rather than landing on
    another entry's range.
    """

    relative = path[len(mount_prefix.rstrip("/")) + 1:]
    if not relative or relative.startswith("/"):
        raise ValueError(f"{path!r} is not inside {mount_prefix!r}")
    if namespace is not None:
        if (len(namespace) != 64
                or any(c not in "0123456789abcdef" for c in namespace)):
            raise ValueError("produced stage namespace must be a 64-character digest")
        relative = f"produced-output/{namespace}/{relative}"
        if named_once and offset == 0:
            return relative
    return f"{relative}{RANGE_SUFFIX}/{offset}-{size}"


def paths_named_once(entries: list[dict[str, object]]) -> set[str]:
    """Paths the manifest names exactly once.

    Decides a produced output's declared name (:func:`stage_relative`) and
    the pre-range spelling of a staged input
    (:func:`pre_range_stage_relative`); it never says a read covers a file.
    """

    counts: dict[str, int] = {}
    for entry in entries:
        path = str(entry["path"])
        counts[path] = counts.get(path, 0) + 1
    return {path for path, count in counts.items() if count == 1}


def pre_range_stage_relative(path: str, offset: int, size: int, *,
                             mount_prefix: str, named_once: bool,
                             namespace: str | None = None) -> str:
    """The name a mover of a generation before range-only naming derived.

    Only the retention censuses read this.  A plan row keeps the tool path of
    the generation that sealed it, so a mover published after a runtime
    publication can still be an older mover writing the bare name for a path
    its manifest names once from offset zero.  A census that decides what
    another publisher may be writing therefore counts both spellings:
    over-retaining is a pass and over-removing is a failure.  Nothing may
    *publish* or *delete* by this name.
    """

    ranged = stage_relative(path, offset, size, mount_prefix=mount_prefix,
                            namespace=namespace, named_once=named_once)
    if namespace is None and named_once and offset == 0:
        return ranged[:ranged.rindex(RANGE_SUFFIX + "/")]
    return ranged


#: Origin-directory stat results that PROVE the declared origin is not
#: reachable from this host: a component that is missing (ENOENT/ENOTDIR) or
#: that this process may not search (EACCES/EPERM).  Every other errno --
#: EIO, ESTALE, ETIMEDOUT -- is unknown I/O, and unknown evidence never
#: becomes a typed refusal (#804).
_ORIGIN_ABSENT_ERRNOS = frozenset({errno.ENOENT, errno.ENOTDIR})
_ORIGIN_DENIED_ERRNOS = frozenset({errno.EACCES, errno.EPERM})

#: One stat of each distinct origin directory classifies reachability, and
#: the callable is module state so a test can present a transient I/O
#: failure a real filesystem will not produce on demand, without touching
#: the copy's own stats.
_ORIGIN_STAT = os.stat


def _deepest_existing(directory: str, stat) -> str | None:
    """The deepest ancestor of ``directory`` this host can stat, or ``None``."""

    current = directory
    while True:
        parent = os.path.dirname(current)
        if parent == current:
            return None
        try:
            stat(parent)
        except OSError:
            current = parent
            continue
        return parent


def origin_reachability_diagnosis(entries: list[dict[str, object]], mounts, *,
                                  stat=None) -> dict[str, object]:
    """Why a copy that staged nothing could not reach its origin directories.

    A manifest names absolute origin paths, and a mover runs on the tier
    host, which need not be the box that produced them: a produced-output
    prefix that exists only on the owner's box is accepted at declaration,
    admission and publication, and then fails on the tier host with the same
    untyped symptom a mover defect produces.  This is the bounded reachability
    diagnosis that separates the two -- one stat per distinct origin
    directory of the window, taken through the same mount map the copy reads
    through, with three-valued results.

    It runs ONLY after the copy has staged nothing and did not overrun, which
    is the one place the classification is needed: a healthy mover does no
    directory census at all, and a mover that adopted already-published
    coverage never asks.  It classifies; it never vetoes -- the copy has
    already run, and the refusal it feeds is the mover's own zero-output one.

    ``unreachable`` is positive proof this host does not have the directory
    -- a missing component (ENOENT/ENOTDIR), a path that is not a directory,
    or a search the process is not permitted (EACCES/EPERM).  Anything else
    (EIO, ESTALE, ETIMEDOUT, a stat that fails without an errno) is
    ``unknown``: unknown is conservative and is never typed as an origin
    refusal.  Mixed results are ``partial``; an empty window is ``empty``.
    """

    stat = stat or _ORIGIN_STAT
    directories: set[str] = set()
    for entry in entries:
        source = mounts.local(str(entry["path"]))
        directories.add(os.path.dirname(source) or "/")
    reachable: list[str] = []
    unreachable: list[dict[str, object]] = []
    unknown: list[dict[str, object]] = []
    for directory in sorted(directories):
        try:
            info = stat(directory)
        except OSError as exc:
            if exc.errno in _ORIGIN_ABSENT_ERRNOS:
                unreachable.append({
                    "path": directory, "reason": "absent",
                    "deepest_existing": _deepest_existing(directory, stat)})
            elif exc.errno in _ORIGIN_DENIED_ERRNOS:
                unreachable.append({
                    "path": directory, "reason": "denied",
                    "deepest_existing": _deepest_existing(directory, stat)})
            else:
                unknown.append({"path": directory,
                                "error": f"{type(exc).__name__}: {exc}"})
            continue
        if statmod.S_ISDIR(info.st_mode):
            reachable.append(directory)
        else:
            unreachable.append({
                "path": directory, "reason": "not-a-directory",
                "deepest_existing": _deepest_existing(directory, stat)})
    if not directories:
        state = "empty"
    elif unknown:
        state = "unknown"
    elif unreachable and not reachable:
        state = "unreachable"
    elif unreachable:
        state = "partial"
    else:
        state = "reachable"
    return {
        "state": state,
        "checked": len(directories),
        # The failing evidence is bounded like the copy's own error list;
        # the counts still say how many were seen.
        "unreachable": unreachable[:20],
        "unreachable_count": len(unreachable),
        "unknown": unknown[:20],
        "unknown_count": len(unknown),
    }


def proc_io(pid: str = "self") -> dict[str, int]:
    """``/proc/PID/io`` as integers, or ``{}`` where it cannot be read.

    Read at both ends and differenced: a rate without ``read_bytes`` beside it
    cannot say whether the bytes came off the device or out of the page cache,
    and that distinction is the whole reason the pacer exists.
    """

    try:
        with open(f"/proc/{pid}/io") as stream:
            text = stream.read()
    except OSError:
        return {}
    out: dict[str, int] = {}
    for line in text.splitlines():
        name, _, value = line.partition(":")
        try:
            out[name.strip()] = int(value.strip())
        except ValueError:
            continue
    return out


def _delta(before: dict[str, int], after: dict[str, int]) -> dict[str, int]:
    return {name: after[name] - before.get(name, 0) for name in sorted(after)}


def cpu_seconds(*, usage=resource.getrusage) -> float:
    """This process and its children's CPU seconds so far, user plus system.

    Read at both ends of the copy and differenced, for the same reason
    ``peak_rss_bytes`` is in the receipt: the next submission's ``cpu`` demand
    should be a measurement of what a mover cost rather than a number someone
    chose (``pb_demand_must_be_measured_not_habitual``).  Children are counted
    because the copy's digest work is where the CPU goes and a future mover may
    spawn it; today it is threads, which ``RUSAGE_SELF`` already covers.
    """

    total = 0.0
    for who in (resource.RUSAGE_SELF, resource.RUSAGE_CHILDREN):
        sample = usage(who)
        total += float(sample.ru_utime) + float(sample.ru_stime)
    return total


#: Grace for a late fragment or an in-flight publisher before an unproven
#: staged name settles to refuse-or-heal.  Every wait sleeps outside the
#: ownership lock in short polls; nothing about the timeout grants
#: permission -- expiry re-verifies, and only re-verified positive absence
#: heals.  Covers the incremental fragment rate limit (FRAGMENT_PUBLISH_S)
#: with wide margin; a publisher slower than this still converges, through
#: the stall policy's retry, never through replacement.
_PUBLISH_GRACE_S = 30.0
_PUBLISH_POLL_S = 0.25

#: Residency subdirectories that never hold consumer fragments.
_NON_FRAGMENT_DIRS = frozenset({"leases", "material"})

#: Byte ceiling for one publisher's lookup reuse (#761, #778), and the
#: conservative sizes it is measured in.  A count of anything -- references,
#: records, paths -- bounds only the thing it counts, so this is bytes, every
#: retained row is priced, and each price is charged ONCE at insert from the
#: actual object: the real packed path bytes, the real mention records, the
#: real per-record overhead.  Nothing is free, including a malformed or empty
#: record, because a row that costs nothing is a row that can be added
#: without limit.
#:
#: What is retained is a packed projection of exactly what the decision
#: reads -- normalized paths in one bytes blob with 8-byte end offsets and
#: 8-byte hash keys, and one fixed 72-byte record per validated ordered
#: mention -- rather than the parsed Python object graph of #761, whose
#: interned strings, set and dict slots, digest strings and identity dicts
#: can refuse retention once a document's object graph is oversized; every
#: later adoption of a destination that document names then re-reads and
#: re-decodes the whole document, which is the amplification #778
#: reproduced at fixture scale (the live forest's exact size and multiplier
#: were not measured).
#:
#: Each figure below is an over-estimate of the retained bytes on a 64-bit
#: CPython build, so :meth:`_index_bytes` never reports under the truth.
#: ``_STR_CHAR_BYTES`` is the UCS-4 width, which is what a non-Latin-1 path
#: actually costs and four times what an ASCII one does: erring high is the
#: point, and it means the few strings still retained whole -- a record's
#: file key and its mover key -- are charged what they are rather than a flat
#: guess.  A packed table's own storage is measured with ``sys.getsizeof``,
#: which reports the array's allocated capacity rather than only its used
#: slots, so growth headroom is charged too; that is a real allocation and
#: it scales with the path count, so it cannot be papered over with a larger
#: constant.
#:
#: Path tables that carry the same paths are shared: one actual table is
#: interned per packed identity ``(blob, ends)``, fragments and sidecars that
#: name the same destinations reference that one table, and it is charged
#: once.  ``_reclaim`` rebuilds the interned set from the live records and
#: recomputes that charge from the real packed bytes, the one accounting that
#: cannot drift.
#:
#: This prices the RETAINED index only.  The transient cost of decoding one
#: 22 MB fragment is a peak that is freed before the next lookup; it belongs
#: to the process's peak RSS, not here, and the two are reported separately.
#:
#: Past the ceiling a record is not retained: that file answers uncached,
#: which costs what it cost before this existed and is never a different
#: answer.  Because every charge is exact and travels with its entry, a
#: refusal is never permanent -- it lifts as soon as the index has room.
_INDEX_BUDGET_BYTES = 192 << 20
_STR_HEADER_BYTES = 64
_STR_CHAR_BYTES = 4
_RECORD_OVERHEAD_BYTES = 512
#: One packed mention record: ``<Q32s4Q`` -- size, raw sha256, identity.
_PACKED_MENTION_BYTES = 72
#: What one retained table owns beyond its own components: the two-tuple it
#: is keyed by and its slot in the interned dictionary.  Charged once per
#: table and only in the interned charge, never again in a record's.
_INTERN_TABLE_BYTES = 224
#: The unsigned width the 64-bit path hash is normalized to before it is
#: stored and bisected; the exact bytes comparison below decides membership.
_HASH_MASK = (1 << 64) - 1
_MENTION_STRUCT = struct.Struct("<Q32s4Q")


def _string_bytes(value: object) -> int:
    """Conservative retained size of one string: header plus UCS-4 width."""

    return _STR_HEADER_BYTES + _STR_CHAR_BYTES * len(str(value))


def _metadata_version(info: "os.stat_result") -> tuple[int, int, int, int, int]:
    """Change evidence for one publication metadata file.

    Device and inode catch a replacement, size and mtime catch a rewrite,
    ctime catches a permission or ownership change that leaves mtime alone.
    The same kind of stat fence every strict reader already uses; nothing
    here invents an identity of its own.
    """

    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns,
            int(getattr(info, "st_ctime_ns", 0)))


def _read_metadata(path: Path) -> tuple[tuple[int, int, int, int, int], bytes]:
    """One metadata file's bytes, with the version those bytes were read at.

    ``fstat`` of the open descriptor rather than a second ``stat`` of the
    name: the version returned names the exact bytes returned, so a
    concurrent replace between a directory scan and this read can never file
    fresh bytes under a stale version, or a stale parse under a fresh one.
    """

    with open(path, "rb") as stream:
        version = _metadata_version(os.fstat(stream.fileno()))
        return version, stream.read()


def _observe(path: Path) -> "tuple[int, int, int, int, int] | OSError":
    """One metadata file's version, or the ``OSError`` its ``stat`` raised.

    The per-lookup stat of the reuse index (#761), taken before the
    publisher's lookup lock rather than under it (#981): once the proof left
    the stage ownership lock, sixteen copy workers proving at once would
    otherwise queue one ``stat`` at a time behind that small lock.  The
    version compare, and any read and parse a changed version needs, still
    run under it, so one file is still parsed once per version.
    """

    try:
        return _metadata_version(os.stat(path))
    except OSError as exc:
        return exc


def _origin_id_of(path_str: str) -> str | None:
    """``st_size:st_mtime_ns:st_ino`` of a copy source, or ``None``.

    The same three numbers :mod:`prewarm_loop` files as
    ``STAGE_SOURCE_XATTR`` at copy time: the existing origin
    change-detection evidence, read here rather than reinvented.
    """

    try:
        sample = os.stat(path_str)
    except OSError:
        return None
    if not statmod.S_ISREG(sample.st_mode):
        return None
    return f"{sample.st_size}:{sample.st_mtime_ns}:{sample.st_ino}"


def _packed_order(names: "list[str] | set[str]") -> list[str]:
    """One deterministic order for a path set, shared by every packer.

    Ascending unsigned 64-bit ``hash`` of the name, then the encoded bytes as
    the tie-break, so two documents that name the same paths build the same
    packed identity and share one table.  The order is process-local (the
    hash is salted) and never leaves the publisher.
    """

    return sorted(names, key=lambda name: (
        hash(name) & _HASH_MASK, name.encode("utf-8", "surrogatepass")))


class _PackedPaths:
    """Exact membership over a packed, shareable set of normalized paths.

    The paths are UTF-8 encoded (``surrogatepass``, so a lone surrogate from
    JSON cannot compare equal to a real path -- the encoding stays injective
    over Python strings) and packed into one bytes blob, with an 8-byte end
    offset and an unsigned 64-bit ``hash`` key per path in the same order.
    Lookup is a C-level ``bisect`` on the key array, followed by an exact
    byte comparison of the blob slice for every candidate with an equal key:
    a hash collision costs one comparison and never a different answer.

    ``retained_bytes`` prices the blob, both arrays and fixed headers.  The
    ``(blob, ends)`` pair is the table's exact identity -- ends fixes every
    boundary, so equal pairs are equal path sequences -- and the one key the
    publisher's intern table shares it under.
    """

    __slots__ = ("blob", "ends", "keys")

    def __init__(self, names: "list[str] | set[str]") -> None:
        ordered = _packed_order(list(names))
        encoded = [name.encode("utf-8", "surrogatepass") for name in ordered]
        self.blob = b"".join(encoded)
        ends = bytearray()
        total = 0
        for item in encoded:
            total += len(item)
            ends += total.to_bytes(8, "little")
        self.ends = bytes(ends)
        self.keys = array.array("Q", (hash(name) & _HASH_MASK
                                      for name in ordered))

    @property
    def retained_bytes(self) -> int:
        """Actual retained size of this table, allocated storage included.

        ``sys.getsizeof`` of the key array reports its allocated capacity,
        not only the slots in use, and the blob, offsets and instance headers
        are measured the same way, so this is never under the bytes the table
        holds.  The intern tuple and its dictionary slot are added once per
        table, in the interned charge only -- never again in a record's.
        """

        if not self.keys:
            return 0
        return (sys.getsizeof(self) + sys.getsizeof(self.blob)
                + sys.getsizeof(self.ends) + sys.getsizeof(self.keys)
                + _INTERN_TABLE_BYTES)

    def intern_key(self) -> tuple[bytes, bytes]:
        """The exact identity one shared table is interned under."""

        return (self.blob, self.ends)

    def index(self, path: str) -> int:
        """The path's position in the packed order, or -1 when absent."""

        keys = self.keys
        count = len(keys)
        if not count:
            return -1
        needle_hash = hash(path) & _HASH_MASK
        position = bisect.bisect_left(keys, needle_hash)
        if position == count or keys[position] != needle_hash:
            return -1
        needle = path.encode("utf-8", "surrogatepass")
        blob, ends = self.blob, self.ends
        while position < count and keys[position] == needle_hash:
            start = (int.from_bytes(ends[position * 8 - 8:position * 8],
                                    "little") if position else 0)
            end = int.from_bytes(ends[position * 8:position * 8 + 8], "little")
            if blob[start:end] == needle:
                return position
            position += 1
        return -1

    def __contains__(self, path: object) -> bool:
        return self.index(str(path)) >= 0


#: One shared empty table for records that name no path; never interned and
#: never charged a table cost (the per-record overhead already prices the
#: row, and the one object is process-wide).
_EMPTY_PATHS = _PackedPaths([])

#: A census slot whose fragment record the reuse index would not keep (#981):
#: its packed table is too big for the budget, so the census holds no copy of
#: it, and each decision that reaches the slot looks the file up again, as
#: every decision did before the census was shared.
_LOOK_AGAIN = object()


def _pack_mention(mention: tuple) -> bytes:
    """One validated mention as its fixed binary record.

    Raises for anything the fixed width cannot carry exactly --
    ``validate_material`` accepts arbitrary-size non-negative integers -- so
    the caller falls back to the uncached answer rather than truncate.
    """

    size, digest, identity = mention
    if not isinstance(digest, str) or not isinstance(identity, Mapping):
        raise ValueError("mention is not a validated (bytes, sha256, file_id)")
    return _MENTION_STRUCT.pack(int(size), bytes.fromhex(digest),
                                int(identity["ino"]), int(identity["size"]),
                                int(identity["mtime_ns"]),
                                int(identity["ctime_ns"]))


def _decode_mentions(packed: bytes) -> tuple:
    """The ``(size, sha256, file_id)`` triples one path's records decode to."""

    out = []
    for offset in range(0, len(packed), _PACKED_MENTION_BYTES):
        size, digest, ino, size_bytes, mtime_ns, ctime_ns = (
            _MENTION_STRUCT.unpack_from(packed, offset))
        out.append((size, digest.hex(),
                    {"ino": ino, "size": size_bytes,
                     "mtime_ns": mtime_ns, "ctime_ns": ctime_ns}))
    return tuple(out)


class _PackedMentions:
    """One sidecar's ordered per-path mentions, packed and shareable.

    ``paths`` is a :class:`_PackedPaths` table (shared with every other
    document naming the same paths); ``values`` is one bytes blob of fixed
    72-byte mention records and ``ends`` the 8-byte end offsets into it, both
    parallel to the table's packed order.  ``get`` reconstructs exactly the
    triples the object representation returned, in the same order, because
    the digest search and the identity search legitimately read different
    mentions of one path.
    """

    __slots__ = ("paths", "values", "ends")

    def __init__(self, paths: _PackedPaths, values: bytes,
                 ends: bytes) -> None:
        self.paths = paths
        self.values = values
        self.ends = ends

    @property
    def values_bytes(self) -> int:
        """Actual retained size of this record's values and its offsets."""

        return (sys.getsizeof(self) + sys.getsizeof(self.values)
                + sys.getsizeof(self.ends))

    @classmethod
    def build(cls, mentions: Mapping[str, tuple]) -> "_PackedMentions | None":
        """The packed projection of one validated sidecar, or ``None``.

        ``None`` means some mention cannot be carried exactly by the fixed
        record.  The caller then decides from the freshly parsed object,
        uncached: the same verdict, never a truncated one.
        """

        try:
            packed = {path: b"".join(_pack_mention(one) for one in carried)
                      for path, carried in mentions.items()}
        except (struct.error, OverflowError, ValueError, TypeError, KeyError):
            return None
        ordered = _packed_order(list(packed))
        values = b"".join(packed[path] for path in ordered)
        ends = bytearray()
        total = 0
        for path in ordered:
            total += len(packed[path])
            ends += total.to_bytes(8, "little")
        return cls(_PackedPaths(ordered), values, bytes(ends))

    def shared_with(self, paths: _PackedPaths) -> "_PackedMentions":
        """The same values on the one actual interned table."""

        return _PackedMentions(paths, self.values, self.ends)

    def get(self, path: str, default: object = ()) -> object:
        """The ordered triples this sidecar carries for ``path``."""

        index = self.paths.index(path)
        if index < 0:
            return default
        start = (int.from_bytes(self.ends[index * 8 - 8:index * 8], "little")
                 if index else 0)
        end = int.from_bytes(self.ends[index * 8:index * 8 + 8], "little")
        return _decode_mentions(self.values[start:end])


class _PublicationRefused(OSError):
    """The publication gate refused one entry; the range stops issuing work.

    The refusal is made per staged name, and it says nothing about the other
    names: they may be perfectly publishable.  It does, however, make the
    range incomplete -- this mover cannot publish what it declared -- so
    every further copy and publication grace is spent on a run that must be
    retried anyway, and the first obstruction is buried under later entry
    errors by the receipt's error cap.  A copier thread that receives this
    signal therefore stops handing out queued entries for the range, bounding
    the wasted work and surfacing the obstruction; the entries already
    dispatched to the active group still finish or refuse, and everything
    they committed keeps its fragment, sidecar and byte count (#853).

    An ordinary source read, digest or filesystem failure is a plain
    ``OSError`` and keeps the existing per-entry handling: one bad source is
    not evidence about the rest of the range.

    A subclass of ``OSError`` so every caller that already catches
    ``OSError`` keeps catching it -- the type breaks no existing behavior.
    The copier deliberately changes its range handling for it, which is the
    point of the type.

    ``refusal`` is set only on a refusal no rerun can clear (#966): a live
    owner holds different bytes under the name.  ``conflict`` then names
    the path and both owners, and the mover's receipt carries both, so it
    exits nonzero and retires its own window instead of being republished
    into the same refusal.  Every other refusal leaves them ``None`` and
    stays retryable.
    """

    def __init__(self, *args: object, refusal: str | None = None,
                 conflict: Mapping[str, object] | None = None) -> None:
        super().__init__(*args)
        self.refusal = refusal
        self.conflict = dict(conflict) if conflict is not None else None


#: The terminal refusal a mover files when a live owner holds different bytes
#: under one of its staged names (#966).
STAGED_DESTINATION_CONFLICT = "staged_destination_conflict"


class _Owners:
    """Who a proof search found naming one staged path it found divergent.

    Filled by :meth:`_StagedPublisher._proof_search` when one is passed in:
    every ``(consumer, mover)`` whose fragment names the path and whose
    record is not a date for a superseded incarnation, except the
    publisher's own pair.  ``complete`` is false when a fragment or a child
    directory could not be read, since an unreadable record may name the
    path too.
    """

    __slots__ = ("pairs", "complete")

    def __init__(self) -> None:
        self.pairs: set[tuple[str, str]] = set()
        self.complete = True


def _divergent_detail(norm: str) -> str:
    return (f"staged destination holds different bytes than manifest "
            f"digest for {norm}; refusing to invalidate its owner")


def _is_action_key(value: object) -> bool:
    return (isinstance(value, str) and len(value) == 64
            and all(character in "0123456789abcdef" for character in value))


def _combined_owner_state(rows: list[dict[str, object]],
                          complete: bool) -> tuple[str, str]:
    """One divergence's state from its judged owners (#966).

    A live owner decides it even beside an unreadable fragment: that owner
    holds different bytes under the name whatever else might.  Otherwise an
    unreadable fragment, or any owner not proven ended, leaves it
    uncertain, and only a complete census of ended owners is ``ended``.
    """

    if any(row["state"] == "live" for row in rows):
        return "live", ""
    if not complete:
        return "uncertain", "a fragment that may name it could not be read"
    for row in rows:
        if row["state"] != "ended":
            return "uncertain", str(row.get("why")
                                    or "an owner's ending is unproven")
    return "ended", ""


class _PhaseClock:
    """Thread-seconds a mover spends in each phase, for its receipt (#981).

    A receipt that reports only totals cannot say where a multi-minute
    window went: chunk 0 of ``234d662cdc4f`` ran 368.7 s, landed its bytes in
    the first 90, and nothing on it split the other 270.  Each phase here is
    summed across the copy workers -- thread-seconds, not wall -- with its
    call count and its longest single call, so a phase that is slow on
    average and a phase that is slow once both show.  The copy's own phases
    (``pace_wait``, ``copy_read``, ``copy_write``, ``hash``, ``fsync``) are
    added once per entry; the publisher's (``adopt_proof``,
    ``publish_decide``, ``ownership_lock_wait``, ``ownership_lock_held``,
    ``publish_poll_sleep``, and, for a divergent name, ``owner_judgement``
    and ``owner_locks_held``, #966) once per call.  ``outcomes`` counts how
    each entry ended: adopted before any copy, published by its own rename,
    adopted at publication after copying, or -- a name every owner of which
    had ended -- replaced at publication.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._phases: dict[str, list[float]] = {}
        self._outcomes: dict[str, int] = {}

    def add(self, phase: str, seconds: float, calls: int = 1) -> None:
        with self._lock:
            row = self._phases.setdefault(phase, [0.0, 0, 0.0])
            row[0] += seconds
            row[1] += calls
            if seconds > row[2]:
                row[2] = seconds

    @contextmanager
    def timing(self, phase: str):
        started = time.perf_counter()
        try:
            yield
        finally:
            self.add(phase, time.perf_counter() - started)

    def outcome(self, name: str) -> None:
        with self._lock:
            self._outcomes[name] = self._outcomes.get(name, 0) + 1

    def report(self) -> dict[str, object]:
        with self._lock:
            return {
                "thread_seconds": {
                    phase: {"seconds": round(row[0], 6), "calls": int(row[1]),
                            "max_s": round(row[2], 6)}
                    for phase, row in sorted(self._phases.items())},
                "outcomes": dict(sorted(self._outcomes.items())),
            }


class _StagedPublisher:
    """Linearize staged-path publication under the stage ownership lock.

    Overlapping consumers stage one content-addressed name for the same
    bytes; an unconditional rename per copy mints a fresh inode/mtime
    and invalidates every other consumer's published material identity
    (and any live cover proof or pin fenced on it). The payload copy
    into the private owner-keyed temp stays outside every lock; this
    gate runs immediately before destination publication and decides
    metadata-only.  What it decides *acts* under the existing
    stage-ownership exclusion (the same lock egress holds across
    snapshot-to-release); what it only *reads* does not hold it (#981).
    The proof search and the pin, claim and in-flight censuses are
    reads -- they run first, without the lock -- and a verdict that acts
    is then re-established under the lock before it acts: an adoption
    commits only while the destination still carries every field of the
    ``file_id`` its proof dated, and a replacement or a refusal is decided
    again, in full, under the lock, exactly as before.  One lock for the
    whole stage root held across every proof made every mover on the
    host prove one entry at a time (about 100 ms each on dl380g10,
    2026-09-23).  The lock never excluded the fragment *writers* --
    ``write_fragment`` runs outside it -- so a search under it was never
    a snapshot of the forest either; what it excludes is the egress's
    scan-to-unlink and a racing first publication's rename, and both are
    what the commit re-checks.  The verdicts:

    * absent destination: this copy publishes (concurrent absentees
      serialize here, so exactly one first publication wins; a loser
      that already passed finds a present path below and converges);
    * present with an adoption proof -- some consumer's fragment entry
      naming this path with equal bytes, a material sidecar digest that
      matches (the declared digest; the copier's own computed digest
      for digest-less manifests, which pin no content; or the origin
      fast path below), and a live stat equal to the sidecar's file
      identity: adopt the published incarnation, discard the temp.
      Unchanged bytes keep every live pin valid, so no pin census is
      needed on this path;
    * present and provably different -- a record that dates the current
      incarnation names a digest that matches neither the declared nor
      the computed digest, or the very inode a record dates was modified
      in place (no legitimate publication writes in place): never a blind
      overwrite.  On a stage mover the owners decide it (#966,
      :meth:`_arbitration`): a live owner is a conflict for an owner to
      resolve, refused terminally and named; an owner whose ending cannot
      be proven is refused retryably, as before; and a name every owner of
      which has provably ended is invalidated and restaged by this copy,
      after the same pin, claim and in-flight censuses a heal passes.  A
      ram promotion, which names no consumer, refuses at once as before.
      A record whose sidecar dates a superseded incarnation -- a different
      inode, the name replaced by a real publication -- is not this: it
      is deferred like an undated vouch while another record may still
      prove the current one (#755);
    * present but unproven, live-pinned, or unreadable: refuse at once
      for pins and unreadable proof state; otherwise wait out the grace
      for a late fragment or an in-flight publisher, then refuse --
      deferring to the action stall policy's retry, which adopts once
      the proof lands.  No timeout ever permits replacement.

    The single exception is re-verified positive absence: no fragment
    names the path, no pin refs it, no live mover claim (own excluded)
    covers it, no sibling copy is in flight, and every census read
    clean -- re-checked after the grace, not merely elapsed.  Only then
    does the copy heal the name as orphan/crash residue.  A live or
    unknown publisher always means wait/refuse, never replace.

    Digest-less (dev/null) manifests converge like the rest: the origin
    fast path adopts without copying where the published file still
    carries the source identity this copy's source stats today
    (``user.pbstage.source``, the prewarm loop's existing evidence);
    otherwise the necessary private copy's own computed digest is
    compared against the stored material proof at final publication --
    the existing file is never rehashed.  No material schema change:
    origin provenance rides the existing xattr where the filesystem
    carries it, and the digest comparison where it does not.

    Never replaces an occupied path except into proven absence (first
    publication) or re-verified orphan residue.  Returns ``(written,
    digest, identity)`` like a copy; raises ``_PublicationRefused`` on a
    gate refusal and plain ``OSError`` on stop.  The worker files both as an
    entry error exactly like a digest mismatch; a refusal additionally stops
    the copier from issuing further entries for this range (#853).
    """

    def __init__(self, *, queue, stage_root, residency_root,
                 mover_action_key: str,
                 manifest_sha256: str, tier_id: str, cas_root,
                 consumer_action_key: str = "") -> None:
        self.queue = queue
        self.stage_root = Path(stage_root)
        self.residency_root = Path(residency_root)
        self.mover = mover_action_key
        self.consumer = str(consumer_action_key)
        self.manifest_sha256 = manifest_sha256
        self.tier_id = tier_id
        self.cas_root = cas_root
        #: The generation this run's material sidecar carries.  A resumed
        #: run starts from its prior own generation (stable reuse, the
        #: ``reader_lease.adopted_generation`` rule: same bytes without
        #: replacement keep their generation) and mints a fresh one only
        #: when it first replaces actual bytes -- new material, new date.
        #: A first-ever run mints once and never re-mints mid-run, exactly
        #: as the single-per-run generation always did.
        self._material_generation: str | None = None
        self._generation_is_resumed = False
        self._generation_bumped = False
        # Invocation-local reuse of parsed publication metadata (#761, #778).
        # ``_proof_search`` runs once per destination -- tens of thousands
        # per mover -- and again on every publish poll, and before this it
        # re-opened and re-parsed every consumer fragment on each run (118
        # files, 334.6 MB, on the live root when this was measured) plus
        # every relevant sidecar.  Each file is now parsed once per version
        # and retained as a packed projection of exactly what a decision
        # reads -- never the parsed object graph, whose bytes can refuse
        # retention once a document's graph is oversized, after which every
        # later lookup re-reads and re-decodes the whole document (the
        # amplification #778 reproduced at fixture scale).  The directory
        # scans and the per-file stat that see additions, changes and
        # removals still run before every decision, so nothing decides on
        # evidence that moved.  Process-local, nothing persisted, dropped
        # with the publisher.  Its own lock because the stage ownership lock
        # is a POSIX file lock, which does not exclude two threads of one
        # process from each other.
        self._lookup_lock = threading.Lock()
        # Each entry is ``(version, record, charge)``: the charge is measured
        # from the real packed object at insert and travels with it, so
        # dropping an entry cannot forget to discount it and no row is ever
        # free.
        self._fragments: dict[str, tuple[tuple, object, int]] = {}
        self._materials: dict[tuple[str, str], tuple[tuple, object, int]] = {}
        #: One actual packed path table per exact identity, shared by every
        #: fragment and sidecar that names those paths and charged once.
        self._interned: dict[tuple[bytes, bytes], _PackedPaths] = {}
        self._interned_cost = 0
        self._fragment_cost = 0
        self._material_cost = 0
        #: Where this publisher's time goes, for the mover's receipt.
        self.clock = _PhaseClock()
        # The single-flight forest census (#981), see :meth:`_shared_census`.
        # ``_census`` is the latest finished census and ``_census_done`` its
        # number; ``_census_started`` counts censuses begun.  Only one runs
        # at a time, so the numbers finish in order.
        self._census_cond = threading.Condition(threading.Lock())
        self._census_started = 0
        self._census_done = 0
        self._census_running = False
        self._census: tuple | None = None
        #: Whether a divergent name is settled by its owners' states (#966).
        #: A stage mover names its consumer and does; a ram promotion names
        #: none and keeps the retryable refusal it always had.
        self._arbitrates = bool(self.consumer)
        #: One divergence is arbitrated at a time per publisher: the owners'
        #: transition locks are taken without blocking, and a sibling copy
        #: thread holding them must read as a wait, not as another process's
        #: contention.
        self._arbitration_lock = threading.Lock()
        #: Owners this run proved ended, by ``(consumer, mover)``, each with
        #: its judged row.  Read and written under ``_arbitration_lock``.
        self._ended_owners: dict[tuple[str, str], dict[str, object]] = {}
        #: The names this run replaced because every owner had ended, each
        #: with the owners it proved, for the receipt.  Appended under
        #: ``_arbitration_lock``.
        self.invalidated: list[dict[str, object]] = []

    @contextmanager
    def _ownership(self):
        """The stage ownership lock, with its wait and its hold timed."""

        asked = time.perf_counter()
        with self.queue.stage_ownership_lock(str(self.stage_root)):
            granted = time.perf_counter()
            self.clock.add("ownership_lock_wait", granted - asked)
            try:
                yield
            finally:
                self.clock.add("ownership_lock_held",
                               time.perf_counter() - granted)

    def begin_material(self, resumed: str | None = None) -> str:
        """Start this run's material generation, optionally resuming one.

        ``resumed`` is this mover's own prior generation, carried over when
        the run preserves qualified prior coverage: unchanged bytes keep
        their date, so a live pin's ``covers`` still match.  ``None`` is a
        first publication, and mints.
        """

        if resumed is not None:
            self._material_generation = resumed
            self._generation_is_resumed = True
        else:
            self._material_generation = reader_lease.mint_generation()
        return self._material_generation

    def material_generation(self) -> str:
        """The generation the sidecar should be written under right now."""

        if self._material_generation is None:
            self.begin_material()
        return self._material_generation

    def _note_replacement(self) -> None:
        """Record that this run replaced actual bytes at a destination.

        The one event that mints a new generation on a resumed run: the
        sidecar is a single document with one generation, and once any
        entry carries bytes this run wrote, the carried-over date no
        longer describes the whole document.  Minted once -- the document
        then keeps that generation for the rest of the run, the same
        one-per-run stability a first publication always had.
        """

        if self._generation_is_resumed and not self._generation_bumped:
            self._material_generation = reader_lease.mint_generation()
            self._generation_bumped = True

    def try_adopt(self, entry: dict[str, object], destination: Path,
                  source_id: str | None = None,
                  ) -> tuple[int, str, dict[str, int]] | None:
        """Adopt an already-published incarnation without copying, if proven.

        Metadata-only: the same proof the publish gate requires, minus the
        copy-compare path (no computed digest exists yet).  Returns
        ``(want, digest, file_id)`` or ``None`` to proceed with the copy.
        Raises ``_PublicationRefused`` for provably divergent bytes, failing
        fast before any copy.

        The proof runs without the stage ownership lock (#981); only the
        commit takes it.  Under the lock, a proof adopts only while the
        destination still carries every field of the ``file_id`` the proof
        dated -- the same identity check every strict reader makes -- so the
        bytes adopted are the bytes the proof's sidecar digest describes.
        Anything else that would act -- a divergence, or a proof whose
        incarnation moved after the search -- is searched again under the
        lock and decided exactly as before.  An unknown, owned or clean
        answer acts on nothing: the copy proceeds, and its publication is
        gated under the lock.

        A divergence on a stage mover is arbitrated by its owners' states
        (#966, :meth:`_arbitration`) before anything is copied: a live owner
        raises the terminal conflict and an unproven ending the retryable
        refusal, as before, so neither costs a copy.  When every owner has
        ended the copy proceeds, and its publication replaces the name by
        rename; the owners' endings are remembered for the run, so the
        publication does not judge them again.  Nothing is unlinked here: a
        rename over the old file keeps its inode allocated until the new one
        takes the name, so the new bytes can never reuse the inode number the
        old owner's material dates, which would read as an in-place write.
        """

        want = int(entry["bytes"])
        declared = entry.get("sha256")
        norm = os.path.normpath(str(destination))
        try:
            present = os.lstat(destination)
        except OSError:
            return None
        if not statmod.S_ISREG(present.st_mode):
            return None
        owners = _Owners() if self._arbitrates else None
        with self.clock.timing("adopt_proof"):
            proof, standing, detail = self._proof_search(
                norm, want, declared, source_id=source_id, owners=owners)
        if proof is None and standing != "divergent":
            return None
        if standing == "divergent" and owners is not None:
            return self._adopt_divergent(destination, norm, want, declared,
                                         source_id, owners, detail)
        with self._ownership():
            if proof is not None and reader_lease.file_id_matches(
                    proof[1], reader_lease.stat_identity(norm)):
                return want, proof[0], proof[1]
            owners = _Owners() if self._arbitrates else None
            proof, standing, detail = self._proof_search(
                norm, want, declared, source_id=source_id, owners=owners)
            if standing == "unknown":
                return None
            if standing == "divergent":
                if owners is None:
                    raise _PublicationRefused(detail)
            elif proof is None:
                return None
            else:
                return want, proof[0], proof[1]
        # A divergence that appeared after the lockless search: arbitrated
        # once the stage lock is released, since the owners' transition
        # locks come before it.
        return self._adopt_divergent(destination, norm, want, declared,
                                     source_id, owners, detail)

    def _adopt_divergent(self, destination: Path, norm: str, want: int,
                         declared: object, source_id: str | None,
                         owners: _Owners, detail: str | None,
                         ) -> tuple[int, str, dict[str, int]] | None:
        """:meth:`try_adopt`'s answer for a name its search found divergent.

        Acts on nothing: a live owner or an unproven ending refuses before
        any copy, and otherwise the copy proceeds to a publication that
        decides the name under the stage lock.
        """

        def redo(fresh: _Owners) -> tuple:
            return self._decide(destination, want, declared, None, source_id,
                                heal=False, owners=fresh)

        with self._arbitration(owners, redo) as (state, rows, verdict, why):
            if state == "changed":
                if verdict[0] == "adopt":
                    return want, verdict[1], verdict[2]
                return None
            if state == "live":
                raise self._conflict(norm, declared, detail, rows)
            if state == "uncertain":
                raise _PublicationRefused(f"{detail}; {why}")
            # ``ended``: the publication replaces it.  ``busy`` or ``moved``:
            # nothing was decided, and the publication arbitrates again.
            return None

    def publish(self, entry: dict[str, object], destination: Path,
                temp_path: Path, computed: str,
                stop: threading.Event | None = None,
                source_id: str | None = None,
                ) -> tuple[int, str, dict[str, int] | None]:
        """Decide one entry's publication; copy already verified in temp.

        Each poll decides first without the stage ownership lock (#981):
        the proof search and the pin, claim and in-flight censuses only
        read, and a ``wait`` acts on nothing, so a waiting poll never takes
        the lock at all.  A verdict that acts takes it and is re-established
        there before it acts: an adoption commits only while the destination
        still carries every field of the ``file_id`` its proof dated, and
        anything else -- a replacement, a refusal, or an adoption whose
        incarnation moved -- is decided again, in full, under the lock,
        exactly as before.  A fresh destination's rename therefore holds the
        lock for one ``lstat`` and the rename, not for anybody's proof.

        On a stage mover a divergent verdict is arbitrated by its owners'
        states (#966, :meth:`_arbitration`): a live owner is the terminal
        conflict, an unproven ending the retryable refusal, and a name every
        owner of which has ended is replaced by this copy.  A divergence
        first seen under the stage lock is decided on the next poll, since
        the owners' transition locks come before that lock.
        """

        want = int(entry["bytes"])
        declared = entry.get("sha256")
        norm = os.path.normpath(str(destination))
        deadline = time.monotonic() + _PUBLISH_GRACE_S
        while True:
            now = time.monotonic()
            heal = now >= deadline
            owners = _Owners() if self._arbitrates else None
            with self.clock.timing("publish_decide"):
                verdict = self._decide(destination, want, declared, computed,
                                       source_id, heal=heal, owners=owners)
            if verdict[0] == "divergent":
                done = self._publish_divergent(
                    destination, norm, temp_path, want, declared, computed,
                    source_id, heal, owners, verdict[1])
                if done is not None:
                    return done
            elif verdict[0] != "wait":
                with self._ownership():
                    if not (verdict[0] == "adopt"
                            and reader_lease.file_id_matches(
                                verdict[2], reader_lease.stat_identity(norm))):
                        verdict = self._decide(
                            destination, want, declared, computed, source_id,
                            heal=heal,
                            owners=_Owners() if self._arbitrates else None)
                    done = self._act(verdict, destination, temp_path, want,
                                     computed)
                    if done is not None:
                        return done
            if stop is not None and stop.is_set():
                temp_path.unlink(missing_ok=True)
                raise OSError(f"stopping before {destination} publishes")
            with self.clock.timing("publish_poll_sleep"):
                time.sleep(_PUBLISH_POLL_S)

    def _act(self, verdict: tuple, destination: Path, temp_path: Path,
             want: int, computed: str,
             ) -> tuple[int, str, dict[str, int] | None] | None:
        """Carry out a publication verdict under the stage lock.

        ``None`` for a verdict that acts on nothing -- a wait, or a
        divergence first seen under the lock -- so the caller polls again.
        """

        if verdict[0] == "replace":
            os.replace(temp_path, destination)
            self._note_replacement()
            self.clock.outcome("renamed")
            return want, computed, self._identity(destination)
        if verdict[0] == "adopt":
            temp_path.unlink(missing_ok=True)
            self.clock.outcome("adopted_at_publication")
            return want, verdict[1], verdict[2]
        if verdict[0] == "refuse":
            temp_path.unlink(missing_ok=True)
            raise _PublicationRefused(verdict[1])
        return None

    def _publish_divergent(self, destination: Path, norm: str,
                           temp_path: Path, want: int, declared: object,
                           computed: str, source_id: str | None, heal: bool,
                           owners: _Owners, detail: str,
                           ) -> tuple[int, str, dict[str, int] | None] | None:
        """:meth:`publish`'s answer for a divergent verdict; ``None`` polls."""

        def redo(fresh: _Owners) -> tuple:
            return self._decide(destination, want, declared, computed,
                                source_id, heal=heal, owners=fresh)

        with self._arbitration(owners, redo) as (state, rows, verdict, why):
            if state == "changed":
                return self._act(verdict, destination, temp_path, want,
                                 computed)
            if state in ("busy", "moved"):
                if heal:
                    temp_path.unlink(missing_ok=True)
                    raise _PublicationRefused(
                        f"{detail}; {why} after the grace, deferring to retry")
                return None
            if state == "live":
                temp_path.unlink(missing_ok=True)
                raise self._conflict(norm, declared, detail, rows)
            if state == "uncertain":
                temp_path.unlink(missing_ok=True)
                raise _PublicationRefused(f"{detail}; {why}")
            blocked = self._invalidation_blocked(norm, destination)
            if blocked:
                temp_path.unlink(missing_ok=True)
                raise _PublicationRefused(
                    f"{detail}; every owner has ended, but {blocked}")
            os.replace(temp_path, destination)
            self._note_replacement()
            self._invalidated(norm, rows)
            self.clock.outcome("replaced_ended_owner")
            return want, computed, self._identity(destination)

    @contextmanager
    def _arbitration(self, owners: _Owners, redo):
        """Judge a divergent name's owners, then decide it again (#966).

        Before this, a name whose recorded owner held different bytes was
        refused forever: the refusal was retryable, the retry met the same
        owner, and a mover whose owner had long ended reran without end
        while holding fill.  The owners' states now decide it.

        Every owner that names it is held by its transition locks --
        consumers, then movers, each set sorted, taken without blocking so a
        long egress or claim never stalls a copy; contention is ``busy`` --
        and each owner not already proven ended in this run is judged under
        them.  Then, under the stage ownership lock, the name is decided
        again by ``redo`` with a fresh collector.  The lock order is the one
        every holder keeps: transition before ownership.

        An owner proven ended is remembered for the rest of the run, so a
        range whose names one dead owner holds is judged once, not once per
        name.  An ending is terminal for the queue unless the same key is
        resubmitted, which the dead-consumer pass and an operator both do,
        and a resubmitted mover may adopt the old bytes under a name this
        run has not reached yet.  So a remembered ending is re-checked under
        the locks at every act, by :meth:`_requeued`'s four stats rather
        than a full judgment; a return drops it and the name is judged
        again.  What a full judgment reads is listed in
        :meth:`_owner_state`.

        Yields ``(state, rows, verdict, why)``:

        * ``busy``: an owner's transition lock is held elsewhere; nothing
          was judged;
        * ``changed``: under the stage lock the name no longer diverges, and
          ``verdict`` is the fresh decision, for the caller to act on while
          the lock is held;
        * ``moved``: it still diverges, but an owner that was not held and
          judged names it now, or an owner remembered as ended is queued
          again;
        * ``live``: an owner's consumer is still queued or claimed -- a real
          conflict, refused terminally and never destroyed;
        * ``uncertain``: an ending could not be proven, or a fragment could
          not be read; the refusal stays retryable, as it always was;
        * ``ended``: every owner that names it now has provably ended.

        ``rows`` are the owners that name it now, each with its state.
        """

        with self._arbitration_lock:
            pending = sorted(pair for pair in owners.pairs
                             if pair not in self._ended_owners)
            judged: dict[tuple[str, str], dict[str, object]] = {}
            with ExitStack() as held:
                consumers = sorted({c for c, _ in owners.pairs
                                    if _is_action_key(c)})
                movers = sorted({m for _, m in owners.pairs
                                 if _is_action_key(m)})
                for key in consumers:
                    if not held.enter_context(self.queue._transition_locked(
                            key, blocking=False)):
                        yield ("busy", [], None,
                               f"owner consumer {key[:12]}'s transition lock "
                               f"is held")
                        return
                for key in movers:
                    if not held.enter_context(self.queue.mover_transition_lock(
                            key, blocking=False)):
                        yield ("busy", [], None,
                               f"owner mover {key[:12]}'s transition lock "
                               f"is held")
                        return
                taken = time.perf_counter()
                held.callback(lambda: self.clock.add(
                    "owner_locks_held", time.perf_counter() - taken))
                if pending:
                    with self.clock.timing("owner_judgement"):
                        rows = self._judge_owners(set(pending))
                    for row in rows:
                        pair = (str(row["consumer_action_key"]),
                                str(row["mover_action_key"]))
                        judged[pair] = row
                        if row["state"] == "ended":
                            self._ended_owners[pair] = row
                with self._ownership():
                    fresh = _Owners()
                    with self.clock.timing("publish_decide"):
                        verdict = redo(fresh)
                    if verdict[0] != "divergent":
                        yield "changed", [], verdict, ""
                        return
                    if not fresh.pairs <= owners.pairs:
                        yield ("moved", [], verdict,
                               "an owner that was not judged names it now")
                        return
                    for pair in sorted(fresh.pairs - judged.keys()):
                        requeued = self._requeued(pair)
                        if requeued:
                            del self._ended_owners[pair]
                            yield "moved", [], verdict, requeued
                            return
                    known = {**self._ended_owners, **judged}
                    rows = [known[pair] for pair in sorted(fresh.pairs)]
                    state, why = _combined_owner_state(rows, fresh.complete)
                    yield state, rows, verdict, why

    def _requeued(self, pair: tuple[str, str]) -> str:
        """Why an owner remembered as ended may have come back, or ``""``.

        A remembered ending is re-checked at every act by the records a
        resubmission writes: a queue record for its consumer or its mover
        in ``ready/`` or ``claimed/``.  Four stats and no listing; the
        caller holds both owners' transition locks, so no record appears
        while it acts.  An unreadable stat counts as a return: the owner is
        judged again in full.
        """

        for key in pair:
            for state in (pool.READY, pool.CLAIMED):
                try:
                    self.queue.item_path(state, key).stat()
                except FileNotFoundError:
                    continue
                except OSError as exc:
                    return (f"owner {key[:12]}'s {state} record is "
                            f"unreadable: {exc}")
                return f"owner {key[:12]} is {state} again"
        return ""

    def _judge_owners(self, pairs: set[tuple[str, str]],
                      ) -> list[dict[str, object]]:
        """Each owner's state, in sorted order; the caller holds its locks."""

        try:
            from stage_release import _unended_owner
        except ImportError as exc:
            return [{"consumer_action_key": consumer,
                     "mover_action_key": mover, "state": "uncertain",
                     "why": f"the ending proof is unavailable: {exc}"}
                    for consumer, mover in sorted(pairs)]
        rows = []
        for consumer, mover in sorted(pairs):
            state, why = self._owner_state(consumer, mover, _unended_owner)
            row: dict[str, object] = {"consumer_action_key": consumer,
                                      "mover_action_key": mover,
                                      "state": state}
            if why:
                row["why"] = why
            rows.append(row)
        return rows

    def _owner_state(self, consumer: str, mover: str,
                     unended) -> tuple[str, str]:
        """``live``, ``ended`` or ``uncertain``, with why when uncertain.

        ``ended`` needs both facts below, each a queue record rather than a
        clock.  The consumer is neither queued nor claimed and has exactly
        one outcome (``stage_release._unended_owner``, the proof the
        dead-owner sweep uses).  The fragment's mover is neither queued nor
        claimed (``residency_plan.live_state``).  Both reads count any entry
        under ``claimed/`` -- the record, its lease, its status sidecar --
        as claimed, so a lease outliving its record reads as not ended.

        ``live`` is a consumer whose queue record itself is in ``claimed/``
        or ``ready/``.  Everything else -- no outcome, a lease without a
        record, a queued or leased mover, a record that cannot be read, a
        key that is not an action key -- is ``uncertain``.

        A sibling -- another mover of this copy's own consumer, as when a
        forward and a reverse pass, or two phases of one plan, stage one
        extent onto one name -- is judged by its mover alone.  Its consumer
        is this copy's own and is live by construction, so it is no
        conflict; the consumer's readers hold pins, which the act checks.

        The consumer's plan is deliberately not a fact here: a DONE
        consumer keeps its frozen plan so a retry republishes the same
        children (``tier_loop``'s dead-consumer pass), and the plan's other
        movers do not name this path.  A live claim that covers it is
        caught by :meth:`_invalidation_blocked`.

        Reads, for an ended owner: two stats and, on a miss, one listing
        each of ``ready/`` and ``claimed/``, once for the consumer and once
        for the mover, plus the consumer's outcome records.  A live owner
        costs one or two more stats of its consumer's record.
        """

        if not (_is_action_key(consumer) and _is_action_key(mover)):
            return "uncertain", "its fragment does not name two action keys"
        if consumer != self.consumer:
            why = unended(self.queue, consumer)
            if why:
                for state in (pool.CLAIMED, pool.READY):
                    try:
                        self.queue.item_path(state, consumer).stat()
                    except FileNotFoundError:
                        continue
                    except OSError as exc:
                        return "uncertain", f"{why}; {exc}"
                    return "live", ""
                return "uncertain", why
        live, error = residency_plan.live_state(self.queue, mover)
        if error:
            return ("uncertain",
                    f"its mover's queue state is uncertain: {error}")
        if live is not None:
            return "uncertain", f"its mover {mover[:12]} is still {live}"
        return "ended", ""

    def _invalidation_blocked(self, norm: str, destination: Path) -> str:
        """Why an ended owner's name may not be invalidated now, or ``""``.

        The same censuses a replacement of unattributed residue passes: no
        live pin, no other live mover claim covering the name, no sibling
        copy in flight, each read clean.  Any of them refuses retryably at
        once instead of waiting out a grace: the owners are proven ended,
        so what remains is a reader or a sibling copy, and the retry
        re-judges both.
        """

        pins = self._live_pins(norm)
        if pins is None:
            return "the pin census is unreadable"
        if pins:
            return f"it is live-pinned by {pins}"
        cover, cover_detail = self._live_claim_cover(norm)
        if cover is None:
            return f"the claim census is unreadable: {cover_detail}"
        if cover:
            return "another live mover claim covers it"
        partials = self._inflight_partials(destination)
        if partials is None:
            return "the copy census is unreadable"
        if partials:
            return f"a copy is in flight ({partials})"
        return ""

    def _invalidated(self, norm: str, rows: list[dict[str, object]]) -> None:
        """Record one replaced name; the caller holds the arbitration lock."""

        self.invalidated.append({"stage_path": norm, "owners": list(rows)})

    def _conflict(self, norm: str, declared: object, detail: str | None,
                  rows: list[dict[str, object]]) -> _PublicationRefused:
        """The terminal refusal for a name a live owner holds, naming both."""

        live = [row for row in rows if row["state"] == "live"]
        held_by = ", ".join(
            f"consumer {row['consumer_action_key']} mover "
            f"{row['mover_action_key']}" for row in live)
        return _PublicationRefused(
            f"{detail}; it is held by live owner {held_by}, and this copy "
            f"is consumer {self.consumer} mover {self.mover}: a conflict "
            f"for an owner to resolve, never retried",
            refusal=STAGED_DESTINATION_CONFLICT,
            conflict={
                "stage_path": norm,
                "consumer_action_key": self.consumer,
                "mover_action_key": self.mover,
                "manifest_sha256": self.manifest_sha256,
                "declared_sha256": (declared if isinstance(declared, str)
                                    and declared else None),
                "owners": list(rows),
            })

    @staticmethod
    def _identity(path: Path) -> dict[str, int] | None:
        try:
            info = os.stat(path)
            return {"ino": info.st_ino, "size": info.st_size,
                    "mtime_ns": info.st_mtime_ns,
                    "ctime_ns": int(getattr(info, "st_ctime_ns", 0))}
        except OSError:
            return None

    def _decide(self, destination: Path, want: int, declared: object,
                computed: str | None, source_id: str | None, heal: bool,
                owners: _Owners | None = None) -> tuple:
        """One publication verdict for ``destination``, deciding nothing twice.

        ``owners``, when given, collects who names a divergent destination,
        and the verdict for one is then ``("divergent", detail)`` rather than
        a refusal, for :meth:`_arbitration` to settle (#966).  Without it a
        divergence refuses, as it always did.
        """

        norm = os.path.normpath(str(destination))
        try:
            present = os.lstat(destination)
        except FileNotFoundError:
            return ("replace",)
        except OSError as exc:
            return ("refuse",
                    f"staged destination unstatable, not replacing "
                    f"{destination}: {exc}")
        if not statmod.S_ISREG(present.st_mode):
            return ("refuse",
                    f"staged destination is not a regular file, not "
                    f"replacing: {destination}")
        proof, standing, detail = self._proof_search(
            norm, want, declared, computed=computed, source_id=source_id,
            owners=owners)
        if proof is not None:
            return ("adopt", proof[0], proof[1])
        if standing == "unknown":
            return ("refuse",
                    f"staged publication proof unreadable for "
                    f"{destination}: {detail}; not replacing")
        if standing == "divergent":
            return ("divergent" if owners is not None else "refuse", detail)
        pins = self._live_pins(norm)
        if pins is None:
            return ("refuse",
                    f"staged pin census unreadable for {destination}; "
                    f"not replacing")
        if pins:
            return ("refuse",
                    f"shared staged name is live-pinned by "
                    f"{pins}, not replacing: {destination}")
        if standing == "owned":
            # A fragment vouches but no usable date exists: a crash between
            # the fragment and its sidecar, a publisher still running, or a
            # sidecar dating an incarnation this name no longer carries.
            # Defer to the stall policy's retry; never replace what
            # another publication names -- including after the grace.
            if heal:
                temp_note = ("still unproven after the grace, "
                             "deferring to retry")
                return ("refuse",
                        f"shared staged name is published elsewhere, "
                        f"{temp_note}: {destination}")
            return ("wait",
                    f"shared staged name is published elsewhere, "
                    f"deferring: {destination}")
        cover, cover_detail = self._live_claim_cover(norm)
        if cover is None:
            return ("refuse",
                    f"staged claim census unreadable for {destination}: "
                    f"{cover_detail}; not replacing")
        if cover:
            # A live mover claim covers this name: its fragment may still
            # land.  Defer; the retry adopts once it does.  After the
            # grace the answer is still refuse, never replace.
            if heal:
                return ("refuse",
                        f"shared staged name still has a live publisher "
                        f"after the grace, deferring to retry: "
                        f"{destination}")
            return ("wait",
                    f"shared staged name has a live publisher, "
                    f"deferring: {destination}")
        partials = self._inflight_partials(destination)
        if partials is None:
            return ("refuse",
                    f"staged copy census unreadable for {destination}; "
                    f"not replacing")
        if partials:
            if heal:
                return ("refuse",
                        f"shared staged name still has a copy in flight "
                        f"({partials}) after the grace, deferring to "
                        f"retry: {destination}")
            return ("wait",
                    f"shared staged name has a copy in flight "
                    f"({partials}), deferring: {destination}")
        # Positive absence, re-verified: no fragment, no pin, no live
        # claim, no copy in flight, every census clean.  Before the grace
        # expires this still waits for a late fragment; only a grace that
        # expires with the absence intact heals the name as orphan/crash
        # residue -- elapsed time alone never permits it.
        if heal:
            return ("replace",)
        return ("wait",
                f"shared staged name unattributed, deferring: {destination}")

    def _live_pins(self, norm: str) -> list[str] | None:
        """Pin ids live on one staged path, or None when unknowable."""

        try:
            owners, tainted = reader_lease.live_for(
                self.queue, {norm}, residency_root=self.residency_root)
        except Exception:
            return None
        if tainted:
            return None
        return sorted(owners.get(norm, []))[:5]

    def _live_claim_cover(self, norm: str) -> tuple[bool | None, str]:
        """Whether another live mover claim covers one staged name.

        The existing containment authority: ``stage_release`` attributes
        in-flight copies from their sealed claims, which exist before any
        fragment does.  This mover's own claim is excluded, so a mover
        never defers to itself.  ``None`` means unknowable (fail closed);
        the claim paths are stage-root-relative there, joined here.
        """

        try:
            from stage_release import _claimed_paths
        except ImportError as exc:
            return None, f"claim authority unavailable: {exc}"
        try:
            paths, tainted = _claimed_paths(
                self.queue, str(self.tier_id), self.cas_root,
                exclude={str(self.mover)})
        except Exception as exc:
            return None, str(exc)
        if tainted:
            return None, "; ".join(tainted[:3])
        root = str(self.stage_root)
        for relative in paths:
            if os.path.normpath(os.path.join(root, str(relative))) == norm:
                return True, "live mover claim"
        return False, ""

    def _inflight_partials(self, destination: Path) -> list[str] | None:
        """Sibling copy temporaries for one destination, sans this mover's.

        The owner-keyed ``.<name>.<owner>.partial`` convention (plus the
        legacy shared ``.<name>.partial``) is this copier's own namespace:
        a sibling means another copy is in flight -- or crashed, in which
        case the sweep reaps it and a later retry proceeds.  ``None``
        means the directory could not be read (fail closed).

        One deliberate narrowing: only a dirent whose *name* could be a
        partial for this destination is stat'ed, so a stat that fails on an
        unrelated sibling no longer fails the whole census closed.  Such a
        name is not in the answer either way -- it cannot be a partial for
        this destination -- and an unreadable *directory* still returns
        ``None``, as does a failure reading a name that does match.
        """

        own = f".{destination.name}.{str(self.mover)[:16]}.partial"
        # The name decides membership; the stat only confirms what a matching
        # name already is.  Asking them in that order spends one string
        # compare on a sibling that cannot be a partial, instead of a stat on
        # every dirent.  This runs once per publish poll -- 120 per entry --
        # and a staged tree can put every entry of a manifest in one
        # directory, so the stats it no longer does are the cost.  The legacy
        # shared ``.<name>.partial`` needs no arm of its own: it carries the
        # same prefix and the same suffix, so the general test names it.
        prefix = f".{destination.name}."
        out = []
        try:
            for entry in os.scandir(destination.parent):
                name = entry.name
                if name == own or not name.startswith(prefix):
                    continue
                if not name.endswith(".partial"):
                    continue
                if entry.is_file(follow_symlinks=False):
                    out.append(name)
        except OSError:
            return None
        return sorted(out)[:5]

    def _index_bytes(self) -> int:
        """What the reuse index currently costs, in retained bytes.

        Every retained structure is in here -- the shared interned table,
        the fragment records and the sidecar records -- each charged once
        from the object it holds.  This is the retained cost, not the peak:
        a 22 MB fragment's decode buffer is freed before the next lookup and
        belongs to the process's peak RSS instead.
        """

        return self._interned_cost + self._fragment_cost + self._material_cost

    def _forget_fragment(self, key: str) -> None:
        """Drop one fragment entry and discount exactly what it was charged."""

        previous = self._fragments.pop(key, None)
        if previous is not None:
            self._fragment_cost -= previous[2]

    def _forget_material(self, key: tuple[str, str]) -> None:
        """Drop one sidecar entry and discount exactly what it was charged."""

        previous = self._materials.pop(key, None)
        if previous is not None:
            self._material_cost -= previous[2]

    def _retain_table(self, table: _PackedPaths,
                      base: int) -> _PackedPaths | None:
        """The one shared table this record references, or ``None``.

        ``base`` is the record's own cost (file key, mover, per-record
        overhead, and for a sidecar its mention values) and is the whole
        charge the caller stores with the record.  A table's own bytes live
        in the interned charge alone: a table already interned is shared and
        costs nothing more, and a new one is charged and inserted only after
        the ceiling admits ``base`` plus its storage -- so a refusal leaves
        the index exactly as it was, never half-inserted, never a duplicate
        blob beside an equal interned one, and never a table paid for twice.

        ``_room_for`` may reclaim unreferenced tables while deciding.  A
        reclaim can only drop tables no retained record names, so the lookup
        is repeated after the check and the table re-interned and charged if
        the check removed the one it was about to share.
        """

        key = table.intern_key()
        cached = self._interned.get(key)
        if cached is None:
            if not self._room_for(base + table.retained_bytes):
                return None
            self._interned[key] = table
            self._interned_cost += table.retained_bytes
            return table
        if not self._room_for(base):
            return None
        if self._interned.get(key) is cached:
            return cached
        if not self._room_for(base + cached.retained_bytes):
            return None
        self._interned[key] = cached
        self._interned_cost += cached.retained_bytes
        return cached

    def _reclaim(self) -> None:
        """Give back what the index holds for metadata that is no longer there.

        Two kinds of residue, both of which would otherwise sit in the index
        for the life of the publisher.  A file that *disappears* from the
        forest is never visited again -- the per-lookup drop paths only run
        on a file the directory scan still lists -- so its entry is dropped
        here, by the one authority that can say it is gone: a stat.  And a
        dropped or replaced record leaves its packed path table interned but
        unreferenced, so the interned set is rebuilt from the tables the
        retained records still reference, and its charge recomputed from
        their real packed bytes rather than adjusted -- the one accounting
        that cannot drift.  One actual table stays per referenced identity,
        however many records share it.
        """

        for key in [k for k in self._fragments if not os.path.exists(k)]:
            self._forget_fragment(key)
        for key in [k for k in self._materials
                    if not reader_lease.material_path(
                        self.residency_root, k[0], k[1]).exists()]:
            self._forget_material(key)
        live: dict[tuple[bytes, bytes], _PackedPaths] = {}
        for _version, record, _charge in self._fragments.values():
            if isinstance(record, tuple) and isinstance(record[1],
                                                        _PackedPaths):
                if record[1].keys:
                    live[record[1].intern_key()] = record[1]
        for _version, record, _charge in self._materials.values():
            if isinstance(record, _PackedMentions) and record.paths.keys:
                live[record.paths.intern_key()] = record.paths
        self._interned = live
        self._interned_cost = sum(table.retained_bytes
                                  for table in live.values())

    def _room_for(self, cost: int) -> bool:
        """Whether the index can hold ``cost`` more bytes, reclaiming first.

        ``cost`` prices the record from the real packed object and charges a
        new shared table only when it is genuinely new, so a yes here is
        never optimistic.  A refusal only ever follows a reclaim, so it means
        the index is genuinely that full -- never that its accounting
        drifted.
        """

        if self._index_bytes() + cost <= _INDEX_BUDGET_BYTES:
            return True
        self._reclaim()
        return self._index_bytes() + cost <= _INDEX_BUDGET_BYTES

    def _keep_fragment(self, key: str, version: tuple, record: object,
                       charge: int) -> None:
        """Retain one fragment entry, replacing and re-pricing any previous."""

        self._forget_fragment(key)
        self._fragments[key] = (version, record, charge)
        self._fragment_cost += charge

    def _remember_verdict(self, key: str, version: tuple,
                          record: object) -> None:
        """Retain a corrupt-or-foreign file's verdict, if there is room.

        A row that names no path still costs its key, its version tuple and
        its dict slot.  Pricing it at zero is how an index with a budget
        grows without limit: a directory of empty or malformed files would
        be admitted without end.  So it is charged and checked like any
        other, and refused like any other when the ceiling is reached.
        """

        self._forget_fragment(key)
        charge = _RECORD_OVERHEAD_BYTES + _string_bytes(key)
        if not self._room_for(charge):
            return
        self._fragments[key] = (version, record, charge)
        self._fragment_cost += charge

    def _fragment_record(self, path: Path, observed: object = None) -> object:
        """One fragment file's reusable record, parsed once per version.

        ``None`` for a file that is gone or is not a residency fragment,
        ``"tainted"`` for one that cannot be read or parsed, otherwise
        ``(mover_action_key, packed set of the normalized staged paths its
        entries name)`` -- everything :meth:`_proof_candidate` asks of a
        fragment and nothing else, so a 22 MB fragment is retained as a
        packed projection rather than as its parsed document or its object
        graph.

        A ``ValueError`` verdict caches against the version that produced
        it: corrupt content cannot heal without the file changing.  An
        ``OSError`` never caches -- a permission or transport failure is
        transient, and the pre-fix code re-attempted it on every lookup, so
        unreadable stays freshly unreadable rather than becoming a held
        verdict.  The stat happens on every lookup, so an added, changed or
        removed file is seen before any decision uses it; ``observed`` is
        that stat when the caller took it before the lookup lock
        (:func:`_observe`).
        """

        key = str(path)
        current = _observe(path) if observed is None else observed
        if isinstance(current, FileNotFoundError):
            self._forget_fragment(key)
            return None
        if isinstance(current, OSError):
            self._forget_fragment(key)
            return "tainted"
        cached = self._fragments.get(key)
        if cached is not None and cached[0] == current:
            return cached[1]
        try:
            version, raw = _read_metadata(path)
        except FileNotFoundError:
            self._forget_fragment(key)
            return None
        except OSError:
            self._forget_fragment(key)
            return "tainted"
        try:
            fragment = json.loads(raw)
        except ValueError:
            self._remember_verdict(key, version, "tainted")
            return "tainted"
        if (not isinstance(fragment, dict)
                or fragment.get("schema")
                != residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1):
            self._remember_verdict(key, version, None)
            return None
        names: set[str] = set()
        for entry in (fragment.get("entries") or {}).values():
            if not isinstance(entry, dict):
                continue
            staged = entry.get("stage_path")
            if not staged:
                # ``normpath("")`` is ``"."``, which never equals an absolute
                # destination -- the pre-fix loop skipped these the same way.
                continue
            names.add(os.path.normpath(str(staged)))
        mover = str(fragment.get("mover_action_key") or "")
        # Priced from the real packed object before anything is interned, so
        # a record that will not be kept leaves nothing behind.
        self._forget_fragment(key)
        base = (_RECORD_OVERHEAD_BYTES + _string_bytes(key)
                + _string_bytes(mover))
        table = _PackedPaths(names)
        if not names:
            if not self._room_for(base):
                return mover, _EMPTY_PATHS
            self._keep_fragment(key, version, (mover, _EMPTY_PATHS), base)
            return mover, _EMPTY_PATHS
        retained = self._retain_table(table, base)
        if retained is None:
            return mover, table
        record = (mover, retained)
        # Anchored on the version the read saw, not the version the stat saw:
        # the content held is the content those bytes carried.
        self._keep_fragment(key, version, record, base)
        return record

    def _material_record(self, consumer: str, mover: str,
                         observed: object = None) -> object:
        """One sidecar's reusable record, validated once per version.

        ``None`` (absent -- an undated vouch), ``"tainted"`` (unreadable or
        invalid), or a packed projection whose ``get`` returns the ordered
        ``(bytes, sha256, file_id)`` mentions one normalized staged path
        carries, exactly as the parsed mapping did.  Ordered and complete
        per path because the digest search and the identity search may
        legitimately land on different mentions of one path.

        ``reader_lease.validate_material`` still runs, so the strictness a
        sidecar is held to is unchanged; only the repeated read is removed.
        A validated mention the fixed record cannot carry exactly -- an
        arbitrary-size integer identity -- is decided from the freshly
        parsed object, uncached and unretained, never truncated.  As with
        fragments, an ``OSError`` never caches, and ``observed`` is the
        per-lookup stat when the caller took it before the lookup lock.
        """

        cache_key = (consumer, mover)
        path = reader_lease.material_path(self.residency_root, consumer, mover)
        current = _observe(path) if observed is None else observed
        if isinstance(current, FileNotFoundError):
            self._forget_material(cache_key)
            return None
        if isinstance(current, OSError):
            self._forget_material(cache_key)
            return "tainted"
        cached = self._materials.get(cache_key)
        if cached is not None and cached[0] == current:
            return cached[1]
        try:
            version, raw = _read_metadata(path)
        except FileNotFoundError:
            self._forget_material(cache_key)
            return None
        except OSError:
            self._forget_material(cache_key)
            return "tainted"
        try:
            body = reader_lease.validate_material(json.loads(raw))
        except ValueError:
            self._forget_material(cache_key)
            charge = _RECORD_OVERHEAD_BYTES + _string_bytes(consumer) \
                + _string_bytes(mover)
            if self._room_for(charge):
                self._materials[cache_key] = (version, "tainted", charge)
                self._material_cost += charge
            return "tainted"
        mentions: dict[str, tuple] = {}
        for mention in (body.get("entries") or {}).values():
            normalized = os.path.normpath(str(mention["stage_path"]))
            mentions[normalized] = mentions.get(normalized, ()) + ((
                mention.get("bytes"), mention.get("sha256"),
                mention.get("file_id")),)
        # Priced from the real packed object before anything is interned, so
        # a record that will not be kept leaves nothing behind.
        self._forget_material(cache_key)
        compact = _PackedMentions.build(mentions)
        if compact is None:
            return mentions
        base = (_RECORD_OVERHEAD_BYTES + _string_bytes(consumer)
                + _string_bytes(mover) + compact.values_bytes)
        table = self._retain_table(compact.paths, base)
        if table is None:
            return mentions
        if table is not compact.paths:
            compact = compact.shared_with(table)
        self._materials[cache_key] = (version, compact, base)
        self._material_cost += base
        return compact

    @staticmethod
    def _published_source_id(norm: str) -> str | None:
        """The origin identity a published file still carries, if any."""

        try:
            raw = os.getxattr(norm, prewarm_loop.STAGE_SOURCE_XATTR)
        except OSError:
            return None
        try:
            return raw.decode()
        except ValueError:
            return None

    def _proof_search(self, norm: str, want: int, declared: object,
                      computed: str | None = None,
                      source_id: str | None = None,
                      owners: _Owners | None = None,
                      ) -> tuple[tuple[str, dict[str, int]] | None, str,
                                 str | None]:
        """An adoptable publication of these bytes, if one is proven.

        Returns ``(proof, standing, detail)`` where ``proof`` is
        ``(record_digest, file_id)`` and ``standing`` is one of:

        * ``"proof"`` -- some consumer's fragment entry names this path
          with equal bytes, a sidecar digest that matches, and a live
          stat equal to the sidecar's file identity.  The match is the
          declared digest; the copier's computed digest for digest-less
          manifests (no repeat hash of the existing file); or the origin
          fast path (the published file still carries this copy's source
          identity) with the sidecar digest riding along;
        * ``"divergent"`` -- a record that dates the *current* incarnation
          names the path but the bytes are provably not these: a digest
          mismatch, or the very inode the record dates was modified in
          place (same ``ino``, drifted mtime/ctime/size -- no legitimate
          publication writes in place).  Immediate refuse, never wait.
          A record whose ``file_id`` names a *different inode* -- the name
          was replaced by a real publication, exactly what the pre-fix
          unconditional rename did -- says nothing about the bytes that
          are there now, so it is skipped and the search keeps looking
          for one that dates the current incarnation (#755);
        * ``"owned"`` -- a fragment names the path without proving it
          (sidecar missing, undated, or dating a superseded incarnation):
          defer, a rerun may date it;
        * ``"clean"`` -- no fragment names the path at all;
        * ``"unknown"`` -- unreadable proof state: fail closed even over
          an otherwise valid proof, since an unreadable fragment might
          name this very path.

        No payload is hashed here: the sidecar digest is the copy-time
        content proof, and stat stability is the change detection.

        The forest is read through :meth:`_shared_census`: one directory
        census, begun after this call began, serves every search waiting on
        it (#981).  Each search then decides its own path from that census
        in the same order, with the same verdicts, and takes the sidecar and
        the live destination stat itself, fresh.

        ``owners`` (#966): without it a divergence returns at once, as it
        always did.  With it the search reads the rest of the census, which
        is already in memory, and collects every other ``(consumer, mover)``
        that names the path with a record that is not a superseded
        incarnation's date; an unreadable fragment or child directory marks
        the collection incomplete.  Divergence still outranks an unreadable
        record, exactly as the early return made it.  The collection is
        filled on every search but meaningful only on a divergence.
        """

        census = self._shared_census()
        if census[0] == "absent":
            return None, "clean", None
        if census[0] == "unreadable":
            return None, "unknown", census[1]
        standing = "clean"
        unknown: str | None = None
        found: tuple[str, dict[str, int]] | None = None
        divergent = False
        own = (self.consumer, str(self.mover))
        for child, name, path, fragment in census[1]:
            if name is None:
                # The child directory itself could not be listed.
                unknown = path
                if owners is not None:
                    owners.complete = False
                continue
            if fragment is _LOOK_AGAIN:
                observed = _observe(path)
                with self._lookup_lock:
                    fragment = self._fragment_record(path, observed)
            candidate = self._candidate_from_record(
                fragment, child, norm, want, declared,
                computed=computed, source_id=source_id)
            if (owners is not None and candidate is not None
                    and candidate not in ("tainted", "stale")
                    and isinstance(fragment, tuple)
                    and (child, str(fragment[0])) != own):
                owners.pairs.add((child, str(fragment[0])))
            if candidate == "tainted":
                unknown = f"{child}/{name}: unreadable"
                if owners is not None:
                    owners.complete = False
            elif candidate == "divergent":
                if owners is None:
                    return None, "divergent", _divergent_detail(norm)
                divergent = True
            elif candidate in ("owned", "stale"):
                # ``owned``: a vouch without a usable date.  ``stale``:
                # a date for an incarnation this name no longer carries
                # -- not evidence that the current bytes differ, so a
                # later record may still prove them.  Neither adopts
                # and neither permits replacement.
                standing = "owned"
            elif candidate is not None and found is None:
                found = candidate
        if divergent:
            return None, "divergent", _divergent_detail(norm)
        if unknown is not None:
            return None, "unknown", unknown
        if found is not None:
            return found, "proof", None
        return None, standing, None

    def _shared_census(self) -> tuple:
        """A census of the residency forest that began after this call did.

        Single flight (#981).  If no census is running, this caller runs
        one.  If one is running, it began before this call, so it is not
        fresh enough: the caller waits for it to finish and for the next
        one, which the first waiter to find no census running starts.  Every
        caller that arrived while a census ran shares that next census.  A
        search therefore sees every fragment added, changed or removed
        before it began, as each search did when it listed the forest
        itself; what goes away is the same listing repeated once per entry.

        Measured on the synthetic forest (434 directories, 145 fragments):
        one census costs about 5 ms of one core.  Sixteen copy workers each
        listing it for themselves spent 45 ms of CPU per entry, because
        ``os.scandir`` gives up the GIL around every ``readdir`` and sixteen
        threads contending for it turn each handoff into a futex round
        trip.  Before #981 the stage ownership lock serialized those
        listings, one entry at a time for the whole host.
        """

        with self._census_cond:
            wanted = self._census_started + 1
            while self._census_done < wanted:
                if not self._census_running:
                    self._census_running = True
                    self._census_started += 1
                    number = self._census_started
                    break
                self._census_cond.wait()
            else:
                return self._census  # type: ignore[return-value]
        try:
            census = self._forest_census()
        except BaseException:
            with self._census_cond:
                self._census_running = False
                self._census_cond.notify_all()
            raise
        with self._census_cond:
            self._census = census
            self._census_done = number
            self._census_running = False
            self._census_cond.notify_all()
        return census

    def _forest_census(self) -> tuple:
        """List the residency forest once, with each fragment's record.

        ``("absent",)`` when the residency root does not exist,
        ``("unreadable", detail)`` when it cannot be listed, otherwise
        ``("listed", slots)``: one ``(child, name, path, record)`` slot per
        fragment file, in the order the search visits them, and a
        ``(child, None, detail, None)`` slot for a child directory that
        could not be listed.  Each record comes from the reuse index (#761),
        stat first, so an unchanged file is not parsed again.  A record the
        index would not keep is held here as :data:`_LOOK_AGAIN`, never as
        a copy: an oversized table stays out of memory between decisions,
        as it did before.
        """

        try:
            children = sorted(
                e.name for e in os.scandir(self.residency_root)
                if e.is_dir() and not e.name.startswith(".")
                and e.name not in _NON_FRAGMENT_DIRS)
        except FileNotFoundError:
            return ("absent",)
        except OSError as exc:
            return ("unreadable", f"{self.residency_root}: {exc}")
        slots: list[tuple] = []
        for child in children:
            cdir = self.residency_root / child
            try:
                names = sorted(
                    e.name for e in os.scandir(cdir)
                    if e.is_file() and e.name.endswith(".json")
                    and not e.name.endswith(".retiring.json")
                    and not e.name.endswith(".tmp"))
            except FileNotFoundError:
                continue
            except OSError as exc:
                slots.append((child, None, f"{child}: {exc}", None))
                continue
            for name in names:
                path = cdir / name
                observed = _observe(path)
                with self._lookup_lock:
                    record = self._fragment_record(path, observed)
                    if isinstance(record, tuple) and record[1].keys:
                        kept = self._fragments.get(str(path))
                        if kept is None or kept[1] is not record:
                            record = _LOOK_AGAIN
                slots.append((child, name, path, record))
        return ("listed", tuple(slots))

    def _proof_candidate(self, fragment_path: Path, consumer: str,
                         norm: str, want: int, declared: object,
                         computed: str | None = None,
                         source_id: str | None = None,
                         ) -> tuple[str, dict[str, int]] | str | None:
        """One fragment file's verdict: proof, "divergent", "stale",
        "owned", "tainted" or None (names nothing here).

        Identity before content: the sidecar's ``file_id`` is compared
        against the live stat first.  A record dating a different inode
        names a superseded incarnation and says nothing about the bytes
        that are there now, whatever digest it carries (#755); a record
        dating the *same* inode that has since changed in place, or a
        digest mismatch on the current incarnation, is divergence.  Only
        a record that dates the current incarnation may prove these
        bytes.

        The fragment and the sidecar are read through the publisher's
        invocation-local reuse (#761), so an unchanged file is parsed once
        per version rather than once per destination.  What is decided is
        unchanged -- same order, same verdicts, same fail-closed answers --
        and the live destination stat below is still taken fresh on every
        call.  Each file's per-lookup stat is taken before the lookup lock
        and only the compare, read and parse run under it (#981); a record
        handed out of the index is immutable, so it is read outside.
        """

        observed = _observe(fragment_path)
        with self._lookup_lock:
            fragment = self._fragment_record(fragment_path, observed)
        return self._candidate_from_record(
            fragment, consumer, norm, want, declared,
            computed=computed, source_id=source_id)

    def _candidate_from_record(self, fragment: object, consumer: str,
                               norm: str, want: int, declared: object,
                               computed: str | None = None,
                               source_id: str | None = None,
                               ) -> tuple[str, dict[str, int]] | str | None:
        """:meth:`_proof_candidate`'s verdict for an already looked-up record.

        ``fragment`` is what :meth:`_fragment_record` returned for the file;
        the sidecar and the live destination stat are taken here, fresh.
        """

        if fragment is None:
            return None
        if fragment == "tainted":
            return "tainted"
        mover, staged_paths = fragment
        if norm not in staged_paths:
            return None
        if not mover:
            return "owned"
        observed = _observe(reader_lease.material_path(
            self.residency_root, consumer, mover))
        with self._lookup_lock:
            sidecar = self._material_record(consumer, mover, observed)
        if sidecar is None:
            # Published vouch without a date: unprovable either way.
            return "owned"
        if sidecar == "tainted":
            return "tainted"
        # Every mention this sidecar makes of the path, in sidecar order:
        # the digest search and the identity search below may legitimately
        # settle on different mentions, exactly as they could before.
        mentions = sidecar.get(norm, ())
        digest: str | None = None
        for size, recorded, _identity in mentions:
            try:
                if int(size) != want:
                    continue
            except (TypeError, ValueError):
                continue
            digest = recorded
            if not isinstance(digest, str) or not digest:
                continue
            break
        else:
            return "owned"
        file_id = reader_lease.stat_identity(norm)
        published = None
        for _size, _recorded, identity in mentions:
            if isinstance(identity, dict):
                published = identity
                break
        if file_id is None or not isinstance(published, dict):
            # Identity unreadable: the record can neither prove these bytes
            # nor clear them, and the fail-closed answer stays divergent.
            return "divergent"
        if int(published.get("ino", -1)) != int(file_id["ino"]):
            # A date for an incarnation this name no longer carries: the
            # name was replaced -- a publication mints a fresh inode, which
            # is exactly what the pre-fix unconditional rename did.  Not
            # proof, not divergence, not permission to overwrite: another
            # record may still date the incarnation that is there (#755).
            return "stale"
        if not reader_lease.file_id_matches(published, file_id):
            # Same inode, changed since the record dated it: an in-place
            # write, which no legitimate publication performs.  The dated
            # bytes are provably not the bytes that are there -- the one
            # shape of stat mismatch that is immediate divergence.
            return "divergent"
        if isinstance(declared, str) and declared:
            if digest != declared:
                return "divergent"
            record_digest: str = declared
        elif (source_id is not None
                and self._published_source_id(norm) == source_id):
            # Origin fast path: the published file still carries this
            # copy's source identity, so the source has not changed since
            # the vouched copy -- adopt without copying.
            record_digest = digest
        elif computed is not None:
            # The necessary private copy's digest against the stored
            # material proof: same content adopts, a changed source
            # refuses -- without rehashing the existing file.
            if computed != digest:
                return "divergent"
            record_digest = digest
        else:
            return "owned"
        return record_digest, file_id


class _Copier:
    """One range, copied in read order by a bounded set of workers."""

    def __init__(self, *, mounts: prewarm_loop.MountMap, pacer, stage_root: Path,
                 mount_prefix: str, block: int, workers: int,
                 owner: str = "",
                 namespace: str | None = None,
                 source_stage_root: Path | str | None = None,
                 publisher: _StagedPublisher | None = None) -> None:
        self.mounts = mounts
        self.pacer = pacer
        self.stage_root = stage_root
        self.mount_prefix = mount_prefix
        self.namespace = namespace
        self.block = block
        self.workers = max(1, workers)
        self.owner = str(owner or "")
        self.publisher = publisher
        #: Per-phase thread-seconds for the receipt, shared with the
        #: publisher so one clock sees the copy and its publication (#981).
        self.clock = publisher.clock if publisher is not None else _PhaseClock()
        #: Read the copy's bytes from an already-staged tree instead of the
        #: pool: a promotion's source is the stage, where split ranges live
        #: under their staged names from byte zero rather than under the
        #: manifest's pool path at the manifest offset.  ``None`` keeps the
        #: pool behavior (``mounts`` plus the entry offset); set, the caller
        #: passes the staged source per entry (see ``run``) and the offset is
        #: always zero.  ``stage_move`` leaves this unset.
        self.source_stage_root = (
            None if source_stage_root is None else Path(source_stage_root))
        self.lock = threading.Lock()
        self.staged: dict[str, dict[str, object]] = {}
        #: Publish-time identity per staged key, filed beside the fragment
        #: as the material sidecar: the map entry stays schema-v1, and this
        #: dates the vouching (see ``reader_lease.write_material``).
        self.sidecar: dict[str, dict[str, object]] = {}
        self.bytes_staged = 0
        self.errors: list[str] = []
        #: Bumped under the lock on every landed entry, so a fragment written
        #: outside the lock can say which snapshot it is and an older one can
        #: be discarded rather than written over a newer one.
        self.generation = 0
        #: Set when the publication gate refused an entry (#853).  Internal
        #: to this one range and never the caller's ``stop`` event: it stops
        #: the dispatch of further entries, while the external event stays
        #: the caller's cancellation/withdrawal decision with its own
        #: terminal.  An already-dispatched entry may still finish or refuse;
        #: what it commits stays committed.  It is set under the dispatch
        #: lock, so a refusal recorded there cannot be followed by another
        #: entry being handed out.
        self.publication_refused = threading.Event()
        #: The first terminal refusal the gate raised, and the conflict it
        #: named (#966); both stay ``None`` for a retryable refusal.  Set
        #: under the dispatch lock beside :attr:`publication_refused`.
        self.refusal: str | None = None
        self.conflict: dict[str, object] | None = None

    def _temporary(self, destination: Path) -> Path:
        """This mover's own temporary beside ``destination`` (#620).

        Stage paths are content-addressed per manifest entry and shared
        between consumers, so two movers -- a dead consumer's unstarted one
        and its successor's -- copy one entry to one destination.  A shared
        ``.<name>.partial`` is then truncated by both and renamed away by the
        winner, and the loser's rename fails with ENOENT (or worse, lands a
        torn prefix).  Keying the temporary by the mover keeps each copy's
        rename its own: both verify the same digest and both land the same
        bytes.  The first 16 hex characters name the mover uniquely enough --
        a collision only reverts to the old sharing -- and keep long staged
        names inside the filename limit.  An empty owner keeps the legacy
        name, so a direct run and every old test stages exactly as before.
        The sweep still recognises both spellings: each starts with ``.``
        and ends with ``.partial``.
        """

        if not self.owner:
            return destination.with_name(f".{destination.name}.partial")
        return destination.with_name(
            f".{destination.name}.{self.owner[:16]}.partial")

    def _copy_one(self, entry: dict[str, object], destination: Path,
                  admission, stop: threading.Event,
                  source: Path | str | None = None,
                  source_offset: int | None = None
                  ) -> tuple[int, str, dict[str, int] | None]:
        """Read this entry's bytes, write them, hash them; return size and digest.

        The temporary lives beside the final name so the publish is a rename
        within one dataset, and it is removed on any failure: a stage littered
        with half-copies would charge the tier for bytes no map ever names.

        ``source``/``source_offset`` override the pool default (the mounts map
        plus the entry's own offset) for copies whose source is an already-
        staged tree, where split ranges live under their staged names from
        byte zero.  Omitted, the pool behavior is byte-identical.

        With a ``publisher`` (both production movers pass one), an
        already-published incarnation is adopted without copying, and the
        final rename goes through the publication gate under the stage
        ownership exclusion; without one (unit-constructed copiers on
        isolated scratch) the legacy direct rename runs.
        """

        want = int(entry["bytes"])
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = self._temporary(destination)
        if source is None:
            source = self.mounts.local(str(entry["path"]))
            source_offset = int(entry["offset"])
        source = str(source)
        offset = int(source_offset
                     if source_offset is not None else entry["offset"])
        # One stat of the source, for the origin fast path: compared
        # against the published file's existing ``user.pbstage.source``
        # evidence, never a new binding.
        source_id = _origin_id_of(source)
        if self.publisher is not None and want > 0:
            try:
                adopted = self.publisher.try_adopt(
                    entry, destination, source_id)
            except OSError:
                temporary.unlink(missing_ok=True)
                raise
            if adopted is not None:
                # Adopting skips the copy, but a crashed predecessor's
                # owner-keyed temp for this destination must still go:
                # it is never the published bytes.
                temporary.unlink(missing_ok=True)
                self.clock.outcome("adopted")
                return adopted
        digest = hashlib.sha256()
        written = 0
        # Per-entry phase sums, added to the receipt's clock once the copy
        # ends: a clock lock per block would cost more than it measures.
        spent = {"pace_wait": 0.0, "copy_read": 0.0, "copy_write": 0.0,
                 "hash": 0.0, "fsync": 0.0}
        now = time.perf_counter
        # O_NOFOLLOW at the leaf and a regular-file check, for the reason the
        # prewarm loop gives: the manifest is submitter-supplied and a symlink
        # swapped in after validation must fail here rather than send this copy
        # reading something outside the mount the manifest declared.
        fd = os.open(source, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
        try:
            if not statmod.S_ISREG(os.fstat(fd).st_mode):
                raise OSError(f"not a regular file: {source}")
            if offset:
                os.lseek(fd, offset, os.SEEK_SET)
            with open(temporary, "wb") as sink:
                buffer = bytearray(self.block)
                view = memoryview(buffer)
                while written < want and not stop.is_set():
                    mark = now()
                    if not admission.acquire(
                            self.limit, stop,
                            self.pacer.hold_s if self.pacer is not None else 0.25):
                        spent["pace_wait"] += now() - mark
                        break
                    try:
                        if self.pacer is not None:
                            self.pacer.wait(stop)
                            if stop.is_set():
                                break
                        read_at = now()
                        spent["pace_wait"] += read_at - mark
                        chunk = os.readv(fd, [view[:min(self.block, want - written)]])
                        mark = now()
                        spent["copy_read"] += mark - read_at
                    finally:
                        admission.release()
                    if not chunk:
                        break
                    sink.write(view[:chunk])
                    hashed_at = now()
                    spent["copy_write"] += hashed_at - mark
                    digest.update(view[:chunk])
                    spent["hash"] += now() - hashed_at
                    written += chunk
                mark = now()
                sink.flush()
                os.fsync(sink.fileno())
                spent["fsync"] += now() - mark
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
        finally:
            os.close(fd)
            for phase, seconds in spent.items():
                self.clock.add(phase, seconds)
        if written != want:
            temporary.unlink(missing_ok=True)
            raise OSError(f"short read on {source}: {written} of {want} bytes")
        mark = now()
        computed = digest.hexdigest()
        self.clock.add("hash", now() - mark, calls=0)
        declared = entry.get("sha256")
        if isinstance(declared, str) and declared != computed:
            # Before the rename, not after.  These bytes are not the manifest's
            # bytes, and the rename is the publish: checking afterwards means
            # unlinking a destination another mover may have staged correctly
            # -- two movers cover one entry whenever a consumer is re-dispatched
            # under a new key, or whenever an entry straddles two adjacent
            # windows -- which leaves that mover's fragment naming bytes that
            # are gone.  Nothing outside this temporary is touched.
            temporary.unlink(missing_ok=True)
            raise OSError(f"digest mismatch on {source}: manifest says "
                          f"{declared[:12]}, the copy is {computed[:12]}")
        if source_id is not None:
            # File the origin change-detection the prewarm loop defined
            # (same name, same format), best-effort: a filesystem that
            # will not carry it still converges through the digest
            # comparison at publication.
            try:
                os.setxattr(temporary, prewarm_loop.STAGE_SOURCE_XATTR,
                            source_id.encode())
            except OSError:
                pass
        if self.publisher is not None:
            with self.clock.timing("publish"):
                return self.publisher.publish(entry, destination, temporary,
                                              computed, stop, source_id)
        os.replace(temporary, destination)
        self.clock.outcome("renamed")
        try:
            info = os.stat(destination)
            identity = {"ino": info.st_ino, "size": info.st_size,
                        "mtime_ns": info.st_mtime_ns,
                        "ctime_ns": int(getattr(info, "st_ctime_ns", 0))}
        except OSError:
            identity = None
        return written, computed, identity

    def run(self, entries: list[dict[str, object]], *,
            stop: threading.Event, on_entry=None,
            named_once: frozenset[str] = frozenset()) -> None:
        """Copy the window; a gate refusal ends dispatch for this run.

        ``named_once`` is :func:`paths_named_once` of the whole manifest; it
        decides produced-output names only (:func:`stage_relative`).

        ``stop`` is the caller's cancellation event and is never set here.  A
        ``_PublicationRefused`` sets the copier's own
        :attr:`publication_refused` under the same small dispatch lock that
        hands out entries, before the entry error is logged, so no worker is
        given a new entry once the refusal is recorded.  The entries already
        dispatched to the active group finish or refuse normally and
        everything they committed stays committed.  The range is incomplete
        either way, so the remaining entries are not judged unpublishable --
        they are left to the retry -- and the first obstruction stays visible
        instead of being buried under later per-entry errors.  Plain
        per-entry failures (a source read, a digest mismatch, an I/O error)
        keep the existing behavior of recording the error and continuing.
        """

        admission = prewarm_loop.Admission()
        self.limit = (self.pacer.depth if self.pacer is not None
                      else (lambda: self.workers))
        work: queuelib.Queue = queuelib.Queue()
        for entry in entries:
            work.put(entry)
        for _ in range(self.workers):
            work.put(None)

        #: Serializes handing out an entry with recording a refusal, so a
        #: refusal can never be followed by another entry being dispatched.
        #: Never held while logging (which takes ``self.lock``) or copying.
        dispatch = threading.Lock()

        def record_error(path: str, exc: object) -> None:
            with self.lock:
                if len(self.errors) < 20:
                    self.errors.append(f"{os.path.basename(path)}: {exc}")

        def take() -> dict[str, object] | None:
            """The next entry, or ``None`` once dispatch has ended."""

            with dispatch:
                if self.publication_refused.is_set():
                    return None
                return work.get()

        def worker() -> None:
            while not stop.is_set():
                entry = take()
                if entry is None:
                    return
                path, offset = str(entry["path"]), int(entry["offset"])
                try:
                    relative = stage_relative(
                        path, offset, int(entry["bytes"]),
                        mount_prefix=self.mount_prefix,
                        namespace=self.namespace,
                        named_once=path in named_once)
                    destination = self.stage_root / relative
                    if self.source_stage_root is not None:
                        # A promotion reads the staged tree, where this same
                        # relative name is what the stage mover wrote -- split
                        # ranges from byte zero, never the manifest's pool
                        # path at the manifest offset.
                        staged_source = self.source_stage_root / relative
                        staged_offset = 0
                    else:
                        staged_source, staged_offset = None, offset
                    written, digest, identity = self._copy_one(
                        entry, destination, admission, stop,
                        source=staged_source, source_offset=staged_offset)
                except _PublicationRefused as exc:
                    # The range cannot publish what it declared, so stop
                    # issuing work: record the refusal under the dispatch
                    # lock first (peers stop being handed entries while this
                    # thread is still logging), then log it outside that lock
                    # -- ``record_error`` takes ``self.lock``.  The entries
                    # already dispatched finish or refuse, and what they
                    # committed stays committed.  This is not the caller's
                    # stop: cancellation and withdrawal keep their own
                    # decision and terminal.
                    with dispatch:
                        self.publication_refused.set()
                        if exc.refusal is not None and self.refusal is None:
                            self.refusal = exc.refusal
                            self.conflict = exc.conflict
                    record_error(path, exc)
                    return
                except (OSError, ValueError) as exc:
                    # An ordinary source read, digest or filesystem failure:
                    # one entry's trouble, not evidence about the range.
                    record_error(path, exc)
                    continue
                record = {
                    "stage_path": str(destination),
                    "bytes": written,
                    "offset": offset,
                    "sha256": digest,
                }
                key = residency_map.residency_map_key(path, offset)
                with self.lock:
                    self.staged[key] = record
                    if identity is not None:
                        self.sidecar[key] = {
                            "stage_path": str(destination),
                            "bytes": written,
                            "sha256": digest,
                            "file_id": identity,
                        }
                    self.bytes_staged += written
                    self.generation += 1
                    # Snapshot under the lock, write outside it.  Holding the
                    # lock across an NFS fsync serializes every other worker
                    # behind one round trip; the generation is what keeps an
                    # older snapshot from landing on top of a newer one once
                    # they are no longer ordered by the lock.
                    snapshot = ((dict(self.staged), dict(self.sidecar),
                                 self.generation)
                                if on_entry else None)
                if snapshot is not None:
                    try:
                        with self.clock.timing("fragment_publication"):
                            on_entry(*snapshot)
                    except (OSError, ValueError) as exc:
                        # A fragment that will not write is a degradation, not
                        # a reason to stop copying, and least of all a reason
                        # to let this thread die inside the lock: that killed
                        # every worker in turn on its next landed entry and
                        # still reported ``errors: []``.  The entry stays in
                        # ``staged`` -- it is on the stage -- so the final
                        # publish names it or fails loudly.
                        with self.lock:
                            if len(self.errors) < 20:
                                self.errors.append(f"fragment publication: {exc}")

        threads = [threading.Thread(target=worker, daemon=True)
                   for _ in range(self.workers)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        admission.close()


class _ProgressReporter:
    """Report this mover's landed bytes to the worker's stall check (#1010).

    A mover sealed with a progress policy (``movement_actions.
    mover_progress_policy``) is ended by the worker's ``no_progress`` rung
    when it commits nothing for its grace.  This is the other half: every
    ``interval_s`` a thread of its own reads the copier's landed bytes and
    commits them through :mod:`prismabuild.progress` when they grew.  An
    entry counts once it is copied, verified and renamed into place, which
    is the durable unit the contract asks for, and the copy loop does no
    extra work for it: the count is the one the copier already keeps under
    its lock per landed entry.

    In the ``warm`` phase the count goes on growing by the bytes read back,
    so the read-back of a large range is not quiet either.  The ``unit``
    names the range, so the ending record of a stalled copy names the bytes
    landed and the range they belong to.

    Without a progress channel -- a mover sealed with no policy, or a direct
    run -- nothing starts and nothing is written.  A report that fails is
    counted and never fails the copy: the worker then reads the mover as
    quiet, which is the honest verdict.
    """

    def __init__(self, copier: "_Copier", *, interval_s: float,
                 range_start_bytes: int, range_end_bytes: int) -> None:
        self.copier = copier
        self.interval_s = max(0.001, float(interval_s))
        self.unit = (f"bytes landed of read-order range "
                     f"[{int(range_start_bytes)}, {int(range_end_bytes)})")
        self.channel = pb_progress.channel() is not None
        self.phase = movement_actions.MOVER_COPY_PHASE
        self.warm_state: dict[str, int] | None = None
        self.reported_units = 0
        self.reported_phase: str | None = None
        self.reports = 0
        self.unwritten = 0
        self.last_reported_unix: float | None = None
        self.refusal: str | None = None
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def units(self) -> int:
        with self.copier.lock:
            landed = int(self.copier.bytes_staged)
        warm = self.warm_state
        return landed + (int(warm.get("bytes", 0)) if warm is not None else 0)

    def report(self) -> None:
        """Commit the count now if it grew or the phase moved on."""

        if not self.channel or self.refusal is not None:
            return
        with self._lock:
            units = self.units()
            if units <= self.reported_units and self.phase == self.reported_phase:
                return
            if units == 0 and self.reported_phase is None:
                # The first phase is granted at launch; a zero count earns
                # nothing and would only be rejected as a replay.
                return
            try:
                written = pb_progress.commit(units, self.phase, unit=self.unit)
            except ValueError as exc:
                # A phase this launch did not declare: the policy and this
                # tool disagree, and every later report would be refused too.
                self.refusal = str(exc)
                return
            if not written:
                self.unwritten += 1
                return
            self.reports += 1
            self.reported_units = max(self.reported_units, units)
            self.reported_phase = self.phase
            self.last_reported_unix = time.time()

    def start(self) -> None:
        if not self.channel or self._thread is not None:
            return

        def loop() -> None:
            while not self._stop.wait(self.interval_s):
                self.report()

        self._thread = threading.Thread(target=loop, daemon=True,
                                        name="stage-move-progress")
        self._thread.start()

    def enter_warm(self, state: dict[str, int]) -> None:
        """The copy is done; count the read-back from here on."""

        self.report()
        self.warm_state = state
        self.phase = movement_actions.MOVER_WARM_PHASE
        self.report()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join()
        self.report()

    def record(self) -> dict[str, object]:
        """What the receipt says about the reports this copy made."""

        return {"channel": self.channel, "interval_s": self.interval_s,
                "reports": self.reports, "units_reported": self.reported_units,
                "phase": self.reported_phase, "unit": self.unit,
                "last_reported_unix": self.last_reported_unix,
                "unwritten": self.unwritten, "refusal": self.refusal}


def warm_staged(paths: "list[str]", *, workers: int = 4,
                block: int = prewarm_loop.BLOCK,
                stop: "threading.Event | None" = None,
                state: "dict[str, int] | None" = None) -> dict[str, object]:
    """Read staged files back, on the box that owns the stage.

    This is the whole of cache layer 2, and it is one sentence long because
    that is all ZFS needs: the ARC is filled by reads, the copy that wrote
    these files is a *write*, and nothing else in PrismaBuild ever opens them
    on this box.  Without this the staged blocks reached the ARC only
    incidentally and left it as other shards landed -- a repeat read on
    dl380g10 fell from 9580 to 7423 MiB/s while the range sat untouched --
    and the consumer paid the SSD price for every one of them: 2402 MB/s
    against 10045 MB/s on the same file at the same concurrency
    (sparky -> dl380g10, ``dd iflag=direct``, 16 x 256 MiB, 2026-09-18).

    Buffered on purpose -- no ``O_DIRECT`` -- because the point is to leave
    the bytes in the server's cache rather than to measure the device.  A file
    that cannot be read is an error on the receipt and never a raise: the copy
    is published and its map is written before this runs, so a failed warm
    costs speed and nothing else.

    ``state``, when given, is the dict the running totals are kept in, so a
    reader outside the warm (the mover's progress reporter, #1010) sees them
    grow; it is updated once per file, never per block.
    """

    stop = stop or threading.Event()
    work: queuelib.Queue = queuelib.Queue()
    for path in paths:
        work.put(path)
    workers = max(1, min(int(workers), len(paths) or 1))
    for _ in range(workers):
        work.put(None)
    lock = threading.Lock()
    if state is None:
        state = {}
    state.update({"bytes": 0, "entries": 0})
    errors: list[str] = []

    def worker() -> None:
        buffer = bytearray(block)
        view = memoryview(buffer)
        while not stop.is_set():
            path = work.get()
            if path is None:
                return
            read = 0
            try:
                fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
            except OSError as exc:
                with lock:
                    if len(errors) < 20:
                        errors.append(f"{os.path.basename(path)}: {exc}")
                continue
            try:
                while not stop.is_set():
                    chunk = os.readv(fd, [view])
                    if not chunk:
                        break
                    read += chunk
            except OSError as exc:
                with lock:
                    if len(errors) < 20:
                        errors.append(f"{os.path.basename(path)}: {exc}")
            finally:
                os.close(fd)
            with lock:
                state["bytes"] += read
                state["entries"] += 1

    started = time.time()
    threads = [threading.Thread(target=worker, daemon=True)
               for _ in range(workers)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    elapsed = max(1e-9, time.time() - started)
    return {"bytes": state["bytes"], "entries": state["entries"],
            "seconds": round(elapsed, 3),
            "mb_per_s": round(state["bytes"] / 1e6 / elapsed, 1),
            "errors": errors}


def arc_warm_verdict(args) -> dict[str, object]:
    """Whether this mover warms its range, decided by the box that owns the tier.

    Read off the live tier record rather than sealed into the argv, for the
    reason ``movers_claimed_on_tier`` is read there: ``primarycache`` is a
    fact about the dataset *now*, and a mover sealed a week ago must not carry
    last week's answer.  Three outcomes, all of them named on the receipt --
    the warm is refused when the ARC may not hold this dataset's data at all,
    when the operator asked for no warm, and when the tier cannot be read,
    because a warm spends reads and an unattested setting does not license
    them.
    """

    mode = str(getattr(args, "warm_after_copy", "auto") or "auto")
    if mode == "never":
        return {"warm": False, "state": "disabled", "primarycache": None,
                "reason": "--warm-after-copy never"}
    if mode == "always":
        return {"warm": True, "state": "warmed", "primarycache": None,
                "reason": "--warm-after-copy always"}
    try:
        queue = pool.PoolQueue(Path(args.pool_root))
        records = queue.tiers()
    except (OSError, pool.PoolContractError) as exc:
        return {"warm": False, "state": "refused", "primarycache": None,
                "reason": f"the tier record could not be read: {exc}"}
    for record in records:
        if str(record.get("tier_id")) != str(args.tier_id):
            continue
        verdict = storage_tiers.stage_arc_eligibility(record)
        if not verdict["eligible"]:
            return {"warm": False, "state": "refused",
                    "primarycache": verdict["primarycache"],
                    "reason": str(verdict["reason"])}
        # No budget question here, and that is a decision rather than an
        # omission: a mover co-demanding the ARC's GiB would bound the
        # published SSD window by min(stage, ARC) and throttle layer 1's
        # read-ahead once the ARC shrinks for an explicit RAM tier (#638's
        # part 3, withdrawn).  The warm is therefore bounded by this mover's
        # own range and by nothing across movers -- successive phases may
        # evict each other's warm, which costs the *pre*-warm and never the
        # residency the verdict gates on.  The RAM tier's movement nodes
        # bring the occupancy budget back, pointed at ``ram_gib``.
        return {"warm": True, "state": "warmed",
                "primarycache": verdict["primarycache"],
                "reason": str(verdict["reason"])}
    return {"warm": False, "state": "refused", "primarycache": None,
            "reason": f"no box announces the tier {args.tier_id!r}"}


def load_manifest(cas_root: Path, action_key: str,
                  manifest_path: str | None) -> dict[str, object]:
    """The manifest this mover moves, from its own sealed request or a file."""

    if manifest_path:
        # The same reader the sealed path gets, not a bare ``json.load``:
        # everything downstream assumes core already refused these shapes, so
        # a zero-byte entry reaches the map's ``_positive`` check and a
        # repeated (path, offset) pair stages two copies under one key.
        # "Testing and one-off moves" is exactly where a hand-written manifest
        # enters, and it reads gzip for free.
        try:
            return pb.load_data_manifest(manifest_path)
        except (pb.ActionContractError, pb.CASTamperError) as exc:
            raise SystemExit(f"{manifest_path}: {exc}") from None
    request = prewarm_loop.sealed_request(cas_root, action_key)
    entry = prewarm_loop.manifest_input_of(request)
    if entry is None:
        raise SystemExit(
            f"action {action_key} seals no data manifest and no --manifest was given")
    manifest = prewarm_loop.load_manifest(cas_root, entry)
    if manifest is None:
        raise SystemExit(f"the data manifest of {action_key} could not be read")
    return manifest


def served_for(args) -> dict[str, object]:
    """The addresses whose reads are this move's own, and how they were found.

    Derived, never configured: the consumer's claim record names its box, and
    that box's worker offer names the client addresses the server's per-client
    counter is keyed by.  Both are records the fleet already writes.  A broken
    link leaves the set empty and the pacer protecting every client, and the
    reason travels into the receipt so the slow case is never silent.
    """

    if args.served_host or args.served_address:
        return {"served_host": args.served_host or "",
                "served_addresses": tuple(args.served_address or ()),
                "served_reason": "named on the command line"}
    try:
        queue = pool.PoolQueue(Path(args.pool_root))
        claim = pool._read_json(
            queue.item_path(pool.CLAIMED, str(args.consumer_action_key)))
    except (OSError, pool.PoolContractError):
        claim = None
    if not isinstance(claim, dict):
        return {"served_host": "", "served_addresses": (),
                "served_reason": "consumer is not claimed, so every client is "
                                 "a client to protect"}
    host = prewarm_loop.claimed_host_of(claim)
    found = prewarm_loop.served_addresses(queue, host)
    return {"served_host": found["served_host"],
            "served_addresses": found["served_addresses"],
            "served_reason": str(found["served_reason"])}


def announced_pool_identity(pool_root: str | Path,
                            tier_id: str) -> dict[str, object] | None:
    """The pool identity the tier loop announced for ``tier_id``, or ``None``.

    Copied into the mover's receipt so the receipt folds can key it to the
    pool it measured (#611).  ``None`` -- no record announced, or one stamped
    by an older generation -- files a receipt exactly as before, which a
    gated fold drops and an ungated one reads.
    """

    try:
        records = pool.PoolQueue(Path(pool_root)).tiers()
    except (OSError, pool.PoolContractError):
        return None
    for record in records:
        if str(record.get("tier_id")) != str(tier_id):
            continue
        identity = record.get("pool_identity")
        return dict(identity) if isinstance(identity, Mapping) else None
    return None


def _resume_own_coverage(queue, *, consumer_action_key: str,
                         mover_action_key: str, tier_id: str,
                         stage_root: Path, manifest_sha256: str,
                         residency_root: Path,
                         window: list[dict[str, object]],
                         mount_prefix: str,
                         namespace: str | None = None,
                         named_once: frozenset[str] = frozenset(),
                         timings: dict[str, float] | None = None,
                         ) -> tuple[dict[str, dict[str, object]],
                                    dict[str, dict[str, object]], str | None]:
    """This mover's own prior coverage, qualified for a same-key retry.

    A retried mover starts every dictionary empty, and its first
    incremental publication *replaces* the fragment and material it filed
    under the same consumer and mover keys -- so the qualified suffix its
    previous attempt published loses proof, is recopied, and pays the
    publication grace per entry.  This is the narrow resume for exactly
    that: the prior **own** records are read and qualified -- document
    reads, header checks and the per-entry file stats together -- inside
    the stage ownership lock, and the retry's publications are never
    smaller than the coverage it inherited.

    Unknown or contradictory ownership refuses the whole invocation
    (``SystemExit``) before any copy or publication.  The causal case:
    the prior own fragment is corrupt or conflicting, old destinations
    still hold bytes, and one declared destination has never landed --
    without the refusal the absent destination replaces cleanly, its
    publication overwrites the tainted fragment, and this or the next
    retry reads the erased evidence as absence.  Confirmed absence of the
    own fragment (``FileNotFoundError``) is the one non-conflict: nothing
    is preserved and nothing that says anything is overwritten, so a
    first publication initializes empty through the ordinary machinery.

    Two tiers, deliberately different.  The **vouch** (the fragment
    record) is preserved for every prior entry this window derives whose
    record agrees with the window's own derivation -- an entry named
    outside this window, or a record at a different extent than the
    manifest derives for a key inside it, is conflicting ownership and
    refuses, never a silent prune into smaller coverage.  The **date**
    (the material sidecar mention) is carried only where the sidecar's
    headers qualify against this invocation and the fragment (consumer,
    mover, tier, stage root, manifest, and the SSD no-epoch convention)
    *and* the existing proof standard still holds per entry: the
    manifest's declared digest agrees, and the sidecar's ``file_id``
    matches the live file (:func:`reader_lease.file_id_matches` over
    :func:`stat_identity`).  A readable sidecar with contradictory
    headers is conflicting state and refuses -- it must never be carried
    and republished under corrected headers -- while an absent or
    unparseable sidecar is the documented crash window (a vouch without a
    date): vouches are kept, no date is invented, and the rerun
    overwrites both as it always has.  No payload is hashed; an undated
    vouch is preserved as exactly that, never upgraded.

    Returns ``(staged_seeds, sidecar_seeds, resumed_generation)``.  Empty
    seeds with a ``None`` generation mean "nothing resumable", which is
    today's behavior, not an error.
    """

    staged: dict[str, dict[str, object]] = {}
    sidecar: dict[str, dict[str, object]] = {}

    # The names this window derives, by the map key the fragment uses.
    # Pure derivation from the sealed inputs -- no state is read -- so it
    # runs before the lock without widening the transaction.
    facts: dict[str, tuple[int, str, str, int]] = {}
    for entry in window:
        path, offset = str(entry["path"]), int(entry["offset"])
        relative = stage_relative(
            path, offset, int(entry["bytes"]),
            mount_prefix=mount_prefix, namespace=namespace,
            named_once=path in named_once)
        key = residency_map.residency_map_key(path, offset)
        facts[key] = (int(entry["bytes"]), os.path.normpath(str(
            Path(stage_root) / relative)), str(entry.get("sha256") or ""),
            offset)

    fragment_path = residency_map.fragment_path(
        residency_root, consumer_action_key, mover_action_key)
    asked = time.perf_counter()
    with queue.stage_ownership_lock(str(stage_root)):
        if timings is not None:
            # The first time a mover queues for the stage ownership lock:
            # an egress's hold is paid here before the start gate (#988).
            timings["resume_lock_wait_s"] = time.perf_counter() - asked
        try:
            with open(fragment_path, "rb") as stream:
                fragment = residency_map.validate_fragment(json.load(stream))
        except FileNotFoundError:
            # Confirmed absence: a first publication, or everything this
            # key ever staged was retired.  Not conflict -- there is no
            # ownership document to preserve or contradict.
            return {}, {}, None
        except (OSError, ValueError) as exc:
            raise SystemExit(
                f"residency_prior_fragment_unreadable: this mover's own "
                f"fragment exists but is neither readable nor valid, and "
                f"refusing is what preserves it -- one landed destination "
                f"would otherwise overwrite it as apparent absence: {exc}")
        conflicts = [
            name for name, bad in (
                ("consumer", str(fragment["consumer_action_key"])
                 != consumer_action_key),
                ("mover", str(fragment["mover_action_key"])
                 != mover_action_key),
                ("tier", str(fragment["tier_id"]) != tier_id),
                ("manifest", str(fragment["manifest_sha256"])
                 != manifest_sha256),
                ("stage_root", os.path.normpath(str(fragment["stage_root"]))
                 != os.path.normpath(str(stage_root))),
                # The SSD stage never dates its fragments: an epoch on a
                # non-ram tier is convention-conflicting state, the same
                # refusal a sidecar epoch draws below.
                ("epoch", bool(fragment.get("epoch"))
                 and not tier_id.startswith(storage_tiers.RAM_TIER_PREFIX)),
            ) if bad]
        if conflicts:
            raise SystemExit(
                f"residency_prior_fragment_conflicting: this mover's own "
                f"fragment disagrees with the invocation on "
                f"{', '.join(conflicts)}; refusing keeps the record rather "
                f"than republishing around it")

        material = reader_lease.read_material(
            residency_root, consumer_action_key, mover_action_key)
        mentions: Mapping[str, Mapping[str, object]] = {}
        generation: str | None = None
        if isinstance(material, Mapping):
            conflicts = [
                name for name, bad in (
                    ("consumer", str(material["consumer_action_key"])
                     != consumer_action_key),
                    ("mover", str(material["mover_action_key"])
                     != mover_action_key),
                    ("tier", str(material["tier_id"]) != tier_id),
                    ("manifest", str(material["manifest_sha256"])
                     != manifest_sha256),
                    ("stage_root",
                     os.path.normpath(str(material["stage_root"]))
                     != os.path.normpath(str(stage_root))),
                    ("epoch", bool(material.get("epoch"))
                     and not tier_id.startswith(
                         storage_tiers.RAM_TIER_PREFIX)),
                ) if bad]
            if conflicts:
                raise SystemExit(
                    f"residency_prior_material_conflicting: this mover's "
                    f"own material sidecar disagrees with the invocation "
                    f"on {', '.join(conflicts)}; carrying its dates would "
                    f"republish contradictory ownership under corrected "
                    f"headers")
            mentions = material["entries"]
            generation = str(material["generation"])
        # An absent or unparseable sidecar is the documented crash window:
        # the vouches stand, no date is invented, the rerun overwrites
        # both as it always has.

        for key, record in dict(fragment["entries"]).items():
            fact = facts.get(str(key))
            if fact is None:
                raise SystemExit(
                    f"residency_prior_fragment_conflicting: this mover's "
                    f"own fragment names {key} outside the window this "
                    f"invocation derives; a same-key fragment is the same "
                    f"command, so this is conflicting ownership, not scope "
                    f"to prune")
            want, destination, declared, offset = fact
            assert isinstance(record, Mapping)
            stage_path = os.path.normpath(str(record["stage_path"]))
            if stage_path != destination or int(record["bytes"]) != want:
                raise SystemExit(
                    f"residency_prior_fragment_conflicting: this mover's "
                    f"own fragment records {key} at a different extent "
                    f"than the manifest derives; refusing keeps the record "
                    f"rather than pruning it into smaller coverage")
            digest = str(record["sha256"])
            # The vouch survives: the name stays published, and the gate
            # below refuses a changed incarnation instead of letting a
            # dropped vouch turn refusal into replacement.
            staged[str(key)] = {
                "stage_path": stage_path, "bytes": want,
                "offset": offset, "sha256": digest,
            }
            mention = mentions.get(str(key))
            if not isinstance(mention, Mapping):
                continue          # vouch without a date stays undated
            try:
                dated = (os.path.normpath(str(mention["stage_path"]))
                         == destination and int(mention["bytes"]) == want)
            except (TypeError, ValueError):
                dated = False
            if not dated or str(mention.get("sha256") or "") != digest:
                continue          # the date disagrees with the vouch
            if declared and digest != declared:
                continue          # not the manifest's bytes: never re-dated
            live = reader_lease.stat_identity(stage_path)
            if live is None or not reader_lease.file_id_matches(
                    mention.get("file_id"), live):
                continue          # changed, replaced or unreadable
            file_id = mention.get("file_id")
            assert isinstance(file_id, Mapping)
            sidecar[str(key)] = {
                "stage_path": stage_path, "bytes": want,
                "sha256": digest, "file_id": dict(file_id),
            }
    if not staged:
        return {}, {}, None
    return staged, sidecar, generation


def _produced_stage_namespace(args) -> str | None:
    """An explicit sealed output opt-in, bound to the existing batch reference.

    Old requests and ordinary inputs keep their original path derivation. A
    new output mover cannot select another batch's directory, an unchecked
    path component, or a caller-supplied manifest through this opt-in.
    """

    namespace = getattr(args, "produced_output_namespace", None)
    if namespace is None:
        return None
    if args.manifest:
        raise SystemExit("produced_stage_namespace: requires the sealed manifest")
    try:
        ref, present = pool._sealed_produced_output_batch(
            args.cas_root, str(args.action_key))
        if not present or ref is None:
            raise pool.PoolContractError("sealed output reference absent")
        kind = storage_tiers.capacity_kind_of(str(args.tier_id))
        demand = {f"{kind}@{args.tier_id}": storage_tiers.stage_tokens_for_bytes(
            int(args.range_end_bytes) - int(args.range_start_bytes))}
        checked = pool.PoolQueue(Path(args.pool_root)).validate_produced_output_batch(
            ref, demand, residency_block={
                "tier_id": str(args.tier_id),
                "manifest_sha256": str(args.manifest_sha256),
                "range_start_bytes": int(args.range_start_bytes),
                "range_end_bytes": int(args.range_end_bytes)})
        if (namespace != checked["batch_namespace"]
                or str(args.consumer_action_key) != namespace):
            raise pool.PoolContractError("sealed batch namespace mismatch")
    except (OSError, ValueError, pool.PoolContractError,
            pb.ActionContractError, pb.CASTamperError, pb.CASUnavailableError) as exc:
        raise SystemExit(f"produced_stage_namespace: {exc}") from None
    return str(namespace)


def move(args, *, stop: threading.Event | None = None) -> dict[str, object]:
    """Stage the declared range and return the receipt, refusing an overrun."""

    stop = stop or threading.Event()
    namespace = _produced_stage_namespace(args)
    cas_root = Path(args.cas_root)
    manifest = load_manifest(cas_root, args.action_key, args.manifest)
    mount_prefix = str(manifest["mount_prefix"])
    entries = prewarm_loop.manifest_read_entries(manifest)
    window = prewarm_loop.entries_between(
        entries, int(args.range_start_bytes), int(args.range_end_bytes))
    declared = int(args.range_end_bytes) - int(args.range_start_bytes)
    if declared <= 0:
        raise SystemExit("--range-end-bytes must be above --range-start-bytes")

    window_bytes = sum(int(entry["bytes"]) for entry in window)
    if window_bytes > declared:
        # Refuse before reading 34 GB rather than after.  ``entries_between``
        # includes an entry straddling the start whole, so a phase table whose
        # boundary cuts an entry would hand this mover more bytes than its
        # tokens reserved -- which is the overrun, found for free.
        raise SystemExit(
            f"residency_overran_reservation: the entries covering "
            f"[{args.range_start_bytes}, {args.range_end_bytes}) are "
            f"{window_bytes} bytes, past the {declared} this range reserved")

    # Two entries that derive one staged name would let the later copy
    # overwrite the earlier while both fragments vouch for it, and compose
    # cannot see it: the two map keys differ, so the same-key conflict never
    # fires.  Staged-input naming (:func:`stage_relative`) is injective, so an
    # input manifest can no longer reach this; a produced output's declared
    # name still can (a declared path that spells another entry's range
    # name), and this refuses it.
    named_once = frozenset(paths_named_once(entries))
    destinations: dict[str, str] = {}
    for entry in window:
        path, offset = str(entry["path"]), int(entry["offset"])
        relative = stage_relative(path, offset, int(entry["bytes"]),
                                  mount_prefix=mount_prefix,
                                  namespace=namespace,
                                  named_once=path in named_once)
        claimed = destinations.get(relative)
        if claimed is not None:
            raise SystemExit(
                f"residency_destination_collision: {claimed!r} and {path!r} "
                f"both stage as {relative!r}; nothing was copied")
        destinations[relative] = path

    pacer = prewarm_loop.pacer_from_args(args)
    if not getattr(args, "unpaced", False):
        prewarm_loop.require_storage_pacing(pacer)
    mounts = prewarm_loop.MountMap(args.mount or [])
    queue = pool.PoolQueue(Path(args.pool_root))
    residency_root = Path(args.residency_root)
    publisher = _StagedPublisher(
        queue=queue, stage_root=Path(args.stage_root),
        residency_root=residency_root,
        mover_action_key=str(args.action_key),
        manifest_sha256=str(args.manifest_sha256),
        tier_id=str(args.tier_id), cas_root=str(args.cas_root),
        consumer_action_key=str(args.consumer_action_key))
    copier = _Copier(
        mounts=mounts, pacer=pacer, stage_root=Path(args.stage_root),
        mount_prefix=mount_prefix, block=args.block, workers=args.max_readers,
        owner=str(args.action_key), publisher=publisher, namespace=namespace)

    manifest_sha256 = args.manifest_sha256

    # A same-key retry inherits its own qualified coverage before it
    # publishes anything: without this, the first incremental publication
    # replaces the fragment and material with the tiny fresh subset and
    # every not-yet-reencountered entry loses proof, is recopied, and pays
    # the publication grace per entry.  Seeded entries are coverage the
    # previous attempt published and this invocation *preserves* -- a kept
    # vouch is not a requalified date and not committed progress; each
    # path is still decided by the ordinary gate as the copier reaches
    # it.  Seeds add no staged bytes and cannot complete a receipt on
    # their own.
    lock_timings: dict[str, float] = {}
    staged_seeds, sidecar_seeds, resumed_generation = _resume_own_coverage(
        queue, consumer_action_key=str(args.consumer_action_key),
        mover_action_key=str(args.action_key), tier_id=str(args.tier_id),
        stage_root=Path(args.stage_root),
        manifest_sha256=str(manifest_sha256),
        residency_root=residency_root, window=window,
        mount_prefix=mount_prefix, namespace=namespace,
        named_once=named_once, timings=lock_timings)
    if staged_seeds:
        copier.staged.update(staged_seeds)
        copier.sidecar.update(sidecar_seeds)
    entries_resumed = len(staged_seeds)
    bytes_resumed = sum(int(record["bytes"])
                        for record in staged_seeds.values())

    last_published = [0.0]
    last_generation = [0]
    publish_lock = threading.Lock()
    # One publish run, one materialization generation -- unless the run
    # resumed one: unchanged bytes keep their prior generation (stable
    # reuse, the ``reader_lease.adopted_generation`` rule) and a fresh one
    # is minted only when the run first replaces actual bytes, which the
    # publisher notes at the gate.
    publisher.begin_material(resumed_generation)

    def publish(staged: dict[str, dict[str, object]],
                identities: dict[str, dict[str, object]] | None = None,
                generation: int = 0,
                *, force: bool = False) -> None:
        """Republish the fragment as entries land, so a crash leaves a prefix.

        Rate-limited, and that limit is the difference between a mover that
        works and one that does not.  The fragment is a single document that
        grows with every entry, it lives on the shared mount, and each publish
        is a write plus an ``fsync`` plus a rename over NFS.  Writing it once
        per entry is quadratic in bytes and pays an NFS round trip per file:
        measured on dl380g10, a range of 9,372 entries ran at **75 KB/s** with
        the pool idle at 2% utilization, while a range of 61 entries of the
        same total size ran at **466 MB/s**.  The cost was never the copy.  A
        crash still leaves a valid prefix; it is at most a couple of seconds
        shorter than the one a per-entry publish would have left, and the
        entries it omits are re-staged by the rerun.

        The fragment goes first, then the material sidecar that dates it: a
        crash between them leaves a vouch without a date, which strict
        readers refuse (safe) and legacy readers use (today's behavior),
        and the rerun overwrites both.
        """

        with publish_lock:
            now = time.monotonic()
            if generation and generation <= last_generation[0]:
                # A snapshot older than the one already on disk.  Writing it
                # would take entries back out of a published fragment, which
                # is the one thing a fragment may never do.
                return
            if not force and now - last_published[0] < FRAGMENT_PUBLISH_S:
                return
            last_published[0] = now
            last_generation[0] = max(last_generation[0], generation)
            residency_map.write_fragment(residency_root, {
                "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
                "consumer_action_key": args.consumer_action_key,
                "mover_action_key": args.action_key,
                "tier_id": args.tier_id,
                "stage_root": str(args.stage_root),
                "manifest_sha256": manifest_sha256,
                "entries": staged,
            })
            if identities:
                reader_lease.write_material(
                    residency_root,
                    consumer_action_key=args.consumer_action_key,
                    mover_action_key=args.action_key,
                    tier_id=args.tier_id, stage_root=str(args.stage_root),
                    manifest_sha256=manifest_sha256,
                    generation=publisher.material_generation(),
                    entries=identities)

    served = served_for(args)
    if pacer is not None:
        # Whose reads are this move's own, and whose are a client to protect.
        # The self is the *consumer's* box, not this one: the point of the
        # split is not to protect a consumer from its own staging.  Naming
        # nobody is the prewarm loop's documented "no self" case, where every
        # client counts and the row is attributed to none of them -- which is
        # also why such a receipt reports a pool-side rate of zero and mints
        # no fill tokens.
        pacer.begin_row(served_host=str(served["served_host"]),
                        served_addresses=tuple(served["served_addresses"]),
                        served_reason=str(served["served_reason"]))
    # How many movers were reading this tier when the copy began, this one
    # included.  Without it ``mean_pool_read_mb_s`` cannot price a single
    # mover: it is what the whole pool delivered, so a window shared by three
    # copies reports three copies' worth, and a next submission that read it as
    # one mover's rate would reserve the entire pool for each of them -- which
    # re-serializes movers through the fill token, the same failure this whole
    # change is about, arriving by another resource kind.
    try:
        concurrent = len(pool.PoolQueue(Path(args.pool_root)).movers_claimed_on_tier(
            str(args.tier_id))) or 1
    except (OSError, pool.PoolContractError):
        concurrent = 0          # unknown, and unknown prices nothing
    before = proc_io()
    cpu_before = cpu_seconds()
    started = time.time()
    # The start gate, same as the promotion's: order this copy's first rename
    # against an egress that may be snapshotting right now.  Acquired and
    # released -- nothing is held during the copy itself.  Timed, because a
    # wait here is an egress's hold paid by this copy (#988).
    gate_started = time.perf_counter()
    pool.PoolQueue(Path(args.pool_root)).ownership_start_gate(args.stage_root)
    start_gate_wait_s = time.perf_counter() - gate_started
    # Reports landed bytes while the copy and the read-back run (#1010).  A
    # thread of its own, started only when this launch has a progress
    # channel; it is a daemon, so a copy that raises ends it with the process.
    reporter = _ProgressReporter(
        copier, interval_s=float(getattr(args, "progress_interval_s", None)
                                 or pool.HEARTBEAT_S),
        range_start_bytes=int(args.range_start_bytes),
        range_end_bytes=int(args.range_end_bytes))
    reporter.start()
    copier.run(window, stop=stop,
               on_entry=None if args.no_incremental_fragment else publish,
               named_once=named_once)
    elapsed = max(1e-9, time.time() - started)
    after = proc_io()
    cpu_used = max(0.0, cpu_seconds() - cpu_before)
    pacing = (pacer.report() if pacer is not None
              else prewarm_loop.inactive_pacing("no pacer"))

    overran = copier.bytes_staged > declared
    receipt: dict[str, object] = {
        "schema": pool.POOL_MOVE_SCHEMA_V1,
        "action_key": args.action_key,
        "consumer_action_key": args.consumer_action_key,
        "tier_id": args.tier_id,
        "stage_root": str(args.stage_root),
        "manifest_sha256": manifest_sha256,
        "range_start_bytes": int(args.range_start_bytes),
        "range_end_bytes": int(args.range_end_bytes),
        "range_bytes": declared,
        "bytes_staged": copier.bytes_staged,
        "entries_declared": len(window),
        "entries_staged": len(copier.staged),
        # Preserved coverage, not copied bytes and not verified progress:
        # a kept vouch has not necessarily been re-dated, and only a
        # landed entry counts toward staged bytes or completeness.
        "entries_resumed": entries_resumed,
        "bytes_resumed": bytes_resumed,
        "complete": copier.bytes_staged == declared and not copier.errors,
        "seconds": round(elapsed, 3),
        # File-side, and named so: what the copy saw, which the ARC can answer
        # without the disks moving.  ``disk_pacing.mean_pool_read_mb_s`` beside
        # it is the pool-side number, summed off the members' own sector
        # counters, and the only one a tier mints from.
        "mb_per_s_file_side": round(copier.bytes_staged / 1e6 / elapsed, 1),
        "disk_pacing": pacing,
        "served": {k: list(v) if isinstance(v, tuple) else v
                   for k, v in served.items()},
        "proc_io": _delta(before, after),
        # What this mover actually cost in memory, so the next
        # submission's ``mem_gb`` is a measurement rather than a habit
        # (``pb_demand_must_be_measured_not_habitual``).  The copy is a
        # bounded window -- ``--readers`` buffers of ``--block`` -- so
        # this should stay flat as the range grows, and a receipt that
        # says otherwise is the bug report.
        "peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
        # The other half of the same story, and the one #603 is about: a
        # mover's sha256 is CPU-bound, it declared no ``cpu`` at all, and
        # ``adaptive_cpu`` reads an absent ``cpu`` as unknown CPU use and
        # serializes it on an otherwise idle host
        # (``adaptive_cpu.py`` ``unbounded_cpu_not_exclusive``).  Differenced
        # across the copy, so the manifest read and the fragment publishes
        # outside it are not charged to the rate.  ``cpu_seconds / seconds``
        # is mean parallelism -- what this copy actually kept busy -- which is
        # the number a next submission can declare and the controller can
        # learn down from.
        "cpu_seconds": round(cpu_used, 3),
        # Where the copy's time went, phase by phase, summed across the copy
        # workers (thread-seconds, not wall), with call counts, the longest
        # single call, and how each entry ended (#981).  Totals alone could
        # not split chunk 0's 270 s tail; this can.
        "phase_timings": copier.clock.report(),
        # How long this mover queued for the stage ownership lock before
        # its copy began (#988): at the start gate, and before it at the
        # read of its own prior coverage.  Either is an egress's hold, paid
        # by a mover; whichever comes first absorbs it.
        "start_gate_wait_s": round(start_gate_wait_s, 6),
        "resume_lock_wait_s": round(
            lock_timings.get("resume_lock_wait_s", 0.0), 6),
        # 0 means "could not be read", which prices nothing, rather than 1.
        storage_tiers.MOVER_CONCURRENCY_FIELD: int(concurrent),
        # What the ledger promised this copy of the pool, beside what the pool
        # actually delivered (``disk_pacing.mean_pool_read_mb_s``) and what
        # this copy achieved.  The three together are what says whether the
        # supply can grow or has found its ceiling.
        storage_tiers.MOVER_FILL_DEMAND_FIELD: int(
            getattr(args, "fill_mb_s_pool_side", 0) or 0),
        "host": socket.gethostname(),
        "errors": copier.errors,
        "unix": time.time(),
    }
    # Which pools this copy read off and wrote onto, for the receipt folds
    # that price the next mover (#611).  Read off the live tier record rather
    # than sealed into the argv, for the same reason ``movers_claimed_on_tier``
    # is read there: the pool a mover measured is a fact about the tier now,
    # and a mover sealed before a rebuild must not carry the old pool's name.
    identity = announced_pool_identity(args.pool_root, str(args.tier_id))
    if identity is not None:
        receipt["pool_identity"] = identity
    if publisher.invalidated:
        # Names this copy took from owners that had all ended (#966), each
        # with the owners it proved; capped like ``errors``.
        receipt["entries_invalidated"] = len(publisher.invalidated)
        receipt["invalidated"] = publisher.invalidated[:20]
    if copier.refusal is not None:
        # A live owner holds different bytes under one of this range's names
        # (#966).  No rerun can clear that, so the mover says so and exits
        # nonzero, and it retires its own window: otherwise the window finds
        # it neither queued nor pinned and republishes it into the same
        # refusal every cycle.  Its fragment stays, so what it adopted keeps
        # its vouch.
        receipt["refusal"] = copier.refusal
        receipt["complete"] = False
        receipt["conflict"] = copier.conflict
        receipt["plan_superseded"] = retire_conflicted_window(
            queue, str(args.consumer_action_key), str(args.action_key),
            copier.conflict or {})
    if overran:
        # The tokens bound what the tier can hold.  Staging past them is the
        # one failure that cannot be left to the consumer to notice, so the
        # fragment goes and the receipt says why.
        receipt["refusal"] = "residency_overran_reservation"
        receipt["complete"] = False
        residency_map.fragment_path(
            residency_root, args.consumer_action_key,
            args.action_key).unlink(missing_ok=True)
        reader_lease.material_path(
            residency_root, args.consumer_action_key,
            args.action_key).unlink(missing_ok=True)
    elif copier.staged:
        publish(copier.staged, copier.sidecar, force=True)
    # Layer 2, after every measurement of layer 1 has been taken.  The warm
    # reads the stage, not the pool, so folding it into ``seconds`` or the
    # pacer's window would price a copy by a read the pool never served --
    # and ``fill_supply_from_records`` would then read a healthy mover as one
    # that fell short of the bandwidth it reserved (#636).  Its own block,
    # its own rate.
    warm = arc_warm_verdict(args)
    outcome = {"bytes": 0, "entries": 0, "seconds": 0.0, "mb_per_s": 0.0,
               "errors": []}
    if overran or not copier.staged:
        warm = {**warm, "warm": False, "state": "skipped",
                "reason": "nothing was published to warm"}
    elif warm["warm"]:
        warm_state: dict[str, int] = {}
        reporter.enter_warm(warm_state)
        outcome = warm_staged(
            [str(record["stage_path"]) for record in copier.staged.values()],
            workers=args.max_readers, block=args.block, stop=stop,
            state=warm_state)
    receipt["arc_warm"] = {
        "state": str(warm["state"]),
        "reason": str(warm["reason"]),
        "primarycache": warm["primarycache"],
        "tier_id": str(args.tier_id),
        **outcome,
    }
    if not copier.staged and not overran:
        # The copy's own evidence cannot say WHY it staged nothing, so the
        # bounded diagnosis runs here, on the empty path only: one stat per
        # distinct origin directory.  A mover that staged nothing because
        # the origin directories are not on this host is a configuration
        # refusal, not an ordinary empty move, and naming it lets the owner
        # read a reachability failure instead of the same symptom a mover
        # defect produces (#804).  Unknown I/O is never typed here; it keeps
        # the ordinary empty-move refusal.
        diagnosis = origin_reachability_diagnosis(window, mounts)
        receipt["origin_reachability"] = diagnosis
        empty = ("origin_unreachable"
                 if diagnosis["state"] == "unreachable"
                 else "residency_moved_nothing")
        receipt["refusal"] = receipt.get("refusal") or empty
    reporter.stop()
    # What this copy told the worker's stall check (#1010), so a receipt
    # says whether the copy was bounded by its progress or by nothing.
    receipt["progress_report"] = reporter.record()
    return receipt


def retire_conflicted_window(queue: pool.PoolQueue, consumer: str,
                             mover: str,
                             conflict: Mapping[str, object]) -> bool:
    """Mark the window that sealed ``mover`` superseded, naming the conflict.

    The existing retirement record (#708): ``residency_window`` stops
    publishing movers from a superseded plan, and a deliberate resubmission
    seals a fresh one.  Bound to the filing that names this mover, under the
    consumer's transition lock (:func:`residency_plan.mark_superseded`), so a
    mover of a replaced plan never retires its replacement.  ``False`` when
    nothing was marked: no plan, a plan that does not name this mover, or a
    record that could not be read or written.
    """

    try:
        refused: list[Exception] = []
        plan, filing = residency_plan.read_filed(
            queue, consumer, on_unreadable=refused.append)
        if (plan is None or refused
                or residency_plan.find_mover_leg(plan, mover) is None):
            return False
        owners = "; ".join(
            f"consumer {row.get('consumer_action_key')} mover "
            f"{row.get('mover_action_key')} ({row.get('state')})"
            for row in conflict.get("owners") or ()
            if isinstance(row, Mapping))
        reason = (f"{STAGED_DESTINATION_CONFLICT}: "
                  f"{conflict.get('stage_path')} holds the bytes of "
                  f"{owners}; consumer {consumer} mover {mover} wants "
                  f"{conflict.get('declared_sha256')}")
        marker = residency_plan.mark_superseded(
            queue, consumer, plan=plan, filing=filing, reason=reason,
            movers=[mover], by="stage-move")
    except (OSError, ValueError, pb.PrismaBuildError):
        return False
    return marker is not None


def own_action_key(declared: str | None) -> str:
    """This node's own action key, from the flag or from the launcher.

    A movement node files its receipt and holds its tier tokens under its own
    key, and that key is ``canonical_sha256`` of the action body --- so a
    ``--action-key`` sealed into the argv would be hashed into the very value
    it states, and no fixed point exists.  The launcher sets
    :data:`prismabuild.core.ACTION_KEY_ENV` for every action it starts, from
    the action in hand, which is the one place the answer is already known.
    The flag stays, because a direct run and every test needs to say which key
    it is acting as.
    """

    key = declared or os.environ.get(pb.ACTION_KEY_ENV) or ""
    if len(key) != 64 or any(character not in "0123456789abcdef" for character in key):
        raise SystemExit(
            f"pass --action-key, or run under a launcher that sets "
            f"{pb.ACTION_KEY_ENV}; got {key!r}")
    return key


def build_parser() -> argparse.ArgumentParser:
    """Every flag this tool takes, in one place a test can also ask.

    Extracted from ``main`` because the alternative was a test fixture that
    re-listed the defaults by hand: when the pacer's ``--served-host`` was
    added here and not there, every test that drives ``move`` started failing
    on an attribute the real tool always supplies.  A fixture built from this
    parser cannot drift from it.
    """

    parser = argparse.ArgumentParser(
        description="copy one byte range of a data manifest's read order onto a stage tier")
    parser.add_argument("--pool-root", required=True,
                        help="the pull queue root this mover files its receipt and "
                             "its residency-map fragment under")
    parser.add_argument("--cas-root", default="",
                        help="the content store this mover reads its own sealed "
                             "request and data manifest from; not needed when "
                             "--manifest names the file directly")
    parser.add_argument("--action-key", default=None,
                        help="this mover's own action key; its receipt and its "
                             "residency-map fragment are filed under it. "
                             f"Defaults to {pb.ACTION_KEY_ENV}, which the "
                             "launcher sets, because a key cannot be sealed "
                             "into the argv it is computed from")
    parser.add_argument("--consumer-action-key", required=True,
                        help="the action these bytes are staged for")
    parser.add_argument("--produced-output-namespace", default=None,
                        help="sealed produced batch namespace for physical path isolation; "
                             "must match the request's produced-output reference")
    parser.add_argument("--tier-id", required=True,
                        help="the storage tier these bytes are staged on, as "
                             "tier_loop announces it")
    parser.add_argument("--stage-root", required=True,
                        help="the staging dataset's mountpoint; every staged file "
                             "lives under it and the map refuses a path that does not")
    parser.add_argument("--manifest-sha256", required=True,
                        help="the manifest these bytes belong to; the consumer's "
                             "residency gate refuses a lead that staged another")
    parser.add_argument("--range-start-bytes", type=int, required=True,
                        help="first byte of the manifest read order this mover "
                             "stages (inclusive)")
    parser.add_argument("--range-end-bytes", type=int, required=True,
                        help="one past the last byte of the range this mover "
                             "stages (exclusive)")
    parser.add_argument("--manifest", default=None,
                        help="read the manifest from this file instead of the "
                             "sealed request (testing and one-off moves)")
    parser.add_argument("--residency-root", default=None,
                        help="where residency-map fragments are filed "
                             "(default <pool-root>/residency)")
    parser.add_argument("--mount", action="append", default=[],
                        help="local=declared, as the prewarm loop takes it")
    parser.add_argument("--block", type=int, default=prewarm_loop.BLOCK,
                        help="bytes per read; the buffer each worker holds")
    parser.add_argument("--fill-mb-s-pool-side", type=int, default=0,
                        help="the pool bandwidth this mover's claim reserved, "
                             "recorded in the receipt so a later cycle can ask "
                             "whether the pool delivered what the ledger "
                             "promised.  0 means the claim reserved none")
    parser.add_argument("--readers", type=int, default=4,
                        help="copy depth while another client is reading the pool")
    parser.add_argument("--max-readers", type=int,
                        default=movement_actions.MOVER_MAX_READERS,
                        help="copy depth while the pool serves nobody but this move")
    parser.add_argument("--warm-after-copy", choices=("auto", "always", "never"),
                        default="auto",
                        help="read the staged range back on this box once it is "
                             "copied and verified, so the file server's ARC holds "
                             "it for the consumer (#638).  'auto' asks the tier's "
                             "own record whether its dataset may cache file data "
                             "at all; 'never' spends no reads on it")
    parser.add_argument("--progress-interval-s", type=float,
                        default=pool.HEARTBEAT_S,
                        help="seconds between reports of landed bytes to the "
                             "worker's stall check, when this mover was sealed "
                             "with a progress policy (#1010).  The worker "
                             "samples on its heartbeat, so a shorter interval "
                             "buys nothing there")
    parser.add_argument("--no-incremental-fragment", action="store_true",
                        help="write the fragment once at the end rather than as "
                             "entries land; the end state is the same")
    parser.add_argument("--receipt", default=None,
                        help="also write the receipt here (it is always filed "
                             "in the queue's movers directory)")
    # The pacer's own arguments, spelled here rather than imported: the same
    # names and defaults as the prewarm loop's, because both read the same
    # pool and a mover that paced differently would not be measuring the same
    # thing.  ``prewarm_loop.py`` is owned by #589 and is not edited to share
    # them; when that lands, one definition replaces these.
    parser.add_argument("--pace-pool", default="storage_pool",
                        help="the ZFS pool whose members the pacer watches")
    parser.add_argument("--disks", default="",
                        help="name the pacer's members directly when zpool "
                             "discovery cannot see them")
    parser.add_argument("--client-active-mb-s", type=float,
                        default=prewarm_loop.CLIENT_ACTIVE_MB_S,
                        help="a client reading at least this fast counts as "
                             "active, and the copy yields to it")
    parser.add_argument("--export-stats", default=prewarm_loop.EXPORT_STATS,
                        help="per-export NFS statistics the pacer attributes "
                             "client reads with")
    parser.add_argument("--nfsd-io", default=prewarm_loop.NFSD_IO,
                        help="server-side NFS byte counters the pacer samples")
    parser.add_argument("--max-util-pct", type=float, default=25.0,
                        help="hold the copy above this member utilization")
    parser.add_argument("--max-read-await-ms", type=float, default=10.0,
                        help="hold the copy above this member read latency")
    parser.add_argument("--max-backlog-ms", type=float, default=2000.0,
                        help="hold the copy above this member queue backlog")
    parser.add_argument("--pace-sample-s", type=float, default=0.25,
                        help="seconds between the pacer's device samples")
    parser.add_argument("--pace-hold-s", type=float, default=0.25,
                        help="seconds a held copy waits before asking again")
    parser.add_argument("--served-host", default="",
                        help="the box whose reads are this move's own; derived "
                             "from the consumer's claim when not given")
    parser.add_argument("--served-address", action="append", default=[],
                        help="a client address of the served box; derived from "
                             "its worker offer when not given")
    parser.add_argument("--unpaced", action="store_true",
                        help="run with no pacer at all; for a fixture or a box "
                             "that serves nothing, never for the storage role")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    args.action_key = own_action_key(args.action_key)
    if args.residency_root is None:
        args.residency_root = str(Path(args.pool_root) / pool.RESIDENCY)

    receipt = move(args)
    queue = pool.PoolQueue(Path(args.pool_root))
    queue.record_move(args.action_key, receipt)
    if args.receipt:
        # The atomic writer every other fleet record uses: a reader of this
        # path must never be able to observe half a document.
        pool._write_json_atomic(Path(args.receipt), receipt)
    print(json.dumps(receipt, indent=1, sort_keys=True, default=str))
    return 1 if receipt.get("refusal") else 0


if __name__ == "__main__":
    raise SystemExit(main())
