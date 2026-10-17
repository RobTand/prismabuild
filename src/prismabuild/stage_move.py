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
from contextlib import ExitStack, contextmanager, suppress
import errno
from collections.abc import Callable, Mapping, Sequence
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


from prismabuild import prewarm_loop  # noqa: E402
from prismabuild import immutable_files  # noqa: E402
from prismabuild import kept_reads, manifest_layout
from prismabuild import core as pb  # noqa: E402
from . import data_manifest as manifest_codec
from prismabuild import movement_actions  # noqa: E402
from prismabuild import pool  # noqa: E402
from prismabuild import progress as pb_progress  # noqa: E402
from prismabuild import reader_lease  # noqa: E402
from prismabuild import residency_plan  # noqa: E402
from prismabuild import residency_map  # noqa: E402
from prismabuild import storage_tiers  # noqa: E402
from prismabuild.stage_marks import (  # noqa: E402
    STAGE_MOVER_XATTR, STAGE_SOURCE_XATTR, source_identity,
)
from prismabuild import staged_path

#: Seconds between republications of the residency fragment while a move runs.
#: See :func:`move`; a per-entry publish costs an NFS round trip per file and
#: was measured at 75 KB/s against 466 MB/s.
FRAGMENT_PUBLISH_S = 5.0


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


class _ReaderPlan:
    """This copy's place in its tier's reader plan (#1091), as the tier loop announces it.

    The tier record's ``reader_plan`` (``tier_loop.reader_plan``) lists the
    copies claimed consumers are blocked on.  A copy on that list is
    *exempt*: the disk pacer never holds it, and it reads at its deepest
    depth.  Every other copy on the tier *yields* while a listed copy is
    ``claimed``: it stops reading until the waited copies land, because a
    later-phase copy on the same four spindles is what made the waited one
    late (2026-09-24: 30.8 MB/s of a 250.6 MB/s pool, 7 copies claimed).

    A listed copy that is still ``ready`` reads nothing, so nothing yields
    to it (#1091 review 1).  It may be unclaimable for want of the very
    stage GiB the yielding copies hold, and yielding to it would then be a
    deadlock.  The claim pass still defers other rows for it, which holds
    nothing.  The worker credits the same stand-aside in its ``no_progress``
    rung, from the same record read on its own side
    (``pool.PoolQueue.reader_plan_stand_aside``).

    "Never held" covers the pacer's blind hold too.  That hold stands in for
    the client verdict while the pool's telemetry is missing, and an exempt
    copy reads whatever the client verdict says; holding it on a missing
    verdict would only delay the copy a consumer is already blocked on.

    The record is re-read only when its file changed, and asked about at
    most every ``hold_s``.  A record older than ``pool.OFFER_TIMEOUT_S`` by
    its ``announced_unix``, or none at all, is no plan: nothing is exempt and
    nothing yields, which is the behavior before #1091, so a dead tier loop
    cannot hold a copy on a wait that has ended.
    """

    def __init__(self, queue: pool.PoolQueue, tier_id: str, mover: str, *,
                 hold_s: float = 0.25,
                 clock: Callable[[], float] = time.monotonic,
                 wall: Callable[[], float] = time.time,
                 sleep: Callable[[float], None] = time.sleep) -> None:
        self.queue = queue
        self.tier_id = str(tier_id)
        self.mover = str(mover)
        self.hold_s = max(0.001, float(hold_s))
        self.clock, self.wall, self.sleep = clock, wall, sleep
        self._lock = threading.Lock()
        self._stamp: tuple[int, int, int] | None = None
        self._declared: frozenset[str] = frozenset()
        #: The listed copies the plan says are ``claimed``, so reading.
        self._reading: frozenset[str] = frozenset()
        self._announced: float | None = None
        self._asked_at: float | None = None
        self._holding = 0
        self._hold_started = 0.0
        self.yielded_s = 0.0
        self.yield_count = 0
        self.exempt_seen = False
        self.declared_seen: list[str] = []

    def _refresh_locked(self) -> None:
        now = self.clock()
        if self._asked_at is not None and now - self._asked_at < self.hold_s:
            return
        self._asked_at = now
        try:
            path = self.queue.records.tier_record_path(self.tier_id)
            status = os.stat(path)
        except (OSError, pool.PoolContractError, ValueError):
            self._stamp, self._announced = None, None
            self._declared = self._reading = frozenset()
            return
        stamp = (status.st_ino, status.st_mtime_ns, status.st_size)
        if stamp == self._stamp:
            return
        self._stamp = stamp
        try:
            record = self.queue.records.text_document_at(path)
        except (OSError, ValueError):
            self._declared = self._reading = frozenset()
            self._announced = None
            return
        plan = record.get(pool.READER_PLAN_FIELD) if isinstance(record, dict) else None
        announced = record.get("announced_unix") if isinstance(record, dict) else None
        if (not isinstance(plan, dict) or isinstance(announced, bool)
                or not isinstance(announced, (int, float))):
            self._declared = self._reading = frozenset()
            self._announced = None
            return
        self._announced = float(announced)
        rows = [row for row in plan.get("declared_wait") or ()
                if isinstance(row, dict)]
        self._declared = frozenset(
            str(row.get("mover_action_key") or "") for row in rows)
        self._reading = frozenset(
            str(row.get("mover_action_key") or "") for row in rows
            if row.get("state") == pool.record_store.CLAIMED)
        for key in sorted(self._declared):
            if key not in self.declared_seen and len(self.declared_seen) < 20:
                self.declared_seen.append(key)

    def _plan_now(self) -> tuple[frozenset[str], frozenset[str]]:
        """``(listed, reading)`` from a fresh plan, else two empty sets."""

        with self._lock:
            self._refresh_locked()
            if (self._announced is None
                    or self.wall() - self._announced > pool.OFFER_TIMEOUT_S):
                return frozenset(), frozenset()
            return self._declared, self._reading

    def exempt(self) -> bool:
        """Whether a claimed consumer is blocked on this copy right now."""

        exempt = self.mover in self._plan_now()[0]
        if exempt:
            self.exempt_seen = True
        return exempt

    def yields(self) -> bool:
        """Whether another waited copy on this tier is reading and this one is not listed."""

        listed, reading = self._plan_now()
        return self.mover not in listed and bool(reading - {self.mover})

    def stand_aside(self, stop: threading.Event) -> None:
        """Stop reading while :meth:`yields`, or until ``stop``.

        Wall seconds any worker stood aside, not the sum over workers, as the
        pacer counts ``held_seconds``: they go on the receipt as
        ``disk_pacing.yielded_seconds``, which ``storage_tiers._fell_short``
        takes out of the copy's reading time.
        """

        if not self.yields():
            return
        with self._lock:
            if self._holding == 0:
                self._hold_started = self.clock()
                self.yield_count += 1
            self._holding += 1
        try:
            while self.yields() and not stop.is_set():
                self.sleep(self.hold_s)
        finally:
            with self._lock:
                self._holding -= 1
                if self._holding == 0:
                    self.yielded_s += self.clock() - self._hold_started

    def report(self) -> dict[str, object]:
        with self._lock:
            yielded = self.yielded_s
            if self._holding:
                yielded += self.clock() - self._hold_started
            return {"exempt": self.exempt_seen,
                    "yielded_seconds": round(yielded, 3),
                    "yields": self.yield_count,
                    "declared_wait_seen": list(self.declared_seen)}


class _Copier:
    """One range, copied in read order by a bounded set of workers."""

    def __init__(self, *, mounts: prewarm_loop.MountMap, pacer, stage_root: Path,
                 mount_prefix: str, block: int, workers: int,
                 owner: str = "",
                 namespace: str | None = None,
                 source_stage_root: Path | str | None = None,
                 publisher: staged_path.StagedPublisher | None = None,
                 fill_mb_s: int = 0) -> None:
        self.mounts = mounts
        self.pacer = pacer
        self.stage_root = stage_root
        self.mount_prefix = mount_prefix
        self.namespace = namespace
        self.block = block
        self.workers = max(1, workers)
        self.owner = str(owner or "")
        self.publisher = publisher
        #: The pool share this mover's claim declared (MB/s), 0 when none:
        #: the bound of #1235's never-held rule, set by ``run`` per window.
        self.fill_mb_s = int(fill_mb_s or 0)
        #: Whether this window's whole copy is too small to matter to the
        #: pool it paces against (#1235); ``run`` decides it once.
        self.never_held = False
        #: Per-phase thread-seconds for the receipt, shared with the
        #: publisher so one clock sees the copy and its publication (#981).
        self.clock = publisher.clock if publisher is not None else staged_path.PhaseClock()
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
        #: The tier's reader plan (#1091), or ``None``: no exemption and no
        #: yield, as before.  ``stage_move`` sets it.
        self.reader_plan: _ReaderPlan | None = None
        #: Bytes read so far, per copy worker thread, landed or not (#1090):
        #: the mover's own landing report prices its live rate from their
        #: sum.  One key per thread, so no lock is taken per block.
        self.read_bytes: dict[int, int] = {}

    def bytes_read(self) -> int:
        """Every byte the copy workers have read so far, landed or not."""

        return sum(list(self.read_bytes.values()))

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
        source_id = staged_path._origin_id_of(source)
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
                # it is never the published bytes.  ``try_adopt`` counted
                # the outcome: a record's proof or the bytes' own.
                temporary.unlink(missing_ok=True)
                return adopted
        digest = hashlib.sha256()
        written = 0
        # Per-entry phase sums, added to the receipt's clock once the copy
        # ends: a clock lock per block would cost more than it measures.
        spent = {"pace_wait": 0.0, "copy_read": 0.0, "copy_write": 0.0,
                 "hash": 0.0, "fsync": 0.0}
        now = time.perf_counter
        plan = self.reader_plan
        me = threading.get_ident()
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
            try:
                temporary_handle = open(temporary, "wb")
            except FileNotFoundError:
                # The directory made above is still empty until this file
                # lands in it, so an egress's empty-directory prune
                # (``stage_release._prune_empty``) can remove it in the gap
                # (#1008 item 2).  One prune removes a directory once; a
                # second would need a second sweep inside this window -- the
                # same race and the same single retry
                # ``residency_map._write_atomic`` already takes for its own
                # directory.
                destination.parent.mkdir(parents=True, exist_ok=True)
                temporary_handle = open(temporary, "wb")
            with temporary_handle as sink:
                buffer = bytearray(self.block)
                view = memoryview(buffer)
                while written < want and not stop.is_set():
                    mark = now()
                    if plan is not None:
                        # Another copy on this tier is one a consumer is
                        # blocked on (#1091): stand aside, holding no slot.
                        plan.stand_aside(stop)
                        if stop.is_set():
                            spent["pace_wait"] += now() - mark
                            break
                    if not admission.acquire(
                            self.limit, stop,
                            self.pacer.hold_s if self.pacer is not None else 0.25):
                        spent["pace_wait"] += now() - mark
                        break
                    try:
                        if self.pacer is not None:
                            if (plan is not None and plan.exempt()) or self.never_held:
                                # Never held (#1091, and #1235's tiny window),
                                # still measured: the pacer samples the pool,
                                # so the receipt carries the delivery the cap
                                # is folded from.
                                sample = getattr(self.pacer, "sample", None)
                                if sample is not None:
                                    sample()
                            else:
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
                    self.read_bytes[me] = self.read_bytes.get(me, 0) + chunk
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
        if self.owner:
            # Name this mover as the copy's writer (#1088), before the
            # rename, so what this copy publishes carries it from its first
            # instant under the final name.
            # Best-effort like the mark below: a copy the filesystem would
            # not mark is judged by the rules that held before this one.
            try:
                os.setxattr(temporary, STAGE_MOVER_XATTR, self.owner.encode())
            except OSError:
                pass
        if source_id is not None:
            # File the origin change-detection the prewarm loop defined
            # (same name, same format), best-effort: a filesystem that
            # will not carry it still converges through the digest
            # comparison at publication.
            try:
                os.setxattr(temporary, STAGE_SOURCE_XATTR,
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
        paced = (self.pacer.depth if self.pacer is not None
                 else (lambda: self.workers))
        plan = self.reader_plan
        # A copy a consumer is blocked on reads at its deepest depth: the
        # pacer's shared depth protects other clients, which it never does
        # at the blocked consumer's expense (#1091).
        self.limit = (paced if plan is None
                      else (lambda: self.workers if plan.exempt() else paced()))
        # #1235: a copy whose whole window its own reserved share delivers
        # within ONE PACER SAMPLE INTERVAL is never held.  The bound is the
        # live pacer's own time resolution (``sample_s``, read off the
        # object, never a copied constant): such a window completes before
        # the pacer can observe its contribution, so a hold cannot be a
        # response to anything the copy caused, while the hold's cost (85 s
        # of consumer latency behind recurring streams, measured) is all
        # the copy waits for.  Its token bucket bounds its pool load to the
        # same share anyway.  Still measured, never held, exactly as
        # #1091's consumer-blocked copy.  A window with no declared share
        # stays held as before, and so does every window bigger than the
        # bound: the large fills' rationing is untouched.
        window_bytes = sum(int(entry["bytes"]) for entry in entries)
        sample_s = float(getattr(self.pacer, "sample_s", 0.0) or 0.0)
        self.never_held = (self.pacer is not None and self.fill_mb_s > 0
                           and sample_s > 0.0 and 0 < window_bytes
                           and window_bytes
                           <= self.fill_mb_s * 1_000_000 * sample_s)
        work: queuelib.Queue = queuelib.Queue()
        for entry in entries:
            work.put(entry)
        for _ in range(self.workers):
            work.put(None)

        #: Serializes handing out an entry with recording a refusal, so a
        #: refusal can never be followed by another entry being dispatched.
        #: Never held while logging (which takes ``self.lock``) or copying.
        dispatch = threading.Lock()

        def record_error(path: str, exc: object,
                        wait_verdict: str | None = None) -> None:
            with self.lock:
                if len(self.errors) < 20:
                    # ``wait_verdict`` (#994) names the grace's own verdict
                    # ahead of the free-text message, so the entry this
                    # refusal is about and the wait it settled from both
                    # survive the 20-entry cap even if later, unrelated
                    # errors bury the rest of the sentence (#853).
                    prefix = (f"publish wait ({wait_verdict}) refused after "
                             f"the grace: " if wait_verdict else "")
                    self.errors.append(
                        f"{os.path.basename(path)}: {prefix}{exc}")

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
                    relative = staged_path.stage_relative(
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
                except staged_path.PublicationRefused as exc:
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
                    record_error(path, exc, wait_verdict=exc.wait_verdict)
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
        declared = pb_progress.declared_phases() or ()
        #: Whether the launch was granted a ``start`` phase: the copy phase
        #: is then entered by a report when the copy starts, so its grace
        #: counts from the first read, not from the interpreter's launch,
        #: and the worker knows the start gate is behind it.
        self.start_declared = movement_actions.MOVER_START_PHASE in declared
        #: ``copy``, then ``warm``.
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
            if (units == 0 and self.reported_phase is None
                    and self.phase == movement_actions.MOVER_COPY_PHASE):
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
        if self.start_declared and self.refusal is None:
            # Enter ``copy`` now, with whatever is already landed (nothing,
            # on a first attempt).  A new phase is advancement, so the copy
            # grace runs from here.
            with self._lock:
                units = self.units()
                try:
                    written = pb_progress.commit(units, self.phase, unit=self.unit)
                except ValueError as exc:
                    self.refusal = str(exc)
                    written = False
                if written:
                    self.reports += 1
                    self.reported_units = max(self.reported_units, units)
                    self.reported_phase = self.phase
                    self.last_reported_unix = time.time()
                elif self.refusal is None:
                    self.unwritten += 1

        def loop() -> None:
            while not self._stop.wait(self.interval_s):
                self.report()

        self._thread = threading.Thread(target=loop, daemon=True,
                                        name="stage-move-progress")
        self._thread.start()

    def enter_warm(self, state: dict[str, int]) -> None:
        """The copy is done; count the read-back from here on."""

        self.report()
        with self._lock:
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
                "start_declared": self.start_declared,
                "phase": self.reported_phase, "unit": self.unit,
                "last_reported_unix": self.last_reported_unix,
                "unwritten": self.unwritten, "refusal": self.refusal}


class _TrailingFragment:
    """The trailing edge of the fragment's rate limit (#1151).

    ``move`` republishes its fragment as entries land, at most once per
    ``FRAGMENT_PUBLISH_S``.  A landing inside that interval is owed to the
    next publication, and before #1151 only another landing paid it.  A copy
    whose landings stopped -- its queue drained behind one straggler, or its
    readers held by the pacer -- therefore kept its last renamed names
    unvouched until its whole range ended.  Another mover of the same staged
    name reads such a name as a live publisher's pending one and waits for
    its vouch; when that mover's own last names are owed the same way, each
    waits on the other until the publication grace refuses both.  On
    2026-09-25 two Stage B phase movers of one boundary file set ended
    ``complete: false`` like this, at 436 and 511 of 512 entries, each
    refusing a name the other had renamed.

    Every ``interval_s`` a thread of its own calls ``pay``, which publishes
    the owed snapshot through the ordinary publication: the same rate limit,
    the same generation fence, the same sidecar-first order.  So a name is
    vouched within about ``FRAGMENT_PUBLISH_S`` of its rename whatever the
    copy does next, a snapshot is never written early or over a newer one,
    and nothing owed means nothing written.  The waiting mover then adopts
    the name through the publication gate's proof, as it always could once
    the vouch landed.
    """

    def __init__(self, pay, *, interval_s: float) -> None:
        self.pay = pay
        self.interval_s = max(0.001, float(interval_s))
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread is not None:
            return

        def loop() -> None:
            while not self._stop.wait(self.interval_s):
                self.pay()

        self._thread = threading.Thread(target=loop, daemon=True,
                                        name="stage-move-trailing-fragment")
        self._thread.start()

    def stop(self) -> None:
        """The copy is over: the final publication carries what is owed."""

        self._stop.set()
        if self._thread is not None:
            self._thread.join()


class _LandingReport:
    """This copy's own report of the bytes it has copied, on every launch (#1090).

    The worker's progress channel exists only when the mover was sealed with
    a progress policy, which needs a measured landing rate, so the copies
    that most need a live price -- cold ones, before anything like them has
    landed -- had none: on 2026-09-24 a claimed copy promised in 8 s landed
    497 s later, and its receipt read ``channel: false, reports: 0``.  This
    report needs no channel.  Every ``interval_s`` a thread of its own files
    ``pool.record_store.RecordStore.mover_landing_path``: the bytes read so far, landed or
    not (``copied_bytes``), the bytes landed (``landed_bytes``) and when the
    copy started, so the tier loop prices the claimed copy's landing from
    its own rate (``residency_plan.expected_landings``, ``reported``).

    It gates nothing and is not the worker's stall evidence.  A write that
    fails is counted and never fails the copy.  The copy's receipt prices it
    once it lands, so the report is removed when the mover ends; one left by
    a crash is an earlier attempt's, and the tier loop keeps a report only
    when it started inside the current claim.
    """

    def __init__(self, copier: "_Copier", queue: pool.PoolQueue, mover: str, *,
                 interval_s: float, range_bytes: int) -> None:
        self.copier = copier
        self.path = queue.records.mover_landing_path(mover)
        self.records = queue.records
        self.mover = str(mover)
        self.interval_s = max(0.001, float(interval_s))
        self.range_bytes = int(range_bytes)
        self.started_unix = time.time()
        self.reports = 0
        self.unwritten = 0
        self.last: dict[str, object] | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def write(self) -> None:
        with self.copier.lock:
            landed = int(self.copier.bytes_staged)
        copied = max(landed, int(self.copier.bytes_read()))
        record = {"schema": pool.MOVER_LANDING_SCHEMA_V1,
                  "mover_action_key": self.mover,
                  "started_unix": self.started_unix,
                  "reported_unix": time.time(),
                  "copied_bytes": copied, "landed_bytes": landed,
                  "range_bytes": self.range_bytes}
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.records.replace_record_at(self.path, record)
        except (OSError, pool.PoolContractError):
            self.unwritten += 1
            return
        self.reports += 1
        self.last = record

    def start(self) -> None:
        if self._thread is not None:
            return
        self.write()

        def loop() -> None:
            while not self._stop.wait(self.interval_s):
                self.write()

        self._thread = threading.Thread(target=loop, daemon=True,
                                        name="stage-move-landing")
        self._thread.start()

    def stop(self) -> None:
        """The copy is over: one last report, then no more."""

        self._stop.set()
        if self._thread is not None:
            self._thread.join()
        self.write()

    def remove(self) -> None:
        try:
            self.path.unlink(missing_ok=True)
        except OSError:
            self.unwritten += 1

    def record(self) -> dict[str, object]:
        """What the receipt says about the reports this copy made."""

        last = self.last or {}
        return {"interval_s": self.interval_s, "reports": self.reports,
                "unwritten": self.unwritten,
                "started_unix": self.started_unix,
                "copied_bytes": last.get("copied_bytes"),
                "landed_bytes": last.get("landed_bytes")}


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
            return manifest_codec.load_data_manifest(manifest_path)
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
    shared = _shared_consumers(args)
    if shared is not None:
        return _served_for_sharers(args, shared)
    try:
        queue = pool.PoolQueue(Path(args.pool_root))
        claim = pool.record_store.value_or_raise(
            queue.records.item(pool.record_store.CLAIMED, str(args.consumer_action_key)),
        )
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


def _shared_consumers(args) -> list[str] | None:
    """The consumers a shared range is staged for, or ``None`` if it is not one.

    A shared mover (#1026) files under its range's share namespace rather
    than under a consumer, so the consumer its reads serve is every live
    consumer still interested in the range
    (``stage_release.shared_interest``).  ``None`` for an ordinary mover;
    an empty list when the range is shared and nobody's interest reads.
    """

    try:
        queue = pool.PoolQueue(Path(args.pool_root))
        namespace = residency_plan.share_namespace_of(
            queue, str(args.action_key))
    except (OSError, ValueError, pool.PoolContractError):
        return None
    if namespace is None or namespace != str(args.consumer_action_key):
        return None
    try:

        interest = staged_path.shared_interest(queue, str(args.action_key))
    except (OSError, ValueError, pool.PoolContractError):
        return []
    return list(interest["interested"])


def _served_for_sharers(args, consumers: Sequence[str]) -> dict[str, object]:
    """:func:`served_for` of a shared range: every sharer's box, joined.

    Each claimed sharer's box is found the way one consumer's is -- its
    claim names the host and that host's offer names its addresses -- and
    the addresses are the union.  A sharer that is not claimed reads
    nothing yet, so it adds no address; with none claimed every client is
    protected, as for one unclaimed consumer.
    """

    queue = pool.PoolQueue(Path(args.pool_root))
    hosts: list[str] = []
    addresses: list[str] = []
    reasons: list[str] = []
    for consumer in consumers:
        try:
            claim = pool.record_store.value_or_raise(
                queue.records.item(pool.record_store.CLAIMED, consumer),
            )
        except (OSError, pool.PoolContractError):
            claim = None
        if not isinstance(claim, dict):
            continue
        found = prewarm_loop.served_addresses(
            queue, prewarm_loop.claimed_host_of(claim))
        host = str(found["served_host"])
        if host and host not in hosts:
            hosts.append(host)
        for address in found["served_addresses"]:     # type: ignore[union-attr]
            if address not in addresses:
                addresses.append(str(address))
        reasons.append(f"{consumer[:12]}: {found['served_reason']}")
    if not hosts:
        return {"served_host": "", "served_addresses": (),
                "served_reason": ("shared range: no sharer is claimed, so "
                                  "every client is a client to protect")}
    return {"served_host": ",".join(sorted(hosts)),
            "served_addresses": tuple(addresses),
            "served_reason": "shared range: " + "; ".join(reasons)}


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


def _parse_own_document(
    path: Path,
    validate: Callable[[object], object],
    trust,
) -> tuple[kept_reads.FileVersion, object] | None:
    """One resume document, parsed before the stage ownership lock (#1008).

    Read under the existing #1045 coarse-clock and local-filesystem fence
    (:func:`trust.file_version`), as :class:`kept_reads.CensusMemo` reads
    its hint pass, so a qualified version can be compared once the lock is held
    (:func:`_reread_own_document`).  A same-key retry's own fragment can be
    large, and this is the mover's *first* wait for the stage lock
    (``resume_lock_wait_s``): parsing it while holding that lock pays, under
    the very lock #988 moved the census out from under, for work the fence
    lets happen outside it instead.

    ``None`` for anything this pass could not use -- absent, unreadable or
    invalid -- and that decides nothing: it is a hint, exactly as the
    pre-lock census is, so the pass under the lock always re-reads a
    document this one returns ``None`` for.
    """

    fence = trust.file_fence()
    try:
        with open(path, "rb") as stream:
            descriptor = stream.fileno()
            version = trust.file_version(
                os.fstat(descriptor), fence, descriptor=descriptor)
            document = validate(json.load(stream))
    except (OSError, ValueError):
        return None
    return (version, document) if version is not None else None


def _reread_own_document(path: Path,
                         prior: tuple[kept_reads.FileVersion, object]
                         | None,
                         validate: Callable[[object], object], trust) -> object:
    """This mover's own document, reusing ``prior`` only at its own version.

    Called under the stage ownership lock.  ``prior`` is
    :func:`_parse_own_document`'s answer from before the lock was taken.
    The file is opened and ``fstat``-ed again regardless -- a document
    added, removed, replaced or rewritten since the pre-lock parse must be
    seen here, the same promise :class:`kept_reads.CensusMemo` keeps for
    the egress's own census -- and the earlier parse is returned without a
    second parse only when the current version is still qualified by the
    local-filesystem/coarse-clock fence and equals the one ``prior`` was read
    at. Lost trust or a same-tick observation requires a fresh parse. Raises
    exactly what a direct open-and-parse would:
    ``FileNotFoundError`` for an absent document, or ``OSError``/
    ``ValueError`` for one that exists but cannot be read or validated.
    """

    fence = trust.file_fence()
    with open(path, "rb") as stream:
        descriptor = stream.fileno()
        version = trust.file_version(
            os.fstat(descriptor), fence, descriptor=descriptor)
        if prior is not None and prior[0] == version:
            return prior[1]
        return validate(json.load(stream))


def _resume_own_coverage(
    queue,
    *,
    consumer_action_key: str,
    mover_action_key: str,
    tier_id: str,
    stage_root: Path,
    manifest_sha256: str,
    residency_root: Path,
    window: list[dict[str, object]],
    mount_prefix: str,
    namespace: str | None = None,
    named_once: frozenset[str] = frozenset(),
    timings: dict[str, float] | None = None,
    trust=None,
) -> tuple[dict[str, dict[str, object]],
                                    dict[str, dict[str, object]], str | None]:
    """This mover's own prior coverage, qualified for a same-key retry.

    A retried mover starts every dictionary empty, and its first
    incremental publication *replaces* the fragment and material it filed
    under the same consumer and mover keys -- so the qualified suffix its
    previous attempt published loses proof, is recopied, and pays the
    publication grace per entry.  This is the narrow resume for exactly
    that: the prior **own** records are read and qualified -- document
    reads, header checks and the per-entry file stats together -- with a
    qualified pre-lock parse rechecked inside the stage ownership lock, and
    the retry's publications are never
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
    unparseable sidecar (a vouch without a date, which a crash between
    the two writes left before #1087) keeps its vouches, invents no date,
    and the rerun overwrites both as it always has.  No payload is hashed;
    an undated vouch is preserved as exactly that, never upgraded.  The
    crash window the movers leave since #1087 is the other one: the
    sidecar lands first, so it may date entries the fragment does not yet
    vouch.  Only the fragment's entries are walked here, so those dates
    are neither carried nor contradicted; each such name reads as
    positive absence at the gate, whose content proof adopts it.

    Returns ``(staged_seeds, sidecar_seeds, resumed_generation)``.  Empty
    seeds with a ``None`` generation mean "nothing resumable", which is
    today's behavior, not an error.

    The fragment and the sidecar are parsed once before the stage ownership
    lock is taken, then reopened and version-checked under it
    (:func:`_parse_own_document`,
    :func:`_reread_own_document`, #1008 item 1) -- the same #761 fence
    :class:`kept_reads.CensusMemo` reads its own hint pass under, so
    this mover's first wait for the lock (``resume_lock_wait_s``) no longer
    pays for opening and parsing its own fragment, and a document that
    changed in the gap, or whose version cannot be trusted, is parsed fresh
    under the lock. Unchanged qualified documents need no second parse.
    """
    trust = kept_reads.filesystem_trust if trust is None else trust

    staged: dict[str, dict[str, object]] = {}
    sidecar: dict[str, dict[str, object]] = {}

    # The names this window derives, by the map key the fragment uses.
    # Pure derivation from the sealed inputs -- no state is read -- so it
    # runs before the lock without widening the transaction.
    facts: dict[str, tuple[int, str, str, int]] = {}
    for entry in window:
        path, offset = str(entry["path"]), int(entry["offset"])
        relative = staged_path.stage_relative(
            path, offset, int(entry["bytes"]),
            mount_prefix=mount_prefix, namespace=namespace,
            named_once=path in named_once)
        key = residency_map.residency_map_key(path, offset)
        facts[key] = (int(entry["bytes"]), os.path.normpath(str(
            Path(stage_root) / relative)), str(entry.get("sha256") or ""),
            offset)

    fragment_path = residency_map.fragment_path(
        residency_root, consumer_action_key, mover_action_key)
    sidecar_path = reader_lease.material_path(
        residency_root, consumer_action_key, mover_action_key)
    # Parsed before the stage ownership lock (#1008 item 1): this is the
    # mover's first wait for it, and a same-key retry's own fragment can be
    # large.  Each parse is a hint, kept with the file version it was read
    # at; the pass under the lock below re-checks that version and re-parses
    # only when it changed, never trusting this one for the decision.
    pre_fragment = _parse_own_document(
        fragment_path, residency_map.validate_fragment, trust=trust)
    pre_sidecar = _parse_own_document(
        sidecar_path, reader_lease.validate_material, trust=trust)
    asked = time.perf_counter()
    # Named as this mover's own hold (#1110): the census runs in the mover's
    # first phase, where its worker looks at the start gate, and an
    # unrecorded hold there is credited as a wait on somebody else.
    with queue.recorded_stage_ownership(
            stage_root, role="mover-resume", action_key=mover_action_key,
            require_record=pb_progress.channel() is not None) as granted:
        if timings is not None:
            # The first time a mover queues for the stage ownership lock:
            # an egress's hold is paid here before the start gate (#988).
            # To the grant, so the record's own write is not called a wait.
            timings["resume_lock_wait_s"] = granted - asked
        try:
            fragment = _reread_own_document(
                fragment_path, pre_fragment, residency_map.validate_fragment, trust=trust)
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

        try:
            material = _reread_own_document(
                sidecar_path, pre_sidecar, reader_lease.validate_material, trust=trust)
        except FileNotFoundError:
            material = None
        except (OSError, ValueError) as exc:
            # :func:`reader_lease.read_material`'s own answer for this case:
            # a taint, not a raise -- the check below treats it exactly as
            # an absent sidecar, per this function's own docstring.
            material = exc
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
        # An absent or unparseable sidecar -- the crash window before
        # #1087 wrote the sidecar first -- keeps the vouches and invents no
        # date; the rerun overwrites both as it always has.  A sidecar
        # ahead of the fragment (the crash window since) dates entries no
        # vouch here names, and the walk below never reads those.

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
    entries = manifest_layout.read_entries(manifest)
    window = manifest_layout.entries_between(
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
    named_once = frozenset(staged_path.paths_named_once(entries))
    destinations: dict[str, str] = {}
    for entry in window:
        path, offset = str(entry["path"]), int(entry["offset"])
        relative = staged_path.stage_relative(path, offset, int(entry["bytes"]),
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
    publisher = staged_path.StagedPublisher(
        queue=queue, stage_root=Path(args.stage_root),
        residency_root=residency_root,
        mover_action_key=str(args.action_key),
        manifest_sha256=str(args.manifest_sha256),
        tier_id=str(args.tier_id), cas_root=str(args.cas_root),
        consumer_action_key=str(args.consumer_action_key))
    copier = _Copier(
        mounts=mounts, pacer=pacer, stage_root=Path(args.stage_root),
        mount_prefix=mount_prefix, block=args.block, workers=args.max_readers,
        owner=str(args.action_key), publisher=publisher, namespace=namespace,
        fill_mb_s=int(getattr(args, "fill_mb_s_pool_side", 0) or 0))
    # The tier's reader plan (#1091): this copy goes first when a consumer is
    # blocked on it, and stands aside while one is blocked on another.
    copier.reader_plan = _ReaderPlan(
        queue, str(args.tier_id), str(args.action_key),
        hold_s=float(getattr(pacer, "hold_s", 0.25) or 0.25))

    manifest_sha256 = args.manifest_sha256
    # This key's last receipt, read before this run files its own: the count
    # an unprovable ending is bounded by lives there (#1004 item 2).
    prior_receipt = prior_move_record(queue, str(args.action_key))

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
    # The newest snapshot the rate limit declined: owed to the next
    # publication, which the trailing flush makes when no landing does
    # (#1151).  ``None`` when nothing is owed.
    owed: list[tuple | None] = [None]
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
                *, force: bool = False) -> bool:
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

        The material sidecar goes first, then the fragment it dates
        (#1087): a crash between them leaves a date that no vouch cites,
        which every reader ignores -- the gate walks fragments, so the name
        reads as positive absence and the #1081 content proof adopts it,
        and the strict reader pins only names both documents carry -- and
        the rerun overwrites both.  The other order left a vouch without a
        date, which the gate reads as another publisher's pending date for
        every later mover of the name, this key's retry included, and
        refuses after the grace forever.

        A snapshot the limit declines is owed, not dropped (#1151).  Only a
        later landing used to pay it, so a copy whose landings stopped -- its
        queue drained behind one straggler, its readers held by the pacer --
        kept its last renamed names unvouched until the whole range ended,
        and another mover of the same staged name waited on them as a live
        publisher's.  :class:`_TrailingFragment` pays the debt once the
        limit allows, through this same function, so the limit and the
        generation fence below hold for it exactly as for a landing.
        """

        with publish_lock:
            now = time.monotonic()
            if generation and generation <= last_generation[0]:
                # A snapshot older than the one already on disk.  Writing it
                # would take entries back out of a published fragment, which
                # is the one thing a fragment may never do.
                return False
            if not force and now - last_published[0] < FRAGMENT_PUBLISH_S:
                # Owed, newest first: a delayed older snapshot never
                # displaces a newer one the limit already declined.
                if owed[0] is None or generation > owed[0][2]:
                    owed[0] = (staged, identities, generation)
                return False
            last_published[0] = now
            last_generation[0] = max(last_generation[0], generation)
            if owed[0] is not None and (not generation
                                        or owed[0][2] <= generation):
                owed[0] = None      # this write carries what was owed
            staged_path.publish_vouch(residency_root, {
                "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
                "consumer_action_key": args.consumer_action_key,
                "mover_action_key": args.action_key,
                "tier_id": args.tier_id,
                "stage_root": str(args.stage_root),
                "manifest_sha256": manifest_sha256,
                "entries": staged,
            }, generation=publisher.material_generation() if identities else None,
                material_entries=identities)
            return True

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
    # This copy's own landed-bytes report, channel or not (#1090).
    landing = _LandingReport(
        copier, queue, str(args.action_key),
        interval_s=float(getattr(args, "progress_interval_s", None)
                         or pool.HEARTBEAT_S),
        range_bytes=declared)
    landing.start()

    def pay_owed() -> None:
        """Publish what the rate limit declined, once it allows (#1151)."""

        with publish_lock:
            snapshot = owed[0]
        if snapshot is None:
            return
        began = time.perf_counter()
        try:
            wrote = publish(*snapshot)
        except (OSError, ValueError) as exc:
            # The same degradation a landing's publication records: the
            # entries stay staged, and the final publish names them or
            # fails loudly.
            with copier.lock:
                if len(copier.errors) < 20:
                    copier.errors.append(f"fragment publication: {exc}")
            return
        if wrote:
            # Counted only when it wrote: the receipt says how often a
            # landing's debt was paid by the trailing edge, not how often
            # the thread looked.
            copier.clock.add("fragment_trailing_publication",
                             time.perf_counter() - began)

    trailing = (None if args.no_incremental_fragment
                else _TrailingFragment(pay_owed,
                                       interval_s=FRAGMENT_PUBLISH_S / 4))
    if trailing is not None:
        trailing.start()
    try:
        copier.run(window, stop=stop,
                   on_entry=None if args.no_incremental_fragment else publish,
                   named_once=named_once)
    finally:
        if trailing is not None:
            trailing.stop()
    landing.stop()
    elapsed = max(1e-9, time.time() - started)
    after = proc_io()
    cpu_used = max(0.0, cpu_seconds() - cpu_before)
    pacing = (pacer.report() if pacer is not None
              else prewarm_loop.inactive_pacing("no pacer"))
    plan_report = copier.reader_plan.report()
    # Stood aside on purpose, like a hold: ``_fell_short`` reads both.
    pacing[storage_tiers.YIELDED_FIELD] = plan_report["yielded_seconds"]

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
        # How many entries waited for another publisher, under which of the
        # four kinds (#994), and how many seconds -- a sibling of
        # ``phase_timings`` rather than a phase inside it, and the field a
        # post-grace refusal used to leave unrecorded: the mover looped on
        # the wait, re-took the host-wide lock every ``_PUBLISH_POLL_S``,
        # then refused with only a free-text message, capped at 20 entries
        # and easily buried by later, unrelated errors (#853), to say why.
        "publish_waits": copier.clock.publish_waits_report(),
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
    bound_unproven_endings(receipt, queue, publisher, prior_receipt,
                           consumer=str(args.consumer_action_key),
                           mover=str(args.action_key))
    if overran:
        # The tokens bound what the tier can hold.  Staging past them is the
        # one failure that cannot be left to the consumer to notice, so the
        # fragment goes and the receipt says why.
        receipt["refusal"] = "residency_overran_reservation"
        receipt["complete"] = False
        staged_path.withdraw_vouch(
            None, residency_root, args.consumer_action_key, args.action_key)
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
    # What the tier loop priced its landing from (#1090), and where it stood
    # in the tier's reader plan (#1091).
    landing.remove()
    receipt["landing_report"] = landing.record()
    receipt["reader_plan"] = plan_report
    return receipt


def prior_move_record(queue: pool.PoolQueue,
                      mover: str) -> dict[str, object] | None:
    """The receipt this mover's key filed on its last run, or ``None``.

    Read before the run files its own.  An unreadable receipt is no prior:
    the count of an unprovable ending starts again, which only delays the
    terminal refusal, never hastens it.
    """

    try:
        return queue.move_record(mover)
    except (OSError, ValueError, pool.PoolContractError):
        return None


def bound_unproven_endings(receipt: dict[str, object], queue: pool.PoolQueue,
                           publisher: "_StagedPublisher", prior: object, *,
                           consumer: str, mover: str) -> None:
    """Count this run's unproven refusals; refuse terminally at the bound.

    An owner whose ending can never be proven -- a legacy consumer with no
    outcome record (#798), a fragment that stays unreadable -- left the
    mover refusing retryably, exiting incomplete, and the window
    republishing it every cycle, forever (#1004 item 2).  Each run now
    files the evidence it refused on and how many consecutive runs refused
    on the same evidence (:func:`unproven_streak`).  At the bound the run
    refuses ``staged_destination_unproven``, naming the path and the
    unproven owners, exits nonzero and retires its own window with the
    #708 record, exactly as the live owner's conflict does.  A live
    conflict in the same run outranks it.  Nothing is replaced either way.
    """

    block = staged_path.unproven_streak(prior, publisher.unproven, now=time.time())
    if block is None:
        return
    receipt["unproven"] = block
    if not block["terminal"] or receipt.get("refusal") is not None:
        return
    receipt["refusal"] = staged_path.STAGED_DESTINATION_UNPROVEN
    receipt["complete"] = False
    conflict = {
        "stage_path": block["stage_path"],
        "consumer_action_key": consumer,
        "mover_action_key": mover,
        "manifest_sha256": publisher.manifest_sha256,
        "declared_sha256": block["declared_sha256"],
        "owners": block["owners"],
        "why": block["why"],
        "runs": block["runs"],
    }
    receipt["conflict"] = conflict
    receipt["plan_superseded"] = retire_conflicted_window(
        queue, consumer, mover, conflict,
        refusal=staged_path.STAGED_DESTINATION_UNPROVEN)


def retire_conflicted_window(queue: pool.PoolQueue, consumer: str,
                             mover: str,
                             conflict: Mapping[str, object], *,
                             refusal: str = staged_path.STAGED_DESTINATION_CONFLICT,
                             ) -> bool:
    """Mark the window that sealed ``mover`` superseded, naming the conflict.

    ``refusal`` is the terminal refusal the reason is filed under: the live
    owner's conflict, or an ending that stayed unprovable (#1004 item 2).
    The marker also carries the conflict itself, structured, so the stalled
    consumer's claim denial and ``pbstatus --starvation`` name the path and
    both owners without parsing the reason (#1004 item 3).

    The existing retirement record (#708): ``residency_window`` stops
    publishing movers from a superseded plan, and a deliberate resubmission
    seals a fresh one.  Bound to the filing that names this mover, under the
    consumer's transition lock (:func:`residency_plan.mark_superseded`), so a
    mover of a replaced plan never retires its replacement.  ``False`` when
    nothing was marked: no plan, a plan that does not name this mover, or a
    record that could not be read or written.
    """

    try:
        namespace = residency_plan.share_namespace_of(queue, mover)
    except (OSError, ValueError, pb.PrismaBuildError):
        return False
    if namespace is not None and namespace == consumer:
        # A shared range's window is every sharer's (#1026): each plan that
        # names this mover would republish it into the same refusal.
        try:

            sharers = staged_path.shared_interest(queue, mover)
        except (OSError, ValueError, pb.PrismaBuildError):
            return False
        marked = [retire_conflicted_window(queue, sharer, mover, conflict,
                                           refusal=refusal)
                  for sharer in sharers["interested"]]
        return any(marked)
    try:
        refused: list[Exception] = []
        plan, filing = residency_plan.read_filed(
            queue, consumer, on_unreadable=refused.append)
        if (plan is None or refused
                or residency_plan.find_mover_leg(plan, mover) is None):
            return False
        owners = "; ".join(
            f"consumer {row.get('consumer_action_key')} mover "
            f"{row.get('mover_action_key')} ({row.get('state')}"
            + (f": {row.get('why')}" if row.get("why") else "") + ")"
            for row in conflict.get("owners") or ()
            if isinstance(row, Mapping))
        reason = (f"{refusal}: "
                  f"{conflict.get('stage_path')} holds the bytes of "
                  f"{owners or 'an owner a fragment it could not read may name'}"
                  f"; consumer {consumer} mover {mover} wants "
                  f"{conflict.get('declared_sha256')}")
        if refusal == staged_path.STAGED_DESTINATION_UNPROVEN:
            reason += (f"; the ending was unproven on {conflict.get('runs')} "
                       f"consecutive runs: {conflict.get('why')}")
        marker = residency_plan.mark_superseded(
            queue, consumer, plan=plan, filing=filing, reason=reason,
            movers=[mover], by="stage-move",
            conflict={**conflict, "refusal": refusal})
    except (OSError, ValueError, pb.PrismaBuildError):
        return False
    return marker is not None




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
    parser.add_argument("--pace-pool", default=movement_actions.MOVER_PACE_POOL,
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
    parser.add_argument("--max-read-await-ms", type=float,
                        default=movement_actions.MOVER_MAX_READ_AWAIT_MS,
                        help="hold the copy above this member read latency")
    parser.add_argument("--max-backlog-ms", type=float,
                        default=movement_actions.MOVER_MAX_BACKLOG_MS,
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
    args.action_key = staged_path.own_action_key(args.action_key)
    if args.residency_root is None:
        args.residency_root = str(Path(args.pool_root) / pool.record_store.RESIDENCY)

    receipt = move(args)
    queue = pool.PoolQueue(Path(args.pool_root))
    queue.record_move(args.action_key, receipt)
    if args.receipt:
        # The atomic writer every other fleet record uses: a reader of this
        # path must never be able to observe half a document.
        immutable_files.write_json_atomic(Path(args.receipt), receipt)
    print(json.dumps(receipt, indent=1, sort_keys=True, default=str))
    return 1 if receipt.get("refusal") else 0


if __name__ == "__main__":
    raise SystemExit(main())
