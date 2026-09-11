#!/usr/bin/env python3
"""Make the next actions' declared bytes resident before anyone claims them.

Why this exists
---------------
A campaign whose working set is far larger than the file server's ARC gets
nothing from caching: every byte is read once and never asked for again, so a
cache that fills behind the reader is a cache that never hits.  The only read
that can be made fast is one that was already resident when the action started.

Measured on the GLM-5.3-Flash census (2026-09-11, dl380g10): a row whose
capture files were resident read at 3298.7 MB/s over NFS; the same size of
never-read row read at 306.9 MB/s.  10.75x, 145.8 s saved on a single row, and
the disks were never the limit -- HDD utilization never passed 88% at any
reader count.

What it does, every poll
------------------------
1. Reads ``ready/`` in PrismaBuild's own claim order and takes the first few
   actions that carry a ``pbcampaign.data-manifest`` input.  It does not claim,
   reserve, reorder or write an item: claim order stays the coordinator's, and
   this loop only reads the same list the workers read.
2. Checks ARC headroom against what the manifests ask for, minus what the
   currently claimed actions still need.
3. Reads each manifest's files, in manifest order, through the host's local
   pool path with bounded parallelism.
4. Writes one record per action under ``pb-queue/prewarm/<action_key>.json``.
   That is the only thing it writes, anywhere.

Pacing
------
Reading fast is not the objective; reading without displacing the fleet is.
On 2026-09-11 eight concurrent readers took the raidz1 to 73-83% utilization,
38-54 ms read await and 11-14 s of backlog, the clients' sync writes queued
behind that, their RPCs passed the RDMA timeout, and both Sparks' GPUs idled
100-130 s per row while the transport reconnected (#499).  The same bytes read
at 8-12% utilization and ~0 ms await cost nobody anything.

So the reader is paced by the disks rather than by a thread count: it samples
``/sys/block/<dev>/stat`` for the pool's own members and holds before a block
whenever the worst disk is above a threshold.  A thread count cannot do this --
it fixes concurrency, not load, and the load a given concurrency produces
depends on what every other tenant is doing at that moment.  The numbers it
sampled go into the prewarm record beside ``mb_per_s``, because a warm that was
fast and a warm that was harmless are different claims.

The shipped caps are the ones a live run passed on, not the midpoint between
the two measured states: one reader at 40% / 15 ms / 4 000 ms kept the disks at
31% peak and still dropped both Sparks' NFS clients to ~50 RPC/s, while one
reader at 25% / 10 ms / 2 000 ms warmed a 63.8 GB row at 146.5 MB/s with the
clients untouched (2026-09-11 09:13:21-09:20:36 UTC).

What it does not do
-------------------
It is not a second dispatcher.  It never decides what runs, where, or next; it
predicts nothing the queue does not already say out loud.  If the coordinator
reorders the queue between two polls, the loop simply warms a different row.

It cannot *guarantee* that a claimed action's data is never evicted, and says
so rather than implying otherwise: ZFS exposes no pin, and the ARC target ``c``
is volatile on a shared box -- it fell 99 GB inside one five-minute window on
2026-09-11 without any tenant asking for the memory.  What it does guarantee is
budgetary: it never *asks* for more than ``(c_max - size) * reserve`` minus the
manifest bytes of every action claimed inside the last ``--claim-grace-min``
minutes, which is the window in which a claimed action is still reading.  Both
headroom numbers are logged every cycle so the gap between the budget and the
outcome stays visible.
"""

from __future__ import annotations

import argparse
import json
import os
import queue as queuelib
import shutil
import socket
import stat as statmod
import subprocess
import sys
import threading
import time
from collections.abc import Callable
from pathlib import Path

# The publisher writes this program twice, ``tools/fleet/prewarm_loop.py`` and
# a flattened ``tools/prewarm_loop.py``, and ``supervise._spawn_role`` runs the
# flat one.  ``parents[2]`` is the repository root under the first spelling and
# the parent of the generation store under the second, so the generation must
# be resolved by the rule that knows both layouts (the rule ``pbtest.py`` uses).
sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
from runtime_paths import generation_root  # noqa: E402

sys.path.insert(0, str(generation_root(__file__) / "src"))

import prismabuild.core as pb  # noqa: E402
from prismabuild import pool  # noqa: E402

#: ZFS reads a whole record either way, and the pool this was measured on uses
#: 1 MiB records; a smaller block only costs syscalls.
BLOCK = 1 << 20
ARCSTATS = "/proc/spl/kstat/zfs/arcstats"
#: ``/sys/block/<dev>/stat`` field offsets, from
#: ``Documentation/block/stat.rst``.  Named because the file is a bare row of
#: integers and an off-by-one here reads writes as reads.
STAT_READS_COMPLETED = 0
STAT_READ_MS = 3
STAT_IN_FLIGHT = 8
STAT_IO_TICKS = 9
STAT_WEIGHTED_IO_MS = 10
SYSFS_BLOCK = "/sys/class/block"
#: Top-level ``zpool status`` groups whose members are not the pool's data
#: spindles.  Pacing on the L2ARC device would hold the reader off an SSD that
#: was never the constraint.
ZPOOL_AUX_GROUPS = frozenset(
    {"cache", "log", "logs", "spares", "spare", "special", "dedup"})
#: The fleet's own store, spelled the way ``worker_loop`` spells it.  Not
#: ``pool.DEFAULT_POOL_ROOT``: that default is ``/mnt/shared/pb-queue``, one
#: directory above the queue this fleet actually keeps, and a loop pointed
#: there reports an empty ready list forever rather than an error.
SH = Path("/mnt/shared/prismabuild-fleet")


# ------------------------------------------------------------------ ARC


def utc(unix: float) -> str:
    """An instant as ISO 8601 in UTC, to the second."""

    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(unix))


def arcstats(path: str = ARCSTATS) -> dict[str, int]:
    out: dict[str, int] = {}
    try:
        with open(path) as handle:
            for line in handle:
                fields = line.split()
                if len(fields) == 3:
                    try:
                        out[fields[0]] = int(fields[2])
                    except ValueError:
                        pass
    except OSError:
        pass
    return out


def arc_headroom(reserve_fraction: float, path: str = ARCSTATS) -> dict[str, int]:
    """The ARC this loop may spend, and the counters that explain the number.

    The budget is a share of ``c_max`` -- the cache's *capacity* -- and not of
    the free space inside it.  That is not an optimism: a cache in steady
    state is full, and this one is.  Measured on dl380g10 at 2026-09-11
    01:59 UTC, after the campaign had been reading for hours: ``size``
    254.05 GB against ``c_max`` 257.70 GB, leaving 3.65 GB free.  A loop that
    budgeted on free space would have refused every 64 GB row forever while
    the ARC sat full of bytes nobody would ask for again -- which is the whole
    failure prewarm exists to fix.  Warming evicts, and it is supposed to: the
    question is never "is there room" but "is what I would displace worth less
    than what I would make resident".  That question is answered by
    subtracting the bytes that are still wanted, in ``cycle``.

    ``size``, ``c`` and the two free-space numbers are reported anyway,
    because they are what a reader needs to tell a refusal caused by the
    protected set from one caused by a shrunken cache: ``c`` is volatile here,
    and fell 99 GB inside one five-minute window on 2026-09-11.
    """

    stats = arcstats(path)
    size = stats.get("size", 0)
    current = stats.get("c", 0)
    ceiling = stats.get("c_max", 0)
    return {
        "arc_size": size,
        "arc_c": current,
        "arc_c_max": ceiling,
        "headroom_nominal": max(0, ceiling - size),
        "headroom_effective": max(0, current - size),
        "capacity_budget": max(0, int(ceiling * reserve_fraction)),
    }


# ------------------------------------------------------------ path mapping


class MountMap:
    """Rewrite a manifest's shared-mount path onto this host's local mount.

    The mapping is host configuration, not a constant: dl380g10 serves
    ``/mnt/shared`` out of a local ZFS dataset also mounted at
    ``/storage_pool/shared``, and reading the local path keeps the warm off the
    server's own NFS client.  A host with no local mount has no business
    running this loop, which is what ``usable`` reports.
    """

    def __init__(self, pairs: list[str]) -> None:
        self.pairs: list[tuple[str, str]] = []
        for pair in pairs:
            shared, _, local = pair.partition("=")
            if not shared.startswith("/") or not local.startswith("/"):
                raise SystemExit(f"prewarm: bad --mount-map entry: {pair!r}")
            self.pairs.append((shared.rstrip("/"), local.rstrip("/")))

    def usable(self) -> bool:
        return any(os.path.isdir(local) for _, local in self.pairs)

    def local(self, path: str) -> str:
        for shared, local in self.pairs:
            if path == shared or path.startswith(shared + "/"):
                candidate = local + path[len(shared):]
                if os.path.isdir(local):
                    return candidate
        return path


# ----------------------------------------------------------------- pacing


def zpool_binary() -> str:
    """``zpool``'s path, resolved rather than left to ``PATH``.

    The loop runs under a supervisor-spawned unit, and a unit's ``PATH`` need
    not carry ``/usr/sbin``.  Losing discovery to a missing ``PATH`` entry
    would silently reinstate #499's unpaced reads, so the sbin paths are tried
    by name when the lookup fails.
    """

    found = shutil.which("zpool")
    if found:
        return found
    for candidate in ("/usr/sbin/zpool", "/sbin/zpool"):
        if os.path.exists(candidate):
            return candidate
    return "zpool"


def pool_member_devices(
    pool: str,
    *,
    runner: Callable[[list[str]], str] | None = None,
    sysfs: str = SYSFS_BLOCK,
) -> list[str]:
    """The block devices behind ``pool``'s data vdevs, as ``sdb``-style names.

    ``zpool status -P`` prints every leaf as an absolute path, so the members
    are read from the pool's own topology rather than guessed from a device
    glob -- a box whose root disks are also ``sd*`` would otherwise be paced by
    a disk the pool never touches.  The auxiliary groups are dropped: an L2ARC
    or SLOG device is not the raidz queue this loop must stay off.

    Returns ``[]`` on any failure, including no ``zpool`` at all.  That is not
    a silent degradation: the caller records the empty list and reports pacing
    inactive, which is the honest answer on a host that has no pool.
    """

    def _run(argv: list[str]) -> str:
        return subprocess.run(
            argv, check=True, text=True, timeout=30,
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL).stdout

    try:
        text = (runner or _run)([zpool_binary(), "status", "-P", pool])
    except (OSError, subprocess.SubprocessError):
        return []

    devices: list[str] = []
    in_config = False
    section = "data"
    for line in text.splitlines():
        stripped = line.strip()
        if not in_config:
            in_config = stripped.startswith("config:")
            continue
        if not stripped:
            continue
        if stripped.startswith("NAME "):
            continue
        if stripped.startswith("errors:"):
            break
        indent = len(line) - len(line.lstrip())
        name = stripped.split()[0]
        # The pool itself and the auxiliary group headers sit at the same
        # depth; anything deeper belongs to whichever of them came last.
        if indent <= 2:
            section = "aux" if name.lower() in ZPOOL_AUX_GROUPS else "data"
            continue
        if section != "data" or not name.startswith("/"):
            continue
        device = whole_disk_of(name, sysfs=sysfs)
        if device and device not in devices:
            devices.append(device)
    return devices


def whole_disk_of(path: str, *, sysfs: str = SYSFS_BLOCK) -> str | None:
    """``/dev/sdb1`` -> ``sdb``: the disk whose queue the partition shares.

    Resolved through sysfs rather than by stripping trailing digits, which
    gets ``nvme0n1`` wrong in both directions.  A partition's sysfs node is a
    child of its disk's node; a whole disk's parent is the ``block`` class
    directory and has no ``dev`` file.
    """

    base = os.path.basename(path)
    try:
        node = Path(sysfs, base).resolve(strict=True)
    except OSError:
        return None
    parent = node.parent
    if parent.name != "block" and (parent / "dev").exists():
        return parent.name
    return node.name


def read_disk_stat(device: str, *, root: str = "/sys/block") -> list[int] | None:
    try:
        with open(f"{root}/{device}/stat") as handle:
            return [int(field) for field in handle.read().split()]
    except (OSError, ValueError):
        return None


class DiskPacer:
    """Hold the reader while the pool's worst disk is above what is harmless.

    The three numbers are the ones Netdata charts, computed the same way, so a
    record written here and the ``disk_util``/``disk_await``/``disk_backlog``
    series an operator reads afterwards are the same quantities:

    * ``util_pct``   = d(io_ticks) / d(wall_ms) * 100
    * ``read_await`` = d(read_ms) / d(reads_completed), and 0 when no read
      completed in the interval -- an interval with no completions has no
      average to report, and inventing one either divides by zero or fabricates
      a stall out of an idle disk.
    * ``backlog_ms`` = d(weighted_io_ms) / d(wall_s)

    Thresholds are arguments, not constants, because they are a property of the
    pool: what is harmless on four spindles is not what is harmless on an SSD.
    The defaults come from the two states measured in #499 and are documented
    at the argument.

    The first sample has no interval and therefore no verdict, so it never
    holds.  This matters: a pacer that treated "unknown" as "over" would stall
    forever on a host whose stat file it cannot read, which is exactly the host
    where pacing is inactive and reading is fine.
    """

    def __init__(
        self,
        devices: list[str],
        *,
        max_util_pct: float,
        max_read_await_ms: float,
        max_backlog_ms: float,
        sample_s: float = 0.5,
        hold_s: float = 0.25,
        stat_source: Callable[[str], list[int] | None] = read_disk_stat,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        reason: str = "",
        notify: Callable[[dict[str, object]], None] | None = None,
    ) -> None:
        self.devices = list(devices)
        self.max_util_pct = float(max_util_pct)
        self.max_read_await_ms = float(max_read_await_ms)
        self.max_backlog_ms = float(max_backlog_ms)
        self.sample_s = max(0.0, float(sample_s))
        self.hold_s = max(0.001, float(hold_s))
        self.stat_source = stat_source
        self.clock = clock
        self.sleep = sleep
        self.reason = reason or ("" if self.devices else "no pool devices")
        #: Called when a hold starts and when it ends.  A loop that goes quiet
        #: for minutes because the campaign itself is saturating the spindles
        #: is behaving correctly, and an operator must be able to see that
        #: rather than infer it from a warm that never finished.
        self.notify = notify
        self._lock = threading.Lock()
        self._previous: dict[str, list[int]] = {}
        self._previous_at = 0.0
        self._sampled_at = 0.0
        self._over = False
        self._readable = True
        self._samples = 0
        self._holds = 0
        self._held_s = 0.0
        self._holding = 0
        self._hold_started = 0.0
        self._totals = {"util_pct": 0.0, "read_await_ms": 0.0, "backlog_ms": 0.0}
        self._maxima = {"util_pct": 0.0, "read_await_ms": 0.0, "backlog_ms": 0.0}
        self._last: dict[str, float] = {}

    @property
    def active(self) -> bool:
        return bool(self.devices)

    # -- sampling ---------------------------------------------------------

    def _measure(self, now: float) -> dict[str, float] | None:
        """One interval's worst-disk numbers, or ``None`` if there is no interval."""

        current: dict[str, list[int]] = {}
        for device in self.devices:
            row = self.stat_source(device)
            if row is not None and len(row) > STAT_WEIGHTED_IO_MS:
                current[device] = row
        #: "Nothing answered" and "too soon to have an interval" are both
        #: *unknown*, but only the first one may clear a hold: an interval of
        #: zero says nothing about the pool, while a silent disk means the
        #: pacer has lost its evidence and must not keep holding on it.
        self._readable = bool(current)
        previous, previous_at = self._previous, self._previous_at
        self._previous, self._previous_at = current, now
        elapsed = now - previous_at
        if not previous or elapsed <= 0:
            return None
        worst = {"util_pct": 0.0, "read_await_ms": 0.0, "backlog_ms": 0.0,
                 "in_flight": 0.0}
        seen = False
        for device, row in current.items():
            before = previous.get(device)
            if before is None:
                continue
            seen = True
            reads = row[STAT_READS_COMPLETED] - before[STAT_READS_COMPLETED]
            read_ms = row[STAT_READ_MS] - before[STAT_READ_MS]
            ticks = row[STAT_IO_TICKS] - before[STAT_IO_TICKS]
            weighted = row[STAT_WEIGHTED_IO_MS] - before[STAT_WEIGHTED_IO_MS]
            worst["util_pct"] = max(
                worst["util_pct"], min(100.0, 100.0 * ticks / (elapsed * 1000.0)))
            worst["read_await_ms"] = max(
                worst["read_await_ms"], (read_ms / reads) if reads > 0 else 0.0)
            worst["backlog_ms"] = max(worst["backlog_ms"], weighted / elapsed)
            worst["in_flight"] = max(worst["in_flight"], float(row[STAT_IN_FLIGHT]))
        return worst if seen else None

    def _refresh_locked(self, now: float) -> None:
        measured = self._measure(now)
        self._sampled_at = now
        if measured is None:
            if not self._readable:
                # A disk that stops answering must not leave a hold latched.
                self._over = False
            return
        self._samples += 1
        for key in self._totals:
            self._totals[key] += measured[key]
            self._maxima[key] = max(self._maxima[key], measured[key])
        self._last = measured
        self._over = (
            (self.max_util_pct > 0 and measured["util_pct"] > self.max_util_pct)
            or (self.max_read_await_ms > 0
                and measured["read_await_ms"] > self.max_read_await_ms)
            or (self.max_backlog_ms > 0
                and measured["backlog_ms"] > self.max_backlog_ms))

    def _verdict(self) -> bool:
        """Is the pool over threshold?  Resamples at most every ``sample_s``."""

        now = self.clock()
        with self._lock:
            if now - self._sampled_at >= self.sample_s:
                self._refresh_locked(now)
            return self._over

    # -- the call the reader makes ----------------------------------------

    def wait(self, stop: threading.Event | None = None) -> None:
        """Block until the pool is under threshold, or ``stop`` is set."""

        if not self.active:
            return
        if not self._verdict():
            return
        self._enter_hold()
        try:
            while self._verdict():
                if stop is not None and stop.is_set():
                    return
                self.sleep(self.hold_s)
        finally:
            self._leave_hold()

    def _enter_hold(self) -> None:
        with self._lock:
            self._holds += 1
            first = self._holding == 0
            if first:
                self._hold_started = self.clock()
            self._holding += 1
            last = dict(self._last)
        if first and self.notify is not None:
            self.notify({"event": "prewarm-hold", "state": "start", **last})

    def _leave_hold(self) -> None:
        with self._lock:
            self._holding -= 1
            done = self._holding == 0
            if done:
                self._held_s += self.clock() - self._hold_started
            held = self._held_s
            last = dict(self._last)
        if done and self.notify is not None:
            self.notify({"event": "prewarm-hold", "state": "end",
                         "held_seconds_total": round(held, 3), **last})

    # -- what it saw ------------------------------------------------------

    def report(self) -> dict[str, object]:
        """The sampled numbers, for the prewarm record.

        Wall seconds *any* reader was held, not the sum over readers: eight
        threads holding one second together cost the pool one second, and a
        sum would report eight.
        """

        with self._lock:
            samples = self._samples
            mean = {key: (self._totals[key] / samples if samples else 0.0)
                    for key in self._totals}
            held = self._held_s
            if self._holding:
                held += self.clock() - self._hold_started
            return {
                "active": self.active,
                "devices": list(self.devices),
                "reason": self.reason,
                "samples": samples,
                "holds": self._holds,
                "held_seconds": round(held, 3),
                "max_util_pct": round(self._maxima["util_pct"], 1),
                "mean_util_pct": round(mean["util_pct"], 1),
                "max_read_await_ms": round(self._maxima["read_await_ms"], 2),
                "mean_read_await_ms": round(mean["read_await_ms"], 2),
                "max_backlog_ms": round(self._maxima["backlog_ms"], 1),
                "mean_backlog_ms": round(mean["backlog_ms"], 1),
                "thresholds": {
                    "max_util_pct": self.max_util_pct,
                    "max_read_await_ms": self.max_read_await_ms,
                    "max_backlog_ms": self.max_backlog_ms,
                },
            }


def pacer_from_args(args) -> DiskPacer:
    """Build the pacer a cycle will use, from arguments and the pool topology.

    A host with no pool, no ``zpool`` or an unreadable ``stat`` file gets an
    inactive pacer that reads at full speed and says so in the record, rather
    than a refusal: the storage role is the only place this loop belongs, and
    the tests run where none of those files exist.
    """

    devices = [name for name in
               (part.strip() for part in (getattr(args, "disks", "") or "").split(","))
               if name]
    reason = "from --disks" if devices else ""
    if not devices and getattr(args, "pace_pool", None):
        devices = pool_member_devices(str(args.pace_pool))
        reason = (f"from zpool status -P {args.pace_pool}" if devices
                  else f"pool {args.pace_pool} has no readable data vdev members")
    return DiskPacer(
        devices,
        max_util_pct=args.max_util_pct,
        max_read_await_ms=args.max_read_await_ms,
        max_backlog_ms=args.max_backlog_ms,
        sample_s=args.pace_sample_s,
        hold_s=args.pace_hold_s,
        reason=reason,
    )


def inactive_pacing(reason: str = "no pacer") -> dict[str, object]:
    """A pacing report of the same shape from a reader that had no pacer.

    Same keys, so a reader of the records never has to branch on whether the
    field is present -- "pacing was off" is a value, not a missing key.
    """

    return DiskPacer([], max_util_pct=0.0, max_read_await_ms=0.0,
                     max_backlog_ms=0.0, reason=reason).report()


# ---------------------------------------------------------------- reading


class Reader:
    """Bounded parallel sequential reads, in the order the manifest lists them.

    Order matters twice: it is the order the action itself will consume the
    bytes, so a partial warm is a useful prefix rather than a random subset,
    and it is the order the files were written, so the disks see something
    close to a sequential sweep.
    """

    def __init__(self, readers: int, mounts: MountMap, block: int = BLOCK,
                 pacer: DiskPacer | None = None) -> None:
        self.readers = max(1, readers)
        self.mounts = mounts
        self.block = block
        #: Consulted before every block.  Concurrency bounds how many reads are
        #: outstanding; only the pacer bounds what they cost the pool.
        self.pacer = pacer

    def read(
        self,
        entries: list[dict[str, object]],
        *,
        budget_bytes: int,
        stop: threading.Event,
        recheck: Callable[[], int] | None = None,
    ) -> dict[str, object]:
        work: "queuelib.Queue[dict[str, object] | None]" = queuelib.Queue()
        for entry in entries:
            work.put(entry)
        for _ in range(self.readers):
            work.put(None)
        lock = threading.Lock()
        state = {"bytes": 0, "entries": 0, "budget": int(budget_bytes)}
        errors: list[str] = []

        def worker() -> None:
            buffer = bytearray(self.block)
            view = memoryview(buffer)
            while not stop.is_set():
                job = work.get()
                if job is None:
                    return
                with lock:
                    if state["bytes"] >= state["budget"]:
                        return
                path = self.mounts.local(str(job["path"]))
                want_total = int(job["bytes"])
                offset = int(job["offset"])
                got = 0
                try:
                    # O_NOFOLLOW at the leaf, and a regular-file check: the
                    # manifest is submitter-supplied, so a symlink swapped in
                    # after validation must fail here rather than send this
                    # loop reading something outside the mount it approved.
                    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
                except OSError as exc:
                    with lock:
                        if len(errors) < 20:
                            errors.append(f"{os.path.basename(path)}: {exc}")
                    continue
                try:
                    if not statmod.S_ISREG(os.fstat(fd).st_mode):
                        raise OSError(f"not a regular file: {path}")
                    if offset:
                        os.lseek(fd, offset, os.SEEK_SET)
                    remaining = want_total
                    while remaining > 0 and not stop.is_set():
                        if self.pacer is not None:
                            self.pacer.wait(stop)
                            if stop.is_set():
                                break
                        chunk = os.readv(fd, [view[: min(self.block, remaining)]])
                        if not chunk:
                            break
                        got += chunk
                        remaining -= chunk
                        with lock:
                            state["bytes"] += chunk
                            if state["bytes"] >= state["budget"]:
                                break
                except OSError as exc:
                    with lock:
                        if len(errors) < 20:
                            errors.append(f"{os.path.basename(path)}: {exc}")
                finally:
                    os.close(fd)
                with lock:
                    state["entries"] += 1
                    if recheck is not None and state["entries"] % 64 == 0:
                        room = recheck()
                        if room <= 0:
                            state["budget"] = state["bytes"]

        started = time.time()
        threads = [threading.Thread(target=worker, daemon=True)
                   for _ in range(self.readers)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        elapsed = max(1e-9, time.time() - started)
        return {
            "bytes_warmed": state["bytes"],
            "entries_warmed": state["entries"],
            "seconds": round(elapsed, 3),
            "mb_per_s": round(state["bytes"] / 1e6 / elapsed, 1),
            "readers": self.readers,
            # Beside ``mb_per_s`` on purpose: a rate alone hid the cost that
            # made #499, and a receipt that reports only how fast the warm was
            # cannot answer whether it was worth what the fleet paid for it.
            "disk_pacing": (self.pacer.report() if self.pacer is not None
                            else inactive_pacing()),
            "errors": errors,
        }


# ------------------------------------------------------------- queue view


def sealed_request(cas_root: Path, action_key: str) -> dict | None:
    path = Path(cas_root) / "requests" / action_key[:2] / f"{action_key}.json"
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return None


def manifest_input_of(request: dict | None) -> dict | None:
    if not isinstance(request, dict):
        return None
    for entry in request.get("inputs") or ():
        if isinstance(entry, dict) and \
                entry.get("id") == pb.PBCAMPAIGN_DATA_MANIFEST_INPUT_ID:
            return entry
    return None


def declared_manifest_bytes(request: dict | None) -> int:
    """What the submitter said the manifest totals, without fetching it.

    ``pbrun`` seals this summary into ``params`` precisely so the headroom
    arithmetic -- which runs for every claimed action on every poll -- never
    has to open and hash a blob.
    """

    if not isinstance(request, dict):
        return 0
    summary = (request.get("params") or {}).get("data_manifest")
    if isinstance(summary, dict):
        value = summary.get("total_bytes")
        if isinstance(value, int) and value >= 0:
            return value
    return 0


def manifest_row_id(manifest: dict) -> str:
    """The submitter's own name for this row, for a human reading the record.

    Everything else about the manifest is one lookup from ``manifest_sha256``,
    which is why the record does not copy the rest of the annotations.  The
    label is the exception because the question asked of a prewarm record --
    "which row was this, and was it still resident when a worker claimed it?"
    -- is asked by people and by campaign tooling that do not have the blob.
    """

    annotations = manifest.get("annotations")
    if not isinstance(annotations, dict):
        return ""
    value = annotations.get("row_id")
    if isinstance(value, (str, int)):
        return str(value)[:200]
    return ""


def load_manifest(cas_root: Path, entry: dict) -> dict | None:
    """Fetch and validate the manifest blob the input row addresses.

    ``input_path`` re-hashes the blob, so a manifest that does not match the
    digest the action key covers is refused here rather than read from.
    """

    try:
        path = pb.PrismaBuildCAS(Path(cas_root)).input_path(entry)
        return pb.load_data_manifest(path)
    except (pb.PrismaBuildError, OSError, ValueError):
        return None


def claimed_reserve(queue: pool.PoolQueue, cas_root: Path, grace_s: float) -> dict:
    """Bytes a claimed action may still be reading, and must not be displaced.

    An action reads its inputs at the start of its run, so a claim older than
    the grace window is past its load phase and its bytes are no longer worth
    reserving.  The window is configuration, not a guess about any one
    campaign: set it to the load phase you measured.
    """

    now = time.time()
    reserved = 0
    keys: list[str] = []
    for path in sorted((queue.root / pool.CLAIMED).glob("*.json")):
        try:
            record = json.loads(path.read_text())
        except (OSError, ValueError):
            continue
        if not isinstance(record, dict):
            continue
        claimed_unix = record.get("claimed_unix")
        if not isinstance(claimed_unix, (int, float)):
            continue
        if now - float(claimed_unix) > grace_s:
            continue
        key = str(record.get("action_key", path.stem))
        size = declared_manifest_bytes(sealed_request(cas_root, key))
        if size:
            reserved += size
            keys.append(key)
    return {"claimed_reserved_bytes": reserved, "claimed_reserved_keys": keys}


def already_warm(queue: pool.PoolQueue, action_key: str, sha256: str) -> bool:
    record = queue.prewarm(action_key)
    return bool(
        record
        and record.get("manifest_sha256") == sha256
        and record.get("status") == "complete"
    )


def warmed_reserve(queue: pool.PoolQueue, ready: list[dict]) -> dict:
    """Bytes this loop already made resident for rows nobody has claimed yet.

    Without this the loop would spend its whole budget on the head of the
    queue, then spend it again on the next row by displacing the first --
    warming two rows and arriving with neither.  A row stays protected until
    it is claimed, at which point ``claimed_reserve`` takes over and the
    grace window retires it.
    """

    reserved = 0
    keys: list[str] = []
    for item in ready:
        record = queue.prewarm(str(item.get("action_key", "")))
        if record and record.get("status") == "complete":
            reserved += int(record.get("bytes_warmed", 0))
            keys.append(str(record.get("action_key")))
    return {"warmed_reserved_bytes": reserved, "warmed_reserved_keys": keys}


# ------------------------------------------------------------------ loop


def cycle(args, queue: pool.PoolQueue, mounts: MountMap, stop: threading.Event,
          pacer: DiskPacer | None = None) -> dict:
    ready = queue.ready_items()
    # One pacer per cycle, shared by every reader thread of every row warmed in
    # it: the disks are one queue, and a per-thread pacer would let N threads
    # each drive the pool to the threshold.
    pacer = pacer if pacer is not None else pacer_from_args(args)
    room = arc_headroom(args.arc_reserve_fraction, args.arcstats)
    cas_root = Path(args.cas_root) if args.cas_root else None
    reserve = claimed_reserve(
        queue,
        cas_root or Path(pool.DEFAULT_POOL_ROOT).parent / "cas",
        args.claim_grace_min * 60.0,
    )
    reserve.update(warmed_reserve(queue, ready))
    protected = (reserve["claimed_reserved_bytes"]
                 + reserve["warmed_reserved_bytes"])
    budget = max(0, room["capacity_budget"] - protected)
    event = {
        "event": "cycle",
        "unix": round(time.time(), 3),
        "host": socket.gethostname(),
        "ready": len(ready),
        "pacing_active": pacer.active,
        "pacing_devices": list(pacer.devices),
        **room,
        **reserve,
        "protected_bytes": protected,
        "budget_after_reserve": budget,
        "warmed": [],
        "skipped": [],
    }

    taken = 0
    for item in ready:
        if taken >= args.lookahead or stop.is_set():
            break
        key = str(item.get("action_key", ""))
        root = Path(item.get("cas_root") or cas_root or "")
        if not key or not root.name:
            continue
        request = sealed_request(root, key)
        entry = manifest_input_of(request)
        if entry is None:
            continue
        manifest = load_manifest(root, entry)
        if manifest is None:
            event["skipped"].append({"action_key": key, "reason": "unreadable manifest"})
            continue
        total = int(manifest["total_bytes"])
        if total < args.min_manifest_bytes:
            # A trivial action is not worth a warm window. On the GLM census
            # only 42 of 132 rows are 864-unit expert rows; the other 90 carry
            # one or two units and load in about 0.3 s, so warming one spends
            # the lookahead on a row that was never going to wait for a disk.
            # Skipping without consuming ``taken`` keeps a run of trivial rows
            # from disabling prewarm for the real row behind them.
            event["skipped"].append({
                "action_key": key, "reason": "below the warm threshold",
                "manifest_bytes": total,
                "min_manifest_bytes": args.min_manifest_bytes,
            })
            continue
        taken += 1
        digest = str(entry["sha256"])
        if already_warm(queue, key, digest):
            event["skipped"].append({"action_key": key, "reason": "already warm"})
            continue
        if total > budget:
            # Refusing is the correct outcome, not a failure: warming a row
            # that does not fit would evict the row that is running to make
            # room for one that is not.
            event["skipped"].append({
                "action_key": key, "reason": "headroom",
                "manifest_bytes": total, "budget": budget,
            })
            continue
        started = time.time()
        if args.dry_run:
            result = {"bytes_warmed": 0, "entries_warmed": 0, "seconds": 0.0,
                      "mb_per_s": 0.0, "readers": args.readers,
                      "disk_pacing": pacer.report(), "errors": []}
        else:
            reader = Reader(args.readers, mounts, pacer=pacer)
            result = reader.read(
                list(manifest["entries"]),
                budget_bytes=budget,
                stop=stop,
                # Re-read rather than trust the cycle's opening number: the
                # ceiling can be lowered under a running warm, and a row that
                # started inside its budget must stop when it leaves it.
                recheck=lambda: arc_headroom(
                    args.arc_reserve_fraction,
                    args.arcstats)["capacity_budget"] - protected,
            )
        after = arc_headroom(args.arc_reserve_fraction, args.arcstats)
        finished = time.time()
        record = {
            "action_key": key,
            # The row the campaign calls this, beside the key PrismaBuild
            # calls it: a receipt nobody can place is a receipt nobody reads.
            "row_id": manifest_row_id(manifest),
            "host": socket.gethostname(),
            "manifest_sha256": digest,
            "manifest_bytes": total,
            "entry_count": int(manifest["entry_count"]),
            "mount_prefix": manifest["mount_prefix"],
            "started_unix": round(started, 3),
            "finished_unix": round(finished, 3),
            # The same two instants in a form a person can line up against a
            # journal, a Netdata window or a claim record without converting
            # anything.  The unix fields stay authoritative.
            "started_utc": utc(started),
            "finished_utc": utc(finished),
            "status": (
                "dry-run" if args.dry_run
                else "complete" if result["bytes_warmed"] >= total
                else "partial"
            ),
            "arc_before": {k: room[k] for k in
                           ("arc_size", "arc_c", "arc_c_max",
                            "headroom_nominal", "headroom_effective")},
            "arc_after": {k: after[k] for k in
                          ("arc_size", "arc_c", "arc_c_max",
                           "headroom_nominal", "headroom_effective")},
            **result,
        }
        if not args.dry_run:
            queue.record_prewarm(key, record)
        # A dry run reads nothing, so ``bytes_warmed`` is 0 and the budget
        # would survive the row untouched: every later row in the same
        # lookahead is then priced against a budget the plan has already spent,
        # and the plan claims to warm more than the ARC can hold.  Charge what
        # the read would have cost.
        budget = max(0, budget - (total if args.dry_run
                                  else result["bytes_warmed"]))
        event["warmed"].append({k: record[k] for k in
                                ("action_key", "status", "manifest_bytes",
                                 "bytes_warmed", "seconds", "mb_per_s",
                                 "disk_pacing")})
    return event


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--pool-root", default=str(SH / "pb-queue"),
                        help="the queue whose ready list to read")
    parser.add_argument("--cas-root", default=str(SH / "cas"),
                        help="fallback CAS root; each ready item names its own")
    parser.add_argument("--mount-map", action="append", required=True,
                        help="SHARED=LOCAL, repeatable: rewrite a manifest's "
                             "shared-mount path onto this host's local mount "
                             "(e.g. /mnt/shared=/storage_pool/shared). "
                             "Required and never defaulted -- which mount a "
                             "box serves is host configuration, and a wrong "
                             "guess reads the network instead of the disks")
    parser.add_argument("--readers", type=int, default=1,
                        help="parallel readers.  8 measured 391.9 MB/s against "
                             "1's 298.9 on the GLM census pool, and cost the "
                             "fleet 100-130 s of both Sparks' GPU time per row "
                             "(#499): 30%% more read rate for a transport "
                             "reset on every client.  1 is what was measured "
                             "to pass on the live fleet -- 63.8 GB at "
                             "146.5 MB/s while both Sparks kept encoding, "
                             "disks at 23%% mean and 41.7%% peak.  One reader "
                             "is already enough to saturate the pool: ZFS "
                             "prefetch issued 98%% of the run's disk reads, "
                             "so a second reader adds queue depth the pacer "
                             "then has to take back")
    parser.add_argument("--lookahead", type=int, default=1,
                        help="how many ready actions ahead to warm.  The ARC "
                             "must hold the running rows as well: on dl380g10 "
                             "c_max is 245 760 MiB (257.7 GB), two live 64 GB "
                             "rows and one "
                             "64 GB lookahead row come to 191 GB against a "
                             "206 GB budget at the default reserve fraction, "
                             "and a second lookahead row does not fit -- it is "
                             "bought by evicting what the running rows read")
    parser.add_argument("--pace-pool", default="storage_pool",
                        help="the ZFS pool whose data vdev members pace the "
                             "reader; its spindles are discovered with "
                             "'zpool status -P'.  A host where that fails "
                             "reads unpaced and records that it did")
    parser.add_argument("--disks", default="",
                        help="comma-separated block devices (sdb,sdc) to pace "
                             "on, overriding --pace-pool discovery")
    parser.add_argument("--max-util-pct", type=float, default=25.0,
                        help="hold while any pool disk is busier than this.  "
                             "Measured on dl380g10 (#499): 8-12%% while the "
                             "campaign alone read, 73-83%% under the 8-reader "
                             "warm that reset every client's RDMA transport.  "
                             "40 was tried first and left the disks at 31%% "
                             "peak but stalled both Sparks' NFS clients "
                             "(their RPC rate fell to ~50/s and their own "
                             "metrics collectors missed samples); 25 held the "
                             "whole fleet inside every criterion.  0 disables")
    parser.add_argument("--max-read-await-ms", type=float, default=10.0,
                        help="hold while any pool disk's mean read service "
                             "time over the last sample exceeds this.  "
                             "Measured: ~0-2 ms harmless, 38-54 ms stalling; "
                             "10 is what the passing run used.  0 disables")
    parser.add_argument("--max-backlog-ms", type=float, default=2000.0,
                        help="hold while any pool disk's queue backlog "
                             "exceeds this.  Measured: 300-450 ms harmless, "
                             "11 000-14 400 ms stalling -- the client sync "
                             "writes queued behind that backlog are what "
                             "passed the RDMA timeout.  2 000 is what the "
                             "passing run used; it kept the 15 s backlog "
                             "mean at 1.4 s.  0 disables")
    parser.add_argument("--pace-sample-s", type=float, default=0.25,
                        help="minimum seconds between /sys/block reads; every "
                             "block's check reads the cached verdict.  The "
                             "burst a wake issues is set by ZFS prefetch, not "
                             "by the reader, so a shorter sample is what "
                             "shortens the burst")
    parser.add_argument("--pace-hold-s", type=float, default=0.25,
                        help="seconds to wait between rechecks while held")
    parser.add_argument("--min-manifest-bytes", type=int, default=1 << 30,
                        help="a manifest smaller than this is passed over "
                             "without consuming the lookahead: a row that "
                             "loads in well under a second cannot be made "
                             "faster by warming it, and spending the window "
                             "on one costs the row behind it")
    parser.add_argument("--poll-s", type=float, default=10.0,
                        help="seconds between polls of the ready list")
    parser.add_argument("--arc-reserve-fraction", type=float, default=0.8,
                        help="fraction of the ARC ceiling c_max this loop may "
                             "hold at once, the rest left as slack for every "
                             "other reader of the box")
    parser.add_argument("--arcstats", default=ARCSTATS,
                        help="where the ARC counters live; a host without "
                             "this file reports zero headroom and warms "
                             "nothing, which is the right answer there")
    parser.add_argument("--claim-grace-min", type=float, default=20.0,
                        help="a claim younger than this still counts its "
                             "manifest bytes against the prewarm budget")
    parser.add_argument("--once", action="store_true",
                        help="run one cycle and exit")
    parser.add_argument("--dry-run", action="store_true",
                        help="decide and report; read nothing, write nothing")
    parser.add_argument("--log", default=None,
                        help="append one JSON object per cycle here")
    args = parser.parse_args(argv)

    mounts = MountMap(list(args.mount_map))
    if not args.dry_run and not mounts.usable():
        raise SystemExit(
            "prewarm: no local mount from --mount-map exists on this host; "
            "this loop belongs on the storage host only")
    queue = pool.PoolQueue(Path(args.pool_root))
    stop = threading.Event()

    def announce(payload: dict[str, object]) -> None:
        print(json.dumps({**payload, "unix": round(time.time(), 3)}),
              file=sys.stderr, flush=True)

    probe = pacer_from_args(args)
    if not args.dry_run and not probe.active:
        raise SystemExit(
            "prewarm: this is the storage host and no disks could be paced "
            f"({probe.report()['reason']}); reading the pool unpaced is the "
            "defect #499 records, so refuse rather than degrade.  Name the "
            "members with --disks if discovery cannot see them.")

    while True:
        pacer = pacer_from_args(args)
        pacer.notify = announce
        event = cycle(args, queue, mounts, stop, pacer=pacer)
        line = json.dumps(event)
        print(line, flush=True)
        if args.log:
            with open(args.log, "a") as handle:
                handle.write(line + "\n")
        if args.once:
            return 0
        time.sleep(args.poll_s)


if __name__ == "__main__":
    raise SystemExit(main())
