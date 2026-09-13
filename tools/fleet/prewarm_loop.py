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

Windows, and following the reader
---------------------------------
A manifest larger than the budget used to be refused whole, which meant the
loop could not help the jobs that need it most: the GLM joint pass reads about
4.75 TB layer-major against an ARC ceiling of 257.7 GB.  When the producer
writes ``annotations.phases`` -- a running byte sum over ``entries`` in the
order the action reads them -- the loop warms through the last entry boundary
that fits, including one inside an oversized phase, and extends the window on
later polls.  Phases remain the accepted-progress frontiers that release
reserve.  What has to fit in the cache is the distance between what the action
has read and what the loop has made resident, never the manifest.

Where the action reports it, the loop reads that distance rather than guessing
it.  An action declaring ``params.progress`` writes ``progress-v1`` records to
``claimed/<key>.progress``; the phases it has finished are read, so they leave
the reserve, and the next row becomes warmable while the running one is still
working instead of when it is claimed.  #523 measured what the claim-shaped
trigger costs: the warm started 4.3 s before the row's own claim and overlapped
the row's own reads at 256.7 MB/s, so the client saw no speedup at all.  An
action that declares no progress policy keeps the claim-plus-grace behaviour,
unchanged.

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
whenever a client is reading *and* the worst disk is over its service-time or
backlog cap.  Both halves are required.  On 2026-09-13 a ``zpool scrub`` drove
sdb to 88% utilization and 100% busy samples, the pacer held for 5709 s
cumulative, and no NFS client read a byte for the whole window: the loop paid
for a busy pool and protected nobody.  Utilization is still measured and still
recorded -- it is the number an operator compares against Netdata -- but it no
longer holds on its own.  A thread count cannot do this --
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
budgetary: it never *asks* for more than ``c_max * reserve`` minus the manifest
bytes of every action claimed inside the last ``--claim-grace-min`` minutes,
which is the window in which a claimed action is still reading, and minus what
it has already warmed for rows nobody has claimed.  The budget is a share of
the cache's *capacity*, not of the free space inside it, because a cache in
steady state is full; ``arc_headroom`` carries the measurement that settled
that.  Both headroom numbers are logged every cycle so the gap between the
budget and the outcome stays visible.
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
from collections.abc import Callable, Mapping
from pathlib import Path

# The publisher writes this program twice, ``tools/fleet/prewarm_loop.py`` and
# a flattened ``tools/prewarm_loop.py``, and ``supervise._spawn_role`` runs the
# flat one.  ``parents[2]`` is the repository root under the first spelling and
# the parent of the generation store under the second, so the generation must
# be resolved by the rule that knows both layouts (the rule ``pbtest.py`` uses).
sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
from runtime_paths import generation_root  # noqa: E402
import worker_loop as runtime_gate  # noqa: E402

sys.path.insert(0, str(generation_root(__file__) / "src"))

import prismabuild.core as pb  # noqa: E402
from prismabuild import pool  # noqa: E402
from prismabuild import progress as progress_v1  # noqa: E402

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
#: Where the kernel's own NFS server says how much it has served.  The ``io``
#: line is ``io <read_bytes> <write_bytes>``, counted for every client of this
#: host.  The prewarm reader reads the pool through the host's local path, so
#: its own bytes never reach this counter and the measurement cannot chase
#: itself.
NFSD_IO = "/proc/net/rpc/nfsd"
#: A hold ends at half the number that started it.  A disk sitting exactly on
#: a cap otherwise flaps the reader once per sample, and those bursts are what
#: the pacer exists to smooth.  This is a property of the control loop rather
#: than of any pool, which is why it is a constant and not an argument.
HOLD_RELEASE_FRACTION = 0.5
#: Default for ``--client-active-mb-s``.  A client that has read nothing this
#: host served is a client no hold protects.  The threshold has to sit *below*
#: the slowest read worth protecting, because a client already slowed by the
#: warm would otherwise read as idle and release the very hold that would give
#: it the disks back.  The slowest measured client read is #523's 26 MB/s
#: cold-start row; a tenth of it is comfortably under that and still an order
#: of magnitude above the few hundred kB/s of attribute and directory traffic
#: an idle mount produces.
CLIENT_ACTIVE_MB_S = 2.6
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
        if not device:
            # A partial topology is not a smaller healthy pool.  Letting the
            # reader pace only the resolvable members would let an unavailable
            # busy vdev disappear behind its quiet peer.
            return []
        if device not in devices:
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


class ClientReadRate:
    """How fast this host's NFS server is feeding its clients, in MB/s.

    The pacer exists to protect those clients (#499), so the first question a
    hold has to answer is whether anybody is reading.  A pool busy with work
    that has no client behind it -- a scrub, a resilver, another tenant's
    write -- is not a reason to stop warming: on 2026-09-13 a ``zpool scrub``
    held the storage role for 5709 s at 100 % utilization and up to 26 s of
    backlog while no client read a byte, so the loop warmed nothing for hours
    and protected nobody.

    ``/proc/net/rpc/nfsd``'s ``io`` line counts bytes ``nfsd`` served.  This
    loop reads the pool through the host's own local path, never through the
    export, so its reads never appear in this counter and cannot make the
    pacer believe a client is busy.

    An unreadable counter is ``None``, and ``None`` means "clients may be
    reading": a host that cannot see its clients must behave like the host
    that can, not like an empty one.
    """

    def __init__(self, path: str = NFSD_IO,
                 clock: Callable[[], float] = time.monotonic,
                 reader: Callable[[], str | None] | None = None) -> None:
        self.path = path
        self.clock = clock
        self.reader = reader if reader is not None else self._read_file
        self._previous: int | None = None
        self._previous_at = 0.0
        self._rate: float | None = None

    def _read_file(self) -> str | None:
        try:
            with open(self.path) as handle:
                return handle.read()
        except OSError:
            return None

    def served_bytes(self) -> int | None:
        text = self.reader()
        if not text:
            return None
        for line in text.splitlines():
            fields = line.split()
            if len(fields) >= 2 and fields[0] == "io":
                try:
                    return int(fields[1])
                except ValueError:
                    return None
        return None

    def sample(self) -> float | None:
        """MB/s served since the last sample, or ``None`` without an interval."""

        total = self.served_bytes()
        now = self.clock()
        if total is None:
            self._previous, self._previous_at, self._rate = None, now, None
            return None
        previous, previous_at = self._previous, self._previous_at
        elapsed = now - previous_at
        if previous is None:
            self._previous, self._previous_at = total, now
            return None
        if elapsed <= 0:
            # Two reads inside one instant are one read.  The last rate is
            # still what is known; "unknown" stays reserved for a counter
            # this host cannot read at all.
            return self._rate
        self._previous, self._previous_at = total, now
        if total < previous:
            # A counter that went backwards is a wrap or a restarted server,
            # and neither is a rate.  Report unknown rather than a negative
            # one, and let the next interval answer.
            self._rate = None
            return None
        self._rate = (total - previous) / 1e6 / elapsed
        return self._rate


class HoldLedger:
    """Hold time for the life of the role, across every cycle's pacer.

    ``main`` builds a pacer per cycle so a topology change is picked up, which
    means a counter living on the pacer answers "this cycle" when the operator
    asked "since the role started".  The ledger is the one piece of pacing
    state that deliberately outlives a cycle, and it is what makes
    ``held_seconds_total`` a total.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.held_s = 0.0
        self.idle_s = 0.0
        self.active_s = 0.0
        self.holds = 0
        self.samples = 0

    def count_sample(self) -> None:
        with self._lock:
            self.samples += 1

    def count_hold(self) -> None:
        with self._lock:
            self.holds += 1

    def add(self, seconds: float, *, clients_active: bool) -> None:
        if seconds <= 0:
            return
        with self._lock:
            self.held_s += seconds
            if clients_active:
                self.active_s += seconds
            else:
                self.idle_s += seconds


class DiskPacer:
    """Hold the reader while a client is reading and the pool is hurting.

    A hold costs the campaign a warm and buys a client faster reads, so it is
    worth paying only when there is a client to pay it to.  The verdict is
    therefore an AND: hold while clients are reading this host's exports *and*
    the pool's worst disk is over the service-time or backlog cap.

    The three numbers are the ones Netdata charts, computed the same way, so a
    record written here and the ``disk_util``/``disk_await``/``disk_backlog``
    series an operator reads afterwards are the same quantities:

    Utilization is measured and recorded but never holds on its own.  It
    answers "is the disk busy", and busy is not the same question: a scrub
    pins every spindle at 100 % for hours with 9-20 ms await and no client
    behind it, and the 2026-09-13 storage role held on exactly that for
    5709 s.  What a client feels is service time and backlog, so those are
    what the reader is held for.  ``--max-util-pct`` is still accepted and
    still recorded, so an operator's existing command line keeps working and
    the number stays in the receipt.

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

    A pacer with no configured disks is intentionally inactive: that is the
    explicit non-storage-host/test shape.  Once disks are configured, however,
    their telemetry is required.  The first complete row establishes a
    baseline; a second complete row establishes the first verdict.  Missing
    even one configured member is not a quiet pool, so it holds the reader
    until a fresh complete interval arrives or the caller stops it.  That
    avoids turning a missing sysfs row or changed topology into #499's
    unpaced storage read.
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
        client_rate: ClientReadRate | None = None,
        client_active_mb_s: float = 0.0,
        ledger: "HoldLedger | None" = None,
    ) -> None:
        # ``--disks`` is a comma-separated operator argument, so preserve the
        # first spelling/order but do not make a duplicate member look like a
        # second required stat row.
        self.devices = list(dict.fromkeys(devices))
        self.max_util_pct = float(max_util_pct)
        self.max_read_await_ms = float(max_read_await_ms)
        self.max_backlog_ms = float(max_backlog_ms)
        self.sample_s = max(0.0, float(sample_s))
        self.hold_s = max(0.001, float(hold_s))
        self.stat_source = stat_source
        self.clock = clock
        self.sleep = sleep
        self.reason = reason or ("" if self.devices else "no pool devices")
        #: Without a source the clients are treated as reading.  A pacer that
        #: cannot see them must behave like one on a busy server, never like
        #: one on an idle server.
        self.client_rate = client_rate
        self.client_active_mb_s = max(0.0, float(client_active_mb_s))
        #: Lifetime hold accounting, shared with the pacers of later cycles.
        self.ledger = ledger if ledger is not None else HoldLedger()
        #: Called when a hold starts and when it ends.  A loop that goes quiet
        #: for minutes because the campaign itself is saturating the spindles
        #: is behaving correctly, and an operator must be able to see that
        #: rather than infer it from a warm that never finished.
        self.notify = notify
        self._lock = threading.Lock()
        self._previous: dict[str, list[int]] = {}
        self._previous_at = 0.0
        # ``None`` makes the first read unconditional.  A valid injected
        # monotonic clock may begin at zero, where a numeric zero sentinel
        # would otherwise skip the baseline for one sample interval.
        self._sampled_at: float | None = None
        self._over = False
        self._readable = True
        self._telemetry_complete = False
        self._missing_devices: list[str] = []
        self._telemetry_gaps = 0
        self._samples = 0
        self._holds = 0
        self._held_s = 0.0
        self._holding = 0
        self._hold_started = 0.0
        self._clients_active = True
        self._client_mb_s: float | None = None
        #: Start of the stretch of hold time not yet charged to a bucket, and
        #: the client state it is charged to.
        self._slice_at = 0.0
        self._slice_active = True
        self._totals = {"util_pct": 0.0, "read_await_ms": 0.0, "backlog_ms": 0.0}
        self._maxima = {"util_pct": 0.0, "read_await_ms": 0.0, "backlog_ms": 0.0}
        self._last: dict[str, float] = {}

    @property
    def active(self) -> bool:
        return bool(self.devices)

    def retarget(self, devices: list[str], reason: str = "") -> None:
        """Adopt a freshly discovered topology without losing the totals.

        The role rediscovers its pool every cycle, because a vdev can be
        replaced under a running loop.  Rebuilding the pacer for that would
        reset ``held_seconds_total`` every poll, so the pacer is kept and only
        its members change; a changed member list drops the baseline, since a
        stat row from a device that is no longer configured is not an interval.
        """

        members = list(dict.fromkeys(devices))
        with self._lock:
            if members == self.devices:
                if reason:
                    self.reason = reason
                return
            self.devices = members
            self.reason = reason or ("" if members else "no pool devices")
            self._previous, self._previous_at = {}, 0.0
            self._sampled_at = None
            self._telemetry_complete = False
            self._missing_devices = []

    def _telemetry_state_locked(self) -> str:
        if not self.devices:
            return "inactive"
        if self._missing_devices:
            return "missing"
        if not self._telemetry_complete:
            return "awaiting_interval"
        return "complete"

    def begin_row(self) -> None:
        """Start bounded per-row accounting without resetting the verdict.

        Rows are warmed sequentially, but share the same disk decision state:
        the previous stat row, cached verdict, completeness state, and an
        existing hold all survive the receipt boundary.  Only the counters
        shown on a row's receipt reset.  If a caller ever begins a row while a
        hold is active, that hold remains active and its future time belongs to
        the new row instead of being cleared.
        """

        with self._lock:
            carrying_hold = self._holding > 0
            self._samples = 0
            self._holds = 1 if carrying_hold else 0
            self._held_s = 0.0
            if carrying_hold:
                self._hold_started = self.clock()
            self._telemetry_gaps = 0
            for key in self._totals:
                self._totals[key] = 0.0
                self._maxima[key] = 0.0

    # -- sampling ---------------------------------------------------------

    def _measure(self, now: float) -> dict[str, float] | None:
        """One interval's worst-disk numbers, or ``None`` if there is no interval."""

        current: dict[str, list[int]] = {}
        for device in self.devices:
            row = self.stat_source(device)
            if row is not None and len(row) > STAT_WEIGHTED_IO_MS:
                current[device] = row
        missing = [device for device in self.devices if device not in current]
        #: A partial sample cannot borrow a quiet peer's verdict.  It also
        #: cannot reuse the pre-gap baseline after a member returns: the first
        #: restored row is a baseline, not an interval that proves the pool
        #: healthy.  The hold that follows is interruptible in ``wait`` and
        #: announced with the missing members.
        was_missing = bool(self._missing_devices)
        self._readable = not missing
        self._missing_devices = missing
        previous, previous_at = self._previous, self._previous_at
        if missing:
            if not was_missing:
                self._telemetry_gaps += 1
            self._previous, self._previous_at = {}, now
            self._telemetry_complete = False
            return None
        self._previous, self._previous_at = current, now
        elapsed = now - previous_at
        if len(previous) != len(self.devices) or elapsed <= 0:
            self._telemetry_complete = False
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
        if not seen:
            self._telemetry_complete = False
            return None
        self._telemetry_complete = True
        return worst

    def _sample_clients_locked(self) -> None:
        if self.client_rate is None:
            self._client_mb_s = None
            self._clients_active = True
            return
        rate = self.client_rate.sample()
        self._client_mb_s = rate
        # Unknown is "active": an unreadable counter must not be the thing
        # that authorizes an unpaced read while a client waits.
        self._clients_active = rate is None or rate > self.client_active_mb_s

    def _pool_is_hurting(self, measured: dict[str, float]) -> bool:
        """Service time or backlog over the cap, with hysteresis on the way out.

        Utilization is deliberately absent: it is recorded, and it never holds.
        """

        scale = HOLD_RELEASE_FRACTION if self._over else 1.0
        return bool(
            (self.max_read_await_ms > 0
             and measured["read_await_ms"] > self.max_read_await_ms * scale)
            or (self.max_backlog_ms > 0
                and measured["backlog_ms"] > self.max_backlog_ms * scale))

    def _refresh_locked(self, now: float) -> None:
        measured = self._measure(now)
        self._sampled_at = now
        self._sample_clients_locked()
        if measured is None:
            if self.active:
                # Configured storage members are required evidence.  A missing
                # row or a post-gap baseline must hold rather than authorize an
                # unpaced payload read.  This is the one hold that does not ask
                # about clients: it is not a busy pool, it is a blind pacer.
                self._over = True
            self._charge_slice_locked(now)
            return
        self._samples += 1
        self.ledger.count_sample()
        for key in self._totals:
            self._totals[key] += measured[key]
            self._maxima[key] = max(self._maxima[key], measured[key])
        self._last = measured
        self._over = self._clients_active and self._pool_is_hurting(measured)
        self._charge_slice_locked(now)

    def _verdict(self) -> bool:
        """Is the pool over threshold?  Resamples at most every ``sample_s``."""

        now = self.clock()
        with self._lock:
            if (self._sampled_at is None
                    or now - self._sampled_at >= self.sample_s):
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

    def _charge_slice_locked(self, now: float) -> None:
        """Bill the hold time since the last charge to idle or active clients.

        Called on every resample, so a hold that starts while a client is
        reading and outlives it is split where the client stopped rather than
        charged whole to the state it happened to begin in.
        """

        if not self._holding:
            return
        self.ledger.add(now - self._slice_at, clients_active=self._slice_active)
        self._slice_at = now
        self._slice_active = self._clients_active

    def _enter_hold(self) -> None:
        with self._lock:
            self._holds += 1
            self.ledger.count_hold()
            first = self._holding == 0
            if first:
                self._hold_started = self.clock()
                self._slice_at = self._hold_started
                self._slice_active = self._clients_active
            self._holding += 1
            last = {**self._last,
                    "clients_active": self._clients_active,
                    "client_read_mb_s": self._client_mb_s,
                    "telemetry_state": self._telemetry_state_locked(),
                    "missing_devices": list(self._missing_devices)}
        if first and self.notify is not None:
            self.notify({"event": "prewarm-hold", "state": "start", **last})

    def _leave_hold(self) -> None:
        with self._lock:
            now = self.clock()
            # Charge the open slice while the hold is still open: closing it
            # first would make the accounting skip the last stretch.
            self._charge_slice_locked(now)
            self._holding -= 1
            done = self._holding == 0
            if done:
                self._held_s += now - self._hold_started
            held = self._held_s
            total = self.ledger.held_s
            last = dict(self._last)
        if done and self.notify is not None:
            self.notify({"event": "prewarm-hold", "state": "end",
                         "held_seconds": round(held, 3),
                         "held_seconds_total": round(total, 3), **last})

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
            total = self.ledger.held_s
            idle = self.ledger.idle_s
            active = self.ledger.active_s
            if self._holding:
                now = self.clock()
                held += now - self._hold_started
                pending = max(0.0, now - self._slice_at)
                total += pending
                if self._slice_active:
                    active += pending
                else:
                    idle += pending
            return {
                "active": self.active,
                "devices": list(self.devices),
                "reason": self.reason,
                "samples": samples,
                "samples_total": self.ledger.samples,
                "holds": self._holds,
                "holds_total": self.ledger.holds,
                "held_seconds": round(held, 3),
                # Row-scoped above, role-scoped here: an operator asking what
                # pacing has cost since the role started is asking a question
                # no single row's receipt can answer.
                "held_seconds_total": round(total, 3),
                "held_while_clients_idle_s": round(idle, 3),
                "held_while_clients_active_s": round(active, 3),
                "clients_active": self._clients_active,
                "client_read_mb_s": (None if self._client_mb_s is None
                                     else round(self._client_mb_s, 1)),
                "client_rate_source": (self.client_rate.path
                                       if self.client_rate is not None else ""),
                "max_util_pct": round(self._maxima["util_pct"], 1),
                "mean_util_pct": round(mean["util_pct"], 1),
                "max_read_await_ms": round(self._maxima["read_await_ms"], 2),
                "mean_read_await_ms": round(mean["read_await_ms"], 2),
                "max_backlog_ms": round(self._maxima["backlog_ms"], 1),
                "mean_backlog_ms": round(mean["backlog_ms"], 1),
                "telemetry_state": self._telemetry_state_locked(),
                "telemetry_gaps": self._telemetry_gaps,
                "missing_devices": list(self._missing_devices),
                "thresholds": {
                    # Recorded, not enforced: utilization no longer holds on
                    # its own.  It stays in the receipt because the number is
                    # what an operator compares against Netdata.
                    "max_util_pct": self.max_util_pct,
                    "max_read_await_ms": self.max_read_await_ms,
                    "max_backlog_ms": self.max_backlog_ms,
                    "client_active_mb_s": self.client_active_mb_s,
                },
            }


def pacer_from_args(args) -> DiskPacer:
    """Build the pacer a cycle will use, from arguments and the pool topology.

    A caller that deliberately supplies no disks gets the explicit inactive
    pacer used by non-storage tests.  ``main`` is stricter for the storage
    role: it refuses a topology discovery failure before a cycle can read.

    The pacer keeps its own hold ledger; ``main`` replaces it with the role's,
    so lifetime hold time survives the pacer this cycle throws away.
    """

    devices = [name for name in
               (part.strip() for part in (getattr(args, "disks", "") or "").split(","))
               if name]
    reason = "from --disks" if devices else ""
    if not devices and getattr(args, "pace_pool", None):
        devices = pool_member_devices(str(args.pace_pool))
        reason = (f"from zpool status -P {args.pace_pool}" if devices
                  else f"pool {args.pace_pool} has no readable data vdev members")
    client_rate = None
    path = getattr(args, "nfsd_io", "") or ""
    if path:
        client_rate = ClientReadRate(path)
    return DiskPacer(
        devices,
        max_util_pct=args.max_util_pct,
        max_read_await_ms=args.max_read_await_ms,
        max_backlog_ms=args.max_backlog_ms,
        sample_s=args.pace_sample_s,
        hold_s=args.pace_hold_s,
        reason=reason,
        client_rate=client_rate,
        client_active_mb_s=getattr(args, "client_active_mb_s",
                                   CLIENT_ACTIVE_MB_S),
    )


def require_storage_pacing(pacer: DiskPacer) -> None:
    """Refuse a storage-role cycle with no discoverable pacing members.

    This is separate from ``DiskPacer`` so direct fixtures can deliberately
    exercise their inactive/no-storage shape.  The supervisor restarts a role
    that exits here; it is safer to wait for that retry than to perform one
    unpaced warm after a transient ``zpool`` failure.
    """

    if pacer.active:
        return
    raise SystemExit(
        "prewarm: this is the storage host and no disks could be paced "
        f"({pacer.report()['reason']}); reading the pool unpaced is the "
        "defect #499 records, so refuse rather than degrade.  Name the "
        "members with --disks if discovery cannot see them.")


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
        if self.pacer is not None:
            self.pacer.begin_row()
        work: "queuelib.Queue[tuple[int, dict[str, object]] | None]" = queuelib.Queue()
        for index, entry in enumerate(entries):
            work.put((index, entry))
        for _ in range(self.readers):
            work.put(None)
        lock = threading.Lock()
        state = {"bytes": 0, "entries": 0, "budget": int(budget_bytes),
                 "reserved": 0}
        completed = [0] * len(entries)
        errors: list[str] = []

        def worker() -> None:
            buffer = bytearray(self.block)
            view = memoryview(buffer)
            while not stop.is_set():
                queued = work.get()
                if queued is None:
                    return
                index, job = queued
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
                        with lock:
                            allowance = max(0, state["budget"] - state["bytes"]
                                            - state["reserved"])
                            if not allowance:
                                break
                            allowance = min(allowance, self.block, remaining)
                            state["reserved"] += allowance
                        try:
                            chunk = os.readv(fd, [view[:allowance]])
                        except OSError:
                            with lock:
                                state["reserved"] -= allowance
                            raise
                        with lock:
                            # Return the reservation and commit its actual
                            # bytes as one transition, so another reader
                            # cannot claim the credit between those steps.
                            state["reserved"] -= allowance
                            state["bytes"] += chunk
                        if not chunk:
                            break
                        got += chunk
                        remaining -= chunk
                        with lock:
                            if state["bytes"] >= state["budget"]:
                                break
                except OSError as exc:
                    with lock:
                        if len(errors) < 20:
                            errors.append(f"{os.path.basename(path)}: {exc}")
                finally:
                    os.close(fd)
                with lock:
                    completed[index] = got
                    if got == want_total:
                        state["entries"] += 1
                    if recheck is not None and state["entries"] % 64 == 0:
                        room = recheck()
                        # ``recheck`` returns this row's total ceiling, not
                        # additional credit after bytes already read.  Keep
                        # enough budget for reads already admitted by sibling
                        # threads; those I/Os cannot be recalled, but no new
                        # credit is issued beyond that cooperative boundary.
                        ceiling = min(state["budget"], max(0, int(room)))
                        state["budget"] = max(
                            state["bytes"] + state["reserved"], ceiling)

        started = time.time()
        threads = [threading.Thread(target=worker, daemon=True)
                   for _ in range(self.readers)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        elapsed = max(1e-9, time.time() - started)
        contiguous = 0
        for entry, got in zip(entries, completed):
            want = int(entry["bytes"])
            if got != want:
                break
            contiguous += want
        return {
            "bytes_warmed": state["bytes"],
            "contiguous_bytes": contiguous,
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


def manifest_phases(manifest: dict) -> list[dict[str, object]]:
    """The read order the producer wrote down, or an empty list.

    ``annotations.phases`` is a running byte sum over ``entries`` in the order
    the action consumes them: ``[{"name", "bytes", "cumulative_bytes"}, ...]``,
    the last boundary being the whole manifest.  PrismaQuant's joint pass
    writes one phase per hidden layer (PQ #524), which is what lets a 4.75 TB
    manifest be warmed a window at a time instead of refused whole.

    A table that does not describe this manifest is treated as absent rather
    than repaired: windowing on the wrong boundaries reads the wrong bytes and
    calls them resident, which is worse than not windowing at all.
    """

    annotations = manifest.get("annotations")
    if not isinstance(annotations, dict):
        return []
    declared = annotations.get("phases")
    if not isinstance(declared, list) or not declared:
        return []
    total = int(manifest.get("total_bytes", 0))
    # A reader submits whole manifest entries.  A phase table whose boundary
    # cuts one in half would make the stated window smaller than the bytes the
    # reader actually faults into ARC, so it is not a usable residency table.
    entry_boundaries: set[int] = set()
    entry_total = 0
    for entry in manifest.get("entries", []):
        entry_total += int(entry.get("bytes", 0))
        entry_boundaries.add(entry_total)
    if entry_total != total:
        return []
    table: list[dict[str, object]] = []
    seen: set[str] = set()
    previous = 0
    for phase in declared:
        if not isinstance(phase, dict):
            return []
        name = phase.get("name")
        cumulative = phase.get("cumulative_bytes")
        if not isinstance(name, str) or not name or name in seen:
            return []
        if isinstance(cumulative, bool) or not isinstance(cumulative, int):
            return []
        if cumulative < previous or cumulative > total \
                or cumulative not in entry_boundaries:
            return []
        seen.add(name)
        table.append({"name": name, "cumulative_bytes": cumulative})
        previous = cumulative
    if previous != total:
        return []
    return table


def consumed_through(phases: list[dict[str, object]], phase_name: str) -> int:
    """Bytes an action reporting ``phase_name`` has finished reading.

    The phase it names is the one it is reading now, so what it is done with
    is everything before that phase began.  Counting the named phase's own
    bytes as consumed would release a window the action is still inside.
    A name this manifest does not carry releases nothing.
    """

    consumed = 0
    for phase in phases:
        if phase["name"] == phase_name:
            return consumed
        consumed = int(phase["cumulative_bytes"])
    return 0


def window_target(phases: list[dict[str, object]], entries: list[dict[str, object]],
                  *, consumed: int, budget: int) -> tuple[int, str]:
    """The furthest entry boundary that stays within ``budget`` of the reader.

    Phase boundaries are accepted-progress frontiers: only a matching worker
    observation can move ``consumed`` to one.  They are not a minimum warm
    unit.  An oversized phase can contain many manifest entries, and any
    entry-aligned prefix inside it is safe to fault into ARC without claiming
    that the action has consumed those bytes.  Return the target boundary and
    its phase name when it happens to end a phase; an in-phase target has no
    phase name.
    """

    phase_names = {int(phase["cumulative_bytes"]): str(phase["name"])
                   for phase in phases}
    target = 0
    cumulative = 0
    for entry in entries:
        cumulative += int(entry["bytes"])
        if cumulative <= consumed:
            continue
        if cumulative - consumed > budget:
            break
        target = cumulative
    return target, phase_names.get(target, "")


def entries_between(entries: list[dict[str, object]], start: int,
                    end: int) -> list[dict[str, object]]:
    """The entries covering ``[start, end)`` of the manifest's own byte order.

    ``cumulative_bytes`` is a sum over these same entries, so a phase boundary
    falls between two of them and the slice is exact.  An entry straddling the
    start is included whole, because a warm reads files, not byte ranges.
    """

    chosen: list[dict[str, object]] = []
    position = 0
    for entry in entries:
        size = int(entry.get("bytes", 0))
        if position >= end:
            break
        if position + size > start:
            chosen.append(entry)
        position += size
    return chosen


def declares_progress(request: dict | None) -> bool:
    """Did the sealed request ask for progress reporting?

    A record beside an action that declared no policy is not a report this
    platform asked for, so the loop does not read it.  Refusing it costs a
    warm; trusting it would let an unsealed file move the budget.
    """

    if not isinstance(request, dict):
        return False
    params = request.get("params")
    return isinstance(params, dict) and pb.PROGRESS_PARAM in params


def progress_phase(queue: pool.PoolQueue, action_key: str,
                   claimed_unix: float) -> dict | None:
    """The matching worker's accepted progress observation, if it has one.

    The action-writable channel is not authority for the storage role: its
    launch token is intentionally known only to the worker.  The claim lease
    carries the worker's already authenticated ``ProgressWatch`` observation,
    keyed to this exact claim and refreshed at heartbeat cadence.
    """

    # The raw channel is owned by the running action and requires a launch
    # token only its worker knows.  The storage role cannot authenticate that
    # token.  Its authority is therefore the matching lease's accepted
    # ProgressWatch observation, which has already checked token, policy,
    # counter monotonicity and regular-file identity at heartbeat cadence.
    path = queue.lease_path(action_key)
    try:
        raw = pb._read_regular_file_nofollow(
            path, where="prewarm progress lease",
            max_bytes=progress_v1.MAX_ACTION_PROGRESS_BYTES)
        record = pb._decode_strict_json(raw, where="prewarm progress lease")
    except (OSError, pb.ActionContractError, pb.CASTamperError,
            pb.CASUnavailableError, RecursionError):
        return None
    if not isinstance(record, dict):
        return None
    if record.get("action_key") != action_key:
        return None
    lease_claimed = record.get("claimed_unix")
    if not isinstance(lease_claimed, (int, float)) or lease_claimed != claimed_unix:
        return None
    observed = record.get("progress_observation")
    if not isinstance(observed, dict) or observed.get("source") != "action-progress":
        return None
    accepted = observed.get("last_accepted")
    if not isinstance(accepted, dict):
        return None
    phase = accepted.get("phase")
    if not isinstance(phase, str) or not phase:
        return None
    return {"phase": phase, "reported_unix": accepted.get("reported_unix"),
            "units_completed": accepted.get("units_completed")}


def resident_bytes(record: Mapping[str, object] | None) -> int:
    """What a prewarm record claims is resident for its manifest.

    ``warmed_bytes`` is the window's running total; ``bytes_warmed`` is what
    the last read moved.  They are the same number for a row warmed in one
    pass, which is every row a generation before windowing ever wrote, so the
    older spelling is the fallback rather than an error.
    """

    if not record:
        return 0
    for key in ("warmed_bytes", "bytes_warmed"):
        value = record.get(key)
        if isinstance(value, int) and not isinstance(value, bool):
            return max(0, value)
    return 0


def claimed_reserve(queue: pool.PoolQueue, cas_root: Path, grace_s: float) -> dict:
    """Bytes a claimed action may still be reading, and must not be displaced.

    An action reads its inputs at the start of its run, so a claim older than
    the grace window is past its load phase and its bytes are no longer worth
    reserving.  The window is configuration, not a guess about any one
    campaign: set it to the load phase you measured.

    An action that reports progress says more than the clock can.  When its
    manifest carries phases and its record names one, the phases before that
    one are read: the ARC may evict them, nothing is waiting for them, and
    holding budget for them keeps the next row cold for no benefit.  Those
    bytes leave the reserve here, which is what lets the loop warm ahead of
    the claim frontier instead of behind it (#523: the loop warmed the next
    row 4.3 s before its claim, while that row was already reading).

    A row this loop warmed a window of reserves that window, not its manifest:
    a 4.75 TB joint pass never had 4.75 TB resident, and reserving bytes that
    were never warmed would take the whole budget away from every other row.
    """

    now = time.time()
    reserved = 0
    released_total = 0
    keys: list[str] = []
    triggers: list[dict[str, object]] = []
    windows: list[dict[str, object]] = []
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
        key = str(record.get("action_key", path.stem))
        root = Path(str(record.get("cas_root") or cas_root))
        request = sealed_request(root, key)
        size = declared_manifest_bytes(request)
        if not size:
            continue
        warmed = queue.prewarm(key)
        declared = declares_progress(request)
        if not (warmed and warmed.get("phased")):
            # A worker can claim between the storage role's ready-list polls.
            # For a phased manifest that must not turn the first missed warm
            # into a whole-manifest reserve: initialize an empty window at the
            # accepted read frontier and let the ordinary claimed-window pass
            # fill it below.  Nothing is charged yet because no bytes have
            # been warmed.
            entry = manifest_input_of(request)
            manifest = load_manifest(root, entry) if entry is not None else None
            phases = manifest_phases(manifest) if manifest else []
            if declared and phases:
                policy = ((request.get("params") or {}).get(pb.PROGRESS_PARAM)
                          if isinstance(request, dict) else None)
                cyclic = isinstance(policy, dict) and bool(policy.get("cycle"))
                reported = (progress_phase(queue, key, float(claimed_unix))
                            if not cyclic else None)
                consumed = (consumed_through(phases, reported["phase"])
                            if reported is not None else 0)
                windows.append({
                    "action_key": key, "cas_root": str(root),
                    "status": "partial", "manifest_sha256": str(entry["sha256"]),
                    "manifest_bytes": size, "warmed_bytes": 0,
                    "consumed_bytes": consumed, "resident_ahead": 0,
                    "phase": reported["phase"] if reported else "",
                })
                continue
            if now - float(claimed_unix) > grace_s and not declared:
                continue
            # No window: the whole manifest is what the claim may still read.
            reserved += size
            keys.append(key)
            continue
        resident = min(size, resident_bytes(warmed))
        policy = ((request.get("params") or {}).get(pb.PROGRESS_PARAM)
                  if isinstance(request, dict) else None)
        cyclic = isinstance(policy, dict) and bool(policy.get("cycle"))
        reported = (progress_phase(queue, key, float(claimed_unix))
                    if declared and not cyclic else None)
        if (now - float(claimed_unix) > grace_s and reported is None
                and not declared):
            continue
        consumed = 0
        if reported is not None:
            # The manifest is opened only here, for a claimed row that is both
            # windowed and reporting: the common path still costs one small
            # JSON read per claim, which is what ``declared_manifest_bytes``
            # exists for.
            entry = manifest_input_of(request)
            manifest = load_manifest(root, entry) if entry is not None else None
            phases = manifest_phases(manifest) if manifest else []
            if phases:
                consumed = consumed_through(phases, reported["phase"])
        ahead = max(0, resident - consumed)
        released = resident - ahead
        reserved += ahead
        released_total += released
        if ahead or released:
            keys.append(key)
        if released:
            triggers.append({"action_key": key, "phase": reported["phase"],
                             "released_bytes": released})
        windows.append({
            "action_key": key, "cas_root": str(root),
            "status": str(warmed.get("status", "")),
            "manifest_sha256": str(warmed.get("manifest_sha256", "")),
            "manifest_bytes": size,
            "warmed_bytes": resident,
            "consumed_bytes": consumed,
            "resident_ahead": ahead,
            "phase": reported["phase"] if reported else "",
        })
    return {"claimed_reserved_bytes": reserved, "claimed_reserved_keys": keys,
            "claimed_released_bytes": released_total,
            "progress_triggers": triggers, "claimed_windows": windows}


def warm_record(queue: pool.PoolQueue, action_key: str,
                sha256: str) -> dict[str, object] | None:
    """This loop's record for this manifest, or ``None`` for another digest.

    A record filed against a different digest describes different bytes, so it
    is not a window this cycle may advance or count.
    """

    record = queue.prewarm(action_key)
    if record and record.get("manifest_sha256") == sha256:
        return record
    return None


def already_warm(queue: pool.PoolQueue, action_key: str, sha256: str) -> bool:
    record = warm_record(queue, action_key, sha256)
    return bool(record and record.get("status") == "complete")


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
        # A window warmed part of the way through a manifest is as resident as
        # a whole one, and displacing it costs the same read twice.
        if record and record.get("status") in ("complete", "partial"):
            resident = resident_bytes(record)
            if not resident:
                continue
            reserved += resident
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
    windows = reserve.pop("claimed_windows")
    reserve.update(warmed_reserve(queue, ready))
    protected = (reserve["claimed_reserved_bytes"]
                 + reserve["warmed_reserved_bytes"])
    budget = max(0, room["capacity_budget"] - protected)
    #: What the budget would be if no claimed action had reported progress.
    #: A row that fits the first and not the second was made warmable by a
    #: progress record, which is how ``trigger`` stays an observation rather
    #: than a label somebody chose.
    released = int(reserve["claimed_released_bytes"])
    budget_before_progress = max(0, budget - released)
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
        "budget_before_progress": budget_before_progress,
        "warmed": [],
        "advanced": [],
        "skipped": [],
    }

    cycle_spent = 0

    def warm(*, key: str, manifest: dict, digest: str, entries: list,
             start_bytes: int, target: int, phase: str, phased: bool,
             trigger: str) -> dict:
        """Read one window of one manifest and file what is now resident."""

        nonlocal budget, budget_before_progress, cycle_spent
        total = int(manifest["total_bytes"])
        want = max(0, target - start_bytes)
        started = time.time()
        if args.dry_run:
            result = {"bytes_warmed": 0, "entries_warmed": 0, "seconds": 0.0,
                      "mb_per_s": 0.0, "readers": args.readers,
                      "disk_pacing": pacer.report(), "errors": []}
        else:
            reader = Reader(args.readers, mounts, pacer=pacer)
            result = reader.read(
                entries,
                budget_bytes=want,
                stop=stop,
                # Re-read rather than trust the cycle's opening number: the
                # ceiling can be lowered under a running warm, and a row that
                # started inside its budget must stop when it leaves it.
                recheck=lambda: max(0, arc_headroom(
                    args.arc_reserve_fraction,
                    args.arcstats)["capacity_budget"] - protected - cycle_spent),
            )
        after = arc_headroom(args.arc_reserve_fraction, args.arcstats)
        finished = time.time()
        resident = start_bytes + int(result.get("contiguous_bytes", result["bytes_warmed"]))
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
                else "complete" if resident >= total
                else "partial"
            ),
            # Everything this loop has made resident for the manifest, against
            # ``bytes_warmed`` below, which is what this read moved.  They
            # differ only for a manifest warmed a window at a time.
            "warmed_bytes": target if args.dry_run else resident,
            "phased": phased,
            "warmed_through_phase": phase,
            "window_start_bytes": start_bytes,
            "trigger": trigger,
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
        spent = want if args.dry_run else int(result["bytes_warmed"])
        cycle_spent += spent
        budget = max(0, budget - spent)
        budget_before_progress = max(0, budget_before_progress - spent)
        return record

    # The running action first.  A claimed action reading a manifest larger
    # than the budget is not in ``ready``, so nothing else in this loop can
    # see it, and its window has to move with its read frontier or the warm
    # stops one phase in and the job goes back to the spindles.  It does not
    # spend ``--lookahead``: that window counts rows no claim has reached.
    for window in windows:
        if stop.is_set():
            break
        key = str(window["action_key"])
        if window["status"] != "partial":
            continue
        root = Path(str(window["cas_root"]))
        entry = manifest_input_of(sealed_request(root, key))
        if entry is None or str(entry.get("sha256")) != window["manifest_sha256"]:
            continue
        manifest = load_manifest(root, entry)
        phases = manifest_phases(manifest) if manifest else []
        if not phases:
            continue
        resident = int(window["warmed_bytes"])
        consumed = int(window["consumed_bytes"])
        # This window's resident bytes are charged to the claimed reserve
        # already; pricing the advance against a budget that also pays for
        # them would stop a window from ever extending.
        row_budget = budget + int(window["resident_ahead"])
        target, phase = window_target(phases, list(manifest["entries"]),
                                      consumed=consumed, budget=row_budget)
        if target <= resident:
            event["advanced"].append({
                "action_key": key, "status": "window full" if target else "headroom",
                "consumed_bytes": consumed, "warmed_bytes": resident,
                "budget": row_budget,
            })
            continue
        start = max(resident, consumed)
        record = warm(
            key=key, manifest=manifest, digest=str(entry["sha256"]),
            entries=entries_between(list(manifest["entries"]), start, target),
            start_bytes=start, target=target, phase=phase, phased=True,
            trigger="progress" if window["phase"] else "claim",
        )
        event["advanced"].append({
            k: record[k] for k in
            ("action_key", "status", "manifest_bytes", "warmed_bytes",
             "bytes_warmed", "warmed_through_phase", "trigger", "seconds",
             "mb_per_s")
        })

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
        digest = str(entry["sha256"])
        phases = manifest_phases(manifest)
        prior = warm_record(queue, key, digest)
        resident = resident_bytes(prior)
        # This row's own warmed bytes are already charged to the budget by
        # ``warmed_reserve``; charging them again would price a row against a
        # ceiling it is itself holding down, and a window could never extend.
        row_budget = budget + resident
        row_budget_before_progress = budget_before_progress + resident
        if phases:
            target, phase = window_target(phases, list(manifest["entries"]),
                                          consumed=0, budget=row_budget)
        else:
            target, phase = (total if total <= row_budget else 0), ""
        if (prior and prior.get("status") == "complete") or (
                target and target <= resident):
            # A row that is already resident needs nothing from this window,
            # so it must not spend it -- the same reason a manifest below the
            # threshold is passed over above.  #523's 2026-09-12 readback:
            # the loop skipped one ready row as already warm from 18:50:04
            # through ten minutes of polls, and warmed the row behind it only
            # once the warm one was claimed, 4.3 s before that row's own
            # claim.  Its bytes stay charged to the budget by
            # ``warmed_reserve``, which is what still bounds how much the loop
            # holds ahead of the claim frontier.  For a windowed row it means
            # everything the budget allows is resident, which is the same
            # claim about a manifest that will never fit whole.
            event["skipped"].append({"action_key": key, "reason": "already warm"})
            continue
        if target <= 0:
            # Refusing is the correct outcome, not a failure: warming a row
            # that does not fit would evict the row that is running to make
            # room for one that is not.  It does not spend the lookahead
            # either -- a row too big for today's budget must not disable
            # prewarm for the rows behind it that do fit.
            event["skipped"].append({
                "action_key": key, "reason": "headroom",
                "manifest_bytes": total, "budget": budget,
                "phased": bool(phases),
            })
            continue
        taken += 1
        record = warm(
            key=key, manifest=manifest, digest=digest,
            entries=(entries_between(list(manifest["entries"]), resident, target)
                     if phases else list(manifest["entries"])),
            start_bytes=resident if phases else 0,
            target=target, phase=phase, phased=bool(phases),
            trigger=("progress" if target > row_budget_before_progress
                     else "claim"),
        )
        event["warmed"].append({k: record[k] for k in
                                ("action_key", "status", "manifest_bytes",
                                 "bytes_warmed", "warmed_bytes", "trigger",
                                 "warmed_through_phase", "seconds", "mb_per_s",
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
                             "bought by evicting what the running rows read.  "
                             "This counts rows a warm could still make "
                             "faster: a row already warmed by this loop is "
                             "passed over without spending the window, and "
                             "the budget, which still charges its bytes, is "
                             "what bounds how many rows stay resident ahead "
                             "of the claim frontier")
    parser.add_argument("--pace-pool", default="storage_pool",
                        help="the ZFS pool whose data vdev members pace the "
                             "reader; its spindles are discovered with "
                             "'zpool status -P'.  A host where that fails "
                             "is refused by the storage role rather than "
                             "read unpaced")
    parser.add_argument("--disks", default="",
                        help="comma-separated block devices (sdb,sdc) to pace "
                             "on, overriding --pace-pool discovery")
    parser.add_argument("--client-active-mb-s", type=float,
                        default=CLIENT_ACTIVE_MB_S,
                        help="clients count as reading when this host's NFS "
                             "server is serving more than this many MB/s "
                             "(read from --nfsd-io).  Nothing is held while "
                             "they are idle: a hold buys a client faster "
                             "reads and costs the campaign a warm, so it is "
                             "worth paying only when there is a client to pay "
                             "it to.  Measured 2026-09-13: a zpool scrub held "
                             "the storage role for 5709 s at 100%% "
                             "utilization with no client reading at all.  The "
                             "default %(default)s is a tenth of the slowest "
                             "client read this pacer exists to protect "
                             "(#523's 26 MB/s cold-start row).  It has to sit "
                             "below that, because a client already slowed by "
                             "the warm would otherwise read as idle and "
                             "release the hold that would give it the disks "
                             "back; it is still far above the few hundred "
                             "kB/s an idle mount's attribute traffic makes.  "
                             "0 holds whenever the disks are over, whoever is "
                             "reading")
    parser.add_argument("--nfsd-io", default=NFSD_IO,
                        help="where this host's NFS server counts the bytes "
                             "it has served (the 'io' line).  A host without "
                             "it cannot tell an idle client from a busy one "
                             "and paces as though clients were always "
                             "reading, which is the safe answer.  This loop "
                             "reads the pool locally, so its own bytes never "
                             "appear here")
    parser.add_argument("--max-util-pct", type=float, default=25.0,
                        help="recorded, and no longer a hold on its own.  "
                             "Utilization answers 'is the disk busy', and a "
                             "scrub is busy with nobody behind it: on "
                             "2026-09-13 this cap alone held the loop for "
                             "5709 s while every client was idle.  What a "
                             "client feels is --max-read-await-ms and "
                             "--max-backlog-ms, and those are what hold.  "
                             "Kept so an existing command line still parses "
                             "and the number stays in the receipt beside the "
                             "rate.  What #499 measured on dl380g10, for "
                             "the record: 8-12%% while the campaign alone "
                             "read, 73-83%% under the 8-reader warm that "
                             "reset every client's RDMA transport")
    parser.add_argument("--max-read-await-ms", type=float, default=10.0,
                        help="hold, while clients are reading, whenever any "
                             "pool disk's mean read service time over the "
                             "last sample exceeds this.  A hold ends at half "
                             "this number, so a disk sitting on the cap does "
                             "not flap the reader once per sample.  "
                             "Measured: ~0-2 ms harmless, 38-54 ms stalling; "
                             "10 is what the passing run used.  0 disables")
    parser.add_argument("--max-backlog-ms", type=float, default=2000.0,
                        help="hold, while clients are reading, whenever any "
                             "pool disk's queue backlog exceeds this; "
                             "released at half this number.  "
                             "Measured: 300-450 ms harmless, "
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
                        help="run one cycle and exit; return 75 if maintenance or runtime rotation defers it")
    parser.add_argument("--dry-run", action="store_true",
                        help="plan without warming data or recording prewarm results; maintenance checks still apply")
    parser.add_argument("--log", default=None,
                        help="append one JSON object per cycle here")
    args = parser.parse_args(argv)

    mounts = MountMap(list(args.mount_map))
    if not args.dry_run and not mounts.usable():
        raise SystemExit(
            "prewarm: no local mount from --mount-map exists on this host; "
            "this loop belongs on the storage host only")
    queue = None
    stop = threading.Event()
    # One ledger for the life of the role: the pacer is rebuilt every cycle so
    # a replaced vdev is picked up, and hold time that reset with it would
    # answer "this poll" to an operator asking "since you started".
    ledger = HoldLedger()
    loaded_commit = runtime_gate.loaded_runtime_commit()
    loaded_generation = runtime_gate._generation_at(runtime_gate.GENERATION_VERSION)

    def announce(payload: dict[str, object]) -> None:
        print(json.dumps({**payload, "unix": round(time.time(), 3)}),
              file=sys.stderr, flush=True)

    def runtime_moved() -> bool:
        current = runtime_gate.published_commit()
        current_generation = runtime_gate._generation_at(runtime_gate.RUNTIME_VERSION)
        return bool((current and current != loaded_commit) or (
            loaded_generation and current_generation
            and loaded_generation != current_generation))

    while True:
        # Finish the current cycle before parking; a marker must never certify
        # a reader that is still inside its file reads. Check rotation first so
        # even a parked role leaves old imports for supervisor replacement.
        if runtime_moved():
            print("prewarm: runtime moved; exiting for supervisor reload",
                  file=sys.stderr, flush=True)
            return 75 if args.once else 0
        gate = runtime_gate.read_maintenance_gate()
        if gate is not None:
            runtime_gate.post_park_marker(gate)
            print("prewarm: maintenance gate closed; storage cycle deferred",
                  file=sys.stderr, flush=True)
            if args.once:
                return 75
            time.sleep(args.poll_s)
            continue
        if queue is None:
            queue = pool.PoolQueue(Path(args.pool_root))
            probe = pacer_from_args(args)
            if not args.dry_run:
                require_storage_pacing(probe)
        pacer = pacer_from_args(args)
        pacer.ledger = ledger
        if not args.dry_run:
            require_storage_pacing(pacer)
        pacer.notify = announce
        # Topology discovery may block. Re-enter the boundary if its setup
        # outlived an open gate or the runtime it was preparing to serve.
        if runtime_moved() or runtime_gate.read_maintenance_gate() is not None:
            continue
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
