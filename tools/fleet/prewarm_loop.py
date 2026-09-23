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

A manifest with no phase table is windowed the same way, because ``entries``
*is* the consumption order and the phase table is only a running sum over it:
the loop warms the longest entry-aligned prefix the budget allows, in the
order the action reads it, and reserves that prefix rather than the manifest.
Only one thing needs the phases, and it is the advance: without them no worker
report maps to a byte count, so an unphased window stops following the reader
the moment its row is claimed.  While the row is still in ``ready`` a later
poll can extend the prefix as the budget grows; after the claim it stays where
the budget left it.  Before #499 the unphased case kept the original
all-or-nothing rule and warmed nothing at all; the 2026-09-17 GLM-5.3-Flash
joint-AURA ``prepare`` declares 469 008 entries and 6.84 TB against a 257.7 GB
ceiling, and a sibling manifest without phases was refused on every poll for
as long as it was queued.  The refusal that remains is narrower and still
right: a single entry larger than the whole budget has no prefix inside it,
because a warm reads files and not byte ranges.

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
whenever a client *other than the action it is warming for* is reading *and*
the worst disk is over its service-time or backlog cap.  Both halves are
required, and the first half names its client.  On 2026-09-17 (#580) the
GLM-5.3-Flash joint-AURA ``prepare`` was the only client on the box, reading
at 160-280 MB/s through the very bytes this loop was fetching for it; the
pacer counted that as a client to protect, held 11.03 h of 20.6 h -- 97.7 %
of the last hour -- and the warm ran at 22.8 MB/s behind a reader it could
never get ahead of, backing off from load that existed because it backed
off.  The served action's reads are the same one-pass bytes read earlier and
at depth, not extra I/O, so they are *self*: the claim names the box running
the action, the box's worker offer names its client addresses, and the
server's per-client ``export_stats`` says what each address read.  Nothing
in that chain is guessed, and a missing link leaves every client protected
as before, with the reason in the receipt.  Depth follows the same verdict:
``--max-readers`` while nobody else is reading, ``--readers`` -- the depth
#499 measured to pass -- while somebody is, and a hold only when that
somebody is also hurt.  On 2026-09-13 a ``zpool scrub`` drove
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

The stage tier, and what it does not buy (#582, opt-in)
-------------------------------------------------------
``--stage`` gives the same window read a second destination: a copy on a ZFS
pool whose name starts with ``prismabuild-stage``.  Nothing else changes --
the same ``Reader``, the same ``DiskPacer``, the same window arithmetic, the
same ARC budget -- and without the flag nothing here runs at all, down to the
fields in the receipts.

The tier is rediscovered every cycle and reported in five states, one of
which is always written even when the loop stages nothing: ``present``,
``absent`` (no pool carries the name), ``full`` (the discovered capacity
leaves nothing above the floor, or the budget ran out mid-cycle),
``unreadable`` (no ``zpool``, no mounted directory, an unwritable one, or a
health that is neither ONLINE nor DEGRADED), and ``faulted`` (a write failed
for something other than exhaustion).  Capacity is the ``prewarm`` dataset's
own ``available`` -- ZFS's arithmetic, which has already withheld the pool's
slop and this dataset's quota -- and members reach a record only as
``/dev/disk/by-id`` names: the
``nvmeXn1`` numbers on this fleet's file server are the reverse of what the
model names suggest, and a record naming one would name the box's root
device.

Release is delete-behind-the-accepted-phase -- the same frontier that already
releases ARC reserve -- with two guards.  A staged object is deleted only
when no other queued row still wants it, because the stage tree is keyed by
path and three measured prepare manifests share 469 007 of 469 008 entries;
and a row that leaves the queue has its band swept, because a last phase
leaves no frontier behind it.  A cycle with several queued rows over the same
bytes can therefore delete nothing and report ``full``.  That is the measured
shape of total overlap, not a leak, and it is the retained-prefix behaviour
#583's revision-2 design argues for -- arrived at, not chosen.

**What this does not establish, and must not be read as establishing:** no
consumer reads *this* tree.  Two trees live on the stage dataset and only one
of them is read: ``stage_move`` (#583) copies a consumer's declared range,
files a residency-map fragment for it, and the consumer opens the staged path
once the composed map names it (#634).  This loop's own ``--stage`` objects
are named for their byte range under :data:`STAGE_LAYOUT`, no map names them,
and nothing opens them -- so a copy here is still a copy, and every record
says so in its ``consumer`` block instead of reporting bytes written as bytes
saved.  Turning it on without a consumer costs SSD writes at pool read rate
and returns nothing, the same wear #582 counts against L2ARC.  A range
written under a mirrored file name would be a short file, which is why an
object here carries its byte range in its name and this tree is a
residency-map source rather than an overlay lower layer.

**``primarycache`` on the stage dataset: ``all``, since 2026-09-18 (#638).**
The rationale that argued for ``metadata`` -- the stage's writes go through
the ARC this loop is filling, so a staged window holds two copies of it --
expired when a consumer began reading the stage.  It is the mover's tree the
consumer reads, and the setting is the dataset's, so the dataset must cache
file data or every consumer read of a staged file reaches the SSD: measured
sparky -> dl380g10 on one 5.37 GB file, ``dd iflag=direct``, 16 x 256 MiB at
matched concurrency, 2402 MB/s on an ARC miss against 10045 MB/s on a hit --
4.18x, 19% of the 100 Gbps link against 80%.  The second copy this loop's own
staged window now holds in ARC is the price of that, paid deliberately.
``tier_loop`` announces the setting on the tier record and refuses the warm
out loud when a rebuilt pool has inherited ``metadata`` again.

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
import errno
import json
import os
import queue as queuelib
import re
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
from prismabuild import storage_tiers  # noqa: E402

#: ZFS reads a whole record either way, and the pool this was measured on uses
#: 1 MiB records; a smaller block only costs syscalls.
BLOCK = 1 << 20
ARCSTATS = "/proc/spl/kstat/zfs/arcstats"
#: ``/sys/block/<dev>/stat`` field offsets, from
#: ``Documentation/block/stat.rst``.  Named because the file is a bare row of
#: integers and an off-by-one here reads writes as reads.
#: Defined once in :mod:`prismabuild.storage_tiers`, which the worker's
#: pool-contention check shares (#1010).
STAT_READS_COMPLETED = storage_tiers.STAT_READS_COMPLETED
#: Sectors read, always 512 bytes each whatever the device's logical block
#: size (``stat.rst``).  The only counter on this box that sees *every* read of
#: the pool -- ZFS issues its device reads from ``zio`` taskq threads, so a
#: reader's own ``/proc/self/io`` ``read_bytes`` is 0 (measured: four of the
#: five live mover receipts report 0 with the spindles at 77-86% utilization),
#: and nfsd's ``export_stats`` sees only what went out over NFS, which a
#: pool-to-stage copy on the file server itself never does.
STAT_READ_SECTORS = storage_tiers.STAT_READ_SECTORS
STAT_READ_MS = storage_tiers.STAT_READ_MS
STAT_IN_FLIGHT = storage_tiers.STAT_IN_FLIGHT
STAT_IO_TICKS = storage_tiers.STAT_IO_TICKS
STAT_WEIGHTED_IO_MS = storage_tiers.STAT_WEIGHTED_IO_MS
SYSFS_BLOCK = "/sys/class/block"
#: Where the kernel's own NFS server says how much it has served.  The ``io``
#: line is ``io <read_bytes> <write_bytes>``, counted for every client of this
#: host.  The prewarm reader reads the pool through the host's local path, so
#: its own bytes never reach this counter and the measurement cannot chase
#: itself.
NFSD_IO = "/proc/net/rpc/nfsd"
#: Where the same server counts what it served *per client*: one
#: ``<export> <client-address> <start-time>`` block per pair, each carrying
#: ``io_read`` and ``io_write``.  Measured on dl380g10 (7.0.0-31,
#: 2026-09-17T21:57Z): over 10 s the two client rows moved 1 630 408 918 B
#: and 5 744 B, and the aggregate ``io`` line moved 1 630 414 662 B -- the
#: same counter, kept per client.  That split is what lets the pacer tell the
#: action it is warming for from a client it must protect (#580).  A block is
#: recreated when the export cache entry expires -- every 901 s on the live
#: box, both rows -- and its counters restart from zero; ``ClientReadRate``
#: carries a client's last rate across that one interval rather than reading
#: the restart as either a negative rate or an idle client.
EXPORT_STATS = "/proc/fs/nfsd/export_stats"
#: Default for ``--max-readers``: the depth the loop reads at while the pool
#: serves nobody but this warm and the action it is warming for.  The pool's
#: measured cold-read curve on dl380g10's raidz1 (2026-09-17): 1 stream
#: 28.6 MB/s, 2 -> 40.5, 4 -> 86.9, 8 -> 134.2, 16 -> 204.7 MB/s -- the same
#: bytes, 7x the rate, from queue depth alone.  16 is the deepest point that
#: curve was measured at, and it is a property of the pool, which is why it
#: is an argument with a measured default and not a law in the loop.
MAX_READERS = 16
#: A hold ends at half the number that started it; defined once in
#: :mod:`prismabuild.storage_tiers`, which the worker's pool-contention check
#: shares (#1010).
HOLD_RELEASE_FRACTION = storage_tiers.HOLD_RELEASE_FRACTION
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
#: A ZFS pool is this fleet's stage tier when its name carries this prefix
#: (#582).  The pool's *name* is the declaration, which is why the rule is
#: not "any unused SSD": on dl380g10 ``nvme0n1p1`` still carries a stale
#: ``zfs_member`` signature from a pool that no longer exists, and a rule
#: that took unused devices would seize it before anybody approved it.
STAGE_POOL_PREFIX = "prismabuild-stage"
#: The only spelling a stage member reaches a record under.  Device numbers
#: are not identity: on dl380g10 the ``nvmeXn1`` numbers are the reverse of
#: what the model names say, so a record naming ``nvme0n1`` -- or an operator
#: acting on one -- would name the box's root device.
BY_ID = "/dev/disk/by-id"
#: The dataset the stage's bytes are written to, when the pool carries one.
#: The pool is created with ``prismabuild-stage/prewarm`` so the tier's own
#: properties -- and, later, its export -- belong to the dataset rather than
#: to the pool root; a pool without it is written at its own mountpoint.
STAGE_DATASET = "prewarm"
#: ZFS's own reserve, in ZFS's own arithmetic, for the one case where this
#: loop cannot ask ZFS for it.  ``zfs get available`` on a dataset has
#: already withheld the pool's slop space -- the reserve a normal write can
#: never consume, ``spa_slop_shift`` -- so a tier sized from ``available``
#: needs no floor of this module's invention and takes none.  Only the
#: fallback to pool-level ``free`` needs one, and the honest value there is
#: the reserve ZFS itself would have withheld: 1/32 of the pool, never less
#: than 128 MiB.  A round number picked for how it looked would be a
#: heuristic standing where an explicit exists.
STAGE_SLOP_SHIFT = 5
STAGE_SLOP_MINIMUM = 128 << 20
#: Separates a stage object's mirrored path from the byte range it holds.
STAGE_OBJECT_MARK = ".pbstage@"
#: Where a staged object carries the identity of what it copied.  The key
#: names a path and a range, which is what a consumer looks an object up by;
#: it says nothing about *which version* of that path was copied, and the
#: manifests this tier exists for carry ``sha256: null`` on every entry, so
#: the manifest cannot break the tie either.  A source rewritten to the same
#: length between the copy and the read would be shadowed by a stale object
#: that nothing could detect.  The three numbers below are already in hand --
#: the reader fstats the source to check it is a regular file -- so the cost
#: is one syscall on a file that is about to be closed anyway.  An extended
#: attribute rather than a sidecar: a second file per object would double the
#: inode count of a 469 008-entry window.
STAGE_SOURCE_XATTR = "user.pbstage.source"
#: ``<st_size>:<st_mtime_ns>:<st_ino>`` of the source, when it was copied.
STAGE_SOURCE_FORMAT = "st_size:st_mtime_ns:st_ino at copy time"
#: A temporary left by a copy that never finished.  ``<key>.<pid>.<tid>.tmp``:
#: matched exactly rather than by suffix, so nothing this loop did not write
#: is ever unlinked.
_STAGE_TEMPORARY = re.compile(r"\.pbstage@\d+\+\d+\.(\d+)\.\d+\.tmp$")
#: Stage roots this process has already reaped.  A crash or a SIGKILL leaves
#: one temporary per in-flight reader, and nothing else ever removes them:
#: ``release`` unlinks by key and the sweep is driven from receipts, so they
#: are invisible to both and only shrink the tier.  Once per process, not
#: once per cycle -- a live temporary belongs to a copy in flight.
_REAPED_STAGE_ROOTS: set[str] = set()
#: Written into every stage record, because the layout *is* the consumer
#: contract and a reader of the record must not have to guess it.
STAGE_LAYOUT = (
    "mirror of the manifest's mount prefix, one object per manifest entry, "
    "named <relative path>.pbstage@<offset>+<bytes>.  A manifest entry is a "
    "byte range, so an object is a range: this tree is a residency-map "
    "source and deliberately not an overlay lower layer, which would serve a "
    "staged range as a whole short file"
)
#: The honest half of every stage record.  Staging writes bytes; it does not
#: make a consumer read them, and the two are separate claims (#582).
STAGE_CONSUMER = {
    "reads_stage": False,
    "verified_reads": None,
    "effect": "copy_only",
    "reason": (
        "no consumer reads this stage: PrismaBuild publishes no residency "
        "map and the export is served from the pool path.  The ARC is keyed "
        "by the on-pool block pointer, so a staged copy does not warm the "
        "path a consumer reads.  Staged bytes are a copy, never a saved read"
    ),
}
#: A write that fails for one of these has exhausted the tier rather than hit
#: a fault, and the tier's state says so for the rest of the cycle.
_STAGE_FULL_ERRNOS = frozenset({errno.ENOSPC, errno.EDQUOT, errno.EFBIG})
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


def pool_member_paths(
    pool: str,
    *,
    runner: Callable[[list[str]], str] | None = None,
) -> list[str] | None:
    """Every data-vdev leaf of ``pool``, as ``zpool status -P`` prints it.

    Split out of :func:`pool_member_devices` so the stage tier can bind the
    same leaves to their ``/dev/disk/by-id`` names (#582) without a second
    copy of this parse: two parses of one topology is how the two disagree.
    ``None`` when the pool cannot be read at all.
    """

    try:
        # One subprocess boundary for every topology question this module
        # asks, so a caller that can answer one can answer all of them.
        text = (runner or run_tool)([zpool_binary(), "status", "-P", pool])
    except (OSError, subprocess.SubprocessError):
        return None

    leaves: list[str] = []
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
        if name not in leaves:
            leaves.append(name)
    return leaves


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

    devices: list[str] = []
    for name in pool_member_paths(pool, runner=runner) or []:
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
    return storage_tiers.read_disk_stat(device, root=root)


# ------------------------------------------------------------- stage tier


def zfs_binary() -> str:
    """``zfs``'s path, resolved the same way ``zpool``'s is and for the same
    reason: a supervised role's ``PATH`` need not carry ``/usr/sbin``, and a
    lookup that fails there would report a stage pool as having no mountpoint
    rather than as unreadable."""

    found = shutil.which("zfs")
    if found:
        return found
    for candidate in ("/usr/sbin/zfs", "/sbin/zfs"):
        if os.path.exists(candidate):
            return candidate
    return "zfs"


def run_tool(argv: list[str]) -> str:
    return subprocess.run(
        argv, check=True, text=True, timeout=30,
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL).stdout


def stage_pool_rows(prefix: str,
                    runner: Callable[[list[str]], str] | None = None,
                    ) -> list[dict[str, object]] | None:
    """Every imported pool whose name carries ``prefix``, with exact bytes.

    ``zpool list -Hp`` prints bytes rather than the rounded ``745G`` a human
    sees, so capacity comes from the pool's own arithmetic and never from a
    parsed suffix.  ``None`` means ``zpool`` could not be run at all, which is
    a different fact from ``[]`` -- no stage pool is imported -- and the two
    reach the record as ``unreadable`` and ``absent``.

    The prefix is the device's own declaration, the way a ZFS label declares a
    data member.  Deliberately *not* "any unused SSD": on dl380g10
    ``nvme0n1p1`` still carries a stale ``zfs_member`` signature, and a rule
    that claimed unused devices would seize it before anybody approved it.
    """

    try:
        text = (runner or run_tool)([
            zpool_binary(), "list", "-Hp",
            "-o", "name,size,allocated,free,health"])
    except (OSError, subprocess.SubprocessError):
        return None
    rows: list[dict[str, object]] = []
    for line in text.splitlines():
        fields = line.split("\t") if "\t" in line else line.split()
        if len(fields) < 5 or not fields[0].startswith(prefix):
            continue
        try:
            size, allocated, free = (int(field) for field in fields[1:4])
        except ValueError:
            continue
        rows.append({"name": fields[0], "size_bytes": size,
                     "allocated_bytes": allocated, "free_bytes": free,
                     "health": fields[4]})
    return rows


def pool_mountpoint(name: str,
                    runner: Callable[[list[str]], str] | None = None,
                    ) -> str | None:
    """Where the stage pool is mounted, asked of ZFS rather than assumed.

    ``/<pool>`` is only the default; a pool with ``mountpoint=legacy`` or
    ``none`` has no directory to write into, and that is a tier this loop
    must report as unreadable rather than write beside.
    """

    try:
        text = (runner or run_tool)(
            [zfs_binary(), "get", "-H", "-o", "value", "mountpoint", name])
    except (OSError, subprocess.SubprocessError):
        return None
    value = text.strip()
    return value if value.startswith("/") else None


def dataset_available(name: str,
                      runner: Callable[[list[str]], str] | None = None,
                      ) -> int | None:
    """Bytes ``name`` can still be written, as the dataset itself reports it.

    The number that describes the thing being written to.  ``zpool list``'s
    ``free`` is pool level and has withheld nothing: not the pool's slop
    space, not this dataset's quota or reservation, not the metadata the
    write itself will cost.  A budget taken from it never binds -- the loop
    reaches the kernel's ENOSPC before it reaches its own hard stop, every
    time the tier fills -- and a stop that never binds is not a stop.

    ``None`` when the dataset cannot answer, which is a fact the caller
    records rather than a number it substitutes.
    """

    try:
        text = (runner or run_tool)(
            [zfs_binary(), "get", "-Hp", "-o", "value", "available", name])
    except (OSError, subprocess.SubprocessError):
        return None
    try:
        value = int(text.strip())
    except ValueError:
        return None
    return value if value >= 0 else None


def by_id_names(paths: "list[str] | tuple[str, ...]", *,
                by_id: str | None = None) -> list[str]:
    """``/dev/disk/by-id`` names for ``paths``, in the order given.

    A stage member reaches a record under this name and no other.  Device
    numbering is not identity: on dl380g10 the ``nvmeXn1`` numbers are the
    reverse of what the model names suggest, and a record -- or an operator
    acting on one -- that said ``nvme0n1`` would name the box's root device.
    An unresolvable path contributes ``"unresolved:<basename>"`` rather than
    a device number, so the gap is visible and still not a number.

    Where several links resolve to one device the shortest model-serial name
    wins, so a record says ``nvme-LT0800KEXVA_CVMD54710026800BGN`` rather
    than its ``nvme-nvme.8086-...`` twin.
    """

    # Read when it is called, never bound when this module is defined: a test
    # -- and a box with a different device tree -- must be able to repoint it.
    by_id = by_id or BY_ID
    resolved: dict[str, list[str]] = {}
    try:
        entries = sorted(os.listdir(by_id))
    except OSError:
        entries = []
    for name in entries:
        try:
            target = os.path.realpath(os.path.join(by_id, name))
        except OSError:
            continue
        resolved.setdefault(target, []).append(name)
    names: list[str] = []
    for path in paths:
        try:
            target = os.path.realpath(path)
        except OSError:
            target = ""
        candidates = [n for n in resolved.get(target, [])
                      if not n.startswith("nvme-nvme.")]
        candidates = candidates or resolved.get(target, [])
        names.append(min(candidates, key=len) if candidates
                     else f"unresolved:{os.path.basename(path)}")
    return names


def stage_prefix_head(mount_prefix: str) -> str:
    """``mount_prefix`` normalised once, with its separator, for a whole pass.

    Every entry of a manifest shares one prefix, and normalising it per entry
    is most of what made a release cost a core: these are 469 008-entry
    manifests and a cycle asks about every entry of every live row.
    """

    prefix = os.path.normpath(str(mount_prefix or ""))
    if not prefix or prefix == ".":
        return ""
    return prefix if prefix.endswith(os.sep) else prefix + os.sep


def stage_entry_id(entry: Mapping[str, object],
                   head: str) -> "tuple[str, int, int] | None":
    """One entry's stage object as ``(relative path, offset, bytes)``.

    The identity a release compares by.  It is the key's own three parts,
    unformatted: building the string cost ``os.path.relpath``, which calls
    ``os.getcwd`` for every entry it is given, and the comparison never
    needed the string.  Relative to the manifest's prefix rather than
    absolute, because two rows with different mount prefixes still meet on
    the same object in the stage tree.

    ``head`` is :func:`stage_prefix_head`'s answer, so the prefix is
    normalised once per pass rather than once per entry.
    """

    path = str(entry.get("path", ""))
    if not path or not head:
        return None
    relative: "str | None" = None
    if path.startswith(head):
        candidate = path[len(head):]
        parts = candidate.split(os.sep)
        # An already-normalised remainder is its own relative form.  Anything
        # else -- an empty component, a ``.``, a ``..`` -- falls through to
        # the exact arithmetic below rather than being guessed at.
        if candidate and all(parts) and not any(
                part in (os.curdir, os.pardir) for part in parts):
            relative = candidate
    if relative is None:
        try:
            relative = os.path.relpath(os.path.normpath(path),
                                       os.path.normpath(head))
        except ValueError:
            return None
        if (relative.startswith("..") or os.path.isabs(relative)
                or relative == "."):
            return None
        if any(part == ".." for part in relative.split(os.sep)):
            return None
    return (relative, int(entry.get("offset", 0) or 0),
            int(entry.get("bytes", 0) or 0))


def stage_object_name(identity: "tuple[str, int, int]") -> str:
    """A stage object's name, formatted from its identity.

    Formatted only for an object that is about to be unlinked: a cycle that
    compared by name paid for 469 008 strings it never used.
    """

    relative, offset, size = identity
    return f"{relative}{STAGE_OBJECT_MARK}{offset}+{size}"


def stage_object_key(entry: Mapping[str, object], mount_prefix: str) -> str | None:
    """The stage's name for one manifest entry, or ``None`` if it has none.

    A manifest entry is a *byte range* of a file, not a file.  Writing a range
    under the file's own mirrored name would leave a short file that an
    overlay would serve as the whole thing, so every stage object carries its
    range in its name and the tree is deliberately **not** an overlay lower
    layer.  It is the source a residency map is written from: a consumer is
    handed ``(path, offset, bytes) -> stage object``, which is the consumer
    contract #583's design names first.  An overlay would need a whole-file
    manifest contract that nothing declares today.

    ``None`` for a path outside the manifest's own mount prefix, or one whose
    relative form escapes it.  The manifest is submitter-supplied, so the one
    rule that matters here is that nothing this function returns can name a
    path outside the stage root.
    """

    identity = stage_entry_id(entry, stage_prefix_head(mount_prefix))
    return None if identity is None else stage_object_name(identity)


class StageTier:
    """A second destination for the window the reader is already reading.

    The same ``Reader``, the same ``DiskPacer`` and the same window arithmetic
    fetch the bytes; this class is where a copy of them lands on an SSD pool
    whose name declares it a stage.  It owns three things and nothing else:
    the tier's discovered identity, a byte budget taken from the pool's own
    ``free``, and the delete-behind release that keeps the tier bounded.

    What it does **not** own is any claim that staging helped.  No consumer
    reads *these* objects: they are named for their byte range under
    :data:`STAGE_LAYOUT`, no residency map names them, and nothing opens them.
    A consumer that reads the stage reads ``stage_move``'s tree on the same
    dataset, by the map composed from that mover's fragments (#583, #634).
    Every record this class writes says so in ``consumer``, beside the bytes,
    because a staged byte nobody reads is a copy and not a saved read -- and a
    receipt that reported the copy as a win would be the #585 shape again, one
    step further up the stack.

    The dataset itself runs ``primarycache=all`` since 2026-09-18 (#638), for
    the mover's tree rather than for this one: a stage the ARC may not cache
    serves every consumer read off the SSD, 2402 MB/s against 10045.  So an
    object written here now costs a second ARC copy of the window this loop is
    warming, which is the trade that setting makes.
    """

    def __init__(self, *, state: str, reason: str, name: str = "",
                 mountpoint: str = "", members: "list[str] | None" = None,
                 size_bytes: int = 0, free_bytes: int = 0,
                 allocated_bytes: int = 0, health: str = "",
                 free_floor_bytes: int = 0, pools: "list[str] | None" = None,
                 dataset: str = "", capacity_bytes: int | None = None,
                 capacity_source: str = "") -> None:
        self.state = state
        self.reason = reason
        self.name = name
        self.dataset = dataset
        self.mountpoint = mountpoint
        self.members = list(members or [])
        self.size_bytes = size_bytes
        self.free_bytes = free_bytes
        self.allocated_bytes = allocated_bytes
        self.health = health
        self.free_floor_bytes = free_floor_bytes
        self.pools = list(pools or [])
        #: What the tier may still be given, and which number that came from.
        #: The dataset's own ``available`` when ZFS answered, the pool's
        #: ``free`` when it did not -- and the record says which, because a
        #: budget and the arithmetic behind it are one fact.
        self.capacity_bytes = (free_bytes if capacity_bytes is None
                               else int(capacity_bytes))
        self.capacity_source = capacity_source or "unread"
        self.budget_bytes = (max(0, self.capacity_bytes - free_floor_bytes)
                             if mountpoint else 0)
        self.staged_bytes = 0
        self.staged_entries = 0
        self.reserved_bytes = 0
        self.released_bytes = 0
        self.released_entries = 0
        #: Objects this tier was asked to release and could not.  A sweep
        #: that marked a row swept on top of one of these would be claiming
        #: bytes are gone that are still on the disk.
        self.release_failures = 0
        #: Bytes written to a temporary that never earned a name, and the
        #: objects they belong to.  ``staged_bytes`` counts only what was
        #: committed, so on a tier that fills mid-window the work the cycle
        #: actually did is invisible without these -- and whether the window
        #: is sized wrong is exactly the question they answer.
        self.aborted_bytes = 0
        self.aborted_entries = 0
        #: Objects committed without their source identity, because the
        #: filesystem refused the extended attribute.  Counted rather than
        #: fatal: identity is what makes a stale object detectable, not what
        #: makes the copy correct.
        self.identity_unrecorded = 0
        #: What a previous process left behind and this one removed.
        self.reaped_entries = 0
        self.reaped_bytes = 0
        #: Seconds spent inside ``write(2)`` on the stage.  The copy happens
        #: in the reader thread's own block cycle, so with the stage on
        #: ``per_reader_mb_s`` reads lower; this is the field that says why.
        self.write_seconds = 0.0
        self.errors: list[str] = []
        self.lock = threading.Lock()

    # -- state ---------------------------------------------------------

    @property
    def usable(self) -> bool:
        return self.state == "present"

    def note(self, message: str) -> None:
        with self.lock:
            if len(self.errors) < 20:
                self.errors.append(message)

    def mark_faulted(self, reason: str) -> None:
        """The tier answered with something other than "no room".

        EIO under ``failmode=continue``, ESTALE, EROFS: the device is there
        and is not working.  Without a state for it every receipt in the
        cycle still said ``present``, ``staged_bytes: 0`` and a reason that
        described a healthy tier, while the loop reopened and refailed once
        per manifest entry -- 469 008 times on a window of that size, into an
        error list that stops at twenty.  The state is the fact, and it also
        ends the retry: ``usable`` is false, so the reader stops asking.
        """

        with self.lock:
            if self.state == "present":
                self.state = "faulted"
                self.reason = reason

    def mark_full(self, reason: str) -> None:
        """The tier ran out mid-cycle.  The warm continues; the stage stops.

        A stage that filled is a fact about the tier, so it becomes the tier's
        state for this cycle rather than an error buried in a list.  The read
        itself is untouched: the ARC destination is the one the loop has
        always had, and losing the second destination must never cost the
        first.
        """

        with self.lock:
            if self.state != "full":
                self.state = "full"
                self.reason = reason

    # -- writing -------------------------------------------------------

    def reserve(self, size: int) -> bool:
        with self.lock:
            if self.state != "present":
                return False
            if self.staged_bytes + self.reserved_bytes + size > self.budget_bytes:
                return False
            self.reserved_bytes += size
            return True

    def settle(self, reserved: int, written: int, *, committed: bool) -> None:
        with self.lock:
            self.reserved_bytes -= reserved
            if committed:
                self.staged_bytes += written
                self.staged_entries += 1
            else:
                self.aborted_bytes += written
                self.aborted_entries += 1

    def note_unrecorded_identity(self) -> None:
        with self.lock:
            self.identity_unrecorded += 1

    def add_write_seconds(self, seconds: float) -> None:
        with self.lock:
            self.write_seconds += max(0.0, seconds)

    def object_path(self, key: str) -> Path:
        return Path(self.mountpoint) / key

    def open_object(self, key: str, declared: int) -> "StageObject | None":
        """A temporary file for one entry, renamed into place only when whole.

        ``O_EXCL`` and ``O_NOFOLLOW`` on a name carrying this process and
        thread: two readers of the same range never share a temporary, and a
        symlink planted under the stage root is refused rather than followed.
        A partial object is never renamed, so nothing under the stage root is
        ever a short file wearing a complete name.

        ``declared`` is the entry's own ``bytes``, carried here so that
        :meth:`StageObject.commit` can refuse a rename the object's content
        does not earn.  The key already states that number; an object whose
        content is shorter than its name would be a lie a consumer cannot
        detect, so the number travels with the handle that has to honour it.
        """

        target = self.object_path(key)
        temporary = target.with_name(
            f"{target.name}.{os.getpid()}.{threading.get_ident()}.tmp")
        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            handle = os.open(
                str(temporary),
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
                0o644)
        except OSError as exc:
            self.note(f"{key}: {exc}")
            if exc.errno in _STAGE_FULL_ERRNOS:
                self.mark_full(f"stage pool out of space: {exc}")
            else:
                self.mark_faulted(f"stage pool fault: {exc}")
            return None
        return StageObject(self, key, handle, temporary, target,
                           declared=int(declared))

    # -- release -------------------------------------------------------

    def release(self, keys: "list[str]") -> None:
        """Delete staged objects, counting only the bytes actually removed.

        A tier with no mountpoint has no objects to name: ``object_path``
        would build a path relative to this process's working directory, and
        an unlink there is a write somewhere nobody asked for.  Refused, and
        counted as a failure so that nothing downstream reads the empty
        result as "already released".
        """

        if not self.mountpoint:
            self.note(f"release refused: the tier has no mountpoint "
                      f"({len(keys)} objects)")
            with self.lock:
                self.release_failures += len(keys)
            return
        for key in keys:
            path = self.object_path(key)
            try:
                size = path.stat().st_size
                path.unlink()
            except FileNotFoundError:
                # Already gone is released: a band re-staged in a later life
                # names objects an earlier sweep removed.
                continue
            except OSError as exc:
                self.note(f"{key}: {exc}")
                with self.lock:
                    self.release_failures += 1
                continue
            with self.lock:
                self.released_bytes += size
                self.released_entries += 1

    def reap_temporaries(self) -> None:
        """Remove temporaries left by a process that is no longer here.

        Only names this loop's own writer builds, and never this process's
        own: a temporary carrying our pid belongs to a copy in flight.
        """

        if not self.mountpoint:
            return
        mine = str(os.getpid())
        for path in Path(self.mountpoint).rglob("*.tmp"):
            found = _STAGE_TEMPORARY.search(path.name)
            if not found or found.group(1) == mine:
                continue
            try:
                size = path.stat().st_size
                path.unlink()
            except OSError as exc:
                self.note(f"{path.name}: {exc}")
                continue
            self.reaped_entries += 1
            self.reaped_bytes += size

    # -- record --------------------------------------------------------

    def record(self) -> dict[str, object]:
        return {
            "state": self.state,
            "reason": self.reason,
            "pool": self.name,
            "dataset": self.dataset,
            "pools_seen": self.pools,
            "mountpoint": self.mountpoint,
            # By-id only.  A device number never enters a record: the numbers
            # on this box are the reverse of what the model names suggest.
            "members": self.members,
            "health": self.health,
            "size_bytes": self.size_bytes,
            "free_bytes": self.free_bytes,
            "allocated_bytes": self.allocated_bytes,
            "capacity_bytes": self.capacity_bytes,
            "capacity_source": self.capacity_source,
            "free_floor_bytes": self.free_floor_bytes,
            "budget_bytes": self.budget_bytes,
            "staged_bytes": self.staged_bytes,
            "staged_entries": self.staged_entries,
            # Written and then discarded: a refused rename, a tier that filled
            # mid-entry, a read that stopped short.  Never folded into
            # ``staged_bytes``, which is committed objects and nothing else.
            "aborted_bytes": self.aborted_bytes,
            "aborted_entries": self.aborted_entries,
            "stage_write_seconds": round(self.write_seconds, 3),
            "released_bytes": self.released_bytes,
            "released_entries": self.released_entries,
            "release_failures": self.release_failures,
            "layout": STAGE_LAYOUT,
            # What a validator reads to tell a staged object from a stale one.
            "object_identity": {"xattr": STAGE_SOURCE_XATTR,
                                "format": STAGE_SOURCE_FORMAT},
            "identity_unrecorded": self.identity_unrecorded,
            "reaped_tmp_entries": self.reaped_entries,
            "reaped_tmp_bytes": self.reaped_bytes,
            "consumer": dict(STAGE_CONSUMER),
            "errors": list(self.errors),
        }


class StageObject:
    """One entry's staged copy, in flight."""

    def __init__(self, tier: StageTier, key: str, handle: int,
                 temporary: Path, target: Path, *, declared: int = 0) -> None:
        self.tier = tier
        self.key = key
        self.handle = handle
        self.temporary = temporary
        self.target = target
        #: The byte count this object's own name declares.
        self.declared = int(declared)
        #: The source's identity, as the reader's own ``fstat`` saw it.
        self.source: "os.stat_result | None" = None
        self.written = 0
        self.reserved = 0
        self.live = True

    def write(self, data: memoryview) -> bool:
        if not self.live:
            return False
        size = len(data)
        if not self.tier.reserve(size):
            self.tier.mark_full("stage budget exhausted for this cycle")
            self.abort()
            return False
        self.reserved += size
        started = time.perf_counter()
        moved = 0
        try:
            while moved < size:
                # ``write(2)`` is allowed to transfer fewer bytes than it was
                # given, and on a pool that runs out partway through a request
                # it does exactly that rather than raising.  A caller that
                # trusted the request length would count bytes the kernel
                # never took and then rename a short object onto a name that
                # declares the full range.  Drain the view or fail the sink.
                taken = os.write(self.handle, data[moved:])
                if taken <= 0:
                    raise OSError(errno.EIO,
                                  "stage write accepted no bytes")
                moved += taken
        except OSError as exc:
            self.written += moved
            self.tier.add_write_seconds(time.perf_counter() - started)
            self.tier.note(f"{self.key}: {exc}")
            if exc.errno in _STAGE_FULL_ERRNOS:
                self.tier.mark_full(f"stage pool out of space: {exc}")
            else:
                self.tier.mark_faulted(f"stage pool fault: {exc}")
            self.abort()
            return False
        self.tier.add_write_seconds(time.perf_counter() - started)
        self.written += moved
        return True

    def identify(self, source: "os.stat_result") -> None:
        """Carry the source's identity, from the ``fstat`` already taken."""

        self.source = source

    def _record_identity(self) -> None:
        if self.source is None:
            self.tier.note_unrecorded_identity()
            return
        value = (f"{self.source.st_size}:{self.source.st_mtime_ns}:"
                 f"{self.source.st_ino}")
        try:
            os.setxattr(self.handle, STAGE_SOURCE_XATTR, value.encode())
        except OSError as exc:
            # A filesystem without user extended attributes still stages; it
            # stages objects a validator cannot date, and the record says how
            # many.
            self.tier.note(f"{self.key}: identity not recorded: {exc}")
            self.tier.note_unrecorded_identity()

    def commit(self) -> None:
        """Rename the temporary into place; only a whole entry gets a name.

        Whole is measured against the entry's declared ``bytes``, not against
        what the reader thinks it handed over: the name states a byte range,
        so an object whose content is shorter than its name is the one thing
        this tree must never contain.
        """

        if not self.live:
            return
        self.live = False
        if self.written != self.declared:
            self.tier.note(f"{self.key}: wrote {self.written} B of the "
                           f"{self.declared} B its name declares")
            try:
                os.close(self.handle)
            except OSError:
                pass
            self._discard()
            self.tier.settle(self.reserved, self.written, committed=False)
            return
        self._record_identity()
        try:
            os.close(self.handle)
            os.replace(self.temporary, self.target)
        except OSError as exc:
            self.tier.note(f"{self.key}: {exc}")
            self._discard()
            self.tier.settle(self.reserved, self.written, committed=False)
            return
        self.tier.settle(self.reserved, self.written, committed=True)

    def abort(self) -> None:
        if not self.live:
            return
        self.live = False
        try:
            os.close(self.handle)
        except OSError:
            pass
        self._discard()
        self.tier.settle(self.reserved, self.written, committed=False)

    def _discard(self) -> None:
        try:
            self.temporary.unlink()
        except OSError:
            pass


def discover_stage(prefix: str, *,
                   runner: Callable[[list[str]], str] | None = None,
                   by_id: str | None = None,
                   free_floor_bytes: int | None = None) -> StageTier:
    """Read the stage tier off the box, every cycle, or say why there is none.

    Rediscovered per cycle on purpose: a stage pool created, exported, filled
    or lost between two polls changes the answer, and a tier remembered from
    an earlier cycle is a claim about the box that stopped being checked.

    The five states are the whole vocabulary, and one of them is always
    recorded.  Discovery can return four of them; ``faulted`` is reached only
    during a cycle, by a tier that was discovered ``present`` and then
    answered a write with something other than "no room":

    ``absent``
        No imported pool carries the prefix.  This is the answer on every box
        but the file server, and on the file server until the pool exists.
    ``unreadable``
        ``zpool`` could not be run, the pool has no mounted directory, that
        directory cannot be written, or the pool's health is neither ONLINE
        nor DEGRADED.  The tier is there and cannot be used.
    ``full``
        The tier is usable and its capacity leaves nothing above the floor.
        Capacity is the ``prewarm`` dataset's own ``available`` -- which has
        already withheld the pool's slop, this dataset's quota and its
        reservation -- and the pool's ``free`` only when the dataset cannot
        answer.  Never a configured size.
    ``present``
        Usable, with budget.  It does not mean anything was staged.
    ``faulted``
        A write failed for a reason that is not exhaustion -- EIO under
        ``failmode=continue``, ESTALE, EROFS.  The tier stops being written
        to for the rest of the cycle and the next cycle rediscovers it.
    """

    #: ``None`` means derived below, from whichever capacity number answers.
    #: An operator who states a floor outranks the derivation.
    stated_floor = free_floor_bytes
    free_floor_bytes = 0 if stated_floor is None else int(stated_floor)
    rows = stage_pool_rows(prefix, runner=runner)
    if rows is None:
        return StageTier(state="unreadable",
                         reason="zpool list could not be run on this host",
                         free_floor_bytes=free_floor_bytes)
    if not rows:
        return StageTier(
            state="absent",
            reason=f"no imported pool is named {prefix}*",
            free_floor_bytes=free_floor_bytes)
    rows.sort(key=lambda row: str(row["name"]))
    chosen = rows[0]
    names = [str(row["name"]) for row in rows]
    name = str(chosen["name"])
    health = str(chosen["health"])
    common = dict(name=name, health=health, pools=names,
                  size_bytes=int(chosen["size_bytes"]),
                  free_bytes=int(chosen["free_bytes"]),
                  allocated_bytes=int(chosen["allocated_bytes"]),
                  free_floor_bytes=free_floor_bytes)
    if health not in ("ONLINE", "DEGRADED"):
        return StageTier(state="unreadable",
                         reason=f"{name} health is {health}", **common)
    dataset = f"{name}/{STAGE_DATASET}"
    mountpoint = pool_mountpoint(dataset, runner=runner)
    if not mountpoint:
        dataset = name
        mountpoint = pool_mountpoint(name, runner=runner)
    common["dataset"] = dataset
    if not mountpoint:
        return StageTier(
            state="unreadable",
            reason=f"{name} has no mounted directory to write into", **common)
    if not os.path.isdir(mountpoint) or not os.access(mountpoint, os.W_OK):
        return StageTier(
            state="unreadable",
            reason=f"{mountpoint} is not a writable directory on this host",
            mountpoint=mountpoint, **common)
    leaves = pool_member_paths(name, runner=runner)
    members = by_id_names(leaves or [], by_id=by_id)
    # Ask the dataset what it can still take.  Falling back to the pool's
    # ``free`` is a real answer to a different question, so it is recorded as
    # such and carries the reserve ZFS would have withheld from it.
    available = dataset_available(dataset, runner=runner)
    if available is None:
        capacity, source = int(chosen["free_bytes"]), "pool free"
        if stated_floor is None:
            free_floor_bytes = max(
                int(chosen["size_bytes"]) >> STAGE_SLOP_SHIFT,
                STAGE_SLOP_MINIMUM)
    else:
        capacity, source = available, "dataset available"
    common["free_floor_bytes"] = free_floor_bytes
    tier = StageTier(state="present", reason="stage pool discovered",
                     mountpoint=mountpoint, members=members,
                     capacity_bytes=capacity, capacity_source=source,
                     **common)
    if tier.budget_bytes <= 0:
        tier.state = "full"
        tier.reason = (f"{name} has {capacity} B of {source} against a "
                       f"{free_floor_bytes} B floor")
    return tier


def stage_ids_between(entries: "list[dict[str, object]]", head: str,
                      start: int, end: int) -> "list[tuple[str, int, int]]":
    """The objects the reader has finished with in ``(start, end]``.

    An entry counts as passed when its *last* byte is behind ``end``: an entry
    straddling either edge was staged whole, because a warm reads files and
    not byte ranges, and half of one is not releasable.

    An empty band returns without walking the manifest.  This runs for every
    window of every cycle whether or not a frontier moved, so a walk up to
    ``end`` on a row with nothing to release was most of the cost of a poll.
    A repeated range -- a v2 read plan may name one twice -- is one object,
    so the band is deduplicated in read order.
    """

    if end <= start:
        return []
    ids: "dict[tuple[str, int, int], None]" = {}
    position = 0
    for entry in entries:
        position += int(entry.get("bytes", 0) or 0)
        if position > end:
            break
        if position <= start:
            continue
        identity = stage_entry_id(entry, head)
        if identity:
            ids[identity] = None
    return list(ids)


def stage_ids_wanted(entries: "list[dict[str, object]]", head: str,
                     consumed: int,
                     candidates: "set[tuple[str, int, int]]",
                     ) -> "set[tuple[str, int, int]]":
    """Which of ``candidates`` this row has *not* read yet.

    Asked of every live row, including the row doing the releasing: a read
    timeline may name the same range in two phases, and the later one is a
    read that has not happened.

    The stage tree is a mirror keyed by path and range, so two rows over the
    same bytes -- which is the shape measured here: 469 007 of 469 008 entries
    identical across three prepare manifests -- name the same objects.  A
    release that only asked the row in front of it would delete the bytes the
    row behind it has not reached.  Nothing reads the stage today, so nothing
    breaks today; the predicate is path-exact now so that it is still correct
    on the day a consumer arrives.
    """

    wanted: "set[tuple[str, int, int]]" = set()
    position = 0
    for entry in entries:
        position += int(entry.get("bytes", 0) or 0)
        if position <= consumed:
            continue
        identity = stage_entry_id(entry, head)
        if identity in candidates:
            wanted.add(identity)
            if len(wanted) == len(candidates):
                # Nothing of this band can survive being wanted twice, so
                # the rest of this manifest cannot change the answer.
                break
    return wanted


def release_stage_band(tier: StageTier, entries: "list[dict[str, object]]",
                       mount_prefix: str, *, start: int, end: int,
                       others: "Callable[[], list[tuple[list, str, int]]]",
                       apply: bool = True) -> dict[str, object]:
    """Delete the band another row does not still want, and say what it kept.

    ``others`` is called only when there is something to delete, because it
    loads manifests: a cycle with no advanced frontier must not pay for the
    rows it would have asked.

    ``apply=False`` is a plan: the same predicate, the same answer, nothing
    unlinked.  A dry run reads the box and writes nothing to it, and deleting
    a staged object is a write however little it looks like one.
    """

    head = stage_prefix_head(mount_prefix)
    candidates = stage_ids_between(entries, head, start, end)
    result: dict[str, object] = {
        "candidate_entries": len(candidates), "retained_entries": 0,
        "deletable_entries": 0, "released_entries": 0, "released_bytes": 0,
        "failed_entries": 0,
    }
    if not candidates:
        return result
    pending = set(candidates)
    # The releasing row is asked first, about its own band.  A v2 read plan
    # may read a range again in a later phase -- forward [0, 1], compute,
    # reverse [1, 0] is the documented shape -- and an object the current
    # phase has passed is then an object the next phase reads.  Excluding the
    # row that is releasing would delete its own revisit at the moment the
    # revisit begins.
    pending -= stage_ids_wanted(entries, head, end, pending)
    for other_entries, other_prefix, other_consumed in (
            others() if pending else ()):
        pending -= stage_ids_wanted(other_entries,
                                    stage_prefix_head(other_prefix),
                                    other_consumed, pending)
        if not pending:
            break
    result["retained_entries"] = len(candidates) - len(pending)
    result["deletable_entries"] = len(pending)
    if not apply:
        return result
    before_entries, before_bytes = tier.released_entries, tier.released_bytes
    before_failures = tier.release_failures
    # Formatted here and nowhere earlier: only the objects being unlinked
    # ever need their names.
    tier.release(sorted(stage_object_name(identity) for identity in pending))
    result["released_entries"] = tier.released_entries - before_entries
    result["released_bytes"] = tier.released_bytes - before_bytes
    result["failed_entries"] = tier.release_failures - before_failures
    return result


def parse_export_stats(text: str) -> dict[str, int]:
    """``io_read`` bytes per client address from ``/proc/fs/nfsd/export_stats``.

    The file is ``# Version 1.1`` comment lines, then one unindented
    ``<path> <client> <start-time>`` header per (export, client) block with
    its ``<stat>: <value>`` rows indented beneath it.  A client mounting two
    exports has two blocks; its bytes are one client's bytes, so they are
    summed.  A malformed row is skipped, not raised: this is read four times
    a second under the pacer's lock.
    """

    reads: dict[str, int] = {}
    client: str | None = None
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        if line[0] in " \t":
            if client is None:
                continue
            key, _, value = line.strip().partition(":")
            if key.strip() == "io_read":
                try:
                    reads[client] = reads.get(client, 0) + int(value)
                except ValueError:
                    pass
            continue
        fields = line.split()
        client = fields[1] if len(fields) >= 2 else None
    return reads


class ClientReadRate:
    """How fast this host's NFS server is feeding its clients, in MB/s.

    The pacer exists to protect those clients (#499), so the first question a
    hold has to answer is whether anybody is reading.  A pool busy with work
    that has no client behind it -- a scrub, a resilver, another tenant's
    write -- is not a reason to stop warming: on 2026-09-13 a ``zpool scrub``
    held the storage role for 5709 s at 100 % utilization and up to 26 s of
    backlog while no client read a byte, so the loop warmed nothing for hours
    and protected nobody.

    The second question is *which* client (#580).  The action this loop is
    warming for reads the very bytes the warm is fetching, and on 2026-09-17
    that consumer was the only client on the box: the loop held 11.03 h of
    20.6 h against it, every hold record carrying the consumer's own
    258-280 MB/s as the client it was protecting, and the warm never got
    ahead of the reader it exists to feed.  So the counter is read per client
    from ``export_stats`` and split three ways: ``total``, ``self`` -- the
    addresses of the box running the served action -- and ``other``, which
    is what a hold protects.  ``/proc/net/rpc/nfsd``'s aggregate ``io`` line
    is the fallback when the per-client file is unreadable; then nothing can
    be attributed, ``other`` is the total, and the pacer behaves exactly as it
    did before the split existed.

    This loop reads the pool through the host's own local path, never through
    the export, so its reads never appear in either counter and cannot make
    the pacer believe a client is busy.

    An unreadable counter is ``None``, and ``None`` means "clients may be
    reading": a host that cannot see its clients must behave like the host
    that can, not like an empty one.
    """

    def __init__(self, path: str = NFSD_IO,
                 clock: Callable[[], float] = time.monotonic,
                 reader: Callable[[], str | None] | None = None,
                 export_stats: str = "",
                 export_reader: Callable[[], str | None] | None = None) -> None:
        self.path = path
        #: Where the per-client counter lives.  ``pacer_from_args`` names it;
        #: a rate built without it reads only the aggregate line, which is
        #: also what keeps a fixture's counter from being overruled by the
        #: real file on a host that has one.
        self.export_stats = export_stats
        self.clock = clock
        self.reader = reader if reader is not None else self._read_file
        self.export_reader = (export_reader if export_reader is not None
                              else self._read_export_stats)
        self._previous: int | None = None
        self._previous_at = 0.0
        self._rate: float | None = None
        #: Per-client counters from the last sample, and the last rate each
        #: client was given: the rate is what carries a client across the
        #: interval in which its counter restarted.
        self._previous_clients: dict[str, int] | None = None
        self._client_rates: dict[str, float] = {}
        #: Where the last sample's total came from, for the receipt.
        self.source = ""
        self.self_mb_s: float | None = None
        self.other_mb_s: float | None = None

    def _read_file(self) -> str | None:
        try:
            with open(self.path) as handle:
                return handle.read()
        except OSError:
            return None

    def _read_export_stats(self) -> str | None:
        if not self.export_stats:
            return None
        try:
            with open(self.export_stats) as handle:
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

    def client_bytes(self) -> dict[str, int] | None:
        """Per-client served bytes, or ``None`` where the file cannot be read."""

        text = self.export_reader()
        if not text:
            return None
        return parse_export_stats(text)

    def sample(self, self_addresses: "tuple[str, ...] | list[str]" = ()) -> float | None:
        """MB/s served since the last sample, or ``None`` without an interval.

        ``self_addresses`` are the client addresses whose reads are the served
        action's own; after the call ``self_mb_s`` and ``other_mb_s`` carry
        the split.  Both come from one read of ``export_stats`` so the two
        halves are the same instant; the aggregate line is read only when
        that file is not there to read.
        """

        clients = self.client_bytes()
        now = self.clock()
        if clients is not None:
            return self._sample_clients(clients, now, tuple(self_addresses))
        self.source = self.path
        self._previous_clients, self._client_rates = None, {}
        total = self.served_bytes()
        if total is None:
            self._previous, self._previous_at, self._rate = None, now, None
            self.self_mb_s = self.other_mb_s = None
            return None
        previous, previous_at = self._previous, self._previous_at
        elapsed = now - previous_at
        if previous is None:
            self._previous, self._previous_at = total, now
            self.self_mb_s = self.other_mb_s = None
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
            self.self_mb_s = self.other_mb_s = None
            return None
        self._rate = (total - previous) / 1e6 / elapsed
        # Nothing can be attributed from the aggregate: every byte is a
        # client's, which is the verdict the pacer gave before #580.
        self.self_mb_s = None
        self.other_mb_s = self._rate
        return self._rate

    def _sample_clients(self, clients: dict[str, int], now: float,
                        self_addresses: tuple[str, ...]) -> float | None:
        self.source = self.export_stats
        self._previous = None
        previous, previous_at = self._previous_clients, self._previous_at
        elapsed = now - previous_at
        if previous is None:
            self._previous_clients, self._previous_at = clients, now
            self._client_rates = {}
            self._rate = None
            self.self_mb_s = self.other_mb_s = None
            return None
        if elapsed <= 0:
            return self._rate
        rates: dict[str, float] = {}
        for client, total in clients.items():
            before = previous.get(client)
            if before is not None and total >= before:
                rates[client] = (total - before) / 1e6 / elapsed
            elif client in self._client_rates:
                # The block was recreated and its counter restarted inside
                # this interval (every 901 s on the live box).  The bytes
                # served before the restart are gone from the file, and
                # reading the remainder as the whole interval would show a
                # client that suddenly read almost nothing -- or, subtracted
                # from a total, a phantom third party.  The last rate is the
                # better estimate for one interval; the next one is exact.
                rates[client] = self._client_rates[client]
            else:
                # A block this loop has never seen was created inside the
                # interval, so its whole counter is bytes served since then.
                rates[client] = total / 1e6 / elapsed
        # A client with no block read nothing since its block expired.
        self._previous_clients, self._previous_at = clients, now
        self._client_rates = rates
        total_rate = sum(rates.values())
        own = sum(rate for client, rate in rates.items() if client in self_addresses)
        self._rate = total_rate
        self.self_mb_s = own
        self.other_mb_s = max(0.0, total_rate - own)
        return total_rate


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

    def snapshot(self) -> dict[str, float | int]:
        """A mark to diff against: what this cycle started with.

        The ledger outlives a cycle by design, so a cycle that wants to
        report its own hold time diffs the ending against this mark
        rather than reading the lifetime totals (#575).
        """

        with self._lock:
            return {"holds": self.holds, "held_s": self.held_s,
                    "idle_s": self.idle_s, "active_s": self.active_s,
                    "samples": self.samples}


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
        readers: int = 1,
        max_readers: int = 0,
    ) -> None:
        # ``--disks`` is a comma-separated operator argument, so preserve the
        # first spelling/order but do not make a duplicate member look like a
        # second required stat row.
        self.devices = list(dict.fromkeys(devices))
        #: The two read depths this pacer admits (#580): ``readers`` while a
        #: client other than the served action is reading -- the depth #499
        #: measured to pass on the live fleet -- and ``max_readers`` while the
        #: pool serves nobody else, where depth is the only lever that gets
        #: the warm ahead of the reader it feeds.  0 means one tier.
        self.readers = max(1, int(readers))
        self.max_readers = max(self.readers, int(max_readers)) if max_readers else self.readers
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
        #: The split behind ``_clients_active``: what the served action's
        #: own box read, and what everyone else did.  ``other`` is the number
        #: the verdict reads.
        self._self_mb_s: float | None = None
        self._other_mb_s: float | None = None
        self._self_total = 0.0
        self._self_samples = 0
        self._shared_samples = 0
        self._client_samples = 0
        #: Whose reads are self for the row being warmed: the host the claim
        #: names, the client addresses its offer announced, and why the set
        #: is what it is when it is empty.
        self._served_host = ""
        self._served_addresses: tuple[str, ...] = ()
        self._served_reason = "no row"
        self._offer_age_s: float | None = None
        #: Start of the stretch of hold time not yet charged to a bucket, and
        #: the client state it is charged to.
        self._slice_at = 0.0
        self._slice_active = True
        self._totals = {"util_pct": 0.0, "read_await_ms": 0.0, "backlog_ms": 0.0}
        self._maxima = {"util_pct": 0.0, "read_await_ms": 0.0, "backlog_ms": 0.0}
        self._last: dict[str, float] = {}
        #: Bytes the pool's members delivered over the intervals this row
        #: sampled, and the wall seconds of those intervals.  A sum over
        #: members rather than a worst-member maximum: the question it answers
        #: is what the pool as a whole delivered, which is what a fill
        #: capacity is rationing.  Accumulated rather than averaged per sample
        #: so a long interval is not weighted like a short one.
        self._pool_read_bytes = 0
        self._pool_read_seconds = 0.0

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

    def begin_row(self, *, served_host: str = "",
                  served_addresses: "tuple[str, ...] | list[str]" = (),
                  served_reason: str = "", offer_age_s: float | None = None) -> None:
        """Start bounded per-row accounting without resetting the verdict.

        Rows are warmed sequentially, but share the same disk decision state:
        the previous stat row, cached verdict, completeness state, and an
        existing hold all survive the receipt boundary.  Only the counters
        shown on a row's receipt reset.  If a caller ever begins a row while a
        hold is active, that hold remains active and its future time belongs to
        the new row instead of being cleared.

        ``served_host`` and ``served_addresses`` say whose reads are *self*
        for this row (#580): the box that claimed the action whose window is
        being warmed, and the client addresses its offer announced.  A row
        nobody has claimed has no self -- its bytes are read for a future
        claimant, and every client reading now is a client to protect, which
        is #499's case exactly.  ``served_reason`` records why the set is
        empty when it is, so a receipt showing the old behaviour says which
        link of the identity chain was missing.
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
            self._self_total = 0.0
            self._self_samples = 0
            self._pool_read_bytes = 0
            self._pool_read_seconds = 0.0
            self._shared_samples = 0
            self._client_samples = 0
            self._served_host = str(served_host or "")
            self._served_addresses = tuple(str(a) for a in served_addresses)
            self._served_reason = str(served_reason or (
                "attributed" if self._served_addresses else "no served action"))
            self._offer_age_s = offer_age_s

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
        interval = storage_tiers.worst_member_interval(previous, current, elapsed)
        if interval is None:
            self._telemetry_complete = False
            return None
        worst, sectors = interval
        # Every member, over the same interval, whoever read: another mover,
        # the prewarm loop, an NFS consumer, a scrub.  That is the point --
        # what the pool delivered is the supply, and a mover's shortfall
        # against it is a fact about contention rather than about capacity.
        self._pool_read_bytes += sectors * 512
        self._pool_read_seconds += elapsed
        self._telemetry_complete = True
        return worst

    def _sample_clients_locked(self) -> None:
        self._client_samples += 1
        if self.client_rate is None:
            self._client_mb_s = None
            self._self_mb_s = self._other_mb_s = None
            self._clients_active = True
            self._shared_samples += 1
            return
        rate = self.client_rate.sample(self._served_addresses)
        self._client_mb_s = rate
        self._self_mb_s = self.client_rate.self_mb_s
        other = self.client_rate.other_mb_s
        self._other_mb_s = other
        if self._self_mb_s is not None:
            self._self_total += self._self_mb_s
            self._self_samples += 1
        # Unknown is "active": an unreadable counter must not be the thing
        # that authorizes an unpaced read while a client waits.  Only the
        # *other* clients' reads decide (#580): the served action's own reads
        # are the bytes this warm is fetching, read by the box it is fetching
        # them for.  The threshold has the hold's own hysteresis -- a client
        # counts as reading above the cap and stops counting below half of
        # it -- so a third party reading in bursts does not flip the read
        # depth once per sample.
        if other is None:
            active = True
        elif self._clients_active:
            active = other > self.client_active_mb_s * HOLD_RELEASE_FRACTION
        else:
            active = other > self.client_active_mb_s
        self._clients_active = active
        if active:
            self._shared_samples += 1

    def _pool_is_hurting(self, measured: dict[str, float]) -> bool:
        """Service time or backlog over the cap, with hysteresis on the way out.

        Utilization is deliberately absent: it is recorded, and it never holds.
        """

        return storage_tiers.pool_is_hurting(
            measured, max_read_await_ms=self.max_read_await_ms,
            max_backlog_ms=self.max_backlog_ms, over=self._over)

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

    def depth(self) -> int:
        """How many block reads the reader may have outstanding right now.

        ``max_readers`` while nobody but the served action is reading this
        host's exports, ``readers`` while somebody else is (#580).  A pacer
        with no disks configured has no client sampling either, and answers
        with the shared depth: the inactive shape reads as it always did.
        """

        if not self.active or self.max_readers == self.readers:
            return self.readers
        self._verdict()
        with self._lock:
            return self.readers if self._clients_active else self.max_readers

    def wait(self, stop: threading.Event | None = None,
             abort: Callable[[], bool] | None = None) -> None:
        """Block until the pool is under threshold, or ``stop`` is set.

        ``abort`` is the warm's liveness verdict (#571): a hold entered for
        a live row must not outlive the row, so a terminal consumer releases
        the wait at the next hold interval instead of pacing nothing for the
        rest of the backlog.  Either answer ends the wait without touching
        the verdict: the hold accounting stays exact whatever ended it.
        """

        if not self.active:
            return
        if not self._verdict():
            return
        self._enter_hold()
        try:
            while self._verdict():
                if stop is not None and stop.is_set():
                    return
                if abort is not None and not abort():
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
                    "self_read_mb_s": self._self_mb_s,
                    "other_read_mb_s": self._other_mb_s,
                    "served_host": self._served_host,
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
            mean_self = (self._self_total / self._self_samples
                         if self._self_samples else None)
            pool_read = (self._pool_read_bytes / 1e6 / self._pool_read_seconds
                         if self._pool_read_seconds > 0 else None)
            shared = ((self._shared_samples / self._client_samples)
                      if self._client_samples else None)
            depth = (self.readers if (self._clients_active or not self.active)
                     else self.max_readers)
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
                # The split behind the verdict (#580): the served action's
                # own reads, everyone else's, and whose reads counted as
                # self.  ``other_read_mb_s`` is the number a hold reads.
                "self_read_mb_s": (None if self._self_mb_s is None
                                   else round(self._self_mb_s, 1)),
                "other_read_mb_s": (None if self._other_mb_s is None
                                    else round(self._other_mb_s, 1)),
                "mean_self_read_mb_s": (None if mean_self is None
                                        else round(mean_self, 1)),
                # What the pool's own members delivered over this row, from
                # their sector counters.  The number a fill capacity is minted
                # from, because it is the only one that sees a local reader:
                # ``mean_self_read_mb_s`` is nfsd bytes to the *consumer's*
                # client addresses and reads 0.0 for every mover ever run.
                # An approximation in one direction only, and stated as one:
                # on raidz1 a read touches parity only on a checksum error, so
                # the member sum is delivered data plus metadata, and it is
                # not corrected by a factor.
                "mean_pool_read_mb_s": (None if pool_read is None
                                        else round(pool_read, 1)),
                "pool_read_bytes": self._pool_read_bytes,
                "pool_read_seconds": round(self._pool_read_seconds, 3),
                "served_host": self._served_host,
                "served_client_addresses": list(self._served_addresses),
                "served_attribution": self._served_reason,
                "offer_age_s": (None if self._offer_age_s is None
                                else round(self._offer_age_s, 1)),
                "readers": self.readers,
                "max_readers": self.max_readers,
                "depth": depth,
                # What share of the samples had another client reading, i.e.
                # how long the reader was at the shared depth.
                "shared_sample_fraction": (None if shared is None
                                           else round(shared, 3)),
                "client_rate_source": (self.client_rate.source
                                       or self.client_rate.path
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
        client_rate = ClientReadRate(
            path, export_stats=getattr(args, "export_stats", EXPORT_STATS) or "")
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
        readers=getattr(args, "readers", 1),
        max_readers=getattr(args, "max_readers", 0) or 0,
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


class Admission:
    """How many block reads may be in flight at once, changeable under load.

    The reader spawns its deepest thread count once and admits ``limit()``
    of them to a read at a time (#580), so the depth can drop while a third
    party reads and rise again when it stops without any thread being
    spawned or joined for it.  A thread refused a slot re-asks every
    ``wait_s``: the limit moves inside the pacer, which does not know who is
    waiting on it.  ``reader_seconds`` integrates the admitted count over
    time, which is what turns a row's bytes into a per-reader rate.
    """

    def __init__(self, clock: Callable[[], float] = time.monotonic) -> None:
        self._cv = threading.Condition()
        self.clock = clock
        self.active = 0
        self.peak = 0
        self.reader_seconds = 0.0
        self._since = clock()

    def _advance_locked(self) -> None:
        now = self.clock()
        self.reader_seconds += self.active * max(0.0, now - self._since)
        self._since = now

    def acquire(self, limit: Callable[[], int], stop: threading.Event,
                wait_s: float,
                abort: Callable[[], bool] | None = None) -> bool:
        with self._cv:
            while self.active >= max(1, int(limit())):
                if stop.is_set():
                    return False
                # A warm whose consumer went terminal stops taking new slots
                # exactly like a warm whose role is stopping (#571): a thread
                # parked here holds no disk, but waking it to read for a dead
                # row would.
                if abort is not None and not abort():
                    return False
                self._cv.wait(wait_s)
            self._advance_locked()
            self.active += 1
            self.peak = max(self.peak, self.active)
            return True

    def release(self) -> None:
        with self._cv:
            self._advance_locked()
            self.active -= 1
            self._cv.notify()

    def close(self) -> None:
        with self._cv:
            self._advance_locked()


#: How often a running warm re-asks the queue whether its row is still live.
#: A check is a handful of small queue reads, so every data block must not
#: ask (#16); every hold interval must not either.  Five seconds bounds the
#: obsolete work after a consumer ends while keeping the steady-state cost to
#: a fraction of one queue read per second per warm -- against the 89 minutes
#: of post-failure reading in #571, prompt enough to matter.
LIVENESS_POLL_S = 5.0


class SelectionWatch:
    """One warm's answer to "is this row still worth reading" (#571).

    Generation-scoped at construction: the queue's ``published_unix`` names
    the exact request the cycle selected, so a later publication under the
    same action key reads as superseded rather than live.  The verdict is
    cached and refreshed at most every ``poll_s`` -- shared by every reader
    thread of the warm, so sixteen threads do not do sixteen queue reads --
    and ``unknown`` (absent or unreadable evidence) answers True: a broken
    mount must not cancel another action's scope, it only defers the next
    verdict to the next poll.
    """

    def __init__(self, queue: pool.PoolQueue, action_key: str,
                 *, published_unix: object = None,
                 poll_s: float = LIVENESS_POLL_S,
                 clock: Callable[[], float] = time.monotonic) -> None:
        self._queue = queue
        self._key = str(action_key)
        self._published_unix = published_unix
        self._poll_s = max(0.0, float(poll_s))
        self._clock = clock
        self._lock = threading.Lock()
        self._verdict = pool.PoolQueue.SELECTION_UNKNOWN
        self._checked_at: float | None = None
        self._stopped = False

    @property
    def verdict(self) -> str:
        """The last verdict asked for, from the queue's shared vocabulary."""

        with self._lock:
            return self._verdict

    @property
    def stopped(self) -> bool:
        """Whether this watch has ever answered "issue no new reads"."""

        with self._lock:
            return self._stopped

    def _refresh_locked(self) -> None:
        try:
            verdict = self._queue.selection_live(
                self._key, published_unix=(self._published_unix
                                           if isinstance(self._published_unix,
                                                         (int, float))
                                           else None))
        except Exception:
            # The queue read itself failed outside the contract: unavailable
            # evidence, which answers unknown rather than cancelling the warm.
            verdict = pool.PoolQueue.SELECTION_UNKNOWN
        self._verdict = verdict
        self._checked_at = self._clock()
        if verdict not in (pool.PoolQueue.SELECTION_LIVE,
                           pool.PoolQueue.SELECTION_UNKNOWN):
            self._stopped = True

    def __call__(self) -> bool:
        """True while new reads for the selected row may still serve it."""

        with self._lock:
            if (self._checked_at is None
                    or self._clock() - self._checked_at >= self._poll_s):
                self._refresh_locked()
            return not self._stopped


class Reader:
    """Bounded parallel sequential reads, in the order the manifest lists them.

    Order matters twice: it is the order the action itself will consume the
    bytes, so a partial warm is a useful prefix rather than a random subset,
    and it is the order the files were written, so the disks see something
    close to a sequential sweep.

    ``readers`` is the depth while another client is reading, ``max_readers``
    the depth while the pool serves nobody but this warm and the action it
    is warming for; the pacer says which applies before every block (#580).
    Without a pacer every thread reads, as before.
    """

    def __init__(self, readers: int, mounts: MountMap, block: int = BLOCK,
                 pacer: DiskPacer | None = None, max_readers: int = 0) -> None:
        self.readers = max(1, readers)
        self.max_readers = max(self.readers, int(max_readers or 0))
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
        served: Mapping[str, object] | None = None,
        stage: "StageTier | None" = None,
        stage_prefix: str = "",
        is_live: Callable[[], bool] | None = None,
    ) -> dict[str, object]:
        """``stage`` is the second destination for these same bytes (#582).

        One window read, two places to put it: the ARC, which is what reading
        the pool path does, and -- when a stage tier was discovered and asked
        for -- a copy on the SSD pool.  The stage never changes what is read,
        in what order, at what depth or under what hold: a tier that fills or
        faults stops being written to and the warm carries on, because losing
        the second destination must not cost the first.

        ``is_live`` stops new reads for a row that went terminal, withdrawn
        or superseded after selection (#571): consulted before every entry,
        before every block, in the admission wait and in the pacing hold, so
        a dead consumer releases the warm within one hold interval rather
        than at the end of its manifest.  Bytes already read stay accounted
        exactly -- stopping issues no new reads and recalls none -- and the
        result names what stopped it under ``stopped_by``, absent when the
        warm ran to its budget.
        """

        staged_before = (stage.staged_bytes, stage.staged_entries) if stage else (0, 0)
        if self.pacer is not None:
            self.pacer.begin_row(**dict(served or {}))
        admission = Admission()
        limit = (self.pacer.depth if self.pacer is not None
                 else (lambda: self.max_readers))
        wait_s = self.pacer.hold_s if self.pacer is not None else 0.25
        work: "queuelib.Queue[tuple[int, dict[str, object]] | None]" = queuelib.Queue()
        for index, entry in enumerate(entries):
            work.put((index, entry))
        for _ in range(self.max_readers):
            work.put(None)
        lock = threading.Lock()
        state = {"bytes": 0, "entries": 0, "budget": int(budget_bytes),
                 "reserved": 0, "stopped_by": ""}
        completed = [0] * len(entries)
        errors: list[str] = []

        def live() -> bool:
            if is_live is None:
                return True
            try:
                ok = bool(is_live())
            except Exception:
                # A watch that raises is unavailable evidence: keep reading
                # rather than let a broken observer cancel the warm.
                return True
            if not ok:
                verdict = getattr(is_live, "verdict", "terminal")
                with lock:
                    if not state["stopped_by"]:
                        state["stopped_by"] = str(verdict)
            return ok

        def worker() -> None:
            buffer = bytearray(self.block)
            view = memoryview(buffer)
            while not stop.is_set() and live():
                queued = work.get()
                if queued is None:
                    return
                if not live():
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
                sink = None
                if stage is not None and stage.usable:
                    key = stage_object_key(job, stage_prefix)
                    if key is None:
                        stage.note(f"{os.path.basename(path)}: outside the "
                                   "manifest's mount prefix")
                    else:
                        sink = stage.open_object(key, want_total)
                try:
                    source = os.fstat(fd)
                    if not statmod.S_ISREG(source.st_mode):
                        raise OSError(f"not a regular file: {path}")
                    if sink is not None:
                        # The identity of what is being copied, from the stat
                        # this loop already takes.
                        sink.identify(source)
                    if offset:
                        os.lseek(fd, offset, os.SEEK_SET)
                    remaining = want_total
                    while remaining > 0 and not stop.is_set() and live():
                        # A slot first, then the hold: a thread that is not
                        # admitted does not wait on the pool, so the count
                        # waiting on a hold is the depth, never the thread
                        # count.
                        if not admission.acquire(limit, stop, wait_s,
                                                    abort=live):
                            break
                        try:
                            if self.pacer is not None:
                                self.pacer.wait(stop, abort=live)
                                if stop.is_set() or not live():
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
                        finally:
                            admission.release()
                        with lock:
                            # Return the reservation and commit its actual
                            # bytes as one transition, so another reader
                            # cannot claim the credit between those steps.
                            state["reserved"] -= allowance
                            state["bytes"] += chunk
                        if not chunk:
                            break
                        if sink is not None and not sink.write(view[:chunk]):
                            # The tier refused these bytes -- full, or a write
                            # error already recorded.  The read keeps going.
                            sink = None
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
                if sink is not None:
                    # Only a whole entry earns its name.  A partial copy is
                    # discarded rather than renamed, so nothing under the
                    # stage root is ever a short file wearing a full name.
                    if got == want_total:
                        sink.commit()
                    else:
                        sink.abort()
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
                   for _ in range(self.max_readers)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        admission.close()
        elapsed = max(1e-9, time.time() - started)
        pacing = (self.pacer.report() if self.pacer is not None
                  else inactive_pacing())
        # Bytes per admitted reader-second: the per-stream rate this row
        # actually got from the pool, and beside it the depth that rate would
        # need to keep pace with the served action's own read rate.  Both are
        # derived from what the row sampled, so an operator can see whether
        # ``--max-readers`` binds -- a keep-pace depth above it says the pool
        # cannot feed this consumer at the depth it was allowed.
        per_reader = (state["bytes"] / 1e6 / admission.reader_seconds
                      if admission.reader_seconds > 0 else 0.0)
        mean_self = pacing.get("mean_self_read_mb_s")
        keep_pace = (int(-(-float(mean_self) // per_reader))
                     if isinstance(mean_self, (int, float)) and per_reader > 0
                     else None)
        contiguous = 0
        for entry, got in zip(entries, completed):
            want = int(entry["bytes"])
            if got != want:
                break
            contiguous += want
        result: dict[str, object] = {
            "bytes_warmed": state["bytes"],
            "contiguous_bytes": contiguous,
            "entries_warmed": state["entries"],
            "seconds": round(elapsed, 3),
            "mb_per_s": round(state["bytes"] / 1e6 / elapsed, 1),
            "readers": self.max_readers,
            # How deep the read actually went, and what one admitted reader
            # got out of the pool (#580).
            "readers_peak": admission.peak,
            "reader_seconds": round(admission.reader_seconds, 3),
            "per_reader_mb_s": round(per_reader, 1),
            "keep_pace_depth": keep_pace,
            # Beside ``mb_per_s`` on purpose: a rate alone hid the cost that
            # made #499, and a receipt that reports only how fast the warm was
            # cannot answer whether it was worth what the fleet paid for it.
            "disk_pacing": pacing,
            "errors": errors,
        }
        if state["stopped_by"]:
            # The row went terminal, withdrawn or superseded mid-warm (#571):
            # no new reads were issued past that verdict, and these are the
            # exact bytes the reads before it moved.
            result["stopped_by"] = state["stopped_by"]
        if stage is not None:
            # What this window put on the stage, never what any consumer read
            # off it.  Absent entirely when no stage was asked for, so a
            # receipt from the default configuration carries no stage field
            # at all rather than a field saying nothing happened.
            result["staged_bytes"] = stage.staged_bytes - staged_before[0]
            result["staged_entries"] = stage.staged_entries - staged_before[1]
        return result


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
        value = summary.get("read_bytes" if summary.get("schema") ==
                            pb.DATA_MANIFEST_SCHEMA_V2 else "total_bytes")
        if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
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

    if manifest.get("schema") == pb.DATA_MANIFEST_SCHEMA_V2:
        # The core validator checked every reference, running sum and phase
        # name before the storage role sees this CAS-backed manifest.
        return [{"name": phase["name"],
                 "cumulative_bytes": phase["cumulative_bytes"]}
                for phase in manifest["read_plan"]["phases"]]
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


def manifest_read_entries(manifest: dict) -> list[dict[str, object]]:
    """Actual read timeline; v1 keeps its original consumption-order list."""

    entries = manifest["entries"]
    if manifest.get("schema") != pb.DATA_MANIFEST_SCHEMA_V2:
        return list(entries)
    return [entries[index]
            for phase in manifest["read_plan"]["phases"]
            for index in phase["entry_indices"]]


def manifest_read_bytes(manifest: dict) -> int:
    if manifest.get("schema") == pb.DATA_MANIFEST_SCHEMA_V2:
        return int(manifest["read_plan"]["read_bytes"])
    return int(manifest["total_bytes"])


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


def warmed_window(record: Mapping[str, object] | None, manifest_bytes: int) -> bool:
    """Does this record describe a window rather than a whole manifest?

    A phased record always does.  So does an unphased record whose resident
    prefix stops short of the manifest, which is what a budget-bounded warm
    of a manifest with no phase table leaves behind.  The distinction matters
    only to the reserve: a row with 8 KB of a 16 KB manifest resident can have
    8 KB displaced, and reserving the manifest would take the whole cache from
    every row behind it for one claim (#499).
    """

    if not record:
        return False
    if record.get("phased"):
        return True
    resident = resident_bytes(record)
    return 0 < resident < manifest_bytes


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
    That is a property of the window, not of the phase table that named its
    boundary, so an unphased prefix reserves its prefix too (#499).
    """

    now = time.time()
    reserved = 0
    released_total = 0
    keys: list[str] = []
    triggers: list[dict[str, object]] = []
    windows: list[dict[str, object]] = []
    #: Every claimed row this function saw, whatever it decided to reserve
    #: for it.  The reserve and the window are budget questions; *live* is a
    #: queue question, and the two skip branches below answer the first
    #: without answering the second.  A row dropped from the reserve is still
    #: a row that is running and still reading the bytes it was warmed.
    live: dict[str, dict[str, object]] = {}
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
        # Recorded before anything below can skip this row: a claimed row is
        # live by definition, and a row with no window has read nothing of
        # what it holds, which is the "wants all of it" shape.
        live.setdefault(key, {"action_key": key, "cas_root": str(root),
                              "consumed_bytes": 0})
        request = sealed_request(root, key)
        size = declared_manifest_bytes(request)
        if not size:
            continue
        warmed = queue.prewarm(key)
        declared = declares_progress(request)
        if not warmed_window(warmed, size):
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
                live[key]["consumed_bytes"] = consumed
                windows.append({
                    "action_key": key, "cas_root": str(root),
                    "status": "partial", "manifest_sha256": str(entry["sha256"]),
                    "manifest_bytes": size, "warmed_bytes": 0,
                    "consumed_bytes": consumed, "resident_ahead": 0,
                    "phase": reported["phase"] if reported else "",
                    "claimed_host": claimed_host_of(record),
                    "residency_state": residency_state_of(record),
                    # The generation this window was selected for: the warm
                    # watches exactly this request, never a same-key
                    # successor published under it (#571).
                    "published_unix": record.get("published_unix"),
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
        live[key]["consumed_bytes"] = consumed
        windows.append({
            "action_key": key, "cas_root": str(root),
            "status": str(warmed.get("status", "")),
            "manifest_sha256": str(warmed.get("manifest_sha256", "")),
            "manifest_bytes": size,
            "warmed_bytes": resident,
            "consumed_bytes": consumed,
            "resident_ahead": ahead,
            "phase": reported["phase"] if reported else "",
            "claimed_host": claimed_host_of(record),
            "residency_state": residency_state_of(record),
            # The generation this window was selected for: the warm watches
            # exactly this request, never a same-key successor (#571).
            "published_unix": record.get("published_unix"),
        })
    return {"claimed_reserved_bytes": reserved, "claimed_reserved_keys": keys,
            "claimed_released_bytes": released_total,
            "progress_triggers": triggers, "claimed_windows": windows,
            "claimed_live_rows": list(live.values())}


def residency_state_of(record: Mapping[str, object]) -> str:
    """What the pool decided about this claim's residency, or ``""``.

    The verdict, not the block.  ``PoolQueue.claim`` writes
    ``residency_verdict`` onto the claim record of every action that asked for
    residency, and only ``resident`` means the consumer is reading the stage
    copy: a consumer whose map is not composed yet was never admitted, and one
    whose later phases were never staged reads the pool for them exactly as it
    always did (#638).
    """

    verdict = record.get("residency_verdict")
    if not isinstance(verdict, Mapping):
        return ""
    return str(verdict.get("state") or "")


def claimed_host_of(record: Mapping[str, object]) -> str:
    """The box a claim record says is running the action, as the pool spells it."""

    return str(record.get("claimed_host") or record.get("host") or "")


def served_addresses(queue: pool.PoolQueue, host: str) -> dict[str, object]:
    """Whose reads are the served action's own: the claimant box's addresses.

    The identity chain (#580) is the claim record's ``claimed_host``, joined
    to the client addresses that box announced on its worker offer
    (``workers/<host>.json``, ``addresses``), which are what the server's
    per-client counter is keyed by.  Every link is a record the fleet already
    writes; nothing here guesses.  A broken link -- no host on the claim, no
    offer, an offer from a runtime that announced no addresses -- leaves the
    set empty and the pacer protecting every client as it did before, and
    the reason travels into the receipt so the old behaviour is never silent.

    The offer's age is reported, not enforced: an offer is refreshed by the
    box's idle loops, so a box whose every loop is busy stops refreshing it,
    and its addresses are the same addresses.  Staleness is a fact about
    whether the box is *offering*, which is not the question asked here.
    """

    if not host:
        return {"served_host": "", "served_addresses": (),
                "served_reason": "claim names no host", "offer_age_s": None}
    path = queue.root / pool.WORKERS / f"{host}.json"
    try:
        record = json.loads(path.read_text())
    except (OSError, ValueError):
        return {"served_host": host, "served_addresses": (),
                "served_reason": f"no offer for {host}", "offer_age_s": None}
    announced = record.get("announced_unix") if isinstance(record, dict) else None
    age = (time.time() - float(announced)
           if isinstance(announced, (int, float)) and not isinstance(announced, bool)
           else None)
    addresses = record.get("addresses") if isinstance(record, dict) else None
    if not isinstance(addresses, list) or not addresses \
            or not all(isinstance(a, str) and a for a in addresses):
        return {"served_host": host, "served_addresses": (),
                "served_reason": f"offer for {host} announces no addresses",
                "offer_age_s": age}
    return {"served_host": host, "served_addresses": tuple(addresses),
            "served_reason": "attributed", "offer_age_s": age}


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


def sweep_orphan_stage(queue: pool.PoolQueue, stage: StageTier,
                       live_keys: "set[str]",
                       others_for: "Callable[[str], object]",
                       fallback_cas_root: "Path | None",
                       *, apply: bool = True) -> list[dict[str, object]]:
    """Release what a row that has left the queue staged and nobody else wants.

    Delete-behind follows a frontier, and a row's last phase leaves no
    frontier behind it: the claim disappears and its tail would sit on the
    stage until somebody noticed.  This is the backstop that makes "bounded"
    true across a restart as well as across a cycle -- the loop keeps no
    memory, so the band a sweep releases is read from the receipt the warm
    wrote, not from anything held in this process.

    A row whose sealed manifest can no longer be read cannot have its objects
    named, so it is reported blocked rather than swept.  That is a leak with
    a receipt, which is the only kind worth having.
    """

    rows: list[dict[str, object]] = []
    try:
        names = sorted(os.listdir(queue.root / pool.PREWARM))
    except OSError:
        return rows
    for name in names:
        if not name.endswith(".json"):
            continue
        key = name[: -len(".json")]
        if key in live_keys:
            continue
        record = queue.prewarm(key)
        block = record.get("stage") if isinstance(record, dict) else None
        if not isinstance(block, dict) or block.get("swept"):
            continue
        through = int(block.get("staged_through_bytes", 0) or 0)
        if through <= 0:
            if apply:
                update_prewarm_stage(queue, key, {"swept": True})
            continue
        root = Path(str(block.get("cas_root") or fallback_cas_root or ""))
        entry = (manifest_input_of(sealed_request(root, key))
                 if root.name else None)
        manifest = load_manifest(root, entry) if entry is not None else None
        if manifest is None:
            if apply:
                update_prewarm_stage(
                    queue, key,
                    {"sweep_blocked":
                     "the sealed manifest is no longer readable"})
            rows.append({"action_key": key, "status": "blocked",
                         "reason": "the sealed manifest is no longer readable"})
            continue
        outcome = release_stage_band(
            stage, manifest_read_entries(manifest),
            str(manifest.get("mount_prefix", "")),
            start=0, end=through, others=lambda: others_for(key),
            apply=apply)
        swept = (apply and not outcome["retained_entries"]
                 and not outcome["failed_entries"])
        if apply:
            update_prewarm_stage(queue, key, {
                "swept": swept,
                "evicted_through_bytes": through if swept else int(
                    block.get("evicted_through_bytes", 0) or 0)})
        rows.append({"action_key": key,
                     "status": ("planned" if not apply else
                                "swept" if swept else
                                "release failed" if outcome["failed_entries"]
                                else "retained by another row"),
                     **outcome})
    return rows


def update_prewarm_stage(queue: pool.PoolQueue, action_key: str,
                         updates: Mapping[str, object]) -> bool:
    """Merge a stage verdict into a receipt this loop already wrote.

    A release happens on a cycle that may warm nothing, so the frontier it
    reached has to reach the receipt without one.  Nothing outside the
    ``stage`` block is touched, and a receipt this loop did not write is left
    alone: the storage role is the only writer of either.
    """

    record = queue.prewarm(action_key)
    if not isinstance(record, dict):
        return False
    block = dict(record.get("stage") or {})
    block.update(dict(updates))
    record["stage"] = block
    queue.record_prewarm(action_key, record)
    return True


def stage_requested(args) -> bool:
    """Is the stage tier asked for at all?

    Off is the default and the whole behaviour: an unset flag means the loop
    runs the way it ran before #582, down to the fields in its receipts.  The
    default flips on a measured campaign result, not on this code landing.
    """

    return bool(getattr(args, "stage", False))


def attribution_row(action_key: str, trigger: str,
                    pacing: Mapping[str, object]) -> dict[str, object]:
    """One warmed row's client attribution, for the cycle event (#575, #585).

    The same split the row's own receipt carries -- whose reads counted as
    *self*, what everyone else read, and whether the telemetry behind the
    verdict was even there -- picked out so a cycle that held nothing still
    says why.  One entry per row, never merged: two rows warmed for two
    different boxes keep their own ``served_host``, so a cycle serving a
    claimed window beside a ready row cannot read as a single verdict.
    """

    return {
        "action_key": action_key,
        "trigger": trigger,
        "served_host": pacing["served_host"],
        "served_attribution": pacing["served_attribution"],
        "self_read_mb_s": pacing["self_read_mb_s"],
        "other_read_mb_s": pacing["other_read_mb_s"],
        "mean_self_read_mb_s": pacing["mean_self_read_mb_s"],
        "mean_pool_read_mb_s": pacing["mean_pool_read_mb_s"],
        "telemetry_state": pacing["telemetry_state"],
        "missing_devices": list(pacing["missing_devices"]),
        "samples": pacing["samples"],
        "holds": pacing["holds"],
        "held_seconds": pacing["held_seconds"],
    }


def cycle_client_attribution(
        rows: list[dict[str, object]], pacer: DiskPacer,
        mark: Mapping[str, float | int]) -> dict[str, object]:
    """The cycle's own pacing verdict, stamped whether it held or not (#585).

    ``rows`` is one :func:`attribution_row` per window warmed this cycle, in
    the order it was warmed.  The hold counters are this cycle's diff against
    ``mark`` -- the ledger outlives the cycle, so its lifetime totals would
    answer "since the role started" to a reader asking "this poll".  A cycle
    that warmed nothing reports no rows and no holds: with ``pacing_active``
    beside it on the event, "nothing to read" stays visibly different from
    "pacing was off", and a row read blind stays visibly different from a row
    read alone, because its entry says ``missing`` where the other's says
    ``complete``.  Only rows warmed *this* cycle appear: the pacer is shared,
    and stamping its current verdict for a row it warmed on an earlier cycle
    would certify a read that never happened.
    """

    ledger = pacer.ledger.snapshot()
    states = sorted({str(row["telemetry_state"]) for row in rows})
    missing = sorted({str(device) for row in rows
                      for device in row["missing_devices"]})
    return {
        "rows": rows,
        "holds": ledger["holds"] - mark["holds"],
        "held_seconds": round(ledger["held_s"] - mark["held_s"], 3),
        "held_while_clients_idle_s": round(ledger["idle_s"] - mark["idle_s"], 3),
        "held_while_clients_active_s": round(
            ledger["active_s"] - mark["active_s"], 3),
        "telemetry_states": states,
        "missing_devices": missing,
    }
def _cycle_mbytes(total_bytes: int) -> str:
    return (f"{total_bytes / 1e9:.1f} GB" if total_bytes >= 1e9
            else f"{total_bytes / 1e6:.1f} MB")


def summarize_cycle(event: Mapping[str, object]) -> str:
    """One line saying what this cycle did, for a human reading the journal.

    The event is a twenty-field nested object nobody reads in a pager; the
    per-cycle question -- did it warm, advance, skip, release, prune or stop
    anything, and why -- gets one line (#596).  Pure formatting over the
    event, so it can never disagree with it.
    """

    warmed = event.get("warmed") if isinstance(event.get("warmed"), list) else []
    advanced = (event.get("advanced") if isinstance(event.get("advanced"), list)
                else [])
    skipped = (event.get("skipped") if isinstance(event.get("skipped"), list)
               else [])
    pruned = (event.get("pruned") if isinstance(event.get("pruned"), list)
              else [])
    warmed_bytes = sum(int(entry.get("bytes_warmed", 0) or 0)
                       for entry in warmed if isinstance(entry, dict))
    warmed_s = sum(float(entry.get("seconds", 0) or 0)
                   for entry in warmed if isinstance(entry, dict))
    stopped: dict[str, int] = {}
    for entry in (*warmed, *advanced):
        reason = entry.get("stopped_by") if isinstance(entry, dict) else None
        if reason:
            stopped[str(reason)] = stopped.get(str(reason), 0) + 1
    skip_reasons: dict[str, int] = {}
    for entry in skipped:
        reason = entry.get("reason") if isinstance(entry, dict) else None
        skip_reasons[str(reason or "unknown")] = (
            skip_reasons.get(str(reason or "unknown"), 0) + 1)
    parts = [
        f"warmed {len(warmed)} rows "
        f"({_cycle_mbytes(warmed_bytes)} in {warmed_s:.1f} s)",
        f"advanced {len(advanced)} windows",
        ("skipped " + str(len(skipped)) + " (" + ", ".join(
            f"{reason} x{count}"
            for reason, count in sorted(skip_reasons.items()))
         + ")" if skipped else "skipped 0"),
        f"pruned {sum(1 for row in pruned if isinstance(row, dict) and row.get('pruned'))} receipts",
    ]
    stage = event.get("stage")
    if isinstance(stage, dict):
        released = stage.get("released") if isinstance(stage.get("released"), list) else []
        orphans = stage.get("orphans") if isinstance(stage.get("orphans"), list) else []
        parts.append(
            f"stage released {len(released)} bands, swept {len(orphans)} orphans")
    if stopped:
        parts.append("stopped: " + ", ".join(
            f"{reason} x{count}" for reason, count in sorted(stopped.items())))
    return "prewarm cycle: " + "; ".join(parts)


def cycle(args, queue: pool.PoolQueue, mounts: MountMap, stop: threading.Event,
          pacer: DiskPacer | None = None) -> dict:
    ready = queue.ready_items()
    # One pacer per cycle, shared by every reader thread of every row warmed in
    # it: the disks are one queue, and a per-thread pacer would let N threads
    # each drive the pool to the threshold.
    pacer = pacer if pacer is not None else pacer_from_args(args)
    room = arc_headroom(args.arc_reserve_fraction, args.arcstats)
    # Rediscovered every cycle, never remembered: a stage pool created,
    # exported, filled or lost between two polls changes the answer, and the
    # record has to be a fact about this cycle (#582).
    stage = (discover_stage(
        getattr(args, "stage_pool_prefix", STAGE_POOL_PREFIX),
        free_floor_bytes=getattr(args, "stage_free_floor_bytes", None))
        if stage_requested(args) else None)
    if (stage is not None and stage.mountpoint and not args.dry_run
            and stage.mountpoint not in _REAPED_STAGE_ROOTS):
        # A dry run does not do it: an unlink is a write, however little it
        # looks like one.
        _REAPED_STAGE_ROOTS.add(stage.mountpoint)
        stage.reap_temporaries()
    stage_rows: list[dict[str, object]] = []
    cas_root = Path(args.cas_root) if args.cas_root else None
    reserve = claimed_reserve(
        queue,
        cas_root or Path(pool.DEFAULT_POOL_ROOT).parent / "cas",
        args.claim_grace_min * 60.0,
    )
    windows = reserve.pop("claimed_windows")
    # Popped beside the windows, and for the same reason: neither belongs in
    # the cycle's log line, and a new key in ``reserve`` would change the
    # record a loop without ``--stage`` writes.
    live_rows = reserve.pop("claimed_live_rows")
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
    #: One attribution entry per window warmed below, in warm order, for the
    #: cycle-level verdict (#575, #585).  The pacer is shared across rows, so
    #: only rows warmed *this* cycle may appear: anything else would certify
    #: a read that never happened.
    paced_rows: list[dict[str, object]] = []
    ledger_mark = pacer.ledger.snapshot()

    cycle_spent = 0

    def warm(*, key: str, manifest: dict, digest: str, entries: list,
             start_bytes: int, target: int, phase: str, phased: bool,
             trigger: str, served_host: str | None = None,
             cas_root_of: "Path | str" = "",
             published_unix: object = None) -> dict:
        """Read one window of one manifest and file what is now resident.

        ``served_host`` is the box running this action, for a claimed row;
        its reads are the pacer's *self* while this window is read (#580).
        ``None`` is a ready row: nobody runs it yet, so it has no self and
        every client reading now is one to protect.

        ``published_unix`` names the exact generation selected: the warm
        stops issuing new reads when that generation goes terminal,
        withdrawn or superseded, however long its manifest is (#571).
        """

        nonlocal budget, budget_before_progress, cycle_spent
        total = manifest_read_bytes(manifest)
        want = max(0, target - start_bytes)
        served = (served_addresses(queue, served_host) if served_host is not None
                  else {"served_host": "", "served_addresses": (),
                        "served_reason": "row not claimed", "offer_age_s": None})
        started = time.time()
        if args.dry_run:
            pacer.begin_row(**served)
            result = {"bytes_warmed": 0, "entries_warmed": 0, "seconds": 0.0,
                      "mb_per_s": 0.0, "readers": args.readers, "readers_peak": 0,
                      "disk_pacing": pacer.report(), "errors": []}
        else:
            reader = Reader(args.readers, mounts, pacer=pacer,
                            max_readers=getattr(args, "max_readers", 0) or 0)
            watch = SelectionWatch(queue, key, published_unix=published_unix)
            result = reader.read(
                entries,
                budget_bytes=want,
                stop=stop,
                served=served,
                is_live=watch,
                # The second destination for the same window read (#582).
                stage=stage,
                stage_prefix=str(manifest.get("mount_prefix", "")),
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
        # The row's verdict, picked out for the cycle event while it is still
        # this row's: the next ``begin_row`` resets these counters, so reading
        # them at cycle end would stamp the last row's numbers on every row.
        paced_rows.append(attribution_row(
            key, trigger, record["disk_pacing"]))
        if stage is not None:
            prior_stage = dict((queue.prewarm(key) or {}).get("stage") or {})
            if prior_stage.get("swept"):
                # A swept key that is warming again is a new life, and a new
                # life is a new ledger.  Carrying the old one forward would
                # leave ``swept`` standing on a receipt whose band has just
                # been re-staged: the sweep skips a swept row forever, so the
                # new band would never be released and would never even
                # appear among the orphans.  The frontiers go with it -- a
                # delete-behind that started at the last life's frontier
                # would never reach this one's early entries.
                for stale in ("swept", "sweep_blocked",
                              "staged_through_bytes", "evicted_through_bytes"):
                    prior_stage.pop(stale, None)
            record["stage"] = {
                **prior_stage,
                "state": stage.state,
                "reason": stage.reason,
                "pool": stage.name,
                "members": stage.members,
                # Bytes this window put on the stage, and how far into the
                # manifest anything may have been staged -- the band a later
                # sweep has to release when this row leaves the queue.
                "staged_bytes": int(result.get("staged_bytes", 0) or 0),
                "staged_entries": int(result.get("staged_entries", 0) or 0),
                "staged_through_bytes": max(
                    int(prior_stage.get("staged_through_bytes", 0) or 0), target),
                "evicted_through_bytes": int(
                    prior_stage.get("evicted_through_bytes", 0) or 0),
                "cas_root": str(cas_root_of),
                "layout": STAGE_LAYOUT,
                # Staging wrote bytes.  It did not make anybody read them,
                # and the record must not let the two be confused.
                "consumer": dict(STAGE_CONSUMER),
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
    def other_live_rows(exclude_key: str):
        """Every other queued row, one manifest at a time.

        A generator rather than a list: these manifests are the 469 008-entry
        kind, and holding two of them at once on the file server would cost
        the ARC this loop exists to fill.

        Driven from every claimed row, not only from the ones that earned a
        window.  A claimed row past its grace with nothing to report is
        dropped from the *reserve*; it is still running, still reading, and
        deleting the bytes it was warmed because a budget stopped counting
        them would be the sweep deciding a live row is finished.
        """

        for other in live_rows:
            other_key = str(other["action_key"])
            if other_key == exclude_key:
                continue
            other_root = Path(str(other["cas_root"]))
            other_entry = manifest_input_of(sealed_request(other_root, other_key))
            other_manifest = (load_manifest(other_root, other_entry)
                              if other_entry is not None else None)
            if other_manifest is None:
                continue
            yield (manifest_read_entries(other_manifest),
                   str(other_manifest.get("mount_prefix", "")),
                   int(other["consumed_bytes"]))
        for other_item in ready:
            other_key = str(other_item.get("action_key", ""))
            if not other_key or other_key == exclude_key:
                continue
            other_root = Path(other_item.get("cas_root") or cas_root or "")
            if not other_root.name:
                continue
            other_entry = manifest_input_of(sealed_request(other_root, other_key))
            other_manifest = (load_manifest(other_root, other_entry)
                              if other_entry is not None else None)
            if other_manifest is None:
                continue
            # A ready row has read nothing yet, so it still wants all of it.
            yield (manifest_read_entries(other_manifest),
                   str(other_manifest.get("mount_prefix", "")), 0)

    def release_window(key: str, manifest: dict, consumed: int) -> dict:
        # A dry run plans the release and performs none of it.  Unlinking a
        # staged object is a write, however little it looks like one, and
        # ``--dry-run`` promises the box is only read.
        apply = not args.dry_run
        prior = dict((queue.prewarm(key) or {}).get("stage") or {})
        start = int(prior.get("evicted_through_bytes", 0) or 0)
        outcome = release_stage_band(
            stage, manifest_read_entries(manifest),
            str(manifest.get("mount_prefix", "")),
            start=start, end=consumed,
            others=lambda: other_live_rows(key), apply=apply)
        if apply and consumed > start:
            update_prewarm_stage(queue, key,
                                 {"evicted_through_bytes": consumed})
        return {"action_key": key, "released_from_bytes": start,
                "consumed_bytes": consumed, "applied": apply, **outcome}

    for window in windows:
        if stop.is_set():
            break
        key = str(window["action_key"])
        partial = window["status"] == "partial"
        # Without a usable stage there is nothing to release, so a window
        # that cannot advance is passed over exactly as it was before #582.
        if not partial and (stage is None or not stage.mountpoint):
            continue
        root = Path(str(window["cas_root"]))
        entry = manifest_input_of(sealed_request(root, key))
        if entry is None or str(entry.get("sha256")) != window["manifest_sha256"]:
            continue
        manifest = load_manifest(root, entry)
        if stage is not None and stage.mountpoint and manifest is not None:
            # Delete behind the accepted phase: the frontier the action's own
            # progress record moved is what releases the bytes behind it.
            stage_rows.append(release_window(
                key, manifest, int(window["consumed_bytes"])))
        if not partial:
            continue
        if window.get("residency_state") == "resident":
            # This consumer was admitted because its window is on the stage,
            # and it opens the staged path.  A warm of the pool path would
            # read blocks nobody reads and evict the stage blocks it *is*
            # reading to hold them: two different datasets, two different ARC
            # entries, one cache (#638).  Recorded rather than silent -- a
            # loop that quietly stops warming a running row is the shape
            # nobody can debug.
            event["skipped"].append({
                "action_key": key, "reason": "the consumer reads the stage",
                "consumed_bytes": int(window["consumed_bytes"]),
                "residency_state": "resident",
            })
            continue
        phases = manifest_phases(manifest) if manifest else []
        if not phases:
            continue
        resident = int(window["warmed_bytes"])
        consumed = int(window["consumed_bytes"])
        # This window's resident bytes are charged to the claimed reserve
        # already; pricing the advance against a budget that also pays for
        # them would stop a window from ever extending.
        row_budget = budget + int(window["resident_ahead"])
        read_entries = manifest_read_entries(manifest)
        target, phase = window_target(phases, read_entries,
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
            entries=entries_between(read_entries, start, target),
            start_bytes=start, target=target, phase=phase, phased=True,
            trigger="progress" if window["phase"] else "claim",
            served_host=str(window.get("claimed_host") or ""),
            cas_root_of=root,
            published_unix=window.get("published_unix"),
        )
        event["advanced"].append({
            k: record[k] for k in
            ("action_key", "status", "manifest_bytes", "warmed_bytes",
             "bytes_warmed", "warmed_through_phase", "trigger", "seconds",
             "mb_per_s", "disk_pacing")
        })
        if record.get("stopped_by"):
            event["advanced"][-1]["stopped_by"] = record["stopped_by"]

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
        total = manifest_read_bytes(manifest)
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
        read_entries = manifest_read_entries(manifest)
        prior = warm_record(queue, key, digest)
        resident = resident_bytes(prior)
        # This row's own warmed bytes are already charged to the budget by
        # ``warmed_reserve``; charging them again would price a row against a
        # ceiling it is itself holding down, and a window could never extend.
        row_budget = budget + resident
        row_budget_before_progress = budget_before_progress + resident
        # ``entries`` is the consumption order whether or not a phase table
        # names boundaries in it, so the window is cut the same way either
        # way.  Without phases the loop simply cannot *advance* the window
        # later: no worker report maps to a byte count (#499).
        target, phase = window_target(phases, read_entries,
                                      consumed=0, budget=row_budget)
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
            # prewarm for the rows behind it that do fit.  What does not fit
            # here is the row's *first entry*, not its manifest: a warm reads
            # files, so there is no entry-aligned prefix inside one oversized
            # entry to make resident.
            event["skipped"].append({
                "action_key": key, "reason": "headroom",
                "manifest_bytes": total, "budget": budget,
                "phased": bool(phases),
            })
            continue
        taken += 1
        record = warm(
            key=key, manifest=manifest, digest=digest,
            entries=entries_between(read_entries, resident, target),
            start_bytes=resident,
            target=target, phase=phase, phased=bool(phases),
            trigger=("progress" if target > row_budget_before_progress
                     else "claim"),
            cas_root_of=root,
            published_unix=item.get("published_unix"),
        )
        warmed_entry = ({k: record[k] for k in
                         ("action_key", "status", "manifest_bytes",
                          "bytes_warmed", "warmed_bytes", "trigger",
                          "warmed_through_phase", "seconds", "mb_per_s",
                          "disk_pacing")})
        if record.get("stopped_by"):
            warmed_entry["stopped_by"] = record["stopped_by"]
        event["warmed"].append(warmed_entry)
    # Who is still queued, for the sweep below and the prune beside it.
    live_keys = {str(item.get("action_key", "")) for item in ready}
    live_keys |= {str(row["action_key"]) for row in live_rows}
    if stage is not None:
        # The non-action is written too.  A cycle that staged nothing, warmed
        # nothing, or found no stage pool at all still says which of the four
        # states the tier was in and why -- #585's lesson is that a correct
        # non-action nobody recorded costs the next reader the diagnosis.
        # A tier with no mountpoint -- ``absent`` on every box but the file
        # server, and most ``unreadable`` ones -- has nothing to release and
        # nowhere to release it from.  Running the release path anyway spent
        # a manifest load per live row and then advanced a frontier as though
        # objects had been removed.  The state is still recorded: a cycle
        # that found no tier says so, which is the whole point of #585.
        orphans = (sweep_orphan_stage(
            queue, stage, live_keys, other_live_rows, cas_root,
            apply=not args.dry_run) if stage.mountpoint else [])
        event["stage"] = {**stage.record(), "released": stage_rows,
                          "orphans": orphans}
    # The verdict, whether it held or not (#575, #585).  A cycle that paced
    # correctly and held nothing is the common case now, and without this it
    # reads exactly like a pacer that never saw a client at all.
    event["client_attribution"] = cycle_client_attribution(
        paced_rows, pacer, ledger_mark)
    # Prune after the sweep, staged or not (#596): the sweep may just have
    # released a retired row's band and marked it swept, which is what makes
    # the receipt deletable.  Terminal and withdrawn receipts are garbage the
    # day they retire, and leaving them makes the sweep list and read every
    # receipt the fleet ever wrote on every 10 s poll.  A dry run reads only,
    # so it prunes nothing.
    event["pruned"] = (queue.sweep_prewarm_receipts(live_keys)
                       if not args.dry_run else [])
    # The claim denials' reason rings retire on the same live set (#991): a
    # terminal or withdrawn key's ring goes, a live key's stays.
    event["denial_rings_pruned"] = (queue.sweep_denial_transitions(live_keys)
                                    if not args.dry_run else [])
    # The tier loop's per-consumer event directories retire the same way
    # (#990), plus a live residency plan keeps its consumer's directory.
    event["consumer_events_pruned"] = (queue.sweep_consumer_events(live_keys)
                                       if not args.dry_run else [])
    event["summary"] = summarize_cycle(event)
    return event


def _parser() -> argparse.ArgumentParser:
    """The role's own parser, built before anything takes the singleton.

    Help and argument refusal must not depend on the lock: ``--help`` beside
    a running role has to print usage, and an unparseable invocation is not a
    second server.
    """

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
                        help="read depth while a client other than the action "
                             "being warmed for is reading this host's exports. "
                             "8 measured 391.9 MB/s against 1's 298.9 on the "
                             "GLM census pool, and cost the fleet 100-130 s of "
                             "both Sparks' GPU time per row (#499): 30%% more "
                             "read rate for a transport reset on every client. "
                             "1 is what was measured to pass on the live fleet "
                             "-- 63.8 GB at 146.5 MB/s while both Sparks kept "
                             "encoding, disks at 23%% mean and 41.7%% peak.  "
                             "That is the depth a third party's reads still "
                             "get; --max-readers is the depth when there is "
                             "no third party (#580)")
    parser.add_argument("--max-readers", type=int, default=MAX_READERS,
                        help="read depth while nobody but this warm and the "
                             "action it is warming for is reading the pool.  "
                             "The depth is the pacer's decision, made from the "
                             "per-client counter every sample; this is its "
                             "ceiling, and a ceiling is a property of the pool: "
                             "dl380g10's raidz1 serves the same cold bytes at "
                             "28.6 MB/s to one stream and 204.7 MB/s to "
                             "sixteen, and the default is the deepest point on "
                             "that measured curve.  Depth is the only lever "
                             "that gets the warm ahead of the reader it feeds: "
                             "a consumer reading cold at 160 MB/s gets that "
                             "rate from single-depth demand reads, so matching "
                             "it cannot gain on it, and once ahead the window "
                             "budget bounds the lead so depth costs nothing.  "
                             "The receipt's keep_pace_depth says what depth "
                             "the served action's own rate would need at the "
                             "per-reader rate the row got, so an operator can "
                             "see when this ceiling binds.  Equal to --readers "
                             "disables the second tier")
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
    parser.add_argument("--export-stats", default=EXPORT_STATS,
                        help="where this host's NFS server counts the bytes "
                             "it served per client address.  The pacer "
                             "attributes the rows whose address the served "
                             "action's box announced on its worker offer to "
                             "that action, and holds only for the rest "
                             "(#580).  A host without the file falls back "
                             "to --nfsd-io, attributes nothing, and protects "
                             "every client as before")
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
    parser.add_argument("--stage", action="store_true",
                        help="also copy each window this loop reads onto a "
                             "stage pool (#582).  OFF by default and inert "
                             "until it is asked for: without it the loop "
                             "behaves and reports exactly as it did before, "
                             "no zpool is queried and no receipt carries a "
                             "stage field.  Read this before turning it on: "
                             "**nothing reads the stage today**.  "
                             "PrismaBuild publishes no residency map, the "
                             "export is served from the pool path, and the "
                             "ARC is keyed by the on-pool block pointer, so "
                             "a staged copy does not warm the path a "
                             "consumer reads.  Enabled without a consumer "
                             "this costs SSD writes at pool read rate and "
                             "returns nothing -- the same wear #582 counted "
                             "against L2ARC.  Every record says so in its "
                             "'consumer' block rather than reporting the "
                             "copy as a saving.  The default flips on a "
                             "measured campaign result, not on this landing")
    parser.add_argument("--stage-pool-prefix", default=STAGE_POOL_PREFIX,
                        help="a ZFS pool whose name starts with this is the "
                             "stage tier; its members are bound by "
                             "/dev/disk/by-id and never by a device number.  "
                             "The name is the declaration, so no device is "
                             "ever taken for being idle: on this fleet's "
                             "file server one idle NVMe still carries a "
                             "stale zfs_member signature")
    parser.add_argument("--stage-free-floor-bytes", type=int, default=None,
                        help="bytes the stage keeps back, overriding what is "
                             "derived.  Capacity is read every cycle from the "
                             "prewarm dataset's own 'available', which has "
                             "already withheld the pool's slop space, the "
                             "dataset's quota and its reservation, so nothing "
                             "further is kept back by default.  Only when the "
                             "dataset cannot answer does capacity fall back "
                             "to the pool's 'free', and the floor then becomes "
                             "the reserve ZFS itself would have withheld: "
                             "1/32 of the pool, never below 128 MiB.  The "
                             "record says which number was used.  A tier with "
                             "nothing above the floor records 'full' and "
                             "stages nothing, which is a state, not an error")
    parser.add_argument("--once", action="store_true",
                        help="run one cycle and exit; return 75 if maintenance or runtime rotation defers it")
    parser.add_argument("--dry-run", action="store_true",
                        help="plan without warming data or recording prewarm results; maintenance checks still apply")
    parser.add_argument("--log", default=None,
                        help="append one JSON object per cycle here")
    return parser


def _serve(args) -> int:
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


def main(argv: list[str] | None = None) -> int:
    """Serve the storage role, single-instance on this box.

    Every valid invocation takes the role's host-local singleton lock around
    the whole cycle, one-shot included: an operator's ``--once`` warms bytes
    and publishes records just as the service does, so exempting it would be
    the bypass the guard exists to close (#709).  Two readers warm the same
    bytes twice and publish contradictory receipts; on 2026-09-19 a duplicate
    supervisor's copy did exactly that and compounded the fill-capacity
    wedge.  The lock is taken by the process that serves, so the launcher
    cannot matter -- a supervisor, a stale generation's supervisor, or a
    hand-started loop all contend on one inode, and the winner holds until
    the block's final close (never an unlink or an explicit unlock).

    Safety never depends on naming the holder: the holder pid is a
    /proc/locks diagnostic the ``RoleLockHeld`` message may carry, read only
    when the flock is refused, and an unreadable holder is still a refusal.
    """

    args = _parser().parse_args(argv)
    try:
        with runtime_gate.role_singleton(Path(__file__)):
            return _serve(args)
    except runtime_gate.RoleLockHeld as held:
        print(f"prewarm: refusing a second storage role; "
              f"{runtime_gate.role_lock_path(Path(__file__))} is held by "
              + (f"pid {held.holder}" if held.holder is not None
                 else "an unreadable holder"),
              file=sys.stderr, flush=True)
        return runtime_gate.ROLE_SINGLETON_HELD_EXIT
    except runtime_gate.RoleLockUnavailable as exc:
        print(f"prewarm: refusing to serve without the storage role "
              f"singleton lock: {exc}", file=sys.stderr, flush=True)
        return runtime_gate.ROLE_SINGLETON_HELD_EXIT


if __name__ == "__main__":
    raise SystemExit(main())
