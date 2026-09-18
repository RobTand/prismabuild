"""Discover the storage tiers a file-serving box offers, never configure them.

Rob, 2026-09-17 (#583): *"I expect that as the topology of my drives changes,
prismabuild will be able to automatically adapt should I choose to add more
ssds or ram."*  So every quantity here is read from the box on every call --
the ARC from ``arcstats``, the stage from the pools that carry PB's own name,
the source pool's members from ``zpool status -P`` -- and nothing is a
hand-set constant.  Add a device to the stage pool and the next cycle offers
more; take the stage pool away and it offers nothing.

Three tiers, each answering a different question:

``pool``
    The raidz1 the export is served from.  Its members are discovered so a
    mover can pace itself by the disks that actually carry the bytes; its
    fill bandwidth is *learned* from the receipts of moves off it, because
    nothing in the topology says what a four-spindle raidz1 sustains at a
    given depth -- receipts do.  No receipt means no fill tokens, which is
    the probe rule: one mover at a time until one has measured the pool.

``stage``
    A ZFS pool whose name starts with :data:`STAGE_POOL_PREFIX`.  The pool
    name is the device's own declaration, the way ``LABEL="storage_pool"``
    on ``sdb1`` declares a data member.  Deliberately **not** "any unused
    SSD": on dl380g10 ``nvme0n1p1`` still carries a ``zfs_member`` signature
    from a pool that no longer exists, and that rule would seize it before
    anybody had approved anything.  Capacity is the pool's own ``size``; free
    space is its own ``free``; members are bound by ``/dev/disk/by-id`` so
    the numbering of ``nvmeXn1`` -- which on that box is the reverse of what
    the device names suggest -- never enters a record.  PB owns eviction on
    this tier, so a reservation here is a pin.

``arc``
    The file server's ARC.  Capacity is ``c_max`` less ``arc_meta_used``,
    both read live; the ``--arc-reserve-fraction 0.8`` the prewarm role
    carried was a constant standing in for the metadata this subtraction
    measures.  ZFS exposes no pin, so a reservation here is a budget, exactly
    as the prewarm role's ``arc_headroom`` treats it.

Tokens are minted from these numbers by whichever loop owns the tier's
ledger (``tier_loop.py``), in the units :func:`tier_tokens` names: one
``stage_gib`` or ``arc_gib`` token per GiB, one ``fill_mb_s_pool_side`` token
per pool-side MB/s.  A demand against a tier is *derived* from the action's
own data manifest by :func:`residency_demand`, never typed by hand: the
manifest declares the exact read set, so the bytes a range needs resident
are arithmetic over it.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
import math
import os
from pathlib import Path
import shutil
import socket
import subprocess
import time

TIER_RECORD_SCHEMA_V1 = "prismabuild.storage_tier.v1"
#: A ZFS pool is a stage tier when its name carries this prefix.
STAGE_POOL_PREFIX = "prismabuild-stage"
ARCSTATS = "/proc/spl/kstat/zfs/arcstats"
SYSFS_BLOCK = "/sys/class/block"
BY_ID = "/dev/disk/by-id"
GIB = 1 << 30
MB = 1_000_000
#: ``zpool status`` groups whose members are not the data path.
ZPOOL_AUX_GROUPS = frozenset({"cache", "logs", "spares", "special", "dedup"})
#: The token kinds a tier mints.  Every demand key on a tier is ``<kind>@<tier_id>``.
STAGE_CAPACITY_KIND = "stage_gib"
ARC_CAPACITY_KIND = "arc_gib"
#: The fill token is named for the side it is measured on, because a
#: file-side rate and a pool-side rate differ by whatever the ARC answered
#: -- one live receipt on dl380g10 reads 1141 MB/s file-side for 206 GB off
#: a four-spindle raidz1 -- and a demand key that did not say which it meant
#: would let the two be compared.  Every record field carrying this quantity
#: uses the same spelling.
FILL_KIND = "fill_mb_s_pool_side"
#: The record field the tier publishes it under, same spelling as the token.
FILL_RECORD_FIELD = "fill_mb_s_pool_side"
TIER_DEMAND_SEPARATOR = "@"


def zpool_binary() -> str:
    """``zpool``'s path, resolved rather than left to ``PATH``.

    A supervised role's ``PATH`` need not carry ``/usr/sbin``; losing
    discovery to that would read as "no tiers", which is a fact about the
    box that has to be true when it is reported.
    """

    found = shutil.which("zpool")
    if found:
        return found
    for candidate in ("/usr/sbin/zpool", "/sbin/zpool"):
        if os.path.exists(candidate):
            return candidate
    return "zpool"


def zfs_binary() -> str:
    found = shutil.which("zfs")
    if found:
        return found
    for candidate in ("/usr/sbin/zfs", "/sbin/zfs"):
        if os.path.exists(candidate):
            return candidate
    return "zfs"


def _run(argv: list[str]) -> str:
    return subprocess.run(
        argv, check=True, text=True, timeout=30,
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL).stdout


Runner = Callable[[list[str]], str]


def whole_disk_of(path: str, *, sysfs: str = SYSFS_BLOCK) -> str | None:
    """``/dev/sdb1`` -> ``sdb``: the disk whose queue the partition shares.

    Resolved through sysfs rather than by stripping trailing digits, which
    gets ``nvme0n1`` wrong in both directions.
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


def pool_member_paths(
    pool: str, *, runner: Runner | None = None,
) -> list[str] | None:
    """Every data-vdev leaf of ``pool`` as ``zpool status -P`` prints it.

    ``None`` when the pool cannot be read at all; ``[]`` when it has no data
    leaf.  Auxiliary groups (cache, log, spare, special, dedup) are dropped:
    an L2ARC device is not the queue a mover must stay off.  The same parse
    ``prewarm_loop.pool_member_devices`` performs, kept behaviourally
    identical so the role and the movers pace the same disks.
    """

    try:
        text = (runner or _run)([zpool_binary(), "status", "-P", pool])
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
        if not stripped or stripped.startswith("NAME "):
            continue
        if stripped.startswith("errors:"):
            break
        indent = len(line) - len(line.lstrip())
        name = stripped.split()[0]
        if indent <= 2:
            section = "aux" if name.lower() in ZPOOL_AUX_GROUPS else "data"
            continue
        if section != "data" or not name.startswith("/"):
            continue
        if name not in leaves:
            leaves.append(name)
    return leaves


def pool_member_devices(
    pool: str, *, runner: Runner | None = None, sysfs: str = SYSFS_BLOCK,
) -> list[str]:
    """The whole disks behind ``pool``'s data vdevs, as ``sdb``-style names.

    ``[]`` on any failure, including a member sysfs cannot resolve: a partial
    topology is not a smaller healthy pool, and pacing only the resolvable
    members would let a busy vdev disappear behind its quiet peer.
    """

    leaves = pool_member_paths(pool, runner=runner)
    if not leaves:
        return []
    devices: list[str] = []
    for leaf in leaves:
        device = whole_disk_of(leaf, sysfs=sysfs)
        if not device:
            return []
        if device not in devices:
            devices.append(device)
    return devices


def by_id_names(
    paths: Iterable[str], *, by_id: str = BY_ID,
) -> dict[str, str | None]:
    """Each device path's ``/dev/disk/by-id`` name, or ``None`` when it has none.

    Bound by real path, never by ``nvmeXn1`` spelling: the by-id link is the
    one name that follows the device across reboots and slot changes.  When
    several links resolve to one device the shortest model-serial one wins,
    so a record says ``nvme-LT0800KEXVA_CVMD54710026800BGN`` rather than its
    ``nvme-nvme.8086-…`` twin.
    """

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
    out: dict[str, str | None] = {}
    for path in paths:
        try:
            target = os.path.realpath(path)
        except OSError:
            out[path] = None
            continue
        candidates = [n for n in resolved.get(target, []) if not n.startswith("nvme-nvme.")]
        candidates = candidates or resolved.get(target, [])
        out[path] = min(candidates, key=len) if candidates else None
    return out


#: The dataset the design creates inside a stage pool, and the one a mover
#: writes into.  Named here so discovery and the movers agree on one place.
STAGE_DATASET = "prewarm"


def _int_field(value: str) -> int | None:
    try:
        return int(value)
    except ValueError:
        return None


def stage_pools(*, runner: Runner | None = None) -> list[dict[str, object]] | None:
    """Every imported pool named for the stage, with exact byte sizes.

    ``zpool list -Hp`` prints bytes, not the rounded ``745G`` a human sees, so
    capacity tokens are minted from the pool's own arithmetic.  ``None`` when
    ``zpool`` cannot be run; ``[]`` when no stage pool is imported, which is
    the answer on every box but the file server and on the file server until
    Rob creates one.
    """

    try:
        text = (runner or _run)([
            zpool_binary(), "list", "-Hp",
            "-o", "name,size,allocated,free,health",
        ])
    except (OSError, subprocess.SubprocessError):
        return None
    pools: list[dict[str, object]] = []
    for line in text.splitlines():
        fields = line.split("\t") if "\t" in line else line.split()
        if len(fields) < 5 or not fields[0].startswith(STAGE_POOL_PREFIX):
            continue
        size, allocated, free = (_int_field(f) for f in fields[1:4])
        if size is None or allocated is None or free is None:
            continue
        pools.append({
            "name": fields[0], "size_bytes": size, "allocated_bytes": allocated,
            "free_bytes": free, "health": fields[4],
        })
    return pools


def stage_dataset(pool: str, *, runner: Runner | None = None) -> dict[str, object] | None:
    """The dataset a mover writes into, with the bytes ZFS says it may write.

    Capacity is minted from ``available`` on the dataset, never from the
    pool's ``size``.  They are different numbers and only one of them is a
    promise: ``size`` is the raw geometry, while ``available`` is what this
    dataset may actually write after parity, slop reservation, quotas and
    whatever its siblings already hold.  Minting from ``size`` puts the ~3%
    slop reserve inside the accounting as an overfill margin -- the ledger
    would hand out tokens for bytes the pool will refuse at ENOSPC.  Minting
    from ``available`` puts it outside by construction.

    ``<pool>/prewarm`` is preferred over the pool's root dataset because that
    is the dataset the design creates and the movers write into; the root is
    the fallback for a pool laid out some other way.
    """

    try:
        text = (runner or _run)([
            zfs_binary(), "list", "-Hp", "-r",
            "-o", "name,available,mountpoint", pool,
        ])
    except (OSError, subprocess.SubprocessError):
        return None
    found: dict[str, dict[str, object]] = {}
    for line in text.splitlines():
        fields = line.split("\t") if "\t" in line else line.split()
        if len(fields) < 3:
            continue
        available = _int_field(fields[1])
        if available is None:
            continue
        found[fields[0]] = {
            "dataset": fields[0], "available_bytes": available,
            "mountpoint": fields[2] if fields[2].startswith("/") else None,
        }
    for name in (f"{pool}/{STAGE_DATASET}", pool):
        record = found.get(name)
        if record is not None:
            return record
    return None


def pool_mountpoint(pool: str, *, runner: Runner | None = None) -> str | None:
    try:
        text = (runner or _run)([zfs_binary(), "get", "-H", "-o", "value", "mountpoint", pool])
    except (OSError, subprocess.SubprocessError):
        return None
    value = text.strip()
    return value if value.startswith("/") else None


def read_arcstats(path: str = ARCSTATS) -> dict[str, int]:
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


def arc_tier(stats: Mapping[str, int]) -> dict[str, object] | None:
    """The RAM tier's capacity from the counters that define it, or ``None``.

    ``c_max`` is the ceiling the ARC is allowed to reach and ``arc_meta_used``
    the part of it that holds metadata rather than the file bytes a warm puts
    there; their difference is the data the tier can hold.  ``size`` and ``c``
    ride along so a reader can tell a refusal caused by a shrunken target from
    one caused by the protected set, as the prewarm role's record does.
    """

    ceiling = stats.get("c_max")
    if not ceiling:
        return None
    metadata = stats.get("arc_meta_used", 0)
    return {
        "tier": "arc",
        "capacity_bytes": max(0, ceiling - metadata),
        "arc_c_max": ceiling,
        "arc_meta_used": metadata,
        "arc_size": stats.get("size", 0),
        "arc_c": stats.get("c", 0),
    }


def tier_id(kind: str, host: str, pool: str | None = None) -> str:
    """``prismabuild-stage:dl380g10`` for a stage pool; ``arc:dl380g10`` for the ARC.

    The pool name is the discovery key, so it is the id; ``:`` rather than
    ``@`` because the demand key grammar is ``<kind>@<tier_id>`` and the id
    must not contain the separator.
    """

    if TIER_DEMAND_SEPARATOR in host or (pool and TIER_DEMAND_SEPARATOR in pool):
        raise ValueError("tier hosts and pool names must not contain '@'")
    return f"{pool}:{host}" if kind == "stage" else f"{kind}:{host}"


def tier_kind_of(tier_id: str) -> str:
    """``prismabuild-stage:dl380g10`` -> ``"stage"``; anything else -> ``"arc"``.

    The tier id *is* the discovery key, so the kind is read off it rather than
    carried beside it: a stage tier is named for the PB-owned pool it is, and
    the only other tier a box offers is its ARC.  One rule, so that the demand
    a range implies and the capacity a ledger mints can never disagree about
    which token kind a tier deals in.
    """

    return "stage" if str(tier_id).startswith(STAGE_POOL_PREFIX) else "arc"


def capacity_kind_of(tier_id: str) -> str:
    """The token kind that tier's capacity is counted in."""

    return (STAGE_CAPACITY_KIND if tier_kind_of(tier_id) == "stage"
            else ARC_CAPACITY_KIND)


def split_demand_key(key: str) -> tuple[str, str | None]:
    """``stage_gib@prismabuild-stage:dl380g10`` -> ``("stage_gib", "prismabuild-stage:dl380g10")``.

    A key without the separator is a host kind and maps to ``(key, None)``.
    """

    kind, separator, tier = key.partition(TIER_DEMAND_SEPARATOR)
    if not separator:
        return key, None
    if not kind or not tier or TIER_DEMAND_SEPARATOR in tier:
        raise ValueError(f"malformed tier demand key {key!r}")
    return kind, tier


def split_demand(
    demand: Mapping[str, int],
) -> tuple[dict[str, int], dict[str, dict[str, int]]]:
    """Host kinds on one side; tier kinds grouped by tier id on the other."""

    host: dict[str, int] = {}
    tiers: dict[str, dict[str, int]] = {}
    for key, need in demand.items():
        kind, tier = split_demand_key(str(key))
        if tier is None:
            host[kind] = int(need)
        else:
            tiers.setdefault(tier, {})[kind] = int(need)
    return host, tiers


#: The field in a reader's ``disk_pacing`` block a fill capacity is minted
#: from: what the pool's members delivered over that reader's window, summed
#: from their sector counters.  Not ``mean_self_read_mb_s``, which is nfsd
#: ``export_stats`` bytes to the served consumer's client addresses: a mover
#: copying pool -> stage runs *on* the file server and is no NFS client, so its
#: own reads are attributed to nobody.  Measured on all five live receipts in
#: ``pb-queue/movers/``: ``mean_self_read_mb_s`` is 0.0 four times and 0.2 once
#: with ``mean_util_pct`` 77-86, while file-side rates read 229-1478 MB/s.
POOL_FILL_FIELD = "mean_pool_read_mb_s"


def fill_rate_from_records(records: Iterable[Mapping[str, object]]) -> float | None:
    """The best pool-side MB/s any recorded read off a tier demonstrated, or ``None``.

    The number is ``disk_pacing.mean_pool_read_mb_s``: the sum over the pool's
    members of sectors read during that reader's window, over the wall seconds
    of the intervals it sampled.  It is deliberately **not** the record's
    file-side ``mb_per_s``.  A warm that finds its bytes already in the ARC
    reports file-side rates the disks never produced -- one live receipt on
    dl380g10 says 1141 MB/s for 206 GB off a four-spindle raidz1, and a live
    mover receipt says 1478 MB/s for 3.3 GB -- and a fill capacity minted from
    that would admit readers the disks cannot feed.

    It is also not ``mean_self_read_mb_s``, which this function read until the
    live receipts refuted it: that field is what nfsd served to the *consumer's*
    addresses, so a local pool-to-stage copy contributes nothing to it and
    every mover receipt reports 0.0 while the spindles are at 80%.  A rate of
    0.2 then minted ``int(0.2) == 0`` fill tokens and the tier announced
    ``"fill_source": "measured"`` over a supply that admits nothing.

    A record without the pool-side field says nothing about the pool, and a
    tier with no such record has no fill tokens, which is the probe rule.  The
    maximum over records is the demonstrated delivery; because the field counts
    *every* reader of the pool rather than only the one being measured, a
    window in which two movers overlapped reports both, so the mint can grow
    past one mover without anybody editing a number.
    """

    best: float | None = None
    for record in records:
        pacing = record.get("disk_pacing")
        if not isinstance(pacing, Mapping):
            continue
        rate = pacing.get(POOL_FILL_FIELD)
        if isinstance(rate, bool) or not isinstance(rate, (int, float)):
            continue
        if rate > 0 and (best is None or rate > best):
            best = float(rate)
    return best


def tier_tokens(record: Mapping[str, object]) -> dict[str, int]:
    """The tokens a tier record mints: whole GiB of capacity, whole MB/s of fill."""

    tokens: dict[str, int] = {}
    tier = record.get("tier")
    capacity = record.get("capacity_bytes")
    if isinstance(capacity, int) and not isinstance(capacity, bool) and capacity > 0:
        if tier == "stage":
            tokens[STAGE_CAPACITY_KIND] = capacity // GIB
        elif tier == "arc":
            tokens[ARC_CAPACITY_KIND] = capacity // GIB
    fill = record.get(FILL_RECORD_FIELD)
    if isinstance(fill, (int, float)) and not isinstance(fill, bool) and int(fill) > 0:
        # ``int(fill) > 0``, not ``fill > 0``.  A measured 0.2 MB/s used to
        # mint a ``fill_mb_s_pool_side`` key worth zero tokens, which admits
        # nothing at all while the tier announces ``fill_source: measured``
        # over it -- a supply that refuses every mover, labelled as a
        # measurement.  A rate below one whole MB/s has not measured a usable
        # capacity, so the kind is absent and the probe rule applies.
        tokens[FILL_KIND] = int(fill)
    return tokens


#: The demand key grammar admission splits on: ``<kind>@<tier_id>``.
#: ``residency_demand`` is the only supported way to produce one, so that a
#: number in a claim record always traces back to a manifest.


def manifest_phase_ranges(manifest: Mapping[str, object]) -> list[dict[str, object]]:
    """The manifest's read order as half-open byte ranges, or ``[]``.

    Both manifest schemas already declare the same thing: a running byte sum
    over the entries in the order the action consumes them.  v2 carries it in
    ``read_plan.phases`` (validated entry by entry in ``core``), v1 in
    ``annotations.phases``.  This turns either into the ranges a movement node
    names, so that the range in a mover's demand is a quotation from the
    consumer's own manifest rather than a number somebody chose.

    A v1 table that does not describe this manifest -- a boundary that cuts an
    entry in half, a sum that does not end at ``total_bytes`` -- yields ``[]``,
    the same refusal ``prewarm_loop.manifest_phases`` makes, because windowing
    on the wrong boundaries reserves for bytes nobody will read.
    """

    schema = manifest.get("schema")
    boundaries: list[tuple[str, int]] = []
    if schema == "prismaquant.prismabuild.data_manifest.v2":
        plan = manifest.get("read_plan")
        if not isinstance(plan, Mapping):
            return []
        phases = plan.get("phases")
        if not isinstance(phases, list) or not phases:
            return []
        for phase in phases:
            if not isinstance(phase, Mapping):
                return []
            boundaries.append((str(phase["name"]), int(phase["cumulative_bytes"])))
    else:
        annotations = manifest.get("annotations")
        if not isinstance(annotations, Mapping):
            return []
        declared = annotations.get("phases")
        if not isinstance(declared, list) or not declared:
            return []
        total = int(manifest.get("total_bytes", 0) or 0)
        entry_boundaries: set[int] = set()
        running = 0
        for entry in manifest.get("entries", []) or []:
            if not isinstance(entry, Mapping):
                return []
            running += int(entry.get("bytes", 0) or 0)
            entry_boundaries.add(running)
        if running != total:
            return []
        previous = 0
        seen: set[str] = set()
        for phase in declared:
            if not isinstance(phase, Mapping):
                return []
            name = phase.get("name")
            cumulative = phase.get("cumulative_bytes")
            if not isinstance(name, str) or not name or name in seen:
                return []
            if isinstance(cumulative, bool) or not isinstance(cumulative, int):
                return []
            if cumulative < previous or cumulative > total or cumulative not in entry_boundaries:
                return []
            seen.add(name)
            boundaries.append((name, cumulative))
            previous = cumulative
        if previous != total:
            return []
    ranges: list[dict[str, object]] = []
    previous = 0
    for name, cumulative in boundaries:
        if cumulative > previous:
            ranges.append({"name": name, "start_bytes": previous, "end_bytes": cumulative})
        previous = cumulative
    return ranges


def stage_tokens_for_bytes(range_bytes: int) -> int:
    """Whole GiB a byte range occupies on a stage, rounded up.

    Up, because a token is the unit PB can refuse on: rounding down would let
    the last partial GiB of every reservation be unaccounted, and a tier that
    admits a little more than it holds is the shape #583 exists to stop.
    """

    if isinstance(range_bytes, bool) or not isinstance(range_bytes, int) or range_bytes <= 0:
        raise ValueError("a residency range must be a positive number of bytes")
    return -(-range_bytes // GIB)


#: What a mover receipt must carry for a next submission to price CPU and
#: memory off it rather than off a habit.  Named here because both the reader
#: (``mover_demand_from_receipts``) and the writer (``stage_move``) are held to
#: it, and a receipt that predates either field simply contributes nothing.
MOVER_CPU_FIELD = "cpu_seconds"
MOVER_RSS_FIELD = "peak_rss_bytes"

def usable_mover_receipts(
    records: Iterable[Mapping[str, object]], *, tier_id: str,
) -> list[Mapping[str, object]]:
    """The receipts of movers that actually copied onto ``tier_id``.

    A refused receipt measured a refusal, and a receipt from another tier
    measured another box's disks, so neither says anything about the next
    mover onto this one.  ``seconds`` must be positive because every number
    derived below is a rate over it.
    """

    out: list[Mapping[str, object]] = []
    for record in records:
        if not isinstance(record, Mapping):
            continue
        if str(record.get("tier_id") or "") != str(tier_id):
            continue
        if record.get("refusal"):
            continue
        seconds = record.get("seconds")
        if isinstance(seconds, bool) or not isinstance(seconds, (int, float)):
            continue
        if not (seconds > 0):
            continue
        out.append(record)
    return out


def mover_demand_from_receipts(
    records: Iterable[Mapping[str, object]],
    *,
    tier_id: str,
    readers: int,
    fallback_mem_gb: int,
) -> dict[str, object]:
    """The ``cpu`` and ``mem_gb`` a next mover declares, and where they came from.

    Measured, not habitual (``pb_demand_must_be_measured_not_habitual``): the
    numbers are the maxima over the live receipts of movers that copied onto
    this tier.  ``cpu`` is ``ceil(cpu_seconds / seconds)`` -- the mean
    parallelism a copy actually kept busy -- and ``mem_gb`` is
    ``ceil(peak_rss_bytes / GiB)``.  Maxima rather than means, because the
    demand is a reservation: a number that half the movers exceed is a
    reservation half of them run outside.

    With no receipt carrying a field there is nothing to measure, and the
    fallback is a *declared bound* rather than a guess at usage.  For CPU that
    bound is the mover's own structure: it copies through ``readers`` paced
    buffers, so ``readers`` is a width the action cannot exceed once its
    allocation pins it there, and the adaptive controller learns down from a
    declared width.  What it must not be is absent -- an absent ``cpu`` is read
    as *unknown* CPU use and serialized on an otherwise idle host
    (``adaptive_cpu`` ``unbounded_cpu_not_exclusive``), which is #603 and the
    whole of why one mover ran at a time in the first live window (#607).

    Returns the two numbers plus ``demand_source``: per field, whether it was
    measured or declared, and the receipts that were read.
    """

    if isinstance(readers, bool) or not isinstance(readers, int) or readers < 1:
        raise ValueError("readers must be a positive whole number of buffers")
    if isinstance(fallback_mem_gb, bool) or not isinstance(fallback_mem_gb, int) \
            or fallback_mem_gb < 1:
        raise ValueError("fallback_mem_gb must be a positive whole GiB")
    usable = usable_mover_receipts(records, tier_id=tier_id)
    cpu_keys: list[str] = []
    mem_keys: list[str] = []
    cpu: int | None = None
    mem_gb: int | None = None
    for record in usable:
        key = str(record.get("action_key") or "")
        seconds = float(record["seconds"])            # type: ignore[arg-type]
        used = record.get(MOVER_CPU_FIELD)
        if not isinstance(used, bool) and isinstance(used, (int, float)) and used > 0:
            want = max(1, math.ceil(float(used) / seconds))
            cpu = want if cpu is None else max(cpu, want)
            cpu_keys.append(key)
        rss = record.get(MOVER_RSS_FIELD)
        if not isinstance(rss, bool) and isinstance(rss, int) and rss > 0:
            want_mem = max(1, -(-rss // GIB))
            mem_gb = want_mem if mem_gb is None else max(mem_gb, want_mem)
            mem_keys.append(key)
    return {
        "cpu": readers if cpu is None else cpu,
        "mem_gb": fallback_mem_gb if mem_gb is None else mem_gb,
        "demand_source": {
            "tier_id": str(tier_id),
            "receipts_read": len(usable),
            "cpu": "receipts" if cpu is not None else "declared_readers",
            "cpu_receipts": sorted(cpu_keys),
            "mem_gb": "receipts" if mem_gb is not None else "declared_fallback",
            "mem_gb_receipts": sorted(mem_keys),
        },
    }


def residency_demand(
    *,
    tier_id: str,
    range_start_bytes: int,
    range_end_bytes: int,
    tier: str | None = None,
    fill_mb_s_pool_side: int | None = None,
) -> dict[str, int]:
    """The tier demand one movement node's range implies, derived not guessed.

    The capacity ask is arithmetic over the manifest: the bytes between two
    read-order boundaries, in whole GiB, in whichever kind the tier id says it
    deals in.  ``tier`` is optional and only cross-checks what the id already
    declares.  The fill ask is the rate the mover
    intends to draw from the source pool, and it is optional because a tier
    with no attributed receipt has no fill tokens to give (the probe rule);
    when given it is **pool-side**, the only side a shared four-spindle pool
    can be rationed on.
    """

    if range_end_bytes <= range_start_bytes:
        raise ValueError("a residency range must be non-empty and half-open")
    if TIER_DEMAND_SEPARATOR in str(tier_id) or not tier_id:
        raise ValueError(f"malformed tier id {tier_id!r}")
    if tier is not None and tier != tier_kind_of(tier_id):
        raise ValueError(
            f"tier {tier!r} disagrees with the kind {tier_id!r} declares")
    kind = capacity_kind_of(tier_id)
    demand = {
        f"{kind}{TIER_DEMAND_SEPARATOR}{tier_id}":
            stage_tokens_for_bytes(range_end_bytes - range_start_bytes),
    }
    if fill_mb_s_pool_side is not None:
        if isinstance(fill_mb_s_pool_side, bool) or not isinstance(fill_mb_s_pool_side, int) \
                or fill_mb_s_pool_side <= 0:
            raise ValueError("fill_mb_s_pool_side must be a positive whole MB/s")
        demand[f"{FILL_KIND}{TIER_DEMAND_SEPARATOR}{tier_id}"] = fill_mb_s_pool_side
    return demand


def discover_tiers(
    *,
    host: str | None = None,
    runner: Runner | None = None,
    arcstats_path: str = ARCSTATS,
    sysfs: str = SYSFS_BLOCK,
    by_id: str = BY_ID,
    source_pool: str | None = None,
    fill_records: Iterable[Mapping[str, object]] = (),
    now: float | None = None,
) -> dict[str, dict[str, object]]:
    """Every tier this box offers, keyed by tier id, read fresh.

    ``source_pool`` names the pool the export is served from; its members are
    recorded on every stage tier so a mover knows which disks it drains.
    ``fill_records`` are the receipts of moves off that pool, from which the
    fill capacity is learned.  A box with no ``zpool`` and no ``arcstats``
    returns ``{}``: it offers no tier, and that is the true answer.
    """

    host = host or socket.gethostname()
    sampled = time.time() if now is None else now
    tiers: dict[str, dict[str, object]] = {}
    members: list[str] = []
    members_by_id: list[str] = []
    if source_pool:
        leaves = pool_member_paths(source_pool, runner=runner) or []
        members = pool_member_devices(source_pool, runner=runner, sysfs=sysfs)
        members_by_id = [n for n in by_id_names(leaves, by_id=by_id).values() if n]
    fill = fill_rate_from_records(fill_records)
    for pool in stage_pools(runner=runner) or []:
        leaves = pool_member_paths(str(pool["name"]), runner=runner) or []
        names = by_id_names(leaves, by_id=by_id)
        identity = tier_id("stage", host, str(pool["name"]))
        dataset = stage_dataset(str(pool["name"]), runner=runner)
        # ``available`` on the dataset, and nothing else.  ``zpool size`` is
        # the raw geometry: parity, the slop reservation and whatever the
        # pool's other datasets hold are all still inside it, so minting from
        # it hands out tokens for bytes ZFS refuses at ENOSPC -- to a mover
        # that has already read them off the disks.  An unreadable dataset
        # therefore offers **nothing** for that cycle, the way a box with no
        # zpool offers nothing, rather than falling back to a number that is
        # wrong in the one direction that costs an artifact.  The next cycle
        # is 60 s away and the record says why this one is empty.
        tiers[identity] = {
            "schema": TIER_RECORD_SCHEMA_V1,
            "tier": "stage",
            "tier_id": identity,
            "host": host,
            "pool": pool["name"],
            "health": pool["health"],
            "capacity_bytes": (int(dataset["available_bytes"])
                               if dataset is not None else 0),
            "capacity_source": ("zfs available" if dataset is not None
                                else "none (no dataset readable)"),
            "dataset": None if dataset is None else dataset["dataset"],
            "pool_size_bytes": pool["size_bytes"],
            "allocated_bytes": pool["allocated_bytes"],
            "free_bytes": pool["free_bytes"],
            "mountpoint": (dataset["mountpoint"] if dataset is not None
                           and dataset.get("mountpoint")
                           else pool_mountpoint(str(pool["name"]), runner=runner)),
            "members_by_id": sorted(n for n in names.values() if n),
            "members_unresolved": sorted(p for p, n in names.items() if not n),
            "source_pool": source_pool,
            "source_members": members,
            "source_members_by_id": sorted(members_by_id),
            FILL_RECORD_FIELD: fill,
            "sampled_unix": sampled,
        }
    arc = arc_tier(read_arcstats(arcstats_path))
    if arc is not None:
        identity = tier_id("arc", host)
        tiers[identity] = {
            "schema": TIER_RECORD_SCHEMA_V1,
            "tier_id": identity,
            "host": host,
            **arc,
            "source_pool": source_pool,
            "source_members": members,
            FILL_RECORD_FIELD: fill,
            "sampled_unix": sampled,
        }
    return tiers


__all__ = [
    "ARCSTATS",
    "FILL_RECORD_FIELD",
    "POOL_FILL_FIELD",
    "manifest_phase_ranges",
    "capacity_kind_of",
    "residency_demand",
    "mover_demand_from_receipts",
    "usable_mover_receipts",
    "MOVER_CPU_FIELD",
    "MOVER_RSS_FIELD",
    "stage_tokens_for_bytes",
    "tier_kind_of",
    "ARC_CAPACITY_KIND",
    "FILL_KIND",
    "GIB",
    "STAGE_CAPACITY_KIND",
    "STAGE_POOL_PREFIX",
    "TIER_DEMAND_SEPARATOR",
    "TIER_RECORD_SCHEMA_V1",
    "arc_tier",
    "by_id_names",
    "discover_tiers",
    "fill_rate_from_records",
    "pool_member_devices",
    "pool_member_paths",
    "read_arcstats",
    "split_demand",
    "split_demand_key",
    "stage_pools",
    "tier_id",
    "tier_tokens",
    "whole_disk_of",
]
