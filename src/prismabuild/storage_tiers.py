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
``stage_gib`` or ``arc_gib`` token per GiB, one ``fill_mb_s`` token per MB/s.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
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
FILL_KIND = "fill_mb_s"
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


def fill_rate_from_records(records: Iterable[Mapping[str, object]]) -> float | None:
    """The best sustained pool-side MB/s any recorded move off a tier drew, or ``None``.

    The number is ``disk_pacing.mean_self_read_mb_s``: what the pool's own
    members delivered to this reader over the whole window, attributed by the
    pacer (#580).  It is deliberately **not** the record's file-side
    ``mb_per_s``.  A warm that finds its bytes already in the ARC reports
    file-side rates the disks never produced -- one live receipt on
    dl380g10 says 1141 MB/s for 206 GB off a four-spindle raidz1 -- and a
    fill capacity minted from that would admit movers the disks cannot feed.
    A record without pool-side attribution therefore says nothing about the
    pool, and a tier with no attributed record has no fill tokens, which is
    the probe rule.  The maximum over records is the demonstrated capacity;
    a later record that beats it raises the mint on the next cycle, so a
    deeper reader or a quieter pool shows up as tokens without anybody
    editing a number.
    """

    best: float | None = None
    for record in records:
        pacing = record.get("disk_pacing")
        if not isinstance(pacing, Mapping):
            continue
        rate = pacing.get("mean_self_read_mb_s")
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
    fill = record.get("fill_mb_s")
    if isinstance(fill, (int, float)) and not isinstance(fill, bool) and fill > 0:
        tokens[FILL_KIND] = int(fill)
    return tokens


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
        tiers[identity] = {
            "schema": TIER_RECORD_SCHEMA_V1,
            "tier": "stage",
            "tier_id": identity,
            "host": host,
            "pool": pool["name"],
            "health": pool["health"],
            "capacity_bytes": pool["size_bytes"],
            "allocated_bytes": pool["allocated_bytes"],
            "free_bytes": pool["free_bytes"],
            "mountpoint": pool_mountpoint(str(pool["name"]), runner=runner),
            "members_by_id": sorted(n for n in names.values() if n),
            "members_unresolved": sorted(p for p, n in names.items() if not n),
            "source_pool": source_pool,
            "source_members": members,
            "source_members_by_id": sorted(members_by_id),
            "fill_mb_s": fill,
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
            "fill_mb_s": fill,
            "sampled_unix": sampled,
        }
    return tiers


__all__ = [
    "ARCSTATS",
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
