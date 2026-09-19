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

``ram``
    An explicit tmpfs the operator mounts on the storage box (#640), filled
    and evicted by the DAG rather than warmed reactively.  The mount is the
    tier: capacity is its own ``statvfs``, the ceiling is its own ``size=``,
    the epoch is a marker file at its root that dies with it on reboot, and
    an inadmissible mount (no ``noswap``, an unreadable ``statvfs``, a
    ceiling that cannot coexist with the ARC floor and the system reserve)
    mints nothing.  Sizing is declared in a versioned policy the tier loop
    reads every cycle, so a change is a publish rather than an ssh.

Tokens are minted from these numbers by whichever loop owns the tier's
ledger (``tier_loop.py``), in the units :func:`tier_tokens` names: one
``stage_gib``, ``arc_gib`` or ``ram_gib`` token per GiB, one
``fill_mb_s_pool_side`` token per pool-side MB/s.  A demand against a tier
is *derived* from the action's own data manifest by
:func:`residency_demand`, never typed by hand: the manifest declares the
exact read set, so the bytes a range needs resident are arithmetic over it.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
import json
import math
import os
from pathlib import Path
import secrets
import shutil
import socket
import subprocess
import tempfile
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
#: ``capacity_source`` of a stage tier whose ``capacity_bytes`` is the dataset's
#: ``available``: what ZFS will still let a writer write, net of what is staged.
WRITABLE_CAPACITY_SOURCE = "zfs available"
ARC_CAPACITY_KIND = "arc_gib"
#: The ``primarycache`` settings under which a dataset's *file data* may live
#: in the ARC.  ``metadata`` and ``none`` cache no data at all, so a consumer
#: read of a staged file goes to the device however often it is repeated:
#: measured on dl380g10, 2026-09-18, sparky reading one 5.37 GB file with
#: ``dd iflag=direct`` at 16 x 256 MiB, an ARC miss served 2402 MB/s against
#: 10045 MB/s on a hit -- 4.18x, 19% of the link against 80%.
ARC_DATA_CACHE_SETTINGS = frozenset({"all"})
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

#: The explicit RAM tier (#640): a tmpfs the operator mounts on the storage
#: box, filled and evicted by the DAG.  Its id is ``ram:<host>``, and the
#: prefix is what every reader of a tier id or a fragment uses to tell its
#: business from the stage's and the ARC's.
RAM_TIER_PREFIX = "ram:"
RAM_CAPACITY_KIND = "ram_gib"
#: The policy file that declares the tier's sizing, read fresh by the tier
#: loop every cycle.  Named here so the loop, the publisher and the tests
#: agree on one place.
RAM_POLICY_FILE = "ram_tier_policy.json"
RAM_TIER_POLICY_SCHEMA_V1 = "prismabuild.ram_tier_policy.v1"
#: The file a mounted tmpfs carries at its root, holding the epoch that
#: dates every range the tier admits.  It is written at bootstrap by the
#: tier loop and dies with the tmpfs on reboot, which is the whole
#: mechanism: an epoch that survived the bytes it named would make the
#: identity a no-op.
RAM_EPOCH_MARKER = ".prismabuild-ram-epoch.json"
RAM_EPOCH_MARKER_SCHEMA_V1 = "prismabuild.ram_epoch.v1"


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


#: The schema a mover receipt carries, owned by ``pool.POOL_MOVE_SCHEMA_V1``.
#: Named here -- rather than imported, which would be circular (``pool``
#: imports this module) -- so the fill fold can tell a mover's measurement,
#: which a pool change invalidates, from a prewarm record, which another role
#: stamps and this gate must keep reading (#611).
MOVER_RECEIPT_SCHEMA = "prismaquant.prismabuild.pool_move.v1"


def _coarse_scan(value: str | None) -> str | None:
    """The topology a ``zpool status`` scan line attests to, coarsely.

    The raw line carries percentages, rates and dates that change every
    cycle; keying receipt folds on it would split every receipt into its own
    topology and price nothing ever again.  What matters is whether a
    resilver or scrub is running and whether the last one finished, because
    that is what changes what the pool delivers.
    """

    if value is None:
        return None
    text = value.strip().lower()
    if not text or text.startswith("none"):
        return "none"
    if "in progress" in text:
        if text.startswith("scrub"):
            return "scrub-in-progress"
        if text.startswith("resilver"):
            return "resilver-in-progress"
        return "scan-in-progress"
    if text.startswith("scrub"):
        return "scrub-done"
    if text.startswith("resilver"):
        return "resilver-done"
    return "scan-done"


def pool_identity(pool: str, *, runner: Runner | None = None) -> dict[str, object]:
    """Which pool ``pool`` is, as ``zpool`` describes it right now (#611).

    The ``guid`` names the pool across imports: destroying and recreating a
    pool with the same name mints a new one, which is exactly the event that
    must invalidate every receipt priced off the old pool's disks.  ``state``
    and the coarse ``scan`` say whether a resilver or scrub is running, and
    ``members`` -- the data-vdev leaves ``pool_member_paths`` reads -- say
    whether a swap or an added vdev changed the spindles since.  Together
    they are what a receipt fold compares before pricing a next mover off an
    older measurement.

    Always a dict, never ``None``: callers compare it, and ``None`` would
    read as "no pool here" -- the one answer that must never compare equal
    to a pool that is there.  A pool ``zpool`` will not read is still an
    identity, with nothing in it; it compares unequal to every readable
    one.  (A box that cannot read ``zpool`` at all discovers no tier, so an
    unreadable *current* identity never gates a real fold.)
    """

    call = runner or _run
    try:
        guid_text = call([zpool_binary(), "get", "-Hp",
                          "-o", "value", "guid", pool])
    except (OSError, subprocess.SubprocessError):
        guid_text = ""
    lines = guid_text.strip().splitlines()
    guid = lines[0].strip() if lines else ""
    try:
        status_text = call([zpool_binary(), "status", "-P", pool])
    except (OSError, subprocess.SubprocessError):
        status_text = ""
    state: str | None = None
    scan: str | None = None
    if status_text:
        in_config = False
        for line in status_text.splitlines():
            stripped = line.strip()
            if not in_config:
                if stripped.startswith("config:"):
                    in_config = True
                    continue
                if state is None and stripped.startswith("state:"):
                    state = stripped[len("state:"):].strip() or None
                elif scan is None and stripped.startswith("scan:"):
                    scan = stripped[len("scan:"):].strip() or None
    try:
        members = pool_member_paths(pool, runner=runner) or []
    except (OSError, subprocess.SubprocessError):
        members = []
    return {"pool": pool, "guid": guid or None, "state": state,
            "scan": _coarse_scan(scan), "members": members}


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

    ``primarycache`` comes back in the same read because it is the same kind
    of fact and costs nothing extra: it says whether the file server's ARC may
    hold this dataset's file data at all, which is the whole of the second
    cache layer (#638).  A box whose ``zfs list`` answers fewer columns leaves
    it ``None`` -- unknown, which is never read as permission.
    """

    try:
        text = (runner or _run)([
            zfs_binary(), "list", "-Hp", "-r",
            "-o", "name,available,mountpoint,primarycache", pool,
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
        cache = fields[3] if len(fields) > 3 else "-"
        found[fields[0]] = {
            "dataset": fields[0], "available_bytes": available,
            "mountpoint": fields[2] if fields[2].startswith("/") else None,
            "primarycache": None if cache in ("", "-") else cache,
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


# ------------------------------------------------------------- the RAM tier


def read_ram_policy(path: str | Path) -> dict[str, object] | None:
    """The ram tier's declared sizing, or ``None`` when it is not there to read.

    ``None`` is the honest answer for a generation that predates the file: it
    discovers no ram tier, exactly as a box with no tmpfs does.  A file that
    is there and does not validate answers the same way, because a policy
    this reader refuses is a policy whose numbers it cannot name in a
    refusal -- and the refusal is where the numbers matter.
    """

    try:
        with open(path) as stream:
            value = json.load(stream)
    except (OSError, ValueError):
        return None
    if not isinstance(value, Mapping):
        return None
    if value.get("schema") != RAM_TIER_POLICY_SCHEMA_V1:
        return None
    mountpoint = value.get("mountpoint")
    if not isinstance(mountpoint, str) or not mountpoint.startswith("/"):
        return None
    policy: dict[str, object] = {"schema": RAM_TIER_POLICY_SCHEMA_V1,
                                 "mountpoint": mountpoint}
    for field in ("ceiling_gib_max", "window_gib_default", "arc_floor_gib",
                  "system_reserve_gib"):
        number = value.get(field)
        if isinstance(number, bool) or not isinstance(number, int) or number <= 0:
            return None
        policy[field] = number
    depth = value.get("prefill_depth")
    if depth is not None and (isinstance(depth, bool) or not isinstance(depth, int)
                              or depth <= 0):
        return None
    policy["prefill_depth"] = depth
    if set(value) != set(policy):
        # A field the writer meant and the reader ignores is the quiet half
        # of a disagreement about how big the tier may be.
        return None
    return policy


def ram_mount_options(mountpoint: str, *,
                      proc_mounts: str = "/proc/mounts") -> list[str] | None:
    """The tmpfs mounted at ``mountpoint``'s own options, or ``None``.

    ``None`` is the answer that announces nothing: no tmpfs at the policy's
    mountpoint means no ram tier, the same way no imported stage pool means
    no stage tier.  The *last* matching line wins, because a remount appends
    to ``/proc/mounts`` and the newer line is the mount that answers now.
    The filesystem must be ``tmpfs``: a bind mount or another filesystem at
    the same path is not the ramdisk, whatever its options say.
    """

    try:
        with open(proc_mounts) as stream:
            text = stream.read()
    except OSError:
        return None
    options: list[str] | None = None
    for line in text.splitlines():
        fields = line.split()
        if len(fields) < 4 or fields[1] != mountpoint or fields[2] != "tmpfs":
            continue
        options = fields[3].split(",")
    return options


def meminfo_total_bytes(path: str = "/proc/meminfo") -> int | None:
    """``MemTotal`` as bytes, or ``None`` when the box would not say."""

    try:
        with open(path) as stream:
            for line in stream:
                if line.startswith("MemTotal:"):
                    return int(line.split()[1]) * 1024
    except (OSError, ValueError, IndexError):
        return None
    return None


def read_worker_mem_gb(workers_dir: str | Path, host: str) -> int | None:
    """The job RAM this box's worker loops offer, or ``None``.

    The pool's worker record ``workers/<host>.json`` carries what the box is
    *configured* to offer under ``capacity.mem_gb`` -- the commitment
    placement reads, not the windowed ``observed_capacity`` beside it, which
    can lag a change by a whole observation window.  A box whose loops have
    not announced names no number, and neither does a torn write or a record
    without a usable offer: all of them answer ``None``, which the admission
    below refuses on rather than reading as zero, because a loop can appear
    between cycles.  The file names its own host; the reader trusts the path
    it was asked for, the way every other reader here trusts the path it was
    given.
    """

    try:
        with open(Path(workers_dir) / f"{host}.json") as stream:
            record = json.load(stream)
    except (OSError, ValueError):
        return None
    if not isinstance(record, Mapping):
        return None
    capacity = record.get("capacity")
    if not isinstance(capacity, Mapping):
        return None
    mem_gb = capacity.get("mem_gb")
    if isinstance(mem_gb, bool) or not isinstance(mem_gb, int) or mem_gb < 0:
        return None
    return mem_gb


def read_ram_epoch(root: str | Path) -> dict[str, object] | None:
    """The epoch a mounted tmpfs carries, read-only, or ``None``.

    The promotion node reads this: it must never *create* an epoch, because
    the tier loop is the single writer of the tier's identity and a mover
    that minted its own would date its own bytes.  A marker that is absent,
    unreadable or not the object this module writes all answer ``None``,
    which the mover refuses on -- a promotion nobody can place in time is
    not one to stage.
    """

    try:
        with open(Path(root) / RAM_EPOCH_MARKER) as stream:
            marker = json.load(stream)
    except (OSError, ValueError):
        return None
    if not isinstance(marker, Mapping):
        return None
    if marker.get("schema") != RAM_EPOCH_MARKER_SCHEMA_V1:
        return None
    epoch = marker.get("epoch")
    if not isinstance(epoch, str) or not epoch or "/" in epoch:
        return None
    return {"schema": RAM_EPOCH_MARKER_SCHEMA_V1, "epoch": epoch,
            "unix": marker.get("unix"), "host": marker.get("host")}


def ensure_ram_epoch(root: str | Path, *, host: str,
                     now: float | None = None) -> dict[str, object] | None:
    """Read the mounted tmpfs's epoch, stamping one at bootstrap if it has none.

    The stamp is mount time plus a random nonce, so two mounts never share an
    epoch even within one clock tick -- a reboot and an operator's remount
    are the same event to this mechanism, and both must read as one.  Written
    by temporary and ``os.replace`` like every other fleet record, so a
    reader never sees half a marker.  ``None`` when the root cannot be
    written: a tier whose epoch cannot be stamped cannot date its ranges,
    and admits nothing.
    """

    marker = read_ram_epoch(root)
    if marker is not None:
        return marker
    sampled = int(time.time() if now is None else now)
    marker = {"schema": RAM_EPOCH_MARKER_SCHEMA_V1,
              "epoch": f"{sampled}-{secrets.token_hex(8)}",
              "unix": sampled, "host": host}
    path = Path(root) / RAM_EPOCH_MARKER
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        handle, temporary = tempfile.mkstemp(
            dir=str(path.parent), prefix=f".{path.name}.")
        try:
            with os.fdopen(handle, "w") as stream:
                json.dump(marker, stream, sort_keys=True)
                stream.write("\n")
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
        except BaseException:
            Path(temporary).unlink(missing_ok=True)
            raise
    except OSError:
        return None
    return marker


def _ram_numbers(*, ceiling_bytes: int, mem_total: int | None,
                 arc: Mapping[str, int], policy: Mapping[str, object],
                 worker_mem_gb: int | None = None,
                 ) -> dict[str, object]:
    """Every number a ram refusal must name, in the shape the record carries."""

    arc_floor = max(int(policy["arc_floor_gib"]) * GIB, int(arc.get("arc_meta_used", 0)))
    reserve = int(policy["system_reserve_gib"]) * GIB
    committed = max(int(arc.get("c_max", 0)), arc_floor)
    allowed = (None if mem_total is None
               else max(0, mem_total - committed - reserve))
    # The box's own offered job capacity, in the same units as everything
    # else.  Anything but a non-negative int is "not announced", which the
    # admission refuses on: the one caller that reads the record already
    # sanitises, and this keeps a direct caller fail-closed too.
    worker_demand = (None if (isinstance(worker_mem_gb, bool)
                              or not isinstance(worker_mem_gb, int)
                              or worker_mem_gb < 0)
                     else worker_mem_gb * GIB)
    window = int(policy["window_gib_default"]) * GIB
    return {
        "ceiling_bytes": ceiling_bytes,
        "window_bytes": window,
        "mem_total_bytes": mem_total,
        "arc_c_max": arc.get("c_max"),
        "arc_size": arc.get("size"),
        "arc_meta_used": arc.get("arc_meta_used"),
        # The declared floor and the metadata the ARC cannot drop, whichever
        # is larger: a floor the box is already above is not a floor.
        "arc_floor_bytes": arc_floor,
        "system_reserve_bytes": reserve,
        "worker_demand_bytes": worker_demand,
        "allowed_ceiling_bytes": allowed,
        # What the window may grow to: the roof's budget less the jobs the
        # box has offered to run beside it (#645).
        "allowed_window_bytes": (None if allowed is None or worker_demand is None
                                 else max(0, allowed - worker_demand)),
    }


def ram_admission(*, ceiling_bytes: int, mount_options: list[str] | None,
                  policy: Mapping[str, object], mem_total: int | None,
                  arc: Mapping[str, int],
                   worker_mem_gb: int | None = None) -> dict[str, object]:
    """Whether the warm path may admit against this mount, and why not.

    Five refusals, each fail-closed and each naming the numbers:

    * **``noswap`` is absent.** A swappable tmpfs can page the "resident"
      bytes out, and a consumer whose gate says resident then pays a swap
      read behind a claim of RAM -- the correctness lie this tier exists to
      end.
    * **The ceiling is over the policy's maximum.** The policy records the
      largest ceiling the operator sanctioned; a bigger mount is a decision
      nobody published.
    * **The ceiling cannot coexist with the ARC and the system reserve.**
      ``size=`` is a limit on file bytes, not an allocation, so a tmpfs whose
      roof plus the ARC's own permission plus the reserve exceeds
      ``MemTotal`` never reaches its ENOSPC -- the OOM killer arrives first,
      which is fail-random rather than fail-closed.  The ARC counts at
      whichever is larger: the ``c_max`` it is permitted now (the runbook's
      shrink is an operational precondition, and the guard refuses until it
      is done) or the floor it may never be shrunk past.
    * **The box's offered job capacity is unknown.** ``worker_mem_gb`` is
      what the tier host's own worker record announces under
      ``capacity.mem_gb``; no announcement is not evidence of no jobs, so
      the guard refuses rather than admitting a window beside demand it
      cannot see.
    * **The window cannot coexist with the jobs beside it.** The roof above
      is the mount's ENOSPC backstop; the window is what PB actually fills,
      capped by the ledger, so it is the window that must fit beside the
      announced worker demand: ``window + worker_demand + max(c_max,
      arc_floor) + reserve > MemTotal`` refuses (#645).  Subtracting the
      demand from the roof instead would refuse tonight's live 240 GiB
      mount, which coexists because PB never fills past its 112 GiB
      window.
    """

    numbers = _ram_numbers(ceiling_bytes=ceiling_bytes, mem_total=mem_total,
                           arc=arc, policy=policy,
                           worker_mem_gb=worker_mem_gb)
    if mount_options is not None and "noswap" not in mount_options:
        return {"admissible": False, "reason": "ram_mount_not_noswap", **numbers}
    if ceiling_bytes > int(policy["ceiling_gib_max"]) * GIB:
        return {"admissible": False, "reason": "ram_ceiling_exceeds_policy_max",
                **numbers}
    if mem_total is None:
        return {"admissible": False, "reason": "ram_meminfo_unreadable", **numbers}
    allowed = numbers["allowed_ceiling_bytes"]
    assert isinstance(allowed, int)
    if ceiling_bytes > allowed:
        return {"admissible": False, "reason": "ram_ceiling_exceeds_memtotal_floor",
                **numbers}
    if numbers["worker_demand_bytes"] is None:
        return {"admissible": False, "reason": "ram_worker_demand_unknown",
                **numbers}
    allowed_window = numbers["allowed_window_bytes"]
    window = numbers["window_bytes"]
    assert isinstance(allowed_window, int) and isinstance(window, int)
    if window > allowed_window:
        return {"admissible": False, "reason": "ram_window_exceeds_memtotal_floor",
                **numbers}
    return {"admissible": True, "reason": None, **numbers}


def ram_tier(policy: Mapping[str, object], *, host: str,
             statvfs: Callable[[str], os.statvfs_result] = os.statvfs,
             proc_mounts: str = "/proc/mounts",
             meminfo_path: str = "/proc/meminfo",
             arcstats_path: str = ARCSTATS,
             stats: Mapping[str, int] | None = None,
             now: float | None = None,
             worker_mem_gb: int | None = None) -> dict[str, object] | None:
    """The ram tier record, or ``None`` when the mount is absent.

    Capacity is the tmpfs's own ``statvfs`` -- ``f_bavail x f_frsize``, what
    the mount may still hold -- never ``MemAvailable``, which moves with
    other tenants' habits and is not placed RAM.  The ceiling is the mount's
    own ``f_blocks x f_frsize``, the roof where a full ``noswap`` tmpfs
    answers ENOSPC.  A mount whose ``statvfs`` cannot be read is announced
    rather than hidden: capacity zero, the refusal on the record, no tokens
    minted, so an operator reading the announced record sees a tmpfs that is
    not answering rather than a tier that quietly vanished.
    """

    mountpoint = str(policy["mountpoint"])
    options = ram_mount_options(mountpoint, proc_mounts=proc_mounts)
    if options is None:
        return None
    epoch = ensure_ram_epoch(mountpoint, host=host, now=now)
    arc = dict(stats) if stats is not None else read_arcstats(arcstats_path)
    mem_total = meminfo_total_bytes(meminfo_path)
    try:
        sampled = statvfs(mountpoint)
        ceiling = int(sampled.f_blocks) * int(sampled.f_frsize)
        capacity = int(sampled.f_bavail) * int(sampled.f_frsize)
        error = None
    except OSError as exc:
        ceiling = capacity = 0
        error = str(exc)
    if error is not None:
        admission: dict[str, object] = {
            **_ram_numbers(ceiling_bytes=0, mem_total=mem_total, arc=arc,
                           policy=policy, worker_mem_gb=worker_mem_gb),
            "admissible": False, "reason": "ram_statvfs_unreadable", "error": error}
    elif epoch is None:
        admission = {**ram_admission(ceiling_bytes=ceiling, mount_options=options,
                                     policy=policy, mem_total=mem_total, arc=arc,
                                     worker_mem_gb=worker_mem_gb),
                     "admissible": False, "reason": "ram_epoch_unwritable"}
    else:
        admission = ram_admission(ceiling_bytes=ceiling, mount_options=options,
                                  policy=policy, mem_total=mem_total, arc=arc,
                                  worker_mem_gb=worker_mem_gb)
    return {
        "schema": TIER_RECORD_SCHEMA_V1,
        "tier": "ram",
        "tier_id": tier_id("ram", host),
        "host": host,
        "mountpoint": mountpoint,
        "mount_options": options,
        "epoch": None if epoch is None else str(epoch["epoch"]),
        "size_bytes": ceiling,
        "ceiling_bytes": ceiling,
        "capacity_bytes": capacity,
        "window_gib": int(policy["window_gib_default"]),
        "ram_admission": admission,
        "sampled_unix": time.time() if now is None else now,
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
    """``prismabuild-stage:dl380g10`` -> ``"stage"``; ``ram:dl380g10`` -> ``"ram"``;
    anything else -> ``"arc"``.

    The tier id *is* the discovery key, so the kind is read off it rather than
    carried beside it: a stage tier is named for the PB-owned pool it is, a
    ram tier for the prefix PB's own policy gave it, and the only other tier
    a box offers is its ARC.  One rule, so that the demand
    a range implies and the capacity a ledger mints can never disagree about
    which token kind a tier deals in.
    """

    text = str(tier_id)
    if text.startswith(STAGE_POOL_PREFIX):
        return "stage"
    if text.startswith(RAM_TIER_PREFIX):
        return "ram"
    return "arc"


def stage_arc_eligibility(record: Mapping[str, object]) -> dict[str, object]:
    """Whether this stage dataset's bytes can reach the file server's ARC.

    The second cache layer is the storage box holding the staged blocks a
    Spark reads over NFS, and ZFS's ``primarycache`` decides whether it may:
    ``all`` caches data and metadata, ``metadata`` and ``none`` cache no file
    data, so a consumer read of a staged file reaches the SSD every time.

    Three answers, never two.  ``all`` permits the warm; ``metadata``/``none``
    refuse it, which is a **configured** refusal and worth saying out loud;
    and a setting this box could not read is unknown, which is not permission
    -- a warm spends reads, and an unattested setting does not license them.
    Returned as data rather than a bool because every consumer of it (the tier
    record, the submitter's plan, the mover's receipt) has to say *why*.
    """

    value = record.get("primarycache")
    setting = str(value) if isinstance(value, str) and value else None
    if setting is None:
        return {"eligible": False, "primarycache": None,
                "reason": ("this box did not report the dataset's primarycache, "
                           "and an unread setting is not permission to warm")}
    if setting in ARC_DATA_CACHE_SETTINGS:
        return {"eligible": True, "primarycache": setting,
                "reason": f"primarycache={setting}: the ARC may hold this dataset's data"}
    return {"eligible": False, "primarycache": setting,
            "reason": (f"primarycache={setting}: the ARC holds no file data for "
                       f"this dataset, so a warm read would land nowhere.  Set "
                       f"primarycache=all on the stage dataset")}


def capacity_kind_of(tier_id: str) -> str:
    """The token kind that tier's capacity is counted in."""

    kind = tier_kind_of(tier_id)
    if kind == "stage":
        return STAGE_CAPACITY_KIND
    if kind == "ram":
        return RAM_CAPACITY_KIND
    return ARC_CAPACITY_KIND


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


#: A receipt whose pool reads are less than this share of the bytes it staged
#: did not measure the pool, and the fill fold skips it whole (#654).  A mover
#: that ADOPTS resident stage ranges -- the fleet's own optimization, where a
#: resubmitted campaign's phases are adopted instead of re-read from the pool
#: -- barely touches the pool: its pacing's ``mean_pool_read_mb_s`` measures
#: only the few device reads it made while its ``bytes_staged`` shows GiB
#: served from the stage.  Live on 2026-09-19 in ``pb-queue/movers/`` on tier
#: ``prismabuild-stage:dl380g10``: adoption receipts 2cdd1396 / a4a07f40 /
#: c1441fda / 4bbaa6aa read pool_read_bytes at 0.003-0.036 of their
#: bytes_staged with mean_pool_read 1.5-6.5 MB/s, while the honest receipt
#: b14ebfcf read 1.81x its bytes_staged at 311.7 MB/s.  Read as a pool
#: measurement, an adoption shortfall sealed a 6.5 MB/s ceiling no later
#: receipt of the same shape could refute and no honest mover could fit under
#: (``never_fits_tier_capacity``), so none could ever land either.
#: ``pool_read_bytes >= POOL_MEASUREMENT_MIN_SHARE * bytes_staged`` is what
#: counts as a measurement; a receipt missing either counter keeps the bare
#: treatment it had before, because nothing in it says "this window read the
#: stage instead of the pool".
POOL_MEASUREMENT_MIN_SHARE = 0.5

#: What a mover reserved of the pool, as it reserved it.  ``stage_move`` copies
#: this out of its own command line into the receipt so a later cycle can ask
#: whether the pool delivered what the ledger had promised -- which is the one
#: question that distinguishes "the supply can grow" from "this is the ceiling".
MOVER_FILL_DEMAND_FIELD = "fill_demand_mb_s_pool_side"

#: How many movers were reading this tier when a copy began, that copy
#: included.  ``mean_pool_read_mb_s`` is the *pool's* delivery, so one copy's
#: share of it is unreadable without this count, and a demand priced off the
#: aggregate would reserve the whole pool per mover.
MOVER_CONCURRENCY_FIELD = "movers_claimed_on_tier"


def _delivered(record: Mapping[str, object]) -> float | None:
    pacing = record.get("disk_pacing")
    if not isinstance(pacing, Mapping):
        return None
    rate = pacing.get(POOL_FILL_FIELD)
    if isinstance(rate, bool) or not isinstance(rate, (int, float)) or rate <= 0:
        return None
    return float(rate)


def _fell_short(record: Mapping[str, object]) -> bool:
    """Whether this reader drew less of the pool than the ledger had promised it.

    Measured, with no tolerance and no constant: the bytes it staged over the
    wall seconds it was *reading* -- its elapsed time less the seconds the
    pacer held it, because a held reader was stopped on purpose and its
    shortfall is the pacer's doing rather than the pool's -- against the
    ``fill_mb_s_pool_side`` its own claim reserved.  A reader that got what it
    reserved says the supply was honest; one that did not, while the pool was
    delivering ``mean_pool_read_mb_s``, says the supply was priced above what
    the disks can give to that many readers at once.
    """

    sealed = record.get(MOVER_FILL_DEMAND_FIELD)
    if isinstance(sealed, bool) or not isinstance(sealed, (int, float)) or sealed <= 0:
        return False
    staged = record.get("bytes_staged")
    seconds = record.get("seconds")
    if isinstance(staged, bool) or not isinstance(staged, (int, float)) or staged <= 0:
        return False
    if isinstance(seconds, bool) or not isinstance(seconds, (int, float)) or seconds <= 0:
        return False
    pacing = record.get("disk_pacing")
    held = 0.0
    if isinstance(pacing, Mapping):
        value = pacing.get("held_seconds")
        if not isinstance(value, bool) and isinstance(value, (int, float)) and value > 0:
            held = float(value)
    span = float(seconds) - held
    if span <= 0:
        return False
    return (float(staged) / 1e6 / span) < float(sealed)


def _measured_the_pool(record: Mapping[str, object]) -> bool:
    """Whether this receipt's window read the bytes it staged off the pool.

    A mover that adopts resident stage ranges stages its bytes from the stage
    and reads the pool only incidentally, so its ``mean_pool_read_mb_s`` says
    nothing about what the pool delivers to a reader that actually reads it
    (#654): the counter below is the tell, and a window whose pool reads are
    less than ``POOL_MEASUREMENT_MIN_SHARE`` of the bytes it staged is not a
    pool measurement at all -- it can set no ceiling, refute none, and raise
    no best, because any of the three would be priced off device reads the
    staged bytes never needed.

    A receipt missing either counter says nothing either way and keeps the
    bare ``_delivered`` treatment: only both counters together can witness
    "the stage, not the pool, served this window".
    """

    pacing = record.get("disk_pacing")
    if not isinstance(pacing, Mapping):
        return True
    pool_read = pacing.get("pool_read_bytes")
    if (isinstance(pool_read, bool) or not isinstance(pool_read, (int, float))
            or pool_read < 0):
        return True
    staged = record.get("bytes_staged")
    if (isinstance(staged, bool) or not isinstance(staged, (int, float))
            or staged <= 0):
        return True
    return float(pool_read) >= POOL_MEASUREMENT_MIN_SHARE * float(staged)


def mover_fill_demand_from_receipts(
    records: Iterable[Mapping[str, object]], *, tier_id: str,
    pool_identity: Mapping[str, object] | None = None,
) -> int | None:
    """The pool bandwidth a next mover reserves, or ``None`` with nothing measured.

    Priced per receipt and maximised across them, never mixed: each receipt
    bounds *one* mover's draw two ways, and the smaller bound is that receipt's
    answer.

    * Its own file-side rate is what a copy of this shape achieved -- but a warm
      one achieved it out of the ARC and the disks never produced it (a live
      receipt: 1478 MB/s for 3.3 GB off four spindles).
    * Its window's pool delivery over the movers that shared that window is what
      one of them can have drawn on average.  The division is the point.
      ``mean_pool_read_mb_s`` is the *pool's* number: a window shared by three
      copies reports three copies' worth, and reading it as one mover's rate
      would reserve the whole pool for each of them -- which re-serializes
      movers through the fill token, the same failure the CPU demand fixes,
      arriving by another resource kind.

    A receipt missing either side, or missing its concurrency count, prices
    nothing; with no receipt priced the answer is ``None`` and the mover
    reserves no fill, because a guessed bandwidth is the habit this replaces.
    """

    best: float | None = None
    for record in usable_mover_receipts(records, tier_id=tier_id,
                                        pool_identity=pool_identity):
        rate = record.get("mb_per_s_file_side")
        if isinstance(rate, bool) or not isinstance(rate, (int, float)) or rate <= 0:
            continue
        delivered = _delivered(record)
        if delivered is None:
            continue
        sharers = record.get(MOVER_CONCURRENCY_FIELD)
        if isinstance(sharers, bool) or not isinstance(sharers, int) or sharers < 1:
            continue
        share = min(float(rate), delivered / sharers)
        best = share if best is None else max(best, share)
    if best is None:
        return None
    demand = int(best)
    return demand if demand > 0 else None


def fill_supply_from_records(
    records: Iterable[Mapping[str, object]],
    *, pool_identity: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """How much pool bandwidth a tier may offer, folded over its receipts.

    The problem this solves is that ``max`` over receipts cannot grow past the
    concurrency that produced them if the number it maximises is one reader's
    own rate.  ``mean_pool_read_mb_s`` is not that number -- it is what the
    whole pool delivered while that reader ran, whoever else was reading -- so
    the maximum does rise the first time two movers overlap.  What is still
    needed is a reason to *let* them overlap the first time, and a reason to
    stop.

    Both are measured, and the whole thing is a pure fold over the receipts in
    ``unix`` order, so the same history mints the same supply however often it
    is read:

    * A receipt that *fell short* of the fill it reserved, while the pool was
      delivering what it delivered, is a measured ceiling: that delivery is
      what the disks give, and the ledger had promised more.  The most recent
      such receipt sets ``ceiling_mb_s``.
    * Any later receipt whose delivery exceeded that ceiling refutes it -- the
      pool did more than the ceiling claimed -- and clears it, which is how a
      resilver finishing or a competing consumer leaving is noticed without an
      age or a decay.
    * ``best_mb_s`` is the highest delivery observed after the last ceiling,
      or over all receipts when there is none.

    A caller with no ceiling may offer ``best_mb_s`` plus one more reader's
    worth and watch what happens: if the pool keeps up, the next receipt raises
    ``best_mb_s``; if it does not, that receipt sets the ceiling and the growth
    stops where the disks stopped.  ``may_grow`` says which case this is.  The
    "one more reader's worth" is the probe rule's number -- the oldest ready
    item's own sealed demand -- and it stays with the loop that can see the
    queue, so nothing here invents a size.

    Residual, stated rather than hidden: at the ceiling the supply can flap by
    one reader, because a window one reader below the ceiling may deliver a
    little more than the ceiling recorded and refute it.  That is harmless
    here -- a consumer is protected from a reader by the pacer's hold, not by
    this ledger -- and removing it would need a hysteresis nothing measures.

    One receipt among these is not a pool measurement at all: a mover that
    adopts resident stage ranges reads the pool only incidentally, so its
    ``mean_pool_read_mb_s`` prices the few device reads it made rather than
    the pool's delivery, and a window that fell short of its sealed fill
    while barely reading the pool would seal a ceiling no later receipt of
    that shape could refute and no honest mover could fit under (#654).  A
    receipt whose pool reads are less than ``POOL_MEASUREMENT_MIN_SHARE`` of
    the bytes it staged is therefore skipped whole -- no ceiling, no
    refutation, no best -- and one missing either counter keeps the bare
    treatment above.
    """

    ordered = sorted(
        (record for record in records if isinstance(record, Mapping)),
        key=lambda record: float(record.get("unix", 0.0) or 0.0))
    ceiling: float | None = None
    best: float | None = None
    ceiling_key: str | None = None
    for record in ordered:
        if pool_identity is not None and (
                record.get("schema") == MOVER_RECEIPT_SCHEMA):
            # A mover's measurement of a pool, gated on the pool it measured
            # (#611): after a resilver or a rebuild the old receipts still
            # name this tier, and the ceiling they set would throttle the new
            # pool by the old one's shortfall.  Anything that is not a mover
            # receipt -- a prewarm record, stamped by another role with no
            # tier and no identity -- is the pool's other measurement and
            # keeps being read; keying that is a separate change.
            stamped = record.get("pool_identity")
            if not isinstance(stamped, Mapping) or stamped != pool_identity:
                continue
        delivered = _delivered(record)
        if delivered is None:
            continue
        if not _measured_the_pool(record):
            continue
        if ceiling is not None and delivered > ceiling:
            ceiling, ceiling_key, best = None, None, None
        if _fell_short(record):
            ceiling = delivered
            ceiling_key = str(record.get("action_key") or "")
            best = None
            continue
        best = delivered if best is None else max(best, delivered)
    return {
        "ceiling_mb_s": ceiling,
        "ceiling_receipt": ceiling_key,
        "best_mb_s": best,
        "may_grow": ceiling is None,
    }


def tier_tokens(record: Mapping[str, object]) -> dict[str, int]:
    """The tokens a tier record mints: whole GiB of capacity, whole MB/s of fill."""

    tokens: dict[str, int] = {}
    tier = record.get("tier")
    capacity = record.get("capacity_bytes")
    if isinstance(capacity, int) and not isinstance(capacity, bool) and capacity > 0:
        if tier == "stage":
            tokens[STAGE_CAPACITY_KIND] = capacity // GIB
        elif tier == "ram":
            # Admitted capacity only, and never past the policy's window: a
            # refused mount (``ram_admission`` on the record) mints nothing
            # however much statvfs reported, and a window smaller than the
            # mount is what PB actually fills.  The ceiling is a roof, not a
            # target.
            admission = record.get("ram_admission")
            if isinstance(admission, Mapping) and admission.get("admissible") is True:
                window = record.get("window_gib")
                gib = capacity // GIB
                if (isinstance(window, int) and not isinstance(window, bool)
                        and window > 0):
                    gib = min(gib, window)
                if gib > 0:
                    tokens[RAM_CAPACITY_KIND] = gib
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


def _checked_phase_boundaries(
    phases: list, *, total: int, entry_boundaries: set[int],
) -> list[tuple[str, int]] | None:
    """One refusal rule for both manifest schemas' phase tables (#594).

    A phase that is not a mapping, a name that is missing, empty, repeated
    or not a string, a cumulative that is not an integer (bools and floats
    included -- ``int()`` accepts ``True`` and truncates, and neither is a
    byte count), a table that steps back, overruns its total, cuts an entry,
    or ends anywhere but the total is not a description of this manifest,
    and a mover priced off it would reserve for bytes nobody reads.  ``None``
    is that refusal; the caller answers it with ``[]``.
    """

    boundaries: list[tuple[str, int]] = []
    previous = 0
    seen: set[str] = set()
    for phase in phases:
        if not isinstance(phase, Mapping):
            return None
        name = phase.get("name")
        cumulative = phase.get("cumulative_bytes")
        if not isinstance(name, str) or not name or name in seen:
            return None
        if isinstance(cumulative, bool) or not isinstance(cumulative, int):
            return None
        if (cumulative < previous or cumulative > total
                or cumulative not in entry_boundaries):
            return None
        seen.add(name)
        boundaries.append((name, cumulative))
        previous = cumulative
    if previous != total:
        return None
    return boundaries


def _read_order_sizes(
    phases: list, entry_sizes: list[int],
) -> list[int] | None:
    """The byte sizes of the v2 read plan's consumption order, or ``None``.

    v1 consumes ``entries`` in list order, so its boundaries are the list's
    own prefix sums.  v2 consumes them in ``read_plan`` order, which exists
    to differ -- so the boundaries a v2 table is checked against are the
    prefix sums of the plan's own ``entry_indices`` expansion, not of the
    list.  Checking a reordered plan against list-order sums would refuse a
    table the core validator accepted, which is the two-readers disagreement
    this shared rule removes.  A plan that does not say what it reads, or
    names entries it cannot have, refuses.
    """

    order: list[int] = []
    for phase in phases:
        indices = phase.get("entry_indices")
        if not isinstance(indices, list):
            return None
        seen: set[int] = set()
        for ref in indices:
            if isinstance(ref, bool) or not isinstance(ref, int):
                return None
            if ref < 0 or ref >= len(entry_sizes):
                return None
            if ref in seen:
                return None
            seen.add(ref)
            order.append(ref)
    return [entry_sizes[index] for index in order]


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
    on the wrong boundaries reserves for bytes nobody will read.  The v2 table
    in ``read_plan.phases`` is held to the same rule, against the plan's own
    consumption order: both schemas declare a running byte sum over the
    entries in the order the action consumes them, so one validator reads
    both.
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
        entries = manifest.get("entries", []) or []
        entry_sizes: list[int] = []
        for entry in entries:
            if not isinstance(entry, Mapping):
                return []
            entry_sizes.append(int(entry.get("bytes", 0) or 0))
        total = int(manifest.get("total_bytes", 0) or 0)
        if sum(entry_sizes) != total:
            return []
        read = plan.get("read_bytes")
        if isinstance(read, bool) or not isinstance(read, int):
            return []
        read_sizes = _read_order_sizes(phases, entry_sizes)
        if read_sizes is None or sum(read_sizes) != read:
            return []
        entry_boundaries: set[int] = set()
        running = 0
        for size in read_sizes:
            running += size
            entry_boundaries.add(running)
        checked = _checked_phase_boundaries(
            phases, total=read, entry_boundaries=entry_boundaries)
        if checked is None:
            return []
        boundaries = checked
    else:
        annotations = manifest.get("annotations")
        if not isinstance(annotations, Mapping):
            return []
        declared = annotations.get("phases")
        if not isinstance(declared, list) or not declared:
            return []
        total = int(manifest.get("total_bytes", 0) or 0)
        entry_boundaries = set()
        running = 0
        for entry in manifest.get("entries", []) or []:
            if not isinstance(entry, Mapping):
                return []
            running += int(entry.get("bytes", 0) or 0)
            entry_boundaries.add(running)
        if running != total:
            return []
        checked = _checked_phase_boundaries(
            declared, total=total, entry_boundaries=entry_boundaries)
        if checked is None:
            return []
        boundaries = checked
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
    pool_identity: Mapping[str, object] | None = None,
) -> list[Mapping[str, object]]:
    """The receipts of movers that actually copied onto ``tier_id``.

    A refused receipt measured a refusal, and a receipt from another tier
    measured another box's disks, so neither says anything about the next
    mover onto this one.  ``seconds`` must be positive because every number
    derived below is a rate over it.

    With ``pool_identity`` given -- the tier's current one, as
    :func:`discover_tiers` stamps it -- only receipts carrying that same
    identity are usable (#611).  After a resilver, a member swap, an added
    vdev or a pool rebuild, the old receipts still name this tier id but
    measured another pool's disks; pricing cpu, mem and the fill share off
    them is pricing the new pool by the old one's habits.  A receipt with no
    identity predates the stamping and cannot prove which pool it measured,
    so it is dropped too: failing closed re-measures through the probe rule
    rather than guessing.  Without the argument there is no gate, exactly as
    before, for tiers announced by a generation that stamps none.
    """

    out: list[Mapping[str, object]] = []
    for record in records:
        if not isinstance(record, Mapping):
            continue
        if str(record.get("tier_id") or "") != str(tier_id):
            continue
        if record.get("refusal"):
            continue
        if pool_identity is not None:
            stamped = record.get("pool_identity")
            if not isinstance(stamped, Mapping) or stamped != pool_identity:
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
    pool_identity: Mapping[str, object] | None = None,
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
    usable = usable_mover_receipts(records, tier_id=tier_id,
                                     pool_identity=pool_identity)
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
            "pool_identity": dict(pool_identity) if pool_identity is not None else None,
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

    The demand names the range's **durable** tier -- the SSD it lands on --
    and nothing else.  The second cache layer's RAM budget is deliberately
    not a leg here: a mover co-demanding the ARC's GiB would bound the
    published SSD window by min(stage, ARC) and then, once the ARC shrinks
    for an explicit RAM tier, throttle layer 1's read-ahead with it (#638's
    part 3, withdrawn for that reason).  RAM occupancy is spent by the RAM
    tier's own movement nodes when that tier exists -- a promotion node's
    demand names ``ram:<host>`` through this same function, one leg per node,
    the withdrawn plumbing aimed at the right actor -- and a warm read-back
    authorises itself against the dataset's ``primarycache`` instead.
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
    ram_policy: Mapping[str, object] | None = None,
    statvfs: Callable[[str], os.statvfs_result] = os.statvfs,
    proc_mounts: str = "/proc/mounts",
    meminfo_path: str = "/proc/meminfo",
    worker_mem_gb: int | None = None,
) -> dict[str, dict[str, object]]:
    """Every tier this box offers, keyed by tier id, read fresh.

    ``source_pool`` names the pool the export is served from; its members are
    recorded on every stage tier so a mover knows which disks it drains.
    ``fill_records`` are the receipts of moves off that pool, from which the
    fill capacity is learned.  A box with no ``zpool`` and no ``arcstats``
    returns ``{}``: it offers no tier, and that is the true answer.

    ``ram_policy`` is the RAM tier's declared sizing (``read_ram_policy``
    validated it); ``None`` discovers no ram tier, and so does a policy whose
    mountpoint carries no tmpfs -- the mount is the tier, the way an imported
    pool is a stage tier.  ``worker_mem_gb`` is what the tier host's worker
    record announces under ``capacity.mem_gb`` (``read_worker_mem_gb``
    reads it); ``None`` refuses the ram admission fail-closed, because no
    announcement is not evidence of no jobs.
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
            "capacity_source": (WRITABLE_CAPACITY_SOURCE if dataset is not None
                                else "none (no dataset readable)"),
            "dataset": None if dataset is None else dataset["dataset"],
            # Whether the file server's ARC may hold this dataset's file data
            # at all: the second cache layer's own precondition, discovered
            # with everything else rather than assumed (#638).
            "primarycache": (dataset.get("primarycache")
                             if dataset is not None else None),
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
            # Which pools these numbers were read off (#611): the guid names
            # the pool across a destroy/recreate, the coarse scan says
            # whether a resilver is running, and the members say whether a
            # swap or an added vdev changed the spindles.  Movers copy this
            # into their receipts, and the receipt folds price only receipts
            # carrying the tier's current one -- so a rebuilt pool
            # re-measures through the probe rule instead of running on the
            # old pool's habits.
            "pool_identity": {
                "stage": pool_identity(str(pool["name"]), runner=runner),
                "source": (pool_identity(source_pool, runner=runner)
                           if source_pool else None),
            },
            FILL_RECORD_FIELD: fill,
            "sampled_unix": sampled,
        }
    arc_stats = read_arcstats(arcstats_path)
    arc = arc_tier(arc_stats)
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
    if ram_policy is not None:
        record = ram_tier(
            ram_policy, host=host, statvfs=statvfs, proc_mounts=proc_mounts,
            meminfo_path=meminfo_path, arcstats_path=arcstats_path,
            stats=arc_stats, now=now, worker_mem_gb=worker_mem_gb)
        if record is not None:
            tiers[str(record["tier_id"])] = record
    return tiers


__all__ = [
    "ARCSTATS",
    "FILL_RECORD_FIELD",
    "POOL_FILL_FIELD",
    "POOL_MEASUREMENT_MIN_SHARE",
    "MOVER_FILL_DEMAND_FIELD",
    "MOVER_CONCURRENCY_FIELD",
    "MOVER_RECEIPT_SCHEMA",
    "fill_supply_from_records",
    "mover_fill_demand_from_receipts",
    "manifest_phase_ranges",
    "capacity_kind_of",
    "stage_arc_eligibility",
    "residency_demand",
    "mover_demand_from_receipts",
    "usable_mover_receipts",
    "MOVER_CPU_FIELD",
    "MOVER_RSS_FIELD",
    "stage_tokens_for_bytes",
    "tier_kind_of",
    "ARC_CAPACITY_KIND",
    "ARC_DATA_CACHE_SETTINGS",
    "FILL_KIND",
    "GIB",
    "RAM_CAPACITY_KIND",
    "RAM_EPOCH_MARKER",
    "RAM_EPOCH_MARKER_SCHEMA_V1",
    "RAM_POLICY_FILE",
    "RAM_TIER_POLICY_SCHEMA_V1",
    "RAM_TIER_PREFIX",
    "STAGE_CAPACITY_KIND",
    "STAGE_POOL_PREFIX",
    "TIER_DEMAND_SEPARATOR",
    "TIER_RECORD_SCHEMA_V1",
    "arc_tier",
    "by_id_names",
    "discover_tiers",
    "ensure_ram_epoch",
    "fill_rate_from_records",
    "meminfo_total_bytes",
    "pool_identity",
    "pool_member_devices",
    "pool_member_paths",
    "ram_admission",
    "ram_mount_options",
    "ram_tier",
    "read_arcstats",
    "read_ram_epoch",
    "read_ram_policy",
    "read_worker_mem_gb",
    "split_demand",
    "split_demand_key",
    "stage_pools",
    "tier_id",
    "tier_tokens",
    "whole_disk_of",
]
