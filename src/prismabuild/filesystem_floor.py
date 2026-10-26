"""Physical filesystem floor through existing host and tier reservation owners.

The default mode is off. Enforce requires an authority-supplied, versioned
mapping, closed population and complete growth bounds in each binding.
Unknown evidence or incomplete token custody is a HOLD. Every held byte
reservation, including local_gib, is charged whole; materialization grants
no credit. Used paths share the same guard and authenticated backing keys.

Refresh snapshots each ledger under its actual owner lock between two floor
sections. Grants since the first section remain charged, even if the owner
snapshot also saw them. Owner locks are never acquired under a floor lock.
A pending or failed refresh leaves a refusal sample. Admission persists its
counter charge before taking tokens, so partial failures cannot admit an
uncharged reservation. Release authority remains with the existing owners.
"""
from __future__ import annotations

import argparse
from collections.abc import Iterable, Mapping
from contextlib import ExitStack, contextmanager
import ipaddress
import json
import math
import os
from pathlib import Path
import re
import shutil
import socket
import subprocess
import sys
import time
import uuid

from . import filesystem_capacity, posix_lock

GIB = 1 << 30
#: The floor is one twentieth (five percent) of the filesystem, rounded up.
FLOOR_DIVISOR = 20
#: Token kinds whose tokens are bytes on a filesystem, with each one's unit.
#: ``ram_gib`` and ``arc_gib`` are memory and the ``*_mb_s`` kinds are rates.
BYTE_KINDS: dict[str, int] = {"spool_gb": GIB, "stage_gib": GIB, "local_gib": GIB,
                              "filesystem_gib": GIB}
FILESYSTEM_KIND = "filesystem_gib"
FLOOR_DIR = "filesystem-floor"
#: Where ``register --filesystem-gib`` mints a filesystem's growth ledger;
#: beside ``reservations/`` (boxes) and ``tier-reservations/`` so no reader
#: of either mistakes it for a box or a tier.
FILESYSTEM_RESERVATIONS = "filesystem-reservations"
MODE_ENV = "PRISMABUILD_FILESYSTEM_FLOOR"
MODES = ("off", "observe", "enforce")
MODE_CACHE_S = 10.0
#: How long the acquisition gate trusts this process's cached mode and
#: bindings.  The loop tick refreshes both outside every lock, so a looping
#: process never reads them inside a ledger section.
GATE_CACHE_S = 120.0
#: A published sample older than this is no sample.  Owners refresh every
#: :data:`REFRESH_INTERVAL_S`; the gap tolerates a slow cycle or two.
SAMPLE_MAX_AGE_S = 300.0
#: A sample stamped this far in the future is accepted as clock skew.
CLOCK_SKEW_S = 30.0
REFRESH_INTERVAL_S = 30.0
LOCK_WAIT_S = 5.0
LOCK_POLL_S = 0.05
COMMAND_TIMEOUT_S = 10.0
OPERATION_PREFIX = "operation-"
OPERATION_LEASE_S = 3600.0
BINDING_SCHEMA = "prismabuild.filesystem_floor.binding.v1"
SAMPLE_SCHEMA = "prismabuild.filesystem_floor.sample.v1"
MEMBER_SCHEMA = "prismabuild.filesystem_floor.member.v1"
OWNER_SCHEMA = "prismabuild.filesystem_floor.operation_owner.v1"
NFS_TYPES = frozenset({"nfs", "nfs4"})


class FloorError(RuntimeError):
    """An identity, record or census that cannot be read or trusted."""

    def __init__(self, message: str, *, reason: str = "unreadable"):
        self.reason = reason
        super().__init__(message)


class FloorRefused(RuntimeError):
    """An enforced refusal; ``verdicts`` says which filesystem and why."""

    def __init__(self, verdicts: list[dict[str, object]]):
        self.verdicts = verdicts
        super().__init__("; ".join(describe_verdict(v) for v in verdicts if not v["allowed"]))


# -- the predicate -----------------------------------------------------------

def floor_bytes(size_bytes: int) -> int:
    return (size_bytes + FLOOR_DIVISOR - 1) // FLOOR_DIVISOR


def floor_verdict(filesystem: str, *, size_bytes: int, free_bytes: int,
            charge_bytes: int = 0, demand_bytes: int = 0,
            floor: int | None = None, **detail) -> dict[str, object]:
    """The one integer comparison, with every term it used."""

    numbers = (size_bytes, free_bytes, charge_bytes, demand_bytes)
    if any(type(n) is not int or n < 0 for n in numbers) or size_bytes <= 0:
        raise FloorError(f"{filesystem}: space or charge is not a whole byte count")
    floor = floor_bytes(size_bytes) if floor is None else floor
    required = floor + charge_bytes + demand_bytes
    inode_refusal = detail.get("inode_refusal")
    allowed = free_bytes >= required and inode_refusal is None
    return {"filesystem": filesystem, "allowed": allowed,
            "reason": ("below_inode_floor" if inode_refusal is not None else
                       "ok" if allowed else "below_floor"),
            "size_bytes": size_bytes, "free_bytes": free_bytes,
            "floor_bytes": floor, "charge_bytes": charge_bytes,
            "demand_bytes": demand_bytes, "required_bytes": required, **detail}


def floor_refusal(filesystem: str, reason: str, **detail) -> dict[str, object]:
    return {"filesystem": filesystem, "allowed": False, "reason": reason, **detail}


def describe_verdict(v: Mapping[str, object]) -> str:
    if v.get("inode_refusal") is not None:
        return f"{v['filesystem']}: {v['reason']} {v['inode_refusal']}"
    if "required_bytes" in v:
        return (f"{v['filesystem']}: {v['reason']} free {v['free_bytes']} "
                f"< floor {v['floor_bytes']} + charge {v['charge_bytes']} + "
                f"demand {v['demand_bytes']}" if not v["allowed"] else
                f"{v['filesystem']}: ok free {v['free_bytes']} >= {v['required_bytes']}")
    return f"{v['filesystem']}: {v['reason']}" + (
        f" ({v['detail']})" if v.get("detail") else "")


# -- identity and sampling ---------------------------------------------------

def _machine_id() -> str:
    value = Path("/etc/machine-id").read_text().strip()
    if not re.fullmatch(r"[0-9a-f]{32}", value):
        raise FloorError("machine identity unknown")
    return value


def _floor_boot_id() -> str:
    return Path("/proc/sys/kernel/random/boot_id").read_text().strip()


def _owner_incarnation() -> dict[str, object]:
    return {"machine_id": _machine_id(), "boot_id": _floor_boot_id(),
            "mount_namespace": os.stat("/proc/self/ns/mnt").st_ino}


def _statx_mount_id(fd: int) -> int:
    import ctypes

    # Linux statx UAPI: stx_mask at 0x00, stx_mnt_id at 0x90, 256 bytes.
    class MountStat(ctypes.Structure):
        _fields_ = [("mask", ctypes.c_uint32), ("before_mount", ctypes.c_byte * 140),
                    ("mount", ctypes.c_uint64), ("after_mount", ctypes.c_byte * 104)]

    libc = ctypes.CDLL(None, use_errno=True)
    try:
        statx = libc.statx
    except AttributeError:
        raise FloorError("descriptor mount identity unavailable") from None
    statx.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int,
                     ctypes.c_uint, ctypes.POINTER(MountStat)]
    statx.restype = ctypes.c_int
    value = MountStat()
    if statx(fd, b"", 0x1000, 0x1000, ctypes.byref(value)) != 0:
        raise FloorError(f"descriptor mount identity failed: errno {ctypes.get_errno()}")
    if not value.mask & 0x1000 or not value.mount:
        raise FloorError("descriptor mount identity unknown")
    return int(value.mount)


def _mount_row(fd: int, *, mount_id: int | None = None) -> tuple[str, str, str]:
    """``(type, source, super options)`` of the mount the descriptor is on."""

    if mount_id is None:
        mount_id = _statx_mount_id(fd)
    rows = [line.split(" - ", 1) for line in
            Path("/proc/self/mountinfo").read_text().splitlines()
            if line.split(" ", 1)[0] == str(mount_id)]
    if len(rows) != 1 or len(rows[0]) != 2 or len(rows[0][1].split()) < 3:
        raise FloorError("mount row unknown")
    fstype, source, options = rows[0][1].split()[:3]
    return fstype, source, options


def _command(argv: list[str]) -> str:
    binary = shutil.which(argv[0]) or next(
        (p for p in (f"/usr/sbin/{argv[0]}", f"/sbin/{argv[0]}") if os.path.exists(p)), None)
    if binary is None:
        raise FloorError(f"{argv[0]} is not installed")
    try:
        return subprocess.run([binary, *argv[1:]], check=True, capture_output=True,
                              text=True, timeout=COMMAND_TIMEOUT_S).stdout
    except (OSError, subprocess.SubprocessError) as exc:
        raise FloorError(f"{' '.join(argv)}: {exc}") from exc


def _zfs_space(pool: str) -> tuple[int, int]:
    """Usable size and free of a pool: its root dataset's used + available.

    ``zpool list`` free includes raidz parity and the slop reservation, so it
    overstates what can be written (zpoolprops(7)); the root dataset's
    ``available`` is what the pool can still give every dataset in it.
    """

    fields = _command(["zfs", "get", "-Hp", "-o", "value", "used,available", pool]).split()
    if len(fields) != 2 or not all(f.isascii() and f.isdigit() for f in fields):
        raise FloorError(f"zfs pool {pool} space unreadable")
    used, available = map(int, fields)
    return used + available, available


def _block_uuid(device: int) -> str | None:
    """The filesystem UUID udev names for a block device, if it names one."""

    try:
        names = os.listdir("/dev/disk/by-uuid")
    except OSError:
        return None
    for name in sorted(names):
        try:
            if os.stat(f"/dev/disk/by-uuid/{name}").st_rdev == device:
                return name
        except OSError:
            continue
    return None


def identify(path: str | os.PathLike) -> dict[str, object]:
    """The filesystem ``path`` is on, its stable key and a fresh sample.

    Symlinks are resolved first (an interpreter path is usually one).  A
    path that is not a directory is identified by its directory, and one
    that does not exist yet by its nearest existing ancestor, where it would
    be created.  Every error is a :class:`FloorError`: an unreadable
    identity is never a pass.
    """

    try:
        real = Path(os.path.abspath(path))
        while True:
            try:
                real = real.resolve(strict=True)
                break
            except FileNotFoundError:
                if real == real.parent:
                    raise
                real = real.parent
        if not real.is_dir():
            real = real.parent
        fd = os.open(real, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    except OSError as exc:
        raise FloorError(f"{path}: {exc}") from exc
    try:
        mount_id = _statx_mount_id(fd)
        fstype, source, options = _mount_row(fd, mount_id=mount_id)
        stat = os.fstat(fd)
        space = os.fstatvfs(fd)
        room = filesystem_capacity.local_disk_room(real, 5, statvfs=lambda path: space)
        size, free = room["size_bytes"], room["free_bytes"]
        found: dict[str, object] = {"path": str(real), "fstype": fstype,
                                    "source": source, "device": stat.st_dev,
                                    "mount_id": mount_id}
        if fstype in NFS_TYPES:
            servers = [o[5:] for o in options.split(",") if o.startswith("addr=")]
            if len(servers) != 1:
                raise FloorError(f"{real}: NFS server address unknown")
            found.update(key=None, server=str(ipaddress.ip_address(servers[0])))
        elif fstype == "zfs":
            pool = source.split("/", 1)[0]
            guid = _command(["zpool", "get", "-Hp", "-o", "value", "guid", pool]).strip()
            if not guid.isascii() or not guid.isdigit():
                raise FloorError(f"zfs pool {pool} guid unreadable")
            size, free = _zfs_space(pool)
            found.update(key=f"zfs:{guid}", pool=pool)
            if source != pool:
                dataset_size, dataset_free = _zfs_space(source)
                found["capacity_constraints"] = [{"filesystem": f"zfs:{guid}:{source}",
                    "size_bytes": dataset_size, "free_bytes": dataset_free}]
        else:
            # The mount's source device, not st_dev: btrfs reports an
            # anonymous st_dev that names no block device.
            uuid_name = None
            if source.startswith("/dev/"):
                try:
                    uuid_name = _block_uuid(os.stat(source).st_rdev)
                except OSError:
                    uuid_name = None
            uuid_name = uuid_name or _block_uuid(stat.st_dev)
            found["key"] = (f"uuid:{uuid_name}" if uuid_name else
                            f"volatile:{_machine_id()}:{_floor_boot_id()}:{fstype}:{stat.st_dev}")
        if size <= 0 or not 0 <= free <= size:
            raise FloorError(f"{real}: space sample invalid")
        found.update(size_bytes=size, free_bytes=free)
        found.update({name: room[name] for name in
                      ("size_inodes", "free_inodes", "floor_inodes", "inode_refusal")})
        return found
    except OSError as exc:
        raise FloorError(f"{real}: {exc}") from exc
    finally:
        os.close(fd)


#: Interfaces whose addresses every box has (container and VM bridges), so
#: they can never say which server an NFS mount names.
_VIRTUAL_INTERFACES = ("lo", "docker", "br-", "veth", "virbr", "cni", "flannel")


def _sample(found: Mapping[str, object]) -> dict:
    """Fresh bytes and inodes after identity was proved outside the floor lock."""

    try:
        if found["fstype"] != "zfs":
            if os.stat(found["path"]).st_dev != found["device"]:
                raise FloorError(f"{found['path']}: filesystem changed under the sample")
        room = filesystem_capacity.local_disk_room(found["path"], 5)
    except OSError as exc:
        raise FloorError(f"{found['path']}: {exc}") from exc
    if found["fstype"] == "zfs":
        room["size_bytes"], room["free_bytes"] = _zfs_space(str(found["pool"]))
    if room["size_bytes"] <= 0 or not 0 <= room["free_bytes"] <= room["size_bytes"]:
        raise FloorError(f"{found['path']}: space sample invalid")
    sampled = {name: room[name] for name in ("size_bytes", "free_bytes",
               "size_inodes", "free_inodes", "floor_inodes", "inode_refusal")}
    if found["fstype"] == "zfs" and found.get("source", found["pool"]) != found["pool"]:
        size, free = _zfs_space(str(found["source"]))
        sampled["capacity_constraints"] = [{"filesystem": str(found["source"]),
                                             "size_bytes": size, "free_bytes": free}]
    return sampled


def _host_addresses() -> list[str]:
    """This host's addresses an NFS client can name it by.

    Loopback, link-local and container/VM bridge addresses are left out: a
    client's ``addr=`` naming one of those names its own box, not this one.
    """

    try:
        rows = json.loads(_command(["ip", "-j", "address", "show"]))
        found = set()
        for row in rows:
            if str(row.get("ifname", "")).startswith(_VIRTUAL_INTERFACES):
                continue
            for a in row.get("addr_info", []):
                address = ipaddress.ip_address(a["local"])
                if not (address.is_loopback or address.is_link_local):
                    found.add(str(address))
        return sorted(found)
    except (FloorError, ValueError, KeyError, TypeError):
        return []


# -- shared records ----------------------------------------------------------

def floor_root(queue_root) -> Path:
    return Path(queue_root) / FLOOR_DIR


def _floor_digest(text: str) -> str:
    from .core import raw_sha256

    return raw_sha256(text.encode())[:32]


def fs_dir(queue_root, key: str) -> Path:
    return floor_root(queue_root) / "fs" / _floor_digest(key)


def member_id(ledger_root: str, ledger: str, kind: str) -> str:
    return f"{ledger_root}/{ledger}/{kind}"


def member_path(queue_root, ledger_root: str, ledger: str, kind: str) -> Path:
    return floor_root(queue_root) / "members" / (
        _floor_digest(member_id(ledger_root, ledger, kind)) + ".json")


def _floor_read(path: Path) -> dict | None:
    try:
        value = json.loads(path.read_text())
    except FileNotFoundError:
        return None
    except (OSError, ValueError) as exc:
        raise FloorError(f"{path}: {exc}") from exc
    if not isinstance(value, dict):
        raise FloorError(f"{path}: not a record")
    return value


def _floor_write(path: Path, value: Mapping) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex[:8]}")
    with open(temporary, "w") as stream:
        json.dump(value, stream)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    parent = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        os.fsync(parent)
    finally:
        os.close(parent)


def bindings(queue_root) -> list[dict]:
    base = floor_root(queue_root) / "fs"
    try:
        names = sorted(os.listdir(base))
    except FileNotFoundError:
        return []
    found = []
    for name in names:
        record = _floor_read(base / name / "binding.json")
        if record is not None:
            found.append(record)
    return found


_MODE_CACHE: dict[str, tuple[float, str]] = {}


def mode(queue_root, *, max_age_s: float = MODE_CACHE_S) -> str:
    """The floor mode in force for this process; ``off`` unless set.

    An unrecognized value is read as ``observe``: it never silently enforces
    and never silently disappears.  A mode file that cannot be read keeps
    the last value this process read (``off`` if none): a mount hiccup
    neither turns the floor on nor off.
    """

    value = os.environ.get(MODE_ENV)
    if value is None:
        key = str(queue_root)
        cached = _MODE_CACHE.get(key)
        now = time.monotonic()
        if cached is not None and now - cached[0] < max_age_s:
            return cached[1]
        try:
            value = (floor_root(queue_root) / "mode").read_text().strip()
        except FileNotFoundError:
            value = "off"
        except (OSError, ValueError) as exc:
            value = cached[1] if cached is not None else "off"
            _log(f"mode unreadable ({exc}); keeping {value}")
        value = value if value in MODES else "observe"
        _MODE_CACHE[key] = (now, value)
        return value
    return value if value in MODES else "observe"


def _log(message: str) -> None:
    print(f"[filesystem-floor] {message}", file=sys.stderr, flush=True)


# -- census ------------------------------------------------------------------

_RECORDS: dict[str, tuple[float, dict[str, dict]]] = {}
_LOCK_PATHS: dict[str, Path] = {}


def _lock_path(directory: Path) -> Path:
    """The floor lock's canonical name, resolved once per process.

    Resolution is a lookup on the shared store; the bindings cache primes
    it outside every lock, so the gate never resolves a name inside one.
    """

    key = str(directory)
    path = _LOCK_PATHS.get(key)
    if path is None:
        directory.mkdir(parents=True, exist_ok=True)
        path = _LOCK_PATHS[key] = posix_lock.canonical(directory / ".floor.lock")
    return path


def member_bindings(queue_root, *, max_age_s: float = GATE_CACHE_S) -> dict[str, dict]:
    """Each bound member's binding, from this process's cache when fresh."""

    key = str(queue_root)
    cached = _RECORDS.get(key)
    now = time.monotonic()
    if cached is not None and now - cached[0] < max_age_s:
        return cached[1]
    found = bindings(queue_root)
    table = {}
    for b in found:
        for m in b.get("members", []):
            mid = member_id(m["ledger_root"], m["ledger"], m["kind"])
            if mid in table:
                raise FloorError(f"conflicting member mapping: {mid}", reason="mapping_unknown")
            table[mid] = b
    for b in found:
        try:
            _lock_path(fs_dir(queue_root, str(b["key"])))
        except OSError:
            pass                  # resolved, or refused, at first use instead
    _RECORDS[key] = (now, table)
    return table


def census(queue_root, binding: Mapping) -> dict[str, int]:
    from .pool import PoolQueue, ResourceLedger

    coverage = _coverage(binding)
    owners: dict[tuple[str, str], set[str]] = {}
    for m in binding["members"]:
        owners.setdefault((m["ledger_root"], m["ledger"]), set()).add(m["kind"])
    counted = {}
    for (root, name), kinds in owners.items():
        owner = (PoolQueue(queue_root).tier(name) if root == "tier-reservations"
                 else ResourceLedger(Path(queue_root) / root, host=name))
        try:
            snapshot = owner.floor_snapshot(kinds)
        except (OSError, RuntimeError) as exc:
            raise FloorError(f"{root}/{name}: {exc}", reason="population_unknown") from exc
        for kind, count in snapshot.items():
            mid = member_id(root, name, kind)
            counted[mid] = count * coverage["bytes_per_token"][mid]
    return counted


def coverage_identity(binding: Mapping) -> str:
    """Scope an authority's coverage to this exact mapping and member version."""

    from .digest_primitives import canonical_sha256

    return canonical_sha256({
        name: binding.get(name) for name in
        ("key", "root", "fstype", "pool", "source", "owner_host", "machine_id",
         "boot_id", "mount_namespace", "registered_unix", "members", "destinations")})


def _coverage(binding: Mapping, *, now: float | None = None) -> dict:
    """Read closure and bounds supplied by the existing release authority."""

    coverage = binding.get("coverage")
    if not isinstance(coverage, dict):
        raise FloorError("no authoritative closed population", reason="population_unknown")
    for field in ("version", "mapping_version", "population_version", "bounds_version",
                  "writer_fence", "authority", "configuration_version"):
        if not isinstance(coverage.get(field), str) or not coverage[field].strip():
            raise FloorError(f"coverage missing {field}", reason="population_unknown")
    if coverage.get("scope_sha256") != coverage_identity(binding):
        raise FloorError("coverage scope changed", reason="mapping_unknown")
    evidence = coverage.get("evidence")
    if not isinstance(evidence, list) or not evidence or any(
            not isinstance(ref, str) or not ref.strip() for ref in evidence):
        raise FloorError("coverage has no authority evidence", reason="population_unknown")
    start, end = coverage.get("valid_from_unix"), coverage.get("valid_until_unix")
    if any(type(n) not in (int, float) or not math.isfinite(n) for n in (start, end)):
        raise FloorError("coverage interval unknown", reason="population_unknown")
    if not start <= (time.time() if now is None else now) < end:
        raise FloorError("coverage outside its effective interval", reason="population_unknown")
    units = coverage.get("bytes_per_token")
    expected = {member_id(m["ledger_root"], m["ledger"], m["kind"]): BYTE_KINDS[m["kind"]]
                for m in binding["members"]}
    if not isinstance(units, dict) or units.keys() != expected.keys() or any(
            type(units[mid]) is not int or units[mid] < unit for mid, unit in expected.items()):
        raise FloorError("whole-reservation bounds incomplete", reason="bounds_unknown")
    extra = coverage.get("additional_bytes")
    if type(extra) is not int or extra < 0:
        raise FloorError("other writer and residue bounds unknown", reason="bounds_unknown")
    return coverage


def _granted(directory: Path) -> int:
    record = _floor_read(directory / "granted.json")
    value = None if record is None else record.get("granted_bytes")
    if type(value) is not int or value < 0:
        raise FloorError(f"{directory}: grant counter malformed")
    return value


@contextmanager
def floor_locked(directory: Path, *, wait_s: float | None = None):
    """The filesystem's floor lock, waited for up to ``wait_s``.

    Yields ``False`` when the wait ran out.  Nothing is acquired inside it.
    A lock that cannot be opened raises ``OSError``; every caller turns
    that into a verdict.
    """

    deadline = time.monotonic() + (LOCK_WAIT_S if wait_s is None else wait_s)
    path = _lock_path(directory)
    while True:
        with posix_lock.held(path, blocking=False, canonicalized=True) as acquired:
            if acquired:
                yield True
                return
        if time.monotonic() >= deadline:
            yield False
            return
        time.sleep(LOCK_POLL_S)


def published_verdict(queue_root, binding: Mapping, *, demand_bytes: int = 0,
                      now: float | None = None) -> dict[str, object]:
    """Check one binding from its published sample and its grant counter.

    Read lock-free by a used-path check (sample first, then the counter,
    which only grows) and under the floor lock by an acquisition.
    """

    key = str(binding["key"])
    directory = fs_dir(queue_root, key)
    if binding.get("status") != "active":
        return floor_refusal(key, "binding_" + str(binding.get("status")),
                       detail=binding.get("status_detail"))
    current = _floor_read(directory / "binding.json")
    if current is None or coverage_identity(current) != coverage_identity(binding):
        return floor_refusal(key, "mapping_unknown")
    binding = current
    try:
        coverage = _coverage(binding, now=now)
    except FloorError as exc:
        return floor_refusal(key, exc.reason, detail=str(exc))
    sample = _floor_read(directory / "sample.json")
    if sample is None or sample.get("key") != key:
        return floor_refusal(key, "no_sample")
    if sample.get("refusal"):
        return floor_refusal(key, sample["refusal"], detail=sample.get("detail"))
    from .digest_primitives import canonical_sha256
    if (sample.get("coverage_sha256") != canonical_sha256(coverage)
            or sample.get("binding_sha256") != coverage_identity(binding)):
        return floor_refusal(key, "population_unknown", detail="sample coverage changed")
    if any(name not in sample for name in
           ("size_inodes", "free_inodes", "floor_inodes", "inode_refusal")):
        return floor_refusal(key, "inode_sample_missing")
    sampled_at = sample.get("sampled_unix")
    grant_position = sample.get("granted_at")
    if (type(sampled_at) not in (int, float) or not math.isfinite(sampled_at)
            or type(grant_position) is not int or grant_position < 0):
        return floor_refusal(key, "population_unknown", detail="invalid sample position")
    age = (time.time() if now is None else now) - sampled_at
    if not -CLOCK_SKEW_S <= age <= SAMPLE_MAX_AGE_S:
        return floor_refusal(key, "sample_stale", detail=f"age {age:.0f}s")
    since = _granted(directory) - grant_position
    if since < 0:
        return floor_refusal(key, "population_unknown", detail="grant counter regressed")
    try:
        return _sample_verdict(key, sample, demand_bytes=demand_bytes,
                               since=since, age=age)
    except (FloorError, KeyError, TypeError, ValueError) as exc:
        return floor_refusal(key, "population_unknown", detail=str(exc))


def _sample_verdict(key: str, sample: Mapping, *, demand_bytes: int,
                    since: int, age: float) -> dict[str, object]:
    """Calculate the verdict independently of the shared record reads."""

    if type(sample["census_bytes"]) is not int or sample["census_bytes"] < 0:
        raise FloorError("invalid census byte count", reason="population_unknown")
    verdict = floor_verdict(key, size_bytes=sample["size_bytes"],
                   free_bytes=sample["free_bytes"],
                   charge_bytes=sample["census_bytes"] + since,
                   demand_bytes=demand_bytes, sample_age_s=round(age, 1),
                   **{name: sample[name] for name in
                      ("size_inodes", "free_inodes", "floor_inodes", "inode_refusal")})
    constraints = [floor_verdict(str(c["filesystem"]), size_bytes=c["size_bytes"],
                    free_bytes=c["free_bytes"], charge_bytes=verdict["charge_bytes"],
                    demand_bytes=demand_bytes) for c in sample.get("capacity_constraints", [])]
    if constraints:
        verdict["constraints"] = constraints
        if any(not c["allowed"] for c in constraints):
            verdict.update(allowed=False, reason="below_dataset_floor")
    return verdict


# -- registration and refresh ------------------------------------------------

def register(queue_root, root, *, members: Iterable[tuple[str, str, str]] = (),
             filesystem_gib: int | None = None, filesystem_ledger: str | None = None,
             now: float | None = None, coverage: Mapping | None = None) -> dict:
    """Bind ``root``'s filesystem, and the named ledger kinds, on this host.

    Must run where ``root`` is local.  Idempotent: an existing binding of the
    same stable key gains the new members.  A member bound to another key is
    refused -- unbind it first.  ``filesystem_gib`` mints (or grows) the
    filesystem's own growth ledger ``filesystem-reservations/<name>`` and
    binds it.  Ends with a refresh, so the binding is usable at once.
    """

    found = identify(root)
    if found["key"] is None:
        raise FloorError(f"{root}: register on the NFS server, where it is local")
    if str(found["key"]).startswith("volatile:"):
        raise FloorError(f"{root}: {found['fstype']} has no stable identity to bind")
    members = [tuple(m) for m in members]
    for ledger_root, _ledger, kind in members:
        if kind not in BYTE_KINDS:
            raise FloorError(f"{kind} is not a byte kind")
        if ledger_root not in ("reservations", "tier-reservations", FILESYSTEM_RESERVATIONS):
            raise FloorError(f"{ledger_root} is not a ledger root")
    if filesystem_gib is not None:
        name = filesystem_ledger or f"fs-{socket.gethostname()}-{found.get('pool') or 'root'}"
        if not re.fullmatch(r"[A-Za-z0-9_.:-]+", name):
            raise FloorError(f"{name!r} is not a ledger name")
        from .pool import ResourceLedger
        ResourceLedger(Path(queue_root) / FILESYSTEM_RESERVATIONS, host=name).ensure_capacity(
            {FILESYSTEM_KIND: int(filesystem_gib)})
        members.append((FILESYSTEM_RESERVATIONS, name, FILESYSTEM_KIND))
    key = str(found["key"])
    directory = fs_dir(queue_root, key)
    with floor_locked(directory, wait_s=60.0) as acquired:
        if not acquired:
            raise FloorError(f"{key}: floor lock busy")
        for ledger_root, ledger, kind in members:
            existing = _floor_read(member_path(queue_root, ledger_root, ledger, kind))
            if existing is not None and existing["key"] != key:
                raise FloorError(f"{member_id(ledger_root, ledger, kind)} is bound to "
                                 f"{existing['key']}; unbind it first")
        binding = _floor_read(directory / "binding.json")
        if binding is None:
            _floor_write(directory / "granted.json", {"granted_bytes": 0})
        binding = binding or {
            "schema": BINDING_SCHEMA, "key": key, "members": [],
            "registered_unix": time.time() if now is None else now}
        listed = {(m["ledger_root"], m["ledger"], m["kind"]) for m in binding["members"]}
        for ledger_root, ledger, kind in members:
            if (ledger_root, ledger, kind) not in listed:
                binding["members"].append(
                    {"ledger_root": ledger_root, "ledger": ledger, "kind": kind})
        binding.update(
            status="active", status_detail=None, root=found["path"],
            fstype=found["fstype"], pool=found.get("pool"),
            source=found.get("source"), **_owner_incarnation(),
            owner_host=socket.gethostname(),
            size_bytes=found["size_bytes"], server_addresses=_host_addresses())
        destination = {"root": found["path"], "source": found.get("source"),
                       "device": found["device"], "mount_id": found.get("mount_id")}
        if destination not in binding.setdefault("destinations", []):
            binding["destinations"].append(destination)
        if coverage is not None:
            binding["coverage"] = dict(coverage)
        _floor_write(directory / "binding.json", binding)
        for m in binding["members"]:
            _floor_write(member_path(queue_root, m["ledger_root"], m["ledger"], m["kind"]),
                   {"schema": MEMBER_SCHEMA, "key": key, **m})
    _RECORDS.pop(str(queue_root), None)
    refreshed = refresh_binding(queue_root, binding, now=now)
    return {"binding": binding, "refresh": refreshed}


def unbind(queue_root, ledger_root: str, ledger: str, kind: str) -> dict | None:
    """Remove one member from its binding (a moved spool, a retired ledger)."""

    path = member_path(queue_root, ledger_root, ledger, kind)
    member = _floor_read(path)
    if member is None:
        return None
    directory = fs_dir(queue_root, member["key"])
    with floor_locked(directory, wait_s=60.0) as acquired:
        if not acquired:
            raise FloorError(f"{member['key']}: floor lock busy")
        binding = _floor_read(directory / "binding.json")
        if binding is not None:
            binding["members"] = [m for m in binding["members"] if
                                  (m["ledger_root"], m["ledger"], m["kind"])
                                  != (ledger_root, ledger, kind)]
            _floor_write(directory / "binding.json", binding)
        path.unlink(missing_ok=True)
    _RECORDS.pop(str(queue_root), None)
    return member


def refresh_binding(queue_root, binding: Mapping, *, now: float | None = None) -> dict:
    """Capture owners outside the floor lock, retaining intervening grants."""

    key = str(binding["key"])
    directory = fs_dir(queue_root, key)
    identified_binding = coverage_identity(binding)
    with floor_locked(directory) as acquired:
        if not acquired:
            return floor_refusal(key, "floor_lock_busy")
        binding = _floor_read(directory / "binding.json")
        if binding is None:
            return floor_refusal(key, "mapping_unknown")
        if coverage_identity(binding) != identified_binding:
            return floor_refusal(key, "mapping_unknown", detail="binding changed during identity capture")
        if binding.get("status") != "active":
            return floor_refusal(key, "binding_" + str(binding.get("status")))
        # A crash or incomplete census leaves dependent admission on HOLD.
        pending = {"schema": SAMPLE_SCHEMA, "key": key, "refusal": "population_unknown",
                   "detail": "refresh incomplete", "refresh_id": uuid.uuid4().hex}
        _floor_write(directory / "sample.json", pending)
        try:
            coverage = _coverage(binding, now=now)
            granted_before = _granted(directory)
        except FloorError as exc:
            return floor_refusal(key, exc.reason, detail=str(exc))
    from .digest_primitives import canonical_sha256

    identity = coverage_identity(binding)
    coverage_hash = canonical_sha256(coverage)
    try:
        found = identify(binding["root"])
    except (FloorError, OSError) as exc:
        return floor_refusal(key, "mapping_unknown", detail=str(exc))
    if (found["key"] != key or binding.get("source") != found.get("source")
            or any(binding.get(field) != value
                   for field, value in _owner_incarnation().items())):
        detail = f"{binding['root']} is now {found['key']}"
        with floor_locked(directory) as acquired:
            current = _floor_read(directory / "binding.json") if acquired else None
            if (current is not None and coverage_identity(current) == identity
                    and _floor_read(directory / "sample.json") == pending):
                _floor_write(directory / "binding.json",
                             {**binding, "status": "moved", "status_detail": detail})
                _floor_write(directory / "sample.json",
                             {**pending, "refusal": "binding_moved", "detail": detail})
        return floor_refusal(key, "binding_moved", detail=detail)
    try:
        counted = census(queue_root, binding)
    except FloorError as exc:
        return floor_refusal(key, exc.reason, detail=str(exc))
    with floor_locked(directory) as acquired:
        if not acquired:
            return floor_refusal(key, "floor_lock_busy")
        if _floor_read(directory / "sample.json") != pending:
            return floor_refusal(key, "population_unknown", detail="refresh superseded")
        current = _floor_read(directory / "binding.json")
        if (current is None or coverage_identity(current) != identity
                or canonical_sha256(_coverage(current, now=now)) != coverage_hash):
            return floor_refusal(key, "population_unknown", detail="coverage changed during census")
        granted = _granted(directory)
        if granted < granted_before:
            return floor_refusal(key, "population_unknown", detail="grant counter regressed")
        space = _sample(found)
        constraints = {str(c["filesystem"]): c for c in space.get("capacity_constraints", [])}
        for destination in binding["destinations"]:
            actual = identify(destination["root"])
            if (actual["key"] != key or actual.get("source") != destination["source"]
                    or actual.get("mount_id") != destination.get("mount_id")
                    or actual["device"] != destination["device"]):
                return floor_refusal(key, "mapping_unknown", detail="destination mapping changed")
            if actual["fstype"] == "zfs":
                for constraint in _sample(actual).get("capacity_constraints", []):
                    constraints[str(constraint["filesystem"])] = constraint
        if constraints:
            space["capacity_constraints"] = list(constraints.values())
        sample = {"schema": SAMPLE_SCHEMA, "key": key, "host": socket.gethostname(),
                  "boot_id": _floor_boot_id(),
                  "sampled_unix": time.time() if now is None else now,
                  **space,
                  "census": counted,
                  "census_bytes": (sum(counted.values()) + granted - granted_before
                                   + coverage["additional_bytes"]),
                  "binding_sha256": identity, "coverage_sha256": coverage_hash,
                  "granted_at": granted}
        _floor_write(directory / "sample.json", sample)
    return _sample_verdict(key, sample, demand_bytes=0, since=0, age=0)


_LAST_REFRESH: dict[str, float] = {}


def refresh_local(queue_root, *, force: bool = False) -> list[dict] | None:
    """Refresh every binding this machine owns, at most once an interval.

    Called by the worker and tier loops on every poll; returns ``None`` when
    not yet due.  Never raises for one binding: its error is its verdict.
    """

    key = str(queue_root)
    now = time.monotonic()
    if not force and now - _LAST_REFRESH.get(key, float("-inf")) < REFRESH_INTERVAL_S:
        return None
    _LAST_REFRESH[key] = now
    machine = _machine_id()
    results = []
    for binding in bindings(queue_root):
        if binding.get("machine_id") != machine or binding.get("status") != "active":
            continue
        try:
            results.append(refresh_binding(queue_root, binding))
        except (FloorError, OSError) as exc:
            results.append(floor_refusal(str(binding["key"]), "refresh_failed", detail=str(exc)))
    return results


# -- admission of byte acquisitions -------------------------------------------

class LedgerGate:
    """The floor check for one ``begin_acquire``; see :func:`ledger_gate`."""

    def __init__(self, queue_root, floor_mode: str, demand: dict[str, int],
                 groups: dict[str, dict], unbound: list[str]):
        self.queue_root = queue_root
        self.mode = floor_mode
        self.demand = demand
        self.groups = groups          # key -> {"binding":..., "bytes": n}
        self.unbound = unbound
        self.verdicts: list[dict[str, object]] = []
        self.locked: set[str] = set()

    @contextmanager
    def admitted(self):
        """Yield whether the acquisition may proceed, holding the floor locks.

        Call :meth:`reserve` before taking tokens, still inside the block.
        """

        self.verdicts = [floor_refusal(m, "unbound_byte_ledger") for m in self.unbound]
        self.locked = set()
        with ExitStack() as stack:
            for key in sorted(self.groups, key=_floor_digest):
                group = self.groups[key]
                try:
                    held = stack.enter_context(floor_locked(fs_dir(self.queue_root, key)))
                except OSError as exc:
                    self.verdicts.append(floor_refusal(key, "floor_lock_error", detail=str(exc)))
                    continue
                if not held:
                    self.verdicts.append(floor_refusal(key, "floor_lock_busy"))
                    continue
                self.locked.add(key)
                try:
                    current = _floor_read(fs_dir(self.queue_root, key) / "binding.json")
                    if current is None or coverage_identity(current) != coverage_identity(group["binding"]):
                        raise FloorError("binding changed", reason="mapping_unknown")
                    coverage = _coverage(current)
                    group["bytes"] = sum(count * coverage["bytes_per_token"][mid]
                                         for mid, count in group["members"].items())
                    self.verdicts.append(published_verdict(
                        self.queue_root, current, demand_bytes=group["bytes"]))
                except (FloorError, OSError, KeyError, ValueError, TypeError) as exc:
                    self.verdicts.append(floor_refusal(key, getattr(exc, "reason", "unreadable"), detail=str(exc)))
            refused = [v for v in self.verdicts if not v["allowed"]]
            if refused:
                _log(("refused " if self.mode == "enforce" else "would refuse ")
                     + "; ".join(describe_verdict(v) for v in refused))
            yield self.mode != "enforce" or not refused

    def reserve(self) -> bool:
        """Persist charges before token mutation; partial writes overcharge."""

        for key, group in self.groups.items():
            if key not in self.locked:
                continue
            directory = fs_dir(self.queue_root, key)
            try:
                _floor_write(directory / "granted.json",
                       {"granted_bytes": _granted(directory) + group["bytes"]})
            except (FloorError, OSError) as exc:
                self.verdicts.append(floor_refusal(key, "grant_persistence_failed", detail=str(exc)))
                _log(f"grant counter for {key} not advanced: {exc}")
                if self.mode == "enforce":
                    return False
        return True

    def shortage(self) -> dict[str, object]:
        refused = [v for v in self.verdicts if not v["allowed"]]
        first = refused[0] if refused else {}
        return {"resource": "filesystem_floor",
                "requested": sum(self.demand.values()) // GIB,
                "available": max(0, int(first.get("free_bytes", 0))
                                 - int(first.get("floor_bytes", 0))
                                 - int(first.get("charge_bytes", 0))) // GIB,
                "filesystem": first.get("filesystem"), "reason": first.get("reason"),
                "detail": describe_verdict(first) if first else None}


def ledger_gate(ledger, demand: Mapping[str, int]) -> LedgerGate | None:
    """The floor gate for a ledger acquisition, or ``None`` when none applies.

    Called before ``begin_acquire`` takes the ledger's mutation lock.  The
    mode and the bindings come from this process's cache, which the loop
    tick refreshes outside every lock; a caller that already holds the
    ledger lock (the claim path) therefore reads no shared record here
    either, except on a cold cache.  ``off`` touches nothing else.
    """

    queue_root = Path(ledger.root).parent
    floor_mode = mode(queue_root, max_age_s=GATE_CACHE_S)
    if floor_mode == "off":
        return None
    wanted = {k: int(v) * BYTE_KINDS[k] for k, v in demand.items()
              if k in BYTE_KINDS and int(v) > 0}
    if not wanted:
        return None
    groups: dict[str, dict] = {}
    unbound: list[str] = []
    try:
        table = member_bindings(queue_root)
    except FloorError as exc:
        _log(f"bindings unreadable: {exc}")
        table = {}
    for kind, amount in sorted(wanted.items()):
        mid = member_id(ledger.root.name, ledger.host, kind)
        binding = table.get(mid)
        if binding is None:
            unbound.append(mid)
            continue
        group = groups.setdefault(binding["key"], {"binding": binding, "bytes": 0,
                                                   "members": {}})
        group["bytes"] += amount
        group["members"][mid] = amount // BYTE_KINDS[kind]
    return LedgerGate(queue_root, floor_mode, wanted, groups, unbound)


# -- used paths ----------------------------------------------------------------

def _local_mapping_matches(found, binding) -> bool:
    return found["key"] == binding["key"] and any(
        all(found.get(field) == destination.get(field)
            for field in ("source", "device", "mount_id"))
        for destination in binding.get("destinations", []))


def check_paths(queue_root, paths: Iterable, *, now: float | None = None
                ) -> list[dict[str, object]]:
    """One verdict per distinct filesystem under ``paths``; never raises."""

    try:
        known = bindings(queue_root)
    except FloorError as exc:
        return [floor_refusal("bindings", "unreadable", detail=str(exc))]
    by_key = {b["key"]: b for b in known}
    seen = set()
    verdicts: list[dict[str, object]] = []
    for path in paths:
        try:
            found = identify(path)
        except FloorError as exc:
            verdicts.append(floor_refusal(str(path), "identity_unknown", detail=str(exc)))
            continue
        name = (f"nfs:{found['server']}:{found.get('source')}" if found["key"] is None
                else str(found["key"]))
        destination = (name, found.get("source"), found.get("device"), found.get("mount_id"))
        if destination in seen:
            continue
        seen.add(destination)
        try:
            if found["key"] is None:
                verdicts.extend(_nfs_verdicts(queue_root, found, name, known, now))
            elif found["key"] in by_key:
                binding = by_key[found["key"]]
                if not _local_mapping_matches(found, binding):
                    verdicts.append(floor_refusal(name, "mapping_unknown", path=str(path)))
                    continue
                published = published_verdict(queue_root, binding, now=now)
                verdicts.append(published)
                if published.get("required_bytes") is not None:
                    # The fresh local sample may be lower than the published one.
                    fresh = _sample_verdict(name, {**found,
                            "census_bytes": published["charge_bytes"]},
                            demand_bytes=0, since=0, age=0)
                    verdicts.append({**fresh, "path": str(path), "sample": "fresh-local"})
            else:
                verdicts.append(floor_refusal(name, "mapping_unknown", path=str(path)))
        except (FloorError, OSError, KeyError, ValueError, TypeError) as exc:
            verdicts.append(floor_refusal(name, "unreadable", detail=str(exc)))
    return verdicts


def _nfs_bindings(found, known, now=None) -> list[dict]:
    """Resolve exact, effective export aliases from the binding authority."""
    identity = {"server": found["server"], "source": found.get("source"),
                **_owner_incarnation(),
                "device": found.get("device"), "mount_id": found.get("mount_id")}
    if not identity["mount_id"] or identity["device"] is None:
        return []
    matched = []
    for b in known:
        try:
            coverage = _coverage(b, now=now)
        except FloorError:
            continue
        if identity in coverage.get("nfs_aliases", []):
            matched.append(b)
    return matched


def _nfs_verdicts(queue_root, found, name, known, now) -> list[dict[str, object]]:
    """Use an authority's exact export mapping; an address alone is unknown."""

    matched = _nfs_bindings(found, known, now)
    if len(matched) != 1:
        return [floor_refusal(name, "mapping_unknown", path=found["path"],
                             detail="no unique authenticated export mapping")]
    published = [published_verdict(queue_root, b, now=now) for b in matched]
    if any("required_bytes" not in v for v in published):
        return published
    client = floor_verdict(
        name, size_bytes=int(found["size_bytes"]), free_bytes=int(found["free_bytes"]),
        charge_bytes=sum(int(v["charge_bytes"]) for v in published),
        floor=max([floor_bytes(int(found["size_bytes"]))]
                  + [int(v["floor_bytes"]) for v in published]),
        path=found["path"], sample="fresh-nfs-client",
        matched=sorted(str(b["key"]) for b in matched),
        **{field: found[field] for field in
           ("size_inodes", "free_inodes", "floor_inodes", "inode_refusal")})
    return [*published, client]


def enforce_paths(queue_root, paths: Iterable, *, label: str) -> list[dict[str, object]]:
    """Check used paths by the current mode: off reads nothing; enforce raises."""

    floor_mode = mode(queue_root)
    if floor_mode == "off":
        return []
    verdicts = check_paths(queue_root, paths)
    refused = [v for v in verdicts if not v["allowed"]]
    if refused:
        _log(f"{label}: " + ("refused " if floor_mode == "enforce" else "would refuse ")
             + "; ".join(describe_verdict(v) for v in refused))
        if floor_mode == "enforce":
            raise FloorRefused(verdicts)
    return verdicts


# -- coordinator growth --------------------------------------------------------

def _process_start(pid: int) -> str | None:
    try:
        raw = Path(f"/proc/{pid}/stat").read_text()
    except OSError:
        return None
    return raw.rsplit(")", 1)[1].split()[19]


def _growth_ledger(queue_root, path):
    """The ``filesystem_gib`` ledger bound to the filesystem under ``path``."""

    from .pool import ResourceLedger

    found = identify(path)
    known = bindings(queue_root)
    if found["key"] is None:
        candidates = _nfs_bindings(found, known)
    else:
        candidates = [b for b in known if _local_mapping_matches(found, b)]
    if len(candidates) != 1:
        raise FloorError(f"{path}: no unique authenticated filesystem mapping",
                         reason="mapping_unknown")
    ledgers = [m for b in candidates for m in b.get("members", [])
               if m["kind"] == FILESYSTEM_KIND]
    if len(ledgers) != 1:
        raise FloorError(f"{path}: {len(ledgers)} filesystem_gib ledgers bound to "
                         f"{found['key'] or 'nfs:' + str(found['server'])}; need exactly one")
    member = ledgers[0]
    return ResourceLedger(Path(queue_root) / member["ledger_root"], host=member["ledger"])


@contextmanager
def operation(queue_root, *, used_paths: Iterable = (), growth_gib: Mapping | None = None,
              label: str = "operation", lease_s: float = OPERATION_LEASE_S):
    """Check used paths and hold a coordinator's growth allowance for the body.

    ``growth_gib`` maps a path to the GiB the body may write under it.  The
    owner record is written before the tokens are taken, so a holder never
    exists without one; release runs in ``finally`` for every ending,
    ``SystemExit`` and ``KeyboardInterrupt`` included.  Yields the verdicts.
    """

    floor_mode = mode(queue_root, max_age_s=0.0)
    if floor_mode == "off":
        yield []
        return

    def refuse(v: dict[str, object], cause: BaseException | None = None) -> None:
        verdicts.append(v)
        _log(f"{label}: " + describe_verdict(v))
        if floor_mode == "enforce":
            raise FloorRefused([v]) from cause

    verdicts: list[dict[str, object]] = []
    try:
        member_bindings(queue_root, max_age_s=0.0)
    except (FloorError, OSError) as exc:
        refuse(floor_refusal("bindings", "unreadable", detail=str(exc)), exc)
    verdicts.extend(enforce_paths(queue_root, [*used_paths, *(growth_gib or {})], label=label))
    taken: list[tuple[object, str, Path]] = []
    try:
        for path, gib in sorted((growth_gib or {}).items(), key=lambda i: str(i[0])):
            if int(gib) <= 0:
                continue
            try:
                ledger = _growth_ledger(queue_root, path)
            except (FloorError, OSError) as exc:
                refuse(floor_refusal(str(path), "no_growth_ledger", detail=str(exc)), exc)
                continue
            holder = (f"{OPERATION_PREFIX}{socket.gethostname()}-{os.getpid()}-"
                      f"{uuid.uuid4().hex[:8]}")
            owner = ledger.base / "operations" / f"{holder}.json"
            try:
                _floor_write(owner, {"schema": OWNER_SCHEMA, "holder": holder, "label": label,
                               "host": socket.gethostname(), "machine_id": _machine_id(),
                               "boot_id": _floor_boot_id(), "pid": os.getpid(),
                               "pid_start": _process_start(os.getpid()),
                               "lease_until": time.time() + lease_s})
                taken.append((ledger, holder, owner))
                admitted = ledger.acquire(holder, {FILESYSTEM_KIND: int(gib)})
            except (FloorError, OSError) as exc:
                refuse(floor_refusal(str(path), "growth_unrecorded", detail=str(exc)), exc)
                continue
            if not admitted:
                refuse(floor_refusal(str(path), "growth_refused",
                               detail=str(ledger.last_token_shortage)))
        yield verdicts
    finally:
        for ledger, holder, owner in reversed(taken):
            try:
                ledger.release(holder)
                owner.unlink(missing_ok=True)
            except OSError as exc:
                _log(f"{label}: {holder} not released ({exc}); the reaper will")


def reap_operations(queue_root, *, now: float | None = None) -> list[str]:
    """Release growth holders whose owner is gone or whose lease ran out.

    A holder on this machine is gone when its boot, pid or pid start time
    differs; on another machine only its lease decides.  An owner record
    whose holder already emptied is removed.
    """

    from .pool import ResourceLedger

    now = time.time() if now is None else now
    base = Path(queue_root) / FILESYSTEM_RESERVATIONS
    try:
        ledgers = sorted(os.listdir(base))
    except FileNotFoundError:
        return []
    machine, boot = _machine_id(), _floor_boot_id()
    reaped = []
    for name in ledgers:
        ledger = ResourceLedger(base, host=name)
        directory = ledger.base / "operations"
        try:
            records = sorted(directory.glob("*.json"))
        except OSError:
            continue
        for path in records:
            try:
                owner = _floor_read(path)
                if owner is None:
                    continue
                local = owner.get("machine_id") == machine
                gone = (now > float(owner.get("lease_until", 0))
                        or (local and (owner.get("boot_id") != boot
                                       or _process_start(int(owner["pid"]))
                                       != owner.get("pid_start"))))
                if gone:
                    ledger.release(str(owner["holder"]))
                    path.unlink(missing_ok=True)
                    reaped.append(str(owner["holder"]))
            except (FloorError, OSError, KeyError, ValueError, TypeError) as exc:
                # One bad record never stops the others being reaped.
                _log(f"reap: {path.name} skipped: {exc}")
    return reaped


# -- loop hooks ----------------------------------------------------------------

def loop_tick(queue_root, *, label: str) -> None:
    """The worker/tier loop's per-poll floor work: refresh, then reap.

    Runs in every mode once a binding exists, so ``status`` shows real
    samples before anyone turns enforcement on.  Never raises.
    """

    try:
        # Refresh this process's cached mode and bindings here, outside every
        # lock, so the acquisition gate never reads them inside one.
        mode(queue_root, max_age_s=0.0)
        member_bindings(queue_root, max_age_s=0.0)
        results = refresh_local(queue_root)
        if results is None:
            return                                   # not due this poll
        for v in results:
            if not v["allowed"]:
                _log(f"{label}: refresh {describe_verdict(v)}")
        reap_operations(queue_root)
    except Exception as exc:                                     # noqa: BLE001
        _log(f"{label}: tick skipped: {type(exc).__name__}: {exc}")


def host_admission(queue_root, paths: Iterable, *, label: str) -> bool:
    """Whether a loop may admit work this poll; only ``enforce`` says no."""

    try:
        enforce_paths(queue_root, paths, label=label)
    except FloorRefused:
        return False
    except Exception as exc:                                     # noqa: BLE001
        _log(f"{label}: used-path check failed: {type(exc).__name__}: {exc}")
        return mode(queue_root) != "enforce"
    return True


# -- command line ----------------------------------------------------------------

def _member(text: str) -> tuple[str, str, str]:
    """``reservations/<host>:<kind>`` or ``tier-reservations/<tier>:<kind>``."""

    ledger, _, kind = text.rpartition(":")
    root, _, name = ledger.partition("/")
    if not (root and name and kind):
        raise argparse.ArgumentTypeError(f"{text!r} is not <ledger-root>/<name>:<kind>")
    return root, name, kind


def floor_status(queue_root) -> dict:
    """Every binding with its published verdict, and unbound byte ledgers."""

    rows = []
    bound = set()
    for b in bindings(queue_root):
        try:
            v = published_verdict(queue_root, b)
        except (FloorError, KeyError, ValueError) as exc:
            v = floor_refusal(b["key"], "unreadable", detail=str(exc))
        rows.append({"key": b["key"], "root": b.get("root"), "owner": b.get("owner_host"),
                     "status": b.get("status"), "members": b.get("members"), "verdict": v})
        bound |= {member_id(m["ledger_root"], m["ledger"], m["kind"]) for m in b["members"]}
    unbound = []
    for root in ("reservations", "tier-reservations", FILESYSTEM_RESERVATIONS):
        try:
            names = sorted(os.listdir(Path(queue_root) / root))
        except OSError:
            continue
        for name in names:
            try:
                minted = os.listdir(Path(queue_root) / root / name / "minted")
            except OSError:
                continue
            for kind in BYTE_KINDS:
                if any(m.startswith(kind + "-") for m in minted) and \
                        member_id(root, name, kind) not in bound:
                    unbound.append(member_id(root, name, kind))
    return {"mode": mode(queue_root), "bindings": rows, "unbound_byte_ledgers": unbound}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m prismabuild.filesystem_floor",
                                 description=__doc__.split("\n\n")[0])
    ap.add_argument("--queue", default="/mnt/shared/prismabuild-fleet/pb-queue")
    sub = ap.add_subparsers(dest="command", required=True)
    reg = sub.add_parser("register", help="bind a local filesystem and ledger kinds")
    reg.add_argument("root")
    reg.add_argument("--member", action="append", type=_member, default=[],
                     help="<ledger-root>/<name>:<kind>, e.g. reservations/sparklina:spool_gb")
    reg.add_argument("--filesystem-gib", type=int, default=None,
                     help="mint this filesystem's filesystem_gib growth ledger to N GiB")
    reg.add_argument("--filesystem-ledger", default=None)
    reg.add_argument("--coverage-json", type=Path, default=None,
                     help="authority-supplied versioned mapping, population and whole-growth bounds")
    unb = sub.add_parser("unbind", help="remove one member from its binding")
    unb.add_argument("member", type=_member)
    sub.add_parser("refresh", help="refresh this machine's bindings now")
    sub.add_parser("reap", help="release growth holders of dead owners")
    sub.add_parser("status", help="bindings, verdicts and unbound byte ledgers")
    chk = sub.add_parser("check", help="check used paths")
    chk.add_argument("paths", nargs="+")
    md = sub.add_parser("mode", help="show or set the fleet-wide mode")
    md.add_argument("value", nargs="?", choices=MODES)
    args = ap.parse_args(argv)
    queue = Path(args.queue)
    if args.command == "register":
        result = register(queue, args.root, members=args.member,
                          filesystem_gib=args.filesystem_gib,
                          filesystem_ledger=args.filesystem_ledger,
                          coverage=_floor_read(args.coverage_json) if args.coverage_json else None)
    elif args.command == "unbind":
        result = unbind(queue, *args.member)
    elif args.command == "refresh":
        result = refresh_local(queue, force=True)
    elif args.command == "reap":
        result = reap_operations(queue)
    elif args.command == "status":
        result = floor_status(queue)
    elif args.command == "check":
        result = check_paths(queue, args.paths)
    else:
        if args.value is not None:
            floor_root(queue).mkdir(parents=True, exist_ok=True)
            _write_text = floor_root(queue) / "mode"
            temporary = _write_text.with_name(f".mode.{os.getpid()}")
            temporary.write_text(args.value + "\n")
            os.replace(temporary, _write_text)
        _MODE_CACHE.clear()
        result = {"mode": mode(queue)}
    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
