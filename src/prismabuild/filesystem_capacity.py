"""One all-used-filesystem guard, owned by the existing reservation ledgers.

No alternate scheduler, sweeper, token store or capability-tag exemption. Native
registration/adoption requires the normal publisher/supervisor authority.
"""
from __future__ import annotations

from collections.abc import Mapping
from contextlib import ExitStack, contextmanager
import contextvars
import hashlib
import os
from pathlib import Path
import re
import socket
import stat
import threading
import time
import uuid

from .local_scratch import LocalScratchError, _canonical_root, _descriptor_mount

GIB = 1 << 30


# One descriptor/physical-space observer for all-used-filesystem admission.
FILESYSTEM_FRAME_SCHEMA = "prismabuild.filesystem_capacity_frame.v1"
FILESYSTEM_FRAME_METHOD = "linux-descriptor-native-physical-v1"
FILESYSTEM_BYTE_KIND = "filesystem_gib"
FILESYSTEM_BYTE_KINDS = frozenset({"spool_gb", "stage_gib", FILESYSTEM_BYTE_KIND})
FILESYSTEM_FRAME_MAX_BYTES = 1 << 20
_NFS_FSID_BYTES = (8, 4, 12, 8, 8, 8, 16, 24)


def _capacity_mount(fd: int) -> dict[str, object]:
    """The held object and its exact kernel mount row, never a path prefix."""
    identity = _descriptor_mount(fd, directory=False)
    ids = [line.split(":", 1)[1].strip() for line in
           Path(f"/proc/self/fdinfo/{fd}").read_text().splitlines()
           if line.startswith("mnt_id:")]
    rows = [line.split(" - ", 1) for line in
            Path("/proc/self/mountinfo").read_text().splitlines()
            if line.split(" ", 1)[0] == ids[0]]
    if len(rows) != 1 or len(rows[0]) != 2:
        raise LocalScratchError("used filesystem mount identity unknown")
    fields = rows[0][1].split()
    if len(fields) != 3 or fields[0] != identity["filesystem_type"]:
        raise LocalScratchError("used filesystem mount changed")
    boot = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
    if str(uuid.UUID(boot)) != boot:
        raise LocalScratchError("used filesystem boot identity malformed")
    machine = Path("/etc/machine-id").read_text().strip()
    if not re.fullmatch(r"[0-9a-f]{32}", machine):
        raise LocalScratchError("native filesystem machine identity unknown")
    namespace = os.stat("/proc/self/ns/mnt")
    return {**identity, "machine_id": machine, "boot_id": boot, "mount_id": int(ids[0]),
            "mount_namespace": [namespace.st_dev, namespace.st_ino],
            "mount_source": fields[1], "mount_options": fields[2]}


def _space_bytes(value) -> tuple[int, int]:
    fields = (value.f_blocks, value.f_bavail, value.f_frsize)
    if any(type(number) is not int for number in fields):
        raise LocalScratchError("used filesystem capacity is not integral")
    blocks, available, fragment = fields
    if blocks <= 0 or fragment <= 0 or not 0 <= available <= blocks:
        raise LocalScratchError("used filesystem capacity is partial or invalid")
    return blocks * fragment, available * fragment


def filesystem_floor(size_bytes: int, free_bytes: int, allowance_bytes: int) -> dict[str, int]:
    """The single integer predicate. Allowances come only from its locked owner.

    The owner deliberately takes no materialization credit. Its complete held
    byte reservations are a conservative upper bound on still-unwritten bytes,
    not a filesystem quota or an exact subtraction of materialized extents.
    """
    if (type(size_bytes) is not int or size_bytes <= 0
            or type(free_bytes) is not int or not 0 <= free_bytes <= size_bytes
            or type(allowance_bytes) is not int or allowance_bytes < 0):
        raise LocalScratchError("used filesystem space or allowance unknown")
    floor = (size_bytes + 19) // 20
    required = floor + allowance_bytes
    if free_bytes < required:
        raise LocalScratchError(
            f"used filesystem capacity HOLD: {free_bytes} free < {floor} floor + "
            f"{allowance_bytes} outstanding byte allowances")
    return {"size_bytes": size_bytes, "free_bytes": free_bytes,
            "floor_bytes": floor, "allowance_bytes": allowance_bytes,
            "required_bytes": required}


def _capacity_command(argv: list[str]) -> str:
    import subprocess
    completed = subprocess.run(argv, check=True, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, text=True, timeout=30)
    if len(completed.stdout.encode()) > FILESYSTEM_FRAME_MAX_BYTES:
        raise LocalScratchError("native filesystem observation exceeds its byte bound")
    return completed.stdout


def _native_physical_space(fd: int, identity: Mapping[str, object]) -> dict[str, object]:
    """Native filesystem/pool arithmetic; a quota's available is not pool free."""
    sampled = os.fstatvfs(fd)
    if str(sampled.f_fsid) != identity["filesystem"]:
        raise LocalScratchError("used filesystem changed during its space sample")
    size, free = _space_bytes(sampled)
    filesystem_type = identity["filesystem_type"]
    if filesystem_type in {"ext4", "xfs", "btrfs", "tmpfs"}:
        filesystem = identity["filesystem"]
        if filesystem_type == "btrfs":
            import fcntl
            info = bytearray(1024)
            # BTRFS_IOC_FS_INFO, immutable filesystem UUID independent of subvolume dev.
            fcntl.ioctl(fd, 0x8400941f, info, True)
            filesystem = str(uuid.UUID(bytes=bytes(info[16:32])))
        key = [identity["machine_id"], filesystem_type, filesystem]
        physical = {"physical_key": key, "physical_size_bytes": size,
                    "physical_free_bytes": free, "dataset_guid": None,
                    "pool_guid": None}
    elif filesystem_type == "zfs":
        dataset = str(identity["mount_source"])
        if not re.fullmatch(r"[A-Za-z0-9_.:-]+(?:/[A-Za-z0-9_.:-]+)*", dataset):
            raise LocalScratchError("native ZFS mount does not name a dataset")
        pool_name = dataset.split("/", 1)[0]
        pool = _capacity_command(["zpool", "list", "-Hp", "-o",
                                  "guid,size,free,health", pool_name]).split()
        guid = _capacity_command(["zfs", "get", "-Hp", "-o", "value",
                                  "guid", dataset]).strip()
        if (len(pool) != 4 or any(not x.isascii() or not x.isdigit()
                                 for x in pool[:3])
                or not guid.isascii() or not guid.isdigit()
                or pool[3] not in {"ONLINE", "DEGRADED"}):
            raise LocalScratchError("native ZFS physical pool/dataset witness unknown")
        physical_size, physical_free = int(pool[1]), int(pool[2])
        if physical_size <= 0 or not 0 <= physical_free <= physical_size:
            raise LocalScratchError("native ZFS physical capacity invalid")
        physical = {"physical_key": [identity["machine_id"], "zfs", pool[0]],
                    "physical_size_bytes": physical_size,
                    "physical_free_bytes": physical_free,
                    "dataset_guid": guid, "pool_guid": pool[0]}
    else:
        raise LocalScratchError("native physical filesystem observation unsupported")
    if _capacity_mount(fd) != identity:
        raise LocalScratchError("used filesystem descriptor changed during observation")
    return {**physical, "size_bytes": size, "free_bytes": free}


def _nfs_descriptor_key(fd: int, identity: Mapping[str, object]) -> dict[str, object]:
    """Read Linux's embedded server handle, without privileged handle opening.

    fs/nfs/export.c embeds three native u32 inode/type words, followed by the
    unsigned-short nfs_fh size and its opaque server handle. Linux nfsd's v1
    handle stores the FSID type at byte 2 and the FSID at byte 4. Unknown ABI,
    server handle, inaccessible syscall or server endpoint refuses, not guesses.
    """
    import ctypes
    import ipaddress
    import stat
    import sys

    class Handle(ctypes.Structure):
        _fields_ = [("handle_bytes", ctypes.c_uint), ("handle_type", ctypes.c_int),
                    ("data", ctypes.c_ubyte * 256)]

    libc = ctypes.CDLL(None, use_errno=True)
    call = libc.name_to_handle_at
    call.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.POINTER(Handle),
                     ctypes.POINTER(ctypes.c_int), ctypes.c_int]
    call.restype = ctypes.c_int
    handle, mount = Handle(), ctypes.c_int()
    handle.handle_bytes = 256
    if call(fd, b"", ctypes.byref(handle), ctypes.byref(mount), 0x1000) != 0:
        raise OSError(ctypes.get_errno(), "native NFS descriptor handle unavailable")
    length = handle.handle_bytes
    if not 20 <= length <= 256 or length % 4 or handle.handle_type != length // 4:
        raise LocalScratchError("native NFS embedded-handle ABI unknown")
    raw = bytes(handle.data[:length])
    inode = ((int.from_bytes(raw[:4], sys.byteorder) << 32)
             | int.from_bytes(raw[4:8], sys.byteorder))
    mode = int.from_bytes(raw[8:12], sys.byteorder)
    size = int.from_bytes(raw[12:14], sys.byteorder)
    info = os.fstat(fd)
    if inode != info.st_ino or mode != stat.S_IFMT(info.st_mode) or not 8 <= size <= 128:
        raise LocalScratchError("native NFS embedded handle differs from held descriptor")
    server = raw[14:14 + size]
    if len(server) != size or server[0] != 1 or server[1] != 0 or server[2] >= 8:
        raise LocalScratchError("native NFS server handle format unknown")
    fsid_size = _NFS_FSID_BYTES[server[2]]
    if size < 4 + fsid_size:
        raise LocalScratchError("native NFS server FSID is partial")
    options = str(identity["mount_options"]).split(",")
    addresses = [part[5:] for part in options if part.startswith("addr=")]
    if len(addresses) != 1:
        raise LocalScratchError("native NFS server endpoint unknown")
    endpoint = str(ipaddress.ip_address(addresses[0]))
    fsid = server[4:4 + fsid_size]
    # nfsd.expkey_show prints native-endian u32 words with %08x.
    key = "".join(f"{int.from_bytes(fsid[i:i + 4], sys.byteorder):08x}"
                  for i in range(0, len(fsid), 4))
    if _capacity_mount(fd) != identity:
        raise LocalScratchError("native NFS descriptor changed during handle observation")
    return {"endpoint": endpoint, "fsid_type": server[2], "fsid": key}


def _native_export_rows() -> list[dict[str, object]]:
    """The kernel's valid FSID-to-export cache, not exports configuration prose."""
    path = Path("/proc/net/rpc/nfsd.fh/content")
    with path.open("rb") as stream:
        raw = stream.read(FILESYSTEM_FRAME_MAX_BYTES + 1)
    if len(raw) > FILESYSTEM_FRAME_MAX_BYTES:
        raise LocalScratchError("native NFS export cache is incomplete")
    rows = []
    for line in raw.decode("utf-8", "strict").splitlines():
        if line.startswith("#") or not line:
            continue
        parts = line.split()
        if len(parts) == 3:
            continue  # Kernel negative cache entry, never an alias witness.
        if (len(parts) != 4 or not parts[1].isascii() or not parts[1].isdigit()
                or int(parts[1]) >= 8 or not parts[2].startswith("0x")
                or not re.fullmatch(r"[0-9a-f]+", parts[2][2:])
                or len(parts[2][2:]) != 2 * _NFS_FSID_BYTES[int(parts[1])]):
            raise LocalScratchError("native NFS export cache format unknown")
        exported = re.sub(r"\\([0-7]{3})", lambda m: chr(int(m[1], 8)), parts[3])
        if _canonical_root(exported) is None:
            raise LocalScratchError("native NFS export root malformed")
        rows.append({"domain": parts[0], "fsid_type": int(parts[1]),
                     "fsid": parts[2][2:], "root": exported})
    return rows


def capture_filesystem_capacity(root: str) -> dict[str, object]:
    """Capture one real owner-native frame; called by the existing owner loop.

    The returned frame is installed under that owner's reservation lock. It is
    not an advisory offer, a registration permission, or standalone qualification.
    A client NFS view cannot mint a physical frame for its server.
    """
    import ipaddress
    import json

    if _canonical_root(root) is None:
        raise LocalScratchError("filesystem capacity root must be canonical and absolute")
    fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        identity = _capacity_mount(fd)
        space = _native_physical_space(fd, identity)
        exports, addresses = [], []
        if Path("/proc/net/rpc/nfsd.fh/content").exists():
            interfaces = json.loads(_capacity_command(["ip", "-j", "address", "show"]))
            for interface in interfaces:
                for address in interface.get("addr_info", []):
                    addresses.append(str(ipaddress.ip_address(address["local"])))
            for row in _native_export_rows():
                other = os.open(str(row["root"]), os.O_RDONLY | os.O_DIRECTORY |
                                os.O_NOFOLLOW | os.O_CLOEXEC)
                try:
                    descriptor = _capacity_mount(other)
                    physical = _native_physical_space(other, descriptor)
                    if physical["physical_key"] == space["physical_key"]:
                        exports.append({**row, "identity": descriptor,
                                        "dataset_guid": physical["dataset_guid"]})
                finally:
                    os.close(other)
        if _capacity_mount(fd) != identity:
            raise LocalScratchError("filesystem owner root changed during native capture")
        return {"schema": FILESYSTEM_FRAME_SCHEMA, "method": FILESYSTEM_FRAME_METHOD,
                "owner_host": socket.gethostname(), "root": root, "identity": identity,
                **space, "server_addresses": sorted(set(addresses)), "exports": exports,
                "sampled_unix": time.time()}
    finally:
        os.close(fd)


def match_filesystem_frame(fd: int, frame: Mapping[str, object]) -> dict[str, object]:
    """Join a currently opened local/NFS object to its native physical owner."""
    identity = _capacity_mount(fd)
    if identity["filesystem_type"] in {"nfs", "nfs4"}:
        key = _nfs_descriptor_key(fd, identity)
        if key["endpoint"] not in frame["server_addresses"]:
            raise LocalScratchError("NFS endpoint has no owner-native physical mapping")
        matches = [row for row in frame["exports"]
                   if row["fsid_type"] == key["fsid_type"] and row["fsid"] == key["fsid"]]
        # Different domains may name the SAME native export; different exports
        # with the same wire FSID are ambiguous and cannot be deduplicated.
        native = {tuple(row["identity"][k] for k in
                        ("boot_id", "device", "filesystem", "root_inode")) for row in matches}
        if len(native) != 1:
            raise LocalScratchError("NFS wire FSID has no unique native export descriptor")
    else:
        physical = _native_physical_space(fd, identity)
        if physical["physical_key"] != frame["physical_key"]:
            raise LocalScratchError("used descriptor belongs to another physical filesystem")
    size, free = _space_bytes(os.fstatvfs(fd))
    if _capacity_mount(fd) != identity:
        raise LocalScratchError("used descriptor changed at the physical owner boundary")
    return {"identity": identity, "size_bytes": size, "free_bytes": free,
            "physical_key": frame["physical_key"]}



BINDING_SCHEMA = "prismabuild.filesystem_owner_binding.v1"
BINDING_FILE = "filesystem-capacity.json"
OPERATION_SCHEMA = "prismabuild.filesystem_operation.v1"
OPERATION_ENV = "PRISMABUILD_FILESYSTEM_OPERATION"
WITNESS_SCHEMA = "prismabuild.filesystem_namespace_witness.v1"
WITNESS_TOOL = "tools/fleet/filesystem_capacity.py"
_ROLES = {"coordinator": frozenset({"source", "cas", "queue", "logs"}),
          "worker": frozenset({"checkout", "cas", "queue", "logs", "output"}),
          "storage": frozenset({"cas", "queue", "logs", "stage"})}
_CLASSES = frozenset().union(*_ROLES.values(), {"scratch", "spool", "pin"})
_TRANSACTION = contextvars.ContextVar("used_filesystem_transaction", default=None)
_ACTIVE = set()
_ACTIVE_LOCK = threading.Lock()
_QUEUE_IDENTITIES = {}
_PROVENANCE_CHECKOUT = None
_PROVENANCE_FREEZE = threading.Lock()
_IN_PROVENANCE = contextvars.ContextVar("used_filesystem_provenance_active", default=False)


def _execution_checkout_owner():
    """Freeze the unchanged executed-checkout owner exactly once per process.

    The unchanged materialize owner materializes a provenance checkout without
    candidate filesystem admission. Caller cutover must not substitute an
    admission-wrapping variant: the owner object is resolved once from the
    installed materialize module and identity-bound here, and admission()
    refuses to run while provenance materialization is active, so a circular
    candidate admission fails closed instead of authorizing its own
    provenance. This is the unchanged provenance-owner seam; the materialize
    owner itself is never modified from this module.
    """
    global _PROVENANCE_CHECKOUT
    owner = _PROVENANCE_CHECKOUT
    if owner is None:
        with _PROVENANCE_FREEZE:
            owner = _PROVENANCE_CHECKOUT
            if owner is None:
                from . import materialize
                owner = getattr(materialize, "_execution_checkout", None)
                if (getattr(owner, "__module__", None) != "prismabuild.materialize"
                        or getattr(owner, "__qualname__", "") != "_execution_checkout"):
                    raise LocalScratchError("unchanged executed-checkout owner is not resolvable")
                _PROVENANCE_CHECKOUT = owner
    return owner


def operation(value, role):
    from . import core, storage_tiers
    if isinstance(value, str):
        if len(value.encode()) > FILESYSTEM_FRAME_MAX_BYTES:
            raise LocalScratchError("filesystem operation exceeds its metadata bound")
        value = core._decode_strict_json(value.encode(), where="filesystem operation")
    if (not isinstance(value, dict) or set(value) != {"schema", "operations"}
            or value["schema"] != OPERATION_SCHEMA or role not in _ROLES
            or not isinstance(value["operations"], dict)
            or set(value["operations"]) - set(_ROLES)):
        raise LocalScratchError("complete versioned filesystem operation required")
    entries = value["operations"].get(role)
    if not isinstance(entries, list) or not entries:
        raise LocalScratchError(f"filesystem {role} growth intent missing")
    classes, roots = set(), set()
    for entry in entries:
        if (not isinstance(entry, dict) or set(entry) != {"root", "max_bytes", "classes", "resource"}
                or _canonical_root(entry["root"]) is None
                or type(entry["max_bytes"]) is not int or entry["max_bytes"] <= 0
                or not isinstance(entry["classes"], list) or not entry["classes"]
                or any(c not in _CLASSES for c in entry["classes"])
                or len(set(entry["classes"])) != len(entry["classes"])
                or not isinstance(entry["resource"], str) or entry["root"] in roots):
            raise LocalScratchError("filesystem growth intent partial or malformed")
        kind, tier = storage_tiers.split_demand_key(entry["resource"])
        if kind not in FILESYSTEM_BYTE_KINDS or (tier is None and kind != "spool_gb"):
            raise LocalScratchError("filesystem growth lacks an existing byte owner")
        roots.add(entry["root"])
        classes.update(entry["classes"])
    if not _ROLES[role] <= classes:
        raise LocalScratchError(f"filesystem {role} omits write classes: {sorted(_ROLES[role]-classes)}")
    return entries


def terms(value, role):
    demand = {}
    for entry in operation(value, role):
        key = entry["resource"]
        demand[key] = demand.get(key, 0) + (entry["max_bytes"] + GIB - 1) // GIB
    return demand


def _owner(value):
    if (not isinstance(value, dict) or set(value) != {"type", "id"}
            or value["type"] not in {"host", "tier"}
            or not isinstance(value["id"], str)
            or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]*", value["id"])):
        raise LocalScratchError("canonical reservation owner unknown")
    return value


def _base(root, owner):
    from . import pool
    _owner(owner)
    namespace = pool.RESERVATIONS if owner["type"] == "host" else pool.TIER_RESERVATIONS
    return Path(root) / namespace / owner["id"]


def _ledger(queue, owner):
    return queue.ledger(owner["id"]) if owner["type"] == "host" else queue.tier_ledger(owner["id"])


def _raw_lock(root, owner):
    # EXACT existing lock leaves. Never recurse into aggregate-aware factories.
    _owner(owner)
    if owner["type"] == "host":
        return _base(root, owner) / ".mutation.lock"
    digest = hashlib.sha256(f"tier-mint:{owner['id']}".encode()).hexdigest()
    return Path(root) / "tier-mint-locks" / f"{digest}.lock"


def _queue_identity(root):
    """Reject another pathname alias for the same lock namespace in this process.

    POSIX locks can be lost if any other descriptor to their inode is closed.
    resolve() alone cannot discover bind-mount aliases; native dev/ino does.
    """
    root = Path(root)
    if not root.is_absolute() or root != root.resolve(strict=True):
        raise LocalScratchError("filesystem owner queue locator is not canonical")
    info = root.stat()
    identity = (os.getpid(), info.st_dev, info.st_ino)
    with _ACTIVE_LOCK:
        previous = _QUEUE_IDENTITIES.setdefault(identity, str(root))
        if previous != str(root):
            raise LocalScratchError("filesystem owner queue has an alternate inode alias")
    return root


def _frame(value, *, fresh=True):
    from . import pool
    keys = {"schema", "method", "owner_host", "root", "identity", "physical_key",
            "physical_size_bytes", "physical_free_bytes", "dataset_guid", "pool_guid",
            "size_bytes", "free_bytes", "server_addresses", "exports", "sampled_unix"}
    if (not isinstance(value, dict) or set(value) != keys
            or value["schema"] != FILESYSTEM_FRAME_SCHEMA or value["method"] != FILESYSTEM_FRAME_METHOD
            or _canonical_root(value["root"]) is None
            or not isinstance(value["physical_key"], list) or len(value["physical_key"]) != 3
            or any(not isinstance(x, str) or not x for x in value["physical_key"])
            or not isinstance(value["identity"], dict)
            or value["identity"].get("machine_id") != value["physical_key"][0]
            or not isinstance(value["server_addresses"], list)
            or not isinstance(value["exports"], list)
            or isinstance(value["sampled_unix"], bool)
            or not isinstance(value["sampled_unix"], (int, float))):
        raise LocalScratchError("native physical frame is partial or untyped")
    # Valid capacity values, without treating a failed floor as malformed state:
    # a full frame stays evidence and can refresh, but never admits.
    for size, free in ((value["size_bytes"], value["free_bytes"]),
                       (value["physical_size_bytes"], value["physical_free_bytes"])):
        if type(size) is not int or size <= 0 or type(free) is not int or not 0 <= free <= size:
            raise LocalScratchError("native physical capacity invalid")
    if fresh and not 0 <= time.time() - value["sampled_unix"] <= pool.OFFER_TIMEOUT_S:
        raise LocalScratchError("native physical frame stale")
    return value


def _read_binding(root, owner, *, optional=False):
    from . import core
    path = _base(root, owner) / BINDING_FILE
    try:
        raw = core._read_regular_file_nofollow(path, where="filesystem owner binding",
                                               max_bytes=FILESYSTEM_FRAME_MAX_BYTES)
    except FileNotFoundError:
        if optional:
            return None
        raise LocalScratchError("physical reservation owner unregistered") from None
    value = core._decode_strict_json(raw, where="filesystem owner binding")
    if (not isinstance(value, dict)
            or set(value) != {"schema", "owner", "generation", "complete", "constraints",
                              "primaries", "registration", "binding_sha256"}
            or value["schema"] != BINDING_SCHEMA or value["owner"] != owner
            or value["complete"] is not True
            or not isinstance(value["generation"], str)
            or not re.fullmatch(r"[0-9a-f]{32}", value["generation"])
            or not isinstance(value["constraints"], list) or not value["constraints"]
            or not isinstance(value["primaries"], list)
            or value["binding_sha256"] != core.canonical_sha256(
                {k: v for k, v in value.items() if k != "binding_sha256"})):
        raise LocalScratchError("filesystem owner registration partial, changed or unknown")
    registration = value["registration"]
    if (not isinstance(registration, dict)
            or set(registration) != {"action_key", "published_unix", "attempt"}
            or not isinstance(registration["action_key"], str)
            or not re.fullmatch(r"[0-9a-f]{64}", registration["action_key"])
            or isinstance(registration["published_unix"], bool)
            or not isinstance(registration["published_unix"], (int, float))
            or not isinstance(registration["attempt"], int)
            or isinstance(registration["attempt"], bool)
            or registration["attempt"] < 1):
        raise LocalScratchError("filesystem owner registration capture unknown")
    for primary in value["primaries"]:
        if (not isinstance(primary, dict) or set(primary) != {"physical_key", "aliases"}
                or not isinstance(primary["physical_key"], list)
                or len(primary["physical_key"]) != 3
                or any(not isinstance(x, str) or not x for x in primary["physical_key"])
                or not isinstance(primary["aliases"], list)):
            raise LocalScratchError("canonical primary population partial")
        for alias in primary["aliases"]:
            if (not isinstance(alias, dict) or set(alias) != {"owner", "kinds", "generation"}
                    or not isinstance(alias["kinds"], list) or not alias["kinds"]
                    or set(alias["kinds"]) - FILESYSTEM_BYTE_KINDS
                    or len(set(alias["kinds"])) != len(alias["kinds"])
                    or not isinstance(alias["generation"], str)
                    or not re.fullmatch(r"[0-9a-f]{32}", alias["generation"])):
                raise LocalScratchError("canonical primary alias population partial")
            _owner(alias["owner"])
    seen = set()
    for constraint in value["constraints"]:
        if (not isinstance(constraint, dict) or set(constraint) != {"kinds", "frame", "primary"}
                or not isinstance(constraint["kinds"], list) or not constraint["kinds"]
                or set(constraint["kinds"]) - FILESYSTEM_BYTE_KINDS
                or len(set(constraint["kinds"])) != len(constraint["kinds"])):
            raise LocalScratchError("physical constraint incomplete")
        _owner(constraint["primary"])
        frame = _frame(constraint["frame"], fresh=False)
        physical = tuple(frame["physical_key"])
        if physical in seen:
            raise LocalScratchError("duplicate physical constraint in owner")
        seen.add(physical)
    return value


def _seal(body):
    from . import core
    return {**body, "binding_sha256": core.canonical_sha256(body)}


def _held(ledger, kinds):
    """Fresh complete private+committed byte tokens, no materialization credit."""
    from . import pool
    total = 0
    try:
        holders = pool._scan_visible(ledger.held_dir)
    except FileNotFoundError:
        if "held" in {p.name for p in pool._scan_visible(ledger.base)}:
            raise LocalScratchError("filesystem holder census changed") from None
        return 0
    for holder in holders:
        if not stat.S_ISDIR(holder.lstat().st_mode):
            raise LocalScratchError("filesystem holder namespace partial")
        for token in pool._scan_visible(holder):
            kind, separator, ordinal = token.name.rpartition("-")
            if kind not in kinds:
                continue
            info = token.lstat()
            if (not separator or not ordinal.isascii() or not ordinal.isdigit()
                    or not stat.S_ISREG(info.st_mode) or info.st_nlink != 1):
                raise LocalScratchError("physical byte token identity unknown")
            total += GIB
    return total


def _population(queue):
    """Read only under registration exclusion; no cached or advisory directory view."""
    from . import pool
    records = {}
    for type_, namespace in (("host", pool.RESERVATIONS), ("tier", pool.TIER_RESERVATIONS)):
        directory = queue.root / namespace
        try:
            owners = pool._scan_visible(directory)
        except FileNotFoundError:
            if directory.name in {p.name for p in pool._scan_visible(directory.parent)}:
                raise LocalScratchError("physical owner namespace changed") from None
            continue
        for path in owners:
            if path.name == ".filesystem-registration.lock":
                continue
            if not stat.S_ISDIR(path.lstat().st_mode):
                raise LocalScratchError("physical owner namespace incomplete")
            owner = {"type": type_, "id": path.name}
            record = _read_binding(queue.root, owner, optional=True)
            if record is None:
                # Legacy physical tokens cannot vanish from a new census.
                ledger = _ledger(queue, owner)
                if _held(ledger, FILESYSTEM_BYTE_KINDS):
                    raise LocalScratchError("physical holders lack canonical registration")
            else:
                records[(type_, path.name)] = record
    return records


def _registration_lock(root):
    from . import pool, posix_lock
    return posix_lock.held(Path(root) / pool.RESERVATIONS / ".filesystem-registration.lock",
                           blocking=False)


def _primary_specs(records):
    """Every constraint must appear in exactly ONE closed primary population."""
    specs = {}
    for owner_key, record in records.items():
        for primary in record["primaries"]:
            if (not isinstance(primary, dict) or set(primary) != {"physical_key", "aliases"}
                    or not isinstance(primary["physical_key"], list)
                    or not isinstance(primary["aliases"], list)):
                raise LocalScratchError("canonical primary population partial")
            key = tuple(primary["physical_key"])
            if key in specs:
                raise LocalScratchError("physical resource has competing primaries")
            specs[key] = (owner_key, primary["aliases"])
    actual = {}
    for owner_key, record in records.items():
        for constraint in record["constraints"]:
            key = tuple(constraint["frame"]["physical_key"])
            owner = constraint["primary"]
            if key not in specs or specs[key][0] != (owner["type"], owner["id"]):
                raise LocalScratchError("physical constraint appoints an unrecognized primary")
            actual.setdefault(key, []).append({"owner": record["owner"],
                "kinds": constraint["kinds"], "generation": record["generation"]})
    from . import core
    for key, (_, aliases) in specs.items():
        if sorted(aliases, key=core.canonical_sha256) != sorted(actual.get(key, []), key=core.canonical_sha256):
            raise LocalScratchError("canonical primary alias population changed or incomplete")
    return specs


def _binding_primaries(root, record):
    specs = {}
    for constraint in record["constraints"]:
        owner = constraint["primary"]
        primary = _read_binding(root, owner)
        key = constraint["frame"]["physical_key"]
        matches = [p for p in primary["primaries"] if p["physical_key"] == key]
        expected = {"owner": record["owner"], "kinds": constraint["kinds"],
                    "generation": record["generation"]}
        if len(matches) != 1 or expected not in matches[0]["aliases"]:
            raise LocalScratchError("alias is outside canonical primary population")
        specs[str(_raw_lock(root, owner))] = owner
    return specs


@contextmanager
def mutation_primaries(ledger):
    """All alias mutations enter raw primary leaves BEFORE their local lock.

    Nonblocking for every multi-primary entrypoint, including releases/recovery.
    A stale/partial registration retains the existing reservation and raises;
    it is never treated as zero held bytes or a new physical resource.
    """
    from . import posix_lock
    root = _queue_identity(ledger.root.parent)
    owner = {"type": "tier" if ledger._tier_census else "host", "id": ledger.host}
    record = _read_binding(root, owner, optional=True)
    if record is None:
        yield True
        return
    with ExitStack() as stack:
        specs = _binding_primaries(root, record)
        for path in sorted(specs):
            if not stack.enter_context(posix_lock.held(path, blocking=False)):
                raise LocalScratchError("canonical physical mutation owner busy")
        if _read_binding(root, owner) != record or _binding_primaries(root, record) != specs:
            raise LocalScratchError("physical mutation binding changed before exclusion")
        yield True


def require_reservation(ledger, demand):
    """A context token alone is not permission: it must belong to live lock scope."""
    physical = {k: n for k, n in demand.items() if k in FILESYSTEM_BYTE_KINDS and n > 0}
    if not physical:
        return
    transaction = _TRANSACTION.get()
    identity = None if transaction is None else transaction["identity"]
    with _ACTIVE_LOCK:
        active = identity in _ACTIVE
    if (not active or transaction["pid"] != os.getpid()
            or transaction["thread"] != threading.get_ident()):
        raise LocalScratchError("physical acquisition lacks an active owner transaction")
    remaining = transaction["remaining"].get(str(ledger.base), {})
    if any(type(n) is not int or n > remaining.get(k, 0) for k, n in physical.items()):
        raise LocalScratchError("physical acquisition exceeds its admitted remaining bound")
    for kind, need in physical.items():
        remaining[kind] -= need


def require_mint(ledger, capacity):
    kinds = {k for k, n in capacity.items() if k in FILESYSTEM_BYTE_KINDS and n > 0}
    if kinds:
        record = _read_binding(ledger.root.parent,
            {"type": "tier" if ledger._tier_census else "host", "id": ledger.host})
        if not kinds <= set().union(*(set(c["kinds"]) for c in record["constraints"])):
            raise LocalScratchError("physical mint has no closed native owner constraint")


@contextmanager
def admission(queue, paths, *, intent, role, additional=None, held_owner=None):
    """The ONE fresh ceil5% + aggregate whole-allowance guard and lock scope."""
    from . import core, pool, posix_lock, storage_tiers
    root = _queue_identity(queue.root)
    if _IN_PROVENANCE.get():
        raise LocalScratchError("candidate admission cannot run inside provenance materialization")
    entries = operation(intent, role)
    additional = {} if additional is None else additional
    with ExitStack() as stack:
        if not stack.enter_context(_registration_lock(root)):
            raise LocalScratchError("physical registration population busy")
        # Registration exclusion spans the whole closed census, decision and
        # acquisition: equal scans are not an atomicity or ABA proof, so a
        # registration replacement or new-owner creation cannot interleave
        # with this aggregate decision. Bodies inside this scope stay short
        # token operations, never payload I/O.
        records = _population(queue)
        specs = _primary_specs(records)
        if not specs:
            raise LocalScratchError("complete native filesystem owners unavailable")
        primaries = {str(_raw_lock(root, {"type": owner[0], "id": owner[1]}))
                     for owner, _ in specs.values()}
        for path in sorted(primaries):
            if not stack.enter_context(posix_lock.held(path, blocking=False)):
                raise LocalScratchError("used physical primary busy")
        for owner in sorted(records):
            ledger = _ledger(queue, {"type": owner[0], "id": owner[1]})
            if not stack.enter_context(ledger._mutation_locked(blocking=False)):
                raise LocalScratchError("used physical accounting alias busy")
            if _read_binding(root, records[owner]["owner"]) != records[owner]:
                raise LocalScratchError("physical alias changed under exclusion")
        allowance, frames, ledgers = {}, {}, {}
        for owner_key, record in records.items():
            ledger = _ledger(queue, record["owner"])
            ledgers[str(ledger.base)] = ledger
            covered = set().union(*(set(c["kinds"]) for c in record["constraints"]))
            asked = additional.get(str(ledger.base), {})
            if set(asked) - covered or any(type(n) is not int or n < 0 for n in asked.values()):
                raise LocalScratchError("additional physical demand is unattributed")
            for constraint in record["constraints"]:
                frame = _frame(constraint["frame"])
                key = tuple(frame["physical_key"])
                frames.setdefault(key, []).append(frame)
                # Whole shared ceilings are charged ONCE to EACH possible FS.
                # Held tokens and additional demand both count GiB units; the
                # aggregate enters the byte predicate as bytes.
                total = _held(ledger, constraint["kinds"])
                total += sum(n for k, n in asked.items() if k in constraint["kinds"])
                allowance[key] = allowance.get(key, 0) + total * GIB
        if set(additional) - set(ledgers):
            raise LocalScratchError("physical demand owner is outside closed population")
        if held_owner is not None:
            for resource, need in terms(intent, role).items():
                kind, tier = storage_tiers.split_demand_key(resource)
                ledger = queue.ledger() if tier is None else queue.tier_ledger(tier)
                names = pool.held_names_visible(ledger, held_owner)
                count = sum(name.rpartition("-")[0] == kind for name in names)
                if count < need:
                    raise LocalScratchError("operation growth lacks its owned held allowance")
        seen, evidence = {}, []
        for value in dict.fromkeys([str(p) for p in paths] + [entry["root"] for entry in entries]):
            fd = os.open(value, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK)
            stack.callback(os.close, fd)
            matches = []
            for key, views in frames.items():
                for frame in views:
                    try:
                        observed = match_filesystem_frame(fd, frame)
                    except (OSError, ValueError, KeyError, TypeError, AttributeError):
                        continue
                    matches.append((key, observed))
                    break
            if len(matches) != 1:
                raise LocalScratchError(f"used object lacks a unique native physical owner: {value}")
            key, observed = matches[0]
            filesystem_floor(observed["size_bytes"], observed["free_bytes"], allowance[key])
            for frame in frames[key]:
                filesystem_floor(frame["physical_size_bytes"], frame["physical_free_bytes"], allowance[key])
            seen[value] = (fd, observed["identity"])
            evidence.append({"path": value, **observed, "allowance_bytes": allowance[key]})
        # Every write-class root has to be backed by the resource it declares,
        # not merely another ledger with enough unrelated token files.
        for entry in entries:
            kind, tier = storage_tiers.split_demand_key(entry["resource"])
            ledger = queue.ledger() if tier is None else queue.tier_ledger(tier)
            record = records.get(("host" if tier is None else "tier", ledger.host))
            if record is None:
                raise LocalScratchError("declared write class has no registered owner")
            fd = seen[entry["root"]][0]
            if not any(kind in c["kinds"] and _matches(fd, c["frame"]) for c in record["constraints"]):
                raise LocalScratchError("declared write root differs from its physical reservation")
        for fd, expected in seen.values():
            if _capacity_mount(fd) != expected:
                raise LocalScratchError("used descriptor changed at final admission boundary")
        identity = object()
        transaction = {"identity": identity, "pid": os.getpid(), "thread": threading.get_ident(),
                       "remaining": {base: dict(asked) for base, asked in additional.items()},
                       "generations": {key: record["generation"] for key, record in records.items()},
                       "primaries": frozenset(primaries)}
        token = _TRANSACTION.set(transaction)
        with _ACTIVE_LOCK:
            _ACTIVE.add(identity)
        try:
            yield {"schema": "prismabuild.used_filesystem_verdict.v1", "role": role,
                   "sampled_unix": time.time(), "filesystems": evidence,
                   "materialization_credit_bytes": 0}
        finally:
            with _ACTIVE_LOCK:
                _ACTIVE.remove(identity)
            _TRANSACTION.reset(token)


def _matches(fd, frame):
    try:
        match_filesystem_frame(fd, frame)
        return True
    except (OSError, ValueError, KeyError, TypeError, AttributeError):
        return False


def action_intent(action):
    return action["environment"]["variables"].get(OPERATION_ENV)


def action_paths(action, *, checkout_root, cas_root, queue_root):
    paths = [Path(checkout_root), Path(cas_root), Path(queue_root), Path(__file__),
             Path(action["task"]["argv"][0]).resolve(strict=True)]
    paths.extend(Path(checkout_root) / entry["path"] for entry in action["code_closure"]["files"])
    from . import core
    cas = core.PrismaBuildCAS(cas_root)
    paths.extend(cas.input_path(entry) for entry in action["inputs"])
    manifest = action["params"].get("data_manifest")
    if isinstance(manifest, dict):
        entry = next((e for e in action["inputs"] if e["id"] == manifest["input_id"]), None)
        if entry is None:
            raise LocalScratchError("used source-bank manifest missing")
        data = core.load_data_manifest(cas.input_path(entry))
        paths.extend(Path(e["path"]) for e in data["entries"])
    return paths


def check_action(queue, action, *, checkout_root, cas_root, extra=(), role="worker", held_owner=None):
    paths = action_paths(action, checkout_root=checkout_root, cas_root=cas_root, queue_root=queue.root)
    with admission(queue, [*paths, *extra], intent=action_intent(action), role=role,
                   held_owner=held_owner):
        pass




def _verified_capture(queue, ref, expected_roots, mapping):
    """Bind the admitted native recipe, every source dependency and producer."""
    from . import action_result, core
    if not isinstance(ref, dict) or set(ref) != {"action_key", "published_unix", "attempt"}:
        raise LocalScratchError("native registration capture selection incomplete")
    checkout_owner = _execution_checkout_owner()
    token = _IN_PROVENANCE.set(True)
    try:
        return _verified_capture_body(queue, ref, expected_roots, mapping,
                                      checkout_owner, action_result)
    finally:
        _IN_PROVENANCE.reset(token)


def _verified_capture_body(queue, ref, expected_roots, mapping, checkout_owner,
                           action_result):
    """Provenance reads with candidate admission refused for their whole scope."""
    from . import core
    result = action_result.read_verified_action_result(
        queue, ref["action_key"], published_unix=ref["published_unix"], attempt=ref["attempt"],
        max_result_bytes=FILESYSTEM_FRAME_MAX_BYTES, max_evidence_bytes=FILESYSTEM_FRAME_MAX_BYTES,
        require_native_producer_context=True)
    request = result["request"]
    payload = core._decode_strict_json(result["payload"], where="native filesystem witness")
    action_result.bind_standard_capture_command(request)
    argv = request["params"]["command"]
    suffix = [WITNESS_TOOL]
    for root in expected_roots:
        suffix.extend(["--root", root])
    suffix.extend(["--mapping", core._canonical_bytes(mapping).decode()])
    if (argv[1:] != suffix
            or not isinstance(payload, dict) or set(payload) != {"schema", "host", "frames"}
            or payload["schema"] != WITNESS_SCHEMA or payload["host"] != result["host"]
            or not isinstance(payload["frames"], list)
            or [frame.get("root") for frame in payload["frames"]] != expected_roots):
        raise LocalScratchError("native witness differs from exact admitted recipe/mapping")
    source_root = Path(__file__).resolve().parents[2]
    sources = (WITNESS_TOOL, "src/prismabuild/filesystem_capacity.py",
               "src/prismabuild/local_scratch.py", "src/prismabuild/core.py")
    expected = tuple(core.raw_sha256(core._read_regular_file_nofollow(
        source_root / name, where="installed filesystem observer")) for name in sources)
    # pbrun's code closure is its stamp, NOT four direct source-file entries.
    # Reuse the existing executed-checkout verifier; all staging/Git/log growth
    # belongs to this registration operation's positive coordinator envelope.
    snapshot = request["params"].get("checkout_snapshot")
    if snapshot is None:
        raise LocalScratchError("native observer has no immutable executed checkout")
    selected, _, _ = action_result._select_terminal(
        queue, ref["action_key"], ref["published_unix"], ref["attempt"], FILESYSTEM_FRAME_MAX_BYTES)
    with checkout_owner({"action_key": request["action_key"],
            "cas_root": selected["cas_root"],
            "checkout_snapshot": snapshot}) as checkout:
        core._verify_pbrun_checkout_identity(request, checkout)
        core.verify_code_closure(request["code_closure"], checkout)
        actual = tuple(core.raw_sha256(core._read_regular_file_nofollow(
            checkout / name, where="executed filesystem observer")) for name in sources)
        if actual != expected:
            raise LocalScratchError("executed filesystem observer source/ABI differs")
    for frame in payload["frames"]:
        _frame(frame)
        if frame["owner_host"] != result["host"]:
            raise LocalScratchError("native frame has another producer host")
    final = action_result.read_verified_action_result(
        queue, ref["action_key"], published_unix=ref["published_unix"], attempt=ref["attempt"],
        max_result_bytes=FILESYSTEM_FRAME_MAX_BYTES, max_evidence_bytes=FILESYSTEM_FRAME_MAX_BYTES,
        require_native_producer_context=True)
    if final != result:
        raise LocalScratchError("selected native registration ending changed during provenance reads")
    return payload["frames"]


def install_registration(queue, mappings):
    """Closed-world, drained bootstrap/re-registration using EXISTING owners.

    This is an operational capture/adoption operation requiring Main's normal
    publisher/supervisor authority, not a self-authorizing submission flag.
    Each exact native request binds its mapping decision and installed source.
    No tokens/offer are minted. A partial install is durably INCOMPLETE and
    remains HOLD, including after a crash; no missing alias becomes zero money.
    """
    from . import core, pool, posix_lock
    root = _queue_identity(queue.root)
    if _TRANSACTION.get() is not None or _IN_PROVENANCE.get():
        raise LocalScratchError(
            "registration cannot run inside a live used-filesystem transaction "
            "or its own provenance materialization")
    if not isinstance(mappings, list) or not mappings:
        raise LocalScratchError("complete native owner registration required")
    candidates = {}
    for mapping in mappings:
        if (not isinstance(mapping, dict) or set(mapping) != {"owner", "constraints", "capture"}
                or not isinstance(mapping["constraints"], list) or not mapping["constraints"]):
            raise LocalScratchError("physical owner registration mapping partial")
        owner = _owner(mapping["owner"])
        key = (owner["type"], owner["id"])
        if key in candidates:
            raise LocalScratchError("physical owner registered twice")
        roots = [entry["root"] for entry in mapping["constraints"]]
        decision = {"owner": owner, "constraints": mapping["constraints"]}
        frames = _verified_capture(queue, mapping["capture"], roots, decision)
        constraints = []
        for entry, frame in zip(mapping["constraints"], frames):
            if (set(entry) != {"root", "kinds", "primary"}
                    or not isinstance(entry["kinds"], list) or not entry["kinds"]
                    or set(entry["kinds"]) - FILESYSTEM_BYTE_KINDS
                    or len(set(entry["kinds"])) != len(entry["kinds"])):
                raise LocalScratchError("physical owner/kind partition unknown")
            _owner(entry["primary"])
            # Host filesystem authority must come from that actual native host,
            # not a same-spelled directory observed from another machine.
            if owner["type"] == "host" and frame["owner_host"] != owner["id"]:
                raise LocalScratchError("host registration captured another native host")
            if owner["type"] == "tier":
                tier = pool.read_queue_record(queue.tier_record_path(owner["id"]))
                if not isinstance(tier, dict) or tier.get("host") != frame["owner_host"]:
                    raise LocalScratchError("tier native capture lacks its existing owner")
            constraints.append({"kinds": sorted(entry["kinds"]), "frame": frame,
                                "primary": entry["primary"]})
        candidates[key] = {"schema": BINDING_SCHEMA, "owner": owner,
            "generation": uuid.uuid4().hex, "complete": True,
            "constraints": constraints, "primaries": [], "registration": mapping["capture"]}
    groups = {}
    for owner_key, record in candidates.items():
        for constraint in record["constraints"]:
            key = tuple(constraint["frame"]["physical_key"])
            primary = constraint["primary"]
            target = (primary["type"], primary["id"])
            if target not in candidates:
                raise LocalScratchError("canonical primary omitted from closed registration")
            previous = groups.setdefault(key, {"owner": target, "aliases": []})
            if previous["owner"] != target:
                raise LocalScratchError("native physical resource has competing primary decisions")
            previous["aliases"].append({"owner": record["owner"], "kinds": constraint["kinds"],
                                         "generation": record["generation"]})
    for key, group in groups.items():
        record = candidates[group["owner"]]
        if not any(tuple(c["frame"]["physical_key"]) == key for c in record["constraints"]):
            raise LocalScratchError("canonical primary has no native physical descriptor")
        record["primaries"].append({"physical_key": list(key), "aliases": group["aliases"]})
    with ExitStack() as stack:
        if not stack.enter_context(_registration_lock(root)):
            raise LocalScratchError("physical registration namespace busy")
        old = _population(queue)
        if set(old) - set(candidates):
            raise LocalScratchError("registration omits an existing owner; removal is not authorized")
        locks = set()
        for owner, record in {**old, **candidates}.items():
            locks.add(str(_raw_lock(root, record["owner"])))
            for constraint in record["constraints"]:
                locks.add(str(_raw_lock(root, constraint["primary"])))
        for path in sorted(locks):
            if not stack.enter_context(posix_lock.held(path, blocking=False)):
                raise LocalScratchError("registration old/new canonical owners busy")
        # Every known owner is drained under exclusion. Unregistered physical
        # begin/mint are refused, so a new unbound owner cannot race this census.
        for owner_key in set(old) | set(candidates):
            ledger = _ledger(queue, {"type": owner_key[0], "id": owner_key[1]})
            if ledger.base.exists() and _held(ledger, FILESYSTEM_BYTE_KINDS):
                raise LocalScratchError("registration requires zero private/committed byte holders")
        for owner_key, previous in old.items():
            if _read_binding(root, previous["owner"]) != previous:
                raise LocalScratchError("old registration changed before drained replacement")
            # A reboot refresh is incarnation replacement under the SAME stable
            # physical key, never another primary or disappearance of old holds.
            old_keys = {tuple(c["frame"]["physical_key"]) for c in previous["constraints"]}
            new_keys = {tuple(c["frame"]["physical_key"]) for c in candidates[owner_key]["constraints"]}
            if old_keys != new_keys:
                raise LocalScratchError("physical remap requires an explicit ownership transfer")
        _primary_specs(candidates)
        for record in candidates.values():
            body = {**record, "complete": False}
            pool._write_json_atomic(_base(root, record["owner"]) / BINDING_FILE, _seal(body))
        for record in candidates.values():
            pool._write_json_atomic(_base(root, record["owner"]) / BINDING_FILE, _seal(record))
        return {"owners": [record["owner"] for record in candidates.values()],
                "physical_resources": len(groups), "tokens_minted": 0}


def refresh_owner(queue, ledger):
    """Refresh only its native owner frames, preserving closed registration."""
    from . import pool
    owner = {"type": "tier" if ledger._tier_census else "host", "id": ledger.host}
    record = _read_binding(queue.root, owner)
    frames = []
    for constraint in record["constraints"]:
        old = constraint["frame"]
        if old["owner_host"] != socket.gethostname():
            raise LocalScratchError("refresh outside native filesystem owner namespace")
        frame = capture_filesystem_capacity(old["root"])
        if (frame["identity"] != old["identity"] or frame["physical_key"] != old["physical_key"]
                or frame["dataset_guid"] != old["dataset_guid"]):
            raise LocalScratchError("physical incarnation changed; drained re-registration required")
        frames.append(frame)
    with ledger._mutation_locked(blocking=False) as acquired:
        if not acquired or _read_binding(queue.root, owner) != record:
            raise LocalScratchError("filesystem owner changed or busy during refresh")
        for constraint, frame in zip(record["constraints"], frames):
            constraint["frame"] = frame
        body = {k: v for k, v in record.items() if k != "binding_sha256"}
        pool._write_json_atomic(ledger.base / BINDING_FILE, _seal(body))
        return body


def bounded_mint(queue, ledger, capacity):
    """Existing minter observes its native registered disk, never invents an offer."""
    kinds = FILESYSTEM_BYTE_KINDS & capacity.keys()
    if not kinds:
        return dict(capacity)
    record = refresh_owner(queue, ledger)
    result = dict(capacity)
    for kind in kinds:
        limits = []
        for constraint in record["constraints"]:
            if kind not in constraint["kinds"]:
                continue
            frame = constraint["frame"]
            size, free = frame["physical_size_bytes"], frame["physical_free_bytes"]
            room = max(0, min(free - (size + 19) // 20,
                             frame["free_bytes"] - (frame["size_bytes"] + 19) // 20))
            limits.append(room // GIB)
        if not limits:
            raise LocalScratchError("physical mint omits a native constraint")
        result[kind] = min(result[kind], *limits)
    return result



_COORDINATOR = contextvars.ContextVar("coordinator_filesystem_hold", default=None)


@contextmanager
def reserve_operation(queue, intent, paths, *, role="coordinator",
                      operation_key=None, demand=None):
    """One committed operationP custody, or one normal claimed actionK window.

    ``role="coordinator"`` takes distinct committed custody under the SAME
    existing ledgers, never an action key: ``operation_key`` names one unique
    ``operation-p-<64 hex>`` holder. The aggregate floor is proven first with
    P's own still-unmaterialized allowance charged, then the tokens are
    committed with the existing ``ResourceLedger.acquire`` -- a committed,
    non-``claiming`` holder that every later census counts and no stale sweep
    recovers. A clean completion releases exactly the committed tokens; any
    other ending retains them for the recorded owner. A same-key entry while
    that custody is live in this pid/thread joins admission-only: no second
    commit, no release, and the outermost frame alone releases. A duplicate
    key with tokens outstanding is refused, never double-charged.

    ``role="worker"``/``"storage"`` is the ordinary actionK lifecycle: the
    claimed record, its resource scope and the sealed request own the held
    tokens. This context neither invents an action owner nor releases a
    claim's tokens; it proves the floor and rechecks the claim's lifetime.
    A standalone publisher without either owner is explicitly blocked.
    """
    from . import core, pool, resource_scope, storage_tiers
    _queue_identity(queue.root)
    if role == "coordinator":
        if not isinstance(operation_key, str) or not re.fullmatch(
                r"operation-p-[0-9a-f]{64}", operation_key):
            raise LocalScratchError(
                "committed operation custody requires a unique operation-p key")
        current = _COORDINATOR.get()
        if (isinstance(current, dict) and current.get("operation_key") == operation_key
                and current.get("pid") == os.getpid()
                and current.get("thread") == threading.get_ident()
                and current.get("queue") == str(queue.root)):
            # Nested join: the outermost frame owns the committed custody and
            # alone releases it; this frame only proves its own exact paths.
            with admission(queue, paths, intent=intent, role=role):
                yield
            return
        wanted = terms(intent, role)
        if demand is not None:
            if (not isinstance(demand, dict)
                    or any(type(k) is not str or type(n) is not int or n < 0
                           for k, n in demand.items())
                    or any(demand.get(resource, 0) < need
                           for resource, need in wanted.items())):
                raise LocalScratchError(
                    "committed operation demand understates its declared growth")
            wanted = dict(demand)
        per_base, ledgers = {}, {}
        for resource, need in wanted.items():
            kind, tier = storage_tiers.split_demand_key(resource)
            ledger = queue.ledger() if tier is None else queue.tier_ledger(tier)
            base = str(ledger.base)
            ledgers[base] = ledger
            per_base[base] = {**per_base.get(base, {}), kind: need}
        owners = {base: {kind: n for kind, n in kinds.items()
                         if kind in FILESYSTEM_BYTE_KINDS}
                  for base, kinds in per_base.items()}
        owners = {base: kinds for base, kinds in owners.items() if kinds}
        committed, entered, completed = [], False, False
        try:
            with admission(queue, paths, intent=intent, role=role,
                           additional=owners) as verdict:
                for base, kinds in owners.items():
                    if pool.held_names_visible(ledgers[base], operation_key):
                        raise LocalScratchError(
                            "committed operation custody already holds tokens")
                    if not ledgers[base].acquire(operation_key, per_base[base]):
                        raise LocalScratchError(
                            "committed operation custody refused under the floor")
                    committed.append(base)
                entered = True
                token = _COORDINATOR.set({"pid": os.getpid(),
                                          "thread": threading.get_ident(),
                                          "queue": str(queue.root), "intent": intent,
                                          "role": role, "operation_key": operation_key})
                try:
                    yield verdict
                    completed = True
                finally:
                    _COORDINATOR.reset(token)
        finally:
            if committed and (completed or not entered):
                # Exact owned rollback of a refused multi-ledger commit, or the
                # one clean release. Anything else retains: crash/uncertainty
                # custody is recovered by the recorded owner, never swept.
                for base in committed:
                    ledgers[base].release(operation_key)
        return
    if role not in ("worker", "storage"):
        raise LocalScratchError("filesystem operation role unknown")
    key = os.environ.get(core.ACTION_KEY_ENV)
    if not key or not re.fullmatch(r"[0-9a-f]{64}", key):
        raise LocalScratchError(
            f"filesystem {role} admission requires its real claimed action owner")
    claim = pool.read_queue_record(queue.item_path(pool.CLAIMED, key))
    if not isinstance(claim, dict):
        raise LocalScratchError("durable action claim is absent or already ended")
    control = claim.get("resource_scope")
    nonce, scope = resource_scope._checked_control_identity(key, control)
    if (os.environ.get(core.ACTION_NONCE_ENV) != nonce
            or os.environ.get(core.ACTION_SCOPE_ENV) != scope
            or os.environ.get(core.QUEUE_ROOT_ENV) != str(queue.root)
            or not resource_scope._process_in_scope(os.getpid(), Path(control["cgroup_path"]))):
        raise LocalScratchError("admission process is outside its exact claimed scope")
    owner = pool._sealed_action_request(str(claim["cas_root"]), key)
    if owner is None or operation(action_intent(owner), role) != operation(intent, role):
        raise LocalScratchError("operation growth differs from its sealed durable owner")
    with admission(queue, paths, intent=intent, role=role, held_owner=key):
        pass
    current = {"pid": os.getpid(), "thread": threading.get_ident(), "queue": str(queue.root),
               "intent": intent, "role": role, "action_key": key, "claim": claim}
    token = _COORDINATOR.set(current)
    try:
        yield
    finally:
        _COORDINATOR.reset(token)
        final = pool.read_queue_record(queue.item_path(pool.CLAIMED, key))
        if not isinstance(final, dict) or not pool._same_claim(final, claim):
            raise LocalScratchError("durable claim owner changed before operation completion")

