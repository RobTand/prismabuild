"""Generation-bound scratch intent, ownership, and descriptor cleanup.

The profile producer must never import this module. Every mutation rereads the
exact durable owner; no claim, lease, or containment evidence is cached here.
"""
from __future__ import annotations

import os
import socket
from collections.abc import Mapping
from pathlib import Path, PurePosixPath

from . import contracts, core as pb, local_scratch as naming
from .record_store import Absent, Present, RecordStore, ScratchOwnerConflict, Unknown

SCRATCH_LIFETIME_SELECTION_SCHEMA_V1 = "prismabuild.scratch_lifetime_selection.v1"
SCRATCH_LIFETIME_RECORD_SCHEMA_V1 = "prismabuild.scratch_lifetime_record.v1"
SCRATCH_LIFETIME_FIELD = "scratch_lifetime_record"


def build_scratch_lifetime_selection(entries: object) -> dict[str, object]:
    """Build one sealed versioned lifetime selection, validated, with no I/O.

    The pool validates the same shape again at publication; the SDK builds the
    exact object the sealed ``naming.DECLARATIONS_ENV`` variable carries. A builder
    output never grants cleanup, quota, or deletion authority. The builder
    enforces the pool's 16 KiB sealed-input bound on its canonical bytes.
    """
    from . import core

    if not isinstance(entries, list) or len(entries) > naming._MAX_DECLARATIONS:
        raise naming.LocalScratchError("scratch lifetime selection needs at most 64 entries")
    checked = []
    for entry in entries:
        if (not isinstance(entry, Mapping) or set(entry) != {"root_env", "name", "lifetime"}
                or not isinstance(entry["root_env"], str) or not naming._NAME.fullmatch(entry["root_env"])
                or not isinstance(entry["name"], str) or not naming._COMPONENT.fullmatch(entry["name"])
                or entry["lifetime"] not in ("ephemeral", "persistent")):
            raise naming.LocalScratchError("scratch lifetime entries need root_env, name and a known lifetime")
        checked.append({"root_env": entry["root_env"], "name": entry["name"],
                        "lifetime": entry["lifetime"]})
    seen = {(entry["root_env"], entry["name"]) for entry in checked}
    if len(seen) != len(checked):
        raise naming.LocalScratchError("duplicate scratch lifetime selection")
    selection: dict[str, object] = {"schema": SCRATCH_LIFETIME_SELECTION_SCHEMA_V1,
                                    "entries": checked}
    if len(core._canonical_bytes(selection)) > naming._MAX_DECLARATION_INPUT_BYTES:
        raise naming.LocalScratchError("scratch lifetime selection exceeds 16 KiB")
    return selection


def selections(variables: Mapping[str, str]) -> list[dict[str, object]]:
    """Validate versioned lifetime intent without granting filesystem authority.

    Legacy arrays retain naming-only semantics. Ephemeral and persistent root
    pairs cannot overlap; intent never creates, traverses, or removes a path.
    """
    selected = naming._scratch_selection_input(variables)
    if selected is None:
        return []
    if isinstance(selected, list):
        naming._scratch_selections(variables)
        return []
    if (not isinstance(selected, dict) or set(selected) != {"schema", "entries"}
            or selected["schema"] != SCRATCH_LIFETIME_SELECTION_SCHEMA_V1):
        raise naming.LocalScratchError("scratch lifetime selection has an unknown schema or fields")
    entries = selected["entries"]
    if not isinstance(entries, list) or len(entries) > naming._MAX_DECLARATIONS:
        raise naming.LocalScratchError("scratch lifetime selection needs at most 64 entries")
    if not entries:
        return []
    for entry in entries:
        if (not isinstance(entry, dict) or set(entry) != {"root_env", "name", "lifetime"}
                or entry["lifetime"] not in ("ephemeral", "persistent")):
            raise naming.LocalScratchError("scratch lifetime entries need root_env, name and a known lifetime")
    names = [{"root_env": entry["root_env"], "name": entry["name"]} for entry in entries]
    # Naming validation and pair accounting keep their existing authoritative home.
    import json
    naming._scratch_selections({**variables, naming.DECLARATIONS_ENV: json.dumps(names)})
    pairs = {pair["root_env"]: pair for pair in naming.scratch_pairs(variables)}
    result = [{**entry, **pairs[entry["root_env"]]} for entry in entries]
    temporary = [PurePosixPath(str(entry["root"])) for entry in result
                 if entry["lifetime"] == "ephemeral"]
    persistent = [PurePosixPath(str(entry["root"])) for entry in result
                  if entry["lifetime"] == "persistent"]
    if any(first == second or first in second.parents or second in first.parents
           for first in temporary for second in persistent):
        raise naming.LocalScratchError("ephemeral and persistent scratch root pairs overlap")
    return result


def publication(variables: Mapping[str, str]) -> list[dict[str, object]]:
    """Negotiate only the new version; legacy refusals remain prelaunch.

    Old workers already refuse object/malformed inputs as naming arrays, so
    ignoring an unrecognized schema here never grants cleanup or launch.
    """
    try:
        selected = naming._scratch_selection_input(variables)
    except naming.LocalScratchError:
        return []
    if isinstance(selected, dict) and selected.get("schema") == SCRATCH_LIFETIME_SELECTION_SCHEMA_V1:
        return selections(variables)
    return []


def validate_record(value: object) -> dict[str, object]:
    """Typed attempt ownership, never containment or an arbitrary-path grant."""
    from . import core

    if (not isinstance(value, Mapping)
            or set(value) != {"schema", "claim_envelope", "entries", "registration_complete"}
            or value["schema"] != SCRATCH_LIFETIME_RECORD_SCHEMA_V1
            or type(value["registration_complete"]) is not bool):
        raise naming.LocalScratchError("scratch lifetime record has unknown fields or schema")
    envelope = value["claim_envelope"]
    if not isinstance(envelope, Mapping) or not isinstance(envelope.get("owner_attempt"), Mapping):
        raise naming.LocalScratchError("scratch lifetime record lacks its attempt envelope")
    checked = naming._scratch_claim_envelope({
        **envelope, "resource_scope": {"action_key": envelope.get("action_key"),
                                      **envelope["owner_attempt"]}})
    if checked != envelope:
        raise naming.LocalScratchError("scratch lifetime envelope disagrees")
    entries = value["entries"]
    if not isinstance(entries, list) or not 0 < len(entries) <= naming._MAX_DECLARATIONS:
        raise naming.LocalScratchError("scratch lifetime record needs 1 to 64 entries")
    normalized, seen = [], set()
    for raw in entries:
        fields = {"root_env", "max_env", "root", "max_bytes", "name", "lifetime",
                  "declaration", "parent_identity", "identity", "cleaned"}
        if (not isinstance(raw, Mapping) or set(raw) != fields
                or raw["lifetime"] not in {"ephemeral", "persistent"}
                or type(raw["cleaned"]) is not bool):
            raise naming.LocalScratchError("scratch lifetime entry is malformed")
        naming = naming._scratch_declaration({
            "schema": naming.EPHEMERAL_SCRATCH_SCHEMA_V1, "lifetime": "ephemeral",
            **{k: raw[k] for k in ("root_env", "max_env", "root", "max_bytes", "name")},
            "owner_action_key": checked["action_key"],
            "owner_published_unix": checked["published_unix"],
            "owner_host": checked["claimed_host"], "owner_attempt": checked["owner_attempt"]})
        pair = (raw["root_env"], raw["name"])
        if pair in seen:
            raise naming.LocalScratchError("scratch lifetime entries are duplicated")
        seen.add(pair)
        entry = dict(raw)
        if raw["lifetime"] == "persistent":
            if (any(raw[k] is not None for k in ("declaration", "parent_identity", "identity"))
                    or raw["cleaned"] is not True):
                raise naming.LocalScratchError("persistent scratch must not carry deletion authority")
        else:
            if raw["declaration"] != naming:
                raise naming.LocalScratchError("scratch lifetime declaration has a foreign owner")
            if raw["parent_identity"] is not None:
                entry["parent_identity"] = _scratch_directory_identity(
                    naming, raw["parent_identity"], parent=True)
            if raw["identity"] is not None:
                entry["identity"] = _scratch_directory_identity(naming, raw["identity"])
                if entry["parent_identity"] != entry["identity"][:-1]:
                    raise naming.LocalScratchError("scratch leaf and parent identities disagree")
            elif value["registration_complete"] and not raw["cleaned"]:
                raise naming.LocalScratchError("ready scratch lacks directory identity")
        normalized.append(entry)
    record = {**value, "claim_envelope": checked, "entries": normalized}
    if len(core._canonical_bytes(record)) > naming._MAX_DECLARATION_RECORD_BYTES:
        raise naming.LocalScratchError("scratch lifetime record exceeds 256 KiB")
    return record


def _intent(record: Mapping[str, object]) -> list[dict[str, object]]:
    return [{k: entry[k] for k in ("root_env", "max_env", "root", "max_bytes", "name", "lifetime")}
            for entry in record["entries"]]


def _directory_birthtime(fd: int) -> int:
    """Linux statx creation identity, unlike mutable ctime/mtime.

    dev/ino can be reused after a removal whose record commit was interrupted.
    Missing immutable creation time therefore refuses this lifetime version.
    """
    import ctypes

    class Timestamp(ctypes.Structure):
        _fields_ = [("seconds", ctypes.c_int64), ("nanoseconds", ctypes.c_uint32),
                    ("reserved", ctypes.c_int32)]

    class Statx(ctypes.Structure):
        _fields_ = [("mask", ctypes.c_uint32), ("blksize", ctypes.c_uint32),
                    ("attributes", ctypes.c_uint64), ("nlink", ctypes.c_uint32),
                    ("uid", ctypes.c_uint32), ("gid", ctypes.c_uint32),
                    ("mode", ctypes.c_uint16), ("spare0", ctypes.c_uint16),
                    ("ino", ctypes.c_uint64), ("size", ctypes.c_uint64),
                    ("blocks", ctypes.c_uint64), ("attributes_mask", ctypes.c_uint64),
                    ("atime", Timestamp), ("btime", Timestamp), ("ctime", Timestamp),
                    ("mtime", Timestamp), ("rdev_major", ctypes.c_uint32),
                    ("rdev_minor", ctypes.c_uint32), ("dev_major", ctypes.c_uint32),
                    ("dev_minor", ctypes.c_uint32), ("mnt_id", ctypes.c_uint64),
                    ("dio_mem_align", ctypes.c_uint32), ("dio_offset_align", ctypes.c_uint32),
                    ("spare3", ctypes.c_uint64 * 12)]

    if ctypes.sizeof(Statx) != 256 or Statx.btime.offset != 0x50:
        raise naming.LocalScratchError("statx ABI unsupported")
    libc = ctypes.CDLL(None, use_errno=True)
    try:
        statx = libc.statx
    except AttributeError:
        raise naming.LocalScratchError("statx unavailable for scratch identity") from None
    statx.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int,
                     ctypes.c_uint, ctypes.POINTER(Statx)]
    statx.restype = ctypes.c_int
    value = Statx()
    # AT_EMPTY_PATH observes the held directory, not a pathname alias.
    if statx(fd, b"", 0x1000, 0x800, ctypes.byref(value)) != 0:
        raise OSError(ctypes.get_errno(), "scratch statx failed")
    if (not value.mask & 0x800 or not 0 <= value.btime.nanoseconds < 1_000_000_000
            or value.btime.seconds <= 0):
        raise naming.LocalScratchError("scratch immutable creation identity unavailable")
    return value.btime.seconds * 1_000_000_000 + value.btime.nanoseconds


def _directory_identity(fd: int, name: str) -> dict[str, object]:
    import stat

    info = os.fstat(fd)
    if not stat.S_ISDIR(info.st_mode):
        raise naming.LocalScratchError("scratch identity is not a directory")
    ids = [line.split(":", 1)[1].strip() for line in
           Path(f"/proc/self/fdinfo/{fd}").read_text().splitlines()
           if line.startswith("mnt_id:")]
    if len(ids) != 1 or not ids[0].isascii() or not ids[0].isdigit():
        raise naming.LocalScratchError("scratch directory mount identity unknown")
    return {"name": name, "dev": info.st_dev, "ino": info.st_ino,
            "uid": info.st_uid, "mnt_id": int(ids[0]), "birth_ns": _directory_birthtime(fd)}


def _check_directory_link(parent: int, name: str, identity: Mapping[str, object]) -> None:
    import stat

    info = os.stat(name, dir_fd=parent, follow_symlinks=False)
    if (not stat.S_ISDIR(info.st_mode)
            or (info.st_dev, info.st_ino, info.st_uid)
            != (identity["dev"], identity["ino"], identity["uid"])):
        raise naming.LocalScratchError("scratch directory link was replaced or renamed")
    fd = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=parent)
    try:
        if _directory_identity(fd, name) != identity:
            raise naming.LocalScratchError("scratch directory creation or mount identity changed")
    finally:
        os.close(fd)


