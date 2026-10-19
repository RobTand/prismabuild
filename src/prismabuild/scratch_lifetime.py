"""Generation-bound scratch intent, ownership, and descriptor cleanup.

The profile producer must never import this module. Every mutation rereads the
exact durable owner; no claim, lease, or containment evidence is cached here.
"""
from __future__ import annotations

from . import claim_identity

import os
import socket
from collections.abc import Mapping
from pathlib import Path, PurePosixPath

from . import contracts, core as pb, local_scratch as scratch_declarations
from .record_store import Absent, AmbiguousClaimHolder, Present, RecordStore, ScratchOwnerConflict, Unknown

SCRATCH_LIFETIME_SELECTION_SCHEMA_V1 = "prismabuild.scratch_lifetime_selection.v1"
SCRATCH_LIFETIME_RECORD_SCHEMA_V1 = "prismabuild.scratch_lifetime_record.v1"
SCRATCH_LIFETIME_FIELD = "scratch_lifetime_record"


def build_scratch_lifetime_selection(entries: object) -> dict[str, object]:
    """Build one sealed versioned lifetime selection, validated, with no I/O.

    The pool validates the same shape again at publication; the SDK builds the
    exact object the sealed ``DECLARATIONS_ENV`` variable carries. A builder
    output never grants cleanup, quota, or deletion authority. The builder
    enforces the pool's 16 KiB sealed-input bound on its canonical bytes.
    """
    from . import core

    if not isinstance(entries, list) or len(entries) > scratch_declarations._MAX_DECLARATIONS:
        raise scratch_declarations.LocalScratchError("scratch lifetime selection needs at most 64 entries")
    checked = []
    for entry in entries:
        if (not isinstance(entry, Mapping) or set(entry) != {"root_env", "name", "lifetime"}
                or not isinstance(entry["root_env"], str) or not scratch_declarations._NAME.fullmatch(entry["root_env"])
                or not isinstance(entry["name"], str) or not scratch_declarations._COMPONENT.fullmatch(entry["name"])
                or entry["lifetime"] not in ("ephemeral", "persistent")):
            raise scratch_declarations.LocalScratchError("scratch lifetime entries need root_env, name and a known lifetime")
        checked.append({"root_env": entry["root_env"], "name": entry["name"],
                        "lifetime": entry["lifetime"]})
    seen = {(entry["root_env"], entry["name"]) for entry in checked}
    if len(seen) != len(checked):
        raise scratch_declarations.LocalScratchError("duplicate scratch lifetime selection")
    selection: dict[str, object] = {"schema": SCRATCH_LIFETIME_SELECTION_SCHEMA_V1,
                                    "entries": checked}
    if len(core._canonical_bytes(selection)) > scratch_declarations._MAX_DECLARATION_INPUT_BYTES:
        raise scratch_declarations.LocalScratchError("scratch lifetime selection exceeds 16 KiB")
    return selection


def selections(variables: Mapping[str, str]) -> list[dict[str, object]]:
    """Validate versioned lifetime intent without granting filesystem authority.

    Legacy arrays retain naming-only semantics. Ephemeral and persistent root
    pairs cannot overlap; intent never creates, traverses, or removes a path.
    """
    selected = scratch_declarations._scratch_selection_input(variables)
    if selected is None:
        return []
    if isinstance(selected, list):
        scratch_declarations._scratch_selections(variables)
        return []
    if (not isinstance(selected, dict) or set(selected) != {"schema", "entries"}
            or selected["schema"] != SCRATCH_LIFETIME_SELECTION_SCHEMA_V1):
        raise scratch_declarations.LocalScratchError("scratch lifetime selection has an unknown schema or fields")
    entries = selected["entries"]
    if not isinstance(entries, list) or len(entries) > scratch_declarations._MAX_DECLARATIONS:
        raise scratch_declarations.LocalScratchError("scratch lifetime selection needs at most 64 entries")
    if not entries:
        return []
    for entry in entries:
        if (not isinstance(entry, dict) or set(entry) != {"root_env", "name", "lifetime"}
                or entry["lifetime"] not in ("ephemeral", "persistent")):
            raise scratch_declarations.LocalScratchError("scratch lifetime entries need root_env, name and a known lifetime")
    names = [{"root_env": entry["root_env"], "name": entry["name"]} for entry in entries]
    # Naming validation and pair accounting keep their existing authoritative home.
    import json
    scratch_declarations._scratch_selections({**variables, scratch_declarations.DECLARATIONS_ENV: json.dumps(names)})
    pairs = {pair["root_env"]: pair for pair in scratch_declarations.scratch_pairs(variables)}
    result = [{**entry, **pairs[entry["root_env"]]} for entry in entries]
    temporary = [PurePosixPath(str(entry["root"])) for entry in result
                 if entry["lifetime"] == "ephemeral"]
    persistent = [PurePosixPath(str(entry["root"])) for entry in result
                  if entry["lifetime"] == "persistent"]
    if any(first == second or first in second.parents or second in first.parents
           for first in temporary for second in persistent):
        raise scratch_declarations.LocalScratchError("ephemeral and persistent scratch root pairs overlap")
    return result


def publication(variables: Mapping[str, str]) -> list[dict[str, object]]:
    """Negotiate only the new version; legacy refusals remain prelaunch.

    Old workers already refuse object/malformed inputs as naming arrays, so
    ignoring an unrecognized schema here never grants cleanup or launch.
    """
    try:
        selected = scratch_declarations._scratch_selection_input(variables)
    except scratch_declarations.LocalScratchError:
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
        raise scratch_declarations.LocalScratchError("scratch lifetime record has unknown fields or schema")
    envelope = value["claim_envelope"]
    if not isinstance(envelope, Mapping) or not isinstance(envelope.get("owner_attempt"), Mapping):
        raise scratch_declarations.LocalScratchError("scratch lifetime record lacks its attempt envelope")
    checked = scratch_declarations._scratch_claim_envelope({
        **envelope, "resource_scope": {"action_key": envelope.get("action_key"),
                                      **envelope["owner_attempt"]}})
    if checked != envelope:
        raise scratch_declarations.LocalScratchError("scratch lifetime envelope disagrees")
    entries = value["entries"]
    if not isinstance(entries, list) or not 0 < len(entries) <= scratch_declarations._MAX_DECLARATIONS:
        raise scratch_declarations.LocalScratchError("scratch lifetime record needs 1 to 64 entries")
    normalized, seen = [], set()
    for raw in entries:
        fields = {"root_env", "max_env", "root", "max_bytes", "name", "lifetime",
                  "declaration", "parent_identity", "identity", "cleaned"}
        if (not isinstance(raw, Mapping) or set(raw) != fields
                or raw["lifetime"] not in {"ephemeral", "persistent"}
                or type(raw["cleaned"]) is not bool):
            raise scratch_declarations.LocalScratchError("scratch lifetime entry is malformed")
        naming = scratch_declarations._scratch_declaration({
            "schema": scratch_declarations.EPHEMERAL_SCRATCH_SCHEMA_V1, "lifetime": "ephemeral",
            **{k: raw[k] for k in ("root_env", "max_env", "root", "max_bytes", "name")},
            "owner_action_key": checked["action_key"],
            "owner_published_unix": checked["published_unix"],
            "owner_host": checked["claimed_host"], "owner_attempt": checked["owner_attempt"]})
        pair = (raw["root_env"], raw["name"])
        if pair in seen:
            raise scratch_declarations.LocalScratchError("scratch lifetime entries are duplicated")
        seen.add(pair)
        entry = dict(raw)
        if raw["lifetime"] == "persistent":
            if (any(raw[k] is not None for k in ("declaration", "parent_identity", "identity"))
                    or raw["cleaned"] is not True):
                raise scratch_declarations.LocalScratchError("persistent scratch must not carry deletion authority")
        else:
            if raw["declaration"] != naming:
                raise scratch_declarations.LocalScratchError("scratch lifetime declaration has a foreign owner")
            if raw["parent_identity"] is not None:
                entry["parent_identity"] = _scratch_directory_identity(
                    naming, raw["parent_identity"], parent=True)
            if raw["identity"] is not None:
                entry["identity"] = _scratch_directory_identity(naming, raw["identity"])
                if entry["parent_identity"] != entry["identity"][:-1]:
                    raise scratch_declarations.LocalScratchError("scratch leaf and parent identities disagree")
            elif value["registration_complete"] and not raw["cleaned"]:
                raise scratch_declarations.LocalScratchError("ready scratch lacks directory identity")
        normalized.append(entry)
    record = {**value, "claim_envelope": checked, "entries": normalized}
    if len(core._canonical_bytes(record)) > scratch_declarations._MAX_DECLARATION_RECORD_BYTES:
        raise scratch_declarations.LocalScratchError("scratch lifetime record exceeds 256 KiB")
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
        raise scratch_declarations.LocalScratchError("statx ABI unsupported")
    libc = ctypes.CDLL(None, use_errno=True)
    try:
        statx = libc.statx
    except AttributeError:
        raise scratch_declarations.LocalScratchError("statx unavailable for scratch identity") from None
    statx.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int,
                     ctypes.c_uint, ctypes.POINTER(Statx)]
    statx.restype = ctypes.c_int
    value = Statx()
    # AT_EMPTY_PATH observes the held directory, not a pathname alias.
    if statx(fd, b"", 0x1000, 0x800, ctypes.byref(value)) != 0:
        raise OSError(ctypes.get_errno(), "scratch statx failed")
    if (not value.mask & 0x800 or not 0 <= value.btime.nanoseconds < 1_000_000_000
            or value.btime.seconds <= 0):
        raise scratch_declarations.LocalScratchError("scratch immutable creation identity unavailable")
    return value.btime.seconds * 1_000_000_000 + value.btime.nanoseconds


def _directory_identity(fd: int, name: str) -> dict[str, object]:
    import stat

    info = os.fstat(fd)
    if not stat.S_ISDIR(info.st_mode):
        raise scratch_declarations.LocalScratchError("scratch identity is not a directory")
    ids = [line.split(":", 1)[1].strip() for line in
           Path(f"/proc/self/fdinfo/{fd}").read_text().splitlines()
           if line.startswith("mnt_id:")]
    if len(ids) != 1 or not ids[0].isascii() or not ids[0].isdigit():
        raise scratch_declarations.LocalScratchError("scratch directory mount identity unknown")
    return {"name": name, "dev": info.st_dev, "ino": info.st_ino,
            "uid": info.st_uid, "mnt_id": int(ids[0]), "birth_ns": _directory_birthtime(fd)}


def _check_directory_link(parent: int, name: str, identity: Mapping[str, object]) -> None:
    import stat

    info = os.stat(name, dir_fd=parent, follow_symlinks=False)
    if (not stat.S_ISDIR(info.st_mode)
            or (info.st_dev, info.st_ino, info.st_uid)
            != (identity["dev"], identity["ino"], identity["uid"])):
        raise scratch_declarations.LocalScratchError("scratch directory link was replaced or renamed")
    fd = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=parent)
    try:
        if _directory_identity(fd, name) != identity:
            raise scratch_declarations.LocalScratchError("scratch directory creation or mount identity changed")
    finally:
        os.close(fd)


def _check_scratch_links(fds: list[int], identities: list[dict[str, object]]) -> None:
    for index in range(1, len(fds)):
        _check_directory_link(fds[index - 1], str(identities[index]["name"]), identities[index])


def _check_scratch_permissions(declaration: Mapping[str, object], fds: list[int]) -> None:
    root_index = len(Path(str(declaration["root"])).parts) - 1
    root = _directory_identity(fds[root_index], ".")
    for fd in fds[root_index + 1:]:
        info = os.fstat(fd)
        identity = _directory_identity(fd, ".")
        if (info.st_uid != os.getuid() or info.st_mode & 0o777 != 0o700
                or identity["dev"] != root["dev"] or identity["mnt_id"] != root["mnt_id"]):
            raise scratch_declarations.LocalScratchError("scratch namespace permissions or filesystem changed")


def _prepare_scratch_directory(declaration: Mapping[str, object]) -> list[dict[str, object]]:
    """Prepare private namespace ancestors only; never create the payload leaf."""
    checked = scratch_declarations._scratch_declaration(declaration)
    parts = scratch_declarations.ephemeral_scratch_path(checked).parts
    root_index = len(Path(str(checked["root"])).parts) - 1
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
    fds, identities = [], []
    try:
        fds.append(os.open("/", flags))
        identities.append(_directory_identity(fds[0], "/"))
        for index, name in enumerate(parts[1:-1], 1):
            if index > root_index:
                try:
                    os.mkdir(name, mode=0o700, dir_fd=fds[-1])
                    os.fsync(fds[-1])
                except FileExistsError:
                    pass
            fds.append(os.open(name, flags, dir_fd=fds[-1]))
            identities.append(_directory_identity(fds[-1], name))
            if index == root_index:
                scratch_declarations._descriptor_identity(fds[-1])
            if index > root_index:
                info = os.fstat(fds[-1])
                if (info.st_uid != os.getuid() or info.st_mode & 0o077
                        or info.st_dev != identities[root_index]["dev"]
                        or identities[-1]["mnt_id"] != identities[root_index]["mnt_id"]):
                    raise scratch_declarations.LocalScratchError("scratch namespace is not private on the root filesystem")
            _check_scratch_links(fds, identities)
        _check_scratch_permissions(checked, fds)
        return identities
    finally:
        for fd in reversed(fds):
            os.close(fd)


def _scratch_directory_identity(declaration: Mapping[str, object], value: object, *, parent=False
                                ) -> list[dict[str, object]]:
    parts = scratch_declarations.ephemeral_scratch_path(declaration).parts
    if parent:
        parts = parts[:-1]
    if not isinstance(value, list) or len(value) != len(parts):
        raise scratch_declarations.LocalScratchError("scratch directory identity chain is incomplete")
    result = []
    for name, raw in zip(parts, value):
        if (not isinstance(raw, dict) or set(raw) != {"name", "dev", "ino", "uid", "mnt_id", "birth_ns"}
                or raw["name"] != name
                or any(type(raw[k]) is not int or raw[k] < 0
                       for k in ("dev", "ino", "uid", "mnt_id", "birth_ns"))
                or raw["ino"] == 0 or raw["mnt_id"] == 0 or raw["birth_ns"] == 0):
            raise scratch_declarations.LocalScratchError("scratch directory identity is malformed")
        result.append(dict(raw))
    if result[-1]["uid"] != os.getuid():
        raise scratch_declarations.LocalScratchError("scratch directory belongs to another uid")
    return result


def _open_scratch_identity_chain(identities: list[dict[str, object]]) -> list[int]:
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
    fds = []
    try:
        fds.append(os.open("/", flags))
        for index, expected in enumerate(identities):
            if index:
                fds.append(os.open(str(expected["name"]), flags, dir_fd=fds[-1]))
            if _directory_identity(fds[-1], str(expected["name"])) != expected:
                raise scratch_declarations.LocalScratchError("scratch directory identity changed")
        _check_scratch_links(fds, identities)
        return fds
    except BaseException:
        for fd in reversed(fds):
            os.close(fd)
        raise


def _create_scratch_directory(declaration: Mapping[str, object], parent_identity=None
                              ) -> list[dict[str, object]]:
    """Create a fresh private leaf, never adopt a pre-existing leaf.

    The pool persists parent identities before calling, then leaf identity
    before launch. A crash in between does not grant lexical-path permission.
    """
    identities = (_prepare_scratch_directory(declaration) if parent_identity is None else
                  _scratch_directory_identity(declaration, parent_identity, parent=True))
    fds = _open_scratch_identity_chain(identities)
    name = scratch_declarations.ephemeral_scratch_path(declaration).name
    try:
        try:
            os.mkdir(name, mode=0o700, dir_fd=fds[-1])
        except FileExistsError:
            raise scratch_declarations.LocalScratchError("scratch leaf already exists; ownership is unknown") from None
        os.fsync(fds[-1])
        fds.append(os.open(name, os.O_RDONLY | os.O_DIRECTORY |
                           os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=fds[-1]))
        identities.append(_directory_identity(fds[-1], name))
        info = os.fstat(fds[-1])
        if (info.st_uid != os.getuid() or info.st_mode & 0o077
                or identities[-1]["dev"] != identities[-2]["dev"]
                or identities[-1]["mnt_id"] != identities[-2]["mnt_id"]):
            raise scratch_declarations.LocalScratchError("scratch leaf is not private on the parent filesystem")
        _check_scratch_links(fds, identities)
        _check_scratch_permissions(declaration, fds)
        return identities
    finally:
        for fd in reversed(fds):
            os.close(fd)


def _uncreated_scratch_directory(declaration: Mapping[str, object], parent_identity) -> None:
    """Prove the prepared leaf absent; an uncommitted existing leaf refuses."""
    if parent_identity is None:
        # The pool cannot call leaf creation before this parent commit.
        return
    identities = _scratch_directory_identity(declaration, parent_identity, parent=True)
    fds = _open_scratch_identity_chain(identities)
    try:
        _check_scratch_permissions(declaration, fds)
        try:
            os.stat(scratch_declarations.ephemeral_scratch_path(declaration).name, dir_fd=fds[-1], follow_symlinks=False)
        except FileNotFoundError:
            _check_scratch_links(fds, identities)
            return
        raise scratch_declarations.LocalScratchError("uncommitted scratch leaf exists; ownership is unknown")
    finally:
        for fd in reversed(fds):
            os.close(fd)


def _check_registered_scratch_directory(declaration: Mapping[str, object], identity: object) -> None:
    identities = _scratch_directory_identity(declaration, identity)
    fds = _open_scratch_identity_chain(identities)
    try:
        _check_scratch_permissions(declaration, fds)
    finally:
        for fd in reversed(fds):
            os.close(fd)


def _remove_scratch_contents(fd: int, guard) -> None:
    """Walk only open owned directories; never follow a child symlink/mount."""
    import stat

    with os.scandir(fd) as entries:
        for entry in entries:
            guard()
            info = os.stat(entry.name, dir_fd=fd, follow_symlinks=False)
            if stat.S_ISDIR(info.st_mode):
                child = os.open(entry.name, os.O_RDONLY | os.O_DIRECTORY |
                                os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=fd)
                try:
                    identity = _directory_identity(child, entry.name)
                    _check_directory_link(fd, entry.name, identity)
                    if (identity["dev"] != os.fstat(fd).st_dev
                            or identity["mnt_id"] != _directory_identity(fd, ".")["mnt_id"]):
                        raise scratch_declarations.LocalScratchError("scratch child crosses a filesystem")

                    def child_guard():
                        guard()
                        _check_directory_link(fd, entry.name, identity)

                    _remove_scratch_contents(child, child_guard)
                    child_guard()
                    os.rmdir(entry.name, dir_fd=fd)
                    os.fsync(fd)
                finally:
                    os.close(child)
            else:
                # unlink does not follow symlinks; its parent remains the held
                # owned directory even if an ancestor pathname changes.
                guard()
                os.unlink(entry.name, dir_fd=fd)
                os.fsync(fd)


def _clean_scratch_directory(declaration: Mapping[str, object], identity: object) -> None:
    """Consume one exact directory identity; absent leaf is idempotent.

    Containment and durable outcome are the caller's responsibility. Ancestry
    must still match; a replacement or missing ancestor is never completion.
    """
    checked = scratch_declarations._scratch_declaration(declaration)
    identities = _scratch_directory_identity(checked, identity)
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
    fds = []
    try:
        fds.append(os.open("/", flags))
        if _directory_identity(fds[0], "/") != identities[0]:
            raise scratch_declarations.LocalScratchError("scratch filesystem root identity changed")
        for index, expected in enumerate(identities[1:], 1):
            try:
                fd = os.open(str(expected["name"]), flags, dir_fd=fds[-1])
            except FileNotFoundError:
                if index != len(identities) - 1:
                    raise
                _check_scratch_links(fds, identities)
                _check_scratch_permissions(checked, fds)
                return
            fds.append(fd)
            if _directory_identity(fd, str(expected["name"])) != expected:
                raise scratch_declarations.LocalScratchError("scratch directory identity changed")
        def guard():
            _check_scratch_links(fds, identities)
            _check_scratch_permissions(checked, fds)
        guard()
        _remove_scratch_contents(fds[-1], guard)
        guard()
        os.rmdir(str(identities[-1]["name"]), dir_fd=fds[-2])
        os.fsync(fds[-2])
        _check_scratch_links(fds[:-1], identities)
        try:
            os.stat(str(identities[-1]["name"]), dir_fd=fds[-2], follow_symlinks=False)
        except FileNotFoundError:
            return
        raise scratch_declarations.LocalScratchError("scratch leaf replaced during removal")
    finally:
        for fd in reversed(fds):
            os.close(fd)


def register_attempt(
    queue, action_key: str, *, claim_snapshot: Mapping[str, object], env: Mapping[str, str],
) -> dict[str, object] | None:
    """Commit ownership through the existing claim/lease owner before launch."""
    store = queue.records
    with queue.action_transition_lock(str(action_key)):
        selected = selections(
            scratch_declarations._sealed_scratch_variables(claim_snapshot))
        if not selected:
            return None
        expected = scratch_declarations._scratch_claim_envelope(claim_snapshot)
        live = _scratch_record_value(store.claimed(action_key))
        if expected["action_key"] != action_key or scratch_declarations._scratch_claim_envelope(live) != expected:
            raise scratch_declarations.LocalScratchError("scratch lifetime claim envelope changed")
        claim_identity.check_lease_identity(action_key, live, _scratch_record_value(store.lease(action_key)), invalid=AmbiguousClaimHolder)
        if _scratch_record_value(store.lease(action_key)) is None:
            raise scratch_declarations.LocalScratchError("scratch registration requires the worker lease")
        if (queue.resolve_claim_holder(action_key, live) != socket.gethostname()
                or expected["claimed_host"] != socket.gethostname()
                or contracts.SCRATCH_LIFETIME_TAG not in live.get("tags", [])):
            raise scratch_declarations.LocalScratchError("scratch lifetime writer lacks owner/capability")
        field = SCRATCH_LIFETIME_FIELD
        if field in live:
            evidence = validate_record(live[field])
            if (evidence["claim_envelope"] != expected
                    or _intent(evidence) != selected):
                raise scratch_declarations.LocalScratchError("conflicting scratch lifetime intent")
            if any(entry["cleaned"] for entry in evidence["entries"] if entry["lifetime"] == "ephemeral"):
                raise scratch_declarations.LocalScratchError("scratch lifetime already consumed")
        else:
            entries = []
            for selection in selected:
                persistent = selection["lifetime"] == "persistent"
                entries.append({**selection, "declaration": None if persistent else
                    scratch_declarations.bind_ephemeral_scratch(
                        queue, root_env=selection["root_env"], name=selection["name"],
                        claim_snapshot=live, env=env),
                    "parent_identity": None, "identity": None, "cleaned": persistent})
            evidence = {"schema": SCRATCH_LIFETIME_RECORD_SCHEMA_V1,
                        "claim_envelope": expected, "entries": entries,
                        "registration_complete": False}
        # Intent is durable before ANY creation. Mirrors may lag after a
        # process failure; replay accepts only monotone ownership evidence.
        _persist(store, claim_snapshot, evidence)
        for entry in evidence["entries"]:
            if entry["lifetime"] == "persistent":
                continue
            if entry["parent_identity"] is None:
                entry["parent_identity"] = _prepare_scratch_directory(entry["declaration"])
                _persist(store, claim_snapshot, evidence)
            if entry["identity"] is None:
                entry["identity"] = _create_scratch_directory(
                    entry["declaration"], entry["parent_identity"])
                _persist(store, claim_snapshot, evidence)
            _check_registered_scratch_directory(entry["declaration"], entry["identity"])
        evidence["registration_complete"] = True
        _persist(store, claim_snapshot, evidence)
        return validate_record(evidence)


def durable_owner(store: RecordStore, record: Mapping[str, object], *, scope_only=False,
                            owner_path: Path | None = None) -> tuple[Path, dict[str, object] | None]:
    """Read the exact existing recovery owner, never a caller-only snapshot."""
    key = str(record["action_key"])
    try:
        path, live = store.scratch_owner(record, scope_only=scope_only, owner_path=owner_path)
    except ScratchOwnerConflict as exc:
        raise scratch_declarations.LocalScratchError(str(exc)) from exc
    field = SCRATCH_LIFETIME_FIELD
    if path == store.lease_path(key) and live is not None and field in live:
        envelope = validate_record(live[field])["claim_envelope"]
        for claim_field, lease_field in (("action_key", "action_key"),
                                        ("claimed_by", "owner"), ("claimed_host", "host"),
                                        ("claimed_unix", "claimed_unix"),
                                        ("published_unix", "published_unix")):
            if live.get(lease_field) != envelope[claim_field]:
                raise scratch_declarations.LocalScratchError("scratch lease contradicts its lifetime owner")
        live = {**live, **{k: v for k, v in envelope.items() if k != "owner_attempt"}}
        if scratch_declarations._scratch_claim_envelope(live) != envelope:
            raise scratch_declarations.LocalScratchError("scratch lease scope contradicts its lifetime owner")
    if owner_path is not None and live is not None and field in live:
        envelope = validate_record(live[field])["claim_envelope"]
        if (envelope["action_key"] != key
                or scratch_declarations._scratch_claim_envelope(live) != envelope):
            raise scratch_declarations.LocalScratchError("scratch recovery record contradicts its lifetime owner")
    return path, live


def _persist(store: RecordStore, record: Mapping[str, object], evidence: Mapping[str, object],
                              *, scope_only=False, owner_path: Path | None = None) -> None:
    """Existing key lock held; preserve live/late ownership and heartbeat."""
    field = SCRATCH_LIFETIME_FIELD
    checked = validate_record(evidence)
    if checked["claim_envelope"] != scratch_declarations._scratch_claim_envelope(record):
        raise scratch_declarations.LocalScratchError("scratch lifetime owner changed before persistence")
    key = str(record["action_key"])
    path, live = durable_owner(store,
        record, scope_only=scope_only, owner_path=owner_path)
    if live is None or scratch_declarations._scratch_claim_envelope(live) != checked["claim_envelope"]:
        raise scratch_declarations.LocalScratchError("scratch lifetime durable owner unavailable")
    lease = None if scope_only or path == store.lease_path(key) else _scratch_record_value(store.lease(key))
    if not scope_only:
        claim_identity.check_lease_identity(key, live, lease, invalid=AmbiguousClaimHolder)
    if lease is not None:
        for field_name, identities in (
            ("resource_scope", ("action_key", "nonce", "scope_id")),
            ("resource_scope_intent", ("action_key", "nonce")),
        ):
            if field_name in lease:
                previous, current = lease[field_name], record.get(field_name)
                if (not isinstance(previous, Mapping) or not isinstance(current, Mapping)
                        or any(previous.get(k) != current.get(k) for k in identities)):
                    raise scratch_declarations.LocalScratchError("contradictory scratch lease scope authority")
    previous_records = [owner[field] for owner in (live, lease)
                        if owner is not None and field in owner]
    for previous_record in previous_records:
        previous = validate_record(previous_record)
        if (previous["claim_envelope"] != checked["claim_envelope"]
                or _intent(previous) !=
                _intent(checked)
                or previous["registration_complete"] and not checked["registration_complete"]):
            raise scratch_declarations.LocalScratchError("contradictory scratch lifetime lease")
        for before, after in zip(previous["entries"], checked["entries"]):
            if (any(before[k] is not None and before[k] != after[k]
                    for k in ("parent_identity", "identity"))
                    or before["cleaned"] and not after["cleaned"]):
                raise scratch_declarations.LocalScratchError("scratch lifetime mirror regressed")
    if live.get(field) != checked:
        store.replace_record_at(path, {**live, field: checked})
    if isinstance(record, dict):
        record[field] = checked
    if lease is not None and lease.get(field) != checked:
        store.replace_record_at(store.lease_path(key), {**lease, field: checked})


def finalize(queue, action_key: str, record: Mapping[str, object],
                                 *, export: object, scope_only=False,
                                 owner_path: Path | None = None) -> dict[str, object]:
    """Only stopped attempt descendants permit consuming owned scratch."""
    store = queue.records
    with queue.action_transition_lock(str(action_key)):
        field = SCRATCH_LIFETIME_FIELD
        _, durable = durable_owner(store,
            record, scope_only=scope_only, owner_path=owner_path)
        if durable is not None and claim_identity.same_claim(durable, record):
            if field in durable:
                authoritative = validate_record(durable[field])
                if isinstance(record, dict):
                    record[field] = authoritative
                else:
                    record = {**record, field: authoritative}
            elif field in record:
                raise scratch_declarations.LocalScratchError("scratch cleanup lacks durable registration")
        if field not in record:
            return {"complete": True, "used": False}
        if durable is None or not claim_identity.same_claim(durable, record):
            raise scratch_declarations.LocalScratchError("scratch cleanup durable owner unavailable")
        evidence = validate_record(record[field])
        if (evidence["claim_envelope"] != scratch_declarations._scratch_claim_envelope(record)
                or action_key != record["action_key"]
                or record["claimed_host"] != socket.gethostname()
                or _intent(evidence) !=
                selections(scratch_declarations._sealed_scratch_variables(record))):
            raise scratch_declarations.LocalScratchError("scratch cleanup has a foreign owner or sealed intent")
        if all(entry["cleaned"] for entry in evidence["entries"]):
            _persist(store, record, evidence, scope_only=scope_only, owner_path=owner_path)
            return {"complete": True, "used": True}
        from . import reader_lease
        valid, reason = reader_lease.export_verdict_proves_empty(
            export, scope_id=record["resource_scope"]["scope_id"])
        if not valid:
            # Legacy payload cleanup can complete without a reader export.
            # A later scratch retry must re-query this exact stopped scope,
            # not permanently trust that legacy shortcut or infer emptiness.
            scope = queue.scope_for_record(record)
            export = scope.export_stopped_verdict()
            valid, reason = reader_lease.export_verdict_proves_empty(export, scope_id=scope.unit)
        if not valid:
            raise scratch_declarations.LocalScratchError(f"scratch containment unproven: {reason}")
        # Verify durable owner/mirror BEFORE deletion, including late attempts.
        _persist(store, record, evidence, scope_only=scope_only, owner_path=owner_path)
        for entry in evidence["entries"]:
            if entry["cleaned"]:
                continue
            if entry["identity"] is None:
                _uncreated_scratch_directory(
                    entry["declaration"], entry["parent_identity"])
            else:
                _clean_scratch_directory(entry["declaration"], entry["identity"])
            entry["cleaned"] = True
            # Crash before this commit re-proves absence through held parent
            # identities. Crash after it never revisits the consumed path.
            _persist(store, record, evidence, scope_only=scope_only, owner_path=owner_path)
        return {"complete": True, "used": True}


def _scratch_record_value(result):
    match result:
        case Present(value):
            return value
        case Absent():
            return None
        case Unknown(error=error) if error is not None:
            raise error
        case Unknown(reason=reason):
            raise scratch_declarations.LocalScratchError(reason)
