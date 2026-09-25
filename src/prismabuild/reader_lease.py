"""Reader leases: pin staged bytes for the whole of a reader's lifetime.

A residency-map fragment says "these bytes are staged"; it does not say who
is reading them.  The egress therefore deletes on ownership (fragments plus
in-flight claims) and has always been blind to the third party: a consumer
that already opened the file, a prefetcher ahead of compute, an mmap still
mapped, a RAM promotion mid-copy reading its source leg.  A lease is that
third party made visible: one small pin file per staged window, written
atomically beside the fragments, consulted by every egress and
reconciliation before anything is unlinked.

Lifetime, in one paragraph: ``acquire`` proves the requested window is
covered by published material (fragments plus publish-time sidecars),
checks the tier epoch and every file's portable identity under the stage
root's ownership lock, and appends one ref to the window's pin; the holder
reads through ``open_pinned``, which checks the descriptor it will actually
read (never stat-then-open); ``release`` drops exactly its own ref,
independent of compute progress; the egress defers to any live ref, marks
the range retiring (closed to new acquires for that material generation),
and deletes only after the last release.  A retry mints a new acquire token
and gets a new ref; an old attempt's ref goes only by explicit release or by
certified containment -- a stale timestamp alone frees nothing.

Object identity (portable across clients): a staged object is
``(tier namespace, epoch, stage path, length, materialization generation,
content digest, backend identity)``.  The backend identity binds
``(ino, size, mtime_ns, ctime_ns)`` -- server-side fields that travel with
the file -- and never ``st_dev``, which is per-client on NFS (observed
64-vs-75 for one file).  The client-local device is a local change fence
only.  The materialization generation is minted per publish run (uuid4):
a retry of the same mover republishes under the same key with a new
generation, so ``mover_action_key`` alone is never the generation and a
same-epoch/path/length republish with different bytes can never ABA-alias
an old pin.  Nothing here hashes payload bytes beyond the copy-time digest
and no whole-model hash exists anywhere.

Membership coordination (host JOIN/RESIGN): this module owns no reaper and
frees nothing on its own.  :func:`refs_for_holder` is the containment
census; :func:`write_scope_attestation` is the broker's proof input;
:func:`containment_certificate_ok` verifies identifiers against those
authoritative records; :func:`release_refs` additionally requires that
certificate -- the attempt's terminal record plus the host broker's
attestation that the scope's processes all stopped.  RESIGN waits for actual
reader lifetimes/scopes (live refs drain, or certified containment after
proven stop) before releasing resources.  Liveness is never guessed from
``kill(0)``, ``/proc`` on another host, heartbeats, or timestamps: unknown
evidence retains the charge with a reason.  No independent reaper or release
path may be built beside this one; pool residency/lifetime ownership stays
here.

Capability: ``reader-lease-v1`` (worker announce tag, same channel as
``progress-v1``/``progress-helper-v1``) is advertised only after this
behavior is qualified and deployed -- never before.  Strict campaigns
require pins (unqualified material refuses instead of falling open);
legacy material without sidecars stays usable only outside strict
capability.
"""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Mapping
from dataclasses import dataclass
import errno
import hashlib
import json
import math
import os
from pathlib import Path
import re
import socket
import stat
import sys
import threading
import time
from typing import NamedTuple
import uuid

#: Worker announce tag for this behavior; advertised only once qualified.
READER_LEASE_TAG = "reader-lease-v1"
#: What one window's pin looks like on disk.
LEASE_SCHEMA_V1 = "prismaquant.prismabuild.reader_pin.v1"
#: What an egress files when it defers to live refs: closed to new acquires
#: for the named material generation only.
RETIRING_SCHEMA_V1 = "prismaquant.prismabuild.reader_retiring.v2"
#: What a mover files at publish beside its fragment: publish-time identity.
MATERIAL_SCHEMA_V1 = "prismaquant.prismabuild.reader_material.v1"
#: Beside the fragments, under ``<pool-root>/residency``: one writer per file.
LEASES_SUBDIR = "leases"
#: Publish-time sidecars live here, never among the fragments (the
#: fragment-owner scan taints on any ``*.json`` it cannot validate).
MATERIAL_SUBDIR = "material"

_HEX = frozenset("0123456789abcdef")
#: ``_HEX`` checked at C speed: ``[0-9a-f]`` is a literal ASCII range, so a
#: full match accepts exactly the strings whose every character is in
#: ``_HEX`` (#893: the per-character generator dominated cover lookups).
_HEX_RUN = re.compile("[0-9a-f]*")


class ReaderLeaseError(ValueError):
    """A pin, a retiring mark, material, or a pinned open that refuses."""


class TierAnnouncementUnreadable(ReaderLeaseError):
    """A RAM tier's announcement could not be read, so its epoch is unknown (#1146).

    Raised by :func:`open_pinned` when ``tiers/<tier_id>.json`` still does
    not read after the bounded retries of :func:`_announced_epoch`: a stale
    NFS handle, an I/O error or timeout, or bytes that do not parse.  It is
    *not* evidence that the epoch moved, and its message never says so; a
    readable announcement with another epoch refuses with "epoch moved
    during the hold" instead.

    It subclasses :class:`ReaderLeaseError` on purpose: the open still
    refuses, because an epoch that cannot be checked cannot be served
    under, and every caller that already fails closed on a
    ``ReaderLeaseError`` keeps doing so.  A consumer that must tell the
    two apart tests for this type (or its name).  The pin and the opening
    ref stay held; nothing is released, so the caller may retry the open
    or release as it chooses.

    ``path`` is the announcement, ``errno`` the last ``OSError``'s errno
    (``None`` for a parse failure or a refusal without one), ``error`` the
    last failure as ``"<type>: <message>"``, ``seconds`` the time spent
    reading, and ``attempts`` the reads made.
    """

    def __init__(self, *, path: str, errno: int | None, error: str,
                 seconds: float, attempts: int) -> None:
        self.path = path
        self.errno = errno
        self.error = error
        self.seconds = seconds
        self.attempts = attempts
        code = _errno_name(errno)
        super().__init__(
            f"tier announcement {path!r} unreadable after {attempts} "
            f"read(s) in {seconds:.3f} s"
            f"{f' ({code})' if code else ''}: {error}; the announced epoch "
            f"is unknown, not moved: refusing")


# --------------------------------------------------------------------------
# Portable file identity
# --------------------------------------------------------------------------

def portable_identity(info) -> dict[str, int]:
    """Server-side identity of a stat result: ``ino/size/mtime_ns/ctime_ns``.

    ``st_dev`` is deliberately excluded: it is per-client on NFS, so two
    hosts stat the same file and disagree.  The local device remains useful
    as a same-host change fence, but it is never part of a cross-host pin.
    """

    return {"ino": int(info.st_ino), "size": int(info.st_size),
            "mtime_ns": int(info.st_mtime_ns),
            "ctime_ns": int(getattr(info, "st_ctime_ns", 0))}


def stat_identity(path: str) -> dict[str, int] | None:
    """Portable identity of the file at ``path``; ``None`` when unstatable."""

    try:
        return portable_identity(os.stat(path))
    except OSError:
        return None


def file_id_matches(published: object, live: object) -> bool:
    """Whether a recorded ``file_id`` is the identity the file has now.

    The one comparison every strict reader, every publication proof and
    every adoption verification makes: a material entry's recorded identity
    against a live ``stat_identity`` of the same name, field by field,
    ignoring anything else either dict carries.  All four fields must be
    present on both sides before any equality counts -- ``{}`` against
    ``{}`` is vacuously equal and still not a match.  ``False`` for
    anything unreadable -- an unstatable file or a malformed record is a
    mismatch, never a pass (#755).
    """

    fields = ("ino", "size", "mtime_ns", "ctime_ns")
    if not isinstance(published, Mapping) or not isinstance(live, Mapping):
        return False
    if any(field not in published or field not in live for field in fields):
        return False
    return all(published[field] == live[field] for field in fields)


def timestamp_only_mismatch(published: object, live: object) -> bool:
    """Whether two identities are one file whose timestamps alone moved (#1096).

    ``True`` only when both carry all four fields, ``ino`` and ``size`` are
    equal, and ``mtime_ns`` or ``ctime_ns`` differs.  This is the mismatch an
    NFS delegation recall makes: the server applies the writer's delegated
    timestamps after the writer recorded the file.  It is never a match by
    itself: a caller that holds the file's recorded sha256 may settle it by
    content (:func:`content_identity`), and every other caller refuses it as
    :func:`file_id_matches` does.
    """

    fields = ("ino", "size", "mtime_ns", "ctime_ns")
    if not isinstance(published, Mapping) or not isinstance(live, Mapping):
        return False
    if any(field not in published or field not in live for field in fields):
        return False
    return (published["ino"] == live["ino"] and published["size"] == live["size"]
            and (published["mtime_ns"] != live["mtime_ns"]
                 or published["ctime_ns"] != live["ctime_ns"]))


#: One read of :func:`content_identity`'s loop.
CONTENT_BLOCK_BYTES = 1 << 20


def content_identity(name: str | os.PathLike, *, dir_fd: int | None = None
                     ) -> dict[str, object]:
    """Hash one regular file and name the identity the hash describes (#1096).

    The file is opened without following a link, relative to ``dir_fd``
    when one is given.  Its identity is read from the open descriptor before
    and after the read, and all four fields must be equal, so the digest
    describes exactly that identity.  One read may race a delegation recall,
    which moves the timestamps while the bytes stay the same, so a read whose
    identity moved is taken once more; a second move raises.  A file that is
    not regular, or whose length is not its size, raises too.

    Returns ``{"sha256", "identity", "bytes", "seconds", "reads"}``.
    Raises :class:`ReaderLeaseError` or ``OSError``.
    """

    started = time.monotonic()
    moved = None
    for reads in (1, 2):
        fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC,
                     dir_fd=dir_fd)
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode):
                raise ReaderLeaseError(f"{name}: not a regular file")
            before = portable_identity(info)
            digest = hashlib.sha256()
            buffer = bytearray(CONTENT_BLOCK_BYTES)
            view = memoryview(buffer)
            count = 0
            while got := os.readv(fd, [buffer]):
                digest.update(view[:got])
                count += got
            after = portable_identity(os.fstat(fd))
        finally:
            os.close(fd)
        if count != before["size"]:
            raise ReaderLeaseError(
                f"{name}: read {count} bytes of a {before['size']}-byte file")
        if before == after:
            return {"sha256": digest.hexdigest(), "identity": after, "bytes": count,
                    "seconds": round(time.monotonic() - started, 6), "reads": reads}
        moved = (before, after)
    raise ReaderLeaseError(f"{name}: changed while it was read twice: "
                           f"{moved[0]!r} became {moved[1]!r}")


def _check_identity(value: object, *, where: str) -> dict[str, int]:
    if not isinstance(value, Mapping):
        raise ReaderLeaseError(f"{where} must be an object")
    out: dict[str, int] = {}
    for field in ("ino", "size", "mtime_ns", "ctime_ns"):
        item = value.get(field)
        if isinstance(item, bool) or type(item) is not int or item < 0:
            raise ReaderLeaseError(f"{where} needs non-negative int {field!r}")
        out[field] = item
    return out


# --------------------------------------------------------------------------
# Material sidecars (mover-published, PB-owned, map schema untouched)
# --------------------------------------------------------------------------

def material_path(root: str | Path, consumer_action_key: str,
                  mover_action_key: str) -> Path:
    """``<root>/material/<consumer>/<mover>.json`` -- beside, never among.

    A separate directory (not the consumer's fragment directory) on purpose:
    the egress's fragment-owner scan taints on any ``*.json`` it cannot
    validate, so a sidecar filed among the fragments would wedge every
    egress fail-closed.  One writer per file is preserved by construction.
    """

    return (Path(root) / MATERIAL_SUBDIR / consumer_action_key
            / f"{mover_action_key}.json")


def mint_generation() -> str:
    """One publish run's materialization generation: never reused, never ABA."""

    return uuid.uuid4().hex


def write_material(root: str | Path, *, consumer_action_key: str,
                   mover_action_key: str, tier_id: str, stage_root: str,
                   manifest_sha256: str, generation: str,
                   entries: Mapping[str, Mapping[str, object]],
                   epoch: str | None = None) -> Path:
    """File one mover's publish-time identity, atomically (mover calls this).

    ``entries`` maps residency-map key -> ``{stage_path, bytes, sha256,
    file_id}`` where ``file_id`` is :func:`stat_identity` taken at rename.
    The map fragment (schema v1, unchanged) keeps vouching; this sidecar
    dates the vouching.  Rewriting under a retry mints a new ``generation``.
    """

    # Exact sidecar convention (Q4): SSD material carries no epoch
    # (absent or ""); only the RAM tier dates its sidecars.  A staged
    # epoch is corrupt at write time, never discovered at open.
    if not tier_id.startswith("ram:") and epoch:
        raise ReaderLeaseError(
            "staged material must not carry an epoch")
    body: dict[str, object] = {
        "schema": MATERIAL_SCHEMA_V1,
        "consumer_action_key": consumer_action_key,
        "mover_action_key": mover_action_key,
        "tier_id": tier_id,
        "stage_root": stage_root,
        "manifest_sha256": manifest_sha256,
        "generation": generation,
        "entries": {str(key): {
            "stage_path": str(entry["stage_path"]),
            "bytes": int(entry["bytes"]),  # type: ignore[arg-type]
            "sha256": str(entry["sha256"]),
            "file_id": _check_identity(
                entry["file_id"],
                where=f"material entry {key!r} file_id"),
        } for key, entry in entries.items()},
    }
    if epoch is not None:
        body["epoch"] = epoch
    checked = validate_material(body)
    path = material_path(root, consumer_action_key, mover_action_key)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with open(tmp, "w") as stream:
        json.dump(checked, stream, sort_keys=True)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(tmp, path)
    return path


def _hex(value: object, length: int, *, where: str) -> str:
    if (not isinstance(value, str) or len(value) != length
            or _HEX_RUN.fullmatch(value) is None):
        raise ReaderLeaseError(f"{where} must be {length} lowercase hex")
    return value


def validate_material(value: object) -> dict[str, object]:
    """Check a material sidecar with fragment-level strictness."""

    if not isinstance(value, Mapping):
        raise ReaderLeaseError("reader material must be an object")
    unknown = sorted(set(value) - {
        "schema", "consumer_action_key", "mover_action_key", "tier_id",
        "stage_root", "manifest_sha256", "generation", "entries", "epoch",
    })
    if unknown:
        raise ReaderLeaseError(f"unknown reader material fields: {unknown}")
    if value.get("schema") != MATERIAL_SCHEMA_V1:
        raise ReaderLeaseError(f"material schema must be {MATERIAL_SCHEMA_V1!r}")
    entries = value.get("entries")
    if not isinstance(entries, Mapping) or not entries:
        raise ReaderLeaseError("material entries must be a non-empty object")
    checked_entries: dict[str, dict[str, object]] = {}
    for key, entry in entries.items():
        if not isinstance(entry, Mapping):
            raise ReaderLeaseError("material entry must be an object")
        unknown_entry = sorted(set(entry) - {
            "stage_path", "bytes", "sha256", "file_id"})
        if unknown_entry:
            raise ReaderLeaseError(
                f"unknown material entry fields: {unknown_entry}")
        stage_path = entry.get("stage_path")
        if (not isinstance(stage_path, str) or not stage_path.startswith("/")
                or stage_path != os.path.normpath(stage_path)):
            raise ReaderLeaseError("material entry stage_path must be normalized absolute")
        size = entry.get("bytes")
        if isinstance(size, bool) or type(size) is not int or size <= 0:
            raise ReaderLeaseError("material entry bytes must be positive")
        checked_entries[str(key)] = {
            "stage_path": stage_path,
            "bytes": size,
            "sha256": _hex(entry.get("sha256"), 64,
                           where=f"material entry {key!r} sha256"),
            "file_id": _check_identity(
                entry.get("file_id"),
                where=f"material entry {key!r} file_id"),
        }
    epoch = value.get("epoch")
    if epoch is not None and (not isinstance(epoch, str) or "/" in epoch):
        raise ReaderLeaseError("material epoch must be a string with no '/'")
    out: dict[str, object] = {
        "schema": MATERIAL_SCHEMA_V1,
        "consumer_action_key": _hex(
            value.get("consumer_action_key"), 64,
            where="material consumer_action_key"),
        "mover_action_key": _hex(
            value.get("mover_action_key"), 64,
            where="material mover_action_key"),
        "tier_id": str(value.get("tier_id") or ""),
        "stage_root": str(value.get("stage_root") or ""),
        "manifest_sha256": _hex(
            value.get("manifest_sha256"), 64,
            where="material manifest_sha256"),
        "generation": _hex(value.get("generation"), 32,
                           where="material generation"),
        "entries": checked_entries,
    }
    if epoch is not None:
        out["epoch"] = epoch
    if not out["tier_id"] or "/" in str(out["tier_id"]):
        raise ReaderLeaseError("material tier_id must be a tier id")
    return out


def read_material(root: str | Path, consumer_action_key: str,
                  mover_action_key: str):
    """A mover's material sidecar, or ``None`` (absent), or the error (taint)."""

    try:
        with open(material_path(root, consumer_action_key,
                                mover_action_key)) as stream:
            return validate_material(json.load(stream))
    except FileNotFoundError:
        return None
    except (OSError, ValueError) as exc:
        return exc


# --------------------------------------------------------------------------
# Pin files with refcounted refs
# --------------------------------------------------------------------------

def leases_root(queue, residency_root=None) -> Path:
    """``<residency>/leases`` -- pins and retiring marks live here."""

    if residency_root is not None:
        return Path(residency_root) / LEASES_SUBDIR
    return Path(queue.root) / "residency" / LEASES_SUBDIR


def _canonical_pin_body(*, consumer_action_key: str, tier_id: str,
                        epoch: str, stage_root: str, start: int, end: int,
                        movers: list[str], generations: Mapping[str, str],
                        keys: Mapping[str, Mapping[str, object]]) -> str:
    """Canonical structured encoding of a pin's identity (JSON, not grammar).

    Paths, digests and generations travel as JSON strings inside a fixed
    structure, so no delimiter character occurring in a path can merge or
    split fields the way a private ``|/,/=`` grammar risks.  The digest
    of this exact byte string names the pin file.
    """

    return json.dumps({
        "consumer": consumer_action_key,
        "tier": tier_id,
        "epoch": epoch,
        "stage_root": stage_root,
        "range": [start, end],
        "movers": sorted(movers),
        "generations": {mover: generations[mover]
                        for mover in sorted(movers)},
        "objects": sorted(
            ({"key": key,
              "bytes": keys[key].get("bytes"),
              "sha256": keys[key].get("sha256"),
              "generation": keys[key].get("generation")}
             for key in keys),
            key=lambda item: str(item["key"])),
    }, sort_keys=True, separators=(",", ":"), allow_nan=False)


def pin_id_for(*, consumer_action_key: str, tier_id: str, epoch: str,
               stage_root: str, start: int, end: int, movers: list[str],
               generations: Mapping[str, str],
               keys: Mapping[str, Mapping[str, object]]) -> str:
    """Deterministic pin name for a window generation AND object keyset.

    The requested object keyset joins the name: two equal-sized distinct
    files under one mover (both offset 0 in source coordinates) must never
    share a pin, or the second acquire would append a ref to entries that
    do not name its bytes.  Canonical form per key is the structured
    ``{key, bytes, sha256, generation}`` object above; physical identity
    (inode/mtime) is enforced at open, not hashed here.  One file per
    keyset, many refs.
    """

    return hashlib.sha256(
        _canonical_pin_body(
            consumer_action_key=consumer_action_key, tier_id=tier_id,
            epoch=epoch, stage_root=stage_root, start=start, end=end,
            movers=movers, generations=generations,
            keys=keys).encode("utf-8")).hexdigest()[:32]


def _entry_identity(entries: list[Mapping[str, object]]) -> list[tuple[str, ...]]:
    """Canonical per-entry identity for same-window comparison."""

    return sorted(
        (str(entry.get("key") or ""),
         str(entry.get("stage_path") or ""),
         str(entry.get("bytes") or ""),
         str(entry.get("sha256") or ""),
         str(entry.get("mover_action_key") or ""),
         str(entry.get("generation") or ""))
        for entry in entries)


def ref_id_for(*, acquire_token: str, host: str, nonce: str,
               scope_id: str) -> str:
    """One logical acquisition's ref: retrying the token reuses the ref."""

    body = "|".join((acquire_token, host, nonce, scope_id))
    return hashlib.sha256(body.encode("utf-8")).hexdigest()[:32]


def validate_pin(value: object) -> dict[str, object]:
    """Check a pin file: window identity plus exactly its live refs."""

    if not isinstance(value, Mapping):
        raise ReaderLeaseError("a reader pin must be an object")
    unknown = sorted(set(value) - {
        "schema", "pin_id", "consumer_action_key", "owner_action_key",
        "tier_id", "epoch",
        "manifest_sha256", "range", "covers", "entries", "ram", "refs",
        "stage_root",
    })
    if unknown:
        raise ReaderLeaseError(f"unknown reader pin fields: {unknown}")
    if value.get("schema") != LEASE_SCHEMA_V1:
        raise ReaderLeaseError(f"pin schema must be {LEASE_SCHEMA_V1!r}")
    refs = value.get("refs")
    if not isinstance(refs, Mapping) or not refs:
        raise ReaderLeaseError("pin refs must be a non-empty object")
    checked_refs: dict[str, dict[str, object]] = {}
    for ref_id, ref in refs.items():
        if not isinstance(ref_id, str) or not ref_id or "/" in ref_id:
            raise ReaderLeaseError("pin ref id must be a non-empty name")
        if not isinstance(ref, Mapping):
            raise ReaderLeaseError("pin ref must be an object")
        unknown_ref = sorted(set(ref) - {
            "acquire_token", "attempt", "holder", "unix"})
        if unknown_ref:
            raise ReaderLeaseError(f"unknown pin ref fields: {unknown_ref}")
        attempt = ref.get("attempt")
        if not isinstance(attempt, Mapping):
            raise ReaderLeaseError("pin ref attempt must be an object")
        for field in ("nonce", "scope_id"):
            if not isinstance(attempt.get(field), str):
                raise ReaderLeaseError(f"pin ref attempt needs str {field!r}")
        holder = ref.get("holder")
        if not isinstance(holder, Mapping) or not holder.get("host"):
            raise ReaderLeaseError("pin ref holder needs a host")
        token = ref.get("acquire_token")
        if not isinstance(token, str) or not token:
            raise ReaderLeaseError("pin ref needs an acquire_token")
        checked_refs[ref_id] = {
            "acquire_token": token,
            "attempt": {"nonce": str(attempt.get("nonce")),
                        "scope_id": str(attempt.get("scope_id"))},
            "holder": dict(holder),
            "unix": ref.get("unix"),
        }
    entries = value.get("entries")
    if not isinstance(entries, list) or not entries:
        raise ReaderLeaseError("pin entries must be a non-empty array")
    checked_entries: list[dict[str, object]] = []
    for entry in entries:
        if not isinstance(entry, Mapping):
            raise ReaderLeaseError("pin entry must be an object")
        unknown_entry = sorted(set(entry) - {
            "key", "stage_path", "bytes", "sha256", "file_id",
            "mover_action_key", "generation"})
        if unknown_entry:
            raise ReaderLeaseError(
                f"unknown pin entry fields: {unknown_entry}")
        stage_path = entry.get("stage_path")
        if (not isinstance(stage_path, str) or not stage_path.startswith("/")
                or stage_path != os.path.normpath(stage_path)):
            raise ReaderLeaseError("pin entry stage_path must be normalized absolute")
        size = entry.get("bytes")
        if isinstance(size, bool) or type(size) is not int or size <= 0:
            raise ReaderLeaseError("pin entry bytes must be positive")
        checked_entries.append({
            "key": str(entry.get("key") or ""),
            "stage_path": stage_path,
            "bytes": size,
            "sha256": _hex(entry.get("sha256"), 64,
                           where="pin entry sha256"),
            "file_id": _check_identity(
                entry.get("file_id"), where="pin entry file_id"),
            "mover_action_key": _hex(
                entry.get("mover_action_key"), 64,
                where="pin entry mover_action_key"),
            "generation": _hex(
                entry.get("generation"), 32,
                where="pin entry generation"),
        })
    covers = value.get("covers")
    if not isinstance(covers, list) or not covers:
        raise ReaderLeaseError("pin covers must be a non-empty array")
    for cover in covers:
        if not isinstance(cover, Mapping):
            raise ReaderLeaseError("pin cover must be an object")
        _hex(cover.get("mover_action_key"), 64, where="pin cover mover")
        _hex(cover.get("generation"), 32, where="pin cover generation")
    span = value.get("range")
    if not isinstance(span, Mapping):
        raise ReaderLeaseError("pin range must be an object")
    # Coordinate space is machine-checked: the span is RNG-01
    # declared-file coordinates ([offset, offset+bytes)); split staged
    # objects live at FD offset 0 (RNG-02); no logical phase cursor
    # (RNG-03) is ever inferred from this span.  Multi-entry windows
    # carry unambiguous per-key ranges via their map keys, never one
    # cumulative source offset.
    if span.get("coordinate_space") != "rng01-source":
        raise ReaderLeaseError(
            "pin range must declare coordinate_space 'rng01-source'")
    stage_root = value.get("stage_root")
    if (not isinstance(stage_root, str) or not stage_root.startswith("/")
            or stage_root != os.path.normpath(stage_root)):
        raise ReaderLeaseError("pin stage_root must be a normalized absolute path")
    return {
        "schema": LEASE_SCHEMA_V1,
        "pin_id": str(value.get("pin_id") or ""),
        "consumer_action_key": _hex(
            value.get("consumer_action_key"), 64,
            where="pin consumer_action_key"),
        "owner_action_key": _hex(
            value.get("owner_action_key"), 64,
            where="pin owner_action_key"),
        "tier_id": str(value.get("tier_id") or ""),
        "epoch": str(value.get("epoch") or ""),
        "stage_root": stage_root,
        "manifest_sha256": str(value.get("manifest_sha256") or ""),
        "range": {"coordinate_space": "rng01-source",
                  "start_bytes": span.get("start_bytes"),
                  "end_bytes": span.get("end_bytes")},
        "covers": [dict(cover) for cover in covers],
        "entries": checked_entries,
        "ram": value.get("ram"),
        "refs": checked_refs,
    }


def retiring_path(root: str | Path, consumer_action_key: str,
                  mover_action_key: str) -> Path:
    """``<leases>/<consumer>/<mover>.retiring.json`` -- bound to a generation."""

    return (Path(root) / consumer_action_key / f"{mover_action_key}.retiring.json")


def validate_retiring(value: object) -> dict[str, object]:
    """A retiring mark names the exact material generation it closes."""

    if not isinstance(value, Mapping):
        raise ReaderLeaseError("a retiring mark must be an object")
    unknown = sorted(set(value) - {
        "schema", "consumer_action_key", "mover_action_key", "generation",
        "unix",
    })
    if unknown:
        raise ReaderLeaseError(f"unknown retiring mark fields: {unknown}")
    if value.get("schema") != RETIRING_SCHEMA_V1:
        raise ReaderLeaseError(
            f"retiring schema must be {RETIRING_SCHEMA_V1!r}")
    return {
        "schema": RETIRING_SCHEMA_V1,
        "consumer_action_key": _hex(
            value.get("consumer_action_key"), 64,
            where="retiring consumer_action_key"),
        "mover_action_key": _hex(
            value.get("mover_action_key"), 64,
            where="retiring mover_action_key"),
        "generation": _hex(value.get("generation"), 32,
                           where="retiring generation"),
        "unix": value.get("unix"),
    }


def retiring_for(root: str | Path, mover_action_key: str
                 ) -> tuple[list[dict[str, object]], list[str]]:
    """Valid retiring marks naming this mover, plus taint on malformed ones.

    Malformed marks fail closed (the caller taints) but never wedge future
    generations: a mark binds one generation, and recovery unlinks marks
    whose generation no live material carries (see :func:`clear_retiring`).
    """

    base = Path(root)
    marks: list[dict[str, object]] = []
    tainted: list[str] = []
    try:
        consumers = sorted(entry.name for entry in os.scandir(base)
                           if entry.is_dir())
    except OSError:
        return marks, []
    name = f"{mover_action_key}.retiring.json"
    for consumer in consumers:
        try:
            with open(base / consumer / name) as stream:
                raw = json.load(stream)
        except FileNotFoundError:
            continue
        except (OSError, ValueError) as exc:
            tainted.append(f"{consumer}/{name}: {exc}")
            continue
        try:
            marks.append(validate_retiring(raw))
        except ReaderLeaseError as exc:
            tainted.append(f"{consumer}/{name}: {exc}")
    return marks, tainted


def write_retiring(root: str | Path, *, consumer_action_key: str,
                   mover_action_key: str, generation: str) -> Path:
    """Close one material generation to new acquires; live refs drain first."""

    path = retiring_path(Path(root), consumer_action_key, mover_action_key)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"schema": RETIRING_SCHEMA_V1,
               "consumer_action_key": consumer_action_key,
               "mover_action_key": mover_action_key,
               "generation": generation,
               "unix": time.time()}
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with open(tmp, "w") as stream:
        json.dump(validate_retiring(payload), stream, sort_keys=True)
        stream.write("\n")
    os.replace(tmp, path)
    return path


def clear_retiring(root: str | Path, *, consumer_action_key: str,
                   mover_action_key: str) -> None:
    """Drop a retiring mark after its delete, or when its generation is gone.

    Supported recovery for stale/malformed marks: called by the egress once
    no live material carries the mark's generation, and by operators only
    after verifying no pin names that generation.  Never unpins a live ref.
    """

    retiring_path(Path(root), consumer_action_key,
                  mover_action_key).unlink(missing_ok=True)


# --------------------------------------------------------------------------
# Pin census (egress/reconcile read this, one walk, string intersection)
# --------------------------------------------------------------------------

def live_for(queue, wanted: set[str] | None, *, residency_root=None,
             memo: dict[str, object] | None = None,
             ) -> tuple[dict[str, list[str]], list[str]]:
    """Which of ``wanted`` paths a live pin refs (``None``: all pinned paths).

    Returns ``(owners, tainted)`` mapping normalized stage path to pin ids.
    Retiring marks and ``*.tmp`` temporaries are never pins.  Anything else
    unreadable or invalid taints the pass: its paths are unknowable, so the
    egress deletes nothing and frees nothing on that pass.

    ``memo`` is a caller-owned dict that lets a second census in the same
    call skip re-parsing a pin whose file is unchanged (#988).  Every pin
    file is still listed and opened; only a pin whose ``fstat`` version
    (device, inode, size, mtime and ctime) equals the one its parse was
    taken under reuses that parse.  A pin gains a ref by an atomic rewrite,
    which is a new inode, so a new ref is never hidden.  A read that failed
    is never remembered.
    """

    root = leases_root(queue, residency_root)
    owners: dict[str, list[str]] = {}
    tainted: list[str] = []
    try:
        consumers = sorted(entry.name for entry in os.scandir(root)
                           if entry.is_dir())
    except FileNotFoundError:
        return owners, []  # no pins ever filed: genuinely empty
    except OSError as exc:
        # Unknown absence: taint, never a clean empty census.
        return owners, [f"{root}: {exc}"]
    for consumer in consumers:
        directory = root / consumer
        try:
            names = sorted(entry.name for entry in os.scandir(directory)
                           if entry.is_file() and entry.name.endswith(".json"))
        except FileNotFoundError:
            continue  # drained between scan and read: genuinely gone
        except OSError as exc:
            tainted.append(f"{consumer}: {exc}")
            continue
        for name in names:
            if name.endswith(".retiring.json") or name.endswith(".tmp"):
                continue
            if not name.endswith(".lease.json"):
                tainted.append(f"{consumer}/{name}: not a pin file")
                continue
            key = str(directory / name)
            try:
                with open(directory / name) as stream:
                    remembered = None
                    if memo is not None:
                        info = os.fstat(stream.fileno())
                        version = (info.st_dev, info.st_ino, info.st_size,
                                   info.st_mtime_ns,
                                   int(getattr(info, "st_ctime_ns", 0)))
                        hit = memo.get(key)
                        if isinstance(hit, tuple) and hit[0] == version:
                            remembered = hit[1], hit[2]
                    if remembered is None:
                        pin = validate_pin(json.load(stream))
            except (OSError, ValueError) as exc:
                if memo is not None:
                    memo.pop(key, None)
                tainted.append(f"{consumer}/{name}: {exc}")
                continue
            if remembered is None:
                pin_id = str(pin["pin_id"])
                entries = pin["entries"]
                assert isinstance(entries, list)
                named: list[str] = []
                for entry in entries:
                    assert isinstance(entry, dict)
                    named.append(os.path.normpath(str(entry["stage_path"])))
                if memo is not None:
                    memo[key] = (version, pin_id, tuple(named))
            else:
                pin_id, named = remembered
            for path in named:
                if wanted is not None and path not in wanted:
                    continue
                known = owners.setdefault(path, [])
                if pin_id not in known:
                    known.append(pin_id)
    return owners, tainted


def refs_for_holder(queue, host: str, *, residency_root=None
                    ) -> list[dict[str, object]]:
    """Census of one host's refs -- the containment input, read-only.

    The membership worker's RESIGN path calls this to learn what must drain.
    Never a reaper trigger on its own; PIDs named here are diagnostics, not
    proof of anything across hosts.

    Only proven absence returns empty: a missing leases namespace (never
    created) or a namespace whose pins all parse with no ref for this
    host.  An unreadable root or owner directory, or an unparseable pin,
    raises (OSError / ValueError) instead of reading as drained -- the
    membership gate catches both into unknown-retain.  No ref for another
    host, and no ref at all, is ever inferred from a file this function
    could not read.
    """

    root = leases_root(queue, residency_root)
    out: list[dict[str, object]] = []
    try:
        consumers = sorted(entry.name for entry in os.scandir(root)
                           if entry.is_dir())
    except FileNotFoundError:
        return out  # no leases namespace ever created: genuinely drained
    for consumer in consumers:
        directory = root / consumer
        try:
            names = sorted(entry.name for entry in os.scandir(directory)
                           if entry.is_file() and entry.name.endswith(".lease.json"))
        except FileNotFoundError:
            continue  # drained between scan and read: genuinely gone
        for name in names:
            try:
                with open(directory / name) as stream:
                    pin = validate_pin(json.load(stream))
            except FileNotFoundError:
                continue  # unlinked between scan and read: genuinely gone
            refs = pin["refs"]
            assert isinstance(refs, dict)
            for ref_id, ref in refs.items():
                assert isinstance(ref, dict)
                holder = ref["holder"]
                assert isinstance(holder, dict)
                if holder.get("host") != host:
                    continue
                out.append({"consumer_action_key": consumer,
                            "pin_id": str(pin["pin_id"]),
                            "ref_id": ref_id,
                            "path": str(directory / name),
                            "ref": ref})
    return out


# --------------------------------------------------------------------------
# Containment certificates: terminal record plus broker attestation
# --------------------------------------------------------------------------

# --------------------------------------------------------------------------
# Broker scope attestations and containment certificates
#
# The host broker (worker_loop/supervise side, coordinated with the
# membership worker's RESIGN path) is the only party that can prove a
# scope's processes all stopped: it owns the container/cgroup.  It files
# one attestation per attempt at a queue-controlled path.  Verification
# below reads that file (authoritative) plus the attempt's terminal
# record/history (authoritative); the caller supplies identifiers only.
# A caller-supplied "scope_empty: true" is never proof.  Unknown evidence
# retains the charge with a reason -- no kill(0), no cross-host /proc, no
# heartbeat expiry, no timestamp expiry.
# --------------------------------------------------------------------------

#: Broker-written per-attempt scope proof, read by containment only.
ATTESTATION_SCHEMA_V1 = "prismaquant.prismabuild.reader_scope_attestation.v1"
ATTESTATIONS_SUBDIR = "broker-attestations"


def attestation_path(queue, action_key: str, nonce: str) -> Path:
    """``<queue>/broker-attestations/<action>/<nonce>.json`` (broker writes)."""

    return (Path(queue.root) / ATTESTATIONS_SUBDIR / action_key
            / f"{nonce}.json")


def read_scope_attestation(queue, action_key: str, nonce: str):
    """A broker attestation, or ``None`` (absent), or the error (taint).

    Read side of a cross-lane contract: the writer is the pool
    resource-scope cleanup, filing from a token-gated broker
    ``export_stopped`` verdict after it proves the scope stopped and
    empty.  This module never writes one; verification additionally
    requires terminal broker telemetry, so a forged file alone proves
    nothing.
    """

    try:
        with open(attestation_path(queue, action_key, nonce)) as stream:
            payload = json.load(stream)
    except FileNotFoundError:
        return None
    except (OSError, ValueError) as exc:
        return exc
    if (not isinstance(payload, Mapping)
            or payload.get("schema") != ATTESTATION_SCHEMA_V1):
        return ReaderLeaseError("not a scope attestation")
    return payload


def export_verdict_proves_empty(export: object, *, scope_id: str
                                ) -> tuple[bool, str]:
    """Whether a broker ``export_stopped`` verdict proves this scope empty.

    Typed and exact, never inferred: ``scope_id`` must name this scope,
    ``stopped_unix`` must be a positive finite time, ``empty`` must be
    exactly True, and ``tickets_pending`` must be exactly False -- a
    missing field is unknown absence, never proof of none.  Release and
    retirement must be exact booleans proving a clean release or a
    settled retirement.  Returns ``(True, "proven")`` or ``(False,
    reason)``; never raises on malformed input.  The verdict carries no
    action or attempt identity beyond the scope id; the caller binds the
    attempt (attestation path, terminal telemetry) before asking.
    """

    if not isinstance(export, Mapping):
        return False, "export-unreadable-retain"
    if not scope_id or export.get("scope_id") != scope_id:
        return False, "export-scope-mismatch-retain"
    stopped = export.get("stopped_unix")
    if (type(stopped) not in (int, float)
            or not math.isfinite(stopped) or not stopped > 0):
        return False, "export-unstopped-retain"
    if export.get("empty") is not True:
        return False, "export-not-empty-retain"
    tickets = export.get("tickets_pending")
    if tickets is True:
        return False, "export-tickets-pending-retain"
    if tickets is not False:
        return False, "export-tickets-unknown-retain"
    released = export.get("released")
    retired = export.get("retired")
    settled = export.get("settled")
    if (type(released) is not bool or type(retired) is not bool
            or type(settled) is not bool):
        return False, "export-verdict-untyped-retain"
    if not ((released and not retired) or (retired and settled)):
        return False, "export-proof-incomplete-retain"
    return True, "proven"


def attestation_proves_empty(queue, action_key: str, nonce: str,
                             scope_id: str) -> tuple[bool, object]:
    """Whether the filed attestation is validated-complete, exact attempt.

    A shortcut on ``scope_empty is True`` alone would bless any file that
    says so.  This re-validates the exact attempt binding (action, nonce,
    scope), the exact export booleans (empty True, tickets False, typed
    release/retirement), a positive finite stop time, and a named
    host/worker pair.  Returns ``(True, attestation)`` or ``(False,
    reason)``.
    """

    attestation = read_scope_attestation(queue, action_key, nonce)
    if attestation is None:
        return False, "no-broker-attestation-retain"
    if isinstance(attestation, Exception):
        return False, f"broker-attestation-unreadable-retain: {attestation}"
    if not isinstance(attestation, Mapping):
        return False, "broker-attestation-unreadable-retain"
    if attestation.get("scope_empty") is not True:
        return False, "scope-not-empty-retain"
    if (str(attestation.get("action_key") or "") != action_key
            or str(attestation.get("nonce") or "") != nonce
            or str(attestation.get("scope_id") or "") != scope_id):
        return False, "attestation-id-mismatch-retain"
    host = attestation.get("host")
    worker = attestation.get("worker")
    if not (isinstance(host, str) and host
            and isinstance(worker, str) and worker):
        return False, "attestation-host-worker-missing-retain"
    stopped = attestation.get("stopped_unix")
    if (type(stopped) not in (int, float)
            or not math.isfinite(stopped) or not stopped > 0):
        return False, "proof-incomplete-retain"
    if attestation.get("empty") is not True:
        return False, "proof-incomplete-retain"
    tickets = attestation.get("tickets_pending")
    if tickets is True:
        return False, "tickets-pending-retain"
    if tickets is not False:
        return False, "tickets-unknown-retain"
    released = attestation.get("released")
    retired = attestation.get("retired")
    settled = attestation.get("settled")
    if (type(released) is not bool or type(retired) is not bool
            or type(settled) is not bool):
        return False, "proof-incomplete-retain"
    if retired and not settled:
        return False, "tombstone-unsettled-retain"
    if not ((released and not retired) or (retired and settled)):
        return False, "proof-incomplete-retain"
    return True, attestation


def attempt_refs_live(queue, owner_action_key: str, nonce: str,
                        scope_id: str, *, residency_root=None
                        ) -> tuple[bool, str]:
    """Whether live refs for this exact attempt still pin bytes.

    Scans the owner's pin directory for refs whose attempt is exactly
    ``(nonce, scope_id)`` (owner from the pin body, never the directory).
    Returns ``(True, reason)`` when at least one such ref stands --
    including when the census is uncertain -- and ``(False, reason)``
    only for proven absence.  Unknown absence holds: an unreadable
    directory is never proof of none; a never-created leases root is
    genuinely empty.  Read-only; never a reaper trigger on its own.
    """

    if not owner_action_key or not nonce or not scope_id:
        return True, "attempt-unbound-hold"
    root = leases_root(queue, residency_root)
    directory = root / owner_action_key
    try:
        names = sorted(entry.name for entry in os.scandir(directory)
                       if entry.is_file()
                       and entry.name.endswith(".lease.json"))
    except FileNotFoundError:
        return False, "no-pins"
    except OSError as exc:
        return True, f"pin-census-uncertain-hold: {exc}"
    for name in names:
        path = directory / name
        try:
            with open(path) as stream:
                pin = validate_pin(json.load(stream))
        except (OSError, ValueError) as exc:
            return True, f"pin-unreadable-hold: {name}: {exc}"
        refs = pin["refs"]
        assert isinstance(refs, dict)
        if str(pin.get("owner_action_key") or "") != owner_action_key:
            continue
        for ref in refs.values():
            if not isinstance(ref, dict):
                continue
            attempt = ref.get("attempt")
            if not isinstance(attempt, dict):
                continue
            if (str(attempt.get("nonce") or "") == nonce
                    and str(attempt.get("scope_id") or "") == scope_id):
                return True, "attempt-ref-live"
    return False, "attempt-refs-released"


def _replay_attestation_from_terminal(queue, action_key: str, nonce: str,
                                      scope_id: str) -> tuple[bool, str]:
    """Refile a lost attestation from the terminal's stored broker export.

    The pool cleanup persists the token-gated ``export_stopped`` verdict
    in the terminal record beside the release reply, so a proof lost to a
    shared-mount blip after ``finish`` -- no CLAIMED row left for the
    cleanup shortcut to retry from -- heals on the deployed egress tick:
    read the terminal's export for this exact attempt, validate it exactly
    as the writer would, and refile through the pool writer.  Never copies
    a verdict, never contacts the broker.  An incomplete export (tickets
    pending, empty False, unstopped) refuses: a later settlement
    reconciles only through the owner worker/broker authority path, never
    through this replay.  Withdrawal markers carry no export and never
    replay.  Returns ``(True, reason)`` when a proof was filed, ``(False,
    reason)`` otherwise; never raises.
    """

    from prismabuild import pool as pool_mod

    try:
        existing = read_scope_attestation(queue, action_key, nonce)
    except Exception as exc:                                    # noqa: BLE001
        return False, f"broker-attestation-unreadable-retain: {exc}"
    if isinstance(existing, Mapping):
        # The writer already spoke -- even scope_empty False is its verdict
        # from this terminal's export.  No replay, no overwrite.
        return False, "attestation-present-no-replay"
    if isinstance(existing, Exception):
        return False, f"broker-attestation-unreadable-retain: {existing}"
    for state in (pool_mod.DONE, pool_mod.FAILED):
        try:
            record = pool_mod._read_json(queue.item_path(state, action_key))
        except (OSError, pool_mod.PoolContractError) as exc:
            return False, f"terminal record unreadable: {state}"
        if not isinstance(record, Mapping):
            continue
        telemetry = _terminal_broker_telemetry(record)
        if telemetry is None:
            continue
        if (str(telemetry.get("action_key") or "") != action_key
                or str(telemetry.get("nonce") or "") != nonce):
            continue
        unit = telemetry.get("scope_unit")
        if not isinstance(unit, str) or not unit:
            unit = telemetry.get("scope_id")
        if unit != scope_id:
            continue
        cleanup = record.get("resource_scope_cleanup")
        export = (cleanup.get("export")
                  if isinstance(cleanup, Mapping) else None)
        ok, reason = export_verdict_proves_empty(export, scope_id=scope_id)
        if not ok:
            return False, reason
        try:
            resolved = queue.resolve_claim_holder(action_key, record)
        except (AttributeError, OSError, ValueError):
            resolved = None
        host = telemetry.get("host")
        if (isinstance(host, str) and host and isinstance(resolved, str)
                and resolved and host != resolved):
            return False, "terminal-host-mismatch-retain"
        try:
            filed = queue._persist_reader_scope_proof(
                record, nonce, scope_id, export)
        except Exception as exc:                                # noqa: BLE001
            return False, f"attestation-replay-failed-retain: {exc}"
        if not filed:
            return False, "attestation-replay-refused-retain"
        return True, f"replayed-terminal-{state}"
    return False, "no-terminal-export-retain"


def _terminal_attempt_ids(record: Mapping[str, object]) -> list[tuple[str, str]]:
    """Attempt (nonce, scope) identities a terminal record carries, if any.

    Reads the real shapes PB writes: ``resource_scope`` control on claims
    and terminals, ``resource_scope_cleanup.telemetry`` filed by cleanup
    (broker sample: ``{action_key, nonce, host, scope_unit}``), and the
    worker-outcome ``resource_telemetry`` top-level or under ``detail``.
    Anything else is not attempt evidence.
    """

    out: list[tuple[str, str]] = []

    def _take(nonce: object, scope: object) -> None:
        if isinstance(nonce, str) and nonce:
            out.append((nonce, str(scope) if isinstance(scope, str) else ""))

    scope = record.get("resource_scope")
    if isinstance(scope, Mapping):
        _take(scope.get("nonce"), scope.get("scope_id") or scope.get("unit"))
    cleanup = record.get("resource_scope_cleanup")
    if isinstance(cleanup, Mapping):
        telemetry = cleanup.get("telemetry")
        if isinstance(telemetry, Mapping):
            _take(telemetry.get("nonce"), telemetry.get("scope_unit"))
    for carrier in (record.get("resource_telemetry"),
                    record.get("detail", {}).get("resource_telemetry")
                    if isinstance(record.get("detail"), Mapping) else None):
        if isinstance(carrier, Mapping):
            _take(carrier.get("nonce"), carrier.get("scope_unit"))
    for field in ("nonce", "attempt_nonce"):
        _take(record.get(field), record.get("scope_id"))
    return out


def _terminal_broker_telemetry(record: Mapping[str, object]
                               ) -> Mapping[str, object] | None:
    """The broker-produced telemetry on a terminal record, if it parses.

    Prefers the cleanup sample (filed beside the release verdict), then
    the worker-outcome telemetry shapes (which carry the action key),
    then the claim control (nonce/scope only, no action key).  Callers
    matching on action identity need a carrier that carries it.
    """

    cleanup = record.get("resource_scope_cleanup")
    if isinstance(cleanup, Mapping):
        telemetry = cleanup.get("telemetry")
        if isinstance(telemetry, Mapping) and telemetry.get("nonce"):
            return telemetry
    for carrier in (record.get("resource_telemetry"),
                    record.get("detail", {}).get("resource_telemetry")
                    if isinstance(record.get("detail"), Mapping) else None):
        if isinstance(carrier, Mapping) and carrier.get("nonce"):
            return carrier
    scope = record.get("resource_scope")
    if isinstance(scope, Mapping) and scope.get("nonce"):
        return scope
    return None


def _history_attempt_terminal(queue, action_key: str, nonce: str,
                              scope_id: str
                              ) -> tuple[bool | None, str]:
    """Whether attempt history proves ``nonce`` terminally closed.

    Returns ``(True, where)`` (a history outcome carries this attempt with
    terminal disposition *and* broker telemetry naming it), ``(False,
    where)`` (history names it non-terminally -- a newer attempt is
    current), or ``(None, reason)`` (history unanswerable: retain).
    """

    base = Path(queue.root) / "attempts" / action_key
    try:
        generations = sorted(entry.name for entry in os.scandir(base)
                             if entry.is_dir())
    except OSError:
        return None, "attempt history unreadable"
    for generation in generations:
        directory = base / generation
        try:
            names = sorted(entry.name for entry in os.scandir(directory)
                           if entry.is_file() and entry.name.endswith(".json"))
        except OSError:
            return None, "attempt history unreadable"
        for name in names:
            try:
                with open(directory / name) as stream:
                    outcome = json.load(stream)
            except (OSError, ValueError):
                return None, "attempt history unreadable"
            if not isinstance(outcome, Mapping):
                continue
            telemetry = _terminal_broker_telemetry(outcome)
            if telemetry is None:
                continue
            if (str(telemetry.get("nonce") or "") != nonce
                    or str(telemetry.get("action_key") or "") != action_key):
                continue
            unit = str(telemetry.get("scope_unit") or ""
                       ) or str(telemetry.get("scope_id") or "")
            if scope_id and unit != scope_id:
                continue
            disposition = str(outcome.get("disposition") or "")
            status = str(outcome.get("status") or "")
            if disposition in ("done", "failed", "withdrawn") or (
                    not disposition and status in (
                        "executed", "cache_hit", "failed",
                        "result-ingestion-failed")):
                return True, f"attempt-history:{generation}/{name}"
            return False, f"attempt-history:{generation}/{name}"
    return None, "attempt not in history"


def containment_certificate_ok(queue, certificate: Mapping[str, object]
                               ) -> tuple[bool, str]:
    """Whether a certificate authorizes releasing another attempt's refs.

    ``certificate`` carries identifiers only -- ``{action_key, nonce,
    scope_id, worker?, incarnation?, host?}``.  Proof is two authoritative
    reads, and neither suffices alone:

    1. The broker attestation file for ``(action_key, nonce)`` must exist
       with ``scope_empty`` true, and its body must bind the same
       action_key, nonce and scope_id (plus worker/incarnation/host where
       the certificate names them).  A caller-writable file asserting
       emptiness, alone, is never proof.
    2. The attempt's terminal record must carry broker telemetry
       (``resource_telemetry`` with ``{action_key, nonce, scope_unit}``)
       naming exactly this attempt: the queue's terminal record for the
       action when its telemetry names this attempt, else the attempt
       history.  A terminal record naming a DIFFERENT attempt (a retry is
       current) consults the older history instead of retaining forever:
       history proving this attempt terminally closed with matching
       telemetry authorizes; history naming it non-terminally, or silence,
       retains.

    Anything unanswerable retains with a reason: no ``kill(0)``, no
    cross-host ``/proc``, no heartbeat or timestamp expiry.
    """

    from prismabuild import pool as pool_mod

    action_key = certificate.get("action_key")
    nonce = certificate.get("nonce")
    scope_id = str(certificate.get("scope_id") or "")
    if (not isinstance(action_key, str) or len(action_key) != 64
            or not isinstance(nonce, str) or not nonce or not scope_id):
        return False, "certificate names no attempt"
    attestation = read_scope_attestation(queue, action_key, nonce)
    if attestation is None:
        return False, "no-broker-attestation-retain"
    if isinstance(attestation, Exception):
        return False, f"broker-attestation-unreadable-retain: {attestation}"
    if attestation.get("scope_empty") is not True:
        return False, "scope-not-empty-retain"
    # The attestation body binds every ID it carries: a file naming another
    # action, attempt or scope proves nothing about this one.
    if (str(attestation.get("action_key") or "") != action_key
            or str(attestation.get("nonce") or "") != nonce
            or str(attestation.get("scope_id") or "") != scope_id):
        return False, "attestation-id-mismatch-retain"
    for field in ("worker", "incarnation", "host"):
        wanted = certificate.get(field)
        if (isinstance(wanted, str) and wanted
                and str(attestation.get(field) or "") != wanted):
            return False, f"attestation-{field}-mismatch-retain"
    # The exact supported proof, read off the attestation body (which the
    # pool cleanup wrote from the broker verdict): a clean release, or a
    # settled retirement.  Tombstones without settlement, absence
    # responses, and verdicts without stopped evidence never qualify,
    # however scope_empty reads.  Every boolean is exact: a missing
    # tickets_pending is unknown, never proof of none.
    stopped = attestation.get("stopped_unix")
    if (type(stopped) not in (int, float)
            or not math.isfinite(stopped) or not stopped > 0):
        return False, "proof-incomplete-retain"
    retired = attestation.get("retired")
    settled = attestation.get("settled")
    released = attestation.get("released")
    if (type(released) is not bool or type(retired) is not bool
            or type(settled) is not bool):
        return False, "proof-incomplete-retain"
    if retired and not settled:
        return False, "tombstone-unsettled-retain"
    tickets = attestation.get("tickets_pending")
    if tickets is True:
        return False, "tickets-pending-retain"
    if tickets is not False:
        return False, "tickets-unknown-retain"
    if attestation.get("empty") is not True:
        return False, "proof-incomplete-retain"
    if not ((released and not retired) or (retired and settled)):
        return False, "proof-incomplete-retain"

    def telemetry_names(telemetry: Mapping[str, object]) -> bool | None:
        """Whether broker telemetry names this exact attempt.

        Returns True (names it), False (names another attempt), or None
        (unanswerable shape).  Sample shapes carry ``scope_unit`` + host;
        control shapes carry ``scope_id`` without host (host stays bound
        by the attestation and holder checks downstream).  A carrier
        without an action key is matched on nonce + scope within this
        action-bound record; one naming another action never matches.
        """

        key = telemetry.get("action_key")
        if isinstance(key, str) and key and key != action_key:
            return False
        if str(telemetry.get("nonce") or "") != nonce:
            return False
        scope = telemetry.get("scope_unit")
        if not isinstance(scope, str) or not scope:
            scope = telemetry.get("scope_id")
        if not isinstance(scope, str) or scope != scope_id:
            return False
        host = telemetry.get("host")
        if isinstance(host, str) and host:
            return host == str(attestation.get("host") or "")
        return True

    terminal_states: dict[str, object] = {}
    names_this_attempt: bool | None = None
    for state in (pool_mod.DONE, pool_mod.FAILED, pool_mod.WITHDRAWN):
        try:
            record = pool_mod._read_json(queue.item_path(state, action_key))
        except (OSError, pool_mod.PoolContractError):
            return False, f"terminal record unreadable: {state}"
        if record is None or not isinstance(record, Mapping):
            continue
        terminal_states[state] = record
        telemetry = _terminal_broker_telemetry(record)
        if telemetry is None:
            # A bare marker (withdrawal with no attempt telemetry)
            # proves nothing about this attempt on its own.
            continue
        verdict = telemetry_names(telemetry)
        if verdict is True:
            host = str(attestation.get("host") or "")
            return True, f"contained-terminal-{state}:{host}"
        names_this_attempt = False
    if names_this_attempt is False:
        # Terminal records name only other attempts: consult the exact
        # older history rather than retaining forever on a supersede...
        historic, _where = _history_attempt_terminal(
            queue, action_key, nonce, scope_id)
        if historic is True:
            host = str(attestation.get("host") or "")
            return True, f"contained-history:{host}"
        if historic is False:
            return False, "superseded-attempt-retain"
        # ...and the other terminal states (a failed record for this
        # attempt survives beside a done record for its successor).
        for state, record in terminal_states.items():
            if not isinstance(record, Mapping):
                continue
            telemetry = _terminal_broker_telemetry(record)
            if telemetry is not None and telemetry_names(telemetry) is True:
                host = str(attestation.get("host") or "")
                return True, f"contained-terminal-{state}:{host}"
        return False, "no-terminal-evidence-retain"
    historic, _where = _history_attempt_terminal(queue, action_key, nonce,
                                                scope_id)
    if historic is True:
        host = str(attestation.get("host") or "")
        return True, f"contained-history:{host}"
    return False, "no-terminal-evidence-retain"


def release_refs(queue, refs: list[dict[str, str]],
                 certificate: Mapping[str, object], *, residency_root=None
                 ) -> dict[str, object]:
    """Drop named refs under a containment certificate -- containment only.

    ``refs`` entries name ``{consumer_action_key, pin_id, ref_id}``.  Each
    ref is removed exactly; a pin whose refs empty is unlinked (evictable).
    Refs failing validation against the certificate's attempt are skipped
    with reasons, never force-released.  Ordinary holders call
    :func:`release` for their own refs; this path is for RESIGN/withdrawal
    containment after proven stop, and for the PB crash reaper for its own
    attempt.  There is no other release path.

    Each ref is settled under the ownership lock of **the root its own pin
    names**, taken before the owner/attempt/host checks -- so a caller that
    already holds one stage root and hands this refs from another is asking
    for a second root inside the first (#780).  Callers hold no stage
    ownership lock, or only the root every ref named.
    """

    ok, reason = containment_certificate_ok(queue, certificate)
    released: list[str] = []
    skipped: list[str] = []
    if not ok:
        return {"ok": False, "reason": reason, "released": released,
                "skipped": [str(ref.get("ref_id", "?")) for ref in refs]}
    attempt_nonce = str(certificate.get("nonce") or "")
    attempt_scope = str(certificate.get("scope_id") or "")
    cert_action = str(certificate.get("action_key") or "")
    attestation = read_scope_attestation(
        queue, str(certificate.get("action_key") or ""), attempt_nonce)
    attested_host = (str(attestation.get("host") or "")
                     if isinstance(attestation, Mapping) else "")
    for ref in refs:
        consumer = str(ref.get("consumer_action_key") or "")
        pin_id = str(ref.get("pin_id") or "")
        ref_id = str(ref.get("ref_id") or "")
        first = None
        candidates, complete = _pin_candidates(
            queue, pin_id, consumer_action_key=consumer or None,
            residency_root=residency_root)
        if not complete:
            # Unknown absence: retain, never force-release unknown pins.
            skipped.append(f"{ref_id}: pin census unreadable")
            continue
        for candidate in candidates:
            first = _read_pin(candidate)
            if first is not None:
                path = candidate
                break
        if first is None:
            released.append(ref_id)  # already gone counts as released
            continue
        if isinstance(first, Exception):
            skipped.append(f"{ref_id}: {first}")
            continue
        stage_root = str(first.get("stage_root") or "")
        if not stage_root:
            skipped.append(f"{ref_id}: pin names no stage")
            continue
        with queue.stage_ownership_lock(stage_root):
            pin = _read_pin(path)
            if pin is None:
                released.append(ref_id)
                continue
            if isinstance(pin, Exception):
                skipped.append(f"{ref_id}: {pin}")
                continue
            if str(pin.get("owner_action_key") or "") != cert_action:
                # A foreign owner's certificate never releases this pin,
                # even when nonce/scope/host/worker strings all repeat.
                skipped.append(f"{ref_id}: owner mismatch")
                continue
            refs_map = pin["refs"]
            assert isinstance(refs_map, dict)
            held = refs_map.get(ref_id)
            if held is None:
                released.append(ref_id)
                continue
            assert isinstance(held, dict)
            held_attempt = held["attempt"]
            assert isinstance(held_attempt, dict)
            if str(held_attempt.get("nonce")) != attempt_nonce:
                skipped.append(f"{ref_id}: attempt mismatch")
                continue
            if str(held_attempt.get("scope_id")) != attempt_scope:
                skipped.append(f"{ref_id}: scope mismatch")
                continue
            held_holder = held.get("holder")
            held_host = (str(held_holder.get("host") or "")
                         if isinstance(held_holder, Mapping) else "")
            if held_host != attested_host:
                # Bound to the pin and the exact attempt: containment for
                # one host never frees another host's ref.
                skipped.append(f"{ref_id}: host mismatch")
                continue
            cert_worker = str(certificate.get("worker") or "")
            held_worker = (str(held_holder.get("worker") or "")
                           if isinstance(held_holder, Mapping) else "")
            if cert_worker and held_worker and held_worker != cert_worker:
                # Exact host/worker/action/attempt correspondence: a
                # certificate for one worker incarnation never frees
                # another's ref.
                skipped.append(f"{ref_id}: worker mismatch")
                continue
            del refs_map[ref_id]
            released.append(ref_id)
            if refs_map:
                pin["refs"] = refs_map
                try:
                    _write_pin(path, pin)
                except ReaderLeaseError as exc:
                    skipped.append(f"{ref_id}: {exc}")
                    released.remove(ref_id)
            else:
                try:
                    path.unlink(missing_ok=True)
                except OSError as exc:
                    skipped.append(f"{ref_id}: {exc}")
                    released.remove(ref_id)
    return {"ok": not skipped, "reason": reason, "released": released,
            "skipped": skipped}


# --------------------------------------------------------------------------
# Automatic reclamation: the egress frees contained attempts by itself
# --------------------------------------------------------------------------

def auto_reclaim(queue, *, residency_root=None) -> dict[str, object]:
    """Release exactly the refs whose attempts are provably contained.

    Scans every owner directory: for each pin, group refs by attempt and
    verify each attempt's terminal broker telemetry plus the
    broker-persisted proof against the pin OWNER's action (never the
    material namespace's terminal); release only the refs that verify,
    never anything else.  Called by the egress when live pins block a
    delete, so ordinary completion, crash/withdrawal cleanup and
    old-attempt drains retire without an operator once their evidence
    exists.  Missing evidence retains with reasons, per attempt, exactly
    as manual containment would.
    Returns ``{"released": [...], "retained": {ref_id: reason}}``.

    **Never call this while holding a stage ownership lock (#780).**  It is
    unscoped by design -- every owner, every root -- and :func:`release_refs`
    takes the ownership lock of the root each pin names.  Under one root's
    lock that is a second root requested inside the first, which deadlocks
    against an egress doing the same from the other side.  The caller takes
    its own root's lock *after* this returns and re-censuses pins there; the
    check-and-act stays atomic because the census, not the reclaim, is what
    the delete decision reads.
    """

    root = leases_root(queue, residency_root)
    try:
        owners = sorted(entry.name for entry in os.scandir(root)
                        if entry.is_dir())
    except OSError as exc:
        return {"released": [], "retained": {"<pin-census>": f"{exc}"}}
    released: list[str] = []
    retained: dict[str, str] = {}
    for owner_dir in owners:
        directory = root / owner_dir
        try:
            names = sorted(entry.name for entry in os.scandir(directory)
                           if entry.is_file()
                           and entry.name.endswith(".lease.json"))
        except OSError as exc:
            retained[f"<pin-census:{owner_dir}>"] = f"{exc}"
            continue
        for name in names:
            path = directory / name
            pin = _read_pin(path)
            if pin is None or isinstance(pin, Exception):
                continue
            # The certificate action comes from the pin's OWNER field,
            # never the directory it was found under: lease owner action
            # vs material scope IDs stay distinct end to end.
            pin_owner = str(pin.get("owner_action_key") or "")
            if not pin_owner:
                continue
            refs_map = pin["refs"]
            assert isinstance(refs_map, dict)
            pin_id = str(pin["pin_id"])
            by_attempt: dict[tuple[str, str], list[str]] = {}
            for ref_id, ref in refs_map.items():
                if not isinstance(ref, dict):
                    continue
                attempt = ref.get("attempt")
                if not isinstance(attempt, dict):
                    continue
                by_attempt.setdefault(
                    (str(attempt.get("nonce") or ""),
                     str(attempt.get("scope_id") or "")),
                    []).append(ref_id)
            for (nonce, scope_id), ref_ids in by_attempt.items():
                if not nonce or not scope_id:
                    for ref_id in ref_ids:
                        retained[ref_id] = "attempt unbound"
                    continue
                # The deployed tick heals a lost proof itself: when the
                # attestation never landed (shared-mount blip after finish)
                # but the terminal carries the validated broker export,
                # refile from that export before judging.  No broker
                # contact, no operator retry; an incomplete export retains.
                ok, _proof = attestation_proves_empty(
                    queue, pin_owner, nonce, scope_id)
                if not ok:
                    _replayed, _replay_reason = (
                        _replay_attestation_from_terminal(
                            queue, pin_owner, nonce, scope_id))
                first = refs_map[ref_ids[0]]
                holder = first.get("holder") if isinstance(first, dict) else None
                certificate: dict[str, object] = {
                    "action_key": pin_owner,
                    "nonce": nonce,
                    "scope_id": scope_id,
                }
                if isinstance(holder, Mapping):
                    for field in ("worker", "host"):
                        if (isinstance(holder.get(field), str)
                                and holder.get(field)):
                            certificate[field] = holder[field]
                outcome = release_refs(
                    queue,
                    [{"consumer_action_key": pin_owner,
                      "pin_id": pin_id, "ref_id": ref_id}
                     for ref_id in ref_ids],
                    certificate, residency_root=residency_root)
                released.extend(outcome["released"])
                reason = str(outcome["reason"])
                for skipped in outcome["skipped"]:
                    retained[str(skipped).split(":")[0]] = reason
    return {"released": released, "retained": retained}


# --------------------------------------------------------------------------
# Window cover lookup (PQ-facing helper): what to acquire, without inventing
# --------------------------------------------------------------------------

#: How many ``(root, consumer, mover)`` pairs the cover lookup keeps
#: validated in this process (#893).  One pair holds that mover's validated
#: material sidecar and fragment: what one pre-#893 lookup parsed for that
#: mover and then dropped.  A reader serves one consumer, whose live movers
#: are one staged window; R11 (GLM Stage A) has 27, about 19 MB of JSON.
#: Pairs of movers that leave the consumer's material directory are pruned
#: on the next lookup, so the cache follows the live set, and the bound only
#: caps a process that looks up many consumers or roots, at about nine times
#: R11's live set.  A live set larger than the bound stays correct: the
#: least recently used pairs are re-read, as every pair was before #893.
COVER_DOCS_CACHE_PAIRS = 256

#: ``(root, consumer, mover) -> (material slot, fragment slot)``; a slot is
#: ``(identity, validated document)`` or ``None``.  Guarded by
#: ``_COVER_DOCS_LOCK``.  Reads and validation run outside the lock, so two
#: threads may both re-read a changed file; the later store wins, and both
#: answers came from their own opens.
_COVER_DOCS: OrderedDict[tuple[str, str, str], tuple[object, object]] = (
    OrderedDict())
_COVER_DOCS_LOCK = threading.Lock()


def clear_cover_docs_cache() -> None:
    """Forget every validated cover document this process holds."""

    with _COVER_DOCS_LOCK:
        _COVER_DOCS.clear()


def _read_cover_doc(path: Path, validate, held):
    """``(identity, validated document)`` for one cover file, reusing ``held``.

    Opens first, so NFS close-to-open revalidates the file's attributes
    exactly as the unconditional re-read did, then takes the identity from
    the descriptor it opened: ``(st_dev, st_ino, st_size, st_mtime_ns,
    st_ctime_ns)``.  Never a separate ``stat`` of the name, whose cached
    attributes can be older than the open.  An unchanged identity returns
    ``held`` without reading; any change reads and validates through the
    same descriptor.  Both writers of these files (``write_material`` and
    ``residency_map.write_fragment``) rename a new inode into place, so every
    republish, including #823's same-generation incremental republish,
    presents a new identity.  An in-place rewrite changes the size or moves
    the modification and change times at the filesystem's timestamp
    granularity; a same-size rewrite of the same inode inside one timestamp
    tick is the one change this identity cannot see, and no writer of these
    files makes one.  Raises what a fresh read raises: ``OSError``
    (``FileNotFoundError`` when absent), or ``ValueError`` for a document
    that does not parse or validate.
    """

    with open(path) as stream:
        info = os.fstat(stream.fileno())
        identity = (info.st_dev, info.st_ino, info.st_size,
                    info.st_mtime_ns, info.st_ctime_ns)
        if held is not None and held[0] == identity:
            return held
        return identity, validate(json.load(stream))


def _remember_cover_docs(slot: tuple[str, str, str], material_slot,
                         fragment_slot) -> None:
    with _COVER_DOCS_LOCK:
        _COVER_DOCS[slot] = (material_slot, fragment_slot)
        _COVER_DOCS.move_to_end(slot)
        while len(_COVER_DOCS) > COVER_DOCS_CACHE_PAIRS:
            _COVER_DOCS.popitem(last=False)


def _forget_cover_docs(slot: tuple[str, str, str]) -> None:
    with _COVER_DOCS_LOCK:
        _COVER_DOCS.pop(slot, None)


def _prune_cover_docs(root: str, consumer_action_key: str,
                      live: set[str]) -> None:
    """Drop the pairs of movers that left the consumer's material directory."""

    with _COVER_DOCS_LOCK:
        gone = [slot for slot in _COVER_DOCS
                if slot[0] == root and slot[1] == consumer_action_key
                and slot[2] not in live]
        for slot in gone:
            del _COVER_DOCS[slot]


def _reused_cover_doc(root, consumer_action_key: str, mover: str,
                      which: str):
    """One mover's sidecar or fragment, as a plain read returns it.

    ``which`` is ``"material"`` or ``"fragment"``.  Returns the validated
    document, ``None`` when the file is absent, or the error when it cannot
    be read or validated: exactly what :func:`read_material`, or a read of
    the fragment, returns.  The file is opened on every call, so the answer
    is as fresh as a plain read (:func:`_read_cover_doc`), and it is parsed
    and validated only when the descriptor's identity differs from the one
    the process validated last.  Shares :func:`_cached_cover_docs`'s store,
    so an acquire reuses what a cover lookup validated and the reverse.
    The returned document is shared and must not be mutated.
    """

    from prismabuild import residency_map as map_mod

    slot = (str(root), consumer_action_key, mover)
    with _COVER_DOCS_LOCK:
        held = _COVER_DOCS.get(slot)
    held_material, held_fragment = held if held is not None else (None, None)
    if which == "material":
        path = material_path(root, consumer_action_key, mover)
        validate = lambda value: validate_material(value)  # noqa: E731
        mine = held_material
    else:
        path = map_mod.fragment_path(root, consumer_action_key, mover)
        validate = lambda value: map_mod.validate_fragment(value)  # noqa: E731
        mine = held_fragment
    try:
        fresh = _read_cover_doc(path, validate, mine)
    except FileNotFoundError:
        _forget_cover_docs(slot)
        return None
    except (OSError, ValueError) as exc:
        _forget_cover_docs(slot)
        return exc
    with _COVER_DOCS_LOCK:
        current = _COVER_DOCS.get(slot, (None, None))
        _COVER_DOCS[slot] = ((fresh, current[1]) if which == "material"
                             else (current[0], fresh))
        _COVER_DOCS.move_to_end(slot)
        while len(_COVER_DOCS) > COVER_DOCS_CACHE_PAIRS:
            _COVER_DOCS.popitem(last=False)
    return fresh[1]


def _cached_cover_docs(root: Path, consumer_action_key: str, mover: str,
                       context: dict | None):
    """Validated (material, fragment) for one mover, identity-checked.

    The process keeps VALIDATED documents keyed by ``(root, consumer,
    mover)`` (#893).  Every call still opens both files, so each answer is
    exactly as fresh as the unconditional re-read it replaces, and a file is
    read and validated again only when the identity of the descriptor just
    opened differs from the one validated (:func:`_read_cover_doc`).
    PrismaQuant passes a fresh ``context`` on every call, so before #893 each
    call re-read and re-validated every mover's documents: 19 MB and 13,956
    fragment entries on R11, two or three times per staged entry.

    The sidecar is checked first on every call.  A caller's persistent
    ``context`` reuses its cached fragment only while that fresh sidecar
    equals the cached one: the stage mover republishes both documents
    incrementally as entries land under ONE generation per run
    (stage_move.publish / begin_material), so the generation alone cannot
    date the pair (#823).  Absence and malformation are NEVER cached; either
    drops the mover's pair, so newly published material is always seen.
    The returned documents are shared across calls and threads, and callers
    must not mutate them.  Selection only: acquire revalidates under the
    ownership lock before anything pins.
    """

    from prismabuild import residency_map as map_mod

    slot = (str(root), consumer_action_key, mover)
    with _COVER_DOCS_LOCK:
        held = _COVER_DOCS.get(slot)
        if held is not None:
            _COVER_DOCS.move_to_end(slot)
    held_material, held_fragment = held if held is not None else (None, None)
    try:
        # Validators resolve at call time: the module attributes stay the
        # one definition of a valid document.
        material_slot = _read_cover_doc(
            material_path(root, consumer_action_key, mover),
            lambda value: validate_material(value), held_material)
    except (OSError, ValueError):
        _forget_cover_docs(slot)
        return None, None
    material = material_slot[1]
    generation = str(material.get("generation") or "")
    if not generation:
        return None, None
    if context is not None:
        hit = context.get(f"cover:{consumer_action_key}:{mover}")
        if (isinstance(hit, dict) and hit.get("generation") == generation
                and hit.get("material") == material):
            _remember_cover_docs(slot, material_slot, held_fragment)
            return hit.get("material"), hit.get("fragment")
    try:
        fragment_slot = _read_cover_doc(
            map_mod.fragment_path(root, consumer_action_key, mover),
            lambda value: map_mod.validate_fragment(value), held_fragment)
    except (OSError, ValueError):
        _forget_cover_docs(slot)
        return None, None
    fragment = fragment_slot[1]
    _remember_cover_docs(slot, material_slot, fragment_slot)
    if context is not None:
        context[f"cover:{consumer_action_key}:{mover}"] = {
            "generation": generation, "material": material,
            "fragment": fragment}
    return material, fragment


def covers_for_keys(root: str | Path, consumer_action_key: str,
                    keys: list[str], *, tier_id: str,
                    manifest_sha256: str, epoch: str,
                    context: dict | None = None) -> dict[str, object]:
    """Resolve a window's covering material from PB-owned records.

    Given requested map keys on one tier, returns the minimal covering
    mover set plus the expected per-key proof (``covers``/``expected``
    for :func:`acquire`) -- read off the consumer's fragments plus
    publish-time sidecars, batched at window granularity, never per
    tensor.  Both SSD and RAM tiers: RAM covers come from ram-tier
    fragments carrying the announced epoch (SSD fragments carry epoch
    ``""``, explicit absence, never a RAM epoch).  ``manifest_sha256``
    and ``epoch`` are REQUIRED keywords so no other readset's material
    can be adopted.  Callers (including PQ) must not invent RAM covers
    from SSD leads: only material the fleet published qualifies.
    Freshness is re-validated under the ownership lock inside
    :func:`acquire`; this lookup is selection, not admission.

    Minimal and nonconflicting: each requested key is attributed to
    exactly one mover; two movers vouching one key with different bytes
    or digests refuse as contradictory proofs, and a selected mover
    whose material or fragment is malformed fails the pass
    (ownership-uncertain) instead of being silently skipped into an
    ``unpublished`` that invites fallback.  The sealed caller's
    expected length/digest stays authoritative downstream: acquire
    proves it against this selection and refuses any gap.

    A key is covered only where both documents name it: a vouch the
    sidecar does not date and a date the fragment does not vouch each
    cover nothing, and neither contradicts another mover's cover
    (#1087).  The movers write the sidecar first, so an in-flight or
    interrupted publication shows dates its fragment does not carry yet;
    they read exactly as the undated vouches of the old order did.  A
    key both documents name with different bytes is still a
    contradiction.

    Returns ``{"ok": True, "covers":
    [{mover_action_key, manifest_sha256}], "manifest_sha256": ...,
    "expected": {key: {bytes, sha256}}}`` or ``{"ok": False,
    "refusal": ...}``.
    """

    base = Path(root)
    if context is None:
        context = {}
    try:
        names = sorted(entry.name for entry in os.scandir(
            base / "material" / consumer_action_key)
            if entry.is_file() and entry.name.endswith(".json"))
    except OSError:
        return {"ok": False, "refusal": "unpublished"}
    _prune_cover_docs(str(base), consumer_action_key,
                      {name[:-len(".json")] for name in names})
    wanted = set(keys)
    # Per-key candidates: mover -> (bytes, digest, generation).
    candidates: dict[str, list[tuple[str, object, object, str]]] = {}
    selected: dict[str, dict[str, object]] = {}
    for name in names:
        mover = name[:-len(".json")]
        if len(mover) != 64 or _HEX_RUN.fullmatch(mover) is None:
            continue
        material, fragment = _cached_cover_docs(
            base, consumer_action_key, mover, context)
        if material is None or fragment is None:
            continue
        if str(material.get("tier_id") or "") != tier_id:
            continue
        if str(material.get("manifest_sha256") or "") != manifest_sha256:
            continue
        # Exact sidecar convention: SSD material carries no epoch (absent
        # or ""), RAM material carries the announced epoch it landed
        # under.  A non-RAM tier naming an epoch is corrupt, not staged.
        material_epoch = material.get("epoch")
        if tier_id.startswith("ram:"):
            if not isinstance(material_epoch, str) or not material_epoch:
                continue
            if material_epoch != str(epoch or ""):
                continue
        else:
            if isinstance(material_epoch, str) and material_epoch:
                return {"ok": False,
                        "refusal": "ownership-uncertain: staged epoch set"}
            if str(material.get("epoch") or "") != str(epoch or ""):
                continue
        if (str(fragment.get("tier_id") or "") != tier_id
                or str(fragment.get("manifest_sha256") or "")
                != manifest_sha256
                or str(fragment.get("epoch") or "") != str(epoch or "")):
            continue
        material_entries = material.get("entries")
        if not isinstance(material_entries, dict):
            continue
        fragment_entries = fragment.get("entries")
        if not isinstance(fragment_entries, dict):
            continue
        # Visit only the wanted keys this mover mentions, from whichever
        # side is smaller: PQ asks for one key per call, and each R11 mover
        # mentions about 500 (#893).  Keys are independent below, so the
        # visiting order within one mover changes nothing; the mover order,
        # which fixes each key's candidate order, is unchanged.
        if len(wanted) < len(material_entries):
            mentions = [(key, material_entries[key]) for key in wanted
                        if key in material_entries]
        else:
            mentions = [(key, mention)
                        for key, mention in material_entries.items()
                        if str(key) in wanted]
        for key, mention in mentions:
            if not isinstance(mention, dict):
                continue
            if str(key) not in fragment_entries:
                # A date no vouch cites is inert (#1087): the movers write
                # the sidecar before the fragment, so an interrupted or
                # in-flight publication dates names its fragment does not
                # carry yet.  It neither covers the key nor contradicts
                # another mover's cover of it.
                continue
            vouched = fragment_entries.get(str(key))
            # The sidecar dates the fragment's vouching: same path,
            # length, digest, or this cover is about different bytes.
            if (not isinstance(vouched, Mapping)
                    or str(vouched.get("stage_path") or "")
                    != str(mention.get("stage_path") or "")
                    or vouched.get("bytes") != mention.get("bytes")
                    or str(vouched.get("sha256") or "")
                    != str(mention.get("sha256") or "")):
                selected[str(key)] = {"tainted": True,
                                      "reason": "sidecar/fragment disagree"}
                continue
            candidates.setdefault(str(key), []).append((
                mover, mention.get("bytes"), mention.get("sha256"),
                str(material.get("generation"))))
            selected.setdefault(str(key), dict(mention))
    if not any(candidates.values()):
        # Nothing published for this readset at all: absence, not a gap.
        return {"ok": False, "refusal": "unpublished"}
    covers: list[dict[str, str]] = []
    expected: dict[str, dict[str, object]] = {}
    seen_movers: set[str] = set()
    for key in keys:
        if key in selected and selected[key].get("tainted"):
            # A selected cover disagreeing with its fragment is a
            # contradiction in the chosen proof, not an absence.
            return {"ok": False,
                    "refusal": "ownership-uncertain: sidecar/fragment disagree"}
        options = candidates.get(key, [])
        if not options:
            return {"ok": False, "refusal": "source-coverage-gap"}
        first = options[0]
        for other in options[1:]:
            if other[1] != first[1] or other[2] != first[2]:
                # Two movers vouch one key with different bytes: picking
                # either would make the pin a guess.
                return {"ok": False,
                        "refusal": "ownership-uncertain: contradictory covers"}
        mover = first[0]
        expected[key] = {"bytes": first[1], "sha256": first[2]}
        if mover not in seen_movers:
            seen_movers.add(mover)
            covers.append({"mover_action_key": mover,
                           "manifest_sha256": manifest_sha256})
    return {"ok": True,
            "covers": covers,
            "manifest_sha256": manifest_sha256,
            "expected": expected}



def resolve_window_covers(queue, *, consumer_action_key: str,
                          tier_id: str, epoch: str,
                          keys: list[str] | None = None,
                          manifest_sha256: str | None = None,
                          residency_root=None, context: dict | None = None
                          ) -> dict[str, object]:
    """Queue-rooted cover lookup; prefers :func:`covers_for_keys`.

    Kept for PB-internal callers that already hold the queue.  New code
    (including PQ) calls :func:`covers_for_keys` directly.
    """

    from prismabuild import pool as pool_mod

    root = Path(residency_root if residency_root is not None
                else Path(queue.root) / pool_mod.RESIDENCY)
    if manifest_sha256 is None:
        return {"ok": False, "refusal": "manifest-unbound"}
    if keys is None:
        all_keys: set[str] = set()
        try:
            names = sorted(entry.name for entry in os.scandir(
                root / "material" / consumer_action_key)
                if entry.is_file() and entry.name.endswith(".json"))
        except OSError:
            return {"ok": False, "refusal": "unpublished"}
        for name in names:
            mover = name[:-len(".json")]
            if len(mover) != 64:
                continue
            material = read_material(root, consumer_action_key, mover)
            if not isinstance(material, dict):
                continue
            if (str(material.get("tier_id") or "") != tier_id
                    or str(material.get("manifest_sha256") or "")
                    != manifest_sha256):
                continue
            material_entries = material.get("entries")
            if not isinstance(material_entries, dict):
                continue
            # Only what the mover's fragment also vouches is published: a
            # date no vouch cites is inert (#1087), and a sidecar with no
            # fragment at all dates nothing.  An unreadable fragment keeps
            # every dated key, so the lookup below meets it as before.
            fragment = _reused_cover_doc(root, consumer_action_key, mover,
                                         "fragment")
            if fragment is None:
                continue
            vouched = (fragment.get("entries")
                       if isinstance(fragment, dict) else None)
            all_keys.update(
                str(key) for key in material_entries
                if not isinstance(vouched, dict) or str(key) in vouched)
        keys = sorted(all_keys)
    result = covers_for_keys(
        root, consumer_action_key, keys, tier_id=tier_id,
        manifest_sha256=manifest_sha256, epoch=epoch, context=context)
    if not result.get("ok"):
        return result
    return {"ok": True, "tier_id": tier_id, "epoch": str(epoch or ""),
            "covers": result["covers"],
            "expected": result["expected"]}


# --------------------------------------------------------------------------
# Acquire / open / release
# --------------------------------------------------------------------------

def acquire(queue, *, consumer_action_key: str, attempt: Mapping[str, str],
            tier_id: str, epoch: str, span: Mapping[str, int],
            holder: Mapping[str, object], acquire_token: str,
            covers: list[Mapping[str, str]],
            expected: Mapping[str, Mapping[str, object]] | None = None,
            ram=None, residency_root=None, context: dict | None = None,
            file_pin: bool = True,
            owner_action_key: str | None = None,
            ) -> dict[str, object]:
    """Pin one window's covering material; refuse anything less than published.

    ``consumer_action_key`` names the MATERIAL namespace (the producing
    consumer whose fragments/sidecars vouch); ``owner_action_key`` names
    the pin owner (the running action holding the ref), defaulting to the
    consumer for single-namespace reads.  Produced-output readers pass
    both: the pin files under the owner, the proof resolves in the
    material namespace, and neither is inferred from terminal records.
    Refs stay attempt-bound to the reader in every case.

    ``covers`` names the mover fragments that must hold the window
    (``[{mover_action_key, manifest_sha256}]``) -- one mover for a consumer
    read, several for a promotion whose source window spans stage movers.
    ``expected`` (map key -> ``{bytes, sha256|None}``) demands full-coverage
    proof: every expected key present with equal length (and equal digest
    where the caller declares one); missing coverage refuses
    ``source-coverage-gap`` before any byte is touched, and staged sources
    are taken from the pinned entries only -- never a recomputed pool path
    at the manifest offset.  ``expected=None`` pins the covers' whole union.

    The whole check-and-file runs under the stage root's ownership lock, so
    an egress either sees the pin or this sees the egress's fragment drop
    first (lock order table in the design note: transition -> ownership ->
    rename/unlink; acquire takes ownership only).  ``context`` is an
    optional caller-owned dict caching fragment/material/epoch reads across
    calls in one process, so a window batch does not re-stat over NFS per
    tensor.  Cached reads are a pre-check only: the lock re-reads fragment
    and material fresh, so no cached stale admission proof can pin.  Every
    read of either document, the locked one included, opens the file and
    parses it only when the descriptor's identity differs from the one this
    process last validated (the cover lookup's store, #893), so an acquire
    of unchanged documents parses neither.

    With ``file_pin=False`` nothing is filed: the coverage proof, identity
    stats and staged source names return for the caller to verify its copy
    against (a promotion proves its source window, then its live claim --
    parsed by the egress from the sealed request -- protects the sources
    through the copy; a durable pin would outlive a crashed promotion's
    claim with no terminal to contain it against).  Returns ``{"ok": True,
    "entries": [...], "generations": {...}}`` in that mode.

    Returns ``{"ok": True, "pin_id": ..., "ref_id": ..., "pin": ...}`` or
    ``{"ok": False, "refusal": <typed string>}``.  Never waits and never
    falls back to the pool: bounded waiting lives in the caller's claim
    path with explicit grace, not in here.  Retrying with the same
    ``acquire_token`` reuses the ref.  ``acquire`` proving fast does NOT
    prove no circular wait: repeated consumer retries can still hold all
    space, which is the PRG-04 capacity policy's job, named next.
    """

    from prismabuild import pool as pool_mod

    root = Path(residency_root if residency_root is not None
                else Path(queue.root) / pool_mod.RESIDENCY)
    leases = root / LEASES_SUBDIR
    if context is None:
        context = {}

    movers = [str(cover.get("mover_action_key") or "") for cover in covers]
    if not movers or any(len(mover) != 64 for mover in movers):
        return {"ok": False, "refusal": "ownership-uncertain: bad covers"}
    owner = owner_action_key or consumer_action_key
    if len(owner) != 64 or any(c not in _HEX for c in owner):
        return {"ok": False, "refusal": "ownership-uncertain: bad owner"}

    # Retiring closes one material generation, never a path: a mark for an
    # older generation than the live material is stale -- ignore it and clean
    # it, so a wedged mark cannot block all future generations of the path.
    for mover in movers:
        marks, tainted = retiring_for(leases, mover)
        if tainted:
            return {"ok": False,
                    "refusal": f"ownership-uncertain: {tainted[0]}"}
        context[f"retiring:{mover}"] = [str(mark["generation"]) for mark in marks]

    def cached_material(mover: str):
        # Successes cache; misses never stick: a cached absence would blind
        # later acquires in this context to newly published material, while
        # the lock re-reads fresh before anything pins.  The sidecar is
        # re-read on every call and dates the cached fragment: the stage
        # mover republishes both documents incrementally as entries land
        # under ONE generation per run, so a changed sidecar drops the
        # fragment cache even when the generation is unchanged (#823).
        key = f"material:{consumer_action_key}:{mover}"
        fkey = f"fragment:{consumer_action_key}:{mover}"
        fresh = _reused_cover_doc(root, consumer_action_key, mover, "material")
        if fresh is None or isinstance(fresh, Exception):
            return fresh
        if context.get(key) != fresh:
            context[key] = fresh
            context.pop(fkey, None)
        return context[key]

    def cached_fragment(mover: str):
        # Fragment reuse is valid only for the sidecar that was cached with
        # it: cached_material (always called first per mover) already
        # dropped this entry when the live sidecar changed.
        key = f"fragment:{consumer_action_key}:{mover}"
        if key not in context:
            fragment = _reused_cover_doc(root, consumer_action_key, mover,
                                         "fragment")
            if fragment is None or isinstance(fragment, Exception):
                return fragment
            context[key] = fragment
        return context[key]

    stage_root = ""
    union: dict[str, dict[str, object]] = {}
    generations: dict[str, str] = {}
    for cover in covers:
        mover = str(cover.get("mover_action_key") or "")
        manifest = str(cover.get("manifest_sha256") or "")
        # Material first: a republished sidecar invalidates the fragment
        # cache before it is read (#823).  Check order below is unchanged.
        material = cached_material(mover)
        fragment = cached_fragment(mover)
        if fragment is None:
            return {"ok": False, "refusal": "unpublished"}
        if isinstance(fragment, Exception):
            return {"ok": False, "refusal": f"ownership-uncertain: {fragment}"}
        if str(fragment.get("tier_id") or "") != tier_id:
            return {"ok": False, "refusal": "unpublished"}
        if str(fragment.get("manifest_sha256") or "") != manifest:
            return {"ok": False, "refusal": "unpublished"}
        fragment_epoch = str(fragment.get("epoch") or "")
        if fragment_epoch != str(epoch or ""):
            return {"ok": False, "refusal": "stale-epoch"}
        if tier_id.startswith("ram:"):
            try:
                live = _announced_epoch(queue, tier_id)
            except TierAnnouncementUnreadable as exc:
                # Unknown, not moved (#1146): the window is unavailable for
                # now, which is what ``stale-epoch`` tells a caller; the
                # detail after the colon says why, as with
                # ``ownership-uncertain: ...``.
                return {"ok": False, "refusal": f"stale-epoch: {exc}"}
            if live is None or fragment_epoch != live:
                return {"ok": False, "refusal": "stale-epoch"}
        if material is None:
            # Staged before publish-time identity existed: unqualifiable.
            return {"ok": False, "refusal": "no-file-identity"}
        if isinstance(material, Exception):
            return {"ok": False, "refusal": f"ownership-uncertain: {material}"}
        if str(material.get("generation") or "") in context[f"retiring:{mover}"]:
            return {"ok": False, "refusal": "retiring"}
        if str(material.get("manifest_sha256") or "") != manifest:
            return {"ok": False, "refusal": "unpublished"}
        # Exact sidecar convention: the sidecar dates this fragment's
        # vouching, so their epochs must agree; a staged epoch is
        # corrupt, never a RAM epoch by another name.
        material_epoch = material.get("epoch")
        if tier_id.startswith("ram:"):
            if material_epoch != fragment_epoch:
                return {"ok": False,
                        "refusal": "ownership-uncertain: sidecar/fragment disagree"}
        elif isinstance(material_epoch, str) and material_epoch:
            return {"ok": False,
                    "refusal": "ownership-uncertain: staged epoch set"}
        if not stage_root:
            stage_root = str(fragment.get("stage_root") or "")
        elif str(fragment.get("stage_root") or "") != stage_root:
            return {"ok": False,
                    "refusal": "ownership-uncertain: covers disagree on stage"}
        generations[mover] = str(material.get("generation"))
        material_entries = material.get("entries")
        assert isinstance(material_entries, dict)
        fragment_entries = fragment.get("entries")
        assert isinstance(fragment_entries, dict)
        for key, mention in material_entries.items():
            assert isinstance(mention, dict)
            if str(key) not in fragment_entries:
                # A date no vouch cites is inert (#1087), exactly as in
                # :func:`covers_for_keys`: never pinned, never a refusal.
                continue
            vouched = fragment_entries.get(str(key))
            # The sidecar dates the fragment's vouching: same path, length,
            # digest, or the sidecar is about different bytes.
            if (not isinstance(vouched, Mapping)
                    or str(vouched.get("stage_path") or "")
                    != str(mention.get("stage_path") or "")
                    or vouched.get("bytes") != mention.get("bytes")
                    or str(vouched.get("sha256") or "")
                    != str(mention.get("sha256") or "")):
                return {"ok": False,
                        "refusal": "ownership-uncertain: sidecar/fragment disagree"}
            if str(key) in union and (
                    union[str(key)]["stage_path"]
                    != str(mention.get("stage_path") or "")
                    or union[str(key)]["bytes"] != mention.get("bytes")
                    or union[str(key)]["sha256"]
                    != str(mention.get("sha256") or "")):
                return {"ok": False,
                        "refusal": "ownership-uncertain: covers disagree"}
            union[str(key)] = {
                "stage_path": str(mention["stage_path"]),
                "bytes": mention["bytes"],
                "sha256": str(mention["sha256"]),
                "file_id": dict(mention["file_id"]),  # type: ignore[arg-type]
                "mover_action_key": mover,
                "generation": generations[mover],
            }

    if expected is not None:
        for key, want in expected.items():
            got = union.get(str(key))
            if got is None:
                return {"ok": False, "refusal": "source-coverage-gap"}
            if got["bytes"] != want.get("bytes"):
                return {"ok": False, "refusal": "source-coverage-gap"}
            declared = want.get("sha256")
            if (isinstance(declared, str) and declared
                    and str(got["sha256"]) != declared):
                return {"ok": False, "refusal": "source-coverage-gap"}
        pinned_keys = [str(key) for key in expected]
    else:
        pinned_keys = sorted(union)
        if not pinned_keys:
            # Every date the covers carry is one no vouch cites (#1087):
            # nothing both documents name is published yet.
            return {"ok": False, "refusal": "unpublished"}

    if not stage_root:
        # Without the stage root there is no correct lock key: refuse
        # instead of pinning under a lock nobody else takes.
        return {"ok": False,
                "refusal": "ownership-uncertain: covers name no stage"}
    with queue.stage_ownership_lock(stage_root):
        # Re-validate inside the lock, bypassing the caller context: an
        # egress either filed its fragment drop and retiring mark before
        # this snapshot (seen below) or waits out there until this pin
        # lands.  Cached reads are a pre-check only, never admission proof.
        # Both documents are opened here, under the lock, so each answer is
        # as fresh as a plain read; one whose descriptor shows the identity
        # the process last validated is not parsed again, which keeps the
        # lock's hold short for the movers that wait on it.
        for cover, mover in zip(covers, movers):
            manifest = str(cover.get("manifest_sha256") or "")
            fresh_fragment = _reused_cover_doc(root, consumer_action_key,
                                               mover, "fragment")
            if fresh_fragment is None:
                return {"ok": False, "refusal": "unpublished"}
            if isinstance(fresh_fragment, Exception):
                return {"ok": False,
                        "refusal": f"ownership-uncertain: {fresh_fragment}"}
            if (str(fresh_fragment.get("tier_id") or "") != tier_id
                    or str(fresh_fragment.get("manifest_sha256") or "")
                    != manifest
                    or str(fresh_fragment.get("epoch") or "")
                    != str(epoch or "")):
                return {"ok": False, "refusal": "unpublished"}
            reread = _reused_cover_doc(root, consumer_action_key, mover,
                                       "material")
            if (reread is None or isinstance(reread, Exception)
                    or str(reread.get("generation") or "")
                    != generations[mover]):
                return {"ok": False, "refusal": "unpublished"}
            marks, tainted = retiring_for(leases, mover)
            if tainted:
                return {"ok": False,
                        "refusal": f"ownership-uncertain: {tainted[0]}"}
            if generations[mover] in [str(mark["generation"]) for mark in marks]:
                return {"ok": False, "refusal": "retiring"}
        entries = []
        for key in pinned_keys:
            source = union[key]
            identity = stat_identity(str(source["stage_path"]))
            if identity is None:
                return {"ok": False, "refusal": "file-missing"}
            published = source["file_id"]
            assert isinstance(published, dict)
            if (identity["size"] != source["bytes"]
                    or identity["ino"] != published["ino"]
                    or identity["mtime_ns"] != published["mtime_ns"]
                    or identity["ctime_ns"] != published["ctime_ns"]):
                return {"ok": False, "refusal": "file-identity-changed"}
            entries.append({
                "key": key,
                "stage_path": str(source["stage_path"]),
                "bytes": source["bytes"],
                "sha256": str(source["sha256"]),
                "file_id": identity,
                "mover_action_key": str(source["mover_action_key"]),
                "generation": str(source["generation"]),
            })
        start = int(span.get("start_bytes", 0))
        end = int(span.get("end_bytes", 0))
        if not file_pin:
            # Proof without a pin: the caller verifies its copy against
            # these entries (staged names, digests, generations) while its
            # live claim protects the sources through the copy.
            return {"ok": True, "entries": entries,
                    "generations": dict(generations),
                    "covers": [{"mover_action_key": mover,
                                "generation": generations[mover]}
                               for mover in movers]}
        pin_id = pin_id_for(
            consumer_action_key=consumer_action_key, tier_id=tier_id,
            epoch=str(epoch or ""), stage_root=stage_root,
            start=start, end=end, movers=movers, generations=generations,
            keys={item["key"]: item for item in entries})
        ref_id = ref_id_for(
            acquire_token=acquire_token, host=str(holder.get("host") or ""),
            nonce=str(attempt.get("nonce") or ""),
            scope_id=str(attempt.get("scope_id") or ""))
        directory = leases / owner
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f"{pin_id}.lease.json"
        pin = _read_pin(path)
        if isinstance(pin, Exception):
            return {"ok": False,
                    "refusal": f"ownership-uncertain: {pin}"}
        if pin is not None:
            same_window = (
                str(pin["tier_id"]) == tier_id
                and str(pin["epoch"]) == str(epoch or "")
                and str(pin.get("stage_root") or "") == stage_root
                and str(pin.get("owner_action_key") or "") == owner
                and [(str(cover.get("mover_action_key") or ""),
                      str(cover.get("generation") or ""))
                     for cover in pin["covers"]]  # type: ignore[union-attr]
                == [(mover, generations[mover]) for mover in movers]
                and _entry_identity(pin["entries"])  # type: ignore[index]
                == _entry_identity(entries))
            if not same_window:
                # A new publish superseded the material this pin names, or
                # the keyset changed under a colliding name (impossible for
                # names this function mints, defense in depth): the caller
                # re-resolves and acquires the new generation; the old pin
                # keeps protecting its own readers until they release.
                return {"ok": False, "refusal": "generation-changed"}
            refs_map = pin["refs"]
            assert isinstance(refs_map, dict)
            if ref_id in refs_map:
                held = refs_map[ref_id]
                assert isinstance(held, dict)
                if str(held.get("acquire_token") or "") != acquire_token:
                    return {"ok": False,
                            "refusal": "ownership-uncertain: ref collision"}
                return {"ok": True, "pin_id": pin_id, "ref_id": ref_id,
                        "pin": pin, "duplicate": True}
            refs_map[ref_id] = {
                "acquire_token": acquire_token,
                "attempt": {"nonce": str(attempt.get("nonce") or ""),
                            "scope_id": str(attempt.get("scope_id") or "")},
                "holder": dict(holder),
                "unix": time.time(),
            }
            pin["refs"] = refs_map
        else:
            pin = {
                "schema": LEASE_SCHEMA_V1,
                "pin_id": pin_id,
                "consumer_action_key": consumer_action_key,
                "owner_action_key": owner,
                "tier_id": tier_id,
                "epoch": str(epoch or ""),
                "stage_root": stage_root,
                "manifest_sha256": str(covers[0].get("manifest_sha256") or ""),
                "range": {"coordinate_space": "rng01-source",
                          "start_bytes": start, "end_bytes": end},
                "covers": [{"mover_action_key": mover,
                            "generation": generations[mover]}
                           for mover in movers],
                "entries": entries,
                "ram": ram,
                "refs": {ref_id: {
                    "acquire_token": acquire_token,
                    "attempt": {
                        "nonce": str(attempt.get("nonce") or ""),
                        "scope_id": str(attempt.get("scope_id") or "")},
                    "holder": dict(holder),
                    "unix": time.time(),
                }},
            }
        try:
            _write_pin(path, pin)
        except ReaderLeaseError as exc:
            return {"ok": False,
                    "refusal": f"ownership-uncertain: {exc}"}
        checked = validate_pin(pin)
        # Stale marks for older generations of these movers can never close
        # a future acquire again: drop them now that the live generation is
        # known, so recovery does not need an operator.
        for mover in movers:
            marks, _ = retiring_for(leases, mover)
            for mark in marks:
                if (str(mark["consumer_action_key"]) == consumer_action_key
                        and str(mark["generation"]) != generations[mover]):
                    clear_retiring(leases, consumer_action_key=consumer_action_key,
                                   mover_action_key=mover)
        return {"ok": True, "pin_id": pin_id, "ref_id": ref_id,
                "pin": checked, "duplicate": False}


def _announced_epoch(queue, tier_id: str) -> str | None:
    """The epoch the fleet announces for a ram tier, or ``None`` when none.

    ``None`` means the tier announces no epoch: no announcement at all, or
    one that reads and names none.  An announcement that cannot be read
    raises :class:`TierAnnouncementUnreadable` instead (#1146): an unread
    record says nothing about the epoch, so it must never pass for one that
    moved or vanished.

    The record is a whole-record file the tier loop replaces with
    ``os.replace`` (``PoolQueue.announce_tier``), so it is read as a
    replaced leaf through the #1017 helper: a read that races a replace
    reads again from a fresh resolution.  That helper does not retry the
    faults an NFS client returns while the server swaps the file or is
    loaded (a stale handle, an I/O error, a timeout), nor bytes that do not
    parse, so those are retried here on :data:`RELEASE_RETRY_DELAYS_S`, for
    the errnos of :data:`RELEASE_RETRYABLE_ERRNOS`.  That is this module's
    existing bound for the same faults on the same mount: four reads, about
    a second in all.  The ``epoch`` it checks does not change until the tier
    re-announces, so a longer wait would only delay the answer, and a
    refusal after it still names what failed.  No lock is held while it
    waits: :func:`acquire` reads the announcement before it takes the stage
    ownership lock, and :func:`open_pinned` takes none.

    A missing file is not retried: the writer replaces the name and never
    removes it, so an absent announcement is the record's actual state.
    Any other failure (another errno, a record that is not a regular file,
    one past the byte bound) is not transient and raises at once.
    """

    from prismabuild import core as pb

    record_path = Path(queue.root) / "tiers" / f"{tier_id}.json"
    started = time.monotonic()
    delays = (*RELEASE_RETRY_DELAYS_S, None)
    for attempt, delay in enumerate(delays, start=1):
        code: int | None = None
        try:
            # The bound the other replaced-leaf readers pass for small JSON
            # records; a live announcement is under 2 KiB.
            raw = pb._read_regular_file_nofollow(
                record_path, where="tier announcement",
                max_bytes=pb.MAX_ACTION_PROGRESS_BYTES, replaced_leaf=True)
            record = json.loads(raw)
            if not isinstance(record, Mapping):
                raise ValueError("the announcement is not a JSON object")
        except FileNotFoundError:
            return None
        except (OSError, pb.CASUnavailableError) as exc:
            # The helper wraps an open or stat failure in
            # CASUnavailableError; the errno is on its cause.
            cause = exc if isinstance(exc, OSError) else exc.__cause__
            code = cause.errno if isinstance(cause, OSError) else None
            failure = exc
            transient = code in RELEASE_RETRYABLE_ERRNOS
        except pb.ReplacedRecordError as exc:
            # Replaced under each of the helper's own rereads: still a
            # writer racing the read, never tamper.
            failure, transient = exc, True
        except (pb.CASTamperError, pb.ActionContractError) as exc:
            # Before ValueError: ActionContractError is one, and neither a
            # planted link nor a refused path clears by itself.
            failure, transient = exc, False
        except (ValueError, RecursionError) as exc:
            # Torn or partial bytes: the next read sees the whole record.
            failure, transient = exc, True
        else:
            epoch = record.get("epoch")
            return str(epoch) if isinstance(epoch, str) and epoch else None
        if not transient or delay is None:
            raise TierAnnouncementUnreadable(
                path=str(record_path), errno=code,
                error=f"{type(failure).__name__}: {failure}",
                seconds=round(time.monotonic() - started, 3),
                attempts=attempt) from failure
        time.sleep(delay)
    raise AssertionError("announcement read loop did not return or raise")


def open_pinned(queue, pin: Mapping[str, object], ref_id: str, key: str,
                *, residency_root=None) -> tuple[int, dict[str, object]]:
    """Open one pinned path for the descriptor that will actually be read.

    The opening ref must be live in the authoritative pin file at open
    time: a stale dict from an already-released ref refuses, even when the
    bytes are unchanged -- a released ref pins nothing.  (A release racing
    this check is holder misuse: release-before-close is forbidden, and the
    descriptor fence below still applies.)

    The ``fstat`` of the returned descriptor must equal the pin's identity --
    the descriptor validated is the descriptor read.  A same-path/length
    republish carries a new inode/mtime/ctime (and the pin names the old
    generation), so it refuses instead of silently serving new bytes (ABA).

    Returns ``(fd, serving_tier_record)`` with ID-07
    ``{tier_id, epoch, pin_id, range_ref}`` recorded at open.  The caller
    owns the descriptor and must close it; the pinning ref must outlive it
    (fork inherits via a registered ref; mmap holds it; async prefetch
    holds it).

    A RAM pin's epoch is checked against the tier's announcement.  A
    readable announcement with another epoch, or none, raises
    :class:`ReaderLeaseError` "epoch moved during the hold".  One that is
    still unreadable after the bounded retries raises
    :class:`TierAnnouncementUnreadable` (a ``ReaderLeaseError``) instead:
    the epoch is unknown, not moved, and the pin and ref stay held (#1146).
    """

    checked = validate_pin(pin)
    pin_id = str(checked["pin_id"])
    owner = str(checked["owner_action_key"])
    live = _read_pin(leases_root(queue, residency_root) / owner
                     / f"{pin_id}.lease.json")
    if live is None or isinstance(live, Exception):
        raise ReaderLeaseError("pin is not live: refusing")
    refs_map = live["refs"]
    assert isinstance(refs_map, dict)
    if ref_id not in refs_map:
        raise ReaderLeaseError("opening ref is not live: refusing")
    tier_id = str(live["tier_id"])
    epoch = str(live["epoch"])
    if epoch:
        # A window open needs the lifetime's epoch, not the header's: if
        # the tier re-announced after this acquire, the material generation
        # this pin names is not current, and reporting the old header epoch
        # must not make it so.  An announcement that does not read raises
        # TierAnnouncementUnreadable from here (#1146): the open refuses,
        # but never as a move nobody observed.  A tier that announces no
        # epoch, or another one, no longer serves this pin's generation.
        current = _announced_epoch(queue, tier_id)
        if current is None or current != epoch:
            raise ReaderLeaseError(
                f"tier {tier_id!r} epoch moved during the hold: refusing")
    entries = live["entries"]
    assert isinstance(entries, list)
    match: dict[str, object] | None = None
    for entry in entries:
        assert isinstance(entry, dict)
        if entry.get("key") == key:
            match = entry
            break
    if match is None:
        # Exact key or nothing: serving a wrong key's range under a
        # right-looking pin is never a fallback.
        raise ReaderLeaseError(f"pin covers no such key {key!r}")
    stage_path = str(match["stage_path"])
    file_id = match["file_id"]
    assert isinstance(file_id, dict)
    fd = os.open(stage_path, os.O_RDONLY | os.O_CLOEXEC)
    try:
        info = os.fstat(fd)
        if (info.st_size != int(file_id["size"])
                or info.st_ino != int(file_id["ino"])
                or info.st_mtime_ns != int(file_id["mtime_ns"])
                or int(getattr(info, "st_ctime_ns", 0))
                != int(file_id["ctime_ns"])):
            raise ReaderLeaseError(
                f"{stage_path!r} changed under its pin: refusing")
    except BaseException:
        os.close(fd)
        raise
    serving = {"tier_id": str(checked["tier_id"]),
               "epoch": str(checked["epoch"]),
               "pin_id": str(checked["pin_id"]),
               "range_ref": key}
    return fd, serving


#: The steps of one release, in the order it runs them; a failed release
#: names the one that failed (#1023).  ``census`` lists the leases root when
#: the named consumer's own directory does not hold the pin; ``read`` is the
#: unlocked read that only learns the pin's stage root (skipped when the
#: caller names it); ``lock`` takes that root's ownership lock;
#: ``locked-read`` is the authoritative read under it; ``stage-root`` is a
#: pin whose stage root is still not the locked one after the release has
#: moved to the root the pin names; ``write`` keeps the other holders' refs;
#: ``unlink`` drops the pin with its last ref.
RELEASE_STEPS = ("census", "read", "lock", "locked-read", "stage-root",
                 "write", "unlink")
#: What an NFS client answers for a fault that can clear by itself: a handle
#: the server replaced (the pin is written by rename), an I/O or RPC timeout,
#: a resource that is busy for now.  A release retries these inside the call
#: and, past its bound, answers ``retryable``; every other errno, and a pin
#: that does not validate, is answered at once.
RELEASE_RETRYABLE_ERRNOS = frozenset({
    errno.ESTALE, errno.EIO, errno.ETIMEDOUT, errno.EAGAIN})
#: The waits between one release's attempts: a transient errno is retried
#: once per entry, about a second in all, and never while the stage lock is
#: held.  A caller with a longer horizon (the mount's own, say) retries a
#: ``retryable`` answer itself.
RELEASE_RETRY_DELAYS_S = (0.05, 0.2, 0.8)
#: The tier event a failed release files for its consumer (#1002's file).
RELEASE_FAILED_EVENT = "reader-release-failed"
#: That event's file, ``residency-events/<consumer>/<host>`` + this: beside
#: the tier loop's ``<host>.jsonl`` rather than in it, because that file has
#: one writer, the host's tier loop, and a reader is not it.
RELEASE_EVENTS_SUFFIX = "-reader-release.jsonl"


def _errno_name(code: int | None) -> str | None:
    return None if code is None else errno.errorcode.get(code, str(code))


@dataclass(frozen=True)
class ReleaseFailure:
    """A release that did not happen, which step failed, and why (#1023).

    Falsy, so ``if not release(...)`` still reads it as the refusal it is;
    never ``False``, so a caller that must know why can ask.  ``True`` stays
    the answer for a release that happened and for a ref already gone.

    ``step`` is one of :data:`RELEASE_STEPS`; ``errno`` is the ``OSError``'s,
    or ``None`` when the step failed on something else (a pin that does not
    validate).  ``retryable`` is true only when the last attempt failed with
    one of :data:`RELEASE_RETRYABLE_ERRNOS` and the call's own retries did
    not outlast it.  ``trail`` is every attempt's ``(step, errno)``, oldest
    first.  ``path`` is the pin, or the leases root for a ``census``.
    ``consumer_action_key`` is the consumer the failure is filed under: the
    one the caller named, else the directory holding the pin, else ``None``.
    The ref is held after any failure; a later release drops it exactly once.
    """

    pin_id: str
    ref_id: str
    step: str
    errno: int | None
    error: str
    retryable: bool
    attempts: int
    trail: tuple[tuple[str, int | None], ...]
    path: str
    consumer_action_key: str | None

    def __bool__(self) -> bool:
        return False

    def record(self) -> dict[str, object]:
        """The failure as a JSON-ready record: an event, a receipt, a log line."""

        return {
            "pin_id": self.pin_id, "ref_id": self.ref_id,
            "step": self.step, "errno": self.errno,
            "errno_name": _errno_name(self.errno), "error": self.error,
            "retryable": self.retryable, "attempts": self.attempts,
            "trail": [[step, code] for step, code in self.trail],
            "path": self.path,
            "consumer_action_key": self.consumer_action_key,
        }


class _Fault(NamedTuple):
    """One attempt's failed step."""

    step: str
    errno: int | None
    error: str
    path: str


def _fault(step: str, cause: BaseException | str, path) -> _Fault:
    if isinstance(cause, BaseException):
        code = cause.errno if isinstance(cause, OSError) else None
        return _Fault(step, code, f"{type(cause).__name__}: {cause}",
                      str(path))
    return _Fault(step, None, cause, str(path))


class _Moved(NamedTuple):
    """The locked read names another stage root than the one locked."""

    stage_root: str


#: The pin is not at this path: look in the next place, if any.
_ABSENT = object()
#: The lock refused before its body ran.
_UNSET = object()


def release(queue, pin_id: str, ref_id: str, *,
            consumer_action_key: str | None = None,
            residency_root=None,
            stage_root=None) -> "bool | ReleaseFailure":
    """Drop exactly one ref: reads complete, the window becomes evictable.

    Explicit and idempotent, independent of compute progress -- progress
    counters are never consulted.  The last release unlinks the pin (then
    evictable); it never deletes (the egress does that, exactly once, after
    the delete).  Releasing never touches another holder's ref: two
    acquires need two releases.

    Under the pin's stage-root ownership lock with a fresh read inside it,
    the same guard as acquire and the egress: a release racing an acquire
    or another release is ordered, never lost.  The pin lives under its
    OWNER directory (material namespace and owner split for produced
    output); the named consumer's pin is read directly, and only a pin that
    is not there (``ENOENT``, never another errno) sends the release to a
    census of the whole leases root, since pin and ref ids are globally
    unique and the exact named ref is the only thing ever dropped.

    ``stage_root`` is the root the pin names (``acquire`` returns the pin):
    a caller that passes it skips the unlocked read that only learns it.
    The locked read stays authoritative: when it names another root, the
    release lets this lock go and takes the pin's own (one stage root at a
    time, #780).

    Returns ``True`` for a release that happened and for a ref already
    gone.  Otherwise returns a falsy :class:`ReleaseFailure` naming the
    step that failed and its errno (#1023), and files it for its consumer
    (:data:`RELEASE_FAILED_EVENT`).  A transient errno
    (:data:`RELEASE_RETRYABLE_ERRNOS`) is retried here first, outside the
    lock, :data:`RELEASE_RETRY_DELAYS_S` bounding it; every attempt re-reads
    the pin, so however many run, the ref is dropped exactly once.
    """

    hint = os.path.normpath(str(stage_root)) if stage_root else None
    delays = tuple(RELEASE_RETRY_DELAYS_S)
    trail: list[_Fault] = []
    while True:
        fault = _release_once(queue, str(pin_id), str(ref_id),
                              consumer_action_key=consumer_action_key,
                              residency_root=residency_root, stage_root=hint)
        if fault is None:
            return True
        trail.append(fault)
        if (fault.errno not in RELEASE_RETRYABLE_ERRNOS
                or len(trail) > len(delays)):
            break
        time.sleep(delays[len(trail) - 1])
    last = trail[-1]
    consumer = consumer_action_key
    if consumer is None and last.step != "census":
        consumer = Path(last.path).parent.name or None
    failure = ReleaseFailure(
        pin_id=str(pin_id), ref_id=str(ref_id), step=last.step,
        errno=last.errno, error=last.error,
        retryable=last.errno in RELEASE_RETRYABLE_ERRNOS,
        attempts=len(trail),
        trail=tuple((fault.step, fault.errno) for fault in trail),
        path=last.path, consumer_action_key=consumer)
    _file_release_failure(queue, failure)
    return failure


def _release_once(queue, pin_id: str, ref_id: str, *,
                  consumer_action_key: str | None, residency_root,
                  stage_root: str | None) -> _Fault | None:
    """One attempt: ``None`` once the ref is gone, else the step that failed."""

    root = leases_root(queue, residency_root)
    name = f"{pin_id}.lease.json"
    own = None
    if consumer_action_key is not None:
        own = root / consumer_action_key / name
        outcome = _release_pin(queue, own, ref_id, stage_root)
        if outcome is not _ABSENT:
            return outcome
    try:
        consumers = sorted(entry.name for entry in os.scandir(root)
                           if entry.is_dir())
    except OSError as exc:
        # Unknown absence: retain, never report released.
        return _fault("census", exc, root)
    for consumer in consumers:
        path = root / consumer / name
        if path == own:
            continue
        # Hinted or not, a census finds the pin by an unlocked read of each
        # candidate: locking to look would take the lock once per consumer
        # directory to find one pin.
        outcome = _release_pin(queue, path, ref_id, None)
        if outcome is not _ABSENT:
            return outcome
    return None


def _release_pin(queue, path: Path, ref_id: str, stage_root: str | None):
    """Drop ``ref_id`` from the pin at ``path``: ``None``, ``_ABSENT`` or a fault."""

    hinted = stage_root is not None
    if not hinted:
        first = _read_pin(path)
        if first is None:
            return _ABSENT
        if isinstance(first, Exception):
            return _fault("read", first, path)
        stage_root = str(first["stage_root"])
    for _root in range(2):
        outcome = _locked_drop(queue, path, ref_id, str(stage_root),
                               hinted=hinted)
        if not isinstance(outcome, _Moved):
            return outcome
        stage_root, hinted = outcome.stage_root, False
    return _fault("stage-root",
                  "the pin's stage root moved again under its own lock, "
                  f"to {stage_root!r}", path)


def _locked_drop(queue, path: Path, ref_id: str, stage_root: str, *,
                 hinted: bool):
    """The locked half of one release; the lock's own failure is ``lock``."""

    outcome = _UNSET
    try:
        with queue.stage_ownership_lock(stage_root):
            outcome = _drop_ref(path, ref_id, stage_root, hinted=hinted)
    except OSError as exc:
        if outcome is _UNSET:
            return _fault("lock", exc, path)
        # The act was decided and done under the lock; a lock that then
        # fails to let go still goes with its descriptor's close
        # (``posix_lock.held``), so the act's answer stands.
    return outcome


def _drop_ref(path: Path, ref_id: str, stage_root: str, *, hinted: bool):
    """Under ``stage_root``'s ownership lock: re-read, then drop the one ref."""

    pin = _read_pin(path)
    if pin is None:
        # Seen here before the lock: a last release won the race, already
        # gone.  Not seen (the caller named the root): look elsewhere.
        return _ABSENT if hinted else None
    if isinstance(pin, Exception):
        return _fault("locked-read", pin, path)
    if pin["stage_root"] != stage_root:
        return _Moved(str(pin["stage_root"]))
    refs_map = pin["refs"]
    assert isinstance(refs_map, dict)
    if ref_id not in refs_map:
        return None  # already gone counts as released
    del refs_map[ref_id]
    if refs_map:
        pin["refs"] = refs_map
        try:
            _write_pin(path, pin)
        except (OSError, ReaderLeaseError) as exc:
            return _fault("write", exc, path)
    else:
        try:
            path.unlink(missing_ok=True)
        except OSError as exc:
            return _fault("unlink", exc, path)
    return None


def _file_release_failure(queue, failure: ReleaseFailure) -> None:
    """Say a failed release on stderr, and file it where its consumer's are.

    ``residency-events/<consumer>/<host>-reader-release.jsonl``: beside the
    tier loops' verdicts (#1002), so :meth:`PoolQueue.consumer_events`, a
    kill's ending record and ``pbstatus --starvation`` read it with them,
    and the same sweeps retire it.  Once the file holds
    ``2 * MAX_CONSUMER_EVENT_LINES`` lines it keeps its newest
    ``MAX_CONSUMER_EVENT_LINES``.  Best-effort: a failure with no consumer
    to file under, or a write that fails, is said on stderr only.
    """

    from prismabuild import pool as pool_mod

    host = socket.gethostname()
    record = failure.record()
    consumer = record.pop("consumer_action_key")
    line = json.dumps({"unix": time.time(), "event": RELEASE_FAILED_EVENT,
                       "consumer": consumer, "host": host,
                       "pid": os.getpid(), **record},
                      sort_keys=True, default=str)
    print(f"PrismaBuild: {line}", file=sys.stderr, flush=True)
    if not isinstance(consumer, str):
        return
    try:
        directory = queue.consumer_events_dir(consumer)
    except (AttributeError, ValueError):
        return
    path = directory / f"{host}{RELEASE_EVENTS_SUFFIX}"
    keep = pool_mod.MAX_CONSUMER_EVENT_LINES
    try:
        directory.mkdir(parents=True, exist_ok=True)
        with open(path, "a") as stream:
            stream.write(line + "\n")
        with open(path) as stream:
            lines = stream.readlines()
        if len(lines) >= 2 * keep:
            temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
            with open(temporary, "w") as stream:
                stream.writelines(lines[-keep:])
            os.replace(temporary, path)
    except OSError as exc:
        print(f"PrismaBuild: {RELEASE_FAILED_EVENT} event unwritten: "
              f"{exc!r}", file=sys.stderr, flush=True)


def _pin_candidates(queue, pin_id: str, *,
                    consumer_action_key: str | None = None,
                    residency_root=None) -> tuple[list[Path], bool]:
    """Candidate pin files plus whether the census is complete.

    An unreadable directory is UNKNOWN absence, never proven no-ref:
    callers must retain/refuse on ``complete False``, never report
    released.  The exact-consumer path is always complete (one file).
    """

    root = leases_root(queue, residency_root)
    if consumer_action_key is not None:
        return [root / consumer_action_key / f"{pin_id}.lease.json"], True
    try:
        consumers = sorted(entry.name for entry in os.scandir(root)
                           if entry.is_dir())
    except OSError:
        return [], False
    return ([root / consumer / f"{pin_id}.lease.json"
             for consumer in consumers], True)


def _read_pin(path: Path):
    """A pin file, or ``None`` (absent), or the error (taint)."""

    try:
        with open(path) as stream:
            return validate_pin(json.load(stream))
    except FileNotFoundError:
        return None
    except (OSError, ValueError) as exc:
        return exc


def _write_pin(path: Path, pin: Mapping[str, object]) -> None:
    checked = validate_pin(pin)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with open(tmp, "w") as stream:
        json.dump(checked, stream, sort_keys=True)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(tmp, path)


def register_inherited_ref(queue, pin_id: str, ref_id: str, *,
                           child_holder: Mapping[str, object],
                           child_token: str,
                           consumer_action_key: str | None = None,
                           residency_root=None) -> dict[str, object]:
    """Register a supported process handoff: fork child / async executor.

    Fork shares the parent's descriptor but must not share its ref lifetime:
    an explicit parent release while the child still reads cannot unpin the
    child.  The child (or the forker on its behalf, before the parent may
    release) registers its own ref on the same pin; the two refs release
    independently.  Unregistered inheritance is NOT supported: a process
    holding an unregistered duplicate is contained before any free, and only
    its own release or certified containment moves the pin.

    Every mutation runs under the pin's stage-root ownership lock with a
    fresh read inside it: a parent release racing this registration either
    lands first (this re-reads and appends) or last (it re-reads and keeps
    this ref).  No lost update either way.  Pins live under their OWNER
    directory; an explicit consumer that misses falls back to a full scan.
    Returns ``{"ok": True, "ref_id": ...}`` or ``{"ok": False, ...}``.
    """

    candidates, complete = _pin_candidates(
        queue, pin_id, consumer_action_key=consumer_action_key,
        residency_root=residency_root)
    if (consumer_action_key is not None
            and not any(path.exists() for path in candidates)):
        candidates, complete = _pin_candidates(
            queue, pin_id, consumer_action_key=None,
            residency_root=residency_root)
    if not complete:
        return {"ok": False,
                "refusal": "ownership-uncertain: pin census unreadable"}
    for path in candidates:
        first = _read_pin(path)
        if first is None:
            continue
        if isinstance(first, Exception):
            return {"ok": False, "refusal": f"ownership-uncertain: {first}"}
        stage_root = str(first.get("stage_root") or "")
        if not stage_root:
            return {"ok": False,
                    "refusal": "ownership-uncertain: pin names no stage"}
        with queue.stage_ownership_lock(stage_root):
            pin = _read_pin(path)
            if pin is None:
                continue  # raced a last release; try the next candidate
            if isinstance(pin, Exception):
                return {"ok": False,
                        "refusal": f"ownership-uncertain: {pin}"}
            refs_map = pin["refs"]
            assert isinstance(refs_map, dict)
            parent = refs_map.get(ref_id)
            if parent is None:
                continue  # not this consumer's pin; keep looking
            if not isinstance(parent, dict):
                return {"ok": False,
                        "refusal": "ownership-uncertain: bad ref"}
            parent_attempt = parent.get("attempt")
            parent_nonce = (str(parent_attempt.get("nonce") or "")
                            if isinstance(parent_attempt, Mapping) else "")
            child_ref = ref_id_for(
                acquire_token=child_token,
                host=str(child_holder.get("host") or ""),
                nonce=parent_nonce, scope_id="")
            refs_map[child_ref] = {
                "acquire_token": child_token,
                "attempt": (dict(parent_attempt)
                            if isinstance(parent_attempt, Mapping)
                            else {"nonce": parent_nonce, "scope_id": ""}),
                "holder": dict(child_holder),
                "unix": time.time(),
            }
            pin["refs"] = refs_map
            try:
                _write_pin(path, pin)
            except ReaderLeaseError as exc:
                return {"ok": False, "refusal": f"ownership-uncertain: {exc}"}
            # The linkage rides the receipt/operator view, not the schema:
            # the pin file stays strictly validatable.
            return {"ok": True, "ref_id": child_ref,
                    "inherited_from": ref_id}
    return {"ok": False, "refusal": "unpublished"}


# --------------------------------------------------------------------------
# Injected reader context (SDK): identity from the execution environment
# --------------------------------------------------------------------------

def _read_claim(queue, action_key: str):
    from prismabuild import pool as pool_mod

    try:
        return pool_mod._read_json(
            queue.item_path(pool_mod.CLAIMED, action_key))
    except (OSError, pool_mod.PoolContractError) as exc:
        return exc


def inspect_claim_context(queue, action_key: str) -> dict[str, object]:
    """Legacy inspection of a live claim's identity; never acquiring.

    Reports what the live claim row names (attempt, worker, holder) for
    diagnostics and offline inspection.  The result is UNQUALIFIED: it
    must never back an acquire, because a superseded process reading a
    successor's claim would bind the wrong attempt.  Strict readers use
    :func:`injected_context`, which requires launch-bound identity.
    """

    claim = _read_claim(queue, action_key)
    if isinstance(claim, Exception):
        return {"ok": False, "refusal": f"claim-unreadable: {claim}"}
    if not isinstance(claim, Mapping):
        return {"ok": False, "refusal": "no-claim-context"}
    control = claim.get("resource_scope")
    nonce = ""
    scope_id = ""
    if isinstance(control, Mapping):
        candidate = control.get("nonce")
        if isinstance(candidate, str) and candidate:
            nonce = candidate
        for field in ("scope_id", "scope_unit", "unit"):
            unit = control.get(field)
            if isinstance(unit, str) and unit:
                scope_id = unit
                break
    return {"ok": True, "inspection": {
        "action_key": action_key,
        "nonce": nonce,
        "scope_id": scope_id,
        "worker": claim.get("claimed_by"),
        "qualified": False,
    }}


def launch_queue_root(env=None) -> "Path | None":
    """The root of the queue that launched this action, or ``None`` (#961).

    The supported way for an action to find its queue: the pool launcher
    publishes the root as ``PRISMABUILD_QUEUE_ROOT`` (``core.QUEUE_ROOT_ENV``)
    for every action it runs, so a consumer never re-derives it from where
    the residency map happens to live.  A launch by a pool generation older
    than #961 carries no such variable; for that launch only, the root is
    read from the residency map's path (``<queue>/residency/<key>.json``),
    the layout that generation wrote.  ``None`` when neither is set: this
    process was not launched by a pool worker, and guessing a root from
    topology would bind to the wrong queue.
    """

    source = dict(os.environ) if env is None else dict(env)
    try:
        from prismabuild.core import QUEUE_ROOT_ENV
    except ImportError:
        QUEUE_ROOT_ENV = "PRISMABUILD_QUEUE_ROOT"
    try:
        from prismabuild.residency_map import RESIDENCY_MAP_ENV
    except ImportError:
        RESIDENCY_MAP_ENV = "PRISMABUILD_RESIDENCY_MAP"
    published = source.get(QUEUE_ROOT_ENV) or ""
    if published:
        return Path(published)
    map_path = source.get(RESIDENCY_MAP_ENV) or ""
    if map_path:
        return Path(map_path).parent.parent
    return None


def injected_context(queue=None, *, env=None, residency_root=None):
    """Build this reader's identity from launch-bound sources, never guessed.

    STRICT: requires a COMPLETE launch-bound nonce/scope pair
    (``ACTION_NONCE_ENV``/``ACTION_SCOPE_ENV``, set by the resource_exec
    proxy from the exact launch identity) AND a COMPLETE matching current
    control record (the live claim row's broker-issued ``resource_scope``
    naming the same nonce and scope).  Either half missing, or any
    mismatch, refuses: a strict pin is never bound from a live claim
    alone, and launch values with no matching claim to check against are
    unbound.  Offline inspection without acquiring is
    :func:`inspect_claim_context`.

    The host is the claim's PB-qualified launcher host (fleet alias) via
    the queue's holder resolution, never the container-local hostname.
    The worker -- and the holder incarnation -- is the claim's full
    ``claimed_by`` identity (repository convention
    ``<host>:<pid>:<tag>``: the worker process incarnation that launched
    this attempt, documented here rather than re-derived from a clock).
    No broker token crosses into reader context.  Returns ``{"ok": True,
    "ctx": {...}}`` or ``{"ok": False, "refusal": ...}``.
    """

    from prismabuild import pool as pool_mod

    source = dict(os.environ) if env is None else dict(env)
    try:
        from prismabuild.core import ACTION_KEY_ENV
    except ImportError:
        ACTION_KEY_ENV = "PRISMABUILD_ACTION_KEY"
    try:
        from prismabuild.core import ACTION_NONCE_ENV, ACTION_SCOPE_ENV
    except ImportError:
        ACTION_NONCE_ENV = "PRISMABUILD_ACTION_NONCE"
        ACTION_SCOPE_ENV = "PRISMABUILD_ACTION_SCOPE"
    try:
        from prismabuild.residency_map import RESIDENCY_MAP_ENV
    except ImportError:
        RESIDENCY_MAP_ENV = "PRISMABUILD_RESIDENCY_MAP"
    action_key = source.get(ACTION_KEY_ENV) or ""
    if len(action_key) != 64 or any(
            c not in _HEX for c in action_key):
        return {"ok": False, "refusal": "no-action-context"}
    map_path = source.get(RESIDENCY_MAP_ENV) or ""
    if not map_path:
        return {"ok": False, "refusal": "no-map-context"}
    if queue is None:
        queue = pool_mod.PoolQueue(launch_queue_root(source))
    try:
        claim = pool_mod._read_json(
            queue.item_path(pool_mod.CLAIMED, action_key))
    except (OSError, pool_mod.PoolContractError) as exc:
        return {"ok": False, "refusal": f"claim-unreadable: {exc}"}
    if not isinstance(claim, Mapping):
        return {"ok": False, "refusal": "no-claim-context"}
    control = claim.get("resource_scope")
    claim_nonce = ""
    claim_scope = ""
    if isinstance(control, Mapping):
        candidate = control.get("nonce")
        if isinstance(candidate, str) and candidate:
            claim_nonce = candidate
        # Control record uses scope_id (verified against
        # ResourceScope.control_record); older rows may use scope_unit.
        for field in ("scope_id", "scope_unit", "unit"):
            unit = control.get(field)
            if isinstance(unit, str) and unit:
                claim_scope = unit
                break
    # No intent fallback in the strict path: BOTH fields must come from
    # the complete control record.  An intent nonce synthesizing a
    # missing control nonce would bind an attempt the broker never
    # issued for this scope.  Legacy inspection without acquiring is
    # inspect_claim_context.
    launch_nonce = source.get(ACTION_NONCE_ENV) or ""
    launch_scope = source.get(ACTION_SCOPE_ENV) or ""
    # Strict: a COMPLETE launch-bound pair plus a COMPLETE matching
    # control record.  No live-claim fallback -- binding from whichever
    # claim merely exists is the successor-adoption path.
    if not launch_nonce or not launch_scope:
        return {"ok": False, "refusal": "no-launch-context"}
    if not claim_nonce or not claim_scope:
        return {"ok": False, "refusal": "no-control-context"}
    if claim_nonce != launch_nonce or claim_scope != launch_scope:
        return {"ok": False, "refusal": "attempt-superseded"}
    nonce, scope_id = launch_nonce, launch_scope
    attempt_source = "launch-env"
    worker = claim.get("claimed_by")
    if not isinstance(worker, str) or not worker:
        return {"ok": False, "refusal": "no-worker-context"}
    # Fleet alias from the queue's own holder resolution (ledger, then
    # record, then intent) -- never the container-local hostname: a census
    # for this host must find container readers holding under the alias.
    try:
        host = queue.resolve_claim_holder(action_key, claim)
    except (AttributeError, OSError, ValueError):
        host = None
    if not isinstance(host, str) or not host:
        return {"ok": False, "refusal": "no-host-context"}
    # The holder incarnation IS the full claimed_by identity
    # (<host>:<pid>:<tag>): the worker process incarnation that launched
    # this attempt.  No separate clock ID is invented.
    incarnation = worker
    return {"ok": True, "ctx": {
        "queue_root": str(queue.root),
        "action_key": action_key,
        "nonce": nonce,
        "scope_id": scope_id,
        "worker": worker,
        "host": host,
        "incarnation": incarnation,
        "attempt_source": attempt_source,
        "map_path": map_path,
        "helper_root": str(Path(__file__).resolve().parents[2]),
    }}


def acquire_for(ctx: Mapping[str, object], *, tier_id: str, epoch: str,
                covers: list[Mapping[str, str]],
                expected: Mapping[str, Mapping[str, object]] | None = None,
                span: Mapping[str, int], acquire_token: str,
                ram=None, context: dict | None = None,
                residency_root=None,
                material_namespace: str | None = None,
                file_pin: bool = True) -> dict[str, object]:
    """Acquire as an injected context: identity fixed, data selected.

    The PQ-facing entry point.  Identity (owner, attempt, holder)
    comes only from ``ctx`` (see :func:`injected_context`): the holder
    records the PB-qualified fleet host and worker (plus incarnation
    where published), never the container-local hostname, so
    ``refs_for_holder`` finds container readers under the fleet alias.
    The caller selects data (tier, epoch, covers, expected window) from
    its map lookup.  ``material_namespace`` names the producing consumer
    whose fragments/sidecars vouch (default: the owner itself); the pin
    files under the owner either way, and refs stay attempt-bound to the
    reader -- an output namespace is never treated as the running
    action, and no namespace is inferred from terminal records.
    Exact signature; no invented IDs cross this boundary.
    """

    from prismabuild import pool as pool_mod

    try:
        queue = pool_mod.PoolQueue(Path(str(ctx["queue_root"])))
        attempt = {"nonce": str(ctx["nonce"]),
                   "scope_id": str(ctx["scope_id"])}
        holder: dict[str, object] = {
            "host": str(ctx["host"]),
            "worker": str(ctx["worker"]),
            "pid": os.getpid(),
        }
        if isinstance(ctx.get("incarnation"), str) and ctx.get("incarnation"):
            holder["incarnation"] = str(ctx["incarnation"])
        owner = str(ctx["action_key"])
        namespace = (str(material_namespace) if material_namespace
                     else owner)
    except (KeyError, TypeError, ValueError) as exc:
        return {"ok": False, "refusal": f"bad-context: {exc}"}
    return acquire(
        queue, consumer_action_key=namespace, attempt=attempt,
        tier_id=tier_id, epoch=epoch,
        span=span, holder=holder, acquire_token=acquire_token,
        covers=covers, expected=expected, ram=ram,
        residency_root=residency_root, context=context,
        file_pin=file_pin, owner_action_key=owner)


def adopted_generation(old_material: Mapping[str, object]) -> str:
    """The generation an adoption carries over: stable reuse, new nothing.

    Same bytes without replacement keep their actual generation -- the
    successor dates its vouching with the publish it took over.  A new
    generation is minted only with new bytes (see :func:`mint_generation`).
    Raises :class:`ReaderLeaseError` when the source material is missing
    or unparseable: adoption without proven generation is refused, never
    guessed.
    """

    generation = old_material.get("generation")
    if not isinstance(generation, str) or len(generation) != 32 or any(
            c not in _HEX for c in generation):
        raise ReaderLeaseError("adoption needs the source material generation")
    entries = old_material.get("entries")
    if not isinstance(entries, Mapping) or not entries:
        raise ReaderLeaseError("adoption needs the source material entries")
    return generation


__all__ = [
    "LEASE_SCHEMA_V1",
    "RETIRING_SCHEMA_V1",
    "MATERIAL_SCHEMA_V1",
    "ATTESTATION_SCHEMA_V1",
    "LEASES_SUBDIR",
    "MATERIAL_SUBDIR",
    "ATTESTATIONS_SUBDIR",
    "READER_LEASE_TAG",
    "ReaderLeaseError",
    "TierAnnouncementUnreadable",
    "ReleaseFailure",
    "RELEASE_STEPS",
    "RELEASE_RETRYABLE_ERRNOS",
    "RELEASE_RETRY_DELAYS_S",
    "RELEASE_FAILED_EVENT",
    "RELEASE_EVENTS_SUFFIX",
    "acquire",
    "attestation_path",
    "attestation_proves_empty",
    "attempt_refs_live",
    "clear_retiring",
    "containment_certificate_ok",
    "export_verdict_proves_empty",
    "launch_queue_root",
    "leases_root",
    "live_for",
    "material_path",
    "mint_generation",
    "open_pinned",
    "pin_id_for",
    "portable_identity",
    "ref_id_for",
    "refs_for_holder",
    "register_inherited_ref",
    "release",
    "release_refs",
    "retiring_for",
    "retiring_path",
    "stat_identity",
    "file_id_matches",
    "timestamp_only_mismatch",
    "content_identity",
    "validate_material",
    "validate_pin",
    "validate_retiring",
    "write_material",
    "write_retiring",
    "read_scope_attestation",
    "injected_context",
    "inspect_claim_context",
    "covers_for_keys",
    "resolve_window_covers",
    "acquire_for",
    "adopted_generation",
]
