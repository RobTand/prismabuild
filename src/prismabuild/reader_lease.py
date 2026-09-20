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

from collections.abc import Mapping
import hashlib
import json
import os
from pathlib import Path
import time
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


class ReaderLeaseError(ValueError):
    """A pin, a retiring mark, material, or a pinned open that refuses."""


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
            or any(c not in _HEX for c in value)):
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


def pin_id_for(*, consumer_action_key: str, tier_id: str, epoch: str,
               start: int, end: int, movers: list[str],
               generations: Mapping[str, str]) -> str:
    """Deterministic pin name for a window generation: one file, many refs.

    The material generations join the name, so a republish files a new pin
    beside the old one instead of joining (or aliasing) it: the old pin
    keeps protecting its own readers until they release.
    """

    window = "|".join((consumer_action_key, tier_id, epoch, str(start),
                       str(end), ",".join(sorted(movers))))
    gens = ",".join(f"{mover}={generations[mover]}" for mover in sorted(movers))
    return hashlib.sha256((window + "|" + gens).encode("utf-8")).hexdigest()[:32]


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
        "schema", "pin_id", "consumer_action_key", "tier_id", "epoch",
        "manifest_sha256", "range", "covers", "entries", "ram", "refs",
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
    return {
        "schema": LEASE_SCHEMA_V1,
        "pin_id": str(value.get("pin_id") or ""),
        "consumer_action_key": _hex(
            value.get("consumer_action_key"), 64,
            where="pin consumer_action_key"),
        "tier_id": str(value.get("tier_id") or ""),
        "epoch": str(value.get("epoch") or ""),
        "manifest_sha256": str(value.get("manifest_sha256") or ""),
        "range": {"start_bytes": span.get("start_bytes"),
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

def live_for(queue, wanted: set[str] | None, *, residency_root=None
             ) -> tuple[dict[str, list[str]], list[str]]:
    """Which of ``wanted`` paths a live pin refs (``None``: all pinned paths).

    Returns ``(owners, tainted)`` mapping normalized stage path to pin ids.
    Retiring marks and ``*.tmp`` temporaries are never pins.  Anything else
    unreadable or invalid taints the pass: its paths are unknowable, so the
    egress deletes nothing and frees nothing on that pass.
    """

    root = leases_root(queue, residency_root)
    owners: dict[str, list[str]] = {}
    tainted: list[str] = []
    try:
        consumers = sorted(entry.name for entry in os.scandir(root)
                           if entry.is_dir())
    except OSError:
        return owners, []
    for consumer in consumers:
        directory = root / consumer
        try:
            names = sorted(entry.name for entry in os.scandir(directory)
                           if entry.is_file() and entry.name.endswith(".json"))
        except OSError as exc:
            tainted.append(f"{consumer}: {exc}")
            continue
        for name in names:
            if name.endswith(".retiring.json") or name.endswith(".tmp"):
                continue
            if not name.endswith(".lease.json"):
                tainted.append(f"{consumer}/{name}: not a pin file")
                continue
            try:
                with open(directory / name) as stream:
                    pin = validate_pin(json.load(stream))
            except (OSError, ValueError) as exc:
                tainted.append(f"{consumer}/{name}: {exc}")
                continue
            pin_id = str(pin["pin_id"])
            entries = pin["entries"]
            assert isinstance(entries, list)
            for entry in entries:
                assert isinstance(entry, dict)
                path = os.path.normpath(str(entry["stage_path"]))
                if wanted is not None and path not in wanted:
                    continue
                owners.setdefault(path, []).append(pin_id)
    return owners, tainted


def refs_for_holder(queue, host: str, *, residency_root=None
                    ) -> list[dict[str, object]]:
    """Census of one host's refs -- the containment input, read-only.

    The membership worker's RESIGN path calls this to learn what must drain.
    Never a reaper trigger on its own; PIDs named here are diagnostics, not
    proof of anything across hosts.
    """

    root = leases_root(queue, residency_root)
    out: list[dict[str, object]] = []
    try:
        consumers = sorted(entry.name for entry in os.scandir(root)
                           if entry.is_dir())
    except OSError:
        return out
    for consumer in consumers:
        directory = root / consumer
        try:
            names = sorted(entry.name for entry in os.scandir(directory)
                           if entry.is_file() and entry.name.endswith(".lease.json"))
        except OSError:
            continue
        for name in names:
            try:
                with open(directory / name) as stream:
                    pin = validate_pin(json.load(stream))
            except (OSError, ValueError):
                continue
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


def write_scope_attestation(queue, *, action_key: str, nonce: str,
                            scope_id: str, host: str, worker: str,
                            incarnation: str, scope_empty: bool) -> Path:
    """File the broker's scope verdict for one attempt (broker calls this).

    ``scope_empty`` True means the broker proved every process of the scope
    stopped (container/cgroup gone, not a PID guess).  Atomic rename, so a
    reader never sees half a verdict.
    """

    payload = {"schema": ATTESTATION_SCHEMA_V1,
               "action_key": action_key, "nonce": nonce,
               "scope_id": scope_id, "host": host, "worker": worker,
               "incarnation": incarnation, "scope_empty": bool(scope_empty),
               "unix": time.time()}
    if (not isinstance(action_key, str) or len(action_key) != 64
            or not isinstance(nonce, str) or not nonce):
        raise ReaderLeaseError("attestation needs an action key and nonce")
    path = attestation_path(queue, action_key, nonce)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with open(tmp, "w") as stream:
        json.dump(payload, stream, sort_keys=True)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(tmp, path)
    return path


def read_scope_attestation(queue, action_key: str, nonce: str):
    """A broker attestation, or ``None`` (absent), or the error (taint)."""

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


def _terminal_attempt_ids(record: Mapping[str, object]) -> list[tuple[str, str]]:
    """Attempt (nonce, scope) identities a terminal record carries, if any."""

    out: list[tuple[str, str]] = []
    scope = record.get("resource_scope")
    if isinstance(scope, Mapping):
        nonce = scope.get("nonce")
        if isinstance(nonce, str) and nonce:
            unit = scope.get("unit")
            out.append((nonce, str(unit) if isinstance(unit, str) else ""))
    for field in ("nonce", "attempt_nonce"):
        nonce = record.get(field)
        if isinstance(nonce, str) and nonce:
            out.append((nonce, str(record.get("scope_id") or "")))
    return out


def _history_attempt_terminal(queue, action_key: str, nonce: str
                              ) -> tuple[bool | None, str]:
    """Whether attempt history names ``nonce`` terminally.

    Returns ``(True, where)`` (terminal evidence for this attempt),
    ``(False, where)`` (history names it non-terminally -- a newer attempt
    is current), or ``(None, reason)`` (history unanswerable).
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
            found = _terminal_attempt_ids(outcome)
            record_nonce = outcome.get("nonce")
            if (isinstance(record_nonce, str) and record_nonce
                    and (record_nonce, "") not in found):
                found.append((record_nonce, ""))
            for attempt_nonce, _scope in found:
                if attempt_nonce != nonce:
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
    scope_id, worker?, incarnation?, host?}``.  Proof comes from two
    authoritative reads: the broker attestation file for
    ``(action_key, nonce)`` (must exist, ``scope_empty`` true, identifiers
    matching), and terminal evidence for the exact attempt -- the queue's
    terminal record for the action when it names this attempt, else the
    attempt history.  A terminal record naming a DIFFERENT attempt (a newer
    attempt is current) refuses: the attempt is superseded, not contained.
    Anything unanswerable retains with a reason.
    """

    from prismabuild import pool as pool_mod

    action_key = certificate.get("action_key")
    nonce = certificate.get("nonce")
    scope_id = str(certificate.get("scope_id") or "")
    if (not isinstance(action_key, str) or len(action_key) != 64
            or not isinstance(nonce, str) or not nonce):
        return False, "certificate names no attempt"
    attestation = read_scope_attestation(queue, action_key, nonce)
    if attestation is None:
        return False, "no-broker-attestation-retain"
    if isinstance(attestation, Exception):
        return False, f"broker-attestation-unreadable-retain: {attestation}"
    if attestation.get("scope_empty") is not True:
        return False, "scope-not-empty-retain"
    if scope_id and str(attestation.get("scope_id") or "") != scope_id:
        return False, "attestation-scope-mismatch-retain"
    for field in ("worker", "incarnation", "host"):
        wanted = certificate.get(field)
        if (isinstance(wanted, str) and wanted
                and str(attestation.get(field) or "") != wanted):
            return False, f"attestation-{field}-mismatch-retain"
    terminal_state = None
    terminal_record = None
    for state in (pool_mod.DONE, pool_mod.FAILED, pool_mod.WITHDRAWN):
        try:
            record = pool_mod._read_json(queue.item_path(state, action_key))
        except (OSError, pool_mod.PoolContractError):
            return False, f"terminal record unreadable: {state}"
        if record is not None:
            terminal_state, terminal_record = state, record
            break
    if terminal_record is not None:
        carried = _terminal_attempt_ids(terminal_record)
        if carried and all(attempt_nonce != nonce for attempt_nonce, _ in carried):
            # The terminal record belongs to another attempt (a retry is
            # current): this attempt is superseded, its refs stay until its
            # own terminal evidence exists.
            return False, "terminal-names-newer-attempt-retain"
        if not carried:
            historic, _where = _history_attempt_terminal(
                queue, action_key, nonce)
            if historic is not True:
                return False, "no-terminal-evidence-retain"
    else:
        historic, _where = _history_attempt_terminal(queue, action_key, nonce)
        if historic is not True:
            return False, "no-terminal-evidence-retain"
    host = str(attestation.get("host") or "")
    return True, f"contained-terminal-{terminal_state or 'history'}:{host}"


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
    """

    ok, reason = containment_certificate_ok(queue, certificate)
    root = leases_root(queue, residency_root)
    released: list[str] = []
    skipped: list[str] = []
    if not ok:
        return {"ok": False, "reason": reason, "released": released,
                "skipped": [str(ref.get("ref_id", "?")) for ref in refs]}
    attempt_nonce = str(certificate.get("nonce") or "")
    attempt_scope = str(certificate.get("scope_id") or "")
    for ref in refs:
        consumer = str(ref.get("consumer_action_key") or "")
        pin_id = str(ref.get("pin_id") or "")
        ref_id = str(ref.get("ref_id") or "")
        path = root / consumer / f"{pin_id}.lease.json"
        try:
            with open(path) as stream:
                pin = validate_pin(json.load(stream))
        except FileNotFoundError:
            released.append(ref_id)  # already gone counts as released
            continue
        except (OSError, ValueError) as exc:
            skipped.append(f"{ref_id}: {exc}")
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
        if (attempt_nonce and str(held_attempt.get("nonce")) != attempt_nonce):
            skipped.append(f"{ref_id}: attempt mismatch")
            continue
        if (attempt_scope
                and str(held_attempt.get("scope_id")) != attempt_scope):
            skipped.append(f"{ref_id}: scope mismatch")
            continue
        del refs_map[ref_id]
        released.append(ref_id)
        if refs_map:
            pin["refs"] = refs_map
            tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
            with open(tmp, "w") as stream:
                json.dump(validate_pin(pin), stream, sort_keys=True)
                stream.write("\n")
            os.replace(tmp, path)
        else:
            path.unlink(missing_ok=True)
    return {"ok": not skipped, "reason": reason, "released": released,
            "skipped": skipped}


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
            ) -> dict[str, object]:
    """Pin one window's covering material; refuse anything less than published.

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
    and material fresh, so no cached stale admission proof can pin.

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
    from prismabuild import residency_map as map_mod

    root = Path(residency_root if residency_root is not None
                else Path(queue.root) / pool_mod.RESIDENCY)
    leases = root / LEASES_SUBDIR
    if context is None:
        context = {}

    movers = [str(cover.get("mover_action_key") or "") for cover in covers]
    if not movers or any(len(mover) != 64 for mover in movers):
        return {"ok": False, "refusal": "ownership-uncertain: bad covers"}

    # Retiring closes one material generation, never a path: a mark for an
    # older generation than the live material is stale -- ignore it and clean
    # it, so a wedged mark cannot block all future generations of the path.
    for mover in movers:
        marks, tainted = retiring_for(leases, mover)
        if tainted:
            return {"ok": False,
                    "refusal": f"ownership-uncertain: {tainted[0]}"}
        context[f"retiring:{mover}"] = [str(mark["generation"]) for mark in marks]

    def cached_fragment(mover: str):
        key = f"fragment:{consumer_action_key}:{mover}"
        if key not in context:
            try:
                with open(map_mod.fragment_path(
                        root, consumer_action_key, mover)) as stream:
                    context[key] = map_mod.validate_fragment(json.load(stream))
            except FileNotFoundError:
                context[key] = None
            except (OSError, ValueError) as exc:
                context[key] = exc
        return context[key]

    def cached_material(mover: str):
        key = f"material:{consumer_action_key}:{mover}"
        if key not in context:
            context[key] = read_material(root, consumer_action_key, mover)
        return context[key]

    stage_root = ""
    union: dict[str, dict[str, object]] = {}
    generations: dict[str, str] = {}
    for cover in covers:
        mover = str(cover.get("mover_action_key") or "")
        manifest = str(cover.get("manifest_sha256") or "")
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
            live = _announced_epoch(queue, tier_id)
            if live is None or fragment_epoch != live:
                return {"ok": False, "refusal": "stale-epoch"}
        material = cached_material(mover)
        if material is None:
            # Staged before publish-time identity existed: unqualifiable.
            return {"ok": False, "refusal": "no-file-identity"}
        if isinstance(material, Exception):
            return {"ok": False, "refusal": f"ownership-uncertain: {material}"}
        if str(material.get("generation") or "") in context[f"retiring:{mover}"]:
            return {"ok": False, "refusal": "retiring"}
        if str(material.get("manifest_sha256") or "") != manifest:
            return {"ok": False, "refusal": "unpublished"}
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

    with queue.stage_ownership_lock(stage_root or "/"):
        # Re-validate inside the lock, bypassing the caller context: an
        # egress either filed its fragment drop and retiring mark before
        # this snapshot (seen below) or waits out there until this pin
        # lands.  Cached reads are a pre-check only, never admission proof.
        for cover, mover in zip(covers, movers):
            manifest = str(cover.get("manifest_sha256") or "")
            try:
                with open(map_mod.fragment_path(
                        root, consumer_action_key, mover)) as stream:
                    fresh_fragment = map_mod.validate_fragment(
                        json.load(stream))
            except FileNotFoundError:
                return {"ok": False, "refusal": "unpublished"}
            except (OSError, ValueError) as exc:
                return {"ok": False,
                        "refusal": f"ownership-uncertain: {exc}"}
            if (str(fresh_fragment.get("tier_id") or "") != tier_id
                    or str(fresh_fragment.get("manifest_sha256") or "")
                    != manifest
                    or str(fresh_fragment.get("epoch") or "")
                    != str(epoch or "")):
                return {"ok": False, "refusal": "unpublished"}
            reread = read_material(root, consumer_action_key, mover)
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
            epoch=str(epoch or ""), start=start, end=end, movers=movers,
            generations=generations)
        ref_id = ref_id_for(
            acquire_token=acquire_token, host=str(holder.get("host") or ""),
            nonce=str(attempt.get("nonce") or ""),
            scope_id=str(attempt.get("scope_id") or ""))
        directory = leases / consumer_action_key
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f"{pin_id}.lease.json"
        try:
            with open(path) as stream:
                pin = validate_pin(json.load(stream))
            same_window = (
                str(pin["tier_id"]) == tier_id
                and str(pin["epoch"]) == str(epoch or "")
                and [(str(cover.get("mover_action_key") or ""),
                      str(cover.get("generation") or ""))
                     for cover in pin["covers"]]  # type: ignore[union-attr]
                == [(mover, generations[mover]) for mover in movers])
            if not same_window:
                # A new publish superseded the material this pin names: the
                # caller re-resolves and acquires the new generation; the old
                # pin keeps protecting its own readers until they release.
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
        except FileNotFoundError:
            pin = {
                "schema": LEASE_SCHEMA_V1,
                "pin_id": pin_id,
                "consumer_action_key": consumer_action_key,
                "tier_id": tier_id,
                "epoch": str(epoch or ""),
                "manifest_sha256": str(covers[0].get("manifest_sha256") or ""),
                "range": {"start_bytes": start, "end_bytes": end},
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
        checked = validate_pin(pin)
        tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
        with open(tmp, "w") as stream:
            json.dump(checked, stream, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp, path)
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
    """The epoch the fleet announces for a ram tier, or ``None`` when none."""

    try:
        record_path = Path(queue.root) / "tiers" / f"{tier_id}.json"
        with open(record_path) as stream:
            record = json.load(stream)
    except (OSError, ValueError):
        return None
    if not isinstance(record, Mapping):
        return None
    epoch = record.get("epoch")
    return str(epoch) if isinstance(epoch, str) and epoch else None


def open_pinned(pin: Mapping[str, object], key: str
                ) -> tuple[int, dict[str, object]]:
    """Open one pinned path for the descriptor that will actually be read.

    The ``fstat`` of the returned descriptor must equal the pin's identity --
    the descriptor validated is the descriptor read.  A same-path/length
    republish carries a new inode/mtime/ctime (and the pin names the old
    generation), so it refuses instead of silently serving new bytes (ABA).

    Returns ``(fd, serving_tier_record)`` with ID-07
    ``{tier_id, epoch, pin_id, range_ref}`` recorded at open.  The caller
    owns the descriptor and must close it; the pinning ref must outlive it
    (fork inherits the ref; mmap holds it; async prefetch holds it).
    """

    checked = validate_pin(pin)
    entries = checked["entries"]
    assert isinstance(entries, list)
    match: dict[str, object] | None = None
    for entry in entries:
        assert isinstance(entry, dict)
        if entry.get("key") == key:
            match = entry
            break
    if match is None and len(entries) == 1:
        # Single-window pins filed by hand or legacy writers name no key.
        match = entries[0]  # type: ignore[assignment]
    if match is None:
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


def release(queue, pin_id: str, ref_id: str, *,
            consumer_action_key: str | None = None,
            residency_root=None) -> bool:
    """Drop exactly one ref: reads complete, the window becomes evictable.

    Explicit and idempotent, independent of compute progress -- progress
    counters are never consulted.  The last release unlinks the pin (then
    evictable); it never deletes (the egress does that, exactly once, after
    the delete).  Releasing never touches another holder's ref: two
    acquires need two releases.
    """

    root = leases_root(queue, residency_root)
    if consumer_action_key is not None:
        candidates = [root / consumer_action_key / f"{pin_id}.lease.json"]
    else:
        candidates = []
        try:
            consumers = sorted(entry.name for entry in os.scandir(root)
                               if entry.is_dir())
        except OSError:
            return True
        for consumer in consumers:
            candidates.append(root / consumer / f"{pin_id}.lease.json")
    for path in candidates:
        try:
            with open(path) as stream:
                pin = validate_pin(json.load(stream))
        except FileNotFoundError:
            continue
        except (OSError, ValueError):
            return False
        refs_map = pin["refs"]
        assert isinstance(refs_map, dict)
        if ref_id not in refs_map:
            return True  # already gone counts as released
        del refs_map[ref_id]
        if refs_map:
            pin["refs"] = refs_map
            tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
            with open(tmp, "w") as stream:
                json.dump(validate_pin(pin), stream, sort_keys=True)
                stream.write("\n")
            os.replace(tmp, path)
        else:
            path.unlink(missing_ok=True)
        return True
    return True


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

    Runs under the stage root's ownership lock (pins live beside fragments
    whose root this resolves from the pin file's own directory).
    Returns ``{"ok": True, "ref_id": ...}`` or ``{"ok": False, ...}``.
    """

    root = leases_root(queue, residency_root)
    candidates = ([root / consumer_action_key / f"{pin_id}.lease.json"]
                  if consumer_action_key is not None else
                  sorted(root.glob(f"*/{pin_id}.lease.json")))
    for path in candidates:
        try:
            with open(path) as stream:
                pin = validate_pin(json.load(stream))
        except FileNotFoundError:
            continue
        except (OSError, ValueError) as exc:
            return {"ok": False, "refusal": f"ownership-uncertain: {exc}"}
        refs_map = pin["refs"]
        assert isinstance(refs_map, dict)
        parent = refs_map.get(ref_id)
        if parent is None:
            continue  # not this consumer's pin; keep looking
        if not isinstance(parent, dict):
            return {"ok": False, "refusal": "ownership-uncertain: bad ref"}
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
            checked = validate_pin(pin)
        except ReaderLeaseError as exc:
            return {"ok": False, "refusal": f"ownership-uncertain: {exc}"}
        tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
        with open(tmp, "w") as stream:
            json.dump(checked, stream, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp, path)
        # The linkage rides the receipt/operator view, not the schema: the
        # pin file stays strictly validatable.
        return {"ok": True, "ref_id": child_ref, "inherited_from": ref_id}
    return {"ok": False, "refusal": "unpublished"}


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
    "acquire",
    "attestation_path",
    "clear_retiring",
    "containment_certificate_ok",
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
    "validate_material",
    "validate_pin",
    "validate_retiring",
    "write_material",
    "write_retiring",
    "write_scope_attestation",
    "read_scope_attestation",
]
