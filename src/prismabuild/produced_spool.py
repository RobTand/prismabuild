"""Bounded producer-local PRECOMMIT spool and ordinary PB export actions.

No staged-read authority lives here. A verified durable export acknowledgement
permits the caller to use its existing canonical produced-output descriptors.
Pending local bytes are never committed units or canonical origin identities.
A write-only template's producer (#912) commits an acknowledged group at its
origin through `ProducedSpool.commit_origin_group`, against the identities the
acknowledgement recorded.  So does a read-back owner for a group it reads back
from this spool and never publishes (#1034).
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import socket
import stat
import time

from . import core, movement_actions, pool, produced_output as po, reader_lease
from . import adaptive_cpu, storage_tiers

API_VERSION = 1
ROOT_ENV = "PRISMABUILD_PRODUCED_SPOOL_ROOT"
MAX_ENV = "PRISMABUILD_PRODUCED_SPOOL_MAX_BYTES"
#: Opt-in for the paced export (#747).  Only ``"1"`` in the producer's sealed
#: environment makes an export reserve its tier's fill and hold its write rate
#: to it; absent, empty or ``"0"`` leaves every export unreserved and unpaced,
#: so publishing a runtime that carries the pacer changes no live export.
PACED_EXPORT_ENV = "PRISMABUILD_PRODUCED_SPOOL_PACED_EXPORT"
#: Opt-in for the host spool window (#747).  Only ``"1"`` in the producer's
#: sealed environment turns the owner's :data:`MAX_ENV` window into a
#: :data:`HOST_WINDOW_KIND` reservation on the host ledger, derived by pbrun
#: and charged at claim against the budget a box declares with
#: ``worker_loop.py --spool-gb``.  Absent, empty or ``"0"`` derives no demand
#: and checks nothing, so publishing a runtime that carries it changes no
#: live producer.
HOST_WINDOW_ENV = "PRISMABUILD_PRODUCED_SPOOL_HOST_WINDOW"
#: The host kind the window is reserved in, in GiB like ``mem_gb``.
HOST_WINDOW_KIND = "spool_gb"
SCHEMA = "prismabuild.produced_spool.v1"
PACING_SCHEMA = "prismabuild.produced_spool.pacing.v1"
#: The copy loop's block, and so the pacer's step: one read, one write.
BLOCK_BYTES = 1 << 20
#: A group's record of the failed groups' files its export has begun to
#: retire (#1097): each path and the identity retired, so a later run of the
#: export finishes a delete an interruption left at its private name.
RETIRING_MARKER = "retiring.json"


class SpoolError(RuntimeError):
    pass


def _identity_evidence(code, *, path, recorded, observed, entry_index=None, detail=None):
    """What an identity refusal compared: the entry, the path, both identities (#1098)."""

    evidence = {"code": code, "entry_index": entry_index, "path": str(path),
                "recorded": recorded, "observed": observed}
    if detail:
        evidence["detail"] = str(detail)
    return evidence


class SpoolIdentityRefusal(SpoolError):
    """An identity refusal, with the entry and both identities it compared (#1098).

    ``code`` is the refusal's name as it was before #1098, so an answer's
    ``refusal`` and a match on the start of the message read as before.
    ``evidence`` names the entry, the path, the identity recorded and the
    identity observed, and the message carries all of them.
    """

    def __init__(self, code, *, path, recorded, observed, entry_index=None, detail=None):
        self.code = code
        self.evidence = _identity_evidence(code, path=path, recorded=recorded,
                                           observed=observed, entry_index=entry_index,
                                           detail=detail)
        entry = f"entry {entry_index} " if entry_index is not None else ""
        text = f"{code}: {entry}{path}: recorded {recorded!r}, observed {observed!r}"
        super().__init__(f"{text} ({detail})" if detail else text)


def _observe(path, *, directory=None):
    """A path's identity now, or why there is none: an identity refusal's observed side."""

    try:
        return _identity(path, directory=directory)
    except FileNotFoundError:
        return "absent"
    except (OSError, SpoolError) as exc:
        return f"unreadable: {exc}"


class SpoolCapacityDeferred(SpoolError):
    """Only the bounded local spool is full; completed exports may free it."""


class _RepinNeeded(Exception):
    """A check without the group's ``.export.lock`` met a timestamp-only mismatch.

    Not a refusal: the caller takes the lock and checks again, and only
    under the lock is the content verified and the re-pin written (#1096).
    """


#: Why a re-pin was made; the only reason there is.
REPIN_REASON = ("timestamps-only mismatch: same inode and size, content sha256 "
                "verified (#1096, an NFS delegation recall)")
_REPIN_FIELDS = frozenset({"from", "to", "reason", "sha256", "bytes", "rehash_s",
                           "reads", "host", "unix", "where"})


def _repinned(proof, observed, *, code, index, path, name, directory, where):
    """``proof`` re-pinned to the file's identity now, when only its timestamps moved (#1096).

    ``observed`` is the identity a check found that does not match
    ``proof["identity"]``.  When the two differ in anything but
    ``mtime_ns``/``ctime_ns``, the check refuses as before.  Otherwise the
    file is read through ``directory`` and hashed
    (`reader_lease.content_identity`).  Its digest must be the proof's
    ``sha256``, the declared digest of the entry, and its inode and size the
    recorded ones; then the returned proof carries the hashed identity, and
    a ``repinned`` record naming the identity it replaces, the one it pins,
    the reason, the seconds the read took and where it was made.  Any other
    answer is ``code``'s refusal, naming the entry and both identities.
    The caller writes the returned proof into the record it checked.
    """

    recorded = proof["identity"]

    def refuse(seen, detail=None):
        return SpoolIdentityRefusal(code, entry_index=index, path=path,
                                    recorded=recorded, observed=seen, detail=detail)

    if not reader_lease.timestamp_only_mismatch(recorded, observed):
        raise refuse(observed)
    try:
        read = reader_lease.content_identity(name, dir_fd=directory)
    except (OSError, reader_lease.ReaderLeaseError) as exc:
        raise refuse(observed, f"only the timestamps moved, and the content "
                               f"could not be verified: {exc}") from None
    now = read["identity"]
    if (now["ino"], now["size"]) != (recorded["ino"], recorded["size"]):
        raise refuse(now, "the file changed while its content was verified")
    if read["sha256"] != proof["sha256"]:
        raise refuse(now, f"only the timestamps moved, but the content sha256 "
                          f"{read['sha256']} is not the recorded {proof['sha256']}")
    repin = {"from": dict(recorded), "to": now, "reason": REPIN_REASON,
             "sha256": read["sha256"], "bytes": read["bytes"],
             "rehash_s": read["seconds"], "reads": read["reads"],
             "host": socket.gethostname(), "unix": round(time.time(), 3),
             "where": where}
    return {**proof, "identity": now,
            "repinned": [*proof.get("repinned", []), repin]}


def _check_repins(proof):
    """A proof's re-pins must chain from its landed identity to its identity now (#1096)."""

    repins = proof["repinned"]
    if not isinstance(repins, list) or not repins:
        raise SpoolError("copy proof re-pin record is corrupt")
    for repin in repins:
        if (not isinstance(repin, dict) or set(repin) != _REPIN_FIELDS
                or repin["reason"] != REPIN_REASON
                or repin["sha256"] != proof.get("sha256")):
            raise SpoolError("copy proof re-pin record is corrupt")
        before = reader_lease._check_identity(repin["from"], where="re-pinned from")
        after = reader_lease._check_identity(repin["to"], where="re-pinned to")
        if not reader_lease.timestamp_only_mismatch(before, after):
            raise SpoolError("copy proof re-pin changes more than timestamps")
    for earlier, later in zip(repins, repins[1:]):
        if earlier["to"] != later["from"]:
            raise SpoolError("copy proof re-pins do not chain")
    if repins[-1]["to"] != proof.get("identity"):
        raise SpoolError("copy proof identity is not its last re-pin")


@contextmanager
def _parent(path, *, create=False):
    path = Path(path)
    descriptor = core._open_directory_nofollow(
        path.parent, where="produced spool parent", create=create, mode=0o700)
    try:
        core._assert_directory_identity(descriptor, path.parent, where="produced spool parent")
        yield descriptor
        core._assert_directory_identity(descriptor, path.parent, where="produced spool parent")
    finally:
        os.close(descriptor)


def _read(path, *, expected_sha256=None):
    path = Path(path)
    try:
        with _parent(path) as directory:
            fd = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
            with os.fdopen(fd, "rb") as handle:
                before = os.fstat(handle.fileno())
                if not stat.S_ISREG(before.st_mode) or before.st_size > 8 << 20:
                    raise SpoolError("unknown-retain: nonregular or oversized spool record")
                raw = handle.read((8 << 20) + 1)
                after = reader_lease.portable_identity(os.fstat(handle.fileno()))
                if reader_lease.portable_identity(before) != after:
                    raise SpoolIdentityRefusal(
                        "unknown-retain: spool record changed during read", path=path,
                        recorded=reader_lease.portable_identity(before), observed=after)
        if expected_sha256 is not None and hashlib.sha256(raw).hexdigest() != expected_sha256:
            raise SpoolError("sealed spool record changed")
        body = core._decode_strict_json(raw, where="spool record")
    except FileNotFoundError:
        return None
    except (OSError, ValueError) as exc:
        raise SpoolError(f"unknown-retain: {path}: {exc}") from exc
    if not isinstance(body, dict):
        raise SpoolError("unknown-retain: spool record is not an object")
    return body


def _write(path, body):
    path = Path(path)
    raw = json.dumps(body, sort_keys=True, separators=(",", ":")).encode() + b"\n"
    temporary = path.name + f".{os.getpid()}.tmp"
    with _parent(path) as directory:
        fd = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW,
                     0o600, dir_fd=directory)
        with os.fdopen(fd, "wb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path.name, src_dir_fd=directory, dst_dir_fd=directory)
        os.fsync(directory)


@contextmanager
def _lock(path):
    path = Path(path)
    with _parent(path) as directory:
        descriptor = os.open(path.name, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW,
                             0o600, dir_fd=directory)
        try:
            if not stat.S_ISREG(os.fstat(descriptor).st_mode):
                raise SpoolError("spool lock is not regular")
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            core._assert_directory_identity(directory, path.parent, where="locked spool parent")
            yield
        finally:
            os.close(descriptor)


def _positive(value, name):
    if type(value) is not int or value <= 0:
        raise SpoolError(f"{name} must be a positive integer")
    return value


def _path(value, root=None):
    path = Path(value)
    if not path.is_absolute() or str(path) != os.path.normpath(str(path)):
        raise SpoolError("path must be absolute and normalized")
    if root is not None and not path.is_relative_to(root):
        raise SpoolError("path escapes its authorized prefix")
    if path.resolve() != path:
        raise SpoolError("symlink paths are not spool authority")
    return path


def _identity(path, *, directory=None):
    path = Path(path)
    if directory is None:
        with _parent(path) as descriptor:
            return _identity(path, directory=descriptor)
    value = os.stat(path.name, dir_fd=directory, follow_symlinks=False)
    if not stat.S_ISREG(value.st_mode):
        raise SpoolError("spool payload is not a regular file")
    return reader_lease.portable_identity(value)


def _matches(path, identity):
    try:
        return reader_lease.file_id_matches(identity, _identity(path))
    except OSError:
        return False


def _local_disk(path):
    # Require disk-backed local storage, never an HDD-backed network path
    # masquerading as a local spool, or unaccounted tmpfs beside GPU memory.
    selected = None
    for line in Path("/proc/self/mountinfo").read_text().splitlines():
        left, right = line.split(" - ", 1)
        mount = left.split()[4].replace("\\040", " ")
        if path.is_relative_to(mount) and (selected is None or len(mount) > selected[0]):
            selected = (len(mount), right.split()[0])
    if selected is None or selected[1] in {"nfs", "nfs4", "cifs", "smb3", "tmpfs", "ramfs"}:
        raise SpoolError("spool root is not a known local disk filesystem")


def host_window_terms(variables):
    """The host demand one producer's spool window reserves, or ``{}`` (#747).

    ``variables`` is the producer's sealed environment.  Off -- no
    :data:`HOST_WINDOW_ENV`, ``""`` or ``"0"`` -- there is no term, whatever
    else the environment says.  On, the window is the owner's byte bound,
    :data:`MAX_ENV`, rounded up to whole GiB: the unit the host ledger
    counts, so a reservation never holds less than the spool may fill.
    """

    switch = variables.get(HOST_WINDOW_ENV, "")
    if switch not in ("", "0", "1"):
        raise SpoolError(f"{HOST_WINDOW_ENV} must be 0 or 1, not {switch!r}")
    if switch != "1":
        return {}
    raw = variables.get(MAX_ENV)
    if (not isinstance(raw, str) or not raw.isascii() or not raw.isdigit()
            or int(raw) <= 0):
        raise SpoolError(f"{HOST_WINDOW_ENV}=1 needs a positive integer "
                         f"{MAX_ENV}, not {raw!r}")
    return {HOST_WINDOW_KIND: -(-int(raw) // storage_tiers.GIB)}


def _export_record(group, owner):
    record = _read(group / "export.json")
    if record is None:
        return None
    if set(record) != {"export_key", "manifest_sha256", "batch_id", "action"}:
        raise SpoolError("export record shape is corrupt")
    action = core.validate_action(record["action"])
    expected = {"manifest_sha256": record["manifest_sha256"],
                "owner": owner, "batch_id": group.name}
    inputs = [entry for entry in action["inputs"] if entry["id"] == "produced-spool-manifest"]
    if (action["action_key"] != record["export_key"] or record["batch_id"] != group.name
            or action["params"].get("produced_spool") != expected
            or len(inputs) != 1 or inputs[0]["sha256"] != record["manifest_sha256"]):
        raise SpoolError("export record is not bound to its sealed action")
    return record


def export_fill(queue, tier_id):
    """The pool-side fill one export reserves on ``tier_id``, or ``None`` (#747).

    An export writes into the pool the stage movers read from, and the
    measured cost of a pool write is displaced mover reads (2026-09-22,
    dl380g10: member reads fell from 175 to 92 MB/s under 150-300 MB/s of
    member writes, and to 19 MB/s above that).  So the export reserves on the
    tier ledger the movers reserve from, by the one rule they are priced by,
    ``storage_tiers.current_fill_offer``.  No export receipt prices a pool
    write yet, so the measured side is ``None`` and the price is the tier's
    current offer: one read MB per written MB, which over-charges a write
    (the same bins displace about 0.4 read MB per written MB) and so errs
    toward the movers.  A tier that announces no fill offer prices nothing,
    and the export stays unreserved and unpaced exactly as before.
    """

    for record in queue.tiers():
        if isinstance(record, dict) and str(record.get("tier_id")) == str(tier_id):
            fill, _offer, _basis = storage_tiers.current_fill_offer(record, None)
            return int(fill) if fill else None
    return None


class ExportPacer:
    """Hold one export's write rate to the fill it reserved.

    A token bucket over the whole export at ``rate_mb_s`` (decimal MB, the
    ledger's unit).  When the copy runs ahead of its schedule the written
    bytes are flushed before the wait, because a ``write`` returns into the
    client's page cache and the NFS client would otherwise send the backlog
    at line rate: the flush is what makes the pool see the paced stream.
    """

    def __init__(self, rate_mb_s):
        self.rate = _positive(rate_mb_s, "pace rate") * 1_000_000
        self.started = time.monotonic()
        self.bytes = 0
        self.held = 0.0
        self.flushes = 0

    def wrote(self, count, handle):
        self.bytes += count
        ahead = self.bytes / self.rate - (time.monotonic() - self.started)
        if ahead <= 0:
            return
        handle.flush()
        os.fdatasync(handle.fileno())
        self.flushes += 1
        ahead = self.bytes / self.rate - (time.monotonic() - self.started)
        if ahead > 0:
            time.sleep(ahead)
            self.held += ahead

    def record(self, tier_id):
        seconds = time.monotonic() - self.started
        return {"schema": PACING_SCHEMA, "tier_id": str(tier_id),
                "rate_mb_s": int(self.rate // 1_000_000), "bytes": self.bytes,
                "seconds": round(seconds, 3), "held_seconds": round(self.held, 3),
                "flushes": self.flushes,
                "mb_per_s_file_side": (round(self.bytes / seconds / 1e6, 1)
                                       if seconds > 0 else None)}


class ProducedSpool:
    def __init__(self, queue, instance, template, *, cas_root, root,
                 max_bytes=32 << 30):
        self.queue = queue
        self.template, self.instance = po._require_bound_contract(template, instance)
        self.owner = str(self.instance["owner_action_key"])
        self.cas_root = str(cas_root)
        self.max_bytes = _positive(max_bytes, "max_bytes")
        self.root = _path(root)
        parent = po._read_producer_request(cas_root, self.owner)
        if isinstance(parent, dict):
            raise SpoolError(str(parent))
        self.cas, self.request = parent
        variables = self.request["environment"]["variables"]
        if (variables.get(ROOT_ENV) != str(self.root)
                or variables.get(MAX_ENV) != str(max_bytes)):
            raise SpoolError("spool root and byte bound must match the sealed producer environment")
        paced = variables.get(PACED_EXPORT_ENV, "")
        if paced not in ("", "0", "1"):
            raise SpoolError(f"{PACED_EXPORT_ENV} must be 0 or 1, not {paced!r}")
        #: Whether this producer's exports reserve fill and pace by default.
        self.paced_export = paced == "1"
        self._live()
        row = pool._read_json(queue.item_path(pool.CLAIMED, self.owner)) or {}
        self.host = str(row.get("claimed_host") or "")
        if not self.host:
            raise SpoolError("source host is unknown")
        #: The host demand this producer's claim must hold for its window,
        #: ``{}`` unless the sealed environment opts in (#747).  The per-owner
        #: bound and the ``statvfs`` check in :meth:`reserve_group` apply
        #: either way; this is what keeps two producers on one box from
        #: overrunning it together.
        self.host_window = host_window_terms(variables)
        resources = row.get("resources") if isinstance(row.get("resources"), dict) else {}
        for kind, need in self.host_window.items():
            try:
                held = int(resources.get(kind, 0))
            except (TypeError, ValueError):
                held = 0
            if held < need:
                raise SpoolError(
                    f"{HOST_WINDOW_ENV}=1 needs the claim to reserve {kind}={need} "
                    f"for a {variables[MAX_ENV]}-byte window; the claimed row "
                    f"reserves {kind}={held}")
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        _local_disk(self.root)
        self.directory = _path(self.root / po.instance_namespace(self.instance), self.root)
        self.directory.mkdir(exist_ok=True, mode=0o700)

    def _live(self):
        refusal = po._require_live_owner(self.queue, self.instance)
        if refusal:
            raise SpoolError(str(refusal))

    def _group(self, batch_id):
        po._name(batch_id, where="spool batch_id")
        return _path(self.directory / batch_id, self.directory)

    def _reservation(self, group):
        record = _read(group / "reservation.json")
        if record is None:
            return None
        if (set(record) != {"schema", "owner", "batch_id", "ceiling_bytes", "released"}
                or record["schema"] != SCHEMA or record["owner"] != self.owner
                or record["batch_id"] != group.name or type(record["released"]) is not bool):
            raise SpoolError("unknown-retain: malformed spool reservation")
        _positive(record["ceiling_bytes"], "stored ceiling")
        if record["released"] and any(path.is_file() or path.is_symlink()
                                        for path in (group / "payload").rglob("*")):
            raise SpoolError("unknown-retain: released spool still has payloads")
        return record

    def reserve_group(self, batch_id, ceiling_bytes):
        self._live()
        _positive(ceiling_bytes, "ceiling_bytes")
        prewrite = po._read_prewrite(po._prewrites_dir(
            self.queue.root, self.instance) / f"{batch_id}.prewrite.json")
        if (prewrite is None or prewrite.get("owner_action_key") != self.owner
                or prewrite.get("owner_attempt") != self.instance["owner_attempt"]):
            raise SpoolError("canonical prewrite must own the group before local allocation")
        if ceiling_bytes > sum(prewrite["class_bytes"].values()):
            raise SpoolError("local ceiling exceeds the canonical prewrite budget")
        group = self._group(batch_id)
        with _lock(self.directory / ".reservation.lock"):
            record = self._reservation(group)
            if record is not None:
                if record.get("ceiling_bytes") != ceiling_bytes or record.get("released"):
                    raise SpoolError("spool reservation replay mismatch or already released")
                return group / "payload"
            reserved = 0
            for sibling in self.directory.iterdir():
                if sibling.is_symlink():
                    raise SpoolError("unknown-retain: symlink in spool namespace")
                if not sibling.is_dir():
                    continue
                other = self._reservation(sibling)
                if other is None:
                    raise SpoolError("unknown-retain: unaccounted spool directory")
                if not other.get("released"):
                    reserved += _positive(other.get("ceiling_bytes"), "stored ceiling")
            if reserved + ceiling_bytes > self.max_bytes:
                raise SpoolCapacityDeferred("local spool byte bound reached")
            free = os.statvfs(self.directory)
            if ceiling_bytes > free.f_bavail * free.f_frsize:
                raise SpoolCapacityDeferred("local filesystem lacks physical allocation headroom")
            group.mkdir(mode=0o700)
            (group / "payload").mkdir(mode=0o700)
            _write(group / "reservation.json", {
                "schema": SCHEMA, "owner": self.owner, "batch_id": batch_id,
                "ceiling_bytes": ceiling_bytes, "released": False})
            return group / "payload"

    def submit_group(self, batch_id, entries, *, paced=None):
        """Seal and publish the export of one reserved group.

        ``paced`` overrides the producer's :data:`PACED_EXPORT_ENV` for this
        group only, which is how an A/B interleaves paced and unpaced exports
        from one producer.  ``None`` takes the producer's setting.  A replay
        keeps whatever the first submission sealed.
        """

        if paced is not None and not isinstance(paced, bool):
            raise SpoolError("paced must be True, False or None")
        paced = self.paced_export if paced is None else paced
        self._live()
        group = self._group(batch_id)
        with _lock(group / ".export.lock"):
            reservation = self._reservation(group)
            if reservation is None or reservation.get("released"):
                raise SpoolError("no active spool reservation")
            old = _export_record(group, self.owner)
            if old is not None:
                manifest = _read(group / "manifest.json", expected_sha256=old["manifest_sha256"])
                keys = ("source_path", "destination_path", "bytes", "sha256")
                if (not isinstance(entries, (list, tuple)) or manifest is None
                        or [{key: entry.get(key) for key in keys} for entry in entries]
                        != [{key: entry.get(key) for key in keys} for entry in manifest["entries"]]):
                    raise SpoolError("export replay changed its group entries")
                if ([entry.get("artifact_class", "payload") for entry in entries]
                        != [entry["artifact_class"] for entry in manifest["entries"]]):
                    raise SpoolError("export replay changed its artifact class")
                return self._publish(old)
            prewrite = po._read_prewrite(po._prewrites_dir(
                self.queue.root, self.instance) / f"{batch_id}.prewrite.json")
            if prewrite is None:
                raise SpoolError("canonical prewrite disappeared before export")
            if not isinstance(entries, (list, tuple)) or not entries:
                raise SpoolError("export needs complete group entries")
            checked = []
            seen = set()
            sources = set()
            class_bytes = {"payload": 0, "checkpoint": 0}
            for index, entry in enumerate(entries):
                source = _path(entry["source_path"], group / "payload")
                destination = _path(entry["destination_path"], Path(self.template["output_prefix"]))
                temporary = Path(str(destination) + ".tmp")
                if str(destination) not in prewrite["paths"] or str(temporary) not in prewrite["paths"]:
                    raise SpoolError("destination and temporary must be owned by the canonical prewrite")
                if destination in seen:
                    raise SpoolError("duplicate canonical destination")
                if source in sources:
                    raise SpoolError("duplicate local source")
                seen.add(destination)
                sources.add(source)
                identity = _identity(source)
                size = _positive(entry["bytes"], "entry bytes")
                if identity["size"] != size:
                    raise SpoolIdentityRefusal("local source size changed", entry_index=index,
                                               path=source, recorded={"bytes": size},
                                               observed=identity)
                artifact_class = entry.get("artifact_class", "payload")
                if artifact_class not in class_bytes:
                    raise SpoolError("export artifact class must be payload or checkpoint")
                class_bytes[artifact_class] += size
                digest = entry.get("sha256")
                po._hex64(digest, where="spool sha256")
                checked.append({"source_path": str(source), "destination_path": str(destination),
                                "bytes": size, "sha256": digest, "source_identity": identity,
                                "artifact_class": artifact_class})
            if any(class_bytes[key] > prewrite["class_bytes"][key] for key in class_bytes):
                raise SpoolError("export exceeds its canonical artifact class budget")
            if sum(entry["bytes"] for entry in checked) > reservation["ceiling_bytes"]:
                raise SpoolError("local group exceeded its reserved byte ceiling")
            actual = {path for path in (group / "payload").rglob("*") if not path.is_dir()}
            if actual != sources:
                raise SpoolError("local group contains unaccounted payloads or temporaries")
            declared = {str(path) for path in prewrite["paths"] if not str(path).endswith(".tmp")}
            if {entry["destination_path"] for entry in checked} != declared:
                raise SpoolError("export group does not cover the canonical prewrite")
            manifest = {"schema": SCHEMA, "owner": self.owner,
                        "instance": self.instance, "template": self.template,
                        "batch_id": batch_id, "group": str(group), "host": self.host,
                        "entries": checked}
            manifest_path = group / "manifest.json"
            _write(manifest_path, manifest)
            manifest_input, _ = self.cas.ingest_input(manifest_path, input_id="produced-spool-manifest")
            templated = po._producer_movement_template(
                self.queue, self.request, self.owner, extra_inputs=[manifest_input])
            if not templated.get("ok"):
                raise SpoolError(str(templated))
            tool = Path(__file__).resolve().parents[2] / "tools" / "fleet" / "produced_export.py"
            command = ["/usr/bin/python3", str(tool), "--queue", str(self.queue.root),
                       "--manifest", str(manifest_path), "--manifest-sha256", manifest_input["sha256"]]
            demand = dict(adaptive_cpu.EXPORT_DEMAND)
            # Opted in, the pool write enters under a reservation on the tier
            # its prewrite names -- the tier its batch stages through, or for
            # a write-only template (#912) the tier a later consumer stages
            # it through -- and is paced to it (#747).  The rate
            # is sealed in the command, so it is part of the export's
            # identity.  Not opted in, the export is sealed as before.
            tier_id = str(prewrite.get("tier") or "")
            fill = export_fill(self.queue, tier_id) if paced and tier_id else None
            if fill:
                demand[f"{storage_tiers.FILL_KIND}{storage_tiers.TIER_DEMAND_SEPARATOR}{tier_id}"] = fill
                command += ["--pace-mb-s", str(fill), "--pace-tier", tier_id]
            action = movement_actions.seal_movement_action(
                templated["template"],
                command=command,
                demand=demand, tags=[self.host],
                log_name=f"produced-export-{batch_id}.log",
                retry_policy={"max_attempts": 3, "retry_safe": True},
                extra_params={"produced_spool": {"manifest_sha256": manifest_input["sha256"],
                                                 "owner": self.owner, "batch_id": batch_id}})
            self.cas.publish_action_request(action)
            record = {"export_key": action["action_key"], "manifest_sha256": manifest_input["sha256"],
                      "batch_id": batch_id, "action": action}
            _write(group / "export.json", record)
            return self._publish(record)

    def _publish(self, record):
        launch = po._producer_launch_context(self.queue, self.owner)
        if not launch.get("ok"):
            raise SpoolError(str(launch))
        key = record["export_key"]
        state = po._mover_live_state(self.queue, key)
        if state == "absent":
            # The row carries exactly the demand the action sealed, so a
            # reservation priced at seal time is the one admission charges.
            self.queue.publish(action_key=key, cas_root=self.cas_root,
                worker_script=launch["worker_script"], **launch["addressing"],
                resources=dict(record["action"]["params"]["demand"]), tags=[self.host],
                max_attempts=3, retry_safe=True, priority=launch["priority"],
                dependent_of=self.owner, refuse_withdrawn=True)
        elif state in {"failed", "withdrawn"}:
            return {"ok": False, "complete": False, "export_key": key,
                    "refusal": f"export-{state}"}
        return {"ok": True, "complete": False, "export_key": key}

    def poll_group(self, batch_id):
        """Whether the group's export is acknowledged, and its files are still the export's.

        Takes no lock unless a destination's timestamps alone moved (#1096):
        then it takes the group's ``.export.lock``, checks again, and
        re-pins under it.  A caller that already holds that lock uses
        `_poll_locked`, because the lock is not reentrant.
        """

        try:
            return self._poll(batch_id, repin=None)
        except _RepinNeeded:
            with _lock(self._group(batch_id) / ".export.lock"):
                return self._poll_locked(batch_id)

    def _poll_locked(self, batch_id):
        """`poll_group` for a caller that holds the group's ``.export.lock``.

        A destination whose timestamps alone moved is verified by its
        content, and the re-pinned receipt is written before this returns.
        """

        return self._poll(batch_id, repin="poll")

    def _poll(self, batch_id, *, repin):
        group = self._group(batch_id)
        record = _export_record(group, self.owner)
        if record is None:
            return {"ok": True, "complete": False}
        key = record["export_key"]
        receipt = _read(group / "receipt.json")
        if receipt is not None:
            try:
                manifest = _read(group / "manifest.json", expected_sha256=record["manifest_sha256"])
                if manifest is None:
                    raise SpoolError("export manifest is missing")
                if _check_receipt(receipt, manifest, record, repin=repin):
                    _write(group / "receipt.json", receipt)
            except SpoolIdentityRefusal as exc:
                return {"ok": False, "complete": False, "refusal": exc.code,
                        "evidence": exc.evidence,
                        **_file_refusal(self.queue, group, self.owner, record,
                                        exc.evidence, where="poll")}
            except (SpoolError, reader_lease.ReaderLeaseError) as exc:
                return {"ok": False, "complete": False, "refusal": str(exc)}
            return {"ok": True, "complete": True, "export_key": key}
        state = po._mover_live_state(self.queue, key)
        if state in {"failed", "withdrawn", "done"}:
            return {"ok": False, "complete": False, "export_key": key,
                    "refusal": f"export-{state}-without-ack"}
        return {"ok": True, "complete": False, "export_key": key}

    def commit_origin_group(self, batch_id, descriptors, *,
                            lifetime=po.ORIGIN_LIFETIME_RETAIN):
        """Commit one exported group as a batch at its origin (#912, #1034).

        A write-only template's group is a handoff a later action stages.  A
        read-back template's group is one its owner reads back from this
        local spool (#1034); nothing stages it, and this commit is what ends
        its prewrite and charges it its actual bytes.

        Only after the group's export receipt is durable: before it, a
        retried export can still replace a landed copy, so an identity taken
        then may not be the one that lasts.  The descriptors must name
        exactly the files the export landed, with their sizes and digests,
        and `produced_output.commit_origin_batch` commits them against the
        identities the receipt recorded.  ``lifetime`` is that commit's
        (#914): ``retain`` by default, or ``consumed`` for a handoff that PB
        retires once its declared consumers succeed.  Returns that commit's
        answer, or the export's refusal.  Independent of `release_group`:
        either may come first.
        """
        group = self._group(batch_id)
        with _lock(group / ".export.lock"):
            result = self._poll_locked(batch_id)
            if not result.get("ok") or not result.get("complete"):
                return {**result, "ok": False,
                        "refusal": result.get("refusal", "export-incomplete-retain")}
            record = _export_record(group, self.owner)
            manifest = _read(group / "manifest.json",
                             expected_sha256=record["manifest_sha256"])
            receipt = _read(group / "receipt.json")
            sealed = [po.validate_descriptor(desc, self.template, self.instance)
                      for desc in descriptors]
            exported = sorted((str(entry["destination_path"]), int(entry["bytes"]),
                               str(entry["sha256"])) for entry in manifest["entries"])
            declared = sorted((str(desc["path"]), int(desc["bytes"]), str(desc["sha256"]))
                              for desc in sealed)
            if exported != declared:
                return {"ok": False, "refusal": "descriptors-are-not-the-export"}
            landed = {str(proof["destination_path"]): proof["identity"]
                      for proof in receipt["entries"]}
        return po.commit_origin_batch(self.queue, self.instance, self.template,
                                      sealed, batch_id=batch_id, landed=landed,
                                      lifetime=lifetime)

    def release_group(self, batch_id):
        group = self._group(batch_id)
        result = self.poll_group(batch_id)
        if not result.get("ok") or not result.get("complete"):
            return {**result, "ok": False, "refusal": result.get("refusal", "export-incomplete-retain")}
        with _lock(group / ".export.lock"):
            # Checked again under the group's lock and before the namespace's:
            # a re-pin reads the group's files (#1096), and the namespace lock
            # is every other group's reservation.  Nothing a release does
            # needs the destinations checked under that second lock.
            result = self._poll_locked(batch_id)
            if not result.get("ok") or not result.get("complete"):
                return {**result, "ok": False, "refusal": result.get("refusal", "export-incomplete-retain")}
            return self._release_locked(group)

    def _release_locked(self, group):
        """Release a checked group; the caller holds its ``.export.lock``."""

        with _lock(self.directory / ".reservation.lock"):
            reservation = self._reservation(group)
            if reservation.get("released"):
                return {"ok": True, "duplicate": True}
            record = _export_record(group, self.owner)
            released = _release_acknowledged(group, record, reservation)
            if "evidence" in released:
                released.update(_file_refusal(self.queue, group, self.owner, record,
                                              released["evidence"], where="release"))
            return released


def _release_acknowledged(group, record, reservation):
    """Unlink one acknowledged group's payloads by the identities it exported.

    The caller holds the group's ``.export.lock`` and the namespace's
    ``.reservation.lock`` and has checked the durable receipt.  Every file
    under ``payload/`` must be a source the manifest names, with the
    ``file_id`` the manifest recorded, or nothing is removed.  Shared by the
    live producer's :meth:`ProducedSpool.release_group` and the retirement
    tick (#1001), which releases a dead producer's groups by the same checks.
    """

    manifest = _read(group / "manifest.json", expected_sha256=record["manifest_sha256"])
    sources = {_path(entry["source_path"], group / "payload"): entry["source_identity"]
               for entry in manifest["entries"]}
    actual = {path for path in (group / "payload").rglob("*") if not path.is_dir()}
    code = "local-spool-changed-retain"

    def refused(path, observed, detail=None):
        return {"ok": False, "refusal": code, "evidence": _identity_evidence(
            code, path=path, recorded=sources.get(path), observed=observed, detail=detail,
            entry_index=next((index for index, entry in enumerate(manifest["entries"])
                              if _path(entry["source_path"], group / "payload") == path),
                             None))}

    for path in sorted(actual):
        if path not in sources:
            return refused(path, _observe(path), "not a source the manifest names")
        if not _matches(path, sources[path]):
            return refused(path, _observe(path))
    for path in actual:
        with _parent(path) as directory:
            observed = _identity(path, directory=directory)
            if not reader_lease.file_id_matches(sources[path], observed):
                return refused(path, observed)
            os.unlink(path.name, dir_fd=directory)
            os.fsync(directory)
    reservation["released"] = True
    _write(group / "reservation.json", reservation)
    return {"ok": True, "released_bytes": reservation["ceiling_bytes"],
            "payload_bytes": sum(int(sources[path]["size"]) for path in actual)}


def _copy_binding(entry, index, manifest_sha256):
    return {"schema": SCHEMA, "kind": "export-copy", "manifest_sha256": manifest_sha256,
            "entry_index": index, "entry_sha256": core.canonical_sha256(entry)}


def _check_copy(proof, entry, index, manifest_sha256):
    binding = _copy_binding(entry, index, manifest_sha256)
    if not isinstance(proof, dict) or any(proof.get(key) != value for key, value in binding.items()):
        raise SpoolError("copy proof manifest/entry binding is corrupt")
    if type(proof["entry_index"]) is not int:
        raise SpoolError("copy proof entry index is corrupt")
    allowed = set(binding) | {"temporary_ino", "ready_identity", "sha256", "complete",
                              "destination_path", "identity", "adopted", "repinned"}
    if set(proof) - allowed:
        raise SpoolError("copy proof has unknown fields")
    if "adopted" in proof and (not isinstance(proof["adopted"], dict)
                               or not proof.get("complete")):
        raise SpoolError("copy proof adoption record is corrupt")
    if "repinned" in proof:
        if not proof.get("complete"):
            raise SpoolError("copy proof re-pin record is corrupt")
        _check_repins(proof)
    _positive(proof.get("temporary_ino"), "copy temporary inode")
    if "complete" in proof and type(proof["complete"]) is not bool:
        raise SpoolError("copy completion flag is corrupt")
    if "ready_identity" in proof:
        reader_lease._check_identity(proof["ready_identity"], where="copy ready identity")
        if (proof["ready_identity"]["size"] != entry["bytes"]
                or proof.get("sha256") != entry["sha256"]):
            raise SpoolError("copy proof size/digest does not bind the declared entry")
    if proof.get("complete"):
        reader_lease._check_identity(proof.get("identity"), where="copy landed identity")
        if ("ready_identity" not in proof or proof["identity"]["size"] != entry["bytes"]
                or proof.get("destination_path") != entry["destination_path"]):
            raise SpoolError("copy proof destination is corrupt")
    return proof


def _check_pacing(pacing):
    if (not isinstance(pacing, dict) or set(pacing) != {
            "schema", "tier_id", "rate_mb_s", "bytes", "seconds", "held_seconds",
            "flushes", "mb_per_s_file_side"}
            or pacing["schema"] != PACING_SCHEMA
            or not isinstance(pacing["tier_id"], str) or not pacing["tier_id"]):
        raise SpoolError("export pacing record is corrupt")
    _positive(pacing["rate_mb_s"], "pacing rate")
    for key in ("bytes", "flushes"):
        if type(pacing[key]) is not int or pacing[key] < 0:
            raise SpoolError("export pacing counters are corrupt")


def _check_receipt(receipt, manifest, record, *, destinations=True, repin=None):
    """Check an export receipt against its manifest and export record.

    ``destinations=False`` checks the acknowledgement only: the binding and
    every entry's completed copy proof, not whether each canonical file is
    still the inode the export landed.  The retirement tick (#1001) asks
    that question of a dead producer's group: whether the export finished,
    not whether its output was since consumed or retired downstream.

    A destination whose identity differs from its proof's in timestamps
    only (#1096) is settled by its content, and only by a caller that holds
    the group's ``.export.lock`` and names itself in ``repin``: the proof in
    ``receipt["entries"]`` is replaced by its re-pinned form (`_repinned`),
    and the caller writes the receipt.  Without ``repin`` such a mismatch
    raises `_RepinNeeded`, so the caller can take the lock and check again.
    Returns how many entries were re-pinned.
    """
    if isinstance(receipt, dict) and "pacing" in receipt:
        _check_pacing(receipt["pacing"])
    if (not isinstance(receipt, dict)
            or set(receipt) - {"pacing"} != {
                "schema", "export_key", "manifest_sha256", "entries"}
            or receipt["schema"] != SCHEMA
            or receipt["export_key"] != record["export_key"]
            or receipt["manifest_sha256"] != record["manifest_sha256"]
            or not isinstance(receipt["entries"], list)
            or len(receipt["entries"]) != len(manifest["entries"])):
        raise SpoolError("export receipt binding is corrupt")
    repinned = 0
    for index, (entry, proof) in enumerate(zip(manifest["entries"], receipt["entries"])):
        _check_copy(proof, entry, index, record["manifest_sha256"])
        path = entry["destination_path"]
        if not proof.get("complete"):
            # ``destinations=False`` never stats canonical storage (#1001).
            raise SpoolIdentityRefusal("export-destination-changed", entry_index=index,
                                       path=path, recorded=proof.get("identity"),
                                       observed=_observe(path) if destinations
                                       else "not checked",
                                       detail="the copy proof is not complete")
        if not destinations:
            continue
        observed = _observe(path)
        if reader_lease.file_id_matches(proof["identity"], observed):
            continue
        if not reader_lease.timestamp_only_mismatch(proof["identity"], observed):
            raise SpoolIdentityRefusal("export-destination-changed", entry_index=index,
                                       path=path, recorded=proof["identity"],
                                       observed=observed)
        if repin is None:
            raise _RepinNeeded(index)
        destination = Path(path)
        try:
            with _parent(destination) as directory:
                seen = _observe(destination, directory=directory)
                if reader_lease.file_id_matches(proof["identity"], seen):
                    continue
                receipt["entries"][index] = _repinned(
                    proof, seen, code="export-destination-changed", index=index,
                    path=path, name=destination.name, directory=directory,
                    where=repin)
        except OSError as exc:
            raise SpoolIdentityRefusal("export-destination-changed", entry_index=index,
                                       path=path, recorded=proof["identity"],
                                       observed=f"unreadable: {exc}") from None
        repinned += 1
    return repinned


# ---------------------------------------------------------------------------
# A canonical destination another attempt's records name (#1097)
# ---------------------------------------------------------------------------


def _live_rows(queue):
    """Every ready and claimed record, by key, read fail-closed.

    `pool.Queue.live_records`'s census, except that a record which cannot
    be read refuses instead of being skipped: the answer decides whether a
    file may be deleted, and an unread row may be the export that writes
    it.  A record that moved between the listing and the read is skipped:
    the caller takes this twice, so a row that stays live is seen once.
    """

    rows = {}
    for state in (pool.READY, pool.CLAIMED):
        directory = queue.dir(state)
        try:
            names = sorted(os.listdir(directory))
        except OSError as exc:
            raise SpoolError(f"unknown-retain: {state} rows: {exc}") from None
        for name in names:
            if not name.endswith(".json") or name.startswith("."):
                continue
            try:
                record = pool._read_json(directory / name)
            except (OSError, ValueError, pool.PoolContractError) as exc:
                raise SpoolError(f"unknown-retain: {state}/{name}: {exc}") from None
            if isinstance(record, dict):
                rows[name[:-len(".json")]] = (state, record)
    return rows


def _export_attempt(queue, record, key, owner):
    """``(batch_id, owner_nonce)`` of ``owner``'s spool export a live row runs.

    Read from the export's own sealed request and manifest in the CAS the
    row names.  ``("", "")`` for a row that is not a spool export of
    ``owner``; ``None`` when a row that may be one cannot be read, which the
    caller treats as an export of any group.  A row names its producer in
    ``dependent_of``; one published before that hint is found as
    `pool.Queue._may_serve_a_producer` finds it, by its export demand.
    """

    dependent_of = record.get("dependent_of")
    if dependent_of is not None and dependent_of != owner:
        return ("", "")
    if dependent_of is None and not queue._may_serve_a_producer(record):
        return ("", "")
    parent = po._read_producer_request(record.get("cas_root"), key)
    if isinstance(parent, dict):
        return None
    cas, request = parent
    params = request.get("params", {}).get("produced_spool")
    if not isinstance(params, dict):
        return ("", "") if dependent_of is None else None
    if params.get("owner") != owner:
        return ("", "") if dependent_of is None else None
    inputs = [item for item in request.get("inputs", [])
              if item.get("id") == "produced-spool-manifest"]
    try:
        if len(inputs) != 1:
            return None
        manifest = _read(cas.input_path(inputs[0]), expected_sha256=inputs[0]["sha256"])
        nonce = manifest["instance"]["owner_attempt"]["nonce"]
    except (OSError, ValueError, KeyError, TypeError, SpoolError, core.ActionContractError):
        return None
    return (str(params.get("batch_id")), str(nonce))


def _claim_text(claim):
    return (f"{claim['owner_kind']} {claim['owner_batch_id']} of "
            f"{claim['owner_action_key'][:12]}.{claim['owner_nonce'][:8]} "
            f"({claim['owner_state']})")


def _file_digest(directory, name, observed):
    """sha256 of the file ``name`` in ``directory``, if it is still ``observed``.

    Opened without following a link; the inode, size and mtime must be the
    observed ones before and after the read, or the file changed under it.
    ctime is not compared: a read by this host can recall another client's
    write delegation and move it (#1096).
    """

    fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
    fields = ("ino", "size", "mtime_ns")
    with os.fdopen(fd, "rb") as handle:
        before = os.fstat(handle.fileno())
        if (not stat.S_ISREG(before.st_mode)
                or any(reader_lease.portable_identity(before)[key] != observed[key]
                       for key in fields)):
            raise SpoolError("unowned or changed canonical destination: "
                             "it changed before it could be verified")
        digest = hashlib.sha256()
        while block := handle.read(BLOCK_BYTES):
            digest.update(block)
        after = reader_lease.portable_identity(os.fstat(handle.fileno()))
    if any(after[key] != observed[key] for key in fields):
        raise SpoolError("unowned or changed canonical destination: "
                         "it changed while it was verified")
    return digest.hexdigest()


def _retire_foreign(directory, name, observed, tag, *, settle=False):
    """Delete the file ``name`` only if it is still the one ``observed`` (#1097).

    #1053's rename-aside delete (`produced_output._unlink_if_committed`),
    through the export's pinned directory descriptor: move the name to a
    private one, so a writer's rename from here on creates a new file;
    unlink the private name only if its inode, size and mtime are the
    observed ones; otherwise link it back, or keep it at the private name
    and refuse when the name was taken again.  ``settle`` first finishes
    what an interrupted call left at the private name
    (`produced_output._settle_retiring_leftover`) and deletes nothing else.
    """

    if settle:
        outcome, why = po._settle_retiring_leftover(name, observed, tag, dir_fd=directory)
    else:
        outcome, why = po._unlink_if_committed(name, observed, tag, dir_fd=directory)
    if outcome != "none":
        os.fsync(directory)
    if outcome == "refuse":
        raise SpoolError(f"canonical destination displaced: {why}")
    if outcome == "superseded" and not settle:
        raise SpoolError("unowned or changed canonical destination: "
                         "it changed before it could be retired")
    return outcome


class _OtherClaims:
    """What other attempts' records say about one group's canonical paths (#1097).

    A producer that fails after exporting leaves canonical files, and a
    successor -- a retry under the same key, or a new key -- exports the same
    paths.  Its prewrite was already granted, because an attempt that has
    ended owns nothing a new write can disturb (#1053), so the export is
    where the files are met.  Who else names a path is answered from the
    #1053 records, read once per export and only once a present file needs
    it: the attempts of every template whose output prefix overlaps this
    one's (`_template_attempts`), less this export's own; each one's
    committed and prewritten paths (`_attempt_owned_paths`); and its state
    (`_attempt_state`).  A path's claims decide:

    * none: nobody's file, refused as before;
    * any attempt that is live, or whose state is unknown: refused;
    * a commit, or the prewrite of an attempt that succeeded (#1053 counts
      it as a commit): the file is adopted when its bytes are this entry's,
      and otherwise refused -- a committed file is never deleted here;
    * only the prewrites of dead attempts: a failed group's export.  Once
      no export of that group can still run, the file is adopted when its
      bytes are this entry's, and retired otherwise.
    """

    def __init__(self, queue, manifest, export_key, group):
        self.queue = queue
        self.manifest = manifest
        self.export_key = str(export_key)
        #: The private-name tag of this export's retirements.
        self.tag = f"export-{self.export_key[:16]}"
        #: Every path this export retired, in order.
        self.retired = []
        #: Each path this export has begun to retire, with the identity it
        #: retires, recorded before the delete starts.  A later run of the
        #: export settles those paths first, so a file an interrupted delete
        #: left at its private name is finished or put back.  One local read
        #: per run; a path it does not name is never looked for.
        self._marker = Path(group) / RETIRING_MARKER
        record = _read(self._marker) or {}
        if record and (record.get("export_key") != self.export_key
                       or not isinstance(record.get("paths"), dict)):
            raise SpoolError("unknown-retain: export retirement record is corrupt")
        self._retiring = dict(record.get("paths", {}))
        self._claims = None
        self._exports = {}

    def settle(self, directory, path):
        """Finish or undo an interrupted retirement of ``path`` by this export."""

        observed = self._retiring.get(str(path))
        if observed is not None:
            _retire_foreign(directory, Path(path).name, observed, self.tag, settle=True)

    def _retire(self, directory, name, observed, path):
        self._retiring[str(path)] = dict(observed)
        _write(self._marker, {"schema": SCHEMA, "export_key": self.export_key,
                              "tag": self.tag, "paths": self._retiring})
        _retire_foreign(directory, name, observed, self.tag)
        self.retired.append(str(path))

    def _read(self):
        instance, template = self.manifest["instance"], self.manifest["template"]
        wanted = set()
        for entry in self.manifest["entries"]:
            wanted.update((str(entry["destination_path"]),
                           str(entry["destination_path"]) + ".tmp"))
        own = (str(instance["owner_action_key"]), str(instance["template_id"]),
               str(instance["owner_attempt"]["nonce"]))
        reads = po._TickReads(self.queue)
        claims = {}
        try:
            attempts = po._template_attempts(
                self.queue.root, po._overlapping_template_ids(self.queue.root, template))
            for owner, template_id, nonce in attempts:
                if (owner, template_id, nonce) == own:
                    continue
                _version, committed, prewritten = reads.owned(
                    po._attempt_scope(self.queue.root, owner, template_id, nonce))
                named = [(kind, path, batch_id)
                         for kind, owned in (("batch", committed), ("prewrite", prewritten))
                         for path, batch_id in sorted(owned.items()) if path in wanted]
                if not named:
                    continue
                state = po._attempt_state(reads.generation(owner), nonce)
                for kind, path, batch_id in named:
                    claims.setdefault(path, []).append({
                        "owner_action_key": owner, "owner_nonce": nonce,
                        "template_id": template_id, "owner_kind": kind,
                        "owner_batch_id": batch_id, "owner_state": state})
        except (po.ProducedOutputError, OSError, ValueError) as exc:
            reason = str(exc)
            if not reason.startswith("unknown-retain"):
                reason = f"unknown-retain: {reason}"
            raise SpoolError(reason) from None
        return claims

    def verdict(self, path, *, refusal):
        """``("committed" | "failed", claims)`` for ``path``, or the refusal.

        ``committed`` carries the committing claims; ``failed`` carries every
        claim, all of them prewrites of dead attempts.
        """

        if self._claims is None:
            self._claims = self._read()
        claims = self._claims.get(str(path), [])
        if not claims:
            raise SpoolError(f"{refusal}: no attempt's records name {path}")
        for claim in claims:
            if claim["owner_state"] not in po._ENDED_ATTEMPT_STATES:
                raise SpoolError(f"{refusal}: {path} is named by {_claim_text(claim)}")
        committed = [claim for claim in claims
                     if claim["owner_kind"] == "batch" or claim["owner_state"] == "succeeded"]
        return ("committed", committed) if committed else ("failed", claims)

    def require_ended_exports(self, claims, path):
        """`require_ended_export` for every failed group that names ``path``.

        A retry chain leaves one dead prewrite per attempt, and any of those
        attempts' exports may still be queued: each one is checked.
        """

        for claim in claims:
            self.require_ended_export(claim, path)

    def require_ended_export(self, claim, path):
        """Refuse while any export of the failed group ``claim`` names may run.

        A dead attempt's export is its own action and can still be queued or
        claimed, and it writes the same names.  Every live row that runs the
        owner's spool export of the same batch id under the same attempt
        nonce holds the path (`_export_attempt`); so does a live row that
        may be one and cannot be read.  Two reads of the ready and claimed
        rows, so a row that moved between them is still seen.
        """

        key = (claim["owner_action_key"], claim["owner_nonce"], claim["owner_batch_id"])
        if key not in self._exports:
            owner, nonce, batch_id = key
            found = {}
            for _ in range(2):
                for row, (state, record) in _live_rows(self.queue).items():
                    if row == self.export_key or row in found:
                        continue
                    ran = _export_attempt(self.queue, record, row, owner)
                    if ran is None or ran == (batch_id, nonce):
                        found[row] = state
            self._exports[key] = found
        found = self._exports[key]
        if found:
            rows = ", ".join(f"{row} ({state})" for row, state in sorted(found.items()))
            raise SpoolError(f"failed-group-export-live: {path} is {_claim_text(claim)}, "
                             f"whose export may still write it: {rows}")

    def take_over(self, directory, destination, entry, index, manifest_sha256, observed):
        """Adopt or retire a present destination; returns the adopted proof or None."""

        refusal = "unowned or changed canonical destination"
        verdict, claims = self.verdict(destination, refusal=refusal)
        claim = claims[0]
        if verdict == "failed":
            self.require_ended_exports(claims, destination)
        digest = (_file_digest(directory, destination.name, observed)
                  if observed["size"] == entry["bytes"] else None)
        if digest == entry["sha256"]:
            landed = _identity(destination, directory=directory)
            if any(landed[key] != observed[key] for key in ("ino", "size", "mtime_ns")):
                raise SpoolError(f"{refusal}: it changed after it was verified")
            return {**_copy_binding(entry, index, manifest_sha256),
                    "temporary_ino": landed["ino"], "ready_identity": landed,
                    "sha256": digest, "complete": True,
                    "destination_path": str(destination), "identity": landed,
                    "adopted": dict(claim)}
        if verdict == "committed":
            raise SpoolError(f"{refusal}: {destination} is committed by "
                             f"{_claim_text(claim)} with other bytes, and a "
                             "committed file is never replaced")
        self._retire(directory, destination.name, observed, destination)
        return None

    def retire_temporary(self, directory, temporary, observed):
        """Retire a failed group's leftover temporary, or refuse."""

        refusal = "unowned export temporary"
        verdict, claims = self.verdict(temporary, refusal=refusal)
        if verdict != "failed":
            raise SpoolError(f"{refusal}: {temporary} is named by {_claim_text(claims[0])}")
        self.require_ended_exports(claims, temporary)
        self._retire(directory, temporary.name, observed, temporary)


def _export_entry(group, entry, index, manifest_sha256, pacer=None, others=None):
    source = _path(entry["source_path"], group / "payload")
    destination = _path(entry["destination_path"])
    temporary = Path(str(destination) + ".tmp")
    proof_path = group / f"copy-{index}.json"
    proof = _read(proof_path)
    if proof is not None:
        _check_copy(proof, entry, index, manifest_sha256)
    with _parent(source) as source_parent, _parent(destination, create=True) as destination_parent:
        def dest_identity(path):
            try:
                return _identity(path, directory=destination_parent)
            except FileNotFoundError:
                return None
        def refuse(code, path, recorded, observed, detail=None):
            return SpoolIdentityRefusal(code, entry_index=index, path=path,
                                        recorded=recorded, observed=observed, detail=detail)
        if proof and proof.get("complete"):
            landed = dest_identity(destination)
            if not reader_lease.file_id_matches(proof["identity"], landed):
                # Only its timestamps moved (#1096): settled by its content,
                # under this export's lock, and the copy proof re-pinned.
                proof = _repinned(proof, landed or "absent",
                                  code="completed destination changed", index=index,
                                  path=destination, name=destination.name,
                                  directory=destination_parent, where="export")
                _write(proof_path, proof)
            return proof
        # Pin the real local source before any unacknowledged recovery can
        # remove a canonical incarnation. Every ancestor and the leaf refuse
        # symlinks, and subsequent I/O uses these held directory descriptors.
        source_fd = os.open(source.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                            dir_fd=source_parent)
        with os.fdopen(source_fd, "rb") as reader:
            source_identity = reader_lease.portable_identity(os.fstat(reader.fileno()))
            regular = stat.S_ISREG(os.fstat(reader.fileno()).st_mode)
            if not regular or not reader_lease.file_id_matches(
                    entry["source_identity"], source_identity):
                raise refuse("local source changed before export", source,
                             entry["source_identity"], source_identity,
                             None if regular else "not a regular file")
            if others is not None:
                # A run interrupted inside `_retire_foreign` may have left a
                # file at its private name; finish it before deciding again.
                for path in (destination, temporary):
                    others.settle(destination_parent, path)
            current = dest_identity(destination)
            ready = (proof or {}).get("ready_identity", {})
            # Only this exact export's known pre-publication inode gets
            # the expected link/rename ctime transition. No post-ACK file
            # gets that exception: completed proofs above require all4.
            if current is not None and any(current.get(key) != ready.get(key)
                                           for key in ("ino", "size", "mtime_ns")):
                # Not this export's own file. A failed group's export, or a
                # commit, of another attempt may name it (#1097): adopt it
                # when its bytes are this entry's, retire it when only a
                # failed group names it, and refuse otherwise.
                code = "unowned or changed canonical destination"
                if others is None:
                    raise refuse(code, destination, ready or None, current)
                try:
                    adopted = others.take_over(destination_parent, destination, entry,
                                               index, manifest_sha256, current)
                except SpoolIdentityRefusal:
                    raise
                except SpoolError as exc:
                    # The takeover's own refusal, with this entry and both
                    # identities attached (#1098).
                    named, _sep, detail = str(exc).partition(": ")
                    raise refuse(named, destination, ready or None, current,
                                 detail or None) from None
                if adopted is not None:
                    _write(proof_path, adopted)
                    return adopted
                landed = dest_identity(destination)
                if landed is not None:
                    raise refuse(code, destination, current, landed,
                                 "a file landed after the failed group's was retired")
                current = None
            if current is not None:
                again = dest_identity(destination)
                if not reader_lease.file_id_matches(current, again):
                    raise refuse("canonical incarnation changed during recovery",
                                 destination, current, again or "absent")
                os.unlink(destination.name, dir_fd=destination_parent)
                os.fsync(destination_parent)
                # Never overlap an old unacknowledged canonical copy with a
                # new temporary. The pinned unchanged LOCAL file stays owned,
                # so temp=0 remains bounded by the canonical payload budget.
            temporary_identity = dest_identity(temporary)
            if temporary_identity is not None:
                owned = {"temporary_ino": (proof or {}).get("temporary_ino")}
                if temporary_identity["ino"] != owned["temporary_ino"]:
                    if others is None:
                        raise refuse("unowned export temporary", temporary, owned,
                                     temporary_identity)
                    # A failed group's export that died mid-copy (#1097).
                    try:
                        others.retire_temporary(destination_parent, temporary,
                                                temporary_identity)
                    except SpoolIdentityRefusal:
                        raise
                    except SpoolError as exc:
                        named, _sep, detail = str(exc).partition(": ")
                        raise refuse(named, temporary, owned, temporary_identity,
                                     detail or None) from None
                else:
                    os.unlink(temporary.name, dir_fd=destination_parent)
            fd = os.open(temporary.name, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW,
                         0o600, dir_fd=destination_parent)
            proof = {**_copy_binding(entry, index, manifest_sha256),
                     "temporary_ino": os.fstat(fd).st_ino}
            digest = hashlib.sha256()
            copied = 0
            with os.fdopen(fd, "wb") as writer:
                _write(proof_path, proof)
                while block := reader.read(BLOCK_BYTES):
                    copied += len(block)
                    if copied > entry["bytes"]:
                        raise refuse("source exceeded declared bytes", source,
                                     {"bytes": entry["bytes"]}, {"bytes_read": copied})
                    writer.write(block)
                    digest.update(block)
                    if pacer is not None:
                        pacer.wrote(len(block), writer)
                writer.flush()
                os.fsync(writer.fileno())
            held = reader_lease.portable_identity(os.fstat(reader.fileno()))
            named = _identity(source, directory=source_parent)
            if (copied != entry["bytes"] or digest.hexdigest() != entry["sha256"]
                    or not reader_lease.file_id_matches(entry["source_identity"], held)
                    or not reader_lease.file_id_matches(entry["source_identity"], named)):
                raise refuse("source changed or digest mismatched while exporting", source,
                             {"bytes": entry["bytes"], "sha256": entry["sha256"],
                              "identity": entry["source_identity"]},
                             {"bytes": copied, "sha256": digest.hexdigest(),
                              "identity": held, "identity_at_path": named})
            proof.update(ready_identity=_identity(temporary, directory=destination_parent),
                         sha256=digest.hexdigest())
            _write(proof_path, proof)
            core._assert_directory_identity(destination_parent, destination.parent,
                                             where="canonical export parent")
            os.link(temporary.name, destination.name,
                    src_dir_fd=destination_parent, dst_dir_fd=destination_parent,
                    follow_symlinks=False)
            os.unlink(temporary.name, dir_fd=destination_parent)
            os.fsync(destination_parent)
            proof.update(complete=True, destination_path=str(destination),
                         identity=_identity(destination, directory=destination_parent))
            _write(proof_path, proof)
            return proof


def export_group(queue, manifest_path, manifest_sha256, export_key, *,
                 pace_mb_s=None, pace_tier=None):
    """Worker payload: local-only reads, canonical writes, durable acknowledgement.

    With ``pace_mb_s`` the canonical writes are held to that rate, the fill
    the export reserved on ``pace_tier``, and the receipt records what the
    pacer did.  Without it the copy runs unpaced, as before.
    """
    if (pace_mb_s is None) != (pace_tier is None):
        raise SpoolError("a paced export names both its rate and its tier")
    manifest = _read(manifest_path, expected_sha256=manifest_sha256)
    if manifest is None or manifest.get("schema") != SCHEMA:
        raise SpoolError("sealed export manifest is absent or malformed")
    group = _path(manifest["group"])
    with _lock(group / ".export.lock"):
        record = _export_record(group, manifest["owner"])
        if record is None or record.get("export_key") != export_key or record.get("manifest_sha256") != manifest_sha256:
            raise SpoolError("export action does not own this local group")
        claim = pool._read_json(queue.item_path(pool.CLAIMED, export_key)) or {}
        if claim.get("claimed_host") != manifest["host"]:
            raise SpoolError("export is not claimed on the source host")
        try:
            return _export_claimed_group(queue, group, manifest, record, manifest_sha256,
                                         export_key, pace_mb_s, pace_tier)
        except SpoolIdentityRefusal as exc:
            # The group's proofs are on this host and go with the group; the
            # refusal's evidence is filed where no spool cleanup reaches (#1098).
            filed = _file_refusal(queue, group, manifest["owner"], record, exc.evidence,
                                  where="export")
            exc.add_note(f"produced_spool refusal: {json.dumps(filed, sort_keys=True)}")
            raise


def _export_claimed_group(queue, group, manifest, record, manifest_sha256, export_key,
                          pace_mb_s, pace_tier):
    """`export_group` once the group's lock is held and its claim checked."""

    receipt = _read(group / "receipt.json")
    if receipt is not None:
        if _check_receipt(receipt, manifest, record, repin="export"):
            _write(group / "receipt.json", receipt)
        return {"ok": True, "duplicate": True, "entries": len(receipt["entries"])}
    prewrite = po._read_prewrite(po._prewrites_dir(queue.root, manifest["instance"]) /
                                f"{manifest['batch_id']}.prewrite.json")
    if (prewrite is None or prewrite.get("owner_action_key") != manifest["owner"]
            or prewrite.get("owner_attempt") != manifest["instance"]["owner_attempt"]):
        raise SpoolError("canonical prewrite authority is absent or changed")
    class_bytes = {"payload": 0, "checkpoint": 0}
    for entry in manifest["entries"]:
        destination = _path(entry["destination_path"], Path(manifest["template"]["output_prefix"]))
        if str(destination) not in prewrite["paths"] or str(destination) + ".tmp" not in prewrite["paths"]:
            raise SpoolError("canonical export path is not owned")
        class_bytes[entry["artifact_class"]] += entry["bytes"]
    if any(class_bytes[key] > prewrite["class_bytes"][key] for key in class_bytes):
        raise SpoolError("canonical artifact class budget changed or exceeded")
    pacer = ExportPacer(pace_mb_s) if pace_mb_s is not None else None
    others = _OtherClaims(queue, manifest, export_key, group)
    landed = [_export_entry(group, entry, index, manifest_sha256, pacer, others)
              for index, entry in enumerate(manifest["entries"])]
    receipt = {"schema": SCHEMA, "export_key": export_key,
               "manifest_sha256": manifest_sha256, "entries": landed}
    if pacer is not None:
        receipt["pacing"] = pacer.record(pace_tier)
    _check_receipt(receipt, manifest, record, repin="export")
    _write(group / "receipt.json", receipt)
    answer = {"ok": True, "entries": len(landed)}
    adopted = sum(1 for proof in landed if "adopted" in proof)
    if adopted or others.retired:
        answer.update(adopted=adopted, retired=list(others.retired))
    return answer


# ---------------------------------------------------------------------------
# A refused group's evidence, filed where no spool cleanup reaches (#1098)
# ---------------------------------------------------------------------------

#: One record per refusal: ``<queue>/<dir>/<owner>/<batch>.<export>.<where>.<code>.json``.
REFUSALS_SUBDIR = "produced-spool-refusals"
REFUSAL_SCHEMA = "prismabuild.produced_spool.refusal.v1"
#: A record holds the group's receipt and copy proofs.  Past this size it
#: keeps only the refused entry's proof, and says how many it left out.
REFUSAL_RECORD_MAX_BYTES = 4 << 20
_COPY_PROOF = re.compile(r"copy-([0-9]+)\.json")


def _refusal_path(queue, owner, record, where, code):
    slug = re.sub(r"[^A-Za-z0-9]+", "-", str(code)).strip("-")[:48] or "refusal"
    return (Path(queue.root) / REFUSALS_SUBDIR / str(owner)
            / f"{record['batch_id']}.{str(record['export_key'])[:16]}.{where}.{slug}.json")


def _file_refusal(queue, group, owner, record, evidence, *, where):
    """File one refused group's evidence under the queue, once (#1098).

    The group's copy proofs and receipt hold the identities the export
    recorded, and they live in the producer's local spool, which the
    retirement tick (#1001) removes with a dead attempt's group.  So the
    refusal, its evidence, the receipt and every copy proof are written
    under the queue when the refusal is made.  One record per group, export,
    caller (``where``) and refusal code: a poll repeated on one refusal files
    it once, and costs one ``stat`` after the first.  Filing never changes
    the refusal: the answer is ``{"evidence_record": path}``, or
    ``{"evidence_record_error": why}`` when the record could not be written.
    """

    try:
        path = _refusal_path(queue, owner, record, where, evidence["code"])
        if path.exists():
            return {"evidence_record": str(path)}
        proofs = []
        for name in sorted((name for name in os.listdir(group) if _COPY_PROOF.fullmatch(name)),
                           key=lambda name: int(_COPY_PROOF.fullmatch(name).group(1))):
            try:
                proofs.append(_read(group / name))
            except SpoolError as exc:
                proofs.append({"unreadable": name, "reason": str(exc)})
        try:
            receipt = _read(group / "receipt.json")
        except SpoolError as exc:
            receipt = {"unreadable": str(exc)}
        body = {"schema": REFUSAL_SCHEMA, "where": where, "host": socket.gethostname(),
                "unix": round(time.time(), 3), "owner": str(owner),
                "batch_id": record["batch_id"], "export_key": record["export_key"],
                "manifest_sha256": record["manifest_sha256"], "group": str(group),
                "code": evidence["code"], "evidence": evidence,
                "receipt": receipt, "copy_proofs": proofs}
        raw = json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
        if len(raw) > REFUSAL_RECORD_MAX_BYTES:
            index = evidence.get("entry_index")
            body["copy_proofs"] = [proof for proof in proofs
                                   if isinstance(proof, dict) and proof.get("entry_index") == index]
            body["copy_proofs_total"] = len(proofs)
            if isinstance(receipt, dict) and isinstance(receipt.get("entries"), list):
                body["receipt"] = {**{key: value for key, value in receipt.items()
                                      if key != "entries"},
                                   "entries_total": len(receipt["entries"])}
            raw = json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
        fd = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o644)
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(raw + b"\n")
                handle.flush()
                os.fsync(handle.fileno())
            try:
                os.link(temporary, path)
            except FileExistsError:
                pass
        finally:
            os.unlink(temporary)
        return {"evidence_record": str(path)}
    except (OSError, ValueError, KeyError, TypeError) as exc:
        return {"evidence_record_error": f"{type(exc).__name__}: {exc}"}


def refusal_records(queue, *, owner=None, limit=None):
    """The filed refusal records, oldest first: every owner's, or one owner's.

    A record that cannot be read is listed as ``{"unreadable": path}``.
    """

    root = Path(queue.root) / REFUSALS_SUBDIR
    try:
        owners = [str(owner)] if owner is not None else sorted(os.listdir(root))
    except OSError:
        return []
    records = []
    for name in owners:
        try:
            files = sorted(item for item in os.listdir(root / name)
                           if item.endswith(".json") and not item.startswith("."))
        except OSError:
            continue
        for item in files:
            try:
                records.append(_read(root / name / item)
                               or {"unreadable": str(root / name / item)})
            except SpoolError:
                records.append({"unreadable": str(root / name / item)})
    records.sort(key=lambda value: float(value.get("unix", 0.0))
                 if isinstance(value.get("unix"), (int, float)) else 0.0)
    return records if limit is None else records[-limit:]


# ---------------------------------------------------------------------------
# Retirement of an ended producer's spool (#1001)
# ---------------------------------------------------------------------------

#: One JSON line per retired namespace, per host: ``<queue>/<dir>/<host>.jsonl``.
#: The host's tick is its only writer.  ``<host>.tick.json`` beside it is the
#: latest tick's full summary (latest-only; the lines are the durable record).
RETIREMENTS_SUBDIR = "produced-spool-retirements"
RETIREMENT_SCHEMA = "prismabuild.produced_spool.retirement.v1"
TICK_SCHEMA = "prismabuild.produced_spool.retirement_tick.v1"
#: How often a host's tick runs.  A namespace is retired only once its owner
#: attempt has ended, and the pool reads a silent claim as ended after
#: ``LEASE_TIMEOUT_S``; ticking faster than the bound it waits on buys at
#: most that much earlier reclaim.
RETIRE_INTERVAL_S = pool.LEASE_TIMEOUT_S
#: Everything a group directory may hold besides ``payload/``: the records
#: ``reserve_group``, ``submit_group`` and ``export_group`` write, their
#: ``_write`` temporaries and the export lock.
_GROUP_FILES = frozenset({"reservation.json", "manifest.json", "export.json",
                          "receipt.json", ".export.lock", RETIRING_MARKER})
#: ``(cas_root, owner) -> spool root or None``.  A sealed request never
#: changes, so its answer is read once per process.
_DECLARED_ROOTS: dict[tuple[str, str], str | None] = {}


def _group_file(name):
    return (name in _GROUP_FILES or (name.startswith("copy-") and name.endswith(".json"))
            or name.endswith(".tmp"))


def declared_roots(queue, cas_root):
    """Every spool root a producer's sealed environment names (#1001).

    The producers are the owners under the produced-output scopes: a spool
    group is reserved only against a prewrite filed there, so every owner
    that ever wrote a spool has a scope.  Each owner's request is read once
    per process.  Returns ``(roots, errors)``.
    """

    scopes = Path(queue.root) / "residency" / po.OUTPUT_SCOPES_SUBDIR
    errors = []
    try:
        owners = po._scope_owners(scopes)
    except FileNotFoundError:
        owners = []
    except OSError as exc:
        return set(), [f"scopes unreadable: {exc}"]
    for owner in owners:
        cache_key = (str(cas_root), owner)
        if cache_key in _DECLARED_ROOTS:
            continue
        parent = po._read_producer_request(cas_root, owner)
        if isinstance(parent, dict):
            errors.append(f"{owner[:12]}: {parent.get('refusal')}")
            continue
        root = parent[1]["environment"]["variables"].get(ROOT_ENV)
        _DECLARED_ROOTS[cache_key] = root if isinstance(root, str) and root else None
    roots = {Path(root) for (cas, _owner), root in _DECLARED_ROOTS.items()
             if cas == str(cas_root) and root}
    return roots, errors


def _namespace_instance(queue, directory, groups):
    """The instance whose namespace ``directory`` is, or ``None``.

    Any group's manifest carries it.  A namespace whose groups never reached
    ``submit_group`` has only reservations, which name the owner: its filed
    instances are matched by ``instance_namespace``, the directory's name.
    """

    owner = None
    for group in groups:
        manifest = _read(group / "manifest.json")
        if manifest is not None and isinstance(manifest.get("instance"), dict):
            instance = po.validate_instance(manifest["instance"])
            if po.instance_namespace(instance) == directory.name:
                return instance
        reservation = _read(group / "reservation.json")
        if reservation is not None and owner is None:
            owner = reservation.get("owner")
    if not isinstance(owner, str) or len(owner) != 64:
        return None
    scopes = Path(queue.root) / "residency" / po.OUTPUT_SCOPES_SUBDIR
    try:
        paths = po._owner_instance_paths(scopes / owner)
    except OSError:
        return None
    for path in paths:
        try:
            instance = po.validate_instance(json.loads(path.read_text()))
        except (OSError, ValueError, po.ProducedOutputError):
            continue
        if po.instance_namespace(instance) == directory.name:
            return instance
    return None


def _payload_bytes(payload):
    """Bytes of the regular files under ``payload/``, never following a link."""

    total = 0
    for parent, _directories, files in os.walk(payload, followlinks=False):
        for name in files:
            info = os.lstat(os.path.join(parent, name))
            if stat.S_ISREG(info.st_mode):
                total += info.st_size
    return total


def _remove_tree(directory):
    """Remove a directory this tick owns, deepest first, never following links.

    ``os.walk`` lists a symlink to a directory among the directories and
    never descends into it; it is unlinked, never ``rmdir``-ed.
    """

    for parent, directories, files in os.walk(directory, topdown=False,
                                              followlinks=False):
        for name in files:
            os.unlink(os.path.join(parent, name))
        for name in directories:
            path = os.path.join(parent, name)
            if os.path.islink(path):
                os.unlink(path)
            else:
                os.rmdir(path)
    os.rmdir(directory)


def _retire_group(queue, directory, group, owner):
    """Release, discard or keep one group of an ended attempt's namespace.

    Under the group's ``.export.lock`` and the namespace's
    ``.reservation.lock``, in :meth:`ProducedSpool.release_group`'s order.
    The export worker holds ``.export.lock`` for its whole run on this host,
    so no export can start or be running while this decides.

    ``.export.lock`` (here) and ``.reservation.lock`` (in
    :func:`retire_namespace`) are unlinked while held, outside
    ``posix_lock``'s tombstone protocol, and that is safe.  The owner attempt
    has ended, so no live producer takes either lock again: a new attempt
    writes a new namespace.  A late export worker that was waiting on the old
    inode fails closed, because ``_lock`` asserts that the directory it
    opened is still the lock's parent, and the group directory is gone.
    """

    name = group.name
    with _lock(group / ".export.lock"), _lock(directory / ".reservation.lock"):
        unknown = sorted(entry.name for entry in os.scandir(group)
                         if entry.name != "payload" and not _group_file(entry.name))
        if unknown or (group / "payload").is_symlink():
            return {"batch_id": name, "kept": f"unknown entries: {unknown[:4]}"}
        reservation = _read(group / "reservation.json")
        record = _export_record(group, owner)
        payload = group / "payload"
        if reservation is not None and (reservation.get("owner") != owner
                                        or reservation.get("batch_id") != name):
            return {"batch_id": name, "kept": "reservation names another owner or batch"}
        if record is None and reservation is None:
            outcome = {"batch_id": name, "discarded": "never-reserved",
                       "bytes": _payload_bytes(payload) if payload.is_dir() else 0}
        elif reservation is None:
            return {"batch_id": name, "kept": "export record without a reservation"}
        elif reservation.get("released") is True:
            if payload.is_dir() and any(files for _parent, _dirs, files in os.walk(payload)):
                return {"batch_id": name,
                        "kept": "unknown-retain: released spool still has payloads"}
            outcome = {"batch_id": name, "released": "released-by-producer", "bytes": 0}
        elif record is None:
            outcome = {"batch_id": name, "discarded": "export-never-submitted",
                       "bytes": _payload_bytes(payload)}
        else:
            receipt = _read(group / "receipt.json")
            if receipt is not None:
                manifest = _read(group / "manifest.json",
                                 expected_sha256=record["manifest_sha256"])
                if manifest is None:
                    return {"batch_id": name, "kept": "receipt without its manifest"}
                try:
                    _check_receipt(receipt, manifest, record, destinations=False)
                except (SpoolError, reader_lease.ReaderLeaseError) as exc:
                    return {"batch_id": name, "kept": f"receipt does not bind: {exc}"}
                released = _release_acknowledged(group, record, dict(reservation))
                if not released.get("ok"):
                    return {"batch_id": name, "kept": released.get("refusal")}
                outcome = {"batch_id": name, "released": "export-acknowledged",
                           "export_key": record["export_key"],
                           "bytes": released["payload_bytes"]}
            else:
                state = po._mover_live_state(queue, record["export_key"])
                if state in (pool.READY, pool.CLAIMED):
                    return {"batch_id": name, "kept": f"export-{state}",
                            "export_key": record["export_key"]}
                if state not in (pool.FAILED, pool.WITHDRAWN, "absent"):
                    return {"batch_id": name, "kept": f"export-{state}-without-ack",
                            "export_key": record["export_key"]}
                outcome = {"batch_id": name, "discarded": f"export-{state}",
                           "export_key": record["export_key"],
                           "bytes": _payload_bytes(payload)}
        # Every payload is accounted for above; remove the group whole.  The
        # export lock goes last, while it is still held.
        for entry in sorted(os.scandir(group), key=lambda item: item.name == ".export.lock"):
            path = Path(entry.path)
            if entry.name == "payload":
                _remove_tree(path)
            else:
                os.unlink(path)
    os.rmdir(group)
    return outcome


def retire_namespace(queue, directory):
    """Retire one namespace whose owner attempt ended, or say why not.

    The owner attempt has ended when ``produced_output._producer_attempt_state``
    reads ``dead`` or ``succeeded``: its latest generation is terminal, or a
    retry under a new nonce holds the claim.  A new attempt writes a new
    namespace (the nonce is in its name) and never reads this one, so the
    only reader left of a group's bytes is its own export action: a group
    whose export is ready or claimed is kept, one whose export failed or was
    withdrawn -- or never submitted -- is discarded, and one with a durable
    acknowledgement is released by ``release_group``'s identity checks.
    """

    started = time.monotonic()
    result = {"namespace": directory.name}
    try:
        entries = list(os.scandir(directory))
        foreign = sorted(entry.name for entry in entries
                         if entry.name != ".reservation.lock"
                         and not entry.is_dir(follow_symlinks=False))
        groups = sorted(Path(entry.path) for entry in entries
                        if entry.is_dir(follow_symlinks=False))
        instance = _namespace_instance(queue, directory, groups)
    except (OSError, SpoolError, po.ProducedOutputError, ValueError) as exc:
        return {**result, "kept": f"unreadable: {exc}"}
    if instance is None:
        return {**result, "kept": "unattributed: no manifest or filed instance names it"}
    owner = str(instance["owner_action_key"])
    result.update(owner=owner, owner_attempt=dict(instance["owner_attempt"]))
    state = po._producer_attempt_state(queue, instance)
    result["attempt_state"] = state
    if state not in po._ENDED_ATTEMPT_STATES:
        return {**result, "kept": f"owner-attempt-{state}"}
    released, discarded, kept = [], [], []
    held = 0.0
    for group in groups:
        began = time.monotonic()
        try:
            outcome = _retire_group(queue, directory, group, owner)
        except (OSError, SpoolError, reader_lease.ReaderLeaseError, ValueError) as exc:
            outcome = {"batch_id": group.name, "kept": f"unknown-retain: {exc}"}
        held += time.monotonic() - began
        (released if "released" in outcome else
         discarded if "discarded" in outcome else kept).append(outcome)
    removed = False
    if not kept and not foreign:
        try:
            os.unlink(directory / ".reservation.lock")
        except FileNotFoundError:
            pass
        try:
            os.rmdir(directory)
            removed = True
        except OSError as exc:
            kept.append({"batch_id": None, "kept": f"namespace not removed: {exc}"})
    if foreign:
        kept.append({"batch_id": None, "kept": f"unknown entries: {foreign[:4]}"})
    return {**result,
            "groups_released": released, "groups_discarded": discarded,
            "groups_kept": kept, "namespace_removed": removed,
            "bytes": sum(int(item.get("bytes", 0)) for item in released + discarded),
            "reason": ("owner attempt ended" if state == "dead"
                       else "owner attempt succeeded"),
            "held_s": round(held, 4),
            "seconds": round(time.monotonic() - started, 4)}


def _append_line(path, body):
    path.parent.mkdir(parents=True, exist_ok=True)
    line = (json.dumps(body, sort_keys=True, separators=(",", ":")) + "\n").encode()
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND | os.O_NOFOLLOW, 0o644)
    try:
        os.write(fd, line)
        os.fsync(fd)
    finally:
        os.close(fd)


def retirement_tick(queue, cas_root, *, host=None, roots=None):
    """Retire every ended attempt's namespace under this host's spool roots.

    ``roots`` defaults to :func:`declared_roots`; only roots present on this
    host and passing the spool's own local-disk check are walked.  Every
    namespace that released, discarded or removed anything is appended to
    ``<queue>/produced-spool-retirements/<host>.jsonl``; the full tick,
    including each kept namespace and its reason, replaces ``<host>.tick.json``.
    """

    started = time.monotonic()
    host = host or socket.gethostname()
    errors = []
    if roots is None:
        roots, errors = declared_roots(queue, cas_root)
    namespaces = []
    walked = []
    for root in sorted({Path(root) for root in roots}):
        try:
            root = _path(root)
            if not root.is_dir():
                continue
            _local_disk(root)
        except (OSError, SpoolError) as exc:
            errors.append(f"{root}: {exc}")
            continue
        walked.append(str(root))
        try:
            children = sorted(entry.path for entry in os.scandir(root)
                              if entry.is_dir(follow_symlinks=False)
                              and len(entry.name) == 64)
        except OSError as exc:
            errors.append(f"{root}: {exc}")
            continue
        for child in children:
            outcome = {"root": str(root), **retire_namespace(queue, Path(child))}
            namespaces.append(outcome)
    records = Path(queue.root) / RETIREMENTS_SUBDIR
    now = round(time.time(), 3)
    for outcome in namespaces:
        if (outcome.get("groups_released") or outcome.get("groups_discarded")
                or outcome.get("namespace_removed")):
            _append_line(records / f"{host}.jsonl",
                         {"schema": RETIREMENT_SCHEMA, "host": host, "unix": now,
                          **outcome})
    summary = {"schema": TICK_SCHEMA, "host": host, "unix": now,
               "roots": walked, "errors": errors,
               "namespaces": len(namespaces),
               "retired": sum(1 for item in namespaces if item.get("namespace_removed")),
               "bytes": sum(int(item.get("bytes", 0)) for item in namespaces),
               "kept": [{"namespace": item["namespace"], "root": item["root"],
                         "reason": item.get("kept") or [group.get("kept") for group
                                                        in item.get("groups_kept", [])]}
                        for item in namespaces if not item.get("namespace_removed")],
               "seconds": round(time.monotonic() - started, 4)}
    _write_tick(records, host, summary)
    return {**summary, "records": namespaces}


def _write_tick(records, host, summary):
    records.mkdir(parents=True, exist_ok=True)
    temporary = records / f".{host}.tick.json.{os.getpid()}.tmp"
    temporary.write_text(json.dumps(summary, sort_keys=True) + "\n")
    os.replace(temporary, records / f"{host}.tick.json")


def record_failed_tick(queue, host, error):
    """Replace ``<host>.tick.json`` with a tick that raised, so it is not stdout-only.

    ``error`` is the exception the tick raised.  The record keeps the tick
    schema, with ``failed`` naming the exception, so ``retirement_records``
    and ``pbstatus`` show the failure where they show every other tick.
    """

    _write_tick(Path(queue.root) / RETIREMENTS_SUBDIR, host,
                {"schema": TICK_SCHEMA, "host": host, "unix": round(time.time(), 3),
                 "failed": f"{type(error).__name__}: {error}"})


def retirement_records(queue, *, limit=None):
    """Every host's retirement lines, oldest first, and each host's last tick."""

    records = Path(queue.root) / RETIREMENTS_SUBDIR
    lines, ticks = [], {}
    try:
        names = sorted(os.listdir(records))
    except OSError:
        return {"retirements": [], "ticks": {}}
    for name in names:
        path = records / name
        try:
            if name.endswith(".jsonl"):
                for line in path.read_text().splitlines():
                    try:
                        value = json.loads(line)
                    except ValueError:
                        continue
                    if isinstance(value, dict):
                        lines.append(value)
            elif name.endswith(".tick.json") and not name.startswith("."):
                ticks[name[:-len(".tick.json")]] = json.loads(path.read_text())
        except (OSError, ValueError):
            continue
    lines.sort(key=lambda value: float(value.get("unix", 0.0))
               if isinstance(value.get("unix"), (int, float)) else 0.0)
    return {"retirements": lines if limit is None else lines[-limit:], "ticks": ticks}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--queue", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--manifest-sha256", required=True)
    parser.add_argument("--pace-mb-s", type=int,
                        help="hold the canonical writes to this rate, the fill reserved")
    parser.add_argument("--pace-tier", help="the tier whose fill the rate was reserved on")
    args = parser.parse_args(argv)
    result = export_group(pool.PoolQueue(args.queue), args.manifest,
                          args.manifest_sha256, os.environ.get("PRISMABUILD_ACTION_KEY", ""),
                          pace_mb_s=args.pace_mb_s, pace_tier=args.pace_tier)
    print(json.dumps(result, sort_keys=True))
    return 0
