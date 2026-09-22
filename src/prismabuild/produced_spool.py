"""Bounded producer-local PRECOMMIT spool and ordinary PB export actions.

No staged-read authority lives here. A verified durable export acknowledgement
permits the caller to use its existing canonical produced-output descriptors.
Pending local bytes are never committed units or canonical origin identities.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import socket
import stat
import time

from . import core, movement_actions, pool, produced_output as po, reader_lease
from . import storage_tiers

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


class SpoolError(RuntimeError):
    pass


class SpoolCapacityDeferred(SpoolError):
    """Only the bounded local spool is full; completed exports may free it."""


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
                if reader_lease.portable_identity(before) != reader_lease.portable_identity(os.fstat(handle.fileno())):
                    raise SpoolError("unknown-retain: spool record changed during read")
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
            for entry in entries:
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
                    raise SpoolError("local source size changed")
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
            demand = {"cpu": 1, "mem_gb": 1}
            # Opted in, the pool write enters under a reservation on the tier
            # its batch stages through, and is paced to it (#747).  The rate
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
                refuse_withdrawn=True)
        elif state in {"failed", "withdrawn"}:
            return {"ok": False, "complete": False, "export_key": key,
                    "refusal": f"export-{state}"}
        return {"ok": True, "complete": False, "export_key": key}

    def poll_group(self, batch_id):
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
                _check_receipt(receipt, manifest, record)
            except (SpoolError, reader_lease.ReaderLeaseError) as exc:
                return {"ok": False, "complete": False, "refusal": str(exc)}
            return {"ok": True, "complete": True, "export_key": key}
        state = po._mover_live_state(self.queue, key)
        if state in {"failed", "withdrawn", "done"}:
            return {"ok": False, "complete": False, "export_key": key,
                    "refusal": f"export-{state}-without-ack"}
        return {"ok": True, "complete": False, "export_key": key}

    def release_group(self, batch_id):
        group = self._group(batch_id)
        result = self.poll_group(batch_id)
        if not result.get("ok") or not result.get("complete"):
            return {**result, "ok": False, "refusal": result.get("refusal", "export-incomplete-retain")}
        with _lock(group / ".export.lock"), _lock(self.directory / ".reservation.lock"):
            result = self.poll_group(batch_id)
            if not result.get("ok") or not result.get("complete"):
                return {**result, "ok": False, "refusal": result.get("refusal", "export-incomplete-retain")}
            reservation = self._reservation(group)
            if reservation.get("released"):
                return {"ok": True, "duplicate": True}
            record = _export_record(group, self.owner)
            manifest = _read(group / "manifest.json", expected_sha256=record["manifest_sha256"])
            sources = {_path(entry["source_path"], group / "payload"): entry["source_identity"]
                       for entry in manifest["entries"]}
            actual = {path for path in (group / "payload").rglob("*") if not path.is_dir()}
            if not actual.issubset(sources) or any(not _matches(path, sources[path]) for path in actual):
                return {"ok": False, "refusal": "local-spool-changed-retain"}
            for path in actual:
                with _parent(path) as directory:
                    if not reader_lease.file_id_matches(sources[path], _identity(path, directory=directory)):
                        return {"ok": False, "refusal": "local-spool-changed-retain"}
                    os.unlink(path.name, dir_fd=directory)
                    os.fsync(directory)
            reservation["released"] = True
            _write(group / "reservation.json", reservation)
            return {"ok": True, "released_bytes": reservation["ceiling_bytes"]}


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
                              "destination_path", "identity"}
    if set(proof) - allowed:
        raise SpoolError("copy proof has unknown fields")
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


def _check_receipt(receipt, manifest, record):
    if isinstance(receipt, dict) and "pacing" in receipt:
        _check_pacing(receipt["pacing"])
        receipt = {key: value for key, value in receipt.items() if key != "pacing"}
    if (not isinstance(receipt, dict) or set(receipt) != {
            "schema", "export_key", "manifest_sha256", "entries"}
            or receipt["schema"] != SCHEMA
            or receipt["export_key"] != record["export_key"]
            or receipt["manifest_sha256"] != record["manifest_sha256"]
            or not isinstance(receipt["entries"], list)
            or len(receipt["entries"]) != len(manifest["entries"])):
        raise SpoolError("export receipt binding is corrupt")
    for index, (entry, proof) in enumerate(zip(manifest["entries"], receipt["entries"])):
        _check_copy(proof, entry, index, record["manifest_sha256"])
        if not proof.get("complete") or not _matches(entry["destination_path"], proof["identity"]):
            raise SpoolError("export-destination-changed")


def _export_entry(group, entry, index, manifest_sha256, pacer=None):
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
        if proof and proof.get("complete"):
            if not reader_lease.file_id_matches(proof["identity"], dest_identity(destination)):
                raise SpoolError("completed destination changed")
            return proof
        # Pin the real local source before any unacknowledged recovery can
        # remove a canonical incarnation. Every ancestor and the leaf refuse
        # symlinks, and subsequent I/O uses these held directory descriptors.
        source_fd = os.open(source.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                            dir_fd=source_parent)
        with os.fdopen(source_fd, "rb") as reader:
            source_identity = reader_lease.portable_identity(os.fstat(reader.fileno()))
            if not stat.S_ISREG(os.fstat(reader.fileno()).st_mode) or not reader_lease.file_id_matches(
                    entry["source_identity"], source_identity):
                raise SpoolError("local source changed before export")
            current = dest_identity(destination)
            if current is not None:
                ready = (proof or {}).get("ready_identity", {})
                # Only this exact export's known pre-publication inode gets
                # the expected link/rename ctime transition. No post-ACK file
                # gets that exception: completed proofs above require all4.
                if any(current.get(key) != ready.get(key) for key in ("ino", "size", "mtime_ns")):
                    raise SpoolError("unowned or changed canonical destination")
                if not reader_lease.file_id_matches(current, dest_identity(destination)):
                    raise SpoolError("canonical incarnation changed during recovery")
                os.unlink(destination.name, dir_fd=destination_parent)
                os.fsync(destination_parent)
                # Never overlap an old unacknowledged canonical copy with a
                # new temporary. The pinned unchanged LOCAL file stays owned,
                # so temp=0 remains bounded by the canonical payload budget.
            temporary_identity = dest_identity(temporary)
            if temporary_identity is not None:
                if temporary_identity["ino"] != (proof or {}).get("temporary_ino"):
                    raise SpoolError("unowned export temporary")
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
                        raise SpoolError("source exceeded declared bytes")
                    writer.write(block)
                    digest.update(block)
                    if pacer is not None:
                        pacer.wrote(len(block), writer)
                writer.flush()
                os.fsync(writer.fileno())
            if (copied != entry["bytes"] or digest.hexdigest() != entry["sha256"]
                    or not reader_lease.file_id_matches(entry["source_identity"],
                        reader_lease.portable_identity(os.fstat(reader.fileno())))
                    or not reader_lease.file_id_matches(entry["source_identity"],
                        _identity(source, directory=source_parent))):
                raise SpoolError("source changed or digest mismatched while exporting")
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
        receipt = _read(group / "receipt.json")
        if receipt is not None:
            _check_receipt(receipt, manifest, record)
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
        landed = [_export_entry(group, entry, index, manifest_sha256, pacer)
                  for index, entry in enumerate(manifest["entries"])]
        receipt = {"schema": SCHEMA, "export_key": export_key,
                   "manifest_sha256": manifest_sha256, "entries": landed}
        if pacer is not None:
            receipt["pacing"] = pacer.record(pace_tier)
        _check_receipt(receipt, manifest, record)
        _write(group / "receipt.json", receipt)
        return {"ok": True, "entries": len(landed)}


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
