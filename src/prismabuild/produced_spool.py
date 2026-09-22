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

from . import core, movement_actions, pool, produced_output as po, reader_lease

API_VERSION = 1
ROOT_ENV = "PRISMABUILD_PRODUCED_SPOOL_ROOT"
MAX_ENV = "PRISMABUILD_PRODUCED_SPOOL_MAX_BYTES"
SCHEMA = "prismabuild.produced_spool.v1"


class SpoolError(RuntimeError):
    pass


class SpoolCapacityDeferred(SpoolError):
    """Only the bounded local spool is full; completed exports may free it."""


def _read(path):
    try:
        body = core._decode_strict_json(Path(path).read_bytes(), where="spool record")
    except FileNotFoundError:
        return None
    except (OSError, ValueError) as exc:
        raise SpoolError(f"unknown-retain: {path}: {exc}") from exc
    if not isinstance(body, dict):
        raise SpoolError("unknown-retain: spool record is not an object")
    return body


def _sync_directory(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _write(path, body):
    path = Path(path)
    raw = json.dumps(body, sort_keys=True, separators=(",", ":")).encode() + b"\n"
    tmp = path.with_name(path.name + f".{os.getpid()}.tmp")
    with open(tmp, "xb") as handle:
        handle.write(raw)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)
    _sync_directory(path.parent)


@contextmanager
def _lock(path):
    with open(path, "a+b") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


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


def _identity(path):
    value = os.lstat(path)
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
        self._live()
        row = pool._read_json(queue.item_path(pool.CLAIMED, self.owner)) or {}
        self.host = str(row.get("claimed_host") or "")
        if not self.host:
            raise SpoolError("source host is unknown")
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

    def submit_group(self, batch_id, entries):
        self._live()
        group = self._group(batch_id)
        with _lock(group / ".export.lock"):
            reservation = self._reservation(group)
            if reservation is None or reservation.get("released"):
                raise SpoolError("no active spool reservation")
            old = _read(group / "export.json")
            if old is not None:
                manifest = _read(group / "manifest.json")
                keys = ("source_path", "destination_path", "bytes", "sha256")
                if (not isinstance(entries, (list, tuple)) or manifest is None
                        or [{key: entry.get(key) for key in keys} for entry in entries]
                        != [{key: entry.get(key) for key in keys} for entry in manifest["entries"]]):
                    raise SpoolError("export replay changed its group entries")
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
                digest = entry.get("sha256")
                po._hex64(digest, where="spool sha256")
                checked.append({"source_path": str(source), "destination_path": str(destination),
                                "bytes": size, "sha256": digest, "source_identity": identity})
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
            action = movement_actions.seal_movement_action(
                templated["template"],
                command=["/usr/bin/python3", str(tool), "--queue", str(self.queue.root),
                         "--manifest", str(manifest_path), "--manifest-sha256", manifest_input["sha256"]],
                demand={"cpu": 1, "mem_gb": 1}, tags=[self.host],
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
            self.queue.publish(action_key=key, cas_root=self.cas_root,
                worker_script=launch["worker_script"], **launch["addressing"],
                resources={"cpu": 1, "mem_gb": 1}, tags=[self.host],
                max_attempts=3, retry_safe=True, priority=launch["priority"],
                refuse_withdrawn=True)
        elif state in {"failed", "withdrawn"}:
            return {"ok": False, "complete": False, "export_key": key,
                    "refusal": f"export-{state}"}
        return {"ok": True, "complete": False, "export_key": key}

    def poll_group(self, batch_id):
        group = self._group(batch_id)
        record = _read(group / "export.json")
        if record is None:
            return {"ok": True, "complete": False}
        key = record["export_key"]
        receipt = _read(group / "receipt.json")
        if receipt is not None:
            if (receipt.get("export_key") != key
                    or receipt.get("manifest_sha256") != record["manifest_sha256"]):
                return {"ok": False, "complete": False, "refusal": "export-receipt-binding"}
            manifest = _read(group / "manifest.json")
            if manifest is None or len(receipt.get("entries", [])) != len(manifest["entries"]):
                return {"ok": False, "complete": False, "refusal": "export-receipt-incomplete"}
            for entry, landed in zip(manifest["entries"], receipt["entries"]):
                if (landed.get("destination_path") != entry["destination_path"]
                        or not _matches(entry["destination_path"], landed.get("identity"))):
                    return {"ok": False, "complete": False, "refusal": "export-destination-changed"}
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
            reservation = self._reservation(group)
            if reservation.get("released"):
                return {"ok": True, "duplicate": True}
            manifest = _read(group / "manifest.json")
            sources = {Path(entry["source_path"]): entry["source_identity"] for entry in manifest["entries"]}
            actual = {path for path in (group / "payload").rglob("*") if not path.is_dir()}
            if not actual.issubset(sources) or any(not _matches(path, sources[path]) for path in actual):
                return {"ok": False, "refusal": "local-spool-changed-retain"}
            for path in actual:
                path.unlink()
            reservation["released"] = True
            _write(group / "reservation.json", reservation)
            return {"ok": True, "released_bytes": reservation["ceiling_bytes"]}


def export_group(queue, manifest_path, manifest_sha256, export_key):
    """Worker payload: local-only reads, canonical writes, durable acknowledgement."""
    raw = Path(manifest_path).read_bytes()
    if hashlib.sha256(raw).hexdigest() != manifest_sha256:
        raise SpoolError("sealed export manifest changed")
    manifest = json.loads(raw)
    group = Path(manifest["group"])
    with _lock(group / ".export.lock"):
        record = _read(group / "export.json")
        if record is None or record.get("export_key") != export_key or record.get("manifest_sha256") != manifest_sha256:
            raise SpoolError("export action does not own this local group")
        claim = pool._read_json(queue.item_path(pool.CLAIMED, export_key)) or {}
        if claim.get("claimed_host") != manifest["host"]:
            raise SpoolError("export is not claimed on the source host")
        prewrite = po._read_prewrite(po._prewrites_dir(queue.root, manifest["instance"]) /
                                    f"{manifest['batch_id']}.prewrite.json")
        receipt = _read(group / "receipt.json")
        if receipt is not None:
            if (receipt.get("export_key") != export_key
                    or receipt.get("manifest_sha256") != manifest_sha256
                    or not isinstance(receipt.get("entries"), list)
                    or len(receipt["entries"]) != len(manifest["entries"])
                    or [entry.get("destination_path") for entry in receipt["entries"]]
                    != [entry["destination_path"] for entry in manifest["entries"]]):
                raise SpoolError("export receipt binding is corrupt")
            if all(_matches(entry["destination_path"], entry["identity"]) for entry in receipt["entries"]):
                return {"ok": True, "duplicate": True, "entries": len(receipt["entries"])}
            raise SpoolError("acknowledged canonical destination changed")
        if prewrite is None or prewrite.get("owner_action_key") != manifest["owner"]:
            raise SpoolError("canonical prewrite authority is absent")
        landed = []
        for index, entry in enumerate(manifest["entries"]):
            source = Path(entry["source_path"])
            destination = Path(entry["destination_path"])
            temporary = Path(str(destination) + ".tmp")
            proof_path = group / f"copy-{index}.json"
            proof = _read(proof_path)
            if proof and proof.get("complete"):
                if not _matches(destination, proof["identity"]):
                    raise SpoolError("completed destination changed")
                landed.append(proof)
                continue
            if not _matches(source, entry["source_identity"]):
                raise SpoolError("local source changed before export")
            if str(destination) not in prewrite["paths"] or str(temporary) not in prewrite["paths"]:
                raise SpoolError("canonical export path is not owned")
            destination.parent.mkdir(parents=True, exist_ok=True)
            # An interrupted final rename may have landed without a post-rename
            # proof. Recopy from the unchanged LOCAL source; never reread HDD
            # payloads, and never replace an unrelated canonical incarnation.
            replace_owned = destination.exists()
            if replace_owned:
                current = _identity(destination)
                prior = (proof or {}).get("ready_identity", {})
                prior_destination = (proof or {}).get("previous_identity")
                if (not reader_lease.file_id_matches(prior_destination, current)
                        and any(current.get(k) != prior.get(k)
                                for k in ("ino", "size", "mtime_ns"))):
                    raise SpoolError("unowned or changed canonical destination")
                owned_destination = current
            if temporary.exists():
                current = _identity(temporary)
                if current["ino"] != (proof or {}).get("temporary_ino"):
                    raise SpoolError("unowned export temporary")
                temporary.unlink()
            fd = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
            proof = {"temporary_ino": os.fstat(fd).st_ino}
            if replace_owned:
                proof["previous_identity"] = owned_destination
            _write(proof_path, proof)
            digest = hashlib.sha256()
            copied = 0
            with os.fdopen(fd, "wb") as writer, open(source, "rb") as reader:
                if not reader_lease.file_id_matches(entry["source_identity"], reader_lease.portable_identity(os.fstat(reader.fileno()))):
                    raise SpoolError("local source changed at open")
                while block := reader.read(1 << 20):
                    copied += len(block)
                    if copied > entry["bytes"]:
                        raise SpoolError("source exceeded declared bytes")
                    writer.write(block)
                    digest.update(block)
                writer.flush()
                os.fsync(writer.fileno())
            if copied != entry["bytes"] or not _matches(source, entry["source_identity"]):
                raise SpoolError("source changed while exporting")
            if entry["sha256"] is not None and digest.hexdigest() != entry["sha256"]:
                raise SpoolError("local source digest mismatch")
            proof.update(ready_identity=_identity(temporary), sha256=digest.hexdigest())
            _write(proof_path, proof)
            if replace_owned:
                # A prior interrupted export owns this exact destination.
                # Recheck its incarnation immediately before replacement.
                current = _identity(destination)
                if not reader_lease.file_id_matches(owned_destination, current):
                    raise SpoolError("canonical destination changed during recovery")
                os.replace(temporary, destination)
            else:
                # Publish without ever replacing an unexpected destination
                # that appeared between the ownership check and this call.
                os.link(temporary, destination)
                temporary.unlink()
            _sync_directory(destination.parent)
            proof.update(complete=True, destination_path=str(destination), identity=_identity(destination))
            _write(proof_path, proof)
            landed.append(proof)
        receipt = {"schema": SCHEMA, "export_key": export_key,
                   "manifest_sha256": manifest_sha256, "entries": landed}
        _write(group / "receipt.json", receipt)
        return {"ok": True, "entries": len(landed)}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--queue", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--manifest-sha256", required=True)
    args = parser.parse_args(argv)
    result = export_group(pool.PoolQueue(args.queue), args.manifest,
                          args.manifest_sha256, os.environ.get("PRISMABUILD_ACTION_KEY", ""))
    print(json.dumps(result, sort_keys=True))
    return 0
