"""Whole-directory resident sets, distinct from rolling residency windows.

The immutable body names bytes. The lease journal is append-only. Mutable
copy records describe each host, never an action admission requirement.
"""
from __future__ import annotations

from collections.abc import Mapping
import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
import tempfile
import time

from . import core, posix_lock

SET_SCHEMA = "prismabuild.resident_set.v1"
COPY_SCHEMA = "prismabuild.resident_copy.v1"
POLICY_SCHEMA = "prismabuild.local_tier_policy.v1"
COPY_STATES = frozenset({"absent", "copying", "resident", "evicting"})


def _name(value, where):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", value):
        raise ValueError(f"{where} must be a safe nonempty name")
    return value


def _set_id(value):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value):
        raise ValueError("set_id must be a sha256 digest")
    return value


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def fsync_directory(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def write_record(path, value):
    """Durable replace for mutable records; never used for the set body."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix="." + path.name, dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(_json(value) + b"\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp, path)
        fsync_directory(path.parent)
    finally:
        Path(tmp).unlink(missing_ok=True)


def validate_lease(value, *, now):
    if not isinstance(value, Mapping) or set(value) not in ({"until", "hard_max"}, {"campaign", "hard_max"}):
        raise ValueError("lease requires until or campaign and hard_max")
    maximum = value["hard_max"]
    if type(maximum) not in (int, float) or not math.isfinite(maximum) or maximum <= now:
        raise ValueError("lease hard_max must be a finite future timestamp")
    out = dict(value)
    if "until" in out:
        until = out["until"]
        if type(until) not in (int, float) or not math.isfinite(until) or not now < until <= maximum:
            raise ValueError("lease until must be in the future and no later than hard_max")
    else:
        _name(out["campaign"], "campaign")
    return out


def directory_files(root):
    """Names and sizes without hashing; symlinks and special files are unsafe."""
    root = Path(root)
    if not root.is_absolute() or root.is_symlink() or not root.is_dir():
        raise ValueError("canonical_root must be an absolute regular directory")
    found = {}
    def walk(directory):
        with os.scandir(directory) as entries:
            for entry in entries:
                info = entry.stat(follow_symlinks=False)
                if stat.S_ISDIR(info.st_mode):
                    walk(Path(entry.path))
                elif stat.S_ISREG(info.st_mode):
                    found[str(Path(entry.path).relative_to(root))] = info.st_size
                else:
                    raise ValueError(f"directory contains symlink or special file: {entry.path}")
    walk(root)
    return found


def validate_set_manifest(manifest, canonical_root):
    normalized = core.validate_data_manifest(manifest)
    root = Path(canonical_root)
    if not root.is_absolute() or str(root) != canonical_root or root == Path("/"):
        raise ValueError("canonical_root must be a normalized absolute directory")
    wanted = {}
    for entry in normalized["entries"]:
        path = Path(entry["path"])
        if not path.is_relative_to(root) or path == root:
            raise ValueError("manifest path is outside canonical_root")
        relative = str(path.relative_to(root))
        if relative in wanted or entry["offset"] != 0:
            raise ValueError("resident set requires one whole-file entry per path")
        if entry["sha256"] is None:
            raise ValueError("resident set requires real sha256 on every entry")
        wanted[relative] = entry["bytes"]
    if directory_files(root) != wanted:
        raise ValueError("manifest must cover the whole canonical directory with matching sizes")
    return normalized


def read_policy(path):
    policy = json.loads(Path(path).read_text())
    if not isinstance(policy, dict) or set(policy) != {"schema", "hosts"} or policy["schema"] != POLICY_SCHEMA or not isinstance(policy["hosts"], dict):
        raise ValueError("invalid local_tier_policy")
    for host, spec in policy["hosts"].items():
        _name(host, "host")
        if not isinstance(spec, dict) or set(spec) != {"root", "maximum_gib", "floor_fraction", "docker_allowance_gib"}:
            raise ValueError("local tier host policy requires root, maximum, floor and docker allowance")
        if not isinstance(spec["root"], str) or not Path(spec["root"]).is_absolute() or Path(spec["root"]) == Path("/"):
            raise ValueError("local tier root must be an absolute non-root directory")
        for key in ("maximum_gib", "docker_allowance_gib", "floor_fraction"):
            if type(spec[key]) not in (int, float) or not math.isfinite(spec[key]) or spec[key] < 0:
                raise ValueError(f"invalid local tier {key}")
        if not .05 <= spec["floor_fraction"] < 1:
            raise ValueError("local tier floor_fraction must preserve at least five percent")
    return policy


class ResidentSets:
    def __init__(self, queue_root):
        self.queue_root = Path(queue_root)
        self.root = self.queue_root / "resident-sets"

    def set_path(self, set_id):
        return self.root / _set_id(set_id) / "body.json"

    def copy_path(self, set_id, host):
        return self.set_path(set_id).parent / "copies" / (_name(host, "host") + ".json")

    def lock(self, set_id):
        return posix_lock.held(self.set_path(set_id).parent / ".record.lock")

    def read(self, set_id):
        record = json.loads(self.set_path(set_id).read_text())
        if record.get("schema") != SET_SCHEMA or record.get("set_id") != set_id or record.get("immutable") is not True:
            raise ValueError("invalid resident set body")
        if hashlib.sha256(_json(record["manifest"])).hexdigest() != set_id:
            raise ValueError("resident set manifest digest mismatch")
        return record

    def _append_lease(self, set_id, row):
        row = {**row, "manifest_sha256": set_id}
        path = self.set_path(set_id).parent / "lease.jsonl"
        with path.open("ab") as stream:
            stream.write(_json(row) + b"\n")
            stream.flush()
            os.fsync(stream.fileno())
        fsync_directory(path.parent)

    def publish(self, *, manifest, canonical_root, hosts, lease, created_by, now=None):
        now = time.time() if now is None else now
        manifest = validate_set_manifest(manifest, canonical_root)
        lease = validate_lease(lease, now=now)
        if not isinstance(hosts, list) or not hosts or len(set(hosts)) != len(hosts):
            raise ValueError("hosts must be a nonempty unique list")
        hosts = sorted(_name(host, "host") for host in hosts)
        if not isinstance(created_by, str) or not created_by.strip():
            raise ValueError("created_by is required")
        set_id = hashlib.sha256(_json(manifest)).hexdigest()
        record = {"schema": SET_SCHEMA, "set_id": set_id, "manifest": manifest,
                  "canonical_root": canonical_root, "hosts": hosts, "lease": lease,
                  "immutable": True, "created_by": created_by, "created_unix": now}
        with self.lock(set_id):
            path = self.set_path(set_id)
            if path.exists():
                existing = self.read(set_id)
                if any(existing[key] != record[key] for key in ("manifest", "canonical_root", "hosts")):
                    raise ValueError("resident set immutable body conflicts with publication")
                raise ValueError("resident set already published; its body cannot be rewritten")
            from . import local_tier, pool
            queue = pool.PoolQueue(self.queue_root)
            acquired = local_tier.reserve(queue, set_id, hosts, manifest["total_bytes"])
            try:
                core._atomic_publish(path, _json(record) + b"\n")
                self._append_lease(set_id, {"event": "published", "lease": lease, "unix": now, "by": created_by})
                for host in hosts:
                    self.write_copy(set_id, host, {"state": "absent", "local_root": None,
                        "verification": [], "bytes": 0, "completed_unix": None})
            except BaseException:
                # Once the immutable body exists, recovery owns its reservations.
                if not path.exists():
                    for ledger in acquired:
                        ledger.release(set_id)
                raise
        return record

    def write_copy(self, set_id, host, record):
        if record.get("state") not in COPY_STATES:
            raise ValueError("invalid resident copy state")
        if host not in self.read(set_id)["hosts"]:
            raise ValueError("host is not declared by resident set")
        write_record(self.copy_path(set_id, host), {**record, "schema": COPY_SCHEMA, "set_id": set_id, "host": host})

    def read_copy(self, set_id, host):
        record = json.loads(self.copy_path(set_id, host).read_text())
        if (record.get("schema") != COPY_SCHEMA or record.get("set_id") != set_id
                or record.get("host") != host or record.get("state") not in COPY_STATES):
            raise ValueError("invalid resident copy record")
        return record

    def status(self, set_id=None):
        if set_id is None:
            if not self.root.exists():
                return []
            return [self.status(path.name) for path in sorted(self.root.iterdir()) if path.is_dir()]
        record = self.read(set_id)
        log = self.set_path(set_id).parent / "lease.jsonl"
        record["lease_log"] = [json.loads(line) for line in log.read_text().splitlines()]
        record["copies"] = {host: self.read_copy(set_id, host) for host in record["hosts"]}
        return record

    def release(self, set_id, *, by, now=None):
        with self.lock(set_id):
            self.read(set_id)
            self._append_lease(set_id, {"event": "released", "unix": time.time() if now is None else now, "by": by})
        return self.status(set_id)
