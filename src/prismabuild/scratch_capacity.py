"""Operator-configured dedicated roots use the ordinary host reservation owner.

Observation never writes scratch, provisions storage or benchmarks it. A live
holder prevents new reservations on its root. Uncertain identity or
capacity offers zero; held tokens remain owned by normal cleanup/recovery.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import os
from pathlib import Path

from . import digest_primitives, filesystem_capacity, filesystem_floor, immutable_files, local_scratch
from . import scratch_lifetime
import re


CONFIG_SCHEMA = "prismabuild.scratch_capacity_roots.v1"
OBSERVATION_SCHEMA = "prismabuild.scratch_capacity_observation.v1"
IDENTITY_FIELDS = frozenset({
    "device", "filesystem", "filesystem_type", "root_inode", "birthtime_ns",
})
BACKING_FIELDS = frozenset({"dataset", "guid", "quota_bytes", "reservation_bytes"})
_DATASET = re.compile(r"[A-Za-z][A-Za-z0-9_.:-]*(?:/[A-Za-z0-9_.:-]+)+\Z")


def zfs_properties(dataset: str) -> dict[str, str]:
    properties = ("guid", "available", "quota", "reservation", "mountpoint", "mounted")
    try:
        raw = filesystem_floor._command([
            "zfs", "get", "-Hp", "-o", "property,value", ",".join(properties), dataset])
        rows = [line.split("\t") for line in raw.splitlines()]
        if (len(raw.encode()) > 16384 or len(rows) != len(properties)
                or any(len(row) != 2 for row in rows)
                or {row[0] for row in rows} != set(properties)):
            raise ValueError("ZFS property population differs")
        return dict(rows)
    except (OSError, ValueError, filesystem_floor.FloorError) as exc:
        raise local_scratch.LocalScratchError("dedicated ZFS allocation is unreadable") from exc


def inspect_root(root: str) -> dict[str, object]:
    """Observe a dedicated mount through checked directory descriptors."""
    if local_scratch._canonical_root(root) is None or root.startswith("//"):
        raise local_scratch.LocalScratchError("dedicated scratch root must be canonical")
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
    fds, identities = [], []
    try:
        fds.append(os.open("/", flags))
        identities.append(scratch_lifetime._directory_identity(fds[0], "/"))
        for name in Path(root).parts[1:]:
            fds.append(os.open(name, flags, dir_fd=fds[-1]))
            identities.append(scratch_lifetime._directory_identity(fds[-1], name))
        identity = local_scratch.descriptor_identity(fds[-1])
        mount = local_scratch.descriptor_backing(fds[-1])
        parent = local_scratch.descriptor_backing(fds[-2])
        if (mount["mount_id"] == parent["mount_id"] or mount["mountpoint"] != root
                or mount["mount_root"] != "/"):
            raise local_scratch.LocalScratchError("dedicated scratch needs its own mounted filesystem")
        if identity["filesystem_type"] != "zfs":
            raise local_scratch.LocalScratchError("dedicated capacity currently requires reserved ZFS backing")
        identity["birthtime_ns"] = scratch_lifetime._directory_birthtime(fds[-1])
        sampled = os.fstatvfs(fds[-1])
        scratch_lifetime._check_scratch_links(fds, identities)
        size = sampled.f_blocks * sampled.f_frsize
        free = sampled.f_bavail * sampled.f_frsize
        if min(size, free) < 0 or not size or free > size:
            raise local_scratch.LocalScratchError("dedicated scratch capacity counters invalid")
        return {"identity": identity, "mount": mount, "size_bytes": size, "free_bytes": free,
                "size_inodes": sampled.f_files, "free_inodes": sampled.f_favail}
    finally:
        for fd in reversed(fds):
            os.close(fd)


@dataclass(frozen=True)
class RootBudget:
    root: str
    max_gib: int
    min_free_gib: int
    identity: dict[str, object]
    backing: dict[str, object]

    @classmethod
    def parse(cls, entry):
        if (not isinstance(entry, dict)
                or set(entry) != {"root", "max_gib", "min_free_gib", "identity", "backing"}
                or type(entry["max_gib"]) is not int or entry["max_gib"] <= 0
                or type(entry["min_free_gib"]) is not int or entry["min_free_gib"] <= 0):
            raise local_scratch.LocalScratchError("dedicated scratch budget malformed")
        root, identity = entry["root"], entry["identity"]
        local_scratch.capacity_kind(root)
        if not isinstance(identity, dict) or identity.get("filesystem_type") != "zfs":
            raise local_scratch.LocalScratchError("dedicated capacity currently requires reserved ZFS backing")
        if (set(identity) != IDENTITY_FIELDS
                or type(identity["birthtime_ns"]) is not int or identity["birthtime_ns"] <= 0
                or any(not isinstance(identity[k], str) or not identity[k].isascii()
                       or not identity[k].isdigit() or int(identity[k]) < 0
                       for k in ("device", "filesystem", "root_inode"))):
            raise local_scratch.LocalScratchError("dedicated scratch identity malformed")
        backing = entry["backing"]
        minimum = (entry["max_gib"] + entry["min_free_gib"]) * local_scratch.GIB
        if (not isinstance(backing, dict) or set(backing) != BACKING_FIELDS
                or not isinstance(backing["dataset"], str) or not _DATASET.fullmatch(backing["dataset"])
                or any(part in {".", ".."} for part in backing["dataset"].split("/"))
                or not isinstance(backing["guid"], str) or not backing["guid"].isascii()
                or not backing["guid"].isdigit() or int(backing["guid"]) <= 0
                or any(type(backing[k]) is not int or backing[k] < minimum
                       for k in ("quota_bytes", "reservation_bytes"))):
            raise local_scratch.LocalScratchError("dedicated ZFS quota/reservation must cover its ceiling and floor")
        return cls(root, entry["max_gib"], entry["min_free_gib"], dict(identity), dict(backing))

    @property
    def kind(self) -> str:
        return local_scratch.capacity_kind(self.root)

    def inspect(self) -> dict[str, object]:
        found = inspect_root(self.root)
        if found["identity"] != self.identity:
            raise local_scratch.LocalScratchError("dedicated scratch root identity changed")
        if (found["mount"]["source"] != self.backing["dataset"]
                or found["mount"]["filesystem_type"] != "zfs"):
            raise local_scratch.LocalScratchError("dedicated scratch backing source changed")
        properties = zfs_properties(self.backing["dataset"])
        numeric = ("guid", "quota", "reservation", "available")
        if (any(not isinstance(properties.get(k), str) or not properties[k].isascii()
                or not properties[k].isdigit() for k in numeric)
                or properties.get("mountpoint") != self.root or properties.get("mounted") != "yes"
                or properties["guid"] != self.backing["guid"]
                or int(properties["quota"]) != self.backing["quota_bytes"]
                or int(properties["reservation"]) != self.backing["reservation_bytes"]):
            raise local_scratch.LocalScratchError("dedicated ZFS allocation changed or is unreserved")
        rechecked = inspect_root(self.root)
        if rechecked["identity"] != found["identity"] or rechecked["mount"] != found["mount"]:
            raise local_scratch.LocalScratchError("dedicated scratch mount changed during allocation read")
        found["free_bytes"] = min(found["free_bytes"], rechecked["free_bytes"], int(properties["available"]))
        size_inodes, free_inodes = found["size_inodes"], found["free_inodes"]
        if (type(size_inodes) is not int or type(free_inodes) is not int
                or min(size_inodes, free_inodes) < 0
                or free_inodes < -(-size_inodes * filesystem_capacity.INODE_FLOOR_PERCENT // 100)):
            raise local_scratch.LocalScratchError("dedicated scratch inode headroom unavailable")
        floor = self.min_free_gib * local_scratch.GIB
        measured = max(0, (found["free_bytes"] - floor) // local_scratch.GIB)
        return {**found, "root": self.root, "kind": self.kind, "backing": dict(self.backing),
                "max_gib": self.max_gib, "floor_bytes": floor,
                "measured_gib": min(self.max_gib, measured)}


class RootInputs:
    """One captured operator configuration; all filesystem evidence is fresh."""

    def __init__(self, config_path, *, ordinary_root: str):
        raw = immutable_files.read_stable_file(
            config_path, where="dedicated scratch capacity config", max_bytes=65536)
        config = digest_primitives.decode_strict_json(raw, where="scratch capacity config")
        if (not isinstance(config, dict) or set(config) != {"schema", "roots"}
                or config["schema"] != CONFIG_SCHEMA or not isinstance(config["roots"], list)
                or not 1 <= len(config["roots"]) <= 32):
            raise local_scratch.LocalScratchError("dedicated scratch capacity config malformed")
        self.roots: dict[str, RootBudget] = {}
        self.detail: dict[str, object] = {}
        self.ordinary_root = ordinary_root
        backings = set()
        for entry in config["roots"]:
            budget = RootBudget.parse(entry)
            root, backing = budget.root, budget.backing
            if (root in self.roots or backing["guid"] in backings
                    or any(b.backing["dataset"] == backing["dataset"] for b in self.roots.values())):
                raise local_scratch.LocalScratchError("dedicated scratch roots alias one capacity")
            backings.add(backing["guid"])
            self.roots[root] = budget

    def _ordinary_identity(self):
        return local_scratch.root_backing(self.ordinary_root)

    def _inspect(self, budget):
        found = budget.inspect()
        ordinary = self._ordinary_identity()
        if all(found["mount"][k] == ordinary[k] for k in ("major_minor", "source")):
            raise local_scratch.LocalScratchError("dedicated scratch aliases ordinary spool backing")
        return found

    def require(self, variables: Mapping[str, str]) -> list[dict[str, object]]:
        selected = local_scratch.capacity_roots(variables)
        evidence = []
        for pair in local_scratch.scratch_pairs(variables):
            if pair["root_env"] not in selected:
                continue
            budget = self.roots.get(pair["root"])
            if budget is None or pair["max_bytes"] > budget.max_gib * local_scratch.GIB:
                raise local_scratch.LocalScratchError("dedicated scratch root/bound is not configured")
            found = self._inspect(budget)
            if found["measured_gib"] < -(-pair["max_bytes"] // local_scratch.GIB):
                raise local_scratch.LocalScratchError("insufficient dedicated scratch room before launch")
            evidence.append(found)
        return evidence

    def observe(self, ledger) -> tuple[dict[str, int], dict[str, int]]:
        """Measured declared/live capacity, with no credit for holders' writes."""
        declared = {b.kind: 0 for b in self.roots.values()}
        live = dict(declared)
        records = []
        try:
            total = ledger.capacity()
            before = ledger.held()
        except (OSError, ValueError) as exc:
            self.detail = {"schema": OBSERVATION_SCHEMA, "roots": [],
                           "error": f"holder/capacity census unavailable: {type(exc).__name__}"}
            return declared, live
        for budget in self.roots.values():
            try:
                found = self._inspect(budget)
                after = ledger.held()
                held = max(before.get(budget.kind, 0), after.get(budget.kind, 0))
                offered = (min(budget.max_gib, total.get(budget.kind, 0), held) if held
                           else found["measured_gib"])
                declared[budget.kind] = budget.max_gib if held else offered
                live[budget.kind] = offered
                records.append({**found, "offered_gib": offered,
                                "basis": "holders-only" if held else "measured-free"})
            except (OSError, ValueError) as exc:
                records.append({"root": budget.root, "kind": budget.kind,
                                "offered_gib": 0, "error": str(exc)[:500]})
        for kind in total:
            if local_scratch.is_capacity_kind(kind) and kind not in live:
                declared[kind] = live[kind] = 0
        self.detail = {"schema": OBSERVATION_SCHEMA, "roots": records}
        return declared, live


def validate_observation(value):
    """Check persisted launch evidence; this grants no admission authority."""
    if (not isinstance(value, dict) or set(value) != {"schema", "roots"}
            or value["schema"] != OBSERVATION_SCHEMA or not isinstance(value["roots"], list)
            or not 1 <= len(value["roots"]) <= 32):
        raise local_scratch.LocalScratchError("dedicated scratch launch observation malformed")
    fields = {"root", "kind", "identity", "mount", "backing", "size_bytes", "free_bytes",
              "size_inodes", "free_inodes", "max_gib", "floor_bytes", "measured_gib"}
    numbers = {"size_bytes", "free_bytes", "size_inodes", "free_inodes", "max_gib",
               "floor_bytes", "measured_gib"}
    seen = set()
    for row in value["roots"]:
        if (not isinstance(row, dict) or set(row) != fields
                or any(type(row[k]) is not int or row[k] < 0 for k in numbers)
                or not row["size_bytes"] or row["free_bytes"] > row["size_bytes"]
                or row["floor_bytes"] % local_scratch.GIB):
            raise local_scratch.LocalScratchError("dedicated scratch launch counters malformed")
        budget = RootBudget.parse({"root": row["root"], "max_gib": row["max_gib"],
                                   "min_free_gib": row["floor_bytes"] // local_scratch.GIB,
                                   "identity": row["identity"], "backing": row["backing"]})
        mount = row["mount"]
        if (not isinstance(mount, dict) or set(mount) != {
                "mount_id", "major_minor", "mount_root", "mountpoint", "filesystem_type", "source"}
                or type(mount["mount_id"]) is not int or mount["mount_id"] <= 0
                or not isinstance(mount["major_minor"], str)
                or re.fullmatch(r"[0-9]+:[0-9]+", mount["major_minor"]) is None
                or mount["mount_root"] != "/" or mount["mountpoint"] != budget.root
                or mount["filesystem_type"] != "zfs" or mount["source"] != budget.backing["dataset"]
                or row["kind"] != budget.kind or budget.root in seen
                or row["measured_gib"] != min(budget.max_gib, max(
                    0, (row["free_bytes"] - row["floor_bytes"]) // local_scratch.GIB))):
            raise local_scratch.LocalScratchError("dedicated scratch launch backing disagrees")
        seen.add(budget.root)
    return value
