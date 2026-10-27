"""Operator-configured dedicated roots use the ordinary host reservation owner.

Observation never writes scratch, provisions storage or benchmarks it. A live
holder freezes its root's previously measured total. Uncertain identity or
capacity offers zero; held tokens remain owned by normal cleanup/recovery.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import os
from pathlib import Path

from . import digest_primitives, filesystem_capacity, immutable_files, local_scratch
from . import scratch_lifetime


CONFIG_SCHEMA = "prismabuild.scratch_capacity_roots.v1"
OBSERVATION_SCHEMA = "prismabuild.scratch_capacity_observation.v1"
IDENTITY_FIELDS = frozenset({
    "device", "filesystem", "filesystem_type", "root_inode", "birthtime_ns",
})


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
        parent = local_scratch._descriptor_mount(fds[-2])
        if (identity["device"], identity["filesystem"]) == (
                parent["device"], parent["filesystem"]):
            raise local_scratch.LocalScratchError("dedicated scratch needs its own mounted filesystem")
        identity["birthtime_ns"] = scratch_lifetime._directory_birthtime(fds[-1])
        sampled = os.fstatvfs(fds[-1])
        scratch_lifetime._check_scratch_links(fds, identities)
        size = sampled.f_blocks * sampled.f_frsize
        free = sampled.f_bavail * sampled.f_frsize
        if min(size, free) < 0 or not size or free > size:
            raise local_scratch.LocalScratchError("dedicated scratch capacity counters invalid")
        return {"identity": identity, "size_bytes": size, "free_bytes": free,
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

    @property
    def kind(self) -> str:
        return local_scratch.capacity_kind(self.root)

    def inspect(self) -> dict[str, object]:
        found = inspect_root(self.root)
        if found["identity"] != self.identity:
            raise local_scratch.LocalScratchError("dedicated scratch root identity changed")
        size_inodes, free_inodes = found["size_inodes"], found["free_inodes"]
        if (type(size_inodes) is not int or type(free_inodes) is not int
                or min(size_inodes, free_inodes) < 0
                or free_inodes < -(-size_inodes * filesystem_capacity.INODE_FLOOR_PERCENT // 100)):
            raise local_scratch.LocalScratchError("dedicated scratch inode headroom unavailable")
        floor = self.min_free_gib * local_scratch.GIB
        measured = max(0, (found["free_bytes"] - floor) // local_scratch.GIB)
        return {**found, "root": self.root, "kind": self.kind,
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
            if (not isinstance(entry, dict)
                    or set(entry) != {"root", "max_gib", "min_free_gib", "identity"}
                    or type(entry["max_gib"]) is not int or entry["max_gib"] <= 0
                    or type(entry["min_free_gib"]) is not int or entry["min_free_gib"] <= 0):
                raise local_scratch.LocalScratchError("dedicated scratch budget malformed")
            root, identity = entry["root"], entry["identity"]
            local_scratch.capacity_kind(root)
            if (not isinstance(identity, dict) or set(identity) != IDENTITY_FIELDS
                    or identity["filesystem_type"] not in local_scratch.LOCAL_FILESYSTEM_TYPES
                    or type(identity["birthtime_ns"]) is not int or identity["birthtime_ns"] <= 0
                    or any(not isinstance(identity[k], str) or not identity[k].isascii()
                           or not identity[k].isdigit() or int(identity[k]) < 0
                           for k in ("device", "filesystem", "root_inode"))):
                raise local_scratch.LocalScratchError("dedicated scratch identity malformed")
            backing = (identity["device"], identity["filesystem"])
            if root in self.roots or backing in backings:
                raise local_scratch.LocalScratchError("dedicated scratch roots alias one capacity")
            backings.add(backing)
            self.roots[root] = RootBudget(root, entry["max_gib"], entry["min_free_gib"], identity)

    def _ordinary_identity(self):
        return local_scratch.root_identity(self.ordinary_root)

    def _inspect(self, budget):
        found = budget.inspect()
        ordinary = self._ordinary_identity()
        if all(found["identity"][k] == ordinary[k] for k in ("device", "filesystem")):
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
                held = before.get(budget.kind, 0) or after.get(budget.kind, 0)
                offered = (min(budget.max_gib, total.get(budget.kind, 0)) if held
                           else found["measured_gib"])
                declared[budget.kind] = live[budget.kind] = offered
                records.append({**found, "offered_gib": offered,
                                "basis": "held-total" if held else "measured-free"})
            except (OSError, ValueError) as exc:
                records.append({"root": budget.root, "kind": budget.kind,
                                "offered_gib": 0, "error": str(exc)[:500]})
        for kind in total:
            if local_scratch.is_capacity_kind(kind) and kind not in live:
                declared[kind] = live[kind] = 0
        self.detail = {"schema": OBSERVATION_SCHEMA, "roots": records}
        return declared, live
