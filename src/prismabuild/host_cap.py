"""Validate a declared partial-host envelope at each capacity boundary."""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import os
from pathlib import Path
import re

from . import box_capacity, fleet_roster

CGROUP_ROOT = Path("/sys/fs/cgroup")
MEMBERSHIP = Path("/proc/self/cgroup")
CAP_OVERHEAD_GB = 1
GIB = 1024**3


def cap_cpu_set(value: str) -> frozenset[int]:
    """Decode the kernel's bounded CPU-list spelling, refusing malformed caps."""
    if not isinstance(value, str) or not value or len(value) > 4096:
        raise ValueError("cap cpus must be a CPU list")
    cpus = set()
    for part in value.strip().split(","):
        if re.fullmatch(r"[0-9]+(?:-[0-9]+)?", part) is None:
            raise ValueError("invalid cap CPU list")
        bounds = [int(v) for v in part.split("-")]
        start, end = bounds[0], bounds[-1]
        if not 0 <= start <= end < 65536:
            raise ValueError("invalid cap CPU range")
        cpus.update(range(start, end + 1))
    return frozenset(cpus)


@dataclass(frozen=True)
class HostCap:
    cpus: frozenset[int]
    mem_gb: int
    slice: str

    @classmethod
    def parse(cls, value: object) -> HostCap:
        if not isinstance(value, Mapping) or set(value) != {"cpus", "mem_gb", "slice"}:
            raise ValueError("cap must declare exactly cpus, mem_gb and slice")
        cpus = cap_cpu_set(value["cpus"])
        memory, unit = value["mem_gb"], value["slice"]
        if type(memory) is not int or memory <= CAP_OVERHEAD_GB:
            raise ValueError("cap mem_gb must exceed the one GiB PB overhead reserve")
        if not isinstance(unit, str) or re.fullmatch(r"[A-Za-z0-9_]+\.slice", unit) is None:
            raise ValueError("cap slice must be a top-level system slice")
        return cls(cpus, memory, unit)


def enforce_host_cap(capacity: Mapping[str, int], box: fleet_roster.Box | None,
                     held, *, detail: dict | None = None) -> dict[str, int]:
    """One live clamp for offers and admission, after any observer smoothing.

    Capacity is a total including existing reservations. Bracket slice usage
    with held reads, taking the lower credit if a sibling releases mid-read.
    Missing or mismatched containment closes every resource, including CPU
    lending. Uncapped hosts do not read cgroups and retain their old behavior.
    """
    result = dict(capacity)
    if box is None or "cap" not in box.entry:
        return result
    evidence = {} if detail is None else detail
    try:
        cap = HostCap.parse(box.entry["cap"])
        root = CGROUP_ROOT / cap.slice
        memberships = [line[3:] for line in MEMBERSHIP.read_text().splitlines()
                       if line.startswith("0::")]
        if len(memberships) != 1 or not memberships[0].startswith("/" + cap.slice + "/"):
            raise ValueError("worker is outside the declared system cap slice")
        relative = Path(memberships[0].lstrip("/"))
        if ".." in relative.parts:
            raise ValueError("invalid worker cgroup path")
        effective = cap_cpu_set((root / "cpuset.cpus.effective").read_text().strip())
        worker_cpus = cap_cpu_set((CGROUP_ROOT / relative / "cpuset.cpus.effective").read_text().strip())
        affinity = frozenset(os.sched_getaffinity(0))
        if effective != cap.cpus or worker_cpus != cap.cpus or not affinity <= worker_cpus:
            raise ValueError("live cpuset differs from declared cap")
        maximum = int((root / "memory.max").read_text())
        if maximum != cap.mem_gb * GIB:
            raise ValueError("slice MemoryMax differs from declared cap")
        before = dict(held() if callable(held) else held or {})
        used = int((root / "memory.current").read_text())
        available = box_capacity.mem_available_gb()
        after = dict(held() if callable(held) else held or {})
        if used < 0:
            raise ValueError("invalid slice memory usage")
        credit = max(0, min(before.get("mem_gb", 0), after.get("mem_gb", 0)))
        if available is None:
            raise ValueError("host memory headroom is unreadable")
        roof = min(cap.mem_gb,
                   credit + max(0, (maximum - used) // GIB - CAP_OVERHEAD_GB),
                   credit + max(0, available - box_capacity.MEMORY_MARGIN_GB))
        result["cpu"] = min(result.get("cpu", 0), len(affinity), len(worker_cpus))
        result["mem_gb"] = min(result.get("mem_gb", 0), roof)
        evidence["host_cap"] = {"slice": cap.slice, "memory_roof_gib": roof}
    except (OSError, ValueError, TypeError) as exc:
        evidence["host_cap_refusal"] = str(exc)
        return {kind: 0 for kind in result}
    return result
