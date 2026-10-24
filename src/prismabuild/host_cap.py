"""Validate a declared partial-host envelope at each capacity boundary."""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import os
from pathlib import Path
import re
import socket
from types import MappingProxyType
import weakref

from . import box_capacity, fleet_roster

CGROUP_ROOT = Path("/sys/fs/cgroup")
MEMBERSHIP = Path("/proc/self/cgroup")
CAP_OVERHEAD_GB = 1
GIB = 1024**3

class _AdmissionRegistry:
    """Only this module owns registrations; instances contain no mint key."""
    def __init__(self):
        self.entries = {}

    def mint(self, kind, payload):
        value = object.__new__(kind)
        identity = id(value)
        reference = weakref.ref(value, lambda _: self.entries.pop(identity, None))
        self.entries[identity] = (reference, payload)
        return value

    def payload(self, value, kind):
        registered = self.entries.get(id(value))
        if type(value) is not kind or registered is None or registered[0]() is not value:
            raise TypeError("admission requires capacity from enforce_host_cap or a registered claim fence proof")
        return registered[1]

    def consume(self, value):
        del self.entries[id(value)]


_ADMISSION_REGISTRY = _AdmissionRegistry()


class _SealedAdmission:
    __slots__ = ("__weakref__",)

    def __new__(cls, *args, **kwargs):
        raise TypeError("admission capabilities require enforce_host_cap or the claim fence check")

    def __copy__(self):
        raise TypeError("admission capabilities cannot be copied")

    def __deepcopy__(self, memo):
        raise TypeError("admission capabilities cannot be copied")

    def __reduce_ex__(self, protocol):
        raise TypeError("admission capabilities cannot be pickled")

    def __init_subclass__(cls, **kwargs):
        if cls.__module__ != __name__:
            raise TypeError("admission capabilities cannot be subclassed")


class CappedCapacity(_SealedAdmission, Mapping[str, int]):
    """Immutable capacity and its original box/held observation, registry-owned."""
    __slots__ = ()

    def __getitem__(self, key):
        return _ADMISSION_REGISTRY.payload(self, CappedCapacity)[0][key]

    def __iter__(self):
        return iter(_ADMISSION_REGISTRY.payload(self, CappedCapacity)[0])

    def __len__(self):
        return len(_ADMISSION_REGISTRY.payload(self, CappedCapacity)[0])


class ClaimFence(_SealedAdmission):
    """A queue/worker binding, requiring a fresh check for each final proof."""
    __slots__ = ()

    def __call__(self):
        state = _ADMISSION_REGISTRY.payload(self, ClaimFence)
        require_claim_fence(self, state[1], state[0])
        return _claim_fence_open(state[1])


class ClaimFenceProof(_SealedAdmission):
    """One successful final check for one queue, worker, host, pass and key."""
    __slots__ = ()


def require_capped_capacity(value: object) -> CappedCapacity:
    _ADMISSION_REGISTRY.payload(value, CappedCapacity)
    return value


def prepare_claim_fence(queue, capacity: CappedCapacity) -> ClaimFence:
    """Bind an entrypoint's observation to its actual queue and worker."""
    require_capped_capacity(capacity)
    return _ADMISSION_REGISTRY.mint(
        ClaimFence, (queue, capacity, socket.gethostname(), os.getpid()))


def require_claim_fence(value, capacity, queue) -> ClaimFence:
    require_capped_capacity(capacity)
    bound_queue, bound_capacity, host, pid = _ADMISSION_REGISTRY.payload(value, ClaimFence)
    if (bound_queue is not queue or bound_capacity is not capacity
            or host != socket.gethostname() or pid != os.getpid()):
        raise TypeError("claim fence belongs to another queue, capacity or worker")
    return value


def recheck_host_cap(capacity: CappedCapacity, *, detail=None) -> CappedCapacity:
    """Re-read the box and held source captured by the original clamp."""
    _values, box, held = _ADMISSION_REGISTRY.payload(capacity, CappedCapacity)
    return enforce_host_cap(capacity, box, held, detail=detail)


def _claim_fence_open(capacity):
    from . import role_session
    if role_session.read_maintenance_gate() is not None:
        return False
    detail = {}
    fresh = recheck_host_cap(capacity, detail=detail)
    require_capped_capacity(fresh)
    return "host_cap_refusal" not in detail and fresh == capacity


def check_claim_fence(fence: ClaimFence, capacity: CappedCapacity, queue, scan, key) -> ClaimFenceProof | None:
    """The sole proof mint, called under the candidate's transition lock."""
    require_claim_fence(fence, capacity, queue)
    if scan.host != socket.gethostname() or not _claim_fence_open(capacity):
        return None
    return _ADMISSION_REGISTRY.mint(
        ClaimFenceProof, (queue, capacity, socket.gethostname(), os.getpid(), scan, scan.owner, key))


def consume_claim_fence(proof: ClaimFenceProof, capacity: CappedCapacity, queue, scan, key) -> None:
    """Refuse missing, forged, replayed or cross-pass proof before the rename."""
    require_capped_capacity(capacity)
    bound_queue, bound_capacity, host, pid, bound_scan, bound_owner, bound_key = _ADMISSION_REGISTRY.payload(proof, ClaimFenceProof)
    _ADMISSION_REGISTRY.consume(proof)
    if (bound_queue is not queue or bound_capacity is not capacity or bound_scan is not scan
            or bound_key != key or bound_owner != scan.owner or host != socket.gethostname() or pid != os.getpid()
            or scan.host != host):
        raise TypeError("claim fence proof belongs to another queue, worker, host, pass or key")


def _installed_cap_present():
    try:
        Path("/etc/prismabuild/host-cap.conf").stat()
    except FileNotFoundError:
        return False
    except OSError:
        return True
    return True


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
                     held, *, detail: dict | None = None) -> CappedCapacity:
    """One live clamp for offers and admission, after any observer smoothing.

    Capacity is a total including existing reservations. Bracket slice usage
    with held reads, taking the lower credit if a sibling releases mid-read.
    Missing or mismatched containment closes every resource, including CPU
    lending. Uncapped hosts do not read cgroups and retain their old behavior.
    """
    result = dict(capacity)
    evidence = {} if detail is None else detail
    try:
        if box is None or "cap" not in box.entry:
            if _installed_cap_present():
                raise ValueError("installed host cap has no readable roster cap")
            return _ADMISSION_REGISTRY.mint(CappedCapacity, (MappingProxyType(result), box, held))
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
        return _ADMISSION_REGISTRY.mint(CappedCapacity, (MappingProxyType({kind: 0 for kind in result}), box, held))
    return _ADMISSION_REGISTRY.mint(CappedCapacity, (MappingProxyType(result), box, held))
