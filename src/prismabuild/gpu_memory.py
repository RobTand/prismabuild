"""Conservative GPU/UMA accounting and selective owned-scope stop suggestions.

This module reads telemetry; it never sends a signal or changes a cgroup. The
privileged broker supplies all its active scopes and must revalidate both its
attempt authority and the cgroup inode before acting on a Decision. Install a
root-owned copy beside that broker, never import writable action/runtime code.

On physical shared-system-memory devices, GPU bytes can overlap memory.current;
on discrete devices, VRAM is a separate budget and never added to system RAM.
Only broker-supplied hardware domains identify shared physical memory, never
CUDA unified virtual addressing. GPU IPC can overlap between processes, so
reported sums are diagnostic and the largest individual report is a proven
lower bound. Unknown hardware disables shared-memory inference. Host-pressure
decisions use only system-memory charges, not discrete VRAM growth.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import math
from pathlib import Path
import re
import subprocess
import time
from typing import Mapping, Sequence

MIB = 1024**2
GIB = 1024**3
SCOPE_RE = re.compile(r"prismabuild-job[0-9a-f]{32}\.slice\Z")


@dataclass(frozen=True)
class Scope:
    scope_id: str
    cgroup_path: str | Path
    budget_bytes: int
    gpu_budget_bytes: int | None = None

    def __post_init__(self):
        if not SCOPE_RE.fullmatch(self.scope_id):
            raise ValueError("invalid broker scope identity")
        if type(self.budget_bytes) is not int or self.budget_bytes <= 0:
            raise ValueError("scope budget must be positive bytes")
        if self.gpu_budget_bytes is not None and (type(self.gpu_budget_bytes) is not int
                or self.gpu_budget_bytes <= 0):
            raise ValueError("GPU scope budget must be positive bytes")


@dataclass(frozen=True)
class JobSample:
    scope_id: str
    cgroup_identity: tuple[int, int] | None
    budget_bytes: int
    host_bytes: int | None
    gpu_reported_bytes: int | None
    gpu_lower_bound_bytes: int | None
    lower_bound_bytes: int | None
    upper_bound_bytes: int | None
    complete: bool
    processes: tuple[dict, ...] = ()
    errors: tuple[str, ...] = ()
    gpu_budget_bytes: int | None = None
    memory_domain: str = "unknown"
    shared_gpu_lower_bound_bytes: int | None = None
    system_lower_bound_bytes: int | None = None


@dataclass(frozen=True)
class Snapshot:
    sampled_monotonic: float
    sampled_unix: float
    duration_s: float
    host_total_bytes: int | None
    host_available_bytes: int | None
    psi_some_avg10: float | None
    psi_full_avg10: float | None
    jobs: tuple[JobSample, ...]
    gpu_query_complete: bool
    foreign_gpu_reported_bytes: int | None = None
    errors: tuple[str, ...] = ()
    foreign_processes: tuple[dict, ...] = ()

    def as_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class Decision:
    scope_id: str
    cgroup_identity: tuple[int, int]
    reason: str
    evidence: dict

    def as_dict(self) -> dict:
        return asdict(self)


def _process_identity(proc: Path, pid: int):
    try:
        text = (proc / str(pid) / "stat").read_text()
        tail = text[text.rindex(")") + 2:].split()
        start = int(tail[19])  # proc stat field 22, after comm (which can contain spaces)
        groups = [line[3:] for line in (proc / str(pid) / "cgroup").read_text().splitlines()
                  if line.startswith("0::")]
        if len(groups) != 1 or not groups[0].startswith("/") or ".." in Path(groups[0]).parts:
            return None
        return start, groups[0]
    except (OSError, ValueError, IndexError):
        return None


def _gpu_processes(timeout_s: float):
    try:
        result = subprocess.run(
            ["/usr/bin/nvidia-smi", "--query-compute-apps=pid,gpu_uuid,used_gpu_memory",
             "--format=csv,noheader,nounits"], capture_output=True, text=True,
            timeout=timeout_s, check=False,
        )
        if result.returncode:
            return None, f"GPU query exited {result.returncode}"
    except (OSError, subprocess.SubprocessError) as exc:
        return None, f"GPU query unavailable: {type(exc).__name__}"
    rows = {}
    for line in result.stdout.splitlines():
        try:
            pid, device, memory = [x.strip() for x in line.split(",")]
            pid = int(pid)
            if pid <= 0 or not device.startswith("GPU-"):
                raise ValueError("invalid GPU process identity")
            used = int(memory) * MIB if memory.isdigit() else None
            identity = pid, device
            # Duplicate rows cannot mint memory. An unavailable duplicate
            # keeps the observation unavailable rather than inventing zero.
            if identity in rows:
                old = rows[identity]
                used = None if old is None or used is None else max(old, used)
            rows[identity] = used
        except (ValueError, TypeError):
            return None, "malformed GPU process query"
    return rows, None


def _host(proc: Path):
    total = available = some = full = None
    errors = []
    try:
        values = {}
        for line in (proc / "meminfo").read_text().splitlines():
            parts = line.split()
            if parts and parts[0] in {"MemTotal:", "MemAvailable:"}:
                if len(parts) != 3 or parts[2] != "kB":
                    raise ValueError("invalid memory unit")
                values[parts[0]] = int(parts[1]) * 1024
        total, available = values["MemTotal:"], values["MemAvailable:"]
        if total <= 0 or not 0 <= available <= total:
            raise ValueError("invalid host memory counters")
    except (OSError, ValueError, KeyError):
        total = available = None
        errors.append("host memory unavailable")
    try:
        values = {}
        for line in (proc / "pressure/memory").read_text().splitlines():
            parts = line.split()
            if parts and parts[0] in {"some", "full"}:
                fields = dict(part.split("=", 1) for part in parts[1:])
                value = float(fields["avg10"])
                if not math.isfinite(value) or not 0 <= value <= 100:
                    raise ValueError("invalid PSI")
                values[parts[0]] = value
        some, full = values["some"], values["full"]
    except (OSError, ValueError, KeyError):
        some = full = None
        errors.append("memory PSI unavailable")
    return total, available, some, full, errors


def collect(scopes: Sequence[Scope], *, proc_root: Path = Path("/proc"),
            cgroup_root: Path = Path("/sys/fs/cgroup"), timeout_s: float = 1.0,
            max_collect_s: float = 3.0,
            gpu_memory_domains: Mapping[str, str] | None = None) -> Snapshot:
    """Sample all active broker scopes without holding its authority lock.

    Caller must run in the host PID/cgroup namespaces. GPU rows are attributed
    only if the PID's start time and kernel cgroup agree before and after the
    bounded query. New/reused/unreadable PIDs remain unknown for this sample.
    Cgroup inode replacement or a missing memory counter also invalidates it.
    Census and attribution honor a cooperative deadline; kernel pseudo-file
    reads cannot be interrupted here, so the broker runs this off its RPC path.
    """
    if not math.isfinite(timeout_s) or not 0 < timeout_s <= 5:
        raise ValueError("GPU query timeout must be in (0, 5] seconds")
    if not math.isfinite(max_collect_s) or not timeout_s <= max_collect_s <= 10:
        raise ValueError("collection deadline must cover query and be <=10 seconds")
    proc_root, cgroup_root = Path(proc_root), Path(cgroup_root)
    domains = {uuid: domain if domain in {"shared_system", "discrete"} else "unknown"
               for uuid, domain in (gpu_memory_domains or {}).items()}
    started = time.monotonic()
    deadline = started + max_collect_s
    timed_out = False
    expected = {}
    initial = {}
    for scope in scopes:
        path = Path(scope.cgroup_path)
        if scope.scope_id in expected or path != cgroup_root / "prismabuild.slice" / scope.scope_id:
            raise ValueError("scope must be an exact unique child of prismabuild.slice")
        expected[scope.scope_id] = scope
        try:
            stat = path.stat()
            initial[scope.scope_id] = stat.st_dev, stat.st_ino
        except OSError:
            initial[scope.scope_id] = None
    before = {}
    try:
        for entry in proc_root.iterdir():
            if time.monotonic() >= deadline - timeout_s:
                timed_out = True
                break
            if entry.name.isdigit():
                pid = int(entry.name)
                identity = _process_identity(proc_root, pid)
                if identity is not None:
                    before[pid] = identity
    except OSError:
        pass
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        rows, error = None, "collection deadline elapsed before GPU query"
    else:
        rows, error = _gpu_processes(min(timeout_s, remaining))
    errors = [error] if error else []
    complete = rows is not None and not timed_out
    if timed_out:
        errors.append("process census exceeded collection deadline")
    attributed = {scope.scope_id: [] for scope in scopes}
    foreign = 0
    foreign_processes = []
    foreign_unknown = False
    for (pid, device), used in (rows or {}).items():
        if time.monotonic() >= deadline:
            complete = False
            errors.append("GPU attribution exceeded collection deadline")
            break
        identity = _process_identity(proc_root, pid)
        if identity is None or identity != before.get(pid):
            complete = False
            errors.append(f"GPU process {pid} identity changed or unavailable")
            continue
        group = Path(identity[1])
        owner = None
        for scope in scopes:
            relative = Path("/") / Path(scope.cgroup_path).relative_to(cgroup_root)
            if group == relative or relative in group.parents:
                owner = scope.scope_id
                break
        if owner is None:
            foreign_processes.append({"pid": pid, "start_ticks": identity[0],
                                      "cgroup": identity[1], "gpu_uuid": device,
                                      "used_bytes": used})
            if used is None:
                foreign_unknown = True
            else:
                foreign += used
        else:
            attributed[owner].append({"pid": pid, "start_ticks": identity[0],
                                      "cgroup": identity[1], "gpu_uuid": device,
                                      "used_bytes": used,
                                      "memory_domain": domains.get(device, "unknown")})
    jobs = []
    for scope in scopes:
        failures = []
        host = None
        path = Path(scope.cgroup_path)
        identity = initial[scope.scope_id]
        try:
            host = int((path / "memory.current").read_text())
            stat = path.stat()
            if host < 0 or identity != (stat.st_dev, stat.st_ino):
                raise ValueError("scope changed during sample")
        except (OSError, ValueError):
            host = None
            failures.append("scope identity or host charge unavailable")
        processes = tuple(attributed[scope.scope_id])
        observed_domains = {row['memory_domain'] for row in processes} or set(domains.values())
        domain = next(iter(observed_domains)) if len(observed_domains) == 1 else "unknown"
        known = complete and all(row["used_bytes"] is not None for row in processes)
        gpu_sum = sum(row["used_bytes"] for row in processes) if known else None
        gpu_min = max((row["used_bytes"] for row in processes), default=0) if known else None
        shared = [row['used_bytes'] for row in processes if row['memory_domain'] == 'shared_system']
        shared_min = max(shared, default=0) if known else None
        shared_sum = sum(shared) if known else None
        lower = max(host, shared_min) if host is not None and shared_min is not None else None
        upper = host + shared_sum if host is not None and shared_sum is not None else None
        if not known:
            failures.append("GPU ownership or memory incomplete")
        jobs.append(JobSample(scope.scope_id, identity, scope.budget_bytes, host, gpu_sum,
                              gpu_min, lower, upper, known and host is not None,
                              processes, tuple(failures), scope.gpu_budget_bytes or scope.budget_bytes,
                              domain, shared_min, lower))
    total, available, some, full, host_errors = _host(proc_root)
    return Snapshot(time.monotonic(), time.time(), time.monotonic() - started,
                    total, available, some, full, tuple(jobs), complete,
                    None if foreign_unknown or not complete else foreign,
                    tuple(errors + host_errors), tuple(foreign_processes))


@dataclass(frozen=True)
class Policy:
    budget_samples: int = 2
    pressure_samples: int = 3
    max_sample_gap_s: float = 5.0
    projection_s: float = 5.0
    reserve_bytes: int = 2 * GIB
    reserve_fraction: float = 0.02
    min_growth_bytes_s: float = 128 * MIB
    dominant_fraction: float = 0.80
    psi_some_avg10: float = 1.0
    psi_full_avg10: float = 0.1

    def __post_init__(self):
        if (type(self.budget_samples) is not int or self.budget_samples < 2
                or type(self.pressure_samples) is not int or self.pressure_samples < 3):
            raise ValueError("guard requires repeated observations")
        for name in ("max_sample_gap_s", "projection_s", "reserve_bytes",
                     "reserve_fraction", "min_growth_bytes_s", "dominant_fraction",
                     "psi_some_avg10", "psi_full_avg10"):
            value = getattr(self, name)
            if isinstance(value, bool) or not math.isfinite(value) or value <= 0:
                raise ValueError(f"invalid policy {name}")
        if self.reserve_fraction >= 1 or not 0.5 < self.dominant_fraction <= 1:
            raise ValueError("invalid guard fractions")


def _system_charge(job: JobSample) -> int | None:
    if job.host_bytes is None:
        return None
    shared = job.shared_gpu_lower_bound_bytes
    if shared is None:
        # Compatibility for an explicitly classified shared-system sample.
        # An old max(host, GPU) lower bound is never trusted on unknown or
        # discrete hardware.
        shared = job.gpu_lower_bound_bytes if job.memory_domain == "shared_system" else 0
    return max(job.host_bytes, shared) if shared is not None else None


def _budget_identity(job: JobSample) -> tuple:
    domains = tuple(sorted({(row.get("gpu_uuid", ""), row.get("memory_domain", "unknown"))
                            for row in job.processes}))
    return (job.scope_id, job.cgroup_identity, job.budget_bytes,
            job.gpu_budget_bytes or job.budget_bytes, job.memory_domain, domains)


class Guard:
    """Stateful conservative decision maker; broker serializes observe calls."""
    def __init__(self, policy: Policy | None = None):
        self.policy = policy or Policy()
        self.previous: Snapshot | None = None
        self.over_budget: dict[tuple, int] = {}
        self.pressure: dict[tuple, int] = {}

    def observe(self, snapshot: Snapshot) -> list[Decision]:
        policy = self.policy
        previous = self.previous
        self.previous = snapshot
        dt = snapshot.sampled_monotonic - previous.sampled_monotonic if previous else 0
        continuous = previous is not None and 0 < dt <= policy.max_sample_gap_s
        if not continuous:
            self.over_budget.clear()
            self.pressure.clear()
        prior = {j.scope_id: j for j in previous.jobs} if continuous else {}
        active = {_budget_identity(j) for j in snapshot.jobs}
        self.over_budget = {k: v for k, v in self.over_budget.items() if k[:-1] in active}
        self.pressure = {k: v for k, v in self.pressure.items() if k in active}
        decisions = []
        growth = {}
        for job in snapshot.jobs:
            key = _budget_identity(job)
            charge = _system_charge(job)
            valid = job.complete and charge is not None and job.cgroup_identity is not None
            gpu_budget = job.gpu_budget_bytes or job.budget_bytes
            metrics = (("system", charge, job.budget_bytes, "memory_budget_exceeded"),
                       ("gpu", job.gpu_lower_bound_bytes, gpu_budget, "gpu_memory_budget_exceeded"))
            for domain, used, budget, reason in metrics:
                counter = (*key, domain)
                excess = valid and used is not None and used > budget
                self.over_budget[counter] = self.over_budget.get(counter, 0) + 1 if excess else 0
                if (self.over_budget[counter] >= policy.budget_samples
                        and not any(d.scope_id == job.scope_id for d in decisions)):
                    decisions.append(Decision(job.scope_id, job.cgroup_identity, reason,
                        {"job": asdict(job), "budget_domain": domain,
                         "used_lower_bound_bytes": used, "budget_bytes": budget,
                         "consecutive_samples": self.over_budget[counter]}))
            old = prior.get(job.scope_id)
            old_charge = _system_charge(old) if old is not None else None
            if (valid and old and old.complete and _budget_identity(old) == key
                    and old_charge is not None):
                growth[job.scope_id] = max(0., (charge - old_charge) / dt)
        host_valid = (continuous and snapshot.host_available_bytes is not None
                      and previous.host_available_bytes is not None
                      and snapshot.host_total_bytes is not None
                      and snapshot.psi_some_avg10 is not None and snapshot.psi_full_avg10 is not None
                      and all(j.complete for j in snapshot.jobs)
                      and len(growth) == len(snapshot.jobs))
        candidate = None
        if host_valid:
            reserve = max(policy.reserve_bytes, int(snapshot.host_total_bytes * policy.reserve_fraction))
            loss = (previous.host_available_bytes - snapshot.host_available_bytes) / dt
            largest = max(growth, key=growth.get) if growth else None
            rate = growth.get(largest, 0.)
            if (largest and rate >= policy.min_growth_bytes_s and loss > 0
                    and rate >= policy.dominant_fraction * sum(growth.values())
                    and rate >= policy.dominant_fraction * loss
                    and snapshot.host_available_bytes <= 2 * reserve
                    and snapshot.host_available_bytes - loss * policy.projection_s <= reserve
                    and (snapshot.psi_some_avg10 >= policy.psi_some_avg10
                         or snapshot.psi_full_avg10 >= policy.psi_full_avg10)):
                job = next(j for j in snapshot.jobs if j.scope_id == largest)
                if _system_charge(job) + rate * policy.projection_s > job.budget_bytes:
                    candidate = largest
        for job in snapshot.jobs:
            key = _budget_identity(job)
            self.pressure[key] = self.pressure.get(key, 0) + 1 if job.scope_id == candidate else 0
            if (self.pressure[key] >= policy.pressure_samples - 1
                    and not any(d.scope_id == job.scope_id for d in decisions)):
                decisions.append(Decision(job.scope_id, job.cgroup_identity, "projected_host_oom",
                    {"job": asdict(job), "host_available_bytes": snapshot.host_available_bytes,
                     "growth_bytes_s": growth[job.scope_id], "projection_s": policy.projection_s,
                     "psi_some_avg10": snapshot.psi_some_avg10,
                     "psi_full_avg10": snapshot.psi_full_avg10,
                     "consecutive_growth_intervals": self.pressure[key]}))
        return decisions
