"""Which logical CPUs are worth running compute on, derived rather than rostered.

Both boxes the fleet runs on are heterogeneous, in different ways, and both
punish the obvious pin:

* **GB10** interleaves two core classes in blocks of five -- Cortex-X925 at
  3.9 GHz on ``5-9,15-19`` and Cortex-A725 at 2.8 GHz on ``0-4,10-14``.  So
  ``taskset -c 0-9`` is *five of each*, not a half, and a job "pinned to half
  the box" runs a third of its threads 39% slower for nothing.
* **dl380g10** is uniform in clock but 2-way SMT: ``0-39`` are physical cores
  and ``40-79`` are their siblings.  A second thread on a busy core is worth
  well under a second core, so those are the last resort.

One rule covers both, because the kernel already publishes the answer to each.
``cpu_capacity`` is its normalised throughput estimate per CPU (present on
heterogeneous ARM, and a flat 1024 on the Xeon), and ``thread_siblings_list``
names the SMT group.  A CPU is **preferred** when it is fast *and* it is the
first thread of its sibling group; everything else is fallback.  Nothing here
hardcodes a core number, so a third box shape needs no edit -- which is the
point, since the ranges above were discovered by accident after a harness had
been running on the wrong half for hours.

Deliberately *not* here: any notion of how many cores a job should get.  That
is admission, and admission is the ledger's.
"""
from __future__ import annotations

import os
from pathlib import Path

SYSFS_CPU = Path("/sys/devices/system/cpu")


def _read_int(path: Path):
    try:
        return int(path.read_text().strip())
    except (OSError, ValueError):
        return None


def parse_cpus(text: str) -> list[int]:
    """Parse a kernel CPU list, refusing malformed or descending ranges."""
    cpus = set()
    for part in text.strip().split(","):
        if not part:
            continue
        bounds = part.split("-")
        if len(bounds) > 2 or any(not x.isdigit() for x in bounds):
            raise ValueError(f"invalid CPU list: {text!r}")
        lo, hi = int(bounds[0]), int(bounds[-1])
        if hi < lo:
            raise ValueError(f"invalid CPU range: {part!r}")
        cpus.update(range(lo, hi + 1))
    return sorted(cpus)


def _read_cpus(path: Path):
    try:
        return parse_cpus(path.read_text())
    except (OSError, ValueError):
        return None


def _online(root: Path):
    online = _read_cpus(root / "online")
    if online is not None:
        return online
    return sorted(int(entry.name[3:]) for entry in root.glob("cpu[0-9]*")
                  if entry.name[3:].isdigit()
                  and _read_int(entry / "online") != 0)


def classify(root: Path = SYSFS_CPU, *, allowed=None, pmu_root=None):
    """Return usable CPUs as (preferred physical fast cores, fallback).

    Class capacity is assessed before the outer affinity is applied, so an
    E-core-only cpuset does not turn its CPUs into fast cores. SMT leadership
    is chosen inside the usable set: an offline or excluded primary thread
    does not demote the only usable thread of a fast physical core. Intel's
    hybrid PMU cpu_atom list identifies E-cores where cpu_capacity is absent.
    """
    online = _online(root)
    if not online:
        return [], []
    caps = {c: (_read_int(root / f"cpu{c}" / "cpu_capacity") or 1024)
            for c in online}
    threshold = (min(caps.values()) + max(caps.values())) / 2.0
    pmu = Path(pmu_root) if pmu_root is not None else root.parent.parent
    efficient = set(_read_cpus(pmu / "cpu_atom" / "cpus") or [])
    cpus = set(online) if allowed is None else set(online) & set(allowed)
    preferred, fallback = [], []
    lead, fast = {}, {}
    for cpu in sorted(cpus):
        siblings = set(_read_cpus(root / f"cpu{cpu}" / "topology" /
                                  "thread_siblings_list") or [cpu]) & cpus
        lead[cpu] = cpu == min(siblings | {cpu})
        fast[cpu] = caps[cpu] >= threshold and cpu not in efficient
        (preferred if fast[cpu] and lead[cpu] else fallback).append(cpu)
    # Stable CPU IDs within each class: small capacity fluctuations must not
    # reinterpret existing ledger token ordinals.
    fallback.sort(key=lambda c: (not fast[c], not lead[c], c))
    return preferred, fallback


def inherited_tiers(root: Path = SYSFS_CPU):
    """Topology within the worker affinity, or None without affinity support."""
    try:
        allowed = os.sched_getaffinity(0)
    except (AttributeError, OSError):
        return None
    preferred, fallback = classify(root, allowed=allowed)
    # Missing sysfs must not silently remove usable capacity.
    known = {int(entry.name[3:]) for entry in root.glob("cpu[0-9]*")
             if entry.name[3:].isdigit()}
    unknown = set(allowed) - known - set(preferred) - set(fallback)
    online = _read_cpus(root / "online")
    if online is not None:
        unknown &= set(online)
    return {"preferred": preferred + sorted(unknown), "fallback": fallback}


def preferred_cpus(root: Path = SYSFS_CPU):
    """The set to pin compute to.  Never empty: falls back to every CPU."""
    preferred, fallback = classify(root)
    return preferred or fallback or sorted(_online(root))


def as_range(cpus) -> str:
    """Render a CPU set the way ``taskset -c`` prints it: ``5-9,15-19``."""
    out, cpus = [], sorted(cpus)
    start = prev = None
    for cpu in cpus:
        if start is None:
            start = prev = cpu
        elif cpu == prev + 1:
            prev = cpu
        else:
            out.append(f"{start}-{prev}" if prev > start else f"{start}")
            start = prev = cpu
    if start is not None:
        out.append(f"{start}-{prev}" if prev > start else f"{start}")
    return ",".join(out)


def pin_to_preferred(root: Path = SYSFS_CPU):
    """Pin this process -- and so every child it forks -- to the fast cores.

    Returns the set applied, or ``None`` where the platform has no affinity
    call.  Intersected with the *current* mask so an outer pin (a taskset, a
    cgroup, a container's cpuset) still wins: widening someone else's
    restriction would be this module overruling an explicit decision.
    """
    try:
        allowed = os.sched_getaffinity(0)
    except (AttributeError, OSError):
        return None
    want = set(classify(root, allowed=allowed)[0])
    if not want or want == allowed:
        return sorted(allowed)
    try:
        os.sched_setaffinity(0, want)
    except OSError:
        return sorted(allowed)
    return sorted(want)
