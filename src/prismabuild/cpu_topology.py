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


def _online(root: Path):
    cpus = []
    for entry in root.glob("cpu[0-9]*"):
        name = entry.name[3:]
        if name.isdigit():
            cpus.append(int(name))
    return sorted(cpus)


def _is_first_sibling(root: Path, cpu: int) -> bool:
    """True when this CPU leads its SMT group (or has no siblings)."""
    text = ""
    try:
        text = (root / f"cpu{cpu}" / "topology" / "thread_siblings_list").read_text()
    except OSError:
        return True  # no SMT information: treat every CPU as its own core
    members = []
    for part in text.strip().split(","):
        lo = part.split("-")[0]
        if lo.isdigit():
            members.append(int(lo))
        if "-" in part:
            hi = part.split("-")[1]
            if lo.isdigit() and hi.isdigit():
                members.extend(range(int(lo), int(hi) + 1))
    return not members or cpu == min(members)


def classify(root: Path = SYSFS_CPU):
    """Split the online CPUs into ``(preferred, fallback)``, best first.

    ``preferred`` is the high-capacity CPUs that lead their SMT group.  The
    capacity threshold is the midpoint of the observed range, not equality with
    the maximum: GB10's fast cores do not all report the same number (997,
    1017 and 1024 all appear), so an equality test would return a single core.
    A machine with one capacity value has every CPU above its own midpoint,
    which is the correct answer for a uniform box.
    """
    cpus = _online(root)
    if not cpus:
        return [], []
    caps = {c: (_read_int(root / f"cpu{c}" / "cpu_capacity") or 1024) for c in cpus}
    lo, hi = min(caps.values()), max(caps.values())
    threshold = (lo + hi) / 2.0
    preferred, fallback = [], []
    for cpu in cpus:
        fast = caps[cpu] >= threshold
        lead = _is_first_sibling(root, cpu)
        (preferred if (fast and lead) else fallback).append(cpu)
    fallback.sort(key=lambda c: (-caps[c], not _is_first_sibling(root, c), c))
    return preferred, fallback


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
    want = set(preferred_cpus(root))
    try:
        allowed = os.sched_getaffinity(0)
    except (AttributeError, OSError):
        return None
    want &= allowed
    if not want or want == allowed:
        return sorted(allowed)
    try:
        os.sched_setaffinity(0, want)
    except OSError:
        return sorted(allowed)
    return sorted(want)
