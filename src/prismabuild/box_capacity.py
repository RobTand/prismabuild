"""What this box can honestly offer the pool *now*, not what it offers at rest.

The ledger is exact about the work the pool scheduled and blind to everything
else.  Measured on sparklina (``gx10-6b77``) 2026-09-04 01:06, six hours into
an out-of-pool encode campaign:

    gx10-6b77 offer {'cpu': 10, 'gpu': 1, 'mem_gb': 40}  ledger free 51  held 0

    $ nvidia-smi --query-compute-apps=pid,used_memory --format=csv
    794915, 1145 MiB
    805673, 1543 MiB
    831002, 1477 MiB
    1242245, 1321 MiB

Four GPU processes, every token free, nothing held.  One GPU action placed
there would have stacked on top of them -- the shape of the 2026-09-03
sparklina OOM, reached without anyone doing anything wrong: those processes
were started by ``ssh box 'setsid nohup ...'``, and the ``require_pool``
PreToolUse hook can only make ``pbrun`` the sole GPU path for the Bash calls of
the session it is installed in.  It cannot cover a subagent's ssh, a cron, or a
person at a prompt.

So the box observes itself, and what it offers is the remainder.

**Attribution is by ledger, not by process tree.**  The obvious implementation
-- walk the GPU compute apps and forgive the ones descended from a pool worker
-- does not work on this fleet and cannot be made to.  Measured on sparky
2026-09-04 01:33, while the pool was running a GPU action of its own:

    claimed 6b4a32b1cb9c   resources {'cpu': 1, 'gpu': 1, 'mem_gb': 16}
    lease   pid 156863     (the worker loop that claimed it)
    nvidia-smi app 261917  ppid 260764 -> ppid 260728
                           containerd-shim-runc-v2, a different session

The action's GPU process is a docker container's child, reparented away from
the worker that started it; a ``setsid`` action would be equally invisible.
Ancestry would therefore call the pool's *own* scheduled work foreign and
retire capacity against it, for as long as that action ran -- the failure this
module exists to prevent, inverted, and self-reinforcing.

What *is* exactly knowable is what the pool has claimed on this host: its held
tokens.  So foreign work is what the box is doing minus what the pool has
reserved, counted in the ledger's own units, and containers, ``setsid`` and
reparenting are all irrelevant to it.

**Falling is slow, recovering is fast.**  A retire deletes free tokens, and the
loop that retired them re-mints them at its next poll -- but a worker loop
spends the whole of an action *inside* ``serve_once``, up to two hours, and
does not poll while it is there.  A single unlucky reading taken just before a
long action therefore retires capacity for the length of that action, which is
the "momentary spike permanently starves the box" failure.  So the offer is the
elementwise **maximum** over the last few observations: it falls only when
every one of them agrees, and it recovers on the first reading that says the
foreign work is gone.  The window is primed with the declared capacity, so a
worker's first polls can still correct ledger drift (that retire is bounded by
the declaration) without a first-reading spike being decisive.

Several loops share one box and one ledger, and ``ensure_capacity`` is
increase-only, so the box's effective offer is the most optimistic *live* loop's:
a loop that has just started re-mints for the two polls its window is primed
for, and a loop inside a long action re-mints nothing at all.  They are reading
the same box, so they agree within a window, and every disagreement in between
is on the recovering side.  That is the same convergence the drift retire
already relies on.

Nothing here reads a GPU's memory as a *pool* of its own.  On GB10 the GPU and
the host share one physical memory, so a compute app's resident bytes are
already inside ``MemAvailable``; counting them twice would clamp a box for work
it had already subtracted.  The per-app figure is still read and recorded, as
the evidence for why an offer fell.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Mapping
from dataclasses import dataclass, field
import os
from pathlib import Path
import subprocess

#: Memory the box keeps for itself rather than offering the queue its last
#: byte.  Carried over from the ``--honest-memory`` flag this replaces.
MEMORY_MARGIN_GB = 8

#: How many consecutive observations must agree before the offer falls.  The
#: only property required of it is "more than one", so that no single reading
#: can retire capacity a loop may then sit on for the length of an action; the
#: value is a policy knob, and at the fleet's 10-20 s polls three of them is
#: 20-60 s of sustained foreign work.
DEFAULT_SAMPLES = 3

NVIDIA_SMI = Path("/usr/bin/nvidia-smi")
MEMINFO = Path("/proc/meminfo")

#: Sentinel for "read this from the box".  ``None`` is a real answer -- it means
#: the reading was attempted and failed -- so it cannot double as "unset".
_READ: object = object()


def gpu_compute_apps(*, timeout_s: float = 5.0) -> list[tuple[int, int]] | None:
    """``(pid, MiB)`` per live GPU compute app; ``None`` if unreadable.

    Unreadable is not evidence of foreign work, so the caller treats ``None``
    as "no observation" and leaves the declaration alone.  A wedged GPU whose
    ``nvidia-smi`` hangs must not be able to stall a worker loop either, which
    is what the timeout is for.
    """

    if not NVIDIA_SMI.exists():
        return None
    try:
        done = subprocess.run(
            [str(NVIDIA_SMI), "--query-compute-apps=pid,used_gpu_memory",
             "--format=csv,noheader,nounits"],
            capture_output=True, text=True, timeout=timeout_s, check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if done.returncode != 0:
        return None
    apps: list[tuple[int, int]] = []
    for line in done.stdout.splitlines():
        parts = [part.strip() for part in line.split(",")]
        if len(parts) < 2:
            continue
        try:
            pid = int(parts[0])
        except ValueError:
            continue
        # A process can report ``[N/A]`` for its memory while still holding the
        # device.  Losing the pid over an unreadable byte count would be the
        # blindness this module exists to remove, so the pid wins.
        try:
            used = int(parts[1])
        except ValueError:
            used = 0
        apps.append((pid, used))
    return apps


def mem_available_gb() -> int | None:
    """``MemAvailable`` in whole GiB, or ``None`` if unreadable."""

    try:
        text = MEMINFO.read_text(encoding="utf-8")
    except OSError:
        return None
    for line in text.splitlines():
        if line.startswith("MemAvailable:"):
            try:
                return int(line.split()[1]) // (1024 * 1024)
            except (IndexError, ValueError):
                return None
    return None


def run_queue() -> float | None:
    """The one-minute load average, or ``None`` if unreadable.

    The least precise of the three readings and the only one available: it is
    an EWMA and it counts uninterruptible sleepers, so it lags a build that
    just started and lingers after one that just ended.  Both errors are in the
    direction of leaving the declaration alone or clamping a little late, and
    the sample window absorbs the rest.
    """

    try:
        return os.getloadavg()[0]
    except OSError:
        return None


@dataclass(frozen=True)
class Observation:
    """One reading of the box, and what it implies about the offer."""

    capacity: dict[str, int] = field(default_factory=dict)
    foreign: dict[str, int] = field(default_factory=dict)
    detail: dict[str, object] = field(default_factory=dict)


def observe(
    declared: Mapping[str, int],
    held: Mapping[str, int] | None = None,
    *,
    margin_gb: int = MEMORY_MARGIN_GB,
    gpu_apps: object = _READ,
    mem_gb: object = _READ,
    load1: object = _READ,
) -> Observation:
    """What the box can honestly offer, given what the pool already holds here.

    ``declared`` is what this worker was configured to offer; the result never
    exceeds it, because observing the box is how an offer *falls*, never how it
    grows past its configuration.  ``held`` is the pool's own consumption, read
    from the ledger -- see the module docstring for why that, and not a process
    tree, is the attribution.

    A kind nothing here knows how to read passes through untouched: an
    unobservable resource is not a busy one.
    """

    wanted = {str(kind): int(value) for kind, value in declared.items()}
    ours = {str(kind): int(value) for kind, value in (held or {}).items()}
    capacity = dict(wanted)
    foreign: dict[str, int] = {}
    detail: dict[str, object] = {}

    def clamp(kind: str, occupied: int) -> None:
        if occupied <= 0:
            return
        foreign[kind] = occupied
        capacity[kind] = max(0, wanted[kind] - occupied)

    if wanted.get("gpu", 0) > 0:
        if gpu_apps is _READ:
            gpu_apps = gpu_compute_apps()
        if gpu_apps is not None:
            apps = list(gpu_apps)                     # type: ignore[arg-type]
            detail["gpu_compute_apps"] = len(apps)
            detail["gpu_used_mib"] = sum(int(used) for _, used in apps)
            # One slot per process the pool did not claim.  The pool prices its
            # own GPU work at one token per action whatever it spawns, so an
            # action running several processes reads as foreign above the first
            # and the box under-offers itself for the length of that action.
            # That is the conservative direction, it recovers when the action
            # finishes, and it is the only mapping from a device to a slot
            # count that does not invent a constant.
            clamp("gpu", len(apps) - max(0, ours.get("gpu", 0)))

    if "mem_gb" in wanted:
        if mem_gb is _READ:
            mem_gb = mem_available_gb()
        if mem_gb is not None:
            available = int(mem_gb)                   # type: ignore[arg-type]
            detail["mem_available_gb"] = available
            # The honest total is what the pool already holds here plus what is
            # physically free, capped by the declaration.  Adding the held part
            # back is not generosity: an action's resident bytes are already
            # missing from ``MemAvailable`` *and* already reserved as tokens, so
            # a clamp that ignored them would charge the box twice for its own
            # work -- which is what ``--honest-memory`` did.
            honest = min(wanted["mem_gb"],
                         max(0, ours.get("mem_gb", 0)) + max(0, available - margin_gb))
            clamp("mem_gb", wanted["mem_gb"] - honest)

    if wanted.get("cpu", 0) > 0:
        if load1 is _READ:
            load1 = run_queue()
        if load1 is not None:
            load = float(load1)                       # type: ignore[arg-type]
            detail["load1"] = round(load, 2)
            # Floor, not round: the reading is noisy, and half a runnable task
            # is not evidence enough to take a core off the offer.
            clamp("cpu", int(load) - max(0, ours.get("cpu", 0)))

    return Observation(capacity=capacity, foreign=foreign, detail=detail)


class CapacityObserver:
    """The offer a worker publishes: observations, held for a few polls.

    ``offer`` returns the elementwise maximum over the last ``samples``
    readings, so the offer falls only when every one of them agrees and
    recovers on the first that disagrees.  See the module docstring for why the
    asymmetry is required rather than tidy: a retire outlives the loop that
    made it for as long as that loop is inside an action.
    """

    def __init__(
        self, *, samples: int = DEFAULT_SAMPLES, margin_gb: int = MEMORY_MARGIN_GB
    ) -> None:
        if int(samples) < 1:
            raise ValueError("a capacity observer needs at least one sample")
        self.samples = int(samples)
        self.margin_gb = int(margin_gb)
        self._history: deque[dict[str, int]] = deque(maxlen=self.samples)
        self.last: Observation | None = None

    def offer(
        self, declared: Mapping[str, int], held: Mapping[str, int] | None = None,
        **overrides: object,
    ) -> dict[str, int]:
        wanted = {str(kind): int(value) for kind, value in declared.items()}
        # Prime the window with the declaration, so a worker's first reading is
        # never decisive on its own.  Drift correction still happens from the
        # first poll: the offer is capped by the declaration, which is the
        # whole of what the drift retire needs.
        while len(self._history) < self.samples - 1:
            self._history.append(dict(wanted))
        seen = observe(wanted, held, margin_gb=self.margin_gb, **overrides)  # type: ignore[arg-type]
        self.last = seen
        self._history.append(dict(seen.capacity))
        return {
            kind: min(value, max(sample.get(kind, value) for sample in self._history))
            for kind, value in wanted.items()
        }
