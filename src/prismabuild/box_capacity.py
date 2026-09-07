"""What this box can honestly offer the pool *now*, not what it offers at rest.

Host memory still comes from ``MemAvailable`` and retains a fixed operating
margin. GPU state comes from one root-published broker snapshot. The snapshot
joins device counters to kernel-verified PrismaBuild scopes, so a job opening
several CUDA contexts remains one attributed job rather than several pieces of
"foreign" work. Worker loops never run ``nvidia-smi`` themselves.

GPU evidence is fail closed. A missing, stale, incomplete or unattributed
snapshot offers zero GPU tokens. A fresh snapshot reports the physical device
count, distinguishes shared-system memory from discrete VRAM, and explicitly
lists foreign processes. Any foreign GPU process closes the offer on today's
single-device GB10 workers. The adaptive admission controller makes the final
claim decision under its host lock; this module keeps the public worker offer
and ledger from contradicting that evidence.

**Host-memory falling is slow and recovery is fast.** ``MemAvailable`` can
move briefly and a retire outlives the loop that made it while that loop runs
an action. The memory offer therefore falls only after the configured sample
window agrees and recovers on the first larger reading. ``rejoin`` discards
pre-action memory samples and caps the first new reading at the ledger total.
GPU evidence does not use that window: broker attribution and freshness are
current safety facts, so GPU capacity changes immediately.

The two memory domains remain separate. ``shared_system`` GPU residency is
already reflected in host ``MemAvailable``. ``discrete`` VRAM is recorded for
the GPU admission and budget controllers and is never added to or subtracted
from the host ``mem_gb`` reservation.

"""

from __future__ import annotations

from collections import deque
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
import math
import os
from pathlib import Path
import time

#: Memory the box keeps for itself rather than offering the queue its last
#: byte.  Carried over from the ``--honest-memory`` flag this replaces.
MEMORY_MARGIN_GB = 8

#: How many consecutive host-memory observations must agree before that offer
#: falls. GPU evidence bypasses this window because its freshness and exact
#: attribution are already bounded by the broker snapshot.
DEFAULT_SAMPLES = 3

MEMINFO = Path("/proc/meminfo")
PROC = Path("/proc")
GPU_CAPACITY_SCHEMA = "prismabuild.gpu_capacity.v1"
GPU_SAMPLE_MAX_AGE_S = 5.0
GPU_MEMORY_DOMAINS = frozenset(("shared_system", "discrete"))

#: Sentinel for "read this from the box".  ``None`` is a real answer -- it means
#: the reading was attempted and failed -- so it cannot double as "unset".
_READ: object = object()


def trusted_gpu_sample() -> Mapping[str, object] | None:
    """Read the broker-owned snapshot through its security-checking reader."""

    try:
        from prismabuild import adaptive_gpu
        value = adaptive_gpu.trusted_sample()
    except (ImportError, OSError, ValueError, TypeError):
        return None
    return value if isinstance(value, Mapping) and value else None


def _number(value: object, *, positive: bool = False) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(number) or number < 0.0 or positive and number <= 0.0:
        return None
    return number


def _gpu_evidence(
    sample: object, *, now: float | None = None,
) -> tuple[list[Mapping[str, object]], list[object], list[object], str | None]:
    """Validate the admission fields a worker consumes from one snapshot."""

    if not isinstance(sample, Mapping):
        return [], [], [], "missing trusted GPU capacity snapshot"
    if (sample.get("schema") != GPU_CAPACITY_SCHEMA
            or sample.get("complete") is not True
            or sample.get("attributed") is not True):
        return [], [], [], "incomplete or unattributed GPU capacity snapshot"
    sampled = _number(sample.get("sampled_unix"), positive=True)
    current = time.time() if now is None else float(now)
    if sampled is None or not 0.0 <= current - sampled <= GPU_SAMPLE_MAX_AGE_S:
        return [], [], [], "stale or future GPU capacity snapshot"
    devices = sample.get("devices")
    foreign = sample.get("foreign_processes")
    jobs = sample.get("jobs")
    if (not isinstance(devices, list) or not devices
            or not isinstance(foreign, list) or not isinstance(jobs, list)):
        return [], [], [], "malformed GPU device or attribution inventory"
    host_total = _number(sample.get("host_total_bytes"), positive=True)
    host_available = _number(sample.get("host_available_bytes"))
    if host_total is None or host_available is None or host_available > host_total:
        return [], [], [], "invalid host memory bounds in GPU snapshot"
    for key in ("memory_pressure_some", "memory_pressure_full",
                "cpu_pressure_some", "cpu_pressure_full"):
        pressure = _number(sample.get(key))
        if pressure is None or pressure > 100.0:
            return [], [], [], f"invalid {key} in GPU snapshot"
    identities: set[str] = set()
    typed_devices: list[Mapping[str, object]] = []
    for device in devices:
        if not isinstance(device, Mapping):
            return [], [], [], "malformed GPU device record"
        identity = device.get("uuid")
        domain = device.get("memory_domain")
        total = _number(device.get("memory_total_bytes"), positive=True)
        free = _number(device.get("memory_free_bytes"))
        used = _number(device.get("memory_used_bytes"))
        if (not isinstance(identity, str) or not identity.startswith("GPU-")
                or identity in identities or domain not in GPU_MEMORY_DOMAINS):
            return [], [], [], "unknown GPU identity, memory domain or bounds"
        raw_memory = tuple(device.get(f"memory_{kind}_bytes")
                           for kind in ("total", "free", "used"))
        if domain == "discrete":
            # Framebuffer counters are the admission budget for a discrete
            # device, so every counter and their joint bound must be known.
            if (total is None or free is None or used is None
                    or free > total or used > total or free + used > total):
                return [], [], [], "unknown GPU identity, memory domain or bounds"
        elif any(value is not None for value in raw_memory):
            # GB10 normally reports N/A for all three counters because the GPU
            # uses host RAM. If a future driver reports them, accept only a
            # complete internally bounded set; host memory remains authoritative.
            if (total is None or free is None or used is None
                    or free > total or used > total or free + used > total):
                return [], [], [], "unknown GPU identity, memory domain or bounds"
        identities.add(identity)
        typed_devices.append(device)
    return typed_devices, foreign, jobs, None


def physical_gpu_count(sample: object, *, now: float | None = None) -> int:
    """Physical devices in one fresh trusted snapshot; zero means unknown."""

    devices, _foreign, _jobs, error = _gpu_evidence(sample, now=now)
    return 0 if error is not None else len(devices)


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


def worker_loops(*, proc: Path = PROC) -> tuple[tuple[int, tuple[str, ...]], ...]:
    """Every PrismaBuild worker loop running on this box, as ``(pid, argv)``.

    A census, deliberately, and not a running total.  A loop knows itself and
    not its siblings, so a count each loop *contributed* to could only ever
    grow: the record would outlive what it described, which is the shape of
    the phantom-node bug (#244) rather than a cure for it.  Reading ``/proc``
    answers the whole question from one loop, and answers it again from
    scratch on the next poll, so a loop that exits is gone from the next
    reading without anybody having to notice it left.

    The matching rule lives here rather than in either caller because two
    readers ask the same question and must not drift: the offer records how
    many loops a box is running, and ``runtime_process_census.py`` refuses a
    runtime activation unless that same set started after it.  A publish gate
    and a metric disagreeing about what a loop *is* would be worse than
    either one alone.

    Cost, measured on sparky 2026-09-06: 4.8 ms median over 556 processes,
    against a 10 s default poll -- 0.05% of one core per loop, or under 1% on
    an eighteen-loop box.  Cheap enough to do every poll, which is what makes
    it a census instead of a cache.
    """

    loops: list[tuple[int, tuple[str, ...]]] = []
    for entry in sorted(proc.glob("[0-9]*"), key=lambda path: int(path.name)):
        try:
            argv = tuple(
                part.decode("utf-8", "replace")
                for part in (entry / "cmdline").read_bytes().split(b"\0")
                if part
            )
        except OSError:
            continue          # exited between the directory read and this one
        if (len(argv) < 2 or "python" not in Path(argv[0]).name
                or not argv[1].endswith("worker_loop.py")):
            continue
        loops.append((int(entry.name), argv))
    return tuple(loops)


@dataclass(frozen=True)
class Observation:
    """One reading of the box, and what it implies about the offer."""

    capacity: dict[str, int] = field(default_factory=dict)
    foreign: dict[str, int] = field(default_factory=dict)
    detail: dict[str, object] = field(default_factory=dict)


def _held_snapshot(
    held: Mapping[str, int] | Callable[[], Mapping[str, int]] | None,
) -> dict[str, int]:
    """One reading of what the pool holds here, taken *now*.

    A callable is a reading the caller can repeat; a mapping is one it already
    took and cannot.  ``observe`` needs the repeatable kind on the memory axis
    -- see the bracket below.
    """

    source = held() if callable(held) else held
    return {str(kind): int(value) for kind, value in (source or {}).items()}


def observe(
    declared: Mapping[str, int],
    held: Mapping[str, int] | Callable[[], Mapping[str, int]] | None = None,
    *,
    margin_gb: int = MEMORY_MARGIN_GB,
    gpu_sample: object = _READ,
    mem_gb: object = _READ,
    load1: object = _READ,
    now: float | None = None,
) -> Observation:
    """What the box can honestly offer, given what the pool already holds here.

    ``declared`` is what this worker was configured to offer; the result never
    exceeds it, because observing the box is how an offer *falls*, never how it
    grows past its configuration. ``held`` accounts for the pool's reserved
    host memory, and should be a **callable** so it can be read on both sides
    of the memory instrument: a mapping is a reading somebody else took at an
    instant this function cannot know, and the memory clamp adds it to one this
    function takes itself. GPU ownership comes only from the broker's
    attributed job and foreign-process inventories; process counts are never
    inferred here.

    A kind nothing here knows how to read passes through untouched: an
    unobservable resource is not a busy one.
    """

    wanted = {str(kind): int(value) for kind, value in declared.items()}
    ours = _held_snapshot(held)
    capacity = dict(wanted)
    foreign: dict[str, int] = {}
    detail: dict[str, object] = {}

    def clamp(kind: str, foreign_units: int) -> None:
        # One name for one quantity: ``foreign`` is what the announce record
        # publishes and what the worker loop prints.
        if foreign_units <= 0:
            return
        foreign[kind] = foreign_units
        capacity[kind] = max(0, wanted[kind] - foreign_units)

    if wanted.get("gpu", 0) > 0:
        if gpu_sample is _READ:
            gpu_sample = trusted_gpu_sample()
        devices, foreign_processes, jobs, error = _gpu_evidence(
            gpu_sample, now=now)
        detail["gpu_capacity_trusted"] = error is None
        if error is not None:
            detail["gpu_capacity_error"] = error
            capacity["gpu"] = 0
        else:
            domains = [str(device["memory_domain"]) for device in devices]
            def memory_sum(kind: str) -> int | None:
                values = [device.get(f"memory_{kind}_bytes") for device in devices]
                return (sum(int(value) for value in values)
                        if all(_number(value) is not None for value in values)
                        else None)
            detail.update({
                "physical_gpus": len(devices),
                "gpu_memory_domains": domains,
                "gpu_memory_total_bytes": memory_sum("total"),
                "gpu_memory_free_bytes": memory_sum("free"),
                "gpu_memory_used_bytes": memory_sum("used"),
                "gpu_attributed_jobs": len(jobs),
                "foreign_gpu_processes": len(foreign_processes),
            })
            capacity["gpu"] = min(wanted["gpu"], len(devices))
            if foreign_processes:
                # The public inventory is already the broker's exact
                # attribution result. Do not subtract job process counts or
                # ledger slots from it: one job may own many CUDA processes.
                clamp("gpu", wanted["gpu"])

    if "mem_gb" in wanted:
        if mem_gb is _READ:
            mem_gb = mem_available_gb()
            # ``ours`` and ``MemAvailable`` are *added* below, so they have to
            # describe the same instant.  They do not: the caller reads the
            # ledger, then this line reads /proc.  A **sibling** loop releasing
            # in between is in both terms at once -- still in ``ours`` because
            # that read is older than its release, already free in
            # ``MemAvailable`` because every ending proves the payload stopped
            # before it returns the tokens (``cleanup_action_containers``:
            # terminate, clean up containers, sample, release scope, and only
            # then ``ledger.release``).  The window is that whole gap, seconds
            # wide, and the box then offers memory it does not have for
            # ``samples`` polls, because ``offer`` takes a maximum.
            #
            # ``rejoin`` does not cover this.  It caps *this* observer after
            # *this* loop's action, and a loop is single-threaded: its own
            # release cannot land inside its own reading.  A sibling's can, and
            # a sibling's ``rejoin`` touches the sibling's observer.  Boxes run
            # 3-16 loops against one ledger.
            #
            # So take the reading twice and keep what is held in both.  A token
            # that vanished mid-reading is not credited to ``ours``; one that
            # appeared mid-reading is not either, which lowers the offer and is
            # the safe direction for a number that admits work.
            if callable(held):
                after = _held_snapshot(held)
                if after != ours:
                    detail["held_moved_during_read"] = {
                        "before": dict(ours), "after": dict(after)}
                    ours = {kind: min(value, after.get(kind, 0))
                            for kind, value in ours.items()}
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
            # Recorded, never clamped.  A ``cpu`` token is a SLOT and the run
            # queue counts THREADS, so the two are not in the same units and
            # the subtraction above has nothing to subtract: an action holding
            # one slot legitimately runs twenty threads, and the box would be
            # charged as foreign for the pool's own work.  Measured on sparky
            # 2026-09-04 -- load1 22.47 against 5 held cpu tokens, and every
            # runnable task was a pool-scheduled action:
            #
            #     python3 -m pytest tests/ -q            (claimed)
            #     python3 -m pytest tests/test_matched_reach.py -q  (claimed)
            #     python -u experiments/refit_trailing_pair.py      (claimed)
            #     python -u experiments/export_tessera_serving.py   (claimed)
            #
            # Clamping on that reading takes a busy-with-our-own-work box from
            # ten cpu slots to zero.  That is the double-charge the ledger
            # attribution exists to prevent, reappearing because the instrument
            # does not measure the quantity the token names.  gpu and mem_gb do
            # not have this problem: a compute app is countable against a gpu
            # token, and MemAvailable is in the token's own units.  So the load
            # travels in ``detail`` for a human to read, and the cpu offer
            # stays the declaration until there is an instrument that can
            # attribute a thread to a reservation.
            detail["load1"] = round(float(load1), 2)  # type: ignore[arg-type]

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
        self, *, samples: int = DEFAULT_SAMPLES, margin_gb: int = MEMORY_MARGIN_GB,
        ledger_total: Mapping[str, int] | None = None,
    ) -> None:
        if int(samples) < 1:
            raise ValueError("a capacity observer needs at least one sample")
        self.samples = int(samples)
        self.margin_gb = int(margin_gb)
        # What this host's ledger already totals, read once by the caller: the
        # window is seeded from it so a restarting loop inherits the box's
        # standing verdict instead of re-asserting the declaration.  An empty
        # mapping means the ledger has never heard of this host -- which is not
        # the same as a host whose gpu total is zero, so a *known* host missing
        # a kind seeds that kind at zero.
        self.ledger_total = ({str(k): int(v) for k, v in ledger_total.items()}
                             if ledger_total else {})
        self._history: deque[dict[str, int]] = deque(maxlen=self.samples)
        # The seed pads the window once, on the first reading this observer
        # ever takes.  ``rejoin`` empties the window without setting this
        # back, because a loop coming out of an action must not be padded --
        # see the module docstring.
        self._seeded = False
        self._cap: dict[str, int] | None = None
        self.last: Observation | None = None

    def rejoin(self, ledger_total: Mapping[str, int] | None = None) -> None:
        """Forget the window: this observer has not been watching the box.

        A worker loop spends the whole of an action inside ``serve_once`` --
        up to ``--timeout-s`` 7200 -- and takes no readings while it is there.
        Its host-memory samples then describe the box before the action, and
        ``offer`` uses their maximum, so they would go on deciding that offer
        for ``samples`` - 1 polls after the action returns: exactly the polls
        in which the loop claims again.

        ``ledger_total`` caps the first offer taken after this call, and is
        the host's ledger total read after the action released its tokens.  It
        is a cap and not a seed because a seed would raise the offer back to
        it, and part of it is the token this loop has just let go of.  Pass
        nothing, or an empty mapping, for a ledger that has no total for this
        host; that is "no verdict to inherit", not "no capacity".
        """

        self._history.clear()
        # An emptied window is not a new one: the pad is what a start gets, and
        # a return must not be given it -- see the module docstring.
        self._seeded = True
        self._cap = ({str(k): int(v) for k, v in ledger_total.items()}
                     if ledger_total else None)

    def offer(
        self, declared: Mapping[str, int],
        held: Mapping[str, int] | Callable[[], Mapping[str, int]] | None = None,
        **overrides: object,
    ) -> dict[str, int]:
        wanted = {str(kind): int(value) for kind, value in declared.items()}
        if not self._seeded:
            # Seed the window once, so a worker's first reading is never
            # decisive on its own -- from the ledger's standing total where
            # there is one, and from the declaration only on a host the ledger
            # has never seen.  Drift correction still happens from the first
            # poll either way: the offer is capped by the declaration, which is
            # the whole of what it needs.
            seed = dict(wanted) if not self.ledger_total else {
                kind: min(value, self.ledger_total.get(kind, 0))
                for kind, value in wanted.items()
            }
            while len(self._history) < self.samples - 1:
                self._history.append(dict(seed))
            self._seeded = True
        seen = observe(wanted, held, margin_gb=self.margin_gb, **overrides)  # type: ignore[arg-type]
        self.last = seen
        sample = dict(seen.capacity)
        if self._cap is not None:
            # The first reading back from an action, held down to what the
            # ledger already totals here.  A kind the ledger has no total for
            # is a kind this host has none of.
            sample = {kind: min(value, self._cap.get(kind, 0))
                      for kind, value in sample.items()}
            self._cap = None
        # The window remembers what was offered, not what was read: recording
        # the uncapped reading would let the next poll's maximum undo a cap
        # that has already been spent.
        self._history.append(sample)
        return {
            # Root-published GPU attribution is a current admission fact, not
            # a noisy per-process estimate. Apply it immediately in both
            # directions; the adaptive controller independently checks the
            # same fresh snapshot under its host lock. Host-memory readings
            # retain the conservative multi-sample fall used before this
            # telemetry existed.
            kind: min(value, sample.get(kind, value)) if kind == "gpu"
            else min(value, max(s.get(kind, value) for s in self._history))
            for kind, value in wanted.items()
        }
