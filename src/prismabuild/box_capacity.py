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

**Two kinds are clamped, one is only recorded.**  The pool prices its own work
at one token per action, and those tokens are the whole of what there is to
subtract with, so a kind is clamped when one action shows up as about one unit
on the instrument.  ``MemAvailable`` is exact: a ``mem_gb`` token is a
gigabyte and the reading is in gigabytes.  A GPU compute app is an
approximation: one context per action is what the pool *prices*, and the one
action measured here opened one (sparky 01:33, below).  Whether that holds for
a pytest suite or a container that spawns its own children is not measured,
which is what the next section is for.  The run queue is not an
approximation at all: a ``cpu`` token is a SLOT, load counts THREADS, and one
action legitimately runs twenty of them, so the error is unbounded and it is
made on every action rather than on some.  Measured on sparky 2026-09-04:
load1 22.47 against 5 held cpu tokens, every runnable task a pool-scheduled
action, which the subtraction scored as 17 foreign cpu and would have taken
the box from ten slots to zero for being busy with the pool's own work.  So
the load travels in ``detail`` for a human to read and clamps nothing, until
an instrument exists that can attribute a thread to a reservation.

**What the gpu clamp gets wrong, and by how much.**  ``len(apps) -
held_gpu`` is the whole of the arithmetic, and it is wrong in both directions
by an amount the ledger cannot know, because the ledger records how many
tokens an action holds and never how many CUDA contexts it opens.

* An action that opens *k* contexts reads as *k*-1 foreign, so the box
  under-offers itself for that action's length.  Pool GPU actions on this
  fleet are pytest suites and bash wrappers that spawn their own children, so
  *k* >= 2 is realistic.  On sparky -- 2 gpu slots, the only box on the fleet
  with more than one -- *k* = 2 costs the second slot until the action ends
  and *k* >= 3 takes the offer to zero.
* A held gpu token whose action has no live context -- before the context is
  created, after it is torn down, or a reservation standing in for something
  else -- forgives one foreign app.  ``out-of-pool-ts60-encode-sparklina``,
  the hand reservation the issue describes, is exactly that shape: while it
  stands, one real foreign encode is invisible to this reading.  So is an
  exclusive campaign between two of its phases -- read live on sparky
  2026-09-04, ``held {'cpu': 1, 'gpu': 2, 'mem_gb': 48}`` against zero compute
  apps.  On sparklina the masking costs nothing, because that box offers one
  gpu slot and the reservation holds it, so the offer is zero either way.  On
  sparky it would cost a slot:
  one context-less reservation beside one foreign process reads ``foreign``
  0, offers both slots and holds one, and a GPU action is placed on top of
  the foreign process.

Both errors end when the holder does, neither can raise the offer above the
declaration, and neither invents a constant.  The alternative -- attribute
apps by walking the process tree -- is measured not to work on this fleet, for
the reason below.

**The two readings are one bracket, not one instant.**  The offer is a
difference between what the ledger holds and what the instrument sees, and
those are two separate reads.  Whichever order they are taken in, an action
that crosses between them is counted wrong, and one of the two directions is
the failure this module exists to prevent:

* Ledger first: an action that finishes inside the reading leaves its token in
  the first read and its CUDA context out of the second, so the token is spent
  forgiving somebody else's compute app and the box offers a slot the foreign
  work is sitting on.
* Instrument first: an action that starts inside the reading is in neither, and
  is charged as foreign.  That direction only under-offers.

So ``held`` is a *verb* here and not a value: pass the ledger's ``held``
method, it is read on both sides of the instruments, and what a token may
forgive is the elementwise **minimum** of the two.  A token that was not held
for the whole of the reading forgives nothing.  The bracket is the width of
the instruments inside it, and the release it races is ``finish``'s.  Timed on
sparky against ``/mnt/shared`` 2026-09-04: ``nvidia-smi --query-compute-apps``
10 ms, ``held()`` on the live ledger 1.0 ms; against 0.25 ms for the
``withdrawn_keys`` listing, 0.02 ms for the claim record, and 4.1 ms for 16
renames on the same share -- that last timed in a scratch directory rather
than in the live ledger.  Two intervals of the same size, which is what makes
the overlap ordinary rather than exotic.  Not timed: ``execute``'s own tail
between the child exiting and ``finish`` being entered.  Nor is the child's
exit the moment its memory comes back, since on this fleet the action is
often a container's process reparented away -- and that error is in the
conservative direction, the box charging itself for bytes it has released.  A
caller with only a snapshot may still pass a mapping; it cannot be bracketed,
and it is read as the whole of the truth.

**What the bracket does not close: a claim is not yet a use.**  Between
``acquire`` and the action allocating anything there is a real interval -- an
interpreter starting, a checkpoint loading -- and through all of it the tokens
are held while the box still shows the memory free and the card without a
context.  The mem_gb arithmetic below adds held tokens back to
``MemAvailable``, so at the moment the pool admits an action *the box's offer
rises by the size of that action's claim*, and on a box already short of
memory that rise is handed straight back out as free tokens.  Quantified in
``test_a_claim_that_has_not_allocated_yet_forgives_memory_still_free``: 48
declared, 20 GB of foreign work, an action holding 16 it has not touched, and
the offer is 36 against 4 GB the box can honour.

That one is not a bug to fix but a degree of freedom with no instrument on it.
Write the offer as a total and it cancels: ``total = held + (available -
margin)`` is the same statement as ``free = available - margin``, so the only
choice being made is how much of ``held`` to credit against a reading that may
or may not already exclude it.  Credit all of it and an unspent claim is
offered twice, which is the paragraph above.  Credit none of it and you have
the ``--honest-memory`` flag this module replaced: an action's bytes are
subtracted once by ``MemAvailable`` and once again as tokens, and a box doing
exactly what the pool told it to do offers ``held`` GB less than it has.  Read
live 2026-09-04, two of dl380g10's sixteen loops held 24 of that box's 60
declared GB; five of the sixteen would hold all of it, and the box would offer
nothing while running the pool's own work.  Nothing between the two
extremes is measurable from ``MemAvailable``, a count of compute apps and a
token, because none of them attributes a byte to a reservation.  The bracket
closes the part that is a *reading* error; this part is an accounting choice,
and the optimistic one is taken deliberately.

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
foreign work is gone.  A worker's first polls still correct ledger drift
whatever the window says, because the offer is capped by the declaration
either way.

**A window only holds evidence, and evidence expires.**  The same two hours
inside ``serve_once`` make the window a liability at the other end of the
action.  Its samples describe the box *before* the action; the maximum is
still taken over them for ``samples`` - 1 polls after the action returns, and
those polls are the moment the loop is about to claim again.  Measured with
these classes 2026-09-04: a loop that polled an idle box three times, claimed
a GPU action, and came back to two foreign encodes went on offering 2 gpu for
two more polls, re-minted the token a sibling loop had retired, and acquired
against it twice.  That is this whole blindness, on a timer tied to the end of
every action.

So ``rejoin`` empties the window, and the first offer after it is capped by
what the ledger already totals for this host.  A loop coming back from an
action believes neither of the two things that would lie to it: not its own
samples, which have expired, and not the token it has just released, which is
why the ledger total is a cap and not a seed.  The price is one poll of
possible under-offer, paid at the end of each action; a retire deletes free
tokens only, and the three to sixteen other loops on a box re-mint them from
their own next claim.

What the cap does not do is make the first poll back *safer* than an ordinary
one.  The ledger total it caps at still counts the token this loop released,
so a reading that lands between two foreign processes can still take that
token -- it just cannot mint a new one.  After ``rejoin`` the first poll back
is exactly as trustworthy as any other single poll, and no more.

Restarting is the same situation with one difference, and it is why the seed
survives beside the cap: a starting loop has never read the box, so there is
nothing to empty.  Several loops share one box and one ledger, and
``ensure_capacity`` is increase-only, so the box's effective offer is the most
optimistic *live* loop's -- which is why a starting loop must not prime its
window with the declaration.  A loop exits on ``--max-idle`` and the
supervisor replaces it, so on a box with three to five loops one of them
restarts every half hour or so; priming from the declaration would have each
restart re-mint, for the length of its window, every free token the other
loops had retired, and any loop on the box could then acquire one.  So an
observer is seeded with what the ledger already totals for this host, capped
by the declaration, and only a box the ledger has never heard of is seeded
with the declaration itself.  The seed can only ever *lower* the offer,
because the offer is a maximum: one reading of an idle box restores it in a
single poll.

Nothing here reads a GPU's memory as a *pool* of its own.  On GB10 the GPU and
the host share one physical memory, so a compute app's resident bytes are
already inside ``MemAvailable``; counting them twice would clamp a box for work
it had already subtracted.  The per-app figure is still read and recorded, as
the evidence for why an offer fell.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Callable, Mapping
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


def _counts(held: Mapping[str, int] | None) -> dict[str, int]:
    return {str(kind): int(value) for kind, value in (held or {}).items()}


def observe(
    declared: Mapping[str, int],
    held: Mapping[str, int] | Callable[[], Mapping[str, int]] | None = None,
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

    Pass ``held`` as the ledger's ``held`` **method** wherever the ledger is at
    hand.  It is then read on both sides of the instruments and a token may
    forgive only what it held throughout, so an action finishing inside the
    reading cannot spend its token on somebody else's compute app.  A plain
    mapping is a snapshot of one instant that the instruments are not from, and
    is trusted as given; it is for callers who have no ledger to ask.

    A kind nothing here knows how to read passes through untouched: an
    unobservable resource is not a busy one.
    """

    wanted = {str(kind): int(value) for kind, value in declared.items()}
    read_held = held if callable(held) else None
    before = _counts(read_held() if read_held is not None else held)  # type: ignore[arg-type]
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

    # Every instrument first, then the ledger again: what a token may forgive
    # is what it held on BOTH sides of these readings.  See the module
    # docstring -- ledger-then-instrument spends a finished action's token on a
    # foreign process, which is the failure this module exists to prevent.
    apps = None
    if wanted.get("gpu", 0) > 0:
        if gpu_apps is _READ:
            gpu_apps = gpu_compute_apps()
        if gpu_apps is not None:
            apps = list(gpu_apps)                     # type: ignore[arg-type]
    if "mem_gb" in wanted:
        if mem_gb is _READ:
            mem_gb = mem_available_gb()
    if wanted.get("cpu", 0) > 0:
        if load1 is _READ:
            load1 = run_queue()

    after = _counts(read_held() if read_held is not None else None)
    # A kind that was not in both readings held nothing across them.  A
    # snapshot caller has one reading and it stands as given.
    ours = (before if read_held is None else
            {kind: min(value, after.get(kind, 0)) for kind, value in before.items()})

    if apps is not None:
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
        if mem_gb is not None:
            available = int(mem_gb)                   # type: ignore[arg-type]
            detail["mem_available_gb"] = available
            # The honest total is what the pool already holds here plus what is
            # physically free, capped by the declaration.  Adding the held part
            # back is not generosity: once an action has allocated its bytes
            # they are missing from ``MemAvailable`` *and* reserved as tokens,
            # so a clamp that ignored them would charge the box twice for its
            # own work -- which is what ``--honest-memory`` did.
            #
            # BEFORE it allocates them the add-back is wrong the other way, and
            # nothing here can tell the two apart: see "a claim is not yet a
            # use" in the module docstring for the arithmetic and for why no
            # instrument on this fleet decides it.
            honest = min(wanted["mem_gb"],
                         max(0, ours.get("mem_gb", 0)) + max(0, available - margin_gb))
            clamp("mem_gb", wanted["mem_gb"] - honest)

    if wanted.get("cpu", 0) > 0:
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
        Its samples then describe the box before the action, and ``offer`` is
        an elementwise maximum, so they would go on deciding the offer for
        ``samples`` - 1 polls after the action returns: exactly the polls in
        which the loop claims again.  Measured with these classes, that lets a
        loop re-mint a gpu token a sibling had retired and acquire against it
        twice, on top of the foreign work the retire was about.

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
        """One reading, held down by the window.  ``held`` is ``observe``'s.

        Which means: pass the ledger's ``held`` method, not its result, so the
        pool's own consumption is read on both sides of the instruments.
        """

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
            kind: min(value, max(s.get(kind, value) for s in self._history))
            for kind, value in wanted.items()
        }
