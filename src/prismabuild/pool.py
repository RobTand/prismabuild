"""Shared-filesystem pull-queue transport: dispatch without a scheduler.

``slurm.py`` and ``dagster.py`` both assume a scheduler that is not installed on
this fleet, so PrismaBuild has never dispatched anything.  This module is the
third transport and the one that runs here: workers pull sealed actions from a
directory on the shared NFS mount and execute them through the *same* canonical
worker argv SLURM would have submitted.

**Every primitive here is ported from ``pqwork``**, the predecessor this
replaces, because those primitives were argued out against real NFS behaviour
and two boxes of production use:

* **Claiming is ``rename()``, never ``flock``.**  ``rename`` is atomic on NFSv4;
  advisory locking over NFS is version-dependent.
* **A claim is a lease, not a grant.**  The claimant refreshes a heartbeat file;
  any worker may return a stale claim to ``ready``.  That is what makes a dead
  box self-healing rather than a permanent hold on the work.
* **Intent is written before the claim**, mirroring ``slurm.py``'s
  intent-before-``sbatch`` discipline, so a crash between the two is
  diagnosable rather than invisible.

**Admission is capacity-aware, and it is built from that live defect rather than
around it.**  The one documented failure on this fleet
(``/mnt/shared/pq-ops/starvation/REPRO-2026-08-30``) was an admission failure,
not a transport failure, and it named three bugs.  This ledger answers each
structurally rather than by policy:

* **Hold-while-gated.**  There, an actor acquired both boxes' whole memory
  budget and *then* waited for a drain condition its own hold prevented from
  ever being observed.  Here a reservation is acquired inside ``claim`` and
  released in ``finish``, so a holder is by construction *running*, never
  waiting.  There is no window in which holding and waiting overlap, so the
  circularity has nowhere to form.
* **No aging.**  There, ``evicted_5x_does_not_fit`` was counted and then the job
  was abandoned: "an eviction counter that only counts is a starvation detector
  wired to nothing."  Here a denial increments ``passes``, ``passes`` is the
  first term of the ready ordering, and past ``STARVATION_FLOOR`` a denied item
  *withholds the host* -- a worker that cannot admit the starved item declines
  to admit a smaller one instead of leapfrogging it.  The counter is wired to
  the decision it describes.
* **Partial-hold waste.**  Acquisition is all-or-nothing: a demand that cannot
  be met in full releases every token it took before returning.

The deadlock the floor could otherwise cause is handled explicitly: an item
whose demand exceeds this host's *total* capacity can never run here, so it is
skipped rather than allowed to withhold a box it would never use.

**Idempotence is free and is not reimplemented.**  ``run_local_action`` looks the
action key up in the CAS first and returns ``cache_hit`` without executing, so a
double dispatch after a stale-lease requeue costs a lookup, not a recomputation.
The transport therefore never needs to reason about "did this already run" --
the CAS is the single answer, which is the whole point of the action key.

**Withdrawal is an operator's decision, and it is filed as one.**  ``finish``,
``reap_stale`` and ``quarantine_orphans`` each describe a *worker's* health; none
of them says "I have changed my mind", so cancelling meant rewriting
``max_attempts`` into a live claimed record and then racing the retry that the
kill would otherwise trigger.  ``withdraw`` is that missing verb.  It writes its
marker *before* it removes anything, and ``claim``, ``finish`` and ``reap_stale``
all consult that marker, so from the moment it exists the action cannot be
claimed, cannot be requeued and cannot be filed as a defect -- whatever a
concurrent worker is doing at the time.  The withdrawal wins the race by
construction rather than by the operator being quick.

**It cancels a run, not a name.**  An action key is a content hash, so the same
command against the same tree fingerprints identically and re-submitting it is
how anybody asks for the same work again.  A marker that blacklisted the key
would therefore make the queue eat that request -- ``claim`` deleting the fresh
record, ``pbrun`` answering the new run with the old run's reason -- with the
only remedy a hand edit of the live queue, which is the thing this verb exists
to abolish.  So the marker is scoped to the generation it was filed against
(``published_unix``, which ``publish`` stamps fresh and every requeue carries
forward), one predicate reads it at every guard site (``withdrawal_covers``),
and a later ``publish`` retires it into ``withdrawn/superseded/``.  Nothing is
ever removed silently: a record the queue drops is filed there first.

Clock skew between claimant and reaper is real but immaterial here: both Sparks
are NTP-synchronised and measured 1.2-2.5 ms apart against a 300 s lease.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time
import uuid

from . import core as pb

POOL_ITEM_SCHEMA_V1 = "prismaquant.prismabuild.pool_item.v1"
POOL_CLAIM_INTENT_SCHEMA_V1 = "prismaquant.prismabuild.pool_claim_intent.v1"
POOL_LEASE_SCHEMA_V1 = "prismaquant.prismabuild.pool_lease.v1"
POOL_OUTCOME_SCHEMA_V1 = "prismaquant.prismabuild.pool_outcome.v1"
POOL_OFFER_SCHEMA_V1 = "prismaquant.prismabuild.pool_offer.v1"

# The `prismaquant.` prefix is kept on purpose.  It is the namespace grammar of
# every receipt already published to this CAS; mixing prefixes inside one store
# would be worse than carrying the history.  See the package docstring.

READY = "ready"
CLAIMED = "claimed"
DONE = "done"
FAILED = "failed"
INTENT = "intent"
#: Where an operator's cancellation is filed.  Deliberately not ``failed``: a
#: withdrawn action is a decision, and putting it in the failure record makes
#: the failure record lie about the fleet.  Four withdrawn test suites are in
#: the live ``failed/`` for exactly that reason.
WITHDRAWN = "withdrawn"
_STATES = (READY, CLAIMED, DONE, FAILED, INTENT, WITHDRAWN)

# Ported verbatim from pqwork: 30 s refresh, 300 s expiry.  The 10x margin is
# what absorbs an NFS stall or a long GC pause without a spurious requeue.
HEARTBEAT_S = 30.0
LEASE_TIMEOUT_S = 300.0

# How long the timeout path gives the launcher's process group to go down.
# The launcher does not just exit when signalled: it relays the signal into the
# action's own session -- ``run_local_action`` gives the action
# ``start_new_session=True`` -- and that relay is itself TERM, grace, KILL,
# grace, i.e. two of core's windows.  SIGKILLing the launcher before it
# finishes would orphan the very action this timeout exists to stop, so the
# budget is core's two windows and not a number of its own; the 5 s on top is
# margin for the launcher's own exit once the relay has returned.
TIMEOUT_GRACE_S = 2.0 * pb._PROCESS_GROUP_GRACE_SECONDS + 5.0

# Retries exist because the CAS makes them free: re-running a completed action
# is a receipt lookup, so the only cost of one more attempt is the attempt.
DEFAULT_MAX_ATTEMPTS = 3

# How many admission denials before a ready item stops being overtaken.  The
# repro's job died at `evicted_5x_does_not_fit`, so five is the count at which
# the old system gave up; the floor has to bite strictly before that or it
# inherits the same outcome.  Three is chosen on that ground alone -- it is a
# policy knob, not a derived constant, and nothing downstream depends on its
# value beyond "small, and less than five".
STARVATION_FLOOR = 3

#: How long a starved item may withhold a host before it keeps its place in the
#: ordering but loses its veto.
#:
#: The withhold exists so small work cannot indefinitely overtake big work.  It
#: assumed the block is transient -- the box is busy *now* and will free up.
#: When the blocking resource is held for hours that assumption inverts and the
#: guard becomes the deadlock it was written to prevent.  Measured on the live
#: fleet 2026-09-04: one GPU action at the head of sparky's queue accumulated
#: **293** denied passes while two multi-hour GPU actions held both slots, and
#: withheld the box the whole time.  Forty-one items queued behind it, 24 of
#: them CPU-only and admissible against the five free cores it was not using.
#:
#: Past this ceiling the item stops *blocking* but keeps every pass it has
#: earned, and passes are the first term of the ready ordering -- so it still
#: gets first refusal on every claim, on every box, ahead of everything behind
#: it.  It loses the veto, not the priority.  Fifteen minutes is longer than
#: any transient this pool produces (the lease timeout is five) and far shorter
#: than the multi-hour actions that turn the guard pathological.
WITHHOLD_CEILING_S = 900.0

RESERVATIONS = "reservations"
PASSES = "passes"
WORKERS = "workers"

#: How long each rung of a withdrawal's signal ladder waits before escalating.
#: Matched to ``core._PROCESS_GROUP_GRACE_SECONDS``, which is the grace the
#: launcher itself gives the action group it reaps on the way out.
WITHDRAW_GRACE_S = 5.0

#: How long a worker's offer stays believable.  A loop re-announces on every
#: poll, and the default poll is 10 s, so two minutes is a dozen missed polls:
#: long enough that a slow NFS write or a long action never makes a live box
#: look dead, short enough that a box taken down does not keep vouching for
#: work nobody can run.
OFFER_TIMEOUT_S = 120.0

DEFAULT_POOL_ROOT = Path(
    os.environ.get("PRISMABUILD_POOL_ROOT", "/mnt/shared/pb-queue")
)

#: Every box mounts this at the same path.  A checkout underneath it is
#: visible to all of them; a checkout outside it exists on exactly one box.
SHARED_ROOT = Path("/mnt/shared")


def is_box_local_path(path: object) -> bool:
    """Does this absolute path exist on exactly one box?

    The rule that decides an action's placement lives here rather than in
    ``pbrun`` because two readers need it and they must not disagree: the
    submitter turns it into a pin (``pbrun.placement_tags``), and the queue
    turns it into a width (``placement_census``).  A second copy is how the
    pin and the measurement of the pin end up describing different fleets.

    A pure string test, deliberately.  The census reads paths recorded by
    OTHER boxes, and ``resolve()`` would follow the reading box's symlinks
    through a tree it does not have -- so resolution belongs at submit time,
    where the path is local and real, and ``pbrun`` does it before this is
    ever asked (``pbrun.py`` resolves ``--cwd`` and publishes the resolved
    string).

    An absent path answers ``False``: an item with no ``checkout_root``
    recorded is a thing we know nothing about, and inventing a pin for it
    would put a number in the census that no path put there.
    """

    text = str(path or "")
    if not text:
        return False
    return not Path(text).is_relative_to(SHARED_ROOT)


class PoolError(pb.PrismaBuildError):
    """A queue-level failure, distinct from an action-level one."""


class PoolContractError(PoolError, ValueError):
    """A queue record does not satisfy its schema."""


def _now() -> float:
    return time.time()


def _write_json_atomic(path: Path, payload: Mapping[str, object]) -> None:
    """Publish a record by rename, so no reader ever sees a partial file."""

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.parent / f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    data = pb._canonical_bytes(dict(payload))
    descriptor = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
    os.replace(tmp, path)


def _read_json(path: Path) -> dict[str, object] | None:
    try:
        raw = path.read_bytes()
    except FileNotFoundError:
        return None
    if not raw:
        return None
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise PoolContractError(f"queue record is not valid JSON: {path}") from exc
    if not isinstance(value, dict):
        raise PoolContractError(f"queue record is not an object: {path}")
    return value


def worker_argv(
    *,
    worker_script: str | Path,
    action_key: str,
    cas_root: str | Path,
    checkout_root: str | Path,
) -> list[str]:
    """The canonical worker launch, identical to SLURM's minus its own gate.

    ``slurm.py`` pins ``worker_argv`` to an exact list and refuses anything else,
    so that what a scheduler runs is what the action key describes.  This
    transport holds the same line: the only difference is the absence of
    ``--require-slurm-initial-start``, which asserts SLURM job membership and is
    meaningless without a scheduler.  Keeping the rest byte-identical is what
    makes a result portable between transports -- an action executed here and
    the same action executed under SLURM must be the same execution, or the CAS
    receipt is comparing two different things.
    """

    return [
        str(worker_script),
        "run-local",
        "--action",
        str(Path(cas_root) / "requests" / action_key[:2] / f"{action_key}.json"),
        "--cas-root",
        str(cas_root),
        "--checkout-root",
        str(checkout_root),
    ]


def _drain(
    process: subprocess.Popen[str], *, timeout_s: float
) -> tuple[str, str, bool]:
    """Collect what the pipes hold without waiting on whoever still holds them.

    The action inherits the launcher's stdout and stderr, so the read side sees
    EOF only when the *action* exits -- not when the launcher does.  An
    unbounded ``communicate()`` after a kill therefore blocks for exactly as
    long as the runaway it was called to stop.  Take the partial output the
    timeout carries instead, close the pipes, and say so.

    Returns ``(stdout, stderr, survived)``, where ``survived`` is True when EOF
    never arrived.
    """

    try:
        out, err = process.communicate(timeout=timeout_s)
        return out or "", err or "", False
    except subprocess.TimeoutExpired as exc:
        # ``communicate`` attaches what it had read to the timeout, undecoded
        # even under ``text=True``.  A truncated log beats no log.
        partial = []
        for chunk in (exc.output, exc.stderr):
            if chunk is None:
                partial.append("")
            elif isinstance(chunk, bytes):
                partial.append(chunk.decode("utf-8", errors="replace"))
            else:
                partial.append(str(chunk))
        for pipe in (process.stdout, process.stderr):
            if pipe is not None:
                pipe.close()
        return partial[0], partial[1], True


def _scan(directory: Path):
    """List a directory that another process may be deleting underneath us.

    Every ledger scan walks entries a concurrent worker is free to remove --
    ``release`` is documented safe to call twice, so two of them will have one
    ``rmdir`` the directory the other is mid-iteration over.  At one worker per
    box that never happens; at sixty it happens within minutes, and the worker
    dies with ``FileNotFoundError`` on ``iterdir`` rather than losing a lease
    gracefully.  A directory that vanished held nothing this caller can still
    act on, so the honest answer is an empty listing, not an exception.
    """

    try:
        return sorted(directory.iterdir())
    except (FileNotFoundError, NotADirectoryError):
        return []


def _glob(directory: Path, pattern: str):
    """``Path.glob`` with the same disappearing-directory contract as `_scan`."""

    try:
        return sorted(directory.glob(pattern))
    except (FileNotFoundError, NotADirectoryError):
        return []


def _process_alive(pid: int) -> bool:
    """True while ``pid`` exists and has not already exited.

    ``os.kill(pid, 0)`` is the usual test and it is the wrong one here.  When
    ``execute`` stops its own child, that child is a zombie between its exit and
    the ``communicate()`` that reaps it -- and a zombie answers signal 0 quite
    happily.  A stop that believed it would climb its whole escalation ladder,
    signalling harder and harder at a process that had already died.
    """

    try:
        with open(f"/proc/{pid}/stat", encoding="utf-8") as handle:
            # The comm field can contain spaces and parentheses; everything
            # after the last ")" is fixed-width, and state is its first field.
            after_comm = handle.read().rsplit(") ", 1)[-1].split()
    except OSError:
        return False
    return bool(after_comm) and after_comm[0] != "Z"


def action_process_groups(launcher_pid: int) -> list[int]:
    """The process groups this launcher's children lead -- i.e. the action.

    ``execute`` launches ``worker.py run-local``, and that launcher is the only
    pid this queue ever holds.  The *action* -- the pytest, the encode, the
    thing holding the cores and the GPU -- is one level further down, and
    ``core.run_local_action`` starts it with ``start_new_session=True``, so it
    leads its own process group and **no signal aimed at the launcher reaches
    it**.  That is precisely why cancelling by hand meant ``kill -TERM -$pgid``
    with a pgid found by eye.  This function is that lookup, done by the queue.

    Only a child that leads its own group is returned (``getpgid(c) == c``).  A
    child sharing someone else's group is sharing *this worker's* -- ``execute``
    does not start a new session -- and signalling that group would take the
    worker loop down with the action.
    """

    try:
        raw = Path(f"/proc/{launcher_pid}/task/{launcher_pid}/children").read_text()
    except OSError:
        return []
    groups: list[int] = []
    for token in raw.split():
        try:
            child = int(token)
        except ValueError:
            continue
        try:
            if os.getpgid(child) == child:
                groups.append(child)
        except OSError:
            continue          # it exited between the read and the lookup
    return groups


def launcher_owns_action(pid: int, action_key: str) -> bool:
    """Is ``pid`` still this action's launcher, or a recycled number?

    The launcher's argv names the action's request file, so the key is in its
    command line.  A lease can outlive the process it describes -- that is what
    makes it a lease -- and a withdrawal that signalled a recycled pid would
    kill whatever the box started next.

    The same predicate as ``find_launcher_pids``, and for the same reason: the
    key alone is not enough, because a command line that *mentions* the key is
    not a launcher.  ``pbrun --withdraw <full digest>`` is one, and a stale
    foreign ``child_pid`` colliding with that shell's pid would have this
    function signal the operator's own terminal.  The canonical ``run-local``
    verb is what separates running the action from talking about it.
    """

    if not action_key:
        return False
    try:
        with open(f"/proc/{pid}/cmdline", "rb") as handle:
            raw = handle.read()
    except OSError:
        return False
    return action_key.encode() in raw and b"run-local" in raw


def find_launcher_pids(action_key: str) -> list[int]:
    """Every live launcher of this action on this box, found from ``/proc``.

    The lease names the process to signal, but only since withdrawal existed: a
    worker started on earlier bytes writes a lease with no ``child_pid``, and a
    cancellation is reached for on a bad night rather than after a fleet roll.
    So the lease is the fast path and this is the one that always works.

    The scan is exact, not a name match.  A launcher's argv carries the 64-hex
    action key *and* the canonical ``run-local`` verb ``worker_argv`` pins, so a
    process with both is a launcher for this action and nothing else is.  The
    verb is required as well as the key so that a withdrawal invoked with the
    full digest cannot match the operator's own command line and signal itself.
    """

    if len(action_key) != 64:
        return []
    needle = action_key.encode()
    mine = os.getpid()
    found: list[int] = []
    for entry in _scan(Path("/proc")):
        if not entry.name.isdigit() or int(entry.name) == mine:
            continue
        try:
            with open(entry / "cmdline", "rb") as handle:
                raw = handle.read()
        except OSError:
            continue          # it exited, or it is not ours to read
        if needle in raw and b"run-local" in raw:
            found.append(int(entry.name))
    return found


def terminate_action(
    launcher_pid: int, *, grace_s: float = WITHDRAW_GRACE_S
) -> dict[str, object]:
    """Stop a running action and everything it started.  Safe when it is gone.

    Three rungs, each with its own reason, and each climbed only when the one
    below went unanswered:

    1. ``SIGTERM`` to the **action's** process group.  This is the operator's
       own manual move, and it is the rung that reaches the work: the launcher
       is not in that group and signalling it does nothing to the pytest.
    2. ``SIGINT`` to the launcher, if it is still alive.  Not ``SIGTERM``:
       Python's default disposition for ``SIGTERM`` kills the interpreter where
       it stands, while ``SIGINT`` raises ``KeyboardInterrupt``, and
       ``core.run_local_action``'s ``except BaseException`` branch then reaps
       its own action group and releases the result lock on the way out.  The
       handled signal is the one that unwinds in order.
    3. ``SIGKILL`` to both, for whatever answers neither.

    Every signal is best effort: a process that has already gone raises
    ``ProcessLookupError``, and that is the successful case, not a failure.
    Which is what makes the whole verb idempotent -- withdrawing twice is two
    lookups and no signals.
    """

    groups = action_process_groups(launcher_pid)
    sent: list[str] = []

    def alive() -> bool:
        return _process_alive(launcher_pid) or any(_process_alive(p) for p in groups)

    def settle(seconds: float) -> bool:
        deadline = _now() + seconds
        while _now() < deadline:
            if not alive():
                return False
            time.sleep(0.05)
        return alive()

    for pgid in groups:
        try:
            os.killpg(pgid, signal.SIGTERM)
            sent.append(f"TERM -{pgid}")
        except OSError:
            pass
    if settle(grace_s / 2.0):
        try:
            os.kill(launcher_pid, signal.SIGINT)
            sent.append(f"INT {launcher_pid}")
        except OSError:
            pass
    if settle(grace_s / 2.0):
        for pgid in groups:
            try:
                os.killpg(pgid, signal.SIGKILL)
                sent.append(f"KILL -{pgid}")
            except OSError:
                pass
        try:
            os.kill(launcher_pid, signal.SIGKILL)
            sent.append(f"KILL {launcher_pid}")
        except OSError:
            pass
    return {
        "launcher_pid": int(launcher_pid),
        "action_pgids": groups,
        "signals": sent,
        "still_alive": alive(),
    }


class _Insufficient(Exception):
    """Internal: a demand could not be met in full."""


class ResourceLedger:
    """Per-host capacity, held as tokens that are acquired by ``rename``.

    Capacity is expressed as *countable* tokens rather than as a number in a
    file that everyone read-modify-writes, because this queue has exactly one
    concurrency primitive it trusts on NFS -- ``rename`` -- and a ledger that
    needed a second one would be a ledger with a second failure mode.  One
    token is one indivisible unit of a resource (``gpu`` is a device, ``mem_gb``
    is a gigabyte), so acquiring is renaming N of them out of ``free/`` and
    releasing is renaming them back.  A worker that dies holding tokens is
    recovered by the same reaper that recovers its claim, since the tokens are
    filed under the action key.

    Capacity is grown but never shrunk here: removing a token that another
    process holds is not expressible as a rename, and a box whose capacity
    dropped mid-flight is a configuration change, not a queue operation.
    """

    def __init__(self, root: str | Path, host: str | None = None) -> None:
        self.root = Path(root)
        self.host = host or socket.gethostname()

    @property
    def base(self) -> Path:
        return self.root / self.host

    @property
    def free_dir(self) -> Path:
        return self.base / "free"

    @property
    def held_dir(self) -> Path:
        return self.base / "held"

    def ensure_capacity(self, capacity: Mapping[str, int]) -> None:
        """Create any missing token of each declared kind, idempotently.

        A token index is present if it is either free or held, so two workers
        declaring the same capacity converge and neither hands back a token the
        other is using.
        """

        self.free_dir.mkdir(parents=True, exist_ok=True)
        self.held_dir.mkdir(parents=True, exist_ok=True)
        for kind, count in sorted(capacity.items()):
            total = int(count)
            if total < 0:
                raise PoolContractError(f"capacity for {kind!r} must not be negative")
            present = {path.name for path in _glob(self.free_dir, f"{kind}-*")}
            for holder in _scan(self.held_dir):
                if holder.is_dir():
                    present.update(path.name for path in _glob(holder, f"{kind}-*"))
            for index in range(total):
                name = f"{kind}-{index:04d}"
                if name in present:
                    continue
                token = self.free_dir / name
                try:
                    descriptor = os.open(token, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
                except FileExistsError:
                    continue
                os.close(descriptor)

    def retire_free_capacity(self, capacity: Mapping[str, int]) -> dict[str, int]:
        """Lower a kind's total to ``capacity`` by deleting FREE tokens only.

        ``ensure_capacity`` is monotonically increasing on purpose -- two
        workers declaring the same box converge, and neither takes back a
        token the other is using.  But a box's honest offer *falls* when work
        arrives that the pool did not schedule, and with only an increasing
        primitive the ledger keeps advertising the high-water mark: a GB10
        offering 96 GB while holding 10 GB free is not admission control, it
        is a promise the box cannot keep.

        Only free tokens are retired, so a running action never loses the
        reservation it is executing under; the total falls as holders finish
        and their tokens are not re-created.  Returns what was retired.
        """

        retired: dict[str, int] = {}
        for kind, count in sorted(capacity.items()):
            target = int(count)
            if target < 0:
                raise PoolContractError(f"capacity for {kind!r} must not be negative")
            free = _glob(self.free_dir, f"{kind}-*")
            held = sum(
                1 for holder in _scan(self.held_dir)
                if holder.is_dir()
                for _ in _glob(holder, f"{kind}-*")
            )
            # Never retire below what is already held: those tokens exist.
            excess = max(0, len(free) + held - target)
            # Retire the HIGHEST-indexed free tokens, not the lowest.
            #
            # ``ensure_capacity`` runs on every claim attempt and fills the
            # slots ``kind-0000 .. kind-{target-1}``, so a retire that ate the
            # low names left a hole the next poll re-minted: a ledger dropped
            # from 96 GB to 40 kept 8 high tokens, and eight of the forty slots
            # below them came straight back.  Measured, not reasoned: gpu 4 ->
            # 1 settled at 2, mem 96 -> 40 settled at 48.  Retiring downward
            # leaves a contiguous prefix, which is exactly the set
            # ``ensure_capacity`` then finds already present.
            #
            # A holder sitting on a high index still causes a partial re-mint,
            # and that is the documented behaviour: the total falls the rest of
            # the way as holders finish and their tokens are not re-created.
            for token in sorted(free, reverse=True)[:excess] if excess else []:
                try:
                    token.unlink()
                    retired[kind] = retired.get(kind, 0) + 1
                except OSError:
                    pass
        return retired

    def capacity(self) -> dict[str, int]:
        """Total tokens of each kind, free or held."""

        counts: dict[str, int] = {}
        for path in _glob(self.free_dir, "*-*"):
            counts[path.name.rsplit("-", 1)[0]] = counts.get(path.name.rsplit("-", 1)[0], 0) + 1
        for holder in _scan(self.held_dir):
            if not holder.is_dir():
                continue
            for path in _glob(holder, "*-*"):
                kind = path.name.rsplit("-", 1)[0]
                counts[kind] = counts.get(kind, 0) + 1
        return counts

    def available(self) -> dict[str, int]:
        """Tokens of each kind not currently held."""

        counts: dict[str, int] = {}
        for path in _glob(self.free_dir, "*-*"):
            kind = path.name.rsplit("-", 1)[0]
            counts[kind] = counts.get(kind, 0) + 1
        return counts

    def acquire(self, action_key: str, demand: Mapping[str, int]) -> bool:
        """Take every token the demand asks for, or none of them.

        All-or-nothing is the repro's third bug stated as code: a multi-resource
        actor that keeps what it managed to get while blocked on what it did not
        is holding resources it cannot use.
        """

        wanted = {k: int(v) for k, v in demand.items() if int(v) > 0}
        if not wanted:
            return True
        destination = self.held_dir / action_key
        destination.mkdir(parents=True, exist_ok=True)
        try:
            for kind, need in sorted(wanted.items()):
                taken = 0
                for token in _glob(self.free_dir, f"{kind}-*"):
                    if taken >= need:
                        break
                    try:
                        os.rename(token, destination / token.name)
                    except (FileNotFoundError, NotADirectoryError):
                        continue      # another worker took it first
                    taken += 1
                if taken < need:
                    raise _Insufficient(kind)
        except _Insufficient:
            self.release(action_key)
            return False
        return True

    def release(self, action_key: str) -> int:
        """Return every token held for this action.  Safe to call twice."""

        destination = self.held_dir / action_key
        if not destination.is_dir():
            return 0
        released = 0
        self.free_dir.mkdir(parents=True, exist_ok=True)
        for token in _scan(destination):
            try:
                os.rename(token, self.free_dir / token.name)
            except OSError:
                continue
            released += 1
        try:
            destination.rmdir()
        except OSError:
            pass
        return released

    def held(self) -> dict[str, int]:
        """Tokens of each kind a running action currently holds on this host.

        This is the pool's own statement of what it is consuming here, and it
        is what ``box_capacity`` subtracts from the box's live load: the set of
        pids under a claimed action is not knowable from a process tree once
        docker or ``setsid`` is involved, but the reservation always is.
        """

        counts: dict[str, int] = {}
        for holder in _scan(self.held_dir):
            if not holder.is_dir():
                continue
            for path in _glob(holder, "*-*"):
                kind = path.name.rsplit("-", 1)[0]
                counts[kind] = counts.get(kind, 0) + 1
        return counts

    def held_keys(self) -> list[str]:
        return sorted(path.name for path in _scan(self.held_dir) if path.is_dir())


class PoolQueue:
    """A directory on a shared filesystem that two or more boxes pull from."""

    def __init__(self, root: str | Path = DEFAULT_POOL_ROOT) -> None:
        self.root = Path(root)
        if not self.root.is_absolute():
            raise PoolContractError("pool root must be absolute")

    # -- layout ---------------------------------------------------------

    def dir(self, state: str) -> Path:
        if state not in _STATES:
            raise PoolContractError(f"unknown queue state: {state!r}")
        return self.root / state

    def item_path(self, state: str, action_key: str) -> Path:
        return self.dir(state) / f"{action_key}.json"

    def lease_path(self, action_key: str) -> Path:
        return self.dir(CLAIMED) / f"{action_key}.lease"

    def ensure_layout(self) -> None:
        for state in _STATES:
            self.dir(state).mkdir(parents=True, exist_ok=True)
        (self.root / WORKERS).mkdir(parents=True, exist_ok=True)

    # -- what the fleet can actually run ---------------------------------

    def announce(
        self,
        *,
        host: str,
        tags: Sequence[str],
        has_gpu: bool,
        capacity: Mapping[str, int] | None = None,
        runtime_commit: str = "",
        observed_capacity: Mapping[str, int] | None = None,
        foreign: Mapping[str, int] | None = None,
        observed_detail: Mapping[str, object] | None = None,
    ) -> None:
        """Record what this worker offers, so a submitter can be told the truth.

        Without this the queue knows what work has been asked for and nothing
        at all about what the fleet can do, so an item whose required tags no
        box offers is indistinguishable from an item whose box is merely busy:
        it sits in ``ready``, reported as pending, while every worker polls
        past it forever.  That is not hypothetical -- a suite submitted with
        ``--tag dl380`` waited ten minutes in front of fifteen idle workers
        that offer ``x86``, and would have waited a day.

        The offer is a *claim about this box, refreshed by this box*, and it
        expires; a stale file is not evidence.  Nothing consumes it for
        scheduling -- placement is still decided by the matching in
        ``claim()`` -- so a wrong or missing offer costs a diagnostic, never a
        misplacement.

        ``capacity`` is what this box is *configured* to offer and is the field
        ``placeable`` reads, because the question a submitter asks is "can any
        box ever run this", not "is a box free this second".
        ``observed_capacity`` is what the box could honestly take right now,
        with work the pool did not schedule subtracted (see
        ``prismabuild.box_capacity``), and ``foreign`` and ``observed_detail``
        say by how much and on what evidence.  The live figure is what the
        ledger is retired to; it is deliberately *not* what ``placeable``
        reads, because a box busy with someone else's work is a slow
        submission, and answering ``False`` there would turn it into a refused
        one.  ``observed_capacity`` is the *windowed* offer, so in the falling
        direction it can lag ``foreign`` by up to ``--observe-samples`` polls:
        a record reading ``observed_capacity {'gpu': 1}`` beside ``foreign
        {'gpu': 2}`` is a box that has seen the foreign work and has not yet
        agreed with itself about it.  The lag is deliberate while a loop is
        polling, and was a hole at the end of an action, where a window full of
        pre-action readings re-minted retired tokens; ``CapacityObserver.
        rejoin`` empties the window there, so a record written on the first
        poll after an action carries no reading older than that poll.  Older
        workers announce none of the three, and a reader must treat their
        absence as "not measured" rather than as zero.
        """

        record = {
            "schema": POOL_OFFER_SCHEMA_V1,
            "host": host,
            "tags": sorted({str(t) for t in tags}),
            "has_gpu": bool(has_gpu),
            "capacity": {str(k): int(v) for k, v in (capacity or {}).items()},
            # What the box can honestly take right now, and the readings
            # behind it.  Additive, optional fields on the same schema: no
            # reader validates the offer record against a field list, and a
            # bump would only invalidate every offer a running loop had
            # already written.
            "observed_capacity": {
                str(k): int(v) for k, v in (observed_capacity or {}).items()},
            "foreign": {str(k): int(v) for k, v in (foreign or {}).items()},
            "observed_detail": dict(observed_detail or {}),
            # Which published bytes are answering for this box.  A loop holds
            # the module it imported at start for its whole life, so without
            # this a fleet running four generations of the code at once looks
            # uniform from the queue.
            "runtime_commit": str(runtime_commit),
            "announced_unix": _now(),
        }
        directory = self.root / WORKERS
        directory.mkdir(parents=True, exist_ok=True)
        _write_json_atomic(directory / f"{host}.json", record)

    def offers(self, *, max_age_s: float = OFFER_TIMEOUT_S) -> list[dict[str, object]]:
        """Every worker offer still fresh enough to believe."""

        directory = self.root / WORKERS
        if not directory.is_dir():
            return []
        now = _now()
        live: list[dict[str, object]] = []
        for path in sorted(directory.glob("*.json")):
            record = _read_json(path)
            if record is None:
                continue
            announced = record.get("announced_unix")
            if not isinstance(announced, (int, float)):
                continue
            if now - float(announced) <= max_age_s:
                live.append(record)
        return live

    def _matching_offers(
        self, item: Mapping[str, object], *, live: Sequence[Mapping[str, object]]
    ) -> list[Mapping[str, object]]:
        """The offers among ``live`` that could run ``item``.

        One matcher, several readers: "can this run at all", "on how many
        boxes", and "which boxes" are the same question asked three ways, and
        a second copy of the rule would be a way for the answers to disagree.
        """

        required = item.get("tags") or []
        if not isinstance(required, list):
            raise PoolContractError("pool item tags must be a list")
        wanted = {str(t) for t in required}
        demand = self.demand_of(item)
        needs_gpu = bool(item.get("needs_gpu")) or demand.get("gpu", 0) > 0
        matches: list[Mapping[str, object]] = []
        for offer in live:
            tags = {str(t) for t in (offer.get("tags") or [])}
            if not wanted.issubset(tags):
                continue
            if needs_gpu and not offer.get("has_gpu"):
                continue
            capacity = offer.get("capacity") or {}
            # A kind the offer does not MENTION is unknown, not zero.  The
            # difference is what a publish looks like from the queue: capacity
            # gains a kind (``cpu``, on 2026-09-04), the offer file is one
            # last-writer-wins record per host, and loops of both generations
            # write it -- so sparky's offer alternated between
            # ``{"gpu": 2, "mem_gb": 48}`` and ``{"cpu": 10, "gpu": 2,
            # "mem_gb": 48}``, 32 and 28 samples of 60 taken one second apart.
            # Read as zero, the older record makes every action carrying the
            # new ``cpu=1`` default unplaceable on a box that plainly runs it:
            # 17 of 60 identical queries answered "no live worker can run this
            # action" for a box whose offer was one to eight seconds old.
            # Refuse on what a box says it cannot fit; never on what it did
            # not say.
            if isinstance(capacity, Mapping) and any(
                int(capacity[kind]) < need
                for kind, need in demand.items() if kind in capacity
            ):
                continue          # this box can never fit it, however idle
            matches.append(offer)
        return matches

    def placeable(
        self, item: Mapping[str, object], *, max_age_s: float = OFFER_TIMEOUT_S
    ) -> bool | None:
        """Can any live worker run this item?  ``None`` means nobody has said.

        Answered from the *declared* capacity, never the observed one.  The
        question is capability -- "this box can never fit it, however idle" --
        and a box temporarily occupied by work the pool did not schedule is
        idle-in-the-future, not incapable.  Reading the live figure here would
        make a busy fleet refuse the submission outright (``pbrun`` raises on
        ``False``) instead of queueing it.

        The three-valued answer is deliberate.  ``False`` is a fact worth
        refusing a submission over; but an empty registry means only that no
        worker has announced yet -- a fleet running loops that predate this
        code, or a queue whose workers are down -- and refusing on *that*
        would turn a missing diagnostic into a broken submit path.  Unknown
        stays unknown.
        """

        live = self.offers(max_age_s=max_age_s)
        if not live:
            return None
        return bool(self._matching_offers(item, live=live))

    def placeable_hosts(
        self, item: Mapping[str, object], *, max_age_s: float = OFFER_TIMEOUT_S
    ) -> list[str] | None:
        """Which live boxes could run this item.  ``None`` means nobody has said.

        The width of an item -- how many boxes it can land on -- is the number
        this fleet had no way to ask for.  A queue that reports only "pending"
        makes an item pinned to one busy box look exactly like an item waiting
        its turn among three, and on 2026-09-03/04 that difference was the
        whole problem: 131 of 391 items carried a hostname tag -- 129 of
        those a consequence of a box-local path, 114 of them pinning
        ``sparky`` from a ``/home/rob/tmp/ts*`` worktree -- while other boxes
        idled.

        A nameless offer is reported as ``"?"`` rather than dropped: it still
        matched, so dropping it would make ``placeable_hosts`` disagree with
        ``placeable`` about whether anything can run the item at all.
        """

        live = self.offers(max_age_s=max_age_s)
        if not live:
            return None
        return sorted({
            str(offer.get("host") or "?")
            for offer in self._matching_offers(item, live=live)
        })

    def placement_census(
        self, *, max_age_s: float = OFFER_TIMEOUT_S
    ) -> dict[str, object]:
        """How wide the waiting queue is, bucketed by how many boxes fit each item.

        "Placeable on exactly one box" is the number worth watching: it is the
        fleet's depth-vs-width, and it was previously obtainable only by
        reading the queue by hand.  ``pinned_to`` names the boxes those items
        are waiting on, because *which* box is queueing is what tells a
        person whether the pin is the reason the fleet looks busy.

        Width is the number of boxes that can RUN an item, which is not the
        number whose tags match it.  A ``checkout_root`` outside
        ``/mnt/shared`` exists on exactly one box, so it caps the width at one
        however many boxes the tags allow -- and ``one_box_by_path`` counts
        the items where the path is what did the capping, because that is the
        number commit-addressed checkouts are meant to drive down and the
        only one that says the migration is working rather than that a box
        went away.

        ``known`` is false when no worker has announced.  The buckets are then
        zero and mean nothing -- the same unknown-stays-unknown rule
        ``placeable`` follows, kept as a field rather than as three ``None``s
        so a printer can read one flag.  ``unreadable`` counts ready records
        this cannot price at all; see the handler below for why it is a count
        and not an exception.
        """

        live = self.offers(max_age_s=max_age_s)
        ready = self.ready_items()
        census: dict[str, object] = {
            "ready": len(ready),
            "offers": len(live),
            "known": bool(live),
            "unplaceable": 0,
            "one_box": 0,
            "one_box_by_path": 0,
            "wide": 0,
            "unreadable": 0,
            "pinned_to": {},
        }
        if not live:
            return census
        pinned: dict[str, int] = {}
        for item in ready:
            # A census is a diagnostic, and a diagnostic must never be the
            # thing that fails.  ``claim`` skips an item tagged for another
            # box at ``_placement_matches``, before ``demand_of`` is reached,
            # so a record with a non-Mapping ``resources`` was harmless to
            # every existing reader; counting it here made one out-of-band
            # write able to raise on every box in the fleet.  Count it and
            # move on -- and report the count, because an item nothing can
            # read is a fact about the queue, not a rounding error.
            try:
                hosts = sorted({
                    str(offer.get("host") or "?")
                    for offer in self._matching_offers(item, live=live)
                })
            except (PoolContractError, ValueError, TypeError):
                census["unreadable"] = int(census["unreadable"]) + 1
                continue
            # Tags say which boxes are ALLOWED to claim it; the checkout says
            # which box can actually run it.  A box-local ``checkout_root``
            # exists on exactly one box, so it caps the width at one however
            # many boxes the tags match -- and without this cap the metric
            # under-reported the very pin it exists to report: an action
            # tagged ``gb10`` over a ``/home/rob/tmp/ts101`` worktree matches
            # two boxes and can run on one, and counted as ``wide``.
            by_path = is_box_local_path(item.get("checkout_root"))
            if not hosts:
                census["unplaceable"] = int(census["unplaceable"]) + 1
                continue
            if not (by_path or len(hosts) == 1):
                census["wide"] = int(census["wide"]) + 1
                continue
            census["one_box"] = int(census["one_box"]) + 1
            if by_path:
                census["one_box_by_path"] = int(census["one_box_by_path"]) + 1
            # Which box: the tags when they answer alone, otherwise the box
            # that published it, which is the box whose tree it is.  When
            # neither answers -- a box-local item whose publisher is not
            # among the boxes its tags match -- the item is still one box
            # wide and simply goes unattributed, so ``pinned_to`` may sum to
            # less than ``one_box``.  A name we cannot prove is worse than a
            # missing one.
            holder = hosts[0] if len(hosts) == 1 else None
            if holder is None:
                published_by = str(item.get("published_by") or "")
                holder = published_by if published_by in hosts else None
            if holder is not None:
                pinned[holder] = pinned.get(holder, 0) + 1
        census["pinned_to"] = dict(sorted(pinned.items()))
        return census

    def offered_tags(self, *, max_age_s: float = OFFER_TIMEOUT_S) -> list[str]:
        """Every tag some live worker offers -- what to print when nothing fits."""

        seen: set[str] = set()
        for offer in self.offers(max_age_s=max_age_s):
            seen.update(str(t) for t in (offer.get("tags") or []))
        return sorted(seen)

    # -- producer -------------------------------------------------------

    def publish(
        self,
        *,
        action_key: str,
        cas_root: str | Path,
        checkout_root: str | Path,
        worker_script: str | Path,
        tags: Sequence[str] = (),
        needs_gpu: bool = False,
        priority: int = 0,
        resources: Mapping[str, int] | None = None,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    ) -> Path:
        """Enqueue one sealed action.  The action itself already lives in the CAS.

        ``resources`` is what this action needs to run on one box -- e.g.
        ``{"gpu": 1, "mem_gb": 8}``.  It is a claim about the action, made by
        the producer that knows it; a worker's ``capacity`` is the matching
        claim about the box.  Omitting it means the action is admitted on
        placement alone, which is the pre-ledger behaviour.
        """

        if not isinstance(action_key, str) or len(action_key) != 64:
            raise PoolContractError("action_key must be a 64-character digest")
        demand = {str(k): int(v) for k, v in dict(resources or {}).items()}
        if any(v < 0 for v in demand.values()):
            raise PoolContractError("resource demand must not be negative")
        if int(max_attempts) < 1:
            raise PoolContractError("max_attempts must be at least 1")
        self.ensure_layout()
        # A submission is what retires a withdrawal.  The key is a content
        # hash -- ``result_and_stamp_names`` says so: *"the same command at the
        # same commit still fingerprints identically"* -- so re-submitting one
        # is the normal way to ask for the same work again, not an attempt to
        # defeat somebody's cancellation.  Treating the marker as a permanent
        # blacklist on the key meant the re-submitted record was deleted by
        # ``claim``'s guard and ``pbrun`` answered the new run with the old
        # run's ``withdrawn_by``, at exit 143, with nothing filed anywhere; the
        # only remedy was ``rm withdrawn/<key>.json`` on the live queue, which
        # is the hand edit this whole verb exists to remove.  The decision is
        # kept -- moved to ``superseded/``, not deleted -- and the new item
        # carries what it revived.
        superseded = self._supersede_withdrawal(action_key)
        item = {
            "schema": POOL_ITEM_SCHEMA_V1,
            "action_key": action_key,
            "cas_root": str(cas_root),
            "checkout_root": str(checkout_root),
            "worker_script": str(worker_script),
            "tags": sorted(str(t) for t in tags),
            "needs_gpu": bool(needs_gpu),
            "priority": int(priority),
            "resources": demand,
            "attempts": 0,
            "max_attempts": int(max_attempts),
            "published_unix": _now(),
            "published_by": socket.gethostname(),
        }
        if superseded is not None:
            item["supersedes_withdrawal"] = {
                "withdrawn_unix": superseded.get("withdrawn_unix"),
                "withdrawn_by": superseded.get("withdrawn_by"),
                "withdrawn_host": superseded.get("withdrawn_host"),
                "reason": superseded.get("reason"),
            }
        path = self.item_path(READY, action_key)
        _write_json_atomic(path, item)
        return path

    # -- consumer -------------------------------------------------------

    def _placement_matches(
        self, item: Mapping[str, object], *, tags: frozenset[str], has_gpu: bool
    ) -> bool:
        if item.get("needs_gpu") and not has_gpu:
            return False
        required = item.get("tags") or []
        if not isinstance(required, list):
            raise PoolContractError("pool item tags must be a list")
        return all(str(t) in tags for t in required)

    def ready_items(self) -> list[dict[str, object]]:
        out: list[dict[str, object]] = []
        ready = self.dir(READY)
        if not ready.is_dir():
            return out
        for path in sorted(ready.glob("*.json")):
            record = _read_json(path)
            if record is not None:
                record["passes"] = self.passes(str(record.get("action_key", "")))
                out.append(record)
        # Aging first, then priority, then oldest.  An item that has been denied
        # admission repeatedly is not merely unlucky -- it is being overtaken --
        # so its denial count outranks the band it was published in.  Within a
        # band a long queue still drains in the order it was filled rather than
        # by digest.  Not a scheduler; a tie-break predictable enough to debug.
        out.sort(
            key=lambda r: (
                -int(r.get("passes", 0)),
                -int(r.get("priority", 0)),
                float(r.get("published_unix", 0.0)),
            )
        )
        return out

    # -- aging ----------------------------------------------------------

    def passes_path(self, action_key: str) -> Path:
        return self.root / PASSES / f"{action_key}.json"

    def passes(self, action_key: str) -> int:
        record = _read_json(self.passes_path(action_key))
        if record is None:
            return 0
        value = record.get("passes", 0)
        return int(value) if isinstance(value, (int, float)) else 0

    def record_pass(self, action_key: str) -> int:
        """Count one admission denial.

        Kept in a sidecar rather than in the item, because rewriting a ready
        item races the claim that may already have moved it: the writer would
        resurrect a claimed action into ``ready`` and hand it to a second
        worker.  A lost increment under contention costs a little ordering
        fairness; a resurrected item costs correctness.
        """

        now = _now()
        # ``first_unix`` is set once and carried forward: it is the clock the
        # withhold ceiling reads, so it must measure the age of the *block*,
        # not the age of the most recent denial.
        prior = _read_json(self.passes_path(action_key)) or {}
        first = prior.get("first_unix")
        if not isinstance(first, (int, float)):
            first = now
        count = self.passes(action_key) + 1
        _write_json_atomic(
            self.passes_path(action_key),
            {"action_key": action_key, "passes": count,
             "first_unix": float(first), "updated_unix": now},
        )
        return count

    def withhold_age(self, action_key: str) -> float:
        """Seconds since this item was first denied admission; 0.0 if never."""

        record = _read_json(self.passes_path(action_key)) or {}
        first = record.get("first_unix")
        if not isinstance(first, (int, float)):
            return 0.0
        return max(0.0, _now() - float(first))

    def _write_claim_intent(self, action_key: str, *, owner: str) -> None:
        _write_json_atomic(
            self.item_path(INTENT, action_key),
            {
                "schema": POOL_CLAIM_INTENT_SCHEMA_V1,
                "action_key": action_key,
                "owner": owner,
                "host": socket.gethostname(),
                "pid": os.getpid(),
                "intent_unix": _now(),
            },
        )

    def write_lease(
        self, action_key: str, *, owner: str, child_pid: int | None = None
    ) -> None:
        """Refresh the claim's heartbeat, and say what is running under it.

        ``pid`` is this *loop's* pid and always has been.  It is not the process
        that runs the action, and signalling it would kill the worker rather
        than the work, so it cannot be what a cancellation aims at.
        ``child_pid`` is the launcher ``execute`` started, which is one lookup
        away from the action's own process group -- see
        ``action_process_groups``.  It is ``None`` in the lease ``claim``
        writes, because at that moment nothing is running yet.
        """

        _write_json_atomic(
            self.lease_path(action_key),
            {
                "schema": POOL_LEASE_SCHEMA_V1,
                "action_key": action_key,
                "owner": owner,
                "host": socket.gethostname(),
                "pid": os.getpid(),
                "child_pid": int(child_pid) if child_pid is not None else None,
                "heartbeat_unix": _now(),
            },
        )

    def ledger(self, host: str | None = None) -> ResourceLedger:
        return ResourceLedger(self.root / RESERVATIONS, host=host)

    @staticmethod
    def demand_of(item: Mapping[str, object]) -> dict[str, int]:
        raw = item.get("resources") or {}
        if not isinstance(raw, Mapping):
            raise PoolContractError("pool item resources must be an object")
        return {str(k): int(v) for k, v in raw.items() if int(v) > 0}

    def claim(
        self,
        *,
        tags: Iterable[str] = (),
        has_gpu: bool = False,
        owner: str | None = None,
        capacity: Mapping[str, int] | None = None,
    ) -> dict[str, object] | None:
        """Take one ready item, atomically.  ``None`` when nothing matches.

        The claim IS the ``rename``.  Two workers racing the same item both call
        it; exactly one succeeds and the loser sees ``FileNotFoundError`` and
        moves on.  Nothing else in this method may fail in a way that leaves the
        item in neither directory.

        When ``capacity`` is given, admission runs *before* the rename and the
        tokens are released again if the rename is lost -- so a worker never
        holds capacity it is not about to use, and never waits while holding.
        A starved item (``passes >= STARVATION_FLOOR``) that this host could
        eventually fit withholds the host rather than being overtaken; one it
        could never fit is skipped, because withholding a box for work that
        will never run there is the deadlock, not the fix.

        ``capacity`` is the box's offer *now*, not its configuration -- a
        worker clamps it to what work the pool did not schedule has left free
        (``prismabuild.box_capacity``).  The never-fits test above reads the
        ledger's *total*, and the retire deletes free tokens only, so the two
        cases part on whether anything is holding: a kind the clamp has taken
        to zero with no holder drops the total under the demand and the item is
        skipped, recording no ``passes`` on a box that cannot presently run it;
        a kind whose holders keep the total at or above the demand is denied
        and aged exactly as before, since from the item's side that is an
        ordinary busy box.  Either way it unwinds by itself, because the offer
        recovers as soon as the foreign work exits.
        """

        owner = owner or f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
        tagset = frozenset(str(t) for t in tags)
        self.ensure_layout()
        # The load-bearing half of ``withdraw``.  A withdrawal that lands while
        # a worker is mid-``finish`` can leave a requeued ready record behind
        # it, and without this guard that record is claimed and the cancelled
        # work runs again -- which is the race the operator used to have to win
        # by hand.  Read once per scan, not once per item.
        withdrawn = self.withdrawn_keys()
        ledger = None
        total: dict[str, int] = {}
        if capacity is not None:
            ledger = self.ledger()
            ledger.ensure_capacity(capacity)
            total = ledger.capacity()
        for item in self.ready_items():
            key = str(item.get("action_key", ""))
            if not key or not self._placement_matches(item, tags=tagset, has_gpu=has_gpu):
                continue
            if key in withdrawn and self.withdrawal_covers(
                    item, action_key=key, withdrawn=withdrawn) is not None:
                # Already filed under ``withdrawn``, and of the generation that
                # was withdrawn: this record is the losing half of a race, not
                # work.  Drop it rather than leave it at the head of ``ready``
                # for every future poll to step over -- but FILE it first.  A
                # queue that removes a record it will not run and says nothing
                # anywhere is the shape ``quarantine_orphans`` names in its own
                # docstring, and the reason this guard was a blocker.
                #
                # A record of a LATER generation falls through and is claimed:
                # somebody asked for this work again after the cancellation,
                # which a content-addressed key makes the ordinary way to ask.
                self._file_superseded(
                    item, key=key, kind="dropped", status="dropped",
                    dropped_unix=_now(), dropped_host=socket.gethostname(),
                    reason="requeued into ready after the withdrawal that "
                           "cancelled this generation",
                )
                self.item_path(READY, key).unlink(missing_ok=True)
                continue
            demand = self.demand_of(item)
            if ledger is not None and demand:
                if any(total.get(kind, 0) < need for kind, need in demand.items()):
                    continue      # never fits this box; not this box's to hold
                if not ledger.acquire(key, demand):
                    denials = self.record_pass(key)
                    if (denials >= STARVATION_FLOOR
                            and self.withhold_age(key) <= WITHHOLD_CEILING_S):
                        # Wired to the decision: stop letting smaller work pass it.
                        return None
                    # Past the ceiling it keeps its passes -- and so its place at
                    # the head of the ordering -- but stops holding the box shut
                    # for work it cannot do anything with.
                    continue
            # Intent precedes the claim, so a crash in between leaves evidence.
            self._write_claim_intent(key, owner=owner)
            src = self.item_path(READY, key)
            dst = self.item_path(CLAIMED, key)
            try:
                os.rename(src, dst)
            except (FileNotFoundError, NotADirectoryError):
                if ledger is not None:
                    ledger.release(key)   # lost the race: hold nothing
                continue
            if self.withdrawal_covers(item, action_key=key) is not None:
                # Withdrawn between the scan above and this rename.  The window
                # is microseconds wide and closing it here costs one listing on
                # a path taken once per claim; leaving it open costs a cancelled
                # action a full run before ``execute`` notices.
                if ledger is not None:
                    ledger.release(key)
                self._file_superseded(
                    item, key=key, kind="dropped", status="dropped",
                    dropped_unix=_now(), dropped_host=socket.gethostname(),
                    reason="withdrawn between the ready scan and the claim",
                )
                dst.unlink(missing_ok=True)
                continue
            claimed = dict(item)
            claimed["claimed_by"] = owner
            claimed["claimed_unix"] = _now()
            claimed["claimed_host"] = socket.gethostname()
            claimed["reserved_on"] = socket.gethostname() if demand else None
            _write_json_atomic(dst, claimed)
            self.write_lease(key, owner=owner)
            self.passes_path(key).unlink(missing_ok=True)
            return claimed
        return None

    # -- self-healing ---------------------------------------------------

    def lease_age(self, action_key: str) -> float | None:
        record = _read_json(self.lease_path(action_key))
        if record is None:
            return None
        beat = record.get("heartbeat_unix")
        if not isinstance(beat, (int, float)):
            raise PoolContractError(f"lease has no heartbeat: {action_key}")
        return _now() - float(beat)

    def reap_stale(self, *, timeout_s: float = LEASE_TIMEOUT_S) -> list[str]:
        """Return claims whose lease has expired to ``ready``.

        A missing lease file also counts as stale: it means the claimant died
        between the rename and the first heartbeat.  Requeueing is safe at any
        time because re-execution hits the CAS, so the worst case of reaping a
        live-but-stalled worker is duplicated work, never a corrupted result.

        **The claim is not atomic with its lease.**  ``claim()`` renames the
        item, then writes the lease; a reaper running inside that window sees a
        claimed item with no lease and would requeue a worker that is alive and
        about to start.  So a missing lease is only stale once the claim itself
        has aged past ``grace_s`` -- and ``claimed_unix`` is written into the
        claim record *before* the lease exists, which is what makes it a usable
        clock here.  A genuinely dead claimant still gets reaped, one grace
        period later.  The default grace is the heartbeat interval: longer than
        the microseconds the window actually spans, far shorter than the lease
        timeout that governs the normal case.
        """

        grace_s = HEARTBEAT_S
        requeued: list[str] = []
        claimed = self.dir(CLAIMED)
        if not claimed.is_dir():
            return requeued
        for path in sorted(claimed.glob("*.json")):
            key = path.stem
            age = self.lease_age(key)
            if age is not None and age <= timeout_s:
                continue
            if age is None:
                record = _read_json(path) or {}
                claimed_unix = record.get("claimed_unix")
                if isinstance(claimed_unix, (int, float)):
                    if _now() - float(claimed_unix) <= grace_s:
                        continue          # claimed moments ago; lease imminent
            record = _read_json(path)
            if record is None:
                # The claim concluded under us.  Both ``finish()`` and this
                # loop write the item's next home and only then unlink the
                # claimed file, so a reaper that globbed before that unlink
                # reads nothing back here -- and two reapers on two boxes race
                # each other for exactly this window.  Treating the absence as
                # an empty record and requeueing it writes a stub with no
                # ``action_key`` over whatever the winner just filed, and
                # ``claim()`` skips a keyless item forever: the item never
                # runs, never fails, and sits at the head of ``ready`` denying
                # every worker that polls past it.  There is nothing to reap --
                # the winner filed the item and released its capacity -- so the
                # loser's only correct move is to leave it alone.
                continue
            if self.withdrawal_covers(record, action_key=key) is not None:
                # A withdrawal that could not finish its own cleanup -- the
                # operator's box died mid-verb, say -- leaves a claimed record
                # whose lease nobody refreshes.  Requeueing that is the one
                # thing withdrawal exists to prevent, so conclude it here
                # instead: capacity back, records gone, nothing counted as
                # reaped because nothing was returned to the pool.
                holder = record.get("claimed_host")
                self.ledger(holder if isinstance(holder, str) else None).release(key)
                path.unlink(missing_ok=True)
                self.lease_path(key).unlink(missing_ok=True)
                continue
            # Read the holder's identity BEFORE the requeue branch strips it.
            # The reaper is frequently NOT the dead claimant's box, and its
            # tokens live under the claimant's ledger, not the reaper's.
            holder = record.get("claimed_host")
            # The filename is the identity; a record that disagrees with it, or
            # has lost it, must not be written back to a queue directory where
            # every consumer addresses items by key.
            record["action_key"] = key
            attempts = int(record.get("attempts", 0)) + 1
            limit = int(record.get("max_attempts", DEFAULT_MAX_ATTEMPTS))
            if attempts >= limit:
                # Exhausted: a claim whose lease keeps dying is not made healthy
                # by a fourth box trying it.  Record it terminally instead.
                record.update(
                    {
                        "schema": POOL_OUTCOME_SCHEMA_V1,
                        "status": "lease_lost_max_attempts",
                        "attempts": attempts,
                        "finished_unix": _now(),
                        "finished_host": socket.gethostname(),
                    }
                )
                _write_json_atomic(self.item_path(FAILED, key), record)
                path.unlink(missing_ok=True)
            else:
                record["attempts"] = attempts
                record["requeued_unix"] = _now()
                for transient in ("claimed_by", "claimed_unix", "claimed_host"):
                    record.pop(transient, None)
                try:
                    _write_json_atomic(self.item_path(READY, key), record)
                except OSError:
                    continue
                path.unlink(missing_ok=True)
            # Whatever the outcome, the dead claimant's capacity goes back.  A
            # reservation outliving its holder is the starvation bug's shape.
            self.ledger(holder if isinstance(holder, str) else None).release(key)
            self.lease_path(key).unlink(missing_ok=True)
            requeued.append(key)
        self.sweep_widowed_leases(timeout_s=timeout_s)
        self.quarantine_orphans()
        return requeued

    def sweep_widowed_leases(self, *, timeout_s: float = LEASE_TIMEOUT_S) -> list[str]:
        """Remove leases in ``claimed/`` whose item record is gone.

        Both cleanup paths unlink the lease beside the record they conclude --
        ``finish`` on every branch, ``reap_stale`` on every outcome -- so a
        lease with no record should not exist.  One did: ``daf08495c8bb`` sat
        in the live queue for seven and a half hours, its pid long dead, with
        no ``.json`` beside it and no mechanism that would ever look at it
        again.  How it was widowed is not established, and this sweep is not a
        theory about that; it is the observation that nothing swept it.

        It reads as live work to anything counting ``claimed/``, which is what
        an operator reads when asking whether the fleet is busy, and it is the
        one shape ``quarantine_orphans`` does not cover -- that sweep is over
        ``ready``, this one is its mirror.

        Aged past ``timeout_s`` before removal, for the same reason
        ``reap_stale`` waits: ``claim()`` writes the lease *after* the rename,
        so a lease that briefly has no record beside it may simply be a claim
        mid-flight in the other direction.  Any tokens still held under the key
        go back, because a reservation outliving its holder is the starvation
        bug's shape.
        """

        swept: list[str] = []
        claimed = self.dir(CLAIMED)
        if not claimed.is_dir():
            return swept
        now = _now()
        for lease in sorted(claimed.glob("*.lease")):
            key = lease.name[: -len(".lease")]
            if self.item_path(CLAIMED, key).exists():
                continue
            record = _read_json(lease) or {}
            beat = record.get("heartbeat_unix")
            try:
                age = now - float(beat)
            except (TypeError, ValueError):
                try:
                    age = now - lease.stat().st_mtime
                except OSError:
                    continue
            if age <= timeout_s:
                continue
            host = record.get("host")
            self.ledger(str(host) if isinstance(host, str) else None).release(key)
            lease.unlink(missing_ok=True)
            swept.append(key)
        return swept

    def quarantine_orphans(self) -> list[str]:
        """File ready records that no consumer can address.

        ``claim()`` addresses an item by ``action_key`` and skips a record that
        has none, so such a record never runs, never fails, and never leaves
        ``ready``: the queue reports work it will not do, and the work it
        stands for is lost in silence.  The reaper race above is one way to
        make one, and a worker still running the pre-fix code is another, so
        the sweep stays whether or not that race can still fire.  Filing them
        is the point -- a countable ``orphaned_stub`` in ``failed`` is a
        defect someone can see; a permanent resident of ``ready`` is not.
        """

        filed: list[str] = []
        ready = self.dir(READY)
        if not ready.is_dir():
            return filed
        for path in sorted(ready.glob("*.json")):
            record = _read_json(path)
            if record is None:
                continue
            # Two ways to be unaddressable, and both belong here.  A record
            # with the wrong (or no) ``action_key`` is skipped by ``claim()``
            # and never runs.  A record that *has* the key but lacks the
            # fields a worker executes with -- ``worker_script``, ``cas_root``,
            # ``checkout_root`` -- is worse: it is claimed, it kills the
            # worker process on ``KeyError``, and it does that three times
            # before it is finally filed.  Seven such items are in the live
            # queue's ``failed`` directory, each having taken a worker down.
            usable = (record.get("action_key") == path.stem
                      and all(record.get(field) for field in
                              ("worker_script", "cas_root", "checkout_root")))
            if usable:
                continue
            record.update(
                {
                    "schema": POOL_OUTCOME_SCHEMA_V1,
                    "action_key": path.stem,
                    "status": "orphaned_stub",
                    "finished_unix": _now(),
                    "finished_host": socket.gethostname(),
                    "detail": {
                        "reason": "ready record is not executable: it lacks a "
                        "matching action_key or the worker_script/cas_root/"
                        "checkout_root a worker runs from; see the reap_stale "
                        "and finish() requeue races",
                    },
                }
            )
            _write_json_atomic(self.item_path(FAILED, path.stem), record)
            path.unlink(missing_ok=True)
            self.ledger(None).release(path.stem)
            filed.append(path.stem)
        return filed

    # -- terminal states ------------------------------------------------

    def finish(
        self,
        action_key: str,
        *,
        status: str,
        detail: Mapping[str, object] | None = None,
    ) -> Path:
        succeeded = status in {"executed", "cache_hit"}
        src = self.item_path(CLAIMED, action_key)
        record = _read_json(src)
        if self.withdrawal_covers(record, action_key=action_key) is not None:
            # An operator cancelled this while it was running.  Filing it under
            # ``done`` or ``failed`` would put the pool's opinion of the work on
            # top of a decision about it, and routing it back to ``ready`` --
            # the retry branch below -- would restart exactly what was
            # cancelled.  That restart is the race a hand-edited
            # ``max_attempts`` was trying to lose.  The withdrawal record is
            # already filed; all that is left here is the cleanup ``finish``
            # would otherwise do on its way past.
            #
            # Read AFTER the record, not before it: a withdrawal that lands
            # between the read and the write must still be seen, and this is
            # the last moment at which it can be.
            #
            # Generation-scoped like every other guard: a marker left over from
            # a cancellation the operator has since re-submitted past must not
            # swallow the NEW run's outcome, which would file it nowhere at
            # all.  ``record is None`` is the one case with no generation to
            # compare, and is treated as covered -- the claim was concluded by
            # somebody else, so there is nothing here to file either way.
            host = (record or {}).get("claimed_host")
            self.ledger(str(host) if isinstance(host, str) else None).release(action_key)
            src.unlink(missing_ok=True)
            self.lease_path(action_key).unlink(missing_ok=True)
            return self.item_path(WITHDRAWN, action_key)
        if record is None:
            # A reaper concluded this claim while the work was still running,
            # so the claim file is gone and the item has already been filed
            # somewhere by the winner.  Synthesising ``{"action_key": key}``
            # here and letting the code below requeue it publishes a record
            # that has the key and nothing else -- no ``worker_script``, no
            # ``cas_root``, no ``checkout_root`` -- *over* the full record the
            # reaper just wrote.  The action can then never run again: every
            # subsequent claim dies on ``KeyError('worker_script')``, and the
            # only copy of where the work lived is gone.  Six actions in the
            # live queue are unrecoverable for exactly this reason.
            #
            # This is the twin of the ``reap_stale`` race fixed in 8b32569 --
            # the same missing-read-treated-as-empty-record on the other side
            # of the same window; that fix's own comment names ``finish()``
            # and only the loop was repaired.  File the outcome terminally so
            # it is countable, and never route it back to ``ready``.
            lost = self.item_path(
                FAILED, action_key) if not succeeded else self.item_path(
                DONE, action_key)
            if not lost.exists():
                _write_json_atomic(lost, {
                    "schema": POOL_OUTCOME_SCHEMA_V1,
                    "action_key": action_key,
                    "status": status if succeeded else "finish_lost_race",
                    "finished_unix": _now(),
                    "finished_host": socket.gethostname(),
                    "detail": {
                        "reason": "the claim was concluded by a reaper while "
                                  "this worker was still running it; the "
                                  "item's own record was not available to "
                                  "carry forward",
                        "worker_detail": dict(detail or {}),
                    },
                })
            self.lease_path(action_key).unlink(missing_ok=True)
            return lost
        host = record.get("claimed_host")
        attempts = int(record.get("attempts", 0)) + 1
        limit = int(record.get("max_attempts", DEFAULT_MAX_ATTEMPTS))
        record.update(
            {
                "schema": POOL_OUTCOME_SCHEMA_V1,
                "status": status,
                "attempts": attempts,
                "finished_unix": _now(),
                "finished_host": socket.gethostname(),
                "detail": dict(detail or {}),
            }
        )
        # Capacity is released before the item is filed, so the next worker to
        # look sees the tokens free rather than racing this rename.
        self.ledger(str(host) if isinstance(host, str) else None).release(action_key)
        if succeeded or attempts >= limit:
            dst = self.item_path(DONE if succeeded else FAILED, action_key)
        else:
            # Retry is cheap by construction: a re-run of work that did land is
            # a CAS receipt lookup, so the only thing another attempt can cost
            # is the attempt.  Failing once is not evidence the action is bad.
            record["requeued_unix"] = _now()
            for transient in ("claimed_by", "claimed_unix", "claimed_host"):
                record.pop(transient, None)
            dst = self.item_path(READY, action_key)
        _write_json_atomic(dst, record)
        src.unlink(missing_ok=True)
        self.lease_path(action_key).unlink(missing_ok=True)
        return dst

    # -- operator decisions ---------------------------------------------

    def withdrawn_keys(self) -> frozenset[str]:
        """Every action an operator has withdrawn.

        Listed rather than stat-ed, one call per decision point.  This queue
        lives on NFS, where a stat of a path that did not exist yet is
        negatively cached and keeps answering ``False`` after the file lands --
        the same reason pbrun's wait loop polls by ``readdir``.  A withdrawal
        that a guard could not see is not a withdrawal.
        """

        try:
            names = os.listdir(self.dir(WITHDRAWN))
        except OSError:
            return frozenset()
        return frozenset(
            name[: -len(".json")] for name in names if name.endswith(".json")
        )

    def superseded_dir(self) -> Path:
        """Where a withdrawal goes once it is no longer the live decision.

        A subdirectory rather than a timestamped sibling, because every reader
        of ``withdrawn/`` addresses it by ``<key>.json``: ``withdrawn_keys``
        lists it, ``find_key`` globs it, ``item_path`` builds the name,
        ``pbrun``'s wait loop lists it and ``tessera_status`` counts ``*.json``
        in it.  A sibling named ``<key>.<unix>.json`` would look to all five
        like an action whose key is nonsense; a subdirectory is invisible to
        every one of them, and still there for the operator asking what was
        cancelled, by whom, and when.
        """

        return self.dir(WITHDRAWN) / "superseded"

    def _file_superseded(
        self,
        record: Mapping[str, object] | None,
        *,
        key: str,
        kind: str,
        **stamps: object,
    ) -> Path:
        """Keep a record that is no longer live, under a name of its own."""

        when = _now()
        payload = dict(record or {})
        payload["action_key"] = key
        payload.update(stamps)
        path = self.superseded_dir() / f"{key}.{when:.6f}.{kind}.json"
        _write_json_atomic(path, payload)
        return path

    def _supersede_withdrawal(self, action_key: str) -> dict[str, object] | None:
        """Retire the live withdrawal for ``action_key``; return what it said."""

        live = self.item_path(WITHDRAWN, action_key)
        record = _read_json(live)
        if record is None:
            return None
        self._file_superseded(
            record, key=action_key, kind="withdrawal",
            superseded_unix=_now(), superseded_host=socket.gethostname(),
        )
        live.unlink(missing_ok=True)
        return record

    def withdrawal_covers(
        self,
        record: Mapping[str, object] | None,
        *,
        action_key: str | None = None,
        withdrawn: frozenset[str] | None = None,
    ) -> dict[str, object] | None:
        """The withdrawal that cancelled THIS record, or ``None``.

        One predicate for all seven guard sites, because the alternative was
        the rule half-applied: ``claim`` scoping the check while ``finish``
        and ``execute`` still matched on the bare key would discard a
        legitimate later run's outcome and kill the run outright.

        **The generation, not the key.**  An action key is a content hash, so
        a withdrawal has to name the *run* it cancelled, not the name of the
        work for all time.  ``published_unix`` is that name: ``publish``
        stamps a fresh one and every requeue -- ``finish``'s retry branch and
        ``reap_stale``'s -- carries the original forward, so the losing half
        of a withdrawal race and a fresh submission are distinguishable
        without asking either of them to declare which it is.  The test is
        equality, not "newer than": only two writers ever put a record in
        ``ready`` -- ``publish``, which stamps a fresh ``published_unix``, and
        the two requeue branches, which copy the original through unchanged --
        so a record whose stamp DIFFERS from the withdrawal's is a different
        request whichever way the difference runs.  Ordering would have made
        the guard depend on the clock never stepping backwards between two
        submissions, which is a promise nothing here needs to make.

        A record with no generation to compare is treated as covered, which is
        the safe direction: the cancelled work does not run.  Every caller
        that then removes such a record files it first, so "covered" never
        means "vanished".

        The marker is read only for keys the listing just reported, so NFS's
        negative cache -- the reason ``withdrawn_keys`` lists rather than
        stats -- is not in this path.  A marker that is gone by the time it is
        read was retired by a re-submission, and the record is not covered.
        """

        key = str(action_key or (record or {}).get("action_key") or "")
        if not key:
            return None
        known = self.withdrawn_keys() if withdrawn is None else withdrawn
        if key not in known:
            return None
        marker = _read_json(self.item_path(WITHDRAWN, key))
        if marker is None:
            return None
        if record is None:
            return marker
        mine = record.get("published_unix")
        theirs = marker.get("published_unix")
        if isinstance(mine, (int, float)) and isinstance(theirs, (int, float)):
            if float(mine) != float(theirs):
                return None
        return marker

    def runtime_of(self, host: str | None) -> str | None:
        """Which published bytes the worker on ``host`` is answering with.

        ``None`` when no live offer names the host at all.  The empty string
        when the offer predates ``runtime_commit`` -- both mean "cannot tell",
        and a withdrawal that cannot tell has to say so rather than imply the
        worker will honour it.
        """

        if not host:
            return None
        for offer in self.offers():
            if str(offer.get("host")) == str(host):
                return str(offer.get("runtime_commit") or "")
        return None

    def find_key(self, prefix: str) -> str:
        """Resolve a key prefix to the one action it names.

        Everything an operator has on screen is a prefix: ``pbrun`` prints
        ``queued 8fc86da0e13f`` and the worker loop logs the same twelve
        characters.  Requiring the full digest to cancel would mean going and
        finding it in the queue directory first, at the moment the box is
        already on fire.  Ambiguity is refused rather than guessed at, because
        the wrong guess here kills someone else's work.
        """

        wanted = str(prefix)
        if not wanted:
            raise PoolContractError("an action key prefix must not be empty")
        # An action key is a hex digest, so anything else is a typo -- and the
        # match below is a glob, where a stray ``*`` would silently name every
        # action in the queue and a stray ``[`` would raise from pathlib.
        if any(character not in "0123456789abcdef" for character in wanted.lower()):
            raise PoolContractError(
                f"an action key is a hex digest; {wanted!r} is not a prefix of one")
        seen: set[str] = set()
        for state in (READY, CLAIMED, DONE, FAILED, WITHDRAWN):
            for path in _glob(self.dir(state), f"{wanted}*.json"):
                seen.add(path.stem)
        if not seen:
            raise PoolContractError(f"no action in the queue starts with {wanted!r}")
        if len(seen) > 1:
            listed = ", ".join(sorted(key[:16] for key in seen))
            raise PoolContractError(
                f"{wanted!r} names {len(seen)} actions ({listed}); "
                "say more of the key")
        return seen.pop()

    def withdraw(
        self,
        action_key: str,
        *,
        reason: str = "",
        by: str = "",
        signal_child: bool = True,
    ) -> dict[str, object]:
        """Cancel an action by operator decision.  Not a defect; not a retry.

        Every other terminal path in this module is the pool's opinion of a
        *worker's* health -- a lease that stopped beating, a record no consumer
        can address, an argv that exited non-zero.  None of them is "I have
        changed my mind", so cancelling meant rewriting ``max_attempts`` into a
        live claimed record, killing the child, and hoping the rewrite landed
        first: if the kill won, ``finish`` requeued the action at its old
        ``max_attempts`` and the whole thing restarted.  Four redundant test
        suites ran to completion on a box at load average 371 because stopping
        them was more dangerous than letting them finish.

        Three things make this the verb that was missing.

        **The marker is written before anything is removed.**  ``claim``,
        ``finish`` and ``reap_stale`` all consult it, so from the instant it
        exists the action cannot be claimed, cannot be requeued and cannot be
        filed under ``done`` or ``failed`` -- whatever a concurrent worker is
        doing at the time.  The withdrawal wins the race by construction; the
        operator does not have to.

        **It lands in ``withdrawn/``, not ``failed/``.**  The failure record is
        what someone reads to ask whether the fleet is broken, and four
        cancellations sitting in it say the fleet is broken when the truth is
        that somebody changed their mind.

        **The signal goes to the action's process group.**  Not to the
        launcher, which is not in it, and not to this loop's pid, which the
        lease has always carried and which is a different process again.  See
        ``terminate_action``.  Cross-box that signal cannot be sent at all, so
        ``execute`` also watches for the marker between heartbeats and stops
        its own child; withdrawal is therefore correct from any box and merely
        *faster* from the one running the work.

        Tokens go back through the same ``ledger(host).release(key)`` path
        ``finish`` uses.  They go back even while a remote action is still
        being stopped: a reservation that outlives its holder is the starvation
        bug's exact shape, and a worker one heartbeat from stopping is the
        smaller risk.  Releasing twice is free, because tokens are filed under
        the action key and the second release finds nothing to return.

        Idempotent.  Run it twice and the second run re-signals, re-releases
        and re-cleans -- all no-ops once they have happened -- and leaves the
        first decision's record, timestamp and reason untouched.

        **It cancels a run, not a name.**  The marker is scoped to the
        generation it was filed against -- see ``withdrawal_covers`` -- and a
        later ``publish`` of the same key retires it into
        ``withdrawn/superseded/``.  An action key is a content hash, so
        re-submitting one is how anybody asks for the same work again; a
        withdrawal that blacklisted the key would make the queue silently eat
        that request, and the only remedy would be a hand edit of the live
        queue.
        """

        key = str(action_key)
        self.ensure_layout()
        withdrawn_path = self.item_path(WITHDRAWN, key)
        claimed_path = self.item_path(CLAIMED, key)
        ready_path = self.item_path(READY, key)

        existing = _read_json(withdrawn_path)
        record = _read_json(claimed_path)
        origin: str | None = CLAIMED if record is not None else None
        if record is None:
            record = _read_json(ready_path)
            origin = READY if record is not None else None
        if record is None and existing is None:
            for state in (DONE, FAILED):
                finished = _read_json(self.item_path(state, key))
                if finished is not None:
                    # Nothing to stop and nothing to file.  Reporting this
                    # rather than raising matters: an operator who withdraws an
                    # action that finished a second earlier got what they asked
                    # for, and should be told so, not told they mistyped.
                    return {
                        "action_key": key,
                        "status": "already_finished",
                        "state": state,
                        "host": finished.get("finished_host"),
                        "released": 0,
                        "signalled": None,
                        "path": str(self.item_path(state, key)),
                        "reason": "",
                    }
            raise PoolContractError(f"no such action in the queue: {key}")

        lease = _read_json(self.lease_path(key)) or {}
        host: str | None = None
        if isinstance(record, Mapping):
            claimed_host = record.get("claimed_host")
            host = claimed_host if isinstance(claimed_host, str) else None
        if host is None and isinstance(lease.get("host"), str):
            host = str(lease["host"])

        if existing is None:
            filed = dict(record or {})
            filed.update(
                {
                    "schema": POOL_OUTCOME_SCHEMA_V1,
                    "action_key": key,
                    "status": "withdrawn",
                    "withdrawn_from": origin or "unknown",
                    "withdrawn_unix": _now(),
                    "withdrawn_host": socket.gethostname(),
                    "withdrawn_by": str(by),
                    "reason": str(reason),
                }
            )
            _write_json_atomic(withdrawn_path, filed)
        else:
            filed = existing

        # Step one of the hand-edit, done by the verb: ``max_attempts: 1``
        # written into the live claimed record.  The marker above stops every
        # worker running THESE bytes, but a loop holds the module it imported
        # at start, so part of the fleet cannot see it until the runtime rolls
        # -- and the signal ladder below runs for seconds, which is exactly
        # when a worker this withdrawal just SIGTERMed calls ``finish``.  An
        # old ``finish`` reads this record: with the limit at one attempt its
        # retry branch is unreachable, so the outcome is filed terminally
        # instead of being requeued and re-run.  That is the race the operator
        # used to have to win by hand, and it is the last of it that new bytes
        # can reach.
        if origin == CLAIMED and isinstance(record, Mapping):
            poisoned = dict(record)
            poisoned["max_attempts"] = 1
            poisoned["withdrawn_unix"] = filed.get("withdrawn_unix")
            poisoned["withdrawn_by"] = str(filed.get("withdrawn_by") or by)
            poisoned["withdrawn_note"] = (
                "withdrawn by an operator; the retry is closed so a worker "
                "that cannot see withdrawn/ files this terminally"
            )
            _write_json_atomic(claimed_path, poisoned)

        # The ready record goes NOW, not after the ladder.  The ladder can run
        # for seconds; a re-submission landing inside it would otherwise be
        # unlinked by a withdrawal that had already been superseded -- the
        # blocker again, in this verb's own hand.  Gated on the generation for
        # the same reason every other guard is.
        if self.withdrawal_covers(
                _read_json(ready_path), action_key=key) is not None:
            ready_path.unlink(missing_ok=True)

        # Signal before releasing, so this box does not admit work on top of an
        # action that is still dying.
        #
        # No host check is needed and none is made: both ways of naming a target
        # verify the *process*, not the record.  A ``child_pid`` copied from
        # another box's lease is a number that means nothing here, and
        # ``launcher_owns_action`` refuses it because the local process at that
        # number is not running this action.  What is here is what gets
        # signalled.
        signalled: dict[str, object] | None = None
        targets: list[int] = []
        child_pid = lease.get("child_pid")
        if isinstance(child_pid, int) and launcher_owns_action(int(child_pid), key):
            targets.append(int(child_pid))
        targets.extend(pid for pid in find_launcher_pids(key) if pid not in targets)
        if signal_child and targets:
            stopped = [terminate_action(pid) for pid in targets]
            signalled = {
                "launcher_pids": [int(one["launcher_pid"]) for one in stopped],
                "action_pgids": [g for one in stopped for g in one["action_pgids"]],
                "signals": [s for one in stopped for s in one["signals"]],
                "still_alive": any(one["still_alive"] for one in stopped),
            }

        released = self.ledger(host).release(key)
        claimed_path.unlink(missing_ok=True)
        self.lease_path(key).unlink(missing_ok=True)
        self.passes_path(key).unlink(missing_ok=True)
        return {
            "action_key": key,
            "status": "already_withdrawn" if existing is not None else "withdrawn",
            "state": origin,
            "host": host,
            # What the holder's worker is running, so the caller can be told
            # whether this withdrawal is one it can see.  ``None`` means no
            # live offer names the host; ``""`` means the offer predates the
            # field.  Both are "cannot tell", and the CLI says so.
            "holder_runtime": self.runtime_of(host),
            "released": released,
            "signalled": signalled,
            "path": str(withdrawn_path),
            "reason": str(filed.get("reason") or ""),
        }

    # -- execution ------------------------------------------------------

    def execute(
        self,
        item: Mapping[str, object],
        *,
        python: str | Path = sys.executable,
        timeout_s: float | None = None,
        heartbeat_s: float = HEARTBEAT_S,
        timeout_grace_s: float = TIMEOUT_GRACE_S,
    ) -> dict[str, object]:
        """Run one claimed item through the canonical worker argv.

        Executes as a subprocess rather than in-process on purpose: it is the
        same launch SLURM would have made, so the executed contract does not
        depend on which transport delivered the action.

        ``timeout_s`` bounds the *action*, not just this launcher.  What is
        launched here is a worker that runs the action as a further child, so
        the timeout signals the launcher's whole process group and the launcher
        relays that into the action's own session; the timeout path itself is
        bounded end to end, because a timeout that can hang is not a timeout.
        """

        key = str(item["action_key"])
        argv = [str(python)] + worker_argv(
            worker_script=item["worker_script"],
            action_key=key,
            cas_root=item["cas_root"],
            checkout_root=item["checkout_root"],
        )
        owner = str(item.get("claimed_by") or "")
        started = _now()
        # Withdrawal checkpoint one of three: before the launch.  A cancellation
        # that landed in the microseconds between ``claim``'s rename and this
        # call would otherwise start the work anyway, and then have to stop it.
        if self.withdrawal_covers(item) is not None:
            return {
                "status": "withdrawn",
                "returncode": None,
                "stdout": "",
                "stderr": "",
                "elapsed_s": 0.0,
                "argv": argv,
            }
        process = subprocess.Popen(
            argv,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            # The launcher leads its own group so the timeout can signal the
            # group rather than the single pid.  ``kill()`` on the pid reaches
            # the launcher only, and leaves the action holding the GPU.
            start_new_session=True,
        )
        # Say which process is the launcher, so a withdrawal ON THIS BOX can
        # signal the action's group at once instead of waiting a heartbeat for
        # the loop below to notice.  The lease's own ``pid`` is this loop, which
        # is not the same process and must never be the signal's target.
        # Inside the try, not before it: this write is the first thing after
        # the Popen, and an unwind here -- a Ctrl-C landing on it, or the lease
        # write itself failing -- would otherwise leave the action running in
        # its own session with nothing left to reap it.  Everything after the
        # Popen belongs under the same guard.
        try:
            self.write_lease(key, owner=owner, child_pid=process.pid)
            # Refresh the lease while the child runs; a long action must not be
            # reaped out from under itself.
            while True:
                try:
                    out, err = process.communicate(timeout=heartbeat_s)
                    break
                except subprocess.TimeoutExpired:
                    # Checkpoint two: the cross-box path.  A withdrawal from another
                    # box cannot signal anything on this one, so this poll is what
                    # makes the verb correct from anywhere -- at a cost of at most
                    # one heartbeat, and none at all when the operator is here.
                    if self.withdrawal_covers(item) is not None:
                        out, err = self._stop_action(process)
                        return {
                            "status": "withdrawn",
                            "returncode": process.returncode,
                            "stdout": out,
                            "stderr": err,
                            "elapsed_s": _now() - started,
                            "argv": argv,
                        }
                    self.write_lease(key, owner=owner, child_pid=process.pid)
                    if timeout_s is not None and _now() - started > timeout_s:
                        # Worst case this branch spends three grace budgets
                        # -- TERM wait, KILL wait, drain (~45 s) -- without
                        # refreshing the lease, against a 300 s expiry.
                        pb._terminate_process_group(
                            process, grace_s=timeout_grace_s
                        )
                        out, err, survived = _drain(
                            process, timeout_s=timeout_grace_s
                        )
                        return {
                            "status": "timeout",
                            # Stays None: ``pbrun`` returns any integer
                            # ``returncode`` as its own exit status, and an
                            # action that finished inside the tick that
                            # crossed the deadline would hand it a 0 for a
                            # record filed as a timeout.  ``status`` is the
                            # authority here; the launcher's exit goes in a
                            # field of its own below.
                            "returncode": None,
                            # Which path the launcher took: 143 is its own
                            # unwind on the relayed TERM, -15/-9 mean it never
                            # handled the signal at all.
                            "launcher_returncode": process.returncode,
                            "stdout": out,
                            "stderr": err,
                            # True when the pipes never reached EOF, so
                            # something in the action's tree outlived SIGKILL
                            # (a D-state GPU wedge does).  This branch still
                            # returns and the ledger token is still released,
                            # so the flag is the only notice that it was
                            # released for a GPU somebody still holds.
                            "action_survived_kill": survived,
                            "elapsed_s": _now() - started,
                            "argv": argv,
                        }
        except BaseException:
            # The launcher leads its own session now, so a Ctrl-C or any other
            # signal reaching this loop no longer reaches it -- before the new
            # session it did, and the launcher's own unwind reaped the action.
            # Unwinding from here without reaping would leave exactly the
            # orphan that session was introduced to bound.
            pb._terminate_process_group(process, grace_s=timeout_grace_s)
            _drain(process, timeout_s=timeout_grace_s)
            raise
        status = "executed" if process.returncode == 0 else "failed"
        # Checkpoint three: on the way out.  When the operator's own signal
        # reached the action group first, the launcher reports the SIGTERM that
        # stopped it and this worker would otherwise log a defect for a
        # decision.  ``finish`` files the outcome correctly either way; this is
        # about the line the worker prints and the record's ``status``.
        if status == "failed" and self.withdrawal_covers(item) is not None:
            status = "withdrawn"
        return {
            "status": status,
            "returncode": process.returncode,
            "stdout": out,
            "stderr": err,
            "elapsed_s": _now() - started,
            "argv": argv,
        }

    def _stop_action(self, process: subprocess.Popen) -> tuple[str, str]:
        """Stop a withdrawn action and collect whatever it managed to say.

        The read is bounded on purpose.  The action inherits the launcher's
        stdout and stderr pipes, so an unbounded ``communicate()`` after a kill
        returns only when the *action* exits -- the very process this is trying
        to stop.  A stop path that can itself hang is not a stop path.
        """

        terminate_action(process.pid)
        try:
            return process.communicate(timeout=WITHDRAW_GRACE_S)
        except subprocess.TimeoutExpired:
            process.kill()
        try:
            return process.communicate(timeout=WITHDRAW_GRACE_S)
        except subprocess.TimeoutExpired:
            return "", ""

    def serve_once(
        self,
        *,
        tags: Iterable[str] = (),
        has_gpu: bool = False,
        python: str | Path = sys.executable,
        timeout_s: float | None = None,
        capacity: Mapping[str, int] | None = None,
    ) -> dict[str, object] | None:
        """Reap, claim, run, record.  ``None`` when the queue had nothing.

        ``None`` also means "nothing this box may admit right now" once
        ``capacity`` is in play -- including the deliberate case where a starved
        item is withholding the host.  A caller that loops should treat it as
        back-pressure and poll again, not as an empty queue.
        """

        self.reap_stale()
        item = self.claim(tags=tags, has_gpu=has_gpu, capacity=capacity)
        if item is None:
            return None
        key = str(item["action_key"])
        try:
            outcome = self.execute(item, python=python, timeout_s=timeout_s)
        except BaseException as exc:                      # noqa: BLE001
            # Never leave a claim dangling: an unexpected failure is recorded as
            # a terminal state, not left for the reaper 300 s later.
            self.finish(key, status="failed", detail={"exception": repr(exc)})
            raise
        self.finish(key, status=str(outcome["status"]), detail=outcome)
        return outcome


def describe_placement_census(census: Mapping[str, object]) -> str:
    """One line of the census, in the words every reader should use for it.

    Kept beside the measurement rather than at each call site so the metric
    has one name wherever it is printed.  A number two tools describe
    differently is a number nobody can grep for.
    """

    if not census.get("known"):
        return f"ready {int(census.get('ready', 0))} (fleet width unknown: no worker has announced)"
    parts = [f"ready {int(census.get('ready', 0))}"]
    one_box = int(census.get("one_box", 0))
    pinned = census.get("pinned_to") or {}
    where = ""
    if isinstance(pinned, Mapping) and pinned:
        where = " (" + ", ".join(f"{host} {n}" for host, n in pinned.items()) + ")"
    parts.append(f"{one_box} on exactly one box{where}")
    # The migration number.  ``one_box`` falls for two very different reasons
    # -- submitters moving to a checkout every box can see, or a box simply
    # going away -- and only the first is the fix working, so the half that
    # a path caused is named separately.
    by_path = int(census.get("one_box_by_path", 0))
    if by_path:
        parts.append(f"{by_path} by a box-local checkout")
    parts.append(f"{int(census.get('wide', 0))} on more than one")
    parts.append(f"{int(census.get('unplaceable', 0))} on none")
    # Printed only when there are any: a zero here would teach readers to skip
    # the clause, which is the one thing it must not be.
    unreadable = int(census.get("unreadable", 0))
    if unreadable:
        parts.append(f"{unreadable} unreadable")
    return ", ".join(parts)
