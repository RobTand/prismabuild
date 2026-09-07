"""Shared-filesystem pull-queue transport: dispatch without a scheduler.

``slurm.py`` and ``dagster.py`` both assume a scheduler that is not installed on
this fleet, so neither is its live transport.  This module is the deployed
transport: workers pull sealed actions from a directory on the shared NFS mount
and execute them through the *same* canonical worker argv SLURM would have
submitted.

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

**A terminal generation stays terminal.**  ``run_local_action`` looks the
action key up in the CAS first and normally makes a retry a cheap
``cache_hit``, but that is not permission to resurrect a generation which the
queue has already filed under ``done`` or ``failed``.  Stale reaping and the
claim boundary both refuse a ready/claimed copy carrying the terminal record's
``published_unix``.  The generation check matters: the same action key may be
submitted again deliberately, and that later request is still work.

**A retry is a producer contract, and an attempt is immutable evidence.**
The pool preserves the explicit ``max_attempts`` supplied by each transport;
``fleet/pbrun`` gives arbitrary commands one attempt unless their producer
declares the whole action retry-safe.  Numerical determinism is deliberately
separate: an action can deterministically write external state before failing.
Every success, failure, or lease loss concluded from its live queue record is
first-writer-published below ``attempts/<action-key>/<generation>/`` with
separate immutable stdout and stderr, and the mutable ready/terminal summary
links that ordered history.  A later refusal can therefore never replace the
causal attempt that did the work.  An operator withdrawal remains the decision
record rather than inventing a worker result for work it cancelled.

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

**A detached container is still the action.**  ``pbrun`` seals a derived
container-owner id and puts its Docker shim first on ``PATH``.  The shim labels
every container it creates and leaves a durable marker; ``finish``, withdrawal
and stale reaping query that label, force-remove its containers and verify the
answer is empty before releasing capacity.  A remote host, a busy creation
transaction or a Docker error keeps the claim and tokens: uncertainty is not
permission to schedule a second action onto the same GPU.

Clock skew between claimant and reaper is real but immaterial here: both Sparks
are NTP-synchronised and measured 1.2-2.5 ms apart against a 300 s lease.
"""

from __future__ import annotations

from collections.abc import Container, Iterable, Iterator, Mapping, Sequence
from typing import NamedTuple
from contextlib import contextmanager
import errno
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import uuid

from . import core as pb
from .materialize import (  # relocated verbatim; see materialize.py
    _cleanup_execution_checkout,
    _now,
    _run_materializer_git,
    _write_json_atomic,
)
from . import materialize, cpu_topology
from . import adaptive_cpu as cpu_admission
from . import adaptive_gpu as gpu_admission
from . import resource_scope

POOL_ITEM_SCHEMA_V1 = "prismaquant.prismabuild.pool_item.v1"
POOL_CLAIM_INTENT_SCHEMA_V1 = "prismaquant.prismabuild.pool_claim_intent.v1"
POOL_LEASE_SCHEMA_V1 = "prismaquant.prismabuild.pool_lease.v1"
POOL_OUTCOME_SCHEMA_V1 = "prismaquant.prismabuild.pool_outcome.v1"
POOL_ATTEMPT_SCHEMA_V1 = "prismaquant.prismabuild.pool_attempt.v1"
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

#: Suffix of a claim that has been moved out of the way while its finisher
#: publishes the item's next home.  Every reader of ``claimed/`` addresses it
#: as ``<key>.json`` or ``<key>.lease`` -- ``reap_stale`` and
#: ``sweep_widowed_leases`` by glob, ``item_path`` and ``find_key`` by name --
#: so a suffix that is neither is invisible to all of them, which is the point.
TOMBSTONE_SUFFIX = ".tombstone"
CONTAINER_OWNERS = "container-owners"
CONTAINER_OWNER_LABEL = "prismabuild.action"
DOCKER = "/usr/bin/docker"
ATTEMPTS = "attempts"

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

# The pool is also a lower-level transport for specialized producers whose
# existing contract explicitly chooses its bound.  ``fleet/pbrun`` supplies a
# separate safe default of one for arbitrary commands; changing this legacy
# pool API default would silently rewrite those producers' policy.
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

#: How much of an unparseable record is kept inline with the evidence.  Enough
#: to recognise a writer's handwriting, little enough that a runaway producer
#: cannot fill the queue root with the file it already failed to write.
UNREADABLE_HEAD_BYTES = 2048

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

# The snapshot bundle travels through the shared CAS, but execution trees stay
# on each worker's local disk. Same spelling on every box, different storage.
# Read at call time rather than bound once, so that overriding it here steers
# what this transport materializes and nothing else.
LOCAL_CHECKOUT_ROOT = materialize.LOCAL_CHECKOUT_ROOT


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


def normalize_placement_tags(tags: Sequence[object]) -> list[str]:
    """Canonicalize the exact tag conjunction the queue matcher enforces."""

    if isinstance(tags, (str, bytes)):
        raise PoolContractError("pool item tags must be a sequence of tags")
    normalized = {str(tag) for tag in tags}
    if any(not tag or tag.strip() != tag for tag in normalized):
        raise PoolContractError("pool item tags must be nonempty trimmed strings")
    return sorted(normalized)


class PoolError(pb.PrismaBuildError):
    """A queue-level failure, distinct from an action-level one."""


class PoolContractError(PoolError, ValueError):
    """A queue record does not satisfy its schema."""


class AmbiguousClaimHolder(PoolContractError):
    """Contradictory committed reservations forbid concluding a claim."""


class ExecutionBudget(NamedTuple):
    """How long this action may run, and why that is the number.

    ``requested`` is the submitter's sealed ``execution_timeout_s``;
    ``ceiling`` is the worker loop's own safety limit; ``effective`` is what
    actually kills the action.  All three, because the two-field version of
    this -- one clamped float -- is what made #293 undiagnosable: the #275
    campaign asked for 13000 s, was silently given 7200 s, and was killed at
    7200 s with nothing anywhere recording that a clamp had happened.  A
    receipt has to be able to say what governed, not just that time ran out.

    ``None`` in any field means unbounded, which is a real answer and not a
    missing one: neither the submitter nor the loop is obliged to name a limit.
    """

    effective: float | None
    requested: float | None
    ceiling: float | None

    @property
    def clamped(self) -> bool:
        """Did the worker's ceiling, and not the submitter, decide?"""

        return (self.requested is not None and self.ceiling is not None
                and self.ceiling < self.requested)

    def as_record(self) -> dict[str, object]:
        """The fields an outcome carries so a receipt can be read years later."""

        return {
            "execution_timeout_s": self.effective,
            "execution_timeout_requested_s": self.requested,
            "execution_timeout_ceiling_s": self.ceiling,
            "execution_timeout_clamped": self.clamped,
        }


def execution_budget(
    item: Mapping[str, object], ceiling: float | None
) -> ExecutionBudget:
    """The deadline this action runs under, with both numbers behind it."""

    requested = _requested_execution_timeout(item)
    if requested is None:
        return ExecutionBudget(ceiling, None, ceiling)
    effective = requested if ceiling is None else min(requested, ceiling)
    return ExecutionBudget(effective, requested, ceiling)


def _execution_timeout(item: Mapping[str, object], ceiling: float | None) -> float | None:
    """Read the deadline from the sealed request, never mutable queue metadata."""
    key = str(item["action_key"])
    request = Path(str(item["cas_root"])) / "requests" / key[:2] / f"{key}.json"
    try:
        raw = pb._read_regular_file_nofollow(request, where="pool action request")
    except FileNotFoundError:
        # Legacy/custom launchers can have no request. The canonical worker
        # independently refuses a missing request before executing any action.
        return ceiling
    action = pb.validate_action(pb._decode_strict_json(raw, where="pool action request"))
    if action["action_key"] != key:
        raise PoolContractError("pool action request does not match the claimed key")
    requested = action["params"].get("execution_timeout_s")
    if requested is None:
        return ceiling
    if (type(requested) not in (int, float) or not math.isfinite(requested)
            or requested <= 0):
        raise PoolContractError("execution_timeout_s must be a positive finite number")
    return float(requested) if ceiling is None else min(float(requested), ceiling)


def _requested_execution_timeout(item: Mapping[str, object]) -> float | None:
    """What the submitter sealed, before any ceiling is applied.

    Reads the same sealed request ``_execution_timeout`` does, and refuses the
    same values, because a budget the receipt reports and a budget the worker
    enforces that disagreed would be worse than either alone.
    """

    return _execution_timeout(item, None)


@contextmanager
def _execution_checkout(item: Mapping[str, object]) -> Iterator[Path]:
    """Yield the live path or a private checkout of the sealed snapshot.

    The sequence itself is ``materialize._execution_checkout``; SLURM runs the
    same one.  What this adds is the root: ``LOCAL_CHECKOUT_ROOT`` is read here,
    at call time, so the queue's root is the queue's to state.
    """

    try:
        with materialize._execution_checkout(
            item, local_checkout_root=LOCAL_CHECKOUT_ROOT
        ) as checkout_root:
            yield checkout_root
    except materialize.MaterializationContractError as exc:
        # The queue's callers and tests speak the queue's contract error; the
        # sequence's own type is the SLURM job's to see.
        raise PoolContractError(str(exc)) from exc


def _publish_immutable(path: Path, raw: bytes, *, where: str) -> None:
    """First-writer-publish one attempt artifact; refuse conflicting bytes."""

    won = pb._atomic_publish(path, raw)
    if won:
        return
    observed = pb._read_regular_file_nofollow(
        path, where=where, require_readonly=True
    )
    if observed != raw:
        raise PoolContractError(
            f"{where} conflicts with the immutable record already filed: {path}"
        )


def _read_json(
    path: Path, *, tolerate_stale: bool = False
) -> dict[str, object] | None:
    """The record at ``path``, or ``None`` if it is not there to read.

    ``tolerate_stale`` extends "not there" to cover ``ESTALE``, and belongs
    only to a caller enumerating a directory whose entries come and go.  A
    worker offer is exactly that: it appears when a loop starts and is gone
    when the box leaves, so a file that vanishes between the ``glob`` and the
    read is ordinary.  ``ENOENT`` has always been read that way here; on NFS
    the same event arrives as ``ESTALE`` through a directory handle the client
    had already cached, and untreated it left this function, ``offers()`` and
    ``placeable_hosts()`` and killed a submission before it queued anything --
    a whole ``pbtest`` shard, 74 tests never run, for two offer files an
    operator had tidied away (#208).

    It is opt-in because ``ESTALE`` is not only "this file vanished": it is
    also what a dead mount or a stale parent handle returns.  An enumerator
    has already had its ``glob`` succeed, so it holds the evidence that the
    directory is live and the entry is not.  A caller addressing one record by
    key holds no such evidence -- ``reclaim`` asserting exactly one terminal
    record, a lease read, a pass record -- and answering "absent" there would
    turn a broken mount into a confident wrong verdict.  Those callers keep
    the default and stay loud.
    """

    try:
        raw = path.read_bytes()
    except FileNotFoundError:
        return None
    except OSError as exc:
        if tolerate_stale and exc.errno == errno.ESTALE:
            return None
        raise
    if not raw:
        return None
    try:
        value = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
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


def _docker_owned_container_ids(owner: str) -> list[str]:
    """Container ids carrying this action's ownership label.

    Docker payloads are children of ``containerd-shim``, not of the action
    group, so the daemon's label index is the authoritative join back to the
    action.  A failed query is an unknown answer and therefore an exception;
    callers retain capacity on it.
    """

    result = subprocess.run(
        [DOCKER, "ps", "-aq", "--filter", f"label={CONTAINER_OWNER_LABEL}={owner}"],
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()
        raise PoolContractError(
            f"docker ownership query failed ({result.returncode}): {detail}")
    return sorted({line.strip() for line in result.stdout.splitlines() if line.strip()})


def _docker_remove_containers(container_ids: list[str]) -> list[str]:
    """Force-remove exactly the container ids the ownership query returned."""

    if not container_ids:
        return []
    result = subprocess.run(
        [DOCKER, "rm", "-f", *container_ids],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()
        raise PoolContractError(
            f"docker cleanup failed ({result.returncode}): {detail}")
    return sorted({line.strip() for line in result.stdout.splitlines() if line.strip()})


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


#: Prefix of a claimant-private acquisition directory under ``held/``.  An
#: action key is 64 hex characters, so a name carrying a dot cannot collide
#: with one, and every reader that addresses a holder by key sees nothing.
ACQUIRING_PREFIX = "claiming."


#: What makes two readings of ``claimed/<key>.json`` the same claim.  The owner
#: alone would be enough for two different workers, but not for one worker's
#: two attempts at the same action, which is the case the reaper creates.
_CLAIM_IDENTITY = ("claimed_by", "claimed_unix", "published_unix", "attempts")


def _same_claim(
    live: Mapping[str, object], snapshot: Mapping[str, object]
) -> bool:
    """Whether a live claimed record is the claim a worker actually ran.

    Only fields the queue writes once per claim, so the guards that rewrite a
    claimed record in place -- a container-cleanup retry, a withdrawal's
    ``max_attempts`` poison, a pending stop -- do not read as a different
    claim.
    """

    return all(
        live.get(field) == snapshot.get(field) for field in _CLAIM_IDENTITY
    )


def _is_acquisition(name: str) -> bool:
    """Whether a holder directory belongs to a claimant rather than an action.

    One predicate for both readers, because the two must agree exactly.  A name
    the sweep declines to recognise but ``held_keys`` reports as an action key
    is a leak nothing owns: no queue directory holds that name, so no reaper
    looks for it, and no sweep frees it.
    """

    return name.startswith(ACQUIRING_PREFIX)


def _acquisition_clock(holder: Path) -> float | None:
    """When a claimant began this acquisition, in seconds since the epoch.

    The clock is in the *name* because there is nowhere else to put it that
    survives: ``rename`` does not touch mtime, and the directory's own mtime is
    bumped by every token moved into it, so a claimant that took one token and
    died looks as fresh as one still working.  A name whose stamp this version
    cannot read falls back to mtime, which is wrong in the conservative
    direction -- too fresh, so swept later -- and never leaves the directory
    unowned.  ``None`` only when the directory has gone.
    """

    parts = holder.name.split(".", 3)
    if len(parts) >= 3:
        try:
            return int(parts[1]) / 1_000_000.0
        except ValueError:
            pass
    try:
        return holder.stat().st_mtime
    except OSError:
        return None


def _acquisition_claimant(name: str) -> tuple[str, int] | None:
    """The host and pid a claimant stamped on its acquisition, if readable."""

    parts = name.split(".")
    if len(parts) < 6:
        return None
    try:
        return parts[3], int(parts[4])
    except ValueError:
        return None


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

    @property
    def minted_dir(self) -> Path:
        """Where the mint right for each token index is recorded, permanently.

        One file per index, created with ``O_EXCL`` and never renamed.  It is
        the only thing that decides whether an index has been minted, because
        the tokens themselves move and a scan of where they move cannot be
        made atomic.
        """

        return self.base / "minted"

    def configure_cpu_tiers(self, tiers: Mapping[str, Sequence[int]]) -> dict:
        """Bind token ordinals to CPUs once; all loops on a host must agree.

        Changing this map requires stopping workers, draining reservations,
        and removing cpu-map.json before restarting. Never reinterpret a held
        ordinal under another affinity or topology.
        """
        record = {kind: list(tiers[kind]) for kind in ("preferred", "fallback")}
        cpus = record["preferred"] + record["fallback"]
        if (not cpus or any(type(c) is not int or c < 0 for c in cpus)
                or len(cpus) != len(set(cpus))):
            raise PoolContractError("CPU tiers must contain distinct nonnegative CPU IDs")
        path = self.base / "cpu-map.json"
        existing = _read_json(path)
        if existing is None:
            if self.held().get("cpu", 0):
                raise PoolContractError("drain legacy CPU reservations before enabling CPU tiers")
            self.base.mkdir(parents=True, exist_ok=True)
            pb._atomic_publish(path, pb._canonical_bytes(record))
            existing = _read_json(path)
        if existing != record:
            raise PoolContractError("CPU tier map differs: stop workers, drain reservations "
                                    "and remove cpu-map.json before changing topology")
        return record

    def cpu_allocation(self, holder: str, tiers: Mapping) -> dict:
        """The actual CPUs represented by this claimant's held tokens."""
        metadata = _read_json(self.held_dir / holder / cpu_admission.METADATA)
        if metadata is not None and "allocation" in metadata:
            return metadata["allocation"]
        ordered = list(tiers["preferred"]) + list(tiers["fallback"])
        cpus = []
        for token in _glob(self.held_dir / holder, "cpu-*"):
            index = int(token.name.split("-")[-1])
            if index >= len(ordered):
                raise PoolContractError("CPU token exceeds configured topology")
            cpus.append(ordered[index])
        return {kind: [c for c in tiers[kind] if c in cpus]
                for kind in ("preferred", "fallback")}

    def free_preferred(self, tiers: Mapping) -> int:
        return sum(int(token.name.split("-")[-1]) < len(tiers["preferred"])
                   for token in _glob(self.free_dir, "cpu-*"))

    def ensure_capacity(self, capacity: Mapping[str, int]) -> None:
        """Create any missing token of each declared kind, idempotently.

        **The marker mints, not the scan.**  This used to snapshot ``free/``,
        then scan ``held/``, then create anything neither listing had shown.
        Movement between the two listings makes a present token invisible to
        both: a token held by A, released while ``held/`` is being scanned and
        re-acquired by B, appears in neither, and ``O_EXCL`` at its free
        pathname does not protect a token of the same name under a holder.  The
        ledger then reported ``cpu=2`` for a configured ``cpu=1`` and admitted
        work against capacity that does not exist.  ``claim`` calls this on
        every poll, so the interleaving overlaps ordinary action turnover
        rather than only initialization.

        So the decision to mint index ``i`` is an ``O_EXCL`` create of
        ``minted/<kind>-<index>``, which never moves and is therefore never
        invisible.  An index whose marker exists is skipped: its token exists
        somewhere, or was deliberately retired.

        **Adoption mints nothing.**  Every ledger already on the shared store
        has free and held tokens and no markers, so the first call creates a
        marker for each token it finds and leaves the totals alone.

        Adoption is a scan, so it inherits the scan's blind spot: a union of
        ``free/`` and ``held/`` is missable in either order, by a concurrent
        release in one and a concurrent acquire in the other.  A token missed
        by adoption is minted a second time here, and that residual duplicate
        is *transient rather than permanent*, which is the property that makes
        it tolerable: the duplicate can only be the free copy of a name whose
        real token is held, and ``release`` renames a held token onto
        ``free/<name>``, replacing it.  The two copies therefore collapse to
        one the moment the holder finishes, without anything ever removing a
        token a holder is using.  An earlier revision of this fix tried to
        remove the duplicate on sight by inode; that reintroduced exactly the
        check-then-act over a set a concurrent rename mutates that the markers
        exist to retire, and it could take a token a second claimant had
        already acquired.

        Ordering note for the one window that remains: a process killed between
        the marker create and the token create loses that index until an
        operator removes the marker.  That direction under-declares capacity,
        which is the safe one; the marker cannot be created second without
        making the duplicate permanent again.
        """

        self.free_dir.mkdir(parents=True, exist_ok=True)
        self.held_dir.mkdir(parents=True, exist_ok=True)
        self.minted_dir.mkdir(parents=True, exist_ok=True)
        # One listing per call rather than an O_EXCL attempt per index: a
        # 96-token memory ledger is polled every few seconds, and the marker
        # remains the arbiter for anything this listing did not show.
        minted = {path.name for path in _scan(self.minted_dir)}
        minted |= self._adopt_present_tokens(minted)
        for kind, count in sorted(capacity.items()):
            total = int(count)
            if total < 0:
                raise PoolContractError(f"capacity for {kind!r} must not be negative")
            for index in range(total):
                name = f"{kind}-{index:04d}"
                if name in minted:
                    continue
                try:
                    descriptor = os.open(
                        self.minted_dir / name,
                        os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                        0o644,
                    )
                except FileExistsError:
                    continue
                os.close(descriptor)
                minted.add(name)
                if self._token_is_held(name):
                    # Adoption did not see it, but a holder has it: the marker
                    # now accounts for that token and nothing is minted.  The
                    # check is a scan and can still miss, which is what the
                    # docstring's transient duplicate is.
                    continue
                try:
                    descriptor = os.open(
                        self.free_dir / name,
                        os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                        0o644,
                    )
                except FileExistsError:
                    continue      # a free token of this name already exists
                os.close(descriptor)

    def _token_is_held(self, name: str) -> bool:
        """Whether any holder here currently contains a token called ``name``."""

        return any(
            (holder / name).exists()
            for holder in _scan(self.held_dir)
            if holder.is_dir()
        )

    def _adopt_present_tokens(self, minted: Container[str]) -> set[str]:
        """Record the mint right for every token this ledger already has.

        Called before minting so a ledger that predates the markers keeps the
        capacity it has instead of having it minted a second time.  Creates
        markers only; it never creates or removes a token.

        ``minted`` is the marker listing already read, so the steady state
        costs the directory walk and no syscall per token: every name is
        already known and only a ledger being adopted opens anything.
        """

        adopted: set[str] = set()
        present = {path.name for path in _glob(self.free_dir, "*-*")}
        for holder in _scan(self.held_dir):
            if holder.is_dir():
                present.update(path.name for path in _glob(holder, "*-*"))
        for name in sorted(present):
            if name in minted:
                continue
            try:
                descriptor = os.open(
                    self.minted_dir / name,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                    0o644,
                )
            except FileExistsError:
                adopted.add(name)
                continue
            except OSError:
                continue
            os.close(descriptor)
            adopted.add(name)
        return adopted

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
                # The mint right goes FIRST, then the token.  Both orders have
                # a two-syscall window, and they fail in opposite directions.
                # Token first leaves a marker with no token, which
                # ``ensure_capacity`` skips forever: capacity silently and
                # permanently lost, undetectable without a scan that cannot be
                # made safe.  Marker first leaves a token with no marker, which
                # the next poll's adoption re-marks and this retire retires
                # again -- the retire simply did not happen, which is the
                # recoverable direction.  Adoption is why: a token with no
                # marker is adopted, never minted a second time.
                (self.minted_dir / token.name).unlink(missing_ok=True)
                try:
                    token.unlink()
                except OSError:
                    continue
                retired[kind] = retired.get(kind, 0) + 1
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

    def begin_acquire(
        self, action_key: str, demand: Mapping[str, int], *,
        adaptive: dict | None = None, cpu_tiers: Mapping | None = None,
        adaptive_gpu: dict | None = None,
    ) -> str | None:
        """Take the whole demand into a directory only this claimant owns.

        Admission runs *before* the ready-to-claimed rename, so at the moment
        tokens are taken it is not yet known which contender will own the
        action.  Filing them under ``held/<action_key>`` gave every contender
        for one key the same rollback target: the loser's ``release`` returned
        the winner's tokens, and a third action was then admitted on capacity
        the winner was already executing against.  The reservation therefore
        belongs to the *claimant* until the rename decides, and only then to
        the action.

        The private directory lives under ``held/`` so that every reader which
        counts what is not free -- ``ensure_capacity``'s holder scan,
        ``capacity``, ``held``, ``retire_free_capacity`` -- accounts for tokens
        in flight without knowing this mechanism exists.  Returns the handle to
        commit or abandon, or ``None`` when the demand could not be met in
        full.

        All-or-nothing on the demand: a multi-resource actor that keeps what it
        managed to get while blocked on what it did not is holding resources it
        cannot use.  That holds for *every* ending, not only the insufficient
        one: an exception raised anywhere between the first rename and the last
        metadata write empties the private directory back into ``free/`` before
        it leaves, because a caller that never receives the handle has no way to
        return the tokens itself.
        """

        wanted = {k: int(v) for k, v in demand.items() if int(v) > 0}
        handle = (
            f"{ACQUIRING_PREFIX}{int(_now() * 1_000_000)}.{action_key}"
            f".{socket.gethostname()}.{os.getpid()}.{uuid.uuid4().hex[:8]}"
        )
        if not wanted:
            return handle
        destination = self.held_dir / handle
        destination.mkdir(parents=True, exist_ok=True)
        try:
            for kind, need in sorted(wanted.items()):
                if kind == "cpu" and adaptive is not None:
                    need -= int(adaptive.get("preferred_borrow", 0))
                taken = 0
                for token in _glob(self.free_dir, f"{kind}-*"):
                    if taken >= need:
                        break
                    try:
                        os.rename(token, destination / token.name)
                    except (FileNotFoundError, NotADirectoryError):
                        continue      # another worker took it first
                    taken += 1
                if taken < need and not ((kind == "cpu" and adaptive is not None
                                           and adaptive.get("borrowing"))
                                          or (kind == "gpu" and adaptive_gpu is not None
                                              and adaptive_gpu.get("probe"))):
                    raise _Insufficient(kind)
            if adaptive_gpu:
                metadata = dict(adaptive_gpu, borrowed_gpu=max(
                    0, wanted.get("gpu", 0) - len(_glob(destination, "gpu-*"))))
                _write_json_atomic(destination / gpu_admission.METADATA, metadata)
            if adaptive is not None and cpu_tiers is not None:
                allocation = self.cpu_allocation(handle, cpu_tiers)
                assigned = set(allocation["preferred"] + allocation["fallback"])
                # Proven idle preferred reservations may be shared before
                # consuming free SMT/efficiency tokens. Unknown donors retain
                # ordinary disjoint physical-token admission.
                for tier in ("preferred", "fallback"):
                    for cpu in adaptive.get("borrowable_cpus", []):
                        if cpu not in cpu_tiers[tier]:
                            continue
                        if len(assigned) >= wanted.get("cpu", 0):
                            break
                        if cpu not in assigned:
                            allocation[tier].append(cpu)
                            assigned.add(cpu)
                if len(assigned) != wanted.get("cpu", 0):
                    raise _Insufficient("cpu")
                for tier in allocation:
                    allocation[tier] = [c for c in cpu_tiers[tier] if c in assigned]
                metadata = dict(adaptive, allocation=allocation,
                                borrowed_cpu=max(0, len(assigned) - len(_glob(destination, "cpu-*"))))
                _write_json_atomic(destination / cpu_admission.METADATA, metadata)
        except _Insufficient:
            self._empty_into_free(destination)
            return None
        except BaseException:
            # All-or-nothing cannot depend on *which* exception ends the
            # attempt.  ``_Insufficient`` is the only ending this function
            # authors, and it was the only ending that returned the tokens;
            # every other one -- ``cpu_allocation`` refusing a token index the
            # configured topology no longer covers, ``_read_json`` on a torn
            # ``.adaptive.json``, an ESTALE or ENOSPC out of ``os.rename`` or
            # either ``_write_json_atomic`` -- left the whole demand under a
            # private holder.  The caller cannot clean that up: the function
            # never returns, so no ``handle`` reaches it and
            # ``abandon_acquire`` has nothing to name.  Only
            # ``sweep_stale_acquisitions`` recovers it, and its grace is
            # ``LEASE_TIMEOUT_S``, so a repeating cause starves the box one
            # five-minute reservation at a time.
            self._empty_into_free(destination)
            raise
        return handle

    def commit_acquire(self, action_key: str, handle: str) -> int:
        """Move a claimant's private tokens under its action.  Count moved.

        Called by the winner of the ready-to-claimed rename, and by nobody
        else.  The move is per token rather than one directory rename: a
        leftover ``held/<action_key>`` from a release that could not remove its
        own directory makes a directory rename fail with ``ENOTEMPTY``, and the
        count this returns has to be exact so the caller can fail closed.  Per
        token loses nothing, because both directories are under ``held/``: at
        no point in the merge is a token countable as free, and at no point can
        a second claimant take one.

        The count is what the caller checks, and it has to be exact, which is
        the other reason the move is per token.  A claimant swept as stale (see
        :meth:`sweep_stale_acquisitions`) and then winning its rename would
        otherwise proceed to run an action with no reservation, which is the
        same over-admission by another road.  One directory rename would be
        atomic but countable only by listing the source *before* it, and a
        sweep landing between the count and the rename would inflate the count
        -- the one direction the caller must not be lied to in.

        A name already present under the destination is left where it is rather
        than renamed over.  There is one token per index, so a collision means
        some earlier incarnation's tokens are filed under this key; replacing
        the file would delete a token with no retire and no marker, and the
        short count instead makes the caller fail closed.
        """

        source = self.held_dir / handle
        destination = self.held_dir / action_key
        if not source.is_dir():
            return 0
        moved = 0
        destination.mkdir(parents=True, exist_ok=True)
        for token in _scan(source):
            landing = destination / token.name
            if landing.exists():
                continue
            try:
                os.rename(token, landing)
            except OSError:
                continue
            if token.name == cpu_admission.METADATA:
                moved += int((_read_json(landing) or {}).get("borrowed_cpu", 0))
            elif token.name == gpu_admission.METADATA:
                moved += int((_read_json(landing) or {}).get("borrowed_gpu", 0))
            else:
                moved += 1
        try:
            source.rmdir()
        except OSError:
            pass
        return moved

    def abandon_acquire(self, handle: str) -> int:
        """Return a claimant's own private tokens.  Its tokens, nothing else."""

        return self._empty_into_free(self.held_dir / handle)

    def acquire(self, action_key: str, demand: Mapping[str, int]) -> bool:
        """Take every token the demand asks for, or none of them.

        The uncontended spelling of begin-then-commit, for a caller that has
        already decided the action is its own.  ``claim`` does not use it: the
        rename that decides ownership sits between the two halves.
        """

        handle = self.begin_acquire(action_key, demand)
        if handle is None:
            return False
        wanted = sum(int(v) for v in demand.values() if int(v) > 0)
        if self.commit_acquire(action_key, handle) < wanted:
            self.abandon_acquire(handle)
            self.release(action_key)
            return False
        return True

    def sweep_stale_acquisitions(
        self, *, grace_s: float = LEASE_TIMEOUT_S
    ) -> list[str]:
        """Free tokens a claimant took and never committed.

        The window between ``begin_acquire`` and ``commit_acquire`` is one
        claim-intent write and one rename, so a private directory older than
        the grace belongs to a claimant that died inside it.  Nothing else
        recovers those tokens: they are filed under a claimant, not an action
        key, so no claimed record names them and ``reap_stale``'s release by
        key cannot see them.

        **The grace is the lease timeout, not the heartbeat.**  This sweep
        decides that a claimant is dead with no heartbeat behind it, and that
        is the judgement ``reap_stale``'s own docstring records getting wrong
        at heartbeat length: it requeued a live seven-second action within a
        second of its claim (issue #36).  Waiting costs almost nothing here,
        because the private directory is under ``held/`` and is honestly
        counted as consumed the whole time, whereas sweeping a live claimant
        costs it its claim.  A sweep that does fire early is still not an
        over-admission: ``commit_acquire`` counts what it moved and its caller
        puts the item back rather than running it unreserved.

        The stamped host and pid buy back the common case.  When the claimant
        was on this host and its process is gone, there is nothing to wait for
        and the heartbeat interval is enough.  A reused pid reads as alive and
        waits the full grace, which is the safe direction.
        """

        swept: list[str] = []
        now = _now()
        local = socket.gethostname()
        for holder in _scan(self.held_dir):
            if not _is_acquisition(holder.name) or not holder.is_dir():
                continue
            started = _acquisition_clock(holder)
            if started is None:
                continue
            bound = grace_s
            claimant = _acquisition_claimant(holder.name)
            if (claimant is not None and claimant[0] == local
                    and not _process_alive(claimant[1])):
                bound = min(bound, HEARTBEAT_S)
            if now - started <= bound:
                continue
            self._empty_into_free(holder)
            swept.append(holder.name)
        return swept

    def release(self, action_key: str) -> int:
        """Return every token held for this action.  Safe to call twice."""

        return self._empty_into_free(self.held_dir / action_key)

    def _empty_into_free(self, holder: Path) -> int:
        """Rename every token under one holder back to ``free/``."""

        if not holder.is_dir():
            return 0
        released = 0
        self.free_dir.mkdir(parents=True, exist_ok=True)
        for token in _scan(holder):
            if token.name in (cpu_admission.METADATA, gpu_admission.METADATA):
                token.unlink(missing_ok=True)
                continue
            try:
                os.rename(token, self.free_dir / token.name)
            except OSError:
                continue
            released += 1
        try:
            holder.rmdir()
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
        """Which actions hold tokens here.

        Claimant-private acquisitions are excluded: they are named for the
        claimant, not for an action, and the contender that will own the action
        is not decided until its rename.  A caller asking which actions hold
        capacity would otherwise be handed a name no queue directory has.
        """

        return sorted(
            path.name for path in _scan(self.held_dir)
            if path.is_dir() and not _is_acquisition(path.name)
        )


class PoolQueue:
    """A directory on a shared filesystem that two or more boxes pull from."""

    def __init__(self, root: str | Path | None = None) -> None:
        # Read the module attribute at call time, not at definition time, so
        # a caller (or the test guard) that re-points ``DEFAULT_POOL_ROOT``
        # after import gets the root it named rather than the live store.
        self.root = Path(DEFAULT_POOL_ROOT if root is None else root)
        self._cpu_deferrals: dict[tuple[str, str], float] = {}
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

    def _entomb_claim(
        self, action_key: str, *, expect: Mapping[str, object] | None = None
    ) -> tuple[Path | None, bool]:
        """Move a claim aside so its own cleanup cannot delete its successor.

        ``finish`` and ``reap_stale`` both used to publish the item's next home
        and only afterwards unlink ``claimed/<key>.json`` and its lease.  A
        worker polling inside that window claims the newly published retry --
        its rename lands on the same claimed filename -- and the old finisher
        then deletes the *new* claim and the *new* lease on its way out.  The
        retry disappears from the live queue, and because no claimed record and
        no lease survive it, a crash of that second worker leaves a reservation
        no reaper can find.

        So the claim moves out of the way first, atomically, to a name no
        reader of ``claimed/`` treats as a claim, and only the tombstone is
        deleted afterwards.  A crash inside the window leaves the tombstone,
        which :meth:`sweep_finish_tombstones` recovers.

        ``expect`` is the claim the caller judged.  A caller reads a claim,
        decides what becomes of it, and only then moves it aside, and the
        claim can conclude and be re-claimed inside that gap -- the reaper's
        gap spans an ``archive_attempt`` write, which is an NFS round trip.
        Without the comparison the mover entombs a *live* claim it never
        judged, deletes that claim's lease, releases its reservation and
        republishes the item, leaving a second worker running an action
        nothing records it holds.  Returns the tombstone and whether the claim
        was the caller's: ``(None, False)`` says a different claim is there
        now and the caller must leave the key alone.  With no ``expect`` the
        move is unconditional, which is what a caller holding the only claim
        on the key wants.
        """

        tombstone = self.dir(CLAIMED) / (
            f"{action_key}.{int(_now() * 1_000_000)}.{socket.gethostname()}"
            f".{os.getpid()}.{uuid.uuid4().hex[:8]}{TOMBSTONE_SUFFIX}"
        )
        try:
            os.rename(self.item_path(CLAIMED, action_key), tombstone)
        except OSError:
            return None, True
        if expect is None:
            return tombstone, True
        entombed = _read_json(tombstone)
        if entombed is not None and not _same_claim(entombed, expect):
            # The rename is what makes this decidable.  Comparing before it
            # tests a name another process can replace between the read and
            # the move; afterwards these bytes are held exclusively and can be
            # put back.  Link rather than rename, so a claim that appeared in
            # the meantime is never replaced; if the link fails,
            # ``sweep_finish_tombstones`` recovers the record and the caller
            # has still touched nothing that was not its own.
            try:
                os.link(tombstone, self.item_path(CLAIMED, action_key))
            except OSError:
                pass
            else:
                tombstone.unlink(missing_ok=True)
            return None, False
        return tombstone, True

    def attempt_generation(self, record: Mapping[str, object]) -> str:
        """Stable directory name for one submission of a content-addressed key."""

        key = str(record.get("action_key") or "")
        published = record.get("published_unix")
        if len(key) != 64 or any(ch not in "0123456789abcdef" for ch in key):
            raise PoolContractError("attempt history requires a full action key")
        if (
            type(published) not in (int, float)
            or not math.isfinite(float(published))
        ):
            raise PoolContractError("attempt history requires published_unix")
        return pb.canonical_sha256(
            {"action_key": key, "published_unix": float(published)}
        )

    def attempt_path(self, record: Mapping[str, object], attempt: int) -> Path:
        """Immutable outcome path for one numbered attempt of one generation."""

        if type(attempt) is not int or attempt < 1:
            raise PoolContractError("attempt number must be a positive integer")
        key = str(record.get("action_key") or "")
        return (
            self.root
            / ATTEMPTS
            / key
            / self.attempt_generation(record)
            / f"{attempt:08d}.json"
        )

    def attempt_log_path(
        self,
        record: Mapping[str, object],
        attempt: int,
        stream: str,
        sha256: str,
    ) -> Path:
        """Immutable stdout/stderr path beside an attempt outcome."""

        if stream not in {"stdout", "stderr"}:
            raise PoolContractError(f"unknown attempt log stream: {stream!r}")
        if (
            not isinstance(sha256, str)
            or len(sha256) != 64
            or any(ch not in "0123456789abcdef" for ch in sha256)
        ):
            raise PoolContractError("attempt log requires a full SHA-256 digest")
        outcome = self.attempt_path(record, attempt)
        return outcome.parent / f"{attempt:08d}.{stream}.{sha256}.log"

    def ensure_layout(self) -> None:
        for state in _STATES:
            self.dir(state).mkdir(parents=True, exist_ok=True)
        (self.root / WORKERS).mkdir(parents=True, exist_ok=True)
        (self.root / ATTEMPTS).mkdir(parents=True, exist_ok=True)

    # -- what the fleet can actually run ---------------------------------

    def announce(
        self,
        *,
        host: str,
        tags: Sequence[str],
        has_gpu: bool,
        capacity: Mapping[str, int] | None = None,
        cpu_tiers: Mapping[str, Sequence[int]] | None = None,
        runtime_commit: str = "",
        observed_capacity: Mapping[str, int] | None = None,
        foreign: Mapping[str, int] | None = None,
        observed_detail: Mapping[str, object] | None = None,
        loops: int | None = None,
        timeout_ceiling_s: float | None = None,
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

        ``loops`` is how many PrismaBuild worker loops are running on the box
        that wrote this record, and it exists because the record itself cannot
        otherwise say: every loop on a box announces into the same
        ``workers/<host>.json``, so the file states *that the box is offering*
        and never *how many loops are*.  That number was load-bearing in the
        2026-09-06 diagnosis and had to be recovered by a hand process census
        on each box, because no series carried it -- twelve loops on one box
        against three on another means nothing as a bare number until you can
        see they were spawned in tranches with none exiting, which is a
        supervisor ratchet.

        It counts loops on the **box**, not loops announcing under this host
        name, because that is the only one of the two a loop can honestly
        measure: ``worker_loop.py`` reads ``socket.gethostname()`` once before
        its poll loop and holds that name for life, so after a rename the box
        runs loops announcing under two names and no loop can tell which of its
        siblings uses which.  The arithmetic that falls out of counting the box
        is the useful one: two host records whose ``loops`` agree, and whose
        sum exceeds either, are one physical box read as two live nodes (#244).

        ``None`` means the census was not taken or could not be read, and is
        written as an absent field for the same reason as the three above: a
        reader must not turn "not measured" into zero, and every loop published
        before this field existed announces without it.
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
            "cpu_tiers": dict(cpu_tiers or {}),
            "runtime_commit": str(runtime_commit),
            "announced_unix": _now(),
        }
        if loops is not None:
            record["loops"] = int(loops)
        if timeout_ceiling_s is not None:
            record["timeout_ceiling_s"] = float(timeout_ceiling_s)
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
            # An offer that went away while we were reading the list is a box
            # that left, which is the same answer as an offer that expired:
            # not currently placeable.  It is never a reason to refuse the
            # submission -- the box it described was at worst one candidate
            # among several.  ``tolerate_stale`` is what makes that true on
            # the shared filesystem the pool actually lives on (#208).
            record = _read_json(path, tolerate_stale=True)
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

    def placement_timeout_ceilings(
        self, item: Mapping[str, object], *, max_age_s: float = OFFER_TIMEOUT_S
    ) -> dict[str, float | None]:
        """The execution ceiling each box that could run this item announces.

        ``None`` for a box whose offer predates the field, which a reader must
        treat as "not measured" and never as unbounded -- the loops that
        starved #275 for six hours announced nothing, and reading their
        silence as "no limit" would reproduce the same false confidence one
        layer up.  An empty dict means nobody eligible has announced at all.

        Separate from ``placeable_hosts`` because the questions differ: that
        one asks whether any box CAN run the item, this one asks how long the
        boxes that can would let it.  A submitter needs both, and needs to be
        able to tell "no box will grant this" from "no box said".
        """

        live = self.offers(max_age_s=max_age_s)
        if not live:
            return {}
        ceilings: dict[str, float | None] = {}
        for offer in self._matching_offers(item, live=live):
            host = str(offer.get("host") or "?")
            announced = offer.get("timeout_ceiling_s")
            ceilings[host] = (
                float(announced)
                if isinstance(announced, (int, float)) and not isinstance(announced, bool)
                else None
            )
        return ceilings

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

    #: The fence marker ``fleet/slurm/cutover.sh`` writes in the queue root
    #: while it retires the pull queue's execution plane.  Same spelling as
    #: that script's ``FENCE_MARKER`` and ``rollback.sh``'s, which cannot
    #: import this module.
    FENCE_NAME = "cutover-fence.json"

    def fence(self) -> dict[str, object] | None:
        """What fenced this queue, or ``None`` when nothing has.

        The marker is the explanation and never the mechanism: the mechanism
        is the write bit on ``ready``, because during a cutover every producer
        and loop on the fleet is still running the published generation, which
        predates the fence and reads no marker.  A refusal that depended on
        this file would protect only the callers that already know about it.
        """

        try:
            data = json.loads(
                (self.root / self.FENCE_NAME).read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return None
        return data if isinstance(data, dict) else None

    def _refuse_if_fenced(self) -> None:
        """Turn the fence's EACCES into a refusal that names the cutover.

        Asked before ``ensure_layout``, so a queue whose ``ready`` does not
        exist yet is not mistaken for a fenced one.  A write into a fenced
        directory fails either way -- that is the point of doing it with the
        filesystem -- and what this adds is a producer being told which
        operation refused it and what to do instead, rather than a
        ``PermissionError`` raised from inside a rename.
        """

        ready = self.dir(READY)
        if not ready.is_dir() or os.access(ready, os.W_OK):
            return
        fenced = self.fence()
        if fenced is None:
            raise PoolContractError(
                f"{ready} is not writable, so this submission was refused "
                "rather than left in a queue it could not enter. No cutover "
                "fence marker explains it, so read the directory's mode."
            )
        raise PoolContractError(
            f"the pull queue is fenced: {ready} is not writable. "
            f"{fenced.get('reason') or 'fleet/slurm/cutover.sh fenced it'}. "
            f"Fenced at {fenced.get('fenced_unix')} from "
            f"{fenced.get('fenced_by')}, recorded in "
            f"{self.root / self.FENCE_NAME}. This submission was refused "
            "rather than stranded in a queue whose workers are being retired."
        )

    def publish(
        self,
        *,
        action_key: str,
        cas_root: str | Path,
        worker_script: str | Path,
        checkout_root: str | Path | None = None,
        checkout_snapshot: object | None = None,
        tags: Sequence[str] = (),
        needs_gpu: bool = False,
        priority: int = 0,
        resources: Mapping[str, int] | None = None,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        retry_safe: bool | None = None,
        container_owner: str | None = None,
    ) -> Path:
        """Enqueue one sealed action.  The action itself already lives in the CAS.

        ``resources`` is what this action needs to run on one box -- e.g.
        ``{"gpu": 1, "mem_gb": 8}``.  It is a claim about the action, made by
        the producer that knows it; a worker's ``capacity`` is the matching
        claim about the box.  Omitting it means the action is admitted on
        placement alone, which is the pre-ledger behaviour.
        """

        self._refuse_if_fenced()
        if not isinstance(action_key, str) or len(action_key) != 64:
            raise PoolContractError("action_key must be a 64-character digest")
        demand = {str(k): int(v) for k, v in dict(resources or {}).items()}
        if any(v < 0 for v in demand.values()):
            raise PoolContractError("resource demand must not be negative")
        if type(max_attempts) is not int or max_attempts < 1:
            raise PoolContractError("max_attempts must be a positive integer")
        if retry_safe is not None and type(retry_safe) is not bool:
            raise PoolContractError("retry_safe must be boolean or null")
        if retry_safe is False and max_attempts > 1:
            raise PoolContractError(
                "max_attempts greater than 1 contradicts retry_safe=false"
            )
        if container_owner is not None:
            self.container_marker(str(container_owner))  # validates the digest
        if checkout_snapshot is None:
            if checkout_root is None:
                raise PoolContractError(
                    "publish requires checkout_root or checkout_snapshot"
                )
            addressing: dict[str, object] = {
                "checkout_root": str(checkout_root)
            }
        else:
            if checkout_root is not None:
                raise PoolContractError(
                    "checkout_root and checkout_snapshot are mutually exclusive"
                )
            addressing = {
                "checkout_snapshot": pb.validate_pbrun_checkout_snapshot(
                    checkout_snapshot
                )
            }
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
            "worker_script": str(worker_script),
            "tags": normalize_placement_tags(tags),
            "needs_gpu": bool(needs_gpu),
            "priority": int(priority),
            "resources": demand,
            "attempts": 0,
            "max_attempts": max_attempts,
            "published_unix": _now(),
            "published_by": socket.gethostname(),
            **addressing,
        }
        if retry_safe is not None:
            item["retry_safe"] = retry_safe
        if container_owner is not None:
            item["container_owner"] = str(container_owner)
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

    #: Everything that describes the claim that has just ended.  A ready item
    #: is claimed by nobody and holds no tokens, so none of it may survive a
    #: requeue.  ``passes`` belongs to the aging sidecar, not to the item.
    _CLAIM_SCOPED_FIELDS = (
        "claimed_by", "claimed_unix", "claimed_host", "reserved_on", "passes", "gpu_admission",
        "cpu_allocation",
        "container_cleanup_pending", "container_cleanup_checked_unix",
        "container_cleanup_attempts", "container_cleanup_first_failed_unix",
        "stop_pending", "resource_scope", "resource_scope_cleanup",
        "finish_pending", "resource_scope_intent",
    )

    def _shape_as_ready_item(
        self, record: dict[str, object], *, action_key: str
    ) -> Path:
        """Turn a concluded claim back into a ready item; say where it goes.

        Three producers write into ``ready``: ``publish`` above, ``finish``'s
        retry branch and ``reap_stale``'s.  The two requeues each spelled the
        shape out for themselves and had drifted apart -- one stamped the
        outcome schema onto a queue item, the other left ``action_key`` to
        whatever the claim happened to carry -- so one directory held records
        a consumer could tell apart by which writer produced them.  Teaching
        the readers to accept both is the fix that drifts again; one writer is
        the fix that cannot.

        The rules are the ones ``publish`` already keeps.  ``schema`` says what
        kind of record this is, and a record in ``ready`` is an item, not an
        outcome.  The filename is the identity, because every consumer
        addresses an item by key.  Nothing claim-scoped survives.

        What does survive is the item's own history: ``attempts`` is what the
        next try is counted against, and ``status``, ``detail`` and
        ``attempt_history`` are how the last one went.
        """

        record["schema"] = POOL_ITEM_SCHEMA_V1
        record["action_key"] = action_key
        record["requeued_unix"] = _now()
        for field in self._CLAIM_SCOPED_FIELDS:
            record.pop(field, None)
        return self.item_path(READY, action_key)

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
            try:
                # An item claimed out from under this listing is one this poll
                # does not offer, which is the answer ``None`` already gives.
                # ``tolerate_stale`` is what keeps that true when the vanishing
                # reaches a cached directory handle as ``ESTALE`` rather than
                # ``ENOENT`` (#212, following #208).  The ``glob`` has already
                # succeeded, so the directory is live and the entry is not.
                record = _read_json(path, tolerate_stale=True)
            except PoolContractError:
                # A record nobody can parse is nobody's work.  Raising it out
                # of here took ``claim`` down on every box at once for one
                # foreign writer's truncated file, and ``reap_stale`` with it
                # -- so the sweep that files the thing was itself among the
                # casualties.  Skip it and keep serving; ``quarantine_orphans``
                # files it, and ``serve_once`` reaches that sweep before its
                # next claim.
                continue
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

    def _discard_claim_intent(self, action_key: str, *, owner: str) -> None:
        """Remove this claimant's intent marker, and only ever its own."""

        path = self.item_path(INTENT, action_key)
        marker = _read_json(path)
        if isinstance(marker, Mapping) and marker.get("owner") == owner:
            path.unlink(missing_ok=True)

    def write_lease(
        self,
        action_key: str,
        *,
        owner: str,
        child_pid: int | None = None,
        container_owner: str | None = None,
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

        lease = {
                "schema": POOL_LEASE_SCHEMA_V1,
                "action_key": action_key,
                "owner": owner,
                "host": socket.gethostname(),
                "pid": os.getpid(),
                "child_pid": int(child_pid) if child_pid is not None else None,
                "heartbeat_unix": _now(),
            }
        if container_owner is not None:
            lease["container_owner"] = str(container_owner)
        claim = _read_json(self.item_path(CLAIMED, action_key))
        if claim is not None and claim.get("claimed_by") == owner:
            for field in ("resource_scope", "resource_scope_intent", "resource_scope_cleanup", "claimed_unix"):
                if field in claim:
                    lease[field] = claim[field]
        _write_json_atomic(self.lease_path(action_key), lease)

    def ledger(self, host: str | None = None) -> ResourceLedger:
        return ResourceLedger(self.root / RESERVATIONS, host=host)

    def container_marker(self, owner: str) -> Path:
        """The durable signal that this action invoked the Docker shim."""

        if (len(owner) != 64
                or any(character not in "0123456789abcdef" for character in owner)):
            raise PoolContractError(
                "container_owner must be a 64-character hex digest")
        return self.root / CONTAINER_OWNERS / f"{owner}.used"

    def _scope_from_record(self, record: Mapping[str, object]) -> resource_scope.ResourceScope:
        control = record.get("resource_scope")
        if not isinstance(control, dict):
            raise PoolContractError("resource scope control must be an object")
        key = str(record.get("action_key") or "")
        if (record.get("claimed_host") or record.get("host")) != socket.gethostname():
            raise PoolContractError("resource scope cleanup must run on its claiming host")
        nonce = control.get("nonce")
        unit = "prismabuild-job" + hashlib.sha256(
            (key + str(nonce)).encode()).hexdigest()[:32] + ".slice"
        if (control.get("action_key") != key or control.get("scope_id") != unit
                or control.get("cgroup_path") != "/sys/fs/cgroup/prismabuild.slice/" + unit
                or control.get("socket_path") != str(resource_scope.BROKER_SOCKET)
                or not isinstance(control.get("token"), str)
                or len(control["token"]) != 64
                or any(c not in "0123456789abcdef" for c in control["token"])):
            raise PoolContractError("invalid resource scope recovery identity")
        scope = resource_scope.ResourceScope(
            key, nonce, control.get("memory_max_bytes"),
            self.ledger().base / "telemetry" / f"{key}.json",
            docker_owner=record.get("container_owner"),
            shape_key=cpu_admission.shape_key(record) if record.get("cas_root") else None,
            **({"gpu_memory_max_bytes": control["gpu_memory_max_bytes"]}
               if control.get("gpu_memory_max_bytes") is not None else {}),
        )
        scope.unit, scope.token = unit, control["token"]
        scope.cgroup_path = Path(control["cgroup_path"])
        started = control.get("started_monotonic")
        valid = (not control.get("create_recovered") and type(started) in (int, float) and math.isfinite(started)
                 and 0 <= started <= time.monotonic()
                 and control.get("boot_id") == Path("/proc/sys/kernel/random/boot_id").read_text().strip())
        # A reboot invalidates elapsed-time accounting, not exact broker
        # authority. Recovery must still stop/release the old scope safely.
        scope._pool_accounting_valid = valid
        if valid:
            scope.started = started
        return scope

    def _recover_resource_scope_creation(self, record: Mapping[str, object]) -> bool:
        """Reconcile durable pre-create identity; never create a kernel group."""
        intent = record.get("resource_scope_intent")
        key = str(record.get("action_key") or "")
        if (not isinstance(intent, dict) or intent.get("action_key") != key
                or intent.get("socket_path") != str(resource_scope.BROKER_SOCKET)
                or (record.get("claimed_host") or record.get("host")) != socket.gethostname()):
            raise PoolContractError("invalid resource scope creation recovery identity")
        scope = resource_scope.ResourceScope(
            key, intent.get("nonce"), intent.get("memory_max_bytes"),
            self.ledger().base / "telemetry" / f"{key}.json",
            docker_owner=record.get("container_owner"),
            **({"gpu_memory_max_bytes": intent["gpu_memory_max_bytes"]}
               if intent.get("gpu_memory_max_bytes") is not None else {}),
        )
        if not scope.recover_create():
            return False
        control = {**scope.control_record(), "create_recovered": True}
        path = self.item_path(CLAIMED, key)
        live = _read_json(path)
        if live is not None and _same_claim(live, record):
            live["resource_scope"] = control
            _write_json_atomic(path, live)
            self.write_lease(key, owner=str(record.get("claimed_by") or ""),
                             container_owner=record.get("container_owner"))
        if not isinstance(record, dict):
            raise PoolContractError("resource scope recovery record must be mutable")
        record["resource_scope"] = control
        return True

    @staticmethod
    def _sample_resource_scope(scope: resource_scope.ResourceScope) -> dict:
        telemetry = scope.sample()
        try:
            status = scope._request("status")
            if status.get("stop_reason"):
                telemetry["termination_reason"] = status["stop_reason"]
            if status.get("termination_evidence"):
                telemetry["termination_evidence"] = status["termination_evidence"]
        except (OSError, ValueError) as exc:
            telemetry["complete"] = False
            telemetry["errors"] = [*telemetry.get("errors", []),
                                   f"scope broker status unavailable: {exc}"]
        if not getattr(scope, "_pool_accounting_valid", True):
            telemetry["complete"] = False
            telemetry["errors"] = [*telemetry.get("errors", []),
                                   "scope accounting start belongs to another boot or is invalid"]
        resource_scope._atomic_json(scope.telemetry_path, telemetry)
        return telemetry

    @staticmethod
    def _resource_failure(telemetry: Mapping[str, object]) -> str | None:
        if telemetry.get("termination_evidence") and telemetry.get("termination_reason"):
            return str(telemetry["termination_reason"])
        # Descendant OOM victims do not prove this attempt exhausted its cap.
        # Parent-local OOM does, even before the kernel accounts a victim.
        if telemetry.get("oom_local", 0) > 0:
            return "memory_limit_oom"
        return None

    def _start_resource_scope(self, item: Mapping[str, object]) -> resource_scope.ResourceScope:
        key = str(item["action_key"])
        request = Path(str(item["cas_root"])) / "requests" / key[:2] / f"{key}.json"
        raw = pb._read_regular_file_nofollow(request, where="contained pool action request")
        action = pb.validate_action(pb._decode_strict_json(raw, where="contained pool action request"))
        demand = action["params"].get("demand")
        if action["action_key"] != key:
            raise PoolContractError("contained action request differs from claimed key")
        if demand is None and action["task"]["definition_id"] != "fleet/pbrun":
            # Existing generic producers (including Tessera) declare resources
            # through publish rather than action params. Preserve that trusted
            # producer contract; pbrun always binds demand into the sealed key.
            demand = item.get("resources")
        if not isinstance(demand, dict) or demand != item.get("resources"):
            raise PoolContractError("pool resource demand differs from sealed action demand")
        memory = demand.get("mem_gb")
        if type(memory) is not int or memory <= 0:
            raise PoolContractError("contained action needs a positive sealed mem_gb demand")
        gpu_memory = action["params"].get("gpu_memory_gb")
        gpu_kwargs = {}
        if gpu_memory is not None:
            if not demand.get("gpu"):
                raise PoolContractError("gpu_memory_gb requires GPU demand")
            try:
                gpu_kwargs["gpu_memory_max_bytes"] = gpu_admission.memory_budget_bytes(gpu_memory)
            except ValueError as exc:
                raise PoolContractError(f"gpu_memory_gb: {exc}") from exc
        if item.get("resource_scope") is not None or item.get("resource_scope_intent") is not None:
            raise PoolContractError("claim already owns a resource scope or creation intent")
        scope = resource_scope.ResourceScope(
            key, uuid.uuid4().hex, memory * 1024 ** 3,
            self.ledger().base / "telemetry" / f"{key}.json",
            docker_owner=item.get("container_owner"),
            shape_key=cpu_admission.shape_key(item),
            **gpu_kwargs,
        )
        path = self.item_path(CLAIMED, key)
        live = _read_json(path)
        if live is None or not _same_claim(live, item):
            raise PoolContractError("claim changed before scope creation")
        intent = {"action_key": key, "nonce": scope.nonce,
                  "memory_max_bytes": scope.memory_max_bytes,
                  "socket_path": str(scope.socket_path), **gpu_kwargs}
        live["resource_scope_intent"] = intent
        _write_json_atomic(path, live)
        if isinstance(item, dict):
            item["resource_scope_intent"] = intent
        self.write_lease(key, owner=str(item.get("claimed_by") or ""),
                         container_owner=item.get("container_owner"))
        # The broker may finish after a client timeout or worker crash. Both
        # claim and lease now retain the exact nonce needed for reconciliation.
        control = scope.create()
        control["started_monotonic"] = scope.started
        control["boot_id"] = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
        try:
            live = _read_json(path)
            if live is None or not _same_claim(live, item):
                raise PoolContractError("claim changed before scope launch")
            live["resource_scope"] = control
            _write_json_atomic(path, live)
            if isinstance(item, dict):
                item["resource_scope"] = control
            self.write_lease(key, owner=str(item.get("claimed_by") or ""),
                             container_owner=item.get("container_owner"))
        except BaseException:
            # Nothing has launched yet, so this scope can be stopped without
            # any process census. Broker failures remain visible to recovery.
            scope.terminate_owned("scope ownership could not be persisted")
            scope.release()
            raise
        return scope

    def cleanup_action_containers(
        self, record: Mapping[str, object], *, reason: str = "completion"
    ) -> dict[str, object]:
        """Prove both direct and Docker payloads stopped before tokens return."""
        if record.get("resource_scope") is None and record.get("resource_scope_intent") is not None:
            try:
                self._recover_resource_scope_creation(record)
            except Exception as exc:                                 # noqa: BLE001
                # The third enumerated tuple on this path, and it is retired
                # for the reason the other two were (#286, #288): the list is
                # of the errors somebody thought of, and this call reaches the
                # broker socket, the shared mount and JSON.  An escape here is
                # fail-OPEN in the expensive direction -- the caller never
                # receives the dict it indexes ``["complete"]`` on, so the
                # claim is never concluded and the lease decays to
                # ``lease_lost_max_attempts`` for a payload that already ran.
                #
                # ``Exception`` and not ``BaseException``: a KeyboardInterrupt
                # or SystemExit still stops the process.
                return {"complete": False, "used": True, "removed": [], "remaining": [],
                        "error": f"resource scope creation reconciliation incomplete: {type(exc).__name__}: {exc}"}
        if record.get("resource_scope") is None:
            return self._cleanup_action_containers(record)
        prior = record.get("resource_scope_cleanup")
        if (isinstance(prior, dict) and prior.get("complete") is True
                and prior.get("nonce") == record["resource_scope"].get("nonce")):
            return {"complete": True, "used": True, "removed": [], "remaining": [],
                    "resource_scope": prior}
        try:
            scope = self._scope_from_record(record)
            scope.terminate_owned(reason)
            containers = self._cleanup_action_containers(record)
            if not containers["complete"]:
                return containers
            telemetry = self._sample_resource_scope(scope)
            released = scope.release()
            cleanup = {"complete": True, "released": released, "telemetry": telemetry,
                       "checked_unix": _now(), "nonce": scope.nonce}
            key = str(record["action_key"])
            path = self.item_path(CLAIMED, key)
            live = _read_json(path)
            if live is not None and _same_claim(live, record):
                live["resource_scope_cleanup"] = cleanup
                _write_json_atomic(path, live)
            if isinstance(record, dict):
                record["resource_scope_cleanup"] = cleanup
            try:
                cpu_admission.record_completion(self.ledger(), record, telemetry)
            except Exception as exc:                                 # noqa: BLE001
                # Deliberately every exception, and the narrow tuple that was
                # here is the defect.  By this line the payload has stopped,
                # the tokens are back and the cleanup record is written; all
                # that is left is learning a shape, and ``record_completion``
                # says of itself that it is "worth having and never worth
                # waiting for", with "failure to attribute produces no learned
                # credit" as its own contract.  A call never worth waiting for
                # is never worth losing an action over.
                #
                # Enumerating what it can raise is what failed.  It reaches a
                # whole subsystem -- the admission lock, ``/proc``, the shared
                # mount, JSON -- and two of that subsystem's honest refusals
                # are bare ``RuntimeError``: ``box_state`` on a directory this
                # uid does not own, and ``Controller.locked`` on a lock file
                # that is not a private regular file.  ``AdmissionBusy`` is a
                # *subclass* of ``RuntimeError`` and is caught inside, which is
                # exactly what made the gap easy to miss.
                #
                # Neither was in the tuple, so the raise escaped this method
                # after ``scope.release()`` and before the caller could finish
                # the claim: the payload had completed, the claim had not, and
                # the lease stopped being renewed until the reaper recorded
                # ``lease_lost_max_attempts``.  Observed on sparky and
                # dl380g10 on 2026-09-06 while their admission directories
                # were mode 0770 (#281, #286).
                #
                # ``Exception`` and not ``BaseException``: a KeyboardInterrupt
                # or SystemExit still stops the process.
                cleanup["learning_error"] = f"{type(exc).__name__}: {exc}"
            return {**containers, "resource_scope": cleanup}
        except Exception as exc:                                     # noqa: BLE001
            # Every exception, and for the opposite reason to the inner
            # handler above.  This block is the part that PROVES the payload
            # stopped -- the resource broker over a socket, Docker, the shared
            # mount -- and an unexpected raise here escaped the method
            # entirely.  All four callers (``finish``, ``reap_stale``, the
            # lease sweep, ``withdraw``) index ``["complete"]`` on a dict they
            # then never receive, so the payload had run, the claim was never
            # concluded, the lease stopped being renewed, and the reaper
            # recorded ``lease_lost_max_attempts``.  That is fail-OPEN in the
            # way that costs the work (#286, #288).
            #
            # ``complete: False`` is the honest answer instead: cleanup could
            # not be proved.  It is fail-closed -- the claim and its tokens are
            # retained and a local reaper retries -- and it is deliberately NOT
            # a decision to release capacity for a payload nobody has shown to
            # have stopped.  A GPU an action still holds must not be handed to
            # somebody else because the box gave up asking.
            #
            # What the old crash bought was a signal: it ran up
            # ``MAX_CONSECUTIVE_ERRORS`` and took the box out of service.  That
            # signal is replaced rather than dropped -- ``_note_cleanup_attempt``
            # counts the retries and dates the first failure, so a cleanup that
            # can never succeed is a visible pinned claim instead of an
            # invisible one.  Relying on the crash was relying on a handler
            # written for bad ITEMS, which had already misfired once: a 0770
            # admission directory made every box run that counter up while
            # announcing full capacity (#281).
            #
            # ``Exception`` and not ``BaseException``: a KeyboardInterrupt or
            # SystemExit still stops the process.
            return {"complete": False, "used": True, "removed": [], "remaining": [],
                    "error": f"resource scope cleanup incomplete: {type(exc).__name__}: {exc}"}

    @staticmethod
    def _note_cleanup_attempt(
        pending: dict[str, object], prior: Mapping[str, object] | None,
        cleanup: Mapping[str, object],
    ) -> None:
        """Record that cleanup was tried again and still could not be proved.

        One writer for all three sites that retain a claim on unproven cleanup
        (``finish``, ``reap_stale``, ``withdraw``), because a count only two of
        them increment measures nothing.

        The two numbers answer the question the retry loop cannot answer about
        itself: a cleanup pending for three seconds and one pending for six
        hours and four hundred attempts write the same ``container_cleanup_
        pending`` record, and an operator acts on them completely differently.
        ``container_cleanup_checked_unix`` already said when it was last tried,
        which is the one thing that is always recent.

        No bound is applied here on purpose.  Concluding such a claim means
        releasing tokens for a payload nobody proved had stopped, and that is a
        fleet policy decision about hardware, not a defect fix -- see #288.
        What this makes possible is deciding it on evidence.
        """

        attempts = (prior or {}).get("container_cleanup_attempts")
        pending["container_cleanup_attempts"] = (
            int(attempts) + 1 if isinstance(attempts, int) and not isinstance(attempts, bool)
            else 1)
        first = (prior or {}).get("container_cleanup_first_failed_unix")
        pending["container_cleanup_first_failed_unix"] = (
            float(first) if isinstance(first, (int, float)) and not isinstance(first, bool)
            else _now())
        pending["container_cleanup_pending"] = dict(cleanup)
        pending["container_cleanup_checked_unix"] = _now()

    def _cleanup_action_containers(
        self, record: Mapping[str, object]
    ) -> dict[str, object]:
        """Remove and verify this action's detached Docker payloads.

        No marker means the Docker shim was never entered.  Once it exists,
        uncertainty is fail-closed: only the claiming host may consult its
        local daemon, an in-flight shim lock is not raced, and every query or
        removal error leaves the claim and reservation in place.
        """

        raw_owner = record.get("container_owner")
        if raw_owner is None:
            return {"complete": True, "used": False, "removed": [], "remaining": []}

        descriptor: int | None = None
        # The boundary starts here, not at ``os.open``.  Everything between
        # this line and the payload proof reads the shared mount -- the marker
        # path, its stat, the hostname -- and a raise from any of it used to
        # leave this method entirely.  ``marker.exists()`` was the live one:
        # ``Path.exists`` re-raises an errno outside ``ENOENT/ENOTDIR/EBADF/
        # ELOOP``, and ESTALE on an NFS handle is outside it, so a stale marker
        # handle escaped rather than answering ``complete: False``.  This is
        # the same fail-OPEN shape as #286/#288 in the sibling path: all four
        # callers index ``["complete"]`` on a dict they never receive, so the
        # payload has run, the claim is never concluded, and the lease decays
        # to ``lease_lost_max_attempts``.  The no-scope path reaches here
        # through ``cleanup_action_containers``'s early return, *outside* that
        # method's broad handler, so this is where it has to be caught.
        try:
            try:
                marker = self.container_marker(str(raw_owner))
            except PoolContractError as exc:
                return {
                    "complete": False,
                    "used": True,
                    "removed": [],
                    "remaining": [],
                    "error": str(exc),
                }
            if not marker.exists():
                return {"complete": True, "used": False, "removed": [], "remaining": []}

            holder = record.get("claimed_host") or record.get("host")
            local = socket.gethostname()
            if isinstance(holder, str) and holder and holder != local:
                return {
                    "complete": False,
                    "used": True,
                    "removed": [],
                    "remaining": [],
                    "error": f"container belongs to {holder}; cleanup must run there",
                }

            descriptor = os.open(
                marker,
                os.O_RDWR | getattr(os, "O_NOFOLLOW", 0),
            )
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return {
                    "complete": False,
                    "used": True,
                    "removed": [],
                    "remaining": [],
                    "error": "Docker ownership transaction is still running",
                }
            before = _docker_owned_container_ids(str(raw_owner))
            removed = _docker_remove_containers(before)
            remaining = _docker_owned_container_ids(str(raw_owner))
            complete = not remaining
            if complete:
                marker.unlink(missing_ok=True)
            return {
                "complete": complete,
                "used": True,
                "removed": removed,
                "remaining": remaining,
            }
        except Exception as exc:                                     # noqa: BLE001
            # Every exception, for the reason the sibling path already records
            # (#286, #288): this block is the part that PROVES the payload
            # stopped -- Docker, the shared mount, a marker stat -- and the
            # enumerated tuple was written against the errors somebody thought
            # of.  ``complete: False`` is the honest answer to any of them:
            # the claim and its tokens are retained, ``_note_cleanup_attempt``
            # counts the retry, and nothing releases capacity for a payload
            # nobody has shown to have stopped.
            #
            # ``Exception`` and not ``BaseException``: a KeyboardInterrupt or
            # SystemExit still stops the process.
            return {
                "complete": False,
                "used": True,
                "removed": [],
                "remaining": [],
                "error": f"{type(exc).__name__}: {exc}",
            }
        finally:
            if descriptor is not None:
                os.close(descriptor)

    @staticmethod
    def demand_of(item: Mapping[str, object]) -> dict[str, int]:
        raw = item.get("resources") or {}
        if not isinstance(raw, Mapping):
            raise PoolContractError("pool item resources must be an object")
        return {str(k): int(v) for k, v in raw.items() if int(v) > 0}

    def _defer_fallback(self, item: Mapping, demand: Mapping) -> bool:
        """Give a compatible host with free preferred CPUs up to 20s to claim.

        Offers and remote ledger scans are advisory snapshots, not an atomic
        fleet allocation. The bounded wait prevents stale-but-fresh offers
        from stranding work. A host that cannot fit the whole demand never
        delays another host, nor does incompatible placement.
        """
        identity = (str(item["action_key"]), repr(item.get("published_unix")))
        started = self._cpu_deferrals.setdefault(identity, time.monotonic())
        if time.monotonic() - started >= 20.0:
            return False
        for offer in self._matching_offers(item, live=self.offers()):
            host = str(offer.get("host") or "")
            if not host or host == socket.gethostname():
                continue
            tiers = offer.get("cpu_tiers")
            if not isinstance(tiers, Mapping) or not tiers.get("preferred"):
                continue
            remote = self.ledger(host)
            if _read_json(remote.base / "cpu-map.json") != tiers:
                continue
            free = remote.available()
            observed = offer.get("observed_capacity") or {}
            if (remote.free_preferred(tiers) >= demand.get("cpu", 0)
                    and all(free.get(k, 0) >= n and observed.get(k, free[k]) >= n
                            for k, n in demand.items())):
                return True
        return False

    def claim(
        self, *, tags: Iterable[str] = (), has_gpu: bool = False,
        owner: str | None = None, capacity: Mapping[str, int] | None = None,
        cpu_tiers: Mapping[str, Sequence[int]] | None = None,
        adaptive_cpu: bool = False,
    ) -> dict[str, object] | None:
        ledger = self.ledger()
        tiers = cpu_tiers or _read_json(ledger.base / "cpu-map.json")
        if adaptive_cpu and capacity is not None and tiers is not None:
            controller = cpu_admission.Controller(ledger, tiers)
            try:
                with controller.locked():
                    return self._claim(tags=tags, has_gpu=has_gpu, owner=owner,
                                       capacity=capacity, cpu_tiers=tiers,
                                       controller=controller,
                                       gpu_controller=gpu_admission.Controller(ledger) if has_gpu else None)
            except cpu_admission.AdmissionBusy:
                # Another loop on this box is mid-decision.  Everything under
                # that lock -- the headroom read, the ``ready`` scan, the
                # record rename, the lease, the tokens -- is on the shared
                # mount, so waiting here means waiting on a filesystem a
                # different machine controls, and the whole box waits with us.
                #
                # ``None`` is already this method's answer for "nothing this
                # box may admit right now", and ``serve_once`` documents it as
                # back-pressure to poll against rather than an empty queue.
                # Returning it hands the loop straight back to its own poll
                # cadence, where announcing lives: the box keeps saying what
                # it is while a sibling is slow, instead of going silent and
                # letting its offer expire.
                return None
        return self._claim(tags=tags, has_gpu=has_gpu, owner=owner,
                           capacity=capacity, cpu_tiers=cpu_tiers)

    def _claim(
        self,
        *,
        tags: Iterable[str] = (),
        has_gpu: bool = False,
        owner: str | None = None,
        capacity: Mapping[str, int] | None = None,
        cpu_tiers: Mapping[str, Sequence[int]] | None = None,
        controller: cpu_admission.Controller | None = None,
        gpu_controller: gpu_admission.Controller | None = None,
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
            if cpu_tiers is None:
                cpu_tiers = _read_json(ledger.base / "cpu-map.json")
            if cpu_tiers is not None:
                cpu_tiers = ledger.configure_cpu_tiers(cpu_tiers)
                if int(capacity.get("cpu", 0)) > sum(map(len, cpu_tiers.values())):
                    raise PoolContractError("CPU capacity exceeds the inherited CPU map")
                ledger.retire_free_capacity({"cpu": int(capacity.get("cpu", 0))})
            ledger.ensure_capacity(capacity)
            total = ledger.capacity()
        ready = self.ready_items()
        live_generations = {(str(item.get("action_key", "")), repr(item.get("published_unix")))
                            for item in ready}
        for generation in list(self._cpu_deferrals):
            if generation not in live_generations:
                self._cpu_deferrals.pop(generation, None)
        for item in ready:
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
            reservation_demand = dict(demand)
            if gpu_controller is not None and demand.get("gpu"):
                # Historical slot counts expressed sharing, not device count.
                # Preserve the sealed demand but reserve this worker's single
                # physical GPU; the controller keeps multi-slot work exclusive.
                reservation_demand["gpu"] = 1
            handle: str | None = None
            adaptive = None
            adaptive_gpu = None
            if controller is not None and not demand:
                # Adaptive admission needs a durable reservation to make an
                # unknown CPU consumer visible to subsequent measurements.
                # Empty legacy demand has no holder; keep it queued instead.
                self.record_pass(key)
                continue
            if ledger is not None and demand:
                if any(total.get(kind, 0) < need for kind, need in reservation_demand.items()):
                    continue      # never fits this box; not this box's to hold
                if controller is not None:
                    adaptive = controller.decision(item, demand)
                    if adaptive is None:
                        self.record_pass(key)
                        continue
                if gpu_controller is not None and demand.get("gpu"):
                    adaptive_gpu = gpu_controller.decision(item, demand)
                    if adaptive_gpu is None:
                        self.record_pass(key)
                        continue
                    gpu_controller.reserve_probe(adaptive_gpu)
                if adaptive_gpu is not None:
                    handle = ledger.begin_acquire(key, reservation_demand, adaptive=adaptive,
                                                  cpu_tiers=cpu_tiers,
                                                  adaptive_gpu=adaptive_gpu)
                else:
                    handle = (ledger.begin_acquire(key, demand) if adaptive is None else
                              ledger.begin_acquire(key, demand, adaptive=adaptive,
                                                   cpu_tiers=cpu_tiers))
                if handle is None:
                    denials = self.record_pass(key)
                    if (denials >= STARVATION_FLOOR
                            and self.withhold_age(key) <= WITHHOLD_CEILING_S):
                        # Wired to the decision: stop letting smaller work pass it.
                        return None
                    # Past the ceiling it keeps its passes -- and so its place at
                    # the head of the ordering -- but stops holding the box shut
                    # for work it cannot do anything with.
                    continue
            if (ledger is not None and handle is not None and cpu_tiers is not None
                    and ledger.cpu_allocation(handle, cpu_tiers)["fallback"]
                    and self._defer_fallback(item, demand)):
                ledger.abandon_acquire(handle)
                continue
            # Intent precedes the claim, so a crash in between leaves evidence.
            self._write_claim_intent(key, owner=owner)
            src = self.item_path(READY, key)
            dst = self.item_path(CLAIMED, key)
            try:
                os.rename(src, dst)
            except (FileNotFoundError, NotADirectoryError):
                if ledger is not None and handle is not None:
                    # Lost the race: hold nothing -- and return only what THIS
                    # claimant took.  Releasing by action key here returned the
                    # winner's reservation and let a third action be admitted
                    # on top of it.
                    ledger.abandon_acquire(handle)
                # Leave no evidence of a claim that did not happen.  The marker
                # is written by rename, so this claimant's copy replaced
                # whatever was there -- and if the winner wrote first, the
                # marker now names the box that LOST while still passing the
                # generation check (#272).
                #
                # Only while it is still this claimant's own: ``owner`` is
                # unique per claimant and the marker carries it.  The check and
                # the unlink are two operations on a shared mount, so a marker
                # written between them is removed as well -- but that leaves no
                # marker, which ``resolve_claim_holder`` already answers as
                # "nobody said", rather than a marker naming the wrong box.
                # The ledger is the exact answer either way; this only keeps
                # the fallback from being confidently wrong.
                self._discard_claim_intent(key, owner=owner)
                continue
            moved = _read_json(dst) or item
            if (not self._placement_matches(moved, tags=tagset, has_gpu=has_gpu)
                    or self.demand_of(moved) != demand):
                # Admission described the scanned generation. A replacement
                # may need a different host or more tokens; put it back for a
                # fresh admission before committing this claimant's tokens.
                if ledger is not None and handle is not None:
                    ledger.abandon_acquire(handle)
                try:
                    os.link(dst, src)
                except OSError:
                    # A still newer submission may own ready already. Leave
                    # the moved record for the reaper, as below.
                    pass
                else:
                    dst.unlink(missing_ok=True)
                    self.item_path(INTENT, key).unlink(missing_ok=True)
                continue
            if ledger is not None and handle is not None:
                # Won the rename, so the reservation stops belonging to this
                # claimant and starts belonging to the action.  Every branch
                # below releases by action key, which is correct only once the
                # tokens are filed under it.
                if (ledger.commit_acquire(key, handle) < sum(reservation_demand.values())
                        or (adaptive is not None and _read_json(
                            ledger.held_dir / key / cpu_admission.METADATA) is None)
                        or (adaptive_gpu is not None and _read_json(
                            ledger.held_dir / key / gpu_admission.METADATA) is None)):
                    # A stale-acquisition sweep took part of the reservation,
                    # or tokens of an earlier incarnation are filed under this
                    # key.  Fail closed rather than run unreserved: ``dst`` is
                    # still byte-identical to the ready record, because the
                    # rewrite below has not happened yet, so putting it back
                    # restores the item exactly as it was.
                    ledger.abandon_acquire(handle)
                    ledger.release(key)
                    # Link rather than rename.  ``publish`` writes ``ready``
                    # unconditionally, so a re-submission of this key can
                    # already be sitting there, and a rename would replace that
                    # new generation with these older bytes and lose the
                    # request.  If it is there, leave the claim for the reaper
                    # instead: an extra reaper cycle costs one attempt, a
                    # clobbered generation costs the whole submission.
                    try:
                        os.link(dst, src)
                    except OSError:
                        pass
                    else:
                        dst.unlink(missing_ok=True)
                        self.item_path(INTENT, key).unlink(missing_ok=True)
                    continue
            terminal = self.terminal_outcome_covers(moved, action_key=key)
            if terminal is not None:
                # A stale reaper can put a generation back in ``ready`` after
                # its outcome was filed, or the outcome can land between the
                # ready scan and this rename.  The rename is the last boundary
                # at which the payload is definitely not executing.
                if ledger is not None:
                    ledger.release(key)
                state, outcome = terminal
                self._file_superseded(
                    moved, key=key, kind="terminal-claim", status="dropped",
                    dropped_unix=_now(), dropped_host=socket.gethostname(),
                    reason="claim lost to an outcome for the same generation "
                           f"filed under {state}",
                    terminal_status=outcome.get("status"),
                )
                dst.unlink(missing_ok=True)
                self.passes_path(key).unlink(missing_ok=True)
                continue
            if self.withdrawal_covers(moved, action_key=key) is not None:
                # Withdrawn between the scan above and this rename.  The window
                # is microseconds wide and closing it here costs one listing on
                # a path taken once per claim; leaving it open costs a cancelled
                # action a full run before ``execute`` notices.
                if ledger is not None:
                    ledger.release(key)
                self._file_superseded(
                    moved, key=key, kind="dropped", status="dropped",
                    dropped_unix=_now(), dropped_host=socket.gethostname(),
                    reason="withdrawn between the ready scan and the claim",
                )
                dst.unlink(missing_ok=True)
                continue
            # From ``moved``, not from ``item``: past the rename the bytes in
            # ``claimed`` are the item, and the scan's copy may be a generation
            # ``publish`` has already replaced.  Rebuilding the claim from the
            # scan wrote that replaced generation back over the one the rename
            # moved, so the worker ran a submission nobody had asked for and
            # ``finish`` filed the outcome under the retired ``published_unix``
            # -- where the waiter on the live one never looked.  The terminal
            # and withdrawal guards above already read ``moved`` for this
            # reason; the record this method returns is the last place that
            # still did not.
            claimed = dict(moved)
            # ``passes`` is not a field of the item; it is the aging sidecar,
            # which ``ready_items`` stamps on its copy so the ready ordering
            # can read it and which this method deletes four lines below.
            # Copying it into the claim freezes a denial count into the
            # claimed record, into every attempt archived from it, and into
            # the done or failed record it becomes -- a number describing a
            # counter that no longer exists, on a record no admission decision
            # ever reads.
            claimed.pop("passes", None)
            claimed.pop("cpu_allocation", None)
            claimed.pop("gpu_admission", None)
            self._cpu_deferrals.pop((key, repr(moved.get("published_unix"))), None)
            claimed["claimed_by"] = owner
            claimed["claimed_unix"] = _now()
            claimed["claimed_host"] = socket.gethostname()
            if ledger is not None and cpu_tiers is not None and demand.get("cpu", 0):
                claimed["cpu_allocation"] = ledger.cpu_allocation(key, cpu_tiers)
            if adaptive_gpu is not None:
                claimed["gpu_admission"] = _read_json(ledger.held_dir / key / gpu_admission.METADATA)
            claimed["reserved_on"] = socket.gethostname() if demand else None
            _write_json_atomic(dst, claimed)
            self.write_lease(
                key,
                owner=owner,
                container_owner=(str(claimed["container_owner"])
                                 if claimed.get("container_owner") else None),
            )
            self.passes_path(key).unlink(missing_ok=True)
            if controller is not None and adaptive is not None:
                controller.admitted(adaptive)
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

    def claim_intent_age(self, action_key: str) -> float | None:
        """Seconds since a claimant declared intent, or ``None`` without one.

        The intent marker precedes the claim rename, so it is the only clock
        that exists for a claimed record ``claim()`` has not yet rewritten.
        """

        record = _read_json(self.item_path(INTENT, action_key))
        if record is None:
            return None
        declared = record.get("intent_unix")
        if not isinstance(declared, (int, float)):
            return None
        return _now() - float(declared)

    def claim_intent_host(self, action_key: str, record: Mapping[str, object]) -> str | None:
        """The box that declared intent to claim this generation, if it said.

        ``claim`` writes the intent marker *before* the rename and rewrites the
        record with ``claimed_host`` after it, so a claimant blocked in between
        leaves a claim that names no box at all.  Reaped, that becomes a
        terminal record whose only hostname is the reaper's -- and a claim must
        not be able to be lost more anonymously than it was taken.

        Generation-scoped, because the marker outlives the claim it belongs to:
        nothing unlinks it on the success path, so a key republished after an
        earlier run still carries that run's marker until the next claimant
        overwrites it.  A marker older than the record's own publication
        describes a different generation and names the wrong box, so it is
        refused rather than guessed with.
        """

        marker = _read_json(self.item_path(INTENT, action_key))
        if marker is None:
            return None
        host = marker.get("host")
        declared = marker.get("intent_unix")
        published = record.get("published_unix")
        if not isinstance(host, str) or not host:
            return None
        if not isinstance(declared, (int, float)) or isinstance(declared, bool):
            return None
        if isinstance(published, (int, float)) and not isinstance(published, bool):
            if float(declared) < float(published):
                return None
        return host

    def claim_reservation_hosts(self, action_key: str) -> list[str]:
        """Every box whose ledger holds committed tokens for this action.

        The ledger is the *effect* of the rename that decides ownership, not a
        report of it: ``begin_acquire`` files a claimant's tokens under a
        private ``held/<handle>`` precisely because the owner is undecided
        while they are taken, and ``commit_acquire`` -- "called by the winner
        of the ready-to-claimed rename, and by nobody else" -- is what moves
        them to ``held/<action_key>``.  So this directory exists on the winner
        and can exist nowhere else, and a loser cannot appear here however the
        race went.  ``release`` rmdirs the holder, so an emptied one does not
        linger as a false answer.

        A list rather than a host, because more than one is a contradiction
        the ledger's own invariant forbids and a caller must be able to refuse
        rather than pick.
        """

        return sorted(
            directory.name
            for directory in _scan(self.root / RESERVATIONS)
            if (directory / "held" / action_key).is_dir()
        )

    def resolve_claim_holder(
        self, action_key: str, record: Mapping[str, object]
    ) -> str | None:
        """The box holding this claim, by the best evidence that exists.

        Read in this order, and the order is the point:

        1. **The ledger.**  Exact, and derived from the rename itself, so it
           cannot name a loser (#272).  It is silent only when the claim holds
           no tokens -- a zero demand -- which is also when naming the wrong
           box costs the ledger nothing.
        2. **The intent marker.**  A proxy: written *before* the rename, so it
           names a claimant rather than the winner.  ``_write_claim_intent``
           writes by rename, so a loser that wrote after the winner replaced
           the winner's marker, and both pass the generation check.  A losing
           claimant now removes its own marker, which shrinks that window
           without closing it -- the removal is a read-then-unlink on a shared
           mount and can only ever degrade to no marker at all, which is the
           honest unknown this method already returns.

        ``AmbiguousClaimHolder`` means multiple ledgers name different boxes;
        callers must retain the claim and reservations rather than conclude it.
        ``None`` means nothing named a box.  Callers must not substitute the
        local hostname for it: the box asking is almost never the holder, and
        releasing against its ledger moves nothing while reporting a number
        that looks like it did.
        """

        hosts = self.claim_reservation_hosts(action_key)
        if len(hosts) == 1:
            return hosts[0]
        if hosts:
            # Two ledgers holding one action contradicts ``commit_acquire``'s
            # single-winner rule.  Refusing is the only answer that cannot
            # make it worse by choosing.
            raise AmbiguousClaimHolder(
                f"ambiguous claim holder for {action_key}: committed reservations "
                f"on {', '.join(hosts)}; claim and reservations retained"
            )
        return self.claim_intent_host(action_key, record)

    def claim_holder_pids(self, host: str | None = None) -> set[int]:
        """The pids on ``host`` that hold a claim of this queue right now.

        ``claim`` writes the lease before it returns and ``finish`` unlinks it,
        so this is exactly the set of loops between those two points --
        including one that has claimed an action and has not yet started
        anything to run it.  Nothing about that loop's process tree says so,
        which is why the question is asked here: the lease carries the
        claiming loop's own pid, and has since it was written.

        Host-qualified, because the queue is shared and a pid is a name only
        one box can resolve.  A lease naming another box is another box's
        business.

        Missing or unreadable ownership is unknown, not idle. A claim is
        renamed before its first lease is written, so inspect claimed items
        and refuse to authorize a signal while any ownership is unresolved.
        """

        host = socket.gethostname() if host is None else host
        pids: set[int] = set()
        for claim in _glob(self.dir(CLAIMED), "*.json"):
            lease = claim.with_suffix(".lease")
            record = _read_json(lease)
            if record is None:
                if not claim.exists():
                    continue  # Finished while the directory was being read.
                raise PoolContractError(f"claim ownership is unknown: {claim}")
            pid = record.get("pid")
            if (not isinstance(record.get("host"), str) or not record["host"]
                    or not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0):
                raise PoolContractError(f"claim ownership is invalid: {lease}")
            if record["host"] == host:
                pids.add(pid)
        return pids

    def _sweep_due(self, *, interval_s: float = HEARTBEAT_S) -> bool:
        """Claim this box's turn to run the reaper, or decline it.

        ``serve_once`` used to reap on every poll of every loop, and the
        reaper reads every claimed record *and its lease*.  A box runs many
        loops (one per class, grown by ``supervise``), and each of them polls
        about once a second while the queue is non-empty, so the box as a
        whole opened every ``claimed/<key>.lease`` in the pool tens of times a
        second -- files that a *different* box is writing to once per
        heartbeat.  Measured on ``dl380g10`` 2026-09-06 at load 0.44 across 80
        CPUs with no process in ``D``: three of twenty ``pb-queue`` file reads
        took 34.2 s, 40.3 s and 11.9 s (the rest under a millisecond), fifteen
        of eighteen worker loops sat in ``__break_lease`` simultaneously, and
        the box's offer aged past ``OFFER_TIMEOUT_S`` -- an idle 80-CPU box
        invisible to placement because its poll could not get back to
        ``announce``.

        The per-read cost differs by box and the throttle does not depend on
        which one applies.  ``dl380g10`` exports the pool, so its local open of
        a remotely-written file recalls that writer's NFSv4 delegation and
        blocks on the remote client; that is why it pays most.  ``sparklina``
        is an ordinary client and was caught with all three of its loops in
        ``D`` on ``rpc_wait_bit_killable`` / ``do_renameat2`` /
        ``open_last_lookups`` at box load 3.5, and ``sparky`` took its own turn
        at an expired offer while ``dl380g10`` was live.  The stale role
        rotates, so the thing to reduce is the multiplier the boxes share --
        loops times polls times records -- not one box's filesystem role.

        The interval is derived, for the conclusions the reaper draws from
        leases.  Every one of those is a statement about a lease, and a
        lease's own writer refreshes it every ``HEARTBEAT_S``; the grace
        ``reap_stale`` applies to a claim with no lease at all is
        ``HEARTBEAT_S`` too.  So no lease input to the sweep can change more
        often than that, and a second sweep inside one heartbeat re-reads
        bytes that cannot have moved.  Detection is unaffected in kind: a
        lease expires at ``LEASE_TIMEOUT_S`` and is noticed within one
        heartbeat of expiring, by this box or by any other box polling the
        same pool.

        One branch of ``reap_stale`` is not derived and is accepted instead.
        ``finish_pending`` retries the saved outcome of a payload that has
        already returned but whose kernel scope has not drained; its input is
        cgroup state, which moves on its own clock, and its capacity return is
        therefore delayed by up to ``HEARTBEAT_S``.  That is a bounded delay
        in giving tokens back, weighed against a poll cycle that measured
        longer than ``OFFER_TIMEOUT_S`` and cost the box all of its capacity.
        Retrying it off this schedule needs a host-local record of which keys
        are pending, which is the enumeration this method exists to avoid.

        The marker is host-local and unlocked on purpose.  Taking the
        admission lock to decide whether to sweep would put this decision
        behind the very NFS waits it exists to prevent, and losing the race
        costs one extra sweep, which is exactly what the code did before.
        """

        try:
            directory, digest = cpu_admission.box_state(self.ledger().base)
        except Exception:                                        # noqa: BLE001
            # No host-local rendezvous (a read-only or absent ``/tmp``, an
            # unresolvable ledger): sweep, as this method's caller always did.
            return True
        marker = directory / f"{digest}.sweep"
        now = _now()
        try:
            if 0 <= now - marker.stat().st_mtime < interval_s:
                return False
        except (FileNotFoundError, NotADirectoryError):
            pass
        except OSError:
            return True
        try:
            descriptor = os.open(marker, os.O_CREAT | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
        except OSError:
            return True
        try:
            os.utime(descriptor, (now, now))
        except OSError:
            pass
        finally:
            os.close(descriptor)
        return True

    def reap_stale(self, *, timeout_s: float = LEASE_TIMEOUT_S) -> list[str]:
        """Return claims whose lease has expired to ``ready``.

        Completed payloads awaiting cleanup carry ``finish_pending``. Their
        claiming host retries the saved outcome on every poll, even with a
        fresh lease: asynchronous scope termination is not a lost lease or a
        new attempt. Capacity remains held until cleanup is proved complete.

        A missing lease file also counts as stale: it means the claimant died
        between the rename and the first heartbeat.  A stale claim is returned
        only while its generation has no filed outcome.  Once ``done`` or
        ``failed`` carries the same ``published_unix``, that terminal record is
        authoritative and the stranded claim is concluded instead; a CAS hit
        does not license putting already-terminal work back in the queue.

        **The claim is not atomic with its lease.**  ``claim()`` renames the
        item, then rewrites the record with ``claimed_unix``, then writes the
        lease; a reaper running inside that window sees a claimed item with no
        lease and would requeue a worker that is alive and about to start.  So
        a missing lease is only stale once the claim itself has aged past
        ``grace_s``.  The clock for that is ``claimed_unix`` once the record
        carries it -- and before it does, the claim-intent marker, which
        ``claim()`` writes *before* the rename.  Between the rename and the
        record rewrite the claimed file is still the ready record, with no
        ``claimed_unix`` at all; on NFS that stretch spans two directory scans
        and is hundreds of milliseconds wide, and reading it as "no clock, so
        stale" requeued a live seven-second action within a second of its
        claim and let a retry's refusal stand as its outcome (issue #36).  A
        genuinely dead claimant still gets reaped, one grace period later.
        The default grace is the heartbeat interval, far shorter than the
        lease timeout that governs the normal case.

        It is **not** longer than the window actually spans, which is what this
        paragraph used to say (#222).  Measured on ``dl380g10`` on 2026-09-06,
        read-only, at load 0.44 across 80 CPUs with nothing in ``D``: one
        pool-record operation took 45.001 s against a 30 s grace, on NFSv4
        delegation recalls of the directories another box rewrites.  The window
        has no upper bound here, so no grace is safe and a larger one is only a
        larger guess.  The grace still decides *when* a leaseless claim is
        taken; what the taking costs is decided below, by asking whether
        anything ever ran under it rather than how long the claimant took to
        say so.

        **This loop reads its records loudly, and the sibling sweeps do not.**
        ``ready_items`` and ``quarantine_orphans`` treat an entry that goes
        away under the ``glob`` as ordinary, including when it arrives as
        ``ESTALE`` through a cached directory handle (#212).  Here ``None`` is
        not "skip an entry", it is a verdict about whether a claim concluded,
        and ``ESTALE`` can carry a state ``ENOENT`` cannot: ``finish()``
        publishes ``finish_pending`` by atomically *replacing* this file, so a
        stale handle can answer "absent" for a record that is present and
        newer.  The ``finish_pending`` guard above deliberately precedes the
        lease check, because a payload awaiting container cleanup is no longer
        heartbeating -- ``execute`` refreshes the lease only while the child
        runs -- so an expired lease is that state's steady condition, not
        evidence against it.  A tolerated ``ESTALE`` on the first read would
        walk past the guard and reap a completed action as ``lease_lost``.
        Tolerating only the first read and skipping the entry would be sound,
        but it is an asymmetry inside the reaper bought for a race nobody has
        observed on this path, and it invites the next reader to "finish" it
        on the read below, where it is not sound.  So the reaper stays loud
        and says why.

        The guard is also re-asked on the read this loop acts from (#215).
        The first read decides whether to guard; the later read is what
        container cleanup, the superseded filing, the attempt archive and the
        requeue all hang off, and an atomic replace publishing
        ``finish_pending`` between the two was an ordinary claim to the guard
        and a pending finish to nothing.  Re-asking there costs one comparison
        on a record already in hand, and makes the guard describe the bytes
        this loop is about to act on rather than the bytes that sent it here.
        """

        grace_s = HEARTBEAT_S
        requeued: list[str] = []
        claimed = self.dir(CLAIMED)
        if not claimed.is_dir():
            return requeued
        terminal_keys = self.terminal_keys()
        for path in sorted(claimed.glob("*.json")):
            key = path.stem
            record = _read_json(path)
            pending_finish = (record or {}).get("finish_pending")
            if pending_finish is not None:
                # Only the owner host can prove its kernel scope is empty.
                # A payload has already returned here, so lease freshness is
                # irrelevant; preserve that result rather than file lease_lost.
                if record.get("claimed_host") != socket.gethostname():
                    continue
                if (not isinstance(pending_finish, dict)
                        or not isinstance(pending_finish.get("status"), str)
                        or not isinstance(pending_finish.get("detail"), dict)):
                    raise PoolContractError(f"invalid pending finish for {key}")
                result = self.finish(
                    key, status=pending_finish["status"],
                    detail=pending_finish["detail"], claim_snapshot=record,
                )
                if result == self.item_path(READY, key):
                    requeued.append(key)
                continue
            age = self.lease_age(key)
            if age is not None and age <= timeout_s:
                continue
            if age is None:
                record = _read_json(path) or {}
                claimed_unix = record.get("claimed_unix")
                if isinstance(claimed_unix, (int, float)):
                    if _now() - float(claimed_unix) <= grace_s:
                        continue          # claimed moments ago; lease imminent
                else:
                    intent_age = self.claim_intent_age(key)
                    if intent_age is not None and intent_age <= grace_s:
                        # Renamed moments ago; the claimant's rewrite, lease and
                        # its own terminal/withdrawal checks are imminent.  Nothing
                        # below may touch this record: a conclusion here would
                        # release tokens the claimant is about to hold.
                        continue
            record = _read_json(path)
            # The claim exactly as it was read, before the archiving below
            # rewrites its attempt fields.  ``_entomb_claim`` compares against
            # this so the loop can only move aside the claim it judged.
            read_claim = dict(record) if record is not None else None
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
            if record.get("finish_pending") is not None:
                # The claim became a pending finish under us.  ``finish``
                # publishes that state by atomically *replacing* this file, so
                # it can land after the guard above read an ordinary claim --
                # and the guard is where the owner-host rule lives.  Acting on
                # this read without re-asking would run a foreign box's
                # container cleanup and file ``lease_lost`` over a payload that
                # has already returned.  Leave it: the next cycle's first read
                # is the guard's read, and it decides on the owner's box under
                # the rule that belongs to it.
                continue
            # One holder, resolved once, before any branch below concludes
            # this claim -- because every one of them releases the claim's
            # tokens, and tokens are filed under the ledger of the box that
            # committed them.  ``claim`` commits them the moment it wins the
            # rename and rewrites the record with ``claimed_host`` only after
            # that, so a claim lost in between is holding real capacity on a
            # box this record does not name.  Releasing that against the
            # default ledger names the reaper instead, whose ``held/<key>``
            # does not exist: the release returns 0 and moves nothing, the
            # holder keeps its tokens, and a reservation outlives its holder
            # -- the starvation shape, reached by an accounting error rather
            # than by a missed call (#261).
            #
            # The intent marker precedes the rename and does name the box, so
            # the recovery is the one #227 added; what changes is that its
            # answer now reaches the release as well as the record.  Both, so
            # the ledger this loop debits and the hostname its terminal record
            # carries are the same box.
            #
            # Read here for the second reason too: the requeue branch below
            # strips ``claimed_host`` on its way to ``ready``, so this is the
            # last point at which every path can still ask.
            #
            # Contradictory ledger evidence must stop even cleanup from making
            # a choice. Refuse just this claim so healthy work can still recover.
            holder = record.get("claimed_host")
            if not isinstance(holder, str) or not holder:
                try:
                    holder = self.resolve_claim_holder(key, record)
                except AmbiguousClaimHolder as exc:
                    print(f"pool reaper: {exc}", file=sys.stderr)
                    continue
                if holder is not None:
                    record["claimed_host"] = holder
            container_cleanup = self.cleanup_action_containers(record, reason="lease_lost")
            if not container_cleanup["complete"]:
                pending = dict(record)
                self._note_cleanup_attempt(pending, record, container_cleanup)
                _write_json_atomic(path, pending)
                continue
            terminal = self.terminal_outcome_covers(
                record, action_key=key, terminal=terminal_keys
            )
            if terminal is not None:
                # The worker already filed this exact generation.  A stale
                # directory view or a cycle racing the final unlink may still
                # expose its old claim, but that copy is cleanup, not a retry.
                state, outcome = terminal
                self._file_superseded(
                    record, key=key, kind="terminal-claim", status="dropped",
                    dropped_unix=_now(), dropped_host=socket.gethostname(),
                    reason="stale claim belongs to a generation already filed "
                           f"under {state}",
                    terminal_status=outcome.get("status"),
                )
                self.ledger(
                    holder if isinstance(holder, str) else None
                ).release(key)
                path.unlink(missing_ok=True)
                self.lease_path(key).unlink(missing_ok=True)
                continue
            if self.withdrawal_covers(record, action_key=key) is not None:
                # A withdrawal that could not finish its own cleanup -- the
                # operator's box died mid-verb, say -- leaves a claimed record
                # whose lease nobody refreshes.  Requeueing that is the one
                # thing withdrawal exists to prevent, so conclude it here
                # instead: capacity back, records gone, nothing counted as
                # reaped because nothing was returned to the pool.
                self.ledger(holder if isinstance(holder, str) else None).release(key)
                path.unlink(missing_ok=True)
                self.lease_path(key).unlink(missing_ok=True)
                continue
            # The filename is the identity; a record that disagrees with it, or
            # has lost it, must not be written back to a queue directory where
            # every consumer addresses items by key.
            record["action_key"] = key
            prior_attempts = int(record.get("attempts", 0))
            if (age is None
                    and record.get("withdrawn_unix") is None
                    and not self.attempt_path(
                        record, prior_attempts + 1).exists()):
                # Nothing ever ran under this claim, so nothing failed under
                # it.  ``claim`` writes the lease before it returns and
                # ``execute`` writes the child pid into it before the payload
                # is launched, so a claim with no lease at all never reached a
                # launch; and no attempt is published under the number this
                # claim would take, so no other writer recorded one either.
                #
                # Both halves are needed.  A lease is also absent from a claim
                # a *finisher* archived and then died holding:
                # ``sweep_finish_tombstones`` restores exactly that record, and
                # relies on this path archiving at the same attempt number so
                # first-writer-wins hands the item the finisher's real outcome.
                # ``finish`` publishes the attempt before it entombs the claim,
                # so the attempt on disk is what tells the two apart.
                #
                # Charging an attempt here is what turned a stalled claimant
                # into lost work: measured on this fleet, a single pool-record
                # operation took 45 s against a 30 s grace, and
                # ``0a44f2e0f62c`` came back ``lease_lost_max_attempts`` with
                # empty stdout and stderr -- an action that never started,
                # unrunnable, out of a queue another box could have taken it
                # from.  Widening the grace only moves the guess; the window
                # has no upper bound on this filesystem.  Releasing does not
                # need one, because it asks what happened rather than how long
                # it took.
                #
                # A withdrawn claim is the one thing a release must not
                # touch.  ``withdraw`` closes the retry by writing
                # ``max_attempts: 1`` onto the live claimed record, so that any
                # reaper -- including one running pre-withdraw bytes, which
                # consults no marker -- concludes the action instead of
                # requeueing it.  A release does not charge an attempt and so
                # is not closed by that limit: it would put an action an
                # operator cancelled straight back in the queue.  The
                # withdrawal stamp travels on the record beside the limit,
                # which is what makes it readable here without the marker.
                #
                # This does not stop a reaper taking a live-but-blocked
                # claimant's claim -- that is not knowable across boxes.  It
                # stops the taking from destroying the work.  The claimant may
                # still unblock and run: the same double-run the destructive
                # requeue already allowed, reconciled the same way, by
                # first-writer-wins on one attempt number.
                self._file_superseded(
                    record, key=key, kind="unstarted-claim", status="released",
                    released_unix=_now(), released_host=socket.gethostname(),
                    reason="claim released without an attempt: no lease was "
                           "written and no attempt was published",
                )
                # Counted, not bounded.  A bound would be the constant this
                # issue exists to avoid, and a release costs an execution
                # nothing; a key whose count climbs is a box that cannot start
                # work, which is the thing to go and look at.
                record["unstarted_releases"] = int(
                    record.get("unstarted_releases", 0) or 0) + 1
                destination = self._shape_as_ready_item(record, action_key=key)
                tombstone, mine = self._entomb_claim(key, expect=read_claim)
                if not mine:
                    # A retry is live under this key and owns everything the
                    # branch below would have taken.  Nothing has been written
                    # outside the superseded filing, which is evidence rather
                    # than state.
                    continue
                self.lease_path(key).unlink(missing_ok=True)
                self.ledger(holder if isinstance(holder, str) else None).release(key)
                try:
                    _write_json_atomic(destination, record)
                except OSError:
                    if tombstone is not None:
                        try:
                            os.link(tombstone, path)
                        except OSError:
                            pass
                        else:
                            tombstone.unlink(missing_ok=True)
                    continue
                if tombstone is None:
                    path.unlink(missing_ok=True)
                else:
                    tombstone.unlink(missing_ok=True)
                requeued.append(key)
                continue
            attempts = prior_attempts + 1
            limit = int(record.get("max_attempts", DEFAULT_MAX_ATTEMPTS))
            if (
                prior_attempts
                and "attempt_history" not in record
                and "attempt_history_missing_before" not in record
            ):
                # Runtime rollout can meet a record already requeued by older
                # bytes.  State the irrecoverable prefix honestly and archive
                # from this attempt onward; inventing links would be worse,
                # while refusing the record would strand live work.
                record["attempt_history_missing_before"] = prior_attempts
            terminal = attempts >= limit
            finished_unix = _now()
            finished_host = socket.gethostname()
            attempt_record = dict(record)
            attempt_record.update(
                {
                    "finished_unix": finished_unix,
                    "finished_host": finished_host,
                }
            )
            telemetry = (container_cleanup.get("resource_scope") or {}).get("telemetry") or {}
            resource_failure = self._resource_failure(telemetry)
            recovery_detail = {
                "reason": "claim lease expired before an outcome was filed",
                "lease_age_s": age,
            }
            if telemetry:
                recovery_detail["resource_telemetry"] = telemetry
            if resource_failure:
                recovery_detail.update(termination_reason=resource_failure,
                                       termination_evidence=telemetry.get("termination_evidence"),
                                       returncode=137)
            record["attempt_history"] = self.archive_attempt(
                attempt_record,
                attempt=attempts,
                status="failed" if resource_failure else (
                    "lease_lost_max_attempts" if terminal else "lease_lost"),
                disposition=FAILED if terminal else "requeued",
                detail=recovery_detail,
            )
            record["attempts"] = attempts
            adopted = self.adopted_attempt_summary(record)
            record.update(
                {
                    "status": adopted["status"],
                    "finished_unix": adopted["finished_unix"],
                    "finished_host": adopted["finished_host"],
                    "detail": adopted["detail"],
                }
            )
            disposition = adopted["disposition"]
            if disposition in {DONE, FAILED}:
                # The immutable winner may be the finisher, not this reaper.
                # File the exact transition it proved rather than the local
                # lease observation that lost the first-writer race.
                record["schema"] = POOL_OUTCOME_SCHEMA_V1
                destination = self.item_path(str(disposition), key)
            else:
                destination = self._shape_as_ready_item(record, action_key=key)
            # Same ordering as ``finish``, and for the same reason: this loop
            # published the requeue and only then unlinked the claim and lease,
            # so a worker that claimed the requeue inside that window had its
            # claim and lease deleted by this reaper.
            tombstone, mine = self._entomb_claim(key, expect=read_claim)
            if not mine:
                # A retry is live under this key: its claim, lease and
                # reservation are its own.  This loop has written nothing
                # outside the attempt archive, which is immutable and
                # first-writer-wins, so leaving now costs the key nothing.
                continue
            self.lease_path(key).unlink(missing_ok=True)
            # Whatever the outcome, the dead claimant's capacity goes back.  A
            # reservation outliving its holder is the starvation bug's shape.
            self.ledger(holder if isinstance(holder, str) else None).release(key)
            try:
                _write_json_atomic(destination, record)
            except OSError:
                # Put the claim back rather than leave the key with no record
                # anywhere.  Link first: the tombstone must not replace a claim
                # that appeared while this was in flight.
                if tombstone is not None:
                    try:
                        os.link(tombstone, path)
                    except OSError:
                        pass
                    else:
                        tombstone.unlink(missing_ok=True)
                continue
            if tombstone is None:
                path.unlink(missing_ok=True)
            else:
                tombstone.unlink(missing_ok=True)
            requeued.append(key)
        self.sweep_widowed_leases(timeout_s=timeout_s)
        self.sweep_stale_acquisitions()
        self.sweep_finish_tombstones()
        self.quarantine_orphans()
        return requeued

    def sweep_finish_tombstones(
        self, *, grace_s: float = LEASE_TIMEOUT_S
    ) -> list[str]:
        """Recover a claim whose finisher died with it moved out of the way.

        ``finish`` and ``reap_stale`` move a claim to a tombstone, publish the
        item's next home, then delete the tombstone.  A process killed inside
        that window leaves a record that no consumer addresses: the key is in
        neither ``ready`` nor ``claimed``, so nothing claims it, nothing reaps
        it, and its waiter never sees an outcome.  This is the only thing that
        looks.

        Three dispositions, and which one applies is decided by what else the
        key has, never by what the tombstone says about itself:

        *   A record in ``ready`` or ``claimed``, **of any generation**, means
            the key has moved on.  Re-injecting these bytes could only start a
            fight with a live record, so the tombstone is filed as evidence and
            removed.  Any generation, not just this one: a crash in this window
            leaves the key addressable nowhere, so a submitter re-publishes it,
            and restoring the old generation over that would have the reaper
            requeue it straight over the new one.
        *   A terminal record of the *same* generation means the publish landed
            and the tombstone is redundant cleanup.  Filed and removed.
        *   Otherwise the publish did not land: link the record back to
            ``claimed/<key>.json`` and let the ordinary reaper conclude it.
            Its lease is already gone, so the missing-lease path applies one
            grace later, and it charges the same attempt number the finisher
            archived -- ``archive_attempt`` is first-writer-wins, so the
            finisher's real outcome is what the record adopts, not this
            reaper's lease observation.

        A record whose attempt links no longer verify is filed rather than
        restored.  Restoring it would hand ``reap_stale`` a record that raises
        from ``archive_attempt``, and that exception stops reaping on every box
        for as long as the record exists.  Verification therefore has to cover
        every way a link can fail to resolve, not only the ones the queue
        itself judges: a missing or tampered immutable outcome raises out of
        ``core``, not out of the pool's contract error, and an escape here
        causes the exact stall this paragraph is about -- ``reap_stale`` calls
        this sweep unguarded, and ``serve_once`` calls ``reap_stale`` before it
        claims.  Being *unable to look* is a third answer and takes neither
        disposition: the tombstone is left for the next sweep, because filing
        it would hide it in ``withdrawn/superseded/`` on the strength of a
        stale directory handle.

        The grace is the lease timeout: nothing is blocked behind this except
        the action's own visibility, and a sweep that fires while a finisher is
        mid-publish would put a claim beside a ready record of the same
        generation.
        """

        swept: list[str] = []
        claimed = self.dir(CLAIMED)
        if not claimed.is_dir():
            return swept
        now = _now()
        for tombstone in sorted(claimed.glob(f"*{TOMBSTONE_SUFFIX}")):
            parts = tombstone.name.split(".", 2)
            key = parts[0]
            if len(key) != 64 or any(ch not in "0123456789abcdef" for ch in key):
                continue
            when: float | None = None
            if len(parts) >= 2:
                try:
                    when = int(parts[1]) / 1_000_000.0
                except ValueError:
                    when = None
            if when is None:
                try:
                    when = tombstone.stat().st_mtime
                except OSError:
                    continue
            if now - when <= grace_s:
                continue
            record = _read_json(tombstone)
            live = any(
                self.item_path(state, key).exists()
                for state in (READY, CLAIMED)
            )
            covered = (
                self.terminal_outcome_covers(record, action_key=key) is not None
                or self.withdrawal_covers(record, action_key=key) is not None
            )
            restorable = record is not None and not live and not covered
            if restorable and "attempt_history" in record:
                try:
                    self.attempt_outcomes(record)
                except (PoolContractError, FileNotFoundError, pb.CASTamperError):
                    # ``attempt_outcomes`` decides most of this by reading the
                    # record and raises ``PoolContractError``, but the link it
                    # checks last is a *file*: ``_open_regular_nofollow``
                    # raises a bare ``FileNotFoundError`` when the immutable
                    # outcome is absent and ``CASTamperError`` when the entry
                    # is not a readonly regular file, and neither derives from
                    # ``PoolContractError``.  Both are positive evidence that
                    # the link does not verify, which is this branch's whole
                    # question, so both file the record rather than restore it.
                    restorable = False
                except pb.CASUnavailableError:
                    # "Could not look" is not evidence, and it must not be
                    # answered either way.  Restoring risks the reaping stall
                    # this guard exists to prevent; filing moves the record
                    # into ``withdrawn/superseded/``, which every reader is
                    # documented to ignore, so a stale handle would lose the
                    # action permanently.  Leave the tombstone for the next
                    # sweep -- the only disposition that keeps the evidence.
                    continue
            if restorable:
                try:
                    os.link(tombstone, self.item_path(CLAIMED, key))
                except OSError:
                    pass
                else:
                    tombstone.unlink(missing_ok=True)
                    swept.append(key)
                    continue
            self._file_superseded(
                record, key=key, kind="finish-tombstone", status="dropped",
                dropped_unix=_now(), dropped_host=socket.gethostname(),
                reason="a finisher was interrupted between moving its claim "
                       "aside and publishing the item's next home; the key "
                       "already has a live or terminal record, so these bytes "
                       "are evidence rather than work",
            )
            tombstone.unlink(missing_ok=True)
            swept.append(key)
        return swept

    def sweep_stale_acquisitions(
        self, *, grace_s: float = LEASE_TIMEOUT_S
    ) -> list[str]:
        """Free tokens a claimant on any box took and never committed.

        Every host's ledger, not just this one: a claimant that died between
        ``begin_acquire`` and ``commit_acquire`` left its tokens under its own
        box's reservations, and the box that notices may not be that box.
        ``reap_stale`` already releases a dead claimant's tokens from whatever
        host held them, so a foreign write here is the established shape and
        not a new one.
        """

        swept: list[str] = []
        for directory in _scan(self.root / RESERVATIONS):
            if not directory.is_dir():
                continue
            swept.extend(
                f"{directory.name}/{name}"
                for name in self.ledger(directory.name).sweep_stale_acquisitions(
                    grace_s=grace_s
                )
            )
        return swept

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
            container_cleanup = self.cleanup_action_containers(record)
            if not container_cleanup["complete"]:
                continue
            self.ledger(str(host) if isinstance(host, str) else None).release(key)
            lease.unlink(missing_ok=True)
            swept.append(key)
        return swept

    def _file_unreadable(self, path: Path, *, reason: str) -> str | None:
        """Take one unparseable queue record out of the live queue, loudly.

        The original bytes and a bounded diagnostic go to ``superseded/``
        so whoever has to find the writer still can; a
        record with the file's own name goes to ``failed/`` because that is
        what ``pbstatus`` and ``pbwait`` read, and a defect nobody counts is
        the silence this sweep exists to end.

        Never over a terminal record.  A corrupt ready file says nothing about
        an ending already filed for that key, and a key with two terminals is
        a worse defect than the one being cleaned up.
        """

        key = path.stem
        # Take the bytes out of the live namespace before diagnosing or filing
        # them. A later publish must not be unlinked by this sweep. Preserve the
        # complete original alongside the bounded inline diagnostic.
        evidence = self.superseded_dir() / f"{key}.{uuid.uuid4().hex}.unreadable.raw"
        evidence.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.rename(path, evidence)
        except FileNotFoundError:
            return None
        try:
            repaired = _read_json(evidence)
        except PoolContractError:
            repaired = None
        else:
            if repaired is not None:
                # The producer repaired/replaced the record after the scan.
                # Restore without overwriting another concurrent publication.
                try:
                    os.link(evidence, path)
                except FileExistsError:
                    pass  # the replacement remains available as evidence
                else:
                    evidence.unlink()
                return None
        raw = evidence.read_bytes()
        self._file_superseded(
            None, key=key, kind="unreadable", state=READY,
            status="unreadable_record",
            filed_unix=_now(), filed_host=socket.gethostname(),
            reason=reason, raw_bytes=len(raw),
            raw_path=str(evidence.relative_to(self.root)),
            raw_head=raw[:UNREADABLE_HEAD_BYTES].decode("utf-8", "replace"),
        )
        if not any(self.item_path(state, key).exists()
                   for state in (DONE, FAILED)):
            pb._atomic_publish(
                self.item_path(FAILED, key),
                pb._canonical_bytes({
                    "schema": POOL_OUTCOME_SCHEMA_V1,
                    "action_key": key,
                    "status": "unreadable_record",
                    "finished_unix": _now(),
                    "finished_host": socket.gethostname(),
                    "detail": {
                        "reason": "the ready record could not be parsed, so no "
                                  "worker could ever claim it; its bytes are "
                                  "kept under withdrawn/superseded/",
                        "parse_error": reason,
                        "bytes": len(raw),
                    },
                }),
            )
        return key

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

        A record whose bytes will not parse is the same defect one step
        earlier, so it takes the same route.  It used to take the whole fleet
        instead: ``_read_json`` refuses a malformed record, and that refusal
        reached ``claim``, ``ready_items``, ``reap_stale`` and this sweep, so
        one foreign writer's truncated file stopped every consumer on every
        box until somebody deleted it by hand.
        """

        filed: list[str] = []
        ready = self.dir(READY)
        if not ready.is_dir():
            return filed
        for path in sorted(ready.glob("*.json")):
            try:
                record = _read_json(path)
            except PoolContractError as exc:
                key = self._file_unreadable(path, reason=str(exc))
                if key is not None:
                    filed.append(key)
                continue
            except OSError as exc:
                if exc.errno != errno.ESTALE:
                    raise
                # The same race the branch below calls ordinary, arriving
                # through a directory handle the client had cached (#208).
                # It is caught here rather than through ``_read_json``'s
                # ``tolerate_stale`` because this sweep *discriminates*
                # ``None``, and the flag would throw away the errno that tells
                # the two cases apart: a record still on disk answers
                # ``path.exists()`` with ``True`` and would be filed as a torn
                # write and unlinked -- a live queue item destroyed on the
                # evidence of a read that never reached it.  (``Path.exists``
                # would not even survive the attempt: ``pathlib._ignore_error``
                # covers ``ENOENT/ENOTDIR/EBADF/ELOOP``, so ``ESTALE`` comes
                # straight back out of it.)  Filing is this sweep's only
                # verb, and it may not be exercised on bytes it has not read.
                continue
            if record is None:
                # ``None`` covers two different things.  The file vanishing
                # under the glob is an ordinary race with a concurrent claim
                # and is not this sweep's business.  A file that is still
                # there and holds zero bytes is a torn write no consumer will
                # ever address, which is exactly what this sweep is for.
                if path.exists():
                    key = self._file_unreadable(path, reason="queue record is empty")
                    if key is not None:
                        filed.append(key)
                continue
            # Two ways to be unaddressable, and both belong here.  A record
            # with the wrong (or no) ``action_key`` is skipped by ``claim()``
            # and never runs.  A record that *has* the key but lacks the
            # fields a worker executes with -- ``worker_script``, ``cas_root``,
            # checkout addressing -- is worse: it is claimed, it kills the
            # worker process before execution, and retries before it is filed.
            addressable = bool(record.get("checkout_root")) or bool(
                record.get("checkout_snapshot")
            )
            usable = (
                record.get("action_key") == path.stem
                and all(
                    record.get(field) for field in ("worker_script", "cas_root")
                )
                and addressable
            )
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
                        "checkout addressing a worker runs from; see the "
                        "reap_stale and finish() requeue races",
                    },
                }
            )
            _write_json_atomic(self.item_path(FAILED, path.stem), record)
            path.unlink(missing_ok=True)
            self.ledger(None).release(path.stem)
            filed.append(path.stem)
        return filed

    # -- attempt evidence and terminal states ----------------------------

    def archive_attempt(
        self,
        record: Mapping[str, object],
        *,
        attempt: int,
        status: str,
        disposition: str,
        detail: Mapping[str, object] | None,
    ) -> list[dict[str, object]]:
        """Publish one immutable outcome plus stdout/stderr, then link it.

        The mutable queue item is the state machine's current pointer.  It is
        necessarily rewritten on retry and therefore cannot also be the audit
        history.  Each attempt is published first-writer-wins under the action
        generation and 1-based attempt number; the ready/terminal record then
        carries the ordered relative links returned here.
        """

        path = self.attempt_path(record, attempt)
        link: dict[str, object] = {
            "attempt": attempt,
            "outcome": str(path.relative_to(self.root)),
        }
        raw_history = (
            record["attempt_history"] if "attempt_history" in record else []
        )
        if not isinstance(raw_history, list) or any(
            not isinstance(entry, Mapping) for entry in raw_history
        ):
            raise PoolContractError("attempt_history must be a list of links")
        history = [dict(entry) for entry in raw_history]
        missing = record.get("attempt_history_missing_before", 0)
        if type(missing) is not int or missing < 0:
            raise PoolContractError(
                "attempt_history_missing_before must be a non-negative integer"
            )
        if history:
            # Refuse a corrupt prefix before extending it.  This also verifies
            # every immutable log rather than trusting links copied through a
            # mutable ready record.  ``finish`` has already advanced the mutable
            # attempt count, so validate the prefix at its own exact length.
            self.attempt_outcomes(
                {
                    **dict(record),
                    "attempts": missing + len(history),
                }
            )
        expected_attempt = missing + len(history) + 1
        if attempt < expected_attempt:
            index = attempt - missing - 1
            if index < 0 or history[index] != link:
                raise PoolContractError(
                    f"attempt {attempt} has conflicting history links")
            return history
        if attempt != expected_attempt:
            raise PoolContractError(
                f"attempt {attempt} does not follow {missing} unrecorded and "
                f"{len(history)} archived attempts"
            )

        if not isinstance(status, str) or not status:
            raise PoolContractError("pool attempt status must be nonempty text")
        if not isinstance(disposition, str) or not disposition:
            raise PoolContractError(
                "pool attempt disposition must be nonempty text"
            )
        retry_safe = record.get("retry_safe")
        if retry_safe is not None and type(retry_safe) is not bool:
            raise PoolContractError("retry_safe must be boolean or null")
        max_attempts = record.get("max_attempts", DEFAULT_MAX_ATTEMPTS)
        if type(max_attempts) is not int or max_attempts < 1:
            raise PoolContractError("max_attempts must be a positive integer")

        details = dict(detail or {})
        logs: dict[str, dict[str, object]] = {}
        for stream in ("stdout", "stderr"):
            value = details.pop(stream, "")
            if value is None:
                value = ""
            if not isinstance(value, str):
                raise PoolContractError(
                    f"pool attempt {stream} must be text or null")
            raw = value.encode("utf-8")
            digest = hashlib.sha256(raw).hexdigest()
            log_path = self.attempt_log_path(
                record, attempt, stream, digest)
            _publish_immutable(
                log_path, raw, where=f"pool attempt {stream}")
            logs[stream] = {
                "path": str(log_path.relative_to(self.root)),
                "bytes": len(raw),
                "sha256": digest,
            }

        outcome = {
            "schema": POOL_ATTEMPT_SCHEMA_V1,
            "action_key": str(record.get("action_key") or ""),
            "published_unix": record.get("published_unix"),
            "published_by": record.get("published_by"),
            "attempt": attempt,
            "max_attempts": max_attempts,
            # ``None`` is honest legacy evidence: lower-level pool producers
            # predate the explicit pbrun retry contract.  Never infer safety
            # merely from a bound greater than one.
            "retry_safe": retry_safe,
            "status": str(status),
            "disposition": str(disposition),
            "claimed_by": record.get("claimed_by"),
            "claimed_unix": record.get("claimed_unix"),
            "claimed_host": record.get("claimed_host"),
            "finished_unix": record.get("finished_unix"),
            "finished_host": record.get("finished_host"),
            "detail": details,
            "logs": logs,
        }
        # Outcome publication is first-writer-wins.  A finisher and a stale
        # reaper can legitimately race on the same numbered attempt; their
        # logs have content-addressed names, and whichever complete outcome
        # links first is the causal record the mutable queue must adopt.  This
        # also repairs a crash after immutable publication but before the
        # ready/terminal summary kept its link.
        pb._atomic_publish(path, pb._canonical_bytes(outcome))
        history.append(link)
        self.attempt_outcomes(
            {
                **dict(record),
                "attempts": attempt,
                "attempt_history": history,
            }
        )
        return history

    def attempt_outcomes(
        self, record: Mapping[str, object]
    ) -> list[dict[str, object]]:
        """Read and verify the immutable attempts linked by a queue outcome."""

        raw_history = (
            record["attempt_history"] if "attempt_history" in record else []
        )
        if not isinstance(raw_history, list):
            raise PoolContractError("attempt_history must be a list")
        outcomes: list[dict[str, object]] = []
        missing = record.get("attempt_history_missing_before", 0)
        if type(missing) is not int or missing < 0:
            raise PoolContractError(
                "attempt_history_missing_before must be a non-negative integer"
            )
        recorded_attempts = record.get("attempts")
        if type(recorded_attempts) is not int or recorded_attempts < 0:
            raise PoolContractError(
                "pool attempt count must be a non-negative integer"
            )
        if recorded_attempts != missing + len(raw_history):
            raise PoolContractError(
                "pool attempt count does not match its missing prefix and "
                "history links"
            )
        for expected_attempt, raw_link in enumerate(
            raw_history, start=missing + 1
        ):
            if not isinstance(raw_link, Mapping):
                raise PoolContractError("attempt_history link must be an object")
            attempt = raw_link.get("attempt")
            if type(attempt) is not int or attempt != expected_attempt:
                raise PoolContractError(
                    "attempt_history numbers must be contiguous and ordered"
                )
            expected = self.attempt_path(record, attempt)
            if raw_link.get("outcome") != str(expected.relative_to(self.root)):
                raise PoolContractError(
                    f"attempt {attempt} outcome link is not its canonical path"
                )
            raw = pb._read_regular_file_nofollow(
                expected,
                where="pool attempt outcome",
                require_readonly=True,
            )
            try:
                value = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise PoolContractError(
                    f"pool attempt outcome is not valid JSON: {expected}"
                ) from exc
            if not isinstance(value, dict):
                raise PoolContractError(
                    f"pool attempt outcome is not an object: {expected}"
                )
            if (
                value.get("schema") != POOL_ATTEMPT_SCHEMA_V1
                or value.get("action_key") != record.get("action_key")
                or value.get("published_unix") != record.get("published_unix")
                or value.get("attempt") != attempt
                or value.get("max_attempts")
                != record.get("max_attempts", DEFAULT_MAX_ATTEMPTS)
                or value.get("retry_safe") != record.get("retry_safe")
            ):
                raise PoolContractError(
                    f"pool attempt outcome differs from its history link: {expected}"
                )
            raw_logs = value.get("logs")
            if not isinstance(raw_logs, Mapping):
                raise PoolContractError(f"pool attempt logs are missing: {expected}")
            expanded = dict(value)
            for stream in ("stdout", "stderr"):
                metadata = raw_logs.get(stream)
                if not isinstance(metadata, Mapping):
                    raise PoolContractError(
                        f"pool attempt {stream} metadata is missing: {expected}"
                    )
                digest = metadata.get("sha256")
                byte_count = metadata.get("bytes")
                if type(byte_count) is not int or byte_count < 0:
                    raise PoolContractError(
                        f"pool attempt {stream} byte count is invalid: {expected}"
                    )
                log_path = self.attempt_log_path(
                    record, attempt, stream, str(digest))
                if metadata.get("path") != str(log_path.relative_to(self.root)):
                    raise PoolContractError(
                        f"pool attempt {stream} link is not its canonical path"
                    )
                log = pb._read_regular_file_nofollow(
                    log_path,
                    where=f"pool attempt {stream}",
                    require_readonly=True,
                )
                if (
                    byte_count != len(log)
                    or metadata.get("sha256") != hashlib.sha256(log).hexdigest()
                ):
                    raise PoolContractError(
                        f"pool attempt {stream} differs from its recorded address"
                    )
                expanded[stream] = log.decode("utf-8")
            outcomes.append(expanded)
        return outcomes

    def adopted_attempt_summary(
        self, record: Mapping[str, object]
    ) -> dict[str, object]:
        """Return the one mutable transition the immutable winner permits.

        A finisher and a stale reaper can both observe the same claim and race
        to publish one attempt number.  ``archive_attempt`` makes that evidence
        first-writer-wins; this method makes its status, disposition, detail,
        and provenance first-writer-wins too.  Both queue writers and readers
        use this rule so a mutable summary cannot route one cause while
        reporting or returning another.
        """

        attempts = self.attempt_outcomes(record)
        if not attempts:
            raise PoolContractError(
                "an attempt-backed queue record has no immutable outcome"
            )
        adopted = attempts[-1]
        status = adopted.get("status")
        disposition = adopted.get("disposition")
        if not isinstance(status, str) or not status:
            raise PoolContractError("pool attempt status must be nonempty text")
        if not isinstance(disposition, str) or not disposition:
            raise PoolContractError(
                "pool attempt disposition must be nonempty text"
            )
        attempt = adopted.get("attempt")
        max_attempts = adopted.get("max_attempts")
        if type(attempt) is not int or type(max_attempts) is not int:
            raise PoolContractError("pool attempt transition has invalid bounds")
        succeeded = status in {"executed", "cache_hit"}
        expected = (
            DONE if succeeded else FAILED if attempt >= max_attempts else "requeued"
        )
        if disposition != expected:
            raise PoolContractError(
                f"pool attempt {attempt} status {status!r} requires "
                f"disposition {expected!r}, not {disposition!r}"
            )
        raw_detail = adopted.get("detail")
        if not isinstance(raw_detail, Mapping):
            raise PoolContractError("pool attempt detail must be an object")
        finished_unix = adopted.get("finished_unix")
        if (
            isinstance(finished_unix, bool)
            or not isinstance(finished_unix, (int, float))
            or not math.isfinite(float(finished_unix))
        ):
            raise PoolContractError("pool attempt finished_unix must be finite")
        finished_host = adopted.get("finished_host")
        if not isinstance(finished_host, str) or not finished_host:
            raise PoolContractError("pool attempt finished_host must be nonempty text")
        detail = dict(raw_detail)
        detail["stdout"] = str(adopted.get("stdout") or "")
        detail["stderr"] = str(adopted.get("stderr") or "")
        return {
            "attempt": attempt,
            "status": status,
            "disposition": disposition,
            "finished_unix": finished_unix,
            "finished_host": finished_host,
            "detail": detail,
        }

    def _finish_late(
        self,
        action_key: str,
        *,
        status: str,
        detail: Mapping[str, object] | None,
        snapshot: Mapping[str, object],
        live: Mapping[str, object],
    ) -> Path:
        """File the result of an attempt that a newer one has already replaced.

        ``finish`` used to read whichever record occupied
        ``claimed/<key>.json`` and prefer it over ``claim_snapshot`` without
        comparing identity.  When a lease expired while its launcher was still
        alive, the reaper requeued the action and a second worker claimed the
        retry, the first worker's ``finish`` then advanced *that* record's
        attempt counter, archived its own result under the second worker's
        identity, filed the generation terminal, released the second worker's
        tokens and removed its claim.  A ``done`` record and an immutable
        attempt both described a result the named attempt never produced, and
        the running retry lost its reservation.

        A worker may conclude only the attempt it executed.  The live claim,
        its lease and its reservation are left exactly as they are, and this
        attempt's result goes where it belongs: its own numbered attempt under
        its own generation, first-writer-wins like every other immutable
        outcome, so a reaper that already filed a lease loss for this attempt
        keeps that record and this one does not overwrite it.

        Containers are deliberately not cleaned up here.  ``container_owner``
        is a property of the action, not of one attempt, so the census cannot
        tell this attempt's payloads from the live attempt's, and removing
        them would stop work that is legitimately running.  The live attempt's
        own conclusion cleans up both.
        """

        attempt = int(snapshot.get("attempts", 0)) + 1
        archived = dict(snapshot)
        archived["action_key"] = action_key
        archived["finished_unix"] = _now()
        archived["finished_host"] = socket.gethostname()
        succeeded = status in {"executed", "cache_hit"}
        limit = int(snapshot.get("max_attempts", DEFAULT_MAX_ATTEMPTS))
        # The same rule ``adopted_attempt_summary`` applies, so this outcome can
        # never be the one that makes a reader refuse the record.
        disposition = (
            DONE if succeeded else FAILED if attempt >= limit else "requeued"
        )
        self.archive_attempt(
            archived,
            attempt=attempt,
            status=status,
            disposition=disposition,
            detail={
                **dict(detail or {}),
                "late_finisher": {
                    "reason": "this claim was requeued and re-claimed while "
                              "this worker was still running it, so its "
                              "result is filed under its own attempt and the "
                              "live claim, lease and reservation were left "
                              "untouched",
                    "live_claimed_by": live.get("claimed_by"),
                    "live_claimed_unix": live.get("claimed_unix"),
                    "live_attempts": live.get("attempts"),
                    "live_published_unix": live.get("published_unix"),
                },
            },
        )
        return self.attempt_path(archived, attempt)

    def finish(
        self,
        action_key: str,
        *,
        status: str,
        detail: Mapping[str, object] | None = None,
        claim_snapshot: Mapping[str, object] | None = None,
    ) -> Path:
        """File an outcome and return the claim's capacity.

        ``claim_snapshot`` is the record this worker actually executed.  The
        live claimed path can disappear under a finishing worker when a reaper
        wins the terminal-file race; the snapshot keeps the reservation's host
        available even then.  It is not used to reconstruct the queue item --
        the lost-race outcome remains deliberately terminal.
        """

        succeeded = status in {"executed", "cache_hit"}
        src = self.item_path(CLAIMED, action_key)
        record = _read_json(src)
        if (record is not None and claim_snapshot is not None
                and not _same_claim(record, claim_snapshot)):
            # Whatever is at ``claimed/<key>.json`` now is not the claim this
            # worker executed, so none of the code below may touch it.
            return self._finish_late(
                action_key, status=status, detail=detail,
                snapshot=claim_snapshot, live=record,
            )
        read_claim = dict(record) if record is not None else None
        effective_record = record or claim_snapshot or {}
        container_cleanup = self.cleanup_action_containers(
            effective_record, reason=str((detail or {}).get("termination_reason") or status))
        if not container_cleanup["complete"]:
            # A detached container is still the action even after its launcher
            # has returned.  Keep the claim as the durable owner of both the
            # work and its tokens; a local reaper retries cleanup, while a
            # remote one sees the claimed host and leaves it alone.
            pending = dict(effective_record)
            pending["action_key"] = action_key
            self._note_cleanup_attempt(pending, effective_record, container_cleanup)
            pending["finish_pending"] = {
                "status": status, "detail": dict(detail or {}),
            }
            live = _read_json(src)
            if live is not None and _same_claim(live, effective_record):
                _write_json_atomic(src, pending)
            return src
        scope_cleanup = container_cleanup.get("resource_scope") or {}
        telemetry = scope_cleanup.get("telemetry") or {}
        resource_failure = self._resource_failure(telemetry)
        if resource_failure:
            status, succeeded = "failed", False
            detail = {**dict(detail or {}), "status": "failed", "returncode": 137,
                      "termination_reason": resource_failure, "resource_telemetry": telemetry,
                      "termination_evidence": telemetry.get("termination_evidence")}
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
            host = (record or claim_snapshot or {}).get("claimed_host")
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
            #
            # Ask what this generation has already been filed as before
            # choosing a directory.  The winner's conclusion is the terminal:
            # a reaper that filed ``failed/`` and a launcher that then
            # succeeded are one attempt with one ending, and writing the
            # launcher's opinion into ``done/`` beside it gives one key two
            # terminals.  ``pbrun`` then answers with whichever record scores
            # higher, ``pool_reset`` offers to re-run work whose receipt is in
            # the CAS, and ``reclaim_terminal_reservation`` refuses the key as
            # ambiguous.  The snapshot carries ``published_unix``, which is
            # what makes the question askable here at all.
            snapshot = dict(claim_snapshot or {})
            snapshot_host = snapshot.get("claimed_host")
            self.ledger(
                str(snapshot_host) if isinstance(snapshot_host, str) else None
            ).release(action_key)
            self.lease_path(action_key).unlink(missing_ok=True)
            try:
                covered = self.terminal_outcome_covers(
                    snapshot, action_key=action_key)
            except PoolContractError:
                # A terminal for this key exists and cannot be read.  Raising
                # here ends the whole ``serve_once`` call over one bad file,
                # and PR #52 introduced that on a branch which used to write
                # unconditionally, so ask what is actually left to do.
                #
                # Nothing, is the answer.  This branch is reached only because
                # a reaper already concluded the claim, so the key HAS an
                # ending; the unreadable record is it.  Writing a second one
                # beside it is the two-terminals defect PR #52 removed, and a
                # generation this read cannot supply is no basis for deciding
                # that this is a different run.  So report the terminal that
                # is there and write nothing: the submitter's own reader
                # reports an unreadable record at once (PR #50), which is
                # where a corrupted queue record has to surface, and repairing
                # it from here would be inventing an ending for an attempt
                # this worker did not archive.
                for state in (DONE, FAILED):
                    unreadable = self.item_path(state, action_key)
                    if unreadable.exists():
                        return unreadable
                raise
            if covered is not None:
                return self.item_path(str(covered[0]), action_key)
            lost = self.item_path(
                DONE if succeeded else FAILED, action_key)
            if not lost.exists():
                filed = {
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
                }
                # Generation-scoped like every other terminal, from the only
                # copy of the item this branch has.  Identity fields only: the
                # snapshot's ``attempts`` predates this attempt, and its
                # ``attempt_history`` links an attempt somebody else archived,
                # which a reader would adopt against the wrong disposition.
                for field in ("published_unix", "published_by", "claimed_by",
                              "claimed_unix", "claimed_host", "max_attempts",
                              "retry_safe"):
                    if field in snapshot:
                        filed[field] = snapshot[field]
                _write_json_atomic(lost, filed)
            return lost
        record.pop("finish_pending", None)
        record.pop("container_cleanup_pending", None)
        host = record.get("claimed_host")
        prior_attempts = int(record.get("attempts", 0))
        attempts = prior_attempts + 1
        limit = int(record.get("max_attempts", DEFAULT_MAX_ATTEMPTS))
        if (
            prior_attempts
            and "attempt_history" not in record
            and "attempt_history_missing_before" not in record
        ):
            record["attempt_history_missing_before"] = prior_attempts
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
        terminal = succeeded or attempts >= limit
        disposition = (
            DONE if succeeded else FAILED if terminal else "requeued"
        )
        # Publish the evidence before the mutable queue pointer moves.  A
        # retry rewrites ``detail`` with its own result, so the history link is
        # the only place the causal attempt can survive that transition.
        record["attempt_history"] = self.archive_attempt(
            record,
            attempt=attempts,
            status=status,
            disposition=disposition,
            detail=detail,
        )
        adopted = self.adopted_attempt_summary(record)
        record.update(
            {
                "status": adopted["status"],
                "finished_unix": adopted["finished_unix"],
                "finished_host": adopted["finished_host"],
                "detail": adopted["detail"],
            }
        )
        disposition = adopted["disposition"]
        if disposition in {DONE, FAILED}:
            dst = self.item_path(str(disposition), action_key)
        else:
            # Reaching this branch is the producer's explicit retry contract,
            # not an inference from deterministic bytes: an argv may mutate
            # external state before failing even when its CAS result would be
            # reproducible.  ``fleet/pbrun`` reaches it only with
            # ``--retry-safe`` and a bound above one.
            dst = self._shape_as_ready_item(record, action_key=action_key)
        # Everything this worker owns goes before the item's next home becomes
        # visible: the claim to a tombstone, then its own lease.  A retry
        # published while either still stood was claimed by the next poll, and
        # the unlinks below then deleted that new claim and its lease.
        # Another cleanup retry can finish and re-claim this key during the
        # archive write. Compare the original identity at the atomic move,
        # not just at entry, before touching the lease or reservation.
        tombstone, mine = self._entomb_claim(action_key, expect=read_claim)
        if not mine or tombstone is None:
            return self.attempt_path(record, attempts)
        self.lease_path(action_key).unlink(missing_ok=True)
        # Capacity is released before the item is filed, so the next worker to
        # look sees the tokens free rather than racing this rename.
        self.ledger(str(host) if isinstance(host, str) else None).release(action_key)
        _write_json_atomic(dst, record)
        if tombstone is None:
            src.unlink(missing_ok=True)
        else:
            tombstone.unlink(missing_ok=True)
        return dst

    def reclaim_terminal_reservation(self, action_key: str) -> dict[str, object]:
        """Return an orphaned reservation only when terminal state proves it.

        This is the bounded repair for a worker that finished on bytes which
        predate ``claim_snapshot``.  It refuses a live or queued action, a
        missing/failed terminal result, a surviving lease, multiple holders,
        and a holder that differs from the host which filed the successful
        outcome.  Those are ambiguous generations, not cleanup opportunities.
        """

        key = str(action_key)
        if len(key) != 64 or any(ch not in "0123456789abcdef" for ch in key):
            raise PoolContractError("action_key must be a 64-character hex digest")
        for state in (READY, CLAIMED):
            if self.item_path(state, key).exists():
                raise PoolContractError(
                    f"refusing to reclaim {key}: action is still {state}")
        if self.lease_path(key).exists():
            raise PoolContractError(
                f"refusing to reclaim {key}: action still has a lease")

        terminals = [
            (state, record)
            for state in (DONE, FAILED, WITHDRAWN)
            if (record := _read_json(self.item_path(state, key))) is not None
        ]
        if len(terminals) != 1:
            raise PoolContractError(
                f"refusing to reclaim {key}: expected exactly one terminal "
                f"record, found {len(terminals)}")
        state, terminal = terminals[0]
        if state != DONE or terminal.get("status") not in {"executed", "cache_hit"}:
            raise PoolContractError(
                f"refusing to reclaim {key}: terminal status is "
                f"{state}/{terminal.get('status')}")

        hosts = self.claim_reservation_hosts(key)
        if not hosts:
            return {"action_key": key, "released": 0, "hosts": []}
        if len(hosts) != 1:
            raise PoolContractError(
                f"refusing to reclaim {key}: reservation is held on {hosts}")
        finished_host = terminal.get("finished_host")
        if finished_host != hosts[0]:
            raise PoolContractError(
                f"refusing to reclaim {key}: successful outcome was filed on "
                f"{finished_host!r}, reservation is held on {hosts[0]!r}")
        terminal_owner = terminal.get("container_owner")
        if terminal_owner and self.container_marker(str(terminal_owner)).exists():
            raise PoolContractError(
                f"refusing to reclaim {key}: container lifecycle verification "
                "is required")

        released = self.ledger(hosts[0]).release(key)
        return {"action_key": key, "released": released, "hosts": hosts}

    # -- operator decisions ---------------------------------------------

    def terminal_keys(self) -> frozenset[str]:
        """Every action key with a worker-filed outcome.

        List rather than ``stat`` for the same NFS reason as
        :meth:`withdrawn_keys`: a negatively cached absence must not make a
        worker execute a ready copy after another box has filed its outcome.
        The record's generation is checked separately, so an old outcome does
        not blacklist this content-addressed name.

        **Loud on anything but absence.**  This set is the evidence
        ``terminal_outcome_covers`` decides on, and every ``OSError`` used to
        answer it the same way an empty directory does.  ``ESTALE`` on a
        cached directory handle is the ordinary way a listing fails on this
        mount -- ``_read_json`` treats it as a first-class event (#208) and
        ``quarantine_orphans`` re-raises every errno that is not ``ESTALE``
        rather than swallowing the class -- so one stale handle reported "no
        outcomes have been filed" for a queue full of them.  ``reap_stale``
        then found no filed outcome for a generation that had one and put it
        back in ``ready``, which is the one thing this method's own caller
        says a CAS hit does not license.  ``_read_json`` states the rule:
        answering "absent" without the evidence that the directory is live
        "would turn a broken mount into a confident wrong verdict".
        """

        keys: set[str] = set()
        for state in (DONE, FAILED):
            try:
                names = os.listdir(self.dir(state))
            except (FileNotFoundError, NotADirectoryError):
                # Absence only.  A queue whose layout has not been created yet
                # legitimately has no ``done`` and no ``failed``, and that is
                # the one reading of "no names" this method may make.
                continue
            keys.update(
                name[: -len(".json")] for name in names if name.endswith(".json")
            )
        return frozenset(keys)

    def terminal_outcome_covers(
        self,
        record: Mapping[str, object] | None,
        *,
        action_key: str | None = None,
        terminal: frozenset[str] | None = None,
    ) -> tuple[str, dict[str, object]] | None:
        """The filed outcome for this record's generation, or ``None``.

        An action key identifies work, not one request to perform it.  The
        equality of ``published_unix`` is already the queue's generation rule
        for withdrawal and is deliberately independent of clock ordering.
        Missing generation evidence cannot suppress a later submission.
        """

        key = str(action_key or (record or {}).get("action_key") or "")
        if not key or record is None:
            return None
        known = self.terminal_keys() if terminal is None else terminal
        if key not in known:
            return None
        mine = record.get("published_unix")
        if not isinstance(mine, (int, float)):
            return None
        for state in (DONE, FAILED):
            outcome = _read_json(self.item_path(state, key))
            if outcome is None:
                continue
            theirs = outcome.get("published_unix")
            if isinstance(theirs, (int, float)) and float(mine) == float(theirs):
                return state, outcome
        return None

    def withdrawn_keys(self) -> frozenset[str]:
        """Every action an operator has withdrawn.

        Listed rather than stat-ed, one call per decision point.  This queue
        lives on NFS, where a stat of a path that did not exist yet is
        negatively cached and keeps answering ``False`` after the file lands --
        the same reason pbrun's wait loop polls by ``readdir``.  A withdrawal
        that a guard could not see is not a withdrawal.

        Which is why an unreadable directory is not an empty one.  Every
        ``OSError`` used to answer ``frozenset()`` here, so an ``ESTALE`` on a
        cached handle said "nothing has been withdrawn" and defeated the
        sentence above: ``_claim`` reads this set as the load-bearing half of
        ``withdraw``, and with it empty the cancelled work is claimed and run
        again -- the race the operator used to have to win by hand.  Loud on
        anything but absence, for the reason :meth:`terminal_keys` records.
        """

        try:
            names = os.listdir(self.dir(WITHDRAWN))
        except (FileNotFoundError, NotADirectoryError):
            # Absence only, for the reason ``terminal_keys`` gives: a queue
            # whose layout has not been created yet has no ``withdrawn``.
            return frozenset()
        return frozenset(
            name[: -len(".json")] for name in names if name.endswith(".json")
        )

    def superseded_dir(self) -> Path:
        """Where records go once a generation decision makes them non-live.

        A subdirectory rather than a timestamped sibling, because every reader
        of ``withdrawn/`` addresses it by ``<key>.json``: ``withdrawn_keys``
        lists it, ``find_key`` globs it, ``item_path`` builds the name,
        ``pbrun``'s wait loop lists it and ``tessera_status`` counts ``*.json``
        in it.  A sibling named ``<key>.<unix>.json`` would look to all five
        like an action whose key is nonsense; a subdirectory is invisible to
        every one of them.  It keeps both retired withdrawals and ready/claimed
        copies dropped because a terminal record already owns their generation,
        so no queue record disappears without evidence.
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
        ``finish`` uses, but only once the action is known to have **stopped**,
        and only after the action-owned container census is empty.  Those are
        two different questions and the verb used to ask only the second: an
        action with no Docker marker reports an empty census immediately, so a
        withdrawal from another box released the holder's tokens and removed
        its claim and lease while the payload was still running, and a
        replacement action was admitted on the holder's only CPU token.  The
        census proves no owned container remains; it says nothing about a
        non-container payload.

        Three things confirm a stop, and nothing else does: the local signal
        ladder reporting ``still_alive`` false; the holder being this host with
        no process owning the action, so there is nothing here to stop; and the
        dead-holder case, which is the reaper's, not this verb's -- once the
        lease stops beating, ``reap_stale``'s withdrawal branch concludes the
        claim and releases the tokens from the holder's own ledger.  Otherwise
        the claim, lease and reservation stay exactly where they are, a
        ``stop_pending`` object is stamped on the claimed record the way
        ``container_cleanup_pending`` is, and the result reports ``released:
        0`` and the holder's host.  The holder's own launcher checkpoint then
        stops the action, files it as withdrawn, and releases the tokens.
        Releasing twice is free, because tokens are filed under the action key
        and the second release finds nothing to return.

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

        # A live marker that does not name the live record is a *stale*
        # decision, not this one, and the idempotent branch below would treat
        # it as one: it keeps ``existing`` verbatim and files nothing new, so
        # the ready guard finds a generation the marker does not cover, leaves
        # it queued, and answers ``already_withdrawn``.  The operator is told
        # the action is cancelled and it runs anyway -- and if the live record
        # is CLAIMED, worse: the cleanup at the end of this verb still unlinks
        # that claim and its lease and hands back its tokens, while nothing on
        # the running box is covered by any marker, so the child runs on and
        # ``finish`` files it under ``failed`` as a lost race.  Retire the
        # stale decision into ``withdrawn/superseded/`` and proceed as a fresh
        # one, which is what the operator asked for by running the verb again.
        #
        # The first decision is kept, not overwritten: ``_supersede_withdrawal``
        # files it under ``withdrawn/superseded/``, where it remains the record
        # of who cancelled the earlier generation and why.  This is the same
        # retirement ``publish`` performs when a re-submission arrives behind a
        # marker, applied to the verb that has the same problem.
        if (existing is not None and record is not None
                and self.withdrawal_covers(record, action_key=key) is None):
            self._supersede_withdrawal(key)
            existing = None

        lease = _read_json(self.lease_path(key)) or {}
        host: str | None = None
        if isinstance(record, Mapping):
            claimed_host = record.get("claimed_host")
            host = claimed_host if isinstance(claimed_host, str) else None
        if host is None and isinstance(lease.get("host"), str):
            host = str(lease["host"])
        if host is None and origin == CLAIMED:
            # Neither field exists in the window between ``claim``'s rename and
            # its record rewrite: the rewrite is what writes ``claimed_host``,
            # and the lease is written after that.  A claim lost in there names
            # no box, and every use of ``host`` below then means the operator's
            # own: the release moves nothing (``release`` empties an absent
            # ``held/<key>`` and returns 0), so the claiming box keeps its
            # tokens and a reservation outlives its holder, while the verb
            # reports ``released: 0`` and ``holder_host: null`` -- a number
            # that is true beside a box that is unknown.
            #
            # ``resolve_claim_holder`` is the recovery #227 added for exactly
            # this window and #261 made the single resolution every concluding
            # branch of ``reap_stale`` reads.  ``withdraw`` is a different verb
            # with the same shape, which is why it was left out then (#271).
            #
            # Only for a CLAIMED record, for the *marker* half of that
            # resolution: the marker is written before the rename, so it exists
            # -- this generation and legitimately -- while a claimant sits
            # between writing it and winning, when the item is still in
            # ``ready`` with that claimant's tokens already acquired.
            # Recovering a holder there and releasing against it would take
            # tokens from a claim that is about to succeed.  The ledger half
            # answers "nobody" on its own in that moment, since the tokens are
            # still under the claimant-private ``held/<handle>``, so this guard
            # is belt and braces for the proxy rather than for the exact
            # evidence.
            host = self.resolve_claim_holder(key, record)
            if host is not None and isinstance(record, dict):
                # Named on the withdrawn record too, for the reason #227 gives:
                # a claim must not be able to be lost more anonymously than it
                # was taken.
                record["claimed_host"] = host

        if existing is None:
            filed = dict(record or {})
            # A withdrawal is an operator's verb, not an attempt, and the
            # copied record can carry links a requeue wrote.  Every reader of a
            # terminal record adopts the immutable attempt whenever
            # ``attempt_history`` is present, and the attempt it adopts says
            # ``requeued`` while the directory says ``withdrawn``, so
            # ``outcome_summary`` refused the record and the operator's
            # decision reached nobody.  Keep the evidence under a name of its
            # own: the links still resolve, and no reader mistakes them for
            # this record's own ending.
            #
            # ``detail`` is the same fact one field over.  A record a requeue
            # has touched carries the returncode, stdout and stderr of the
            # attempt that failed, and under ``status: withdrawn`` that
            # describes an ending this record does not have: ``pbrun`` wrote
            # the failed attempt's stderr to the operator's terminal and only
            # then said who withdrew the action, and ``pbstatus`` showed its
            # returncode on the withdrawn row.  A cancellation has no detail of
            # its own -- ``withdrawn_by`` and ``reason`` are what it has to say
            # -- so the field is kept as evidence rather than left where every
            # reader takes it for this record's ending.
            for field, kept in (
                ("attempt_history", "attempt_history_before_withdrawal"),
                ("attempt_history_missing_before",
                 "attempt_history_missing_before_withdrawal"),
                ("detail", "detail_before_withdrawal"),
            ):
                if field in filed:
                    filed[kept] = filed.pop(field)
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
        live = dict(record) if isinstance(record, Mapping) else None
        if origin == CLAIMED and live is not None:
            live["max_attempts"] = 1
            live["withdrawn_unix"] = filed.get("withdrawn_unix")
            live["withdrawn_by"] = str(filed.get("withdrawn_by") or by)
            live["withdrawn_note"] = (
                "withdrawn by an operator; the retry is closed so a worker "
                "that cannot see withdrawn/ files this terminally"
            )
            _write_json_atomic(claimed_path, live)

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

        container_cleanup = self.cleanup_action_containers(record or lease, reason="withdrawn")
        # Why the action is known to have stopped, or why it is not.  A claim
        # is the only state with a payload to stop; ``ready`` never started.
        stop_pending: dict[str, object] | None = None
        if origin == CLAIMED:
            local = socket.gethostname()
            if signalled is not None:
                if signalled["still_alive"]:
                    stop_pending = {"reason": "the action's process group "
                                              "survived the signal ladder"}
            elif host is not None and host != local:
                stop_pending = {
                    "reason": f"the action is held on {host}, which this box "
                              "cannot signal; its own worker stops it at the "
                              "next heartbeat and releases the reservation "
                              "then, and the reaper concludes it if that box "
                              "is gone",
                }
            elif targets:
                stop_pending = {
                    "reason": "a local process owns the action and no signal "
                              "was sent",
                }
            # Otherwise the holder is this host and nothing here is running
            # the action, so there is nothing left to stop.
            if stop_pending is not None:
                stop_pending.update({
                    "holder_host": host,
                    "checked_unix": _now(),
                    "checked_host": local,
                })
        if container_cleanup["complete"] and stop_pending is None:
            released = self.ledger(host).release(key)
            claimed_path.unlink(missing_ok=True)
            self.lease_path(key).unlink(missing_ok=True)
            self.passes_path(key).unlink(missing_ok=True)
        else:
            # The decision is already durable in withdrawn/, but the run is
            # not gone yet.  Preserve the claim, lease and reservation as its
            # ownership record; the holder's worker/reaper concludes it.
            released = 0
            if origin == CLAIMED and live is not None:
                # From the poisoned copy, not from the record as it was read.
                # Rebuilding this from ``record`` wrote the pre-poison bytes
                # back over the ``max_attempts: 1`` above, which is the one
                # thing that stops a worker running older bytes from requeueing
                # a cancelled action.
                pending = dict(live)
                if not container_cleanup["complete"]:
                    self._note_cleanup_attempt(pending, live, container_cleanup)
                if stop_pending is not None:
                    pending["stop_pending"] = stop_pending
                _write_json_atomic(claimed_path, pending)
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
            "container_cleanup": container_cleanup,
            # ``None`` when the action is known to have stopped.  Otherwise
            # why the reservation is still held, and on which box.
            "stop_pending": stop_pending,
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
        containment: bool = False,
    ) -> dict[str, object]:
        """Materialize a sealed checkout and optionally contain the worker."""
        if not isinstance(item, dict):
            item = dict(item)
        # Priced here rather than only inside, so every ending carries it --
        # a timeout, a clean exit and a withdrawal all leave a receipt that
        # says which deadline was in force and whether the box's ceiling, not
        # the submitter, chose it (#293).  ``_execute_in_checkout`` re-derives
        # the same number from the same sealed request, which is idempotent
        # under the clamp; passing the effective value keeps the two in step
        # without giving either one a second source of truth.
        budget = execution_budget(item, timeout_s)
        with _execution_checkout(item) as checkout_root:
            outcome = self._execute_in_checkout(
                item, checkout_root=checkout_root, python=python,
                timeout_s=budget.effective, heartbeat_s=heartbeat_s,
                timeout_grace_s=timeout_grace_s, containment=containment,
            )
        outcome.update(budget.as_record())
        if item.get("resource_scope") is not None:
            telemetry = self._sample_resource_scope(self._scope_from_record(item))
            outcome["resource_telemetry"] = telemetry
            resource_failure = self._resource_failure(telemetry)
            if resource_failure:
                outcome.update(status="failed", returncode=137,
                               termination_reason=resource_failure,
                               termination_evidence=telemetry.get("termination_evidence"))
                outcome["stderr"] = str(outcome.get("stderr") or "") + (
                    f"\nPrismaBuild: action resource containment stopped this attempt: {resource_failure}.\n")
        return outcome

    def _execute_in_checkout(
        self,
        item: Mapping[str, object],
        *,
        checkout_root: str | Path,
        python: str | Path = sys.executable,
        timeout_s: float | None = None,
        heartbeat_s: float = HEARTBEAT_S,
        timeout_grace_s: float = TIMEOUT_GRACE_S,
        containment: bool = False,
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
        timeout_s = _execution_timeout(item, timeout_s)
        argv = [str(python)] + worker_argv(
            worker_script=item["worker_script"],
            action_key=key,
            cas_root=item["cas_root"],
            checkout_root=checkout_root,
        )
        allocation = item.get("cpu_allocation")
        if allocation is not None:
            host = str(item.get("reserved_on") or "")
            if host != socket.gethostname():
                raise PoolContractError("CPU allocation belongs to another host")
            ledger = self.ledger(host)
            tiers = _read_json(ledger.base / "cpu-map.json")
            if tiers is None or ledger.cpu_allocation(key, tiers) != allocation:
                raise PoolContractError("CPU allocation differs from held reservation")
            cpus = list(allocation["preferred"]) + list(allocation["fallback"])
            if len(cpus) != self.demand_of(item).get("cpu", 0):
                raise PoolContractError("CPU allocation does not cover demand")
            if cpus:
                if not set(cpus) <= os.sched_getaffinity(0):
                    raise PoolContractError("CPU allocation exceeds current affinity")
                # taskset applies affinity before exec, without preexec_fn in
                # this multithread-capable parent. Descendants inherit it.
                argv = ["/usr/bin/taskset", "--cpu-list", cpu_topology.as_range(cpus),
                        *argv]
        owner = str(item.get("claimed_by") or "")
        started = _now()
        deadline = None if timeout_s is None else time.monotonic() + timeout_s
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
                "cpu_allocation": allocation,
            }
        scope = self._start_resource_scope(item) if containment else None
        if scope is not None:
            # The broker launches taskset inside the aggregate slice. The
            # stdio proxy itself is not an attributed action process.
            argv = scope.wrap_argv(argv)
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
            self.write_lease(
                key,
                owner=owner,
                child_pid=process.pid,
                container_owner=(str(item["container_owner"])
                                 if item.get("container_owner") else None),
            )
            # Refresh the lease while the child runs; a long action must not be
            # reaped out from under itself.
            next_heartbeat = time.monotonic() + heartbeat_s
            while True:
                try:
                    interval = min(heartbeat_s, 2.0) if scope is not None else heartbeat_s
                    if deadline is not None:
                        interval = min(interval, max(0.0, deadline - time.monotonic()))
                    out, err = process.communicate(timeout=interval)
                    break
                except subprocess.TimeoutExpired:
                    if scope is not None:
                        telemetry = self._sample_resource_scope(scope)
                        resource_failure = self._resource_failure(telemetry)
                        if resource_failure:
                            scope.terminate_owned(resource_failure)
                            pb._terminate_process_group(process, grace_s=timeout_grace_s)
                            out, err, survived = _drain(process, timeout_s=timeout_grace_s)
                            return {
                                "status": "failed", "returncode": 137,
                                "termination_reason": resource_failure,
                                "termination_evidence": telemetry.get("termination_evidence"),
                                "resource_telemetry": telemetry,
                                "stdout": out, "stderr": err,
                                "action_survived_kill": survived,
                                "elapsed_s": _now() - started,
                                "argv": argv, "cpu_allocation": allocation,
                            }
                    # Checkpoint two: the cross-box path.  A withdrawal from another
                    # box cannot signal anything on this one, so this poll is what
                    # makes the verb correct from anywhere -- at a cost of at most
                    # one heartbeat, and none at all when the operator is here.
                    if self.withdrawal_covers(item) is not None:
                        if scope is not None:
                            scope.terminate_owned("withdrawn")
                        out, err = self._stop_action(process)
                        return {
                            "status": "withdrawn",
                            "returncode": process.returncode,
                            "stdout": out,
                            "stderr": err,
                            "elapsed_s": _now() - started,
                            "argv": argv,
                            "cpu_allocation": allocation,
                        }
                    if time.monotonic() >= next_heartbeat:
                        self.write_lease(
                            key, owner=owner, child_pid=process.pid,
                            container_owner=(str(item["container_owner"])
                                             if item.get("container_owner") else None),
                        )
                        next_heartbeat = time.monotonic() + heartbeat_s
                    if deadline is not None and time.monotonic() >= deadline:
                        # Worst case this branch spends three grace budgets
                        # -- TERM wait, KILL wait, drain (~45 s) -- without
                        # refreshing the lease, against a 300 s expiry.
                        if scope is not None:
                            scope.terminate_owned("timeout")
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
                            "cpu_allocation": allocation,
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
            "cpu_allocation": allocation,
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

    def _defer_unstarted_claim(self, item: Mapping[str, object]) -> None:
        """Return a broker-refused launch without recording an execution attempt."""
        key = str(item["action_key"])
        record = _read_json(self.item_path(CLAIMED, key))
        if record is None or not _same_claim(record, item):
            return
        if record.get("resource_scope") is not None:
            raise PoolContractError("cannot defer an attempt that already owns a resource scope")
        tombstone, mine = self._entomb_claim(key, expect=record)
        if tombstone is None or not mine:
            return
        host = record.get("claimed_host")
        self.lease_path(key).unlink(missing_ok=True)
        self.ledger(host if isinstance(host, str) else None).release(key)
        destination = self._shape_as_ready_item(record, action_key=key)
        record["maintenance_deferred_unix"] = _now()
        _write_json_atomic(tombstone, record)
        try:
            os.link(tombstone, destination)
        except FileExistsError:
            self._file_superseded(
                record, key=key, kind="maintenance-deferred", status="dropped",
                reason="a newer publication already owns ready after maintenance deferral",
            )
        tombstone.unlink(missing_ok=True)

    def serve_once(
        self,
        *,
        tags: Iterable[str] = (),
        has_gpu: bool = False,
        python: str | Path = sys.executable,
        timeout_s: float | None = None,
        capacity: Mapping[str, int] | None = None,
        cpu_tiers: Mapping[str, Sequence[int]] | None = None,
        adaptive_cpu: bool = False,
        containment: bool = False,
    ) -> dict[str, object] | None:
        """Reap, claim, run, record.  ``None`` when the queue had nothing.

        ``None`` also means "nothing this box may admit right now" once
        ``capacity`` is in play -- including the deliberate case where a starved
        item is withholding the host.  A caller that loops should treat it as
        back-pressure and poll again, not as an empty queue.
        """

        if self._sweep_due():
            self.reap_stale()
        item = self.claim(tags=tags, has_gpu=has_gpu, capacity=capacity,
                          cpu_tiers=cpu_tiers, adaptive_cpu=adaptive_cpu)
        if item is None:
            return None
        key = str(item["action_key"])
        try:
            outcome = self.execute(item, python=python, timeout_s=timeout_s,
                                   **({"containment": True} if containment else {}))
        except resource_scope.ResourceUnavailable:
            # Only create emits this typed maintenance refusal: no payload or
            # scope has been created, so capacity returns without an attempt.
            self._defer_unstarted_claim(item)
            return None
        except BaseException as exc:                      # noqa: BLE001
            # Never leave a claim dangling: an unexpected failure is recorded as
            # a terminal state, not left for the reaper 300 s later.
            self.finish(
                key,
                status="failed",
                detail={"exception": repr(exc)},
                claim_snapshot=item,
            )
            raise
        self.finish(
            key,
            status=str(outcome["status"]),
            detail=outcome,
            claim_snapshot=item,
        )
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
