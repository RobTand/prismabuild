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

Clock skew between claimant and reaper is real but immaterial here: both Sparks
are NTP-synchronised and measured 1.2-2.5 ms apart against a 300 s lease.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
import json
import os
from pathlib import Path
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
_STATES = (READY, CLAIMED, DONE, FAILED, INTENT)

# Ported verbatim from pqwork: 30 s refresh, 300 s expiry.  The 10x margin is
# what absorbs an NFS stall or a long GC pause without a spurious requeue.
HEARTBEAT_S = 30.0
LEASE_TIMEOUT_S = 300.0

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

RESERVATIONS = "reservations"
PASSES = "passes"
WORKERS = "workers"

#: How long a worker's offer stays believable.  A loop re-announces on every
#: poll, and the default poll is 10 s, so two minutes is a dozen missed polls:
#: long enough that a slow NFS write or a long action never makes a live box
#: look dead, short enough that a box taken down does not keep vouching for
#: work nobody can run.
OFFER_TIMEOUT_S = 120.0

DEFAULT_POOL_ROOT = Path(
    os.environ.get("PRISMABUILD_POOL_ROOT", "/mnt/shared/pb-queue")
)


# -- holding an action to its own declaration ---------------------------------
#
# ``mem_gb`` was a reservation and nothing more: the ledger admitted work
# against a declared demand, and an action that exceeded its declaration ran to
# completion anyway.  On a box where the GPU and the host share one 128 GB pool
# an over-declaration is contained nowhere, and the kernel's own OOM killer
# picks a victim from the whole box rather than from the offender -- on sparky
# at 2026-09-01 09:58:41 it took ``pqwork.service``, 20.6 MB peak, which was
# consuming nothing.
#
# ``MemoryMax`` inverts that.  The action runs inside a transient user unit
# whose cgroup limit IS its own declaration, so the process that exceeds the
# figure it published is the one the kernel kills and nothing else on the box
# is a candidate.  A userspace watchdog is the wrong shape for the same job:
# it is reactive, it can only kill what it started, and on unified memory it
# is the recorded Ray failure mode of a monitor killing healthy ranks.
#
# **Scope is measured, not assumed** -- see
# ``docs/memory_enforcement_2026-09-04.md``.  A cap that silently does not bind
# is worse than no cap, because the ledger would then read as enforced, so what
# the cgroup charges was measured on this hardware before the mechanism was
# ported.  The outcome record says whether an action was capped and at what
# figure; nothing here claims more than that.

#: Systemd properties every capped launch carries.  ``MemorySwapMax=0`` is not
#: decoration: with swap available an over-budget action slides into swap and
#: thrashes instead of failing, which converts a loud kill into a slow box.
CAP_UNIT_PREFIX = "pbcap-"

#: Environment names systemd sets *for* a unit.  Forwarding the launcher's
#: copies would hand the child another process's identity.
_UNIT_MANAGED_ENV = frozenset({
    "INVOCATION_ID", "JOURNAL_STREAM", "LISTEN_FDS", "LISTEN_FDNAMES",
    "LISTEN_PID", "MAINPID", "MANAGERPID", "NOTIFY_SOCKET", "SERVICE_RESULT",
    "SYSTEMD_EXEC_PID", "WATCHDOG_PID", "WATCHDOG_USEC", "EXIT_CODE",
    "EXIT_STATUS", "REMOTE_ADDR", "REMOTE_PORT",
})

_CAP_SUPPORT: tuple[bool, str] | None = None


def memory_capping_supported(*, timeout_s: float = 60.0) -> tuple[bool, str]:
    """Can this box start a capped transient user unit?  Probed once.

    Capping needs the ``memory`` controller delegated to the user manager,
    which is a property of the box, not of the code.  Without a probe a box
    lacking delegation would fail every capped action the instant it started
    and the queue would faithfully requeue each one forever -- a capability
    gap wearing the costume of a flaky job.

    Returns ``(supported, detail)``; ``detail`` is the reason when it is not,
    so a degraded box says why rather than merely behaving differently.
    Measured 2026-09-04: sparky, gx10-6b77 and dl380g10 all delegate
    ``cpu memory pids`` and all linger, so all three enforce.
    """

    global _CAP_SUPPORT
    if _CAP_SUPPORT is None:
        try:
            probe = subprocess.run(
                ["systemd-run", "--user", "--quiet", "--wait",
                 "-p", "MemoryMax=64M", "-p", "MemoryAccounting=yes",
                 "--", "/bin/true"],
                capture_output=True, text=True, timeout=timeout_s,
            )
            if probe.returncode == 0:
                _CAP_SUPPORT = (True, "")
            else:
                detail = (probe.stderr or probe.stdout or "").strip()
                _CAP_SUPPORT = (
                    False,
                    f"systemd-run --user refused a capped unit "
                    f"(rc={probe.returncode}): {detail[:300]}",
                )
        except (OSError, subprocess.SubprocessError) as exc:
            _CAP_SUPPORT = (False, f"{type(exc).__name__}: {exc}")
    return _CAP_SUPPORT


def cap_unit_name(action_key: str, owner: str = "") -> str:
    """A transient unit name unique to this *attempt*, not to the action.

    The action key alone is not enough.  A lease that expires while its child
    is still running is returned to ``ready`` by the reaper and claimed again,
    possibly by another loop on the same box, so two attempts at one action can
    overlap -- and two units of one name cannot.  The claim owner already
    carries a per-claim uuid, which is exactly the nonce this needs.
    """

    safe_key = "".join(c for c in str(action_key) if c.isalnum())[:32]
    nonce = "".join(c for c in str(owner) if c.isalnum())[-12:]
    if not nonce:
        nonce = uuid.uuid4().hex[:12]
    return f"{CAP_UNIT_PREFIX}{safe_key}-{nonce}"


def capped_launch_argv(
    argv: Sequence[str],
    *,
    cap_gb: int,
    unit: str,
    cwd: str | Path | None = None,
    env: Mapping[str, str] | None = None,
) -> list[str]:
    """Wrap a launch in a transient unit whose memory limit is ``cap_gb``.

    The wrapper must not change *what* is executed, only what bounds it.  So
    the working directory and the environment are carried across explicitly:
    ``systemd-run --user`` starts a unit from the **user manager's** context,
    not the caller's, and a child that quietly lost ``TRITON_CACHE_DIR`` or
    gained a different ``PATH`` is a different execution wearing the same
    action key.  That is not hypothetical either -- the first run of the CUDA
    measurement forwarded ``CUDA_VISIBLE_DEVICES`` unconditionally, an unset
    name became an empty value, and both GPU arms saw no device at all.  Only
    names that are actually set are forwarded, for that reason.

    ``--pipe`` keeps stdout and stderr as pipes the caller can read, which is
    what the outcome record and the worker's error tail are made of; it
    implies ``--wait`` and propagates an ordinary exit code.  A *killed* unit
    is the one case it cannot express -- it returns 1 -- so the caller reads
    the unit's own ``Result``/``ExecMainStatus`` back afterwards.
    """

    if int(cap_gb) <= 0:
        raise PoolContractError("a memory cap must be a positive number of GB")
    launch = [
        "systemd-run", "--user", "--quiet", "--pipe", "--wait",
        f"--unit={unit}",
        "-p", f"MemoryMax={int(cap_gb)}G",
        # Without this an over-budget action slides into swap instead of
        # failing, and a loud kill becomes a slow box.
        "-p", "MemorySwapMax=0",
        "-p", "MemoryAccounting=yes",
    ]
    if cwd is not None:
        launch += ["-p", f"WorkingDirectory={cwd}"]
    for name, value in sorted((env or {}).items()):
        if name in _UNIT_MANAGED_ENV:
            continue
        if "\0" in name or "\n" in name or "=" in name:
            continue
        if "\0" in str(value) or "\n" in str(value):
            continue
        launch += [f"--setenv={name}={value}"]
    return launch + ["--"] + [str(a) for a in argv]


def _systemctl(*args: str, timeout_s: float = 15.0) -> subprocess.CompletedProcess | None:
    try:
        return subprocess.run(
            ["systemctl", "--user", *args],
            capture_output=True, text=True, timeout=timeout_s,
        )
    except (OSError, subprocess.SubprocessError):
        return None


def unit_outcome(unit: str) -> dict[str, object]:
    """What the transient unit says happened, after ``systemd-run`` returned.

    ``systemd-run --wait`` reports how *it* ended, which is not how the service
    ended: a unit killed by its own cgroup returns 1, while the unit records
    ``Result=oom-kill`` and ``ExecMainStatus=9``.  The status the caller wants
    is the child's, so it is read back rather than inferred -- and the OOM flag
    is used for one narrow purpose, to say *why* an action died.
    """

    shown = _systemctl(
        "show", unit, "-p", "Result", "-p", "ExecMainCode", "-p",
        "ExecMainStatus", "-p", "MemoryPeak",
    )
    fields: dict[str, str] = {}
    if shown is not None and shown.returncode == 0:
        for line in (shown.stdout or "").splitlines():
            if "=" in line:
                name, _, value = line.partition("=")
                fields[name.strip()] = value.strip()

    def _int(name: str) -> int | None:
        raw = fields.get(name, "")
        try:
            value = int(raw)
        except (TypeError, ValueError):
            return None
        # systemd prints an unset 64-bit property as [UINT64_MAX].
        return None if value >= (1 << 63) else value

    code = _int("ExecMainCode")
    status = _int("ExecMainStatus")
    returncode: int | None = None
    if code == 1 and status is not None:
        returncode = status
    elif code in (2, 3) and status:
        # CLD_KILLED / CLD_DUMPED.  Python's own convention for a signalled
        # child is a negative return code, so the translation is exact.
        returncode = -status
    return {
        "result": fields.get("Result", ""),
        "exec_main_code": code,
        "exec_main_status": status,
        "memory_peak": _int("MemoryPeak"),
        "returncode": returncode,
        "oom_killed": fields.get("Result", "") == "oom-kill",
    }



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
        enforces_mem_gb: bool | None = None,
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
        """

        record = {
            "schema": POOL_OFFER_SCHEMA_V1,
            "host": host,
            "tags": sorted({str(t) for t in tags}),
            "has_gpu": bool(has_gpu),
            "capacity": {str(k): int(v) for k, v in (capacity or {}).items()},
            # Which published bytes are answering for this box.  A loop holds
            # the module it imported at start for its whole life, so without
            # this a fleet running four generations of the code at once looks
            # uniform from the queue.
            "runtime_commit": str(runtime_commit),
            # Whether a declared ``mem_gb`` is a limit here or only a
            # reservation.  Capping needs the memory controller delegated to
            # the user manager, which is a property of the box, so three boxes
            # can differ and the offer is the only place that difference shows
            # up as a fleet fact rather than as one worker's log line.
            #
            # Three-valued, like ``placeable``: ``None`` means this caller did
            # not say.  The alternative -- probing here -- would start a
            # transient unit from inside every announce, including the ones a
            # test makes, and would let a submitter's guess be published as a
            # box's answer.  The worker that runs the actions is the only
            # thing that knows, so it is the only thing that states it.
            "enforces_mem_gb": (
                None if enforces_mem_gb is None else bool(enforces_mem_gb)
            ),
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

    def placeable(
        self, item: Mapping[str, object], *, max_age_s: float = OFFER_TIMEOUT_S
    ) -> bool | None:
        """Can any live worker run this item?  ``None`` means nobody has said.

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
        required = item.get("tags") or []
        if not isinstance(required, list):
            raise PoolContractError("pool item tags must be a list")
        wanted = {str(t) for t in required}
        demand = self.demand_of(item)
        needs_gpu = bool(item.get("needs_gpu")) or demand.get("gpu", 0) > 0
        for offer in live:
            tags = {str(t) for t in (offer.get("tags") or [])}
            if not wanted.issubset(tags):
                continue
            if needs_gpu and not offer.get("has_gpu"):
                continue
            capacity = offer.get("capacity") or {}
            if isinstance(capacity, Mapping) and any(
                int(capacity.get(kind, 0)) < need for kind, need in demand.items()
            ):
                continue          # this box can never fit it, however idle
            return True
        return False

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

        count = self.passes(action_key) + 1
        _write_json_atomic(
            self.passes_path(action_key),
            {"action_key": action_key, "passes": count, "updated_unix": _now()},
        )
        return count

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

    def write_lease(self, action_key: str, *, owner: str) -> None:
        _write_json_atomic(
            self.lease_path(action_key),
            {
                "schema": POOL_LEASE_SCHEMA_V1,
                "action_key": action_key,
                "owner": owner,
                "host": socket.gethostname(),
                "pid": os.getpid(),
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
        """

        owner = owner or f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
        tagset = frozenset(str(t) for t in tags)
        self.ensure_layout()
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
            demand = self.demand_of(item)
            if ledger is not None and demand:
                if any(total.get(kind, 0) < need for kind, need in demand.items()):
                    continue      # never fits this box; not this box's to hold
                if not ledger.acquire(key, demand):
                    denials = self.record_pass(key)
                    if denials >= STARVATION_FLOOR:
                        # Wired to the decision: stop letting smaller work pass it.
                        return None
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

    # -- execution ------------------------------------------------------

    def execute(
        self,
        item: Mapping[str, object],
        *,
        python: str | Path = sys.executable,
        timeout_s: float | None = None,
        heartbeat_s: float = HEARTBEAT_S,
    ) -> dict[str, object]:
        """Run one claimed item through the canonical worker argv.

        Executes as a subprocess rather than in-process on purpose: it is the
        same launch SLURM would have made, so the executed contract does not
        depend on which transport delivered the action.

        **The item's own ``mem_gb`` is the limit it runs under.**  The
        reservation and the limit are one object, held in one place: the tokens
        this action took out of the ledger in ``claim`` are the number its
        cgroup refuses to let it exceed.  A declaration nothing enforces is an
        honour system, and on a box whose GPU and host share one pool the
        kernel's answer to a breach is to kill a bystander.

        Capping is by the *item's* declaration, not by whether this worker
        passed a ``capacity``: the demand is the action's own claim about
        itself, and a smoke worker running a declared item should hold it to
        the same figure a fleet loop would.  An item that declares nothing runs
        exactly as it did before.
        """

        key = str(item["action_key"])
        argv = [str(python)] + worker_argv(
            worker_script=item["worker_script"],
            action_key=key,
            cas_root=item["cas_root"],
            checkout_root=item["checkout_root"],
        )
        owner = str(item.get("claimed_by") or "")
        cap_gb = int(self.demand_of(item).get("mem_gb", 0))
        unit: str | None = None
        cap_detail = ""
        launch = argv
        if cap_gb > 0:
            supported, why = memory_capping_supported()
            if supported:
                unit = cap_unit_name(key, owner)
                # A previous attempt's failed unit of the same name would make
                # this launch fail on the name rather than on the work.
                _systemctl("reset-failed", unit)
                launch = capped_launch_argv(
                    argv, cap_gb=cap_gb, unit=unit,
                    cwd=os.getcwd(), env=os.environ,
                )
            else:
                # Loud, not silent: an unenforced declaration is recorded as
                # unenforced, so a ledger is never read as a limit it is not.
                cap_detail = why
        # What the caller gets back either way, so the two paths cannot drift.
        cap_fields: dict[str, object] = {
            "declared_mem_gb": cap_gb,
            "capped": unit is not None,
            "cap_unit": unit or "",
            "cap_unavailable": cap_detail,
        }
        started = _now()
        process = subprocess.Popen(
            launch, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        try:
            # Refresh the lease while the child runs; a long action must not be
            # reaped out from under itself.
            while True:
                try:
                    out, err = process.communicate(timeout=heartbeat_s)
                    break
                except subprocess.TimeoutExpired:
                    self.write_lease(key, owner=owner)
                    if timeout_s is not None and _now() - started > timeout_s:
                        if unit is not None:
                            # Killing ``systemd-run`` does not stop the service
                            # it started, and under ``--pipe`` the service
                            # holds the pipe this call is about to read -- so a
                            # plain kill here would leave the timeout bounding
                            # nothing and block on ``communicate`` until the
                            # work ended by itself.  Measured with the stop
                            # suppressed: a one-second timeout was still
                            # running 100 s later with its unit active.  Stop
                            # the unit; the launcher then exits.
                            _systemctl("stop", unit, timeout_s=30.0)
                        process.kill()
                        out, err = process.communicate()
                        return {
                            "status": "timeout",
                            "returncode": None,
                            "stdout": out,
                            "stderr": err,
                            "elapsed_s": _now() - started,
                            "argv": argv,
                            **cap_fields,
                        }
            returncode = process.returncode
            if unit is not None:
                reported = unit_outcome(unit)
                if reported.get("returncode") is not None:
                    returncode = int(reported["returncode"])  # type: ignore[arg-type]
                cap_fields["unit_result"] = reported.get("result", "")
                cap_fields["oom_killed"] = bool(reported.get("oom_killed"))
                cap_fields["memory_peak_bytes"] = reported.get("memory_peak")
                if reported.get("oom_killed"):
                    err = (err or "") + (
                        f"\n[prismabuild] killed by its own cgroup: this action "
                        f"declared {cap_gb} GB and exceeded it.\n"
                    )
            return {
                "status": "executed" if returncode == 0 else "failed",
                "returncode": returncode,
                "stdout": out,
                "stderr": err,
                "elapsed_s": _now() - started,
                "argv": argv,
                **cap_fields,
            }
        finally:
            # A failed transient unit lingers until somebody resets it, and the
            # name carries a per-attempt nonce, so a leak is never cleaned up
            # by the next launch.  In ``finally`` because the paths that skip
            # it are the ones that leak: an exception between the launch and
            # the return is exactly what ``serve_once``'s own handler exists
            # for.
            if unit is not None:
                _systemctl("reset-failed", unit)

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
