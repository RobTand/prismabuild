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
            for token in free[:excess] if excess else []:
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
        self.quarantine_orphans()
        return requeued

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
        process = subprocess.Popen(
            argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        # Refresh the lease while the child runs; a long action must not be
        # reaped out from under itself.
        while True:
            try:
                out, err = process.communicate(timeout=heartbeat_s)
                break
            except subprocess.TimeoutExpired:
                self.write_lease(key, owner=owner)
                if timeout_s is not None and _now() - started > timeout_s:
                    process.kill()
                    out, err = process.communicate()
                    return {
                        "status": "timeout",
                        "returncode": None,
                        "stdout": out,
                        "stderr": err,
                        "elapsed_s": _now() - started,
                        "argv": argv,
                    }
        return {
            "status": "executed" if process.returncode == 0 else "failed",
            "returncode": process.returncode,
            "stdout": out,
            "stderr": err,
            "elapsed_s": _now() - started,
            "argv": argv,
        }

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
