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

**What is deliberately NOT ported: the reservation ledger.**  pqwork's ledger is
its largest and subtlest component, and the evidence says it is also where this
fleet actually breaks -- the one documented live defect
(``/mnt/shared/pq-ops/starvation/REPRO-2026-08-30``) is an admission/reservation
failure, not a transport failure.  Reproducing a naive static reservation model
would be importing the known failure mode on day one.  v1 admits one action per
worker slot and nothing more; capacity-aware admission is a later, evidence-led
decision.

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
    ) -> Path:
        """Enqueue one sealed action.  The action itself already lives in the CAS."""

        if not isinstance(action_key, str) or len(action_key) != 64:
            raise PoolContractError("action_key must be a 64-character digest")
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
                out.append(record)
        # Oldest first within a priority band, so a long queue drains in the
        # order it was filled rather than by digest.  Not a scheduler; just a
        # tie-break that makes behaviour predictable enough to debug.
        out.sort(key=lambda r: (-int(r.get("priority", 0)), float(r.get("published_unix", 0.0))))
        return out

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

    def claim(
        self, *, tags: Iterable[str] = (), has_gpu: bool = False, owner: str | None = None
    ) -> dict[str, object] | None:
        """Take one ready item, atomically.  ``None`` when nothing matches.

        The claim IS the ``rename``.  Two workers racing the same item both call
        it; exactly one succeeds and the loser sees ``FileNotFoundError`` and
        moves on.  Nothing else in this method may fail in a way that leaves the
        item in neither directory.
        """

        owner = owner or f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
        tagset = frozenset(str(t) for t in tags)
        self.ensure_layout()
        for item in self.ready_items():
            key = str(item.get("action_key", ""))
            if not key or not self._placement_matches(item, tags=tagset, has_gpu=has_gpu):
                continue
            # Intent precedes the claim, so a crash in between leaves evidence.
            self._write_claim_intent(key, owner=owner)
            src = self.item_path(READY, key)
            dst = self.item_path(CLAIMED, key)
            try:
                os.rename(src, dst)
            except (FileNotFoundError, NotADirectoryError):
                continue          # lost the race; another worker has it
            claimed = dict(item)
            claimed["claimed_by"] = owner
            claimed["claimed_unix"] = _now()
            claimed["claimed_host"] = socket.gethostname()
            _write_json_atomic(dst, claimed)
            self.write_lease(key, owner=owner)
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
        """

        requeued: list[str] = []
        claimed = self.dir(CLAIMED)
        if not claimed.is_dir():
            return requeued
        for path in sorted(claimed.glob("*.json")):
            key = path.stem
            age = self.lease_age(key)
            if age is not None and age <= timeout_s:
                continue
            try:
                os.rename(path, self.item_path(READY, key))
            except (FileNotFoundError, NotADirectoryError):
                continue
            self.lease_path(key).unlink(missing_ok=True)
            requeued.append(key)
        return requeued

    # -- terminal states ------------------------------------------------

    def finish(
        self,
        action_key: str,
        *,
        status: str,
        detail: Mapping[str, object] | None = None,
    ) -> Path:
        state = DONE if status in {"executed", "cache_hit"} else FAILED
        src = self.item_path(CLAIMED, action_key)
        record = _read_json(src) or {"action_key": action_key}
        record.update(
            {
                "schema": POOL_OUTCOME_SCHEMA_V1,
                "status": status,
                "finished_unix": _now(),
                "finished_host": socket.gethostname(),
                "detail": dict(detail or {}),
            }
        )
        dst = self.item_path(state, action_key)
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
    ) -> dict[str, object] | None:
        """Reap, claim, run, record.  ``None`` when the queue had nothing."""

        self.reap_stale()
        item = self.claim(tags=tags, has_gpu=has_gpu)
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
