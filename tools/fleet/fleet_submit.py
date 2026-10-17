"""One place a producer hands a sealed action to whichever transport is live.

A producer that seals its own action and enqueues it directly -- such as
``seal_and_publish`` -- once called ``PoolQueue.publish`` itself, which was
fine while there was one dispatcher and becomes a bypass the moment there are
two: after the cutover, publishing into the pull queue puts 120 export shards
somewhere no worker drains, and publishing succeeds, so nothing says so.

This is deliberately *not* ``pbrun``.  These producers build their own action
bodies -- their own closures, environments and result paths -- and routing them
through ``pbrun`` would re-seal the work as a shell command and lose exactly
that.  What they share with ``pbrun`` is only the last step: hand the sealed
action to the pull queue, or to the SLURM lane.  That step is here, once.

**The lane addresses a checkout only through the action's sealed snapshot.**
``slurm_job`` materializes ``params.checkout_snapshot`` on the node that won
the allocation; a ``checkout_root`` is a path chosen by the submitter, and the
scheduler is free to run the job somewhere that path means something else or
nothing at all.  Refusing a ``checkout_root``-addressed action was the whole
answer, and it was the wrong half of one: every producer in this tree builds
exactly that shape, so the refusal fired 120 times on one export and named no
way forward.  So a checkout root given for the SLURM lane is *sealed* here --
through ``pbrun``'s own snapshot builder, not a second one -- and the action
that reaches the scheduler is the snapshot-addressed action the node needs.

**Sealing moves the action key once, and only for an action that had none.**
The snapshot is an input and a param, so the key is a different hash and the
re-sealed request is published beside the original: this is the same one-time
move the snapshot-ancestry change (#35) accepted, for the same reason -- the
key describes what will actually run.  An action that already carries a
snapshot is submitted untouched, so its key is stable across transports, and
``Submission.action_key`` is what a producer should print.

**Endings are filed by ``pbwait``, not here.**  ``submit`` returns as soon as
the scheduler has the job, so nothing in this module ever sees the ending; the
lane's submission record under ``<lane root>/<key>/latest.json`` is what makes
the job findable afterwards (``slurm_lane.resolve_recorded``), and ``pbwait``
derives the ending from that record plus the CAS receipt and writes the
terminal record under ``done/`` or ``failed/``.  A producer that submits and
exits therefore leaves a job the fleet can still account for.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
import os
from pathlib import Path
import sys
from collections.abc import Mapping, Sequence

SH = Path("/mnt/shared/prismabuild-fleet")
sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
from runtime_paths import generation_root  # noqa: E402
RUNTIME_ROOT = generation_root(__file__)
sys.path.insert(0, str(RUNTIME_ROOT / "src"))
from prismabuild import container_images, core as pb, local_dependencies, pool, slurm_lane  # noqa: E402
from submission_frontend import store_for, render_submission
from prismabuild.submission_types import SubmissionRequest, SubmissionRefused, FleetStore, ProducerPublication
from prismabuild import submission, action_client

def configured_store():
    return store_for(__file__, root=SH)


TRANSPORTS = ("pool", "slurm")
DEFAULT_TRANSPORT_ENV = "PRISMABUILD_TRANSPORT"

#: The job entry, as a sibling of this file.  The published runtime writes
#: every fleet script twice -- ``tools/<name>`` and ``tools/fleet/<name>`` --
#: so a sibling resolves in both layouts, while a path built from
#: ``RUNTIME_ROOT`` has to guess which one it is looking at.
JOB_ENTRY = Path(__file__).resolve(strict=True).parent / "slurm_job.py"
WORKER_SCRIPT = RUNTIME_ROOT / "tools" / "prismabuild_worker.py"

#: What a producer's action gets if it does not say otherwise: no deadline.
#: The pull queue never enforced one, and under SLURM an unset deadline sends
#: no ``--time``, so the job runs while it is running.  A producer that wants
#: a deadline asks for one, and SLURM enforces that one.
DEFAULT_TIMEOUT_S: float | None = None


class SubmitRefused(Exception):
    """The transport cannot carry this action, said before anything is queued."""


@dataclass(frozen=True)
class Submission:
    """Where one action went, in terms the producer can print."""

    transport: str
    where: Path
    job_id: str | None = None
    #: The key of the action that was actually submitted.  Not always the key
    #: the caller sealed: the SLURM lane seals a checkout root into the action,
    #: which is a new hash.  A producer prints this one.
    action_key: str = ""

    def describe(self) -> str:
        return "queued" if self.job_id is None else f"slurm job {self.job_id}"


def default_transport() -> str:
    """The transport this box is cut over to, or the pull queue.

    Three answers, in order, and the order is the point.

    ``PRISMABUILD_TRANSPORT`` in the environment wins, because a person or a
    test saying "this submission goes to SLURM" is never overruled by a file.
    It is checked before it wins: ``argparse`` applies ``choices=`` to what it
    parses and never to a default, so an unchecked value became
    ``args.transport`` in every producer, and every producer branches on
    ``transport == "slurm"``. ``PRISMABUILD_TRANSPORT=slrum`` therefore read
    as "not slurm" and put the work in the pull queue under a name nobody had.

    Otherwise the *published runtime generation* answers.  Cutover used to be
    described as one environment variable, which is a fine description of one
    shell and no description at all of a fleet: agents on three boxes start
    ``pbrun`` from cron, from systemd user units and from each other, and there
    is no single environment to export into.  What every one of them does share
    is the generation they execute -- ``/mnt/shared/prismabuild-fleet/repo``,
    an immutable directory with a receipt beside it -- so the default travels
    in the receipt.  It is optional and absent means ``pool``, so every
    generation published before the cutover keeps the behaviour it had.

    That also makes rollback exactly as atomic as publication was: pointing
    ``repo`` back at the previous generation restores the previous default in
    the same namespace operation, with no environment to unset on three boxes.

    A checkout has no receipt, so a checkout defaults to the pull queue until
    somebody says otherwise on the command line.
    """

    try:
        return submission.default_transport(store=configured_store())
    except SubmissionRefused as exc:
        raise SystemExit(exc.reason or exc.returncode) from None


def add_transport_argument(parser: argparse.ArgumentParser) -> None:
    """The one flag every producer grows, spelled the same way ``pbrun`` does."""

    parser.add_argument(
        "--transport", choices=TRANSPORTS, default=default_transport(),
        help="which dispatcher carries these submissions (env "
             "PRISMABUILD_TRANSPORT, else the published runtime "
             "generation's default_transport); the pull queue stays the "
             "default until the fleet has cut over to SLURM")


#: One bundle per (store, checkout, working-tree state) per process, not one
#: per action.  A dispatcher seals 120 shards out of one tree in one loop, and
#: ``git bundle create`` over that tree 120 times is 119 bundles of identical
#: bytes.  Caching also narrows the window the seal refuses on: the roster is
#: read once, so a tree that moves mid-loop is caught by the identity check
#: rather than producing a hundred subtly different snapshots.
#:
#: The store is part of the key because ingestion is part of the work.  A
#: snapshot record is a reference into one CAS, so returning the first store's
#: record for a second store hands the caller an input whose blob was never
#: written there: the submission succeeds and the node cannot materialize the
#: tree.  Keying on the store root makes the second call do the ingest the
#: caller asked for.
_SNAPSHOT_CACHE: dict[tuple[str, str, str, str], dict[str, object]] = {}


def seal_checkout_snapshot(
    checkout_root: str | Path,
    *,
    cas: pb.PrismaBuildCAS,
    max_bytes: int | None = None,
) -> dict[str, object]:
    try:
        return submission.snapshot_for_producer(checkout_root, cas=cas, max_bytes=max_bytes)
    except SubmissionRefused as exc:
        raise SubmitRefused(exc.reason) from None


def seal_checkout_into_action(
    action: Mapping[str, object],
    *,
    cas: pb.PrismaBuildCAS,
    checkout_root: str | Path,
    max_bytes: int | None = None,
) -> tuple[dict[str, object], dict[str, object]]:
    try:
        return submission.seal_producer_checkout(action, cas=cas, checkout_root=checkout_root, max_bytes=max_bytes)
    except SubmissionRefused as exc:
        raise SubmitRefused(exc.reason) from None


def submit(
    action: Mapping[str, object],
    *,
    cas: pb.PrismaBuildCAS,
    request_path: str | Path,
    transport: str,
    worker_script: str | Path = WORKER_SCRIPT,
    checkout_root: str | Path | None = None,
    tags: Sequence[str] = (),
    needs_gpu: bool = False,
    priority: int = 0,
    resources: Mapping[str, int] | None = None,
    timeout_s: float | None = DEFAULT_TIMEOUT_S,
    max_attempts: int = 1,
    retry_safe: bool | None = None,
    queue_root: str | Path | None = None,
    lane_root: str | Path | None = None,
    job_entry: str | Path = JOB_ENTRY,
    sbatch: str = "sbatch",
    checkout_snapshot_max_bytes: int | None = None,
) -> Submission:
    """Enqueue one sealed action on the named transport.

    The action is already in the CAS; this decides who will run it, and on the
    SLURM lane it also settles how the node will find the tree.  The demand
    vocabulary is the producer's -- ``{"gpu": 1, "mem_gb": 16}`` -- and
    ``LaneResources.from_demand`` maps it to scheduler flags rather than
    letting a second vocabulary grow here.

    Under the pull queue a ``checkout_root`` travels on the queue item, so the
    action is untouched.  Under SLURM it is sealed into the action as a
    snapshot and the re-sealed request is republished, which changes the
    action key: the returned ``Submission.action_key`` is the key that was
    actually submitted, and it is what a producer should print.

    Args:
        action: The sealed action to run.
        cas: The store holding the action request, and the snapshot bundle.
        request_path: The published request for ``action``.
        transport: ``"pool"`` or ``"slurm"``.
        worker_script: The worker entry point the job or item execs.
        checkout_root: The tree the action runs in, or ``None`` for an action
            that already carries its own ``params.checkout_snapshot``.
        tags: Placement tags; a node Feature conjunction under SLURM.
        needs_gpu: Pull-queue only; the lane reads ``resources``.
        priority: Pull-queue only.
        resources: The producer's demand, ``{"gpu": 1, "mem_gb": 16}``.
        timeout_s: A deadline to enforce, or ``None`` for none.
        max_attempts: How many runs this action may have.
        retry_safe: Whether the producer declared the command idempotent.
        queue_root: The pull queue root, also where withdrawals live, or
            ``None`` for ``SH / "pb-queue"`` as it stands when this runs.
        lane_root: The SLURM lane root, or ``None`` for the configured one.
        job_entry: The batch job's entry point.
        sbatch: The submit binary, for tests.
        checkout_snapshot_max_bytes: A lowered snapshot disk bound.

    Returns:
        Where the action went, and the key it went under.

    Raises:
        SubmitRefused: The transport cannot carry this action, said before
            anything is queued.
    """

    current = configured_store()
    store = FleetStore(current.root, current.runtime_root, current.roster_path,
                       queue_root=Path(queue_root) if queue_root is not None else current.queue_root, cas_root=Path(cas.root), d38_config_path=current.d38_config_path)
    request = SubmissionRequest(transport=transport, detach=True, priority=priority,
        timeout_s=timeout_s, max_attempts=max_attempts, retry_safe=bool(retry_safe),
        cwd=str(checkout_root or Path.cwd()))
    producer = ProducerPublication(worker_script=Path(worker_script), checkout_root=checkout_root,
        tags=tuple(tags), needs_gpu=needs_gpu, resources=dict(resources or {}), retry_safe=retry_safe,
        lane_root=lane_root, job_entry=Path(job_entry), sbatch=sbatch)
    report = action_client.ClientReport()
    result = submission.submit_producer(action, cas=cas, request=request,
        store=store, producer=producer, request_path=request_path,
        checkout_snapshot_max_bytes=checkout_snapshot_max_bytes, report=report)
    if isinstance(result, SubmissionRefused):
        raise SubmitRefused(result.reason) from None
    identity = report.identity
    if identity is None:
        raise SubmitRefused(f"submission ended with {type(report.ending.verdict).__name__}")
    where = (identity.submission if not isinstance(identity, action_client.CachedClientIdentity)
             else cas.receipt_path(identity.action_key))
    return Submission(transport=transport, where=where,
        job_id=identity.job_id if isinstance(identity, action_client.SlurmClientIdentity) else None,
        action_key=identity.action_key)
