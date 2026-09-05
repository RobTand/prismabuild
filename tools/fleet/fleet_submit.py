"""One place a producer hands a sealed action to whichever transport is live.

Three tools seal their own actions and enqueue them directly --
``dispatch_tessera_shards``, ``dispatch_tessera_ladder`` and
``seal_and_publish``.  Each called ``PoolQueue.publish`` itself, which was
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
the allocation and refuses an action that carries none; a ``checkout_root`` is
a path chosen by the submitter, and the scheduler is free to run the job
somewhere that path means something else or nothing at all.  So a
``checkout_root``-addressed action is refused here, at submit time, rather
than by every job it would have placed.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
import os
from pathlib import Path
import sys
from collections.abc import Mapping, Sequence

SH = Path("/mnt/shared/prismabuild-fleet")
sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
from runtime_paths import generation_root  # noqa: E402
RUNTIME_ROOT = generation_root(__file__)
sys.path.insert(0, str(RUNTIME_ROOT / "src"))
from prismabuild import core as pb, pool, slurm_lane  # noqa: E402

TRANSPORTS = ("pool", "slurm")
DEFAULT_TRANSPORT_ENV = "PRISMABUILD_TRANSPORT"

#: The job entry, as a sibling of this file.  The published runtime writes
#: every fleet script twice -- ``tools/<name>`` and ``tools/fleet/<name>`` --
#: so a sibling resolves in both layouts, while a path built from
#: ``RUNTIME_ROOT`` has to guess which one it is looking at.
JOB_ENTRY = Path(__file__).resolve(strict=True).parent / "slurm_job.py"
WORKER_SCRIPT = RUNTIME_ROOT / "tools" / "prismabuild_worker.py"

#: What a producer's action gets if it does not say otherwise.  The pull queue
#: ignores it; SLURM turns it into ``--time`` and enforces it.
DEFAULT_TIMEOUT_S = 7200.0


class SubmitRefused(Exception):
    """The transport cannot carry this action, said before anything is queued."""


@dataclass(frozen=True)
class Submission:
    """Where one action went, in terms the producer can print."""

    transport: str
    where: Path
    job_id: str | None = None

    def describe(self) -> str:
        return "queued" if self.job_id is None else f"slurm job {self.job_id}"


def default_transport() -> str:
    """The transport this box is cut over to, or the pull queue."""

    return os.environ.get(DEFAULT_TRANSPORT_ENV) or "pool"


def add_transport_argument(parser: argparse.ArgumentParser) -> None:
    """The one flag every producer grows, spelled the same way ``pbrun`` does."""

    parser.add_argument(
        "--transport", choices=TRANSPORTS, default=default_transport(),
        help="which dispatcher carries these submissions (env "
             "PRISMABUILD_TRANSPORT); the pull queue stays the default until "
             "the fleet has cut over to SLURM")


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
    timeout_s: float = DEFAULT_TIMEOUT_S,
    max_attempts: int = 1,
    retry_safe: bool | None = None,
    queue_root: str | Path = SH / "pb-queue",
    lane_root: str | Path | None = None,
    job_entry: str | Path = JOB_ENTRY,
    sbatch: str = "sbatch",
) -> Submission:
    """Enqueue one sealed action on the named transport.

    The action is already in the CAS; this only decides who will run it.  The
    demand vocabulary is the producer's -- ``{"gpu": 1, "mem_gb": 16}`` -- and
    ``LaneResources.from_demand`` maps it to scheduler flags rather than
    letting a second vocabulary grow here.
    """

    if transport not in TRANSPORTS:
        raise SubmitRefused(f"unknown transport {transport!r}")
    key = str(action["action_key"])

    if transport == "pool":
        queue = pool.PoolQueue(queue_root)
        path = queue.publish(
            action_key=key,
            cas_root=str(cas.root),
            checkout_root=checkout_root,
            worker_script=str(worker_script),
            tags=list(tags),
            needs_gpu=needs_gpu,
            priority=priority,
            resources=dict(resources or {}),
            max_attempts=max_attempts,
            retry_safe=retry_safe,
        )
        return Submission(transport="pool", where=path)

    params = action.get("params")
    snapshot = params.get("checkout_snapshot") if isinstance(params, Mapping) else None
    if snapshot is None:
        raise SubmitRefused(
            f"{key[:12]}: the SLURM lane runs only snapshot-addressed actions. "
            "slurm_job materializes params.checkout_snapshot on the node that "
            "won the allocation and refuses an action that carries none, so a "
            "checkout_root would be a submitter's path handed to a scheduler "
            "free to place the job elsewhere. Seal the checkout as a snapshot "
            "(as pbrun does) before submitting this action to SLURM."
        )
    if checkout_root is not None:
        raise SubmitRefused(
            f"{key[:12]}: checkout_root and the lane's sealed snapshot are two "
            "answers to where this action runs; the lane reads the snapshot."
        )

    # A submission is what retires a withdrawal -- the same rule
    # ``PoolQueue.publish`` applies, and the lane's ``run`` with it.  Leaving a
    # live marker in place would make the re-submitted action unrunnable and
    # the only remedy a hand edit of the queue.
    slurm_lane.supersede_withdrawal(queue_root, key)
    lane_resources = slurm_lane.LaneResources.from_demand(dict(resources or {}))
    job = slurm_lane.submit(
        action,
        cas=cas,
        request_path=request_path,
        placement=list(tags),
        resources=lane_resources,
        partition=slurm_lane.partition_for(lane_resources, list(tags)),
        timeout_s=timeout_s,
        worker_script=worker_script,
        job_entry=job_entry,
        root=lane_root,
        sbatch=sbatch,
        retry_safe=retry_safe,
        max_attempts=max_attempts,
    )
    return Submission(transport="slurm", where=job.record_path, job_id=job.job_id)
