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
    """The transport this box is cut over to, or the pull queue."""

    return os.environ.get(DEFAULT_TRANSPORT_ENV) or "pool"


def add_transport_argument(parser: argparse.ArgumentParser) -> None:
    """The one flag every producer grows, spelled the same way ``pbrun`` does."""

    parser.add_argument(
        "--transport", choices=TRANSPORTS, default=default_transport(),
        help="which dispatcher carries these submissions (env "
             "PRISMABUILD_TRANSPORT); the pull queue stays the default until "
             "the fleet has cut over to SLURM")


#: One bundle per (checkout, working-tree state) per process, not one per
#: action.  A dispatcher seals 120 shards out of one tree in one loop, and
#: ``git bundle create`` over that tree 120 times is 119 bundles of identical
#: bytes.  Caching also narrows the window the seal refuses on: the roster is
#: read once, so a tree that moves mid-loop is caught by the identity check
#: rather than producing a hundred subtly different snapshots.
_SNAPSHOT_CACHE: dict[tuple[str, str, str], dict[str, object]] = {}


def seal_checkout_snapshot(
    checkout_root: str | Path,
    *,
    cas: pb.PrismaBuildCAS,
    max_bytes: int | None = None,
) -> dict[str, object]:
    """Seal one checkout as the immutable snapshot the lane addresses.

    ``pbrun`` owns this sequence -- roster, size and transform bounds, bundle
    with ancestry, CAS ingest -- and it is imported rather than reproduced, so
    a producer's snapshot and an interactive ``pbrun``'s are the same object
    under the same rules.

    Args:
        checkout_root: The tree the action runs in. Must be a Git worktree.
        cas: The store the bundle is ingested into.
        max_bytes: A lowered local-disk bound, or ``None`` for pbrun's own.

    Returns:
        The ``params.checkout_snapshot`` record, cached per working-tree state.

    Raises:
        SubmitRefused: The tree cannot be sealed, with pbrun's own reason.
    """

    import pbrun  # deferred: only the SLURM lane needs the sealer

    root = Path(checkout_root)
    bound = (
        pbrun.CHECKOUT_SNAPSHOT_MAX_BYTES if max_bytes is None else int(max_bytes)
    )
    try:
        identity = pb.git_checkout_identity(root)
    except pb.ActionContractError as exc:
        raise SubmitRefused(
            f"{root}: the SLURM lane addresses a checkout through a sealed "
            f"snapshot, and this one cannot be identified: {exc}"
        ) from None
    cached = _SNAPSHOT_CACHE.get(
        key := (str(root), str(identity["head"]), str(identity["dirty_sha256"]))
    )
    if cached is not None:
        return cached
    try:
        snapshot = pbrun.build_git_checkout_snapshot(
            root, cas=cas, max_bytes=bound, expected_identity=identity
        )
    except SystemExit as exc:
        # ``pbrun``'s sealer refuses by exiting, which is right for a terminal
        # and wrong for a library: a dispatcher must get a refusal it can name
        # the action against, not a process that stops mid-loop.
        raise SubmitRefused(f"{root}: {exc}") from None
    _SNAPSHOT_CACHE[key] = snapshot
    return snapshot


def seal_checkout_into_action(
    action: Mapping[str, object],
    *,
    cas: pb.PrismaBuildCAS,
    checkout_root: str | Path,
    max_bytes: int | None = None,
) -> tuple[dict[str, object], dict[str, object]]:
    """Re-seal one action around a snapshot of the tree it runs in.

    Args:
        action: The sealed action the producer built, carrying no snapshot.
        cas: The store the bundle is ingested into.
        checkout_root: The tree the action runs in.
        max_bytes: A lowered local-disk bound, or ``None`` for pbrun's own.

    Returns:
        The re-sealed action and its snapshot record. The action key differs
        from the caller's, because the snapshot is part of what will run.

    Raises:
        SubmitRefused: The tree cannot be sealed.
    """

    snapshot = seal_checkout_snapshot(
        checkout_root, cas=cas, max_bytes=max_bytes
    )
    body = {name: value for name, value in action.items() if name != "action_key"}
    params = dict(body.get("params") or {})
    params["checkout_snapshot"] = snapshot
    body["params"] = params
    body["inputs"] = list(body.get("inputs") or []) + [snapshot["input"]]
    return pb.seal_action(body), snapshot


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
    queue_root: str | Path = SH / "pb-queue",
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
        queue_root: The pull queue root, also where withdrawals live.
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
        return Submission(transport="pool", where=path, action_key=key)

    params = action.get("params")
    snapshot = params.get("checkout_snapshot") if isinstance(params, Mapping) else None
    if snapshot is not None:
        if checkout_root is not None:
            raise SubmitRefused(
                f"{key[:12]}: checkout_root and the lane's sealed snapshot are "
                "two answers to where this action runs; the lane reads the "
                "snapshot."
            )
    elif checkout_root is not None:
        # The producer named a tree, so seal it rather than refuse it.  The
        # key moves, once, because the snapshot is part of what runs -- and
        # the request is republished under the new key so the node can read
        # the action it was actually sent.
        action, _snapshot = seal_checkout_into_action(
            action, cas=cas, checkout_root=checkout_root,
            max_bytes=checkout_snapshot_max_bytes,
        )
        key = str(action["action_key"])
        request_path = cas.publish_action_request(action)
    else:
        raise SubmitRefused(
            f"{key[:12]}: the SLURM lane runs only snapshot-addressed actions. "
            "slurm_job materializes params.checkout_snapshot on the node that "
            "won the allocation and refuses an action that carries none, so a "
            "checkout_root would be a submitter's path handed to a scheduler "
            "free to place the job elsewhere. Seal the checkout as a snapshot "
            "(pass checkout_root, or build one as pbrun does) before "
            "submitting this action to SLURM."
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
    return Submission(
        transport="slurm", where=job.record_path, job_id=job.job_id,
        action_key=key,
    )
