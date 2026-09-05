#!/usr/bin/python3
"""What a SLURM batch job runs: materialize the snapshot, then the worker.

The action's sealed checkout has to be reconstructed on the node that won the
allocation, not on the box that submitted it -- the scheduler is free to place
the job anywhere the constraints allow, and a path materialized at submit time
exists on exactly one machine.  So the batch script execs this, and this does
three things and nothing else:

1. materialize the sealed snapshot through ``prismabuild.materialize``, the
   same sequence the pull-queue worker runs, so the tree an action executes in
   does not depend on which transport delivered it;
2. leave the node-side Epilog the two facts it will need after this process is
   gone -- the container-ownership label the Docker shim stamps, and the tree to
   remove -- and leave that state file in place on the way out, because the
   Epilog is the single owner of node-side cleanup;
3. exec the canonical ``run-local`` worker argv inside that tree, via
   ``pool.worker_argv``, so the launch is the pull queue's launch to the byte.

Stdlib only, and ``/usr/bin/python3`` on both architectures: this is a
launcher, and the action's own sealed argv[0] selects the interpreter the work
actually needs.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import socket
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
from runtime_paths import generation_root  # noqa: E402

RUNTIME_ROOT = generation_root(__file__)
sys.path.insert(0, str(RUNTIME_ROOT / "src"))
from prismabuild import core, materialize, pool, slurm_lane  # noqa: E402

#: The environment variable the Docker shim reads to label containers, and
#: therefore the one the Epilog needs to find them again.  Read from the sealed
#: action rather than recomputed: recomputing an identity is how two answers to
#: one question get into a system.
CONTAINER_OWNER_ENV = "PRISMABUILD_CONTAINER_OWNER"

#: The durable marker the shim writes on first container creation, and the
#: file the pull queue's ``cleanup_action_containers`` unlinks once an action's
#: containers are gone.  Under SLURM nothing unlinked it, so the shared
#: ``container-owners/`` directory grew one file per containerized action and
#: never shrank.  The Epilog does it now, which is why the path is written down
#: here: it is in the sealed environment, so it is read rather than rebuilt.
CONTAINER_MARKER_ENV = "PRISMABUILD_CONTAINER_MARKER"


def _load_action(path: Path) -> dict[str, object]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise SystemExit(f"slurm_job: action request is not an object: {path}")
    return value


def _sealed_variable(action: dict[str, object], name: str) -> str:
    environment = action.get("environment")
    variables = (
        environment.get("variables") if isinstance(environment, dict) else None
    )
    if not isinstance(variables, dict):
        return ""
    return str(variables.get(name) or "")


def _container_owner(action: dict[str, object]) -> str:
    return _sealed_variable(action, CONTAINER_OWNER_ENV)


def _container_marker(action: dict[str, object]) -> str:
    return _sealed_variable(action, CONTAINER_MARKER_ENV)


def _queue_item(action: dict[str, object], *, cas_root: Path) -> dict[str, object]:
    """The three fields ``materialize`` reads, taken from the sealed action.

    Shaped like a pool item on purpose: the materializer's input contract is
    the same for both transports, so neither gets its own dialect of it.  An
    action is addressed one of two ways, and both reach here as the pool
    would carry them: a sealed snapshot, materialized on whichever node won
    the allocation, or a box-local checkout root -- a path, which exists on
    exactly one machine and reaches this launcher only because the submitter
    pinned the job there (``--here`` and a non-portable checkout both become
    a host constraint).  Refusing the second shape made ``--here`` a
    pool-only flag for no reason the scheduler imposes.
    """

    raw_params = action.get("params")
    params = raw_params if isinstance(raw_params, dict) else {}
    item: dict[str, object] = {
        "action_key": str(action["action_key"]),
        "cas_root": str(cas_root),
    }
    snapshot = params.get("checkout_snapshot")
    if snapshot is not None:
        item["checkout_snapshot"] = snapshot
        return item
    root = params.get("checkout_root")
    if root:
        item["checkout_root"] = str(root)
        return item
    raise SystemExit(
        "slurm_job: this action carries neither a checkout snapshot nor a "
        "checkout root, so there is no tree to execute it in"
    )


def _worker_environment(
    environment: dict[str, str], *, lane_dir: str, job_id: str
) -> dict[str, str]:
    """The worker's environment, plus where to leave the action's exit status.

    The worker exits 1 for any failure, so its status cannot say what the
    action's was, and the launch argv cannot carry the question either:
    ``pool.worker_argv`` is pinned byte-identical across both transports so
    that one action means one execution whichever delivered it. The request
    travels in the environment instead, and only when this job has both a lane
    directory to write in and an id to name the file after.

    The action itself never sees this variable. ``run_local_action`` builds the
    sealed environment the action's argv runs in, and this is not in it.
    """

    if not lane_dir or not job_id:
        return dict(environment)
    return {
        **environment,
        core.ACTION_STATUS_PATH_ENV: str(
            slurm_lane.action_status_path(lane_dir, job_id)
        ),
    }


def _write_job_state(
    path: Path,
    *,
    action_key: str,
    container_owner: str,
    container_marker: str,
    container_job: str,
    checkout_dir: Path | None,
    local_checkout_root: Path,
) -> None:
    """Leave the Epilog what it cannot ask this process for once it is killed.

    Written as flat ``key=value`` lines, not JSON: the reader is a shell script
    running as root on the node, and giving it a parser to get wrong would be a
    worse trade than giving it ``sed``.
    """

    lines = [
        f"action_key={action_key}",
        f"container_owner={container_owner}",
        f"container_marker={container_marker}",
        f"container_job={container_job}",
        f"checkout_dir={checkout_dir or ''}",
        f"local_checkout_root={local_checkout_root}",
        f"host={socket.gethostname()}",
        f"pid={os.getpid()}",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.parent / f".{path.name}.{os.getpid()}.tmp"
    tmp.write_text("\n".join(lines) + "\n", encoding="utf-8")
    # Mode stated rather than inherited from the submitter's umask.  The reader
    # is the Epilog, and it reaches this file as a root-squashed user over NFS:
    # under ``umask 077`` the file is 0600, every ``sed`` reads nothing, and the
    # containers and the checkout leak while the state file itself is still
    # deleted.  Nothing here is a secret -- an action key, a container label,
    # and a path under a root every job's user can already list.
    os.chmod(tmp, 0o644)
    os.replace(tmp, path)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--action", required=True,
                        help="the published CAS action request to execute")
    parser.add_argument("--cas-root", required=True,
                        help="the CAS root holding the action request and "
                             "receiving its result")
    parser.add_argument("--worker", required=True,
                        help="tools/prismabuild_worker.py to exec")
    parser.add_argument("--worker-python", default="/usr/bin/python3",
                        help="interpreter that launches the worker on the "
                             "node that won the allocation, not on the "
                             "submitter")
    parser.add_argument("--lane-dir", default="",
                        help="this action's lane directory: the job's logs and "
                             "the action's exit status go here")
    parser.add_argument("--job-state-root", default="",
                        help="where to leave this job's Epilog state file; "
                             "defaults to the node-side root the Epilog reads "
                             f"(${slurm_lane.JOB_STATE_ROOT_ENV}, else "
                             f"{slurm_lane.DEFAULT_JOB_STATE_ROOT})")
    parser.add_argument("--checkout-root", default="",
                        help="box-local root for materialized trees")
    parser.add_argument("--job-id", default="",
                        help="the job this is; SLURM_JOB_ID when SLURM set it")
    args = parser.parse_args(argv)

    action = _load_action(Path(args.action))
    cas_root = Path(args.cas_root)
    key = str(action["action_key"])
    owner = _container_owner(action)
    marker = _container_marker(action)
    local_root = (
        Path(args.checkout_root) if args.checkout_root
        else materialize.LOCAL_CHECKOUT_ROOT
    )
    # SLURM sets this in every batch job, so the script does not pass it.  The
    # explicit form is for a launcher re-run by hand, and for a test that must
    # not claim SLURM membership the worker would then insist on attesting.
    job_id = (
        args.job_id
        or os.environ.get("SLURM_JOB_ID")
        or os.environ.get("SLURM_JOBID")
        or ""
    )
    # Resolved here, on the node, and never handed down by the submitter: the
    # Epilog reads the same variable and the same default, and it can see
    # neither slurmd's environment nor the submitter's.  A batch script that
    # carried the submitter's lane root pointed the job at a directory the
    # Epilog would never look in, so a killed job leaked its checkout and its
    # containers with nothing said.
    state_root = slurm_lane.job_state_directory(args.job_state_root or None)
    state_path = state_root / f"{job_id}.job" if job_id else None

    # Ask the CAS before materializing anything, and before writing the
    # Epilog's state file: nothing has been created yet, so there is nothing
    # for the Epilog to clean up on this path.
    #
    # ``run-local`` asks the same question, but only once the snapshot is
    # checked out -- which for a large snapshot is minutes of git and disk to
    # learn what one lookup on the shared mount already knows.  It costs a job
    # id either way, and that is the point of the singleton dependency the
    # submitter sends: the second caller of one action key waits for the first,
    # then arrives here, reads the receipt the first published, and ends
    # without running or materializing anything.
    cas = core.PrismaBuildCAS(cas_root)
    try:
        receipt = cas.lookup(action)
    except core.PrismaBuildError as exc:
        # A request this launcher cannot validate, or a receipt it cannot
        # verify, is not its verdict to give.  Say so and take the ordinary
        # path, where the worker asks the same question with the checkout in
        # place and answers it the way it always did.
        print(f"slurm_job: the CAS could not be asked about {key[:12]} "
              f"({exc}); materializing and letting the worker decide",
              file=sys.stderr, flush=True)
        receipt = None
    if receipt is not None:
        if args.lane_dir and job_id:
            # What the submitter reads to file ``cache_hit`` rather than
            # ``executed``.  From outside, the two look alike: a receipt
            # exists either way.
            slurm_lane.write_cache_hit(
                slurm_lane.cache_hit_path(args.lane_dir, job_id),
                action_key=key,
                job_id=job_id,
                result_digest=receipt.get("result_digest"),
            )
        print(f"slurm_job: {key[:12]} is already in the CAS; nothing to run",
              flush=True)
        return 0

    def leave_epilog_state(checkout_dir: Path | None) -> None:
        if state_path is None:
            return
        _write_job_state(
            state_path,
            action_key=key,
            container_owner=owner,
            container_marker=marker,
            container_job=job_id,
            checkout_dir=checkout_dir,
            local_checkout_root=local_root,
        )

    # Written before materialization: a job killed while git is still fetching
    # has containers only if the action started one (it has not), but the
    # ownership label has to be on disk before anything can create one.
    leave_epilog_state(None)

    item = _queue_item(action, cas_root=cas_root)
    with materialize._execution_checkout(
        item,
        local_checkout_root=local_root,
        # The tree is named the instant ``mkdtemp`` makes it, before the fetch
        # that fills it runs.  ``epilog.sh`` removes a checkout only when
        # ``checkout_dir`` is non-empty, and the real path used to be written
        # only after the fetch finished -- which for a large snapshot is
        # minutes of git.  A job killed by a time limit or a ``scancel`` in
        # that window left its tree under the local checkout root forever,
        # because nothing that ran afterwards knew the name ``mkdtemp`` chose.
        #
        # An action addressed by a live checkout root materializes nothing, so
        # this is never called for one and the Epilog is never handed a path
        # to a tree it must not delete.
        on_temporary=leave_epilog_state,
    ) as checkout_root:
        worker = [str(args.worker_python)] + pool.worker_argv(
            worker_script=args.worker,
            action_key=key,
            cas_root=cas_root,
            checkout_root=checkout_root,
        )
        # A child rather than an exec: on a normal ending the materializer's
        # cleanup runs on the way out of the context manager, and an exec would
        # replace the process that owes it.  On a scheduler kill it does not:
        # SLURM's time limit signals the whole step, and Python's default
        # SIGTERM disposition ends this interpreter at once, with no finally
        # and no __exit__.  Either way the Epilog runs afterwards, reads the
        # state file written above, and removes the checkout and any containers
        # as root; smoke row 8 is the evidence for that path.
        completed = subprocess.run(
            worker,
            check=False,
            env=_worker_environment(
                dict(os.environ), lane_dir=str(args.lane_dir), job_id=job_id
            ),
        )
    # The state file is deliberately NOT removed here.  It used to be, on every
    # ending this process reached, and that made the Epilog's first check --
    # "no state file, nothing to do" -- true for exactly the jobs whose
    # containers were still running.  A container the action started is
    # reparented to containerd-shim and outlives the job whether the job was
    # killed or not; under the pull queue `finish` removed it on every ending,
    # and under SLURM the Epilog is the only thing that can.  So the Epilog
    # owns all of it and this process leaves it the file it needs.
    #
    # Nothing is cleaned twice: the materializer already removed the checkout
    # on a normal ending, and the Epilog removes a recorded tree only if it is
    # still a directory.  Nothing is left behind either: the Epilog deletes the
    # state file itself, as the job's user, for the NFS reason it documents.
    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
