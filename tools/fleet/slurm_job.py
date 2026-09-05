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
   remove -- because a job killed at its time limit does not get to clean up
   after itself;
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


def _load_action(path: Path) -> dict[str, object]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise SystemExit(f"slurm_job: action request is not an object: {path}")
    return value


def _container_owner(action: dict[str, object]) -> str:
    environment = action.get("environment")
    variables = (
        environment.get("variables") if isinstance(environment, dict) else None
    )
    if not isinstance(variables, dict):
        return ""
    return str(variables.get(CONTAINER_OWNER_ENV) or "")


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


def _temporary_root(checkout: Path, base: Path) -> Path | None:
    """The per-action tree the materializer made, from the path it yielded.

    ``mkdtemp`` created one directory directly under ``base`` and everything
    else hangs below it, so the tree to remove is the ancestor whose parent is
    the root.  Derived rather than reported so the materializer keeps the
    signature both transports share.
    """

    try:
        resolved_base = base.resolve()
        current = checkout.resolve()
    except OSError:
        return None
    while current != current.parent:
        if current.parent == resolved_base:
            return current
        current = current.parent
    return None


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
        f"checkout_dir={checkout_dir or ''}",
        f"local_checkout_root={local_checkout_root}",
        f"host={socket.gethostname()}",
        f"pid={os.getpid()}",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.parent / f".{path.name}.{os.getpid()}.tmp"
    tmp.write_text("\n".join(lines) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--action", required=True,
                        help="the published CAS action request to execute")
    parser.add_argument("--cas-root", required=True)
    parser.add_argument("--worker", required=True,
                        help="tools/prismabuild_worker.py to exec")
    parser.add_argument("--worker-python", default="/usr/bin/python3")
    parser.add_argument("--lane-dir", default="",
                        help="this action's lane directory: the job's logs and "
                             "the action's exit status go here")
    parser.add_argument("--job-state-root", default="",
                        help="where to leave this job's Epilog state file")
    parser.add_argument("--checkout-root", default="",
                        help="box-local root for materialized trees")
    parser.add_argument("--job-id", default="",
                        help="the job this is; SLURM_JOB_ID when SLURM set it")
    args = parser.parse_args(argv)

    action = _load_action(Path(args.action))
    cas_root = Path(args.cas_root)
    key = str(action["action_key"])
    owner = _container_owner(action)
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
    state_path = (
        Path(args.job_state_root) / f"{job_id}.job"
        if args.job_state_root and job_id else None
    )

    # Written before materialization as well as after it: a job killed while
    # git is still fetching has containers only if the action started one (it
    # has not), but it may already own a partial tree, and the Epilog can only
    # remove what somebody wrote down.
    if state_path is not None:
        _write_job_state(
            state_path,
            action_key=key,
            container_owner=owner,
            checkout_dir=None,
            local_checkout_root=local_root,
        )

    item = _queue_item(action, cas_root=cas_root)
    with materialize._execution_checkout(
        item, local_checkout_root=local_root
    ) as checkout_root:
        if state_path is not None:
            _write_job_state(
                state_path,
                action_key=key,
                container_owner=owner,
                checkout_dir=_temporary_root(Path(checkout_root), local_root),
                local_checkout_root=local_root,
            )
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
        # and no __exit__.  That case is the Epilog's, which reads the state
        # file written above and removes the checkout and any containers as
        # root; smoke row 8 is the evidence for that path.
        completed = subprocess.run(
            worker,
            check=False,
            env=_worker_environment(
                dict(os.environ), lane_dir=str(args.lane_dir), job_id=job_id
            ),
        )
    if state_path is not None:
        # Removed last: from here on the Epilog has nothing left to do that
        # this process has not already done.
        try:
            state_path.unlink()
        except OSError:
            pass
    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
