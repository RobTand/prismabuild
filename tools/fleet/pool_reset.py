#!/usr/bin/env python3
"""Re-submit the queue's failed items as fresh actions.

A failed item cannot simply be moved back to ``ready``, and the failure
signatures in the live queue are the proof.  Of fifty failures, thirty-two
were the *pinned* state going stale rather than the work going wrong:

* thirteen died on ``declared result path must be absent before execution``
  -- the result file the first attempt left behind, which the retry then
  refused to overwrite, so one transient failure became a permanent one and
  all three attempts burned in a fraction of a second each;
* twelve died on ``live code closure differs from the action-pinned
  closure`` -- the tree moved on between sealing and running, which in a
  checkout several agents commit to is not an accident but the norm;
* seven died on ``KeyError('worker_script')`` -- an item that reached a
  worker without the fields a worker needs.

Requeueing any of those re-runs the same stale pin and re-fails identically.
So this tool resets the *work*, not the record: it recovers each action's
command, working directory and demand, and submits it again through
``pbrun``, which re-seals the closure against the tree as it is now and
declares a fresh per-action result path.  The failed record is filed as
``reset`` so the queue's failure count means something afterwards.

Duplicates are collapsed by (working directory, argv): thirteen attempts at
one suite are one piece of work, and re-running it thirteen times would be a
way of looking busy.

**Which dispatcher a reset rides is a property of the record.**  Since the
SLURM lane files its endings in these same two directories, ``failed/`` holds
records from both transports at once, and re-submitting a lane-filed failure
into the pull queue puts it in a queue no worker drains once the fleet has cut
over.  So each record's own ``transport`` field decides, ``--transport slurm``
forces the whole reset onto the lane, and the child ``pbrun`` is always told
explicitly -- an ambient ``PRISMABUILD_TRANSPORT`` must not silently re-route
work whose ending the other transport filed.

**A sealed action is reset as itself, not re-sealed.**  Everything above is
about a *path-addressed* action, whose pins go stale because a live tree moves
underneath them.  A snapshot-addressed action has no such tree: the checkout is
a commit carried through the CAS, and the declared result path lives in a
materialized tree the last job took away with it, so neither staleness can
reach it.  Re-sealing one through ``pbrun`` would mint a different action key
for the same work, throw away the memoization that makes a re-enqueue free, and
on a box holding no copy of the source could not be done at all.  So such a
record is re-submitted through ``slurm_lane`` unchanged, with the resources,
constraint and exclusivity its own ending recorded.

**A reset detaches, and files no ending.**  ``--apply`` starts every
re-submission and returns; nothing here stays alive to watch a job, and an
ending written now would say ``failed`` about work that is still queued.  A
re-sealed action goes out through a detached ``pbrun``, and a sealed one
through ``slurm_lane.run(detach=True)``, which records the submission under
``<lane root>/<key>/latest.json``.  Either way the ending is ``pbwait``'s to
file, from that record and the CAS receipt -- which is why the key and the job
id are printed: they are what an operator hands ``pbwait``.

``--priority`` reaches both halves.  The lane turns it into the ``sbatch
--nice`` the controller subtracts, so a reset queues behind interactive work
under either dispatcher.  It comes from this tool's own flag rather than from
the record, because the terminal record carries no priority: an ending says
what the work did, not how far back it was queued.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time

SH = Path("/mnt/shared/prismabuild-fleet")
sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
from runtime_paths import generation_root  # noqa: E402
RUNTIME_ROOT = generation_root(__file__)
sys.path.insert(0, str(RUNTIME_ROOT / "src"))
from collections.abc import Mapping  # noqa: E402
from prismabuild import core as pb, pool, slurm_lane  # noqa: E402

PBRUN = RUNTIME_ROOT / "tools" / "pbrun.py"
#: A stale declared result is cleared through ``core.repair_local_result`` and
#: never by globbing.  The prefix is the pool's own dropping, but the file name
#: is per *action fingerprint*, and pbrun tees a live run into the very same
#: path -- so a glob in a shared checkout unlinks the logs of every action
#: currently running there, each of which then burns its full runtime and dies
#: at "action succeeded without its declared result file".  On
#: ``/mnt/shared/tessera-x86`` that is up to fourteen concurrent actions, and
#: this tool did exactly that to them before the audit caught it.
#: ``repair_local_result`` refuses an unclaimed path, a symlink, and an action
#: that already has a CAS receipt, and it takes the same output lock a live
#: producer holds -- which is the whole difference between removing *this*
#: action's leftover and removing somebody else's live log.
RESULT_PREFIX = "pbrun_result."


#: Which dispatcher carries a re-submission.  ``pbrun`` owns the vocabulary;
#: this tool only decides which word to hand it, per record.
TRANSPORTS = ("pool", "slurm")
DEFAULT_TRANSPORT_ENV = "PRISMABUILD_TRANSPORT"


def _request_path(action_key: str, *, cas_root: Path) -> Path:
    """Where the CAS published this action's request.

    The same layout ``pbwait.recorded_action`` reads, spelled once here: the
    lane needs the path as well as the body, because ``slurm_job`` is handed
    the file rather than the object.
    """

    return Path(cas_root) / "requests" / action_key[:2] / f"{action_key}.json"


def _request(action_key: str, *, cas_root: Path) -> dict | None:
    try:
        return json.loads(
            _request_path(action_key, cas_root=cas_root).read_text())
    except (OSError, ValueError):
        return None


def record_transport(record: Mapping, *, requested: str = "pool") -> str:
    """Which transport this ending's work goes back out on.

    The record wins when it names one: the lane stamps ``transport: "slurm"``
    on every ending it files, and that stamp is the only evidence available
    afterwards about which dispatcher ran the action.  ``--transport slurm``
    carries everything else with it, which is what an operator wants on the
    day of the cutover, and nothing here can move a lane-filed failure back
    onto a queue that has no workers.
    """

    if str(record.get("transport") or "") == "slurm":
        return "slurm"
    return "slurm" if requested == "slurm" else "pool"


#: The GRES that means the whole device rather than a sharable slot.  ``gpu:1``
#: and ``shard:N`` are two requests against one GPU: asking for the device is
#: what ``--exclusive`` means, and asking for shards is what a slot means.
EXCLUSIVE_GRES = "gpu:1"


def record_exclusive(record: Mapping) -> bool:
    """Whether this ending's action had the whole device to itself.

    ``resources`` cannot answer it: the lane records the producer's own demand
    vocabulary there, and ``{"gpu": 1}`` is the same claim for an exclusive
    action and a one-slot one.  The lane files the GRES it actually submitted
    beside it, and that is the distinction.

    Args:
        record: One terminal record read out of ``failed/``.

    Returns:
        True when the lane submitted this action with ``--gres=gpu:1``.
    """

    detail = record.get("detail")
    detail = detail if isinstance(detail, Mapping) else {}
    slurm = detail.get("slurm")
    slurm = slurm if isinstance(slurm, Mapping) else {}
    return str(slurm.get("gres") or "") == EXCLUSIVE_GRES


def action_snapshot(action: Mapping) -> Mapping | None:
    """The sealed checkout an action carries, or ``None`` for a path-addressed one."""

    params = action.get("params")
    params = params if isinstance(params, Mapping) else {}
    snapshot = params.get("checkout_snapshot")
    return snapshot if isinstance(snapshot, Mapping) else None


def _recover(record: dict, *, cas_root: Path) -> tuple[dict | None, str]:
    """Rebuild what a submission needs, or say what is missing.

    Two shapes come back, because a failed action is recovered two ways.  A
    path-addressed action is ``reseal``: its command and tree are recovered
    and ``pbrun`` seals it again, which is what clears a stale closure and a
    stale result path.  A snapshot-addressed action is ``resubmit``: it is
    already sealed, and neither staleness can reach it -- the tree is a commit
    and the result path lives in a materialized checkout that no longer
    exists -- so the same action goes back out unchanged.

    Args:
        record: One terminal record read out of ``failed/``.
        cas_root: The store holding the action requests.

    Returns:
        The plan and an empty reason, or ``None`` and why not.
    """

    key = str(record.get("action_key") or "")
    if len(key) != 64:
        return None, "record carries no action key"
    request = _request(key, cas_root=cas_root)
    if request is None:
        return None, "action request is not in the CAS"
    argv = ((request.get("params") or {}).get("command")
            or (request.get("task") or {}).get("argv"))
    if not isinstance(argv, list) or not argv:
        return None, "action request carries no command"
    plan = {
        "key": key,
        "action": request,
        "argv": [str(a) for a in argv],
        "cwd": None,
        "mode": "reseal",
        "demand": dict(record.get("resources") or {}),
        "tags": [str(t) for t in (record.get("tags") or [])],
        "exclusive": record_exclusive(record),
        "retry_safe": bool(record.get("retry_safe")),
        "max_attempts": max(1, int(record.get("max_attempts") or 1)),
        "request_path": str(_request_path(key, cas_root=cas_root)),
    }
    if action_snapshot(request) is not None:
        # Nothing to recover: the action names its own tree, by commit.
        return {**plan, "mode": "resubmit"}, ""
    # The action's own ``working_directory`` is relative to wherever the
    # worker put it (it is literally "." for a pbrun action), so the absolute
    # path lives on the queue item as ``checkout_root``.  Recovering the wrong
    # one submits the command against the wrong tree.
    cwd = record.get("checkout_root") or (request.get("task") or {}).get(
        "working_directory")
    if not cwd or not str(cwd).startswith("/"):
        return None, "no absolute working directory on the item or the action"
    if not Path(cwd).is_dir():
        return None, f"working directory is gone: {cwd}"
    return {**plan, "cwd": str(cwd)}, ""


def _clear_stale_result(
    action: object, cwd: str, *, cas_root: Path = SH / "cas"
) -> tuple[list[str], str]:
    """Clear only this action's own leftover declared result, under its claim."""

    try:
        outcome = pb.repair_local_result(
            action, cas_root=cas_root, checkout_root=cwd)
    except Exception as exc:                                     # noqa: BLE001
        # A refusal here is information, not a failure: "already has a CAS
        # receipt" means the work landed and there is nothing to reset.
        return [], f"{type(exc).__name__}: {exc}"
    removed = outcome.get("removed") if isinstance(outcome, Mapping) else None
    if isinstance(removed, str):
        return [removed], ""
    return ([str(removed)] if removed else []), ""


def plan_resets(
    queue: pool.PoolQueue,
    *,
    cas_root: Path = SH / "cas",
    transport: str = "pool",
    include_reset: bool = False,
) -> tuple[list[dict], list[tuple[str, str]]]:
    """The distinct pieces of work in ``failed/``, and what could not be read.

    Every ending in that directory is a candidate, whichever transport filed
    it: a SLURM ``TIMEOUT`` is a failure the same way an exit code is, and the
    lane files it here with ``detail.slurm.state`` saying which it was.  Each
    plan carries the transport its own record names, so a mixed queue during
    the cutover resets each half onto the dispatcher that ran it.
    """

    failed = sorted(queue.dir(pool.FAILED).glob("*.json"))
    # A withdrawal is a decision, and re-submitting it would undo it.  Two ways
    # a cancelled action still reaches ``failed/``: a worker running bytes that
    # predate ``withdraw`` files its own outcome there (the verb writes
    # ``max_attempts: 1`` into the claimed record precisely so that outcome is
    # terminal rather than a retry), and any worker can lose the claim to a
    # reaper and take ``finish``'s lost-race branch.  The SLURM lane reaches it
    # a third way, filing both the marker and a ``withdrawn`` ending.
    #
    # The guard is generation-scoped -- ``withdrawal_covers`` compares the
    # marker's ``published_unix`` to the record's -- because an action key is a
    # content hash and re-submitting one is the normal way to ask for the same
    # work again.  Matching on the bare key instead turns one cancellation into
    # a permanent ban on the work it named.
    withdrawn = queue.withdrawn_keys()

    plans: dict[tuple[str, str], dict] = {}
    skipped: list[tuple[str, str]] = []
    for path in failed:
        try:
            record = json.loads(path.read_text())
        except (OSError, ValueError) as exc:
            skipped.append((path.stem[:12], f"unreadable: {exc}"))
            continue
        if record.get("status") == "reset" and not include_reset:
            continue
        marker = queue.withdrawal_covers(record, withdrawn=withdrawn)
        if marker is not None or record.get("withdrawn_unix"):
            who = (record.get("withdrawn_by")
                   or (marker or {}).get("withdrawn_by") or "an operator")
            skipped.append((path.stem[:12],
                            f"withdrawn by {who}; a decision, not a defect"))
            continue
        plan, why = _recover(record, cas_root=cas_root)
        if plan is None:
            skipped.append((path.stem[:12], why))
            continue
        plan["transport"] = record_transport(record, requested=transport)
        if plan["mode"] == "resubmit" and plan["transport"] != "slurm":
            # Re-submitting a sealed action is a lane verb: the pull-queue
            # path here is `pbrun`, which re-seals against a live tree, and a
            # snapshot-addressed action has no live tree to name.  Leaving the
            # pull queue alone is deliberate -- it drains until the cutover.
            skipped.append((path.stem[:12], (
                "a snapshot-addressed action is re-submitted as itself, which "
                "only the SLURM lane does; re-run with --transport slurm once "
                "the fleet has cut over")))
            continue
        # A sealed action is its own signature.  Two endings for one key are
        # one piece of work, and two different sealed actions that happen to
        # share an argv are not -- which the pull queue's (tree, argv) pair
        # cannot tell apart, because a sealed action names no tree.
        signature = (
            (plan["cwd"], json.dumps(plan["argv"])) if plan["cwd"]
            else (plan["key"], "")
        )
        previous = plans.get(signature, {})
        plan["paths"] = previous.get("paths", []) + [path]
        # One piece of work, several endings: if any of them was carried by the
        # lane, the work rides the lane.  A pull-queue record cannot vouch for
        # a queue that has no workers left.
        if previous.get("transport") == "slurm":
            plan["transport"] = "slurm"
        plans[signature] = plan
    return list(plans.values()), skipped


def resubmit_sealed(
    plan: Mapping,
    *,
    cas_root: Path,
    queue_root: Path,
    priority: int = -10,
    timeout_s: float | None = None,
    lane_root: str | Path | None = None,
    runtime_root: Path = RUNTIME_ROOT,
    **lane_commands,
):
    """Send one already-sealed action back to the scheduler, unchanged.

    A reset of a sealed action is a re-submission of the *same* action, not a
    re-seal: the two staleness bugs this tool exists for cannot reach it.  A
    snapshot pins the tree by commit, so no live checkout can have moved under
    it, and the declared result path lives in a materialized tree the last job
    took away with it, so nothing is left behind to refuse.  Re-sealing it
    through ``pbrun`` would instead mint a *different* action key for the same
    work and throw away the memoization -- and on a box with no copy of the
    source tree it could not be done at all.

    **This detaches, and files no ending.**  ``--apply`` starts every reset and
    returns; nothing here stays alive to watch a job, and an ending written now
    would say ``failed`` about work still queued.  So the submission is
    recorded (``<lane root>/<key>/latest.json``) and the ending is ``pbwait``'s
    to file, from that record and the CAS receipt, exactly as for
    ``pbrun --detach``.  The key and the job id are printed for that reason:
    they are what an operator hands ``pbwait``.

    Args:
        plan: One ``mode="resubmit"`` plan from ``plan_resets``.
        cas_root: The store holding the action request and its receipt.
        queue_root: The pull queue root, where withdrawals are read.
        priority: How far behind interactive work to queue it; the lane sends
            it as the ``--nice`` the controller subtracts.
        timeout_s: A deadline to enforce, or ``None`` for none.
        lane_root: The SLURM lane root, or ``None`` for the configured one.
        runtime_root: The generation whose worker and job entry are used.
        **lane_commands: Scheduler binaries, for tests.

    Returns:
        The accepted ``slurm_lane.SubmittedJob``, or ``None`` if none was.
    """

    action = plan["action"]
    resources = slurm_lane.LaneResources.from_demand(
        dict(plan["demand"]), exclusive=bool(plan.get("exclusive")))
    tags = [str(tag) for tag in plan["tags"]]
    result = slurm_lane.run(
        action,
        cas=pb.PrismaBuildCAS(cas_root),
        request_path=plan["request_path"],
        placement=tags,
        resources=resources,
        partition=slurm_lane.partition_for(resources, tags),
        priority=int(priority),
        timeout_s=timeout_s,
        worker_script=runtime_root / "tools" / "prismabuild_worker.py",
        job_entry=runtime_root / "tools" / "fleet" / "slurm_job.py",
        retry_safe=bool(plan.get("retry_safe")),
        max_attempts=int(plan.get("max_attempts") or 1),
        root=lane_root,
        queue_root=queue_root,
        detach=True,
        **lane_commands,
    )
    last = result.last
    return None if last is None else last[0]


def submit_command(
    plan: Mapping,
    *,
    transport: str,
    priority: int = -10,
    timeout_s: float | None = None,
    python: str = sys.executable,
    pbrun: Path = PBRUN,
) -> list[str]:
    """The ``pbrun`` invocation that re-submits one plan.

    ``--transport`` is always stated rather than left to ``pbrun``'s default:
    that default reads ``PRISMABUILD_TRANSPORT`` out of whatever shell the
    operator happens to be in, and a bulk reset that re-routes half the queue
    because of an exported variable is exactly the surprise this tool exists
    to remove.

    ``--exclusive`` is restored from the GRES the lane recorded, because the
    demand alone cannot say it: ``{"gpu": 1}`` re-emitted on its own becomes
    ``shard:1``, a sharable slot, and an action that failed while it had the
    device to itself would be retried beside other work.

    Args:
        plan: One recovered piece of work.
        transport: The dispatcher this re-submission rides.
        priority: How far behind interactive work to queue it.
        timeout_s: A deadline to enforce, or ``None`` for none.
        python: The interpreter that runs ``pbrun``.
        pbrun: The submitter to run.

    Returns:
        The argv to run.
    """

    command = [
        python, str(pbrun),
        "--transport", str(transport),
        "--cwd", plan["cwd"],
        "--priority", str(priority),
    ]
    if timeout_s is not None:
        # Only when an operator asked for one.  Under SLURM a deadline is an
        # enforced kill, and elapsed time is not evidence that a worker is
        # dead: a job still making progress at 90 minutes is a job to leave
        # running.  The pull queue never enforced one either.
        command += ["--timeout-s", str(timeout_s)]
    if plan.get("exclusive"):
        command.append("--exclusive")
    for tag in plan["tags"]:
        command += ["--tag", tag]
    if plan["demand"]:
        command += ["--demand", ",".join(
            f"{k}={v}" for k, v in sorted(plan["demand"].items()))]
    return command + ["--"] + list(plan["argv"])


def _file_reset(plan: Mapping, *, reason: str) -> None:
    """Mark this plan's endings ``reset`` so the failure count means something."""

    for path in plan["paths"]:
        record = json.loads(path.read_text())
        record["status"] = "reset"
        record["detail"] = {
            "reason": reason,
            "reset_unix": time.time(),
            "reset_host": socket.gethostname(),
        }
        path.write_text(json.dumps(record, indent=1))


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--apply", action="store_true",
                    help="actually submit; the default only reports")
    ap.add_argument("--priority", type=int, default=-10,
                    help="submit behind everything interactive (default -10)")
    ap.add_argument("--timeout-s", type=float, default=None,
                    help="enforce this deadline on each re-submission; the "
                         "default sends none, because elapsed time is not "
                         "evidence that a worker is dead and under SLURM a "
                         "deadline is an enforced kill")
    ap.add_argument("--limit", type=int, default=0,
                    help="submit at most this many (0 = all)")
    ap.add_argument("--include-reset", action="store_true",
                    help="re-include items a previous run already marked reset "
                         "(use when that run's submissions did not survive)")
    ap.add_argument(
        "--transport", choices=TRANSPORTS,
        default=os.environ.get(DEFAULT_TRANSPORT_ENV) or "pool",
        help="dispatcher for records that do not name one (env "
             "PRISMABUILD_TRANSPORT); a record filed by the SLURM lane always "
             "goes back out on the lane whatever this says")
    ap.add_argument("--queue-root", default=str(SH / "pb-queue"),
                    help=argparse.SUPPRESS)
    ap.add_argument("--cas-root", default=str(SH / "cas"),
                    help=argparse.SUPPRESS)
    args = ap.parse_args(argv)

    queue = pool.PoolQueue(Path(args.queue_root))
    cas_root = Path(args.cas_root)
    ordered, skipped = plan_resets(
        queue, cas_root=cas_root, transport=args.transport,
        include_reset=args.include_reset)

    print(f"{len(ordered)} distinct pieces of work, "
          f"{len(skipped)} unrecoverable")
    for key, why in skipped:
        print(f"  skip {key}  {why}")

    if args.limit:
        ordered = ordered[: args.limit]
    for plan in ordered:
        label = (f"{plan['key'][:12]} x{len(plan['paths'])} "
                 f"{plan['transport']} {plan['cwd'] or 'sealed checkout'}")
        if plan["mode"] == "resubmit":
            if not args.apply:
                print(f"  would resubmit {label}\n    "
                      f"the sealed action itself, unchanged")
                continue
            job = resubmit_sealed(
                plan, cas_root=cas_root, queue_root=Path(args.queue_root),
                priority=args.priority, timeout_s=args.timeout_s)
            if job is None:                     # unreachable: run submits once
                print(f"  nothing submitted for {label}")
                continue
            # The key and the job id, because they are what an operator hands
            # ``pbwait``: this detached, so the ending is filed by whoever
            # waits, from the submission record this just wrote.
            print(f"  resubmitted {label} as slurm job {job.job_id}"
                  f"  (pbwait.py {plan['key'][:12]})")
            _file_reset(plan, reason=(
                "re-submitted as the same sealed action by pool_reset; the "
                f"ending is pbwait's to file for slurm job {job.job_id}"))
            continue
        cleared, note = ([], "")
        if args.apply:
            cleared, note = _clear_stale_result(
                plan["action"], plan["cwd"], cas_root=cas_root)
        command = submit_command(
            plan, transport=plan["transport"], priority=args.priority,
            timeout_s=args.timeout_s)
        if not args.apply:
            print(f"  would submit {label}\n    {' '.join(command[3:])}")
            continue
        if cleared:
            print(f"  cleared this action's stale result: {cleared[0]}")
        elif note:
            print(f"  no result to clear ({note})")
        proc = subprocess.Popen(
            command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        print(f"  submitted {label} (pid {proc.pid})")
        _file_reset(plan, reason="re-submitted as a fresh action by pool_reset")
    if not args.apply:
        print("\nnothing submitted; re-run with --apply")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
