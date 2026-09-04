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
from prismabuild import core as pb, pool  # noqa: E402

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


def _request(action_key: str, *, cas_root: Path) -> dict | None:
    path = Path(cas_root) / "requests" / action_key[:2] / f"{action_key}.json"
    try:
        return json.loads(path.read_text())
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


def _recover(record: dict, *, cas_root: Path) -> tuple[dict | None, str]:
    """Rebuild what a submission needs, or say what is missing."""

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
    return {
        "key": key,
        "action": request,
        "argv": [str(a) for a in argv],
        "cwd": str(cwd),
        "demand": dict(record.get("resources") or {}),
        "tags": [str(t) for t in (record.get("tags") or [])],
    }, ""


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
        signature = (plan["cwd"], json.dumps(plan["argv"]))
        previous = plans.get(signature, {})
        plan["paths"] = previous.get("paths", []) + [path]
        # One piece of work, several endings: if any of them was carried by the
        # lane, the work rides the lane.  A pull-queue record cannot vouch for
        # a queue that has no workers left.
        if previous.get("transport") == "slurm":
            plan["transport"] = "slurm"
        plans[signature] = plan
    return list(plans.values()), skipped


def submit_command(
    plan: Mapping,
    *,
    transport: str,
    priority: int = -10,
    timeout_s: float = 5400.0,
    python: str = sys.executable,
    pbrun: Path = PBRUN,
) -> list[str]:
    """The ``pbrun`` invocation that re-submits one plan.

    ``--transport`` is always stated rather than left to ``pbrun``'s default:
    that default reads ``PRISMABUILD_TRANSPORT`` out of whatever shell the
    operator happens to be in, and a bulk reset that re-routes half the queue
    because of an exported variable is exactly the surprise this tool exists
    to remove.
    """

    command = [
        python, str(pbrun),
        "--transport", str(transport),
        "--cwd", plan["cwd"],
        "--priority", str(priority),
        "--timeout-s", str(timeout_s),
    ]
    for tag in plan["tags"]:
        command += ["--tag", tag]
    if plan["demand"]:
        command += ["--demand", ",".join(
            f"{k}={v}" for k, v in sorted(plan["demand"].items()))]
    return command + ["--"] + list(plan["argv"])


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--apply", action="store_true",
                    help="actually submit; the default only reports")
    ap.add_argument("--priority", type=int, default=-10,
                    help="submit behind everything interactive (default -10)")
    ap.add_argument("--timeout-s", type=float, default=5400.0)
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
        cleared, note = ([], "")
        if args.apply:
            cleared, note = _clear_stale_result(
                plan["action"], plan["cwd"], cas_root=cas_root)
        command = submit_command(
            plan, transport=plan["transport"], priority=args.priority,
            timeout_s=args.timeout_s)
        label = (f"{plan['key'][:12]} x{len(plan['paths'])} "
                 f"{plan['transport']} {plan['cwd']}")
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
        for path in plan["paths"]:
            record = json.loads(path.read_text())
            record["status"] = "reset"
            record["detail"] = {
                "reason": "re-submitted as a fresh action by pool_reset",
                "reset_unix": time.time(),
                "reset_host": socket.gethostname(),
            }
            path.write_text(json.dumps(record, indent=1))
    if not args.apply:
        print("\nnothing submitted; re-run with --apply")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
