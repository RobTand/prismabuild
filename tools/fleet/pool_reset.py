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
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import socket
import subprocess
import sys
import time

SH = Path("/mnt/shared/prismabuild-fleet")
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
from collections.abc import Mapping  # noqa: E402
from prismabuild import core as pb, pool  # noqa: E402

PBRUN = Path(__file__).resolve().parent / "pbrun.py"
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


def _request(action_key: str) -> dict | None:
    path = SH / "cas" / "requests" / action_key[:2] / f"{action_key}.json"
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return None


def _recover(record: dict) -> tuple[dict | None, str]:
    """Rebuild what a submission needs, or say what is missing."""

    key = str(record.get("action_key") or "")
    if len(key) != 64:
        return None, "record carries no action key"
    request = _request(key)
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


def _clear_stale_result(action: object, cwd: str) -> tuple[list[str], str]:
    """Clear only this action's own leftover declared result, under its claim."""

    try:
        outcome = pb.repair_local_result(
            action, cas_root=SH / "cas", checkout_root=cwd)
    except Exception as exc:                                     # noqa: BLE001
        # A refusal here is information, not a failure: "already has a CAS
        # receipt" means the work landed and there is nothing to reset.
        return [], f"{type(exc).__name__}: {exc}"
    removed = outcome.get("removed") if isinstance(outcome, Mapping) else None
    if isinstance(removed, str):
        return [removed], ""
    return ([str(removed)] if removed else []), ""


def main() -> int:
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
    args = ap.parse_args()

    queue = pool.PoolQueue(SH / "pb-queue")
    failed = sorted(queue.dir(pool.FAILED).glob("*.json"))

    plans: dict[tuple[str, str], dict] = {}
    skipped: list[tuple[str, str]] = []
    for path in failed:
        try:
            record = json.loads(path.read_text())
        except (OSError, ValueError) as exc:
            skipped.append((path.stem[:12], f"unreadable: {exc}"))
            continue
        if record.get("status") == "reset" and not args.include_reset:
            continue
        plan, why = _recover(record)
        if plan is None:
            skipped.append((path.stem[:12], why))
            continue
        signature = (plan["cwd"], json.dumps(plan["argv"]))
        plan["paths"] = plans.get(signature, {}).get("paths", []) + [path]
        plans[signature] = plan

    print(f"{len(failed)} failed items -> {len(plans)} distinct pieces of work, "
          f"{len(skipped)} unrecoverable")
    for key, why in skipped:
        print(f"  skip {key}  {why}")

    ordered = list(plans.values())
    if args.limit:
        ordered = ordered[: args.limit]
    for plan in ordered:
        cleared, note = ([], "")
        if args.apply:
            cleared, note = _clear_stale_result(plan["action"], plan["cwd"])
        command = [
            sys.executable, str(PBRUN),
            "--cwd", plan["cwd"],
            "--priority", str(args.priority),
            "--timeout-s", str(args.timeout_s),
        ]
        for tag in plan["tags"]:
            command += ["--tag", tag]
        if plan["demand"]:
            command += ["--demand", ",".join(
                f"{k}={v}" for k, v in sorted(plan["demand"].items()))]
        command += ["--"] + plan["argv"]
        label = f"{plan['key'][:12]} x{len(plan['paths'])} {plan['cwd']}"
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
