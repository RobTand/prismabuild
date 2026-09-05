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
work whose ending the other transport filed.  For a record that names no
transport the answer comes from ``fleet_submit.default_transport``: the
environment, then the published generation's receipt, then the pull queue.

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
ending written now would say ``failed`` about work that is still queued.  It
does wait one shared ``REFUSAL_WINDOW_S`` before stamping the records, because
a child that refuses does so at once and a run that reported it as submitted
was reporting work that does not exist.  Nothing is signalled at the end of
that window: a child still running has been admitted, and its output is kept
under ``<queue root>/resets/`` either way.  A
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
from pathlib import Path
import socket
import subprocess
import sys
import time

SH = Path("/mnt/shared/prismabuild-fleet")
sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
from runtime_paths import (  # noqa: E402
    fleet_tool, generation_root, tool_candidates,
)
RUNTIME_ROOT = generation_root(__file__)
sys.path.insert(0, str(RUNTIME_ROOT / "src"))
from collections.abc import Mapping, Sequence  # noqa: E402
import fleet_submit  # noqa: E402
import pbrun  # noqa: E402
from prismabuild import core as pb, pool, slurm_lane  # noqa: E402

#: The submitter this tool re-submits through, under whichever layout the
#: runtime containing it uses.  ``None`` when neither layout has one, which
#: ``submit_command`` refuses on rather than handing a child a path that is
#: not there.
PBRUN = fleet_tool("pbrun.py", root=RUNTIME_ROOT)
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


#: Which dispatcher carries a re-submission is ``fleet_submit``'s vocabulary,
#: read from there rather than restated here: the cutover travels in the
#: published generation's receipt, and a second copy of the list is a second
#: place for it to go stale.


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
        # Nothing to run when the key already holds a receipt.  A key is a
        # content hash and is submitted again every time the same work is
        # asked for, so an older generation's failed/ record can sit beside a
        # newer generation's receipt; a submission would only spend a job to
        # be told ``cache_hit``.  The path-addressed branch gets the same
        # refusal from ``repair_local_result`` at apply time.
        if _has_receipt(request, cas_root=cas_root):
            return None, "already has a CAS receipt; the work landed"
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


def _has_receipt(request: Mapping, *, cas_root: Path) -> bool:
    """Whether the CAS already holds a verified receipt for this action.

    A request the CAS cannot validate, or a receipt it cannot verify, counts as
    no receipt: the reset then plans the action the way it always did, and the
    lane's own ``cache_hit`` check is the one that answers at run time.
    """

    try:
        return pb.PrismaBuildCAS(cas_root).lookup(request) is not None
    except (pb.PrismaBuildError, OSError, ValueError):
        return False


def _clear_stale_result(
    action: object, cwd: str, *, cas_root: Path | None = None
) -> tuple[list[str], str]:
    """Clear only this action's own leftover declared result, under its claim."""

    if cas_root is None:
        # Resolved on the call, so a repointed ``SH`` is honoured; see the
        # same note on ``fleet_submit.submit``.
        cas_root = SH / "cas"

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
    cas_root: Path | None = None,
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

    if cas_root is None:
        # Resolved on the call, so a repointed ``SH`` is honoured; see the
        # same note on ``fleet_submit.submit``.
        cas_root = SH / "cas"

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

    Raises:
        slurm_lane.SlurmLaneError: The controller refused the submission --
            an unknown Feature, an impossible GRES.  The caller reports it
            and moves on to the next plan; this record stays ``failed``.

    The partition is derived again from the demand and the tags, as
    ``pbrun`` derives it, because the terminal record carries neither the
    partition the submitter was given nor ``--anywhere``.  An action that
    was submitted portable therefore goes back out as untagged CPU work, to
    the ``cpu`` partition rather than to every box: narrower than it was,
    and still correct, since the partition is not part of the action.
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

    ``--detach`` is what this tool already meant.  A reset submits and walks
    away, and an attached ``pbrun`` instead waits out ``--wait-s``, which
    defaults to a day: the child sat in the background holding a log open for
    the whole run, and the only thing it printed that this tool could act on
    was whatever it said before it refused.  Detached, the child submits, says
    on one line where the work went, and exits -- so the re-submission becomes
    something this tool can *read* rather than infer from a clock.  Detaching
    carries one attempt, which is ``pbrun``'s own default, and this tool asks
    for no more.

    Args:
        plan: One recovered piece of work.
        transport: The dispatcher this re-submission rides.
        priority: How far behind interactive work to queue it.
        timeout_s: A deadline to enforce, or ``None`` for none.
        python: The interpreter that runs ``pbrun``.
        pbrun: The submitter to run.

    Returns:
        The argv to run.

    Raises:
        SystemExit: There is no ``pbrun.py`` under this runtime root. Nothing
            can be re-submitted at all, so it is said once rather than as a
            child's "can't open file" per recovered record.
    """

    if pbrun is None or not Path(pbrun).is_file():
        looked = " and ".join(
            str(candidate)
            for candidate in tool_candidates("pbrun.py", root=RUNTIME_ROOT)
        )
        raise SystemExit(
            f"pool_reset: no pbrun.py to re-submit through; looked for "
            f"{looked}"
        )
    command = [
        python, str(pbrun),
        "--detach",
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


#: How often ``--apply`` says which re-submissions have not answered yet.
#: Not a deadline: nothing is signalled when it elapses, and no record is
#: stamped from the fact that it has.  A child that is still sealing a tree is
#: making progress, and the only thing this interval decides is how often the
#: operator is told so.
PROGRESS_INTERVAL_S = 10.0

#: How often the children are polled between those lines.  Small enough that a
#: batch of fast submissions is not paced by it, large enough that waiting for
#: a slow seal costs nothing.
POLL_S = 0.25

#: Where a re-submission's own output is kept, under the queue root.  A
#: directory of its own, because every reader of this queue addresses the
#: state directories by ``<key>.json`` and none of them looks here.
RESETS = "resets"


def resubmission_log(queue_root: Path, key: str) -> Path:
    """Where one re-submission's output goes.

    Named by key and start time, so a second reset of the same action does not
    overwrite the evidence from the first.
    """

    return Path(queue_root) / RESETS / f"{key}.{time.time():.6f}.log"


def start_resubmission(
    command: Sequence[str], *, queue_root: Path, key: str
) -> tuple[subprocess.Popen, Path]:
    """Start one detached re-submission, keeping what it says.

    Its output used to go to ``/dev/null``, which is the whole of why a
    refusal was invisible: the child said why it would not run and nobody
    could read it.
    """

    log = resubmission_log(queue_root, key)
    log.parent.mkdir(parents=True, exist_ok=True)
    with log.open("wb") as handle:
        process = subprocess.Popen(
            list(command), stdout=handle, stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    return process, log


def announced_submission(said: str) -> dict | None:
    """The line ``pbrun --detach`` printed, found among everything else.

    ``pbrun`` writes that line to stdout and everything meant for a person to
    stderr, and this tool sends both to one file, so the line is identified by
    the schema it carries rather than by being the only thing there.  The
    schema is imported rather than restated: a fleet runs a published runtime
    generation, and a second copy of a versioned name is a second place for it
    to go stale.
    """

    for raw in said.splitlines():
        raw = raw.strip()
        if not raw.startswith("{"):
            continue
        try:
            value = json.loads(raw)
        except ValueError:
            continue
        if (isinstance(value, dict)
                and value.get("schema") == pbrun.DETACH_SCHEMA_V1):
            return value
    return None


def submission_outcome(process: subprocess.Popen, log: Path) -> dict:
    """What this child did with the re-submission, from what it said.

    The child is waited for.  A fixed window in its place was a guess wearing
    a number: a re-submission seals the tree and writes a bundle of up to
    512 MiB into the CAS over NFS before it can say anything, so a slow
    submission and a refused one looked the same from outside, and the slow one
    was reported as submitted and stamped ``reset``.  Nothing here is
    signalled and nothing is given up on; a child that has not answered is
    named in a progress line and waited for.

    Returns:
        ``{"state": ...}`` where the state is ``submitted`` with the
        ``line`` the child printed, ``refused`` with the tail of what it said,
        or ``unclear`` when it exited zero without printing the line at all,
        which is a broken contract rather than a submission.
    """

    returncode = process.wait()
    try:
        said = log.read_text(encoding="utf-8", errors="replace")
    except OSError:
        said = ""
    tail = [line for line in said.splitlines() if line.strip()][-3:]
    if returncode != 0:
        return {
            "state": "refused",
            "detail": "\n    ".join(
                [f"the re-submission exited {returncode}; its output is {log}"]
                + tail
            ),
        }
    line = announced_submission(said)
    if line is None:
        return {
            "state": "unclear",
            "detail": "\n    ".join(
                [f"the re-submission exited 0 without saying where the work "
                 f"went; its output is {log}"] + tail
            ),
        }
    return {"state": "submitted", "line": line}


def _file_reset(plan: Mapping, *, reason: str,
                submission: Mapping | None = None) -> None:
    """Mark this plan's endings ``reset`` so the failure count means something.

    Three things this rewrite is careful about, because the tool resets the
    work and not the record.

    **It publishes by rename.**  ``path.write_text`` truncates and then writes,
    and these records are read from three boxes over NFS: a reader taking
    ``claim``'s path through ``terminal_outcome_covers`` could see the half of
    the file that had landed and raise with the item already moved into
    ``claimed/``.  ``pool._write_json_atomic`` is what every other writer in
    the queue uses, and it is what this uses now.

    **The rewritten record stays readable.**  Changing ``status`` while leaving
    ``attempt_history`` in place made ``pbrun.outcome_summary`` refuse the
    record: it adopts the immutable attempt whenever those links are present,
    and the attempt still says ``failed``.  The links are kept under a name of
    their own, exactly as ``pool.withdraw`` keeps them.

    **The evidence stays.**  Replacing ``detail`` with the reason destroyed the
    returncode and the output tails, which are the whole reason somebody reads
    a failed record afterwards, and on a lane-filed record it destroyed the
    GRES a later reset needs to restore ``--exclusive``.  The reset is recorded
    beside ``detail``, not on top of it.
    """

    for path in plan["paths"]:
        record = pool._read_json(path)
        if record is None:
            continue
        record["status"] = "reset"
        record["reset"] = {
            "reason": reason,
            "reset_unix": time.time(),
            "reset_host": socket.gethostname(),
        }
        if submission is not None:
            # What the child said, as it said it.  A re-sealed action carries a
            # key of its own, and without this the only record of which run
            # replaced this one was a log line on the operator's terminal.
            record["reset"]["submission"] = dict(submission)
        for field, kept in (
            ("attempt_history", "attempt_history_before_reset"),
            ("attempt_history_missing_before",
             "attempt_history_missing_before_reset"),
        ):
            if field in record:
                record[kept] = record.pop(field)
        pool._write_json_atomic(path, record)


def build_parser() -> argparse.ArgumentParser:
    """This tool's flags, with ``--transport`` spelled the shared way.

    The default used to be ``PRISMABUILD_TRANSPORT`` or ``pool``, which is a
    description of one shell and no description of a fleet. Once a generation
    publishes ``default_transport: slurm``, a reset reading only the
    environment would re-submit every recovered action into the pull queue no
    worker drains. ``fleet_submit.add_transport_argument`` is the one place
    that order is decided -- environment, then the published generation's
    receipt, then the pull queue -- and every producer reads it there.

    The default is evaluated when the parser is built, so a caller that
    changes the generation builds a new one.
    """

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
    # A record filed by the SLURM lane always goes back out on the lane
    # whatever this says; this only answers for records that name no transport.
    fleet_submit.add_transport_argument(ap)
    ap.add_argument("--queue-root", default=str(SH / "pb-queue"),
                    help=argparse.SUPPRESS)
    ap.add_argument("--cas-root", default=str(SH / "cas"),
                    help=argparse.SUPPRESS)
    return ap


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

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
    refused: list[str] = []
    started: list[tuple[dict, str, subprocess.Popen, Path]] = []
    for plan in ordered:
        label = (f"{plan['key'][:12]} x{len(plan['paths'])} "
                 f"{plan['transport']} {plan['cwd'] or 'sealed checkout'}")
        if plan["mode"] == "resubmit":
            if not args.apply:
                print(f"  would resubmit {label}\n    "
                      f"the sealed action itself, unchanged")
                continue
            try:
                job = resubmit_sealed(
                    plan, cas_root=cas_root, queue_root=Path(args.queue_root),
                    priority=args.priority, timeout_s=args.timeout_s)
            except slurm_lane.SlurmLaneError as exc:
                # ``sbatch`` refusing is this transport's capability gate --
                # an unknown Feature, an impossible GRES -- and it is the
                # moment the pull queue's own matcher would have spoken.  One
                # such record must not end a 120-shard reset, and the record
                # stays ``failed`` so the next run can try it again.
                print(f"  refused {label}\n    {exc}")
                refused.append(plan["key"])
                continue
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
        process, log = start_resubmission(
            command, queue_root=Path(args.queue_root), key=plan["key"])
        print(f"  submitted {label} (pid {process.pid})\n    output: {log}")
        started.append((plan, label, process, log))
    # Every record is stamped from what its child said, and none from a clock.
    # A refusal used to leave the operator with a green run, a record saying
    # the work had been re-submitted, and nothing in flight anywhere; PR #52
    # caught the refusals that arrive inside a shared ten-second window and
    # reported the rest as submitted, which is the same sentence with a
    # smaller subject.  A re-submission seals a tree and writes a bundle into
    # the CAS before it can say anything, so that window was separating fast
    # from slow and not refused from submitted.
    #
    # Detached, the child answers: one line naming the key, the transport and
    # the generation, or a non-zero exit.  So wait for it.  Nothing is
    # signalled and nothing is given up on -- a child that has not answered is
    # named in a progress line, because a submission that is still working is
    # not a submission to punish.
    unclear: list[str] = []
    outstanding = list(started)
    announced = time.monotonic()
    while outstanding:
        pending = []
        for entry in outstanding:
            plan, label, process, log = entry
            if process.poll() is None:
                pending.append(entry)
                continue
            answer = submission_outcome(process, log)
            if answer["state"] == "refused":
                print(f"  refused {label}\n    {answer['detail']}")
                refused.append(plan["key"])
                continue
            if answer["state"] == "unclear":
                # Not stamped.  The record stays failed so the next run plans
                # it again, which is free: an action key is a content hash, so
                # a duplicate re-submission of work already queued is answered
                # from the CAS or attaches to the run in flight.
                print(f"  unclear {label}\n    {answer['detail']}")
                unclear.append(plan["key"])
                continue
            line = answer["line"]
            landed = str(line.get("action_key") or "")
            # The key the child minted, which is what an operator hands
            # ``pbwait`` -- and which this tool could not name before, because
            # re-sealing a path-addressed action mints a key the plan's own
            # record does not carry.
            print(f"  {line.get('status')} {label}\n    as "
                  f"{landed[:12]} on {line.get('transport')}"
                  + (f", slurm job {line['job_id']}" if line.get("job_id")
                     else "")
                  + f"  (pbwait.py {landed[:12]})")
            _file_reset(plan, reason=(
                "re-submitted as a fresh action by pool_reset; the child "
                f"reported {line.get('status')} for {landed} on "
                f"{line.get('transport')}"), submission=line)
        outstanding = pending
        if not outstanding:
            break
        now = time.monotonic()
        if now - announced >= PROGRESS_INTERVAL_S:
            announced = now
            print(f"  still waiting on {len(outstanding)} re-submission(s): "
                  + ", ".join(entry[1] for entry in outstanding), flush=True)
        time.sleep(POLL_S)
    if not args.apply:
        print("\nnothing submitted; re-run with --apply")
    if unclear:
        print(f"\n{len(unclear)} exited 0 without saying where the work went "
              f"and were left failed")
    if refused or unclear:
        print(f"\n{len(refused)} refused and left failed")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
