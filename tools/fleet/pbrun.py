#!/usr/bin/env python3
"""Parse and display a typed PrismaBuild submission."""
from __future__ import annotations
import argparse
from dataclasses import fields
import math
import json
from collections.abc import Sequence
import os
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
from runtime_paths import generation_root
RUNTIME_ROOT = generation_root(__file__)
sys.path.insert(0, str(RUNTIME_ROOT / "src"))
from prismabuild import action_edges, submission, action_client, client_channel, core as pb, pool, fleet_roster
from prismabuild.submission_types import FleetStore, SubmissionRequest, SubmissionRefused, Display
from prismabuild.submission import TRANSPORTS, RESIDENCY_MODES, RESIDENCY_RAM_MODES, DEFAULT_MAX_ATTEMPTS, CHECKOUT_SNAPSHOT_MAX_BYTES


def configured_store() -> FleetStore:
    return FleetStore(Path("/mnt/shared/prismabuild-fleet"), RUNTIME_ROOT, fleet_roster.Roster.beside(__file__))


def render(event: Display) -> None:
    print(event.text, file=sys.stderr if event.channel == "stderr" else sys.stdout,
          end=event.end, flush=event.flush)


def request_from_args(args: argparse.Namespace) -> SubmissionRequest:
    if args.request_json is not None:
        try:
            # submitted this job or joined it.
            result = slurm_lane.resume(
                submission,
                action=action,
                cas=cas,
                queue_root=queue,
                wait_s=wait_s,
                **{name: value for name, value in lane_commands.items()
                   if name in _RESUME_COMMANDS},
            )
        else:
            result = slurm_lane.run(
                action,
                cas=cas,
                request_path=request_path,
                placement=tags,
                resources=resources,
                partition=slurm_lane.partition_for(
                    resources, tags, anywhere=anywhere),
                # ``--priority`` is a queue hint on either transport: the pool
                # sorts its ready list on it, and SLURM subtracts the derived nice
                # from the base priority its scheduler assigned.  Dropping it here
                # is what let ``pool_reset``'s bulk ``--priority -10`` land
                # alongside interactive work instead of behind it.
                priority=priority,
                priority_reason=priority_reason,
                timeout_s=timeout_s,
                worker_script=runtime_root / "tools" / "prismabuild_worker.py",
                job_entry=runtime_root / "tools" / "fleet" / "slurm_job.py",
                retry_safe=retry_safe,
                max_attempts=max_attempts,
                root=lane_root,
                # Eleven fleet tools and Tessera's ``merge_suite`` read one action's
                # ending out of this directory.  The lane files it there so the
                # cutover is a change to one dispatcher and not to every reader.
                queue_root=queue,
                wait_s=wait_s,
                detach=detach,
                on_submit=announce_submission,
                **lane_commands,
            )
    except slurm_lane.SubmissionFateUnknown as exc:
        # Not a refusal, so not reported as one and not filed as one.  sbatch
        # stopped answering and the controller could not settle whether it
        # took the job, so a job of this action may be queued right now.  The
        # exit code is the one that already means "no verdict yet".
        print(f"pbrun: the fate of this submission is unknown.\n  {exc}",
              file=sys.stderr, flush=True)
        return report.finish(action_client.ClientUnsettled(key,
            getattr(report.identity, 'published_unix', None), action_client.UnsettledReason.SUBMISSION_FATE))
    except slurm_lane.SlurmLaneError as exc:
        if getattr(exc, "job_id", None):
            # sbatch accepted this before the lane failed, so this is a record
            # that would not write and not a refusal.  Reported as a refusal it
            # told a submitter to fix the ``--tag`` of a job that was already
            # queued, which is both wrong and expensive to act on.
            return _unfiled_record(exc, key=key, action=action, cas=cas, report=report)
        raise SystemExit(
            f"pbrun: slurm refused this action.\n"
            f"  required tags: {tags or '(any box)'}\n"
            f"  demand:        {demand}\n"
            f"  {exc}\n"
            f"Fix the --tag, or read `sinfo -N -l` for a node that offers it."
        ) from exc
    except OSError as exc:
        return _lane_io_failure(
            exc, key=key, action=action, cas=cas, report=report,
            job_id=(str(attached[2].get("job_id") or "") if attached else
                    str(submitted_job_ids[-1]) if submitted_job_ids else ""))

    last = result.last
    if last is None:                       # unreachable: run always submits
        print("pbrun: nothing was submitted", file=sys.stderr)
        return report.finish(action_client.ClientRefused(1, key))
    job, outcome = last
    if detach:
        print(sorted_json(action_client.detached_submission_record(
            key,
            transport="slurm",
            status="submitted",
            queue_root=queue,
            published_unix=result.published_unix,
            job_id=job.job_id,
            submission=job.record_path,
        )), flush=True)
        return report.finish(action_client.ClientSubmitted(report.identity, job.record_path))

    def execution(code, status, replay=None):
        return report.finish(action_client.ClientExecution(key, result.published_unix, status,
            outcome.exit_code, None, None, outcome.state == 'TIMEOUT', code, replay,
            receipt_published=result.receipt is not None, transport_state=outcome.state,
            transport_signal=outcome.signal))

    total = len(result.attempts)
    for index, (attempted, reported) in enumerate(result.attempts, start=1):
        print(f"pbrun: attempt {index}/{total} slurm job {attempted.job_id} "
              f"{reported.state}", file=sys.stderr)
        _echo(attempted.stdout_path, sys.stdout)
        _echo(attempted.stderr_path, sys.stderr)

    if result.receipt is not None:
        if slurm_lane.job_was_cache_hit(job):
            # The job started, read the receipt somebody else published, and
            # ran nothing.  Saying "executed" here would attribute that
            # somebody else's work to this job, with this job's elapsed time.
            print(f"pbrun: cache_hit -- slurm job {job.job_id} found "
                  f"{key[:12]} already in the CAS and ran nothing",
                  file=sys.stderr)
            return execution(0, 'cache_hit',
                action_client.CachedResult(result.receipt['result']['sha256']))
        print(f"pbrun: executed via slurm job {job.job_id} ({outcome.state})",
              file=sys.stderr)
        return execution(0, 'executed')
    if outcome.state == "CANCELLED":
        print(f"pbrun: withdrawn -- slurm job {job.job_id} was cancelled",
              file=sys.stderr)
        return execution(action_client.WITHDRAWN_EXIT, 'withdrawn')
    if outcome.state == slurm_lane.WAIT_TIMEOUT_STATE:
        print(f"pbrun: gave up waiting for {key[:12]}; slurm job "
              f"{job.job_id} is still queued or running "
              f"(pbrun --transport slurm --withdraw {key[:12]} stops it)",
              file=sys.stderr)
        return report.finish(action_client.ClientPatienceExpired(key, result.published_unix))
    if outcome.state == slurm_lane.UNKNOWN_STATE:
        # The controller answered that it knows no such job and there is no
        # receipt.  That is not a failure and it is not filed as one: the job
        # may have been purged past MinJobAge, or the controller's memory of
        # it went with a restart while it runs on.  Same exit as giving up,
        # because the truth is the same -- no verdict yet.
        print(f"pbrun: no scheduler command can describe slurm job "
              f"{job.job_id} for {key[:12]} and it has published no receipt; "
              f"it may still be running as slurm job {job.job_id}, or have "
              f"been purged past MinJobAge; look under {job.directory}",
              file=sys.stderr)
        return report.finish(action_client.ClientUnsettled(key, result.published_unix,
            action_client.UnsettledReason.SCHEDULER_UNKNOWN))
    # Say the thing that is actually wrong.  A job that exits zero without
    # publishing a receipt has not done the work, and reporting its status
    # would report success for an action nothing can look up.
    #
    # `succeeded`, not `exit_code == 0`: SLURM reports a job it killed at the
    # time limit as `ExitCode=0:15` -- exit code zero, signal fifteen -- so
    # reading the code alone told an operator whose job was killed by the
    # scheduler that it "exited 0 but published no receipt", which sends them
    # to a job log that says nothing while the state that explains it,
    # TIMEOUT, was already in hand.  Measured in fleet/slurm/smoke row 5 on
    # 2026-09-04: state=TIMEOUT, returncode=0, signal=15.
    if outcome.succeeded:
        print(f"pbrun: slurm job {job.job_id} exited 0 but published no "
              f"receipt for {key[:12]}; see {job.stdout_path} and "
              f"{job.stderr_path}", file=sys.stderr)
        return execution(1, 'failed')
    if outcome.state == "OUT_OF_MEMORY":
        # The state alone sends an operator to a job log that a SIGKILLed
        # process never got to write.  What decided the ending is the number
        # this submission declared, which the log does not carry and the
        # caller may not have typed at all -- `mem_gb` defaults to 4.
        declared = int(demand.get("mem_gb", 0) or 0)
        print(f"pbrun: slurm job {job.job_id} exceeded the "
              f"{declared} GiB it declared; raise it with "
              f"--demand mem_gb={max(declared * 2, 8)} (or whatever the "
              f"action really needs)", file=sys.stderr)
    print(f"pbrun: failed ({outcome.state}) after {total} attempt(s); "
          f"logs {job.stdout_path} and {job.stderr_path}", file=sys.stderr)
    if isinstance(outcome.exit_code, int) and outcome.exit_code:
        return execution(action_client.reported_exit(outcome.exit_code, key=key), 'failed')
    return execution(1, 'failed')


def _echo(path, stream) -> None:
    """Print a job log if it is there, and say nothing when it is not.

    An absent log is normal -- a job cancelled before it started never opened
    one -- and turning that into an error would replace the reason the action
    failed with a complaint about the file that would have explained it.
    """

    try:
        stream.write(Path(path).read_text(encoding="utf-8", errors="replace"))
    except OSError:
        return


def withdraw_routed(
    prefixes, *, transport: str, reason: str = "", by: str = "",
    lane_root=None, queue_root=None, scancel: str = "scancel",
    squeue: str = "squeue", queue=None,
) -> int:
    """Send each prefix to the transport that recorded it.

    An operator's shell need not name the transport.  A SLURM job has a
    submission record under the lane root and a pull-queue item has none, so
    the record decides where a prefix goes; ``--transport slurm`` still sends
    every prefix to the lane.  Before this, ``--withdraw`` on a box whose
    environment did not name the transport went to the pull queue, found
    nothing there, and left the SLURM job running with exit status 2.
    """

    lane_prefixes, pool_prefixes = [], []
    for prefix in prefixes:
        if transport == "slurm" or slurm_lane.resolve_recorded(
            str(prefix), root=lane_root
        ):
            lane_prefixes.append(prefix)
        else:
            pool_prefixes.append(prefix)
    rc = 0
    if lane_prefixes:
        rc = max(rc, withdraw_slurm_main(
            lane_prefixes, reason=reason, by=by, lane_root=lane_root,
            queue_root=queue_root, scancel=scancel, squeue=squeue,
        ))
    if pool_prefixes:
        if queue is None:
            root = SH / "pb-queue" if queue_root is None else Path(queue_root)
            queue = pool.PoolQueue(root)
        rc = max(rc, withdraw_main(queue, pool_prefixes, reason=reason, by=by))
    return rc


def withdraw_slurm_main(
    prefixes, *, reason: str = "", by: str = "", lane_root=None,
    queue_root=None, scancel: str = "scancel", squeue: str = "squeue",
) -> int:
    """Cancel every job under each named action, refusing an ambiguous prefix.

    Same shape as the pool's withdrawal and for the same reasons: a prefix is
    what an operator has, one bad name must not stop the other three, and a
    prefix matching two actions is refused rather than guessed -- the wrong
    guess here kills somebody else's work.

    The marker and the terminal record are written *before* ``scancel``, as the
    pool writes its marker before it signals anything.  Two things depend on
    that order.  ``pool_reset`` skips a re-submission only on a marker or a
    ``withdrawn_unix``, so a cancellation with neither is re-submitted by the
    next bulk reset -- a decision undone by a tool that could not see it was a
    decision.  And the submitting ``pbrun``, still blocked in ``wait``, will
    file its own account of the same generation the moment the job reports
    ``CANCELLED``; whichever arrives first is kept, and this one knows who
    asked and why, which the other cannot.

    Every job under the key's name is cancelled, not just the one
    ``latest.json`` records.  The submitter cannot close the double-submit
    window and ``slurm_lane.submit`` says so: two ``pbrun``s that look at the
    same instant both find nothing in the CAS and both submit, and the
    controller holds the second PENDING on ``Dependency``.  Cancelling the
    recorded id alone left that sibling queued, and when the first job left the
    singleton released it -- so it ran the action the marker on disk exists to
    stop.  ``sibling_jobs`` is the listing, scoped to this user, and a
    controller that will not answer it costs the enrichment rather than the
    cancellation: the recorded id is always asked for.

    A refusal is reported per job and the verb fails only when every cancel was
    refused.  A sibling that finished between the listing and the cancel is the
    ordinary case, and the operator still got the run stopped.
    """

    rc = 0
    for prefix in prefixes:
        found = slurm_lane.resolve_recorded(str(prefix), root=lane_root)
        if not found:
            print(f"pbrun: no slurm submission matches {prefix!r}",
                  file=sys.stderr)
            rc = rc or 2
            continue
        if len(found) > 1:
            keys = ", ".join(sorted(str(r["action_key"])[:12] for r in found))
            print(f"pbrun: {prefix!r} matches {len(found)} submissions "
                  f"({keys}); name more characters", file=sys.stderr)
            rc = rc or 2
            continue
        record = found[0]
        key = str(record["action_key"])
        job_id = str(record["job_id"])
        queue = SH / "pb-queue" if queue_root is None else Path(queue_root)
        try:
            marker = _file_slurm_withdrawal(
                queue, record, reason=reason, by=by, scancel_command=scancel)
        except (OSError, slurm_lane.SlurmLaneError) as exc:
            _withdrawal_write_failure(exc, key=key, job_id=job_id, accepted=[])
            rc = action_client.RECORD_WRITE_FAILED_EXIT
            continue
        if marker is None:
            print(f"pbrun: {key[:12]} already has an outcome filed; "
                  f"nothing to withdraw", file=sys.stderr)
            continue
        accepted, refused = [], []
        for target in _jobs_to_cancel(key, job_id, squeue=squeue):
            if slurm_lane.cancel(target, scancel=scancel):
                accepted.append(target)
            else:
                refused.append(target)
        if accepted:
            try:
                _stamp_scancel_accepted(queue, record)
            except (OSError, slurm_lane.SlurmLaneError) as exc:
                _withdrawal_write_failure(
                    exc, key=key, job_id=job_id, accepted=accepted)
                rc = action_client.RECORD_WRITE_FAILED_EXIT
            why = f" -- {reason}" if reason else ""
            jobs = ", ".join(accepted)
            plural = "s" if len(accepted) > 1 else ""
            print(f"pbrun: cancelled slurm job{plural} {jobs} for {key[:12]}"
                  f" by {by or 'an operator'}{why}", file=sys.stderr)
        for target in refused:
            print(f"pbrun: scancel refused slurm job {target} for {key[:12]}; "
                  f"it may already have finished", file=sys.stderr)
        if refused and not accepted:
            rc = rc or 2
    return rc


def _withdrawal_write_failure(exc, *, key: str, job_id: str, accepted) -> None:
    stage = (f"scancel accepted cancellation for job(s) {', '.join(accepted)}, "
             "but its acceptance stamp could not be written"
             if accepted else
             "the withdrawal record could not be written; no cancellation was sent")
    print(
        f"pbrun: {stage}.\n"
        f"  slurm job: {job_id}\n"
        f"  record:    {getattr(exc, 'filename', '') or '(see reason)'}\n"
        f"  reason:    {getattr(exc, 'strerror', '') or exc}\n"
        "Restore record writes, then retry "
        f"`tools/fleet/pbrun.py --transport slurm --withdraw {key[:12]}`.",
        file=sys.stderr, flush=True,
    )


def _jobs_to_cancel(action_key: str, job_id: str, *, squeue: str) -> list[str]:
    """The recorded job and every sibling the controller still holds for it.

    The recorded id leads and is never dropped: it is the one fact that does
    not depend on the controller answering.  A ``squeue`` that fails costs the
    siblings, not the cancellation.
    """

    targets = [job_id] if job_id else []
    try:
        siblings = slurm_lane.sibling_jobs(action_key, squeue=squeue)
    except slurm_lane.SlurmLaneError:
        return targets
    for sibling, _state in siblings:
        if sibling not in targets:
            targets.append(sibling)
    return targets


def _file_slurm_withdrawal(
    queue_root: Path, submission, *, reason: str, by: str, scancel_command: str,
):
    """File the marker and the terminal record for one cancellation.

    Returns ``None`` when this generation already has an outcome filed -- the
    action finished a moment before the operator asked, which is them getting
    what they wanted rather than them mistyping, and is what
    ``PoolQueue.withdraw`` reports as ``already_finished``.

    Each directory is listed before the name in it is read.  These are the same
    NFS directories ``read_withdrawal_marker`` was written for: a lookup of a
    name that did not exist yet is negatively cached, so ``exists()`` keeps
    answering False after the ending has landed.  On a stale answer this verb
    filed a withdrawal over a run already in ``done/`` and reported work that
    succeeded as cancelled.  ``slurm_lane._read_json_object`` is the general
    form of that revalidation; ``read_withdrawal_marker`` itself is pinned to
    ``withdrawn/`` and this loop reads all three terminal directories.
    """

    key = str(submission["action_key"])
    published_unix = _submission_generation(submission)
    for state in (pool.DONE, pool.FAILED, pool.WITHDRAWN):
        filed = queue_root / state / f"{key}.json"
        filed_record = slurm_lane._read_json_object(filed)
        if filed_record is None:
            continue
        theirs = slurm_lane._record_generation(filed_record)
        if theirs is None or theirs != published_unix:
            continue
        if state == pool.WITHDRAWN:
            # A bare marker is a withdrawal still in flight (or one whose
            # scancel never landed); only a record carrying the job's ending
            # says this generation is over.
            if "detail" not in filed_record:
                continue
            # The record this verb writes below carries ``detail`` too, and it
            # is written before ``scancel`` runs.  Until ``scancel`` accepts,
            # that record is a decision the scheduler has not honoured, not an
            # ending; a second ask must reach ``scancel`` again rather than
            # read its own first attempt as "already finished".
            if _withdrawal_still_in_flight(filed_record):
                continue
        return None

    _, marker = slurm_lane.publish_withdrawal(
        queue_root=queue_root, action_key=key, reason=reason, by=by,
        submission=submission,
    )
    job_id = str(submission["job_id"])
    directory = Path(str(submission.get("directory") or "."))
    slurm_lane.publish_outcome(
        queue_root=queue_root,
        action_key=key,
        published_unix=published_unix,
        published_by=str(submission.get("published_by") or ""),
        status="withdrawn",
        attempts=int(submission.get("attempt") or 1),
        max_attempts=int(submission.get("max_attempts") or 1),
        retry_safe=submission.get("retry_safe"),
        addressing={},
        resources=submission.get("resources") or {},
        tags=submission.get("constraint") or [],
        detail={
            "slurm": {
                "job_id": job_id,
                "state": "CANCELLED",
                "partition": None,
                # Derived, not spelled: the record is named by generation and
                # attempt together, because one action key is submitted again
                # every time somebody asks for the same work again.
                "submission_record_path": str(
                    slurm_lane.submission_record_path(
                        directory,
                        published_unix=published_unix,
                        attempt=int(submission.get("attempt") or 1),
                    )),
                "stdout_path": str(submission.get("stdout") or ""),
                "stderr_path": str(submission.get("stderr") or ""),
            },
            "cancelled_with": scancel_command,
            # The last sample the submitter's wait recorded, so the record of
            # a withdrawal says what the job was (not) doing when the operator
            # decided.  None when no wait ever sampled it.
            "liveness": slurm_lane.read_liveness(
                key, root=directory.parent if directory.name == key else None),
        },
        # No ``SubmittedJob`` here -- this runs from the operator's box, off the
        # recorded submission -- so the job id the readers use as ``claimed_by``
        # is passed rather than derived.
        claimed_by=job_id,
        withdrawn_by=marker.get("withdrawn_by"),
        withdrawn_unix=marker.get("withdrawn_unix"),
        reason=str(marker.get("reason") or ""),
    )
    return marker


def _submission_generation(submission) -> float:
    """The ``published_unix`` a submission record belongs to."""

    published_unix = submission.get("published_unix")
    if isinstance(published_unix, (int, float)):
        return float(published_unix)
    # A submission record from before the generation stamp. Withdraw it, but
    # do not claim to know which request it belonged to.
    return float(submission.get("submitted_unix") or 0.0)


def _withdrawal_still_in_flight(record) -> bool:
    """Whether a ``withdrawn/`` record is one ``--withdraw`` wrote and has not
    yet seen ``scancel`` accept.

    ``cancelled_with`` is stamped by ``_file_slurm_withdrawal`` before the
    cancel is attempted; ``scancel_accepted_unix`` only after ``scancel``
    returned 0.  The submitter's own ending never writes the first, so a record
    with the first and without the second is a withdrawal whose ``scancel``
    was refused or never ran.
    """

    detail = record.get("detail")
    return (
        isinstance(detail, dict)
        and "cancelled_with" in detail
        and "scancel_accepted_unix" not in detail
    )


def _stamp_scancel_accepted(queue_root: Path, submission) -> None:
    """Record on the withdrawal that ``scancel`` took the job.

    Rewrites only a record of this generation that ``_file_slurm_withdrawal``
    wrote; the submitter's ending for the same generation is refused by
    ``publish_outcome`` while that record exists, so nothing else writes this
    file in between.
    """

    key = str(submission["action_key"])
    filed = queue_root / pool.WITHDRAWN / f"{key}.json"
    try:
        record = json.loads(filed.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return
    if not isinstance(record, dict):
        return
    if not slurm_lane._same_generation(filed, _submission_generation(submission)):
        return
    detail = record.get("detail")
    if not isinstance(detail, dict) or "cancelled_with" not in detail:
        return
    detail["scancel_accepted_unix"] = time.time()
    materialize._write_json_atomic(filed, record, trailing_newline=True)


def withdraw_main(q, prefixes, *, reason: str = "", by: str = "") -> int:
    """Withdraw each named action and say what happened to it.

    Takes the queue rather than building one, for the same reason
    ``report_pool_outcome`` does: the part worth testing is the reporting and the
    prefix resolution, and neither should need a live fleet to exercise.

    Keys are accepted as prefixes because a prefix is what an operator has --
    ``pbrun`` prints ``queued 8fc86da0e13f`` and the worker loop logs the same
    twelve characters.  One bad name does not stop the rest: withdrawing four
    suites at once is the case this exists for, and three of four is a better
    outcome than none of four.
    """

    rc = 0
    published = published_commit()
    for prefix in prefixes:
        try:
            key = q.find_key(str(prefix))
            result = q.withdraw(key, reason=reason, by=by)
        except Exception as exc:                                  # noqa: BLE001
            print(f"pbrun: {exc}", file=sys.stderr)
            rc = 2
            continue
        status = str(result.get("status"))
        if status == "already_finished":
            print(f"pbrun: {key[:12]} had already finished "
                  f"({result.get('state')}); nothing to withdraw",
                  file=sys.stderr)
            continue
        where = result.get("state") or "nowhere"
        note = [f"released {result.get('released', 0)} token(s)"]
        container_cleanup = result.get("container_cleanup") or {}
        if not container_cleanup.get("complete", True) and not container_cleanup.get("deferred"):
            note.append("container cleanup unverified; claim and tokens retained")
            error = str(container_cleanup.get("error") or "").strip()
            if error:
                note.append(error)
            rc = 2
        stop_pending = result.get("stop_pending")
        if stop_pending:
            # Say that the reservation is still out, and whose it is to return.
            # A caller told "withdrawn, released 0" without this would read the
            # zero as "there was nothing to release".
            note.append(
                "release pending on "
                f"{stop_pending.get('holder_host') or result.get('host') or 'the holder'}"
                "; its worker returns the tokens when it stops the action")
            detail = str(stop_pending.get("reason") or "").strip()
            if detail:
                note.append(detail)
        signalled = result.get("signalled") or {}
        if signalled.get("signals"):
            note.append("signalled " + ", ".join(signalled["signals"]))
        elif where == "claimed":
            # Say so rather than imply the work stopped.  Cross-box that is the
            # normal case and the remote worker stops within a heartbeat, but a
            # caller who reads "withdrawn" and assumes "already dead" would be
            # wrong for those seconds.
            note.append("stop requested through the generation marker; "
                        "its worker checks it at the next heartbeat")
        if status == "already_withdrawn":
            note.insert(0, "already withdrawn")
        if result.get("residency_plan_superseded"):
            # The window this consumer froze is stopped with it (#708): say so
            # where the operator reads the outcome, because the remedy -- a
            # resubmission that seals a fresh plan at today's price -- is only
            # available after this one's work ends.
            note.append("marked its residency plan superseded; a resubmission "
                        "seals a fresh one at the current price once the old "
                        "work has ended")
        print(f"pbrun: withdrew {key[:12]} from {where}; " + "; ".join(note),
              file=sys.stderr)
        # An older loop may not understand durable generation decisions.
        # Its reservation remains held; upgrade through the drained rollout.
        if where == "claimed":
            runtime = result.get("holder_runtime")
            host = result.get("host") or "the holder"
            if runtime is None:
                print(f"pbrun: WARNING no live offer from {host}; cannot tell "
                      f"whether its worker can see this withdrawal",
                      file=sys.stderr)
            elif not runtime:
                print(f"pbrun: WARNING {host} announces no runtime commit; "
                      f"cannot tell whether its worker can see this withdrawal",
                      file=sys.stderr)
            elif published and runtime != published:
                print(f"pbrun: WARNING {host} is running runtime "
                      f"{runtime[:12]}, not the published {published[:12]}: "
                      f"its support for durable cancellation is unverified; "
                      f"the reservation stays held until its worker or reaper "
                      f"completes cleanup. Upgrade through the drained fleet rollout.",
                      file=sys.stderr)
    return rc


def _profile_mode(text: str) -> str:
    """``--profile`` values, validated here so a bad one costs no submission.

    A mode may carry one option after a colon (``nsys:600`` traces the first
    ten minutes), and the whole string is sealed, so two windows are two
    actions.
    """

    try:
        return pb.parse_profile_mode(text)
    except pb.ProfileBackendUnavailable as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def require_reseal_key(key: object) -> None:
    if not isinstance(key, str) or re.fullmatch(r"[0-9a-f]{64}", key) is None:
        raise ValueError("--as-sealed-by requires a full lowercase 64-hex action key")


def reseal_wrapper(key: str) -> Path:
    """Recover only the wrapper location; the complete new seal must still match.

    Never run an old submitter or copy its command, inputs or environment into
    a new request. The caller's current work is sealed by this submitter, with
    the original immutable Docker wrapper, and checked before publication.
    """
    require_reseal_key(key)
    where = f"--as-sealed-by {key}"
    prefix = action_edges.request_wrapper(SH / "cas", key, where=where)
    return verify_retained_wrapper(prefix, where=where)


def verify_retained_wrapper(prefix: str, *, where: str) -> Path:
    """The Docker wrapper at ``prefix``, proved to be a retained generation's.

    The wrapper must sit in a published generation under the store, beside a
    receipt naming that generation, and hash to what the receipt recorded.
    ``--as-sealed-by`` reads an old action's wrapper back this way, and a
    deferred release (#913) seals into its template's generation only after
    this check.  Raises ``ValueError`` (or ``OSError``) naming what failed.
    """

    wrapper = Path(prefix)
    generation = wrapper.parent
    if (str(wrapper) != prefix or wrapper.name != "tools"
            or generation.parent != SH / "runtime-generations"
            or generation.name.startswith(".")):
        raise ValueError(f"{where}: wrapper is not in a retained fleet generation")
    raw_version = pb._read_regular_file_nofollow(
        generation / "RUNTIME_VERSION.json", where=where,
        require_readonly=True, max_bytes=1024 * 1024)
    version = pb._decode_strict_json(raw_version, where=where)
    if (not isinstance(version, dict)
            or version.get("schema") != "prismaquant.prismabuild.runtime_version.v1"
            or version.get("generation") != generation.name
            or not isinstance(version.get("files"), dict)):
        raise ValueError(f"{where}: invalid retained generation receipt")
    shim = pb._read_regular_file_nofollow(
        wrapper / "docker", where=where, require_readonly=True,
        max_bytes=16 * 1024 * 1024)
    if hashlib.sha256(shim).hexdigest() != version["files"].get("tools/docker"):
        raise ValueError(f"{where}: retained Docker wrapper differs from its receipt")
    return wrapper


# --------------------------------------------------------------------------
# Sealing an action, in two stages
#
# Everything that reads the filesystem or the environment happens once, in
# ``freeze_action_template``; turning that template into a sealed action
# happens once per action, in ``seal_action_from_template``.  An ordinary
# submission is the two in a row and nothing else, which is what
# ``test_the_ordinary_pbrun_action_is_sealed_unchanged.py`` pins.
#
# The split exists for #517.  A decomposed parent seals many children off one
# frozen source tree: re-snapshotting a mutable checkout per child would give
# the children different code closures, and cloning this file's hashing rules
# into the decomposer would give them different keys.  So the decomposer calls
# stage A once and stage B per child, through the same code an ordinary
# ``pbrun`` uses -- there is no second sealer to keep in step.
# --------------------------------------------------------------------------


def gang_declaration(args) -> dict | None:
    """``--gang-*`` as a sealed declaration, refusing what a gang cannot be (#1517)."""
    fields = (args.gang_group, args.gang_size, args.gang_index)
    if all(field is None for field in fields):
        return None
    if any(field is None for field in fields):
        raise SystemExit("pbrun: --gang-group, --gang-size and --gang-index go together")
    if args.transport != "pool":
        raise SystemExit("pbrun: a gang member needs --transport pool")
    if args.max_attempts != 1 or args.retry_safe:
        raise SystemExit("pbrun: a gang member gets one attempt: an unsuccessful one "
                         "ends the whole gang (drop --max-attempts/--retry-safe)")
    if getattr(args, "after", None):
        raise SystemExit("pbrun: a gang member cannot be deferred with --after")
    from prismabuild import _gang
    try:
        return _gang.declaration({"group": args.gang_group, "size": args.gang_size,
                                  "index": args.gang_index})
    except _gang.GangContractError as exc:
        raise SystemExit(f"pbrun: {exc}") from None


def freeze_action_template(
    *,
    command: Sequence[str],
    cwd: Path,
    logical_cwd: str,
    demand: Mapping[str, int],
    placement: Mapping[str, object],
    variables: Mapping[str, str],
    determinism: str,
    retry_policy: Mapping[str, object],
    host_class: str | None,
    measurement: bool,
    transport: str,
    pool_measurement_class: bool,
    data_manifest_path: str | None,
    produced_output_template_path: str | None = None,
    checkout_snapshot_max_bytes: int,
    snapshot_refs: Sequence[str],
    exclusive: bool,
    gpu_memory_gb: float | None,
    execution_timeout_s: float | None,
    progress: Mapping[str, object] | None,
    awaited_batch: Mapping[str, object] | None = None,
    profile: object | None,
    container_image_refs: Sequence[str] = (),
    d38_namespace: str | None = None,
    wrapper_dir: Path | None = None,
    dependency_queries: Mapping[str, str] | None = None,
    gang: Mapping[str, object] | None = None,
    requires_files: list[dict] | None = None,
    resident_set: str | None = None,
    target_evidence: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """Read the tree and the environment once, and freeze what they say.

    Everything in here is a measurement of the submitter's box at one instant
    -- the checkout's commit and its dirty digest, the bytes of the data
    manifest, the local toolchain -- so taking it a second time for a second
    action can produce a different answer but never a better one.  What comes
    back is the half of an action body that every action sealed from this
    template shares, plus the CAS the ingestion went into.

    Placement and demand are resolved against the live fleet by the caller, so
    they arrive already decided; this reads nothing about who might run the
    work.

    ``measurement`` spells the task class and the scope at once, deliberately:
    they are the same statement, and a caller that could set them separately
    could seal a measurement with no platform to measure on.
    """

    wrapper_dir = CONTAINER_WRAPPER_DIR if wrapper_dir is None else wrapper_dir
    variables = dict(variables)
    scratch_io = scratch_io_intent(variables, transport=transport)
    recorder = None
    if scratch_io is not None:
        from prismabuild import local_scratch
        if contracts.SCRATCH_IO_TAG not in placement.get("required_tags", []):
            raise SystemExit("pbrun: sealed scratch I/O declaration requires local-scratch-io-v1")
    if "tools/fleet/local_scratch_profile.py" in command:
        from prismabuild import local_scratch
        try:
            recorder = local_scratch.check_profile_request(
                command, variables, demand, cwd=logical_cwd, determinism=determinism)
            if transport != "pool":
                raise local_scratch.LocalScratchError("scratch recorder requires pool admission")
            if profile is not None or container_image_refs:
                raise local_scratch.LocalScratchError("scratch recorder does not support injected profiler/image runtime")
        except local_scratch.LocalScratchError as exc:
            raise SystemExit(f"pbrun: {exc}") from None
    # A declared spool bound is accounted by default (#905), and this must
    # precede the ownership and stamp fingerprints below: they hash this
    # environment, and the sealed action re-derives its owner from it, so both
    # must see the same effective contract.
    normalize_spool_declaration(variables)
    # Docker's payload is reparented to containerd-shim and therefore survives
    # a kill of every process group below the action launcher.  Put the fleet's
    # Docker shim first even under --no-default-env; it records a durable marker
    # and adds the derived ownership label which withdrawal/finish query before
    # returning capacity.  This is control-plane state, not an optional action
    # convenience, so a caller cannot override either identity variable.
    #
    # Normalize every other environment value first.  The owner then hashes
    # the exact action-defining state available before its own two recursive
    # variables are injected, including the deployed wrapper path.
    prior_path = variables.get("PATH") or "/usr/local/bin:/usr/bin:/bin"
    variables["PATH"] = (local_scratch.PROFILE_PATH if recorder is not None
                         else f"{wrapper_dir}:{prior_path}")
    identity = _git_identity(cwd)
    # Which repository this template's tree belongs to (#1565): the owning
    # repository's name -- a linked worktree resolves to its main repo --
    # ``"unknown"`` when Git cannot place the checkout.  A submitter's
    # handle, never sealed -- the queue row carries it, so the action key
    # is byte-identical with and without it.
    tested_repository = tested_repository_name(cwd)
    marker_root = SH / "pb-queue" / pool.CONTAINER_OWNERS
    # This owner belongs to the template's own command, and its only job here
    # is to be part of what the stamp name is fingerprinted over.  Ownership
    # itself is settled per action in ``seal_action_from_template``, because
    # two actions sealed off one template run as two lifecycles: the Docker
    # label a shared owner would give them makes one child's cleanup remove
    # the other's live payload, and one child's ``<owner>.used`` marker blocks
    # the other's reclaim.  An unmodified command re-derives this exact digest
    # there, so an ordinary submission is unchanged.
    owner = container_owner(
        command,
        cwd,
        demand,
        variables,
        determinism=determinism,
        retry_policy=retry_policy,
        marker_root=marker_root,
        identity=identity,
        logical_cwd=logical_cwd,
        placement=placement,
        container_images=container_image_refs,
    )
    marker = marker_root / f"{owner}.used"
    variables[CONTAINER_OWNER_ENV] = owner
    variables[CONTAINER_MARKER_ENV] = str(marker)

    # Migrate the former broad prefix globs before identity asks Git for its
    # untracked roster; otherwise a legitimate prefix-bearing payload remains
    # hidden for this submission even though the new grammar is exact.
    keep_droppings_out_of_git(cwd)
    log_name, stamp_name = result_and_stamp_names(
        command,
        cwd,
        demand,
        variables,
        identity=identity,
        logical_cwd=logical_cwd,
        placement=placement,
    )
    # Seal the stamp only in the private snapshot index. Publishing it in the
    # source tree creates both litter and races: another submitter can hash a
    # scratch name just as it is renamed. Unlinking the final stamp also races
    # with readers sealing the same fingerprint. No shared stamp path exists
    # now; workers still verify the same name and bytes in the materialization.
    payload = json.dumps(
        {"cwd": logical_cwd, **identity}, indent=1, sort_keys=True
    )
    cas = pb.PrismaBuildCAS(SH / "cas")
    checkout_snapshot = build_git_checkout_snapshot(
        cwd,
        stamp_name=stamp_name,
        stamp_payload=payload,
        cas=cas,
        max_bytes=checkout_snapshot_max_bytes,
        expected_identity=identity,
        snapshot_refs=list(snapshot_refs),
    )
    inputs = [checkout_snapshot["input"]]
    origin_batch_refs = None
    if data_manifest_path is not None:
        # Refuse a malformed source before ingestion, then derive the sealed
        # summary from verified CAS bytes. The source can be replaced between
        # these reads; its earlier totals/encoding must not describe a later
        # blob. Discard the preliminary parse before allocating another one.
        pb.load_data_manifest(data_manifest_path)
        manifest_input, _ = cas.ingest_input(
            data_manifest_path,
            input_id=pb.PBCAMPAIGN_DATA_MANIFEST_INPUT_ID,
        )
        manifest, manifest_encoding = pb.read_data_manifest(cas.input_path(manifest_input))
        require_declared_origin_batches(
            manifest, transport=transport, queue_root=SH / "pb-queue")
        annotations = manifest.get("annotations")
        if isinstance(annotations, Mapping):
            origin_batch_refs = annotations.get(_ORIGIN_BATCHES_ANNOTATION)
        inputs.append(manifest_input)
        data_manifest_summary = {
            "input": manifest_input,
            "mount_prefix": manifest["mount_prefix"],
            "entry_count": manifest["entry_count"],
            "total_bytes": manifest["total_bytes"],
        }
        if manifest["schema"] == pb.DATA_MANIFEST_SCHEMA_V2:
            require_deployed_read_plan_storage()
            require_linear_read_plan_progress(manifest, progress)
            data_manifest_summary["schema"] = pb.DATA_MANIFEST_SCHEMA_V2
            data_manifest_summary["read_bytes"] = manifest["read_plan"]["read_bytes"]
        # Preserve ordinary v1 action identity; only compressed inputs need
        # the encoding declaration. The CAS digest still covers wire bytes.
        if manifest_encoding != "identity":
            data_manifest_summary["content_encoding"] = manifest_encoding
    else:
        data_manifest_summary = None
    # The local-disk demand -- the spool window and any declared scratch --
    # is derived from the sealed environment, so the two are checked together
    # here, where both are final (#747, #911).  Off, a sealed ``spool_gb`` is
    # refused rather than carried unexplained.
    spool_terms = local_disk_terms(variables, transport=transport)
    sealed_spool = {kind: int(need) for kind, need in demand.items()
                    if kind == _SPOOL_WINDOW_KIND}
    if sealed_spool != spool_terms:
        raise SystemExit(
            f"pbrun: sealed {_SPOOL_WINDOW_KIND} demand {sealed_spool or 'none'} "
            f"disagrees with the environment's local-disk demand "
            f"{spool_terms or 'none'}; {_SPOOL_WINDOW_ENV}, {_SPOOL_MAX_ENV} and "
            f"the pairs {_SCRATCH_PAIRS_ENV} names are the only source of it")
    produced_declaration = None
    produced_validated = None
    if produced_output_template_path is not None:
        from prismabuild import produced_output as produced_mod

        # Single bounded capture: read at most MAX+1 once, publish those
        # exact bytes to the CAS, then validate the captured blob. A second
        # read of the path could bind OLD bytes in params while the CAS
        # captures changed NEW bytes; ingesting the held bytes closes it.
        # Reuses the existing CAS staging/hard-link machinery via
        # ingest_bytes (indistinguishable blob, same race handling).
        try:
            with open(produced_output_template_path, "rb") as handle:
                raw_template = handle.read(
                    pb.PRODUCED_OUTPUT_TEMPLATE_MAX_BYTES + 1)
        except OSError as exc:
            raise SystemExit(
                f"pbrun: cannot read --produced-output-template: {exc}") from None
        if len(raw_template) > pb.PRODUCED_OUTPUT_TEMPLATE_MAX_BYTES:
            raise SystemExit(
                "pbrun: --produced-output-template exceeds "
                f"{pb.PRODUCED_OUTPUT_TEMPLATE_MAX_BYTES} bytes: "
                "templates are envelopes, not payloads")
        try:
            template_input, _ = cas.ingest_bytes(
                raw_template,
                input_id=pb.PRODUCED_OUTPUT_TEMPLATE_INPUT_ID,
            )
        except pb.ActionContractError as exc:
            raise SystemExit(
                f"pbrun: --produced-output-template ingest: {exc}") from None
        try:
            candidate = json.loads(raw_template.decode())
        except (UnicodeDecodeError, ValueError) as exc:
            raise SystemExit(
                f"pbrun: --produced-output-template is not JSON: {exc}") from None
        try:
            produced_validated = produced_mod.validate_template(candidate)
        except produced_mod.ProducedOutputError as exc:
            raise SystemExit(
                f"pbrun: --produced-output-template invalid: {exc}") from None
        # Derive the qualified tier demand from the bounded working window
        # here (never the durable corpus), so the sealed demand and the
        # sealed declaration cannot drift apart between prepare and freeze.
        try:
            window_terms = produced_mod.owner_demand_terms(produced_validated)
        except produced_mod.ProducedOutputError as exc:
            raise SystemExit(
                f"pbrun: --produced-output-template demand: {exc}") from None
        for qualified, need in window_terms.items():
            if demand.get(qualified, 0) != int(need):
                raise SystemExit(
                    "pbrun: --produced-output-template window demand "
                    f"{qualified}={need} disagrees with the sealed demand; "
                    "the template is the only source of tier demand")
        # The CAS digest covers the captured bytes; the declaration below
        # binds the canonical template identity to that input row, so the
        # key moves with the template and a post-seal edit changes nothing.
        try:
            produced_declaration = produced_mod.build_declaration(
                produced_validated, template_input)
        except produced_mod.ProducedOutputError as exc:
            raise SystemExit(
                f"pbrun: --produced-output-template declaration: {exc}") from None
        inputs.append(template_input)
    execution_scope, toolchain = host_class_scope(
        host_class, measurement=measurement, transport=transport,
        target_evidence=target_evidence)
    if recorder is not None:
        # Actual executable/version facts, verified by the normal worker
        # preflight and bound into its real receipt; never guessed hashes.
        if target_evidence is not None:
            # The class is the worker's: its interpreter is not this box's, so
            # the packet's own declaration is the only source (#1598).
            declared = getattr(target_evidence, "recorder", None)
            # Both refusals are correctness checks, not D32 seals (CEO ruling on
            # dec-1007-143227-359f): facts about another file are not comparable
            # to this command's executable, and absent facts are missing input.
            # They also protect the content-addressed action key.
            if declared is None or declared["path"] != command[0]:
                raise SystemExit(
                    "pbrun: a scratch recorder under --target-evidence needs the "
                    f"worker's identity of {command[0]!r}; collect the packet with "
                    f"pbevidence.py --recorder-python {command[0]}")
            toolchain = {**toolchain, **{k: v for k, v in declared.items() if k != "path"}}
        else:
            toolchain = {**toolchain, **pb.executable_toolchain_contract(command[0]),
                         **pb._probe_python_toolchain(Path(command[0]))}
    if pool_measurement_class and demand.get("gpu", 0) and (
        "cuda_compute_capability" not in toolchain or "nvidia_driver" not in toolchain
    ):
        raise SystemExit("pbrun: class-scoped GPU measurement requires live accelerator "
                         "model, compute capability and driver evidence")
    params: dict[str, object] = {
        "command": list(command),
        "cwd": logical_cwd,
        "demand": demand,
        "placement": placement,
        "checkout_snapshot": checkout_snapshot,
        "retry_policy": retry_policy,
    }
    if dependency_queries:
        params["dependency_queries"] = dict(dependency_queries)
        if local_dependencies.TAG in placement["required_tags"]:
            params["local_dependencies"] = dict(dependency_queries)
    if resident_set is not None:
        from prismabuild import resident_sets
        params["resident_set"] = resident_sets._set_id(resident_set)
    if recorder is not None:
        params["local_scratch_profile"] = recorder
    declared_interpreter = interpreter_of(command)
    if declared_interpreter is not None:
        # The placement requirement travels in the sealed body (#1263), so
        # the row, the matcher and the claim all read one authority: the
        # exact path this action will exec.
        params["interpreter"] = declared_interpreter
    if requires_files is not None:
        # Sealed like the interpreter (#1495): the row carries the claim-
        # relevant projection, and the requirement is part of the action's
        # own identity, so a changed digest re-keys the action.
        params["requires_files"] = requires_files
    if data_manifest_summary is not None:
        # A summary, not the list: the prewarm budget and the ARC check read
        # these two numbers every poll, and making them fetch and parse a
        # 200 KB blob to learn a byte count would put the manifest on the
        # scheduler's hot path. The list itself stays in the CAS.
        params["data_manifest"] = data_manifest_summary
    if produced_declaration is not None:
        # Sealed like the data manifest: the input row carries the bytes,
        # this declaration binds the canonical template identity to it, so
        # the action key covers both. Absent, the key is byte-identical to
        # before this flag existed.
        params[pb.PRODUCED_OUTPUT_TEMPLATE_PARAM] = produced_declaration
    if demand.get("gpu"):
        params["gpu_exclusive"] = bool(exclusive)
        if gpu_memory_gb is not None:
            params["gpu_memory_gb"] = gpu_memory_gb
    if execution_timeout_s is not None:
        params["execution_timeout_s"] = execution_timeout_s
    if gang is not None:
        # Sealed membership (#1517): the group, its size and this index are
        # part of the action key. Absent, the key is byte-identical to before.
        params["gang"] = dict(gang)
    if progress is not None:
        # Sealed, like the profiler mode and for the same reason: an action
        # admitted under the progress contract is a different action from its
        # unbounded twin, so the store never answers one with the other's
        # receipt.  Absent, the key is byte-identical to what it was before
        # this flag existed.
        params[pb.PROGRESS_PARAM] = progress
    if awaited_batch is not None:
        # Sealed beside the progress policy it credits (#1666): a coordinator
        # the worker credits is a different action from one it does not.
        # Absent, the key is byte-identical to before this flag existed.
        params[pb.AWAITED_BATCH_PARAM] = awaited_batch
    if profile is not None:
        # Sealed, and only when asked for.  Present, it makes a profiled run a
        # different action from its unprofiled twin, which is what stops the
        # CAS from answering a profile request with a receipt that has none.
        # Absent, the key is byte-identical to what it was before this flag
        # existed, so nothing already in the store is orphaned.
        params[pb.PROFILE_PARAM] = profile
    if container_image_refs:
        # Sealed for the same reason: an image-pinned action is a different
        # action from its unpinned twin, and the requirement has to travel on
        # the action's own bytes -- the queue item's copy is a scheduling
        # projection of this one, never the other way around (#714).  Absent,
        # the key is byte-identical to what it was before this flag existed.
        params["container_images"] = list(container_image_refs)
    if d38_namespace is not None:
        # Sealed before the key is computed (D38): the namespace the preflight
        # proved is part of the job's identity, so a changed namespace is a
        # changed job and the old receipt no longer binds it.  Absent, the key
        # is byte-identical to what it was before this flag existed.
        params[d38_gate.NAMESPACE_PARAM] = d38_namespace
    template = {
        "cas": cas,
        "marker_root": marker_root,
        "checkout_identity": identity,
        "tested_repository": tested_repository,
        "log_name": log_name,
        "stamp_name": stamp_name,
        "produced_output_template": produced_validated,
        "task": {
            "definition_id": "fleet/pbrun",
            "definition_version": "v1",
            "task_class": "measurement" if measurement else "generation",
            # A pytest or a timing run is not byte-reproducible and must not
            # claim to be: the CAS only enforces canonical equality on
            # "deterministic", so mislabelling one would be a false receipt.
            "determinism": determinism,
            "artifact_family": "generic",
            "artifact_kind": "generic",
            "working_directory": ".",
        },
        "inputs": inputs,
        "code_closure": build_stamp_closure(stamp_name, payload),
        "params": params,
        "environment": {"variables": variables, "toolchain": toolchain},
        "execution_scope": execution_scope,
    }
    if origin_batch_refs is not None:
        # The batches the sealed data manifest declares, for ``main`` to file
        # this consumer against once its key is known (#914).  Only when
        # declared, so every other template is the one it always was.
        template[_ORIGIN_BATCHES_TEMPLATE_KEY] = list(origin_batch_refs)
    return template


#: The template entries that are the submitter's own handles rather than any
#: part of a sealed action: the CAS it ingested into, the marker namespace and
#: the recorded checkout identity ownership is re-derived against, and the two
#: fingerprinted names.  Everything else in a template is, by construction,
#: the half of an action that no action sealed from it varies -- which is why
#: :func:`template_action_common` subtracts rather than enumerates.  A field
#: added to the template is therefore never silently dropped from a parent's
#: identity: it either belongs to the shared half, in which case it must also
#: be named in ``decomposition._ACTION_COMMON_KEYS``, or it is a submitter
#: handle and belongs here.  Named in neither, ``validate_action_common``
#: refuses the record -- which is the right answer, because nobody has yet
#: decided which of the two it is.
#:
#: ``produced_output_template`` is a handle: the binding declaration is sealed
#: in ``params`` (``pb.PRODUCED_OUTPUT_TEMPLATE_PARAM``) and so is already in
#: the shared half, and this top-level entry is the validated template the
#: submitter projects into its own queue row.  Counting it twice would put the
#: same declaration in the parent key under two spellings; leaving it unnamed
#: refused every Stage A freeze, because the template carries the entry --
#: ``None`` when no template was declared -- whether or not the flag was given.
#:
#: ``produced_output_batches`` is a handle too: the refs are the sealed data
#: manifest's own annotation, so the key already covers them.  The entry is
#: present only when the manifest declares batches (#914).
#: ``tested_repository`` is a handle too: the name of the tree the template
#: froze, carried by the queue row rather than the sealed body, so no action
#: sealed from the template varies with it (#1565).
_TEMPLATE_SUBMITTER_KEYS = frozenset(
    {"cas", "marker_root", "checkout_identity", "tested_repository",
     "log_name", "stamp_name",
     "produced_output_template", "produced_output_batches"}
)


def template_action_common(template: Mapping[str, object]) -> dict[str, object]:
    """The half of every action sealed from this template that none of them varies.

    A decomposition's parent has to be keyed on exactly this.  Key it on less
    -- on the producer's declared command, demand and environment alone -- and
    two campaigns that differ in placement, retry policy, timeout, profiler
    mode or the local toolchain collapse onto one parent while their children
    take different keys; the publication index then refuses the second run with
    a key mismatch that names the child rather than the cause.

    The two container variables come off, because ownership is per action by
    the time anything is sealed, and a child's own owner is a function of this
    record and its own command.
    """

    shared = {name: value for name, value in template.items()
              if name not in _TEMPLATE_SUBMITTER_KEYS}
    shared["params"] = {name: value
                        for name, value in shared["params"].items()
                        if name != "command"}
    variables = dict(shared["environment"]["variables"])
    variables.pop(CONTAINER_OWNER_ENV, None)
    variables.pop(CONTAINER_MARKER_ENV, None)
    shared["environment"] = {**shared["environment"], "variables": variables}
    return shared


def seal_action_from_template(
    template: Mapping[str, object],
    *,
    command: Sequence[str] | None = None,
    result_path: str | None = None,
    extra_inputs: Sequence[Mapping[str, object]] = (),
    extra_params: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """Turn one frozen template into one sealed action.

    The command and the result path are rebuilt here rather than carried in
    the template because they are the two things a decomposed child varies:
    its own batch resolved into the argument list, its own manifest to write.
    For an ordinary submission nothing is overridden and this is the
    template's own command, teed to the template's own log.

    The four overrides are the whole of what makes a child different from an
    ordinary action, and they are deliberately narrow.  Each one lands in the
    sealed body, so a child's key is an ordinary ``pbrun`` key over a command,
    an input list and a ``params`` that say which batch of which plan it
    measured -- not a new kind of action with its own hashing rules.

    ``command`` still runs under the same wrapper and still tees to a log; the
    log just stops being the declared result, because a decomposed child's
    result is its manifest.  ``extra_inputs`` are appended after the
    template's, so the checkout snapshot stays ``inputs[0]``, which is where
    the worker's materialization looks for it.  ``extra_params`` may not
    rewrite anything the template froze: a child that could restate its own
    command, demand or snapshot would be a different action wearing a
    template's identity.

    Container ownership is settled here rather than in the template because it
    is a property of one running action, not of the source tree.  Two children
    sealed off one template are two Docker lifecycles on what may be one box:
    a shared ownership label makes the first to finish ``docker rm -f`` the
    other's live payload, and a shared ``<owner>.used`` marker makes each
    one's reclaim wait on the other's use.  So the owner is re-derived from
    this action's own command and re-injected.  With nothing overridden the
    inputs are the template's, so the digest is the template's and an ordinary
    submission seals byte for byte what it did before this split existed.
    """

    params = dict(template["params"])
    if extra_params:
        overwritten = sorted(set(extra_params) & set(params))
        if overwritten:
            raise SystemExit(
                "pbrun: refusing to seal an action whose extra params restate "
                f"the frozen template's: {', '.join(overwritten)}"
            )
        params.update(extra_params)
    command = list(template["params"]["command"] if command is None else command)
    params["command"] = command
    # Strip the two injected variables back off to recover exactly the mapping
    # the template hashed, so an unoverridden command lands on the same digest.
    variables = dict(template["environment"]["variables"])
    variables.pop(CONTAINER_OWNER_ENV, None)
    variables.pop(CONTAINER_MARKER_ENV, None)
    marker_root = template["marker_root"]
    owner = container_owner(
        command,
        params["cwd"],
        params["demand"],
        variables,
        determinism=template["task"]["determinism"],
        retry_policy=params["retry_policy"],
        marker_root=marker_root,
        # Recorded by the template, never re-read: the tree may have moved on
        # since, and every action sealed from one template must answer for the
        # tree that template froze.
        identity=template["checkout_identity"],
        logical_cwd=params["cwd"],
        placement=params["placement"],
        # The action's own declared images, so a child or an override that
        # changes them re-derives its own Docker lifecycle rather than sharing
        # the template's or a sibling's (#714 review).  A movement node cuts
        # its params from _MOVEMENT_PARAM_KEYS, which does not carry them.
        container_images=params.get("container_images") or (),
    )
    variables[CONTAINER_OWNER_ENV] = owner
    variables[CONTAINER_MARKER_ENV] = str(marker_root / f"{owner}.used")
    log_name = str(template["log_name"])
    recorder = params.get("local_scratch_profile")
    if recorder is not None:
        from prismabuild import local_scratch
        if (local_scratch.check_profile_request(command, variables, params["demand"],
                cwd=params["cwd"], determinism=template["task"]["determinism"], derived=True) != recorder
                or result_path not in (None, local_scratch.PROFILE_RESULT)):
            raise SystemExit("pbrun: scratch recorder override differs from sealed file-result contract")
        argv, effective_result = command, local_scratch.PROFILE_RESULT
    else:
        argv = movement_actions.standard_capture_argv(
            command, log_name,
            path_prefix=variables['PATH'].split(':', 1)[0])
        effective_result = log_name if result_path is None else result_path
    body = {
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {
            **template["task"],
            "argv": argv,
            "result_path": effective_result,
        },
        "inputs": [*template["inputs"], *extra_inputs],
        "code_closure": template["code_closure"],
        "params": params,
        "environment": {**template["environment"], "variables": variables},
        "execution_scope": template["execution_scope"],
    }
    try:
        return pb.seal_action(body)
    except pb.ActionContractError as exc:
        # A refused contract is the caller's to fix; nothing has been queued
        # or ingested, so say what was refused and stop.  A traceback here
        # names core.py internals for what is a submission error (issue #21).
        raise SystemExit(f"pbrun: refusing to seal the action: {exc}") from None


#: The params a movement node keeps from the template it was cut off.  The
#: construction itself now lives in ``prismabuild.movement_actions`` (the
#: one ordinary movement path, shared verbatim with the produced-output
#: writer lane); pbrun keeps its surface and injects its own
#: ``container_owner``, whose definition stays with the submission
#: machinery that owns git identity and stamp names.
def seal_movement_action(
    template: Mapping[str, object],
    *,
    command: Sequence[str],
    demand: Mapping[str, int],
    tags: Sequence[str],
    log_name: str,
    retry_policy: Mapping[str, object] | None = None,
    extra_params: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """Seal one movement or egress node off the submission that needs it.

    See ``prismabuild.movement_actions.seal_movement_action`` for the
    construction: a movement node is a sibling that shares the submission's
    checkout snapshot, code closure and environment variables, while its
    command is a fleet tool, its demand is tier tokens, it is placed on the
    box that owns the stage, and it is sealed as portable generation work
    whatever the submission is (#944). The submitter's ``container_owner``
    settles ownership here, exactly as before.
    """

    return movement_actions.seal_movement_action(
        template, command=command, demand=demand, tags=tags,
        log_name=log_name, retry_policy=retry_policy,
        container_owner_fn=container_owner, extra_params=extra_params)


def resolve_stage_tier(queue, declared: str | None) -> dict[str, object]:
    """Which stage tier this submission's bytes land on, from what the fleet says.

    Discovered rather than configured, because the tiers are: ``tier_loop``
    announces what each box actually has, so a submitter that hard-coded a
    pool name would keep asking for a dataset after Rob moved it.  One stage
    tier is the unambiguous case and needs no flag; none is a refusal naming
    the loop that would have announced it; more than one is a refusal that
    lists them, because picking for the operator is picking which box the
    campaign reads from.
    """

    stages = [record for record in queue.tiers()
              if record.get("tier") == "stage" and not record.get("retired")]
    if declared is not None:
        for record in stages:
            if str(record.get("tier_id")) == declared:
                return record
        raise SystemExit(
            f"pbrun: no live stage tier {declared!r}; the fleet announces "
            f"{sorted(str(r.get('tier_id')) for r in stages) or 'none'}")
    if not stages:
        raise SystemExit(
            "pbrun: --residency stage needs a stage tier, and no box announces "
            "one.  A storage box mints it by running the tiers role "
            "(tier_loop.py); until it does, nothing can reserve stage capacity")
    if len(stages) > 1:
        raise SystemExit(
            "pbrun: more than one stage tier is live; name one with "
            f"--residency-tier: {sorted(str(r.get('tier_id')) for r in stages)}")
    return stages[0]


def resolve_ram_tier(queue, stage_tier: Mapping[str, object],
                     ) -> dict[str, object] | None:
    """The ram tier that sits in front of this stage, from what the fleet says.

    Discovered rather than configured, because the tiers are: the promotion
    node runs on the box that owns the stage, so the ram tier that feeds the
    same consumers is that box's, and a ram tier on another host is another
    stage's leg.  None is the ordinary answer -- no tmpfs mounted, or a
    generation predating the policy -- and it seals no ram leg, exactly as a
    submission before the tier existed did.
    """

    host = str(stage_tier.get("host") or "")
    rams = [record for record in queue.tiers()
            if record.get("tier") == "ram" and not record.get("retired")
            and str(record.get("host") or "") == host]
    if not rams:
        return None
    if len(rams) > 1:
        raise SystemExit(
            "pbrun: more than one ram tier is live on "
            f"{host or 'the stage host'}: the policy declares one mountpoint, "
            f"and the fleet announces {sorted(str(r.get('tier_id')) for r in rams)}")
    return rams[0]


def movement_tools(tier: Mapping[str, object], *,
                   mover: str = "stage_move.py") -> tuple[str, str, str]:
    """The interpreter and the two movement scripts, as the tier announces them.

    The construction lives in ``prismabuild.movement_actions`` (the one
    ordinary movement path, shared verbatim with the produced-output
    writer lane); see there for why the answer comes off the tier record
    and never off this process.
    """

    return movement_actions.movement_tools(tier, mover=mover)


def current_fill_offer(tier: Mapping[str, object],
                       measured: int | None) -> tuple[int | None, int | None, str]:
    """The fill a freshly sealed mover may reserve; see ``storage_tiers``.

    The rule moved to ``storage_tiers.current_fill_offer`` so the
    produced-output exporter prices its pool writes by the same rule (#747).
    """

    return storage_tiers.current_fill_offer(tier, measured)


def reader_declaration(args) -> dict[str, int]:
    """What the submitter declares about the consumer's reading (#909).

    ``--residency-prefetch-depth-gib`` and ``--residency-read-mb-s``, as the
    residency plan's ``reader`` block: the tier prices the window from them
    instead of from the consumer's memory reservation and the tier's own fill
    offer.  Either may be absent; an empty declaration adds nothing to the
    plan.  Read with ``getattr`` because a deferred submission filed before
    #909 carries neither.
    """

    reader: dict[str, int] = {}
    depth = getattr(args, "residency_prefetch_depth_gib", None)
    if depth is not None:
        if isinstance(depth, bool) or not isinstance(depth, int) or depth < 0:
            raise SystemExit(
                "pbrun: --residency-prefetch-depth-gib must be a whole number "
                "of GiB, 0 or more")
        reader["prefetch_depth_bytes"] = int(depth) * storage_tiers.GIB
    rate = getattr(args, "residency_read_mb_s", None)
    if rate is not None:
        if isinstance(rate, bool) or not isinstance(rate, int) or rate <= 0:
            raise SystemExit("pbrun: --residency-read-mb-s must be a positive "
                             "whole MB/s")
        reader["read_mb_s"] = int(rate)
    return reader


def prelaunch_capacity_refusal(
    tier: Mapping[str, object], tier_id: str, bound: Mapping[str, int],
    prelaunch: object, manifest_sha256: str,
) -> str | None:
    """Why a declared prefix cannot fit this stage tier, or ``None`` (#1594).

    Compares the peak with the tier's minted capacity.  Headroom is not
    checked: a fit prefix with no free room waits.  Unknown capacity never
    refuses.
    """

    minted = storage_tiers.minted_tokens(tier).get(
        storage_tiers.capacity_kind_of(tier_id))
    if minted is None or bound["peak_gib"] <= minted:
        return None
    return (
        f"pbrun: prelaunch prefix {prelaunch} of "
        f"{manifest_sha256[:12]} needs peak "
        f"{bound['peak_gib']} GiB (retained "
        f"{bound['retained_gib']} GiB + suffix "
        f"{bound['suffix_gib']} GiB) on stage tier {tier_id}, "
        f"above the tier's minted capacity of {minted} GiB.  "
        f"Nothing was sealed or published.")


def split_prelaunch_cuts(
    ranges: Sequence[Mapping[str, object]],
    cuts: Sequence[list[tuple[int, int]]],
    prelaunch_names: frozenset[str] | set[str],
) -> tuple[list[list[tuple[int, int]]], list[list[tuple[int, int]]]]:
    """The declared prefix's cuts and the streaming suffix's, chosen by name.

    ``cuts`` holds one entry per range, and ``ranges`` omits an empty phase,
    so the manifest's declared count and the cut list can disagree: slicing
    by count would take a suffix phase's cuts into the prefix.  The names
    are what the manifest declared, so the split follows them (#1594).
    """

    if len(ranges) != len(cuts):
        raise SystemExit("pbrun: prelaunch cuts and ranges disagree; "
                         "nothing was sealed or published.")
    prefix = [chunks for span, chunks in zip(ranges, cuts)
              if str(span["name"]) in prelaunch_names]
    suffix = [chunks for span, chunks in zip(ranges, cuts)
              if str(span["name"]) not in prelaunch_names]
    return (prefix, suffix)


def residency_leg_cuts(
    record: Mapping[str, object],
    *,
    leg: str,
    ranges: Sequence[Mapping[str, object]],
    read_entries: Sequence[Mapping[str, object]],
) -> list[list[tuple[int, int]]]:
    """Every phase's chunk ranges on one leg, in read order.

    The chunk size is the tier's announced sizing, never this box's: the pin
    when the loop mints one, else the same window-quarter derivation the
    loop announces with.  A tier that announces no sizing seals whole-phase
    pairs -- chunking is a sealing-time property, and an unchunkable phase
    keeps the shape it always had.

    Cuts fall only on the manifest's entry boundaries (#965).  A mover
    stages every entry its range touches, so a cut inside an entry made both
    neighbouring movers overrun their reservations and refuse
    ``residency_overran_reservation`` on every attempt.  Each chunk is
    therefore whole entries, reserved at their bytes; an entry larger than
    the chunk is a chunk of its own.  An entry larger than the window the
    tier announces can never be admitted, so it refuses here, by name,
    before anything is sealed.  A record that announces no window (the
    stage's, which carries only its host's chunk) has nothing to refuse on.
    """

    def whole_gib(value: object) -> int | None:
        if isinstance(value, int) and not isinstance(value, bool) and value > 0:
            return value
        return None

    announced_chunk = whole_gib(record.get("promotion_chunk_gib"))
    announced_window = whole_gib(record.get("window_gib"))
    chunk_gib = announced_chunk
    if chunk_gib is None and announced_window is not None:
        chunk_gib = storage_tiers.promotion_chunk_gib_for_window(announced_window)
    spans = [(int(span["start_bytes"]), int(span["end_bytes"])) for span in ranges]
    if chunk_gib is None:
        return [[span] for span in spans]
    sizes = [int(entry.get("bytes", 0) or 0) for entry in read_entries]
    window_bytes = (None if announced_window is None
                    else announced_window * storage_tiers.GIB)
    cuts: list[list[tuple[int, int]]] = []
    for span, (start, end) in zip(ranges, spans):
        try:
            cuts.append(manifest_layout.split_range_into_chunks(
                start, end, chunk_gib * storage_tiers.GIB,
                entry_bytes=sizes, window_bytes=window_bytes))
        except manifest_layout.EntryExceedsWindow as exc:
            entry = read_entries[exc.entry_index]
            raise SystemExit(
                f"pbrun: residency_entry_exceeds_window: phase "
                f"{span['name']!r} reads {entry.get('path')!r} at offset "
                f"{entry.get('offset')}, {exc.end_bytes - exc.start_bytes} "
                f"bytes, which is larger than the {announced_window} GiB "
                f"window of the {leg} tier {record.get('tier_id')}.  A mover "
                f"stages whole entries, so no chunk can hold this one and "
                f"its mover could never be admitted.  Nothing was sealed or "
                f"published.") from None
    return cuts


def residency_stage_rows(
    template: Mapping[str, object],
    *,
    consumer_action_key: str,
    tier: Mapping[str, object],
    args: argparse.Namespace,
    queue,
    cas,
    movement_receipts: Sequence[Mapping[str, object]] | None = None,
) -> dict[str, object]:
    """Seal every movement and egress node this submission will ever have.

    All of them, now, before anything is published: an action key is a hash of
    an action body, so the only way a coordinator can publish the window's
    later phases is for the submitter to have sealed them and written the rows
    down.  That is also what freezes the decomposition -- a restart republishes
    these same children rather than cutting a new partition of the read order.

    The ranges are the manifest's own phases.  Nothing here chooses a boundary:
    a cut through an entry would hand a mover more bytes than its tokens
    reserved, and ``manifest_phase_ranges`` refuses a phase table that does not
    describe its own manifest.  Chunks inside a phase are cut at the same
    entries' boundaries (``residency_leg_cuts``, #965).
    """

    manifest_input = template["params"].get("data_manifest")   # type: ignore[union-attr]
    if not isinstance(manifest_input, Mapping):
        raise SystemExit(
            "pbrun: --residency stage needs --data-manifest: the byte ranges a "
            "mover stages are read-order offsets into that list, and there is "
            "no other way to say which bytes a range means")
    entry = manifest_input["input"]
    manifest, _ = pb.read_data_manifest(cas.input_path(entry))
    ranges = manifest_layout.manifest_phase_ranges(manifest)
    if not ranges:
        raise SystemExit(
            "pbrun: --residency stage needs a manifest that declares its read "
            "order in phases; this one declares none, so there is no boundary "
            "to stage up to that is not invented here")
    # Read the declared prelaunch-resident prefix (#1594).  Mark its phases
    # on the sealed plan.  Refuse its peak when it exceeds the tier.  A v1
    # manifest reaches this reader unchecked by ``core``.  Refuse a
    # non-boolean or a non-prefix declaration here.
    try:
        prelaunch = manifest_layout.manifest_prelaunch_phases(manifest)
    except ValueError as exc:
        raise SystemExit(
            f"pbrun: data manifest {str(entry['sha256'])[:12]} declares an "
            f"unusable prelaunch prefix: {exc}; nothing was sealed or "
            f"published.") from None
    prelaunch_names = frozenset(prelaunch)
    tier_id = str(tier["tier_id"])
    # A consumer that is already staged keeps the window it was frozen with.
    # Receipts price a *new* window; they must never repartition a frozen one.
    # The demand now depends on which receipts exist at submission, so a
    # resubmission of the same consumer after one more mover filed would seal
    # different mover keys and a different plan -- and ``residency_plan.freeze``
    # is first-writer, so it would refuse the whole submission with both bodies
    # in hand.  The frozen plan is the answer to that question, already agreed.
    #
    # The one exception is a plan a withdrawal marked superseded (#708): the
    # request is new, the old window must not be resurrected at the price it
    # was cancelled for, and the only supported reprice is this sealing path
    # run again -- after the old window's work has ended.
    #
    # The reading and the filing are one captured identity, never a key read
    # and a later "whatever is filed": a replacement can land between an
    # advisory check and a reap, and a caller that never saw it must not
    # archive it.  ``reap`` rechecks the exact ``(plan, filing)`` inside the
    # consumer's lock; when it removes nothing, this loop reads what is
    # actually filed now and decides again from that, rather than treating a
    # refused reap as "the old filing is gone" (#708 review).
    refusals: list[Exception] = []

    def filed_now():
        seen = len(refusals)
        plan, identity = residency_plan.read_filed(
            queue, consumer_action_key, on_unreadable=refusals.append)
        if plan is None and len(refusals) > seen:
            # A plan body this reader refuses, or a stat that failed: unknown
            # state, and a fresh seal over it would be a guess (#708 review).
            raise SystemExit(
                f"pbrun: the residency plan filed for "
                f"{consumer_action_key[:12]} cannot be read "
                f"({refusals[-1]!r}); refusing to reuse or replace it. An "
                f"operator must resolve it under {residency_plan.SUPERSEDED}/.")
        return plan, identity

    frozen, filing = filed_now()
    for _attempt in range(3):
        if frozen is None:
            break
        marker = residency_plan.superseded(queue, frozen)
        if marker is None:
            return {
                "plan": frozen,
                "residency": {
                    "schema": pool.RESIDENCY_SCHEMA_V1,
                    "manifest_sha256": str(frozen["manifest_sha256"]),
                    "manifest_bytes": int(frozen["manifest_bytes"]),
                    "tier_id": str(frozen["tier_id"]),
                    "leads": residency_plan.leads_for(frozen),
                },
                "reused_frozen_plan": True,
            }
        if marker.get("unreadable"):
            # Unknown retirement is not "not retired": without a readable
            # marker nobody can say which window the operator stopped, so a
            # fresh seal would be a guess and is refused (#708 review).
            raise SystemExit(
                f"pbrun: the supersession marker for {consumer_action_key[:12]} "
                f"is unreadable ({marker.get('error') or 'unknown state'}); "
                f"refusing to reuse or replace its frozen plan. An operator "
                f"must resolve the marker under {residency_plan.SUPERSEDED}/.")
        # A withdrawal marked this window superseded (#708).  Rows are sealed
        # with the resources their keys hash, so the only supported way to
        # request the range again at today's price is a fresh plan through
        # this same sealing path -- and only once the old window's ownership
        # has ended.  A live consumer or a claimed child is not replaced from
        # under itself: the old plan is not rewritten, and this refuses.
        safe, why = residency_plan.handoff_safe(queue, consumer_action_key, frozen)
        if not safe:
            raise SystemExit(
                f"pbrun: the residency plan for {consumer_action_key[:12]} was "
                f"superseded ({marker.get('reason') or 'withdrawn'}), and its "
                f"work has not ended: {why}.  Withdraw the consumer and let "
                f"the tiers loop reap the old window, then resubmit; sealed "
                f"rows and a frozen plan are never rewritten in place.")
        reaped = residency_plan.reap(
            queue, consumer_action_key, reason="superseded-reseal",
            plan=frozen, filing=filing)
        if reaped is not None:
            frozen = None
            break
        # The locked reap removed nothing: the handoff went live, or another
        # handoff changed the filing, after the advisory check above.  Read
        # the filing that actually stands and decide again from it; a stable
        # refusal follows on the next pass.
        frozen, filing = filed_now()
    else:
        raise SystemExit(
            f"pbrun: the residency plan for {consumer_action_key[:12]} kept "
            f"changing while this submission decided against it; nothing was "
            f"sealed and nothing published. Retry the submission.")
    stage_root = str(tier.get("mountpoint") or "")
    if not stage_root.startswith("/"):
        raise SystemExit(
            f"pbrun: stage tier {tier_id} announces no mountpoint to write into")
    digest = str(entry["sha256"])
    tags = [str(tier["host"])]
    mover_python, mover_tool, egress_tool = movement_tools(tier)
    announced = residency_plan.mover_announcement(tier)
    pool_root = str(SH / "pb-queue")
    # The ram leg, when a ram tier sits in front of this stage (#640): a
    # promotion node and an egress node per phase, sealed here with the rest
    # of the plan, because an action key is a hash and the coordinator cannot
    # publish children the submitter never sealed.  ``auto`` is the default so
    # the tier turns on with the mount; ``off`` is the A/B's other arm.
    ram_tier = None
    if str(getattr(args, "residency_ram", "auto") or "auto") == "auto":
        ram_tier = resolve_ram_tier(queue, tier)
    ram_tier_id = None
    ram_root = ""
    ram_python = ram_tool = ram_egress_tool = ""
    if ram_tier is not None:
        ram_tier_id = str(ram_tier["tier_id"])
        ram_identity = ram_tier.get("pool_identity")
        if not isinstance(ram_identity, Mapping):
            ram_identity = None
        ram_root = str(ram_tier.get("mountpoint") or "")
        if not ram_root.startswith("/"):
            raise SystemExit(
                f"pbrun: ram tier {ram_tier_id} announces no mountpoint to "
                f"promote into")
        ram_python, ram_tool, ram_egress_tool = movement_tools(
            ram_tier, mover=movement_actions.RAM_PROMOTE_SCRIPT)

    # Every chunk of both legs is cut here, before a single node is sealed or
    # published: an entry that no chunk of a tier can hold refuses the whole
    # submission now, rather than after half its nodes reached the CAS.
    read_entries = manifest_layout.manifest_read_entries(manifest)
    stage_cuts = residency_leg_cuts(tier, leg="stage", ranges=ranges,
                                    read_entries=read_entries)
    ram_cuts = (None if ram_tier is None else
                residency_leg_cuts(ram_tier, leg="ram", ranges=ranges,
                                   read_entries=read_entries))
    if prelaunch:
        # Fit the declared prefix peak to the minted tier capacity (#1594).
        # The sizes are the same chunk demands the sealed legs will carry,
        # cut at the same boundaries.  This refuses what the plan demands.
        # Headroom is not checked: a fit prefix with no free room waits.
        # Unknown capacity never refuses.
        prefix_cuts, suffix_cuts = split_prelaunch_cuts(
            ranges, stage_cuts, prelaunch_names)

        def _cut_gib(cuts: list[list[tuple[int, int]]]) -> list[int]:
            return [sum(storage_tiers.stage_tokens_for_bytes(cend - cstart)
                        for cstart, cend in chunks) for chunks in cuts]

        bound = residency_plan.prelaunch_peak_gib(
            _cut_gib(prefix_cuts), _cut_gib(suffix_cuts))
        refusal = prelaunch_capacity_refusal(
            tier, tier_id, bound, prelaunch, str(entry["sha256"]))
        if refusal is not None:
            raise SystemExit(refusal)

    # One read of the live receipts for the whole window: every mover in it has
    # the same structure and reads the same pool, so they price alike, and a
    # per-phase read would give two phases of one plan different demands
    # because a mover finished between them.
    readers = int(args.residency_mover_readers)
    # The consumer's own reading, as its submitter declares it (#909).  A
    # frozen plan returned above keeps the declaration it was frozen with.
    reader = reader_declaration(args)
    # A logical freeze supplies one invocation-local observation for all its
    # siblings. None retains the standalone submission's fresh observation;
    # an explicitly empty snapshot must not trigger another live census.
    # Mover receipts and egress receipts share the movers directory, and one
    # pass reads both (#1021): the movers price the copy, the egresses price
    # the release (``movement_actions.egress_price``).  A caller's snapshot
    # that holds no egress receipt seals every egress as before.
    observed = (queue.move_records(schemas=(pool.POOL_MOVE_SCHEMA_V1,
                                            pool.POOL_EGRESS_SCHEMA_V1))
                if movement_receipts is None else movement_receipts)
    receipts = [record for record in observed
                if not (isinstance(record, Mapping) and record.get("schema")
                        == pool.POOL_EGRESS_SCHEMA_V1)]
    # Which pools the receipts must have measured to price this window
    # (#611): the tier's current identity, as the tier loop announced it.
    # ``None`` -- a tier last announced by an older generation -- prices off
    # every usable receipt, exactly as before.
    tier_identity = tier.get("pool_identity")
    if not isinstance(tier_identity, Mapping):
        tier_identity = None
    priced = storage_tiers.mover_demand_from_receipts(
        receipts, tier_id=tier_id, readers=readers,
        fallback_mem_gb=int(args.residency_mover_mem_gb),
        pool_identity=tier_identity)
    # The pool bandwidth a mover reserves, once anything has measured it.  It
    # is what makes concurrency a ledger decision rather than an accident: the
    # tier mints what the disks delivered plus one probe mover's worth, and a
    # mover that reserves nothing can never be rationed against another.  One
    # copy's measured rate (#909): the slowest landing of this manifest's
    # latest window onto this tier when that window holds two copies or more,
    # else the median single-reader share of the tier's latest window (#958),
    # never the tier's whole offer.  The tier's *current* offer caps it: a fresh seal
    # asks no more than admission will honour on this cycle (#708), and with
    # nothing measured the offer is the stated bound, named in demand_source.
    fill_price = storage_tiers.mover_fill_price(
        receipts, tier_id=tier_id, pool_identity=tier_identity,
        manifest_sha256=digest)
    fill, offered_fill, fill_basis = current_fill_offer(tier, fill_price["mb_s"])
    mover_retry_policy = {
        "max_attempts": int(args.residency_mover_max_attempts),
        # True by construction, not by the operator's say-so: ``stage_move``
        # copies to a mover-keyed ``.<name>.partial``, verifies the digest,
        # then ``os.replace``s, and files its fragment only for entries it
        # verified.
        "retry_safe": True,
    }
    # A stage mover's stall grace (#1010), from what the tier has measured
    # (``movement_actions.mover_progress_policy``): its next landing at the
    # slowest measured landing of this manifest on this tier, capped by the
    # fill it reserves, plus the time a report takes to reach the stall
    # check.  Measured means a landing of this manifest only (``landing`` on
    # the price, whatever basis sealed the fill): a median share of other
    # manifests' copies is not a landing of this one, while one copy is --
    # a window of one prices no fill seal (#958) but still bounds how long a
    # copy of this manifest has gone between landings.  Time the pool is
    # the bottleneck is not priced here: the worker credits it on its own
    # sample of the pool's members (``core.POOL_CONTENTION_PARAM``), so a
    # policy is sealed only with the members the storage role announced.
    # With either unmeasured the mover is sealed as before, with no stall
    # grace, and ``demand_source.mover_progress`` says which was missing.
    landing_bytes_per_s: float | None = None
    landing = fill_price.get("landing")
    if isinstance(landing, Mapping) and landing.get("mb_s"):
        slowest = int(landing["mb_s"])                     # type: ignore[arg-type]
        if fill is not None:
            slowest = min(slowest, int(fill))
        landing_bytes_per_s = float(slowest * storage_tiers.MB)
    source_members = tier.get("source_members")
    pool_members = ([str(member) for member in source_members if member]
                    if isinstance(source_members, Sequence)
                    and not isinstance(source_members, (str, bytes)) else [])
    # How many movers were copying on the tier while the pricing window
    # landed, at most: the rate above was measured under that load, and a
    # reviewer comparing it with the load now needs both numbers.
    window_concurrency = None
    if isinstance(landing, Mapping):
        counts = [record.get(storage_tiers.MOVER_CONCURRENCY_FIELD)
                  for record in receipts
                  if isinstance(record, Mapping)
                  and str(record.get("manifest_sha256") or "") == digest
                  and str(record.get("consumer_action_key") or "")
                  == str(landing.get("window_consumer") or "")]
        counts = [int(count) for count in counts
                  if isinstance(count, int) and not isinstance(count, bool)]
        window_concurrency = max(counts) if counts else None
    entry_sizes = [int(read_entry.get("bytes", 0) or 0)
                   for read_entry in read_entries]
    entry_starts: list[int] = []
    position = 0
    for size in entry_sizes:
        entry_starts.append(position)
        position += size
    mover_progress: dict[str, object] = {}
    # A stage egress's stall grace (#1021), from the egresses this stage has
    # receipted (``movement_actions.egress_progress_policy``): the census,
    # this chunk's unlinks and the settle at the slowest measured terms, plus
    # the time a report takes to reach the stall check.  Sealed only with the
    # pool members the worker judges contention by, as a mover's is: the
    # census reads the queue on the source pool, and time that pool is the
    # bottleneck is credited, not priced.  Unmeasured, the egress is sealed
    # as before, with no stall grace, and ``demand_source.egress_progress``
    # says which term was missing.
    egress_terms = movement_actions.egress_price(observed, stage_root=stage_root)
    egress_progress: dict[str, object] = {}

    def chunk_entry_bytes(cstart: int, cend: int) -> list[int]:
        """The sizes of the read-order entries a chunk stages.

        Chunks are cut on entry boundaries (#965), so this is exactly the
        window ``stage_move`` copies (``manifest_layout.entries_between``).
        """

        low = bisect.bisect_right(entry_starts, cstart) - 1
        return [entry_sizes[index]
                for index in range(max(0, low), len(entry_sizes))
                if entry_starts[index] < cend
                and entry_starts[index] + entry_sizes[index] > cstart]

    phases: list[dict[str, object]] = []
    for ordinal, span in enumerate(ranges):
        start, end = int(span["start_bytes"]), int(span["end_bytes"])
        # The stage leg is cut into chunks (#675) the way the ram leg is
        # (#673): one movement node plus one egress node per chunk, in read
        # order, so the SSD refills as it frees instead of sawtoothing a
        # whole phase at a time.  The cuts were made above, at entry
        # boundaries, before anything was sealed (#965).  A phase that fits
        # in one chunk seals today's whole-phase pair, and a tier that
        # announces no sizing seals it too.
        chunk_ranges = stage_cuts[ordinal]

        def seal_stage_mover(cstart: int, cend: int, csuffix: str,
                             namespace: str) -> tuple[dict, dict]:
            """Seal one chunk's mover, filing its fragment under ``namespace``.

            ``namespace`` is the consumer's own key for a per-consumer mover,
            and the range's :func:`residency_plan.share_namespace` for a
            shared one (#1026): no one consumer's fragment directory may hold
            bytes the others read, or that consumer's death would retire
            them.  Returns the row and its stall-grace derivation.
            """

            chunk_demand = storage_tiers.residency_demand(
                tier_id=tier_id, range_start_bytes=cstart,
                range_end_bytes=cend, fill_mb_s_pool_side=fill)
            # Measured, not habitual, and above all *present*: a row without a
            # ``cpu`` key is read by ``adaptive_cpu`` as unknown CPU use and
            # refused whenever the box already holds anything
            # (``unbounded_cpu_not_exclusive``), which is what ran the first live
            # window one large mover at a time with 17 idle worker loops (#607,
            # #603).  Both numbers come off ``pb-queue/movers/`` receipts when
            # there are any, and ``demand_source`` on the plan says which receipts
            # were read and which field fell back to a declared bound.
            chunk_demand["cpu"] = int(priced["cpu"])
            chunk_demand["mem_gb"] = int(priced["mem_gb"])
            progress, derivation = movement_actions.mover_progress_policy(
                chunk_entry_bytes(cstart, cend),
                landing_bytes_per_s=landing_bytes_per_s)
            contention: dict[str, object] | None = None
            if progress is not None and not pool_members:
                # No members to judge the pool by: a pacer hold would be
                # charged to the copy, so no grace at all, as before.
                progress = None
                derivation = {**derivation, "basis": "unmeasured",
                              "unmeasured": "pool members", "grace_s": None}
            elif progress is not None:
                contention = movement_actions.pool_contention_spec(
                    members=pool_members, stage_root=stage_root,
                    priced_bytes_per_s=float(landing_bytes_per_s))  # type: ignore[arg-type]
            elif derivation["basis"] == "unmeasured":
                derivation = {**derivation, "unmeasured": "landing rate"}
            derivation = {**derivation,
                          "landing_window_movers": window_concurrency,
                          "pool_contention": contention}
            chunk_mover = seal_movement_action(
                template,
                command=[mover_python, mover_tool,
                         "--pool-root", pool_root,
                         "--cas-root", str(SH / "cas"),
                         "--consumer-action-key", namespace,
                         "--tier-id", tier_id,
                         "--stage-root", stage_root,
                         "--manifest-sha256", digest,
                         "--range-start-bytes", str(cstart),
                         "--range-end-bytes", str(cend),
                         # Stated on the command, so the width the row reserves and
                         # the width the copy runs at cannot drift apart.
                         "--readers", str(readers)]
                        + ([] if fill is None else
                           # Carried so the receipt can say what the ledger had
                           # promised this copy; a later cycle compares that with
                           # what the copy achieved, and a shortfall is the
                           # measured ceiling on how many movers the pool feeds.
                           ["--fill-mb-s-pool-side", str(fill)]),
                demand=chunk_demand,
                # A progress-governed action is placed only on a worker
                # that enforces the contract and exports the helper
                # environment the mover reports through.
                tags=[*tags, *contracts.declared_contract_tags(contracts.Publication(
                    progress=progress, pool_contention=progress is not None))],
                retry_policy=mover_retry_policy,
                log_name=f"stage-move-{ordinal:04d}-{span['name']}{csuffix}.log",
                extra_params=(None if progress is None
                              else {pb.PROGRESS_PARAM: progress,
                                    pb.POOL_CONTENTION_PARAM: contention}))
            cas.publish_action_request(chunk_mover)
            mover_row = {
                **publication_row(
                    chunk_mover, args=args, queue=queue,
                    max_attempts=int(args.residency_mover_max_attempts),
                    retry_safe=True),
                # The row, not only the sealed body.  ``residency_pin_holds``
                # reads the *queue record* to decide whether a concluding
                # mover keeps its tier tokens, so a row without this block
                # ends ``executed`` and hands its tokens straight back -- the
                # ledger reads its full supply free while 34 GB sit on the
                # stage, which is the one invariant #583 rests on.  The
                # consumer's block names leads; a mover's names the range it
                # makes resident, which is what the pin is checked against.
                "residency": {
                    "schema": pool.RESIDENCY_SCHEMA_V1,
                    "manifest_sha256": digest,
                    "manifest_bytes": int(entry["bytes"]),
                    "tier_id": tier_id,
                    "range_start_bytes": cstart,
                    "range_end_bytes": cend,
                },
            }
            return mover_row, derivation

        def seal_stage_chunk(cstart: int, cend: int,
                             csuffix: str) -> tuple[dict, dict]:
            """One chunk's mover row and this consumer's egress row for it.

            With sharing on (the default, #1026) the mover is the one every
            consumer of this exact range names: the first submitter of the
            range seals and registers it, and every later one puts that
            registered row in its own plan.  A mover's key is the hash of a
            body that carries its submission's checkout, pricing and log
            name, so a second submitter cannot derive the same key; the
            registration is how two submitters agree on it
            (:func:`residency_plan.register_shared_range`).  A registration
            sealed against another tier announcement -- another runtime
            generation's mover -- is reused only while its mover is live;
            otherwise this submission seals the range's mover afresh and
            replaces it.  The egress is
            always this consumer's own: for a shared range it drops this
            consumer's interest, and the last interest to go deletes.
            """

            if str(getattr(args, "residency_share", "auto") or "auto") == "auto":
                namespace = residency_plan.share_namespace(
                    digest, tier_id, cstart, cend)
                try:
                    record, sealed_here = residency_plan.register_shared_range(
                        queue, manifest_sha256=digest, tier_id=tier_id,
                        start=cstart, end=cend,
                        seal=lambda: seal_stage_mover(
                            cstart, cend, csuffix, namespace),
                        registered_by=consumer_action_key,
                        # Sealed against the announcement
                        # ``movement_tools`` read ``mover_python`` and
                        # ``mover_tool`` from.  A registration sealed
                        # against another one is reused only while its
                        # mover is live.
                        sealed_against=announced)
                except (residency_plan.ResidencyPlanError, OSError,
                        pool.PoolContractError) as exc:
                    raise SystemExit(
                        f"pbrun: the shared range [{cstart}, {cend}) of "
                        f"{digest[:12]} on {tier_id} cannot be registered "
                        f"or read ({exc}); nothing was published. "
                        f"--residency-share off stages it per consumer."
                    ) from None
                mover_row = dict(record["mover_row"])        # type: ignore[arg-type]
                mover_key = str(record["mover_action_key"])
                derivation = {
                    **dict(record.get("derivation") or {}),  # type: ignore[arg-type]
                    "share_namespace": namespace,
                    "registered_by": str(record.get("registered_by") or ""),
                    "sealed_here": bool(sealed_here)}
            else:
                mover_row, derivation = seal_stage_mover(
                    cstart, cend, csuffix, consumer_action_key)
                mover_key = str(mover_row["action_key"])
            mover_progress[mover_key] = derivation
            egress_policy, egress_derivation = (
                movement_actions.egress_progress_policy(
                    chunk_entry_bytes(cstart, cend), price=egress_terms))
            egress_contention: dict[str, object] | None = None
            if egress_policy is not None and not pool_members:
                # No members to judge the pool by: a census slowed by
                # another reader would be charged to the egress, so no
                # grace at all, as before.
                egress_policy = None
                egress_derivation = {**egress_derivation, "basis": "unmeasured",
                                     "unmeasured": "pool members",
                                     "grace_s": None}
            elif egress_policy is not None:
                egress_contention = movement_actions.pool_contention_spec(
                    members=pool_members, stage_root=stage_root,
                    priced_bytes_per_s=float(
                        egress_derivation["priced_bytes_per_s"]))  # type: ignore[arg-type]
            else:
                egress_derivation = {**egress_derivation,
                                     "unmeasured": "egress receipts"}
            egress_derivation = {**egress_derivation,
                                 "mover_action_key": mover_key,
                                 "pool_contention": egress_contention}
            chunk_egress = seal_movement_action(
                template,
                command=[mover_python, egress_tool,
                         "--pool-root", pool_root,
                         "--mover-action-key", mover_key,
                         "--consumer-action-key", consumer_action_key,
                         "--stage-root", stage_root],
                # No tier demand: an egress *returns* capacity, and one that had to
                # reserve some before it could give any back would deadlock exactly
                # when the stage is full -- which is the only moment it matters.
                # CPU and memory it must still declare, and bounded: a row without
                # a ``cpu`` key is *unknown* CPU use to ``adaptive_cpu``, refused
                # whenever the box holds anything (#603, #607) -- and the box an
                # egress runs on is the stage's own file server, whose resident
                # loops mean it always holds something.  A release that cannot
                # claim there deadlocks the tier through the same door the comment
                # above closes: the concluding movers pin their ranges' tokens, the
                # egress is the only node that returns them, and one waiting for an
                # empty box waits forever.  One CPU is a declared bound, the width
                # of the single-process unlink-and-record an egress is -- not a
                # measurement, because an egress files no receipts of its own, and
                # pricing it off the movers' copy receipts would measure the wrong
                # node entirely (the #655 lesson).
                demand={"cpu": 1, "mem_gb": 1},
                # A progress-governed egress is placed only on a worker that
                # enforces the contract, credits the pool on its own sample,
                # and never credits the egress's own hold of the stage's
                # ownership lock as a wait (``contracts.EGRESS_PROGRESS_TAG``).
                tags=[*tags, *contracts.declared_contract_tags(contracts.Publication(
                    progress=egress_policy, pool_contention=egress_policy is not None,
                    egress=egress_policy is not None))],
                # The egress's retry policy is a movement node's, as the
                # mover's is (#950): with the consumer's single attempt, one
                # transient unlink error left the range's bytes holding their
                # tokens until pressure eviction.  An egress's second attempt
                # finds released what the first released.
                retry_policy=mover_retry_policy,
                log_name=f"stage-release-{ordinal:04d}-{span['name']}{csuffix}.log",
                extra_params=(None if egress_policy is None
                              else {pb.PROGRESS_PARAM: egress_policy,
                                    pb.POOL_CONTENTION_PARAM: egress_contention}))
            cas.publish_action_request(chunk_egress)
            egress_progress[str(chunk_egress["action_key"])] = egress_derivation
            # No block on the egress: it reserves no tier capacity, and
            # ``validate_residency`` refuses a range whose stage demand is
            # below the range's own floor.  An egress finds its mover by
            # ``--mover-action-key``, not by a range of its own.
            return mover_row, publication_row(
                chunk_egress, args=args, queue=queue,
                max_attempts=int(args.residency_mover_max_attempts),
                retry_safe=True)

        stage_chunks = None
        stage_mover_row: dict[str, object] | None = None
        stage_egress_row: dict[str, object] | None = None
        if len(chunk_ranges) == 1:
            stage_mover_row, stage_egress_row = seal_stage_chunk(start, end, "")
        else:
            stage_chunks = []
            for cindex, (cstart, cend) in enumerate(chunk_ranges):
                chunk_mover_row, chunk_egress_row = seal_stage_chunk(
                    cstart, cend, f"-c{cindex:02d}")
                stage_chunks.append({
                    "chunk_index": cindex,
                    "start_bytes": cstart, "end_bytes": cend,
                    "stage_gib": storage_tiers.stage_tokens_for_bytes(
                        cend - cstart),
                    "mover_row": chunk_mover_row,
                    "egress_row": chunk_egress_row,
                })
        ram_mover_row = None
        ram_egress_row = None
        ram_chunks = None
        if ram_tier_id is not None:
            # The withdrawn #639 part 3's plumbing, aimed at the right actor:
            # one occupancy leg per movement node, ``ram_gib`` on the ram tier,
            # priced off the promotion receipts exactly the way a stage
            # mover's demand is priced off its own.
            #
            # The leg is cut into chunks (#673): one promotion node plus one
            # egress node per chunk, in read order, so the tmpfs refills as
            # it frees instead of sawtoothing a whole phase at a time.  The
            # cuts were made above, at entry boundaries, before anything was
            # sealed (#965).  A phase that fits in one chunk seals today's
            # whole-phase pair, and a tier that announces no sizing seals it
            # too.
            assert ram_cuts is not None
            chunk_ranges = ram_cuts[ordinal]

            def seal_ram_chunk(cstart: int, cend: int,
                               csuffix: str) -> tuple[dict, dict]:
                ram_demand = storage_tiers.residency_demand(
                    tier_id=ram_tier_id, range_start_bytes=cstart,
                    range_end_bytes=cend)
                ram_priced = storage_tiers.mover_demand_from_receipts(
                    receipts, tier_id=ram_tier_id, readers=readers,
                    fallback_mem_gb=int(args.residency_mover_mem_gb),
                    pool_identity=ram_identity)
                ram_demand["cpu"] = int(ram_priced["cpu"])
                # The runtime term only bounds the copier's own buffers and
                # metadata.  The destination is a tmpfs, whose pages stay
                # charged to the writing cgroup (writeback never reclaims
                # them), so the row must reserve the range it will write as
                # well -- otherwise the promotion OOMs at its own cap partway
                # through, which is how four live 4-11 GiB promotions died at
                # exactly 1 GiB on 2026-09-19.  ``ram_gib`` for the range's
                # retention on the tier is untouched; this only makes the
                # action's memory demand honest.
                ram_demand["mem_gb"] = storage_tiers.ram_promotion_mem_gb(
                    runtime_mem_gb=int(ram_priced["mem_gb"]),
                    range_bytes=cend - cstart)
                ram_mover = seal_movement_action(
                    template,
                    command=[ram_python, ram_tool,
                             "--pool-root", pool_root,
                             "--cas-root", str(SH / "cas"),
                             "--consumer-action-key", consumer_action_key,
                             "--tier-id", ram_tier_id,
                             "--ram-root", ram_root,
                             "--source-stage-root", stage_root,
                             "--manifest-sha256", digest,
                             "--range-start-bytes", str(cstart),
                             "--range-end-bytes", str(cend),
                             "--readers", str(readers)],
                    demand=ram_demand, tags=tags,
                    retry_policy=mover_retry_policy,
                    log_name=f"ram-promote-{ordinal:04d}-{span['name']}{csuffix}.log")
                ram_egress = seal_movement_action(
                    template,
                    command=[ram_python, ram_egress_tool,
                             "--pool-root", pool_root,
                             "--mover-action-key", str(ram_mover["action_key"]),
                             "--consumer-action-key", consumer_action_key,
                             "--stage-root", ram_root],
                    # No tier demand, for the same reason as the stage's egress,
                    # and one declared CPU for the same reason as its cpu: a
                    # tmpfs promotion's release runs on the same never-empty file
                    # server, and unknown CPU use would refuse to run beside the
                    # loops that make it never-empty.
                    demand={"cpu": 1, "mem_gb": 1}, tags=tags,
                    retry_policy=mover_retry_policy,
                    log_name=f"ram-release-{ordinal:04d}-{span['name']}{csuffix}.log")
                cas.publish_action_request(ram_mover)
                cas.publish_action_request(ram_egress)
                mover_row = {
                    **publication_row(
                        ram_mover, args=args, queue=queue,
                        max_attempts=int(args.residency_mover_max_attempts),
                        retry_safe=True),
                    # The pin the ram window and the pin check read: a promotion
                    # row without it releases its occupancy the moment it
                    # finishes -- bytes on a roof-limited tmpfs that no token
                    # stands for are ENOSPC waiting to happen (#640).
                    "residency": {
                        "schema": pool.RESIDENCY_SCHEMA_V1,
                        "manifest_sha256": digest,
                        "manifest_bytes": int(entry["bytes"]),
                        "tier_id": ram_tier_id,
                        "range_start_bytes": cstart,
                        "range_end_bytes": cend,
                    },
                }
                return mover_row, publication_row(
                    ram_egress, args=args, queue=queue,
                    max_attempts=int(args.residency_mover_max_attempts),
                    retry_safe=True)

            if len(chunk_ranges) == 1:
                ram_mover_row, ram_egress_row = seal_ram_chunk(
                    start, end, "")
            else:
                ram_chunks = []
                for cindex, (cstart, cend) in enumerate(chunk_ranges):
                    mover_row, egress_row = seal_ram_chunk(
                        cstart, cend, f"-c{cindex:02d}")
                    ram_chunks.append({
                        "chunk_index": cindex,
                        "start_bytes": cstart, "end_bytes": cend,
                        "stage_gib": storage_tiers.stage_tokens_for_bytes(
                            cend - cstart),
                        "ram_mover_row": mover_row,
                        "ram_egress_row": egress_row,
                    })
        phase_record: dict[str, object] = {
            "name": str(span["name"]),
            "start_bytes": start, "end_bytes": end,
            "stage_gib": storage_tiers.stage_tokens_for_bytes(end - start),
        }
        if stage_chunks is not None:
            phase_record["stage_chunks"] = stage_chunks
        else:
            assert stage_mover_row is not None and stage_egress_row is not None
            phase_record["mover_row"] = stage_mover_row
            # No block on the egress: it reserves no tier capacity, and
            # ``validate_residency`` refuses a range whose stage demand is
            # below the range's own floor.  An egress finds its mover by
            # ``--mover-action-key``, not by a range of its own.
            phase_record["egress_row"] = stage_egress_row
        if ram_mover_row is not None:
            phase_record["ram_mover_row"] = ram_mover_row
            phase_record["ram_egress_row"] = ram_egress_row
        if ram_chunks is not None:
            phase_record["ram_chunks"] = ram_chunks
        # Carry the declared prefix on the frozen plan (#1594).  Discovery,
        # admission and reuse derive leads from ``leads_for(plan)`` with no
        # manifest in hand.  Only declared phases carry the key.  Undeclared
        # plans stay byte-identical.
        if str(span["name"]) in prelaunch_names:
            phase_record["resident_before_launch"] = True
        phases.append(phase_record)
    plan = residency_plan.build_plan(
        consumer_action_key=consumer_action_key, tier_id=tier_id,
        stage_root=stage_root, manifest_sha256=digest,
        manifest_bytes=int(entry["bytes"]), phases=phases,
        ram_tier_id=ram_tier_id, reader=reader,
        # Which receipts priced these movers' cpu and mem_gb, so a demand in
        # the queue traces back to a measurement rather than to a habit.  On
        # the plan, not on a row: ``tier_loop`` publishes a row as
        # ``queue.publish(**row)``, whose parameters are a closed set.  The
        # fill entry names the basis: which tier offer capped it, or that
        # only receipts priced it.
        demand_source={**priced["demand_source"],
                       "fill_mb_s_pool_side": fill,
                       "fill": fill_basis,
                       "fill_measured": dict(fill_price),
                       "tier_offer_mb_s": offered_fill,
                       # Each stage mover's stall grace and every term of
                       # it (#1010), by mover key: ``basis`` is ``landing``
                       # when a measured landing priced it and the tier
                       # named the pool members the worker judges contention
                       # by (``pool_contention``, as sealed); ``unmeasured``
                       # (naming the missing term) when the mover was sealed
                       # with no grace.
                       "mover_progress": mover_progress,
                       # Each stage egress's stall grace and every term of
                       # it (#1021), by egress key: ``basis`` is ``egress``
                       # when the stage's receipted egresses priced it and
                       # the tier named the pool members; ``unmeasured``
                       # (naming the missing term) when the egress was
                       # sealed with no grace.
                       "egress_progress": egress_progress})
    return {
        "plan": plan,
        "residency": {
            "schema": pool.RESIDENCY_SCHEMA_V1,
            "manifest_sha256": digest,
            "manifest_bytes": int(entry["bytes"]),
            "tier_id": tier_id,
            "leads": residency_plan.leads_for(plan),
        },
    }


def seal_decomposed_child(
    template: Mapping[str, object],
    *,
    request: Mapping[str, object],
    plan: Mapping[str, object],
    child_ordinal: int,
    roster_input: Mapping[str, object],
    batch_input: Mapping[str, object],
    cas,
    data_manifest: Mapping[str, object] | None = None,
    prepared_batches=None,
) -> dict[str, object]:
    """Seal the ``child_ordinal``-th child of one plan, off one template.

    The four overrides ``seal_action_from_template`` accepts are exactly what
    a decomposition varies, and this is where they are filled in -- once, so
    that the campaign that publishes children and the test that pins what a
    child is are making the same call.  Two spellings of a child seal is how
    a suite comes to pass on a body nothing produces.

    Pure: the envelope is already a CAS blob by the time this is asked, and
    the path sealed into the command is that blob's name.  ``blob_path``
    rather than ``input_path`` on purpose -- the digest came from the ingest
    that wrote it, and re-reading a campaign's worth of batches to re-learn
    what each ingest just proved would put the whole roster through sha256 a
    second time for nothing.
    """

    command = dc.resolve_task_batch(
        template["params"]["command"],
        batch_path=cas.blob_path(str(batch_input["sha256"])),
    )
    inputs = [roster_input, batch_input]
    if prepared_batches is not None:
        prepared_batches.require_bound(request, plan)
    params = {dc.LOGICAL_BATCH_PARAM: (
        prepared_batches.membership(child_ordinal) if prepared_batches is not None
        else dc.logical_batch_param(request, plan, child_ordinal=child_ordinal))}
    if data_manifest is not None:
        if template["params"].get("data_manifest") is not None:
            raise SystemExit("a projected child cannot also inherit a shared data manifest")
        manifest = pb.validate_data_manifest(data_manifest)
        entry, _ = cas.ingest_bytes(
            pb._canonical_file_bytes(manifest), input_id=pb.PBCAMPAIGN_DATA_MANIFEST_INPUT_ID)
        inputs.append(entry)
        params["data_manifest"] = {"input": entry, "mount_prefix": manifest["mount_prefix"],
                                   "entry_count": manifest["entry_count"],
                                   "total_bytes": manifest["total_bytes"]}
    return seal_action_from_template(
        template,
        command=command,
        result_path=dc.child_result_manifest_path(child_ordinal),
        # The roster before the batch, in that order, on every child: the
        # input list reaches the key, so the order is part of the identity and
        # not a detail of how this loop was written.
        extra_inputs=inputs,
        extra_params=params,
    )


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Read one submission's arguments, and nothing about the world.

    Split out so a caller that is not the command line -- ``pbcampaign``
    decomposing one logical request into many actions -- can build the same
    namespace the CLI builds and hand it to ``prepare_submission``.  Every
    refusal below is a statement about the arguments alone, so it holds
    wherever they came from.

    ``--progress-phase`` is parsed here rather than at its use: the policy is
    the normalized form of two flags, a typo in one of them is an argument
    error, and saying so before ``--withdraw`` is where ``main`` said it.
    """

    ap = argparse.ArgumentParser(
        description="Submit one command to the PrismaBuild fleet and wait for "
                    "it. Either the pull queue or SLURM carries it, per "
                    "--transport or the published generation's default; the "
                    "result does not depend on which."
    )
    ap.add_argument("--client-events-fd", type=int, default=None, help=argparse.SUPPRESS)
    ap.add_argument("--demand", default="",
                    help="resource demand, e.g. gpu=1,mem_gb=16")
    ap.add_argument("--gpu", action="store_true",
                    help="shorthand for gpu=1,mem_gb=16")
    ap.add_argument("--cpus", type=int, default=1,
                    help="cores this action will actually use; a parallel test "
                         "run wants its -n, not 1")
    ap.add_argument("--exclusive", action="store_true",
                    help="demand the whole GPU capacity of one box")
    ap.add_argument("--gpu-memory-gb", type=float, default=None,
                    help="GPU memory budget in GiB; separate VRAM on discrete GPUs, "
                         "a subset of --demand mem_gb on unified-memory GPUs")
    ap.add_argument("--gpu-capacity", type=int, default=0,
                    help="slots to demand for --exclusive; 0 reads the largest "
                         "a matching box actually offers")
    ap.add_argument("--tag", action="append", default=[],
                    help="require a box offering this tag (e.g. a hardware class)")
    ap.add_argument(
        "--requires-files", default=None, metavar="JSON",
        help="a JSON array of {path, sha256} entries this action's bytes "
             "depend on; the row requires the dependency-digest capability "
             "tag, the claim hashes the actual bytes before spending an "
             "attempt, and a missing or drifted digest is a named denial, "
             "never a run (#1495)")
    ap.add_argument(
        "--d38-receipt", default=None, metavar="KEY",
        help="the full 64-character action key of the CPU preflight that "
             "proves this GPU job (D38). Its CAS receipt must bind this job's "
             "identity, images and namespace")
    ap.add_argument(
        "--d38-exception", default=None, metavar="DECISION_ID",
        help="an explicit CEO decision that grants D38 for exactly this job; "
             "never together with --d38-receipt")
    ap.add_argument(
        "--d38-namespace", default=None, metavar="PATH",
        help="a JSON namespace descriptor (execution mode, cwd, interpreter, "
             "mounts, prerequisite identities). Its digest is sealed into the "
             "job, so a changed namespace is a changed job")
    ap.add_argument(
        "--container-image", action="append", default=[], metavar="REF",
        help="require the claiming box's local Docker to positively hold this "
             "image before the action is claimed (repeatable). Accepts "
             "sha256:<64 hex> for a local image ID, "
             "repository@sha256:<64 hex> for a manifest digest, or "
             "content:sha256:<64 hex> for a store-independent content "
             "reference; a mutable tag is refused because it cannot be part "
             "of the action's identity. An image ID is what the claiming "
             "box's own image store calls the image and the two Sparks run "
             "different stores, so prefer the content form for portable "
             "work and read one with "
             "'python3 -m prismabuild.container_images <repo:tag>' (#805). "
             "PrismaBuild does not pull, load or transfer images: the "
             "reference must already be local on the claiming box, a box that "
             "cannot show it leaves the item ready for a box that can, and "
             "dispatch refuses when no recorded eligible worker reports it. "
             "Sealed into the action key and requires the container-image-v1 "
             "worker capability")
    ap.add_argument("--measurement", action="store_true",
                    help="seal task_class=measurement (pool: verified platform, "
                         "local unless --host-class is explicit; "
                         "SLURM: --host-class required): the result is numerics "
                         "that do not transfer across architectures")
    ap.add_argument("--host-class", default=None, metavar="CLASS",
                    help="require CLASS placement (e.g. gb10). Pool measurements "
                         "opt into matching workers, with verified platform, ABI, "
                         "driver and device models; external dependencies must be "
                         "identical across the class. SLURM seals host_class_keyed "
                         "and attests its constraint through the controller")
    ap.add_argument("--target-evidence", default=None, metavar="PATH",
                    help="seal the class facts of a pool --measurement "
                         "--host-class submission from this packet instead of "
                         "probing this box (#1598). Run tools/fleet/pbevidence.py "
                         "as an action on a worker of the class to make one. "
                         "Each worker still checks the facts against its own "
                         "live facts before it runs")
    ap.add_argument("--anywhere", action="store_true",
                    help="assert that command/tool/data dependencies outside "
                         "the snapshot are identical on every eligible worker")
    ap.add_argument("--here", action="store_true",
                    help="pin the materialized checkout to this box")
    ap.add_argument("--resident-set", help="explicit resident set declaration for bounded lease renewal; Phase 1 serves canonical paths")
    ap.add_argument(
        "--data-manifest",
        help="path to a plain JSON or gzip data manifest (64 MiB stored; "
             "gzip expands to at most 512 MiB) naming the shared-mount bytes this "
             "action will read; attached as a second content-addressed input "
             "so a storage-role loop can make them resident before the action "
             "is claimed. It is part of the action key: the same command with "
             "a manifest is a different action from the same command without "
             "one, and from the same command with a different manifest",
    )
    ap.add_argument(
        "--after", action="append", default=[], metavar="PRODUCER:TEMPLATE_ID",
        help="defer this submission until PRODUCER succeeds (#913).  PRODUCER "
             "is an action key, or the pending id another --after submission "
             "printed; TEMPLATE_ID is the write-only produced-output template "
             "it declares.  PB then builds this action's data manifest from "
             "--data-manifest (the static part, optional) followed by every "
             "origin-only batch the producer's successful attempt committed "
             "under the template, seals the action and publishes it.  The "
             f"command may carry {action_edges.DATA_MANIFEST_PLACEHOLDER} and "
             f"{action_edges.DATA_MANIFEST_SHA256_PLACEHOLDER}, each once, as a "
             "whole argument; they become that manifest's path and the SHA-256 "
             "the sealed request binds for it.  "
             "Repeatable; pool transport only",
    )
    ap.add_argument(
        "--supersedes", default=None, metavar="KEY_OR_PENDING_ID",
        help="state that this submission replaces a failed or withdrawn key, "
             "or an unreleased deferred submission, so that --after edges "
             "naming the old one follow this one (#913).  Every publish moves "
             "every key; this is how a resubmission keeps its dependents",
    )
    ap.add_argument(
        "--produced-output-template", default=None, metavar="PATH",
        help="path to a tiny validated produced-output template JSON "
             "(at most 64 KiB) declaring the bounded working window this "
             "action will stage for bytes it produces itself. Captured as an "
             "ordinary CAS declared input plus action params, so changing the "
             "template changes the action key and editing the file after seal "
             "changes nothing the worker reads. The qualified tier demand is "
             "derived from the window (never the durable corpus) and added "
             "to the explicit CPU/memory/GPU reservation; the claim holds "
             "all of it before the producer starts. Pool transport only.",
    )
    ap.add_argument(
        "--residency", choices=RESIDENCY_MODES, default="none",
        help="stage this action's declared bytes onto a storage tier before it "
             "runs (#583).  'stage' seals one movement node per phase of the "
             "data manifest's read order and one egress node each, and admits "
             "the action only once its first phase's bytes are on the tier and "
             "still pinned there; the tiers loop publishes the window -- the "
             "first phase included -- taking over a range already resident "
             "rather than copying it, as accepted progress advances.  Needs --data-manifest, because a byte range "
             "is meaningless without the list it indexes.  'none', the "
             "default, publishes exactly what it published before",
    )
    ap.add_argument(
        "--residency-tier", default=None,
        help="which announced stage tier to stage onto; only needed when the "
             "fleet announces more than one",
    )
    ap.add_argument(
        "--residency-ram", choices=RESIDENCY_RAM_MODES, default="auto",
        help="seal a ram leg onto the residency plan: one promotion node and "
             "one egress node per phase, copying each landed stage range into "
             "the ram tier the storage box announces (#640).  'auto' seals the "
             "leg when a ram tier is live on the stage's own host, which is "
             "the tier turning on with the mount; 'off' is the A/B's other "
             "arm.  A plan already frozen keeps the leg it was frozen with",
    )
    ap.add_argument(
        "--residency-share", choices=("auto", "off"), default="auto",
        help="stage each range once for every consumer that reads it (#1026): "
             "the first submission of a range seals and registers its mover "
             "under the range's identity, and every later submission of the "
             "same manifest range on the same tier names that mover, so the "
             "tier holds one copy charged once.  'off' seals a mover per "
             "consumer, as before, and is the A/B's other arm.  A plan "
             "already frozen keeps the movers it was frozen with",
    )
    ap.add_argument(
        "--residency-mover-mem-gb", type=int, default=1,
        help="memory one movement node reserves when no mover receipt has "
             "measured one yet.  The copy is a bounded read-ahead window, so "
             "this does not grow with the range; once receipts exist the row "
             "takes the maximum peak_rss_bytes they report instead",
    )
    ap.add_argument(
        "--residency-mover-readers", type=int, default=4,
        help="paced read buffers one movement node copies through.  It is "
             "passed to the mover and declared as its cpu demand until a "
             "receipt's cpu_seconds measures one, so the two cannot disagree",
    )
    ap.add_argument(
        "--residency-prefetch-depth-gib", type=int, default=None,
        help="how many GiB this action holds ahead of the phase it is reading "
             "(#909).  Carried on the residency plan, so the tier prices the "
             "window's read-ahead at it rather than at this action's memory "
             "reservation (mem_gb plus its GPU budget), which is the bound "
             "used when nothing is declared.  A fact about the reader, "
             "supplied by whoever knows its prefetch; 0 is a reader that "
             "holds nothing ahead",
    )
    ap.add_argument(
        "--residency-read-mb-s", type=int, default=None,
        help="how fast this action reads its staged bytes, in MB/s (#909).  "
             "Carried on the residency plan: the tier prices the window's "
             "consumption at it until the action reports progress, and its "
             "movers reserve it as their pool fill.  Undeclared, the window "
             "is priced at its run-ahead bound until it reports",
    )
    ap.add_argument(
        "--residency-mover-max-attempts", type=int, default=3,
        help="attempts one movement node gets, independent of --max-attempts. "
             "A mover copies to a temporary, verifies the digest and renames, "
             "so a retry is safe and a transient read error should not cost "
             "the whole staged range",
    )
    ap.add_argument(
        "--checkout-snapshot-max-bytes",
        type=int,
        default=CHECKOUT_SNAPSHOT_MAX_BYTES,
        help="lower the hard fleet ceiling for both the logical materialized "
             "Git tree and compressed bundle (cannot raise it)",
    )
    ap.add_argument(
        "--snapshot-ref",
        action="append",
        default=[],
        metavar="NAME",
        help="also advertise this source branch in the snapshot bundle, so "
             "the action can spell it (e.g. --snapshot-ref master for a "
             "master...HEAD gate); repeatable",
    )
    ap.add_argument("--cwd", default=os.getcwd(),
                    help="directory the command runs in, on this box and "
                         "inside the checkout being sealed; it is recorded "
                         "in the action as a path relative to the checkout "
                         "root, so the action stays portable")
    ap.add_argument(
        "--as-sealed-by", metavar="ACTION_KEY",
        help="reuse the retained runtime wrapper of this full action key and "
             "require the complete current seal to match before submission; "
             "collect pre-publication receipts without creating a new key",
    )
    ap.add_argument("--deterministic", action="store_true",
                    help="declare byte-identical output; enables CAS reuse")
    ap.add_argument("--gang-group", default=None,
                    help="32-hex gang group this action is a member of (#1517); use "
                         "pbgang.py, which seals every member and files the group")
    ap.add_argument("--gang-size", type=int, default=None,
                    help="members in the gang (with --gang-group)")
    ap.add_argument("--gang-index", type=int, default=None,
                    help="this member's index, 0..size-1 (with --gang-group)")
    ap.add_argument(
        "--retry-safe",
        action="store_true",
        help=("declare the whole command idempotent across failed attempts, "
              "including every external side effect"),
    )
    ap.add_argument(
        "--max-attempts",
        type=int,
        default=DEFAULT_MAX_ATTEMPTS,
        help=("bounded attempts for a --retry-safe action; arbitrary commands "
              "default to one"),
    )
    ap.add_argument("--timeout-s", type=float, default=None,
                    help="positive execution deadline in seconds, enforced by "
                         "both transports; the pool worker's safety ceiling "
                         "(7200 s by default, announced per box and reported "
                         "here when it would cut this request short) also "
                         "applies. Queue waiting is bounded by --wait-s")
    ap.add_argument("--progress-phase", "--progress", action="append", default=None,
                    metavar="NAME=SECONDS",
                    help="declare one phase of this action and the quiet it is "
                         "allowed in it, in order, once per phase. Declaring "
                         "any of them admits the action under the progress "
                         "contract: it is then bounded by how long it goes "
                         "without committing work rather than by how long it "
                         "runs, the worker's ceiling clamps each phase's "
                         "allowance instead of the whole run, and --timeout-s "
                         "if given still ends it whatever it is doing. The "
                         "action reports with prismabuild.progress.commit, or "
                         "by running $PRISMABUILD_ACTION_PROGRESS_HELPER when "
                         "it cannot import PrismaBuild")
    ap.add_argument("--progress-cycle", action="store_true",
                    help="allow progress phases to repeat; each phase gets one "
                         "allowance between increases in cumulative committed "
                         "units. Requires --progress-phase and cyclic-capable workers")
    ap.add_argument("--awaited-batch", default=None, metavar="PARENT:PLAN:STATE",
                    help="declare the decomposed batch this coordinator awaits, "
                         "as parent and plan keys and the controller-state "
                         "directory, separated by colons. Valid "
                         "only with --progress-phase on pool transport: the "
                         "worker's stall watch credits quiet while a verified "
                         "awaited child is ready or claimed (#1666)")
    ap.add_argument("--wait-s", type=float, default=86400.0,
                    help="give up waiting for a worker to pick this up")
    ap.add_argument(
        "--filesystem-growth-gib", type=int, default=1, metavar="GIB",
        help="the GiB this submission may write to the shared store (snapshot, "
             "CAS objects, queue record), reserved from its filesystem_gib "
             "ledger while it is written when the used-filesystem floor is "
             "observed or enforced (#1483); ignored while the floor is off")
    ap.add_argument(
        "--detach", action="store_true",
        help="seal and submit exactly as usual, print one JSON line naming the "
             "action key, the transport, the job id or queue record and the "
             "terminal-record paths, and exit 0 without waiting; an action "
             "already in the CAS prints status=cache_hit and submits nothing, "
             "and one already running prints status=attached and joins that "
             "run rather than starting a second copy of it. "
             "Wait for it later with pbwait.py. Incompatible with "
             "--max-attempts greater than 1: a retry needs somebody alive to "
             "see the attempt fail",
    )
    ap.add_argument("--priority", type=int, default=0,
                    help="a queue hint, higher runs sooner; not part of the "
                         "action's identity, so two submissions that differ "
                         "only in priority are the same action. The pull "
                         "queue sorted its ready list on it, before age; the "
                         "SLURM lane spends it as a --nice "
                         "(slurm_lane.nice_for), scaled so one priority step "
                         "outranks submission order rather than one later "
                         "submission")
    ap.add_argument("--priority-reason", type=pool.normalize_priority_reason,
                    default=None, help="optional explanation of the priority, "
                    "stored with the submission and shown by pbstatus; not "
                    "part of the action identity or an admission requirement. "
                    "Use single-line printable text, at most 1024 characters")
    ap.add_argument("--profile", type=_profile_mode, default=None,
                    metavar="{" + ",".join(pb.PROFILE_MODES) + "}",
                    help="run a profiler around this action's child and store "
                         "the profile as a CAS blob named on the ending. "
                         "Unlike a queue hint this IS part of the action's "
                         "identity: a profiled run has its own key, so it is "
                         "never answered from an unprofiled receipt and never "
                         "an A/B arm against one. 'sample' is py-spy at "
                         f"{pb.PROFILE_SAMPLE_RATE_HZ} Hz over the whole "
                         "process tree, or 'sample:HZ' seals a positive "
                         "whole rate in samples per second ('sample:10' "
                         "samples ten times a second); 'nsys' is Nsight "
                         "Systems over CUDA and NVTX, and takes a window in "
                         "seconds ('nsys:600' "
                         "traces the first ten minutes and lets the action run "
                         "on); 'torch' is a contract the action opts into, "
                         "exporting its own Chrome trace to the path in "
                         f"{pb.TorchProfileBackend.OUT_ENV}. Measured overhead "
                         "per mode is in docs/operating_prismabuild.md, with "
                         "the box load it was measured under")
    ap.add_argument("--env", action="append", default=[],
                    help="K=V added to the action's environment (repeatable)")
    ap.add_argument("--no-default-env", action="store_true",
                    help="declare only --env, without the fleet defaults")
    ap.add_argument("--withdraw", action="append", default=[], metavar="KEY",
                    help="cancel this queued or running action (a key prefix is "
                         "enough) instead of submitting; repeatable")
    ap.add_argument("--reason", default="",
                    help="why, recorded on the withdrawal record, or on the "
                         "release --release-origin-consumer files")
    ap.add_argument(
        "--release-origin-consumer", nargs=2, default=None,
        metavar=("BATCH_REF", "CONSUMER_KEY"),
        help="release one consumer's declaration on one consumed origin batch "
             "instead of submitting (#926).  BATCH_REF is the batch's ref, as "
             "JSON or a path to a JSON file (output-origin-retirement-stalled "
             "prints it); CONSUMER_KEY is the declared consumer, which must "
             "have failed or been withdrawn, or be unpublished with nothing "
             "left that can publish it (#945).  The tier loop then retires the "
             "batch once every other declared consumer has succeeded")
    ap.add_argument(
        "--transport", choices=TRANSPORTS,
        default=default_transport(),
        help="which dispatcher carries this submission (env "
             "PRISMABUILD_TRANSPORT, else the published runtime generation's "
             "default_transport); the pull queue stays the default until the "
             "fleet has cut over to SLURM")
    ap.add_argument("command", nargs=argparse.REMAINDER,
                    help="the command to run, after a bare --; every word "
                         "past it belongs to the command and not to pbrun")
    args = ap.parse_args(argv)
    if args.release_origin_consumer is not None and (
            args.withdraw or args.after or args.supersedes is not None
            or args.as_sealed_by is not None):
        ap.error("--release-origin-consumer is not a submission; it takes no "
                 "--withdraw, --after, --supersedes or --as-sealed-by")
    if args.as_sealed_by is not None and args.withdraw:
        ap.error("--as-sealed-by cannot be combined with --withdraw")
    if args.after or args.supersedes is not None:
        # A deferred submission is sealed at release, by the tiers loop,
        # against the pull queue's own records (#913).
        if args.withdraw:
            ap.error("--after and --supersedes cannot be combined with --withdraw")
        if args.transport != "pool":
            ap.error("--after and --supersedes need the pull queue, where "
                     "producers, releases and supersessions are filed")
    if args.after and args.as_sealed_by is not None:
        ap.error("--after cannot be combined with --as-sealed-by: a deferred "
                 "action has no key until it is released")
    if args.timeout_s is not None and (
        not math.isfinite(args.timeout_s) or args.timeout_s <= 0
    ):
        raise SystemExit("pbrun: --timeout-s must be a positive finite number")
    args.progress_policy = parse_progress_phases(
        args.progress_phase, cycle=args.progress_cycle)
    args.awaited_batch = parse_awaited_batch(args.awaited_batch)
    # Some things an argument gets wrong can only be judged once the demand
    # is resolved -- a GPU budget on a slot that reserves no GPU is the case
    # -- and they are argument errors all the same.  So the parser that
    # produced this namespace travels with it, and ``prepare_submission``
    # reports them the way every other argument error reports: usage,
    # message, exit 2.
    args.refuse_argument = ap.error
    return args


def prepare_submission(args: argparse.Namespace) -> dict[str, object]:
    """Resolve one submission against this box, and freeze what it says.

    Everything between the arguments and the frozen template: the checkout
    gates, the default environment, the demand, the placement the fleet's
    offers decide, and the refusals each of those can raise.  None of it is
    optional for a decomposed submission -- a parent's children inherit this
    placement and this environment, so they inherit these refusals too, and
    a second copy of the block in ``pbcampaign`` would be a second answer to
    the same questions.

    The returned ``cwd`` and ``portable_checkout`` are here only because the
    submission notices report them; the template carries everything else,
    including the resolved tags and demand.
    """

    progress_policy = args.progress_policy
    wrapper_dir = CONTAINER_WRAPPER_DIR
    if args.as_sealed_by is not None:
        try:
            wrapper_dir = reseal_wrapper(args.as_sealed_by)
        except (OSError, ValueError, pb.PrismaBuildError) as exc:
            raise SystemExit(f"pbrun: cannot reseal: {exc}") from None
    command = args.command
    if command and command[0] == "--":
        command = command[1:]
    if not command:
        raise SystemExit("nothing to run: pbrun [options] -- <command>")
    if args.max_attempts < 1:
        raise SystemExit("pbrun: --max-attempts must be at least 1")
    if args.max_attempts > 1 and not args.retry_safe:
        raise SystemExit(
            "pbrun: --max-attempts greater than 1 requires --retry-safe; "
            "--deterministic covers result bytes, not external side effects"
        )
    if args.detach and args.max_attempts > 1:
        raise SystemExit(detached_attempts_refusal(args.max_attempts))
    gang = gang_declaration(args)
    retry_policy = {
        "max_attempts": args.max_attempts,
        "retry_safe": args.retry_safe,
    }
    determinism = "deterministic" if args.deterministic else "stochastic"
    # An image reference is sealed into the action's identity, so a mutable
    # tag or a malformed digest is an argument error here, before any
    # checkout work, exactly like a missing --cwd.
    image_refusal = container_image_refusal(args.container_image)
    if image_refusal is not None:
        args.refuse_argument(f"--container-image: {image_refusal}")
    images = container_images.normalize_refs(args.container_image)
    try:
        require_container_image_scope(images=images, transport=args.transport)
    except ValueError as exc:
        args.refuse_argument(str(exc))
    # The digest requirements are sealed into the action's identity, so a
    # malformed entry is an argument error here, before any checkout work,
    # exactly like a malformed image reference (#1495).
    requirements = None
    if args.requires_files is not None:
        try:
            parsed = json.loads(args.requires_files)
        except ValueError as exc:
            args.refuse_argument(f"--requires-files is not JSON: {exc}")
        try:
            requirements = dependency_digest.validate_requirements(parsed)
        except ValueError as exc:
            args.refuse_argument(f"--requires-files: {exc}")

    cwd = Path(args.cwd).resolve()
    if not cwd.is_dir():
        # Say which of the two things went wrong.  A closure is computed from
        # the checkout and stamped inside it, so pbrun submits only for a
        # checkout on the box it is running on -- the QUEUE is shared, the
        # filesystem is not.  The old message named a missing directory, which
        # is right for a typo and actively misleading for the other case: a
        # cross-box submission ("run the suite on sparklina's checkout, from
        # sparky") reads as "the path is wrong" when the path is correct and
        # simply belongs to another box.  Submitting from that box is not a
        # workaround; it is where the closure can honestly be taken.
        raise SystemExit(
            f"--cwd is not a directory on {socket.gethostname()}: {cwd}\n"
            f"pbrun reads the source checkout to seal its bytes, so it can "
            f"only submit for a checkout on the box it runs on. If this path "
            f"exists on another box, submit from there -- the queue is "
            f"shared, the filesystem is not."
        )

    repository_root = git_repository_root(cwd)
    if repository_root is None:
        raise SystemExit(
            "pbrun: --cwd must be inside a Git checkout so its exact bytes "
            "can be sealed and materialized through the CAS; mutable "
            "path-addressed submission is not supported"
        )
    require_checkout_snapshot_limit(args.checkout_snapshot_max_bytes)
    # Before the stamp is written, before Git hashes a byte, and before
    # anything reaches the CAS or the queue: an unresolvable ref name is a
    # typo, and the only cheap moment to say so is now.
    require_complete_history(repository_root)
    resolve_snapshot_refs(repository_root, list(args.snapshot_ref))
    early_paths = snapshot_path_roster(repository_root)
    require_working_tree_size(
        repository_root,
        early_paths,
        max_bytes=args.checkout_snapshot_max_bytes,
    )
    require_untransformed_checkout(repository_root, early_paths)
    require_checkout_owned_scripts(
        command, cwd, repository_root=repository_root
    )
    portable_checkout = True
    logical_cwd = cwd.relative_to(repository_root).as_posix() or "."

    # `run_local_action` builds the child's environment from *these* and
    # nothing else, so an empty dict is not "inherit the caller". Keep the
    # caller's additions separate as well: placement resolves argv[0] against
    # the complete declared PATH, while its conservative data-path screen must
    # not mistake fleet-owned defaults for caller-owned external inputs.
    variables = {} if args.no_default_env else {
        "HOME": "/home/rob",
        "TMPDIR": "/home/rob/tmp",
        "TRITON_CACHE_DIR": "/home/rob/.triton-cache",
        "PATH": "/usr/local/bin:/usr/bin:/bin",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
    }
    caller_variables: dict[str, str] = {}
    for entry in args.env:
        if "=" not in entry:
            raise SystemExit(f"--env expects K=V, got {entry!r}")
        key, value = entry.split("=", 1)
        if key in {CONTAINER_OWNER_ENV, CONTAINER_MARKER_ENV}:
            raise SystemExit(
                f"pbrun: {key} is derived by the container lifecycle; "
                "callers may not set it")
        variables[key] = value
        caller_variables[key] = value

    demand = _parse_demand(args.demand)
    require_disk_metadata_scope(demand, transport=args.transport)
    if args.gpu_memory_gb is not None:
        try:
            adaptive_gpu.memory_budget_bytes(args.gpu_memory_gb)
        except ValueError as exc:
            args.refuse_argument(f"--gpu-memory-gb: {exc}")
    if args.gpu:
        demand.setdefault("gpu", 1)
        demand.setdefault("mem_gb", 16)
    demand.setdefault("mem_gb", 4)
    # Cores are a demand like any other, and the default of one is what makes
    # this safe to add to a live fleet: every action already in flight keeps
    # the admission it had.  What it buys is a way for an action that will
    # take twenty-four cores to SAY twenty-four, which nothing could express
    # before -- and on 2026-09-04 four `pytest -n 24` runs each declaring
    # `mem_gb=4` were admitted to one 80-core box together, load average 371.
    if args.cpus < 1:
        raise SystemExit("--cpus must be at least 1")
    demand.setdefault("cpu", args.cpus)
    if not args.no_default_env:
        for name in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS"):
            variables.setdefault(name, str(demand["cpu"]))
    # A produced-output producer's local spool window -- accounted by default
    # when the environment declares a bound (#905), opt-in otherwise (#747) --
    # and any bounded local scratch it declares (#911).  Derived like the
    # template's tier demand, never typed: ``_parse_demand`` has already
    # refused a typed ``spool_gb``.  With neither declared this adds nothing,
    # and the demand is byte-for-byte what it was.
    demand.update(local_disk_terms(variables, transport=args.transport))

    if args.anywhere and args.here:
        raise SystemExit("--anywhere and --here contradict each other")
    if args.anywhere and args.tag:
        # The same contradiction with the second constraint spelled as a
        # class rather than as a hostname: --anywhere asserts that every
        # eligible worker can run this action, and --tag says only the boxes
        # offering that tag may.  Both landed before, and --anywhere won the
        # part the SLURM lane reads -- an action tagged x86 went to the
        # default partition as portable work.
        raise SystemExit(
            "--anywhere and --tag contradict each other: --anywhere asserts "
            "every eligible worker can run this action, and --tag admits only "
            "the boxes offering "
            f"{', '.join(sorted(set(args.tag)))}.  Drop whichever is not true."
        )
    target_evidence = None
    target_evidence_path = getattr(args, "target_evidence", None)
    if target_evidence_path is not None:
        missing = [name for name, present in (
            ("--measurement", args.measurement),
            ("--host-class", args.host_class is not None),
            ("the pool transport", args.transport == "pool")) if not present]
        if missing:
            raise SystemExit(
                "pbrun: --target-evidence needs " + ", ".join(missing) + ": it "
                "stands in for the local probe of a class-scoped pool "
                "measurement (#1598)")
        target_evidence = load_target_evidence(
            target_evidence_path, host_class=args.host_class)
        print(
            f"pbrun: sealing the facts of class {args.host_class} from "
            f"--target-evidence {target_evidence_path}, not from this box; "
            "each worker checks them against its own live facts before it runs",
            file=sys.stderr, flush=True)
    require_host_class_scope(
        measurement=args.measurement, host_class=args.host_class,
        transport=args.transport, anywhere=args.anywhere,
    )
    pool_measurement = args.measurement and args.transport == "pool"
    pool_measurement_class = pool_measurement and args.host_class is not None
    # One bounded offer snapshot serves default-class proof and the later
    # admission advice. Explicit placement and measurements never read it here.
    q = None
    offer_snapshot = None

    def offer_queue():
        nonlocal q, offer_snapshot
        if q is None:
            q = pool.PoolQueue(SH / "pb-queue")
        if offer_snapshot is None:
            offer_snapshot = bounded_offer_snapshot(q)
        return offer_snapshot

    # Check original inputs before the collector binds external relative paths.
    if portable_checkout:
        require_relocatable_checkout(command[1:], variables, cwd, repository_root=repository_root)
    # Resolve against the same shim-prefixed PATH the captured action receives.
    invocation_environment = {
        **variables, "PATH": f"{wrapper_dir}:{variables.get('PATH') or '/usr/local/bin:/usr/bin:/bin'}"}
    tags, dependency_queries, command, bound_caller = placement_contract(
        cwd,
        explicit=[*args.tag, *([args.host_class] if pool_measurement_class else [])],
        here=args.here or (pool_measurement and not pool_measurement_class),
        hostname=socket.gethostname(), portable_checkout=portable_checkout,
        command=command, repository_root=repository_root,
        environment=invocation_environment, caller_environment=caller_variables,
        anywhere=args.anywhere, needs_gpu=bool(demand.get("gpu")),
        offer_queue=offer_queue if args.transport == "pool" else None,
    )
    variables = {**variables, **bound_caller}
    tags = pool.normalize_placement_tags(tags)
    if args.host_class is not None:
        # The class rides the placement axis, the same way --tag does, so the
        # action key moves with it and the SLURM lane seals it as
        # --constraint.  A union rather than a replacement: a hostname pin a
        # box-local executable earned stays, and the class narrows it further.
        tags = pool.normalize_placement_tags([*tags, args.host_class])
    tags = pool.normalize_placement_tags([
        *tags, *contracts.declared_contract_tags(contracts.Publication(
            progress=progress_policy,
            awaited_batch=getattr(args, "awaited_batch", None) is not None
                and progress_policy is not None,
            gang=gang is not None, container_images=bool(images),
            scratch_io=scratch_io_intent(variables, transport=args.transport) is not None,
            requires_files=bool(requirements)))])
    require_reachable_runtime(
        tags, hostname=socket.gethostname(), runtime_root=RUNTIME_ROOT)
    # Other paths retain lazy discovery until submission advice needs it.

    placement = {"required_tags": tags}
    if args.exclusive and args.transport == "slurm":
        # SLURM already has a word for the whole device.  ``gpu:1`` and
        # ``shard:N`` are mutually exclusive requests against one GPU, so
        # exclusivity is a different GRES name rather than a bigger count, and
        # the count the pool had to read off worker offers is not needed.
        demand["gpu"] = args.gpu_capacity or 1
        demand["mem_gb"] = max(int(demand.get("mem_gb", 0)), 16)
    elif args.exclusive:
        # "All of one box" is a fact about the boxes, and guessing it does not
        # fail loudly -- it fails as an action nobody can ever claim.  The
        # default was 4 while sparky declares 2 and sparklina 1, so every
        # --exclusive submission asked for twice the slots that exist and sat
        # in ``ready`` forever.  Read it from what the fleet announces, which
        # needs the placement tags, so it happens after them.
        demand["gpu"] = args.gpu_capacity or exclusive_gpu_demand(
            offer_queue(), tags)
        demand["mem_gb"] = max(int(demand.get("mem_gb", 0)), 16)

    produced_template_opt = getattr(args, "produced_output_template", None)
    if produced_template_opt is not None:
        if args.transport != "pool":
            raise SystemExit(
                "pbrun: --produced-output-template needs the pull queue: "
                "tier working-window reservations live in the pool ledgers")
        from prismabuild import produced_output as produced_mod

        # Bounded pre-read for demand derivation only; freeze captures once
        # and cross-checks, so a file swapped between here and there fails
        # closed there rather than sealing drifted demand.
        try:
            with open(produced_template_opt, "rb") as handle:
                raw_pre = handle.read(pb.PRODUCED_OUTPUT_TEMPLATE_MAX_BYTES + 1)
        except OSError as exc:
            raise SystemExit(
                f"pbrun: cannot read --produced-output-template: {exc}") from None
        if len(raw_pre) > pb.PRODUCED_OUTPUT_TEMPLATE_MAX_BYTES:
            raise SystemExit(
                "pbrun: --produced-output-template exceeds "
                f"{pb.PRODUCED_OUTPUT_TEMPLATE_MAX_BYTES} bytes")
        try:
            pre_body = json.loads(raw_pre.decode())
        except (UnicodeDecodeError, ValueError) as exc:
            raise SystemExit(
                f"pbrun: --produced-output-template is not JSON: {exc}") from None
        try:
            pre_validated = produced_mod.validate_template(pre_body)
            pre_terms = produced_mod.owner_demand_terms(pre_validated)
        except produced_mod.ProducedOutputError as exc:
            raise SystemExit(
                f"pbrun: --produced-output-template invalid: {exc}") from None
        # The explicit user reservation (CPU/memory/GPU) is preserved; the
        # qualified tier demand is derived from the bounded working window,
        # never typed by hand and never the durable corpus.
        for qualified, need in pre_terms.items():
            if qualified in demand:
                raise SystemExit(
                    f"pbrun: --demand must not name tier demand {qualified!r}: "
                    "the produced-output template derives it")
            demand[qualified] = int(need)

    if portable_checkout:
        require_relocatable_checkout(
            command[:1], {}, cwd, repository_root=repository_root
        )

    # A CPU slot must not be able to run GPU work.  The pool's whole claim is
    # that the ledger knows what is on each accelerator, and that claim was
    # false in one direction: an action submitted WITHOUT ``--gpu`` inherited a
    # visible device and ran CUDA anyway.  A pytest suite queued as a 4 GB CPU
    # action executed its ``skipif(not torch.cuda.is_available())`` tests on a
    # box whose GPU slots were held by somebody else -- work the ledger could
    # not see, contending with work it had promised exclusivity to.
    #
    # The rule is enforced the way ``require_pool.py`` enforces its own escape
    # hatch, by the kernel rather than by belief: with no device visible the
    # child cannot do GPU work, so a mis-declared action fails instead of
    # stealing.  Declaring a device on a slot that did not reserve one is the
    # mis-declaration itself, so it is refused rather than honoured -- the fix
    # is ``--gpu``, and the message says so.  This applies under
    # ``--no-default-env`` too: an empty environment means every device is
    # visible, which is the case this exists for.
    declared = variables.get("CUDA_VISIBLE_DEVICES")
    try:
        require_gpu_memory_scope(
            gpu_memory_gb=args.gpu_memory_gb, gpu=bool(demand.get("gpu")),
            transport=args.transport,
        )
    except ValueError as exc:
        args.refuse_argument(str(exc))
    try:
        require_progress_scope(
            progress=progress_policy, transport=args.transport)
        require_awaited_batch_scope(
            awaited=getattr(args, "awaited_batch", None),
            progress=progress_policy, transport=args.transport)
    except ValueError as exc:
        args.refuse_argument(str(exc))
    if not demand.get("gpu"):
        if declared not in (None, ""):
            raise SystemExit(
                f"pbrun: this action reserves no GPU but sets "
                f"CUDA_VISIBLE_DEVICES={declared!r}.\n"
                "A CPU slot that touches the GPU is work the ledger cannot "
                "see, contending with work it promised exclusivity to.\n"
                "Add --gpu (and --gpu-capacity N if you need more than one "
                "slot), or drop the variable.")
        variables["CUDA_VISIBLE_DEVICES"] = ""

    template = freeze_action_template(
        command=command,
        cwd=cwd,
        logical_cwd=logical_cwd,
        demand=demand,
        placement=placement,
        variables=variables,
        determinism=determinism,
        retry_policy=retry_policy,
        host_class=args.host_class,
        measurement=args.measurement,
        transport=args.transport,
        pool_measurement_class=bool(pool_measurement_class),
        target_evidence=target_evidence,
        # A deferred submission's manifest is built at release (#913); its
        # static part is ingested beside the template, never sealed into it.
        data_manifest_path=(None if getattr(args, "after", None)
                            else args.data_manifest),
        produced_output_template_path=produced_template_opt,
        checkout_snapshot_max_bytes=args.checkout_snapshot_max_bytes,
        snapshot_refs=args.snapshot_ref,
        exclusive=args.exclusive,
        gpu_memory_gb=args.gpu_memory_gb,
        execution_timeout_s=args.timeout_s,
        progress=progress_policy,
        awaited_batch=getattr(args, "awaited_batch", None),
        profile=args.profile,
        container_image_refs=images,
        d38_namespace=(d38_gate.load_namespace(args.d38_namespace)[1]
                       if getattr(args, "d38_namespace", None) else None),
        wrapper_dir=wrapper_dir,
        dependency_queries=dependency_queries,
        gang=gang,
        resident_set=getattr(args, "resident_set", None),
        requires_files=requirements,
    )
    return {
        "args": args,
        "template": template,
        "cwd": cwd,
        "portable_checkout": portable_checkout,
        "offer_queue": offer_queue,
    }


def announce_placement(
    queue,
    action: Mapping[str, object],
    *,
    args: argparse.Namespace,
    cwd: Path,
    portable_checkout: bool,
) -> None:
    """Say how this work will be placed, and refuse it if it cannot be.

    Four notices and one refusal, all of them about one intent -- the tags,
    the GPU need and the demand -- which is why they are one function.  A
    decomposed campaign calls it once, before it publishes any child: the
    children of one plan share their whole placement, so asking the census N
    times would print the same paragraph N times and answer it N times.

    The refusal is the part that must not be skipped.  A required tag no box
    has offered is not a slow submission -- the item matches no worker's
    filter, so it sits in ``ready`` while every idle worker polls past it --
    and a campaign that published forty such children would have forty of
    them to withdraw.
    """

    params = action["params"]
    tags = params["placement"]["required_tags"]
    demand = params["demand"]
    progress_policy = args.progress_policy
    intent = {"tags": tags, "needs_gpu": bool(demand.get("gpu")), "resources": demand}
    if params.get("container_images"):
        # The sealed requirement, so every verdict below -- placeable,
        # placement_hosts, the refusal -- reads the same matcher a claim does.
        intent["container_images"] = list(params["container_images"])
    if params.get("interpreter"):
        # Same authority for the interpreter (#1263): the probe carries the
        # sealed path and the capability tag publish will add, so placeable
        # answers for the row it is about to write.
        intent["interpreter"] = str(params["interpreter"])
        intent["tags"] = [*tags, contracts.INTERPRETER_TAG]
    if params.get("local_dependencies"):
        intent["local_dependencies"] = dict(params["local_dependencies"])
    if params.get("requires_files"):
        # Same authority for the digest requirements (#1495): the probe
        # carries the sealed declarations and the capability tag publish adds, so
        # placeable answers for the row it is about to write.
        intent["requires_files"] = [dict(entry) for entry in params["requires_files"]]
        intent["tags"] = pool.normalize_placement_tags(
            [*tags, contracts.DEPENDENCY_DIGEST_TAG])
    # Say how wide this action is before saying it was queued.  A pin is a
    # consequence of the checkout path, and nothing used to report it, so a
    # submitter narrowed the fleet to one box without being told.
    notice = pin_notice(
        queue,
        intent,
        cwd=cwd,
        hostname=socket.gethostname(),
        here=args.here,
        portable_checkout=portable_checkout,
    )
    if notice:
        print(notice, file=sys.stderr, flush=True)

    # An offer can be fresh but stamped ahead of this submitter. Keep that
    # discrepancy visible, including an offer too far ahead to use at all.
    for host, skew in queue.offer_clock_skews().items():
        disposition = "tolerated" if skew <= pool.OFFER_FUTURE_TOLERANCE_S else "ignored"
        print(
            f"pbrun: {host} offer announced {skew:.3f}s in the future "
            f"(clock skew; {disposition}, limit {pool.OFFER_FUTURE_TOLERANCE_S:g}s)",
            file=sys.stderr, flush=True,
        )

    # Before the placement verdicts, not after.  ``intent`` now requires
    # ``PROGRESS_TAG``, so on a fleet that offers none the generic "no recorded
    # worker can run this action" would fire first and name a tag the operator
    # never typed.  Asked of the boxes eligible on every OTHER tag, which is
    # also the honest question: of the boxes that could run this work, which
    # can keep its stall policy?
    progress_tags = contracts.declared_contract_tags(contracts.Publication(
        progress=progress_policy,
        awaited_batch=progress_policy is not None
            and getattr(args, "awaited_batch", None) is not None))
    progress_notice = progress_contract_notice(
        queue,
        {**intent, "tags": [
            t for t in tags
            if t not in progress_tags
        ]},
        policy=progress_policy,
        requested_timeout_s=args.timeout_s,
        awaited_batch=getattr(args, "awaited_batch", None),
    )
    if progress_notice:
        print(progress_notice, file=sys.stderr, flush=True)

    image_notice = container_image_notice(queue, intent)
    if image_notice:
        print(image_notice, file=sys.stderr, flush=True)

    # Refuse work the RECORDED fleet cannot run, at the one moment the caller
    # is still watching.  A required tag no box has offered is not a slow
    # submission: the item matches no worker's placement filter, so it sits in
    # `ready` -- counted, reported as pending -- while every idle worker polls
    # past it until `--wait-s` expires a day later.
    #
    # Capability and liveness are deliberately different reads of the SAME
    # matcher.  Worker offers expire for claiming and fleet-width diagnostics,
    # but the latest record from each host remains evidence of what that box
    # can fit.  dl380g10 and gx10-6b77 have both spent longer than the 120 s TTL
    # inside work; while they were between announcements a fresh nonmatching
    # offer made ``placeable`` answer False and pbrun rejected a caller willing
    # to wait two hours.  An unbounded age reads retained capability and lets
    # ``--wait-s`` own an offline/busy box.  ``None`` still means no worker has
    # ever announced and stays a warning, so a fleet whose loops predate the
    # registry can submit unchecked.
    #
    # Images are one more term in the same matcher, and they make the refusal
    # sharper rather than a new kind of check: a declared reference no
    # recorded eligible offer positively reports is exactly as unplaceable as
    # a tag no box offers, and it fails here instead of in the container
    # (#714).
    # The interpreter's own verdict first (#1266 review): absent raises below
    # with the path named; unknown publishes with a notice and probes the
    # rest of the intent WITHOUT the interpreter requirement, because the
    # first submission of any path is unknown by construction and refusing it
    # would deadlock the tool on its own first use -- the claim-time stat is
    # the guard, and the fleet's next poll answers for the path.
    probe_intent = intent
    if intent.get("interpreter"):
        verdict, path = interpreter_submission_verdict(queue, intent)
        if verdict == "unknown":
            print(f"pbrun: no worker has answered for the interpreter "
                  f"{path} yet; the action publishes, the claim-time check "
                  "guards every box, and the fleet's next poll places it "
                  "where the path lives.", file=sys.stderr, flush=True)
            probe_intent = {
                name: value for name, value in intent.items()
                if name != "interpreter"}
            probe_intent["tags"] = [
                tag for tag in probe_intent.get("tags") or []
                if tag != contracts.INTERPRETER_TAG]
    # The digest requirements' verdict (#1495), asked where the caller is
    # still watching, in the interpreter's shape (#1266): a fleet that offers
    # no dependency-digest capability is refused -- nothing can ever claim
    # the row, and unlike a first path there is no first use that must
    # publish; a unanimous absent is refused naming the paths; a capable box
    # that has not answered for a path yet publishes with a notice, because
    # the claim gate hashes the bytes on every box and the fleet's next poll
    # answers for them.
    if intent.get("requires_files"):
        verdict = queue.dependency_placement_verdict(probe_intent)
        paths = ", ".join(sorted(
            str(entry["path"]) for entry in intent["requires_files"]))
        if verdict == "unknown_capability":
            raise SystemExit(
                "pbrun: no recorded worker offers the "
                f"{contracts.DEPENDENCY_DIGEST_TAG} capability, so "
                f"nothing can ever claim this action's pinned dependencies "
                f"({paths}); run a worker generation that carries the "
                "contract before submitting it (#1495)\n"
                + "\n".join(contracts.contract_remedies([contracts.DEPENDENCY_DIGEST_TAG])))
        if verdict == "absent":
            raise SystemExit(
                "pbrun: every recorded worker that could claim this action "
                f"names its required dependencies absent: {paths} (#1495)")
        if verdict == "unknown_paths":
            print(
                "pbrun: no worker has answered for the required "
                f"dependencies ({paths}) yet; the action publishes, the "
                "claim-time digest check guards every box, and the fleet's "
                "next poll places it where the bytes live.",
                file=sys.stderr, flush=True)
            probe_intent = {
                name: value for name, value in probe_intent.items()
                if name != "requires_files"}
            probe_intent["tags"] = [tag for tag in probe_intent.get("tags") or []
                                    if tag != contracts.DEPENDENCY_DIGEST_TAG]
    live_verdict = queue.placeable(probe_intent)
    capability_verdict = queue.placeable(
        probe_intent, max_age_s=RECORDED_OFFER_MAX_AGE_S)
    if capability_verdict is False:
        # The counterfactuals below start from ``probe_intent``, the intent the
        # verdict above was asked of (#1595).  An unknown interpreter is
        # already out of it.  Starting from ``intent`` kept the interpreter
        # requirement, so every question failed on the interpreter and the
        # missing image or capability was never named.  The interpreter keeps
        # its own verdict, asked of ``intent`` below.
        images = (queue.image_placement_census(
            probe_intent, max_age_s=RECORDED_OFFER_MAX_AGE_S)
            if intent.get("container_images") else None)
        image_blocked = bool(images and images["capable"])
        capability_blocked = bool(images and images["eligible"] and not image_blocked)
        image_line = ""
        if image_blocked:
            image_line = (
                "  container images: "
                + ", ".join(intent["container_images"])
                + " (no recorded eligible worker reports "
                + ("it" if len(intent["container_images"]) == 1 else "them")
                + ")\n")
        elif capability_blocked:
            image_line = (
                "  container images: " + ", ".join(intent["container_images"])
                + f" (no eligible worker offers {contracts.CONTAINER_IMAGE_TAG})\n")
        interpreter_line = ""
        if intent.get("interpreter"):
            # Absent is unanimous and final (#1266 review): every capable
            # eligible offer answered, and every answer named the path
            # missing.  Anything less is unknown, which publishes with a
            # notice instead of refusing -- a first submission is unknown by
            # construction, and the claim-time stat remains the guard.
            verdict, _path = interpreter_submission_verdict(queue, intent)
            if verdict == "absent":
                interpreter_line = (
                    "  interpreter:    " + str(intent["interpreter"])
                    + " (every recorded eligible worker names it absent)\n")
        capacity_line = ("" if image_blocked or capability_blocked else
                         placement_capacity_notice(queue, probe_intent))
        remedy = (
            "Load or pull the image on a box that offers these tags and the "
            f"{contracts.CONTAINER_IMAGE_TAG} capability, then wait for its worker's "
            "next inventory refresh. An image declaration is a claim-time "
            "requirement, so a box that cannot positively show it will not "
            "run the action."
            if image_line else
            "Fix the listed capacity demand or required tags, or wait for an "
            "offer that can fit them."
        )
        if capability_blocked:
            remedy = (f"Start a worker offering {contracts.CONTAINER_IMAGE_TAG} "
                      "on an eligible box; its image inventory must then "
                      "report the declared reference.")
        if interpreter_line:
            remedy = (
                "The interpreter path is part of the action's placement: a "
                "worker must report it in its offer before the action can "
                "run there. Install the interpreter on a box that offers "
                "these tags, let its worker's next poll answer for the path, "
                "and resubmit; submitting anyway would only queue a command "
                "that dies with 127 on the first box to claim it.")
        recorded_tags = queue.offered_tags(max_age_s=RECORDED_OFFER_MAX_AGE_S)
        missing_code_tags = set(tags) - set(recorded_tags)
        if capability_blocked:
            missing_code_tags.add(contracts.CONTAINER_IMAGE_TAG)
        guidance = contracts.contract_remedies(missing_code_tags)
        if guidance:
            remedy += "\n" + "\n".join(guidance)
        raise SystemExit(
            f"pbrun: no recorded worker can run this action.\n"
            f"  required tags: {tags or '(any box)'}\n"
            f"{image_line}"
            f"{interpreter_line}"
            f"  demand:        {demand}\n"
            f"{capacity_line}"
            f"  offered on record: "
            f"{recorded_tags}\n"
            f"{remedy}"
        )
    producer_room_refusal = producer_allowance_room_refusal(queue, action)
    if producer_room_refusal is not None:
        raise SystemExit(producer_room_refusal)
    if capability_verdict is None:
        if intent.get("container_images"):
            # No worker has announced at all, so no inventory exists to place
            # an image-pinned action against.  Submitting unchecked would put
            # the action back in the exact position #714 describes: any old
            # loop that matches the tags could claim it and fail inside the
            # container.  Refused.
            raise SystemExit(
                "pbrun: no worker offers on record, and this action declares "
                "container image "
                + ", ".join(intent["container_images"])
                + ".\n"
                "  An image requirement is checked against the claiming "
                "worker's local Docker inventory, and no worker has announced "
                "one. Start a worker whose loop offers "
                f"{contracts.CONTAINER_IMAGE_TAG}, load the image on a box that "
                "offers these tags, and resubmit."
            )
        print("pbrun: no worker offers on record; submitting unchecked",
              file=sys.stderr, flush=True)
    elif live_verdict is not True:
        print(
            "pbrun: no matching worker is live now; a recorded capable worker "
            "is between announcements or offline.  Submitting so --wait-s "
            f"{args.wait_s:g} owns how long to wait.",
            file=sys.stderr, flush=True,
        )

    ceiling_notice = timeout_ceiling_notice(
        queue, intent, requested=args.timeout_s if progress_policy is None else None)
    if ceiling_notice:
        print(ceiling_notice, file=sys.stderr, flush=True)


def interpreter_submission_verdict(queue, intent: Mapping[str, object]):
    """``("present"|"absent"|"unknown", detail)`` for the sealed path (#1266).

    One rule, asked where the caller is still watching: absent raises, naming
    the path; unknown publishes with a notice, because a first submission is
    unknown by construction and refusing it would deadlock the tool on its
    own first use -- the claim-time stat remains the guard on every box.
    """

    interpreter = str(intent.get("interpreter") or "")
    verdict = queue.interpreter_placement_verdict(
        intent, interpreter, max_age_s=RECORDED_OFFER_MAX_AGE_S)
    return verdict, interpreter


def interpreter_of(command: Sequence[str]) -> str | None:
    """The absolute interpreter a command names, when it names one (#1263).

    ``argv[0]`` that is an absolute path to a ``python*`` binary.  Nothing is
    guessed from a wrapper, a venv name or a tag: the item declares exactly
    the bytes it will exec, and the fleet answers for that path or not at
    all.  ``None`` keeps today's behaviour byte for byte.
    """

    if not command:
        return None
    first = str(command[0])
    if not first.startswith("/"):
        return None
    name = first.rsplit("/", 1)[-1]
    if not name.startswith("python"):
        return None
    return first


def publication_row(
    action: Mapping[str, object],
    *,
    args: argparse.Namespace,
    queue,
    max_attempts: int | None = None,
    retry_safe: bool | None = None,
    tested_repository: str | None = None,
) -> dict[str, object]:
    """The queue row that submits one sealed action.

    A queue row and the action it points at are two spellings of one
    submission, so every field that describes the work is read off the sealed
    body itself rather than off a template or a local the caller happens to
    still be holding.  That matters most for the owner: the lifecycle cleanup
    asks Docker for a label derived from it, so a row naming a different owner
    than the body would query a label nothing carries.  It matters for the
    rest because a decomposed child seals its own ``params``: reading the row
    off the body is what makes a child's row describe the child.

    Only the submitter's own handles -- priority, the attempt ceiling, retry
    safety -- come from ``args``, and they are exactly the fields no action
    body carries, because they say how hard to try rather than what to run.
    The tested-repository tag (#1565) is the one caller-supplied field that
    names the work: it arrives as an argument because it is frozen in the
    template, which the sealed body must not carry, and a caller that cannot
    name the tree it froze passes nothing.

    ``pbcampaign`` publishes decomposed children through this too.  The
    alternative is a second copy of the literal, which is how a row and a body
    come to disagree about one action.
    """

    params = action["params"]
    demand = params["demand"]
    variables = action["environment"]["variables"]
    row: dict[str, object] = {
        "action_key": str(action["action_key"]),
        "cas_root": str(SH / "cas"),
        "worker_script": str(RUNTIME_ROOT / "tools" / "prismabuild_worker.py"),
        "tags": params["placement"]["required_tags"],
        "needs_gpu": bool(demand.get("gpu")),
        "priority": args.priority,
        "resources": demand,
        # ``args`` for the submitter's own work; overridden only by a movement
        # node, whose idempotence is a property of the mover rather than of
        # what the submitter asked for (#603).
        "max_attempts": (args.max_attempts if max_attempts is None
                         else int(max_attempts)),
        "container_owner": str(variables[CONTAINER_OWNER_ENV]),
        "checkout_snapshot": params["checkout_snapshot"],
    }
    priority_reason = getattr(args, "priority_reason", None)
    if priority_reason is not None:
        parameters = inspect.signature(queue.publish).parameters
        if ("priority_reason" not in parameters
                and not any(p.kind is inspect.Parameter.VAR_KEYWORD
                            for p in parameters.values())):
            raise pool.PoolContractError(
                "loaded queue runtime does not support priority_reason; "
                "use a matching runtime or omit --priority-reason")
        row["priority_reason"] = pool.normalize_priority_reason(priority_reason)
    if tested_repository is not None:
        # The template's frozen answer for the tree this action seals (#1565).
        # Guarded like the annotation above: a loaded runtime that cannot
        # carry the tag refuses rather than silently untagging the submission.
        parameters = inspect.signature(queue.publish).parameters
        if ("tested_repository" not in parameters
                and not any(p.kind is inspect.Parameter.VAR_KEYWORD
                            for p in parameters.values())):
            raise pool.PoolContractError(
                "loaded queue runtime does not support tested_repository; "
                "use a matching runtime")
        row["tested_repository"] = pool.normalize_tested_repository(
            tested_repository)
    if params.get("container_images"):
        # Derived from the sealed body, never re-read from the caller: the row
        # describes the action, so the action's own params are the authority.
        row["container_images"] = list(params["container_images"])
    if params.get("interpreter"):
        row["interpreter"] = str(params["interpreter"])
    for field in ("local_dependencies", "dependency_queries"):
        if params.get(field):
            row[field] = dict(params[field])
    if params.get("gang"):
        row["gang"] = dict(params["gang"])
    if params.get("requires_files") and "requires_files" in (
            inspect.signature(queue.publish).parameters):
        # Derived from the sealed body like the interpreter (#1495), and
        # guarded like retry_safe: in a mixed runtime window the row simply
        # omits the field and the capability tag in the row's tags alone
        # still fences every worker that cannot read the requirement.
        row["requires_files"] = [
            {"path": str(entry["path"]), "sha256": str(entry["sha256"])}
            for entry in params["requires_files"]]
    # The repo checkout can advance just before the atomic runtime generation
    # rolls.  The previous PoolQueue already accepts the safety-critical bound,
    # so keep that mixed window usable; add the explanatory annotation once the
    # loaded runtime exposes it.  The sealed action params carry the full
    # contract in both cases.
    if "retry_safe" in inspect.signature(queue.publish).parameters:
        row["retry_safe"] = (args.retry_safe if retry_safe is None
                             else bool(retry_safe))
    if params.get("resident_set") is not None:
        row["resident_set"] = params["resident_set"]
    return row


#: The submitter's own handles a deferred submission freezes (#913): what
#: ``publication_row`` and ``residency_stage_rows`` read off ``args`` when the
#: release publishes the consumer.  Everything else about the action is in
#: its frozen template.
_DEFERRED_PUBLICATION_ARGS = (
    "priority", "priority_reason", "max_attempts", "retry_safe", "residency",
    "residency_tier",
    "residency_ram", "residency_share", "residency_mover_mem_gb",
    "residency_mover_readers",
    "residency_mover_max_attempts",
    # The reader's declaration (#909), which a deferred consumer's plan must
    # carry exactly as a direct submission's does.
    "residency_prefetch_depth_gib", "residency_read_mb_s",
    # D38 evidence the release checks again against the sealed key.
    "d38_receipt", "d38_exception",
)


def check_supersession_or_exit(q, old: str, *, new: str, new_kind: str) -> None:
    """Refuse now what :func:`file_supersession_or_exit` would refuse, writing nothing (#1585).

    For a submission that still has fallible work to do before its row can
    publish: the record is immutable, so one filed for a replacement that then
    refuses strands the old key at a successor that never existed.
    """

    try:
        action_edges.check_supersession(q, old, new=new, new_kind=new_kind)
    except action_edges.ActionEdgeError as exc:
        raise SystemExit(f"pbrun: --supersedes: {exc}") from None


def file_supersession_or_exit(q, old: str, *, new: str, new_kind: str) -> None:
    """File that ``new`` replaces ``old``, or refuse the submission (#913)."""

    try:
        action_edges.file_supersession(q, old, new=new, new_kind=new_kind)
    except action_edges.ActionEdgeError as exc:
        raise SystemExit(f"pbrun: --supersedes: {exc}") from None
    print(f"pbrun: {new[:12]} supersedes {old[:12]}; edges that name it "
          f"follow this submission", file=sys.stderr, flush=True)


def template_record(template: Mapping[str, object]) -> dict[str, object]:
    """A frozen template as JSON: everything but the CAS handle (#913).

    The CAS is the one live object a template carries; the release rebuilds
    it from the recorded root.  The marker root is a path, recorded as text.
    """

    record = {name: value for name, value in template.items() if name != "cas"}
    record["marker_root"] = str(template["marker_root"])
    try:
        return json.loads(json.dumps(record))
    except (TypeError, ValueError) as exc:
        raise SystemExit(
            f"pbrun: this submission cannot be deferred: its frozen template "
            f"is not plain data ({exc})") from None


def template_from_record(record: Mapping[str, object], *,
                         cas_root: str | Path) -> dict[str, object]:
    """The frozen template a deferred record carries, ready to seal."""

    template = dict(record)
    template["cas"] = pb.PrismaBuildCAS(Path(cas_root))
    template["marker_root"] = Path(str(record["marker_root"]))
    return template


def resolve_after_edges(q, cas_root: Path,
                        texts: Sequence[str]) -> list[dict[str, str]]:
    """Every ``--after`` edge, checked against what the queue has filed.

    An unknown producer refuses: a key needs a readable row or terminal
    record, a pending id a deferred record.  So does a producer that does
    not declare the template, or a template that is not write-only.
    """

    edges: list[dict[str, str]] = []
    for text in texts:
        try:
            edge = action_edges.parse_edge(text)
            kind = action_edges.producer_kind(q, edge["producer"])
            if kind == action_edges.PRODUCER_KEY:
                declared = action_edges.declared_template_id(
                    cas_root, edge["producer"])
                if declared != edge["template_id"]:
                    raise action_edges.ActionEdgeError(
                        f"the producer declares template {declared!r}, not "
                        f"{edge['template_id']!r}")
                action_edges.require_write_only_template(
                    q.root, edge["template_id"])
            else:
                deferred = action_edges.read_deferred(q.root, edge["producer"])
                assert deferred is not None
                produced = deferred["template"].get("produced_output_template")
                if (not isinstance(produced, Mapping)
                        or produced.get("template_id") != edge["template_id"]):
                    raise action_edges.ActionEdgeError(
                        "the deferred producer does not declare template "
                        f"{edge['template_id']!r}")
                if not produced.get("write_only"):
                    raise action_edges.ActionEdgeError(
                        f"template {edge['template_id']!r} is not write-only")
        except action_edges.ActionEdgeError as exc:
            raise SystemExit(f"pbrun: --after {text}: {exc}") from None
        edges.append({**edge, "kind": kind})
    return edges


def submit_deferred(prepared: Mapping[str, object],
                    args: argparse.Namespace, *, report=None):
    """File this submission for release once its producers succeed (#913).

    Everything ``prepare_submission`` checks has been checked: the checkout,
    the environment, the demand and the placement, which is announced and
    refused here exactly as for an ordinary submission.  What is left is
    what depends on the producer's bytes -- the data manifest, the key, the
    residency plan -- and the tiers loop's release does that.
    """

    report = report if report is not None else action_client.ClientReport()
    template = prepared["template"]
    d38_gate.refuse_deferred(args, template["params"])
    cas = template["cas"]
    q = pool.PoolQueue(SH / "pb-queue")
    edges = resolve_after_edges(q, Path(cas.root), args.after)
    try:
        action_edges.resolve_command(template["params"]["command"], "", "")
    except action_edges.ActionEdgeError as exc:
        raise SystemExit(f"pbrun: {exc}") from None
    require_releasable_template(template)
    static_input = None
    if args.data_manifest is not None:
        pb.load_data_manifest(args.data_manifest)
        static_input, _ = cas.ingest_input(
            args.data_manifest, input_id=pb.PBCAMPAIGN_DATA_MANIFEST_INPUT_ID)
        static, encoding = pb.read_data_manifest(cas.input_path(static_input))
        if encoding != "identity":
            raise SystemExit(
                "pbrun: a deferred submission's --data-manifest must be plain "
                "JSON: the release rewrites it with the committed batches")
        if static["schema"] == pb.DATA_MANIFEST_SCHEMA_V2:
            require_deferred_read_plan(static, edges, args=args)
        else:
            require_declared_origin_batches(
                static, transport=args.transport, queue_root=q.root)
            if (args.residency == "stage"
                    and not manifest_layout.manifest_phase_ranges(static)):
                raise SystemExit(
                    "pbrun: --residency stage needs the static --data-manifest "
                    "to declare its read order in phases; the release adds one "
                    "phase per committed batch after them")
    announce_placement(
        prepared["offer_queue"](), {"params": template["params"]}, args=args,
        cwd=prepared["cwd"], portable_checkout=prepared["portable_checkout"])
    body = action_edges.deferred_body(
        edges=edges, template=template_record(template), cas_root=cas.root,
        static_manifest=static_input,
        publication={name: getattr(args, name)
                     for name in _DEFERRED_PUBLICATION_ARGS})
    try:
        pending_id, path = action_edges.file_deferred(q.root, body)
    except action_edges.ActionEdgeError as exc:
        raise SystemExit(f"pbrun: cannot file the deferred submission: {exc}") from None
    if args.supersedes is not None:
        file_supersession_or_exit(q, args.supersedes, new=pending_id,
                                  new_kind=action_edges.PRODUCER_PENDING)
    producers = ", ".join(f"{edge['producer'][:12]}:{edge['template_id']}"
                          for edge in edges)
    print(f"pbrun: deferred {pending_id[:12]} until {producers} succeeds; the "
          f"tiers loop seals and publishes it then", file=sys.stderr, flush=True)
    if args.detach:
        code = report.finish(action_client.ClientDeferredSubmitted(pending_id, path,
            action_edges.published_path(q.root, pending_id)))
        print(sorted_json(action_client.reported_submission(report)), flush=True)
        return code
    # Returned, not run: ``main`` waits after the submission's used-
    # filesystem growth allowance is released (#1483).
    return functools.partial(await_release, q, pending_id, wait_s=args.wait_s,
                             report=report, cas_root=Path(cas.root))


def require_deferred_read_plan(static: Mapping[str, object],
                               edges: Sequence[Mapping[str, object]], *,
                               args: argparse.Namespace) -> None:
    """Refuse a deferred v2 read plan the release could not place (#946).

    The static plan names, under ``annotations.produced_output_slots``, the
    read phase each ``--after`` edge fills (`action_edges.after_slots`). The
    release puts the committed batches there, so what is checked here is
    everything a submission of the placed plan would be checked for that
    does not depend on those bytes: the published storage generation reads
    v2, every read phase is a progress phase in order, and with
    ``--residency stage`` the placed plan has a phase boundary to stage up
    to. That last holds unless the plan's first phase reads nothing and fills
    no slot, because a boundary at byte 0 ends no read.
    """

    if args.transport != "pool":
        raise SystemExit(
            f"pbrun: --after needs the pull queue; --transport {args.transport} "
            "has none")
    try:
        slots = action_edges.after_slots(static, edges)
    except action_edges.ActionEdgeError as exc:
        raise SystemExit(f"pbrun: --after: {exc}") from None
    require_deployed_read_plan_storage()
    require_linear_read_plan_progress(static, args.progress_policy)
    first = static["read_plan"]["phases"][0]
    if (args.residency == "stage" and not first["entry_indices"]
            and first["name"] not in {slot["phase"] for slot in slots}):
        raise SystemExit(
            f"pbrun: --residency stage: read phase {first['name']!r} opens the "
            "plan and reads nothing, so it ends no read to stage up to")


def require_releasable_template(template: Mapping[str, object]) -> None:
    """Refuse a deferred submission that no release could seal (#913).

    The tiers loop runs the published generation, and seals a deferred
    consumer into the generation its template was frozen under: that one,
    or a retained generation whose wrapper matches its receipt
    (``sealing_runtime``).  A template frozen by a ``pbrun`` outside the
    generation store, such as a development checkout's, is neither, and
    would be held until superseded.  Refuse it at submission instead.
    """

    prefix = action_edges.template_wrapper(template)
    try:
        published = (SH / "repo").resolve(strict=True)
    except OSError as exc:
        raise SystemExit(
            f"pbrun: --after: cannot read the published runtime: {exc}") from None
    if prefix == str(published / "tools"):
        return
    try:
        verify_retained_wrapper(prefix, where="--after")
    except (OSError, ValueError, pb.PrismaBuildError) as exc:
        raise SystemExit(
            f"pbrun: --after: no release could seal this submission ({exc}); "
            f"submit it with the published {SH / 'repo' / 'tools' / 'pbrun.py'}"
        ) from None


def await_release(q, pending_id: str, *, wait_s: float, report=None,
                  cas_root: Path | None = None) -> int:
    """Wait for a deferred submission's release, then for its action.

    The release record names the key and the generation the tiers loop
    published, which is what the ordinary wait is pinned to.  Waiting longer
    than ``wait_s`` in total exits ``GAVE_UP_EXIT``; the submission stays
    filed and is released whenever its producers succeed.
    """

    report = report if report is not None else action_client.ClientReport()
    deadline = time.monotonic() + max(0.0, float(wait_s))
    while True:
        try:
            published = action_edges.read_published(q.root, pending_id)
        except action_edges.ActionEdgeError as exc:
            print(f"pbrun: {exc}", file=sys.stderr, flush=True)
            return report.finish(action_client.ClientUnavailable(
                None, None, action_wait.WaitEnd.UNRETRIED))
        if published is not None:
            key = str(published["action_key"])
            generation = float(published['published_unix'])
            report.publish_identity(action_client.PoolClientIdentity(key, generation, q.root,
                cas_root if cas_root is not None else SH / 'cas', None,
                action_client.ClientArrival.SUBMITTED))
            print(f"pbrun: {pending_id[:12]} was released as {key[:12]}",
                  file=sys.stderr, flush=True)
            remaining = deadline - time.monotonic()
            # ``report_pool_outcome`` reads ``wait_s=0`` as "observe once", with the
            # full read budget.  A positive wait that ran out while the
            # release was being read is not that request: clamping it to 0
            # handed a caller past its deadline one more full read (#938).
            if wait_s > 0 and remaining <= 0:
                print(f"pbrun: the wait ended before {key[:12]}'s outcome was "
                      f"read; the action runs regardless. Run pbwait.py "
                      f"{key[:12]} to follow it", file=sys.stderr, flush=True)
                return report.finish(action_client.ClientPatienceExpired(key, generation))
            return action_client.report_pool_outcome(
                q, key, wait_s=max(0.0, remaining),
                generation=generation, report=report)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            print(f"pbrun: {pending_id[:12]} is still deferred; it stays filed "
                  f"and is released when its producers succeed",
                  file=sys.stderr, flush=True)
            return report.finish(action_client.ClientDeferredTimeout(pending_id))
        time.sleep(min(action_wait.POLL_S, remaining))


def sealing_runtime(template: Mapping[str, object]) -> dict[str, str]:
    """The generation a deferred template is sealed into, proved present (#913).

    It is the template's own: a consumer frozen by ``pbrun --after`` runs
    under the generation that froze it, as an ordinary submission sealed
    just before a publish does.  When that is this process's own generation,
    the caller has already checked it is the published one.  Otherwise it
    must be a retained generation whose wrapper matches its receipt, the
    check ``--as-sealed-by`` makes.  Raises
    ``action_edges.RuntimeGenerationUnavailable`` when it is not, so that
    nothing is ever sealed into a different generation instead.
    """

    prefix = action_edges.template_wrapper(template)
    if prefix == str(CONTAINER_WRAPPER_DIR):
        root = RUNTIME_ROOT
    else:
        try:
            root = verify_retained_wrapper(
                prefix, where="deferred release").parent
        except (OSError, ValueError, pb.PrismaBuildError) as exc:
            raise action_edges.RuntimeGenerationUnavailable(
                f"runtime-generation-unavailable: {exc}") from None
    return {"root": str(root), "generation": action_edges.generation_name(root)}


def release_deferred(q, pending_id: str, record: Mapping[str, object], *,
                     producers: Sequence[Mapping[str, object]] = ()
                     ) -> dict[str, object]:
    """Seal and publish one deferred consumer whose producers succeeded (#913).

    ``producers`` holds each edge's resolved ``{key, nonce, template_id}``.
    The first release pins the batch refs, the manifest and the key in a
    first-writer record before anything is published.  A release that finds
    that record resumes from it: it never resolves the producers again, and
    ``producers`` is not read.  After that it is ``main``'s own path: the CAS
    request, the origin-consumer declarations (#914), the window and the
    row.  A key the queue already carries -- published before a crash kept
    the last step from being recorded -- is not published again; its
    generation is recorded instead.  Last, the release records that
    generation, which is what waiters and chains read.

    Returns the event to log.  Raises ``ActionEdgeError`` (or ``SystemExit``
    from pbrun's own refusals) when the consumer cannot be released yet.
    """

    from prismabuild import produced_output as produced_mod

    cas_root = Path(str(record["cas_root"]))
    template = template_from_record(record["template"], cas_root=cas_root)
    cas = template["cas"]
    pinned = action_edges.read_release(q.root, pending_id)
    if pinned is None:
        static = None
        if record["static_manifest"] is not None:
            static, _ = pb.read_data_manifest(
                cas.input_path(record["static_manifest"]))
        if static is not None and static["schema"] == pb.DATA_MANIFEST_SCHEMA_V2:
            # Each edge's batches go where the plan reads them (#946).
            manifest = action_edges.place_after_slots(q.root, static, producers)
        else:
            refs: list[dict[str, object]] = []
            for producer in producers:
                refs.extend(action_edges.committed_batch_refs(
                    q.root, producer_key=str(producer["key"]),
                    nonce=str(producer["nonce"]),
                    template_id=str(producer["template_id"])))
            try:
                batches = produced_mod.origin_batch_manifest(q.root, refs)
            except produced_mod.ProducedOutputError as exc:
                raise action_edges.ActionEdgeError(str(exc)) from None
            manifest = action_edges.merged_manifest(static, batches)
        runtime = sealing_runtime(template)
        manifest_input, _ = cas.ingest_bytes(
            pb._canonical_file_bytes(manifest),
            input_id=pb.PBCAMPAIGN_DATA_MANIFEST_INPUT_ID)
    else:
        manifest_input = dict(pinned["manifest_input"])
        manifest, _ = pb.read_data_manifest(cas.input_path(manifest_input))
    summary = {"input": manifest_input, "mount_prefix": manifest["mount_prefix"],
               "entry_count": manifest["entry_count"],
               "total_bytes": manifest["total_bytes"]}
    if manifest["schema"] == pb.DATA_MANIFEST_SCHEMA_V2:
        # As ``freeze_action_template`` seals a submitted v2 plan.
        summary["schema"] = pb.DATA_MANIFEST_SCHEMA_V2
        summary["read_bytes"] = manifest["read_plan"]["read_bytes"]
    command = action_edges.resolve_command(
        template["params"]["command"], cas.input_path(manifest_input),
        str(manifest_input["sha256"]))
    action = seal_action_from_template(
        template, command=command, extra_inputs=[manifest_input],
        extra_params={"data_manifest": summary})
    key = str(action["action_key"])
    all_refs = list((manifest["annotations"] or {}).get(
        _ORIGIN_BATCHES_ANNOTATION) or [])
    if pinned is None:
        pinned = action_edges.file_release(
            q.root, pending_id, action_key=key,
            producers=[dict(item) for item in producers], refs=all_refs,
            manifest_input=manifest_input, runtime=runtime)
        resumed = False
    else:
        resumed = True
    if str(pinned["action_key"]) != key:
        raise action_edges.ActionEdgeError(
            f"release-key-mismatch: the pinned release names "
            f"{str(pinned['action_key'])[:12]}, and this runtime seals "
            f"{key[:12]}")
    match RecordStore(q.root).standing(key):
        case Present(QueueRow(state, existing)):
            pass
        case Present(Moving()):
            state, existing = "moving", None
        case Absent():
            state, existing = "absent", None
        case Unknown():
            state, existing = "unknown", None
    if state == "absent":
        if resumed:
            # About to publish: the generation must still be there, and the
            # pinned batches must still be the ones the release pinned.
            # ``lstat`` only, as at submission, unless an origin's timestamps
            # alone moved and its content must be read (#1111).
            sealing_runtime(template)
            try:
                produced_mod.load_origin_batches(q.root, all_refs)
            except produced_mod.ProducedOutputError as exc:
                raise action_edges.ActionEdgeError(str(exc)) from None
        options = argparse.Namespace(**dict(record["publication"]))
        # D38 again, now that the key exists: a record an older client filed
        # carries no evidence for it, and nothing is published without it.
        d38_gate.require(options, action, cas=cas, queue_root=q.root,
                         transport="pool")
        cas.publish_action_request(action)
        sealed = {**template,
                  "params": {**template["params"], "data_manifest": summary},
                  "inputs": [*template["inputs"], manifest_input]}
        with submission_window(q, key, all_refs):
            declare_origin_consumers(q, all_refs, consumer_action_key=key)
            _queued, generation = publish_consumer_row(
                q, action, sealed, key=key, args=options, cas=cas, attach=True)
    elif state == "unknown" or existing is None or isinstance(
            existing.get("published_unix"), bool) or not isinstance(
            existing.get("published_unix"), (int, float)):
        raise action_edges.ActionEdgeError(
            f"release-row-unreadable: {key[:12]} reads {state}")
    else:
        # Published before a crash kept this from being recorded: that row
        # is the release's generation, whatever became of it since.
        generation = float(existing["published_unix"])
    if generation is None:
        raise action_edges.ActionEdgeError(
            f"release-generation-unknown: {key[:12]} was published with no "
            "generation stamp")
    action_edges.file_published(q.root, pending_id, action_key=key,
                                published_unix=float(generation))
    return {"event": action_edges.RELEASED_EVENT, "pending_id": pending_id,
            "action_key": key, "published_unix": float(generation),
            "resumed": resumed, "runtime": dict(pinned["runtime"]),
            "producers": [dict(item) for item in pinned["producers"]],
            "refs": all_refs, "manifest_sha256": str(manifest_input["sha256"])}


def publish_consumer_row(q, action: Mapping[str, object],
                         template: Mapping[str, object], *, key: str,
                         args: argparse.Namespace, cas,
                         attach: bool = False,
                         before_publish=None) -> tuple[object, float | None]:
    """Publish one sealed action's row, with its window when it stages.

    Returns ``(queued_path, generation)``; ``queued_path`` is ``None`` when
    the queue was already carrying the key and this call attached to it.
    ``main`` and the deferred release (#913) are its two callers, so a
    released consumer is published exactly as a submitted one is.
    ``attach`` is the release's: a staged row it finds already live is the
    one it published before a crash, and it waits on that row instead of
    refusing.

    Everything the window will ever publish is sealed and written down before
    the consumer's own row goes in, so a crash between the two leaves a
    frozen plan and no queue rows rather than a half-published window.

    ``before_publish`` is called, with nothing fallible left but the publish
    itself, immediately before the row goes in.  A submission that supersedes
    an ended key files its immutable record there (#1585): every refusal the
    window's preparation can raise -- the stage tier, the phase table, the
    seal, the row's own checks -- comes first, so none can strand a record at
    a replacement that never published.
    """

    staged = None
    if args.residency == "stage":
        # One ownership transaction, under the consumer's existing transition
        # lock: handoff, seal and the consumer's publication are indivisible.  A dead consumer's cleanup rereads an old failed or
        # withdrawn terminal every cycle, and between ``freeze`` and the
        # consumer's own row it would see a filed plan nobody owns and reap
        # it.  ``residency_stage_rows``, ``freeze``, ``reap`` and ``publish``
        # all take this same lock and nest inside it (#708 review).
        with q._transition_locked(key):
            staged = residency_stage_rows(
                template, consumer_action_key=key,
                tier=resolve_stage_tier(q, args.residency_tier),
                args=args, queue=q, cas=cas)
            # A seal is a new generation of this consumer's window, and so is
            # a resubmission that reuses its frozen plan.  The predecessor's
            # *visible* child cancellations -- an operator's withdrawal, or
            # the dead-consumer pass that stopped a dead consumer's movers --
            # do not cover it, but the window reads them as live and would
            # supersede it before its second phase ever published.  So the
            # plan is filed first, which is what makes this consumer's
            # interest visible to the dead-consumer pass, and then the
            # predecessor's markers are retired as evidence under this
            # consumer's lock and every mover's (#708 review, #1114).  A
            # cancellation filed after that is the new plan's own decision
            # and still supersedes it; automatic publication never retires
            # one.  A release re-attaching to its own live row (#913) renews
            # nothing: that window already owns its markers.
            renew = not (attach and staged.get("reused_frozen_plan"))
            try:
                renewal = residency_plan.seal_window(
                    q, staged["plan"], renew=renew)
            except residency_plan.ResidencyPlanError as exc:
                raise SystemExit(f"pbrun: {exc}") from None
            if renewal["retired"]:
                print(
                    f"pbrun: renewing {key[:12]}: retired "
                    f"{len(renewal['retired'])} predecessor cancellation "
                    f"marker(s); their decisions stay under "
                    f"{q.superseded_dir()}", file=sys.stderr, flush=True)
            publication = publication_row(
                action, args=args, queue=q,
                tested_repository=template.get("tested_repository"))
            publication["residency"] = staged["residency"]
            if template.get("produced_output_template") is not None:
                publication["produced_output_template"] = template[
                    "produced_output_template"]
            if before_publish is not None:
                before_publish()
            if attach:
                # A release resuming after a crash (#913) finds its own row:
                # the plan above was first-writer and reused, and the row is
                # the one generation to wait on, not a second copy.
                queued_path, generation = publish_or_attach(
                    q, publication, key=key)
            else:
                # Not ``publish_or_attach``: a staged submission has already
                # frozen its window plan, which is first-writer and has its
                # own answer for a second seal of the same body, and the
                # duplicate #812 describes is a plain shard submission.
                queued_path = publish_or_refuse(q, publication)
                generation = published_generation(q, key, queued_path)
            # The consumer's row and nothing else.  Every phase of the
            # frozen plan is the tiers loop's to publish, the first included:
            # the loop adopts before it publishes, and its adoption pass skips
            # any leg whose row already exists, so a lead published here could
            # never be taken over from a range already on the tier.  One
            # publisher also means no interleaving to arbitrate.  A cold lead
            # therefore waits for the next cycle, which staged submissions
            # already depend on for phases 1..n.
            lead = staged["plan"]["phases"][0]
            print(f"pbrun: staging {len(staged['plan']['phases'])} phases onto "
                  f"{staged['plan']['tier_id']}; phase {lead['name']!r} "
                  f"({lead['stage_gib']} GiB) next, for the tiers loop to "
                  f"adopt or publish",
                  file=sys.stderr, flush=True)
    else:
        publication = publication_row(
            action, args=args, queue=q,
            tested_repository=template.get("tested_repository"))
        if template.get("produced_output_template") is not None:
            publication["produced_output_template"] = template[
                "produced_output_template"]
        if before_publish is not None:
            before_publish()
        queued_path, generation = publish_or_attach(q, publication, key=key)
    return queued_path, generation


def release_origin_consumer_cli(batch_ref: str, consumer_key: str, *,
                                reason: str, by: str) -> int:
    """``--release-origin-consumer``: release one declaration (#926, #945)."""

    from prismabuild import produced_output as produced_mod

    text = batch_ref
    if not batch_ref.lstrip().startswith("{"):
        try:
            text = Path(batch_ref).read_text()
        except OSError as exc:
            raise SystemExit(
                f"pbrun: --release-origin-consumer: BATCH_REF is neither JSON "
                f"nor a readable file: {exc}") from None
    try:
        ref = json.loads(text)
    except ValueError as exc:
        raise SystemExit(
            f"pbrun: --release-origin-consumer: BATCH_REF is not JSON: {exc}"
        ) from None
    q = pool.PoolQueue(SH / "pb-queue")
    # What an unpublished declaration's release compares against (#945): the
    # generation live now, and the CAS its sealed request sits in.  Unknown,
    # it refuses that case alone.
    try:
        live_runtime = (SH / "repo").resolve(strict=True)
    except OSError:
        live_runtime = None
    try:
        result = produced_mod.release_origin_consumer(
            q, ref, consumer_action_key=consumer_key, by=by, reason=reason,
            live_runtime=live_runtime, cas_root=SH / "cas")
    except (produced_mod.ProducedOutputError, OSError, ValueError) as exc:
        raise SystemExit(f"pbrun: --release-origin-consumer: {exc}") from None
    print(json.dumps({**result, "consumer_action_key": consumer_key,
                      "ref": ref}, sort_keys=True), flush=True)
    return 0


def main(*, publication_canary_intent=None, authorize_canary=None, report=None) -> int:
    report = report if report is not None else action_client.ClientReport()
    writer = None
    previous_observer = report.on_event
    with action_wait.client_readers():
        try:
            args = parse_args()
            fd = getattr(args, 'client_events_fd', None)
            if fd is not None:
                try:
                    writer = client_channel.ClientEventWriter(fd)
                except client_channel.ClientProtocolError as exc:
                    raise SystemExit(f'pbrun: {exc}') from None

                def send(event):
                    writer.send(event)
                    if previous_observer is not None:
                        previous_observer(event)

                report.on_event = send
            # Internal publisher handoff only; there is no public self-authorizing
            # switch. PoolQueue independently checks the publisher's exact-key grant.
            if publication_canary_intent is not None:
                if (not callable(authorize_canary) or args.transport != "pool"
                        or not args.detach or args.after or args.withdraw
                        or args.release_origin_consumer is not None):
                    raise SystemExit("pbrun: publication canary requires a bounded publisher handoff")
            if args.withdraw:
                # Withdrawing is not a submission and must not need one: the operator
                # cancelling four suites has no command to give and no checkout to
                # stamp, so this returns before any of the submit machinery runs.
                if [c for c in args.command if c != "--"]:
                    raise SystemExit("pbrun: --withdraw takes no command")
                try:
                    who = getpass.getuser()
                except Exception:                                        # noqa: BLE001
                    who = "unknown"      # no passwd entry is not a reason to refuse
                return report.finish(action_client.ClientCommand(withdraw_routed(
                    args.withdraw, transport=args.transport, reason=args.reason,
                    by=f"{who}@{socket.gethostname()}",
                )))
            if args.release_origin_consumer is not None:
                # Like a withdrawal, an operator's release is not a submission.
                if [c for c in args.command if c != "--"]:
                    raise SystemExit("pbrun: --release-origin-consumer takes no command")
                try:
                    who = getpass.getuser()
                except Exception:                                        # noqa: BLE001
                    who = "unknown"
                return report.finish(action_client.ClientCommand(release_origin_consumer_cli(
                    *args.release_origin_consumer, reason=args.reason,
                    by=f"{who}@{socket.gethostname()}")))
            # The used-filesystem floor (#1483): check what this submission writes,
            # and hold its growth allowance on the shared store while the snapshot,
            # CAS objects and queue record are written.  Released, whatever ends the
            # submission, before an attached pool or ``--after`` wait begins.  The
            # SLURM lane submits and waits in one call, so it holds no growth (its
            # used paths are still checked).  ``off`` (the default) reads only the
            # mode.
            try:
                with filesystem_floor.operation(
                        SH / "pb-queue", label="pbrun",
                        used_paths=[getattr(args, "cwd", None) or os.getcwd(), SH / "cas",
                                    SH / "pb-queue", tempfile.gettempdir()],
                        growth_gib=({SH / "cas": args.filesystem_growth_gib}
                                    if args.transport == "pool" else {})):
                    submitted = submit_and_publish(
                        args, publication_canary_intent=publication_canary_intent,
                        authorize_canary=authorize_canary, report=report)
            except filesystem_floor.FloorRefused as exc:
                raise SystemExit(f"pbrun: a used filesystem is below its floor; nothing "
                                 f"submitted: {exc}") from None
            code = submitted() if callable(submitted) else submitted
            if report.ending is None:
                raise RuntimeError('submission returned without a typed client ending')
            return code
        except OfferDiscoveryTimedOut:
            if report.ending is None:
                report.finish(action_client.ClientOfferTimeout())
            raise
        except SystemExit as exc:
            if report.ending is None:
                if isinstance(exc.code, int) and exc.code not in {0, 1, 2}:
                    raise
                code = (exc.code if isinstance(exc.code, int)
                        else 0 if exc.code is None else 1)
                if code == 0:
                    report.finish(action_client.ClientCommand(code))
                else:
                    identity = report.identity
                    report.finish(action_client.ClientRefused(code,
                        identity.action_key if identity is not None else None,
                        getattr(identity, 'published_unix', None)))
            raise
        finally:
            report.on_event = previous_observer
            if writer is not None:
                writer.close()


def submit_and_publish(args, *, publication_canary_intent=None,
                       authorize_canary=None, report=None):
    """Seal, publish and submit; an attached wait is returned, not run."""

    report = report if report is not None else action_client.ClientReport()
    prepared = prepare_submission(args)
    if args.after:
        return submit_deferred(prepared, args, report=report)
    template = prepared["template"]
    cwd = prepared["cwd"]
    portable_checkout = prepared["portable_checkout"]
    # The resolved half of the submission is the template's, not a second
    # copy kept alongside it: what was sealed is what the notices describe
    # and what the queue row is published with.
    tags = template["params"]["placement"]["required_tags"]
    demand = template["params"]["demand"]
    cas = template["cas"]
    action = (seal_action_from_template(
        template, extra_params={"publication_canary": publication_canary_intent})
        if publication_canary_intent is not None else seal_action_from_template(template))
    key = str(action["action_key"])

    if args.as_sealed_by is not None and key != args.as_sealed_by:
        raise SystemExit(
            f"pbrun: --as-sealed-by expected {args.as_sealed_by}, but current "
            f"work seals to {key}; nothing submitted. Restore the original "
            "checkout, command, inputs and options, or omit --as-sealed-by "
            "to request different work. A changed sealing contract can also "
            "prevent reproduction by this client."
        )

    placement_announced = False
    if publication_canary_intent is not None:
        # The placement verdict runs BEFORE the grant (#1278).  The grant is
        # one per runtime generation and host and is never replenished, so a
        # submission refused at placement after the mint would spend the
        # slot on work that never ran.
        announce_placement(
            prepared["offer_queue"](), action, args=args, cwd=cwd,
            portable_checkout=portable_checkout)
        placement_announced = True
        authorize_canary(action)
    request_path = cas.publish_action_request(action)

    cached = cas.lookup(action) if args.detach else None
    if cached is not None:
        identity = action_client.CachedClientIdentity(key, SH / 'pb-queue', Path(cas.root),
            action_client.ClientTransport(args.transport), cached['result']['sha256'])
        report.publish_identity(identity)
        # Nothing to submit and nothing to wait for.  The attached path lets
        # the worker discover this and file an ending, which is right when
        # somebody is holding the terminal open; detached, that ending would be
        # a job scheduled, a checkout materialized and a node occupied to learn
        # what this process already knows.  A campaign re-run is the case: every
        # row a hit, no new job ids.
        print(f"pbrun: {key[:12]} is already in the CAS; nothing submitted",
              file=sys.stderr, flush=True)
        print(sorted_json(action_client.detached_submission_record(
            key,
            transport=args.transport,
            status="cache_hit",
            queue_root=SH / "pb-queue",
        )), flush=True)
        return report.finish(action_client.ClientSubmitted(identity))

    if args.detach:
        # Not in the CAS, but perhaps already running: a campaign whose waiter
        # died is re-run to find out where it got to, and every row still on a
        # node must be attached to rather than submitted again.
        try:
            live = bounded_attachment(pool.PoolQueue(SH / "pb-queue"), key)
        except (action_wait.OutcomeReadUnavailable, OSError) as exc:
            print(f"pbrun: unavailable detached attachment for {key[:12]}: {exc}",
                  file=sys.stderr, flush=True)
            return report.finish(action_client.ClientUnavailable(
                key, None, action_wait.WaitEnd.UNRETRIED))
        if live is not None:
            transport, generation = live["transport"], live["generation"]
            record, job_id = live["submission"], live["job_id"]
            identity = (action_client.PoolClientIdentity(key, generation, SH / 'pb-queue',
                Path(cas.root), Path(record) if record is not None else None,
                action_client.ClientArrival.ATTACHED)
                if transport == 'pool' else action_client.SlurmClientIdentity(
                    key, generation, SH / 'pb-queue', Path(cas.root),
                    Path(record) if record is not None else None, job_id,
                    action_client.ClientArrival.ATTACHED))
            report.publish_identity(identity)
            print(f"pbrun: {key[:12]} is already running "
                  f"({transport}{' job ' + job_id if job_id else ''}); "
                  f"attaching to it rather than submitting a second copy",
                  file=sys.stderr, flush=True)
            print(sorted_json(action_client.detached_submission_record(
                key,
                transport=transport,
                status="attached",
                queue_root=SH / "pb-queue",
                published_unix=generation,
                job_id=job_id or None,
                submission=record,
            )), flush=True)
            return report.finish(action_client.ClientSubmitted(identity))

    # D38: new GPU work needs preflight evidence before anything runnable is
    # published, on every transport.  There is no liveness or cache exemption
    # here: a read taken now is stale by the time the queue decides whether the
    # key is new.  Without evidence this process publishes and submits nothing;
    # it may only wait on a pool run it can see, or refuse.  (The detached
    # branches above already answered a cache hit and a live attachment without
    # publishing.)
    verdict = d38_gate.judge_publication(
        args, action, cas=cas, queue_root=SH / "pb-queue",
        transport=args.transport)
    if verdict is not None:
        if not args.detach:
            try:
                live = bounded_attachment(pool.PoolQueue(SH / "pb-queue"), key)
            except (action_wait.OutcomeReadUnavailable, OSError):
                live = None
            if live is not None and live["transport"] == "pool":
                report.publish_identity(action_client.PoolClientIdentity(key, live['generation'],
                    SH / 'pb-queue', Path(cas.root),
                    Path(live['submission']) if live['submission'] is not None else None,
                    action_client.ClientArrival.ATTACHED))
                print(f"pbrun: {key[:12]} is already running on the pool; "
                      f"waiting on that run, and publishing nothing, because "
                      f"D38 evidence is missing", file=sys.stderr, flush=True)
                return functools.partial(
                    action_client.report_pool_outcome, pool.PoolQueue(SH / "pb-queue"), key,
                    wait_s=args.wait_s, generation=live["generation"], report=report)
        raise d38_gate.refusal_exit(verdict, action)

    if args.transport == "slurm":
        # Everything below this point reads the pull queue -- worker offers,
        # the placement census, the ready directory -- and none of it describes
        # a SLURM fleet.  Worse, it would answer *wrongly*: a retained offer
        # from a loop that has been stopped for the cutover would refuse a
        # submission the scheduler can place perfectly well.  The capability
        # check SLURM keeps is its own: an unknown Feature or an impossible
        # GRES makes ``sbatch`` refuse at submit time, which is the same moment
        # and the same intent as ``capability_verdict`` below.
        return slurm_outcome(
            action,
            cas=cas,
            request_path=request_path,
            # Built here because only ``main`` knows the checkout and the
            # flags it was asked with.  The census is unavailable rather than
            # empty: this branch builds no PoolQueue on purpose, and worker
            # offers do not describe a SLURM fleet.
            placement_notice=pin_notice(
                _NoCensus(),
                {"tags": tags, "needs_gpu": bool(demand.get("gpu")),
                 "resources": demand},
                cwd=cwd,
                hostname=socket.gethostname(),
                here=args.here,
                portable_checkout=portable_checkout,
                unknown_census=NO_CENSUS,
            ),
            tags=tags,
            demand=demand,
            exclusive=args.exclusive,
            timeout_s=args.timeout_s,
            wait_s=args.wait_s,
            retry_safe=args.retry_safe,
            max_attempts=args.max_attempts,
            priority=args.priority,
            priority_reason=args.priority_reason,
            anywhere=args.anywhere,
            detach=args.detach,
            report=report,
        )

    q = pool.PoolQueue(SH / "pb-queue")

    if not placement_announced:
        announce_placement(
            prepared["offer_queue"](), action, args=args, cwd=cwd,
            portable_checkout=portable_checkout)

    # Read the decision this submission is about to supersede, so the caller is
    # told rather than surprised.  ``publish`` retires the marker -- a key is a
    # content hash, so re-submitting one is how anybody asks for the same work
    # again -- and a submission that silently revived somebody's cancellation
    # would be as bad as the blacklist it replaced.
    superseding = None
    try:
        superseding = json.loads(
            q.item_path("withdrawn", key).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        superseding = None

    # A consumer of consumed produced-output batches is filed against them
    # before its row exists, so their retirement cannot run ahead of it.  A
    # crash after this and before the row leaves the batches held and
    # reported, never deleted (#914).  From the first declaration through
    # the row this process holds the key's transition lock, so an operator
    # cannot release the declaration while the row is still to come (#945).
    origin_refs = template.get(_ORIGIN_BATCHES_TEMPLATE_KEY) or []
    with submission_window(q, key, origin_refs):
        if origin_refs:
            declare_origin_consumers(q, origin_refs, consumer_action_key=key)
        # Filed before the row, so an edge that names the replaced key
        # follows this one from the moment it can run (#913), but only once
        # everything that can still refuse has had its say (#1585): the
        # record is immutable, and one filed for a replacement that then
        # refused named a key that never published, which the corrected
        # submission (another key) could not replace and a reader following
        # it could not find.  What can be refused now is refused now, with
        # nothing written; the record itself goes in right before the row.
        supersede = None
        if args.supersedes is not None:
            check_supersession_or_exit(q, args.supersedes, new=key,
                                       new_kind=action_edges.PRODUCER_KEY)

            def supersede() -> None:
                file_supersession_or_exit(q, args.supersedes, new=key,
                                          new_kind=action_edges.PRODUCER_KEY)

        queued_path, generation = publish_consumer_row(
            q, action, template, key=key, args=args, cas=cas,
            before_publish=supersede)
    identity = action_client.PoolClientIdentity(key, generation, q.root, Path(cas.root),
        queued_path, (action_client.ClientArrival.SUBMITTED if queued_path is not None
                      else action_client.ClientArrival.ATTACHED))
    report.publish_identity(identity)
    # Say that the slot has no device, every time.  The mask is correct and it
    # is also a silent narrowing: a suite that used to run its CUDA tests now
    # skips them, and a skip that nobody announced reads as the same green.
    if superseding is not None and queued_path is not None:
        # Only a publication retires a marker.  An attached duplicate
        # published nothing, so it superseded nothing.
        who = superseding.get("withdrawn_by") or "an operator"
        why = str(superseding.get("reason") or "").strip()
        print(f"pbrun: {key[:12]} had been withdrawn by {who}"
              f"{' -- ' + why if why else ''}; this submission supersedes that "
              f"decision", file=sys.stderr, flush=True)
    masked = "" if demand.get("gpu") else "  [no GPU: CUDA_VISIBLE_DEVICES='']"
    verb = "queued" if queued_path is not None else "attached to"
    # The one line that names the full key, so a caller can cite the action
    # rather than a prefix of it; ``pbtest`` records it per shard (#1012).
    # Every later line names it by prefix, which is what ``--withdraw`` takes.
    print(f"pbrun: {verb} {key} tags={tags} demand={demand}{masked} "
          f"published_unix={generation}",
          file=sys.stderr, flush=True)

    if args.detach:
        if queued_path is None:
            # The same display path ``_attachment_value`` names, read once:
            # this process started nothing, so it reports where the run it
            # joined lives rather than a submission of its own.
            ready = q.item_path(pool.READY, key)
            display_path = ready if ready.exists() else q.item_path(pool.CLAIMED, key)
            print(sorted_json(action_client.detached_submission_record(
                key,
                transport="pool",
                status="attached",
                queue_root=q.root,
                published_unix=generation,
                submission=display_path,
            )), flush=True)
            return report.finish(action_client.ClientSubmitted(identity, display_path))
        print(sorted_json(action_client.detached_submission_record(
            key,
            transport="pool",
            status="submitted",
            queue_root=q.root,
            published_unix=generation,
            submission=queued_path,
        )), flush=True)
        return report.finish(action_client.ClientSubmitted(identity))

    return functools.partial(action_client.report_pool_outcome, q, key, wait_s=args.wait_s,
                             generation=generation, report=report)


if __name__ == "__main__":
    raise SystemExit(main())
