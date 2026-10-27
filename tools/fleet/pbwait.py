"""Wait for actions somebody already submitted, and print one table of endings.

``pbrun --detach`` submits and returns.  This is the other half: given the keys
it printed, block until each one's ending is filed, then say in one table what
happened to all of them.  It exits 0 only if every key ended with the work
done.

Three things follow from where the endings come from.

*A wait is for one run of the work.*  An action key is a content hash, so one
key accumulates the terminal record of every run of the same work.  Which run
is meant is read from the newest submission anybody recorded -- the SLURM
lane's ``latest.json`` or the pull queue's own item -- and a record of another
generation is not this run's answer.

*Under SLURM the waiter files the ending.*  ``slurm_lane.run`` writes the
terminal record because it is the process holding the submission open; a
detached submission has no such process.  So this resumes the recorded job,
waits on it, and files what ``run`` would have filed, through
``slurm_lane.resume``.  Under the pull queue the worker files it, and this only
watches.

*The CAS is the authority on whether work was done.*  A key with a receipt and
no submission outstanding ended as a cache hit, whichever transport originally
delivered it, and that is reported as ``cas`` rather than as a transport.

    pbwait.py 8fc86da0e13f 4b19a02cc551
    pbwait.py --wait-s 3600 $(cut -d'"' -f4 keys.jsonl)
"""
from __future__ import annotations

import argparse
import concurrent.futures
import json
import math
import os
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
_PB_ENTRYPOINT = Path(__file__).resolve(strict=True)
sys.path.insert(0, str(_PB_ENTRYPOINT.parents[
    2 if _PB_ENTRYPOINT.parent.name == "fleet" else 1] / "src"))
from prismabuild.runtime_paths import generation_root  # noqa: E402

RUNTIME_ROOT = generation_root(__file__)
sys.path.insert(0, str(RUNTIME_ROOT / "src"))
from prismabuild import action_result, action_client  # noqa: E402
from prismabuild.submission_frontend import store_for, render_submission
from prismabuild.submission_types import SubmissionRequest, SubmissionRefused


from prismabuild import action_wait, endings  # noqa: E402
from prismabuild import core as pb, pool, slurm_lane  # noqa: E402

from prismabuild import submission
from prismabuild import submission

#: What this exits with when it stopped waiting before the work finished.
#: ``pbrun`` spells it the same way and means the same thing by it: the work is
#: still running.
GAVE_UP_EXIT = action_client.GAVE_UP_EXIT

#: What this exits with when the key an operator typed names nothing this can
#: wait on.  ``pbrun --withdraw`` exits 2 for the same two refusals, and the
#: operating guide's exit-code table says ``pbrun`` and ``pbwait`` use the same
#: codes.  Exit 1 is reserved for "the action failed", so a wrapper that reads
#: a 1 here mistakes a typo for a build that ran and lost.
MISNAMED_EXIT = 2

#: How wide a key prints.  Twelve characters is what every fleet log line
#: shows, so it is what an operator has to compare against.
KEY_WIDTH = 12

#: A prefix is only a convenience for an already recorded action. Resolving it
#: must not leave the caller in a hard shared-filesystem operation before the
#: actual wait has begun. This is separate from ``--wait-s``.
PREFIX_RESOLUTION_READ_TIMEOUT_S = 5.0

_COLUMNS = (
    ("key", "key"),
    ("status", "status"),
    ("transport", "transport"),
    ("job", "job"),
    ("host", "host"),
    ("elapsed", "elapsed"),
    ("returncode", "rc"),
    ("receipt", "receipt"),
    ("note", "note"),
)


# --------------------------------------------------------------------------
# Finding the run
# --------------------------------------------------------------------------

def _misnamed(message: str) -> SystemExit:
    """Refuse a key with the code the exit-code table gives a misnamed one.

    The message goes to stderr rather than to ``SystemExit``, because a
    ``SystemExit`` carrying a string exits 1 and prints the string, and 1 is
    the code for an action that failed.  Both halves are needed: the operator
    reads the message, and the wrapper reads the 2.
    """

    print(message, file=sys.stderr)
    return SystemExit(MISNAMED_EXIT)


def _prefix_candidates(q, text: str, *, lane_root=None) -> list[str]:
    """Read recorded prefix namespaces in the isolated bounded reader.

    Missing optional directories are empty namespaces. Other errors escape to
    the parent, which refuses rather than treating a partial census as a
    unique or absent prefix.
    """

    found = {
        str(record["action_key"])
        for record in slurm_lane.resolve_recorded(
            text, root=lane_root, report_unavailable=True)
    }
    found.update(q.records.wait_prefix(text))
    return sorted(found)


def _prefix_unavailable(message: str) -> SystemExit:
    print(f"pbwait: prefix resolution unavailable: {message}", file=sys.stderr)
    return SystemExit(action_client.RECORD_WRITE_FAILED_EXIT)


def resolve_key(q, name: str, *, lane_root=None) -> str:
    """Turn what an operator has into the key the records are filed under.

    A full digest is taken as given, because a key that nothing has recorded
    yet is exactly the case this tool has to be able to wait on.  A prefix has
    to resolve against something already recorded -- there is nothing else to
    resolve it against -- and an ambiguous one is refused rather than guessed.
    """

    text = str(name or "").strip().lower()
    if len(text) == 64:
        return text
    if not text:
        raise _misnamed("pbwait: an empty key resolves to nothing")
    try:
        found = set(action_wait.read_observation(
            "pbwait prefix resolution",
            lambda: _prefix_candidates(q, text, lane_root=lane_root),
            budget_s=PREFIX_RESOLUTION_READ_TIMEOUT_S,
        ))
    except (action_wait.OutcomeReadUnavailable, OSError) as exc:
        raise _prefix_unavailable(str(exc)) from None
    if not found:
        raise _misnamed(
            f"pbwait: nothing recorded matches {name!r}; a prefix can only be "
            "resolved against a submission or an ending that already exists, "
            "so name the whole key for work that may not be submitted yet"
        )
    if len(found) > 1:
        listed = ", ".join(sorted(key[:KEY_WIDTH] for key in found))
        raise _misnamed(
            f"pbwait: {name!r} matches {len(found)} actions ({listed}); "
            "name more characters"
        )
    return found.pop()


#: ``pbrun`` reads the same submissions to decide whether to attach to a run
#: instead of starting a second copy of it, so the reading lives there and this
#: is the waiter's name for it.
outstanding = submission.outstanding_submission


def recorded_action(cas, key: str):
    """The sealed action ``pbrun`` published for this key, or ``None``.

    Absent means the CAS was cleared or the key was never submitted from here.
    That is a missing lookup, not a failure: it only costs the ability to say
    ``cache_hit`` without a terminal record.
    """

    return cas.read_action_request(key)


def _bounded_observation_value(q, key: str, cas, generation, *, lane_root=None, bounded_archives=False):
    """Read every non-mutating fact one ``pbwait`` pass needs in its child."""

    found = outstanding(q, key, lane_root=lane_root)
    if generation is None and found is not None:
        generation = found[1]
    effective_generation = generation
    try:
        landed, next_generation = action_wait.outcome_poll(
            q, key, generation, report_unreadable=True, bounded_archives=bounded_archives)
        outcome = {
            "generation": next_generation,
            **action_client.encode_outcome_ending(landed),
        }
    except action_wait.UnreadableTerminal as exc:
        # Keep the already selected generation with an unreadable record. The
        # parent needs it to decide whether this is the recorded SLURM run it
        # may repair; pbrun's public helper intentionally only needs the error.
        outcome = {"generation": effective_generation,
                   "unreadable": {"path": str(exc.path), "reason": exc.reason}}
    # An ending is the first answer.  In particular, do not let a later CAS
    # payload verification hide an already-filed (or unreadable) terminal
    # record, and do not spend that work when no SLURM recovery needs it.
    if outcome.get("landed") is not None or outcome.get("superseded") is not None:
        action = None
        receipt = None
    elif (effective_generation is not None
          and outcome.get("generation") != effective_generation):
        # ``outcome_poll`` named a preemption successor. Follow that exact
        # lineage before asking CAS about the stopped generation: a slow CAS
        # lookup must not hide a successor that has already landed.
        action = None
        receipt = None
    elif outcome.get("unreadable") is not None:
        # A SLURM waiter can heal a malformed old terminal through its parent
        # resume. It needs the sealed request, but not a receipt lookup that
        # could hide the already-filed unreadable record.
        action = (
            recorded_action(cas, key)
            if found is not None and found[0] == "slurm"
            and found[1] == effective_generation else None
        )
        receipt = None
    else:
        action = recorded_action(cas, key)
        receipt = None if action is None else cas.lookup(action)
    return {
        "found": None if found is None else {
            "transport": found[0], "generation": found[1],
            "submission": found[2],
        },
        "outcome": outcome,
        "observed_generation": effective_generation,
        "action": action,
        "receipt_published": receipt is not None,
    }


def bounded_observation(q, key: str, cas, generation, *, lane_root=None, budget_s=None, bounded_archives=False):
    """Return one verified read-only wait snapshot without blocking its parent.

    The child only reads the submission namespaces, terminal records, immutable
    attempt evidence, sealed request, and CAS receipt/payload.  In particular,
    it never resumes a SLURM job or files an ending.  The generation selected
    by a preemption handoff returns to the parent, which keeps that exact
    identity on every later observation.
    """

    value = action_wait.read_observation(
        "pbwait observation",
        lambda: _bounded_observation_value(
            q, key, cas, generation, lane_root=lane_root, bounded_archives=bounded_archives),
        budget_s=(action_wait.OUTCOME_READ_TIMEOUT_S if budget_s is None else budget_s),
    )
    if not isinstance(value, dict):
        raise action_wait.OutcomeReadUnavailable(
            "pbwait observation returned an invalid payload")
    raw_found = value.get("found")
    if raw_found is None:
        found = None
    elif (isinstance(raw_found, dict)
          and raw_found.get("transport") in {"pool", "slurm"}
          and isinstance(raw_found.get("generation"), (int, float))
          and not isinstance(raw_found.get("generation"), bool)
          and isinstance(raw_found.get("submission"), dict)):
        found = (
            raw_found["transport"], float(raw_found["generation"]),
            raw_found["submission"],
        )
    else:
        raise action_wait.OutcomeReadUnavailable(
            "pbwait observation returned an invalid submission")
    raw_outcome = value.get("outcome")
    if not isinstance(raw_outcome, dict):
        raise action_wait.OutcomeReadUnavailable(
            "pbwait observation returned an invalid outcome")
    unreadable = raw_outcome.get("unreadable")
    if unreadable is not None:
        if (not isinstance(unreadable, dict)
                or not isinstance(unreadable.get("path"), str)
                or not isinstance(unreadable.get("reason"), str)):
            raise action_wait.OutcomeReadUnavailable(
                "pbwait observation returned an invalid unreadable ending")
        unreadable = Path(unreadable["path"]), unreadable["reason"]
    next_generation = raw_outcome.get("generation")
    if (next_generation is not None
            and (not isinstance(next_generation, (int, float))
                 or isinstance(next_generation, bool))):
        raise action_wait.OutcomeReadUnavailable(
            "pbwait observation returned an invalid generation")
    landed = action_client.decode_outcome_ending(q, key, next_generation, raw_outcome)
    action = value.get("action")
    if action is not None and not isinstance(action, dict):
        raise action_wait.OutcomeReadUnavailable(
            "pbwait observation returned an invalid sealed action")
    receipt_published = value.get("receipt_published")
    if not isinstance(receipt_published, bool):
        raise action_wait.OutcomeReadUnavailable(
            "pbwait observation returned an invalid receipt result")
    observed_generation = value.get("observed_generation")
    if (observed_generation is not None
            and (not isinstance(observed_generation, (int, float))
                 or isinstance(observed_generation, bool))):
        raise action_wait.OutcomeReadUnavailable(
            "pbwait observation returned an invalid observed generation")
    if isinstance(landed, endings.PublicationSupersession):
        expected = generation if generation is not None else found[1] if found else None
        if next_generation != observed_generation or next_generation != expected:
            raise action_wait.OutcomeReadUnavailable("publication supersession changed the watched generation")
    return found, landed, (
        float(next_generation) if next_generation is not None else None
    ), action, receipt_published, unreadable, (
        float(observed_generation) if observed_generation is not None else None
    )


# --------------------------------------------------------------------------
# Waiting for one
# --------------------------------------------------------------------------

def _row(key: str, status: str, **fields) -> dict:
    row = {
        "action_key": key, "status": status, "transport": "-", "job": "-",
        "host": "-", "elapsed_s": None, "returncode": None,
        "action_returncode": None, "action_signal": None,
        "receipt_published": None, "note": None, "phases": {},
        "succeeded": status in {"executed", "cache_hit"},
    }
    row.update(fields)
    return row


def _job_id(found) -> str:
    """The job id the submission recorded, or ``-``.

    On a waiting row this is the whole of what an operator can take to
    ``squeue`` or ``sacct``, and it was printed only after the ending landed:
    the rows that named no job were exactly the rows somebody was reading
    because they wanted to go and look.
    """

    if found is None:
        return "-"
    submission = found[2] if isinstance(found[2], dict) else {}
    return str(submission.get("job_id") or "-")


def _from_ending(q, ending: endings.ObservedEnding, *, context=None) -> dict:
    if context is not None:
        context.clear()
    if isinstance(ending, endings.PublicationSupersession):
        def retained_publication_phases():
            sources = (q.records.attempt_documents(ending.action_key, generation=ending.published_unix),
                       q.records.superseded_records(ending.action_key, fresh=True))
            for records in sources:
                for _, record in records:
                    if (isinstance(record, dict)
                            and record.get("action_key") == ending.action_key
                            and endings.publication_generation(record.get("published_unix")) == ending.published_unix
                            and isinstance(record.get("phases"), dict)):
                        return record["phases"]
            return {}
        try:
            phases = action_wait.read_observation("publication timing", retained_publication_phases,
                budget_s=action_wait.OUTCOME_READ_TIMEOUT_S)
        except (action_wait.OutcomeReadUnavailable, OSError, pool.PoolContractError):
            phases = {}
        if context is not None:
            context.update({"claimed": ending.record.get("claimed_unix") is not None,
                            "published_unix": ending.published_unix})
        return _row(
            ending.action_key, "superseded", transport="pool",
            published_unix=ending.published_unix,
            phases=phases,
            superseded_by_published_unix=ending.successor_published_unix,
            note=(f"publication {ending.published_unix!r} superseded by "
                  f"publication {ending.successor_published_unix!r}; no execution verdict"),
        )
    outcome_path, outcome = ending
    if context is not None:
        # The consumer's result reader authenticates the selected attempt below.
        # Legacy rendering expands whole logs and must not precede that read.
        rendered = {"summary": action_client.outcome_summary(
            q, outcome_path, outcome, verify_attempts=False)}
    else:
        rendered = action_client.bounded_outcome_render(
            q, outcome_path, outcome, budget_s=action_wait.OUTCOME_READ_TIMEOUT_S,
            diagnose_conflicts=True)
    diagnostic = rendered.get("integrity_error")
    if diagnostic is not None:
        return _row(
            diagnostic["action_key"], "record_error", integrity_error=diagnostic,
            note=("terminal summary conflicts with verified immutable attempt; "
                  f"fields={','.join(diagnostic['mismatched_fields'])}; "
                  f"immutable attempt={diagnostic['attempt_path']}; "
                  "use --json for verified log references; not an action or CAS verdict"),
        )
    summary = rendered["summary"]
    scheduler = summary["detail"].get("slurm") or {}
    if context is not None:
        context.update({
            "ending": (outcome_path, outcome),
            "published_unix": outcome.get("published_unix"),
            "attempt": (outcome.get("withdrawn_attempt") or outcome).get("attempts"),
            "started_unix": outcome.get("claimed_unix"),
            "ended_unix": outcome.get("finished_unix"),
            "reason": summary.get("reason") or summary["detail"].get("error")
                      or summary["detail"].get("message"),
            "result_ended": summary["status"] != "withdrawn" or summary["withdrawn_attempt"] is not None,
            "withdrawn_from": outcome.get("withdrawn_from"),
            "claimed": (outcome.get("claimed_unix") is not None
                        or bool(summary["claimed_host"]) or outcome.get("withdrawn_from") == "claimed"),
            "scope_required": any(outcome.get(field) is not None
                                  for field in ("resource_scope", "resource_scope_intent")),
            "containers_required": outcome.get("container_owner") is not None,
            "resource_profile": summary.get("resource_profile"),
            "profile": summary.get("profile"),
        })
        for field in ("resource_scope_cleanup", "container_cleanup"):
            value = summary[field]
            context[field] = value if value is not None else summary["detail"].get(field)
    return _row(
        summary["action_key"] or "",
        summary["status"],
        transport=summary["transport"],
        phases=outcome.get("phases", {}),
        # What an operator needs to read the logs the record names: under SLURM
        # that is the job id, and under the pull queue there is no such handle
        # -- the worker ran it in a process that is gone.
        job=str(scheduler.get("job_id") or "-"),
        # Where the work was, not who wrote the record.  The reaper files
        # most of the fleet's lost claims under its own ``finished_host``, so
        # a waiter that printed that field told the operator the failure
        # belonged to whichever box swept (#262).  ``outcome_summary``
        # already recovers ``claimed_host`` from the immutable attempt when
        # the terminal record does not carry it, so the fallback here is
        # reached only by an ending that names no claimant anywhere.
        host=summary["claimed_host"] or summary["finished_host"] or "-",
        elapsed_s=summary["elapsed_s"],
        returncode=summary["returncode"],
        action_returncode=summary["action_returncode"],
        action_signal=summary["action_signal"],
        receipt_published=summary["receipt_published"],
        succeeded=summary["succeeded"],
        withdrawn_attempt=summary["withdrawn_attempt"],
        resource_scope_cleanup=summary["resource_scope_cleanup"],
        container_cleanup=summary["container_cleanup"],
    )


def wait_one(
    q,
    key: str,
    *,
    cas,
    deadline: float,
    generation: float | None = None,
    lane_root=None,
    queue_root=None,
    consumer=False,
    until="ended",
    **lane_commands,
) -> dict:
    """Await one row with the detached profile and its shared absolute deadline."""
    lane_commands.setdefault(
        "on_notice",
        lambda text: print(f"pbwait: {text}", file=sys.stderr, flush=True))
    context = {"generation": generation, "found": None, "action": None,
               "receipt_published": False, "handoff": False}
    ending_context = {} if consumer else None

    def present(row):
        return consumer_ending_row(q, row, cas=cas, context=ending_context) if consumer else row

    def waiting_row():
        found = context['found']
        return _row(key, 'waiting', transport=found[0] if found else '-', job=_job_id(found),
                    phases=found[2].get('phases', {}) if found else {})

    def observe(*, budget_s, generation, remaining_s):
        context['generation'] = generation
        try:
            row = _look_once_checked(
                q, key, cas=cas, remaining_s=remaining_s, budget_s=budget_s,
                state=context, lane_root=lane_root, queue_root=queue_root,
                ending_context=ending_context, **lane_commands)
        except (action_wait.OutcomeObservationTimedOut, action_wait.OutcomeReaderRetained) as exc:
            return action_wait.ReadFault(exc, context['generation'], diagnostic=waiting_row())
        except (action_wait.UnreadableTerminal, action_wait.OutcomeReadUnavailable,
                pool.PoolContractError, OSError, slurm_lane.SlurmLaneError) as exc:
            return action_wait.Ready(
                present(_observation_error_row(key, exc, waiting=waiting_row())), context['generation'])
        selected = context['generation']
        if row is not None:
            row = present(row)
            if consumer and until == "cleaned" and row["status"] != "unreadable" \
                    and not row["diagnostics"]["cleanup"]["complete"]:
                return action_wait.Pending(selected, diagnostic=row)
            return action_wait.Ready(row, selected)
        handoff = context['handoff']
        context['handoff'] = False
        return action_wait.Pending(selected, handoff, present(waiting_row()))

    def notice(event):
        reaped = ('the reader has exited and was reaped, '
                  if event.reader_reaped else '')
        print(f"pbwait: {key[:12]} not observed yet ({event.fault.error}); "
              f"{reaped}{event.count} consecutive unavailable read(s), "
              "retrying inside --wait-s", file=sys.stderr, flush=True)

    result = action_wait.await_action(
        observe, generation=generation, patience=action_wait.WaitUntil(deadline),
        profile=action_wait.AwaitProfile.DETACHED,
        tool='pbwait', subject=key[:12], notice=notice,
        bell=action_wait.queue_bell.Client(q.root), bell_key=key,
        use_bell=lambda: context['found'] is not None and context['found'][0] == 'pool')
    if isinstance(result, action_wait.Ready):
        return result.value
    if isinstance(result, action_wait.Unreadable):
        return present(_row(key, 'unreadable', note=str(result.error)))
    if isinstance(result, action_wait.Unavailable):
        waiting = result.fault.diagnostic
        if waiting is None:
            waiting = _row(key, 'waiting')
        row = _observation_error_row(key, result.fault.error, waiting=waiting)
        if result.ending is action_wait.WaitEnd.RETAINED:
            still = [child.to_record() for child in result.retained]
            row = dict(row, retained_readers=still)
            if result.had_patience:
                if still:
                    ending = (f"that reader was still retained when the wait "
                              f"ended, {result.waited_s:.1f}s after its read timed out "
                              f"(retained reader={json.dumps(still, sort_keys=True)}), "
                              "so no second reader was started beside it")
                else:
                    ending = ("its reader was reaped when the wait ended, with "
                              "no time left to read again")
                row['note'] = (
                    f"{row['note']}; {ending}, after {result.count} "
                    "consecutive unavailable read(s). The action may still be "
                    "running or may already have landed")
        elif result.ending is action_wait.WaitEnd.DEADLINE and result.had_patience:
            row = dict(row, note=(
                f"{row['note']}; still unavailable when the wait ended after "
                f"{result.count} consecutive unavailable read(s). The "
                "action may still be running or may already have landed"))
        return present(row)
    return result.diagnostic if result.diagnostic is not None else present(_row(key, 'waiting'))


def _observation_error_row(key, exc, *, waiting):
    """Render an observation failure using only its already captured context."""
    if isinstance(exc, action_wait.UnreadableTerminal):
        return _row(key, 'unreadable', note=str(exc))
    if isinstance(exc, action_wait.OutcomeObservationTimedOut):
        return _row(key, 'record_error', note=str(exc), observation_timed_out=True)
    if isinstance(exc, action_wait.OutcomeReaderRetained):
        return _row(key, 'record_error', note=str(exc), retained_readers=exc.retained)
    if isinstance(exc, action_wait.RetainedIdentityUnavailable):
        return _row(key, 'record_error', note=str(exc), unresolved_readers=exc.records)
    if isinstance(exc, (pool.PoolContractError, action_wait.OutcomeReadUnavailable)):
        return _row(key, 'record_error', note=str(exc))
    job_id = str(getattr(exc, 'job_id', '') or waiting['job'])
    print(f"pbwait: {exc}; retry tools/fleet/pbwait.py {key[:12]}", file=sys.stderr)
    return _row(key, 'record_error', transport=waiting['transport'], job=job_id, note=str(exc))


def _look_once_checked(
    q,
    key: str,
    *,
    cas,
    remaining_s,
    budget_s=None,
    generation: float | None = None,
    state=None,
    lane_root=None,
    queue_root=None,
    ending_context=None,
    **lane_commands,
):
    """One pass of ``wait_one``, or ``None`` when there is nothing yet.

    ``None`` is the one answer that means "look again": no submission is
    recorded, no ending is filed, and the CAS holds no receipt.  Every other
    answer is a row, because every other answer is about a run this can name.

    A wait can begin before submission exists. Once a publication is selected,
    later publications cannot replace its generation. Receipts are checked
    before asking SLURM; this waiter may be the only process that can file a
    completed scheduler job's terminal record. Resume stays in the parent,
    after the bounded read has ended, with freshly measured remaining patience.
    """

    previous_generation = generation
    if state is not None:
        generation = state["generation"]
        previous_generation = generation
    found, landed, generation, action, receipt, unreadable, observed_generation = bounded_observation(
        q, key, cas, generation, lane_root=lane_root, budget_s=budget_s,
        bounded_archives=ending_context is not None)
    if state is not None:
        state["generation"] = generation
        state["found"] = found
        state["action"] = action
        state["receipt_published"] = receipt
        state["handoff"] = (
            observed_generation is not None and generation != observed_generation
        )
    slurm_run = (found is not None and found[0] == "slurm"
                 and found[1] == generation)

    if landed is not None:
        return _from_ending(q, landed, context=ending_context)
    if unreadable is not None:
        if slurm_run and action is not None:
            # The lane owns recovery of its own missing/corrupt terminal
            # record. This mutation remains in the durable waiter parent;
            # only the observations around it are disposable children.
            slurm_lane.resume(
                found[2], action=action, cas=cas,
                queue_root=q.root if queue_root is None else queue_root,
                wait_s=0.0, **lane_commands,
            )
            landed, generation = action_client.bounded_outcome_observation(
                q, key, generation, budget_s=action_wait.OUTCOME_READ_TIMEOUT_S)
            if state is not None:
                state["generation"] = generation
            if landed is not None:
                return _from_ending(q, landed, context=ending_context)
        return _row(
            key, "unreadable", transport=found[0] if found else "-",
            job=_job_id(found), note=f"{unreadable[1]}: {unreadable[0]}",
        )

    if receipt and not (found is not None and found[0] == "pool"):
        # The CAS is asked before the controller, and it outranks it.  A
        # receipt says the work was done whatever the scheduler goes on to
        # say -- and after ``MinJobAge`` the scheduler says nothing at all,
        # having forgotten a job that ran perfectly well.  Waiting on a
        # forgotten job for a verdict already in hand is the whole of
        # ``--wait-s`` spent to learn nothing.
        if slurm_run:
            # The ending is still missing, and every reader of ``pb-queue``
            # expects one.  ``resume`` with no patience polls the controller
            # once for provenance, then files from the receipt either way.
            slurm_lane.resume(
                found[2], action=action, cas=cas,
                queue_root=q.root if queue_root is None else queue_root,
                wait_s=0.0, **lane_commands,
            )
            landed, generation = action_client.bounded_outcome_observation(
                q, key, generation, budget_s=action_wait.OUTCOME_READ_TIMEOUT_S)
            if state is not None:
                state["generation"] = generation
            if landed is not None:
                return _from_ending(q, landed, context=ending_context)
        # Nothing outstanding, or nothing that could file: the work is done
        # and was memoized.  Which transport delivered it does not enter into
        # it, which is the property the CAS exists to give.
        return _row(key, "cache_hit", transport="cas", receipt_published=True)

    if slurm_run:
        if action is None:
            return _row(
                key, "unreadable", transport="slurm",
                job=_job_id(found),
                host=str(found[2].get("submitted_host") or "-"),
                note="no sealed action in the CAS to resume the job with",
            )
        # Under SLURM nobody else will file this ending: the submitter
        # detached.  Resuming the recorded job is what makes the record appear
        # for every reader that expects one, this table included.
        slurm_lane.resume(
            found[2],
            action=action,
            cas=cas,
            queue_root=q.root if queue_root is None else queue_root,
            wait_s=remaining_s(),
            **lane_commands,
        )
        landed, generation = action_client.bounded_outcome_observation(
            q, key, generation, budget_s=action_wait.OUTCOME_READ_TIMEOUT_S)
        if state is not None:
            state["generation"] = generation
        if landed is not None:
            return _from_ending(q, landed, context=ending_context)
        return _row(key, "waiting", transport="slurm",
                    job=_job_id(found),
                    host=str(found[2].get("submitted_host") or "-"))

    if found is None:
        # Nothing has been recorded under this key at all.  Say so by
        # returning nothing, so the caller looks again for a submission as
        # well as for an ending.
        return None

    # A pull-queue item: the worker files the ending.  ``wait_one`` owns the
    # deadline and polling interval, while each following pass repeats the
    # bounded observation above.
    return None


def wait_for_keys(
    q, keys, *, cas, wait_s: float, generations=None, lane_root=None,
    queue_root=None, **lane_commands,
) -> list[dict]:
    """Wait for every key at once, under one deadline, and return their rows.

    In threads rather than in turn: a key's wait is a poll of the filesystem
    and, under SLURM, of the controller, so twenty of them cost one deadline
    instead of twenty.  A serial loop would also report the last key's ending
    only after the first key's, which for a campaign is the whole wall-clock.
    """

    unique = list(dict.fromkeys(str(key) for key in keys))
    if not unique:
        return []
    stamped = dict(generations or {})
    deadline = action_wait.time.monotonic() + float(wait_s)
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=min(32, len(unique))
    ) as pens:
        futures = {
            pens.submit(
                wait_one, q, key, cas=cas, deadline=deadline,
                generation=stamped.get(key),
                lane_root=lane_root, queue_root=queue_root, **lane_commands,
            ): key
            for key in unique
        }
        rows = {futures[future]: future.result()
                for future in concurrent.futures.as_completed(futures)}
    return [rows[key] for key in unique]


# --------------------------------------------------------------------------
# Saying what happened
# --------------------------------------------------------------------------

def _cell(row: dict, field: str) -> str:
    if field == "key":
        return str(row["action_key"])[:KEY_WIDTH] or "-"
    if field == "elapsed":
        value = row.get("elapsed_s")
        return f"{float(value):.1f}s" if isinstance(value, (int, float)) else "-"
    if field == "returncode":
        value = row.get("returncode")
        if not isinstance(value, int):
            return "-"
        # The launcher's status is 1 for every failure, so the action's own is
        # named beside it whenever the two differ. One column, because an
        # operator reads this table across a campaign's worth of rows.
        action = row.get("action_returncode")
        if isinstance(action, int) and not isinstance(action, bool) \
                and action != value:
            signal = row.get("action_signal")
            if isinstance(signal, int) and not isinstance(signal, bool):
                return f"{value} (action signal {signal})"
            return f"{value} (action {action})"
        return str(value)
    if field == "receipt":
        value = row.get("receipt_published")
        return "-" if value is None else ("yes" if value else "no")
    return str(row.get(field) or "-")


def render(rows) -> str:
    """One table, wide enough for what is in it and no wider."""

    body = [[_cell(row, field) for field, _ in _COLUMNS] for row in rows]
    widths = [
        max(len(header), *(len(line[index]) for line in body)) if body
        else len(header)
        for index, (_, header) in enumerate(_COLUMNS)
    ]
    lines = ["  ".join(
        header.ljust(widths[index]) for index, (_, header) in enumerate(_COLUMNS)
    ).rstrip()]
    for line in body:
        lines.append("  ".join(
            cell.ljust(widths[index]) for index, cell in enumerate(line)
        ).rstrip())
    return "\n".join(lines)


def verdict(rows) -> int:
    """0 when every action's work is done, and otherwise which way it is not.

    ``cache_hit`` counts as done.  That is the pull queue's own rule, applied
    in ``attempt_ending``, which routes both it and ``executed`` to
    ``done/``; a memoized result is the same result.

    A failure outranks a wait: an action that is still running is news the
    caller can act on later, and one that failed is news they have to act on
    now.
    """

    if any(row["status"] == "record_error" for row in rows):
        return action_client.RECORD_WRITE_FAILED_EXIT
    if any(not row["succeeded"] and row["status"] not in {"waiting", "superseded"} for row in rows):
        return 1
    if any(row["status"] == "superseded" for row in rows):
        return action_client.PUBLICATION_SUPERSEDED_EXIT
    if any(row["status"] == "waiting" for row in rows):
        return GAVE_UP_EXIT
    return 0


def consumer_ending_row(q, row, *, cas, context=None):
    """Project one observed row onto the versioned consumer contract (#1892)."""
    context = context or {}
    status = row["status"]
    if status in {"record_error", "unreadable"}:
        status = "unreadable"
    elif status not in {"executed", "cache_hit", "failed", "withdrawn",
                        "superseded", "waiting", "refused"}:
        status = "failed"
    diagnostics = {key: value for key, value in row.items()
                   if key not in {"action_key", "status", "succeeded", "phases"}}
    diagnostics.update({key: context.get(key) for key in ("resource_profile", "profile")})
    cleanup = context.get("resource_scope_cleanup", row.get("resource_scope_cleanup"))
    containers = context.get("container_cleanup", row.get("container_cleanup"))
    no_attempt = not context.get("claimed") and status in {
        "cache_hit", "withdrawn", "superseded", "refused", "failed"}
    scope_complete = (cleanup.get("complete") is True if cleanup is not None
                      else not context.get("scope_required", True))
    # Normal-finish invariant: PoolQueue._cleanup_payload_containers in
    # src/prismabuild/pool.py:15010 refuses incomplete container cleanup before
    # filing resource_scope_cleanup at :15097 and :15100. Its Docker cleaner
    # removes then re-lists containers at :15280 and :15281. Pinned by
    # test_scope_cleanup_is_persisted_only_after_containers_are_removed.
    # An explicit container record always requires complete is True.
    containers_complete = (containers.get("complete") is True if containers is not None
                           else (cleanup is not None and cleanup.get("complete") is True)
                           or not context.get("containers_required", True) or no_attempt)
    diagnostics["cleanup"] = {
        "complete": bool(containers_complete and (no_attempt or (
            scope_complete and (status != "withdrawn" or context.get("result_ended", True))))),
        "resource_scope": cleanup, "containers": containers,
    }
    result = None
    reason = context.get("reason") or row.get("note")
    number = context.get("attempt")
    published = context.get("published_unix", row.get("published_unix"))
    if status in {"executed", "cache_hit", "failed", "withdrawn", "refused"}:
        if type(number) is int and number > 0 and context.get("result_ended", True):
            try:
                result = action_wait.read_observation(
                    "pbwait verified ending result",
                    lambda: action_result.read_verified_ending_result(
                        q, row["action_key"], published_unix=published, attempt=number,
                        ending=context.get("ending"), cas_root=cas.root),
                    budget_s=action_wait.OUTCOME_READ_TIMEOUT_S)
            except (action_wait.OutcomeObservationTimedOut, action_wait.OutcomeReaderRetained) as exc:
                result = action_result.unverified_ending_result(exc)
            except (action_result.ActionResultError, action_wait.OutcomeReadUnavailable,
                    OSError, pool.PoolError, pb.PrismaBuildError, ValueError) as exc:
                status, reason = "unreadable", str(exc)
        elif status == "executed" or (status == "failed" and number != 0):
            status, reason = "unreadable", "execution ending has no immutable attempt evidence"
    if status == "waiting":
        # A zero-patience result still distinguishes an unknown full key from
        # recorded work. Keep these filesystem reads inside the bounded child.
        def recorded_wait():
            found = outstanding(q, row["action_key"])
            return {"known": found is not None or recorded_action(cas, row["action_key"]) is not None,
                    "published_unix": found[1] if found else None}
        try:
            recorded = action_wait.read_observation(
                "pbwait recorded key", recorded_wait,
                budget_s=action_wait.OUTCOME_READ_TIMEOUT_S)
            published = recorded["published_unix"]
            if not recorded["known"]:
                raise _misnamed(f"pbwait: nothing recorded matches {row['action_key']!r}")
        except (action_wait.OutcomeReadUnavailable, OSError) as exc:
            status, reason = "unreadable", str(exc)
    generation = None
    if published is not None:
        try:
            generation = q.records.attempt_generation({
                "action_key": row["action_key"], "published_unix": published})
        except pool.PoolError as exc:
            status, reason = "unreadable", str(exc)
    action_exit = row.get("action_returncode")
    if type(action_exit) is not int:
        action_exit = 0 if status in {"executed", "cache_hit"} else None
    signal = row.get("action_signal")
    if type(signal) is not int:
        signal = None
    if status in {"failed", "withdrawn", "refused", "unreadable"} and not reason:
        reason = f"action ended {status}"
    return {
        "action_key": row["action_key"], "status": status,
        "succeeded": status in {"executed", "cache_hit"},
        "exit_code": action_exit, "signal": signal,
        "host": None if row.get("host") == "-" else row.get("host"),
        "attempt": number,
        "publication": {"published_unix": published, "generation": generation},
        "started_unix": context.get("started_unix"), "ended_unix": context.get("ended_unix"),
        "reason": str(reason) if reason is not None else None,
        "result": result, "diagnostics": diagnostics,
        "phases": row.get("phases", {}),
    }


def consumer_endings_verdict(rows):
    """Stable JSON exits, independent of historical human CLI exits."""
    if any(row["status"] == "unreadable" for row in rows):
        return action_client.RECORD_WRITE_FAILED_EXIT
    return verdict(rows)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description="Wait for submitted PrismaBuild actions and report them."
    )
    ap.add_argument("--wait-s", type=float, default=86400.0,
                    help="how long to wait for ALL of them, not for each")
    output = ap.add_mutually_exclusive_group()
    output.add_argument("--json", action="store_true",
                    help="emit ordered per-key rows, including read-only integrity diagnostics")
    output.add_argument("--endings", action="store_true",
                        help="emit prismabuild.endings.v1 with verified attempt results")
    ap.add_argument("--until", choices=("ended", "cleaned"),
                    help="ending or verified cleanup boundary (default ended; requires --endings)")
    ap.add_argument("keys", nargs="+", metavar="KEY",
                    help="action key, or a prefix of one already recorded")
    ap.add_argument("--reconcile-pool", action="store_true",
                    help="verify a failed broker-EOF action's receipt and append immutable recovery evidence")
    ap.add_argument("--reconcile-cleanup", action="store_true",
                    help="on the owning host, verify scope absence or release for one failed or pending attempt")
    ap.add_argument("--generation",
                    help="exact pool attempt-generation digest; required with --reconcile-pool or --reconcile-cleanup")
    ap.add_argument("--attempt", type=int,
                    help="final attempt number; required with --reconcile-pool or --reconcile-cleanup")
    args = ap.parse_args(argv)

    if args.until is not None and not args.endings:
        ap.error("--until requires --endings")
    if args.endings and (not math.isfinite(args.wait_s) or args.wait_s < 0):
        ap.error("--wait-s must be finite and non-negative")

    if args.reconcile_pool and args.reconcile_cleanup:
        ap.error('--reconcile-pool and --reconcile-cleanup are mutually exclusive')
    if args.reconcile_pool or args.reconcile_cleanup:
        if args.endings:
            ap.error("reconciliation cannot combine with --endings")
        if len(args.keys) != 1 or args.generation is None or args.attempt is None:
            ap.error("reconciliation requires one KEY, --generation and --attempt")
    elif args.generation is not None or args.attempt is not None:
        ap.error("--generation and --attempt require --reconcile-pool or --reconcile-cleanup")

    queue = pool.PoolQueue(store_for(__file__).root / "pb-queue")
    cas = pb.PrismaBuildCAS(store_for(__file__).root / "cas")
    keys = [resolve_key(queue, name) for name in args.keys]
    if args.reconcile_cleanup:
        try:
            result = queue.reconcile_cleanup(keys[0], args.generation, args.attempt)
        except (OSError, ValueError) as exc:
            print(f'pbwait: cleanup reconciliation refused: {exc}', file=sys.stderr)
            return 1
        print(pb.sorted_json(result))
        return 0
    if args.reconcile_pool:
        from prismabuild.pool_reconcile import reconcile
        try:
            result = reconcile(queue, keys[0], cas=cas, generation=args.generation,
                               attempt=args.attempt)
        except (OSError, ValueError, pb.CASTamperError) as exc:
            print(f"pbwait: pool reconciliation refused: {exc}", file=sys.stderr)
            return 1
        print(json.dumps(result, sort_keys=True))
        return 0
    options = {"consumer": True, "until": args.until or "ended"} if args.endings else {}
    rows = wait_for_keys(queue, keys, cas=cas, wait_s=args.wait_s, **options)
    if args.endings:
        by_key = {row["action_key"]: row for row in rows}
        ordered = [by_key[key] for key in keys]
        complete = all(row["status"] not in {"waiting", "unreadable"}
                       and (args.until != "cleaned" or row["diagnostics"]["cleanup"]["complete"])
                       for row in ordered)
        print(json.dumps({"schema": "prismabuild.endings.v1",
                          "complete": complete,
                          "endings": ordered}, sort_keys=True))
        if args.until == "cleaned" and not complete and not any(
                row["status"] == "unreadable" for row in ordered):
            return GAVE_UP_EXIT
        return consumer_endings_verdict(ordered)
    print(json.dumps(rows, sort_keys=True) if args.json else render(rows))
    return verdict(rows)


if __name__ == "__main__":
    raise SystemExit(main())
