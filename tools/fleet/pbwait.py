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
import os
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
from runtime_paths import generation_root  # noqa: E402

RUNTIME_ROOT = generation_root(__file__)
sys.path.insert(0, str(RUNTIME_ROOT / "src"))
from prismabuild import core as pb, pool, slurm_lane  # noqa: E402

import pbrun  # noqa: E402

#: What this exits with when it stopped waiting before the work finished.
#: ``pbrun`` spells it the same way and means the same thing by it: the work is
#: still running.
GAVE_UP_EXIT = pbrun.GAVE_UP_EXIT

#: What this exits with when the key an operator typed names nothing this can
#: wait on.  ``pbrun --withdraw`` exits 2 for the same two refusals, and the
#: operating guide's exit-code table says ``pbrun`` and ``pbwait`` use the same
#: codes.  Exit 1 is reserved for "the action failed", so a wrapper that reads
#: a 1 here mistakes a typo for a build that ran and lost.
MISNAMED_EXIT = 2

#: How wide a key prints.  Twelve characters is what every fleet log line
#: shows, so it is what an operator has to compare against.
KEY_WIDTH = 12

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
    found = {
        str(record["action_key"])
        for record in slurm_lane.resolve_recorded(text, root=lane_root)
    }
    for state in (pool.READY, pool.CLAIMED, pool.DONE, pool.FAILED,
                  pool.WITHDRAWN):
        try:
            names = os.listdir(q.dir(state))
        except OSError:
            continue
        found.update(
            entry[: -len(".json")] for entry in names
            if entry.startswith(text) and entry.endswith(".json")
        )
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
outstanding = pbrun.outstanding_submission


def recorded_action(cas, key: str):
    """The sealed action ``pbrun`` published for this key, or ``None``.

    Absent means the CAS was cleared or the key was never submitted from here.
    That is a missing lookup, not a failure: it only costs the ability to say
    ``cache_hit`` without a terminal record.
    """

    path = Path(cas.root) / "requests" / key[:2] / f"{key}.json"
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return value if isinstance(value, dict) else None


def unreadable_terminal(q, key: str):
    """A terminal record filed for this key that cannot be read, or ``None``.

    Returns ``(path, reason)``. ``pbrun.terminal_record`` answers ``None`` for
    a record it cannot parse, which is the right answer to "is this the
    generation I asked about" and the wrong answer to "has anything been
    filed". Without this, the one state where the answer is on disk and
    unreadable is the state a waiter spends its whole ``--wait-s`` on.

    Terminal records are published by rename, in ``materialize.
    _write_json_atomic`` and ``slurm_lane._write_latest``, so a record that
    does not parse is a fault and never a write still in flight. There is
    nothing to wait for.

    An unreadable record cannot say which generation it belongs to, so one
    left over from an older run of the same key ends the wait too. That is
    deliberate: the operator is handed the path of the file to fix, which is
    the only move available either way. Guessing the generation from the
    file's modification time would put a guess where the record's own answer
    should be.
    """

    for state in (pool.DONE, pool.FAILED, pool.WITHDRAWN):
        path = q.item_path(state, key)
        try:
            # By readdir, like ``pbrun.terminal_record``: a stat of a path that
            # did not exist yet is negatively cached on NFS.
            if path.name not in os.listdir(path.parent):
                continue
        except OSError:
            continue
        try:
            record = json.loads(path.read_text(encoding="utf-8"))
        except PermissionError:
            return path, "permission denied"
        except OSError as exc:
            return path, str(exc.strerror or type(exc).__name__).lower()
        except ValueError:
            return path, "not valid JSON"
        if not isinstance(record, dict):
            return path, "not a JSON object"
    return None


# --------------------------------------------------------------------------
# Waiting for one
# --------------------------------------------------------------------------

def _row(key: str, status: str, **fields) -> dict:
    row = {
        "action_key": key, "status": status, "transport": "-", "job": "-",
        "host": "-", "elapsed_s": None, "returncode": None,
        "action_returncode": None, "action_signal": None,
        "receipt_published": None, "note": None,
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


def _from_record(q, outcome_path, outcome) -> dict:
    summary = pbrun.outcome_summary(q, outcome_path, outcome)
    scheduler = summary["detail"].get("slurm") or {}
    return _row(
        summary["action_key"] or "",
        summary["status"],
        transport=summary["transport"],
        # What an operator needs to read the logs the record names: under SLURM
        # that is the job id, and under the pull queue there is no such handle
        # -- the worker ran it in a process that is gone.
        job=str(scheduler.get("job_id") or "-"),
        host=summary["finished_host"] or "-",
        elapsed_s=summary["elapsed_s"],
        returncode=summary["returncode"],
        action_returncode=summary["action_returncode"],
        action_signal=summary["action_signal"],
        receipt_published=summary["receipt_published"],
        succeeded=summary["succeeded"],
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
    **lane_commands,
) -> dict:
    """Block until this action's ending is filed, and reduce it to one row.

    ``deadline`` is a monotonic instant shared by every key in one call, so
    ``--wait-s`` bounds the whole wait rather than each key in turn.

    The order is: an ending already filed for this generation, then the CAS
    receipt, then the scheduler.  The receipt outranks the scheduler because
    it is the only one of the two that survives ``MinJobAge``.

    ``generation`` says which run is meant.  A caller that submitted the work
    knows it -- ``pbrun --detach`` prints it -- and should pass it, because
    reading it back off the queue is a race: a worker can claim and finish the
    item before this looks, leaving nothing outstanding to read it from.

    A wait that finds no submission keeps looking for one, and not only for an
    ending.  ``resolve_key`` accepts a full key for work nothing has recorded
    yet, so a wait legitimately starts before its submission exists -- and
    under SLURM this waiter is the only thing that will ever file that
    submission's ending.  Watching terminal files alone therefore spent the
    whole of ``--wait-s`` on a job that had run and finished, and a second
    wait started afterwards reported it at once.
    """

    # Said on stderr, beside the table this returns a row for: a wait that
    # prints nothing for an hour gives an operator no way to tell a job the
    # scheduler is holding on purpose from one that is stuck.  ``resume``
    # reports both through this.
    lane_commands.setdefault(
        "on_notice",
        lambda text: print(f"pbwait: {text}", file=sys.stderr, flush=True))
    while True:
        row = _look_once(
            q, key, cas=cas, deadline=deadline, generation=generation,
            lane_root=lane_root, queue_root=queue_root, **lane_commands,
        )
        if row is not None:
            return row
        # Nothing is recorded and nothing is filed.  ``>=`` so a caller with
        # no patience does not spend a poll interval finding that out, which
        # is how ``pbrun.landed_outcome`` spells the same test.
        if time.monotonic() >= deadline:
            return _row(key, "waiting")
        # Read on each pass rather than captured: a test that shortens the
        # interval sets it on the module.
        time.sleep(max(0.0, min(pbrun.POLL_S, deadline - time.monotonic())))


def _look_once(
    q,
    key: str,
    *,
    cas,
    deadline: float,
    generation: float | None = None,
    lane_root=None,
    queue_root=None,
    **lane_commands,
):
    """One pass of ``wait_one``, or ``None`` when there is nothing yet.

    ``None`` is the one answer that means "look again": no submission is
    recorded, no ending is filed, and the CAS holds no receipt.  Every other
    answer is a row, because every other answer is about a run this can name.

    Split out of ``wait_one`` so the pass reads in one screen and the loop
    around it holds nothing but the deadline and the interval.
    """

    found = outstanding(q, key, lane_root=lane_root)
    if generation is None and found is not None:
        # A submission this pass discovered is the run the wait is about, and
        # a caller that named a generation keeps it.  Deriving it per pass
        # cannot drift: the only pass that asks for another one is a pass that
        # found no submission, and so derived nothing.
        generation = found[1]

    landed = pbrun.landed_outcome(q, key, wait_s=0.0, generation=generation)
    if landed is not None:
        return _from_record(q, *landed)

    action = recorded_action(cas, key)
    receipt = None if action is None else cas.lookup(action)
    slurm_run = (found is not None and found[0] == "slurm"
                 and found[1] == generation)

    if receipt is not None and not (found is not None and found[0] == "pool"):
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
            landed = pbrun.landed_outcome(
                q, key, wait_s=0.0, generation=generation)
            if landed is not None:
                return _from_record(q, *landed)
        # Nothing outstanding, or nothing that could file: the work is done
        # and was memoized.  Which transport delivered it does not enter into
        # it, which is the property the CAS exists to give.
        return _row(key, "cache_hit", transport="cas", receipt_published=True)

    broken = unreadable_terminal(q, key)
    if broken is not None:
        # An ending was filed and cannot be read. Waiting is what a caller does
        # for an ending that has not arrived; this one has.
        return _row(
            key, "unreadable",
            transport=found[0] if found is not None else "-",
            job=_job_id(found),
            note=f"{broken[1]}: {broken[0]}",
        )

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
            wait_s=max(0.0, deadline - time.monotonic()),
            **lane_commands,
        )
        landed = pbrun.landed_outcome(q, key, wait_s=0.0, generation=generation)
        if landed is not None:
            return _from_record(q, *landed)
        return _row(key, "waiting", transport="slurm",
                    job=_job_id(found),
                    host=str(found[2].get("submitted_host") or "-"))

    if found is None:
        # Nothing has been recorded under this key at all.  Say so by
        # returning nothing, so the caller looks again for a submission as
        # well as for an ending.
        return None

    # A pull-queue item: the worker that claims it files the ending, so this
    # only watches, and it may watch out the whole deadline.
    landed = pbrun.landed_outcome(
        q, key, wait_s=max(0.0, deadline - time.monotonic()),
        generation=generation,
    )
    if landed is None:
        return _row(key, "waiting", transport=found[0], job=_job_id(found))
    return _from_record(q, *landed)


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
    deadline = time.monotonic() + float(wait_s)
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
    in ``adopted_attempt_summary``, which routes both it and ``executed`` to
    ``done/``; a memoized result is the same result.

    A failure outranks a wait: an action that is still running is news the
    caller can act on later, and one that failed is news they have to act on
    now.
    """

    if any(not row["succeeded"] and row["status"] != "waiting" for row in rows):
        return 1
    if any(row["status"] == "waiting" for row in rows):
        return GAVE_UP_EXIT
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description="Wait for submitted PrismaBuild actions and report them."
    )
    ap.add_argument("--wait-s", type=float, default=86400.0,
                    help="how long to wait for ALL of them, not for each")
    ap.add_argument("keys", nargs="+", metavar="KEY",
                    help="action key, or a prefix of one already recorded")
    args = ap.parse_args(argv)

    queue = pool.PoolQueue(pbrun.SH / "pb-queue")
    cas = pb.PrismaBuildCAS(pbrun.SH / "cas")
    keys = [resolve_key(queue, name) for name in args.keys]
    rows = wait_for_keys(queue, keys, cas=cas, wait_s=args.wait_s)
    print(render(rows))
    return verdict(rows)


if __name__ == "__main__":
    raise SystemExit(main())
