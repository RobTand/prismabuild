#!/usr/bin/env python3
"""Show the selected fleet transport: workers, active jobs, and endings.

An operator looking at this fleet has three questions and, without this
command, three places to look them up. Pool status reads worker offers,
ready/claimed records and persisted admission evidence from the shared queue.
Explicit SLURM status uses ``sinfo`` to say which boxes the
controller can still reach.  ``squeue`` says what is waiting and why.  The
terminal records under ``pb-queue/done`` and ``pb-queue/failed`` say how the
last few actions ended, and they are the only one of the three that survives
``MinJobAge``.  This prints all three, in that order, from one command with no
arguments.

Two of the three answers are only useful when they carry PrismaBuild's own
vocabulary, so the tables join rather than transcribe:

*   A job name is ``pb-<key12>``, which is a twelve-character prefix of an
    action key and nothing an operator can act on.  The jobs table resolves it
    against the lane's own submission record, so the row also carries the
    resources the action asked for, the constraint it was placed under, and the
    box that submitted it.
*   The scheduler forgets a job, and the endings table does not.  It reads the
    same three directories the pull queue files its endings in, so a SLURM ending
    and a pull-queue ending appear side by side, each labelled with the
    transport that produced it.

Every scheduler call has a bounded
timeout, the endings are read newest-first with a cap, and a table that cannot
be read prints why instead of raising.  A controller that is not installed --
the state of every box in this fleet until the install runs -- prints one line
saying so and the endings table still prints, because those records are files
on the shared mount and do not need a scheduler to be read.

Nothing here writes. Missing or inaccessible active pool directories report
unknown state rather than an empty queue. File reads are a diagnostic census,
not an atomic scheduler snapshot or a process-liveness proof.
"""
from __future__ import annotations

import argparse
import json
import math
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import time
from collections.abc import Iterable, Mapping, Sequence

sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
from runtime_paths import generation_root  # noqa: E402
from fleet_submit import TRANSPORTS, default_transport  # noqa: E402

RUNTIME_ROOT = generation_root(__file__)
sys.path.insert(0, str(RUNTIME_ROOT / "src"))
from prismabuild import pool, slurm_lane as sl  # noqa: E402

#: Where the fleet keeps the queue both transports file their endings in.  The
#: same spelling ``pbrun``, ``pool_reset`` and ``tessera_status`` use.
SHARED_ROOT = Path("/mnt/shared/prismabuild-fleet")
DEFAULT_QUEUE_ROOT = SHARED_ROOT / "pb-queue"

#: How long any one scheduler command has to answer.  Shorter than the lane's
#: own 60 s, deliberately: a ``pbrun`` waiting on a job is willing to wait for
#: a busy controller, and a person looking at a status screen is not.
COMMAND_TIMEOUT_S = 20.0

#: How many endings to read by default.  The cap is the point -- the shared
#: mount holds every action this fleet has ever run, and stat'ing all of them
#: over NFS to print twenty rows is the slow path this avoids.
DEFAULT_RECENT = 20

#: The node states that mean a box is working.  Everything else -- down, drain,
#: fail, unknown -- is flagged with the controller's own reason.
HEALTHY_NODE_STATES = frozenset({"idle", "mixed", "allocated"})

#: What SLURM appends to a state to qualify it: ``*`` for a node the controller
#: cannot reach, ``~`` powered down, ``#`` powering up, ``%`` powering down,
#: ``$`` in a reservation, ``@`` pending reboot, ``!`` pending power-down.  A
#: node reading ``idle*`` is not idle in any sense an operator cares about, so
#: the suffix is stripped off before the healthy test and reported on its own.
NODE_STATE_FLAGS = "*~#!%$@^-"

#: What each flag means, so the table says it rather than printing punctuation.
NODE_FLAG_MEANINGS = {
    "*": "not responding",
    "~": "powered down",
    "#": "powering up",
    "%": "powering down",
    "$": "reserved",
    "@": "reboot pending",
    "!": "power-down pending",
    "^": "reboot issued",
    "-": "planned",
}

#: The node-centric ``sinfo`` fields this reads, named explicitly so that the
#: parser below and the scheduler agree on the column order.  Verified against
#: ``sinfo --helpformat`` and a live 25.11.2 controller: ``%e`` is the node's
#: own free memory and reads ``N/A`` without a running ``slurmd``, ``%G`` reads
#: ``(null)`` on a node with no GRES, and ``%E`` reads ``none`` when the
#: controller has no reason to give.
SINFO_FIELDS = ("%N", "%P", "%T", "%C", "%m", "%e", "%G", "%f", "%O", "%E")
SINFO_FORMAT = "|".join(SINFO_FIELDS)

#: The ``squeue`` fields, same contract.  ``%l`` prints the literal string
#: ``UNLIMITED`` for a job submitted with no ``--time``, which on this fleet is
#: every job whose producer did not pass ``--timeout-s``; ``%r`` prints a
#: sentence for a pending job and ``None`` for a running one.
SQUEUE_FIELDS = ("%i", "%T", "%P", "%N", "%M", "%l", "%u", "%j", "%r")
SQUEUE_FORMAT = "|".join(SQUEUE_FIELDS)

#: What a PrismaBuild job is named: ``slurm_lane.submit`` passes
#: ``--job-name=pb-<first twelve hex of the action key>``.  A job that does not
#: match belongs to somebody else and lists with an empty key column.
JOB_NAME_KEY = re.compile(r"^pb-([0-9a-f]{12})$")

#: One ``scontrol show node`` record starts here.
_SCONTROL_NODE = re.compile(r"(?m)^NodeName=")

#: What the tables print where a value is genuinely unknown.  Distinct from a
#: value of zero, and distinct from ``-`` for a field the record does not have.
UNKNOWN = "?"
ABSENT = "-"


class SchedulerUnavailable(Exception):
    """A scheduler command could not be run, or refused to answer.

    Carries the single line the status screen prints in place of the table,
    because "why is this table missing" is the whole content of the failure.
    """


def _scheduler_output(argv: Sequence[str], *, program: str) -> str:
    """Run one scheduler command and return its stdout.

    Args:
        argv: The command to run, argv[0] included.
        program: The name to use in the message when the command fails.

    Returns:
        The command's standard output.

    Raises:
        SchedulerUnavailable: The program is absent, timed out, or exited
            non-zero.  The exception's message is the operator-facing line.
    """

    try:
        completed = subprocess.run(
            list(argv), capture_output=True, text=True,
            timeout=COMMAND_TIMEOUT_S,
        )
    except FileNotFoundError:
        raise SchedulerUnavailable(
            f"{program}: not found; SLURM is not installed on this box"
        ) from None
    except subprocess.TimeoutExpired:
        raise SchedulerUnavailable(
            f"{program}: no answer within {COMMAND_TIMEOUT_S:.0f}s"
        ) from None
    except OSError as exc:
        raise SchedulerUnavailable(f"{program}: {exc}") from None
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout).strip().splitlines()
        raise SchedulerUnavailable(
            f"{program}: {detail[0] if detail else completed.returncode}"
        )
    return completed.stdout


def _text(raw: str | None) -> str | None:
    """One scheduler field, with SLURM's several spellings of null read as one.

    ``slurm_lane`` already decides which strings SLURM uses for "no answer"
    (``(null)``, ``N/A``, ``none``, ``unknown``), and this reuses that decision
    rather than keeping a second list that can drift from it.
    """

    return sl._parse_slurm_text(raw)


def _int(raw: str | None) -> int | None:
    """One scheduler field that should be a whole number, or ``None``."""

    text = (raw or "").strip()
    try:
        return int(text)
    except ValueError:
        return None


def split_node_state(raw: str) -> tuple[str, list[str]]:
    """Separate a node state from the flag characters SLURM suffixes to it.

    Args:
        raw: The state as ``sinfo`` printed it, such as ``idle*`` or
            ``drained``.

    Returns:
        The bare state in lowercase, and the meaning of each suffix flag.
    """

    state = (raw or "").strip()
    flags: list[str] = []
    while state and state[-1] in NODE_STATE_FLAGS:
        flags.append(NODE_FLAG_MEANINGS.get(state[-1], state[-1]))
        state = state[:-1]
    return state.lower(), list(reversed(flags))


def _scontrol_nodes(names: Sequence[str], *, scontrol: str,
                    ) -> tuple[dict[str, dict], str | None]:
    """Ask ``scontrol`` for the two node facts ``sinfo`` cannot print.

    ``AllocMem`` is the memory the scheduler has handed out, which is a
    different number from the node's free memory that ``sinfo -o %e`` reports;
    and ``GresUsed`` is how much of a node's GRES is in use, which has no
    ``sinfo`` format code at all.  One call covers the whole node list, which
    ``scontrol`` accepts as a comma-separated list.

    ``-d`` is not decorative.  The controller packs ``GresUsed`` only under
    ``SHOW_DETAIL``, so without the flag the field is absent for every node and
    the GRES-in-use column reads unknown forever -- measured against a live
    25.11.2 controller, where ``scontrol show node`` printed ``Gres=`` and no
    ``GresUsed=``, and ``scontrol -d show node`` printed both.

    Returns:
        What each node said, and the reason the call failed when it did.  A
        failure is not fatal -- the node table still prints -- but it is
        reported, because the two columns then read unknown for a reason that
        has nothing to do with the fleet.
    """

    if not names:
        return {}, None
    try:
        raw = _scheduler_output(
            [scontrol, "-d", "show", "node", ",".join(names)],
            program="scontrol",
        )
    except SchedulerUnavailable as exc:
        return {}, str(exc)
    found: dict[str, dict] = {}
    for chunk in _SCONTROL_NODE.split(raw):
        if not chunk.strip():
            continue
        # The record's own leading `NodeName=` is what the split consumed, so
        # put it back before the field regex reads the block.
        fields = dict(sl._SCONTROL_FIELD.findall("NodeName=" + chunk))
        name = fields.get("NodeName", "").strip()
        if name:
            found[name] = fields
    return found, None


def read_nodes(*, sinfo: str = "sinfo", scontrol: str = "scontrol",
               ) -> tuple[list[dict], str | None]:
    """Describe every node the controller knows about.

    ``sinfo -N`` prints one row per node per partition, so rows are merged by
    node name and the partitions collected: an operator reads the fleet as a
    list of boxes, not as a list of memberships.

    Args:
        sinfo: The ``sinfo`` executable to run.
        scontrol: The ``scontrol`` executable to run for allocated memory and
            GRES in use.

    Returns:
        One dictionary per node in the order ``sinfo`` listed them, and the
        reason the ``scontrol`` detail is missing when it is.

    Raises:
        SchedulerUnavailable: ``sinfo`` could not be reached.
    """

    raw = _scheduler_output(
        [sinfo, "-N", "-h", "-o", SINFO_FORMAT], program="sinfo"
    )
    order: list[str] = []
    rows: dict[str, dict] = {}
    for line in raw.splitlines():
        if not line.strip():
            continue
        fields = line.split("|")
        name = sl._field(fields, 0).strip()
        if not name:
            continue
        # The default partition carries a trailing `*`, which names the
        # configuration rather than this node.
        partition = sl._field(fields, 1).strip().rstrip("*")
        if name in rows:
            if partition and partition not in rows[name]["partitions"]:
                rows[name]["partitions"].append(partition)
            continue
        state, flags = split_node_state(sl._field(fields, 2))
        # `%C` is allocated/idle/other/total; the first two are the ones an
        # operator schedules against.
        cpus = sl._field(fields, 3).split("/")
        rows[name] = {
            "node": name,
            "partitions": [partition] if partition else [],
            "state": state,
            "flags": flags,
            "healthy": state in HEALTHY_NODE_STATES and not flags,
            "cpus_alloc": _int(sl._field(cpus, 0)),
            "cpus_idle": _int(sl._field(cpus, 1)),
            "cpus_total": _int(sl._field(cpus, 3)),
            "memory_total_mib": _int(sl._field(fields, 4)),
            "memory_free_mib": _int(sl._field(fields, 5)),
            "memory_alloc_mib": None,
            "gres": _text(sl._field(fields, 6)),
            "gres_used": None,
            "features": [
                part for part in sl._field(fields, 7).split(",") if part.strip()
            ],
            "load": _text(sl._field(fields, 8)),
            # Read from `sinfo`, not from `scontrol`: a reason is a sentence
            # with spaces in it, and the lane's field regex stops at the first
            # one.
            "reason": _text(sl._field(fields, 9)),
        }
        order.append(name)
    detail, note = _scontrol_nodes(order, scontrol=scontrol)
    for name, row in rows.items():
        fields = detail.get(name, {})
        row["memory_alloc_mib"] = _int(fields.get("AllocMem"))
        row["gres_used"] = _text(fields.get("GresUsed"))
    return [rows[name] for name in order], note


def _lane_index(*, root: str | Path | None) -> dict[str, list[str]]:
    """Map every twelve-character key prefix in the lane root to its full keys.

    ``slurm_lane.resolve_recorded`` answers the same question, and it lists the
    lane root once per call.  A jobs table asks it once per queued job, so the
    listing is done once and the per-key record read stays in the lane.

    The listing is ``slurm_lane.recorded_keys``, which ``pbsweep`` reads too:
    one answer to "which keys does this lane root hold", so a job this table
    can name is a job the sweeper can reconcile.
    """

    index: dict[str, list[str]] = {}
    for name in sl.recorded_keys(root):
        index.setdefault(name[:12], []).append(name)
    return index


def _submission(prefix: str, index: Mapping[str, list[str]],
                *, root: str | Path | None) -> tuple[dict | None, str | None]:
    """The lane's record for one job-name prefix, and any caveat about it.

    Returns:
        The submission record and ``None``, or ``None`` and the reason there
        is no single record to show.
    """

    keys = index.get(prefix, [])
    if not keys:
        return None, "no submission recorded in the lane root"
    if len(keys) > 1:
        # Two action keys sharing twelve hex characters.  Guessing which one
        # this job is would attribute somebody else's resources to it.
        return None, f"{len(keys)} action keys share this prefix"
    record = sl.recorded_submission(keys[0], root=root)
    if record is None:
        return None, "the lane's latest.json could not be read"
    return record, None


def read_jobs(*, squeue: str = "squeue", lane_root: str | Path | None = None,
              ) -> list[dict]:
    """Describe every job in the queue, joined to PrismaBuild's own records.

    Args:
        squeue: The ``squeue`` executable to run.
        lane_root: The SLURM lane root to resolve job names against.  ``None``
            uses the lane's own default, environment override included.

    Returns:
        One dictionary per job, in the order ``squeue`` listed them.

    Raises:
        SchedulerUnavailable: ``squeue`` could not be reached.
    """

    raw = _scheduler_output(
        [squeue, "-h", "-o", SQUEUE_FORMAT], program="squeue"
    )
    index = _lane_index(root=lane_root)
    jobs: list[dict] = []
    for line in raw.splitlines():
        if not line.strip():
            continue
        fields = line.split("|")
        job_id = sl._field(fields, 0).strip()
        if not job_id:
            continue
        name = sl._field(fields, 7).strip()
        match = JOB_NAME_KEY.match(name)
        key_prefix = match.group(1) if match else None
        limit = sl._field(fields, 5).strip()
        row = {
            "job_id": job_id,
            "state": sl._field(fields, 1).strip().upper(),
            "partition": sl._field(fields, 2).strip().rstrip("*"),
            "node": _text(sl._field(fields, 3)),
            "elapsed": sl._field(fields, 4).strip(),
            # A job submitted without `--time` has no limit, and `squeue` says
            # so in the same word the fleet's partitions are configured with.
            # An empty field means the same thing and is reported the same way,
            # rather than as missing data.
            "time_limit": limit or "UNLIMITED",
            "user": sl._field(fields, 6).strip(),
            "name": name,
            "action_key_prefix": key_prefix,
            # `squeue` prints the reason for a pending job and `None` for a
            # running one, and parenthesises it in some layouts.
            "reason": _text(sl._field(fields, 8).strip().strip("()")),
            "resources": None,
            "constraint": None,
            "submitted_host": None,
            "note": None,
        }
        if key_prefix is not None:
            record, note = _submission(key_prefix, index, root=lane_root)
            row["note"] = note
            if record is not None:
                row["action_key"] = record.get("action_key")
                row["resources"] = record.get("resources")
                row["constraint"] = record.get("constraint")
                row["submitted_host"] = record.get("submitted_host")
                recorded_id = str(record.get("job_id") or "")
                if recorded_id and recorded_id != job_id:
                    # `latest.json` points at the newest submission of this
                    # action key, which after a retry or a resubmission is a
                    # different job from the one in this row.
                    row["note"] = (
                        f"the lane's latest record is job {recorded_id}"
                    )
        jobs.append(row)
    _name_the_job_ahead(jobs)
    return jobs


def _name_the_job_ahead(jobs: list[dict]) -> None:
    """Say which job a singleton-held job is waiting for.

    Every PrismaBuild submission carries ``--dependency=singleton`` under the
    job name ``pb-<key12>``, so a job PENDING with reason ``Dependency`` is
    queued behind another job of the same action key -- the design working,
    not something an operator has to fix.  The job it waits for is already in
    this listing, so naming it costs no second call to the controller.
    """

    ahead: dict[str, str] = {}
    for job in jobs:
        if job.get("action_key_prefix") and job["state"] != "PENDING":
            ahead.setdefault(str(job["name"]), str(job["job_id"]))
    for job in jobs:
        if job["state"] != "PENDING":
            continue
        if str(job.get("reason") or "") != "Dependency":
            continue
        running = ahead.get(str(job["name"]))
        note = (
            f"waiting for job {running} of the same action" if running
            else "waiting for another job of the same action"
        )
        job["note"] = f"{job['note']}; {note}" if job.get("note") else note


def _pool_records(directory: Path) -> tuple[dict[str, dict | None], list[str]]:
    """Audit a live directory without treating failed reads as an empty queue."""
    records: dict[str, dict | None] = {}
    notes: list[str] = []
    try:
        with os.scandir(directory) as entries:
            paths = sorted(Path(entry.path) for entry in entries if entry.name.endswith('.json'))
    except OSError as exc:
        return records, [f"pool {directory}: {_unreadable_reason(exc)}"]
    for path in paths:
        try:
            if not stat.S_ISREG(path.lstat().st_mode):
                raise ValueError("not a regular queue record")
            record = pool._read_json(path)
            if record is None:
                # A rename may have raced this census. Unknown, rather than
                # a confident empty answer; a later screen can settle it.
                raise ValueError("record disappeared or is empty")
            records[path.stem] = record
        except (OSError, ValueError) as exc:
            records[path.stem] = None
            notes.append(f"pool {path}: {exc}")
    return records, notes


def _releases(record: Mapping[str, object]) -> int | None:
    """How often this action was returned to the queue without starting.

    ``reap_stale`` releases a claim that never wrote a lease and never
    published an attempt: the action leaves ``ready``, nothing runs, and it
    comes back with its attempt count untouched and this counter raised
    (issue #222).  The release is right; the silence was not.  ``None`` when
    the record has never been released, so an untouched action reads as absent
    rather than as a measured zero.
    """

    count = record.get("unstarted_releases")
    return count if type(count) is int and count > 0 else None


def _age(timestamp: object, now: float) -> float | None:
    if type(timestamp) not in (int, float) or not math.isfinite(timestamp):
        return None
    return now - timestamp


def _admission_sample(path: Path, *, now: float, max_age_s: float) -> dict:
    """Read persisted evidence only; controller decisions can write state."""
    try:
        record = pool._read_json(path)
        if record is None:
            return {"state": "unavailable", "note": "no sample recorded"}
        age = _age(record.get("sampled_unix"), now)
        return {"state": "fresh" if age is not None and 0 <= age <= max_age_s else "stale",
                "age_s": age, "record": record}
    except (OSError, ValueError) as exc:
        return {"state": "unavailable", "note": str(exc)}


def read_pool(queue_root: str | Path) -> dict:
    """Read worker offers, active records and existing admission evidence.

    This is a non-atomic diagnostic census, never a new admission decision.
    File errors retain unknown counts and unreadable rows. PoolQueue supplies
    the resource, placement, denial-count and lease rules used by workers.
    """
    queue = pool.PoolQueue(Path(queue_root).absolute())
    now = time.time()
    workers, worker_notes = _pool_records(queue.root / pool.WORKERS)
    ready, ready_notes = _pool_records(queue.dir(pool.READY))
    claimed, claim_notes = _pool_records(queue.dir(pool.CLAIMED))
    notes = [*worker_notes, *ready_notes, *claim_notes]
    nodes: list[dict] = []
    live: list[dict] = []
    for host, offer in workers.items():
        if (offer is None or re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]*', host) is None
                or offer.get("schema") != pool.POOL_OFFER_SCHEMA_V1
                or offer.get("host") != host or not isinstance(offer.get("capacity"), dict)
                or not isinstance(offer.get("tags"), list)
                or any(type(v) is not int or v < 0 for v in offer['capacity'].values())):
            nodes.append({"node": host, "state": "unreadable", "healthy": False,
                          "reason": "invalid worker offer"})
            notes.append(f"pool worker {host}: invalid worker offer")
            continue
        age = _age(offer.get("announced_unix"), now)
        fresh = age is not None and 0 <= age <= pool.OFFER_TIMEOUT_S
        if fresh:
            live.append(offer)
        else:
            notes.append(f"pool worker {host}: stale or invalid offer timestamp")
        base = queue.ledger(host).base / 'adaptive'
        nodes.append({
            "node": host, "transport": "pool", "state": "live" if fresh else "stale",
            "healthy": fresh, "age_s": age, "capacity": offer.get("capacity"),
            "observed_capacity": offer.get("observed_capacity"),
            "foreign": offer.get("foreign"), "observed_detail": offer.get("observed_detail"),
            "features": offer.get("tags"), "has_gpu": offer.get("has_gpu"),
            "runtime_commit": offer.get("runtime_commit"),
            "reason": None if fresh else "offer expired or timestamp invalid",
            "admission": {
                "cpu": _admission_sample(base / 'cpu-sample.json', now=now,
                                         max_age_s=pool.cpu_admission.MAX_SAMPLE_AGE_S),
                "gpu": _admission_sample(base / 'gpu-state.json', now=now,
                                         max_age_s=pool.gpu_admission.MAX_SAMPLE_AGE_S)
                       if offer.get('has_gpu') else {"state": "not applicable"},
            },
        })
    jobs: list[dict] = []
    valid_counts = {pool.READY: len(ready) if not ready_notes else None,
                    pool.CLAIMED: len(claimed) if not claim_notes else None}
    for state, records in ((pool.READY, ready), (pool.CLAIMED, claimed)):
        for key, record in records.items():
            row = {"action_key": key, "action_key_prefix": key[:12], "transport": "pool",
                   "state": state.upper(), "node": None, "reason": None}
            try:
                if (record is None or re.fullmatch('[a-f0-9]{64}', key) is None
                        or record.get('action_key') != key or record.get('schema') != pool.POOL_ITEM_SCHEMA_V1):
                    raise ValueError('invalid action record')
                row.update(resources=queue.demand_of(record), constraint=record.get('tags'),
                           submitted_host=record.get('published_by'),
                           unstarted_releases=_releases(record),
                           age_s=_age(record.get('claimed_unix') if state == pool.CLAIMED
                                      else record.get('published_unix'), now))
                if state == pool.READY:
                    hosts = sorted({str(offer['host']) for offer in queue._matching_offers(record, live=live)})
                    if pool.is_box_local_path(record.get('checkout_root')):
                        hosts = [host for host in hosts if host == record.get('published_by')]
                    row.update(placeable_hosts=hosts if live else None,
                               admission_passes=queue.passes(key),
                               admission_wait_s=queue.withhold_age(key))
                    row['reason'] = ('no fresh worker offers; placement unknown' if not live
                                     else 'no matching live worker' if not hosts
                                     else 'awaiting admission; matching worker capacity is not a grant')
                    if row['unstarted_releases']:
                        # A key the reaper keeps handing back reads exactly
                        # like a key nobody has got to yet, and the reason
                        # above is the one an operator acts on.  Issue #263:
                        # the count was on the record and no reader said it,
                        # so a bouncing action looked like a quiet queue for
                        # the whole of ``pbrun --wait-s``.
                        row['reason'] += (
                            f"; released {row['unstarted_releases']} time"
                            f"{'' if row['unstarted_releases'] == 1 else 's'} "
                            "before starting")
                else:
                    row.update(node=record.get('claimed_host'), owner=record.get('claimed_by'),
                               cpu_allocation=record.get('cpu_allocation'),
                               gpu_admission=record.get('gpu_admission'),
                               cleanup_pending=bool(record.get('finish_pending') or record.get('stop_pending')
                                                    or record.get('container_cleanup_pending')))
                    age = queue.lease_age(key)
                    row['lease_age_s'] = age
                    row['stale'] = age is None or not math.isfinite(age) or not 0 <= age <= pool.LEASE_TIMEOUT_S
                    if row['stale']:
                        row['reason'] = 'lease missing, stale or invalid; process liveness unknown'
                    elif row['cleanup_pending']:
                        row['reason'] = 'cleanup pending; reservation retained'
            except (OSError, ValueError, TypeError, KeyError) as exc:
                row.update(state='UNREADABLE', reason=str(exc))
                valid_counts[state] = None
                notes.append(f"pool {state}/{key}: {exc}")
            jobs.append(row)
    empty = (valid_counts[pool.READY] == 0 and valid_counts[pool.CLAIMED] == 0)
    return {"nodes": nodes, "jobs": jobs, "notes": notes,
            "queue": {"ready": valid_counts[pool.READY], "claimed": valid_counts[pool.CLAIMED],
                      "empty": empty if all(v is not None for v in valid_counts.values()) else None,
                      "complete": not notes, "live_workers": len(live),
                      "sampled_unix": now}}


def pool_node_lines(nodes: Sequence[Mapping[str, object]]) -> list[str]:
    if not nodes:
        return ["no worker offers recorded"]
    return render_table(("NODE", "STATE", "OFFER AGE", "CAPACITY", "OBSERVED", "ADMISSION", "NOTE"), (
        (n['node'], n['state'], n.get('age_s'), n.get('capacity'), n.get('observed_capacity'),
         ', '.join(f"{k}: {v['state']}" for k, v in n.get('admission', {}).items()), n.get('reason'))
        for n in nodes))


def pool_job_lines(jobs: Sequence[Mapping[str, object]], summary: Mapping[str, object]) -> list[str]:
    if not jobs:
        return ["no jobs ready or claimed" if summary.get('empty') is True else "pool job state unavailable"]
    return render_table(("KEY", "STATE", "NODE", "RESOURCES", "AGE", "PASSES", "RELEASES",
                         "MATCHING", "NOTE"), (
        (j['action_key_prefix'], j['state'], j.get('node'), j.get('resources'), j.get('age_s'),
         j.get('admission_passes'), j.get('unstarted_releases'), j.get('placeable_hosts'),
         j.get('reason')) for j in jobs))


def _ending_paths(queue_root: str | Path, limit: int) -> list[os.DirEntry]:
    """The ``limit`` newest terminal records by modification time.

    Only the directory entries are stat'ed here.  Reading every record on the
    shared mount to print twenty rows is the slow path this exists to avoid,
    and an absent directory is a fleet that has not run under that transport
    rather than an error.
    """

    entries: list[tuple[float, os.DirEntry]] = []
    for state in (pool.DONE, pool.FAILED, pool.WITHDRAWN):
        directory = Path(queue_root) / state
        try:
            with os.scandir(directory) as scan:
                for entry in scan:
                    if not entry.name.endswith(".json") or not entry.is_file():
                        continue
                    try:
                        entries.append((entry.stat().st_mtime, entry))
                    except OSError:
                        continue
        except OSError:
            continue
    entries.sort(key=lambda pair: pair[0], reverse=True)
    return [entry for _, entry in entries[: max(0, int(limit))]]


def _transport(record: Mapping[str, object]) -> str:
    """Which transport filed one ending, read from the schema it was filed under.

    The two writers use distinct schema ids on purpose, so this reads the id
    rather than trusting a ``transport`` field only one of them writes.  An
    unrecognized schema prints verbatim: a reader that renames an unknown
    record after one it does know is how a wrong table becomes a confident one.
    """

    schema = str(record.get("schema") or "")
    if schema == sl.OUTCOME_SCHEMA_V1:
        return "slurm"
    if schema == pool.POOL_OUTCOME_SCHEMA_V1:
        return "pool"
    return schema or UNKNOWN


def _unreadable_row(entry: os.DirEntry, reason: str) -> dict:
    """One record this could not read, kept as a row instead of dropped.

    The row carries the path and the reason because those are the whole of
    what an operator can act on: the key is only what the file is named, and
    every other column is inside the file nobody could read.
    """

    try:
        mtime = entry.stat().st_mtime
    except OSError:
        mtime = 0.0
    return {
        "action_key": entry.name[:-5],
        "status": "unreadable",
        "transport": UNKNOWN,
        "host": None,
        "elapsed_s": None,
        "returncode": None,
        "action_returncode": None,
        "action_signal": None,
        "receipt_published": None,
        "slurm_state": None,
        "unreadable": reason,
        # The record's own finished time is unreadable too, so the file's
        # modification time is what places the row. It is the same clock
        # ``_ending_paths`` selected the record by.
        "finished_unix": mtime,
        "path": entry.path,
    }


def _unreadable_reason(exc: Exception) -> str:
    """Why a record could not be read, in the terms that pick the fix.

    A record nobody may read and a record nobody can parse are different
    faults: the first is a mode or a mount, the second is a writer that
    stopped. Terminal records are published by rename, so a half-written one
    is a fault rather than a write still in flight.
    """

    if isinstance(exc, PermissionError):
        return "permission denied"
    if isinstance(exc, OSError):
        return str(exc.strerror or type(exc).__name__).lower()
    return "not valid JSON"


def read_endings(queue_root: str | Path, *, limit: int = DEFAULT_RECENT,
                 ) -> list[dict]:
    """Describe how the newest actions ended, under either transport.

    Args:
        queue_root: The queue root holding ``done``, ``failed`` and ``withdrawn``.
        limit: How many records to read, newest by modification time first.

    Returns:
        One dictionary per ending, newest first by the time the record says the
        work finished, falling back to the file's modification time. A record
        that cannot be read is returned as a row with status ``unreadable``,
        naming its path and why, rather than left out.
    """

    rows: list[dict] = []
    for entry in _ending_paths(queue_root, limit):
        try:
            record = json.loads(Path(entry.path).read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            # Not dropped. ``limit`` sliced the newest records before any of
            # them was read, so dropping one printed the same empty table a
            # fleet that had filed nothing prints, and the two states call for
            # opposite responses.
            rows.append(_unreadable_row(entry, _unreadable_reason(exc)))
            continue
        if not isinstance(record, dict):
            rows.append(_unreadable_row(entry, "not a JSON object"))
            continue
        detail = record.get("detail")
        detail = detail if isinstance(detail, dict) else {}
        slurm = detail.get("slurm")
        slurm = slurm if isinstance(slurm, dict) else {}
        try:
            mtime = entry.stat().st_mtime
        except OSError:
            mtime = 0.0
        finished = record.get("finished_unix")
        rows.append({
            "action_key": str(record.get("action_key") or entry.name[:-5]),
            "status": str(record.get("status") or UNKNOWN),
            "transport": _transport(record),
            "host": record.get("finished_host") or record.get("claimed_host"),
            "elapsed_s": detail.get("elapsed_s"),
            "returncode": detail.get("returncode"),
            # The action's own ending, on the records that carry one: the
            # launcher's status above is 1 for every failure.
            "action_returncode": detail.get("action_returncode"),
            "action_signal": detail.get("action_signal"),
            # The pull queue's records carry no receipt field at all, which is
            # a different thing from a receipt that was not published.
            "receipt_published": detail.get("receipt_published"),
            "slurm_state": slurm.get("state"),
            # Present on every row so a reader of the JSON can test one field
            # rather than the absence of one.
            "unreadable": None,
            "finished_unix": (
                float(finished)
                if isinstance(finished, (int, float)) else mtime
            ),
            "path": entry.path,
        })
    rows.sort(key=lambda row: row["finished_unix"], reverse=True)
    return rows


# --------------------------------------------------------------------------
# Printing
# --------------------------------------------------------------------------

def _cell(value: object) -> str:
    """One table cell, with the two kinds of missing kept distinct."""

    if value is None:
        return UNKNOWN
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, float):
        return f"{value:.1f}"
    if isinstance(value, (list, tuple)):
        return ",".join(str(part) for part in value) or ABSENT
    text = str(value)
    return text if text else ABSENT


def _action_returncode(ending: Mapping[str, object]) -> object:
    """The action's own status, when it is not already in the RC column."""

    action = ending.get("action_returncode")
    if not isinstance(action, int) or isinstance(action, bool):
        return ABSENT
    if action == ending.get("returncode"):
        return ABSENT
    signal = ending.get("action_signal")
    if isinstance(signal, int) and not isinstance(signal, bool):
        return f"signal {signal}"
    return action


def render_table(headers: Sequence[str], rows: Iterable[Sequence[object]],
                 ) -> list[str]:
    """Lay out one table as aligned lines, header included."""

    body = [[_cell(cell) for cell in row] for row in rows]
    widths = [len(head) for head in headers]
    for row in body:
        for column, cell in enumerate(row):
            widths[column] = max(widths[column], len(cell))
    lines = ["  ".join(
        head.ljust(widths[column]) for column, head in enumerate(headers)
    ).rstrip()]
    for row in body:
        lines.append("  ".join(
            cell.ljust(widths[column]) for column, cell in enumerate(row)
        ).rstrip())
    return lines


def _memory(row: Mapping[str, object]) -> str:
    alloc = row.get("memory_alloc_mib")
    total = row.get("memory_total_mib")
    return f"{_cell(alloc)}/{_cell(total)}"


def _cpus(row: Mapping[str, object]) -> str:
    return f"{_cell(row.get('cpus_alloc'))}/{_cell(row.get('cpus_idle'))}"


def _gres(row: Mapping[str, object]) -> str:
    """GRES in use out of GRES declared, or ``-`` on a node that declares none.

    A node with no GRES is a fact the controller stated, so it prints as absent
    rather than as unknown -- ``dl380g10`` has no GPU and that is its
    configuration, not a gap in this table.
    """

    if row.get("gres") is None:
        return ABSENT
    return f"{_cell(row.get('gres_used'))} of {_cell(row.get('gres'))}"


def node_lines(nodes: Sequence[Mapping[str, object]]) -> list[str]:
    """The nodes table, with anything not schedulable flagged and explained."""

    if not nodes:
        return ["no nodes; the controller listed none"]
    headers = (
        "NODE", "PARTITIONS", "STATE", "CPU A/I", "MEM A/T", "GRES",
        "FEATURES", "LOAD", "FLAG",
    )
    rows = []
    for node in nodes:
        flag = ABSENT
        if not node.get("healthy"):
            parts = list(node.get("flags") or [])
            reason = node.get("reason")
            # The controller's reason for an unreachable node is its own words
            # for the flag, so printing both says one thing twice.
            if reason and str(reason).lower() not in {
                part.lower() for part in parts
            }:
                parts.append(str(reason))
            flag = "; ".join(parts) if parts else "not schedulable"
        rows.append((
            node.get("node"), node.get("partitions"), node.get("state"),
            _cpus(node), _memory(node), _gres(node), node.get("features"),
            node.get("load"), flag,
        ))
    return render_table(headers, rows)


def job_lines(jobs: Sequence[Mapping[str, object]]) -> list[str]:
    """The jobs table, with the action key and its sealed placement joined in."""

    if not jobs:
        return ["no jobs queued or running"]
    headers = (
        "JOBID", "STATE", "PART", "NODE", "ELAPSED", "LIMIT", "USER", "KEY",
        "RESOURCES", "CONSTRAINT", "FROM", "REASON",
    )
    rows = []
    for job in jobs:
        resources = job.get("resources")
        if isinstance(resources, Mapping):
            resources = ",".join(
                f"{name}={value}" for name, value in sorted(resources.items())
            )
        reasons = [
            str(part) for part in (job.get("reason"), job.get("note")) if part
        ]
        rows.append((
            job.get("job_id"), job.get("state"), job.get("partition"),
            job.get("node") or ABSENT, job.get("elapsed"),
            job.get("time_limit"), job.get("user"),
            job.get("action_key_prefix") or ABSENT, resources,
            job.get("constraint"), job.get("submitted_host"),
            "; ".join(reasons) or ABSENT,
        ))
    return render_table(headers, rows)


def queue_root_note(queue_root: str | Path) -> str | None:
    """Why this root can file no endings, or ``None`` if it could file some.

    An empty table used to answer three questions with one sentence: a fleet
    that has filed nothing, a path that does not exist, and a path that is not
    a queue.  Only the first means wait.  A mistyped ``--queue-root`` read as
    the first, so an operator waited on a screen that could never fill.

    Never raises, for the same reason nothing else here does: a status screen
    that fails is a screen nobody can use to find out why.
    """

    root = Path(queue_root)
    try:
        if not root.exists():
            return f"no queue root at {root}: the path does not exist"
        if not root.is_dir():
            return f"no queue root at {root}: the path is not a directory"
        filed_in = [state for state in (pool.DONE, pool.FAILED, pool.WITHDRAWN)
                    if (root / state).is_dir()]
    except OSError as exc:
        reason = str(exc.strerror or type(exc).__name__).lower()
        return f"queue root {root} cannot be read: {reason}"
    if not filed_in:
        return (f"{root} is not a queue root: it holds no done/, failed/ or "
                "withdrawn/ directory")
    return None


def ending_lines(endings: Sequence[Mapping[str, object]],
                 *, note: str | None = None) -> list[str]:
    """The endings table: how the newest actions finished, under either transport.

    ``note`` replaces the empty-table line, so a root that can file nothing
    says which kind of empty it is rather than the one kind that means wait.
    """

    if not endings:
        return [note or "no endings filed under done/, failed/ or withdrawn/"]
    headers = (
        "KEY", "STATUS", "VIA", "HOST", "ELAPSED", "RC", "ACTION RC",
        "RECEIPT", "SLURM", "NOTE",
    )
    rows = []
    for ending in endings:
        elapsed = ending.get("elapsed_s")
        reason = ending.get("unreadable")
        rows.append((
            str(ending.get("action_key"))[:12], ending.get("status"),
            ending.get("transport"), ending.get("host"),
            f"{float(elapsed):.1f}s" if isinstance(elapsed, (int, float))
            else UNKNOWN,
            ending.get("returncode"),
            # `-` unless the transport recorded the action's own ending and it
            # differs from the launcher's status, which is 1 for every failure.
            _action_returncode(ending),
            # `-` where the record has no such field, `no` where it has one
            # saying no receipt was published.
            ABSENT if ending.get("receipt_published") is None
            else ending.get("receipt_published"),
            ending.get("slurm_state") or ABSENT,
            # The path and the reason, on the one kind of row where every
            # other column is inside a file nobody could read.
            f"{reason}: {ending.get('path')}" if reason else ABSENT,
        ))
    return render_table(headers, rows)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Show the fleet's nodes, its jobs, and how work ended.")
    parser.add_argument("--transport", choices=TRANSPORTS, default=None,
                        help="active scheduler (default: environment, then deployed runtime)")
    parser.add_argument(
        "--json", action="store_true",
        help="print one JSON object with the three lists and any scheduler "
             "notes, and nothing else")
    parser.add_argument(
        "--recent", type=int, default=DEFAULT_RECENT,
        help=f"how many endings to read (default {DEFAULT_RECENT})")
    parser.add_argument(
        "--lane-root", default=None,
        help="the SLURM lane root job names are resolved against (default: "
             f"${sl.LANE_ROOT_ENV}, else {sl.DEFAULT_LANE_ROOT})")
    parser.add_argument(
        "--queue-root", default=str(DEFAULT_QUEUE_ROOT),
        help=f"the queue root holding done/ and failed/ (default "
             f"{DEFAULT_QUEUE_ROOT})")
    parser.add_argument("--sinfo", default="sinfo", help=argparse.SUPPRESS)
    parser.add_argument("--squeue", default="squeue", help=argparse.SUPPRESS)
    parser.add_argument("--scontrol", default="scontrol", help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    transport = args.transport or default_transport()
    if args.recent < 0:
        parser.error("--recent cannot be negative")

    notes: list[str] = []
    nodes: list[dict] = []
    jobs: list[dict] = []
    node_note = job_note = detail_note = None
    pool_summary = None
    if transport == "pool":
        try:
            result = read_pool(args.queue_root)
            nodes, jobs, pool_summary = result['nodes'], result['jobs'], result['queue']
            notes.extend(result['notes'])
        except Exception as exc:                   # noqa: BLE001 - diagnostic
            node_note = job_note = f"pool: unavailable ({type(exc).__name__}: {exc})"
            pool_summary = {"ready": None, "claimed": None, "empty": None, "complete": False}
    else:
        try:
            nodes, detail_note = read_nodes(sinfo=args.sinfo, scontrol=args.scontrol)
        except SchedulerUnavailable as exc:
            node_note = str(exc)
        except Exception as exc:                   # noqa: BLE001 - diagnostic
            node_note = f"sinfo: unavailable ({type(exc).__name__})"
        try:
            jobs = read_jobs(squeue=args.squeue, lane_root=args.lane_root)
        except SchedulerUnavailable as exc:
            job_note = str(exc)
        except Exception as exc:                   # noqa: BLE001 - diagnostic
            job_note = f"squeue: unavailable ({type(exc).__name__})"
    for note in (node_note, detail_note, job_note):
        if note:
            notes.append(note)

    endings: list[dict] = []
    ending_note = None
    try:
        endings = read_endings(args.queue_root, limit=args.recent)
    except Exception as exc:                       # noqa: BLE001 - diagnostic
        ending_note = f"endings: unavailable ({type(exc).__name__})"
        notes.append(ending_note)
    # Asked only when the table came back empty, and asked then because an
    # empty table is three different answers.  A wrapper reads the `--json`
    # screen, so the diagnosis travels with the notes as well as under the
    # table.
    empty_note = None
    if ending_note is None and not endings:
        empty_note = queue_root_note(args.queue_root)
        if empty_note:
            notes.append(empty_note)

    if args.json:
        print(json.dumps({
            "schema": "prismabuild.pbstatus.v1",
            "transport": transport,
            "pool": pool_summary,
            "nodes": nodes,
            "jobs": jobs,
            "endings": endings,
            "scheduler": notes,
        }, sort_keys=True, indent=1))
        return 0

    print("== nodes")
    print("\n".join([node_note] if node_note else
                    pool_node_lines(nodes) if transport == "pool" else node_lines(nodes)))
    # Under the table, not instead of it: `sinfo` answered, and the two columns
    # `scontrol` fills read unknown for a reason the operator needs stated.
    if detail_note:
        print(detail_note)
    print()
    print("== jobs")
    print("\n".join([job_note] if job_note else
                    pool_job_lines(jobs, pool_summary) if transport == "pool" else job_lines(jobs)))
    if transport == "pool":
        for note in notes:
            print(note)
    print()
    print(f"== endings (newest {args.recent})")
    print("\n".join([ending_note] if ending_note
                     else ending_lines(endings, note=empty_note)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
