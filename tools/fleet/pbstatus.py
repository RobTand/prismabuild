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
import select
import signal
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

#: How long the whole run has to read the shared queue root.  A diagnostic
#: that can wait forever is worse than one that says it could not read the
#: mount: on 2026-09-07 fifteen ``pbstatus`` processes sat 33-49 minutes each
#: in ``__nfs_lookup_revalidate`` on sparky, at 0.0% CPU and 282 MB, and the
#: only thing they changed about the box was its load average, which they
#: inflated to ~14.7 while the GPU idled at 3%.  They were ``hard``-mount
#: waits doing what ``hard`` is for, not driver wedges, and they all exited on
#: their own when the mount cleared -- which is the argument for a deadline
#: rather than for reaping: the answer arrives eventually and is worthless
#: long before it does, because the operator cannot tell a slow mount from a
#: dead fleet while they wait.  Ten seconds is longer than a healthy census
#: (tens of milliseconds) by two orders of magnitude and shorter than a
#: person's patience.  Issue #350.
DEFAULT_TIMEOUT_S = 10.0

#: What the run exits with when the queue root did not answer.  Distinct from
#: ``1`` on purpose: a wrapper must be able to tell "I could not read the
#: fleet" from "I read it and something in it is wrong".
EXIT_INCOMPLETE = 3

#: How long to wait for a killed child before treating it as retained.  Same
#: reasoning as ``mount_latency.KILL_GRACE_S``: ``SIGKILL`` is delivered at the
#: child's next scheduling point, so an immediate ``WNOHANG`` says "still
#: running" about a child that is already dying.
KILL_GRACE_S = 0.25

#: The slice of the budget the wedged-peer scan may spend.  Capped so that a
#: scan which blocks cannot consume the census's budget: the peers this counts
#: are, by definition, tasks whose kernel state can hold a lock a reader of
#: ``/proc/<pid>/cmdline`` needs.
PEER_SCAN_BUDGET_S = 1.0

#: How many PIDs the peer scan will look at.  ``/proc`` on a busy box is large
#: and this is a warning line, not a census.
PEER_SCAN_MAX_PIDS = 4096

#: The basename a peer is recognised by, matched as a whole path component so
#: that a pytest process whose command line names ``test_pbstatus_pool.py``
#: is not counted as a wedged ``pbstatus``.
SCRIPT_NAME = Path(__file__).name

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


def _pool_sidecar(path: Path) -> dict | None | Exception:
    """Retain read failures for the row that owns this observation."""
    try:
        return pool._read_json(path)
    except (OSError, ValueError) as exc:
        return exc


def _admission_sample(record: dict | None | Exception, *, now: float, max_age_s: float) -> dict:
    """Classify persisted evidence after every census read has finished."""
    if isinstance(record, Exception):
        return {"state": "unavailable", "note": str(record)}
    if record is None:
        return {"state": "unavailable", "note": "no sample recorded"}
    age = _age(record.get("sampled_unix"), now)
    return {"state": "fresh" if age is not None and 0 <= age <= max_age_s else "stale",
            "age_s": age, "record": record}


def _valid_pool_offer(host: str, offer: dict | None) -> bool:
    return (offer is not None and re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]*', host) is not None
            and offer.get("schema") == pool.POOL_OFFER_SCHEMA_V1
            and offer.get("host") == host and isinstance(offer.get("capacity"), dict)
            and isinstance(offer.get("tags"), list)
            and all(type(v) is int and v >= 0 for v in offer['capacity'].values()))


def _valid_pool_item(key: str, record: dict | None) -> bool:
    return (record is not None and re.fullmatch('[a-f0-9]{64}', key) is not None
            and record.get('action_key') == key and record.get('schema') == pool.POOL_ITEM_SCHEMA_V1)


def read_pool(queue_root: str | Path) -> dict:
    """Read worker offers, active records and existing admission evidence.

    This is a non-atomic diagnostic census, never a new admission decision.
    File errors retain unknown counts and unreadable rows. PoolQueue supplies
    the resource and placement rules used by workers. Read all sidecars before
    sampling time, so a later stalled read cannot extend any earlier lifetime.
    """
    queue = pool.PoolQueue(Path(queue_root).absolute())
    workers, worker_notes = _pool_records(queue.root / pool.WORKERS)
    ready, ready_notes = _pool_records(queue.dir(pool.READY))
    claimed, claim_notes = _pool_records(queue.dir(pool.CLAIMED))
    admission = {}
    for host, offer in workers.items():
        if _valid_pool_offer(host, offer):
            base = queue.ledger(host).base / 'adaptive'
            admission[host] = {
                'cpu': _pool_sidecar(base / 'cpu-sample.json'),
                'gpu': _pool_sidecar(base / 'gpu-state.json') if offer.get('has_gpu') else None,
            }
    sidecars = {}
    for state, records in ((pool.READY, ready), (pool.CLAIMED, claimed)):
        for key, record in records.items():
            if _valid_pool_item(key, record):
                path = queue.passes_path(key) if state == pool.READY else queue.lease_path(key)
                sidecars[state, key] = _pool_sidecar(path)
    now = time.time()
    notes = [*worker_notes, *ready_notes, *claim_notes]
    # The half of ``notes`` that means "this census is missing something",
    # kept apart from the half that means "the fleet is in this state".  A
    # directory that would not list and a record that would not parse make the
    # census partial; a worker that stopped announcing is a fleet fact, fully
    # read.  ``complete`` below is the first, and only the first, because it is
    # what the run's exit status is derived from and a stale offer must not
    # spend the signal that says the mount did not answer.
    unreadable = [*worker_notes, *ready_notes, *claim_notes]
    nodes: list[dict] = []
    live: list[dict] = []
    for host, offer in workers.items():
        if not _valid_pool_offer(host, offer):
            nodes.append({"node": host, "state": "unreadable", "healthy": False,
                          "reason": "invalid worker offer"})
            notes.append(f"pool worker {host}: invalid worker offer")
            unreadable.append(f"pool worker {host}: invalid worker offer")
            continue
        age = _age(offer.get("announced_unix"), now)
        fresh = age is not None and 0 <= age <= pool.OFFER_TIMEOUT_S
        if fresh:
            live.append(offer)
        else:
            notes.append(f"pool worker {host}: stale or invalid offer timestamp")
        nodes.append({
            "node": host, "transport": "pool", "state": "live" if fresh else "stale",
            "healthy": fresh, "age_s": age, "capacity": offer.get("capacity"),
            "observed_capacity": offer.get("observed_capacity"),
            "foreign": offer.get("foreign"), "observed_detail": offer.get("observed_detail"),
            "features": offer.get("tags"), "has_gpu": offer.get("has_gpu"),
            "runtime_commit": offer.get("runtime_commit"),
            # Absent on a pre-#254 offer, and that is "not measured": the
            # reader below prints it as unknown and never as zero.
            "loops": offer.get("loops"),
            "timeout_ceiling_s": offer.get("timeout_ceiling_s"),
            "reason": None if fresh else "offer expired or timestamp invalid",
            "admission": {
                "cpu": _admission_sample(admission[host]['cpu'], now=now,
                                         max_age_s=pool.cpu_admission.MAX_SAMPLE_AGE_S),
                "gpu": _admission_sample(admission[host]['gpu'], now=now,
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
                if not _valid_pool_item(key, record):
                    raise ValueError('invalid action record')
                sidecar = sidecars[state, key]
                if isinstance(sidecar, Exception):
                    raise sidecar
                row.update(resources=queue.demand_of(record), constraint=record.get('tags'),
                           submitted_host=record.get('published_by'),
                           unstarted_releases=_releases(record),
                           age_s=_age(record.get('claimed_unix') if state == pool.CLAIMED
                                      else record.get('published_unix'), now))
                if state == pool.READY:
                    denial = sidecar or {}
                    count, first = denial.get('passes', 0), denial.get('first_unix')
                    hosts = sorted({str(offer['host']) for offer in queue._matching_offers(record, live=live)})
                    if pool.is_box_local_path(record.get('checkout_root')):
                        hosts = [host for host in hosts if host == record.get('published_by')]
                    row.update(placeable_hosts=hosts if live else None,
                               admission_passes=int(count) if isinstance(count, (int, float)) else 0,
                               admission_wait_s=max(0.0, now - float(first))
                               if isinstance(first, (int, float)) else 0.0)
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
                                                    or record.get('container_cleanup_pending')),
                               # How hard, and for how long.  A cleanup pending
                               # for three seconds and one pending for six hours
                               # over four hundred attempts wrote the same row,
                               # and an operator acts on them very differently
                               # (#288).  ``None`` on a record written before
                               # these fields existed.
                               cleanup_attempts=record.get('container_cleanup_attempts'),
                               cleanup_pending_s=_age(
                                   record.get('container_cleanup_first_failed_unix'), now))
                    beat = None if sidecar is None else sidecar.get('heartbeat_unix')
                    if sidecar is not None and not isinstance(beat, (int, float)):
                        raise pool.PoolContractError(f"lease has no heartbeat: {key}")
                    age = None if sidecar is None else now - float(beat)
                    row['lease_age_s'] = age
                    row['stale'] = age is None or not math.isfinite(age) or not 0 <= age <= pool.LEASE_TIMEOUT_S
                    if row['stale']:
                        row['reason'] = 'lease missing, stale or invalid; process liveness unknown'
                    elif row['cleanup_pending']:
                        row['reason'] = 'cleanup pending; reservation retained'
                        if row.get('cleanup_attempts'):
                            row['reason'] += (
                                f"; {row['cleanup_attempts']} attempt"
                                f"{'' if row['cleanup_attempts'] == 1 else 's'}")
                            if row.get('cleanup_pending_s') is not None:
                                row['reason'] += (
                                    f" over {row['cleanup_pending_s']:.0f}s")
            except (OSError, ValueError, TypeError, KeyError) as exc:
                row.update(state='UNREADABLE', reason=str(exc))
                valid_counts[state] = None
                notes.append(f"pool {state}/{key}: {exc}")
                unreadable.append(f"pool {state}/{key}: {exc}")
            jobs.append(row)
    empty = (valid_counts[pool.READY] == 0 and valid_counts[pool.CLAIMED] == 0)
    return {"nodes": nodes, "jobs": jobs, "notes": notes,
            "queue": {"ready": valid_counts[pool.READY], "claimed": valid_counts[pool.CLAIMED],
                      "empty": empty if all(v is not None for v in valid_counts.values()) else None,
                      "complete": not unreadable, "unreadable": unreadable,
                      "live_workers": len(live),
                      "sampled_unix": now}}


def pool_node_lines(nodes: Sequence[Mapping[str, object]]) -> list[str]:
    if not nodes:
        return ["no worker offers recorded"]
    return render_table(
        ("NODE", "STATE", "OFFER AGE", "LOOPS", "KILL AT", "CAPACITY", "OBSERVED",
         "ADMISSION", "NOTE"), (
            (n['node'], n['state'], n.get('age_s'),
             ABSENT if n.get('loops') is None else n['loops'],
             # What this box will kill an action at, whatever --timeout-s asked
             # for.  ABSENT is "the offer predates the field", not "no limit".
             ABSENT if n.get('timeout_ceiling_s') is None
             else f"{float(n['timeout_ceiling_s']):g}s",
             n.get('capacity'), n.get('observed_capacity'),
             ', '.join(f"{k}: {v['state']}" for k, v in n.get('admission', {}).items()),
             n.get('reason'))
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
    withdrawal_mtimes: dict[str, float] = {}
    for state in (pool.DONE, pool.FAILED, pool.WITHDRAWN):
        directory = Path(queue_root) / state
        try:
            with os.scandir(directory) as scan:
                for entry in scan:
                    if not entry.name.endswith(".json") or not entry.is_file():
                        continue
                    try:
                        entries.append((entry.stat().st_mtime, entry))
                        if state == pool.WITHDRAWN:
                            withdrawal_mtimes[entry.name[:-5]] = entry.stat().st_mtime
                    except OSError:
                        continue
        except OSError:
            continue
    # A cancellation is durable before its visible summary is written. Keep
    # that ending visible if the operator crashed between the two writes.
    decisions = Path(queue_root) / pool.WITHDRAWN / "decisions"
    try:
        with os.scandir(decisions) as scan:
            directories = [entry for entry in scan if entry.is_dir()]
    except OSError:
        directories = []
    for directory in directories:
        try:
            with os.scandir(directory.path) as scan:
                candidates = [(entry.stat().st_mtime, entry) for entry in scan
                              if entry.name.endswith(".json")]
        except OSError:
            continue
        if candidates:
            newest = max(candidates, key=lambda pair: pair[0])
            if newest[0] > withdrawal_mtimes.get(directory.name, float("-inf")):
                entries.append(newest)
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
        "action_key": (Path(entry.path).parent.name
                       if Path(entry.path).parent.parent.name == "decisions"
                       else entry.name[:-5]),
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
            path = Path(entry.path)
            if path.parent.parent.name == "decisions":
                record = pool.PoolQueue(Path(queue_root))._read_withdrawal_decision(path)
            else:
                record = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError, pool.PoolContractError) as exc:
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
            # The box the action was ON, not the box that filed its ending.
            # ``reap_stale`` stamps itself as ``finished_host`` -- correctly,
            # since the reaper is who filed the record -- and it files most of
            # the fleet's lost claims, so preferring that field named
            # whichever box happened to sweep.  That is how a diagnosis
            # started on the wrong box, and why the bias is not cosmetic: the
            # box that reaps most is the box that runs most, so misattributed
            # failures pile onto the machine that already looks busiest.
            #
            # ``finished_host`` stays as the fallback rather than being
            # dropped, because it is the only box an ending with no claimant
            # names at all -- a cache hit, or a SLURM record filed by the
            # waiter (#227, #262).
            "host": record.get("claimed_host") or record.get("finished_host"),
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


class Deadline:
    """The whole run's budget, so that one slow section cannot spend another's.

    Held in ``time.monotonic`` because a status screen must not change its
    mind about how long it has waited when NTP steps the clock.  A budget of
    zero or less means no deadline at all, which is what this command did
    before issue #350 and what ``--timeout-s 0`` still asks for.
    """

    def __init__(self, timeout_s: float) -> None:
        self.timeout_s = float(timeout_s)
        self.bounded = self.timeout_s > 0
        self._expires = time.monotonic() + self.timeout_s if self.bounded else None

    def remaining(self) -> float | None:
        if self._expires is None:
            return None
        return self._expires - time.monotonic()


def _proc_stat_fields(pid: int, proc: Path) -> list[bytes] | None:
    """The fields of ``/proc/<pid>/stat`` after ``comm``, or ``None``.

    Split on the last ``)`` because ``comm`` is the only field that can hold a
    space or a parenthesis.  ``fields[0]`` is the state and ``fields[19]`` is
    ``starttime`` -- fields 3 and 22 of ``proc(5)``.
    """
    try:
        raw = (proc / str(pid) / "stat").read_bytes()
    except OSError:
        return None
    close = raw.rfind(b")")
    if close < 0:
        return None
    fields = raw[close + 2:].split()
    return fields if len(fields) > 19 else None


def wedged_peers(*, proc: Path = Path("/proc"), self_pid: int | None = None,
                 limit: int = PEER_SCAN_MAX_PIDS,
                 budget_s: float = PEER_SCAN_BUDGET_S) -> dict:
    """Count other ``pbstatus`` processes already in uninterruptible sleep.

    This is the signal the operator actually wants and the one the wedged
    instances themselves cannot give: the fifteen on sparky were only found
    because somebody went looking for something else.  It never refuses to
    run on the strength of it -- a refusal would hide the fleet from the one
    person trying to see it, at exactly the moment the fleet is worth seeing.

    ``/proc/<pid>/stat`` is read for every candidate and
    ``/proc/<pid>/cmdline`` only for the ones already in ``D``, because
    reading another task's command line takes that task's ``mmap_read_lock``
    and a process blocked in an NFS page fault holds it.  The scan is bounded
    by both a PID count and a wall clock, and its caller runs it in an
    abandonable child for the case where a single ``cmdline`` read blocks
    anyway.  That child is killable even when it does block: current kernels
    take the lock through ``mmap_read_lock_killable``, so the scan path
    answers ``SIGKILL`` and does not itself leave the corpses this counts.
    """
    self_pid = os.getpid() if self_pid is None else self_pid
    started = time.monotonic()
    try:
        ticks = float(os.sysconf("SC_CLK_TCK")) or 100.0
    except (ValueError, OSError):                  # pragma: no cover - libc
        ticks = 100.0
    try:
        uptime_s = float((proc / "uptime").read_text().split()[0])
    except (OSError, ValueError, IndexError):
        uptime_s = None
    try:
        names = os.listdir(proc)
    except OSError as exc:
        return {"peers": [], "scanned": 0, "truncated": False,
                "note": f"{proc} could not be listed: {exc}"}

    peers: list[dict] = []
    scanned = 0
    truncated = False
    for name in names:
        if not name.isdigit():
            continue
        pid = int(name)
        if pid == self_pid:
            continue
        if scanned >= limit or time.monotonic() - started > budget_s:
            truncated = True
            break
        scanned += 1
        fields = _proc_stat_fields(pid, proc)
        if fields is None or fields[0] != b"D":
            continue
        try:
            argv = (proc / name / "cmdline").read_bytes().split(b"\0")
        except OSError:
            continue
        if not any(os.path.basename(arg.decode("utf-8", "replace")) == SCRIPT_NAME
                   for arg in argv if arg):
            continue
        age_s = None
        if uptime_s is not None:
            try:
                age_s = round(max(0.0, uptime_s - int(fields[19]) / ticks), 1)
            except (ValueError, ZeroDivisionError):
                age_s = None
        peers.append({"pid": pid, "age_s": age_s})
    peers.sort(key=lambda peer: (peer["age_s"] is None, -(peer["age_s"] or 0.0)))
    return {"peers": peers, "scanned": scanned, "truncated": truncated, "note": None}


def _reap_within(pid: int, grace_s: float) -> bool:
    """Poll for a child's exit for a bounded time.  Never blocks in ``wait``.

    Copied in shape from ``tools/fleet/mount_latency.py`` ``_reap_within``:
    an unreaped child in ``D`` is exactly what this command must not join.
    """
    deadline = time.monotonic() + grace_s
    while True:
        try:
            done, _status = os.waitpid(pid, os.WNOHANG)
        except ChildProcessError:
            return True
        except OSError:
            return False
        if done == pid:
            return True
        if time.monotonic() >= deadline:
            return False
        time.sleep(0.01)


def _isolate_child_fds(write_fd: int) -> int:
    """Leave the forked reader owning its own pipe and nothing else.

    A child this process may have to abandon inherits every descriptor the
    caller held, and a descriptor is not a private copy: the open file
    description behind it is shared, so an abandoned reader keeps the caller's
    resources alive after the caller has let go of them.  Two consequences were
    reproduced by the review of #358, and they are one defect seen twice.

    A wrapper that captures ``pbstatus``'s output waits for EOF on the pipe, and
    EOF arrives when the last writer closes.  The parent exiting ``3`` is not
    that: the abandoned child still holds the write end, so the wrapper blocks
    on a command that has already returned.  And an ``flock`` lives on the open
    file description rather than on the process, so an inherited lock fd left
    open in the child holds the caller's lock after the caller closed it --
    against a lock the child was never told about and cannot release.

    So the child keeps exactly two things: the IPC writer it must answer on,
    and ``/dev/null`` on the three standard streams so that a write from
    anything it calls has somewhere to go.  Everything else is closed here,
    before the section runs.  ``/proc/self/fd`` is the list the kernel already
    keeps; the ``SC_OPEN_MAX`` sweep is for a box without ``/proc`` mounted,
    where a status screen still has to answer.

    Returns the descriptor the caller must write on, which is ``write_fd``
    moved out of the way first when the pipe landed on 0, 1 or 2.
    """
    if write_fd < 3:
        write_fd = os.dup(write_fd)
    null = os.open(os.devnull, os.O_RDWR)
    for target in (0, 1, 2):                       # write_fd is above these now
        try:
            os.dup2(null, target)
        except OSError:                            # pragma: no cover - kernel
            pass
    if null > 2:
        try:
            os.close(null)
        except OSError:                            # pragma: no cover - kernel
            pass
    try:
        # Materialised before any closing: the listing's own directory
        # descriptor is gone by the time this list is walked, and closing a
        # closed descriptor is the ``OSError`` swallowed below.
        open_fds = [int(name) for name in os.listdir("/proc/self/fd")
                    if name.isdigit()]
    except OSError:                                # pragma: no cover - no /proc
        try:
            limit = int(os.sysconf("SC_OPEN_MAX"))
        except (ValueError, OSError):
            limit = 4096
        open_fds = list(range(3, min(limit, 65536)))
    for fd in open_fds:
        if fd <= 2 or fd == write_fd:
            continue
        try:
            os.close(fd)
        except OSError:
            pass
    return write_fd


def bounded(section: str, read, *, deadline: Deadline, abandoned: list,
            cap_s: float | None = None) -> dict:
    """Run one read of the shared mount in a child this process can abandon.

    The shape is ``tools/fleet/mount_latency.py`` ``MountSampler._run_probe``,
    and for its reason: a ``stat`` on a hard NFS mount need not return at the
    caller's deadline, even after a signal, so no in-process timeout -- thread,
    alarm or otherwise -- can bound it.  Only a separate process can be
    abandoned.  The parent stops reading at the deadline and never joins a
    child that may still be blocked in the kernel.

    Two differences from the sampler, both because this is a short-lived
    command rather than a long-lived daemon.  The whole-run deadline means an
    expiry in one section leaves nothing for the next, so at most one
    queue-root child is ever abandoned per run; and an abandoned child is
    recorded by PID and ``starttime`` in ``abandoned`` so the report names
    exactly which process it left behind, rather than leaving the operator to
    guess which of the ``D``-state peers was this run's.

    The returned dict is one of:

    ``{"status": "ok", "value": ...}``
        the read completed; ``value`` has been through JSON when a child ran.
    ``{"status": "error", "type": ..., "error": ...}``
        the read raised, exactly as it would have in-process.
    ``{"status": "timed_out", "elapsed_s": ...}``
        the deadline expired first, or a previous section had already spent
        the budget and this one was never started.
    """
    if not deadline.bounded:
        # ``--timeout-s 0``: the pre-#350 path, in-process and unchanged.
        try:
            return {"status": "ok", "value": read()}
        except Exception as exc:                   # noqa: BLE001 - diagnostic
            return {"status": "error", "type": type(exc).__name__, "error": str(exc)}

    remaining = deadline.remaining() or 0.0
    if cap_s is not None:
        remaining = min(remaining, cap_s)
    if remaining <= 0:
        return {"status": "timed_out", "elapsed_s": 0.0, "started": False}

    read_fd, write_fd = os.pipe()
    started = time.monotonic()
    pid = os.fork()
    if pid == 0:                                   # child
        code = 1
        try:
            os.close(read_fd)
            # Before the section runs, and before anything can block in it:
            # what this child still holds is what it holds for as long as it
            # lives, and this is the child the parent may have to abandon.
            write_fd = _isolate_child_fds(write_fd)
            try:
                payload = json.dumps({"status": "ok", "value": read()})
                code = 0
            except Exception as exc:               # noqa: BLE001 - diagnostic
                payload = json.dumps({"status": "error",
                                      "type": type(exc).__name__,
                                      "error": str(exc)})
                code = 0
            os.write(write_fd, payload.encode("utf-8"))
        except BaseException:                      # noqa: BLE001 - last resort
            pass
        finally:
            try:
                os.close(write_fd)
            except OSError:
                pass
            # ``_exit``, never ``exit``: the child inherited this process's
            # buffered stdout and must not flush a second copy of it -- into
            # ``/dev/null`` now, but ``exit`` would also run the parent's
            # ``atexit`` handlers and finalisers, which are not this child's
            # to run.
            os._exit(code)

    os.close(write_fd)
    chunks: list[bytes] = []
    saw_eof = False
    while True:
        left = deadline.remaining() or 0.0
        if cap_s is not None:
            left = min(left, cap_s - (time.monotonic() - started))
        if left <= 0:
            break
        try:
            ready, _, _ = select.select([read_fd], [], [], left)
        except OSError:
            break
        if not ready:
            break
        try:
            chunk = os.read(read_fd, 65536)
        except OSError:
            break
        if not chunk:
            saw_eof = True
            break
        chunks.append(chunk)
    try:
        os.close(read_fd)
    except OSError:
        pass
    elapsed = time.monotonic() - started

    # EOF is the only proof the payload is whole.  A census can be larger than
    # the pipe buffer, so "some bytes arrived" is compatible with a child that
    # is still writing, and reporting that as a parse error would file a mount
    # timeout under the wrong cause.
    if saw_eof and chunks:
        _reap_within(pid, KILL_GRACE_S)
        try:
            return json.loads(b"".join(chunks).decode("utf-8"))
        except ValueError as exc:
            return {"status": "error", "type": "ValueError",
                    "error": f"unreadable {section} payload: {exc}"}
    if saw_eof:
        _reap_within(pid, KILL_GRACE_S)
        return {"status": "error", "type": "RuntimeError",
                "error": f"the {section} reader exited without a payload"}

    # Nothing whole came back in time.  ``SIGKILL`` stops a runnable child and
    # some killable kernel waits; an uninterruptible one may remain, and a
    # timeout is not a reaping proof.  Verify, and retain ownership of what
    # did not exit.
    try:
        os.kill(pid, signal.SIGKILL)
    except OSError:
        pass
    if not _reap_within(pid, KILL_GRACE_S):
        abandoned.append({"section": section, "pid": pid,
                          "starttime_ticks": _starttime_ticks(pid),
                          "since_unix": round(time.time() - elapsed, 3)})
    return {"status": "timed_out", "elapsed_s": round(elapsed, 3), "started": True}


def _starttime_ticks(pid: int) -> int | None:
    """The child's ``starttime``, which is what makes the PID an identity.

    A PID alone is reusable and therefore not ownership: #349 asks for exact
    retained ownership of an abandoned reader, and ``(pid, starttime)`` is the
    pair the kernel will not hand to a different process.
    """
    fields = _proc_stat_fields(pid, Path("/proc"))
    if fields is None:
        return None
    try:
        return int(fields[19])
    except ValueError:
        return None

def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Show the fleet's nodes, its jobs, and how work ended.",
        epilog=f"Exit status: 0 the fleet was read whole; {EXIT_INCOMPLETE} "
               f"some of it could not be read -- a section ran out of "
               f"--timeout-s, a section raised, or the pool census held records "
               f"nobody could read -- and the output printed above is partial, "
               f"not an empty fleet.")
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
    parser.add_argument(
        "--timeout-s", type=float, default=DEFAULT_TIMEOUT_S,
        help=f"how long the whole run may spend reading the queue root "
             f"(default {DEFAULT_TIMEOUT_S:g}; 0 waits indefinitely, which is "
             f"what a hard mount will do). On expiry this prints what it read, "
             f"says which section is missing, and exits {EXIT_INCOMPLETE}")
    parser.add_argument("--sinfo", default="sinfo", help=argparse.SUPPRESS)
    parser.add_argument("--squeue", default="squeue", help=argparse.SUPPRESS)
    parser.add_argument("--scontrol", default="scontrol", help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    transport = args.transport or default_transport()
    if args.recent < 0:
        parser.error("--recent cannot be negative")
    if args.timeout_s < 0:
        parser.error("--timeout-s cannot be negative; 0 means no deadline")

    deadline = Deadline(args.timeout_s)
    abandoned: list[dict] = []
    timed_out: list[str] = []
    # Kept apart from ``timed_out`` on purpose.  "The mount did not answer in
    # time" and "the read raised" are different faults with different fixes,
    # and collapsing them would lose the half a wrapper can act on; what they
    # have in common is only that neither read the fleet.
    unavailable: list[dict] = []
    pool_partial = False

    # Before anything touches the mount, and to stderr so it cannot be
    # mistaken for part of the census.  Counted, never enforced: an operator
    # running this during an outage needs the fleet, not a refusal.
    scan = bounded("wedged-peers", wedged_peers, deadline=deadline,
                   abandoned=abandoned, cap_s=PEER_SCAN_BUDGET_S)
    peer_note = None
    if scan["status"] == "ok" and scan["value"]["peers"]:
        peers = scan["value"]["peers"]
        oldest = peers[0]["age_s"]
        peer_note = (
            f"pbstatus: {len(peers)} other pbstatus process"
            f"{'' if len(peers) == 1 else 'es'} on this box "
            f"{'is' if len(peers) == 1 else 'are'} in uninterruptible sleep"
            + (f", oldest {oldest:.0f}s" if oldest is not None else "")
            + f" (pid{'' if len(peers) == 1 else 's'} "
            + ",".join(str(peer["pid"]) for peer in peers[:8])
            + ")"
            # A truncated scan stopped before the end of /proc, so the count
            # above is a floor, not the answer.  Saying "3" when the true
            # number is 15 is the same class of lie as an empty listing that
            # is really a wedged mount, so the line says which one it is.
            + (f" (scan stopped after {scan['value']['scanned']} pids, "
               "so this count is a floor)"
               if scan["value"]["truncated"] else "")
            + "; the shared mount is probably not answering")
    elif scan["status"] == "timed_out":
        peer_note = ("pbstatus: the scan for wedged pbstatus peers did not "
                     "finish; the count is unknown")
    if peer_note:
        print(peer_note, file=sys.stderr)

    notes: list[str] = []
    nodes: list[dict] = []
    jobs: list[dict] = []
    node_note = job_note = detail_note = None
    pool_summary = None
    if transport == "pool":
        read = bounded("pool", lambda: read_pool(args.queue_root),
                       deadline=deadline, abandoned=abandoned)
        if read["status"] == "ok":
            result = read["value"]
            nodes, jobs, pool_summary = result['nodes'], result['jobs'], result['queue']
            notes.extend(result['notes'])
            # The census answered and is still missing rows.  Nothing timed out
            # and nothing raised, so this is the only place the run learns it
            # did not read the whole queue.
            pool_partial = pool_summary.get("complete") is False
        elif read["status"] == "error":
            node_note = job_note = f"pool: unavailable ({read['type']}: {read['error']})"
            unavailable.append({"section": "pool", "type": read["type"],
                                "error": read["error"]})
            pool_summary = {"ready": None, "claimed": None, "empty": None,
                            "complete": False, "unreadable": [node_note]}
        else:
            timed_out.append("pool")
            node_note = job_note = (
                f"pool: incomplete -- queue root did not answer within "
                f"{args.timeout_s:g}s")
            pool_summary = {"ready": None, "claimed": None, "empty": None,
                            "complete": False, "unreadable": [node_note]}
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
    read = bounded("endings", lambda: read_endings(args.queue_root, limit=args.recent),
                   deadline=deadline, abandoned=abandoned)
    if read["status"] == "ok":
        endings = read["value"]
    elif read["status"] == "error":
        ending_note = f"endings: unavailable ({read['type']})"
        unavailable.append({"section": "endings", "type": read["type"],
                            "error": read["error"]})
        notes.append(ending_note)
    else:
        timed_out.append("endings")
        ending_note = (f"endings: incomplete -- queue root did not answer "
                       f"within {args.timeout_s:g}s")
        notes.append(ending_note)
    # Asked only when the table came back empty, and asked then because an
    # empty table is three different answers.  A wrapper reads the `--json`
    # screen, so the diagnosis travels with the notes as well as under the
    # table.
    empty_note = None
    if ending_note is None and not endings:
        read = bounded("queue-root", lambda: queue_root_note(args.queue_root),
                       deadline=deadline, abandoned=abandoned)
        if read["status"] == "ok":
            empty_note = read["value"]
        elif read["status"] == "error":
            # Why the endings table is empty is itself unread.  Before, this
            # branch fell through silently and the empty table printed as
            # though the question had been asked and answered.
            unavailable.append({"section": "queue-root", "type": read["type"],
                                "error": read["error"]})
            empty_note = (f"queue root: unreadable ({read['type']}: "
                          f"{read['error']}); this table is not an empty queue")
        elif read["status"] == "timed_out":
            timed_out.append("queue-root")
            empty_note = (f"queue root: incomplete -- it did not answer within "
                          f"{args.timeout_s:g}s; this table is not an empty queue")
        if empty_note:
            notes.append(empty_note)

    # Whole means every required section read, and read entirely: the deadline
    # held, nothing raised, and the pool census came back with every record
    # legible.  Any one of those failing is a partial answer, and a partial
    # answer that calls itself complete is worse than no answer.  What this
    # does not cover is SLURM reachability: `sinfo` or `squeue` missing is a
    # statement about the scheduler, is reported in `scheduler`, and has always
    # exited 0.
    complete = not timed_out and not unavailable and not pool_partial

    if args.json:
        print(json.dumps({
            "schema": "prismabuild.pbstatus.v1",
            "transport": transport,
            # New with #350 and the reason the exit code exists: a reader that
            # keys on the lists alone cannot tell a truncated census from an
            # empty fleet, and those are the two answers that matter.  It is
            # every way the read fell short, not only the deadline: a prompt
            # ``PermissionError`` on the queue root also produces empty lists,
            # and certifying that as a complete read of an empty fleet is the
            # same lie the exit code exists to stop.
            "complete": complete,
            "timed_out_sections": timed_out,
            # Read, but not readable: the section raised.  Separate from
            # ``timed_out_sections`` because a wrapper's next move differs --
            # a timeout says look at the mount, this says look at the error.
            "unavailable_sections": unavailable,
            # Which process, exactly, this run left behind -- ``(pid,
            # starttime)`` rather than a PID, because a PID is reusable and
            # therefore not an identity (#349).
            "abandoned_children": abandoned,
            "pool": pool_summary,
            "nodes": nodes,
            "jobs": jobs,
            "endings": endings,
            "scheduler": notes,
        }, sort_keys=True, indent=1))
        return _incomplete(timed_out, unavailable, pool_partial, args.timeout_s)

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
    return _incomplete(timed_out, unavailable, pool_partial, args.timeout_s)


def _incomplete(timed_out: Sequence[str], unavailable: Sequence[Mapping[str, object]],
                pool_partial: bool, timeout_s: float) -> int:
    """Say what is missing, on stderr, and exit distinctly.

    On stderr because the tables above it are real and a wrapper capturing
    stdout should keep them; distinctly because "I could not read the fleet"
    and "I read it and it is empty" are the two answers this command existed
    to confuse, and a truncated listing that exits 0 is the more dangerous
    half.  A section that raised gets its own line rather than being folded
    into the deadline's: both mean the census is partial, and only one means
    the mount is slow.
    """
    if not (timed_out or unavailable or pool_partial):
        return 0
    if timed_out:
        print(f"pbstatus: incomplete -- queue root did not answer within "
              f"{timeout_s:g}s ({', '.join(timed_out)} pending)", file=sys.stderr)
    for section in unavailable:
        print(f"pbstatus: incomplete -- the {section['section']} read failed "
              f"({section['type']}: {section['error']})", file=sys.stderr)
    if pool_partial:
        print("pbstatus: incomplete -- the pool census could not read every "
              "record; the counts above are partial", file=sys.stderr)
    return EXIT_INCOMPLETE


if __name__ == "__main__":
    raise SystemExit(main())
