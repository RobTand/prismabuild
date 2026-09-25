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
from contextlib import contextmanager
import contextvars
import importlib.util
import json
import math
import os
from pathlib import Path
import re
import select
import shlex
import signal
import stat
import subprocess
import sys
import time
from collections.abc import Iterable, Mapping, Sequence

sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
from runtime_paths import generation_root  # noqa: E402
from fleet_submit import TRANSPORTS, default_transport  # noqa: E402
import upgrade_client  # noqa: E402

RUNTIME_ROOT = generation_root(__file__)
sys.path.insert(0, str(RUNTIME_ROOT / "src"))
from prismabuild import (  # noqa: E402
    core as pb, pool, slurm_lane as sl,
)
from prismabuild import action_edges  # noqa: E402
from prismabuild import produced_output  # noqa: E402
from prismabuild import residency_map, residency_plan, storage_tiers  # noqa: E402
from prismabuild import window_credit  # noqa: E402
from prismabuild.core import _sigterm_unwinds_this_process  # noqa: E402

#: Where the fleet keeps the queue both transports file their endings in.  The
#: same spelling ``pbrun``, ``pool_reset`` and ``tessera_status`` use.
SHARED_ROOT = Path("/mnt/shared/prismabuild-fleet")
DEFAULT_QUEUE_ROOT = SHARED_ROOT / "pb-queue"


def read_rollout_summary(repo_link: str | Path, epoch: str | None = None) -> dict:
    """Read an active barrier, with no epoch as the normal idle state."""
    runtime = Path(repo_link)
    snapshot = upgrade_client.read_rollout({
        "runtime": str(runtime),
        "generation_store": str(runtime.parent / "runtime-generations"),
        "rollout_root": str(runtime.parent / "rollout"),
    }, epoch=epoch)
    if snapshot is None:
        return {"state": "idle", "epoch": None, "pending_phase": None,
                "outstanding_hosts": [], "failed_hosts": []}
    intent, markers = snapshot["intent"], snapshot["markers"]
    roster = list(intent["roster"])
    def missing(phase):
        return [host for host in roster
                if upgrade_client.marker_name(host, phase) not in markers]
    def decision(phase):
        return upgrade_client.marker_name(None, phase) in markers
    failed = [host for host in roster
              if upgrade_client.marker_name(host, "failed") in markers]
    if decision("terminal"):
        pending, outstanding = "terminal", []
    elif decision("rollback"):
        outstanding = missing("rolled-back")
        if outstanding: pending = "rolled-back"
        elif not decision("reverted"): pending = "reverted"
        elif not decision("resume"): pending = "resume"
        else:
            outstanding = missing("resumed")
            pending = "resumed" if outstanding else "terminal"
    else:
        outstanding = missing("drained")
        if outstanding: pending = "drained"
        elif not decision("activated"): pending = "activated"
        else:
            outstanding = missing("rotated")
            if outstanding: pending = "rotated"
            elif not decision("resume"): pending = "resume"
            else:
                outstanding = missing("resumed")
                pending = "resumed" if outstanding else "terminal"
    return {"state": "active", "epoch": intent["epoch"],
            "from_generation": intent["from_generation"],
            "to_generation": intent["to_generation"],
            "live_generation": snapshot["live_generation"],
            "intent_sha256": snapshot["intent_sha256"], "roster": roster,
            "pending_phase": pending, "outstanding_hosts": outstanding,
            "failed_hosts": failed}


def rollout_lines(summary: Mapping[str, object] | None) -> list[str]:
    if summary is None:
        return ["rollout status unavailable"]
    if summary.get("state") == "idle":
        return ["no active rollout epoch"]
    outstanding = ", ".join(summary.get("outstanding_hosts") or []) or ABSENT
    return [f"epoch {summary.get('epoch')}  live {summary.get('live_generation')}",
            f"pending {summary.get('pending_phase')}  outstanding {outstanding}"]

#: How long any one scheduler command has to answer.  Shorter than the lane's
#: own 60 s, deliberately: a ``pbrun`` waiting on a job is willing to wait for
#: a busy controller, and a person looking at a status screen is not.
COMMAND_TIMEOUT_S = 20.0

# Terminal rows are summaries, but pull-queue writers can copy complete action
# logs into them. One audit result made a single row 213.2 MiB, so merely asking
# for recent status expanded the collector past its service memory limit. The
# immutable attempt log remains the interface for fetching full output.
MAX_ENDING_RECORD_BYTES = 8 * 1024 * 1024

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

#: The host helper the NFS readahead reading is taken through, relative to the
#: runtime generation this command was launched from.  It is a script rather
#: than an importable module because an operator installs a copy of it as root
#: on each client, so it is loaded by path.  Absent from a generation published
#: before it travelled, which the reading reports rather than raising on.
NFS_READAHEAD_HELPER = "fleet/storage/nfs_readahead.py"
HOST_STORAGE_BUDGET_S = 0.25

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


#: The reader that serves this census's queue reads while one is installed
#: (:func:`kept_reads`), or ``None`` for the plain reads below.  ``pbmetrics``
#: installs the one it keeps between scrapes, so a directory nothing was filed
#: in since the last scrape is not listed again and a record whose version
#: holds is not read again (#1020).  Every other caller reads plainly.
_KEPT_READS: contextvars.ContextVar[object | None] = contextvars.ContextVar(
    "pbstatus_kept_reads", default=None)


@contextmanager
def kept_reads(reader):
    """Serve the census's directory and sidecar reads from ``reader``.

    ``reader`` answers :func:`_pool_records`, :func:`_pool_sidecar`,
    :func:`_starvation_sidecar`, :func:`read_endings` and the claim-denial
    host listing with the same values and the same failures the plain reads
    return.  ``pbmetrics`` passes the reader it keeps between scrapes
    (#1020); nothing else sets one, so every other caller reads afresh.
    """

    token = _KEPT_READS.set(reader)
    try:
        yield reader
    finally:
        _KEPT_READS.reset(token)


def _pool_records(directory: Path) -> tuple[dict[str, dict | None], list[str]]:
    """Audit a live directory without treating failed reads as an empty queue."""
    reader = _KEPT_READS.get()
    if reader is not None:
        return reader.pool_records(directory)
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


def _progress_observation(claim: dict, lease: dict | None, *, now: float) -> str | None:
    """How long this action has been quiet, against what it is allowed.

    The column beside it -- OUTPUT age -- is the number #480 says proves
    nothing: a launcher writing log lines is not an application committing
    work.  This is the other reading, and it is the one the watchdog acts on.
    ``None`` when the action declared no policy, so a whole-run ceiling still
    governs it and ``KILL AT`` is the number to read.

    Held to the same attempt identity as the execution observation: a lease
    naming another claim describes another run.
    """

    value = (lease or {}).get("progress_observation")
    if not isinstance(value, dict) or lease is None:
        return None
    if any(lease.get(left) is None or lease.get(left) != claim.get(right)
           for left, right in _OBSERVATION_IDENTITY):
        return None
    quiet, grace = value.get("quiet_s"), value.get("grace_s")
    if not all(type(v) in (int, float) and math.isfinite(v) and v >= 0
               for v in (quiet, grace)):
        return None
    phase = value.get("phase")
    accepted = value.get("accepted_count")
    said = f"{phase if isinstance(phase, str) and phase else '?'} "
    said += f"quiet {float(quiet):.0f}/{float(grace):g}s"
    if type(accepted) is int:
        said += f" ({accepted} accepted)"
    return said


#: What a lease must say about itself before either observation on it is read.
_OBSERVATION_IDENTITY = (("action_key", "action_key"), ("owner", "claimed_by"),
                         ("host", "claimed_host"), ("claimed_unix", "claimed_unix"),
                         ("published_unix", "published_unix"))


def _execution_observation(claim: dict, lease: dict | None, *, now: float) -> dict:
    """Interpret optional execution evidence without granting recovery rights."""
    unknown = {"state": "unavailable", "launcher_alive": None,
               "age_s": None, "last_output_age_s": None,
               "note": "execution observation unavailable"}
    value = (lease or {}).get("execution_observation")
    if value is None:
        return unknown
    if (not isinstance(value, dict)
            or any(lease.get(left) is None or lease.get(left) != claim.get(right)
                   for left, right in _OBSERVATION_IDENTITY)):
        return {**unknown, "state": "invalid", "note": "execution observation identity invalid"}
    sampled = value.get("sampled_unix")
    output = value.get("last_output_unix")
    try:
        age = _age(sampled, now)
        beat_age = _age(lease.get("heartbeat_unix"), now)
        output_age = _age(output, now)
    except OverflowError:
        return {**unknown, "state": "invalid", "note": "execution observation timestamps invalid"}
    counts = [value.get(name) for name in ("stdout_bytes", "stderr_bytes")]
    if (age is None or age < 0 or beat_age is None or beat_age < 0 or age < beat_age
            or type(value.get("launcher_alive")) is not bool
            or any(type(count) is not int or count < 0 for count in counts)
            or (output is not None and (output_age is None or output_age < age))
            or (output is None and any(counts))):
        return {**unknown, "state": "invalid", "note": "execution observation values invalid"}
    if age > pool.LEASE_TIMEOUT_S or beat_age > pool.LEASE_TIMEOUT_S:
        return {**unknown, "state": "stale", "age_s": age,
                "note": "execution observation stale; launcher liveness unknown"}
    alive = value["launcher_alive"]
    note = ("launcher running" if alive else
            "launcher exited; descendant liveness unknown")
    note += ("; no output observed" if output is None else f"; output {output_age:.0f}s ago")
    note += f"; {_child_note(value.get('child'))}"
    return {**value, "state": "fresh", "age_s": age,
            "last_output_age_s": output_age, "note": note}


def _child_note(child: object) -> str:
    """One clause about the payload the resource daemon forked.

    The launcher's own liveness says nothing about it -- ``766d7ae5...`` held
    a claim for an hour on ``launcher_alive: true`` while the payload sat in a
    futex wait (#600) -- so the reader that decides whether to surface a claim
    needs the child's own liveness, CPU and silence in the same line.  An
    observation that predates the field, or one the worker could not take,
    reads as unobserved: never as an absent child.
    """

    if not isinstance(child, dict):
        return "child unobserved"
    if child.get("source") != "resource-scope-cgroup" or "alive" not in child:
        return "child unobserved"
    count = child.get("pid_count")
    clause = ("child running" if child.get("alive") else "child gone")
    if isinstance(count, int):
        clause += f" ({count} pid{'' if count == 1 else 's'})"
    cpu = child.get("cpu_seconds")
    if isinstance(cpu, (int, float)) and not isinstance(cpu, bool):
        clause += f", cpu {float(cpu):.0f}s"
    silent = child.get("silent_s")
    if isinstance(silent, (int, float)) and not isinstance(silent, bool):
        clause += f", silent {float(silent):.0f}s"
    return clause


def _pool_sidecar(path: Path) -> dict | None | Exception:
    """Retain read failures for the row that owns this observation."""
    reader = _KEPT_READS.get()
    if reader is not None:
        return reader.sidecar(path)
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


def _reservation_hosts(queue: pool.PoolQueue, notes: list[str]) -> list[Path]:
    """The host directories under ``reservations/``, one listing."""

    hosts = []
    with os.scandir(queue.root / pool.RESERVATIONS) as entries:
        for entry in entries:
            try:
                if entry.is_dir():
                    hosts.append(Path(entry.path))
            except OSError as exc:
                notes.append(f"pool claim denials {entry.name}: {exc}")
    return hosts


def _pool_claim_denials(queue: pool.PoolQueue) -> tuple[list[dict], list[str]]:
    """Read optional host snapshots without turning a bad diagnostic into a bad job."""
    denials, notes = [], []
    reader = _KEPT_READS.get()
    try:
        if reader is not None:
            hosts = reader.subdirectories(queue.root / pool.RESERVATIONS)
        else:
            hosts = _reservation_hosts(queue, notes)
    except FileNotFoundError:
        return denials, notes
    except OSError as exc:
        return denials, [f"pool claim denials: {exc}"]
    for directory in sorted(hosts):
        snapshot = _pool_sidecar(directory / 'adaptive' / pool.CLAIM_DENIALS)
        if snapshot is None:
            continue  # Older workers and hosts that have skipped nothing.
        if isinstance(snapshot, Exception):
            notes.append(f"pool claim denials {directory.name}: {snapshot}")
            continue
        records = snapshot.get('records')
        if (snapshot.get('schema') != pool.CLAIM_DENIALS_SCHEMA_V1
                or not isinstance(records, dict) or len(records) > pool.MAX_CLAIM_DENIALS):
            notes.append(f"pool claim denials {directory.name}: invalid snapshot")
            continue
        for denial in records.values():
            try:
                valid = (isinstance(denial, dict)
                         and re.fullmatch('[a-f0-9]{64}', str(denial.get('action_key'))) is not None
                         and denial.get('host') == directory.name
                         and isinstance(denial.get('reason'), str) and bool(denial['reason'])
                         and isinstance(denial.get('evidence'), dict)
                         and all(type(denial.get(field)) in (int, float)
                                 and math.isfinite(denial[field]) and denial[field] >= 0
                                 for field in ('published_unix', 'denied_unix')))
            except OverflowError:
                valid = False
            if valid:
                denials.append(denial)
            else:
                notes.append(f"pool claim denial {directory.name}: invalid record")
    return denials, notes


def _claim_denials_for(record: dict, denials: Sequence[dict], *, now: float) -> list[dict]:
    """Keep each host's latest observation for this exact submission generation."""
    by_host = {}
    published = record.get('published_unix')
    if type(published) not in (int, float):
        return []
    for denial in denials:
        if (denial['action_key'] != record['action_key']
                or denial['published_unix'] != published or denial['denied_unix'] > now):
            continue
        host = denial['host']
        if host in by_host and by_host[host]['denied_unix'] >= denial['denied_unix']:
            continue
        decision = denial['evidence'].get('decision')
        subreason = decision.get('reason') if isinstance(decision, dict) else None
        by_host[host] = {**denial, 'age_s': now - denial['denied_unix'],
                         'decision_reason': subreason if isinstance(subreason, str) else None}
    return [by_host[host] for host in sorted(by_host)]


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
    denial_records, denial_notes = _pool_claim_denials(queue)
    now = time.time()
    notes = [*worker_notes, *ready_notes, *claim_notes, *denial_notes]
    # The half of ``notes`` that means "this census is missing something",
    # kept apart from the half that means "the fleet is in this state".  A
    # directory that would not list and a record that would not parse make the
    # census partial; a worker that stopped announcing is a fleet fact, fully
    # read.  ``complete`` below is the first, and only the first, because it is
    # what the run's exit status is derived from and a stale offer must not
    # spend the signal that says the mount did not answer.
    unreadable = [*worker_notes, *ready_notes, *claim_notes, *denial_notes]
    nodes: list[dict] = []
    live: list[dict] = []
    for host, offer in workers.items():
        if not _valid_pool_offer(host, offer):
            nodes.append({"node": host, "state": "unreadable", "healthy": False,
                          "reason": "invalid worker offer"})
            notes.append(f"pool worker {host}: invalid worker offer")
            unreadable.append(f"pool worker {host}: invalid worker offer")
            continue
        timing = pool.offer_timing(offer.get("announced_unix"), now=now)
        age = timing.age_s
        fresh = age is not None and age <= pool.OFFER_TIMEOUT_S
        if timing.clock_skew_s:
            disposition = "tolerated" if age is not None else "ignored"
            notes.append(
                f"pool worker {host}: offer announced {timing.clock_skew_s:.3f}s "
                f"in the future (clock skew; {disposition}, "
                f"limit {pool.OFFER_FUTURE_TOLERANCE_S:g}s)")
        if fresh:
            live.append(offer)
        else:
            notes.append(f"pool worker {host}: stale or invalid offer timestamp")
        nodes.append({
            "node": host, "transport": "pool", "state": "live" if fresh else "stale",
            "healthy": fresh, "age_s": age, "capacity": offer.get("capacity"),
            "offer_clock_skew_s": timing.clock_skew_s,
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
                row['admission_denials'] = _claim_denials_for(record, denial_records, now=now)
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
                    row['preempted_by'] = record.get('preempted_by')
                    if row['preempted_by']:
                        # This row is a requeue, not a first submission, and an
                        # operator reading a queue that grew a row it did not
                        # submit should be told which admission decision put it
                        # there (#364).
                        row['reason'] += (
                            "; requeued after preemption by "
                            f"{str(row['preempted_by'])[:12]}")
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
                    observation = _execution_observation(record, sidecar, now=now)
                    row['execution_observation'] = observation
                    row['progress_observation'] = _progress_observation(
                        record, sidecar, now=now)
                    row['reason'] = '; '.join(filter(None, (row.get('reason'), observation['note'])))
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


# --------------------------------------------------------------------------
# Starvation telemetry (--starvation, issue #661)
# --------------------------------------------------------------------------
#
# One JSON blob answering "what is waiting on data": which claimed consumers
# look stalled on their leads, what each residency plan has staged and
# promoted, where the read cursor stands against the promoted frontier, what
# the tiers currently offer and hold, and which claim denials block movers.
#
# Read-only and zero new state: every section below derives from records the
# fleet already files (claims, leases, plans, fragments, tier announcements,
# ledgers, denial snapshots).  Whatever cannot be derived from those records
# is emitted as the string ``"not_observable"`` with its entry in the
# ``not_observable`` gap list saying what would have to be recorded -- that
# list is a deliverable for the MCP observability design (#660), not an
# error.

#: Schema of the --starvation JSON blob.
STARVATION_SCHEMA_V1 = "prismabuild.pbstatus.starvation.v1"

#: What ``not_observable`` fields are spelled, so a reader can test one field
#: rather than the absence of one.
NOT_OBSERVABLE = "not_observable"

#: A claimed consumer counts as waiting on data past half its progress grace
#: with a payload scope to match.  A reporting heuristic, never admission:
#: the watchdog owns liveness, this owns the sentence "look here first".
STARVATION_QUIET_FRACTION = 0.5

#: Payload silence corroborating a quiet claim.  Same standing as the
#: fraction above: a number this blob reports beside the verdict, not a gate.
STARVATION_CHILD_SILENT_S = 30.0


def _starvation_sidecar(path: Path) -> dict | None | Exception:
    """One claim-adjacent record for the starvation census, failures retained."""

    reader = _KEPT_READS.get()
    if reader is not None:
        return reader.sidecar(path)
    try:
        return pool._read_json(path)
    except (OSError, ValueError) as exc:
        return exc


def _starvation_accepted_phase(lease: Mapping[str, object] | None) -> str | None:
    """The phase the consumer's progress record says it is reading now.

    The same field ``tier_loop.live_consumers`` reads through
    ``prewarm_loop.progress_phase`` (the lease's authenticated accepted
    observation), parsed here so this census does not import the tier loop.
    ``None`` is "no accepted progress": a ready consumer with no lease, or a
    claim whose worker has accepted nothing yet.
    """

    if not isinstance(lease, Mapping):
        return None
    observed = lease.get("progress_observation")
    if not isinstance(observed, Mapping):
        return None
    accepted = observed.get("last_accepted")
    if not isinstance(accepted, Mapping):
        return None
    phase = accepted.get("phase")
    return phase if isinstance(phase, str) and phase else None


def _starvation_quiet(lease: Mapping[str, object] | None,
                      ) -> tuple[float | None, float | None]:
    """How long this claim has been quiet, and against what it is allowed.

    The same ``quiet_s``/``grace_s`` pair ``_progress_observation`` renders
    for the jobs table, as numbers rather than prose.  ``(None, None)`` when
    the lease carries no progress observation, so "no policy" and "quiet"
    never read the same.
    """

    if not isinstance(lease, Mapping):
        return None, None
    observed = lease.get("progress_observation")
    if not isinstance(observed, Mapping):
        return None, None
    quiet, grace = observed.get("quiet_s"), observed.get("grace_s")
    if not all(type(value) in (int, float) and math.isfinite(value) and value >= 0
               for value in (quiet, grace)):
        return None, None
    return float(quiet), float(grace)


def _starvation_child(lease: Mapping[str, object] | None) -> dict | None:
    """The payload scope's own liveness, CPU and silence, or ``None``.

    Read off the lease's ``execution_observation`` beside the progress one:
    a launcher writing log lines is not an application committing work, and
    a claim quiet at the progress level with a busy child is not waiting on
    data.  Lenient where ``_execution_observation`` is strict -- identity and
    freshness there decide the jobs table, here the raw numbers travel with
    the verdict so the reader audits the rule rather than trusting it.
    """

    if not isinstance(lease, Mapping):
        return None
    observed = lease.get("execution_observation")
    if not isinstance(observed, Mapping):
        return None
    child = observed.get("child")
    if not isinstance(child, Mapping):
        return None
    out: dict[str, object] = {}
    for field in ("alive", "pid_count", "cpu_seconds", "silent_s"):
        value = child.get(field)
        if isinstance(value, bool) and field != "alive":
            continue
        if value is None or isinstance(value, (bool, int, float, str)):
            out[field] = value
    return out or None


def _starvation_waiting(quiet: float | None, grace: float | None,
                        child: Mapping[str, object] | None) -> tuple[bool, str]:
    """Whether this claimed consumer reads as waiting on data, and the rule.

    Quiet past half its grace with a payload scope to match: silent long
    enough to corroborate, or unobserved so nothing exonerates it.  The rule
    travels as a string because a boolean without its inputs is an opinion.
    """

    rule = (f"quiet_s/grace_s >= {STARVATION_QUIET_FRACTION:g} and "
            f"(child unobserved or silent_s >= {STARVATION_CHILD_SILENT_S:g}s)")
    if quiet is None or grace is None or grace <= 0:
        return False, rule
    if quiet / grace < STARVATION_QUIET_FRACTION:
        return False, rule
    silent = (child or {}).get("silent_s")
    if isinstance(silent, bool) or not isinstance(silent, (int, float)):
        return True, rule
    return bool(silent >= STARVATION_CHILD_SILENT_S), rule


def _starvation_holders(queue: pool.PoolQueue, tier_id: str,
                        keys: Iterable[str], notes: list[str]) -> dict[str, bool | None]:
    """Which mover keys hold tier tokens now, the pin the tier loop reads.

    A mover holding tokens is resident by definition -- the invariant
    ``tier_loop._mover_state`` stages from -- so the ledger answers "what is
    on the tier" without walking the device.  An unreadable ledger answers
    ``None`` per key rather than False: "could not read" is not "not staged".
    """

    try:
        ledger = queue.tier_ledger(tier_id)
    except (OSError, ValueError, pool.PoolContractError) as exc:
        notes.append(f"starvation tier ledger {tier_id}: {exc}")
        return {key: None for key in keys}
    staged: dict[str, bool | None] = {}
    for key in keys:
        try:
            staged[key] = bool(ledger.holder_tokens(key))
        except (OSError, ValueError, pool.PoolContractError) as exc:
            notes.append(f"starvation tier ledger {tier_id} holder {key[:12]}: {exc}")
            staged[key] = None
    return staged


def _starvation_plan_entry(queue: pool.PoolQueue, key: str, raw: object,
                           *, ready: set[str], claimed: set[str],
                           notes: list[str], unreadable: list[str],
                           now: float) -> dict:
    """One residency plan's promotion state, derived, never decided."""

    prefix = key[:12]
    if not isinstance(raw, Mapping):
        notes.append(f"starvation plan {prefix}: not a JSON object")
        unreadable.append(f"starvation plan {prefix}: not a JSON object")
        return {"consumer_action_key_prefix": prefix, "valid": False,
                "note": "not a JSON object"}
    try:
        plan = residency_plan.validate_plan(raw)
    except ValueError as exc:
        notes.append(f"starvation plan {prefix}: {exc}")
        unreadable.append(f"starvation plan {prefix}: {exc}")
        return {"consumer_action_key_prefix": prefix, "valid": False,
                "note": str(exc)}
    tier_id = str(plan["tier_id"])
    ram_tier_id = plan.get("ram_tier_id")
    ram_tier_id = str(ram_tier_id) if isinstance(ram_tier_id, str) else None
    state = ("claimed" if key in claimed else
             "ready" if key in ready else "absent")
    accepted: str | None = None
    if state == "claimed":
        lease = _starvation_sidecar(queue.lease_path(key))
        if isinstance(lease, Exception):
            notes.append(f"starvation plan {prefix} lease: {lease}")
            unreadable.append(f"starvation plan {prefix} lease: {lease}")
        else:
            accepted = _starvation_accepted_phase(lease)
    # Fragments are per consumer, one file per mover; an invalid one is
    # skipped by the reader, exactly as the consumer's own compose sees it.
    try:
        fragments = residency_map.read_fragments(
            queue.residency_fragment_root(), key)
    except (OSError, ValueError) as exc:
        notes.append(f"starvation plan {prefix} fragments: {exc}")
        unreadable.append(f"starvation plan {prefix} fragments: {exc}")
        fragments = []
    fragment_movers = {str(fragment.get("mover_action_key")): str(fragment.get("tier_id"))
                       for fragment in fragments
                       if isinstance(fragment.get("mover_action_key"), str)}
    mover_keys = []
    for phase in plan["phases"]:
        chunks = phase.get("stage_chunks")
        if isinstance(chunks, list):
            mover_keys += [str(chunk["mover_row"]["action_key"])
                           for chunk in chunks if isinstance(chunk, Mapping)]
        elif "mover_row" in phase:
            mover_keys.append(str(phase["mover_row"]["action_key"]))
    ram_keys = []
    for phase in plan["phases"]:
        chunks = phase.get("ram_chunks")
        if isinstance(chunks, list):
            ram_keys += [str(chunk["ram_mover_row"]["action_key"])
                         for chunk in chunks if isinstance(chunk, Mapping)]
        elif "ram_mover_row" in phase:
            ram_keys.append(str(phase["ram_mover_row"]["action_key"]))
    staged = _starvation_holders(queue, tier_id, mover_keys, notes)
    ram_staged = (_starvation_holders(queue, ram_tier_id, ram_keys, notes)
                  if ram_tier_id is not None else {})
    # Reservations and residency, reported apart (#759).  ``_starvation_
    # holders`` above answers the ledger's question -- what is booked -- and
    # the cursor used to print that as ``staged``, which is how it called the
    # head phase staged while the copy was still running and the identity
    # proof saw nothing.  ``staged`` is now the one shared readiness
    # predicate the tier window gates on, so the census and the gate cannot
    # disagree; ``reserved`` keeps the booking visible beside it rather than
    # hiding the capacity it is really using.  Evidence that cannot be read
    # reports ``None`` -- unknown -- never a clean ``False``.
    stage_resident: set[str] = set()
    ram_resident: set[str] = set()
    resident_known = True
    try:
        stage_resident = residency_plan.resident_movers(queue, plan, tier_id)
        if ram_tier_id is not None:
            ram_resident = residency_plan.resident_movers(
                queue, plan, ram_tier_id)
    except (OSError, ValueError) as exc:
        notes.append(f"starvation plan {prefix} residency: {exc}")
        unreadable.append(f"starvation plan {prefix} residency: {exc}")
        resident_known = False

    def _resident(mover: str, *, ram: bool = False) -> bool | None:
        """Published residency for one key, or ``None`` when it is unknown."""

        if not resident_known:
            return None
        return mover in (ram_resident if ram else stage_resident)

    names = [str(phase["name"]) for phase in plan["phases"]]
    accepted_index = names.index(accepted) if accepted in names else None
    phases: list[dict] = []
    for index, phase in enumerate(plan["phases"]):
        stage: dict | None = None
        stage_chunks: list[dict] | None = None
        stage_fragment = False
        if "mover_row" in phase:
            mover_key = str(phase["mover_row"]["action_key"])
            stage = {"mover_action_key_prefix": mover_key[:12],
                     "published": mover_key in ready or mover_key in claimed
                                  or bool(staged.get(mover_key)),
                     "staged": _resident(mover_key),
                     "reserved": bool(staged.get(mover_key)),
                     "fragment": mover_key in fragment_movers}
            stage_fragment = mover_key in fragment_movers
        elif isinstance(phase.get("stage_chunks"), list):
            # A chunked phase reports per chunk, beside the phase's own
            # ``stage`` staying ``None``: the census shape for whole-phase
            # legs is unchanged, and a chunked phase is never mistaken for
            # one with no leg at all.
            stage_chunks = []
            assert isinstance(phase["stage_chunks"], list)
            for chunk in phase["stage_chunks"]:
                chunk_key = str(chunk["mover_row"]["action_key"])  # type: ignore[index]
                chunk_fragment = chunk_key in fragment_movers
                stage_fragment = stage_fragment or chunk_fragment
                stage_chunks.append({
                    "chunk_index": chunk.get("chunk_index"),  # type: ignore[union-attr]
                    "mover_action_key_prefix": chunk_key[:12],
                    "published": chunk_key in ready or chunk_key in claimed
                                 or bool(staged.get(chunk_key)),
                    "staged": _resident(chunk_key),
                    "reserved": bool(staged.get(chunk_key)),
                    "fragment": chunk_fragment,
                    "fragment_tier_id": fragment_movers.get(chunk_key)})
        ram: dict | None = None
        ram_chunks: list[dict] | None = None
        if "ram_mover_row" in phase:
            ram_key = str(phase["ram_mover_row"]["action_key"])
            ram = {"mover_action_key_prefix": ram_key[:12],
                   "published": ram_key in ready or ram_key in claimed
                                or bool(ram_staged.get(ram_key)),
                   "staged": _resident(ram_key, ram=True),
                   "reserved": bool(ram_staged.get(ram_key)),
                   # The question the ram tier exists to answer: which phases
                   # have promotion fragments and which have none.
                   "fragment": ram_key in fragment_movers,
                   "fragment_tier_id": fragment_movers.get(ram_key)}
        elif isinstance(phase.get("ram_chunks"), list):
            # A chunked phase reports per chunk, beside the phase's own
            # ``ram`` staying ``None``: the census shape for whole-phase
            # legs is unchanged, and a chunked phase is never mistaken for
            # one with no ram leg at all.
            ram_chunks = []
            assert isinstance(phase["ram_chunks"], list)
            for chunk in phase["ram_chunks"]:
                chunk_key = str(chunk["ram_mover_row"]["action_key"])  # type: ignore[index]
                ram_chunks.append({
                    "chunk_index": chunk.get("chunk_index"),  # type: ignore[union-attr]
                    "mover_action_key_prefix": chunk_key[:12],
                    "published": chunk_key in ready or chunk_key in claimed
                                 or bool(ram_staged.get(chunk_key)),
                    "staged": _resident(chunk_key, ram=True),
                    "reserved": bool(ram_staged.get(chunk_key)),
                    "fragment": chunk_key in fragment_movers,
                    "fragment_tier_id": fragment_movers.get(chunk_key)})
        phases.append({"name": str(phase["name"]),
                       "start_bytes": phase["start_bytes"],
                       "end_bytes": phase["end_bytes"],
                       "stage": stage, "ram": ram,
                       "stage_chunks": stage_chunks,
                       "ram_chunks": ram_chunks,
                       "stage_fragment": stage_fragment})
    # A retired window (#708) is a stall no cycle ends: nothing republishes
    # its movers until an operator resubmits the consumer.  When a mover's
    # terminal refusal retired it (#966, #1004) the marker names why, the
    # path and both owners, and so does this entry (#1004 item 3).
    try:
        superseded = residency_plan.supersession_summary(
            residency_plan.superseded(queue, plan))
    except (OSError, ValueError, pool.PoolContractError) as exc:
        notes.append(f"starvation plan {prefix} supersession: {exc}")
        unreadable.append(f"starvation plan {prefix} supersession: {exc}")
        superseded = {"unreadable": True, "error": str(exc)}
    if isinstance(superseded, Mapping) and superseded.get("unreadable"):
        notes.append(f"starvation plan {prefix} supersession: "
                     f"{superseded.get('error')}")
        unreadable.append(f"starvation plan {prefix} supersession: "
                          f"{superseded.get('error')}")
    entry = {"consumer_action_key_prefix": prefix, "valid": True,
             "tier_id": tier_id, "ram_tier_id": ram_tier_id,
             "state": state, "superseded": superseded,
             "accepted_phase": accepted,
             "accepted_index": accepted_index, "phases": phases,
             "cursor_gap": _starvation_cursor_gap(
                 names, accepted_index, phases, now=now)}
    return entry


def _starvation_chunked_staged(chunks: object):
    """Fold a chunked leg's per-chunk readiness into one tri-state.

    A chunked phase is staged only when every chunk is: a partially moved
    phase still needs room on the tier, and counting it staged would tell
    the census the frontier is further than the bytes prove.  One chunk
    known missing makes the phase known-unstaged, because that hole is a
    fact whatever the others say.  Otherwise -- no hole, but a chunk whose
    evidence could not be read -- the phase is UNKNOWN, and folding that to
    ``False`` here would assert the hole the census could not see (#759).
    """

    if not isinstance(chunks, list) or not chunks:
        return None
    states = [chunk.get("staged") for chunk in chunks
              if isinstance(chunk, dict)]
    if states and all(state is True for state in states):
        return True
    if any(state is False for state in states):
        return False
    return None


def _starvation_leg_present(phase: dict, leg: str) -> bool:
    """Whether this phase has that leg at all, chunked or whole.

    Absence is not readiness and not its negation: a phase with no ram leg
    has nothing to be unstaged about, so it is counted on neither side.
    """

    if leg == "stage":
        return phase["stage"] is not None or phase.get("stage_chunks") is not None
    return phase["ram"] is not None or phase.get("ram_chunks") is not None


def _starvation_leg_staged(phase: dict, leg: str):
    """Whether one census phase counts as staged on one leg.

    Three answers, and they stay three: ``True`` staged, ``False`` known
    not staged, ``None`` either no such leg or evidence that could not be
    read.  Callers must test ``is True`` and ``is False`` separately --
    ``is not True`` merges the last two and republishes an unknown as a
    fact, which is the #759 error one level up from the leg it fixed.
    """

    if leg == "stage":
        if phase["stage"] is not None:
            return phase["stage"].get("staged")
        return _starvation_chunked_staged(phase.get("stage_chunks"))
    if phase["ram"] is not None:
        return phase["ram"].get("staged")
    return _starvation_chunked_staged(phase.get("ram_chunks"))


def _starvation_span(states: list, wanted: object) -> int:
    """The bytes of every phase whose leg is in exactly ``wanted`` state."""

    return sum(int(phase["end_bytes"]) - int(phase["start_bytes"])
               for phase, state in states if state is wanted)


def _starvation_cursor_gap(names: list[str], accepted_index: int | None,
                           phases: list[dict], *, now: float) -> dict:
    """Where the read cursor stands against the promoted frontier, per leg.

    The read cursor is the accepted phase -- the last phase the consumer's
    progress record vouches for -- and the frontier is what the ledgers say
    is staged.  Both are phases, never bytes: the consumer's live byte offset
    inside the phase it is reading is not recorded anywhere, which is gap
    entry ``reader_live_cursor`` rather than a number invented here.
    """

    gap: dict[str, object] = {"accepted_phase": names[accepted_index]
                              if accepted_index is not None else None,
                              "live_byte_cursor": NOT_OBSERVABLE}
    for leg in ("stage", "ram"):
        remaining = [phase for index, phase in enumerate(phases)
                     if (accepted_index is None or index >= accepted_index)
                     and _starvation_leg_present(phase, leg)]
        states = [(phase, _starvation_leg_staged(phase, leg))
                  for phase in remaining]
        # ``unstaged`` counts ``False`` and only ``False``.  The previous
        # spelling was ``is not True``, which swept every unknown into a
        # count of bytes an operator reads as definitely missing -- the leg
        # reported honestly and the aggregate undid it (#759).
        gap[leg] = {
            "remaining_phases": len(remaining),
            "staged_phases": sum(1 for _, state in states if state is True),
            "unstaged_phases": [phase["name"] for phase, state in states
                                if state is False],
            "unstaged_bytes": _starvation_span(states, False),
            # Additive with the two above: staged + unstaged + unknown is
            # every remaining phase that has this leg.
            "unknown_phases": [phase["name"] for phase, state in states
                               if state is None],
            "unknown_bytes": _starvation_span(states, None),
        }
    return gap


def _starvation_tiers(queue: pool.PoolQueue, *, now: float,
                      notes: list[str], unreadable: list[str]) -> list[dict]:
    """What each tier currently offers and holds: announcement plus ledger."""

    try:
        announced = {str(record.get("tier_id")): record
                     for record in queue.tiers()
                     if isinstance(record, dict)}
    except (OSError, ValueError, pool.PoolContractError) as exc:
        notes.append(f"starvation tiers: {exc}")
        unreadable.append(f"starvation tiers: {exc}")
        announced = {}
    try:
        tier_ids = queue.tier_ids()
    except OSError as exc:
        notes.append(f"starvation tier ledgers: {exc}")
        unreadable.append(f"starvation tier ledgers: {exc}")
        tier_ids = sorted(announced)
    tiers: list[dict] = []
    for tier_id in sorted(set(tier_ids) | set(announced)):
        record = announced.get(tier_id)
        supply = (record.get("fill_supply") if isinstance(record, Mapping) else None)
        supply = dict(supply) if isinstance(supply, Mapping) else None
        sampled = (record.get("sampled_unix") if isinstance(record, Mapping) else None)
        try:
            ledger = queue.tier_ledger(tier_id)
            capacity, available = ledger.capacity(), ledger.available()
        except (OSError, ValueError, pool.PoolContractError) as exc:
            notes.append(f"starvation tier ledger {tier_id}: {exc}")
            unreadable.append(f"starvation tier ledger {tier_id}: {exc}")
            capacity, available = None, None
        held: dict[str, int] | None = None
        if isinstance(capacity, dict) and isinstance(available, dict):
            held = {kind: int(capacity.get(kind, 0)) - int(available.get(kind, 0))
                    for kind in sorted(set(capacity) | set(available))}
        # What the tier loop's admission commitment saw on its last cycle
        # (#930), read as filed: the loop's own verdict, never re-priced
        # here.  None where no loop files one -- not a stage tier, or a loop
        # from before #930.
        try:
            commitment = queue.tier_commitment(tier_id)
        except (OSError, ValueError, pool.PoolContractError) as exc:
            notes.append(f"starvation tier commitment {tier_id}: {exc}")
            unreadable.append(f"starvation tier commitment {tier_id}: {exc}")
            commitment = None
        filed = (commitment.get("filed_unix") if isinstance(commitment, Mapping)
                 else None)
        tiers.append({
            "tier_id": tier_id,
            "announced": record is not None,
            # Identity off the announcement, read never derived: kind groups
            # ram/stage/arc records for one box, host names the box, epoch
            # dates the ram mount, and window_gib is the promotion window the
            # tier was announced with. Each is None where the tier announced
            # none, which is how an unannounced ledger row differs from a
            # tier that measured zero.
            "tier_kind": (record.get("tier") if isinstance(record, Mapping)
                          else None),
            "host": (record.get("host") if isinstance(record, Mapping)
                     else None),
            "epoch": (record.get("epoch") if isinstance(record, Mapping)
                      else None),
            "window_gib": (record.get("window_gib")
                           if isinstance(record, Mapping) else None),
            # The tier loop's latest fold, read, not recomputed: best is the
            # highest pool delivery since the last ceiling, ceiling the most
            # recent measured shortfall, may_grow whether another reader fits.
            "fill_source": record.get("fill_source") if isinstance(record, Mapping) else None,
            "fill_supply": supply,
            "announced_fill_mb_s": (
                record.get(storage_tiers.FILL_RECORD_FIELD)
                if isinstance(record, Mapping) else None),
            "capacity_bytes": record.get("capacity_bytes") if isinstance(record, Mapping) else None,
            "sampled_age_s": (now - float(sampled)
                              if isinstance(sampled, (int, float)) else None),
            "ledger_capacity": capacity,
            "ledger_available": available,
            "ledger_held": held,
            "commitment": commitment,
            "commitment_age_s": (now - float(filed)
                                 if isinstance(filed, (int, float))
                                 and not isinstance(filed, bool) else None),
        })
    return tiers


def _starvation_commitment_waits(tiers: Sequence[Mapping[str, object]]) -> list[dict]:
    """Every newcomer waiting on a stage tier's joint commitment (#930).

    Flattened from each tier's filed commitment record: a newcomer the
    commitment refused, one whose tier's census did not read, and one
    waiting behind either.  Each carries its footprint, the committed
    total, the capacity and the terms that make up the gap -- the holders
    and windows, each marked evictable or not -- as the gate saw them.
    """

    waits: list[dict] = []
    for tier in tiers:
        record = tier.get("commitment")
        if not isinstance(record, Mapping):
            continue
        for entry in record.get("waiting") or ():
            if not isinstance(entry, Mapping):
                continue
            ahead = entry.get("waiting_on")
            ahead_reason = (ahead.get("reason") if isinstance(ahead, Mapping)
                            else None)
            if not (entry.get("reason") == window_credit.REASON_COMMITMENT
                    or ahead_reason == window_credit.REASON_COMMITMENT
                    or "error" in entry):
                continue
            waits.append({"tier_id": tier.get("tier_id"),
                          "age_s": tier.get("commitment_age_s"),
                          "over_committed_gib": record.get("over_committed_gib"),
                          **dict(entry)})
    return waits


def _starvation_denials(queue: pool.PoolQueue, *, notes: list[str]) -> list[dict]:
    """Top claim-denial reasons per host, and the ones blocking movers.

    The snapshot holds each host's latest verdict per action generation
    (bounded, ungated by time), so this is a census of what hosts last said,
    not a window.  A denial names no residency, so "blocking a mover" is a
    join against the live ready/claimed items carrying a range -- the same
    range test ``movers_claimed_on_tier`` stages on -- and a denial whose key
    has since left the active queue reads as unknown rather than as either.
    """

    denials, denial_notes = _pool_claim_denials(queue)
    notes.extend(denial_notes)
    movers: set[str] = set()
    live: set[str] = set()
    for state in (pool.READY, pool.CLAIMED):
        try:
            with os.scandir(queue.dir(state)) as entries:
                paths = sorted(Path(entry.path) for entry in entries
                               if entry.name.endswith(".json"))
        except OSError as exc:
            notes.append(f"starvation mover join {state}: {exc}")
            continue
        for path in paths:
            record = _starvation_sidecar(path)
            if not isinstance(record, dict):
                continue
            key = record.get("action_key")
            if key != path.stem:
                continue
            live.add(str(key))
            residency = record.get("residency")
            if isinstance(residency, Mapping) and "range_start_bytes" in residency:
                movers.add(str(key))
    by_host: dict[str, dict] = {}
    for denial in denials:
        host = str(denial.get("host"))
        reason = str(denial.get("reason"))
        key = str(denial.get("action_key"))
        slot = by_host.setdefault(host, {"reasons": {}, "mover_reasons": {},
                                         "unknown_keys": 0})
        slot["reasons"][reason] = slot["reasons"].get(reason, 0) + 1
        if key in movers:
            slot["mover_reasons"][reason] = slot["mover_reasons"].get(reason, 0) + 1
        elif key not in live:
            slot["unknown_keys"] += 1
    top: list[dict] = []
    for host in sorted(by_host):
        slot = by_host[host]
        top.append({
            "host": host,
            "denials": sum(slot["reasons"].values()),
            "top_reasons": [{"reason": reason, "count": count} for reason, count in
                            sorted(slot["reasons"].items(),
                                   key=lambda item: (-item[1], item[0]))],
            "mover_blocked": sum(slot["mover_reasons"].values()),
            "mover_blocked_reasons": [
                {"reason": reason, "count": count} for reason, count in
                sorted(slot["mover_reasons"].items(),
                       key=lambda item: (-item[1], item[0]))],
            "unknown_keys": slot["unknown_keys"],
        })
    return top


def _starvation_starved(jobs: Sequence[Mapping[str, object]]) -> list[dict]:
    """Ready items a box refused and will not withhold for (#924).

    A starved item withholds its box only while the holders in its way will
    drain soon.  When one of them will not -- a progress-governed campaign
    holder with no total bound, a bounded one already past the pool's
    transient line, one past its own declared end (#939), load the pool does
    not own, or a veto that work ahead of
    it kept refilling -- the item stops holding the box shut, and this is
    where it says so instead of going quiet.  Each row is the host's latest
    verdict for this exact submission, with the reason and the holders it
    named.
    """

    starved: list[dict] = []
    for job in jobs:
        if job.get("state") != "READY":
            continue
        for denial in job.get("admission_denials") or ():
            reason = str(denial.get("reason", ""))
            if not reason.endswith(("_starved", "_past_ceiling")):
                continue
            evidence = denial.get("evidence")
            detail = evidence.get("starved") if isinstance(evidence, Mapping) else None
            starved.append({
                "action_key": job.get("action_key"),
                "action_key_prefix": job.get("action_key_prefix"),
                "host": denial.get("host"),
                "reason": reason,
                "why": detail.get("why") if isinstance(detail, Mapping) else None,
                "decision_reason": denial.get("decision_reason"),
                "admission_passes": job.get("admission_passes"),
                "admission_wait_s": job.get("admission_wait_s"),
                "denial_age_s": denial.get("age_s"),
                "holders": (detail.get("holders")
                            if isinstance(detail, Mapping) else None),
            })
    return starved


def _starvation_dependents(queue: pool.PoolQueue, *, notes: list[str],
                           unreadable: list[str], now: float) -> list[dict]:
    """A starved producer in one place (#990): what each claimed action waits on.

    For every claimed action that owns rows -- a residency plan's movers,
    egresses and RAM promotions, or produced-output exports naming it in
    ``dependent_of`` -- the same record a kill's ending carries: the rows,
    their state, how long they have been ready, each one's latest denial and
    reason ring, and the tier loops' newest verdicts about the action.  One
    census of ``ready/`` and ``claimed/`` for all of them.
    """

    live, listed = queue.live_records()
    for error in listed:
        notes.append(f"starvation dependents: {error}")
        unreadable.append(f"starvation dependents: {error}")
    owners = {str(record.get("dependent_of")) for _state, record in live.values()
              if isinstance(record.get("dependent_of"), str)}
    out: list[dict] = []
    for key, (state, record) in sorted(live.items()):
        if state != pool.CLAIMED:
            continue
        residency = record.get("residency")
        planned = isinstance(residency, Mapping) and bool(residency.get("leads"))
        if key not in owners and not planned:
            continue
        try:
            found = queue.dependent_rows(key, now=now, live_records=(live, []),
                                         include_local=False)
        except (OSError, ValueError, pool.PoolContractError) as exc:
            notes.append(f"starvation dependents {key[:12]}: {exc}")
            unreadable.append(f"starvation dependents {key[:12]}: {exc}")
            continue
        events = queue.consumer_events(key)
        out.append({"action_key": key, "action_key_prefix": key[:12],
                    "node": record.get("claimed_host"),
                    **found,
                    "tier_events": events[-pool.MAX_ENDING_EVENTS:],
                    "tier_events_total": len(events)})
    return out


def _starvation_gaps() -> list[dict]:
    """What this blob cannot derive, and what would have to be recorded.

    The gap list is a deliverable for the MCP observability design (#660):
    each entry names the field that reads ``"not_observable"``, why no
    existing record carries it, and the record that would.
    """

    return [
        {"field": "waiting_claims[].cpu_growth",
         "why_not_observable": "one census carries one cpu_seconds sample of "
            "the payload scope; growth is a difference of two samples of the "
            "same scope nonce, which a single snapshot cannot take",
         "would_need": "consecutive snapshots differenced per scope nonce "
            "(what pbmetrics recent-cores does across scrapes), or the scope "
            "sampler publishing a short-window rate beside the lifetime counter"},
        {"field": "residency_plans[].cursor_gap.live_byte_cursor",
         "why_not_observable": "progress records name the last accepted phase, "
            "never the consumer's byte offset inside the phase it is reading",
         "would_need": "the consumer reporting read_bytes beside accepted_phase "
            "on its progress record, in the manifest's read order"},
        {"field": "residency_plans[] accepted_phase for ready consumers",
         "why_not_observable": "a ready consumer has no lease, so no accepted "
            "observation exists until it is claimed; its window position reads "
            "as the beginning",
         "would_need": "the coordinator publishing the last accepted phase it "
            "saw for a ready consumer, or the progress channel pre-claim"},
        {"field": "denial history for finished movers",
         "why_not_observable": "each live key's reason transitions are kept "
            "in denial-transitions/ (#991) and a live consumer's tier-loop "
            "verdicts in residency-events/ (#990), but a finished mover's ring "
            "is retired with it; only a kill's ending record keeps the rings "
            "of the rows it was waiting on",
         "would_need": "a bounded terminal denial archive beside done/"},
        {"field": "tiers[] live disk delivery",
         "why_not_observable": "the announced fill is the last cycle's learned "
            "rate from mover receipts; instantaneous pool delivery between "
            "cycles is sampled by no record",
         "would_need": "the tier loop publishing its per-cycle disk pacing "
            "sample beside the announced record"},
    ]


def _starvation_census_unreadable(queue: pool.PoolQueue, *, notes: list[str],
                                  unreadable: list[str]) -> list[dict]:
    """Every ledger whose last retire could not read a holder (#936).

    A holder that stays unreadable refuses every mint on its ledger and
    bounds every shrink by the mint markers instead of the truth, and a
    claim the lower-bound total cannot seat is refused as
    ``tier_census_unreadable``.  The ledger's own report names the holder,
    its errno, the kinds asked and retired and how many consecutive
    censuses it has lasted, so the refusal is attributed here rather than
    read as an empty ledger.  Host ledgers and tier ledgers both file one;
    an exact census removes it.
    """

    ledgers: list[tuple[str, str, pool.ResourceLedger]] = []
    try:
        with os.scandir(queue.root / pool.RESERVATIONS) as entries:
            hosts = sorted(entry.name for entry in entries if entry.is_dir())
    except FileNotFoundError:
        hosts = []
    except OSError as exc:
        notes.append(f"starvation host ledgers: {exc}")
        unreadable.append(f"starvation host ledgers: {exc}")
        hosts = []
    ledgers.extend(("host", host, queue.ledger(host)) for host in hosts)
    try:
        ledgers.extend(("tier", tier_id, queue.tier_ledger(tier_id))
                       for tier_id in queue.tier_ids())
    except (OSError, ValueError, pool.PoolContractError) as exc:
        notes.append(f"starvation tier census reports: {exc}")
        unreadable.append(f"starvation tier census reports: {exc}")
    reports: list[dict] = []
    for kind, name, ledger in ledgers:
        try:
            report = pool._read_json(ledger.census_report_path)
        except (OSError, pool.PoolContractError) as exc:
            notes.append(f"starvation {kind} ledger {name} census report: {exc}")
            unreadable.append(f"starvation {kind} ledger {name} census report: {exc}")
            continue
        if report is None:
            continue
        holders = report.get("unreadable")
        holders = [dict(entry) for entry in holders
                   if isinstance(entry, Mapping)] if isinstance(holders, list) else []
        reports.append({"ledger_kind": kind, "ledger_id": name, **report,
                        "unreadable": holders})
        named = ", ".join(f"{entry.get('holder')} errno {entry.get('errno')}"
                          for entry in holders)
        notes.append(
            f"{kind} ledger {name}: {len(holders)} unreadable holder(s) for "
            f"{report.get('cycles')} census cycle(s); mints refused, last "
            f"retire {report.get('retired')} ({named})")
    return reports


#: Schema of the --blocked-origins JSON blob (#926).
BLOCKED_ORIGINS_SCHEMA_V1 = "prismabuild.pbstatus.blocked_origins.v1"


def blocked_origin_remedies(ref: Mapping[str, object], key: str) -> dict[str, str]:
    """The two commands that let one blocked consumer's batch go (#926).

    Computed here, at display time, from the batch's ref and the consumer's
    key; the tick's own report carries neither.  ``release`` is exact.
    ``resubmit`` names what only the submitter knows as placeholders: the
    consumer's own options and command, resubmitted with ``--supersedes``.
    """

    batch = shlex.quote(json.dumps(dict(ref), sort_keys=True,
                                   separators=(",", ":")))
    return {
        "release": (f"pbrun.py --release-origin-consumer {batch} {key} "
                    "--reason '<why>'"),
        "resubmit": (f"pbrun.py --priority -10 --supersedes {key} "
                     "<the consumer's options> -- <its command>"),
    }


#: What frees an orphaned prewrite (#949, #1053): the one remedy the tier
#: cycle's event names too. PB never deletes a file whose identity no commit
#: recorded, so the operator does.
ORPHANED_PREWRITE_REMEDY = produced_output.ORPHANED_PREWRITE_REMEDY


def read_blocked_origins(queue_root: str | Path) -> dict:
    """List the consumed origin batches that only an operator can free (#926).

    A batch is blocked when every declared consumer still holding it failed
    or was withdrawn (`produced_output.blocked_origin_batches`, which reads
    the same records the retirement tick does and writes nothing).  Each
    holding consumer is listed with its state, any ``superseded_by`` that did
    not apply, and the remedies.  ``complete`` is false when a record could
    not be read; the unreadable ones are named.

    ``orphaned_prewrites`` lists the prewrites, of any template (#1053),
    whose attempt ended before committing and whose files belong to no batch
    (#949), each with the remedy.

    ``held_by_unknown`` lists each consumed batch or ended prewrite that the
    tier cycle would free now but for another attempt, whose state cannot be
    read, that has prewritten one of its paths (#1065): the holders, why
    each is unknown, whether it is orphaned (no queue row past the lease
    timeout) and, for an orphaned one, the remedy.
    """

    queue = pool.PoolQueue(queue_root)
    found = produced_output.blocked_origin_batches(queue)
    blocked = []
    for batch in found["blocked"]:
        holding = set(batch["holding"])
        blocked.append({
            **batch,
            "remedies": [
                {"action_key": item["action_key"], "state": item["state"],
                 **({"superseded_by": item["superseded_by"]}
                    if "superseded_by" in item else {}),
                 **blocked_origin_remedies(batch["ref"], str(item["action_key"]))}
                for item in batch["consumers"]
                if item["action_key"] in holding],
        })
    orphaned = [{**item, "remedy": ORPHANED_PREWRITE_REMEDY}
                for item in found["orphaned_prewrites"]]
    return {"schema": BLOCKED_ORIGINS_SCHEMA_V1, "blocked": blocked,
            "orphaned_prewrites": orphaned,
            "held_by_unknown": list(found["held_by_unknown"]),
            "unreadable": list(found["unreadable"]),
            "complete": not found["unreadable"]}


def read_starvation(queue_root: str | Path, *, now: float | None = None) -> dict:
    """Answer "what is waiting on data" as one JSON-serializable blob.

    A non-atomic diagnostic census over the pool queue root, reusing
    :func:`read_pool` for the queue walk: claims and leases for quiet
    consumers, plans and fragments for promotion state, tier announcements
    and ledgers for fill and occupancy, denial snapshots for who blocks
    movers.  Nothing here writes, nothing admits, nothing schedules.

    Args:
        queue_root: The queue root holding ready/claimed state and the
            residency, tier and reservation records beside it.
        now: The clock to age samples against; ``time.time()`` when omitted.

    Returns:
        The starvation blob: waiting claims, residency plans with cursor
        gaps, tiers, per-host denial tops, the ``not_observable`` gap list,
        and the notes.  ``complete`` is False when any input was unreadable.
    """

    moment = time.time() if now is None else float(now)
    queue = pool.PoolQueue(Path(queue_root).absolute())
    census = read_pool(queue_root)
    notes = list(census.get("notes", []))
    unreadable = list(census.get("queue", {}).get("unreadable", []))
    jobs = census.get("jobs", [])
    ready = {str(job["action_key"]) for job in jobs if job.get("state") == "READY"}
    claimed = {str(job["action_key"]) for job in jobs if job.get("state") == "CLAIMED"}
    waiting: list[dict] = []
    for job in jobs:
        if job.get("state") != "CLAIMED":
            continue
        key = str(job.get("action_key"))
        claim = _starvation_sidecar(queue.item_path(pool.CLAIMED, key))
        if isinstance(claim, Exception) or not isinstance(claim, dict):
            notes.append(f"starvation claim {key[:12]}: {claim}")
            unreadable.append(f"starvation claim {key[:12]}: {claim}")
            continue
        residency = claim.get("residency")
        if not isinstance(residency, Mapping) or not residency.get("leads"):
            continue  # Not a consumer: nothing to wait on.
        lease = _starvation_sidecar(queue.lease_path(key))
        if isinstance(lease, Exception):
            notes.append(f"starvation lease {key[:12]}: {lease}")
            unreadable.append(f"starvation lease {key[:12]}: {lease}")
            lease = None
        quiet, grace = _starvation_quiet(lease)
        child = _starvation_child(lease)
        is_waiting, rule = _starvation_waiting(quiet, grace, child)
        observed = (lease.get("progress_observation") if isinstance(lease, Mapping)
                    else None)
        phase = (observed.get("phase") if isinstance(observed, Mapping)
                 and isinstance(observed.get("phase"), str) else None)
        waiting.append({
            "action_key_prefix": key[:12],
            "node": claim.get("claimed_host"),
            "phase": phase,
            "accepted_phase": _starvation_accepted_phase(lease),
            "quiet_s": quiet,
            "grace_s": grace,
            "quiet_fraction": (quiet / grace if quiet is not None and grace else None),
            "child": child,
            # Single-sample growth is gap entry cpu_growth, not a number here.
            "cpu_growth": NOT_OBSERVABLE,
            "waiting_on_data": is_waiting,
            "waiting_rule": rule,
        })
    plans: list[dict] = []
    try:
        with os.scandir(queue.root / pool.RESIDENCY_PLANS) as entries:
            plan_paths = sorted(Path(entry.path) for entry in entries
                                if entry.name.endswith(".json"))
    except FileNotFoundError:
        plan_paths = []  # No staged consumer has ever filed a plan.
    except OSError as exc:
        notes.append(f"starvation residency plans: {exc}")
        unreadable.append(f"starvation residency plans: {exc}")
        plan_paths = []
    for path in plan_paths:
        raw = _starvation_sidecar(path)
        if isinstance(raw, Exception):
            notes.append(f"starvation plan {path.stem[:12]}: {raw}")
            unreadable.append(f"starvation plan {path.stem[:12]}: {raw}")
            plans.append({"consumer_action_key_prefix": path.stem[:12],
                          "valid": False, "note": str(raw)})
            continue
        plans.append(_starvation_plan_entry(
            queue, path.stem, raw, ready=ready, claimed=claimed,
            notes=notes, unreadable=unreadable, now=moment))
    tiers = _starvation_tiers(queue, now=moment, notes=notes, unreadable=unreadable)
    commitment_waits = _starvation_commitment_waits(tiers)
    denial_top = _starvation_denials(queue, notes=notes)
    starved = _starvation_starved(jobs)
    for entry in starved:
        # The sequence, not only the latest verdict (#991).
        entry["denial_transitions"] = (
            queue.denial_transitions(str(entry["action_key"]))
            if isinstance(entry.get("action_key"), str) else [])
    dependents = _starvation_dependents(queue, notes=notes, unreadable=unreadable,
                                        now=moment)
    census_unreadable = _starvation_census_unreadable(
        queue, notes=notes, unreadable=unreadable)
    return {"schema": STARVATION_SCHEMA_V1,
            "queue_root": str(queue.root),
            "sampled_unix": moment,
            "complete": bool(census.get("queue", {}).get("complete", False))
                        and not unreadable,
            "notes": notes,
            "waiting_claims": waiting,
            "residency_plans": plans,
            "tiers": tiers,
            "joint_commitment_waits": commitment_waits,
            "denial_top": denial_top,
            "starved": starved,
            "claimed_dependents": dependents,
            "census_unreadable": census_unreadable,
            "not_observable": _starvation_gaps()}



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
             # An action admitted under the progress contract is the exception:
             # this ceiling re-aims at each phase's quiet instead of the whole
             # run, and the job table's PROGRESS column is what governs it.
             ABSENT if n.get('timeout_ceiling_s') is None
             else f"{float(n['timeout_ceiling_s']):g}s",
             n.get('capacity'), n.get('observed_capacity'),
             ', '.join(f"{k}: {v['state']}" for k, v in n.get('admission', {}).items()),
             n.get('reason'))
            for n in nodes))


def _token_shortage_text(denial: Mapping[str, object]) -> str:
    evidence = denial.get('evidence')
    shortage = evidence.get('token_shortage') if isinstance(evidence, dict) else None
    if not isinstance(shortage, dict):
        return ''
    resource, requested, available = (shortage.get(k) for k in ('resource', 'requested', 'available'))
    if (not isinstance(resource, str) or not re.fullmatch(r'[a-z][a-z0-9_]{0,63}', resource)
            or type(requested) is not int or type(available) is not int
            or not 0 <= available < requested):
        return ''
    return f" [{resource}: requested {requested}, available {available}; waiting for release]"


def pool_job_lines(jobs: Sequence[Mapping[str, object]], summary: Mapping[str, object]) -> list[str]:
    if not jobs:
        return ["no jobs ready or claimed" if summary.get('empty') is True else "pool job state unavailable"]
    return render_table(("KEY", "STATE", "NODE", "RESOURCES", "AGE", "LEASE", "OUTPUT", "PROGRESS",
                         "PASSES", "DENIAL", "RELEASES", "MATCHING", "NOTE"), (
        (j['action_key_prefix'], j['state'], j.get('node'), j.get('resources'), j.get('age_s'),
         j.get('lease_age_s'), (j.get('execution_observation') or {}).get('last_output_age_s'),
         j.get('progress_observation'),
         j.get('admission_passes'), None if not j.get('admission_denials') else '; '.join(
             f"{denial['host']}: {denial['reason']}"
             + (f"/{denial['decision_reason']}" if denial.get('decision_reason') else '')
             + _token_shortage_text(denial)
             + f" ({denial['age_s']:.0f}s ago)"
             for denial in j['admission_denials']),
         j.get('unstarted_releases'), j.get('placeable_hosts'),
         j.get('reason')) for j in jobs))


def recent_ending_paths(queue_root: str | Path, limit: int) -> list[os.DirEntry]:
    """Read-only terminal-entry selection shared by structured status readers.

    Returns the newest entries, including durable withdrawal decisions, without
    expanding record payloads. Filesystem errors propagate; callers must bound
    the shared-mount read.
    """
    return _ending_paths(queue_root, limit)


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
                    entries.append((entry.stat().st_mtime, entry))
                    if state == pool.WITHDRAWN:
                        withdrawal_mtimes[entry.name[:-5]] = entry.stat().st_mtime
        except FileNotFoundError:
            try:
                directory.stat()
            except FileNotFoundError:
                continue
            # A predicate would also return False on EACCES/EIO in Python 3.14.
            # Only a second ENOENT proves this optional directory is absent.
            raise  # The directory exists, but a selected entry was not readable.
    # A cancellation is durable before its visible summary is written. Keep
    # that ending visible if the operator crashed between the two writes.
    decisions = Path(queue_root) / pool.WITHDRAWN / "decisions"
    try:
        with os.scandir(decisions) as scan:
            directories = [entry for entry in scan if entry.is_dir()]
    except FileNotFoundError:
        directories = []
    for directory in directories:
        with os.scandir(directory.path) as scan:
            candidates = [(entry.stat().st_mtime, entry) for entry in scan
                          if entry.name.endswith(".json")]
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
        **{field: None for field in pool.RESOURCE_SUMMARY_FIELDS},
        "unreadable": reason,
        # The record's own finished time is unreadable too, so the file's
        # modification time is what places the row. It is the same clock
        # ``_ending_paths`` selected the record by.
        "finished_unix": mtime,
        "timing_finished_unix": None,
        "published_unix": None,
        "claimed_unix": None,
        "path": entry.path,
    }


class _EndingRecordTooLarge(ValueError):
    pass


def _read_ending(entry: os.DirEntry, queue_root: str | Path) -> dict:
    """Read one selected terminal record without an unbounded allocation."""

    if entry.stat().st_size > MAX_ENDING_RECORD_BYTES:
        raise _EndingRecordTooLarge
    path = Path(entry.path)
    if path.parent.parent.name == "decisions":
        return pool.PoolQueue(Path(queue_root))._read_withdrawal_decision(path)
    with path.open("rb") as handle:
        raw = handle.read(MAX_ENDING_RECORD_BYTES + 1)
    if len(raw) > MAX_ENDING_RECORD_BYTES:
        raise _EndingRecordTooLarge
    return json.loads(raw)


def _unreadable_reason(exc: Exception) -> str:
    """Why a record could not be read, in the terms that pick the fix.

    A record nobody may read and a record nobody can parse are different
    faults: the first is a mode or a mount, the second is a writer that
    stopped. Terminal records are published by rename, so a half-written one
    is a fault rather than a write still in flight.
    """

    if isinstance(exc, _EndingRecordTooLarge):
        return f"record exceeds {MAX_ENDING_RECORD_BYTES}-byte reader limit"
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

    reader = _KEPT_READS.get()
    if reader is not None:
        return reader.endings(Path(queue_root), limit)
    rows = [ending_row(entry, queue_root)
            for entry in _ending_paths(queue_root, limit)]
    rows.sort(key=lambda row: row["finished_unix"], reverse=True)
    return rows


def ending_row(entry: os.DirEntry, queue_root: str | Path) -> dict:
    """One selected terminal record as :func:`read_endings` reports it.

    ``entry`` is anything with the ``path``, ``name`` and ``stat()`` of an
    ``os.DirEntry``: ``pbmetrics`` passes the entries it keeps between
    scrapes, so both readers project a record through this one function.
    """

    try:
        record = _read_ending(entry, queue_root)
    except (OSError, ValueError, pool.PoolContractError) as exc:
        # Not dropped. ``limit`` sliced the newest records before any of
        # them was read, so dropping one printed the same empty table a
        # fleet that had filed nothing prints, and the two states call for
        # opposite responses.
        return _unreadable_row(entry, _unreadable_reason(exc))
    if not isinstance(record, dict):
        return _unreadable_row(entry, "not a JSON object")
    detail = record.get("detail")
    detail = detail if isinstance(detail, dict) else {}
    slurm = detail.get("slurm")
    slurm = slurm if isinstance(slurm, dict) else {}
    try:
        mtime = entry.stat().st_mtime
    except OSError:
        mtime = 0.0
    finished = record.get("finished_unix")
    return {
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
        # Why a withdrawal happened, when admission rather than an operator
        # made it: the foreground action this one yielded the box to
        # (#364).  A field rather than prose in ``reason`` so a reader can
        # test it, and present as ``None`` everywhere else for the same
        # reason ``unreadable`` is.
        "preempted_by": record.get("preempted_by"),
        # What the run cost and how loaded its box was (#372 Tier 0).
        # Absent, not zero, on every record filed before it existed.
        **pool.resource_profile_summary(detail),
        # Where a profiled run left its profile (#372 Tier 1).  ``None``
        # on every other row, which is most of them.
        "profile": detail.get("profile"),
        # Present on every row so a reader of the JSON can test one field
        # rather than the absence of one.
        "unreadable": None,
        "finished_unix": (
            float(finished)
            if isinstance(finished, (int, float)) else mtime
        ),
        # Keep the record's own value separate from the filesystem-mtime
        # fallback above. The fallback orders and selects status rows; it
        # is not evidence of when execution finished.
        "timing_finished_unix": finished,
        "published_unix": record.get("published_unix"),
        "claimed_unix": record.get("claimed_unix"),
        "path": entry.path,
    }


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


def queue_root_note(queue_root: str | Path, *, raise_errors: bool = False) -> str | None:
    """Why this root can file no endings, or ``None`` if it could file some.

    An empty table used to answer three questions with one sentence: a fleet
    that has filed nothing, a path that does not exist, and a path that is not
    a queue.  Only the first means wait.  A mistyped ``--queue-root`` read as
    the first, so an operator waited on a screen that could never fill.

    By default, read failures become a note for direct display callers.
    The bounded census requests exceptions so its completeness flag also
    records the failed read; turning an exception into text must not certify it.
    """

    root = Path(queue_root)
    try:
        try:
            root_stat = root.stat()
        except FileNotFoundError:
            return f"no queue root at {root}: the path does not exist"
        if not stat.S_ISDIR(root_stat.st_mode):
            return f"no queue root at {root}: the path is not a directory"
        filed_in = []
        for state in (pool.DONE, pool.FAILED, pool.WITHDRAWN):
            try:
                terminal_stat = (root / state).stat()
            except FileNotFoundError:
                continue
            if stat.S_ISDIR(terminal_stat.st_mode):
                filed_in.append(state)
    except OSError as exc:
        if raise_errors:
            raise
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
        "RECEIPT", "SLURM", "RESOURCE", "NOTE",
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
            # Peak memory, the bytes the action moved, and the GPU power it
            # drew against the device's own reference -- one column, because
            # three numbers nobody can see beside the run they belong to are
            # three numbers nobody reads.
            pool.describe_resource_profile(ending),
            # The path and the reason, on the one kind of row where every
            # other column is inside a file nobody could read -- and otherwise
            # the cost of a preemption, which is the one ending whose cause is
            # another action rather than this one's own exit.
            f"{reason}: {ending.get('path')}" if reason
            else f"preempted by {str(ending['preempted_by'])[:12]}"
            if ending.get("preempted_by")
            # A profiled ending says where its blob is, so a human can open it
            # in speedscope without going back to the record.
            else pb.describe_profile(ending.get("profile"))
            or ABSENT,
        ))
    return render_table(headers, rows)


class Deadline:
    """The whole run's budget, so that one slow section cannot spend another's.

    Held in ``time.monotonic`` because a status screen must not change its
    mind about how long it has waited when NTP steps the clock.  A budget of
    zero or less means no deadline at all, which is what this command did
    before issue #350 and what ``--timeout-s 0`` still asks for.

    Only zero asks for that, so a non-finite budget is refused rather than
    quietly granted one.  ``NaN`` compares false against everything, so it
    would leave ``bounded`` false and select the unbounded path without ever
    saying so; positive infinity would reach ``select`` as an infinite timeout,
    which is the same waiting-forever this class exists to end.
    """

    def __init__(self, timeout_s: float) -> None:
        self.timeout_s = float(timeout_s)
        if not math.isfinite(self.timeout_s):
            raise ValueError(
                f"a deadline must be a finite number of seconds, not {timeout_s!r}")
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


def _load_module(path: Path, name: str):
    """Load a fleet helper that is a script rather than an importable module."""

    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"{path} cannot be loaded as a module")
    module = importlib.util.module_from_spec(spec)
    # A status observation must not create __pycache__ in a mutable checkout.
    exec(compile(path.read_bytes(), str(path), "exec"), module.__dict__)
    return module


def nfs_readahead_reading(*, helper_path: str | Path | None = None,
                          mountinfo: str | Path | None = None,
                          bdi_root: str | Path | None = None) -> dict:
    """Report this box's /mnt/shared NFS readahead window. Never refuse on it.

    Issue #523 measured both Spark clients at ``read_ahead_kb=1024`` with
    ``rsize=1M``, which holds a serial reader to about one READ RPC in flight,
    and proposes 16 MiB. Nothing in the fleet could say what the window is
    now, so a box could sit at the low value indefinitely with no operator
    seeing it. This says.

    Three properties this must keep, and they are the whole design:

    *   **Read-only.** Mount discovery is ``nfs_readahead.shared_mount`` and
        the reading is ``nfs_readahead.observe``, which writes nothing. The
        window is changed by an operator running the host unit, never here.
    *   **Never a gate.** The result reaches ``--json`` and, when there is
        something to act on, one stderr line. It reaches neither
        ``timed_out_sections``, ``unavailable_sections``, ``complete`` nor the
        exit status. A host setting that no admission decision depends on must
        not become one by being reported.
    *   **Quiet where there is nothing to say.** dl380g10 is the storage
        server and has no NFS client mount at /mnt/shared, so ``shared_mount``
        raises there every time. That is the box being what it is, not a
        fault: it reads as ``not_nfs_client``, carries its reason, and warns
        about nothing.

    The attributes are local, but loading the helper reads the generation on
    NFS. ``main`` bounds this entire call after the required census reads,
    using their remaining deadline and a short optional-observation cap.

    Scope: **this box only.** The reading is local sysfs, so a
    ``pbstatus`` run reports the host it runs on and says nothing about any
    other client in the fleet. Fleet-wide reporting would have to carry the
    value in the worker offer (``pool_offer.v1``); that is a separate change
    and is not made here.
    """

    reading = {"check": "nfs_readahead", "mount": "/mnt/shared",
               "state": "unavailable", "read_ahead_kib": None,
               "recommended_kib": None, "bdi": None, "source": None,
               "warning": False, "note": ""}
    path = (Path(helper_path) if helper_path is not None
            else RUNTIME_ROOT / NFS_READAHEAD_HELPER)
    try:
        helper = _load_module(path, "pbstatus_nfs_readahead")
        reading["recommended_kib"] = helper.RECOMMENDED_KIB
        observe = helper.observe
    except Exception as exc:                       # noqa: BLE001 - diagnostic
        # A generation published before this helper travelled, or a checkout
        # without it.  The check did not run; saying so is the honest answer
        # and is not a fault of the box being looked at.
        reading["note"] = (
            f"host storage: the /mnt/shared readahead helper at {path} could "
            f"not be loaded ({type(exc).__name__}); the window was not read")
        return reading
    overrides = {}
    if mountinfo is not None:
        overrides["mountinfo"] = Path(mountinfo)
    if bdi_root is not None:
        overrides["bdi_root"] = Path(bdi_root)
    try:
        observed = observe(**overrides)
    except ValueError as exc:
        reading["state"] = "not_nfs_client"
        reading["note"] = (
            f"host storage: /mnt/shared is not a single NFS client export on "
            f"this box ({exc}); no readahead window to report")
        return reading
    except OSError as exc:
        reading["state"] = "unreadable"
        reading["note"] = (
            f"host storage: /mnt/shared is an NFS client mount and its "
            f"readahead window could not be read ({type(exc).__name__}: {exc})")
        return reading
    except Exception as exc:                       # noqa: BLE001 - diagnostic
        reading["state"] = "unreadable"
        reading["note"] = (
            f"host storage: reading the /mnt/shared readahead window raised "
            f"{type(exc).__name__}: {exc}")
        return reading
    for field in ("bdi", "source", "read_ahead_kib", "recommended_kib"):
        reading[field] = observed[field]
    kib, recommended = observed["read_ahead_kib"], observed["recommended_kib"]
    if kib < recommended:
        reading["state"] = "below_recommended"
        reading["warning"] = True
        reading["note"] = (
            f"host storage: /mnt/shared read_ahead_kb={kib} on bdi "
            f"{observed['bdi']} ({observed['source']}) is below the "
            f"{recommended} KiB proposed in issue #523.  Reported only -- "
            f"nothing is refused and no admission changes.  See "
            f"docs/fleet_storage.md to install the host unit")
    else:
        reading["state"] = "ok"
        reading["note"] = (
            f"host storage: /mnt/shared read_ahead_kb={kib} on bdi "
            f"{observed['bdi']} meets the {recommended} KiB recommendation")
    return reading


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
    moved out of the way first when the pipe landed on 0, 1 or 2.  It is moved
    in a loop, not once: ``os.dup`` hands back the lowest free descriptor, and
    when the caller closed its own standard streams the lowest free descriptor
    is another one below 3 -- the read end this child has just closed.  Each
    turn of the loop spends one of the three low slots, so it ends after at
    most three, and the copies left behind are closed by the ``dup2`` below.
    """
    while write_fd < 3:
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
            cap_s: float | None = None, announce_retained: bool = True) -> dict:
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

    ``announce_retained=False`` leaves the naming of a retained reader to the
    caller, which gets the same record in ``abandoned``.  A waiting client
    meets a retained reader once per turn of its wait and prints its own
    notice at its own interval (#1048); the line below is not throttled.  A
    signal that unwinds this call still prints it, because then the caller
    never sees ``abandoned``.
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

    # Reuse the worker's signal unwinding contract so a signal aimed only at
    # this parent still reaches the exact reader it owns through cleanup.
    with _sigterm_unwinds_this_process():
        token = _ANNOUNCE_RETAINED.set(announce_retained)
        try:
            return _bounded_reader(section, read, deadline=deadline,
                                   abandoned=abandoned, cap_s=cap_s)
        finally:
            _ANNOUNCE_RETAINED.reset(token)


#: Whether ``_stop_reader`` names a retained reader on stderr; ``bounded``
#: sets it for the one call.  A context variable, so a waiting client's threads
#: (``pbwait.wait_for_keys``) each keep their own, and every stand-in for
#: ``_stop_reader`` keeps its signature.
_ANNOUNCE_RETAINED: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "pbstatus_announce_retained", default=True)


def _stop_reader(pid: int, section: str, started: float, abandoned: list) -> None:
    try:
        os.kill(pid, signal.SIGKILL)
    except OSError:
        pass
    if not _reap_within(pid, KILL_GRACE_S):
        child = {"section": section, "pid": pid,
                 "starttime_ticks": _starttime_ticks(pid),
                 "since_unix": round(time.time() - (time.monotonic() - started), 3)}
        abandoned.append(child)
        # Also report ownership during cancellation, when main cannot emit JSON.
        # A caller that asked to name the reader itself cannot while an
        # exception (a signal's unwinding) is on its way out, so say it here.
        if _ANNOUNCE_RETAINED.get() or sys.exc_info()[1] is not None:
            print(f"pbstatus: retained reader {json.dumps(child, sort_keys=True)}",
                  file=sys.stderr)


def _bounded_reader(section: str, read, *, deadline: Deadline, abandoned: list,
                    cap_s: float | None) -> dict:
    read_fd, write_fd = os.pipe()
    started = time.monotonic()
    try:
        pid = os.fork()
    except BaseException:
        os.close(read_fd)
        os.close(write_fd)
        raise
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

    reaped = False
    try:
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
        elapsed = time.monotonic() - started

        # EOF is the only proof the payload is whole.  A census can be larger than
        # the pipe buffer, so "some bytes arrived" is compatible with a child that
        # is still writing, and reporting that as a parse error would file a mount
        # timeout under the wrong cause.
        if saw_eof and chunks:
            reaped = _reap_within(pid, KILL_GRACE_S)
            try:
                return json.loads(b"".join(chunks).decode("utf-8"))
            except ValueError as exc:
                return {"status": "error", "type": "ValueError",
                        "error": f"unreadable {section} payload: {exc}"}
        if saw_eof:
            reaped = _reap_within(pid, KILL_GRACE_S)
            return {"status": "error", "type": "RuntimeError",
                    "error": f"the {section} reader exited without a payload"}

        return {"status": "timed_out", "elapsed_s": round(elapsed, 3), "started": True}
    finally:
        try:
            os.close(read_fd)
        finally:
            if not reaped:
                _stop_reader(pid, section, started, abandoned)


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
        "--blocked-origins", action="store_true",
        help="print one JSON blob listing every consumed origin batch whose "
             "remaining consumers all failed or were withdrawn, with the "
             "pbrun --release-origin-consumer and --supersedes remedies for "
             "each (#926), and every write-only prewrite whose attempt ended "
             "before committing and whose files belong to no batch (#949), "
             "and nothing else")
    parser.add_argument(
        "--starvation", action="store_true",
        help="print one JSON blob answering what is waiting on data "
             "(quiet claimed consumers, residency plan promotion state, "
             "cursor gaps, tier fill and occupancy, denial tops, and the "
             "not_observable gap list), and nothing else")
    parser.add_argument(
        "--spool-retirements", action="store_true",
        help="print one JSON blob with every host's produced-spool "
             "retirements (the newest --recent lines) and each host's latest "
             "retirement tick, including the namespaces it kept and why "
             "(#1001), and nothing else")
    parser.add_argument(
        "--deferred", action="store_true",
        help="print one JSON blob listing every consumer filed with pbrun "
             "--after and not yet released, with the runtime generation it is "
             "pinned to and whether that is the published one "
             "(off_published), and nothing else")
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
    parser.add_argument("--repo-link", default=str(SHARED_ROOT / "repo"),
                        help="published runtime link used to read rollout status")
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
    transport = args.transport
    if args.recent < 0:
        parser.error("--recent cannot be negative")
    # Finiteness first, because ``-inf < 0`` is true and would answer with the
    # wrong complaint: the fault in ``nan``, ``inf`` and ``-inf`` alike is that
    # they are not a number of seconds, and only 0 asks for no deadline.
    if not math.isfinite(args.timeout_s):
        parser.error("--timeout-s must be a finite number of seconds; "
                     "0 means no deadline")
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

    if args.deferred:
        # Pool-only, like --starvation: deferred submissions are pull-queue
        # records (#913).  The published generation is the repo link's.
        def read_deferred_listing() -> dict:
            try:
                published = Path(args.repo_link).resolve(strict=True)
            except OSError:
                published = None
            return action_edges.deferred_listing(
                args.queue_root, published_root=published)

        read = bounded("deferred", read_deferred_listing, deadline=deadline,
                       abandoned=abandoned)
        if read["status"] == "ok":
            print(json.dumps(read["value"], sort_keys=True, indent=1))
            return 0 if read["value"]["complete"] else EXIT_INCOMPLETE
        if read["status"] == "error":
            print(f"pbstatus: deferred read failed "
                  f"({read['type']}: {read['error']})", file=sys.stderr)
        else:
            print(f"pbstatus: deferred read did not answer within "
                  f"{args.timeout_s:g}s", file=sys.stderr)
        return EXIT_INCOMPLETE

    if args.spool_retirements:
        # Pool-only: each host's retirement tick files its records under the
        # queue root (#1001).
        from prismabuild import produced_spool

        read = bounded("spool-retirements",
                       lambda: produced_spool.retirement_records(
                           pool.PoolQueue(args.queue_root), limit=args.recent),
                       deadline=deadline, abandoned=abandoned)
        if read["status"] == "ok":
            print(json.dumps(read["value"], sort_keys=True, indent=1))
            return 0
        if read["status"] == "error":
            print(f"pbstatus: spool-retirements read failed "
                  f"({read['type']}: {read['error']})", file=sys.stderr)
        else:
            print(f"pbstatus: spool-retirements read did not answer within "
                  f"{args.timeout_s:g}s", file=sys.stderr)
        return EXIT_INCOMPLETE

    if args.blocked_origins:
        # Pool-only: consumed batches and their declarations are pull-queue
        # records (#914, #926).
        read = bounded("blocked-origins",
                       lambda: read_blocked_origins(args.queue_root),
                       deadline=deadline, abandoned=abandoned)
        if read["status"] == "ok":
            print(json.dumps(read["value"], sort_keys=True, indent=1))
            return 0 if read["value"]["complete"] else EXIT_INCOMPLETE
        if read["status"] == "error":
            print(f"pbstatus: blocked-origins read failed "
                  f"({read['type']}: {read['error']})", file=sys.stderr)
        else:
            print(f"pbstatus: blocked-origins read did not answer within "
                  f"{args.timeout_s:g}s", file=sys.stderr)
        return EXIT_INCOMPLETE

    if args.starvation:
        # One blob, one section, same deadline machinery as every other read
        # of the shared mount.  Pool-only by construction: starvation is a
        # property of the pull queue's residency records, not of a scheduler.
        read = bounded("starvation", lambda: read_starvation(args.queue_root),
                       deadline=deadline, abandoned=abandoned)
        if read["status"] == "ok":
            print(json.dumps(read["value"], sort_keys=True, indent=1))
            return 0 if read["value"]["complete"] else EXIT_INCOMPLETE
        if read["status"] == "error":
            print(f"pbstatus: starvation read failed "
                  f"({read['type']}: {read['error']})", file=sys.stderr)
        else:
            print(f"pbstatus: starvation read did not answer within "
                  f"{args.timeout_s:g}s", file=sys.stderr)
        return EXIT_INCOMPLETE

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

    transport_note = None
    if transport is None:
        selected = bounded("transport", default_transport, deadline=deadline,
                           abandoned=abandoned)
        if selected["status"] == "ok":
            transport = selected["value"]
        elif selected["status"] == "error":
            unavailable.append({"section": "transport", "type": selected["type"],
                                "error": selected["error"]})
            transport_note = f"transport: unavailable ({selected['type']}: {selected['error']})"
        else:
            timed_out.append("transport")
            transport_note = "transport: incomplete -- runtime metadata did not answer"

    notes: list[str] = [transport_note] if transport_note else []
    nodes: list[dict] = []
    jobs: list[dict] = []
    node_note = job_note = transport_note
    detail_note = None
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
    elif transport == "slurm":
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
        for ending in endings:
            if ending.get("unreadable"):
                unavailable.append({"section": "endings", "type": "UnreadableRecord",
                                    "error": f"{ending['path']}: {ending['unreadable']}"})
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
        read = bounded("queue-root", lambda: queue_root_note(args.queue_root, raise_errors=True),
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

    # Required fleet reads go first. Loading this optional helper also reads
    # NFS, so it must share the deadline and exact-child cleanup machinery.
    # Its availability says nothing about census completeness or admission.
    storage_read = bounded("host-storage", nfs_readahead_reading,
                           deadline=deadline, abandoned=abandoned,
                           cap_s=HOST_STORAGE_BUDGET_S)
    if storage_read["status"] == "ok":
        host_storage = storage_read["value"]
    else:
        host_storage = {
            "check": "nfs_readahead", "mount": "/mnt/shared",
            "state": "unavailable", "read_ahead_kib": None,
            "recommended_kib": None, "bdi": None, "source": None,
            "warning": False, "read_status": storage_read["status"],
            "note": "host storage: readahead observation unavailable "
                    f"({storage_read['status']}); the window was not read",
        }
        if storage_read["status"] == "error":
            host_storage["error"] = storage_read["error"]
        print(host_storage["note"], file=sys.stderr)
    if host_storage["warning"] or host_storage["state"] == "unreadable":
        print(host_storage["note"], file=sys.stderr)

    rollout = None
    read = bounded("rollout", lambda: read_rollout_summary(args.repo_link),
                   deadline=deadline, abandoned=abandoned)
    if read["status"] == "ok":
        rollout = read["value"]
    elif read["status"] == "error":
        unavailable.append({"section": "rollout", "type": read["type"],
                            "error": read["error"]})
    else:
        timed_out.append("rollout")

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
            # This box's /mnt/shared NFS readahead window, read-only and
            # scoped to the host this ran on (#523).  Deliberately not folded
            # into ``scheduler``: that list is what the census could not read,
            # and a wrapper acting on it would then act on a host setting that
            # gates nothing.  It never changes ``complete`` or the exit status.
            "host_storage": host_storage,
            "pool": pool_summary,
            "nodes": nodes,
            "jobs": jobs,
            "endings": endings,
            "rollout": rollout,
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
    print()
    print("== rollout")
    print("\n".join(rollout_lines(rollout)))
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
    if "transport" in timed_out:
        print(f"pbstatus: incomplete -- runtime metadata did not answer within "
              f"{timeout_s:g}s (transport pending)", file=sys.stderr)
    queue_pending = [section for section in timed_out if section != "transport"]
    if queue_pending:
        print(f"pbstatus: incomplete -- queue root did not answer within "
              f"{timeout_s:g}s ({', '.join(queue_pending)} pending)", file=sys.stderr)
    for section in unavailable:
        print(f"pbstatus: incomplete -- the {section['section']} read failed "
              f"({section['type']}: {section['error']})", file=sys.stderr)
    if pool_partial:
        print("pbstatus: incomplete -- the pool census could not read every "
              "record; the counts above are partial", file=sys.stderr)
    return EXIT_INCOMPLETE


if __name__ == "__main__":
    raise SystemExit(main())
