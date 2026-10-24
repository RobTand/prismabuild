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
import importlib.util
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
_PB_ENTRYPOINT = Path(__file__).resolve(strict=True)
sys.path.insert(0, str(_PB_ENTRYPOINT.parents[
    2 if _PB_ENTRYPOINT.parent.name == "fleet" else 1] / "src"))
from prismabuild.runtime_paths import generation_root  # noqa: E402
from prismabuild.fleet_submit import TRANSPORTS, default_transport  # noqa: E402
import upgrade_client  # noqa: E402

RUNTIME_ROOT = generation_root(__file__)
sys.path.insert(0, str(RUNTIME_ROOT / "src"))
from prismabuild import census as fleet_census  # noqa: E402
from prismabuild import (  # noqa: E402
    _bounded_reader as _reader, core as pb, pool, slurm_lane as sl,
)
from prismabuild import action_edges  # noqa: E402

#: Where the fleet keeps the queue both transports file their endings in.  The
#: same spelling ``pbrun`` and ``pool_reset`` use.
SHARED_ROOT = Path("/mnt/shared/prismabuild-fleet")
DEFAULT_QUEUE_ROOT = SHARED_ROOT / "pb-queue"


def rollout_lines(summary: Mapping[str, object] | None) -> list[str]:
    if summary is None:
        return ["rollout status unavailable"]
    if summary.get("state") == "idle":
        return ["no active rollout epoch"]
    outstanding = ", ".join(summary.get("outstanding_hosts") or []) or ABSENT
    return [f"epoch {summary.get('epoch')}  live {summary.get('live_generation')}",
            f"pending {summary.get('pending_phase')}  outstanding {outstanding}"]

#: A ``pending`` canary record older than this is a canary that started and
#: never reported (#978): the process died or the box rebooted, and nothing
#: else will ever replace the record.  Six hours is longer than any canary run
#: the driver's own budgets allow, so a live run is never called stale.
CANARY_PENDING_STALE_S = 6 * 3600.0

#: Canary states that mean "no verdict exists for this generation".
CANARY_NEEDS_RUN = frozenset({"missing", "unreadable", "skipped", "pending_stale"})


def read_canary_summary(repo_link: str | Path, now: float | None = None) -> dict:
    """Say whether the active generation has a canary verdict (#978).

    The verdict is the ``<generation>.canary.json`` sidecar the publisher
    writes beside the sealed generation.  A generation that was staged in
    PrismaBuild and activated outside it has no sidecar, and that absence is
    the case this read exists to name.  Read-only, and it never raises: a link
    that does not resolve is ``unavailable``, not a missing verdict.
    """
    link = Path(repo_link)
    try:
        generation = link.resolve(strict=True).name
    except OSError as exc:
        return {"state": "unavailable", "generation": None, "record": None,
                "error": f"{type(exc).__name__}: {exc}"}
    record_path = link.parent / "runtime-generations" / f"{generation}.canary.json"
    base = {"generation": generation, "record": str(record_path)}
    try:
        raw = record_path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return {**base, "state": "missing"}
    except OSError as exc:
        return {**base, "state": "unreadable",
                "error": f"{type(exc).__name__}: {exc}"}
    try:
        record = json.loads(raw)
        status = record["canary_status"]
        if record.get("generation") != generation or not isinstance(status, str):
            raise ValueError("record does not describe this generation")
    except (ValueError, KeyError, TypeError) as exc:
        return {**base, "state": "unreadable",
                "error": f"{type(exc).__name__}: {exc}"}
    recorded = record.get("recorded_unix")
    age = (time.time() if now is None else now) - recorded \
        if isinstance(recorded, (int, float)) else None
    state = status
    if status == "pending" and (age is None or age > CANARY_PENDING_STALE_S):
        state = "pending_stale"
    elif status == "not_run" and record.get("canary_exit") is None:
        state = "skipped"  # the publisher skipped it; the canary never ran
    return {**base, "state": state, "canary_exit": record.get("canary_exit"),
            "age_s": age, "detail": record.get("detail")}


def canary_lines(summary: Mapping[str, object] | None) -> list[str]:
    if summary is None or summary.get("state") == "unavailable":
        return ["canary status unavailable"]
    generation, state = summary.get("generation"), summary.get("state")
    if state == "missing":
        return [f"missing canary verdict for generation {generation}"]
    if state == "unreadable":
        return [f"unreadable canary verdict for generation {generation}: "
                f"{summary.get('error')}"]
    if state == "skipped":
        return [f"missing canary verdict for generation {generation} "
                "(the publisher skipped the canary)"]
    if state == "pending_stale":
        return [f"missing canary verdict for generation {generation} "
                "(a canary started and never reported)"]
    return [f"generation {generation}  canary {state}"]

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
KILL_GRACE_S = _reader.KILL_GRACE_S

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
#: and this is a warning line, not a fleet_census.
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
                try:
                    row["priority_reason"] = pool.normalize_priority_reason(
                        record.get("priority_reason"))
                except pool.PoolContractError as exc:
                    row["priority_reason"] = None
                    row["note"] = str(exc)
                recorded_id = str(record.get("job_id") or "")
                if recorded_id and recorded_id != job_id:
                    # `latest.json` points at the newest submission of this
                    # action key, which after a retry or a resubmission is a
                    # different job from the one in this row.
                    row["note"] = "; ".join(filter(None, (
                        row["note"], f"the lane's latest record is job {recorded_id}"
                    )))
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


def _measurement_drain_text(denial: Mapping[str, object]) -> str:
    evidence = denial.get('evidence')
    verdict = evidence.get('withhold') if isinstance(evidence, Mapping) else None
    if isinstance(verdict, Mapping) and verdict.get('why') == 'draining_for_measurement':
        return f"/draining_for_measurement {str(verdict.get('measurement_key', ''))[:12]}"
    return ''


def pool_job_lines(jobs: Sequence[Mapping[str, object]], summary: Mapping[str, object]) -> list[str]:
    if not jobs:
        return ["no jobs ready or claimed" if summary.get('empty') is True else "pool job state unavailable"]
    return render_table(("KEY", "STATE", "NODE", "RESOURCES", "PRIORITY", "PRIORITY REASON",
                         "AGE", "LEASE", "OUTPUT", "PROGRESS",
                         "PASSES", "DENIAL", "RELEASES", "MATCHING", "NOTE"), (
        (j['action_key_prefix'], j['state'], j.get('node'), j.get('resources'),
         j.get('priority'), j.get('priority_reason'), j.get('age_s'),
         j.get('lease_age_s'), (j.get('execution_observation') or {}).get('last_output_age_s'),
         j.get('progress_observation'),
         j.get('admission_passes'), None if not j.get('admission_denials') else '; '.join(
             f"{denial['host']}: {denial['reason']}"
             + (f"/{denial['decision_reason']}" if denial.get('decision_reason') else '')
             + _token_shortage_text(denial)
             + _measurement_drain_text(denial)
             + f" ({denial['age_s']:.0f}s ago)"
             for denial in j['admission_denials']),
         j.get('unstarted_releases'), j.get('placeable_hosts'),
         j.get('reason')) for j in jobs))


def legacy_custody_lines(holds):
    """Show holds that still need the explicit pre-contract adoption step."""
    if not holds:
        return ["no pre-contract holds awaiting custody adoption"]
    return render_table(("LEDGER", "HOLDER", "CUSTODY", "REASON"), (
        (row["kind"] + ":" + row["ledger"], row["holder"][:12], row["status"], row["reason"] + (
            " host=" + str(row["proof"].get("host")) + " roots=" + ",".join(
                str(scan["path"]) for scan in row["proof"]["scan"]["roots"])
            if row.get("proof") else ""))
        for row in holds))


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


def _superseded_note(ending: Mapping[str, object]) -> str:
    """The NOTE for a retained earlier generation's row (#1178)."""

    answer = ending.get("superseded_by")
    if not isinstance(answer, Mapping):
        return ABSENT
    generation = answer.get("generation")
    stamp = f" {float(generation):.1f}" if isinstance(
        generation, (int, float)) else ""
    return f"superseded by {answer.get('state')}{stamp}"


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
        "RESOURCES", "CONSTRAINT", "FROM", "PRIORITY REASON", "REASON",
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
            job.get("constraint"), job.get("submitted_host"), job.get("priority_reason"),
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
        for state in (pool.record_store.DONE, pool.record_store.FAILED, pool.record_store.WITHDRAWN):
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
            else _superseded_note(ending)
            if ending.get("superseded_by") else
            f"preempted by {str(ending['preempted_by'])[:12]}"
            if ending.get("preempted_by")
            # A profiled ending says where its blob is, so a human can open it
            # in speedscope without going back to the record.
            else pb.describe_profile(ending.get("profile"))
            or ABSENT,
        ))
    return render_table(headers, rows)


Deadline = _reader.Deadline
_proc_stat_fields = _reader._proc_stat_fields


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


def bounded(section: str, read, *, deadline: _reader.Deadline, abandoned: list,
            cap_s: float | None = None, announce_retained: bool = True) -> dict:
    """Compatibility entry point for the shared private reader infrastructure.

    The deadline, status dictionaries, JSON boundary and retained-child notice
    remain unchanged. The caller still owns and fences its abandoned readers.
    """
    return _reader.bounded(section, read, deadline=deadline, abandoned=abandoned,
                           cap_s=cap_s, announce_retained=announce_retained)


# Keep existing diagnostic imports discoverable; implementation/fault injection
# belongs to the core boundary rather than to this CLI's private name bindings.
def _reap_within(pid: int, grace_s: float) -> bool:
    """Existing retained-reader consumers use the same core reap boundary."""
    return _reader._reap_within(pid, grace_s)


_reap_status_within = _reader._reap_status_within
_reader_exit_detail = _reader._reader_exit_detail
_write_reader_payload = _reader._write_reader_payload
_isolate_child_fds = _reader._isolate_child_fds
_ANNOUNCE_RETAINED = _reader._ANNOUNCE_RETAINED
_stop_reader = _reader._stop_reader
_bounded_reader = _reader._bounded_reader
_starttime_ticks = _reader._starttime_ticks


def read_resident_sets(queue_root, *, now=None):
    """Read declarations, copy states, lease decisions and measured capacity."""
    from prismabuild import local_resident, resident_sets
    store = resident_sets.ResidentSets(queue_root)
    sets, capacity, unreadable = [], [], []
    try:
        directories = list(store.root.iterdir())
    except FileNotFoundError:
        directories = []
    except OSError as exc:
        directories = []
        unreadable.append(f"{store.root}: {exc}")
    for directory in sorted(directories):
        try:
            if not directory.is_dir():
                continue
            try:
                (directory / "body.json").stat()
            except FileNotFoundError:
                continue  # A refused publication may leave only its lock.
            row = store.status(directory.name)
            row["lease_active"] = local_resident.lease_active(store, directory.name, now=now)
            sets.append(row)
        except (ValueError, OSError) as exc:
            unreadable.append(f"{directory}: {exc}")
    root = Path(queue_root) / "resident-capacity"
    try:
        paths = list(root.iterdir())
    except FileNotFoundError:
        paths = []
    except OSError as exc:
        paths = []
        unreadable.append(f"{root}: {exc}")
    for path in sorted(paths):
        try:
            row = json.loads(path.read_text())
            if not isinstance(row, dict) or not isinstance(row.get("host"), str):
                raise ValueError("invalid resident capacity record")
            ledger = pool.PoolQueue(queue_root).tier('local:' + row['host'])
            row["held"] = ledger.holdings()
            capacity.append(row)
        except (ValueError, OSError) as exc:
            unreadable.append(f"{path}: {exc}")
    return {"sets": sets, "capacity": capacity, "complete": not unreadable, "unreadable": unreadable}


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
    parser.add_argument("--resident-sets", action="store_true",
                        help="print resident sets, host copy states, leases and capacity as JSON")
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
    if args.resident_sets:
        read = bounded("resident-sets", lambda: read_resident_sets(args.queue_root),
                       deadline=deadline, abandoned=abandoned)
        if read["status"] == "ok":
            print(json.dumps(read["value"], sort_keys=True, indent=1))
            return 0 if read["value"]["complete"] else EXIT_INCOMPLETE
        print("pbstatus: resident-sets read " + read["status"], file=sys.stderr)
        return EXIT_INCOMPLETE

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
                       lambda: fleet_census.blocked_origins(args.queue_root).value,
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
        read = bounded("starvation", lambda: fleet_census.starvation(args.queue_root),
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
    # mistaken for part of the fleet_census.  Counted, never enforced: an operator
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
        read = bounded("pool", lambda: fleet_census.action_rows(args.queue_root).value,
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
    read = bounded("endings", lambda: fleet_census.endings(args.queue_root, limit=args.recent),
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
    read = bounded("rollout", lambda: upgrade_client.read_rollout_summary(args.repo_link),
                   deadline=deadline, abandoned=abandoned)
    if read["status"] == "ok":
        rollout = read["value"]
    elif read["status"] == "error":
        unavailable.append({"section": "rollout", "type": read["type"],
                            "error": read["error"]})
    else:
        timed_out.append("rollout")

    # Report-only (#978): a missing verdict is a line to act on, not a partial
    # census, so no outcome here changes ``complete`` or the exit status.
    canary = {"state": "unavailable", "generation": None, "record": None}
    read = bounded("canary", lambda: read_canary_summary(args.repo_link),
                   deadline=deadline, abandoned=abandoned)
    if read["status"] == "ok":
        canary = read["value"]

    legacy_custody = None
    if transport == "pool":
        from prismabuild.legacy_custody import LegacyCustodyMigration
        read = bounded("legacy-custody", lambda: LegacyCustodyMigration(
            pool.PoolQueue(args.queue_root)).status_rows(),
            deadline=deadline, abandoned=abandoned)
        if read["status"] == "ok":
            legacy_custody = read["value"]
        else:
            notes.append("legacy custody inventory unavailable; adoption status is unknown")

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
            "canary": canary,
            "legacy_custody": legacy_custody,
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
    if transport == "pool":
        print("== legacy cleanup custody")
        print("\n".join(legacy_custody_lines(legacy_custody)) if legacy_custody is not None
              else "legacy custody inventory unavailable; adoption status is unknown")
        print()
    print(f"== endings (newest {args.recent})")
    print("\n".join([ending_note] if ending_note
                     else ending_lines(endings, note=empty_note)))
    print()
    print("== rollout")
    print("\n".join(rollout_lines(rollout)))
    print()
    print("== canary")
    print("\n".join(canary_lines(canary)))
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
