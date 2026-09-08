"""Submit one sealed action to SLURM and wait for it.  Thin, on purpose.

``slurm.py`` is the other SLURM client in this tree and it is not this one.  It
exists for a restartable orchestrator: sealed submission intents, adoption of a
job whose submitter died mid-``sbatch``, append-only poll journals, a mutation
ledger.  Every one of those answers "who owns this job if the process that
submitted it is gone", and an interactive ``pbrun --wait`` has no such question
-- the submitter is a person's terminal, and when it dies the right answer is
``scancel``, not adoption.  So this module is a transport and nothing else:
write a script, ``sbatch`` it, record what was submitted, poll, report.

Four decisions are worth stating, because each had an alternative.

**The truth is the CAS, not the exit code.**  A job that exits 0 without
publishing a receipt did not do the work, and a job that exits non-zero after
publishing one did.  ``wait`` therefore reports the scheduler's state and log
paths as *diagnosis*; the caller asks ``cas.lookup(action)`` for the verdict.

The pull queue reaches the same answer by a different rule, and the difference
is deliberate rather than incidental.  Its verdict is the launcher's exit code
(``pool.py``: ``status = "executed" if process.returncode == 0 else "failed"``),
and the two rules agree whenever the launcher exits, because ``core.py``
publishes a receipt only on a clean run.  They part on one ending: a launcher
that publishes its receipt and is then signalled.  The queue files ``failed``;
this lane files ``executed``, because the receipt is in the CAS and the work it
attests was done.  The lane trusts the receipt.

**Submission does not use ``sbatch --wait``.**  It would hand back the job's
exit status directly, and it costs two things this lane needs more.  The job id
has to be durable *before* anything blocks -- otherwise a withdrawal has no id
to cancel and a killed ``pbrun`` leaks a running job -- and ``sbatch`` writes
that id to a pipe, where libc buffers it with no flush guarantee until exit.
So: ``sbatch --parsable`` returns immediately, the id is sealed to disk, and
``wait`` polls.

**Polling reads ``sacct`` first and does not depend on it.**  This fleet starts
without ``slurmdbd``, and with accounting storage off ``sacct`` fails for every
job, forever.  ``scontrol show job`` answers for ``MinJobAge`` seconds after a
job ends (the fleet config raises that deliberately, and says why), and
``squeue`` answers while it is alive.  ``sacct`` is tried first anyway, so that
deploying ``slurmdbd`` later is a configuration change and not a code change.

**A running job is never killed on elapsed time.**  ``wait`` samples what the
scheduler's accounting says the job is doing (``sstat``: CPU time, RSS, I/O)
and how its logs grow, appends each sample to ``liveness.jsonl`` in the lane
directory, and *reports* a job whose samples have not moved for
``STALL_WINDOW_S``.  It cancels nothing.  A stall ends by an operator's
``pbrun --withdraw`` or by a ``--time`` the submitter asked for, and both are
the same thing: a person's decision, recorded as one.  The liveness section
below states the policy and derives the window.

Retries are resubmissions, never ``--requeue``.  SLURM uses one flag for
operator requeue and automatic restart, so a requeued job cannot be
distinguished from a rescheduled one after the fact; ``--no-requeue`` is set and
a retry is a new job id with its own record.  The bound is the producer's
``max_attempts``, and it applies only to an action whose producer declared the
whole command retry-safe -- numerical determinism is not that declaration.

Stdlib only: this module is imported by the submitter, and the submitter is a
box that must not need a package installed to talk to the scheduler.
"""
from __future__ import annotations

from collections import deque
from collections.abc import Iterator, Mapping, Sequence
import contextlib
from dataclasses import dataclass, field
import getpass
import hashlib
import json
import math
import os
from pathlib import Path
import re
import secrets
import shlex
import socket
import subprocess
import time
from typing import Callable
import uuid

from . import core as pb
from . import posix_lock
from . import pool

SUBMISSION_SCHEMA_V1 = "prismaquant.prismabuild.slurm_lane_submission.v1"

#: Where a lane directory keeps the immutable batch scripts, one per distinct
#: set of bytes, named by their sha256.  ``job.sh`` beside it is a pointer to
#: the newest and is not what any job executes; see ``submit``.
SCRIPT_DIRNAME = "scripts"

#: The terminal record this lane files where the pull queue files its own.
#:
#: Eleven fleet tools and Tessera's ``merge_suite`` read one action's ending out
#: of ``pb-queue/done/<key>.json`` or ``pb-queue/failed/<key>.json``.  The queue
#: writes those from ``PoolQueue.finish``, on the worker holding the claim.
#: There is no such worker here, so the submitter writes them -- in the same two
#: directories, with the fields those readers name, under a schema id of its own
#: so that a reader can still tell a SLURM ending from a pull-queue one.
#:
#: **The submitter is the writer, and that is this record's one real gap.**  A
#: ``pbrun`` killed mid-wait, or one whose ``--wait-s`` expired, files nothing
#: for a job that ends afterwards.  ``finish`` runs on the box doing the work
#: and cannot miss it.  Closing this would mean writing the record from the job
#: script's own exit trap, which cannot see the CAS verdict the record reports.
OUTCOME_SCHEMA_V1 = "prismaquant.prismabuild.slurm_outcome.v1"

#: Where the lane keeps one directory per action: the script it submitted, the
#: sealed record of each submission, and the job's own stdout/stderr.  On the
#: shared mount because the submitter writes it and the compute node reads it.
#: Overridable so a test can drive the whole lane without a fleet.
LANE_ROOT_ENV = "PRISMABUILD_SLURM_LANE_ROOT"
DEFAULT_LANE_ROOT = "/mnt/shared/prismabuild-fleet/slurm"

#: Job-state files the node-side Epilog reads to clean up after a job it did
#: not launch.  One flat directory keyed by job id: the Epilog knows
#: ``SLURM_JOB_ID`` and nothing else for certain.
JOB_STATE_DIRNAME = "jobs"

#: Where those files live, resolved on the *node* and never by the submitter.
#:
#: The lane root above is a submitter-side record location and it is
#: overridable per process.  This one is not the same question.  A submitter
#: that exported ``PRISMABUILD_SLURM_LANE_ROOT`` used to bake its own answer
#: into the batch script, while the Epilog -- which cannot see slurmd's
#: environment, let alone the submitter's -- kept reading its own default.  The
#: two disagreed, the Epilog found no state file, and a killed job leaked its
#: checkout and its containers with nothing said.
#:
#: So the job script carries no job-state path at all.  ``slurm_job`` resolves
#: it on the node from ``JOB_STATE_ROOT_ENV`` or this default, and
#: ``fleet/slurm/epilog.sh`` reads the same variable and the same default --
#: the one place the two sides have to agree, spelled once on each side because
#: a shell script cannot import this module.  The value is what the Epilog
#: already hardcoded, so nothing about the deployed fleet moves.
JOB_STATE_ROOT_ENV = "PRISMABUILD_SLURM_JOB_STATE_ROOT"
DEFAULT_JOB_STATE_ROOT = f"{DEFAULT_LANE_ROOT}/{JOB_STATE_DIRNAME}"

#: ``/usr/bin/python3`` on both architectures, deliberately.  A job may land on
#: aarch64 or x86_64 and ``fleet_boxes.json`` names a different venv per box, so
#: no single venv path is correct for the launcher.  It does not need to be:
#: ``prismabuild_worker.py`` is stdlib-only by construction, and the action's
#: own sealed argv[0] selects whatever interpreter the work requires.
DEFAULT_JOB_PYTHON = "/usr/bin/python3"

#: The nice value a ``--priority 0`` job carries, and the origin every other
#: priority is measured from.
#:
#: SLURM has no submitter-settable priority *number*: ``PriorityType=priority/
#: basic`` orders by an internal base priority, and the one lever an
#: unprivileged submitter has over it is ``--nice``, which is SUBTRACTED from
#: that base.  So a higher pool priority has to become a smaller nice, and the
#: base exists because a NEGATIVE nice -- a boost -- requires SlurmUser
#: privilege that the submitting user does not have.  A base of 2**30 leaves
#: every realistic priority on the non-negative side of that line and inside
#: sbatch's +-2147483645 range.
#:
#: The scale is what makes priority mean what it meant on the pool.  The pool
#: sorted its ready queue on priority before age, so a ``--priority -10`` reset
#: sat behind every interactive item however old the reset grew.  Under
#: ``priority/basic`` the base priority steps down by one per submission and
#: the nice is subtracted from it, so a nice one unit larger sinks a job behind
#: exactly one later submission.  Multiplying the priority by 2**20 makes one
#: priority step outrank a million submissions, which is the pool's order for
#: any queue this fleet will hold.
#:
#: This is a queue hint and nothing more.  It is not part of the action
#: identity, it does not reach ``seal_action``, and two submissions of one
#: action that differ only in priority are the same action.
NICE_BASE = 1 << 30

#: Nice units per priority step; see ``NICE_BASE``.
NICE_SCALE = 1 << 20

#: The largest nice ``sbatch`` accepts (its manual: "adjustment range is
#: +/- 2147483645").  A priority of -1024 or below reaches it.
NICE_MAX = 2147483645

#: How long ``wait`` leaves between polls of a job that has not finished.
DEFAULT_POLL_S = 5.0

#: How long any one scheduler command may take before the lane gives up on it.
#: A hung ``squeue`` against a busy controller must not become a hung ``pbrun``
#: -- and, since the outage fix, not a dead one either: ``wait`` treats the
#: timeout as "no answer this poll" and asks again.
COMMAND_TIMEOUT_S = 60.0

#: How often ``wait`` repeats that it cannot reach the scheduler.  A bound on
#: chatter, not evidence of anything.
NOTICE_EVERY_S = 300.0

#: States after which SLURM has nothing further to say about a job.
TERMINAL_STATES = frozenset({
    "BOOT_FAIL", "CANCELLED", "COMPLETED", "DEADLINE", "FAILED", "NODE_FAIL",
    "OUT_OF_MEMORY", "PREEMPTED", "REVOKED", "SPECIAL_EXIT", "TIMEOUT",
})

#: The terminal states a *retry* can honestly answer.
#:
#: ``TIMEOUT`` is in the set because the pull queue puts it there: ``finish``
#: reads ``succeeded = status in {"executed", "cache_hit"}``, so an action its
#: ``execute`` killed at ``timeout_s`` is dispositioned ``requeued`` while
#: attempts remain.  The same ``--retries`` therefore buys the same number of
#: runs on either transport.  It earns its keep when the first attempt was
#: starved rather than slow -- and under SLURM the retry can land on a
#: different node, which the queue's retry could not.
#:
#: ``CANCELLED`` stays out: it is an operator's decision, and re-running it
#: would be overruling them.  ``DEADLINE`` stays out too -- that is a partition
#: or QOS deadline in absolute time, so a resubmission meets it immediately.
RETRIABLE_STATES = frozenset({
    "BOOT_FAIL", "FAILED", "NODE_FAIL", "OUT_OF_MEMORY", "PREEMPTED", "TIMEOUT",
})

#: How much of a job log the terminal record carries inline.
#:
#: The pull queue caps nothing: ``execute`` holds the action's output as a
#: string it already had in memory, and ``finish`` files all of it.  This lane
#: holds a *filename* instead, and a build log on this fleet reaches hundreds of
#: megabytes -- so filing the whole thing would turn one job into a JSON record
#: nothing can parse, on NFS, for no reader that wants it (``merge_suite`` reads
#: the return code and the host; ``pool_reset`` reads the addressing).  The full
#: files stay where they are, named by ``detail.slurm.stdout_path`` beside the
#: tail.  This is a deliberate divergence from the queue, not a port of it.
STREAM_TAIL_BYTES = 256 * 1024

#: What ``wait`` reports when the caller's own patience ran out first.  The job
#: is still queued or running; nothing has been cancelled.
WAIT_TIMEOUT_STATE = "WAIT_TIMEOUT"

#: What a detached submission reports in place of a scheduler state.  Nothing
#: polled the job, so no state is known, and inventing ``PENDING`` would be an
#: answer this process never asked the controller for.
DETACHED_STATE = "DETACHED"

#: What it reports when no scheduler command can say anything about the job --
#: purged past ``MinJobAge`` with no accounting behind it.  The CAS still can.
UNKNOWN_STATE = "UNKNOWN"


class SlurmLaneError(pb.PrismaBuildError):
    """A scheduler command failed, or answered something unusable."""


class CommandTimedOut(SlurmLaneError):
    """A scheduler command did not answer within ``COMMAND_TIMEOUT_S``.

    Distinct from every other failure because it is the only one that leaves
    the question open on the *submitting* side: ``sbatch`` may have been
    accepted and the answer lost, so a plain refusal would tell the caller a
    job does not exist while it runs.  A read that hangs is one reader out of
    three: ``query_provenance`` tries the others before this reaches ``wait``,
    which treats it as no answer this poll -- because it is a
    ``SlurmLaneError`` -- and keeps polling.
    """


class SubmissionFateUnknown(SlurmLaneError):
    """``sbatch`` did not answer and the controller could not settle it.

    Not a refusal.  Nothing is filed, because nothing is known: a job may be
    queued under this action's name right now.  The message names the job
    name, the submission's comment, and the ``squeue`` an operator can run.
    """


class ControllerUnreachable(SlurmLaneError):
    """A scheduler command could not reach ``slurmctld`` at all.

    Not an answer about the job.  ``systemctl restart slurmctld`` on the
    controller box, or any recovery window, makes every ``scontrol`` and
    ``squeue`` fail with this for a while; the job on its node neither knows
    nor cares.  ``wait`` keeps polling through it.
    """


#: What ``scontrol``/``squeue`` print when the controller is not there to ask,
#: as opposed to when it answered that there is no such job.  Matched against
#: stderr, case-insensitively.  ``Invalid job id specified`` is deliberately
#: not here: that is an answer.
_UNREACHABLE_MARKERS = (
    "unable to contact slurm controller",
    "connection refused",
    "connect failure",
    "socket timed out",
    "zero bytes were transmitted",
    "protocol authentication error",
)


def _unreachable(completed: subprocess.CompletedProcess[str]) -> bool:
    text = f"{completed.stderr or ''}\n{completed.stdout or ''}".lower()
    return any(marker in text for marker in _UNREACHABLE_MARKERS)


def lane_root(explicit: str | Path | None = None) -> Path:
    """The lane root this process is to use, environment included."""

    if explicit is not None:
        return Path(explicit)
    return Path(os.environ.get(LANE_ROOT_ENV) or DEFAULT_LANE_ROOT)


def nice_for(priority: int) -> int:
    """The ``--nice`` value that carries one pool priority.

    Clamped at zero rather than refused: ``sbatch`` rejects a negative nice
    from an unprivileged submitter, and a priority past ``NICE_BASE //
    NICE_SCALE`` is asking for a boost this user cannot be granted.  Zero is
    the most this lane can do for it, and it is still ordered ahead of every
    ordinary submission.  Clamped at ``NICE_MAX`` for the same reason at the
    other end: ``sbatch`` refuses a nice past its range, and a priority that
    low is asking to sit behind everything, which ``NICE_MAX`` already does.
    """

    return min(NICE_MAX, max(0, NICE_BASE - int(priority) * NICE_SCALE))


def lane_directory(action_key: str, *, root: str | Path | None = None) -> Path:
    """One directory per action key: script, records, logs."""

    key = str(action_key)
    if len(key) != 64 or any(ch not in "0123456789abcdef" for ch in key):
        raise SlurmLaneError(f"action key is not a 64-hex digest: {key!r}")
    return lane_root(root) / key


def job_state_directory(explicit: str | Path | None = None) -> Path:
    """Where a job leaves the facts its Epilog needs after it is gone.

    Read on the node that runs the job, not on the box that submitted it.  An
    explicit path is for a launcher driven by hand and for the tests; the
    environment variable is the node-side override the Epilog honours in the
    same way; the default is what both sides fall back to.
    """

    if explicit is not None:
        return Path(explicit)
    return Path(os.environ.get(JOB_STATE_ROOT_ENV) or DEFAULT_JOB_STATE_ROOT)


def submission_record_path(
    directory: str | Path, *, published_unix: float, attempt: int
) -> Path:
    """Where one submission's sealed record goes: generation, then attempt.

    An action key is a content hash, so asking for the same work again is the
    same key -- and the lane directory is per key.  Naming the record by the
    attempt alone therefore collided across *runs*: the second submission of a
    key wrote ``submissions/001.json`` on top of the first one's, the bytes
    differed (a new job id, a new generation), and the first-writer publish
    below refused.  ``pbrun`` reported that as ``slurm refused this action``,
    which named neither the collision nor the fact that the refusal came from
    this module rather than from ``sbatch``.  Measured in the container smoke:
    every re-run of one action -- including the CAS hit that is the whole point
    of a content-addressed build -- failed at submit, forever.

    So the generation is part of the name.  ``published_unix`` is the run, and
    ``_same_generation`` already treats it as the identity of one; the attempt
    is the retry within it, which is what ``max_attempts`` bounds.  Together
    they name exactly one submission, and the record stays immutable for the
    thing it records.
    """

    return (
        Path(directory) / "submissions"
        / f"{float(published_unix):.6f}-{int(attempt):03d}.json"
    )


def action_status_path(directory: str | Path, job_id: str) -> Path:
    """Where the node leaves this job's action exit status, beside its logs.

    Named for the job rather than fixed, and for the same reason the logs are:
    one lane directory holds every attempt of one action key, a retry is a new
    job id in that directory, and a fixed name would let attempt one's exit
    status be read onto attempt two's record -- including onto a ``done/``
    record, when the retry succeeded.

    ``core`` writes this file only for an action that ran and ended by itself,
    so an absent file is the normal case and means the ending was the worker's
    verdict rather than the action's.
    """

    return Path(directory) / f"{str(job_id)}.action.json"


def read_action_status(path: str | Path) -> dict[str, object]:
    """The action's ending from its sidecar, or an empty mapping.

    Empty covers every way there is nothing to say: no file, unreadable bytes,
    text that is not a JSON object, or a status field that is not an integer.
    The caller files what comes back, so a malformed sidecar leaves the record
    exactly as it was before there were sidecars.
    """

    try:
        raw = Path(path).read_text(encoding="utf-8")
    except OSError:
        return {}
    try:
        value = json.loads(raw)
    except ValueError:
        return {}
    if not isinstance(value, Mapping):
        return {}
    status: dict[str, object] = {}
    for field in ("action_returncode", "action_signal"):
        number = value.get(field)
        if isinstance(number, int) and not isinstance(number, bool):
            status[field] = number
    if "action_returncode" not in status:
        # An ending without a returncode is not an ending, and filing a
        # ``action_signal`` alone would state one.  The rest of the file is
        # still real: a killed run leaves its partial profile here and no
        # returncode at all, because there was no ending to record.
        status.pop("action_signal", None)
    profile = value.get("profile")
    if isinstance(profile, Mapping):
        status["profile"] = dict(profile)
    return status


#: What the node leaves beside its logs when it found the work already done.
CACHE_HIT_SCHEMA_V1 = "prismaquant.prismabuild.slurm_cache_hit.v1"


def cache_hit_path(directory: str | Path, job_id: str) -> Path:
    """Where the node says this job published nothing because the CAS had it.

    Named for the job rather than fixed, for the reason
    ``action_status_path`` is: one lane directory holds every attempt of one
    action key, and a fixed name would let one attempt's answer be read onto
    another attempt's record.
    """

    return Path(directory) / f"{str(job_id)}.cache-hit.json"


def write_cache_hit(
    path: str | Path,
    *,
    action_key: str,
    job_id: str,
    result_digest: object = None,
) -> None:
    """Record that this job ran nothing because the result already existed.

    The submitter cannot tell the two apart from outside.  All it sees is a
    receipt, and a receipt exists whether this job published it or read it, so
    without this file the ending said ``executed`` for a job that executed
    nothing -- with the node's elapsed time attached to it.  The node knows
    which happened, so the node writes it down.
    """

    _write_json_atomic(Path(path), {
        "schema": CACHE_HIT_SCHEMA_V1,
        "action_key": str(action_key),
        "job_id": str(job_id),
        "result_digest": result_digest,
        "found_unix": _now(),
        "found_host": socket.gethostname(),
    })


def read_cache_hit(path: str | Path) -> dict[str, object]:
    """The node's cache-hit marker, or an empty mapping when there is none.

    Empty covers every way there is nothing to say: no file, unreadable bytes,
    text that is not a JSON object, or another schema.  An absent marker means
    the job ran the work, which is what every job did before this file existed.
    """

    try:
        raw = Path(path).read_text(encoding="utf-8")
    except OSError:
        return {}
    try:
        value = json.loads(raw)
    except ValueError:
        return {}
    if not isinstance(value, Mapping):
        return {}
    if value.get("schema") != CACHE_HIT_SCHEMA_V1:
        return {}
    return dict(value)


def job_was_cache_hit(job: "SubmittedJob | None") -> bool:
    """Whether this job reported a hit rather than an execution."""

    if job is None:
        return False
    return bool(read_cache_hit(cache_hit_path(job.directory, job.job_id)))


def submission_comment(action_key: str, *, attempt: int, nonce: str) -> str:
    """What one ``sbatch`` invocation calls itself, in the job's ``Comment``.

    ``sbatch`` can be accepted and then hang past ``COMMAND_TIMEOUT_S``, and
    the job id it was about to print is lost with it.  The job name is not
    enough to find that job again: it is ``pb-<key12>``, which every attempt
    of every submission of one action key shares.  A fresh nonce per
    invocation is, and the controller carries it: ``squeue -o %k`` and
    ``scontrol show job`` both read it back.

    The whole key is in it rather than the twelve-character prefix, so that a
    comment read off the controller identifies the action without a lane
    directory to resolve a prefix against.
    """

    return f"pb:{str(action_key)}:{int(attempt)}:{str(nonce)}"


def adoption_argv(action_key: str, *, squeue: object = "squeue") -> list[object]:
    """The ``squeue`` that answers whether the controller took a submission.

    ``--states=all`` because the answer must cover a job that was accepted and
    finished inside the same ``COMMAND_TIMEOUT_S`` window -- a job held behind
    a sibling and released into a cache hit is exactly that fast, and the
    default ``squeue`` would not list it.  Matching on the comment is what
    makes the wider listing safe.
    """

    return [
        squeue, "-h", "-u", getpass.getuser(),
        f"--name=pb-{str(action_key)[:12]}", "--states=all", "-o", "%i|%k",
    ]


def find_submitted_job(
    action_key: str, *, comment: str, squeue: Command = "squeue"
) -> list[str]:
    """Job ids under this action's name whose comment is this invocation's.

    Args:
        action_key: The action key, whose first twelve characters name the job.
        comment: The exact ``submission_comment`` sent with the submission.
        squeue: The ``squeue`` command to ask.

    Returns:
        Every job id the controller holds with that comment.  Empty means the
        controller answered and holds none, which is an answer: it did not take
        the submission.

    Raises:
        ControllerUnreachable: ``squeue`` could not reach ``slurmctld``.
        CommandTimedOut: ``squeue`` did not answer either.
        SlurmLaneError: ``squeue`` refused for some other reason.
    """

    completed = _run(adoption_argv(action_key, squeue=squeue), where="squeue")
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout).strip()
        if _unreachable(completed):
            raise ControllerUnreachable(
                f"squeue could not reach the controller: {detail}")
        raise SlurmLaneError(f"squeue refused the question: {detail}")
    found = []
    for line in completed.stdout.splitlines():
        job_id, _, text = line.strip().partition("|")
        if job_id.strip() and text.strip() == comment:
            found.append(job_id.strip())
    return found


def format_time_limit(timeout_s: float) -> str:
    """Seconds to what ``--time`` accepts, rounded up, never rounded to zero.

    Rounded *up* because a limit is the point past which SLURM sends TERM and
    then KILL: rounding down would kill an action a fraction inside the budget
    its submitter asked for.  A whole minute is the floor because SLURM's own
    enforcement granularity is the minute, so a smaller number would promise a
    precision the scheduler does not have.
    """

    if not isinstance(timeout_s, (int, float)) or isinstance(timeout_s, bool):
        raise SlurmLaneError("timeout_s must be a number of seconds")
    if not math.isfinite(float(timeout_s)) or float(timeout_s) <= 0:
        raise SlurmLaneError(
            f"timeout_s must be a positive finite number of seconds: "
            f"{timeout_s!r}"
        )
    seconds = max(60, int(math.ceil(float(timeout_s))))
    days, rest = divmod(seconds, 86400)
    hours, rest = divmod(rest, 3600)
    minutes, seconds = divmod(rest, 60)
    clock = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    return f"{days}-{clock}" if days else clock


@dataclass(frozen=True)
class LaneResources:
    """What one action asks of a node, in the scheduler's own vocabulary."""

    cpus: int = 1
    memory_mib: int = 4096
    gpu_slots: int = 0
    exclusive_gpu: bool = False

    def __post_init__(self) -> None:
        if self.cpus < 1:
            raise SlurmLaneError("cpus must be at least 1")
        if self.memory_mib < 1:
            raise SlurmLaneError("memory_mib must be at least 1")
        if self.gpu_slots < 0:
            raise SlurmLaneError("gpu_slots cannot be negative")
        if self.exclusive_gpu and self.gpu_slots < 1:
            raise SlurmLaneError(
                "an exclusive GPU action must declare a GPU demand"
            )

    @classmethod
    def from_demand(
        cls, demand: Mapping[str, object], *, exclusive: bool = False
    ) -> LaneResources:
        """Read ``pbrun``'s demand vocabulary without inventing a second one."""

        gpu = int(demand.get("gpu", 0) or 0)
        return cls(
            cpus=max(1, int(demand.get("cpu", 1) or 1)),
            memory_mib=max(1, int(demand.get("mem_gb", 4) or 4)) * 1024,
            gpu_slots=gpu,
            exclusive_gpu=bool(exclusive and gpu),
        )

    def demand(self) -> dict[str, int]:
        """The pool-shaped resource claim, for the terminal record.

        ``pool_reset`` rebuilds a submission out of the ``resources`` it reads
        off a failed record, so the record has to speak the vocabulary the
        producer used -- ``{"gpu": 1, "cpu": 8, "mem_gb": 32}`` -- and not the
        scheduler flags this dataclass exists to derive.
        """

        claim = {"cpu": int(self.cpus), "mem_gb": int(self.memory_mib) // 1024}
        if self.gpu_slots:
            claim["gpu"] = int(self.gpu_slots)
        return claim

    def gres(self) -> str | None:
        """The GRES request, or ``None`` for work that must not see a device.

        ``gpu:1`` and ``shard:N`` are mutually exclusive requests against the
        same device -- asking for the whole GPU is exactly what ``--exclusive``
        means, and asking for shards is exactly what a slot means.  That is why
        exclusivity is a different GRES name here rather than a bigger count.
        """

        if not self.gpu_slots:
            return None
        if self.exclusive_gpu:
            return "gpu:1"
        return f"shard:{self.gpu_slots}"


#: The fleet's partition names, as ``fleet/slurm/slurm.conf`` declares them.
#: The default partition is deliberately not named here: a tagged action is
#: sent there and its sealed ``--constraint`` picks the node.
GPU_PARTITION = "gpu"
CPU_PARTITION = "cpu"


def partition_for(
    resources: LaneResources, placement: Sequence[str], *, anywhere: bool = False
) -> str | None:
    """Which partition carries an action, read off what it already declares.

    The fleet's rule is that CPU-only work goes to the CPU box.  It is stated
    here without naming a box, so it holds on a fleet that grows:

    * a GPU demand goes to the GPU partition, the only place shards exist;
    * work the submitter asserted portable with ``--anywhere`` goes to the
      default partition, every box, where node weight prefers the CPU box
      and a GPU box takes it only when the CPU box is full.  This is the
      one opt-in to a GPU box's cores, because its memory is one pool
      shared with its GPU;
    * otherwise no GPU demand and no placement tag goes to the CPU
      partition;
    * anything tagged goes to the default partition, where the sealed
      ``--constraint`` picks the node.  The tag is a hostname pin from a
      box-local executable or a class the submitter named, and forcing a
      partition on top of it is how a CPU-only action whose interpreter lives
      on a GPU box becomes unschedulable: no node in the CPU partition carries
      that box's feature.

    The answer is a function of two sealed inputs, the demand and the
    effective placement, so it adds nothing to the action's identity.
    """

    if resources.gpu_slots:
        return GPU_PARTITION
    if anywhere:
        return None
    if not [tag for tag in placement if str(tag)]:
        return CPU_PARTITION
    return None


@dataclass(frozen=True)
class JobProvenance:
    """What the scheduler knows about one job, beyond whether it ended.

    ``claimed_host``/``finished_host`` are what ``merge_suite`` prints as the
    box that ran an arm and ``elapsed_s`` is what it prints as the duration, so
    all three are lifted from the scheduler rather than guessed from the
    submitter's own clock and hostname -- which under SLURM is not even the
    right box.  Every field but ``state`` is optional: a controller with no
    slurmdbd purges a job past ``MinJobAge``, after which nothing can say when
    it started, and a record that says so is worth more than no record.
    """

    state: str
    exit_code: int | None = None
    signal: int | None = None
    start_unix: float | None = None
    end_unix: float | None = None
    elapsed_s: float | None = None
    node: str | None = None
    partition: str | None = None
    #: Why the job is not running yet, in the controller's own word:
    #: ``Dependency``, ``Resources``, ``Priority``.  ``sacct`` does not carry
    #: it, so it is ``None`` on the accounting path and on every job the
    #: scheduler has already finished with.
    reason: str | None = None


@dataclass(frozen=True)
class SubmittedJob:
    """One accepted ``sbatch``, and where to look for everything it leaves."""

    action_key: str
    job_id: str
    attempt: int
    argv: list[str]
    script: Path
    directory: Path
    stdout_path: Path
    stderr_path: Path
    record_path: Path
    #: The run this submission belongs to, as ``published_unix`` in its
    #: record.  Carried on the job because the generation is what every
    #: withdrawal question is scoped to, and a caller holding the job should
    #: not have to re-read the record to ask one.  ``None`` only on a job a
    #: caller built without naming a generation.
    published_unix: float | None = None


@dataclass(frozen=True)
class Outcome:
    """What the scheduler ended up saying about one job."""

    job_id: str
    state: str
    exit_code: int | None
    signal: int | None
    stdout_path: Path | None
    stderr_path: Path | None
    provenance: JobProvenance | None = None
    #: ``LivenessMonitor.summary()`` for the wait that produced this outcome:
    #: the latest sample and ``stalled_since``.  ``None`` only for an outcome
    #: built somewhere other than ``wait`` (a withdrawal filed from another
    #: box, a test).
    liveness: dict[str, object] | None = None

    @property
    def terminal(self) -> bool:
        return self.state in TERMINAL_STATES

    @property
    def succeeded(self) -> bool:
        return self.state == "COMPLETED" and self.exit_code == 0


@dataclass
class RunResult:
    """Every attempt this lane made, and the receipt that decides the verdict."""

    action_key: str
    attempts: list[tuple[SubmittedJob, Outcome]] = field(default_factory=list)
    receipt: dict[str, object] | None = None
    #: The generation every attempt in this run was stamped with.  A detached
    #: caller has to know it: the terminal record it will look for later is
    #: identified by generation and not by key, because the key is a content
    #: hash and the same key is submitted again every time somebody asks for
    #: the same work again.
    published_unix: float = 0.0

    @property
    def last(self) -> tuple[SubmittedJob, Outcome] | None:
        return self.attempts[-1] if self.attempts else None


#: A scheduler command as the lane is given it: the executable's name or path,
#: or a callable that takes the arguments after it and returns what
#: ``subprocess.run`` would.  The callable form exists for tests, which drive
#: ``wait`` through hundreds of polls on a fake clock and cannot afford a
#: process per poll; the lane treats both forms alike, so a hang (a
#: ``TimeoutExpired`` raised by the callable) reads the same either way.
Command = str | Callable[[Sequence[str]], "subprocess.CompletedProcess[str]"]


def _run(argv: Sequence[object], *, where: str) -> subprocess.CompletedProcess[str]:
    head, rest = argv[0], [str(arg) for arg in argv[1:]]
    try:
        if callable(head):
            return head(rest)
        return subprocess.run(
            [str(head), *rest],
            capture_output=True,
            text=True,
            timeout=COMMAND_TIMEOUT_S,
        )
    except subprocess.TimeoutExpired as exc:
        # Held apart from every other failure by its type and not by its words:
        # a command that hung may have done what it was asked before it stopped
        # answering (see ``CommandTimedOut``), and the message an operator
        # already reads for this is the one below.
        raise CommandTimedOut(f"{where} failed: {exc}") from exc
    except (OSError, subprocess.SubprocessError) as exc:
        raise SlurmLaneError(f"{where} failed: {exc}") from exc


@contextlib.contextmanager
def _naming_job(job_id: str) -> Iterator[None]:
    """Stamp an accepted job's id on whatever this block raises.

    Every record this lane writes is written *after* the thing it records: the
    submission record after ``sbatch`` returned an id, the terminal record
    after the receipt landed.  A write that fails there leaves a real job on
    the fleet and nothing on disk that names it, so the id has to travel on the
    failure or no caller can report it.  ``pbrun`` reads it back as
    ``exc.job_id`` and prints it beside the path that would not write.

    An id already on the exception is left alone: the innermost writer is the
    one that knows which job it was writing for.
    """

    try:
        yield
    except BaseException as exc:
        if getattr(exc, "job_id", None) is None:
            exc.job_id = str(job_id)          # type: ignore[attr-defined]
        raise


def _publish_record(path: Path, payload: Mapping[str, object]) -> None:
    """First-writer-publish one submission record; refuse conflicting bytes."""

    raw = pb._canonical_file_bytes(dict(payload))
    path.parent.mkdir(parents=True, exist_ok=True)
    if pb._atomic_publish(path, raw):
        return
    observed = pb._read_regular_file_nofollow(
        path, where="SLURM lane submission record", require_readonly=True
    )
    if observed != raw:
        raise SlurmLaneError(
            f"a different submission is already recorded at {path}"
        )


def _write_latest(path: Path, payload: Mapping[str, object]) -> None:
    """Point at the newest attempt by rename, so a reader sees one or the other.

    The temp name carries a UUID and is created ``O_EXCL``, which is the shape
    ``materialize._write_json_atomic`` uses and for the reason this lane needs
    it: a lane directory is on the shared mount and two boxes submitting one
    action key write into it.  A pid is unique only within a box, so
    ``.latest.json.<pid>.tmp`` was one file for both of them -- the first
    writer renamed it away and the second's ``os.replace`` raised
    ``FileNotFoundError`` after its own ``sbatch`` had been accepted, leaving a
    queued job with nothing in ``latest.json`` for ``--withdraw`` to resolve.

    Flushed and fsynced before the rename, so the bytes a reader on another box
    sees after this returns are the whole record rather than a hole.  The temp
    file is removed if anything goes wrong on the way, because a lane directory
    an operator reads should not accumulate the debris of failed writes.
    """

    raw = pb._canonical_file_bytes(dict(payload))
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.parent / f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    descriptor = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise


def _now() -> float:
    return time.time()


def _publish_bytes_if_absent(
    path: Path, raw: bytes, *, mode: int = 0o644
) -> bool:
    """Link ``raw`` into place at ``path`` only when nothing is there yet.

    The writer under ``_publish_json_if_absent``, which is where the reason
    for the shape lives.  Separated because the batch script is bytes and a
    mode rather than a JSON payload, and it is published under the same rule:
    the name is the digest of these bytes, so a name that already exists holds
    the same bytes and nothing was substituted.

    Returns True when this call linked the file and False when one was already
    there; the temp file is gone either way.
    """

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.parent / f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    descriptor = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        # Explicit, because ``os.open``'s mode is masked by the umask and a
        # script an operator reproduces by hand should be executable.
        os.chmod(tmp, mode)
        try:
            os.link(tmp, path)
        except FileExistsError:
            return False
        return True
    finally:
        tmp.unlink(missing_ok=True)


def _publish_json_if_absent(path: Path, payload: Mapping[str, object]) -> bool:
    """File ``payload`` at ``path`` only when nothing is there yet.

    Link-first: the bytes go to a unique temp file that is then hard-linked
    to ``path``, and ``os.link`` refuses a name that already exists -- the
    same primitive the CAS publishes receipts with.  Where
    ``_write_json_atomic`` replaces whatever it finds, this never does, which
    is the rule a ``cache_hit`` ending lives under: it is filed only when the
    key has no ending at all (``publish_outcome``).

    An existence check followed by a rename has a window, and row 15b of
    fleet/slurm/smoke measured two submitters of one key inside it on
    2026-09-05 (run-20260905T122808): started together, both polled on the
    same tick, the executing job's record was filed, and the hit's rename
    then replaced it.  The run that did the work keeps the record.

    Returns True when this call filed the record and False when a record was
    already there; the temp file is gone either way.
    """

    return _publish_bytes_if_absent(
        path, pb._canonical_file_bytes(dict(payload)))


def _write_json_atomic(path: Path, payload: Mapping[str, object]) -> None:
    """Replace a terminal record whole, the way ``pool._write_json_atomic`` does.

    Same writer as ``_write_latest`` above, which is where the uniqueness of
    the temp name, the fsync and the cleanup live.

    The mutable summary is a pointer, not an audit log: the queue rewrites it on
    every retry and so must this.  First-writer-wins belongs to the *immutable*
    records -- the submission records above -- and ``_land_summary`` is what
    keeps two writers of one key from overwriting each other: it compares
    generations before this call and re-reads after it, so an older waiter
    cannot leave its ending standing over a later run's.
    """

    _write_latest(path, payload)


def job_script_text(
    *,
    request_path: str | Path,
    cas_root: str | Path,
    worker_script: str | Path,
    job_entry: str | Path,
    lane_directory_path: str | Path,
    job_python: str = DEFAULT_JOB_PYTHON,
    worker_python: str = DEFAULT_JOB_PYTHON,
    local_checkout_root: str | Path | None = None,
) -> str:
    """The batch script: materialize inside the job, then run the worker.

    Deterministic in its arguments, which is what lets ``submit`` name the
    file by the digest of these bytes: two submissions that would send the
    same script share one immutable file, and a submission that would send a
    different one gets a different name rather than replacing anything.

    Materialization happens *here*, on the node that won the allocation, rather
    than at submit time on the submitter's box.  A checkout materialized before
    submission would be a path on one machine handed to a scheduler free to run
    the job on another, and the whole point of the sealed snapshot is that the
    tree is reconstructed wherever the work lands.

    Every path is absolute because ``--export=NIL`` leaves the job with only
    SLURM's own variables: there is no ``PATH`` to resolve a bare name against.

    One path is deliberately absent: where the job leaves its Epilog state
    file.  That is a node-side location, and a script that carried the
    submitter's answer to it was how a submitter with
    ``PRISMABUILD_SLURM_LANE_ROOT`` set silently disabled node-side cleanup.
    ``slurm_job`` resolves it there, from the variable and default the Epilog
    reads.
    """

    argv = [
        str(job_python),
        str(job_entry),
        "--action", str(request_path),
        "--cas-root", str(cas_root),
        "--worker", str(worker_script),
        "--worker-python", str(worker_python),
        "--lane-dir", str(lane_directory_path),
    ]
    if local_checkout_root is not None:
        argv.extend(["--checkout-root", str(local_checkout_root)])
    return (
        "#!/bin/bash\n"
        "# Generated by prismabuild.slurm_lane.  Do not edit: the submission\n"
        "# record beside this file names the exact bytes that were submitted.\n"
        "#\n"
        "# The job materializes the sealed snapshot on the node that won the\n"
        "# allocation and then runs the canonical run-local worker argv inside\n"
        "# it -- the same argv the pull queue execs, so a result is the same\n"
        "# result whichever transport delivered the action.\n"
        "set -u\n"
        f"exec {shlex.join(argv)}\n"
    )


def submit(
    action: Mapping[str, object],
    *,
    cas: pb.PrismaBuildCAS,
    request_path: str | Path,
    placement: Sequence[str] = (),
    resources: LaneResources,
    timeout_s: float | None,
    worker_script: str | Path,
    job_entry: str | Path,
    root: str | Path | None = None,
    job_python: str = DEFAULT_JOB_PYTHON,
    worker_python: str = DEFAULT_JOB_PYTHON,
    local_checkout_root: str | Path | None = None,
    partition: str | None = None,
    priority: int = 0,
    attempt: int = 1,
    sbatch: Command = "sbatch",
    squeue: Command = "squeue",
    published_unix: float | None = None,
    published_by: str | None = None,
    retry_safe: bool | None = None,
    max_attempts: int = 1,
) -> SubmittedJob:
    """Write the script, ``sbatch`` it, seal what was submitted, return the id.

    The record is written *after* the id is known and before anything blocks on
    the job, because a job id nobody wrote down is a job nobody can withdraw.

    An ``sbatch`` that is accepted and *then* hangs past ``COMMAND_TIMEOUT_S``
    leaves exactly that: a job on the fleet with no submission record, which
    nothing can wait on, withdraw or report.  So every invocation names itself
    in the job's ``Comment`` (``submission_comment``, a fresh nonce each time)
    and a timeout asks the controller whether it took the job.  If it did, the
    record is written as if ``sbatch`` had printed that id.  If the controller
    answers that it did not, the refusal stands.  If the controller cannot be
    asked, this raises ``SubmissionFateUnknown`` and files nothing: not knowing
    is not the same as knowing it was refused.
    """

    key = str(action["action_key"])
    directory = lane_directory(key, root=root)
    directory.mkdir(parents=True, exist_ok=True)

    text = job_script_text(
        request_path=request_path,
        cas_root=cas.root,
        worker_script=worker_script,
        job_entry=job_entry,
        lane_directory_path=directory,
        job_python=job_python,
        worker_python=worker_python,
        local_checkout_root=local_checkout_root,
    )
    raw = text.encode("utf-8")
    # Named by the digest of its own bytes, because a mutable ``job.sh`` was
    # not the submission's script but the directory's.  An action key is a
    # content hash, so two callers submitting one key is the ordinary case,
    # and the deployment's runtime generation may legitimately roll between
    # them: the second caller replaced ``job.sh`` before the first caller's
    # ``sbatch`` had read it, so job A ran runtime B's request and CAS root
    # while A's sealed record still named A's.  Content addressing removes the
    # substitution rather than serializing around it -- two submitters with
    # the same bytes share the file, and different bytes are different names.
    digest = hashlib.sha256(raw).hexdigest()
    script = directory / SCRIPT_DIRNAME / f"{digest}.sh"
    _publish_bytes_if_absent(script, raw, mode=0o755)
    # A pointer to the newest attempt's script, for a reader that wants one
    # name.  It is not what any job executes and nothing derives a submission
    # from it: ``sbatch`` is handed the immutable path above and the record
    # names it with its digest.  The temp name carries a UUID for the reason
    # ``_write_latest`` gives: a lane directory is on the shared mount and a
    # pid is unique only within a box.
    pointer = directory / "job.sh"
    tmp = directory / f".job.sh.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    try:
        tmp.write_bytes(raw)
        tmp.chmod(0o755)
        os.replace(tmp, pointer)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise

    nice = nice_for(priority)
    stdout_template = directory / "%j.out"
    stderr_template = directory / "%j.err"
    # The program as the record will name it.  ``sbatch`` may be a callable
    # -- the ``Command`` form every other scheduler command already takes, so
    # that a test can drive a submission without a process -- and the repr of
    # a bound method is not a thing a sealed record can say was submitted.
    program = sbatch if isinstance(sbatch, str) else "sbatch"
    # One invocation's own name, sealed into the record with the rest of the
    # argv, so an operator holding the record can find the job by its comment.
    comment = submission_comment(
        key, attempt=attempt, nonce=secrets.token_hex(8))
    argv = [
        program,
        "--parsable",
        # One flag covers operator requeue and automatic restart, so a requeued
        # job is indistinguishable from a rescheduled one afterwards.  A retry
        # here is a new job id with its own record instead.
        "--no-requeue",
        # Only SLURM's own variables reach the job.  The action's environment
        # is the sealed one core.run_local_action builds, and nothing else has
        # any business travelling from the submitter's shell to the node.
        "--export=NIL",
        f"--job-name=pb-{key[:12]}",
        # One job per action key at a time, held by the controller rather than
        # by whoever asked second.  An action key is a content hash, so two
        # callers asking for the same work is the ordinary case, and the window
        # between one caller's CAS lookup and the other's ``sbatch`` is not one
        # any submitter-side check can close: ``pbrun`` looks the key up and
        # attaches to a live submission, but two processes that look at the
        # same instant both find nothing and both submit.  Measured on the
        # fleet as two jobs of one action, materializing one checkout twice,
        # taking the GPU twice and racing to publish one receipt.
        #
        # ``singleton`` is scoped by the job name above, per user, so it holds
        # exactly the second job of this key.  It waits PENDING with reason
        # ``Dependency`` until the first job leaves, then starts, finds the
        # receipt the first published, and ends as a cache hit without
        # materializing anything.  It is a queue order, not a refusal: nothing
        # is cancelled and nothing is lost if the first job fails, because the
        # second then runs the work itself.
        "--dependency=singleton",
        # Without this the job inherits the submitter's cwd, which is a
        # box-local checkout that need not exist on the node that runs it.
        f"--chdir={directory}",
        f"--output={stdout_template}",
        f"--error={stderr_template}",
        f"--mem={resources.memory_mib}M",
        f"--cpus-per-task={resources.cpus}",
        # The pool recorded a priority and sorted its ready queue on it.  SLURM
        # has no submitter-settable priority number, so the same ordering is
        # expressed as a nice the controller subtracts; see ``NICE_BASE``.
        # Sent on every submission, including priority 0, so that the flag is
        # not the thing that differs between an ordinary job and a deprioritized
        # one -- only its value is.
        f"--nice={nice}",
        # What this invocation of sbatch calls itself.  The job name is shared
        # by every attempt of every submission of this key; this is not.
        f"--comment={comment}",
    ]
    if timeout_s is not None:
        # A deadline is sent only when the submitter asked for one.  Wall-clock
        # is not evidence of death: a job that is still progressing at any
        # elapsed time is left running, and the partition's MaxTime is
        # UNLIMITED so that an unset deadline means exactly that.  An explicit
        # --timeout-s still becomes --time and SLURM enforces it.
        argv.append(f"--time={format_time_limit(timeout_s)}")
    gres = resources.gres()
    if gres:
        argv.append(f"--gres={gres}")
    tags = [str(tag) for tag in placement]
    if tags:
        # A conjunction, matching the queue matcher this lane replaces: every
        # tag must hold, so every tag is a node Feature and they are ANDed.
        argv.append(f"--constraint={'&'.join(tags)}")
    if partition:
        argv.append(f"--partition={partition}")
    argv.append(str(script))

    try:
        completed = _run([sbatch, *argv[1:]], where="sbatch")
    except CommandTimedOut as exc:
        job_id = _adopt_hung_submission(key, comment=comment, squeue=squeue,
                                        timeout=exc)
    else:
        if completed.returncode != 0:
            detail = (completed.stderr or completed.stdout).strip()
            raise SlurmLaneError(
                f"sbatch refused this action: {detail or completed.returncode}"
            )
        output = completed.stdout.strip()
        if not output or "\n" in output or "\r" in output:
            raise SlurmLaneError(
                f"sbatch --parsable returned no single job id: "
                f"{completed.stdout!r}"
            )
        # --parsable prints "<id>" or "<id>;<cluster>" on a federated
        # controller.
        job_id = output.split(";", 1)[0].strip()
        if not job_id.isdigit():
            raise SlurmLaneError(
                f"sbatch returned no numeric job id: {output!r}")

    generation = (
        float(published_unix) if published_unix is not None else time.time()
    )
    record = {
        "schema": SUBMISSION_SCHEMA_V1,
        "action_key": key,
        "attempt": int(attempt),
        "job_id": job_id,
        "argv": list(argv),
        "script": str(script),
        # The bytes ``sbatch`` was handed, named by their own digest.  A
        # submission record that says which script ran is only worth as much
        # as the script's immutability, so the record carries the check an
        # operator can repeat: ``sha256sum`` of the path above.
        "script_sha256": digest,
        "directory": str(directory),
        "stdout": str(directory / f"{job_id}.out"),
        "stderr": str(directory / f"{job_id}.err"),
        "request": str(request_path),
        "cas_root": str(cas.root),
        "constraint": tags,
        "gres": gres or "",
        # Empty means the default partition: the constraint decided.
        "partition": partition or "",
        # What the submitter's priority became.  Recorded rather than derived
        # again by a reader: ``NICE_BASE`` may move, and a record that says
        # what was sent stays readable when it does.
        "nice": nice,
        # Empty means no deadline was requested: the job runs while it runs.
        "time_limit": "" if timeout_s is None else format_time_limit(timeout_s),
        "cpus": resources.cpus,
        "memory_mib": resources.memory_mib,
        "submitted_unix": time.time(),
        "submitted_host": socket.gethostname(),
        # The generation, carried so that ``--withdraw`` can build a complete
        # terminal record from ``latest.json`` alone, on any box, without the
        # submitting process still being alive to tell it.
        "published_unix": generation,
        "published_by": str(
            published_by if published_by is not None else socket.gethostname()
        ),
        "retry_safe": retry_safe,
        "max_attempts": int(max_attempts),
        "resources": resources.demand(),
    }
    record_path = submission_record_path(
        directory, published_unix=generation, attempt=attempt
    )
    with _naming_job(job_id):
        _publish_record(record_path, record)
        _write_latest(directory / "latest.json", record)
    return SubmittedJob(
        action_key=key,
        job_id=job_id,
        attempt=int(attempt),
        argv=list(argv),
        script=script,
        directory=directory,
        stdout_path=directory / f"{job_id}.out",
        stderr_path=directory / f"{job_id}.err",
        record_path=record_path,
        published_unix=generation,
    )


def _adopt_hung_submission(
    action_key: str,
    *,
    comment: str,
    squeue: Command,
    timeout: CommandTimedOut,
) -> str:
    """The job id of a submission ``sbatch`` took but never reported.

    Args:
        action_key: The action key that was submitted.
        comment: The comment that invocation of ``sbatch`` sent.
        squeue: The ``squeue`` command to ask.
        timeout: The timeout that started this.

    Returns:
        The job id the controller holds for this invocation.

    Raises:
        SlurmLaneError: The controller answered that it holds no such job, so
            the submission was refused after all.
        SubmissionFateUnknown: The controller could not be asked, or answered
            with more than one job carrying this invocation's comment.  Nothing
            is filed either way.
    """

    name = f"pb-{str(action_key)[:12]}"
    operator = shlex.join(
        [str(part) for part in adoption_argv(action_key, squeue="squeue")])
    try:
        found = find_submitted_job(action_key, comment=comment, squeue=squeue)
    except SlurmLaneError as exc:
        raise SubmissionFateUnknown(
            f"{timeout}, and the controller could not be asked whether it "
            f"took the job ({exc}). Nothing has been filed for this "
            f"submission, and a job named {name} with Comment={comment} may "
            f"be queued right now. Run: {operator}"
        ) from timeout
    if len(found) > 1:
        raise SubmissionFateUnknown(
            f"{timeout}, and {len(found)} jobs ({', '.join(found)}) carry "
            f"Comment={comment}, which one submission cannot have produced. "
            f"Nothing has been filed. Run: {operator}"
        ) from timeout
    if not found:
        # An answer, and the only one that lets the refusal stand: the
        # controller holds no job this invocation created.
        raise SlurmLaneError(
            f"sbatch refused this action: {timeout}, and the controller holds "
            f"no job named {name} with Comment={comment}"
        ) from timeout
    return found[0]


def _exit_fields(raw: str) -> tuple[int | None, int | None]:
    """SLURM's ``code:signal`` pair, or two ``None`` when it said nothing."""

    text = (raw or "").strip()
    if not text:
        return (None, None)
    code, _, sig = text.partition(":")
    try:
        exit_code = int(code)
    except ValueError:
        return (None, None)
    try:
        signal_number = int(sig) if sig else None
    except ValueError:
        signal_number = None
    return (exit_code, signal_number)


#: What SLURM prints where it has no answer.  Read as null, never as a value.
_ABSENT = frozenset({"", "unknown", "none", "none assigned", "n/a", "(null)"})


def _parse_slurm_time(raw: str | None) -> float | None:
    """SLURM stamps local time with no zone, so read it in the local zone."""

    text = (raw or "").strip()
    if text.lower() in _ABSENT:
        return None
    for layout in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"):
        try:
            return time.mktime(time.strptime(text, layout))
        except ValueError:
            continue
    return None


def _parse_slurm_duration(raw: str | None) -> float | None:
    """``[D-]HH:MM:SS`` or ``MM:SS`` -- the inverse of ``format_time_limit``."""

    text = (raw or "").strip()
    if text.lower() in _ABSENT:
        return None
    days = 0.0
    if "-" in text:
        head, _, text = text.partition("-")
        try:
            days = float(int(head))
        except ValueError:
            return None
    parts = text.split(":")
    if len(parts) not in (2, 3):
        return None
    try:
        values = [float(part) for part in parts]
    except ValueError:
        return None
    if len(parts) == 2:
        hours, minutes, seconds = 0.0, values[0], values[1]
    else:
        hours, minutes, seconds = values
    return days * 86400.0 + hours * 3600.0 + minutes * 60.0 + seconds


def _parse_slurm_text(raw: str | None) -> str | None:
    text = (raw or "").strip()
    return None if text.lower() in _ABSENT else text


def _field(fields: Sequence[str], index: int) -> str:
    return fields[index] if index < len(fields) else ""


def _sacct_state(job_id: str, *, sacct: Command) -> JobProvenance | None:
    completed = _run(
        [
            sacct, "-j", job_id, "--parsable2", "--noheader",
            "-o", "JobID,State,ExitCode,Start,End,Elapsed,NodeList,Partition",
        ],
        where="sacct",
    )
    if completed.returncode != 0:
        # Accounting storage is off until slurmdbd is deployed; that is a
        # configuration fact, not a failure of this action.
        return None
    for line in completed.stdout.splitlines():
        fields = line.split("|")
        if len(fields) < 3 or fields[0].strip() != job_id:
            continue
        state = fields[1].strip().split()[0] if fields[1].strip() else ""
        if not state:
            continue
        exit_code, signal_number = _exit_fields(fields[2])
        return JobProvenance(
            state=state,
            exit_code=exit_code,
            signal=signal_number,
            start_unix=_parse_slurm_time(_field(fields, 3)),
            end_unix=_parse_slurm_time(_field(fields, 4)),
            elapsed_s=_parse_slurm_duration(_field(fields, 5)),
            node=_parse_slurm_text(_field(fields, 6)),
            partition=_parse_slurm_text(_field(fields, 7)),
        )
    return None


_SCONTROL_FIELD = re.compile(r"(\w+)=(\S*)")


def _scontrol_state(job_id: str, *, scontrol: Command) -> JobProvenance | None:
    completed = _run([scontrol, "show", "job", job_id], where="scontrol")
    if completed.returncode != 0:
        if _unreachable(completed):
            raise ControllerUnreachable(
                f"scontrol could not reach the controller: "
                f"{(completed.stderr or completed.stdout).strip()}"
            )
        return None
    fields = dict(_SCONTROL_FIELD.findall(completed.stdout))
    state = fields.get("JobState", "").strip()
    if not state:
        return None
    exit_code, signal_number = _exit_fields(fields.get("ExitCode", ""))
    return JobProvenance(
        state=state,
        exit_code=exit_code,
        signal=signal_number,
        start_unix=_parse_slurm_time(fields.get("StartTime")),
        end_unix=_parse_slurm_time(fields.get("EndTime")),
        elapsed_s=_parse_slurm_duration(fields.get("RunTime")),
        node=_parse_slurm_text(fields.get("NodeList")),
        partition=_parse_slurm_text(fields.get("Partition")),
        reason=_parse_slurm_text(fields.get("Reason")),
    )


def _squeue_state(job_id: str, *, squeue: Command) -> JobProvenance | None:
    completed = _run(
        [squeue, "-h", "-j", job_id, "-o", "%T|%r"], where="squeue"
    )
    if completed.returncode != 0:
        if _unreachable(completed):
            raise ControllerUnreachable(
                f"squeue could not reach the controller: "
                f"{(completed.stderr or completed.stdout).strip()}"
            )
        return None
    lines = completed.stdout.strip().splitlines()
    if not lines or not lines[0].strip():
        return None
    state, _, reason = lines[0].strip().partition("|")
    if not state.strip():
        return None
    return JobProvenance(
        state=state.strip().split()[0],
        # ``%r`` is bare in the default layout and parenthesised in some, and
        # ``pbstatus`` strips the same characters off the same field.
        reason=_parse_slurm_text(reason.strip().strip("()")),
    )


def sibling_jobs(
    action_key: str, *, squeue: Command = "squeue"
) -> list[tuple[str, str]]:
    """Every job the controller holds under this action key's job name.

    Args:
        action_key: The action key, whose first twelve characters name the job.
        squeue: The ``squeue`` command to ask.

    Scoped to this user, because the job name is.  ``--dependency=singleton``
    holds one job per name *per user*, so a second person's job of the same
    action is a job this listing must not report: a caller that cancels what
    this returns would cancel somebody else's work.

    Returns:
        ``(job_id, state)`` pairs in the order ``squeue`` listed them, and an
        empty list when the controller answered that it holds none.

    Raises:
        SlurmLaneError: ``squeue`` could not be run at all.  A non-zero exit is
            not raised: this is an enrichment, and a caller that cannot have it
            says less rather than failing.
    """

    completed = _run(
        [squeue, "-h", "-u", getpass.getuser(),
         f"--name=pb-{str(action_key)[:12]}", "-o", "%i|%T"],
        where="squeue",
    )
    if completed.returncode != 0:
        return []
    found: list[tuple[str, str]] = []
    for line in completed.stdout.splitlines():
        job_id, _, state = line.strip().partition("|")
        if job_id.strip():
            found.append((job_id.strip(), state.strip().upper()))
    return found


def _dependency_notice(job: "SubmittedJob", *, squeue: Command) -> str:
    """What to say about a job the singleton dependency is holding.

    Waiting behind the running job of the same action is the design working,
    not a stall and not a refusal, so this names the job ahead when the
    controller will say which one it is and says the same thing without the id
    when it will not.
    """

    ahead = ""
    try:
        for job_id, state in sibling_jobs(job.action_key, squeue=squeue):
            if job_id != job.job_id and state not in ("PENDING",):
                ahead = job_id
                break
    except SlurmLaneError:
        ahead = ""
    behind = f"slurm job {ahead}" if ahead else "another job"
    return (
        f"slurm job {job.job_id}: waiting for {behind} to finish the same "
        f"action {job.action_key[:12]}; one job per action key runs at a "
        f"time, and this one starts when that one leaves"
    )


def query_provenance(
    job_id: str,
    *,
    sacct: Command = "sacct",
    scontrol: Command = "scontrol",
    squeue: Command = "squeue",
) -> JobProvenance | None:
    """Everything the scheduler will say, from whichever tool can say it.

    ``sacct`` first because it is the only one that still answers about a job
    older than ``MinJobAge`` -- and the only one that is inert until slurmdbd
    exists, which is why it is not the only one asked.  The fallbacks answer
    fewer fields, and the missing ones stay null rather than being invented.

    ``None`` means the controller *answered* and knows no such job.  A
    controller that cannot be reached raises ``ControllerUnreachable`` instead
    of being read as "no such job": before that distinction existed, a
    ``systemctl restart slurmctld`` turned every running job's wait into
    ``UNKNOWN`` on the next poll, and ``run`` filed it as failed.

    The three readers are independent programs against independent daemons,
    so one that cannot be *run* does not stop the others.  ``sacct`` talks to
    slurmdbd and ``scontrol`` and ``squeue`` talk to slurmctld: accounting can
    hang while the controller is healthy and holds the job's ending, and a
    ``sacct`` that hung used to escape this loop before ``scontrol`` was ever
    asked.  Every poll then started again at the same hung call, so a job that
    had completed stayed unobserved until the caller's wait expired, or
    forever when there was no wait budget.

    A failure is therefore carried rather than raised, and raised only when no
    reader could establish the state.  The first one is what raises, because it
    is the failure that started the outage.  An answer of ``None`` from a
    reader that *did* answer is not enough on its own when another could not
    be asked: ``squeue`` legitimately knows nothing about a finished job, so
    treating that as "no such job" would turn an accounting outage into an
    ``UNKNOWN`` ending.  The distinction ``wait`` reads is preserved: ``None``
    only when all three answered and none knew the job.
    """

    failure: SlurmLaneError | None = None
    for reader in (
        lambda: _sacct_state(job_id, sacct=sacct),
        lambda: _scontrol_state(job_id, scontrol=scontrol),
        lambda: _squeue_state(job_id, squeue=squeue),
    ):
        try:
            answer = reader()
        except SlurmLaneError as exc:
            if failure is None:
                failure = exc
            continue
        if answer is not None:
            return answer
    if failure is not None:
        raise failure
    return None


def query_state(
    job_id: str,
    *,
    sacct: Command = "sacct",
    scontrol: Command = "scontrol",
    squeue: Command = "squeue",
) -> tuple[str, int | None, int | None] | None:
    """The ending alone: state, exit code, signal, or ``None`` if unknown."""

    answer = query_provenance(
        job_id, sacct=sacct, scontrol=scontrol, squeue=squeue)
    if answer is None:
        return None
    return (answer.state, answer.exit_code, answer.signal)


# --------------------------------------------------------------------------
# Liveness: sampled evidence that a running job is doing something
# --------------------------------------------------------------------------
#
# The policy this implements (Rob, 2026-09-04): a worker that is actively doing
# something and not visibly dead is never killed on elapsed time.  Wall-clock
# is evidence of wall-clock, not of death.  So ``wait`` samples what the
# scheduler's own accounting says the job is doing, derives ``progressing``
# from whether any of it moved since the previous sample, records every sample
# on the lane directory, and *reports* a job that has not moved for a window.
# Nothing here cancels anything.  A stall ends only by an operator's deliberate
# ``pbrun --withdraw`` or by a deadline the submitter asked for (``--time``,
# which ``submit`` sends only when ``timeout_s`` is given).
#
# What is read, and why it is the job's and not the action's:
#
# * ``sstat`` -- CPU time (``AveCPU``, and ``cpu=`` milliseconds out of
#   ``TRESUsageInTot``), RSS and I/O bytes of the job's steps, gathered by
#   ``jobacct_gather/cgroup`` on the node from the job's own cgroup.  The action
#   does nothing to produce it, so an action that never heard of this lane is
#   measured exactly as well as one that did, and the numbers are taken where
#   the work runs rather than on the submitter.  ``sstat`` reads running steps
#   from ``slurmd`` and needs no ``slurmdbd``; the container smoke (row 13)
#   confirms it answers on this fleet's configuration.
# * the growth of the job's ``<jobid>.out`` and ``<jobid>.err`` under the lane
#   directory -- the only evidence left when ``sstat`` is absent or refuses.
#
# What is *not* read: GPU residency.  On GB10 every per-process GPU figure
# reads null (``nvidia-smi`` per-process memory is empty; ``utilization.memory``
# is a hard 0; ``gpu_utilization`` reads 96% for stalled and saturated kernels
# alike), so claiming GPU evidence would be claiming a measurement nobody can
# take.  The one honest GPU load signal the fleet has is board power against
# its envelope (``nvidia-smi --query-gpu=power.draw``), and ``gpu_power_sample``
# below is the named seam for it.  It returns ``None`` today: it would need to
# run on the node rather than on the submitter, and the container the smoke
# runs in has no GPU to verify it against.

LIVENESS_SCHEMA_V1 = "prismaquant.prismabuild.slurm_liveness.v1"

#: One append-only file per lane directory, one JSON line per sample.  Never
#: rewritten: issue #16 measured a 69 s NFS stall on a queue file rewritten
#: every heartbeat, and an appended file is the access pattern NFS serves well.
#: Nothing on a hot path reads it; ``read_liveness`` reads its last line on an
#: operator's request.
LIVENESS_FILENAME = "liveness.jsonl"

#: The floor for "no output bytes" evidence: the longest a client was measured
#: to see stale state from the shared mount (issue #16, 69 s).  A ``stat`` of
#: the job's log can therefore report an unchanged size for that long while
#: the job is writing.
NFS_STALL_FLOOR_S = 69.0

#: The floor for CPU-time evidence: ``JobAcctGatherFrequency``, the interval at
#: which ``jobacct_gather/cgroup`` refreshes what ``sstat`` reports.  The
#: fleet's ``slurm.conf`` does not set it, so it is SLURM's default of 30 s
#: (``scontrol show config`` in the smoke quotes the effective value).  Two
#: ``sstat`` calls inside one interval read the same gather, so a sample
#: cadence faster than this reads nothing new.
ACCT_GATHER_S = 30.0

#: How often ``wait`` takes a sample while the job is RUNNING.  Equal to the
#: accounting interval, for the reason above; the poll (``DEFAULT_POLL_S``) is
#: faster, and most polls take no sample.
LIVENESS_SAMPLE_S = ACCT_GATHER_S

#: How long a job must show no progress before ``wait`` reports it.
#:
#: Derived, not chosen.  The binding floor is the NFS one: a sample taken
#: inside a 69 s stall can read an unchanged log size for a job that is
#: writing, so a window has to be long enough that the samples across it
#: cannot all sit inside one such stall.  ``ceil(69 / 30) = 3`` cadences cover
#: the stall itself; one more cadence is the sample that establishes the
#: baseline the others are compared against.  That is 4 cadences, 120 s.
#: The same span straddles at least three accounting gathers, so an unchanged
#: ``TotalCPU`` across it is at least two full gather intervals of zero CPU and
#: not a phase artifact of sampling at the gather's own period.
STALL_WINDOW_S = (
    (math.ceil(NFS_STALL_FLOOR_S / LIVENESS_SAMPLE_S) + 1) * LIVENESS_SAMPLE_S
)

#: How often the report is repeated while the stall continues.  This one is a
#: chattiness bound with no measurement behind it: five windows, ten minutes,
#: so a stall that lasts an hour prints six lines and not sixty.
STALL_REPORT_EVERY_S = 5 * STALL_WINDOW_S

#: How many samples ``wait`` keeps in memory: enough to hold one window plus
#: the baseline and the sample that ends it.  The file holds all of them.
LIVENESS_HISTORY = int(STALL_WINDOW_S // LIVENESS_SAMPLE_S) + 2

#: What ``sstat`` is asked for.  Every name is one ``sstat --helpformat``
#: prints on 25.11.2 -- checked in the container smoke (row 13), which is how
#: ``TotalCPU`` was found to be an ``sacct`` field that ``sstat`` refuses
#: (``Invalid field requested: "TotalCPU"``, 2026-09-05).  ``AveCPU`` is the
#: CPU time per task as ``[DD-]HH:MM:SS``; the ``cpu=`` entry of
#: ``TRESUsageInTot`` is printed the same way (the smoke's real line was
#: ``cpu=00:00:00`` next to ``AveCPU`` ``00:00:00``), not as a millisecond
#: count as this comment first claimed.  ``MaxRSS`` is recorded in whatever
#: unit ``--noconvert`` prints and not relabelled: the smoke printed
#: ``MaxRSS`` ``20164608`` beside ``mem=20094976`` in the TRES list, which is
#: bytes for a process that size, not KiB.  Only the change between samples
#: is evidence, so the unit is a matter for the record, not the verdict.
SSTAT_FORMAT = (
    "JobID,AveCPU,MinCPU,MaxRSS,MaxDiskRead,MaxDiskWrite,NTasks,TRESUsageInTot"
)

#: The sample fields compared between consecutive samples.  ``progressing`` is
#: true when any of them changed; a field that is ``None`` on either side is
#: not evidence either way.
PROGRESS_FIELDS = (
    "cpu_s", "tres_cpu_s", "rss", "disk_read", "disk_write", "out_bytes",
    "err_bytes",
)


def gpu_power_sample(node: str | None) -> dict[str, object] | None:
    """The seam for the fleet's one honest GPU load signal: board power.

    Not implemented, and the docstring says why rather than the code guessing.
    Per-process GPU telemetry on GB10 reads null, so the only signal worth
    sampling is ``nvidia-smi --query-gpu=power.draw,power.limit`` read against
    the envelope on the node the job runs on.  That is a node-side read the
    submitter cannot take, and the container smoke has no GPU to verify it in.
    Until it exists, every sample records ``"gpu": null`` and no reader may
    treat the absence as idleness.
    """

    del node
    return None


def _parse_slurm_size(raw: str | None, *, unit_bytes: float = 1.0) -> float | None:
    """``1234``, ``1234K``, ``0.05M``, ``2G`` to a number in ``unit_bytes``.

    An unsuffixed value is taken in ``unit_bytes``; a suffixed one is scaled
    from the suffix.  Only the *change* between two samples is evidence, so
    the unit matters for the record and not for the verdict.
    """

    text = (raw or "").strip()
    if text.lower() in _ABSENT:
        return None
    scale = {"K": 1024.0, "M": 1024.0 ** 2, "G": 1024.0 ** 3,
             "T": 1024.0 ** 4, "P": 1024.0 ** 5}
    suffix = text[-1].upper()
    if suffix in scale:
        try:
            return float(text[:-1]) * scale[suffix] / unit_bytes
        except ValueError:
            return None
    try:
        return float(text)
    except ValueError:
        return None


def _parse_tres_cpu(raw: str) -> float | None:
    """The ``cpu=`` entry of ``TRESUsageInTot`` in seconds.

    ``sstat`` 25.11.2 prints it as ``[DD-]HH:MM:SS`` (the smoke's real line
    read ``cpu=00:00:00``); a bare number is taken as seconds so a build that
    prints one is still read.  The first parser matched only leading digits
    and read ``00:00:45`` as ``0``.
    """

    text = raw.strip()
    if ":" in text:
        return _parse_slurm_duration(text)
    try:
        return float(text)
    except ValueError:
        return None


def _parse_sstat(job_id: str, stdout: str) -> dict[str, object]:
    """Aggregate every step ``sstat -a`` printed for one job.

    CPU time and disk bytes are summed across steps and RSS is the maximum,
    because the question is whether the *job* moved and not which step did.
    """

    steps: list[str] = []
    cpu = min_cpu = read = write = 0.0
    tres_cpu: float | None = None
    rss: float | None = None
    ntasks = 0
    seen_cpu = seen_io = False
    for line in stdout.splitlines():
        fields = line.split("|")
        if len(fields) < 7:
            continue
        step = fields[0].strip()
        if step != job_id and not step.startswith(f"{job_id}."):
            continue
        steps.append(step)
        value = _parse_slurm_duration(fields[1])
        if value is not None:
            cpu += value
            seen_cpu = True
        value = _parse_slurm_duration(fields[2])
        if value is not None:
            min_cpu += value
        match = re.search(r"(?:^|,)cpu=([^,]+)", _field(fields, 7))
        if match:
            value = _parse_tres_cpu(match.group(1))
            if value is not None:
                tres_cpu = (tres_cpu or 0.0) + value
        value = _parse_slurm_size(fields[3])
        if value is not None:
            rss = value if rss is None else max(rss, value)
        value = _parse_slurm_size(fields[4])
        if value is not None:
            read += value
            seen_io = True
        value = _parse_slurm_size(fields[5])
        if value is not None:
            write += value
            seen_io = True
        try:
            ntasks += int(fields[6].strip() or 0)
        except ValueError:
            pass
    return {
        "steps": steps,
        "cpu_s": cpu if seen_cpu else None,
        "tres_cpu_s": tres_cpu,
        "min_cpu_s": min_cpu if seen_cpu else None,
        "rss": rss,
        "disk_read": read if seen_io else None,
        "disk_write": write if seen_io else None,
        "ntasks": ntasks if steps else None,
    }


def _file_size(path: Path) -> int:
    try:
        return os.path.getsize(path)
    except OSError:
        return 0


def liveness_path(directory: str | Path) -> Path:
    """Where one lane directory keeps its samples."""

    return Path(directory) / LIVENESS_FILENAME


@dataclass(frozen=True)
class StallReport:
    """What ``wait`` hands its caller when a running job has not moved.

    A report, not a verdict: the job is still running and nothing has been
    done to it.  The caller prints it; an operator decides.
    """

    action_key: str
    job_id: str
    node: str | None
    stalled_since: float
    stalled_for_s: float
    samples_without_progress: int
    evidence: tuple[str, ...]
    path: Path


class LivenessMonitor:
    """The sampler ``wait`` owns for one job: cadence, history, verdict, file.

    ``sample`` never raises.  Liveness is evidence about a job and a failure
    to gather it is recorded on the sample as ``sstat_error``; it must never
    end a ``wait`` that the scheduler's own answers would have continued.
    """

    def __init__(
        self,
        job: SubmittedJob,
        *,
        sstat: Command = "sstat",
        clock: Callable[[], float] = time.monotonic,
        sample_s: float = LIVENESS_SAMPLE_S,
        window_s: float = STALL_WINDOW_S,
        report_every_s: float = STALL_REPORT_EVERY_S,
        history: int = LIVENESS_HISTORY,
    ) -> None:
        self.job = job
        self.sstat = sstat
        self.clock = clock
        self.sample_s = float(sample_s)
        self.window_s = float(window_s)
        self.report_every_s = float(report_every_s)
        self.samples: deque[dict[str, object]] = deque(maxlen=max(2, int(history)))
        self.count = 0
        self.path = liveness_path(job.directory)
        self.record_error: str | None = None
        self._last_sample_mono: float | None = None
        self._stalled_since_unix: float | None = None
        self._stalled_since_mono: float | None = None
        self._without_progress = 0
        self._last_report_mono: float | None = None

    @property
    def latest(self) -> dict[str, object] | None:
        return self.samples[-1] if self.samples else None

    @property
    def stalled_since(self) -> float | None:
        """Unix time of the first sample that showed no progress, or ``None``
        when the latest sample moved (or nothing has been compared yet)."""

        return self._stalled_since_unix

    def due(self) -> bool:
        """Is it time for another sample?  Bounded by the cadence, never by
        the poll."""

        if self._last_sample_mono is None:
            return True
        return self.clock() - self._last_sample_mono >= self.sample_s

    def _read_sstat(self) -> tuple[dict[str, object], str | None]:
        argv = [
            self.sstat, "-j", self.job.job_id, "-a", "-P", "-n", "--noconvert",
            f"--format={SSTAT_FORMAT}",
        ]
        try:
            completed = _run(argv, where="sstat")
        except SlurmLaneError as exc:
            return {}, str(exc)
        if completed.returncode != 0:
            detail = (completed.stderr or completed.stdout).strip()
            return {}, f"sstat exited {completed.returncode}: {detail}"
        parsed = _parse_sstat(self.job.job_id, completed.stdout)
        if not parsed["steps"]:
            return parsed, "sstat printed no step for this job"
        return parsed, None

    def sample(self, provenance: JobProvenance | None = None) -> dict[str, object]:
        """Take one sample, compare it with the previous, append it to the file."""

        now_mono = self.clock()
        self._last_sample_mono = now_mono
        accounting, error = self._read_sstat()
        node = provenance.node if provenance is not None else None
        current: dict[str, object] = {
            "schema": LIVENESS_SCHEMA_V1,
            "unix": _now(),
            "action_key": self.job.action_key,
            "job_id": self.job.job_id,
            "attempt": self.job.attempt,
            "node": node,
            "steps": list(accounting.get("steps") or []),
            "cpu_s": accounting.get("cpu_s"),
            "tres_cpu_s": accounting.get("tres_cpu_s"),
            "min_cpu_s": accounting.get("min_cpu_s"),
            "rss": accounting.get("rss"),
            "disk_read": accounting.get("disk_read"),
            "disk_write": accounting.get("disk_write"),
            "ntasks": accounting.get("ntasks"),
            "out_bytes": _file_size(self.job.stdout_path),
            "err_bytes": _file_size(self.job.stderr_path),
            "gpu": gpu_power_sample(node),
            "sstat_error": error,
            "evidence": (
                ["output"] if error is not None else ["sstat", "output"]
            ),
        }
        previous = self.latest
        if previous is None:
            progressing: bool | None = None
        else:
            progressing = any(
                previous.get(name) is not None
                and current.get(name) is not None
                and previous.get(name) != current.get(name)
                for name in PROGRESS_FIELDS
            )
        if progressing is False:
            if self._stalled_since_unix is None:
                self._stalled_since_unix = float(current["unix"])
                self._stalled_since_mono = now_mono
            self._without_progress += 1
        elif progressing is True:
            self._stalled_since_unix = None
            self._stalled_since_mono = None
            self._without_progress = 0
            self._last_report_mono = None
        current["progressing"] = progressing
        current["stalled_since"] = self._stalled_since_unix
        self.samples.append(current)
        self.count += 1
        self._append(current)
        return current

    def _append(self, sample: Mapping[str, object]) -> None:
        line = json.dumps(dict(sample), sort_keys=True, separators=(",", ":"))
        try:
            with open(self.path, "a", encoding="utf-8") as handle:
                handle.write(line + "\n")
        except OSError as exc:
            # The evidence is still in memory and reaches the outcome record.
            self.record_error = f"{type(exc).__name__}: {exc}"

    def stall_report(self) -> StallReport | None:
        """A report when the stall has lasted the window and none was issued
        in the last ``report_every_s``; otherwise ``None``."""

        if self._stalled_since_mono is None or self._stalled_since_unix is None:
            return None
        now_mono = self.clock()
        stalled_for = now_mono - self._stalled_since_mono
        if stalled_for < self.window_s:
            return None
        if (
            self._last_report_mono is not None
            and now_mono - self._last_report_mono < self.report_every_s
        ):
            return None
        self._last_report_mono = now_mono
        latest = self.latest or {}
        return StallReport(
            action_key=self.job.action_key,
            job_id=self.job.job_id,
            node=latest.get("node") if isinstance(latest.get("node"), str) else None,
            stalled_since=self._stalled_since_unix,
            stalled_for_s=stalled_for,
            samples_without_progress=self._without_progress,
            evidence=tuple(str(e) for e in (latest.get("evidence") or ())),
            path=self.path,
        )

    def summary(self) -> dict[str, object]:
        """What the outcome record carries under ``detail.liveness``."""

        return {
            "schema": LIVENESS_SCHEMA_V1,
            "samples": self.count,
            "sample_s": self.sample_s,
            "window_s": self.window_s,
            "latest": self.latest,
            "stalled_since": self._stalled_since_unix,
            "samples_without_progress": self._without_progress,
            "path": str(self.path),
            "record_error": self.record_error,
        }


def read_liveness(
    action_key: str, *, root: str | Path | None = None
) -> dict[str, object] | None:
    """The newest liveness sample recorded for an action key, or ``None``.

    For a reader that wants "stalled since" without re-implementing the file:
    every line carries its own ``stalled_since``, so the last line is the
    whole answer.  Reads a bounded tail, never the file.  Returns
    ``{"latest": <sample>, "stalled_since": ..., "job_id": ..., "path": ...}``.
    """

    try:
        directory = lane_directory(action_key, root=root)
    except SlurmLaneError:
        return None
    path = liveness_path(directory)
    try:
        size = os.path.getsize(path)
        with open(path, "rb") as handle:
            bound = 64 * 1024
            if size > bound:
                handle.seek(size - bound)
            raw = handle.read()
    except OSError:
        return None
    lines = [line for line in raw.decode("utf-8", errors="replace").splitlines()
             if line.strip()]
    if not lines:
        return None
    try:
        sample = json.loads(lines[-1])
    except json.JSONDecodeError:
        return None
    if not isinstance(sample, dict):
        return None
    return {
        "latest": sample,
        "stalled_since": sample.get("stalled_since"),
        "progressing": sample.get("progressing"),
        "job_id": sample.get("job_id"),
        "unix": sample.get("unix"),
        "path": str(path),
    }


def wait(
    job: SubmittedJob,
    *,
    sacct: Command = "sacct",
    scontrol: Command = "scontrol",
    squeue: Command = "squeue",
    sstat: Command = "sstat",
    poll_s: float = DEFAULT_POLL_S,
    wait_s: float | None = None,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
    on_stall: Callable[[StallReport], None] | None = None,
    on_notice: Callable[[str], None] | None = None,
) -> Outcome:
    """Poll until the job reaches a terminal state, the caller's patience ends,
    or the scheduler answers that it knows no such job.

    None of the three is a verdict on the work.  The caller reads the CAS for
    that; this says which job to read the logs of, and why it stopped.

    While the job is RUNNING, a ``LivenessMonitor`` samples it at its own
    cadence and ``on_stall`` is called when the samples have not moved for
    ``STALL_WINDOW_S``.  That call is a report and only a report: nothing in
    this loop cancels a job, on any evidence.

    A poll that cannot be answered -- the controller unreachable, a scheduler
    command hung past ``COMMAND_TIMEOUT_S`` -- is not an answer about the job
    either.  The loop says so through ``on_notice`` (once, then at most every
    ``NOTICE_EVERY_S``, and once more when polling recovers) and keeps
    polling.  The caller's own ``wait_s`` still bounds how long it waits.
    """

    deadline = None if wait_s is None else clock() + float(wait_s)
    last: JobProvenance | None = None
    monitor = LivenessMonitor(job, sstat=sstat, clock=clock)
    trouble: str | None = None
    trouble_since: float | None = None
    last_notice: float | None = None
    # Held apart from the outage timer above: an outage is the scheduler not
    # answering, and this is the scheduler answering something the caller
    # should not read as trouble.
    dependency_notice: float | None = None
    while True:
        try:
            answer = query_provenance(
                job.job_id, sacct=sacct, scontrol=scontrol, squeue=squeue
            )
        except SlurmLaneError as exc:
            # ``ControllerUnreachable`` or a command that hung or failed to
            # start.  Neither says anything about the job, so neither ends
            # the wait.  Before this, an unreachable controller was read as
            # "no such job" and a hung ``scontrol`` propagated out of here
            # into pbrun's "sbatch refused this action" handler -- with the
            # job running on in both cases.
            now = clock()
            if trouble_since is None:
                trouble_since = now
            trouble = str(exc)
            if last_notice is None or now - last_notice >= NOTICE_EVERY_S:
                last_notice = now
                if on_notice is not None:
                    on_notice(
                        f"slurm job {job.job_id}: the scheduler could not be "
                        f"asked ({trouble}); still waiting, the job is not "
                        f"affected"
                    )
            if deadline is not None and now > deadline:
                return Outcome(
                    job_id=job.job_id,
                    state=WAIT_TIMEOUT_STATE,
                    exit_code=None,
                    signal=None,
                    stdout_path=job.stdout_path,
                    stderr_path=job.stderr_path,
                    provenance=last,
                    liveness=monitor.summary(),
                )
            sleep(poll_s)
            continue
        if trouble is not None:
            if on_notice is not None:
                on_notice(
                    f"slurm job {job.job_id}: the scheduler answers again "
                    f"after {clock() - (trouble_since or clock()):.0f} s"
                )
            trouble = trouble_since = last_notice = None
        if answer is None:
            return Outcome(
                job_id=job.job_id,
                state=UNKNOWN_STATE,
                exit_code=None,
                signal=None,
                stdout_path=job.stdout_path,
                stderr_path=job.stderr_path,
                # Whatever the last answering poll said is still the best
                # account of where this job ran, and it is the only one left
                # once the controller has forgotten the job.
                provenance=last,
                liveness=monitor.summary(),
            )
        last = answer
        if answer.state in TERMINAL_STATES:
            return Outcome(
                job_id=job.job_id,
                state=answer.state,
                exit_code=answer.exit_code,
                signal=answer.signal,
                stdout_path=job.stdout_path,
                stderr_path=job.stderr_path,
                provenance=answer,
                liveness=monitor.summary(),
            )
        if answer.state == "PENDING" and (answer.reason or "") == "Dependency":
            # Every submission carries ``--dependency=singleton``, so this is
            # the second caller of one action key queued behind the first.
            # Said once and then at most every ``NOTICE_EVERY_S``, because a
            # caller that sees nothing for an hour has no way to tell a job
            # that is waiting on purpose from one that is stuck.
            now = clock()
            if (dependency_notice is None
                    or now - dependency_notice >= NOTICE_EVERY_S):
                dependency_notice = now
                if on_notice is not None:
                    on_notice(_dependency_notice(job, squeue=squeue))
        if answer.state == "RUNNING" and monitor.due():
            monitor.sample(answer)
            report = monitor.stall_report()
            if report is not None and on_stall is not None:
                on_stall(report)
        if deadline is not None and clock() > deadline:
            return Outcome(
                job_id=job.job_id,
                state=WAIT_TIMEOUT_STATE,
                exit_code=None,
                signal=None,
                stdout_path=job.stdout_path,
                stderr_path=job.stderr_path,
                provenance=answer,
                liveness=monitor.summary(),
            )
        sleep(poll_s)


def cancel(job_id: str, *, scancel: Command = "scancel") -> bool:
    """Stop one job.  SLURM sends TERM, then KILL after ``KillWait``."""

    completed = _run([scancel, str(job_id)], where="scancel")
    return completed.returncode == 0


def recorded_keys(root: str | Path | None = None) -> list[str]:
    """Every action key this lane root holds a directory for, sorted.

    One listing, and the only one: a caller with a question per key would
    otherwise list the root once per key, and on the shared mount that listing
    is the expensive part.  ``pbstatus``'s jobs table and the sweeper both ask
    this, and ``resolve_recorded`` stays separate because it answers about a
    prefix an operator typed rather than about the root as a whole.

    A name that is not a full key is skipped rather than reported: the job
    state directory lives here, and so would anything an operator left behind.
    """

    try:
        names = sorted(os.listdir(lane_root(root)))
    except OSError:
        return []
    return [
        name for name in names
        if name != JOB_STATE_DIRNAME and len(name) == 64
    ]


def recorded_action(
    cas: pb.PrismaBuildCAS, action_key: str
) -> dict[str, object] | None:
    """The sealed action published for this key, or ``None``.

    The CAS request is the only copy of the action a box that did not submit
    it can read, and reading it is what lets ``cas.lookup`` be asked at all.
    Absent means the CAS was cleared or the key was never submitted from this
    fleet: a missing lookup, not a failure.
    """

    key = str(action_key)
    path = Path(cas.root) / "requests" / key[:2] / f"{key}.json"
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        value = pb.validate_action(value)
    except (OSError, ValueError):
        return None
    return value if value["action_key"] == key else None


def recorded_submission(
    action_key: str, *, root: str | Path | None = None
) -> dict[str, object] | None:
    """The newest submission recorded for this action key, if any."""

    try:
        directory = lane_directory(action_key, root=root)
    except SlurmLaneError:
        return None
    try:
        raw = (directory / "latest.json").read_text(encoding="utf-8")
    except OSError:
        return None
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return None
    return value if isinstance(value, dict) else None


def submitted_job(submission: Mapping[str, object]) -> SubmittedJob:
    """Rebuild the accepted submission from the record it wrote.

    ``latest.json`` is enough to describe a job to anyone on any box, which is
    what lets an ending be filed by a process that did not submit it.
    ``resume`` and ``sweep`` both start here, so they describe the same job in
    the same terms and a swept ending names what a resumed one names.
    """

    key = str(submission["action_key"])
    generation = submission.get("published_unix")
    if not isinstance(generation, (int, float)) or isinstance(generation, bool):
        # A submission record from before the generation stamp.  Wait for it,
        # but do not claim to know which request it belonged to.
        generation = float(submission.get("submitted_unix") or 0.0)
    published_unix = float(generation)
    attempt = int(submission.get("attempt") or 1)
    directory = Path(str(submission.get("directory") or "."))
    return SubmittedJob(
        action_key=key,
        job_id=str(submission["job_id"]),
        attempt=attempt,
        argv=[str(value) for value in (submission.get("argv") or [])],
        script=Path(str(submission.get("script") or "")),
        directory=directory,
        stdout_path=Path(str(submission.get("stdout") or "")),
        stderr_path=Path(str(submission.get("stderr") or "")),
        record_path=submission_record_path(
            directory, published_unix=published_unix, attempt=attempt
        ),
        published_unix=published_unix,
    )


def resolve_recorded(
    prefix: str, *, root: str | Path | None = None
) -> list[dict[str, object]]:
    """Every recorded submission whose action key starts with ``prefix``.

    A prefix rather than a key because a prefix is what an operator has: every
    log line prints twelve characters.  Ambiguity is the caller's to refuse --
    resolving it by guessing would cancel somebody else's work.
    """

    text = str(prefix or "").strip().lower()
    if not text:
        return []
    base = lane_root(root)
    found: list[dict[str, object]] = []
    try:
        names = sorted(os.listdir(base))
    except OSError:
        return []
    for name in names:
        if name == JOB_STATE_DIRNAME or not name.startswith(text):
            continue
        record = recorded_submission(name, root=root)
        if record is not None:
            found.append(record)
    return found


# --------------------------------------------------------------------------
# The terminal records, where the pull queue's readers already look
# --------------------------------------------------------------------------

def read_stream_tail(path: str | Path, *, limit: int | None = None) -> str:
    """The last ``limit`` bytes of a job log, headed by what was dropped.

    An absent log is normal and reads as empty text: a job cancelled before it
    started never opened one, and turning that into an error would replace the
    reason an action failed with a complaint about the file that would have
    explained it.
    """

    bound = STREAM_TAIL_BYTES if limit is None else int(limit)
    try:
        size = os.path.getsize(path)
        with open(path, "rb") as handle:
            head = ""
            if size > bound:
                handle.seek(size - bound)
                head = (
                    f"[truncated: {size - bound} of {size} bytes omitted; "
                    f"the whole log is in the file named beside this record]\n"
                )
            raw = handle.read()
    except OSError:
        return ""
    return head + raw.decode("utf-8", errors="replace")


def _queue_dir(queue_root: str | Path, state: str) -> Path:
    directory = Path(queue_root) / state
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def _record_generation(record: Mapping[str, object] | None) -> float | None:
    """The run one record belongs to, or ``None`` when it names none.

    ``published_unix`` is the generation in both transports, and a record that
    carries no readable one cannot be placed in time.  Every caller here reads
    that as "cannot be proved older", which is the direction that keeps a
    decision: ``pool.withdrawal_covers`` reads a record with no generation as
    covered for the same reason.
    """

    if not isinstance(record, Mapping):
        return None
    theirs = record.get("published_unix")
    if isinstance(theirs, bool) or not isinstance(theirs, (int, float)):
        return None
    return float(theirs)


def _read_json_object(path: Path) -> dict[str, object] | None:
    """The JSON object at ``path``, revalidated first, or ``None``.

    Listing the parent before the read is the rule ``read_withdrawal_marker``
    and ``pbrun.terminal_record`` already follow on these directories: they are
    on NFS with default attribute caching, where a lookup of a name that did
    not exist yet is negatively cached and keeps answering ENOENT after the
    file has landed.
    """

    try:
        if path.name not in os.listdir(path.parent):
            return None
    except OSError:
        return None
    try:
        record = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return record if isinstance(record, dict) else None


def _same_generation(path: Path, published_unix: float) -> bool:
    """Is the record already there this submission's own, or an older one?

    An action key is a content hash, so the same key is re-submitted every time
    somebody asks for the same work again.  ``published_unix`` equality is the
    queue's own generation rule -- ``terminal_outcome_covers`` states it, and
    states that an old outcome must not blacklist a later submission -- so a
    writer defers to a record of its own generation and replaces an older one.
    A *newer* record is not replaced either; ``_land_summary`` holds that half
    of the rule, because equality alone let a delayed waiter file its ending
    over a later run's.
    """

    try:
        existing = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return False
    theirs = _record_generation(existing if isinstance(existing, dict) else None)
    return theirs is not None and theirs == float(published_unix)


def detail_status_and_returncode(
    status: str, outcome: Outcome | None
) -> tuple[str, int | None]:
    """``detail.status`` and ``detail.returncode`` in the pull queue's terms.

    The readers of these records were written against ``PoolQueue.finish``,
    and two of its conventions carry meaning a scheduler's raw exit fields do
    not.  SLURM reports a job it killed at its time limit as ``ExitCode=0:15``:
    exit code zero, signal fifteen.  Filed as ``returncode=0`` under
    ``failed/``, that zero reads as a pass to any reader that takes zero as
    success, and Tessera's ``merge_suite`` does.  The pool filed a timeout as
    ``status="timeout"`` with ``returncode=None`` (status is the authority;
    ``pbrun`` separately maps the record to its public CLI exit codes), so that
    is what a ``TIMEOUT`` job files here.  A job that died by any other signal
    carries the negative signal number, which is how ``subprocess`` reports a
    signalled child and therefore what the pool's records carried.  The raw
    ``code:signal`` pair stays in ``detail.signal`` and ``detail.slurm.state``.
    """

    if outcome is None:
        return status, None
    if status == "failed" and outcome.state == "TIMEOUT":
        return "timeout", None
    if outcome.signal:
        return status, -int(outcome.signal)
    return status, outcome.exit_code


def _submitted_gres(job: SubmittedJob | None) -> str | None:
    """The ``--gres`` this job was submitted with, or ``None`` for no device.

    Args:
        job: The accepted submission, or ``None`` when there was none.

    Returns:
        ``"gpu:1"`` for a whole device, ``"shard:N"`` for slots, ``None``
        when the job asked for no device or no submission is known.
    """

    if job is None:
        return None
    for flag in job.argv:
        if str(flag).startswith("--gres="):
            return str(flag).split("=", 1)[1]
    return None


def _newer_ending_stands(
    existing: Mapping[str, object], published_unix: float
) -> bool:
    """Is the record at the summary a later run's ending, and not this one's
    to replace?

    A ``cache_hit`` is the one exception, and it is not a generation rule.  A
    hit is the ending of a run that executed nothing: it says the receipt was
    already in the CAS.  The record of the run that *did* the work carries the
    job id, the elapsed time and the logs, and it is the one every reader
    wants, so an execution replaces a standing hit whichever generation each
    belongs to.  ``_file_ending`` and ``pbrun.cached_outcome`` state the same
    rule from the other side: a hit is filed only when the key has no ending
    at all, link-first, so it can never replace one.

    A record with no readable generation cannot be shown to be later, and is
    replaced as it always was.
    """

    theirs = _record_generation(existing)
    if theirs is None or theirs <= float(published_unix):
        return False
    return str(existing.get("status") or "") != "cache_hit"


@contextlib.contextmanager
def _summary_lock(path: Path) -> Iterator[None]:
    """Serialize summary writers on local filesystems and NFSv4.

    The lock inode is permanent. Unlinking it would let a waiting writer lock
    the old inode while a new writer locks its replacement. POSIX byte locks
    interoperate between the NFSv4 mount and a server-local filesystem path;
    flock does not necessarily do so. NFS mounts with local-only locking are
    not supported. Kernel locks are released if the writer exits or crashes.
    """

    directory = path.parent.parent / ".summary-locks"
    lock_path = directory / f"{path.stem}.lock"
    with posix_lock.held(lock_path):
        yield


def _land_summary(
    path: Path,
    payload: Mapping[str, object],
    *,
    published_unix: float,
    refuse_same_generation: bool,
) -> bool:
    """Compare and publish while holding the key's shared POSIX lock."""

    with _summary_lock(path):
        # A delayed waiter may choose a different terminal state than the
        # newer run. Comparing only the target filename would let its old
        # failure land alongside that newer run's success (or withdrawal).
        for state in (pool.DONE, pool.FAILED, pool.WITHDRAWN):
            sibling = path.parent.parent / state / path.name
            try:
                present = sibling.name in os.listdir(sibling.parent)
            except FileNotFoundError:
                present = False
            if not present or sibling == path:
                continue
            existing = _read_json_object(sibling)
            if existing is not None and _newer_ending_stands(
                existing, published_unix
            ):
                return False
        return _land_summary_locked(
            path, payload, published_unix=published_unix,
            refuse_same_generation=refuse_same_generation,
        )


def _land_summary_locked(
    path: Path,
    payload: Mapping[str, object],
    *,
    published_unix: float,
    refuse_same_generation: bool,
) -> bool:
    """Leave ``payload`` standing at ``path`` unless a newer ending is there.

    The mutable terminal summary is one name per key, and every run of one
    content-addressed key files its ending at that name.  So the rule is an
    ordering and not an equality: a later generation replaces an earlier one,
    and an earlier one may not replace a later one.  Only equality was
    checked, so a waiter that resumed an old job after a newer run of the same
    key had already finished replaced the newer ending with its own --
    ``pbrun.terminal_record(path, <newer generation>)`` then answered ``None``
    and a generation-specific waiter timed out with an ending on disk saying
    the older run.

    The per-key lock covers the comparison and the write. A reread alone
    cannot enforce ordering: an older writer can replace the newer record
    after the newer writer has finished its reread and returned. The reread
    below remains a repair for an uncoordinated writer crossing this call,
    but all current summary writers must take the lock for ordering to hold.
    The absent case is link-first (``_publish_json_if_absent``), so two
    writers arriving at an empty name cannot both think they were first.

    Repair stops on anything but a readable older generation.  A record with
    no generation is not evidence this one is stale, and a *vanished* record
    must not be recreated: ``supersede_withdrawal`` unlinks a withdrawal
    marker, and a repair loop that re-published one would revive a decision a
    later submission had already retired.

    A crash releases the lock; the atomic rename leaves either the previous
    summary or the new summary standing.

    Args:
        path: The terminal summary, ``<state>/<key>.json``.
        payload: The record to land.
        published_unix: The generation ``payload`` belongs to.
        refuse_same_generation: Whether a record of this same generation is
            left standing.  False only for the withdrawal enrichment, whose
            whole job is to replace the marker of its own generation with the
            marker plus the job's ending.

    Returns:
        True when a record of this generation or newer stands at ``path`` and
        this call put it there, False when a record this call may not replace
        was found instead.
    """

    floor = float(published_unix)
    while True:
        existing = _read_json_object(path)
        if existing is None:
            if not _publish_json_if_absent(path, payload):
                # A record landed between the read and the link.  Compare
                # against it rather than replacing it unseen. If the name
                # contains unreadable JSON, repeating this loop cannot make
                # progress. Preserve its bytes before filing the known ending.
                if (path.name in os.listdir(path.parent)
                        and _read_json_object(path) is None):
                    archive = path.parent / "unreadable"
                    archive.mkdir(parents=True, exist_ok=True)
                    os.rename(path, archive / (
                        f"{path.stem}.{time.time_ns()}.{secrets.token_hex(4)}.json"
                    ))
                continue
        else:
            if _newer_ending_stands(existing, floor) or (
                _record_generation(existing) == floor
                and (refuse_same_generation or "detail" in existing)
            ):
                return False
            _write_json_atomic(path, payload)
        after = _read_json_object(path)
        standing = None if after is None else _record_generation(after)
        if standing is None or standing >= floor:
            # Ours, or a later run's, or one that names no generation -- and a
            # vanished one, which is deliberately not re-published.
            return True


def publish_outcome(
    *,
    queue_root: str | Path,
    action_key: str,
    published_unix: float,
    published_by: str,
    status: str,
    attempts: int,
    max_attempts: int,
    retry_safe: bool | None,
    addressing: Mapping[str, object] | None = None,
    resources: Mapping[str, int] | None = None,
    tags: Sequence[str] = (),
    detail: Mapping[str, object] | None = None,
    job: SubmittedJob | None = None,
    outcome: Outcome | None = None,
    receipt: Mapping[str, object] | None = None,
    error: str | None = None,
    claimed_by: str | None = None,
    withdrawn_by: str | None = None,
    withdrawn_unix: float | None = None,
    reason: str | None = None,
) -> Path | None:
    """File one action's ending under ``done/``, ``failed/`` or ``withdrawn/``.

    Which directory is decided by the CAS, not by the exit status: a receipt
    means the work was done whatever the job said afterwards, and no receipt
    means it was not, even from a job that exited zero.  That is the rule
    ``PoolQueue.finish`` applies through ``succeeded``; only the machinery
    underneath it differs.

    Returns the path written, or ``None`` when a record this call may not
    replace was already there: one of this same generation, because the first
    account of a generation is the one that stands, or one of a *newer*
    generation, because a later run's ending is not an older waiter's to
    overwrite.  ``_land_summary`` states both halves and applies them across
    the write rather than only before it, and ``_newer_ending_stands`` carries
    the one exception, a standing ``cache_hit``.
    """

    key = str(action_key)
    provenance = outcome.provenance if outcome is not None else None
    if status in ("executed", "cache_hit"):
        # ``cache_hit`` is the pull queue's word for work the CAS already
        # held; ``pbrun`` files it here only when the key has no ``done/``
        # record at all (see ``pbrun.cached_outcome``).
        state = pool.DONE
    elif status == "withdrawn":
        # The pool's rule, from ``PoolQueue.withdraw``: a withdrawal lands in
        # ``withdrawn/``, never ``failed/``.  A withdrawn action is a decision,
        # and a record of it under ``failed/`` makes the failure record lie
        # about the fleet -- every reader that counts failures counts it.
        # The marker ``publish_withdrawal`` filed there is the decision; this
        # record enriches it with the job's ending and keeps its fields.
        state = pool.WITHDRAWN
    else:
        state = pool.FAILED
    path = _queue_dir(queue_root, state) / f"{key}.json"
    decision: dict[str, object] = {}
    existing = _read_json_object(path)
    if existing is not None:
        if _newer_ending_stands(existing, published_unix):
            # A later run of this key has already ended.  Its account stands:
            # this one is a delayed waiter for a request nobody is waiting on
            # any more.  ``_land_summary`` applies the same rule again across
            # the write, for a record that lands while this one is built.
            return None
        theirs = _record_generation(existing)
        if state == pool.WITHDRAWN:
            if "detail" in existing and theirs == float(published_unix):
                # A full ending is already filed for this generation.
                return None
            decision = {
                field: existing[field]
                for field in ("withdrawn_unix", "withdrawn_by",
                              "withdrawn_host", "withdrawn_from", "reason")
                if field in existing
            }
        elif theirs == float(published_unix):
            return None

    detail_status, returncode = detail_status_and_returncode(status, outcome)
    body: dict[str, object] = {
        "status": detail_status,
        "returncode": returncode,
        "signal": outcome.signal if outcome is not None else None,
        "elapsed_s": provenance.elapsed_s if provenance is not None else None,
        "stdout": read_stream_tail(job.stdout_path) if job is not None else "",
        "stderr": read_stream_tail(job.stderr_path) if job is not None else "",
        "error": error,
        "receipt_published": receipt is not None,
        "result_digest": (
            receipt.get("result_digest") if isinstance(receipt, Mapping) else None
        ),
    }
    if job is not None:
        # The action's own ending, when the node left one. ``returncode`` above
        # is the launcher's and stays that -- eleven fleet tools and Tessera's
        # ``merge_suite`` read it as such -- so the action's goes in a field of
        # its own, and is absent when there is nothing to say.
        body.update(read_action_status(
            action_status_path(job.directory, job.job_id)
        ))
    if job is not None or outcome is not None:
        body["slurm"] = {
            "job_id": job.job_id if job is not None
            else (outcome.job_id if outcome is not None else None),
            "state": outcome.state if outcome is not None else None,
            "partition": provenance.partition if provenance is not None else None,
            # The device request as sent, because ``resources`` cannot carry
            # it.  ``LaneResources.demand()`` speaks the producer's vocabulary
            # -- ``{"gpu": 1}`` -- and that same claim is ``gpu:1`` for a whole
            # device and ``shard:1`` for one sharable slot.  ``pool_reset``
            # rebuilds a submission out of ``resources``, so without this it
            # re-emitted an exclusive action's demand as a shard and quietly
            # dropped ``--exclusive``.  Read off the submitted argv rather than
            # recomputed: what the scheduler was told is the fact worth filing.
            "gres": _submitted_gres(job),
            "submission_record_path": str(job.record_path) if job is not None else None,
            "stdout_path": str(job.stdout_path) if job is not None else None,
            "stderr_path": str(job.stderr_path) if job is not None else None,
        }
    if outcome is not None and outcome.liveness is not None:
        body["liveness"] = dict(outcome.liveness)
    body.update(dict(detail or {}))

    finished_unix = provenance.end_unix if provenance is not None else None
    record: dict[str, object] = {
        "schema": OUTCOME_SCHEMA_V1,
        # Not the pool's schema id, deliberately.  Every reader surveyed takes
        # these records by field name, and a distinct id is what lets one tell
        # a SLURM ending from a pull-queue one without guessing.
        "transport": "slurm",
        "action_key": key,
        "published_unix": float(published_unix),
        "published_by": str(published_by),
        "status": status,
        "attempts": int(attempts),
        "max_attempts": int(max_attempts),
        "retry_safe": retry_safe,
        "resources": {str(k): int(v) for k, v in dict(resources or {}).items()},
        "tags": [str(tag) for tag in tags],
        "claimed_by": (
            job.job_id if job is not None else claimed_by
        ),
        "claimed_unix": provenance.start_unix if provenance is not None else None,
        "claimed_host": provenance.node if provenance is not None else None,
        # The scheduler's end time when it has one.  When it does not -- a
        # purged job, a controller with no accounting -- this still has to be a
        # number, because ``pool.terminal_outcome_covers`` and ``pbrun`` both
        # read it, so it falls back to when this record was filed.
        "finished_unix": float(finished_unix) if finished_unix is not None else _now(),
        "finished_host": provenance.node if provenance is not None else None,
        "detail": body,
        **dict(addressing or {}),
    }
    if withdrawn_by is not None:
        record["withdrawn_by"] = withdrawn_by
    if withdrawn_unix is not None:
        record["withdrawn_unix"] = float(withdrawn_unix)
    if reason is not None:
        record["reason"] = reason
    # The marker's decision fields win over this call's: the first writer of
    # a withdrawal is the one who decided it, and the pool's readers of this
    # directory (``withdrawn_keys``, ``withdrawal_covers``, ``pool_reset``)
    # read exactly those fields.
    record.update(decision)
    if status == "cache_hit":
        # Filed only when the key has no ending at all, and link-first so that
        # holds against a writer this call never saw: ``_file_ending``'s
        # ``exists()`` check and the executing submitter's own filing can
        # interleave, and a rename here replaced the execution's record with
        # the hit's.  ``None`` means a record already stands, as it does for a
        # same-generation record above.
        if not _publish_json_if_absent(path, record):
            return None
        return path
    if not _land_summary(
        path, record, published_unix=float(published_unix),
        # The withdrawal enrichment replaces the marker of its own generation
        # with the marker plus the job's ending, which is the one write that
        # legitimately lands on a record of the same generation.
        refuse_same_generation=state != pool.WITHDRAWN,
    ):
        return None
    return path


def read_withdrawal_marker(
    queue_root: str | Path, action_key: str
) -> tuple[Path, dict[str, object] | None]:
    """The marker filed for this key, revalidated before it is read.

    ``pb-queue/withdrawn`` is on NFS with default attribute caching, and this
    fleet already measured what that does to a lookup of a name that did not
    exist yet: the client caches the negative entry, so ``exists()`` keeps
    answering False and ``open()`` keeps raising ``ENOENT`` after the marker
    has landed.  ``pbrun.terminal_record`` polls the terminal directories by
    ``os.listdir`` for exactly that reason, and this is the same rule for the
    same directory.

    Listing the parent is what revalidates the entry, so the listing happens
    first and the read happens only for a name the directory actually holds.
    What that costs is one ``READDIRPLUS`` per call; what it buys is that the
    three readers below cannot be told a decision was never made.

    An operator on another box withdraws a key inside the cache window: the
    marker is written, the terminal record is written, then ``scancel`` runs.
    The submitter's ``wait`` returns ``CANCELLED``, ``_file_ending`` asks
    ``withdrawal_covers``, and on a stale answer it calls
    ``publish_withdrawal``, whose own read misses too -- so the operator's
    marker is replaced, ``withdrawn_by`` becomes ``slurm:scancel``, the reason
    is lost, and ``publish_outcome`` copies both into the terminal record.  The
    same stale read in ``supersede_withdrawal`` leaves a live marker in place
    after a submission has retired it, and the one in ``run``'s retry gate lets
    a ``--retry-safe`` run submit attempt 2 of a withdrawn action.

    Returns:
        The marker's path, and the record it holds -- ``None`` when the
        directory does not hold that name, or holds bytes that are not a JSON
        object.
    """

    key = str(action_key)
    directory = Path(queue_root) / pool.WITHDRAWN
    path = directory / f"{key}.json"
    try:
        if path.name not in os.listdir(directory):
            return path, None
    except OSError:
        # No directory yet, or one this box cannot list.  Either way there is
        # nothing to read, and inventing a decision would be worse than
        # missing one.
        return path, None
    try:
        record = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return path, None
    return path, record if isinstance(record, dict) else None


def publish_withdrawal(
    *,
    queue_root: str | Path,
    action_key: str,
    reason: str = "",
    by: str = "",
    submission: Mapping[str, object] | None = None,
) -> tuple[Path, dict[str, object]]:
    """File the marker ``pool_reset`` reads before it re-submits anything.

    ``pool_reset`` skips a re-submission only on ``withdrawn_keys()`` or a
    top-level ``withdrawn_unix``.  Without a marker here, an operator's
    cancellation is re-submitted by the next bulk reset -- which is the outcome
    ``PoolQueue.withdraw``'s own docstring calls a decision, not a defect.

    Written in the pool's own outcome schema, because every reader of this
    directory is a pool reader and the marker is the pool's shape, not this
    lane's.  Idempotent: a second withdrawal leaves the first decision, its
    timestamp and its reason exactly where they were.
    """

    key = str(action_key)
    _queue_dir(queue_root, pool.WITHDRAWN)
    path, existing = read_withdrawal_marker(queue_root, key)
    if existing is not None:
        return path, existing
    filed = dict(submission or {})
    filed.update({
        "schema": pool.POOL_OUTCOME_SCHEMA_V1,
        "transport": "slurm",
        "action_key": key,
        "status": "withdrawn",
        "withdrawn_from": "slurm",
        "withdrawn_unix": _now(),
        "withdrawn_host": socket.gethostname(),
        "withdrawn_by": str(by),
        "reason": str(reason),
    })
    _write_json_atomic(path, filed)
    return path, filed


def withdrawal_covers(
    queue_root: str | Path, action_key: str, published_unix: float
) -> dict[str, object] | None:
    """The withdrawal filed against *this* generation, or ``None``.

    Writing the marker first is only half of what makes a withdrawal win the
    race; the other half is that everything which could overrule it reads the
    marker before acting.  ``PoolQueue`` does this at every claim, every finish
    and every requeue, and says so: from the instant the marker exists the
    action cannot be claimed, cannot be requeued and cannot be filed under
    ``done`` or ``failed``.

    Two things go wrong in this lane without it, both inside one poll interval.
    A job that finishes with a receipt just as ``--withdraw`` files its marker
    leaves ``scancel`` with nothing to cancel, and the submitter -- still in
    ``wait``, still seeing ``COMPLETED`` -- files a second terminal record for
    the same generation under ``done``.  And a retry-safe run whose attempt
    fails in that same window submits the next attempt and carries an action
    the operator cancelled through to completion.

    Scoped to the generation, like ``withdrawal_covers`` on the queue: a later
    submission of the same content-addressed key is a new request for the work,
    not an attempt to defeat somebody's cancellation.
    """

    _, record = read_withdrawal_marker(queue_root, action_key)
    if record is None:
        return None
    # The generation is checked on the record this already read, rather than by
    # a second open of the same path: one revalidated read is the whole point.
    theirs = record.get("published_unix")
    if not isinstance(theirs, (int, float)) or isinstance(theirs, bool):
        return None
    return record if float(theirs) == float(published_unix) else None


def supersede_withdrawal(
    queue_root: str | Path, action_key: str, published_unix: float | None
) -> dict[str, object] | None:
    """Retire a withdrawal of an EARLIER run, because a submission retires one.

    ``PoolQueue.publish`` does this and says at length why: the marker stops a
    claim, so leaving it in place makes the re-submitted action unrunnable and
    the only remedy a hand edit of the live queue.  This lane submits without
    going through ``publish``, so it does the same thing itself or inherits the
    bug the queue already fixed.  The decision is kept, not deleted.

    Only an *earlier* generation is retired.  ``submit`` makes ``latest.json``
    visible before it returns, so an operator on another box can resolve that
    record and withdraw the run while the submitting process is still between
    ``sbatch`` and its own next step.  Retiring whatever marker happened to be
    there then erased that decision: the retry gate found no marker and a
    ``--retry-safe`` run submitted attempt 2 of the action somebody had just
    cancelled, and the ending was filed as a failure rather than as the
    withdrawal it was.  A withdrawal of the generation being submitted is left
    exactly where it is, and it is what stops the remaining attempts (``run``)
    and what decides the ending (``_file_ending``).

    A marker naming no generation *is* retired.  Every writer in this tree
    stamps the generation on the marker it files -- ``publish_withdrawal``
    takes it from the submission and ``pbrun._file_slurm_withdrawal`` from
    ``latest.json`` -- so a marker without one belongs to a run older than the
    generation stamp itself, and leaving it in place is exactly the
    unrunnable-action state the pool fixed.

    ``published_unix`` is the generation being submitted, or ``None`` from a
    caller that cannot name one.  A caller that cannot name its own generation
    cannot compare, so it retires nothing.

    The claim on the marker is one ``rename``, which is what makes the
    comparison safe against a publication that crosses it.  The old shape read
    the marker, wrote an archive copy and then unlinked the live name, so a
    withdrawal filed inside that window was unlinked unread.  Now the rename
    takes whichever marker is at the name at that instant, the archived bytes
    are re-read, and a marker this call had no right to retire is linked back.
    A crash between the rename and the decision leaves the marker under
    ``superseded/`` instead of at the live name, which is a state a reader can
    see and repair; the window it replaces lost the bytes.

    Returns the withdrawal that was retired, or ``None`` when none was.
    """

    key = str(action_key)
    live, record = read_withdrawal_marker(queue_root, key)
    if record is None:
        return None
    if published_unix is None:
        return None
    theirs = _record_generation(record)
    if theirs is not None and theirs >= float(published_unix):
        return None
    when = _now()
    archive_directory = _queue_dir(queue_root, pool.WITHDRAWN) / "superseded"
    archive_directory.mkdir(parents=True, exist_ok=True)
    archive = archive_directory / f"{key}.{when:.6f}.withdrawal.json"
    try:
        os.rename(live, archive)
    except OSError:
        # Gone between the read and the claim: another submission of a later
        # generation retired it, and archiving it is that call's business.
        return None
    claimed = _read_json_object(archive)
    generation = _record_generation(claimed)
    if claimed is None or (generation is not None
                           and generation >= float(published_unix)):
        # A withdrawal landed at the live name between the read and the
        # rename, and this call has no right to retire it.  Put it back.
        try:
            os.link(archive, live)
        except FileExistsError:
            # A third writer already filed a marker there.  These bytes stay
            # under ``superseded/`` as history rather than being deleted.
            return None
        except OSError:
            return None
        archive.unlink(missing_ok=True)
        return None
    kept = dict(claimed or {})
    kept.update({
        "action_key": key,
        "superseded_unix": when,
        "superseded_host": socket.gethostname(),
    })
    _write_json_atomic(archive, kept)
    return claimed


def run(
    action: Mapping[str, object],
    *,
    cas: pb.PrismaBuildCAS,
    request_path: str | Path,
    placement: Sequence[str] = (),
    resources: LaneResources,
    timeout_s: float | None,
    worker_script: str | Path,
    job_entry: str | Path,
    retry_safe: bool = False,
    max_attempts: int = 1,
    root: str | Path | None = None,
    queue_root: str | Path | None = None,
    job_python: str = DEFAULT_JOB_PYTHON,
    worker_python: str = DEFAULT_JOB_PYTHON,
    local_checkout_root: str | Path | None = None,
    partition: str | None = None,
    priority: int = 0,
    sbatch: Command = "sbatch",
    sacct: Command = "sacct",
    scontrol: Command = "scontrol",
    squeue: Command = "squeue",
    sstat: Command = "sstat",
    poll_s: float = DEFAULT_POLL_S,
    wait_s: float | None = None,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
    on_submit: Callable[[SubmittedJob], None] | None = None,
    on_stall: Callable[[StallReport], None] | None = None,
    on_notice: Callable[[str], None] | None = None,
    detach: bool = False,
) -> RunResult:
    """Submit, wait, and resubmit while the producer's contract allows it.

    A retry is bounded by ``max_attempts`` and gated on ``retry_safe``, exactly
    as the pull queue bounds it, because the reason is the same one: an action
    may write external state before it fails, and numerical determinism says
    nothing about that.  A receipt in the CAS ends the loop whatever the exit
    code said, and so does a cancellation -- an operator's decision is not a
    defect to retry around.

    ``wait_s`` is one budget for the whole run, as ``pbrun.await_outcome``
    holds one deadline across the pool's retries: each attempt's ``wait`` is
    given what is left of it, so three attempts cannot turn a 30 minute
    ``--wait-s`` into ninety.  A retry submitted after the budget is spent
    still goes out (the retry policy is about the action, the budget about
    how long this caller stays) and its wait ends on the first poll.
    ``detach`` submits the first attempt and returns there, without waiting and
    without filing an ending.  The caller is saying that something else reads
    the job out later, so this must not file a verdict it has not observed: an
    ending written now would say ``failed`` for a job that is still queued.
    Everything up to the return is the same call, which is the point -- a
    detached submission asks the scheduler for exactly what an attached one
    asks for, partition and constraint and GRES included.  Retries stay with
    the attached form: a resubmission needs somebody alive to see the attempt
    fail, and after this returns nobody is.
    """

    key = str(action["action_key"])
    result = RunResult(action_key=key)
    attempts = max(1, int(max_attempts)) if retry_safe else 1
    deadline = None if wait_s is None else clock() + float(wait_s)
    # One generation for the whole run, stamped on every submission record and
    # on the ending.  Retries are attempts within it, not new requests.
    published_unix = _now()
    result.published_unix = published_unix
    published_by = socket.gethostname()
    for attempt in range(1, attempts + 1):
        if (
            attempt > 1
            and queue_root is not None
            and withdrawal_covers(queue_root, key, published_unix) is not None
        ):
            # Cancelled between this run's attempts.  ``scancel`` had nothing
            # left to stop, so nothing but this check keeps the next attempt
            # from carrying the withdrawn action through to completion.
            break
        job = submit(
            action,
            cas=cas,
            request_path=request_path,
            placement=placement,
            resources=resources,
            timeout_s=timeout_s,
            worker_script=worker_script,
            job_entry=job_entry,
            root=root,
            job_python=job_python,
            worker_python=worker_python,
            local_checkout_root=local_checkout_root,
            partition=partition,
            priority=priority,
            attempt=attempt,
            sbatch=sbatch,
            squeue=squeue,
            published_unix=published_unix,
            published_by=published_by,
            retry_safe=retry_safe,
            max_attempts=max_attempts,
        )
        if attempt == 1 and queue_root is not None:
            # A submission is what retires a withdrawal; see
            # ``supersede_withdrawal``.  After ``sbatch`` accepted, not before:
            # a refused submission has retired nothing, and an operator's
            # decision must not be moved aside by a job that never existed.
            with _naming_job(job.job_id):
                supersede_withdrawal(queue_root, key, published_unix)
        if on_submit is not None:
            on_submit(job)
        if detach:
            result.attempts.append((job, Outcome(
                job_id=job.job_id,
                state=DETACHED_STATE,
                exit_code=None,
                signal=None,
                stdout_path=job.stdout_path,
                stderr_path=job.stderr_path,
            )))
            return result
        outcome = wait(
            job,
            sacct=sacct,
            scontrol=scontrol,
            squeue=squeue,
            sstat=sstat,
            poll_s=poll_s,
            wait_s=None if deadline is None else max(0.0, deadline - clock()),
            sleep=sleep,
            clock=clock,
            on_stall=on_stall,
            on_notice=on_notice,
        )
        result.attempts.append((job, outcome))
        result.receipt = cas.lookup(action)
        if result.receipt is not None:
            break
        if outcome.state not in RETRIABLE_STATES:
            break
    if queue_root is not None:
        last_job = result.attempts[-1][0] if result.attempts else None
        with _naming_job(last_job.job_id if last_job is not None else ""):
            _file_ending(
                result,
                action=action,
                queue_root=queue_root,
                published_unix=published_unix,
                published_by=published_by,
                resources=resources,
                tags=placement,
                max_attempts=max_attempts,
                retry_safe=retry_safe,
            )
    return result


def resume(
    submission: Mapping[str, object],
    *,
    action: Mapping[str, object],
    cas: pb.PrismaBuildCAS,
    queue_root: str | Path,
    wait_s: float | None = None,
    poll_s: float = DEFAULT_POLL_S,
    sacct: Command = "sacct",
    scontrol: Command = "scontrol",
    squeue: Command = "squeue",
    sstat: Command = "sstat",
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
    on_stall: Callable[[StallReport], None] | None = None,
    on_notice: Callable[[str], None] | None = None,
) -> RunResult:
    """Wait for a job somebody else submitted, and file the ending they did not.

    ``run`` files the terminal record because it is the process holding the
    submission open.  A detached submission has no such process, so the ending
    has to be filed by whoever waits -- and the recorded submission is enough
    to do it from, on any box, which ``pbrun --withdraw`` already relies on:
    ``_file_slurm_withdrawal`` builds a complete terminal record out of
    ``latest.json`` alone.  This is the same reconstruction with the job's own
    outcome in it rather than an operator's decision.

    A wait that runs out of patience files nothing.  The job is still queued or
    running, and an ending written now would say ``failed`` about work nothing
    has watched -- which is the one thing a terminal record must never do.  A
    job the controller cannot account for at all files nothing either, for the
    same reason: not knowing is not the same as knowing it failed.

    ``wait_s=0`` is the useful non-waiting case.  It polls the controller once
    for provenance and then files what the receipt says, which is how a waiter
    that already holds the receipt files the missing ending without waiting on
    a job the scheduler may have forgotten.
    """

    job = submitted_job(submission)
    key = job.action_key
    published_unix = float(job.published_unix or 0.0)
    result = RunResult(action_key=key, published_unix=published_unix)
    outcome = wait(
        job, sacct=sacct, scontrol=scontrol, squeue=squeue, sstat=sstat,
        poll_s=poll_s, wait_s=wait_s, sleep=sleep, clock=clock,
        on_stall=on_stall, on_notice=on_notice,
    )
    result.attempts.append((job, outcome))
    result.receipt = cas.lookup(action)
    if ending_status(
        job, outcome, receipt=result.receipt,
        marker=withdrawal_covers(queue_root, key, published_unix),
    ) is None:
        # A fast path only: ``_file_ending`` holds the rule (it files nothing
        # for these two states after the marker and receipt checks), and
        # ``run`` reaches it through the same function.  Returning here saves
        # rebuilding the resources below for an ending that will not be
        # written.
        return result

    # Rebuilt from what was submitted rather than round-tripped through the
    # pool-shaped demand: ``gres`` is where exclusivity lives, and
    # ``LaneResources.demand`` does not carry it.
    gres = str(submission.get("gres") or "")
    _, _, count = gres.partition(":")
    resources = LaneResources(
        cpus=max(1, int(submission.get("cpus") or 1)),
        memory_mib=max(1, int(submission.get("memory_mib") or 4096)),
        gpu_slots=int(count) if count.isdigit() else 0,
        exclusive_gpu=gres.startswith("gpu:"),
    )
    with _naming_job(str(submission.get("job_id") or "")):
        _file_ending(
            result,
            action=action,
            queue_root=queue_root,
            published_unix=published_unix,
            published_by=str(submission.get("published_by") or ""),
            resources=resources,
            tags=[str(tag) for tag in (submission.get("constraint") or [])],
            max_attempts=int(submission.get("max_attempts") or 1),
            retry_safe=bool(submission.get("retry_safe")),
        )
    return result


def ending_status(
    job: SubmittedJob,
    outcome: Outcome,
    *,
    receipt: Mapping[str, object] | None,
    marker: Mapping[str, object] | None,
) -> str | None:
    """Which ending this job has, or ``None`` when it has not ended.

    The whole verdict, and the only place it is decided.  ``_file_ending``
    calls this and then writes what it says; ``sweep`` calls it to report what
    would be written without writing anything.  Two spellings of this ladder
    would let a reconcile report and the record it files disagree, and a
    reconcile that files a different ending from the one it reported is the
    defect ``PoolQueue.finish`` and ``reap_stale`` had between them.

    The order is an order of authority.  An operator's decision outranks
    everything: a job that finished inside the window between the marker
    landing and ``scancel`` reaching it is still a withdrawal, and filing it
    under ``done`` would leave one generation with two terminal records.  The
    CAS outranks the scheduler, because a receipt says the work was done
    whatever the scheduler goes on to say, and after ``MinJobAge`` it says
    nothing at all.  Only then is the job's own state read.

    ``None`` is the two states that are not endings: the caller stopped
    watching, or the controller answered that it knows no such job.  Neither
    is evidence the work failed, and ``publish_outcome`` is first-writer-wins
    per generation, so a ``failed`` filed on either would stand over the
    receipt the job publishes a minute later.

    Args:
        job: The submission this would be the ending of.
        outcome: What the scheduler last said about it.
        receipt: The CAS receipt for the action, or ``None``.
        marker: The withdrawal marker covering this generation, or ``None``.

    Returns:
        ``"withdrawn"``, ``"cache_hit"``, ``"executed"``, ``"failed"``, or
        ``None`` when nothing has ended.
    """

    if marker is not None:
        return "withdrawn"
    if receipt is not None:
        # Told apart by the node, not here.  All a submitter can see is that a
        # receipt exists, and one exists whether this job published it or read
        # it; ``write_cache_hit`` beside the logs is the node saying which.
        return "cache_hit" if job_was_cache_hit(job) else "executed"
    if outcome.state in (UNKNOWN_STATE, WAIT_TIMEOUT_STATE):
        return None
    if outcome.state == "CANCELLED":
        return "withdrawn"
    return "failed"


def _file_ending(
    result: RunResult,
    *,
    action: Mapping[str, object],
    queue_root: str | Path,
    published_unix: float,
    published_by: str,
    resources: LaneResources,
    tags: Sequence[str],
    max_attempts: int,
    retry_safe: bool,
) -> None:
    """Write the terminal record the pull queue's readers expect.

    ``ending_status`` decides *what* the ending is and states why; this writes
    it.  The split is what lets ``sweep`` report the same verdict without
    filing anything.

    A ``cache_hit`` never overwrites an ending already filed for this key.
    That is ``pbrun.cached_outcome``'s rule and the reason is the same: the
    record of the run that did the work is the one every reader wants, and a
    re-run that ran nothing has nothing to add to it.  With no record at all
    the hit is filed, so the fleet's readers still get an ending.
    """

    last = result.last
    if last is None:                 # unreachable: run always submits once
        return
    job, outcome = last
    marker = withdrawal_covers(queue_root, result.action_key, published_unix)
    status = ending_status(
        job, outcome, receipt=result.receipt, marker=marker)
    if status is None:
        # Nothing has ended.  Before this branch existed, a controller restart
        # filed ``failed`` for jobs that were still running.
        return

    if status == "cache_hit" and (
        _queue_dir(queue_root, pool.DONE) / f"{result.action_key}.json"
    ).exists():
        # See the docstring: the run that did the work keeps the record.
        return

    withdrawn_by = withdrawn_unix = None
    if status == "withdrawn":
        if marker is None:
            # Cancelled outside ``pbrun --withdraw`` -- by an operator running
            # ``scancel`` directly, or by the scheduler.  The marker still has
            # to exist or ``pool_reset`` re-submits the decision.
            _, marker = publish_withdrawal(
                queue_root=queue_root, action_key=result.action_key,
                reason="the job was cancelled", by="slurm:scancel",
                submission={"published_unix": published_unix},
            )
        withdrawn_by = marker.get("withdrawn_by")
        withdrawn_unix = marker.get("withdrawn_unix")

    params = action.get("params")
    addressing: dict[str, object] = {}
    if isinstance(params, Mapping):
        for name in ("checkout_snapshot", "checkout_root"):
            if params.get(name) is not None:
                addressing[name] = params[name]
                break

    publish_outcome(
        queue_root=queue_root,
        action_key=result.action_key,
        published_unix=published_unix,
        published_by=published_by,
        status=status,
        attempts=len(result.attempts),
        max_attempts=int(max_attempts),
        retry_safe=bool(retry_safe),
        addressing=addressing,
        resources=resources.demand(),
        tags=tags,
        job=job,
        outcome=outcome,
        receipt=result.receipt,
        withdrawn_by=withdrawn_by,
        withdrawn_unix=withdrawn_unix,
        reason=(
            str(marker.get("reason") or "") if status == "withdrawn" else None
        ),
    )


# --------------------------------------------------------------------------
# The sweeper: reconciling endings nobody asked about
# --------------------------------------------------------------------------
#
# Every ending this lane files is filed by somebody who asked about that one
# key.  ``run`` asks because it is holding the submission open, ``resume``
# because a waiter named the key, ``pbwait`` because an operator typed it.  A
# job that ends while nothing is watching therefore leaves its verdict in the
# controller's accounting and never becomes a record: ``pbrun --detach``
# followed by a waiter that died is the ordinary way to produce one, and
# ``pool_reset``'s re-submission of a sealed action is another, because it
# detaches on purpose and leaves the ending to whoever waits.
#
# ``sweep`` is the process that asks about all of them.  It is not a second
# filing path: it reconciles what the lane root records against what the queue
# holds, and hands every gap to ``resume``, which is the same call ``pbwait``
# makes.  A swept ending is byte-identical to a polled one because it is
# written by the same function from the same inputs.
#
# What it will not do is invent a verdict.  A job the controller cannot
# account for, and whose action has no receipt in the CAS, is reported as
# unknown and files nothing -- ``ending_status`` returns ``None`` for exactly
# that, and the sweeper reads the same answer the filer would.

#: What a sweep says about one key.  ``ALREADY_FILED``, ``SUPERSEDED``,
#: ``UNRECORDED`` and ``NO_ACTION`` are settled from disk alone; the rest cost
#: one poll of the controller each, which is why the disk checks come first.
SWEPT = "swept"                #: an ending was missing and this filed it
MISSING = "missing"            #: an ending is missing and ``--apply`` would file it
ALREADY_FILED = "filed"        #: this generation's ending is already on disk
SUPERSEDED = "superseded"      #: a later run of this key has already ended
UNRECORDED = "unrecorded"      #: the lane directory has no readable submission
NO_ACTION = "no-sealed-action"  #: the CAS holds no request to resolve the key with
WAITING = "waiting"            #: the job is still queued or running
NO_VERDICT = "no-verdict"      #: the controller knows no such job and there is no receipt
UNREACHABLE = "unreachable"    #: the controller could not be asked


def _filed_index(queue_root: str | Path) -> dict[str, list[tuple[str, Path]]]:
    """Every terminal record on disk, keyed by action key.

    One ``listdir`` per state directory rather than one ``stat`` per key.  The
    live lane root holds a directory per action this fleet has ever submitted,
    and asking the shared mount about each of them separately is the slow path
    a sweep would otherwise spend all its time in.

    Read by ``listdir`` for the reason ``pbrun.terminal_record`` gives: a
    ``stat`` of a path that did not exist yet is negatively cached on NFS, so
    a record filed by another box can stay invisible for ``acdirmax``.
    """

    index: dict[str, list[tuple[str, Path]]] = {}
    for state in (pool.DONE, pool.FAILED, pool.WITHDRAWN):
        directory = Path(queue_root) / state
        try:
            names = os.listdir(directory)
        except OSError:
            continue
        for name in names:
            if not name.endswith(".json"):
                continue
            index.setdefault(name[: -len(".json")], []).append(
                (state, directory / name)
            )
    return index


def _ending_on_disk(
    filed: Sequence[tuple[str, Path]], published_unix: float
) -> str | None:
    """Whether this generation's ending is already filed, and if not, why not.

    Returns ``ALREADY_FILED`` when a record this run may not replace stands,
    ``SUPERSEDED`` when a later run's ending stands, and ``None`` when the
    ending is still missing.

    The two comparisons are ``publish_outcome``'s own, reused rather than
    restated: ``_newer_ending_stands`` for a later generation, equality for
    this one.  A ``withdrawn/`` record of this generation carrying no
    ``detail`` is the marker ``publish_withdrawal`` filed and not an ending,
    and it is deliberately not counted as one -- the withdrawal enrichment is
    the one write that legitimately lands on a record of its own generation.
    """

    floor = float(published_unix)
    superseded = False
    for state, path in filed:
        record = _read_json_object(path)
        if record is None:
            # Filed and unreadable.  Not this sweeper's to replace: a rewrite
            # would destroy the evidence an operator needs to repair it.
            return ALREADY_FILED
        if _newer_ending_stands(record, floor):
            superseded = True
            continue
        if _record_generation(record) != floor:
            continue
        if state == pool.WITHDRAWN and "detail" not in record:
            continue
        return ALREADY_FILED
    return SUPERSEDED if superseded else None


def _reported_outcome(
    job: SubmittedJob,
    *,
    sacct: Command,
    scontrol: Command,
    squeue: Command,
) -> Outcome:
    """What ``wait`` would return for this job, without waiting and without writing.

    ``wait`` is the wrong call for a report: while a job is RUNNING it samples
    liveness, and a sample is appended to ``liveness.jsonl`` in the lane
    directory.  A read-only reconcile must leave the lane exactly as it found
    it, so this asks the controller once and builds the outcome from the
    answer, in the three shapes ``wait``'s own return sites use.

    Raises:
        SlurmLaneError: the controller could not be asked.  Not an answer
            about the job, and the caller reports it as such rather than as
            an ending.
    """

    answer = query_provenance(
        job.job_id, sacct=sacct, scontrol=scontrol, squeue=squeue)
    if answer is None:
        state, exit_code, signal = UNKNOWN_STATE, None, None
    elif answer.state in TERMINAL_STATES:
        state, exit_code, signal = answer.state, answer.exit_code, answer.signal
    else:
        # Alive.  A caller with no patience left is exactly what ``wait``
        # reports as a wait timeout, and ``ending_status`` files nothing for it.
        state, exit_code, signal = WAIT_TIMEOUT_STATE, None, None
    return Outcome(
        job_id=job.job_id,
        state=state,
        exit_code=exit_code,
        signal=signal,
        stdout_path=job.stdout_path,
        stderr_path=job.stderr_path,
        provenance=answer,
    )


def sweep(
    *,
    cas: pb.PrismaBuildCAS,
    queue_root: str | Path,
    root: str | Path | None = None,
    apply: bool = False,
    keys: Sequence[str] | None = None,
    sacct: Command = "sacct",
    scontrol: Command = "scontrol",
    squeue: Command = "squeue",
    sstat: Command = "sstat",
) -> list[dict[str, object]]:
    """Reconcile the lane's submissions against the endings filed for them.

    Every key with a recorded submission and no ending for that submission's
    generation is classified, and under ``apply`` the missing ending is filed
    through ``resume`` -- the same call ``pbwait`` makes for one key, so the
    record is the one a waiter would have written.

    The classification is read-only in both modes and is done before anything
    is filed, so the report and the filing take the same decision from the
    same ladder.  ``apply`` then costs a second poll of the controller for the
    keys it files, which is the price of not having two spellings of the
    verdict.

    Safe beside a live waiter.  Two writers of one key reach
    ``publish_outcome`` and ``_land_summary``, which links first at an empty
    name and refuses a record of its own generation, so a sweeper and a poller
    racing on one key leave exactly one record and neither raises.

    Args:
        cas: The CAS the receipts and the sealed requests live in.
        queue_root: The queue root holding ``done``, ``failed`` and ``withdrawn``.
        root: The lane root to reconcile.  ``None`` uses the lane's default.
        apply: File the missing endings.  The default only reports.
        keys: Reconcile only these action keys.  ``None`` is every key the
            lane root records.
        sacct: The ``sacct`` executable.
        scontrol: The ``scontrol`` executable.
        squeue: The ``squeue`` executable.
        sstat: The ``sstat`` executable, used only by ``resume``.

    Returns:
        One row per key, in key order, each carrying ``action_key``,
        ``job_id``, ``generation``, ``disposition``, ``status`` and ``note``.
        ``status`` is the ending's own word (``executed``, ``failed``,
        ``cache_hit``, ``withdrawn``) on a row that has one and ``None``
        otherwise.
    """

    wanted = recorded_keys(root) if keys is None else [str(key) for key in keys]
    filed = _filed_index(queue_root)
    rows: list[dict[str, object]] = []
    for key in wanted:
        submission = recorded_submission(key, root=root)
        if submission is None or not submission.get("job_id"):
            rows.append(_sweep_row(
                key, UNRECORDED,
                note="no readable latest.json in the lane directory"))
            continue
        try:
            job = submitted_job(submission)
            if job.action_key != key or not math.isfinite(job.published_unix):
                raise ValueError("submission key or generation is invalid")
        except (KeyError, TypeError, ValueError, OverflowError) as exc:
            rows.append(_sweep_row(
                key, UNRECORDED, note=f"invalid latest.json: {exc}"))
            continue
        generation = float(job.published_unix or 0.0)
        standing = _ending_on_disk(filed.get(key, ()), generation)
        if standing is not None:
            rows.append(_sweep_row(
                key, standing, job_id=job.job_id, generation=generation))
            continue
        action = recorded_action(cas, key)
        if action is None:
            rows.append(_sweep_row(
                key, NO_ACTION, job_id=job.job_id, generation=generation,
                note="no sealed action in the CAS to resolve the receipt with"))
            continue
        try:
            outcome = _reported_outcome(
                job, sacct=sacct, scontrol=scontrol, squeue=squeue)
        except SlurmLaneError as exc:
            rows.append(_sweep_row(
                key, UNREACHABLE, job_id=job.job_id, generation=generation,
                note=str(exc)))
            continue
        status = ending_status(
            job, outcome,
            receipt=cas.lookup(action),
            marker=withdrawal_covers(queue_root, key, generation),
        )
        if status is None:
            # Not an ending.  ``UNKNOWN`` is the controller saying it knows no
            # such job, and with no receipt behind it there is no verdict to
            # file; anything else is a job still queued or running.
            rows.append(_sweep_row(
                key,
                NO_VERDICT if outcome.state == UNKNOWN_STATE else WAITING,
                job_id=job.job_id, generation=generation,
                note=(
                    "the controller knows no such job and the CAS holds no "
                    "receipt; nothing filed"
                    if outcome.state == UNKNOWN_STATE
                    else f"slurm says {_scheduler_says(outcome)}"
                ),
            ))
            continue
        if not apply:
            rows.append(_sweep_row(
                key, MISSING, job_id=job.job_id, generation=generation,
                status=status))
            continue
        with _naming_job(job.job_id):
            result = resume(
                submission, action=action, cas=cas, queue_root=queue_root,
                wait_s=0.0, sacct=sacct, scontrol=scontrol, squeue=squeue,
                sstat=sstat,
            )
        # Read back, rather than repeated from the classification above.  The
        # controller can move between the two polls, and a row naming the
        # verdict this sweep predicted rather than the one it filed would be
        # exactly the divergence the shared ladder exists to prevent.
        landed = _filed_ending(queue_root, key, generation)
        last = result.last
        waiting = (
            last is not None and last[1].state == WAIT_TIMEOUT_STATE
            and last[1].provenance is not None
        )
        rows.append(_sweep_row(
            key,
            SWEPT if landed is not None else (WAITING if waiting else NO_VERDICT),
            job_id=job.job_id, generation=generation,
            status=landed,
            note=None if landed is not None else (
                "the job's state moved while this ran; nothing was filed"
            ),
        ))
    return rows


def _filed_ending(
    queue_root: str | Path, action_key: str, published_unix: float
) -> str | None:
    """The status of this generation's ending, or ``None`` when none stands."""

    floor = float(published_unix)
    for state in (pool.DONE, pool.FAILED, pool.WITHDRAWN):
        record = _read_json_object(
            Path(queue_root) / state / f"{action_key}.json")
        if record is not None and _record_generation(record) == floor:
            if state == pool.WITHDRAWN and "detail" not in record:
                continue
            return str(record.get("status") or "")
    return None


def _scheduler_says(outcome: Outcome) -> str:
    """The scheduler's own word for a job that has not ended."""

    provenance = outcome.provenance
    return str(provenance.state if provenance is not None else outcome.state)


def _sweep_row(
    action_key: str,
    disposition: str,
    *,
    job_id: str | None = None,
    generation: float | None = None,
    status: str | None = None,
    note: str | None = None,
) -> dict[str, object]:
    """One row of a sweep, with every field present on every row."""

    return {
        "action_key": action_key,
        "job_id": job_id,
        "generation": generation,
        "disposition": disposition,
        "status": status,
        "note": note,
    }
