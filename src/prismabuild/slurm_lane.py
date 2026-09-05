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
That is the same rule the pull queue follows, which is what lets an action
executed under either transport mean the same thing.

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
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
import json
import math
import os
from pathlib import Path
import re
import shlex
import socket
import subprocess
import time
from typing import Callable

from . import core as pb
from . import pool

SUBMISSION_SCHEMA_V1 = "prismaquant.prismabuild.slurm_lane_submission.v1"

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

#: ``/usr/bin/python3`` on both architectures, deliberately.  A job may land on
#: aarch64 or x86_64 and ``fleet_boxes.json`` names a different venv per box, so
#: no single venv path is correct for the launcher.  It does not need to be:
#: ``prismabuild_worker.py`` is stdlib-only by construction, and the action's
#: own sealed argv[0] selects whatever interpreter the work requires.
DEFAULT_JOB_PYTHON = "/usr/bin/python3"

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


def lane_directory(action_key: str, *, root: str | Path | None = None) -> Path:
    """One directory per action key: script, records, logs."""

    key = str(action_key)
    if len(key) != 64 or any(ch not in "0123456789abcdef" for ch in key):
        raise SlurmLaneError(f"action key is not a 64-hex digest: {key!r}")
    return lane_root(root) / key


def job_state_directory(*, root: str | Path | None = None) -> Path:
    """Where a job leaves the facts its Epilog needs after it is gone."""

    return lane_root(root) / JOB_STATE_DIRNAME


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


def _run(argv: Sequence[str], *, where: str) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            list(argv),
            capture_output=True,
            text=True,
            timeout=COMMAND_TIMEOUT_S,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise SlurmLaneError(f"{where} failed: {exc}") from exc


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
    """Point at the newest attempt by rename, so a reader sees one or the other."""

    raw = pb._canonical_file_bytes(dict(payload))
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.parent / f".{path.name}.{os.getpid()}.tmp"
    tmp.write_bytes(raw)
    os.replace(tmp, path)


def _now() -> float:
    return time.time()


def _write_json_atomic(path: Path, payload: Mapping[str, object]) -> None:
    """Replace a terminal record whole, the way ``pool._write_json_atomic`` does.

    The mutable summary is a pointer, not an audit log: the queue rewrites it on
    every retry and so must this.  First-writer-wins belongs to the *immutable*
    records -- the submission records above -- and the generation check in
    ``publish_outcome`` is what keeps two writers inside one generation from
    overwriting each other.
    """

    _write_latest(path, payload)


def job_script_text(
    *,
    request_path: str | Path,
    cas_root: str | Path,
    worker_script: str | Path,
    job_entry: str | Path,
    lane_directory_path: str | Path,
    job_state_root: str | Path,
    job_python: str = DEFAULT_JOB_PYTHON,
    worker_python: str = DEFAULT_JOB_PYTHON,
    local_checkout_root: str | Path | None = None,
) -> str:
    """The batch script: materialize inside the job, then run the worker.

    Materialization happens *here*, on the node that won the allocation, rather
    than at submit time on the submitter's box.  A checkout materialized before
    submission would be a path on one machine handed to a scheduler free to run
    the job on another, and the whole point of the sealed snapshot is that the
    tree is reconstructed wherever the work lands.

    Every path is absolute because ``--export=NIL`` leaves the job with only
    SLURM's own variables: there is no ``PATH`` to resolve a bare name against.
    """

    argv = [
        str(job_python),
        str(job_entry),
        "--action", str(request_path),
        "--cas-root", str(cas_root),
        "--worker", str(worker_script),
        "--worker-python", str(worker_python),
        "--lane-dir", str(lane_directory_path),
        "--job-state-root", str(job_state_root),
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
    attempt: int = 1,
    sbatch: str = "sbatch",
    published_unix: float | None = None,
    published_by: str | None = None,
    retry_safe: bool | None = None,
    max_attempts: int = 1,
) -> SubmittedJob:
    """Write the script, ``sbatch`` it, seal what was submitted, return the id.

    The record is written *after* the id is known and before anything blocks on
    the job, because a job id nobody wrote down is a job nobody can withdraw.
    """

    key = str(action["action_key"])
    directory = lane_directory(key, root=root)
    directory.mkdir(parents=True, exist_ok=True)
    state_root = job_state_directory(root=root)
    state_root.mkdir(parents=True, exist_ok=True)

    script = directory / "job.sh"
    text = job_script_text(
        request_path=request_path,
        cas_root=cas.root,
        worker_script=worker_script,
        job_entry=job_entry,
        lane_directory_path=directory,
        job_state_root=state_root,
        job_python=job_python,
        worker_python=worker_python,
        local_checkout_root=local_checkout_root,
    )
    # Rewritten rather than published immutably: the script is derived from the
    # action and the deployment, and the deployment's runtime generation may
    # legitimately roll between two submissions of one action key.  What must
    # not drift is the record of what each submission actually sent, and that
    # is first-writer-published below.
    tmp = directory / f".job.sh.{os.getpid()}.tmp"
    tmp.write_text(text, encoding="utf-8")
    tmp.chmod(0o755)
    os.replace(tmp, script)

    stdout_template = directory / "%j.out"
    stderr_template = directory / "%j.err"
    argv = [
        str(sbatch),
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
        # Without this the job inherits the submitter's cwd, which is a
        # box-local checkout that need not exist on the node that runs it.
        f"--chdir={directory}",
        f"--output={stdout_template}",
        f"--error={stderr_template}",
        f"--mem={resources.memory_mib}M",
        f"--cpus-per-task={resources.cpus}",
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

    completed = _run(argv, where="sbatch")
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout).strip()
        raise SlurmLaneError(
            f"sbatch refused this action: {detail or completed.returncode}"
        )
    output = completed.stdout.strip()
    if not output or "\n" in output or "\r" in output:
        raise SlurmLaneError(
            f"sbatch --parsable returned no single job id: {completed.stdout!r}"
        )
    # --parsable prints "<id>" or "<id>;<cluster>" on a federated controller.
    job_id = output.split(";", 1)[0].strip()
    if not job_id.isdigit():
        raise SlurmLaneError(f"sbatch returned no numeric job id: {output!r}")

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
        "directory": str(directory),
        "stdout": str(directory / f"{job_id}.out"),
        "stderr": str(directory / f"{job_id}.err"),
        "request": str(request_path),
        "cas_root": str(cas.root),
        "constraint": tags,
        "gres": gres or "",
        # Empty means the default partition: the constraint decided.
        "partition": partition or "",
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
    )


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


def _sacct_state(job_id: str, *, sacct: str) -> JobProvenance | None:
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


def _scontrol_state(job_id: str, *, scontrol: str) -> JobProvenance | None:
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
    )


def _squeue_state(job_id: str, *, squeue: str) -> JobProvenance | None:
    completed = _run(
        [squeue, "-h", "-j", job_id, "-o", "%T"], where="squeue"
    )
    if completed.returncode != 0:
        if _unreachable(completed):
            raise ControllerUnreachable(
                f"squeue could not reach the controller: "
                f"{(completed.stderr or completed.stdout).strip()}"
            )
        return None
    state = completed.stdout.strip().splitlines()
    if not state or not state[0].strip():
        return None
    return JobProvenance(state=state[0].strip().split()[0])


def query_provenance(
    job_id: str,
    *,
    sacct: str = "sacct",
    scontrol: str = "scontrol",
    squeue: str = "squeue",
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
    """

    for reader in (
        lambda: _sacct_state(job_id, sacct=sacct),
        lambda: _scontrol_state(job_id, scontrol=scontrol),
        lambda: _squeue_state(job_id, squeue=squeue),
    ):
        answer = reader()
        if answer is not None:
            return answer
    return None


def query_state(
    job_id: str,
    *,
    sacct: str = "sacct",
    scontrol: str = "scontrol",
    squeue: str = "squeue",
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
        sstat: str = "sstat",
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
    sacct: str = "sacct",
    scontrol: str = "scontrol",
    squeue: str = "squeue",
    sstat: str = "sstat",
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


def cancel(job_id: str, *, scancel: str = "scancel") -> bool:
    """Stop one job.  SLURM sends TERM, then KILL after ``KillWait``."""

    completed = _run([scancel, str(job_id)], where="scancel")
    return completed.returncode == 0


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


def _same_generation(path: Path, published_unix: float) -> bool:
    """Is the record already there this submission's own, or an older one?

    An action key is a content hash, so the same key is re-submitted every time
    somebody asks for the same work again.  ``published_unix`` equality is the
    queue's own generation rule -- ``terminal_outcome_covers`` states it, and
    states that an old outcome must not blacklist a later submission -- so a
    writer defers to a record of its own generation and replaces an older one.
    """

    try:
        existing = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return False
    theirs = existing.get("published_unix") if isinstance(existing, dict) else None
    return isinstance(theirs, (int, float)) and float(theirs) == float(published_unix)


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
    ``pbrun`` returns any integer returncode as its own exit status), so that
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
    """File one action's ending under ``done/`` or ``failed/``.

    Which directory is decided by the CAS, not by the exit status: a receipt
    means the work was done whatever the job said afterwards, and no receipt
    means it was not, even from a job that exited zero.  That is the rule
    ``PoolQueue.finish`` applies through ``succeeded``; only the machinery
    underneath it differs.

    Returns the path written, or ``None`` when a record of this same generation
    was already there -- the first account of a generation is the one that
    stands, and a later generation replaces it.
    """

    key = str(action_key)
    provenance = outcome.provenance if outcome is not None else None
    state = pool.DONE if status == "executed" else pool.FAILED
    path = _queue_dir(queue_root, state) / f"{key}.json"
    if path.exists() and _same_generation(path, published_unix):
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
    if job is not None or outcome is not None:
        body["slurm"] = {
            "job_id": job.job_id if job is not None
            else (outcome.job_id if outcome is not None else None),
            "state": outcome.state if outcome is not None else None,
            "partition": provenance.partition if provenance is not None else None,
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
    _write_json_atomic(path, record)
    return path


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
    path = _queue_dir(queue_root, pool.WITHDRAWN) / f"{key}.json"
    try:
        existing = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(existing, dict):
            return path, existing
    except (OSError, ValueError):
        pass
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

    marker = Path(queue_root) / pool.WITHDRAWN / f"{action_key}.json"
    if not marker.exists() or not _same_generation(marker, published_unix):
        return None
    try:
        record = json.loads(marker.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return record if isinstance(record, dict) else None


def supersede_withdrawal(
    queue_root: str | Path, action_key: str
) -> dict[str, object] | None:
    """Retire a live withdrawal, because a submission is what retires one.

    ``PoolQueue.publish`` does this and says at length why: the marker stops a
    claim, so leaving it in place makes the re-submitted action unrunnable and
    the only remedy a hand edit of the live queue.  This lane submits without
    going through ``publish``, so it does the same thing itself or inherits the
    bug the queue already fixed.  The decision is kept, not deleted.
    """

    key = str(action_key)
    live = Path(queue_root) / pool.WITHDRAWN / f"{key}.json"
    try:
        record = json.loads(live.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(record, dict):
        return None
    when = _now()
    kept = dict(record)
    kept.update({
        "action_key": key,
        "superseded_unix": when,
        "superseded_host": socket.gethostname(),
    })
    archive = _queue_dir(queue_root, pool.WITHDRAWN) / "superseded"
    archive.mkdir(parents=True, exist_ok=True)
    _write_json_atomic(archive / f"{key}.{when:.6f}.withdrawal.json", kept)
    live.unlink(missing_ok=True)
    return record


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
    sbatch: str = "sbatch",
    sacct: str = "sacct",
    scontrol: str = "scontrol",
    squeue: str = "squeue",
    sstat: str = "sstat",
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
    if queue_root is not None:
        # A submission is what retires a withdrawal; see ``supersede_withdrawal``.
        supersede_withdrawal(queue_root, key)
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
            attempt=attempt,
            sbatch=sbatch,
            published_unix=published_unix,
            published_by=published_by,
            retry_safe=retry_safe,
            max_attempts=max_attempts,
        )
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
    sacct: str = "sacct",
    scontrol: str = "scontrol",
    squeue: str = "squeue",
    sstat: str = "sstat",
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

    key = str(submission["action_key"])
    generation = submission.get("published_unix")
    if not isinstance(generation, (int, float)) or isinstance(generation, bool):
        # A submission record from before the generation stamp.  Wait for it,
        # but do not claim to know which request it belonged to.
        generation = float(submission.get("submitted_unix") or 0.0)
    published_unix = float(generation)
    attempt = int(submission.get("attempt") or 1)
    directory = Path(str(submission.get("directory") or "."))
    job = SubmittedJob(
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
    )
    result = RunResult(action_key=key, published_unix=published_unix)
    outcome = wait(
        job, sacct=sacct, scontrol=scontrol, squeue=squeue, sstat=sstat,
        poll_s=poll_s, wait_s=wait_s, sleep=sleep, clock=clock,
        on_stall=on_stall, on_notice=on_notice,
    )
    result.attempts.append((job, outcome))
    result.receipt = cas.lookup(action)
    if outcome.state in (WAIT_TIMEOUT_STATE, UNKNOWN_STATE) and result.receipt is None:
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

    ``cache_hit`` is not among the statuses this can produce, and that is a
    limit rather than a choice: the queue learns it from ``run_local``'s own
    verdict on the box, while all this sees is a receipt that exists.  A hit
    and a fresh execution are therefore both ``executed`` here, and the job's
    own log is where the difference is visible.
    """

    last = result.last
    if last is None:                 # unreachable: run always submits once
        return
    job, outcome = last
    # The marker outranks whatever the job went on to do.  A job that finished
    # inside the window between the marker landing and ``scancel`` reaching it
    # is still a withdrawal, and filing it under ``done`` would leave one
    # generation with two terminal records -- which ``merge_suite`` refuses to
    # resolve and ``reclaim_terminal_reservation`` refuses to act on.
    marker = withdrawal_covers(queue_root, result.action_key, published_unix)
    if marker is not None:
        status = "withdrawn"
    elif result.receipt is not None:
        status = "executed"
    elif outcome.state in (UNKNOWN_STATE, WAIT_TIMEOUT_STATE):
        # No ending has happened.  The submitter stopped watching, or the
        # controller answered that it knows no such job (purged past
        # MinJobAge with no accounting behind it, and no receipt yet).  A
        # terminal record here is a lie with consequences: ``publish_outcome``
        # is first-writer-wins per generation, so a ``failed/`` record filed
        # now would stand even when the job publishes its receipt minutes
        # later, and ``done/`` would never be written.  Before this branch
        # existed, that is exactly what a controller restart produced.
        return
    elif outcome.state == "CANCELLED":
        status = "withdrawn"
    else:
        status = "failed"

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
