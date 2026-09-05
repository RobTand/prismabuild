"""Submit one sealed action to SLURM and wait for it.  Thin, on purpose.

``slurm.py`` is the other SLURM client in this tree and it is not this one.  It
exists for a restartable orchestrator: sealed submission intents, adoption of a
job whose submitter died mid-``sbatch``, append-only poll journals, a mutation
ledger.  Every one of those answers "who owns this job if the process that
submitted it is gone", and an interactive ``pbrun --wait`` has no such question
-- the submitter is a person's terminal, and when it dies the right answer is
``scancel``, not adoption.  So this module is a transport and nothing else:
write a script, ``sbatch`` it, record what was submitted, poll, report.

Three decisions are worth stating, because each had an alternative.

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
#: A hung ``squeue`` against a busy controller must not become a hung ``pbrun``.
COMMAND_TIMEOUT_S = 60.0

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


def wait(
    job: SubmittedJob,
    *,
    sacct: str = "sacct",
    scontrol: str = "scontrol",
    squeue: str = "squeue",
    poll_s: float = DEFAULT_POLL_S,
    wait_s: float | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> Outcome:
    """Poll until the job reaches a terminal state, the caller's patience ends,
    or the scheduler stops knowing about it.

    None of the three is a verdict on the work.  The caller reads the CAS for
    that; this says which job to read the logs of, and why it stopped.
    """

    deadline = None if wait_s is None else time.monotonic() + float(wait_s)
    last: JobProvenance | None = None
    while True:
        answer = query_provenance(
            job.job_id, sacct=sacct, scontrol=scontrol, squeue=squeue
        )
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
            )
        if deadline is not None and time.monotonic() > deadline:
            return Outcome(
                job_id=job.job_id,
                state=WAIT_TIMEOUT_STATE,
                exit_code=None,
                signal=None,
                stdout_path=job.stdout_path,
                stderr_path=job.stderr_path,
                provenance=answer,
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
    poll_s: float = DEFAULT_POLL_S,
    wait_s: float | None = None,
    sleep: Callable[[float], None] = time.sleep,
    on_submit: Callable[[SubmittedJob], None] | None = None,
    detach: bool = False,
) -> RunResult:
    """Submit, wait, and resubmit while the producer's contract allows it.

    A retry is bounded by ``max_attempts`` and gated on ``retry_safe``, exactly
    as the pull queue bounds it, because the reason is the same one: an action
    may write external state before it fails, and numerical determinism says
    nothing about that.  A receipt in the CAS ends the loop whatever the exit
    code said, and so does a cancellation -- an operator's decision is not a
    defect to retry around.

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
            poll_s=poll_s,
            wait_s=wait_s,
            sleep=sleep,
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
    sleep: Callable[[float], None] = time.sleep,
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
        job, sacct=sacct, scontrol=scontrol, squeue=squeue,
        poll_s=poll_s, wait_s=wait_s, sleep=sleep,
    )
    result.attempts.append((job, outcome))
    result.receipt = cas.lookup(action)
    if outcome.state in (WAIT_TIMEOUT_STATE, UNKNOWN_STATE) and result.receipt is None:
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
