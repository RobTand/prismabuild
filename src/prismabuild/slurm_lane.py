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

SUBMISSION_SCHEMA_V1 = "prismaquant.prismabuild.slurm_lane_submission.v1"

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

#: The terminal states a *retry* can honestly answer.  ``CANCELLED`` is an
#: operator's decision and re-running it would be overruling them; ``TIMEOUT``
#: and ``DEADLINE`` describe a wall clock the retry would meet identically.
RETRIABLE_STATES = frozenset({
    "BOOT_FAIL", "FAILED", "NODE_FAIL", "OUT_OF_MEMORY", "PREEMPTED",
})

#: What ``wait`` reports when the caller's own patience ran out first.  The job
#: is still queued or running; nothing has been cancelled.
WAIT_TIMEOUT_STATE = "WAIT_TIMEOUT"

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
    timeout_s: float,
    worker_script: str | Path,
    job_entry: str | Path,
    root: str | Path | None = None,
    job_python: str = DEFAULT_JOB_PYTHON,
    worker_python: str = DEFAULT_JOB_PYTHON,
    local_checkout_root: str | Path | None = None,
    partition: str | None = None,
    attempt: int = 1,
    sbatch: str = "sbatch",
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
        f"--time={format_time_limit(timeout_s)}",
        f"--mem={resources.memory_mib}M",
        f"--cpus-per-task={resources.cpus}",
    ]
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
        "time_limit": format_time_limit(timeout_s),
        "cpus": resources.cpus,
        "memory_mib": resources.memory_mib,
        "submitted_unix": time.time(),
        "submitted_host": socket.gethostname(),
    }
    record_path = directory / "submissions" / f"{int(attempt):03d}.json"
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


def _sacct_state(job_id: str, *, sacct: str) -> tuple[str, int | None, int | None] | None:
    completed = _run(
        [
            sacct, "-j", job_id, "--parsable2", "--noheader",
            "-o", "JobID,State,ExitCode",
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
        return (state, exit_code, signal_number)
    return None


_SCONTROL_FIELD = re.compile(r"(\w+)=(\S*)")


def _scontrol_state(
    job_id: str, *, scontrol: str
) -> tuple[str, int | None, int | None] | None:
    completed = _run([scontrol, "show", "job", job_id], where="scontrol")
    if completed.returncode != 0:
        return None
    fields = dict(_SCONTROL_FIELD.findall(completed.stdout))
    state = fields.get("JobState", "").strip()
    if not state:
        return None
    exit_code, signal_number = _exit_fields(fields.get("ExitCode", ""))
    return (state, exit_code, signal_number)


def _squeue_state(
    job_id: str, *, squeue: str
) -> tuple[str, int | None, int | None] | None:
    completed = _run(
        [squeue, "-h", "-j", job_id, "-o", "%T"], where="squeue"
    )
    if completed.returncode != 0:
        return None
    state = completed.stdout.strip().splitlines()
    if not state or not state[0].strip():
        return None
    return (state[0].strip().split()[0], None, None)


def query_state(
    job_id: str,
    *,
    sacct: str = "sacct",
    scontrol: str = "scontrol",
    squeue: str = "squeue",
) -> tuple[str, int | None, int | None] | None:
    """What the scheduler currently says, from whichever tool can say it.

    ``sacct`` first because it is the only one that still answers about a job
    older than ``MinJobAge`` -- and the only one that is inert until slurmdbd
    exists, which is why it is not the only one asked.
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
    while True:
        answer = query_state(
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
            )
        state, exit_code, signal_number = answer
        if state in TERMINAL_STATES:
            return Outcome(
                job_id=job.job_id,
                state=state,
                exit_code=exit_code,
                signal=signal_number,
                stdout_path=job.stdout_path,
                stderr_path=job.stderr_path,
            )
        if deadline is not None and time.monotonic() > deadline:
            return Outcome(
                job_id=job.job_id,
                state=WAIT_TIMEOUT_STATE,
                exit_code=None,
                signal=None,
                stdout_path=job.stdout_path,
                stderr_path=job.stderr_path,
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


def run(
    action: Mapping[str, object],
    *,
    cas: pb.PrismaBuildCAS,
    request_path: str | Path,
    placement: Sequence[str] = (),
    resources: LaneResources,
    timeout_s: float,
    worker_script: str | Path,
    job_entry: str | Path,
    retry_safe: bool = False,
    max_attempts: int = 1,
    root: str | Path | None = None,
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
) -> RunResult:
    """Submit, wait, and resubmit while the producer's contract allows it.

    A retry is bounded by ``max_attempts`` and gated on ``retry_safe``, exactly
    as the pull queue bounds it, because the reason is the same one: an action
    may write external state before it fails, and numerical determinism says
    nothing about that.  A receipt in the CAS ends the loop whatever the exit
    code said, and so does a cancellation -- an operator's decision is not a
    defect to retry around.
    """

    result = RunResult(action_key=str(action["action_key"]))
    attempts = max(1, int(max_attempts)) if retry_safe else 1
    for attempt in range(1, attempts + 1):
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
        )
        if on_submit is not None:
            on_submit(job)
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
            return result
        if outcome.state not in RETRIABLE_STATES:
            return result
    return result
