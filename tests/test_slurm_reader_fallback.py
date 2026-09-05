"""An accounting outage does not hide a healthy controller's answer.

``sacct`` talks to slurmdbd and ``scontrol`` and ``squeue`` talk to slurmctld,
so accounting can hang while the controller is healthy and holds the job's
ending.  ``query_provenance`` called its three readers without handling a
per-reader failure, so a ``sacct`` that hung past ``COMMAND_TIMEOUT_S`` escaped
the loop before ``scontrol`` was ever asked, and ``wait`` read that as the
scheduler as a whole being unavailable.  Every later poll started again at the
same hung call: a job that had completed stayed unobserved until the caller's
wait expired, or forever with no wait budget.

Issue #67.  The readers are independent programs, so one that cannot be run is
carried and the next one is asked; an outage is reported only when none of them
can establish the state.
"""
from __future__ import annotations

from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import slurm_lane as sl  # noqa: E402

REPOSITORY = Path(__file__).resolve().parents[1]
WORKER = REPOSITORY / "tools" / "prismabuild_worker.py"
JOB_ENTRY = REPOSITORY / "tools" / "fleet" / "slurm_job.py"
KEY = "c" * 64
ACTION = {"action_key": KEY, "params": {}}


def _completed(argv, stdout: str = "", returncode: int = 0, stderr: str = ""):
    return subprocess.CompletedProcess(argv, returncode, stdout, stderr)


class _Clock:
    """A clock that only moves when the wait sleeps."""

    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def sleep(self, delta: float) -> None:
        self.now += float(delta)


def _submit(tmp_path: Path, job_id: str = "3001") -> sl.SubmittedJob:
    cas = SimpleNamespace(root=tmp_path / "cas", lookup=lambda _action: None)
    return sl.submit(
        ACTION, cas=cas, request_path=tmp_path / "request.json",
        resources=sl.LaneResources(), timeout_s=None, worker_script=WORKER,
        job_entry=JOB_ENTRY, root=tmp_path / "lane", published_unix=300.0,
        sbatch=lambda argv: _completed(argv, job_id),
    )


def test_a_hung_sacct_still_reaches_the_controller_that_knows_the_ending(
    tmp_path: Path
) -> None:
    """Pre-fix: ``outcome.state == "COMPLETED"`` failed with ``'WAIT_TIMEOUT'``
    and ``calls["scontrol"]`` was 0 after three polls."""

    job = _submit(tmp_path)
    calls = {"sacct": 0, "scontrol": 0, "squeue": 0}
    clock = _Clock()

    def hung_sacct(argv):
        calls["sacct"] += 1
        raise subprocess.TimeoutExpired("sacct", 60.0)

    def healthy_scontrol(argv):
        calls["scontrol"] += 1
        return _completed(argv, "JobId=3001 JobState=COMPLETED ExitCode=0:0")

    def healthy_squeue(argv):
        calls["squeue"] += 1
        return _completed(argv, "")

    outcome = sl.wait(
        job, sacct=hung_sacct, scontrol=healthy_scontrol,
        squeue=healthy_squeue, wait_s=1.0, poll_s=1.0, clock=clock,
        sleep=clock.sleep,
    )

    assert outcome.state == "COMPLETED"
    assert outcome.exit_code == 0
    assert calls["scontrol"] >= 1
    assert calls["sacct"] >= 1


def test_an_outage_is_reported_only_when_no_reader_can_answer(
    tmp_path: Path
) -> None:
    """The distinction ``wait`` reads is preserved: nothing established the
    state, so this is an outage the caller is told about and keeps waiting
    through, not an ending."""

    job = _submit(tmp_path, "3002")
    clock = _Clock()
    notices: list[str] = []

    def hung_sacct(argv):
        raise subprocess.TimeoutExpired("sacct", 60.0)

    def down(argv):
        return _completed(
            argv, returncode=1,
            stderr="slurm_load_jobs error: Unable to contact slurm controller "
                   "(connect failure)\n",
        )

    outcome = sl.wait(
        job, sacct=hung_sacct, scontrol=down, squeue=down, wait_s=1.0,
        poll_s=1.0, clock=clock, sleep=clock.sleep,
        on_notice=notices.append,
    )

    assert outcome.state == sl.WAIT_TIMEOUT_STATE
    # The first failure is the one that started the outage, and it is what the
    # operator is told about.
    assert "the scheduler could not be asked (sacct failed" in notices[0]
    assert "the job is not affected" in notices[0]


def test_three_readers_that_answer_no_such_job_are_still_unknown(
    tmp_path: Path
) -> None:
    """A reader that *answered* is not a reader that failed.  All three know
    no such job, so the controller has forgotten it and the wait ends
    ``UNKNOWN`` rather than reporting an outage."""

    job = _submit(tmp_path, "3003")

    def silent(argv):
        return _completed(argv, "")

    outcome = sl.wait(
        job, sacct=silent, scontrol=silent, squeue=silent, poll_s=0.0)

    assert outcome.state == sl.UNKNOWN_STATE
