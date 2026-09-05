"""What an operator reads when a lane record will not write.

Every fact the SLURM lane keeps is written *after* the thing it records: the
submission record after ``sbatch`` accepted the job, the terminal record after
the receipt landed in the CAS. A full mount, a queue directory somebody
tightened, a stale NFS handle -- any of them turns that write into an
``OSError`` at a point where the work itself is fine.

Two things were wrong with how that reached the caller:

*   The ``OSError`` was not caught at all, so an operator got a raw traceback
    ending in a temp file name. Nothing said the job id, nothing said the
    receipt was already in the CAS, and nothing said the run was recoverable.

*   A ``SlurmLaneError`` raised after ``sbatch`` accepted the job was reported
    with the words written for a refusal: "slurm refused this action ... Fix
    the --tag". The tag was fine. The job was queued while its submitter was
    being told to change the submission.

So both failures now name the accepted job, name what could not be written,
and say which command files the ending. The exit code is 74, which is neither
``GAVE_UP_EXIT`` nor ``WITHDRAWN_EXIT``: this is not "no verdict yet" and it is
not a withdrawal.
"""
from __future__ import annotations

import os
from pathlib import Path
import sys

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))

from prismabuild import core as pb  # noqa: E402
from prismabuild import pool  # noqa: E402
from prismabuild import slurm_lane as sl  # noqa: E402

import pbrun  # noqa: E402

from test_slurm_lane import _runnable_action, fleet  # noqa: E402,F401


def _readable_but_unwritable(directory: Path) -> None:
    """Let a reader list and read this directory, but let nobody create in it.

    A queue directory nothing can create in is the cheapest faithful stand-in
    for the failure this reports: the readers that run first (``PoolQueue``,
    ``live_submission``, ``_same_generation``) all still work, so the only
    thing that fails is the write.
    """

    directory.mkdir(parents=True, exist_ok=True)
    os.chmod(directory, 0o555)


@pytest.fixture
def unwritable(tmp_path: Path):
    """Restore every mode this test tightened, whatever the test did."""

    tightened: list[Path] = []

    def tighten(directory: Path) -> Path:
        _readable_but_unwritable(directory)
        tightened.append(directory)
        return directory

    try:
        yield tighten
    finally:
        for directory in tightened:
            os.chmod(directory, 0o755)


def test_a_queue_it_cannot_write_is_reported_rather_than_raised(
    tmp_path: Path, fleet: Path, unwritable, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The work ran, the receipt is in the CAS, and ``done/`` will not take the
    record. That is a report with a job id in it, not a traceback."""

    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "run")
    monkeypatch.setenv("PRISMABUILD_LOCAL_CHECKOUT_ROOT",
                       str(tmp_path / "materialized"))
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    action = _runnable_action(tmp_path, cas)
    request = cas.publish_action_request(action)
    queue = tmp_path / "queue"
    done = unwritable(queue / pool.DONE)

    code = pbrun.slurm_outcome(
        action, cas=cas, request_path=request, tags=["x86"],
        demand={"cpu": 1, "mem_gb": 4}, exclusive=False,
        timeout_s=600.0, wait_s=60.0, retry_safe=False, max_attempts=1,
        runtime_root=REPOSITORY, poll_s=0.0, queue_root=queue,
    )

    assert code == pbrun.RECORD_WRITE_FAILED_EXIT
    assert code not in (pbrun.GAVE_UP_EXIT, pbrun.WITHDRAWN_EXIT, 0)
    err = capsys.readouterr().err
    assert str(done) in err
    assert "Permission denied" in err
    assert "pbwait" in err
    # The work is not lost, and the line says so rather than leaving an
    # operator to guess whether re-running would repeat it.
    assert cas.lookup(action) is not None
    assert "receipt" in err


def test_the_report_names_the_job_slurm_accepted(
    tmp_path: Path, fleet: Path, unwritable, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """An operator cannot ``scancel`` or ``sacct`` a job whose id nothing
    printed, and the record that would have carried it is the one that failed."""

    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "run")
    monkeypatch.setenv("PRISMABUILD_LOCAL_CHECKOUT_ROOT",
                       str(tmp_path / "materialized"))
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    action = _runnable_action(tmp_path, cas)
    request = cas.publish_action_request(action)
    queue = tmp_path / "queue"
    unwritable(queue / pool.DONE)

    pbrun.slurm_outcome(
        action, cas=cas, request_path=request, tags=["x86"],
        demand={"cpu": 1, "mem_gb": 4}, exclusive=False,
        timeout_s=600.0, wait_s=60.0, retry_safe=False, max_attempts=1,
        runtime_root=REPOSITORY, poll_s=0.0, queue_root=queue,
    )

    err = capsys.readouterr().err
    submitted = [line for line in err.splitlines() if "submitted" in line]
    job_id = submitted[0].split("slurm job ", 1)[1].split()[0]
    reported = [line for line in err.splitlines()
                if "could not" in line or "slurm job:" in line]
    assert any(job_id in line for line in reported), err


def test_the_lane_names_the_job_on_a_submission_record_it_cannot_write(
    tmp_path: Path, fleet: Path, unwritable, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The lane's own contract, checked where the id is known.

    ``submit`` writes both records after ``sbatch`` returned, so it is the only
    place that still holds the job id when the write fails. If it does not
    stamp the id on the failure, no caller can recover it.
    """

    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "exit:0")
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    action = _runnable_action(tmp_path, cas)
    request = cas.publish_action_request(action)
    key = str(action["action_key"])
    unwritable(sl.lane_directory(key) / "submissions")

    with pytest.raises(OSError) as caught:
        sl.submit(
            action, cas=cas, request_path=request,
            resources=sl.LaneResources.from_demand({"cpu": 1}, exclusive=False),
            timeout_s=None,
            worker_script=REPOSITORY / "tools" / "prismabuild_worker.py",
            job_entry=REPOSITORY / "tools" / "fleet" / "slurm_job.py",
        )

    assert str(getattr(caught.value, "job_id", "")).isdigit()


def test_a_failure_after_sbatch_accepted_is_not_reported_as_a_bad_tag(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The job is queued. Telling its submitter to fix the ``--tag`` sends them
    to change a submission the controller has already taken."""

    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "exit:0")
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    action = _runnable_action(tmp_path, cas)
    request = cas.publish_action_request(action)

    def refuse(path, payload) -> None:
        raise sl.SlurmLaneError(
            f"a different submission is already recorded at {path}")

    monkeypatch.setattr(sl, "_publish_record", refuse)

    code = pbrun.slurm_outcome(
        action, cas=cas, request_path=request, tags=["x86"],
        demand={"cpu": 1, "mem_gb": 4}, exclusive=False,
        timeout_s=600.0, wait_s=60.0, retry_safe=False, max_attempts=1,
        runtime_root=REPOSITORY, poll_s=0.0, queue_root=tmp_path / "queue",
    )

    assert code == pbrun.RECORD_WRITE_FAILED_EXIT
    err = capsys.readouterr().err
    assert "Fix the --tag" not in err
    assert "already recorded" in err
    assert "pbwait" in err


def test_a_refusal_before_sbatch_still_says_to_fix_the_tag(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The other half of the split. A refusal with no job behind it is a
    refusal, and the advice written for one is the right advice."""

    monkeypatch.setenv("FAKE_SBATCH_REFUSE", "1")
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    action = _runnable_action(tmp_path, cas)
    request = cas.publish_action_request(action)

    with pytest.raises(SystemExit) as caught:
        pbrun.slurm_outcome(
            action, cas=cas, request_path=request, tags=["x86"],
            demand={"cpu": 1, "mem_gb": 4}, exclusive=False,
            timeout_s=600.0, wait_s=60.0, retry_safe=False, max_attempts=1,
            runtime_root=REPOSITORY, poll_s=0.0, queue_root=tmp_path / "queue",
        )

    assert "Fix the --tag" in str(caught.value)
