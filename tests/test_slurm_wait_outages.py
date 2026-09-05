"""What ``wait`` does when the scheduler cannot be asked, or answers nothing.

Two defects from the SchedMD-docs review of 2026-09-05, both in the poll
loop, both with the job running on unaffected while the submitter drew the
wrong conclusion:

* every reader answered ``None`` on *any* non-zero exit, so a controller that
  could not be reached (``systemctl restart slurmctld``) read as "no such
  job", ``wait`` returned ``UNKNOWN`` on that one poll, ``run`` stopped, and
  ``_file_ending`` filed ``failed/<key>.json`` -- first-writer-wins per
  generation, so the receipt the job published minutes later never reached
  ``done/``;
* ``_run`` raises ``SlurmLaneError`` when a command hangs past
  ``COMMAND_TIMEOUT_S``, and ``wait`` let it propagate into pbrun's
  "sbatch refused this action ... fix the --tag" handler.

Both fakes are the shared fixture's, switched by environment variable from a
hook on the fake clock, so the outage begins and ends at chosen moments.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import core as pb  # noqa: E402
from prismabuild import slurm_lane as sl  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_slurm_lane import REPOSITORY, _paper_action, _submit, fleet  # noqa: E402,F401
from test_slurm_liveness import FakeClock, FakeScheduler, scheduler  # noqa: E402,F401

import pbrun  # noqa: E402

WORKER = REPOSITORY / "tools" / "prismabuild_worker.py"
JOB_ENTRY = REPOSITORY / "tools" / "fleet" / "slurm_job.py"


def test_an_unreachable_controller_is_not_read_as_no_such_job(
    tmp_path: Path, fleet: Path, scheduler: FakeScheduler, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Pre-fix: ``wait`` returned ``UNKNOWN`` on the first poll of the outage
    (``assert outcome.state == "COMPLETED"`` failed with ``'UNKNOWN'``).
    Now it keeps polling, says so once, and reads the ending when the
    controller is back."""

    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "RUNNING")
    # The fleet's configuration: no slurmdbd, so sacct is inert and the
    # controller is what scontrol and squeue have to reach.
    monkeypatch.setenv("FAKE_SACCT_DISABLED", "1")
    job = _submit(tmp_path, resources=sl.LaneResources())
    clock = FakeClock()
    start = clock.now

    def outage(now: float) -> None:
        if start + 10 <= now < start + 700:
            monkeypatch.setenv("FAKE_CONTROLLER_DOWN", "1")
        else:
            monkeypatch.delenv("FAKE_CONTROLLER_DOWN", raising=False)
        if now >= start + 700:
            (fleet / f"{job.job_id}.state").write_text("COMPLETED|0:0\n")
    clock.hooks.append(outage)
    notices: list[tuple[float, str]] = []

    outcome = sl.wait(
        job, **scheduler.commands, poll_s=5.0, sleep=clock.sleep, clock=clock,
        on_notice=lambda text: notices.append((clock.now - start, text)),
    )

    assert outcome.state == "COMPLETED"
    assert not (fleet / "cancelled").exists()
    said = [text for _, text in notices]
    assert said[0].startswith("slurm job 1000: the scheduler could not be asked")
    assert "Unable to contact slurm controller" in said[0]
    assert "the job is not affected" in said[0]
    assert said[-1].startswith("slurm job 1000: the scheduler answers again")
    # Once, then at most every NOTICE_EVERY_S through a 690 s outage, then
    # the recovery line: 3 + 1, not one per 5 s poll.
    assert len(said) == 4
    assert [when for when, _ in notices][:3] == [10.0, 310.0, 610.0]


def test_the_controller_answering_no_such_job_is_still_unknown(
    tmp_path: Path, fleet: Path, scheduler: FakeScheduler
) -> None:
    """The one answer that *does* end the wait: ``Invalid job id specified``
    is the controller saying it has no such job."""

    job = _submit(tmp_path, resources=sl.LaneResources())
    for record in fleet.glob("*.state"):
        record.unlink()
    assert sl.wait(job, **scheduler.commands, poll_s=0.0).state == sl.UNKNOWN_STATE


def test_the_wait_bound_still_holds_through_an_outage(
    tmp_path: Path, fleet: Path, scheduler: FakeScheduler, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "RUNNING")
    monkeypatch.setenv("FAKE_CONTROLLER_DOWN", "1")
    job = _submit(tmp_path, resources=sl.LaneResources())
    clock = FakeClock()
    outcome = sl.wait(job, **scheduler.commands, poll_s=5.0, wait_s=60.0, sleep=clock.sleep,
                      clock=clock)
    assert outcome.state == sl.WAIT_TIMEOUT_STATE
    assert clock.now - 1000.0 <= 70.0


@pytest.mark.parametrize("verdict", ["UNKNOWN", "WAIT_TIMEOUT"])
def test_no_ending_files_no_terminal_record(
    tmp_path: Path, fleet: Path, scheduler: FakeScheduler, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str], verdict: str,
) -> None:
    """Pre-fix: ``run`` filed ``failed/<key>.json`` with ``status=failed`` for
    a job the scheduler had merely forgotten (``assert not filed.exists()``
    failed).  First-writer-wins per generation then kept the receipt's
    ``done/`` record out forever.  Now nothing is filed, and pbrun says the
    job may still be running, with the gave-up exit rather than a failure."""

    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "RUNNING")
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    action = _paper_action(tmp_path, f"forgotten-{verdict}")
    request = cas.publish_action_request(action)
    key = str(action["action_key"])
    clock = FakeClock()
    queue = tmp_path / "queue"

    def forget(now: float) -> None:
        if verdict == "UNKNOWN" and now >= 1000.0 + 20.0:
            for record in fleet.glob("*.state"):
                record.unlink()
    clock.hooks.append(forget)

    code = pbrun.slurm_outcome(
        action, cas=cas, request_path=request, tags=[],
        demand={"cpu": 1, "mem_gb": 4}, exclusive=False,
        timeout_s=None, wait_s=(None if verdict == "UNKNOWN" else 30.0),
        retry_safe=False, max_attempts=1, runtime_root=REPOSITORY,
        queue_root=queue, **scheduler.commands, poll_s=5.0, sleep=clock.sleep, clock=clock,
    )

    assert code == pbrun.GAVE_UP_EXIT
    for state in ("done", "failed", "withdrawn"):
        assert not (queue / state / f"{key}.json").exists()
    assert not (fleet / "cancelled").exists()
    err = capsys.readouterr().err
    if verdict == "UNKNOWN":
        assert "no scheduler command can describe slurm job 1000" in err
        assert "may still be running as slurm job 1000" in err
        assert f"look under {sl.lane_directory(key)}" in err
    else:
        assert "gave up waiting" in err

    # The operator the message points at can still stop the job: with no
    # record filed for this generation, the withdrawal reaches ``scancel``.
    # Pre-fix, ``failed/<key>.json`` existed and withdraw_slurm_main said
    # "already has an outcome filed" and never cancelled anything.
    rc = pbrun.withdraw_slurm_main(
        [key[:12]], reason="dead", by="tester",
        lane_root=Path(str(sl.lane_root())), queue_root=queue,
    )
    assert rc == 0
    assert (fleet / "cancelled").read_text().split() == ["1000"]
    assert "already has an outcome filed" not in capsys.readouterr().err
    record = json.loads((queue / "withdrawn" / f"{key}.json").read_text())
    assert record["status"] == "withdrawn"


def test_wait_s_is_one_budget_across_retries(
    tmp_path: Path, fleet: Path, scheduler: FakeScheduler, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pre-fix: ``run`` handed the whole ``wait_s`` to every attempt's
    ``wait``, so with ``--wait-s 60`` a first attempt that failed at 40 s let
    the second wait until 100 s (``assert elapsed <= 70`` failed with
    ``elapsed == 105.0``).  The pool holds one deadline across retries
    (``pbrun.await_outcome``); the lane now does the same."""

    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "RUNNING")
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    action = _paper_action(tmp_path, "one-budget")
    request = cas.publish_action_request(action)
    clock = FakeClock()
    start = clock.now

    def fail_first_attempt(now: float) -> None:
        if now >= start + 40.0:
            (fleet / "1000.state").write_text("FAILED|7:0\n")
    clock.hooks.append(fail_first_attempt)

    result = sl.run(
        action, cas=cas, request_path=request,
        resources=sl.LaneResources(), timeout_s=None,
        worker_script=WORKER, job_entry=JOB_ENTRY,
        retry_safe=True, max_attempts=3, **scheduler.commands, poll_s=5.0, wait_s=60.0,
        sleep=clock.sleep, clock=clock,
    )

    elapsed = clock.now - start
    assert [job.attempt for job, _ in result.attempts] == [1, 2]
    assert result.attempts[0][1].state == "FAILED"
    assert result.attempts[1][1].state == sl.WAIT_TIMEOUT_STATE
    assert elapsed <= 60.0 + 2 * 5.0, elapsed


def test_a_receipt_still_wins_when_the_scheduler_has_forgotten_the_job(
    tmp_path: Path, fleet: Path, scheduler: FakeScheduler, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Purged past MinJobAge *with* a receipt is an execution, filed as one."""

    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "RUNNING")
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    action = _paper_action(tmp_path, "purged-with-receipt")
    request = cas.publish_action_request(action)
    key = str(action["action_key"])
    clock = FakeClock()
    queue = tmp_path / "queue"
    receipt_root = cas.root / "receipts" / key[:2]

    def forget(now: float) -> None:
        if now >= 1000.0 + 20.0:
            for record in fleet.glob("*.state"):
                record.unlink()
            receipt_root.mkdir(parents=True, exist_ok=True)
            monkeypatch.setattr(
                cas, "lookup", lambda _a: {"result_digest": "sha256:abc"})
    clock.hooks.append(forget)

    result = sl.run(
        action, cas=cas, request_path=request, resources=sl.LaneResources(),
        timeout_s=None, worker_script=WORKER, job_entry=JOB_ENTRY,
        queue_root=queue, **scheduler.commands, poll_s=5.0, sleep=clock.sleep, clock=clock,
    )
    assert result.last[1].state == sl.UNKNOWN_STATE
    record = json.loads((queue / "done" / f"{key}.json").read_text())
    assert record["status"] == "executed"


def test_a_hung_scheduler_command_does_not_end_the_wait(
    tmp_path: Path, fleet: Path, scheduler: FakeScheduler, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Pre-fix: ``SlurmLaneError('scontrol failed: Command ... timed out
    after 0.2 seconds')`` propagated out of ``wait`` and ``slurm_outcome``
    raised ``SystemExit('pbrun: slurm refused this action ... Fix the --tag,
    ...')`` while the job ran on.  Now the timeout is one unanswered poll.

    Both controller clients hang here, because since issue #67 one hung
    command is not an unanswered poll: ``query_provenance`` asks the next
    reader, and an outage is reported only when none of the three can
    establish the state.  A slurmctld that hangs ``scontrol`` hangs ``squeue``
    with it, and ``sacct`` is inert on this fleet.  The first failure is what
    the notice names, which is still the hung ``scontrol``.
    """

    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "RUNNING")
    monkeypatch.setenv("FAKE_SACCT_DISABLED", "1")
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    action = _paper_action(tmp_path, "hung")
    request = cas.publish_action_request(action)
    key = str(action["action_key"])
    clock = FakeClock()
    polls = 0

    def hang_then_finish(now: float) -> None:
        nonlocal polls
        polls += 1
        if polls <= 2:
            monkeypatch.setenv("FAKE_SCONTROL_HANG", "2")
        else:
            monkeypatch.delenv("FAKE_SCONTROL_HANG", raising=False)
        if polls >= 4:
            for record in fleet.glob("*.state"):
                record.write_text("COMPLETED|0:0\n")
    monkeypatch.setenv("FAKE_SCONTROL_HANG", "2")
    clock.hooks.append(hang_then_finish)

    def hanging_squeue(argv):
        hang = os.environ.get("FAKE_SCONTROL_HANG")
        if hang:
            raise subprocess.TimeoutExpired(["squeue", *argv], float(hang))
        return scheduler.squeue(argv)

    code = pbrun.slurm_outcome(
        action, cas=cas, request_path=request, tags=[],
        demand={"cpu": 1, "mem_gb": 4}, exclusive=False,
        timeout_s=None, wait_s=None, retry_safe=False, max_attempts=1,
        runtime_root=REPOSITORY, queue_root=tmp_path / "queue",
        **scheduler.commands_with(squeue=hanging_squeue),
        poll_s=5.0, sleep=clock.sleep, clock=clock,
    )

    err = capsys.readouterr().err
    assert "slurm refused this action" not in err
    assert "the scheduler could not be asked (scontrol failed" in err
    assert "timed out" in err
    assert "the scheduler answers again" in err
    assert "attempt 1/1 slurm job 1000 COMPLETED" in err
    assert code == 1                      # a paper action publishes no receipt
    assert not (fleet / "cancelled").exists()
    assert (tmp_path / "queue" / "failed" / f"{key}.json").exists()
