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
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import core as pb  # noqa: E402
from prismabuild import slurm_lane as sl  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_slurm_lane import REPOSITORY, _paper_action, _submit, fleet  # noqa: E402,F401
from test_slurm_liveness import FakeClock  # noqa: E402

import pbrun  # noqa: E402

WORKER = REPOSITORY / "tools" / "prismabuild_worker.py"
JOB_ENTRY = REPOSITORY / "tools" / "fleet" / "slurm_job.py"


def test_an_unreachable_controller_is_not_read_as_no_such_job(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch
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
        job, poll_s=5.0, sleep=clock.sleep, clock=clock,
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
    tmp_path: Path, fleet: Path
) -> None:
    """The one answer that *does* end the wait: ``Invalid job id specified``
    is the controller saying it has no such job."""

    job = _submit(tmp_path, resources=sl.LaneResources())
    for record in fleet.glob("*.state"):
        record.unlink()
    assert sl.wait(job, poll_s=0.0).state == sl.UNKNOWN_STATE


def test_the_wait_bound_still_holds_through_an_outage(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "RUNNING")
    monkeypatch.setenv("FAKE_CONTROLLER_DOWN", "1")
    job = _submit(tmp_path, resources=sl.LaneResources())
    clock = FakeClock()
    outcome = sl.wait(job, poll_s=5.0, wait_s=60.0, sleep=clock.sleep,
                      clock=clock)
    assert outcome.state == sl.WAIT_TIMEOUT_STATE
    assert clock.now - 1000.0 <= 70.0


@pytest.mark.parametrize("verdict", ["UNKNOWN", "WAIT_TIMEOUT"])
def test_no_ending_files_no_terminal_record(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch,
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
        queue_root=queue, poll_s=5.0, sleep=clock.sleep, clock=clock,
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
    record = json.loads((queue / "failed" / f"{key}.json").read_text())
    assert record["status"] == "withdrawn"

