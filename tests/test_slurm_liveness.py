"""Liveness in the SLURM lane: sampled evidence, reported stalls, no kills.

The policy under test (Rob, 2026-09-04): a job that is actively doing
something and not visibly dead is never killed on elapsed time.  So these
tests drive ``wait`` with a fake clock and fake scheduler binaries, and the
claims are about what the lane *records* and *reports* -- and about the one
thing it must never do, which is call ``scancel`` on any evidence at all.
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
from test_slurm_lane import (  # noqa: E402,F401
    REPOSITORY, _paper_action, _runnable_action, _submit, fleet,
)

import pbrun  # noqa: E402


class FakeClock:
    """A monotonic clock the injected ``sleep`` advances.

    Every poll's ``sleep(poll_s)`` moves time forward by ``poll_s`` and runs
    the test's own hook, which is how a test ends a RUNNING job or writes to
    its log at a chosen moment.
    """

    def __init__(self) -> None:
        self.now = 1000.0
        self.hooks: list = []

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += float(seconds)
        for hook in list(self.hooks):
            hook(self.now)


def _running_job(tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
                 seed: str = "live") -> sl.SubmittedJob:
    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "RUNNING")
    return _submit(tmp_path, resources=sl.LaneResources(), seed=seed)


def _finish_at(clock: FakeClock, fleet: Path, job: sl.SubmittedJob,
               when: float) -> None:
    def hook(now: float) -> None:
        if now >= clock.start + when:
            (fleet / f"{job.job_id}.state").write_text("COMPLETED|0:0\n")
    clock.start = clock.now
    clock.hooks.append(hook)


def _samples(job: sl.SubmittedJob) -> list[dict]:
    path = sl.liveness_path(job.directory)
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line]


# --------------------------------------------------------------------------
# The window is derived, and the pieces it is derived from are the measured ones
# --------------------------------------------------------------------------

def test_the_window_is_arithmetic_over_the_measured_floors() -> None:
    """69 s is issue #16's NFS stall; 30 s is JobAcctGatherFrequency's default.
    The window is the smallest number of cadences that clears the larger floor,
    plus the baseline cadence -- not a number somebody liked."""

    assert sl.NFS_STALL_FLOOR_S == 69.0
    assert sl.ACCT_GATHER_S == 30.0
    assert sl.LIVENESS_SAMPLE_S == sl.ACCT_GATHER_S
    assert sl.STALL_WINDOW_S == 120.0
    assert sl.STALL_WINDOW_S > sl.NFS_STALL_FLOOR_S + sl.LIVENESS_SAMPLE_S
    assert sl.STALL_REPORT_EVERY_S >= sl.STALL_WINDOW_S
    assert sl.LIVENESS_HISTORY * sl.LIVENESS_SAMPLE_S > sl.STALL_WINDOW_S


# --------------------------------------------------------------------------
# What wait records
# --------------------------------------------------------------------------

def test_a_progressing_job_is_sampled_at_the_cadence_and_never_reported(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_SSTAT_MODE", "progress")
    job = _running_job(tmp_path, monkeypatch)
    clock = FakeClock()
    _finish_at(clock, fleet, job, 400.0)
    reports: list[sl.StallReport] = []

    outcome = sl.wait(job, poll_s=5.0, sleep=clock.sleep, clock=clock,
                      on_stall=reports.append)

    assert outcome.state == "COMPLETED"
    assert reports == []
    assert not (fleet / "cancelled").exists()
    samples = _samples(job)
    # 400 s of RUNNING at a 30 s cadence: one baseline plus thirteen more,
    # not one per 5 s poll.
    assert 13 <= len(samples) <= 15
    assert samples[0]["progressing"] is None
    assert all(s["progressing"] is True for s in samples[1:])
    assert all(s["stalled_since"] is None for s in samples)
    assert all(s["evidence"] == ["sstat", "output"] for s in samples)
    assert samples[-1]["cpu_s"] > samples[0]["cpu_s"]
    # The sample is the scheduler's accounting, asked for by name.
    argv = (fleet / "sstat.argv").read_text().splitlines()[0]
    assert f"-j {job.job_id} -a -P -n --noconvert --format={sl.SSTAT_FORMAT}" in argv
    assert outcome.liveness["samples"] == len(samples)
    assert outcome.liveness["stalled_since"] is None
    assert outcome.liveness["latest"] == samples[-1]


def test_a_job_whose_samples_stop_moving_is_reported_and_not_cancelled(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The stall is reported after the window, repeated at the bound, and the
    job is left exactly where it was.  It ends because it ends."""

    monkeypatch.setenv("FAKE_SSTAT_MODE", "frozen")
    job = _running_job(tmp_path, monkeypatch)
    clock = FakeClock()
    _finish_at(clock, fleet, job, 1300.0)
    reports: list[tuple[float, sl.StallReport]] = []

    outcome = sl.wait(
        job, poll_s=5.0, sleep=clock.sleep, clock=clock,
        on_stall=lambda report: reports.append((clock.now, report)),
    )

    assert outcome.state == "COMPLETED"
    assert not (fleet / "cancelled").exists()
    assert (fleet / f"{job.job_id}.state").read_text().startswith("COMPLETED")
    # Baseline at t=0, first no-progress sample at t=30, window 120 s: the
    # first report lands at t=150 and the next one a report interval later.
    assert [when - 1000.0 for when, _ in reports] == [150.0, 750.0]
    first = reports[0][1]
    assert first.job_id == job.job_id
    assert first.action_key == job.action_key
    assert first.node == "sparky"
    assert first.stalled_for_s == 120.0
    assert first.samples_without_progress == 5     # t=30,60,90,120,150
    assert first.evidence == ("sstat", "output")
    assert first.path == sl.liveness_path(job.directory)
    samples = _samples(job)
    assert samples[0]["progressing"] is None
    assert all(s["progressing"] is False for s in samples[1:])
    assert samples[1]["stalled_since"] == samples[1]["unix"]
    assert all(s["stalled_since"] == samples[1]["unix"] for s in samples[1:])
    assert outcome.liveness["stalled_since"] == samples[1]["unix"]
    assert outcome.liveness["samples_without_progress"] == len(samples) - 1


def test_progress_resets_the_stall_clock(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A job that moves after a quiet spell is not still 'stalled since'
    the quiet spell.  ``sstat`` is frozen here; the log is what moves."""

    monkeypatch.setenv("FAKE_SSTAT_MODE", "frozen")
    job = _running_job(tmp_path, monkeypatch)
    clock = FakeClock()
    _finish_at(clock, fleet, job, 400.0)

    def write_at_200(now: float) -> None:
        if now >= clock.start + 200.0 and not job.stdout_path.exists():
            job.stdout_path.write_text("still here\n")
    clock.hooks.append(write_at_200)
    reports: list[sl.StallReport] = []

    outcome = sl.wait(job, poll_s=5.0, sleep=clock.sleep, clock=clock,
                      on_stall=reports.append)

    assert outcome.state == "COMPLETED"
    samples = _samples(job)
    moved = [s for s in samples if s["progressing"] is True]
    assert len(moved) == 1 and moved[0]["out_bytes"] == len("still here\n")
    assert moved[0]["stalled_since"] is None
    after = samples[samples.index(moved[0]) + 1:]
    assert after and all(s["stalled_since"] == after[0]["unix"] for s in after)
    assert after[0]["unix"] > samples[1]["unix"]
    # One report for the first quiet spell (t=150) and one for the second
    # (t=360, a full window after the write), each dated from its own start.
    assert len(reports) == 2
    assert reports[0].stalled_since == samples[1]["unix"]
    assert reports[1].stalled_since == after[0]["unix"]
    assert reports[1].stalled_for_s == sl.STALL_WINDOW_S


def test_the_liveness_file_is_append_only(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Issue #16 measured a 69 s NFS stall on a file rewritten every heartbeat.
    This file is only ever appended to: what was there stays byte-identical."""

    monkeypatch.setenv("FAKE_SSTAT_MODE", "progress")
    job = _running_job(tmp_path, monkeypatch)
    clock = FakeClock()
    _finish_at(clock, fleet, job, 300.0)
    path = sl.liveness_path(job.directory)
    seen: list[bytes] = []

    def snapshot(now: float) -> None:
        if path.exists():
            seen.append(path.read_bytes())
    clock.hooks.append(snapshot)

    sl.wait(job, poll_s=5.0, sleep=clock.sleep, clock=clock)

    final = path.read_bytes()
    assert seen and len({len(s) for s in seen}) > 1
    for earlier in seen:
        assert final.startswith(earlier)
    assert final.endswith(b"\n")
    assert all(json.loads(line) for line in final.decode().splitlines())


@pytest.mark.parametrize("how", ["fail", "absent"])
def test_without_sstat_the_evidence_is_the_logs_and_the_reason_is_recorded(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch, how: str
) -> None:
    """No accounting is a configuration fact, not a dead job: the sample says
    which evidence it has, and the log's growth still counts as progress."""

    monkeypatch.setenv("FAKE_SSTAT_MODE", "fail")
    sstat = "sstat" if how == "fail" else str(tmp_path / "no-such-sstat")
    job = _running_job(tmp_path, monkeypatch)
    clock = FakeClock()
    _finish_at(clock, fleet, job, 200.0)
    written = 0

    def grow(now: float) -> None:
        nonlocal written
        with job.stdout_path.open("a") as handle:
            handle.write("tick\n")
        written += 5
    clock.hooks.append(grow)
    reports: list[sl.StallReport] = []

    outcome = sl.wait(job, sstat=sstat, poll_s=5.0, sleep=clock.sleep,
                      clock=clock, on_stall=reports.append)

    assert outcome.state == "COMPLETED"
    assert reports == []
    samples = _samples(job)
    assert len(samples) >= 6
    assert all(s["evidence"] == ["output"] for s in samples)
    assert all(s["cpu_s"] is None and s["steps"] == [] for s in samples)
    if how == "fail":
        assert all("sstat exited 1" in s["sstat_error"] for s in samples)
        assert all("no steps running" in s["sstat_error"] for s in samples)
    else:
        assert all("sstat failed" in s["sstat_error"] for s in samples)
    assert all(s["progressing"] is True for s in samples[1:])
    assert samples[-1]["out_bytes"] > samples[0]["out_bytes"]
    assert outcome.liveness["latest"]["sstat_error"]


def test_a_pending_job_is_not_sampled(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Queue time is not a stall, and ``sstat`` has no steps to read."""

    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "PENDING")
    job = _submit(tmp_path, resources=sl.LaneResources())
    clock = FakeClock()
    outcome = sl.wait(job, poll_s=5.0, wait_s=300.0, sleep=clock.sleep,
                      clock=clock)
    assert outcome.state == sl.WAIT_TIMEOUT_STATE
    assert not (fleet / "sstat.argv").exists()
    assert _samples(job) == []
    assert outcome.liveness["samples"] == 0
    assert outcome.liveness["latest"] is None


def test_the_gpu_seam_claims_nothing() -> None:
    assert sl.gpu_power_sample("sparky") is None


def test_read_liveness_returns_the_last_line_without_reading_the_file(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_SSTAT_MODE", "frozen")
    job = _running_job(tmp_path, monkeypatch)
    root = Path(str(sl.lane_root()))
    assert sl.read_liveness(job.action_key, root=root) is None
    clock = FakeClock()
    _finish_at(clock, fleet, job, 200.0)
    sl.wait(job, poll_s=5.0, sleep=clock.sleep, clock=clock)

    found = sl.read_liveness(job.action_key, root=root)
    samples = _samples(job)
    assert found is not None
    assert found["latest"] == samples[-1]
    assert found["stalled_since"] == samples[1]["unix"]
    assert found["progressing"] is False
    assert found["job_id"] == job.job_id
    assert found["path"] == str(sl.liveness_path(job.directory))
    assert sl.read_liveness("0" * 64, root=root) is None
    assert sl.read_liveness("not-a-key", root=root) is None


def test_the_outcome_record_carries_the_liveness_summary(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_SSTAT_MODE", "frozen")
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    action = _paper_action(tmp_path, "recorded")
    request = cas.publish_action_request(action)
    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "RUNNING")
    clock = FakeClock()
    queue = tmp_path / "queue"

    def finish(now: float) -> None:
        if now >= 1200.0:
            for record in (tmp_path / "slurm-state").glob("*.state"):
                record.write_text("FAILED|1:0\n")
    clock.hooks.append(finish)

    result = sl.run(
        action, cas=cas, request_path=request, resources=sl.LaneResources(),
        timeout_s=None, worker_script=REPOSITORY / "tools" / "prismabuild_worker.py",
        job_entry=REPOSITORY / "tools" / "fleet" / "slurm_job.py",
        queue_root=queue, poll_s=5.0, sleep=clock.sleep, clock=clock,
    )
    job, outcome = result.last
    record = json.loads((queue / "failed" / f"{action['action_key']}.json").read_text())
    liveness = record["detail"]["liveness"]
    assert liveness["schema"] == sl.LIVENESS_SCHEMA_V1
    assert liveness == outcome.liveness
    assert liveness["latest"]["job_id"] == job.job_id
    assert liveness["stalled_since"] is not None
    assert liveness["latest"]["stalled_since"] == liveness["stalled_since"]
    assert liveness["path"] == str(sl.liveness_path(job.directory))
    assert liveness["window_s"] == sl.STALL_WINDOW_S


# --------------------------------------------------------------------------
# pbrun's side: one line to stderr, and nothing done to the job
# --------------------------------------------------------------------------

def test_pbrun_reports_a_stall_on_stderr_and_cancels_nothing(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("FAKE_SSTAT_MODE", "frozen")
    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "RUNNING")
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    action = _paper_action(tmp_path, "stalled")
    request = cas.publish_action_request(action)
    key = str(action["action_key"])
    clock = FakeClock()

    def finish(now: float) -> None:
        if now >= 1000.0 + 800.0:
            for record in fleet.glob("*.state"):
                record.write_text("COMPLETED|0:0\n")
    clock.hooks.append(finish)

    code = pbrun.slurm_outcome(
        action, cas=cas, request_path=request, tags=[],
        demand={"cpu": 1, "mem_gb": 4}, exclusive=False,
        timeout_s=None, wait_s=None, retry_safe=False, max_attempts=1,
        runtime_root=REPOSITORY, queue_root=tmp_path / "queue",
        poll_s=5.0, sleep=clock.sleep, clock=clock,
    )

    err = capsys.readouterr().err
    lines = [line for line in err.splitlines() if "has shown no progress" in line]
    # Reported at t=150 and repeated at t=750; finished at t=800.
    assert len(lines) == 2
    assert lines[0] == (
        f"pbrun: {key[:12]} slurm job 1000 has shown no progress for 2 min on "
        f"sparky; it is still running. Withdraw with pbrun --withdraw "
        f"{key[:12]} if it is dead."
    )
    assert "for 12 min on sparky" in lines[1]
    assert not (fleet / "cancelled").exists()
    assert code == 1                      # no receipt: a paper action ran nothing
    assert "slurm job 1000 exited 0 but published no receipt" in err
    record = json.loads((tmp_path / "queue" / "failed" / f"{key}.json").read_text())
    assert record["detail"]["liveness"]["stalled_since"] is not None


def test_a_withdrawal_carries_the_last_liveness_sample(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FAKE_SSTAT_MODE", "frozen")
    job = _running_job(tmp_path, monkeypatch, seed="withdrawn")
    clock = FakeClock()
    _finish_at(clock, fleet, job, 200.0)
    sl.wait(job, poll_s=5.0, sleep=clock.sleep, clock=clock)
    # Pretend the job is still running when the operator arrives.
    (fleet / f"{job.job_id}.state").write_text("RUNNING|0:0\n")
    queue = tmp_path / "queue"
    root = Path(str(sl.lane_root()))

    rc = pbrun.withdraw_slurm_main(
        [job.action_key[:12]], reason="dead", by="tester",
        lane_root=root, queue_root=queue,
    )

    assert rc == 0
    record = json.loads((queue / "failed" / f"{job.action_key}.json").read_text())
    assert record["status"] == "withdrawn"
    liveness = record["detail"]["liveness"]
    assert liveness["job_id"] == job.job_id
    assert liveness["stalled_since"] is not None
    assert liveness["latest"] == _samples(job)[-1]
