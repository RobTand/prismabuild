"""The terminal records the SLURM lane files where pool consumers read them.

Eleven fleet tools and Tessera's ``merge_suite`` read one action's ending out of
``pb-queue/done/<key>.json`` or ``pb-queue/failed/<key>.json``.  The pull queue
writes those from ``PoolQueue.finish``, on the worker.  Under SLURM there is no
worker holding a claim, so the submitter writes them instead -- and if it does
not, every one of those readers goes blind on the day of the cutover.

What is asserted here is the record, not the scheduler: the directory it lands
in, the fields those consumers name, the generation rule that decides whether a
second writer may overwrite it, and the withdrawal marker that stops
``pool_reset`` from re-submitting a cancellation.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import textwrap
import uuid

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import core as pb  # noqa: E402
from prismabuild import pool  # noqa: E402
from prismabuild import slurm_lane as sl  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

import pbrun  # noqa: E402

from test_slurm_lane import (  # noqa: E402
    JOB_ENTRY,
    WORKER,
    _paper_action,
    _runnable_action,
    fleet,
)

__all__ = ["fleet"]


def _queue(tmp_path: Path) -> Path:
    root = tmp_path / "pb-queue"
    root.mkdir(parents=True, exist_ok=True)
    return root


def _record(queue_root: Path, state: str, key: str) -> dict:
    path = queue_root / state / f"{key}.json"
    assert path.exists(), f"no {state}/ record at {path}"
    return json.loads(path.read_text(encoding="utf-8"))


# --------------------------------------------------------------------------
# The three endings
# --------------------------------------------------------------------------

def test_a_job_that_published_a_receipt_is_filed_under_done(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``merge_suite --resume`` recovers an exit status nobody watched by
    reading exactly these fields out of ``done/``.  The receipt decides the
    status, not the exit code: that is the rule ``PoolQueue.finish`` applies,
    and the whole reason this transport can be swapped underneath it."""

    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "run")
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    action = _runnable_action(tmp_path, cas)
    request = cas.publish_action_request(action)
    queue_root = _queue(tmp_path)

    result = sl.run(
        action, cas=cas, request_path=request,
        placement=["x86"], resources=sl.LaneResources(cpus=2, memory_mib=2048),
        timeout_s=600.0, worker_script=WORKER, job_entry=JOB_ENTRY,
        queue_root=queue_root, poll_s=0.0,
        worker_python=sys.executable, job_python=sys.executable,
        local_checkout_root=tmp_path / "checkouts",
    )
    assert result.receipt is not None

    key = str(action["action_key"])
    assert not (queue_root / pool.FAILED / f"{key}.json").exists()
    record = _record(queue_root, pool.DONE, key)

    assert record["schema"] == sl.OUTCOME_SCHEMA_V1
    assert record["transport"] == "slurm"
    assert record["action_key"] == key
    assert record["status"] == "executed"
    assert record["attempts"] == 1
    assert record["max_attempts"] == 1
    assert record["retry_safe"] is False
    assert record["tags"] == ["x86"]
    assert record["resources"] == {"cpu": 2, "mem_gb": 2}
    assert isinstance(record["published_unix"], float)
    assert record["published_by"]
    assert "checkout_snapshot" in record or "checkout_root" in record

    # The fields merge_suite and pbrun read by name.
    detail = record["detail"]
    assert detail["status"] == "executed"
    assert detail["returncode"] == 0
    assert detail["receipt_published"] is True
    assert detail["result_digest"] == result.receipt.get("result_digest")
    assert isinstance(detail["stdout"], str)
    assert isinstance(detail["stderr"], str)
    assert detail["error"] is None
    assert record["claimed_by"] == result.attempts[0][0].job_id
    assert record["claimed_host"]
    assert record["finished_host"]
    assert isinstance(record["finished_unix"], float)

    slurm = detail["slurm"]
    assert slurm["job_id"] == result.attempts[0][0].job_id
    assert slurm["state"] == "COMPLETED"
    assert Path(slurm["submission_record_path"]).exists()
    assert slurm["stdout_path"].endswith(".out")
    assert slurm["stderr_path"].endswith(".err")


def test_a_job_that_published_no_receipt_is_filed_under_failed(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``pool_reset`` reads ``failed/`` to decide what to re-submit, and it
    reads ``checkout_root``/``resources``/``tags`` off the record to rebuild the
    submission.  A failure with no receipt belongs there whatever sbatch said."""

    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "exit:7")
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    action = _paper_action(tmp_path, "failed-record")
    request = cas.publish_action_request(action)
    queue_root = _queue(tmp_path)

    result = sl.run(
        action, cas=cas, request_path=request,
        placement=["gb10"], resources=sl.LaneResources(gpu_slots=1),
        timeout_s=600.0, worker_script=WORKER, job_entry=JOB_ENTRY,
        queue_root=queue_root, poll_s=0.0,
    )
    assert result.receipt is None

    key = str(action["action_key"])
    assert not (queue_root / pool.DONE / f"{key}.json").exists()
    record = _record(queue_root, pool.FAILED, key)
    assert record["status"] == "failed"
    assert record["detail"]["status"] == "failed"
    assert record["detail"]["returncode"] == 7
    assert record["detail"]["receipt_published"] is False
    assert record["detail"]["result_digest"] is None
    assert record["tags"] == ["gb10"]
    assert record["resources"]["gpu"] == 1
    assert record["detail"]["slurm"]["state"] == "FAILED"


def test_a_withdrawal_files_the_marker_pool_reset_reads(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``pool_reset`` skips a re-submission only on ``withdrawn_keys()`` or a
    top-level ``withdrawn_unix``.  Without both halves an operator's
    cancellation is re-submitted by the next bulk reset -- which the pool's own
    ``withdraw`` docstring calls a decision, not a defect."""

    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "PENDING")
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    action = _paper_action(tmp_path, "withdrawn-record")
    request = cas.publish_action_request(action)
    queue_root = _queue(tmp_path)
    key = str(action["action_key"])

    job = sl.submit(
        action, cas=cas, request_path=request, placement=["gb10"],
        resources=sl.LaneResources(gpu_slots=1), timeout_s=600.0,
        worker_script=WORKER, job_entry=JOB_ENTRY,
        published_unix=1757000000.0, published_by="sparky",
        retry_safe=False, max_attempts=1,
    )

    assert pbrun.withdraw_slurm_main(
        [key[:12]], reason="changed my mind", by="rob@sparky",
        queue_root=queue_root,
    ) == 0

    marker = _record(queue_root, pool.WITHDRAWN, key)
    assert marker["status"] == "withdrawn"
    assert marker["withdrawn_by"] == "rob@sparky"
    assert marker["reason"] == "changed my mind"
    assert isinstance(marker["withdrawn_unix"], float)
    assert marker["action_key"] == key
    assert key in pool.PoolQueue(queue_root).withdrawn_keys()

    outcome = _record(queue_root, pool.FAILED, key)
    assert outcome["status"] == "withdrawn"
    assert outcome["withdrawn_by"] == "rob@sparky"
    assert isinstance(outcome["withdrawn_unix"], float)
    assert outcome["reason"] == "changed my mind"
    assert outcome["detail"]["slurm"]["job_id"] == job.job_id


# --------------------------------------------------------------------------
# Generation, not permanence
# --------------------------------------------------------------------------

def test_the_first_writer_of_a_generation_keeps_the_record(
    tmp_path: Path, fleet: Path
) -> None:
    """An action key is a content hash, so the same key is re-submitted every
    time somebody asks for the same work again.  A record may therefore be
    replaced by a *later* generation and never by a second writer racing inside
    its own -- which is the pool's ``published_unix`` equality rule, not a
    filesystem lock."""

    queue_root = _queue(tmp_path)
    key = "b" * 64
    common = dict(
        queue_root=queue_root, action_key=key, published_by="sparky",
        status="failed", attempts=1, max_attempts=1, retry_safe=False,
        addressing={"checkout_root": "/mnt/shared/x86"},
        resources={"cpu": 1}, tags=["x86"],
    )

    first = sl.publish_outcome(published_unix=100.0, detail={"note": "first"},
                               **common)
    assert first is not None
    assert _record(queue_root, pool.FAILED, key)["detail"]["note"] == "first"

    # Same generation: the first writer's account stands.
    assert sl.publish_outcome(published_unix=100.0, detail={"note": "second"},
                              **common) is None
    assert _record(queue_root, pool.FAILED, key)["detail"]["note"] == "first"

    # A later generation is a new request for the same work, and replaces it.
    assert sl.publish_outcome(published_unix=200.0, detail={"note": "later"},
                              **common) is not None
    assert _record(queue_root, pool.FAILED, key)["detail"]["note"] == "later"


def test_a_new_submission_retires_the_withdrawal_it_supersedes(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``PoolQueue.publish`` moves a live withdrawal into
    ``withdrawn/superseded/`` for a reason it states at length: the marker
    stops a claim, so leaving it in place makes the re-submitted action
    unrunnable and the only remedy a hand edit of the live queue.  The lane
    submits without going through ``publish``, so it has to do the same thing
    itself or inherit the bug the pool already fixed."""

    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "exit:0")
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    action = _paper_action(tmp_path, "revived")
    request = cas.publish_action_request(action)
    queue_root = _queue(tmp_path)
    key = str(action["action_key"])

    (queue_root / pool.WITHDRAWN).mkdir(parents=True, exist_ok=True)
    (queue_root / pool.WITHDRAWN / f"{key}.json").write_text(json.dumps({
        "schema": pool.POOL_OUTCOME_SCHEMA_V1, "action_key": key,
        "status": "withdrawn", "withdrawn_unix": 1.0, "withdrawn_by": "rob",
        "reason": "an older decision",
    }), encoding="utf-8")

    sl.run(
        action, cas=cas, request_path=request,
        resources=sl.LaneResources(), timeout_s=600.0,
        worker_script=WORKER, job_entry=JOB_ENTRY,
        queue_root=queue_root, poll_s=0.0,
    )

    assert not (queue_root / pool.WITHDRAWN / f"{key}.json").exists()
    assert key not in pool.PoolQueue(queue_root).withdrawn_keys()
    kept = sorted((queue_root / pool.WITHDRAWN / "superseded").glob(f"{key}.*"))
    assert kept, "the decision must be kept, not deleted"
    assert json.loads(kept[0].read_text())["reason"] == "an older decision"


# --------------------------------------------------------------------------
# Provenance lifted from the scheduler
# --------------------------------------------------------------------------

def test_the_record_carries_what_sacct_said_about_the_job(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``claimed_host``/``finished_host`` are what ``merge_suite`` prints as the
    box that ran the arm, and ``elapsed_s`` is what it prints as the duration.
    Both come from the scheduler, so both are parsed from it rather than
    guessed from the submitter's own clock and hostname."""

    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "exit:0")
    monkeypatch.setenv("FAKE_SACCT_START", "2026-09-04T10:00:00")
    monkeypatch.setenv("FAKE_SACCT_END", "2026-09-04T10:15:30")
    monkeypatch.setenv("FAKE_SACCT_ELAPSED", "00:15:30")
    monkeypatch.setenv("FAKE_SACCT_NODELIST", "dl380g10")
    monkeypatch.setenv("FAKE_SACCT_PARTITION", "cpu")
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    action = _paper_action(tmp_path, "provenance")
    request = cas.publish_action_request(action)
    queue_root = _queue(tmp_path)

    sl.run(
        action, cas=cas, request_path=request,
        resources=sl.LaneResources(), timeout_s=600.0,
        worker_script=WORKER, job_entry=JOB_ENTRY,
        queue_root=queue_root, poll_s=0.0,
    )

    record = _record(queue_root, pool.FAILED, str(action["action_key"]))
    assert record["claimed_host"] == "dl380g10"
    assert record["finished_host"] == "dl380g10"
    assert record["detail"]["elapsed_s"] == 930.0
    assert record["detail"]["slurm"]["partition"] == "cpu"
    assert record["claimed_unix"] is not None
    assert record["finished_unix"] > record["claimed_unix"]


def test_an_unparseable_scheduler_answer_leaves_nulls_not_a_refusal(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A controller with no slurmdbd purges a job past ``MinJobAge`` and then
    nothing can say when it started.  That is a missing measurement, and a
    record that says so is worth more than no record at all."""

    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "exit:0")
    monkeypatch.setenv("FAKE_SACCT_START", "Unknown")
    monkeypatch.setenv("FAKE_SACCT_END", "Unknown")
    monkeypatch.setenv("FAKE_SACCT_ELAPSED", "INVALID")
    monkeypatch.setenv("FAKE_SACCT_NODELIST", "None assigned")
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    action = _paper_action(tmp_path, "no-provenance")
    request = cas.publish_action_request(action)
    queue_root = _queue(tmp_path)

    sl.run(
        action, cas=cas, request_path=request,
        resources=sl.LaneResources(), timeout_s=600.0,
        worker_script=WORKER, job_entry=JOB_ENTRY,
        queue_root=queue_root, poll_s=0.0,
    )

    record = _record(queue_root, pool.FAILED, str(action["action_key"]))
    assert record["claimed_unix"] is None
    assert record["claimed_host"] is None
    assert record["detail"]["elapsed_s"] is None
    # finished_unix and finished_host still have to be answerable, because
    # pbrun prints them and pool.terminal_outcome_covers reads the generation.
    assert isinstance(record["finished_unix"], float)


def test_a_long_job_log_is_tailed_rather_than_inlined_whole(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The pull queue holds an action's output as a string it already had in
    memory.  The lane holds a filename, and a build log on this fleet reaches
    hundreds of megabytes -- so it files a bounded tail and names the full file
    beside it, rather than turning one job into a JSON nothing can parse."""

    queue_root = _queue(tmp_path)
    directory = tmp_path / "lane" / ("c" * 64)
    directory.mkdir(parents=True)
    noisy = directory / "1000.out"
    noisy.write_text("x" * (sl.STREAM_TAIL_BYTES * 3), encoding="utf-8")

    tail = sl.read_stream_tail(noisy)
    assert len(tail.encode("utf-8")) <= sl.STREAM_TAIL_BYTES + 512
    assert tail.startswith("[truncated")
    assert sl.read_stream_tail(directory / "absent.out") == ""


# --------------------------------------------------------------------------
# The marker is read, not only written
# --------------------------------------------------------------------------

def test_a_job_slurm_killed_at_its_limit_files_the_pools_timeout_convention(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """SLURM reports a time-limit kill as ``ExitCode=0:15``.  Filed as
    ``returncode=0`` under ``failed/``, ``merge_suite`` (which scans
    ``failed/`` too and takes an integer 0 as an observed pass) called a
    scheduler-killed arm green.  The pool filed a timeout as
    ``status="timeout"``, ``returncode=None``; so does the lane now."""

    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "TIMEOUT")
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    action = _paper_action(tmp_path, "over-time-record")
    request = cas.publish_action_request(action)
    queue_root = _queue(tmp_path)
    key = str(action["action_key"])

    result = sl.run(
        action, cas=cas, request_path=request, placement=["x86"],
        resources=sl.LaneResources(), timeout_s=60.0,
        worker_script=WORKER, job_entry=JOB_ENTRY,
        queue_root=queue_root, poll_s=0.0,
    )
    assert result.receipt is None

    record = _record(queue_root, pool.FAILED, key)
    assert record["status"] == "failed"
    detail = record["detail"]
    assert detail["status"] == "timeout"
    assert detail["returncode"] is None
    assert detail["signal"] == 15
    assert detail["slurm"]["state"] == "TIMEOUT"


#: A sealed task that ends by itself, with a status no launcher would invent.
_EXITS_SEVEN = "raise SystemExit(7)\n"


def _run_the_job(
    request: Path, *, cas_root: Path, tmp_path: Path, lane_dir: Path, job_id: str
) -> subprocess.CompletedProcess:
    """Run the node's launcher the way the batch script does.

    Directly rather than through the fake ``sbatch``: the sidecar is named for
    the job, the launcher reads that id out of ``SLURM_JOB_ID``, and no fake
    can set that trio without also claiming a cgroup membership this box does
    not have (``core._collect_worker_evidence``). The lane's own tests drive
    the launcher this way for the same reason.
    """

    return subprocess.run(
        [sys.executable, str(JOB_ENTRY),
         "--action", str(request), "--cas-root", str(cas_root),
         "--worker", str(WORKER), "--worker-python", sys.executable,
         "--lane-dir", str(lane_dir), "--job-id", job_id,
         "--checkout-root", str(tmp_path / "materialized")],
        capture_output=True, text=True,
    )


def test_the_actions_own_exit_status_reaches_the_terminal_record(
    tmp_path: Path, fleet: Path
) -> None:
    """``returncode`` is the launcher's 1; ``action_returncode`` is the 7.

    The launcher exits 1 for every failure, so the record said 1 for an action
    that exited 7 and the 7 survived only as prose in a stderr tail. Both
    numbers are on the record now, each meaning what its name says.
    """

    cas_root = tmp_path / "cas"
    cas = pb.PrismaBuildCAS(cas_root)
    action = _runnable_action(tmp_path, cas, task_body=_EXITS_SEVEN)
    request = cas.publish_action_request(action)
    key = str(action["action_key"])
    lane_dir = sl.lane_directory(key)
    lane_dir.mkdir(parents=True)
    queue_root = _queue(tmp_path)

    completed = _run_the_job(
        request, cas_root=cas_root, tmp_path=tmp_path,
        lane_dir=lane_dir, job_id="4242",
    )
    assert completed.returncode == 1, completed.stderr
    assert json.loads(
        sl.action_status_path(lane_dir, "4242").read_text(encoding="utf-8")
    ) == {"action_returncode": 7}

    job = sl.SubmittedJob(
        action_key=key, job_id="4242", attempt=1, argv=[],
        script=lane_dir / "job.sh", directory=lane_dir,
        stdout_path=lane_dir / "4242.out", stderr_path=lane_dir / "4242.err",
        record_path=lane_dir / "submissions" / "1.json",
    )
    sl.publish_outcome(
        queue_root=queue_root, action_key=key, published_unix=100.0,
        published_by="sparky", status="failed", attempts=1, max_attempts=1,
        retry_safe=False, job=job,
        outcome=sl.Outcome(
            job_id="4242", state="FAILED", exit_code=1, signal=None,
            stdout_path=job.stdout_path, stderr_path=job.stderr_path,
        ),
    )

    detail = _record(queue_root, pool.FAILED, key)["detail"]
    assert detail["returncode"] == 1, "the launcher's own status is unchanged"
    assert detail["action_returncode"] == 7
    assert "action_signal" not in detail


def test_a_record_with_no_sidecar_carries_no_action_status(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Absent, not null: the field is the action's own ending or is not there."""

    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "exit:7")
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    action = _paper_action(tmp_path, "no-sidecar")
    request = cas.publish_action_request(action)
    queue_root = _queue(tmp_path)

    sl.run(
        action, cas=cas, request_path=request, placement=[],
        resources=sl.LaneResources(), timeout_s=600.0,
        worker_script=WORKER, job_entry=JOB_ENTRY,
        queue_root=queue_root, poll_s=0.0,
    )

    detail = _record(queue_root, pool.FAILED, str(action["action_key"]))["detail"]
    assert detail["returncode"] == 7
    assert "action_returncode" not in detail
    assert "action_signal" not in detail


def test_a_resumed_ending_carries_the_action_status_too(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A detached run's ending is filed by whoever waits, and is one shape.

    ``pbwait`` reaches ``_file_ending`` through ``resume`` rather than through
    ``run``, and a resumed record missing a field would be a second shape none
    of the readers expects.
    """

    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "exit:1")
    cas_root = tmp_path / "cas"
    cas = pb.PrismaBuildCAS(cas_root)
    action = _runnable_action(tmp_path, cas, task_body=_EXITS_SEVEN)
    request = cas.publish_action_request(action)
    key = str(action["action_key"])
    queue_root = _queue(tmp_path)

    job = sl.submit(
        action, cas=cas, request_path=request, placement=[],
        resources=sl.LaneResources(), timeout_s=600.0,
        worker_script=WORKER, job_entry=JOB_ENTRY,
        worker_python=sys.executable, job_python=sys.executable,
        local_checkout_root=tmp_path / "checkouts",
    )
    # The scheduler said it started the job and this is the job: the launcher
    # run the fake did not run.
    assert _run_the_job(
        request, cas_root=cas_root, tmp_path=tmp_path,
        lane_dir=job.directory, job_id=job.job_id,
    ).returncode == 1

    submission = sl.recorded_submission(key)
    assert submission is not None
    sl.resume(
        submission, action=action, cas=cas, queue_root=queue_root,
        wait_s=0.0, poll_s=0.0,
    )

    detail = _record(queue_root, pool.FAILED, key)["detail"]
    assert detail["returncode"] == 1
    assert detail["action_returncode"] == 7


def test_a_malformed_sidecar_leaves_the_record_as_it_was(
    tmp_path: Path, fleet: Path
) -> None:
    """A diagnostic that cannot be read is dropped, never guessed at."""

    directory = tmp_path / "lane" / ("c" * 64)
    directory.mkdir(parents=True)
    sidecar = sl.action_status_path(directory, "77")
    for text in ("{", "[]", '{"action_returncode": "7"}', '{"action_signal": 9}'):
        sidecar.write_text(text, encoding="utf-8")
        assert sl.read_action_status(sidecar) == {}
    assert sl.read_action_status(directory / "nothing.json") == {}
    sidecar.write_text(
        '{"action_returncode": -9, "action_signal": 9}', encoding="utf-8"
    )
    assert sl.read_action_status(sidecar) == {
        "action_returncode": -9, "action_signal": 9,
    }


def test_detail_returncode_follows_the_pull_queues_convention() -> None:
    """A signalled job carries the negative signal, as ``subprocess`` reports
    a signalled child and as the pool's records therefore carried it; a plain
    exit carries its code; a timeout carries ``None`` behind ``timeout``."""

    def outcome(state: str, code: int | None, signal: int | None) -> sl.Outcome:
        return sl.Outcome(job_id="1", state=state, exit_code=code, signal=signal,
                          stdout_path=None, stderr_path=None)

    assert sl.detail_status_and_returncode(
        "executed", outcome("COMPLETED", 0, None)) == ("executed", 0)
    assert sl.detail_status_and_returncode(
        "failed", outcome("FAILED", 1, None)) == ("failed", 1)
    assert sl.detail_status_and_returncode(
        "failed", outcome("OUT_OF_MEMORY", 0, 9)) == ("failed", -9)
    assert sl.detail_status_and_returncode(
        "failed", outcome("TIMEOUT", 0, 15)) == ("timeout", None)
    assert sl.detail_status_and_returncode(
        "withdrawn", outcome("CANCELLED", 0, 15)) == ("withdrawn", -15)
    assert sl.detail_status_and_returncode("failed", None) == ("failed", None)


def test_withdraw_without_a_named_transport_still_finds_the_slurm_job(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The submission record decides the transport.  Before, an operator whose
    shell did not name SLURM sent the prefix to the pull queue, which found
    nothing and left the job running with exit status 2."""

    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "RUNNING")
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    action = _paper_action(tmp_path, "routed-withdrawal")
    request = cas.publish_action_request(action)
    queue_root = _queue(tmp_path)
    key = str(action["action_key"])
    job = sl.submit(
        action, cas=cas, request_path=request, placement=["gb10"],
        resources=sl.LaneResources(gpu_slots=1), timeout_s=600.0,
        worker_script=WORKER, job_entry=JOB_ENTRY,
        published_unix=1757000000.0, published_by="sparky",
        retry_safe=False, max_attempts=1,
    )

    assert pbrun.withdraw_routed(
        [key[:12]], transport="pool", reason="routed", by="rob@sparky",
        queue_root=queue_root,
    ) == 0
    cancelled = (Path(os.environ["FAKE_SLURM_STATE"]) / "cancelled").read_text()
    assert job.job_id in cancelled.split()
    assert _record(queue_root, pool.WITHDRAWN, key)["status"] == "withdrawn"

    # A prefix nobody recorded on the lane still goes to the pull queue, which
    # says so in its own words.
    assert pbrun.withdraw_routed(
        ["deadbeef0000"], transport="pool", reason="", by="rob@sparky",
        queue=pool.PoolQueue(queue_root),
    ) == 2


def _withdraw_mid_flight(queue_root: Path, job) -> None:
    """File a marker the way ``pbrun --withdraw`` files one, while the job runs.

    ``on_submit`` fires after the submission record exists and before ``wait``,
    which is exactly the window in which ``scancel`` finds nothing left to
    cancel.
    """

    submission = json.loads(job.record_path.read_text(encoding="utf-8"))
    sl.publish_withdrawal(
        queue_root=queue_root, action_key=job.action_key,
        reason="wrong branch", by="rob@sparky", submission=submission,
    )


def test_a_job_that_finishes_before_scancel_lands_is_still_a_withdrawal(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The marker outranks what the job went on to do.

    ``--withdraw`` writes the marker, then ``scancel`` reports that the job may
    already have finished -- and it had, with a receipt.  If the submitter files
    its own ``executed`` account anyway, one generation carries a ``withdrawn/``
    marker, a ``failed/`` record and a ``done/`` record at once: ``merge_suite``
    cannot resolve the double match and ``reclaim_terminal_reservation``
    refuses on two terminals.  The pool avoids this by reading the marker
    before every finish; so does this.
    """

    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "run")
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    action = _runnable_action(tmp_path, cas)
    request = cas.publish_action_request(action)
    queue_root = _queue(tmp_path)
    key = str(action["action_key"])

    result = sl.run(
        action, cas=cas, request_path=request,
        resources=sl.LaneResources(cpus=1, memory_mib=1024), timeout_s=600.0,
        worker_script=WORKER, job_entry=JOB_ENTRY,
        queue_root=queue_root, poll_s=0.0,
        worker_python=sys.executable, job_python=sys.executable,
        local_checkout_root=tmp_path / "checkouts",
        on_submit=lambda job: _withdraw_mid_flight(queue_root, job),
    )
    assert result.receipt is not None          # the work really did happen

    assert not (queue_root / pool.DONE / f"{key}.json").exists()
    record = _record(queue_root, pool.FAILED, key)
    assert record["status"] == "withdrawn"
    assert record["withdrawn_by"] == "rob@sparky"
    assert record["reason"] == "wrong branch"
    # Honest about the receipt even so: the work was done, the request was not
    # wanted, and both halves are on the record.
    assert record["detail"]["receipt_published"] is True


def test_a_withdrawal_between_attempts_stops_the_next_one(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A retry-safe run whose attempt fails inside the same window would
    otherwise submit again and carry the cancelled action to completion.  The
    pull queue checks ``withdrawal_covers`` before every claim; the lane checks
    it before every resubmission, which is the same guarantee at its own
    granularity."""

    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "exit:7")
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    action = _paper_action(tmp_path, "withdrawn-mid-retry")
    request = cas.publish_action_request(action)
    queue_root = _queue(tmp_path)

    result = sl.run(
        action, cas=cas, request_path=request,
        resources=sl.LaneResources(), timeout_s=600.0,
        worker_script=WORKER, job_entry=JOB_ENTRY,
        retry_safe=True, max_attempts=3,
        queue_root=queue_root, poll_s=0.0,
        on_submit=lambda job: _withdraw_mid_flight(queue_root, job),
    )

    assert len(result.attempts) == 1
    submissions = result.attempts[0][0].directory / "submissions"
    # One record, and its name ends in the attempt: the generation in front of
    # it is what keeps a second run of this key from colliding with this one.
    names = sorted(x.name for x in submissions.iterdir())
    assert len(names) == 1 and names[0].endswith("-001.json"), names
    record = _record(queue_root, pool.FAILED, str(action["action_key"]))
    assert record["status"] == "withdrawn"
    assert record["attempts"] == 1


def test_the_record_says_whether_the_job_had_the_whole_device(
    tmp_path: Path, fleet: Path
) -> None:
    """``resources`` cannot answer it, so ``detail.slurm.gres`` does.

    ``LaneResources.demand()`` files the producer's own vocabulary, and
    ``{"gpu": 1}`` is the same claim for an action that had the device to
    itself and one that took a single sharable slot.  ``pool_reset`` rebuilds
    a submission out of that dictionary, so without the GRES it re-emitted an
    exclusive action as ``shard:1`` -- retried beside other work, which is the
    one thing ``--exclusive`` was asking not to happen.
    """

    queue_root = _queue(tmp_path)
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    filed = {}
    for name, resources in (
        ("whole", sl.LaneResources(gpu_slots=1, exclusive_gpu=True)),
        ("slot", sl.LaneResources(gpu_slots=1)),
        ("none", sl.LaneResources()),
    ):
        action = _paper_action(tmp_path, name)
        job = sl.submit(
            action, cas=cas, request_path=cas.publish_action_request(action),
            resources=resources, timeout_s=None, worker_script=WORKER,
            job_entry=JOB_ENTRY,
        )
        sl.publish_outcome(
            queue_root=queue_root, action_key=job.action_key,
            published_unix=1.0, published_by="sparky", status="failed",
            attempts=1, max_attempts=1, retry_safe=False,
            resources=resources.demand(), job=job,
        )
        filed[name] = _record(
            queue_root, pool.FAILED, job.action_key)["detail"]["slurm"]["gres"]

    assert filed == {"whole": "gpu:1", "slot": "shard:1", "none": None}
