"""The lane sweeper: endings nobody asked about, filed anyway.

The lane files an ending only when somebody polls for that key, so a job that
ends while nothing is watching leaves its verdict in the controller's
accounting and never becomes a record. Every reader of ``pb-queue`` then sees
an action that never finished, and ``pbcampaign``'s recovery is to re-run its
manifest.

Four properties are pinned here, and each is a way the sweeper could be wrong
rather than merely absent:

*It files what a waiter would have filed.* The record has to be the execution's
own -- job id, elapsed time, receipt -- and not the ``cache_hit`` a re-run
produces. That is what makes a swept ending worth having.

*It never invents a verdict.* A job the controller cannot account for, with no
receipt behind it, files nothing. A ``failed`` filed on ignorance would stand:
``publish_outcome`` is first-writer-wins per generation.

*A receipt outranks the scheduler.* The same job, once the CAS holds its
receipt, files ``executed`` even though the controller has forgotten it. This
is the lane's standing doctrine and the sweeper does not get its own.

*It is safe beside a live poller.* Two writers of one key produce one record
and neither raises.

Everything runs against the fake scheduler in ``test_slurm_lane``: no
controller is contacted, and no test writes outside ``tmp_path``.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import sys
import threading

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import core as pb  # noqa: E402
from prismabuild import pool  # noqa: E402
from prismabuild import slurm_lane as sl  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

import pbrun  # noqa: E402

from test_slurm_lane import (  # noqa: E402
    JOB_ENTRY, WORKER, _runnable_action, fleet,  # noqa: F401
)


def _detach(tmp_path: Path, *, queue_root: Path, cas: pb.PrismaBuildCAS,
            action: dict) -> sl.SubmittedJob:
    """Submit one action with nobody waiting for it, as ``pbrun --detach`` does.

    The fake ``sbatch`` runs the script synchronously, so by the time this
    returns the job has completed, published its receipt, and left no terminal
    record: exactly the state an operator finds after a waiter died.
    """

    request = cas.publish_action_request(action)
    result = sl.run(
        action, cas=cas, request_path=request,
        resources=sl.LaneResources.from_demand({"cpu": 1, "mem_gb": 1}),
        timeout_s=None, worker_script=WORKER, job_entry=JOB_ENTRY,
        queue_root=queue_root, local_checkout_root=tmp_path / "checkouts",
        detach=True,
    )
    job, _ = result.last
    return job


def _terminal(queue_root: Path, key: str) -> tuple[str, dict] | None:
    """The ending filed for this key under any of the three states."""

    for state in (pool.DONE, pool.FAILED, pool.WITHDRAWN):
        path = queue_root / state / f"{key}.json"
        if path.exists():
            return state, json.loads(path.read_text(encoding="utf-8"))
    return None


def test_a_job_that_ends_with_nobody_watching_is_filed_by_the_sweeper(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch,  # noqa: F811
) -> None:
    """The whole debt item, end to end.

    Detach a real job, let it run to completion and publish its receipt, and
    show that no ending exists. Sweep, and the ending appears -- as the
    execution's own record, with the job id and the receipt on it, not as the
    ``cache_hit`` that re-running the work would have produced. A poll
    afterwards reads that record instead of resuming anything.
    """

    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "run")
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    queue_root = tmp_path / "pb-queue"
    action = _runnable_action(tmp_path, cas)
    job = _detach(tmp_path, queue_root=queue_root, cas=cas, action=action)
    key = job.action_key

    # The work is done and nothing filed an ending for it.
    assert cas.lookup(action) is not None
    assert _terminal(queue_root, key) is None

    report = sl.sweep(cas=cas, queue_root=queue_root)
    assert [row["disposition"] for row in report] == [sl.MISSING]
    assert report[0]["status"] == "executed"
    # A report writes nothing.
    assert _terminal(queue_root, key) is None

    applied = sl.sweep(cas=cas, queue_root=queue_root, apply=True)
    assert [row["disposition"] for row in applied] == [sl.SWEPT]

    filed = _terminal(queue_root, key)
    assert filed is not None
    state, record = filed
    assert state == pool.DONE
    assert record["status"] == "executed"
    assert record["transport"] == "slurm"
    assert record["detail"]["receipt_published"] is True
    assert record["detail"]["slurm"]["job_id"] == job.job_id

    # And a wait for the key now reads that record rather than resuming a job
    # or re-running the work.
    landed = pbrun.landed_outcome(
        pool.PoolQueue(queue_root), key, wait_s=0.0,
        generation=record["published_unix"])
    assert landed is not None
    assert landed[1]["status"] == "executed"

    # A second sweep has nothing left to do and files nothing new.
    again = sl.sweep(cas=cas, queue_root=queue_root, apply=True)
    assert [row["disposition"] for row in again] == [sl.ALREADY_FILED]


def test_a_job_the_controller_cannot_account_for_files_no_ending(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch,  # noqa: F811
) -> None:
    """Requirement four: not knowing is not knowing it failed.

    The job is purged from the controller and the CAS holds no receipt, so
    there is no recorded verdict anywhere. The sweeper says so and writes
    nothing. Filing ``failed`` here would stand for good: ``publish_outcome``
    is first-writer-wins per generation, so the receipt a slow job publishes
    afterwards could never reach ``done/``.
    """

    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    queue_root = tmp_path / "pb-queue"
    action = _runnable_action(tmp_path, cas)
    # Accepted, never run: no receipt, and the controller is about to forget it.
    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "PENDING")
    job = _detach(tmp_path, queue_root=queue_root, cas=cas, action=action)
    (fleet / f"{job.job_id}.state").unlink()
    assert cas.lookup(action) is None

    rows = sl.sweep(cas=cas, queue_root=queue_root, apply=True)

    assert [row["disposition"] for row in rows] == [sl.NO_VERDICT]
    assert rows[0]["status"] is None
    assert _terminal(queue_root, job.action_key) is None


def test_a_forgotten_job_with_a_receipt_is_filed_as_executed(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch,  # noqa: F811
) -> None:
    """The CAS outranks the scheduler, in the sweeper as in ``pbwait``.

    Same purged job as above, except that the work was done and the receipt is
    in the CAS. A receipt is a recorded verdict, so the ending is filed -- and
    it records that the scheduler could say nothing, rather than hiding it.
    """

    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "run")
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    queue_root = tmp_path / "pb-queue"
    action = _runnable_action(tmp_path, cas)
    job = _detach(tmp_path, queue_root=queue_root, cas=cas, action=action)
    assert cas.lookup(action) is not None
    (fleet / f"{job.job_id}.state").unlink()

    rows = sl.sweep(cas=cas, queue_root=queue_root, apply=True)

    assert [row["disposition"] for row in rows] == [sl.SWEPT]
    state, record = _terminal(queue_root, job.action_key)
    assert state == pool.DONE
    assert record["status"] == "executed"
    assert record["detail"]["receipt_published"] is True
    assert record["detail"]["slurm"]["state"] == sl.UNKNOWN_STATE


def test_a_running_job_is_reported_waiting_and_nothing_is_written(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch,  # noqa: F811
) -> None:
    """A job still on a node is not an ending, and a report leaves no trace.

    The liveness journal is the trace to watch for: ``wait`` samples a RUNNING
    job and appends to ``liveness.jsonl`` in the lane directory, so a reconcile
    that went through ``wait`` would write into the lane it was only reading.
    """

    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    queue_root = tmp_path / "pb-queue"
    action = _runnable_action(tmp_path, cas)
    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "RUNNING")
    job = _detach(tmp_path, queue_root=queue_root, cas=cas, action=action)
    before = sorted(os.listdir(job.directory))

    rows = sl.sweep(cas=cas, queue_root=queue_root)

    assert [row["disposition"] for row in rows] == [sl.WAITING]
    assert "RUNNING" in str(rows[0]["note"])
    assert _terminal(queue_root, job.action_key) is None
    assert sorted(os.listdir(job.directory)) == before


def test_a_sweeper_and_a_poller_racing_on_one_key_file_one_record(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch,  # noqa: F811
) -> None:
    """Requirement two: one record, no exception, whoever gets there first.

    Both sides reach ``publish_outcome``, which links first at an empty name.
    The interleave is forced rather than hoped for: the first link is failed
    once, which is exactly what the loser of the real race sees, and
    ``_land_summary`` then has to re-read and stop rather than overwrite.
    """

    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "run")
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    queue_root = tmp_path / "pb-queue"
    action = _runnable_action(tmp_path, cas)
    job = _detach(tmp_path, queue_root=queue_root, cas=cas, action=action)
    submission = sl.recorded_submission(job.action_key)

    real_link = sl._publish_json_if_absent
    failed_once = threading.Event()

    def _link(path: Path, payload) -> bool:
        # The loser's view of the race: the name was taken between its read
        # and its link.  Exactly once, so the second attempt is real.
        if not failed_once.is_set():
            failed_once.set()
            return False
        return real_link(path, payload)

    monkeypatch.setattr(sl, "_publish_json_if_absent", _link)

    start = threading.Barrier(2)
    errors: list[BaseException] = []

    def _poll() -> None:
        try:
            start.wait()
            sl.resume(submission, action=action, cas=cas,
                      queue_root=queue_root, wait_s=0.0)
        except BaseException as exc:              # noqa: BLE001 - reported
            errors.append(exc)

    def _sweep() -> None:
        try:
            start.wait()
            sl.sweep(cas=cas, queue_root=queue_root, apply=True)
        except BaseException as exc:              # noqa: BLE001 - reported
            errors.append(exc)

    threads = [threading.Thread(target=_poll), threading.Thread(target=_sweep)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert errors == []
    assert failed_once.is_set()
    filed = sorted(
        f"{state}/{path.name}"
        for state in (pool.DONE, pool.FAILED, pool.WITHDRAWN)
        for path in (queue_root / state).glob("*.json")
    )
    assert filed == [f"{pool.DONE}/{job.action_key}.json"]
    _, record = _terminal(queue_root, job.action_key)
    assert record["status"] == "executed"
    assert record["published_unix"] == submission["published_unix"]


def test_a_newer_runs_ending_is_not_replaced_by_an_older_submission(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch,  # noqa: F811
) -> None:
    """A sweep of a stale submission leaves the later run's account standing.

    One action key accumulates a submission record per run, and ``latest.json``
    names the newest. A sweep must not resurrect an older generation's ending
    over it, which is the rule ``publish_outcome`` states and this reuses
    rather than restates.
    """

    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "run")
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    queue_root = tmp_path / "pb-queue"
    action = _runnable_action(tmp_path, cas)
    job = _detach(tmp_path, queue_root=queue_root, cas=cas, action=action)
    submission = dict(sl.recorded_submission(job.action_key))

    # A later run of the same key ended and filed its account.
    sl.publish_outcome(
        queue_root=queue_root, action_key=job.action_key,
        published_unix=float(submission["published_unix"]) + 100.0,
        published_by="sparky", status="failed", attempts=1, max_attempts=1,
        retry_safe=False)

    rows = sl.sweep(
        cas=cas, queue_root=queue_root, apply=True, keys=[job.action_key])

    assert [row["disposition"] for row in rows] == [sl.SUPERSEDED]
    state, record = _terminal(queue_root, job.action_key)
    assert (state, record["status"]) == (pool.FAILED, "failed")


def test_the_tool_reports_by_default_and_files_only_under_apply(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch,  # noqa: F811
    capsys: pytest.CaptureFixture[str],
) -> None:
    """``pbsweep.py`` is read-only until an operator says otherwise."""

    import pbsweep

    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "run")
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    queue_root = tmp_path / "pb-queue"
    action = _runnable_action(tmp_path, cas)
    job = _detach(tmp_path, queue_root=queue_root, cas=cas, action=action)
    argv = [
        "--lane-root", str(sl.lane_root(None)),
        "--queue-root", str(queue_root),
        "--cas-root", str(cas.root),
    ]

    assert pbsweep.main(argv) == 0
    printed = capsys.readouterr().out
    assert job.action_key[:12] in printed
    assert sl.MISSING in printed
    assert "--apply" in printed
    assert _terminal(queue_root, job.action_key) is None

    assert pbsweep.main([*argv, "--apply"]) == 0
    assert _terminal(queue_root, job.action_key)[0] == pool.DONE


def test_the_tool_says_which_keys_it_could_not_resolve(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch,  # noqa: F811
) -> None:
    """An unresolved key is reported and gets its own exit status.

    Distinct from 1, which every fleet tool keeps for "the work failed":
    nothing here runs work, and a cron entry has to be able to tell a sweep
    that found a job nobody can account for from one that found nothing.
    """

    import pbsweep

    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    queue_root = tmp_path / "pb-queue"
    action = _runnable_action(tmp_path, cas)
    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "PENDING")
    job = _detach(tmp_path, queue_root=queue_root, cas=cas, action=action)
    (fleet / f"{job.job_id}.state").unlink()

    code = pbsweep.main([
        "--lane-root", str(sl.lane_root(None)),
        "--queue-root", str(queue_root),
        "--cas-root", str(cas.root),
        "--apply",
    ])

    assert code == pbsweep.UNRESOLVED_EXIT
    assert _terminal(queue_root, job.action_key) is None


@pytest.mark.parametrize("state", ["PENDING", "forgotten"])
def test_withdrawal_without_receipt_is_enriched_even_before_scheduler_acknowledges(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch, state: str,
) -> None:
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    queue_root = tmp_path / "pb-queue"
    action = _runnable_action(tmp_path, cas)
    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "PENDING")
    job = _detach(tmp_path, queue_root=queue_root, cas=cas, action=action)
    if state == "forgotten":
        (fleet / f"{job.job_id}.state").unlink()
    sl.publish_withdrawal(
        queue_root=queue_root, action_key=job.action_key,
        submission=sl.recorded_submission(job.action_key), reason="operator stop")

    rows = sl.sweep(cas=cas, queue_root=queue_root, apply=True)

    assert rows[0]["disposition"] == sl.SWEPT
    _, record = _terminal(queue_root, job.action_key)
    assert record["status"] == "withdrawn"
    assert record["detail"]["slurm"]["job_id"] == job.job_id
    assert record["reason"] == "operator stop"
    assert sl.sweep(cas=cas, queue_root=queue_root)[0]["disposition"] == sl.ALREADY_FILED


def test_apply_reports_unresolved_when_verdict_disappears_between_polls(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    import pbsweep

    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    queue_root = tmp_path / "pb-queue"
    action = _runnable_action(tmp_path, cas)
    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "FAILED")
    job = _detach(tmp_path, queue_root=queue_root, cas=cas, action=action)
    real_resume = sl.resume

    def forgotten_before_resume(*args, **kwargs):
        (fleet / f"{job.job_id}.state").unlink()
        return real_resume(*args, **kwargs)

    monkeypatch.setattr(sl, "resume", forgotten_before_resume)
    code = pbsweep.main([
        "--lane-root", str(sl.lane_root(None)), "--queue-root", str(queue_root),
        "--cas-root", str(cas.root), "--apply", "--json"])

    assert code == pbsweep.UNRESOLVED_EXIT
    row = json.loads(capsys.readouterr().out)["rows"][0]
    assert row["disposition"] == sl.NO_VERDICT
    assert row["status"] is None
    assert _terminal(queue_root, job.action_key) is None


def test_bad_submission_does_not_prevent_other_keys_from_being_reconciled(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    queue_root = tmp_path / "pb-queue"
    action = _runnable_action(tmp_path, cas)
    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "run")
    job = _detach(tmp_path, queue_root=queue_root, cas=cas, action=action)
    bad_key = "0" * 64
    bad_directory = sl.lane_root(None) / bad_key
    bad_directory.mkdir()
    (bad_directory / "latest.json").write_text(json.dumps({"job_id": "broken"}))

    rows = sl.sweep(cas=cas, queue_root=queue_root, apply=True)

    assert [(r["action_key"], r["disposition"]) for r in rows] == [
        (bad_key, sl.UNRECORDED), (job.action_key, sl.SWEPT)]
    assert _terminal(queue_root, job.action_key)[1]["status"] == "executed"


def test_request_for_another_key_cannot_supply_the_swept_verdict(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    queue_root = tmp_path / "pb-queue"
    action = _runnable_action(tmp_path, cas)
    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "PENDING")
    job = _detach(tmp_path, queue_root=queue_root, cas=cas, action=action)
    # A misplaced but valid request belongs to a different content key.
    other = pb.seal_action({k: v for k, v in action.items() if k != "action_key"} | {
        "environment": {"variables": {"DIFFERENT": "1"}, "toolchain": {}}})
    request = cas.publish_action_request(action)
    request.chmod(0o644)
    request.write_text(json.dumps(other))

    rows = sl.sweep(cas=cas, queue_root=queue_root, apply=True)

    assert rows[0]["disposition"] == sl.NO_ACTION
    assert _terminal(queue_root, job.action_key) is None
