"""A submission that arrives after the wait began is still this wait's job.

``resolve_key`` accepts a full action key for work nothing has recorded yet,
which is what lets an operator start waiting before the thing being waited for
is submitted. Under SLURM that waiter is also the only process that will ever
file the submission's ending, because the submitter detached. So a wait that
read the recorded submission once and then watched terminal files alone could
not discover the job at all: it spent its whole ``--wait-s`` on a job that had
already finished, returned ``waiting`` and exit 75, and a second wait started
straight afterwards reported the ending immediately.

Both endings are covered, because they leave by different doors: a failed job
is filed from what the controller says, and a successful one from the CAS
receipt that outranks the controller.

A parent-side observation adapter submits once after the first complete
not-found snapshot and before the next observation begins. The real bounded
readers stay read-only: killing a disposable reader cannot interrupt publication
and leave behind a witness that prevents another submission. Observation history
lives in the parent, and a lock/event orders publication without sleeps or a
scheduler race. The submission exists from the second pass onward, never before.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import sys
import threading

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
from prismabuild import core as pb  # noqa: E402
from prismabuild import pool, slurm_lane  # noqa: E402

sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))
from prismabuild import fleet_submit  # noqa: E402
import pbrun  # noqa: E402
from prismabuild import submission as submit_core
import pbwait  # noqa: E402

from test_slurm_lane import fleet, _runnable_action  # noqa: E402,F401


def _submits_after_the_first_observation(
    monkeypatch: pytest.MonkeyPatch, *, action, cas, queue
) -> list[dict]:
    """Publish in the waiter parent between two real bounded observations.

    ``_look_once_checked`` calls the module-global ``bounded_observation`` in
    the parent. Only after that read has returned an entirely empty snapshot
    do we publish the sealed request and detach the real SLURM submission.
    Return the original snapshot unchanged, so that pass still reports pending;
    the next pass discovers both submission and any receipt together, not a
    receipt alone that could be misreported as a cache hit.
    """

    passes = []
    parent_pid = os.getpid()
    lock = threading.Lock()
    published = threading.Event()
    original = pbwait.bounded_observation

    def _observe_then_submit(*args, **kwargs):
        assert os.getpid() == parent_pid
        # Hold the lock across the read and publication, so another observation
        # cannot begin in the gap even if a caller ever uses multiple threads.
        with lock:
            snapshot = original(*args, **kwargs)
            found, landed, generation, sealed, receipt, unreadable, observed = snapshot
            passes.append({"action_present": sealed is not None})
            if not published.is_set():
                assert (found, landed, generation, sealed, receipt, unreadable, observed) == (
                    None, None, None, None, False, None, None)
                request = cas.publish_action_request(action)
                fleet_submit.submit(
                    action, cas=cas, request_path=request, transport="slurm",
                    queue_root=queue.root,
                )
                published.set()
            return snapshot

    monkeypatch.setattr(pbwait, "bounded_observation", _observe_then_submit)
    return passes


def _wait(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, verdict: str):
    """One wait started before the submission, and the rows it returned."""

    monkeypatch.setattr(submit_core.action_wait, "POLL_S", 0.0)
    monkeypatch.setenv("FAKE_SBATCH_VERDICT", verdict)
    monkeypatch.setenv(
        "PRISMABUILD_LOCAL_CHECKOUT_ROOT", str(tmp_path / "checkouts"))
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    action = _runnable_action(tmp_path, cas)
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.records.ensure_layout()
    key = str(action["action_key"])

    # Observe the real boundary and publication calls without replacing their
    # results. A submission from a disposable reader must fail this contract,
    # even on a machine fast enough to finish it inside the read budget.
    parent_pid = os.getpid()
    events = []
    snapshots = []
    original_observation = pbwait.bounded_observation
    original_publish = cas.publish_action_request
    original_submit = fleet_submit.submit

    def observe(*args, **kwargs):
        snapshot = original_observation(*args, **kwargs)
        assert os.getpid() == parent_pid
        snapshots.append(snapshot)
        events.append("observation")
        return snapshot

    def publish(*args, **kwargs):
        assert os.getpid() == parent_pid, "publication must run in the waiter parent"
        assert events == ["observation"]
        found, landed, generation, sealed, receipt, unreadable, observed = snapshots[0]
        assert (found, landed, generation, sealed, receipt, unreadable, observed) == (
            None, None, None, None, False, None, None)
        events.append("request")
        return original_publish(*args, **kwargs)

    def submit(*args, **kwargs):
        assert os.getpid() == parent_pid, "submission must run in the waiter parent"
        assert events == ["observation", "request"]
        events.append("submission")
        return original_submit(*args, **kwargs)

    monkeypatch.setattr(pbwait, "bounded_observation", observe)
    monkeypatch.setattr(cas, "publish_action_request", publish)
    monkeypatch.setattr(fleet_submit, "submit", submit)
    passes = _submits_after_the_first_observation(
        monkeypatch, action=action, cas=cas, queue=queue)
    rows = pbwait.wait_for_keys(
        queue, [key], cas=cas, wait_s=30.0, queue_root=queue.root, poll_s=0.0)

    assert slurm_lane.recorded_submission(key) is not None
    assert events[:3] == ["observation", "request", "submission"]
    assert events[3:] and all(event == "observation" for event in events[3:])
    found_snapshots = snapshots[1:]
    submission = slurm_lane.recorded_submission(key)
    assert all(snapshot[0] == ("slurm", submission["published_unix"], submission)
               for snapshot in found_snapshots)
    assert all(snapshot[2] == submission["published_unix"] for snapshot in found_snapshots)
    assert len(rows) == 1
    terminal_paths = [
        path
        for state in (pool.record_store.DONE, pool.record_store.FAILED,
                      pool.record_store.WITHDRAWN)
        for path in queue.records.dir(state).glob("*.json")
    ]
    assert len(terminal_paths) == 1
    terminal = json.loads(terminal_paths[0].read_text(encoding="utf-8"))
    assert terminal["action_key"] == key
    assert terminal["published_unix"] == submission["published_unix"]
    assert passes[0] == {"action_present": False}
    assert all(row == {"action_present": True} for row in passes[1:])
    return key, queue, rows, passes


def test_a_later_detached_submission_that_failed_is_found_and_filed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fleet: Path
) -> None:
    """The wait discovers the job, resumes it, and reports what it did.

    main: the first wait returns ``failed`` rather than ``waiting``, so the
    verdict is 1 and not 75.
    branch: the ending is filed under ``failed/`` for every other reader of
    the queue, which is the half nobody else can do for a detached lane
    submission.
    """

    key, queue, rows, passes = _wait(tmp_path, monkeypatch, "exit:7")

    submissions = [json.loads(line) for line in
                   (fleet / "submissions.jsonl").read_text(encoding="utf-8").splitlines()]
    assert len(submissions) == 1
    assert str(submissions[0]["job_id"]) == rows[0]["job"]
    assert rows[0]["status"] == "failed", rows
    assert rows[0]["transport"] == "slurm"
    assert pbwait.verdict(rows) == 1
    filed = queue.records.item_path(pool.record_store.FAILED, key)
    record = json.loads(filed.read_text(encoding="utf-8"))
    assert record["status"] == "failed"
    assert record["detail"]["slurm"]["state"] == "FAILED"
    assert rows[0]["job"] == record["detail"]["slurm"]["job_id"]
    # The wait looked for the submission more than once, which is the whole of
    # what makes the discovery possible.
    assert len(passes) >= 2


def test_a_later_detached_submission_that_worked_is_found_and_filed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fleet: Path
) -> None:
    """The same discovery, ending on the receipt rather than on the controller.

    branch: a successful job publishes a CAS receipt while it runs, and the
    resumed ending records it. Reporting ``cache_hit`` here would be wrong in
    a way the table hides: it is this submission's own run, and it files no
    record under ``done/``.
    """

    key, queue, rows, passes = _wait(tmp_path, monkeypatch, "run")

    submissions = [json.loads(line) for line in
                   (fleet / "submissions.jsonl").read_text(encoding="utf-8").splitlines()]
    assert len(submissions) == 1
    assert str(submissions[0]["job_id"]) == rows[0]["job"]
    assert rows[0]["status"] == "executed", rows
    assert rows[0]["transport"] == "slurm"
    assert rows[0]["receipt_published"] is True
    assert pbwait.verdict(rows) == 0
    record = json.loads(
        queue.records.item_path(pool.record_store.DONE, key).read_text(encoding="utf-8"))
    assert record["status"] == "executed"
    assert record["detail"]["receipt_published"] is True
    assert rows[0]["job"] == record["detail"]["slurm"]["job_id"]
    assert len(passes) >= 2


def test_a_key_nothing_ever_records_still_gives_up_as_waiting(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Polling for a submission must not turn patience into a hang.

    branch: with no submission and no ending, the wait still ends at its
    deadline with ``waiting`` and 75, which is what a caller reads as "the
    work is still out there".
    """

    monkeypatch.setattr(submit_core.action_wait, "POLL_S", 0.0)
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.records.ensure_layout()
    cas = pb.PrismaBuildCAS(tmp_path / "cas")

    rows = pbwait.wait_for_keys(queue, ["c" * 64], cas=cas, wait_s=0.0)

    assert rows[0]["status"] == "waiting"
    assert rows[0]["transport"] == "-"
    assert rows[0]["job"] == "-"
    assert pbwait.verdict(rows) == pbwait.GAVE_UP_EXIT
