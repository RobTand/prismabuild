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

The submission is made inside a stubbed ``recorded_action`` rather than from a
thread, so the test carries no sleeps and no wall-clock bounds. The stub fires
on the first pass, after that pass has already looked for a submission and
found none, which is exactly the ordering the defect needs: the submission
exists from the second pass onward and never before it.
"""

from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
from prismabuild import core as pb  # noqa: E402
from prismabuild import pool, slurm_lane  # noqa: E402

sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))
import fleet_submit  # noqa: E402
import pbrun  # noqa: E402
import pbwait  # noqa: E402

from test_slurm_lane import fleet, _runnable_action  # noqa: E402,F401


def _submits_on_the_first_lookup(
    monkeypatch: pytest.MonkeyPatch, *, action, cas, queue
) -> list[int]:
    """Make the detached submission land between the first pass and the second.

    ``recorded_action`` is the hook because of where it sits in one pass: the
    submission has already been looked for by then, so this pass still sees
    none, and the receipt a successful job publishes during ``submit`` is not
    read until the next pass, by which time the submission is there to be
    resumed. Hooking the submission lookup instead would let the same pass see
    a receipt with nothing outstanding and call it a cache hit, which files no
    terminal record and is not what a detached lane submission means.
    """

    passes: list[int] = []
    original = pbwait.recorded_action

    def _also_submit(*args, **kwargs):
        answer = original(*args, **kwargs)
        passes.append(len(passes) + 1)
        if len(passes) == 1:
            request = cas.publish_action_request(action)
            fleet_submit.submit(
                action, cas=cas, request_path=request, transport="slurm",
                queue_root=queue.root,
            )
        return answer

    monkeypatch.setattr(pbwait, "recorded_action", _also_submit)
    return passes


def _wait(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, verdict: str):
    """One wait started before the submission, and the rows it returned."""

    monkeypatch.setattr(pbrun, "POLL_S", 0.0)
    monkeypatch.setenv("FAKE_SBATCH_VERDICT", verdict)
    monkeypatch.setenv(
        "PRISMABUILD_LOCAL_CHECKOUT_ROOT", str(tmp_path / "checkouts"))
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    action = _runnable_action(tmp_path, cas)
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    key = str(action["action_key"])

    passes = _submits_on_the_first_lookup(
        monkeypatch, action=action, cas=cas, queue=queue)
    rows = pbwait.wait_for_keys(
        queue, [key], cas=cas, wait_s=30.0, queue_root=queue.root, poll_s=0.0)

    assert slurm_lane.recorded_submission(key) is not None
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

    assert rows[0]["status"] == "failed", rows
    assert rows[0]["transport"] == "slurm"
    assert pbwait.verdict(rows) == 1
    filed = queue.item_path(pool.FAILED, key)
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

    assert rows[0]["status"] == "executed", rows
    assert rows[0]["transport"] == "slurm"
    assert rows[0]["receipt_published"] is True
    assert pbwait.verdict(rows) == 0
    record = json.loads(
        queue.item_path(pool.DONE, key).read_text(encoding="utf-8"))
    assert record["status"] == "executed"
    assert record["detail"]["receipt_published"] is True
    assert len(passes) >= 2


def test_a_key_nothing_ever_records_still_gives_up_as_waiting(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Polling for a submission must not turn patience into a hang.

    branch: with no submission and no ending, the wait still ends at its
    deadline with ``waiting`` and 75, which is what a caller reads as "the
    work is still out there".
    """

    monkeypatch.setattr(pbrun, "POLL_S", 0.0)
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    cas = pb.PrismaBuildCAS(tmp_path / "cas")

    rows = pbwait.wait_for_keys(queue, ["c" * 64], cas=cas, wait_s=0.0)

    assert rows[0]["status"] == "waiting"
    assert rows[0]["transport"] == "-"
    assert rows[0]["job"] == "-"
    assert pbwait.verdict(rows) == pbwait.GAVE_UP_EXIT
