"""A submission retires an earlier withdrawal, never the current one.

``submit`` makes ``latest.json`` visible before it returns, so an operator on
another box can resolve that record and withdraw the run while the submitting
process is still between ``sbatch`` and its own next step.  ``run`` then called
``supersede_withdrawal``, which archived whichever marker was present with no
generation condition: the decision was erased, the retry gate found no marker,
and a ``--retry-safe`` run submitted attempt 2 of the action somebody had just
cancelled.  The ending was filed as a failure rather than as the withdrawal it
was.

Issue #65.  The interleaving is injected at ``_write_latest``, which is the
publication boundary the second process reads across; the withdrawal code, the
generation comparison and every file operation are the real ones.
"""
from __future__ import annotations

import json
from pathlib import Path
import socket
import subprocess
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import pool  # noqa: E402
from prismabuild import slurm_lane as sl  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import pbrun  # noqa: E402

REPOSITORY = Path(__file__).resolve().parents[1]
WORKER = REPOSITORY / "tools" / "prismabuild_worker.py"
JOB_ENTRY = REPOSITORY / "tools" / "fleet" / "slurm_job.py"
KEY = "a" * 64
ACTION = {"action_key": KEY, "params": {}}


def _completed(argv, stdout: str = "", returncode: int = 0):
    return subprocess.CompletedProcess(argv, returncode, stdout, "")


class _Fleet:
    """One in-process scheduler: ``sbatch`` counts, ``sacct`` fails the job."""

    def __init__(self) -> None:
        self.submitted: list[str] = []
        self.cancelled: list[str] = []

    def sbatch(self, argv):
        job_id = str(2001 + len(self.submitted))
        self.submitted.append(job_id)
        return _completed(argv, job_id)

    def sacct(self, argv):
        # Attempt 1 failed, which is what makes the retry gate the thing under
        # test: without the marker the next attempt goes out.
        return _completed(argv, f"{argv[1]}|FAILED|1:0|||||\n")

    def squeue(self, argv):
        return _completed(argv, "")

    def cancel(self, job_id, **_kwargs) -> bool:
        self.cancelled.append(str(job_id))
        return True


def _run(
    tmp_path: Path, fleet: _Fleet, *, queue: Path, lane: Path, **kwargs
) -> sl.RunResult:
    cas = SimpleNamespace(root=tmp_path / "cas", lookup=lambda _action: None)
    return sl.run(
        ACTION, cas=cas, request_path=tmp_path / "request.json",
        resources=sl.LaneResources(), timeout_s=None, worker_script=WORKER,
        job_entry=JOB_ENTRY, root=lane, queue_root=queue,
        sbatch=fleet.sbatch, sacct=fleet.sacct, squeue=fleet.squeue,
        **kwargs,
    )


def test_a_withdrawal_of_this_run_survives_the_submission_that_follows_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Pre-fix: ``submitted == ['2001']`` failed with ``['2001', '2002']``.

    The operator's withdrawal was archived by the submitter that had just
    published the record they read, so nothing stopped attempt 2 and the
    ending was ``failed``.
    """

    queue = tmp_path / "pb-queue"
    lane = tmp_path / "lane"
    fleet = _Fleet()
    marker = queue / pool.WITHDRAWN / f"{KEY}.json"
    published = sl._write_latest
    withdrawn: list[int] = []

    def write_then_withdraw(path: Path, payload) -> None:
        published(path, payload)
        if path.name != "latest.json" or withdrawn:
            return
        withdrawn.append(1)
        # Another process resolves the record this call just published and
        # runs the real withdrawal: marker, terminal record, then scancel.
        assert pbrun.withdraw_slurm_main(
            [KEY], lane_root=lane, queue_root=queue, by="audit-operator",
            reason="stop this generation", squeue=fleet.squeue,
        ) == 0
        assert marker.exists()

    monkeypatch.setattr(sl, "_write_latest", write_then_withdraw)
    monkeypatch.setattr(sl, "cancel", fleet.cancel)

    _run(tmp_path, fleet, queue=queue, lane=lane,
         retry_safe=True, max_attempts=2)

    assert fleet.submitted == ["2001"]
    assert fleet.cancelled == ["2001"]
    assert marker.exists()
    assert not (queue / pool.FAILED / f"{KEY}.json").exists()
    ending = json.loads(
        (queue / pool.WITHDRAWN / f"{KEY}.json").read_text(encoding="utf-8"))
    assert ending["status"] == "withdrawn"
    assert ending["withdrawn_by"] == "audit-operator"
    assert ending["reason"] == "stop this generation"
    assert not list(
        (queue / pool.WITHDRAWN / "superseded").glob(f"{KEY}.*.json"))


def test_a_withdrawal_filed_between_the_read_and_the_claim_is_not_lost(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The comparison is safe against a publication that crosses it.

    Pre-fix the archive copy was written and the live name unlinked whatever
    was there, so a withdrawal filed inside that window went to
    ``superseded/`` unread.  Now the claim is one ``rename``, the archived
    bytes are re-read, and a marker this submission had no right to retire is
    linked back.
    """

    queue = tmp_path / "pb-queue"
    lane = tmp_path / "lane"
    fleet = _Fleet()
    directory = sl.lane_directory(KEY, root=lane)
    marker = queue / pool.WITHDRAWN / f"{KEY}.json"
    # An earlier run of this key was withdrawn; that decision is stale and a
    # submission is what retires it.
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text(json.dumps({
        "schema": pool.POOL_OUTCOME_SCHEMA_V1, "action_key": KEY,
        "status": "withdrawn", "withdrawn_from": "slurm",
        "published_unix": 100.0, "withdrawn_unix": 101.0,
        "withdrawn_host": socket.gethostname(), "withdrawn_by": "rob@sparky",
        "reason": "the earlier run",
    }), encoding="utf-8")
    original = sl.read_withdrawal_marker
    crossed: list[float] = []

    def read_then_withdraw(queue_root, action_key):
        answer = original(queue_root, action_key)
        if crossed:
            return answer
        # An operator withdraws the run that was just submitted, replacing the
        # stale marker between this read and the rename that claims it.
        generation = json.loads(
            (directory / "latest.json").read_text(encoding="utf-8")
        )["published_unix"]
        crossed.append(float(generation))
        marker.write_text(json.dumps({
            "schema": pool.POOL_OUTCOME_SCHEMA_V1, "action_key": KEY,
            "status": "withdrawn", "withdrawn_from": "slurm",
            "published_unix": float(generation), "withdrawn_unix": 202.0,
            "withdrawn_host": socket.gethostname(),
            "withdrawn_by": "audit-operator", "reason": "stop this generation",
        }), encoding="utf-8")
        return answer

    monkeypatch.setattr(sl, "read_withdrawal_marker", read_then_withdraw)
    monkeypatch.setattr(sl, "cancel", fleet.cancel)

    _run(tmp_path, fleet, queue=queue, lane=lane)

    assert crossed, "the injection never ran"
    assert marker.exists()
    kept = json.loads(marker.read_text(encoding="utf-8"))
    assert kept["published_unix"] == crossed[0]
    assert kept["withdrawn_by"] == "audit-operator"
    assert not list(
        (queue / pool.WITHDRAWN / "superseded").glob(f"{KEY}.*.json"))
