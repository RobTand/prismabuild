"""An older waiter cannot file its ending over a later run's.

An action key is a content hash, so one key holds the ending of every run of
the same work at one name.  ``publish_outcome`` refused only an *equal*
generation, so a waiter that resumed an old job after a newer run of the same
key had already ended replaced the newer record with its own:
``pbrun.terminal_record(path, <newer generation>)`` then answered ``None``, a
generation-specific waiter timed out with an ending on disk, and an unscoped
reader was handed the older run.

Issue #66.  The rule is an ordering, and it is applied across the write rather
than only before it, so two writers that cross inside the comparison still
leave the later run's ending standing.
"""
from __future__ import annotations

import json
from pathlib import Path
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


def _completed(argv, stdout: str = ""):
    return subprocess.CompletedProcess(argv, 0, stdout, "")


def _file(queue_root: Path, generation: float, *, status: str = "failed"):
    return sl.publish_outcome(
        queue_root=queue_root, action_key=KEY, published_unix=generation,
        published_by="rob@test", status=status, attempts=1, max_attempts=1,
        retry_safe=None,
    )


def _failed(queue_root: Path) -> Path:
    return queue_root / pool.FAILED / f"{KEY}.json"


def test_an_older_waiter_does_not_replace_a_newer_generations_ending(
    tmp_path: Path
) -> None:
    """Pre-fix: ``record["published_unix"] == 200.0`` failed with ``100.0``."""

    assert _file(tmp_path, 200.0) is not None

    assert _file(tmp_path, 100.0) is None

    record = json.loads(_failed(tmp_path).read_text(encoding="utf-8"))
    assert record["published_unix"] == 200.0
    assert pbrun.terminal_record(_failed(tmp_path), 200.0) is not None
    assert pbrun.terminal_record(_failed(tmp_path), 100.0) is None


def test_a_newer_ending_still_replaces_an_older_one(tmp_path: Path) -> None:
    """The half that already worked, and has to keep working: a later run is a
    new request for the work, and its ending is the current one."""

    assert _file(tmp_path, 100.0) is not None

    assert _file(tmp_path, 200.0) is not None

    record = json.loads(_failed(tmp_path).read_text(encoding="utf-8"))
    assert record["published_unix"] == 200.0


def test_an_ending_this_writer_crossed_is_repaired(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Two writers cross inside the comparison.

    The older writer read an empty name, so its replace lands after the newer
    one's.  Read-then-rename cannot see that; the re-read can, and rewrites
    the later run's ending over it.
    """

    replace = sl._write_json_atomic
    crossed: list[int] = []
    older = {
        "schema": sl.OUTCOME_SCHEMA_V1, "transport": "slurm",
        "action_key": KEY, "published_unix": 100.0, "published_by": "rob@test",
        "status": "failed", "detail": {"status": "failed"},
    }

    def crossing_write(path: Path, payload) -> None:
        replace(path, payload)
        if crossed or path.name != f"{KEY}.json":
            return
        crossed.append(1)
        # The older waiter's own replace, which its stale read licensed.
        replace(path, older)

    # The older run ended first, so the newer writer replaces that record and
    # is the one the injection crosses.
    assert _file(tmp_path, 100.0) is not None
    monkeypatch.setattr(sl, "_write_json_atomic", crossing_write)

    assert _file(tmp_path, 200.0) is not None

    assert crossed, "the injection never ran"

    record = json.loads(_failed(tmp_path).read_text(encoding="utf-8"))
    assert record["published_unix"] == 200.0


def test_a_delayed_resume_of_an_older_job_leaves_the_newer_ending(
    tmp_path: Path
) -> None:
    """The issue's own interleaving, through the real ``resume``.

    Two submissions of one key, generations 100 and 200; the newer job is read
    out first and the older observer resumes afterwards.
    """

    queue = tmp_path / "pb-queue"
    lane = tmp_path / "lane"
    cas = SimpleNamespace(root=tmp_path / "cas", lookup=lambda _action: None)

    def _submit(generation: float, job_id: str) -> sl.SubmittedJob:
        return sl.submit(
            ACTION, cas=cas, request_path=tmp_path / "request.json",
            resources=sl.LaneResources(), timeout_s=None, worker_script=WORKER,
            job_entry=JOB_ENTRY, root=lane, published_unix=generation,
            sbatch=lambda argv: _completed(argv, job_id),
        )

    first = _submit(100.0, "1001")
    second = _submit(200.0, "1002")
    for job in (second, first):
        sl.resume(
            json.loads(job.record_path.read_text(encoding="utf-8")),
            action=ACTION, cas=cas, queue_root=queue,
            sacct=lambda argv: _completed(argv, f"{argv[1]}|FAILED|7:0|||||\n"),
        )

    record = json.loads(_failed(queue).read_text(encoding="utf-8"))
    assert record["published_unix"] == 200.0
    assert record["claimed_by"] == "1002"
    assert pbrun.terminal_record(_failed(queue), 200.0) is not None
