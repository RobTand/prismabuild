"""A wait is for one run of the work, not for the name of it.

An action key is a content hash, so re-submitting a key is how anybody asks for
the same work again -- ``PoolQueue.publish`` says so at length, and both
transports already distinguish the runs by ``published_unix``:
``terminal_outcome_covers`` refuses to let an old outcome blacklist a later
submission, and ``slurm_lane._same_generation`` refuses to let a later writer
overwrite an account of its own generation.

``pbrun``'s own wait did not.  It watched three directories for the key and
returned the first record it found, so a re-submission of work that had been
run before answered with the previous run's status the instant it was queued.
The wait took no generation at all; with it ignored, the first assertion below
reads::

    assert pbrun.await_outcome(queue, KEY, wait_s=0.05, generation=200.0) == 75
    E   assert 3 == 75

-- exit 3 from a run that had finished before this one was submitted, with the
new item still sitting in ``ready`` and no worker having touched it.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import pool  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import pbrun  # noqa: E402

KEY = "d" * 64


def _file(queue: pool.PoolQueue, state: str, record: dict) -> Path:
    path = queue.item_path(state, KEY)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(record), encoding="utf-8")
    return path


def _outcome(generation, *, status: str, returncode: int) -> dict:
    return {
        "schema": pool.POOL_OUTCOME_SCHEMA_V1,
        "action_key": KEY,
        "status": status,
        "published_unix": generation,
        "finished_unix": 1.0,
        "finished_host": "sparky",
        "attempts": 1,
        "detail": {"returncode": returncode, "elapsed_s": 1.0,
                   "stdout": "", "stderr": ""},
    }


@pytest.fixture()
def queue(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> pool.PoolQueue:
    monkeypatch.setattr(pbrun, "POLL_S", 0.001)
    made = pool.PoolQueue(tmp_path / "pb-queue")
    made.ensure_layout()
    return made


def test_an_earlier_runs_ending_does_not_answer_this_run(queue) -> None:
    """The old ending is evidence about the old request, and nothing else."""

    _file(queue, pool.FAILED, _outcome(100.0, status="failed", returncode=3))
    assert pbrun.await_outcome(
        queue, KEY, wait_s=0.05, generation=200.0
    ) == 75

    _file(queue, pool.DONE, _outcome(200.0, status="executed", returncode=0))
    assert pbrun.await_outcome(
        queue, KEY, wait_s=0.05, generation=200.0
    ) == 0


def test_a_caller_that_does_not_know_the_generation_takes_what_is_filed(
    queue,
) -> None:
    """``None`` is not "any generation is wrong"; it is "this caller cannot
    say", which is what every reader had before generations were stamped."""

    _file(queue, pool.FAILED, _outcome(100.0, status="failed", returncode=3))
    assert pbrun.await_outcome(queue, KEY, wait_s=0.05) == 3


def test_an_ending_with_no_generation_stands(queue) -> None:
    """``PoolQueue.finish`` files one with no ``published_unix`` when a reaper
    concluded the claim underneath the worker.  It is the only account of what
    happened, so refusing it would hang the caller rather than inform them."""

    record = _outcome(100.0, status="failed", returncode=3)
    del record["published_unix"]
    _file(queue, pool.FAILED, record)
    assert pbrun.await_outcome(
        queue, KEY, wait_s=0.05, generation=200.0
    ) == 3


@pytest.mark.parametrize("withdrawal", ["visible", "decision"])
def test_this_runs_withdrawal_beats_an_old_unstamped_success(
    queue, withdrawal
) -> None:
    """An exact ending is stronger evidence than the legacy fallback."""

    legacy = _outcome(100.0, status="executed", returncode=0)
    del legacy["published_unix"]
    _file(queue, pool.DONE, legacy)
    current = _outcome(200.0, status="withdrawn", returncode=143)
    if withdrawal == "visible":
        expected = _file(queue, pool.WITHDRAWN, current)
    else:
        expected = queue.withdrawal_decision_path(current)
        queue._persist_withdrawal_decision(current)

    path, ending = pbrun.landed_outcome(
        queue, KEY, wait_s=0.05, generation=200.0)

    assert path == expected
    assert ending["status"] == "withdrawn"
