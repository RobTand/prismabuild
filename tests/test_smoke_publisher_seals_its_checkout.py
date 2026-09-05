"""The dispatcher smoke command, run against a fake scheduler for real.

This is the one command an operator runs to prove a transport works, and it
advertised both.  Its SLURM path could not run: it created an ordinary
directory, wrote the closure member into it, and handed it to the submit path
as a checkout root, and the lane addresses a checkout only through a sealed
snapshot.  The sealer refused the directory before any scheduler command was
reached, and the operator got a traceback where the submission record was
advertised.

The existing transport test stubs ``fleet_submit.submit``, so it checks
argument routing and never the Git-snapshot contract that made the real
invocation fail.  This one goes through ``fleet_submit.submit`` to a fake
``sbatch``.
"""
from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))

from prismabuild import core as pb  # noqa: E402
from prismabuild import pool  # noqa: E402

import fleet_submit  # noqa: E402
import seal_and_publish  # noqa: E402

from test_slurm_lane import _submissions, fleet  # noqa: E402

__all__ = ["fleet"]


def _run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, transport: str,
         capsys: pytest.CaptureFixture) -> dict:
    """The command, on a fresh store, with the real submit path.

    ``submit``'s ``queue_root`` default is bound at function definition, so
    repointing ``fleet_submit.SH`` does not reach it and this tool names no
    queue root of its own. The wrapper supplies one under ``tmp_path`` and
    calls the real ``submit``, so the path under test is unchanged and the
    live queue is untouched.
    """

    monkeypatch.setattr(seal_and_publish, "SH", tmp_path)
    monkeypatch.setattr(fleet_submit, "SH", tmp_path)
    monkeypatch.setattr(seal_and_publish, "RUNTIME_ROOT", REPOSITORY)
    real = fleet_submit.submit

    def confined(action, **kwargs):
        kwargs.setdefault("queue_root", tmp_path / "pb-queue")
        return real(action, **kwargs)

    monkeypatch.setattr(seal_and_publish.fleet_submit, "submit", confined)
    fleet_submit._SNAPSHOT_CACHE.clear()
    assert seal_and_publish.main(["--transport", transport]) == 0
    return json.loads(capsys.readouterr().out)


def test_the_slurm_smoke_reaches_the_scheduler_through_a_real_snapshot(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture,
) -> None:
    """A fresh temp store, sealed and submitted, with a coherent record."""

    printed = _run(tmp_path, monkeypatch, "slurm", capsys)
    checkout = tmp_path / "checkout"

    # The smoke checkout is a Git repository with a commit to parent the
    # snapshot on, and only the closure member is in it.
    head = subprocess.run(
        ["git", "-C", str(checkout), "rev-parse", "--verify", "HEAD"],
        capture_output=True, text=True)
    assert head.returncode == 0
    tracked = subprocess.run(
        ["git", "-C", str(checkout), "ls-tree", "-r", "--name-only", "HEAD"],
        capture_output=True, text=True, check=True)
    assert tracked.stdout.split() == ["task_code.py"]

    # And the submission is the one the scheduler actually took.
    rows = _submissions(fleet)
    assert len(rows) == 1
    key = printed["action_key"]
    assert key != printed["sealed_action_key"]
    assert f"--job-name=pb-{key[:12]}" in rows[0]["argv"]
    assert printed["transport"] == "slurm"
    assert printed["submitted"].startswith("slurm job ")
    assert Path(printed["where"]).is_file()

    # The request the node will read: snapshot-addressed, under the key it
    # was submitted with, and the bundle is in the store.
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    submitted = json.loads(
        (cas.root / "requests" / key[:2] / f"{key}.json").read_text(
            encoding="utf-8"))
    assert submitted["action_key"] == key
    snapshot = submitted["params"]["checkout_snapshot"]
    assert snapshot["subdirectory"] == "."
    assert snapshot["input"] in submitted["inputs"]
    assert Path(cas.input_path(snapshot["input"])).is_file()


def test_the_pull_queue_smoke_still_publishes_one_ready_item(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture,
) -> None:
    """The other transport is unchanged: no snapshot, one queue item."""

    printed = _run(tmp_path, monkeypatch, "pool", capsys)

    assert printed["transport"] == "pool"
    assert printed["action_key"] == printed["sealed_action_key"]
    item = Path(printed["where"])
    assert item.is_file()
    assert item.parent.name == pool.READY
    assert json.loads(item.read_text(encoding="utf-8"))["checkout_root"] == str(
        tmp_path / "checkout")


def test_a_second_run_leaves_the_checkouts_history_alone(
    tmp_path: Path, fleet: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture,
) -> None:
    """The smoke command commits once, and only when there is no commit."""

    first = _run(tmp_path, monkeypatch, "slurm", capsys)
    checkout = tmp_path / "checkout"
    log = subprocess.run(
        ["git", "-C", str(checkout), "rev-list", "--count", "HEAD"],
        capture_output=True, text=True, check=True)
    assert log.stdout.strip() == "1"

    second = _run(tmp_path, monkeypatch, "slurm", capsys)
    again = subprocess.run(
        ["git", "-C", str(checkout), "rev-list", "--count", "HEAD"],
        capture_output=True, text=True, check=True)
    assert again.stdout.strip() == "1"
    # Same tree, same commit, same snapshot: one key, so re-running the smoke
    # is the free re-enqueue the fleet is built on rather than new work.
    assert second["action_key"] == first["action_key"]
