"""``pbrun --detach`` submits and returns, and says on one line where it went.

A campaign is N actions across three boxes, and a submitter that blocks until
each one ends can only fan out by holding N processes open.  ``--detach`` is
the other half: seal and submit exactly as usual, print one machine-readable
line, exit 0.  What has to be true of that line is that a later wait can find
the ending from it alone -- which is why it carries the generation and not only
the key, an action key being a content hash that one run does not own.

The short-circuit on a CAS hit is the part with a cost attached.  Attached, a
repeat submission runs a job that discovers the receipt and does nothing, which
is right while somebody holds the terminal open.  Detached, that is a node
occupied and a checkout materialized to learn what the submitting process
already knew, and a re-run of a fifty-row campaign is fifty of them.
"""
from __future__ import annotations

import json
from pathlib import Path
import socket
import subprocess
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import pool  # noqa: E402
from prismabuild import slurm_lane as sl  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import pbrun  # noqa: E402

from test_slurm_lane import fleet, _submissions  # noqa: E402,F401

__all__ = ["fleet"]


def _checkout(tmp_path: Path) -> Path:
    work = tmp_path / "work"
    work.mkdir()
    (work / "seed.txt").write_text("sealed\n", encoding="utf-8")
    for args in (
        ("init", "-q"),
        ("config", "user.email", "test@example.invalid"),
        ("config", "user.name", "PrismaBuild test"),
        ("add", "seed.txt"),
        ("commit", "-qm", "sealed tree"),
    ):
        completed = subprocess.run(
            ["git", "-C", str(work), *args], capture_output=True, text=True,
        )
        assert completed.returncode == 0, completed.stderr
    return work


def _queue(tmp_path: Path) -> pool.PoolQueue:
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.announce(
        host="sparky", tags=["sparky", "gb10"], has_gpu=True,
        capacity={"cpu": 4, "mem_gb": 16, "gpu": 1},
    )
    return queue


def _run_pbrun(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, work: Path,
    *options: str, command=("/bin/bash", "-lc", "printf ok"),
) -> int:
    monkeypatch.setattr(pbrun, "SH", tmp_path)
    monkeypatch.setattr(pbrun, "POLL_S", 0.001)
    monkeypatch.setattr(socket, "gethostname", lambda: "sparky")
    monkeypatch.setattr(sys, "argv", [
        "pbrun.py", "--cwd", str(work), "--wait-s", "0.01", *options,
        "--", *command,
    ])
    return pbrun.main()


def _one_json_line(captured) -> dict:
    """The single object on stdout.  Prose goes to stderr; this is the contract."""

    lines = [line for line in captured.out.splitlines() if line.strip()]
    assert len(lines) == 1, f"stdout carried {len(lines)} lines: {lines!r}"
    return json.loads(lines[0])


# --------------------------------------------------------------------------
# The two transports
# --------------------------------------------------------------------------

def test_a_detached_pool_submission_queues_and_returns_without_waiting(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    """Nothing drains this queue, so an attached submit would spend ``--wait-s``
    and exit 75.  Detached it returns 0 with the item in ``ready`` and the line
    naming where the ending will be filed."""

    work = _checkout(tmp_path)
    queue = _queue(tmp_path)
    assert _run_pbrun(tmp_path, monkeypatch, work, "--detach") == 0

    line = _one_json_line(capsys.readouterr())
    assert line["schema"] == pbrun.DETACH_SCHEMA_V1
    assert line["transport"] == "pool"
    assert line["status"] == "submitted"
    assert line["job_id"] is None
    key = line["action_key"]
    assert len(key) == 64

    item = json.loads(Path(line["submission"]).read_text(encoding="utf-8"))
    assert item["action_key"] == key
    # The generation, so a later wait can tell this run's ending from the
    # ending of a run of the same work last week.
    assert line["published_unix"] == item["published_unix"]
    assert line["done"] == str(queue.item_path(pool.DONE, key))
    assert line["failed"] == str(queue.item_path(pool.FAILED, key))
    assert line["withdrawn"] == str(queue.item_path(pool.WITHDRAWN, key))
    assert not queue.item_path(pool.DONE, key).exists()
    assert not queue.item_path(pool.FAILED, key).exists()


def test_a_detached_slurm_submission_prints_the_job_id_and_files_no_ending(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fleet: Path, capsys
) -> None:
    """The job is still PENDING when this returns, so an ending filed now would
    say ``failed`` for work nothing has run.  The submission record is what the
    later wait resumes from, and the line names it."""

    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "PENDING")
    work = _checkout(tmp_path)
    _queue(tmp_path)
    assert _run_pbrun(
        tmp_path, monkeypatch, work, "--detach", "--transport", "slurm"
    ) == 0

    line = _one_json_line(capsys.readouterr())
    assert line["transport"] == "slurm"
    assert line["status"] == "submitted"
    key = line["action_key"]

    submissions = _submissions(fleet)
    assert len(submissions) == 1
    assert line["job_id"] == str(submissions[0]["job_id"])

    record = json.loads(Path(line["submission"]).read_text(encoding="utf-8"))
    assert record["action_key"] == key
    assert record["job_id"] == line["job_id"]
    assert line["published_unix"] == record["published_unix"]
    assert not Path(line["done"]).exists()
    assert not Path(line["failed"]).exists()


def test_a_detached_submission_asks_for_the_partition_an_attached_one_asks_for(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fleet: Path, capsys
) -> None:
    """Detaching must not be a second submit path.  GPU demand goes to the GPU
    partition either way; a detached submission that dropped ``--partition``
    would be a silent regression no ending could show."""

    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "PENDING")
    work = _checkout(tmp_path)
    _queue(tmp_path)
    assert _run_pbrun(
        tmp_path, monkeypatch, work,
        "--detach", "--transport", "slurm", "--gpu",
    ) == 0
    capsys.readouterr()

    argv = _submissions(fleet)[0]["argv"]
    assert f"--partition={sl.GPU_PARTITION}" in argv
    assert any(a.startswith("--gres=") for a in argv)


# --------------------------------------------------------------------------
# Work already in the CAS
# --------------------------------------------------------------------------

def test_a_re_run_attaches_to_the_job_that_is_still_running(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fleet: Path, capsys
) -> None:
    """A campaign whose waiter died is re-run to find out where it got to.

    Every row still on a node has to be attached to, not submitted again: two
    copies of one action materialize the same checkout twice, take the GPU
    twice, and race to publish one receipt.  The CAS cannot prevent it -- there
    is no receipt until the first copy finishes.
    """

    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "RUNNING")
    work = _checkout(tmp_path)
    _queue(tmp_path)
    assert _run_pbrun(
        tmp_path, monkeypatch, work, "--detach", "--transport", "slurm"
    ) == 0
    first = _one_json_line(capsys.readouterr())
    assert first["status"] == "submitted"

    assert _run_pbrun(
        tmp_path, monkeypatch, work, "--detach", "--transport", "slurm"
    ) == 0
    second = _one_json_line(capsys.readouterr())
    assert second["status"] == "attached"
    assert second["action_key"] == first["action_key"]
    assert second["job_id"] == first["job_id"]
    assert second["published_unix"] == first["published_unix"]
    # The submission it attached to is the one a later pbwait resumes from.
    assert Path(second["submission"]).exists()
    assert len(_submissions(fleet)) == 1, "a second job was submitted"

    # Live is not the same as recorded.  Once the job has ended without an
    # ending filed, asking again is asking for the work to be done -- and
    # nothing is running to do it.
    (fleet / f"{first['job_id']}.state").write_text("FAILED|1:0\n",
                                                    encoding="utf-8")
    assert _run_pbrun(
        tmp_path, monkeypatch, work, "--detach", "--transport", "slurm"
    ) == 0
    third = _one_json_line(capsys.readouterr())
    assert third["status"] == "submitted"
    assert third["job_id"] != first["job_id"]
    assert len(_submissions(fleet)) == 2


def test_a_detached_submission_of_finished_work_submits_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fleet: Path, capsys
) -> None:
    """Run it once on the pull queue for a real receipt, then ask SLURM for the
    same work.  The key is a content hash and carries no transport, so the
    second submission is a hit -- and the point of detaching is that it costs
    no job id."""

    work = _checkout(tmp_path)
    queue = _queue(tmp_path)
    command = ("/bin/bash", "-lc", "printf cached > result.txt")
    assert _run_pbrun(
        tmp_path, monkeypatch, work, "--detach", command=command
    ) == 0
    first = _one_json_line(capsys.readouterr())

    served = queue.serve_once(
        tags=["sparky"], python=sys.executable, timeout_s=60.0,
        capacity={"cpu": 4, "mem_gb": 16, "gpu": 1},
    )
    assert served is not None and served["status"] == "executed", served

    def _refuse(*_args, **_kwargs):
        raise AssertionError("a CAS hit must not reach sbatch")

    monkeypatch.setattr(pbrun.slurm_lane, "run", _refuse)
    assert _run_pbrun(
        tmp_path, monkeypatch, work, "--detach", "--transport", "slurm",
        command=command,
    ) == 0
    second = _one_json_line(capsys.readouterr())

    assert second["action_key"] == first["action_key"]
    assert second["status"] == "cache_hit"
    assert second["job_id"] is None
    assert _submissions(fleet) == []


# --------------------------------------------------------------------------
# What detaching cannot promise
# --------------------------------------------------------------------------

def test_detach_refuses_more_than_one_attempt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A retry is a submission made after somebody watched the first one fail.
    Detached, nobody watches, so the choice is between running one attempt for
    a caller who asked for three and saying so."""

    work = _checkout(tmp_path)
    _queue(tmp_path)
    with pytest.raises(SystemExit) as raised:
        _run_pbrun(
            tmp_path, monkeypatch, work,
            "--detach", "--retry-safe", "--max-attempts", "3",
        )
    assert "--detach submits one attempt" in str(raised.value)
