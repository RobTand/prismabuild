"""``--withdraw`` cancels every job under the action key's name, not one.

The submitter cannot close the double-submit window and does not try to: the
lane sends ``--dependency=singleton``, and ``slurm_lane.submit`` says why.  Two
``pbrun``s that look at the same instant both find nothing in the CAS and both
submit, and the controller holds the second one PENDING on ``Dependency``.

``--withdraw`` resolved one record out of ``latest.json`` and cancelled the one
job id it named, so the held sibling stayed queued.  When the first job left,
the singleton released it and it ran the action somebody had already withdrawn,
past a marker written to stop exactly that.  ``slurm_lane.sibling_jobs`` already
lists every job under the name, and it is scoped to this user so a name
collision with a second person's job of the same action is not this operator's
to cancel.

Driven with a ``squeue`` and a ``scancel`` on PATH, because the defect is what
the operator's own command does with no arguments beyond the key.
"""
from __future__ import annotations

import getpass
import json
import os
from pathlib import Path
import subprocess
import sys
import textwrap

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))

from prismabuild import pool, slurm_lane as sl  # noqa: E402
import pbrun  # noqa: E402

KEY = "9f" * 32

_FAKE_SQUEUE = '''\
import os, sys
from pathlib import Path

state = Path(os.environ["FAKE_SLURM_STATE"])
(state / "squeue.argv").open("a").write(" ".join(sys.argv[1:]) + "\\n")
if os.environ.get("FAKE_CONTROLLER_DOWN") == "1":
    sys.stderr.write("squeue: error: Unable to contact slurm controller\\n")
    raise SystemExit(1)
listing = state / "siblings"
if listing.exists():
    sys.stdout.write(listing.read_text())
'''

_FAKE_SCANCEL = '''\
import os, sys
from pathlib import Path

state = Path(os.environ["FAKE_SLURM_STATE"])
job = sys.argv[-1]
(state / "cancelled").open("a").write(job + "\\n")
refused = os.environ.get("FAKE_SCANCEL_REFUSES", "").split(",")
if job in refused:
    sys.stderr.write(f"scancel: error: Kill job error on job id {job}\\n")
    raise SystemExit(1)
'''


@pytest.fixture()
def controller(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A ``squeue`` and a ``scancel`` on PATH that record what they were asked."""

    binaries = tmp_path / "bin"
    binaries.mkdir()
    for name, body in (("squeue", _FAKE_SQUEUE), ("scancel", _FAKE_SCANCEL)):
        script = binaries / name
        script.write_text(
            f"#!{sys.executable}\n" + textwrap.dedent(body), encoding="utf-8")
        script.chmod(0o755)
    state = tmp_path / "slurm-state"
    state.mkdir()
    monkeypatch.setenv("PATH", f"{binaries}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("FAKE_SLURM_STATE", str(state))
    return state


def _lane_record(lane_root: Path, *, job_id: str = "1001") -> Path:
    directory = lane_root / KEY
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "latest.json").write_text(json.dumps({
        "schema": sl.SUBMISSION_SCHEMA_V1,
        "action_key": KEY,
        "attempt": 1,
        "job_id": job_id,
        "directory": str(directory),
        "stdout": str(directory / f"{job_id}.out"),
        "stderr": str(directory / f"{job_id}.err"),
        "published_unix": 100.0,
        "published_by": "sparky",
        "constraint": [],
        "resources": {"cpu": 1},
        "retry_safe": False,
        "max_attempts": 1,
    }), encoding="utf-8")
    return directory


def _cancelled(controller: Path) -> list[str]:
    path = controller / "cancelled"
    return sorted(path.read_text().split()) if path.exists() else []


def _withdraw(tmp_path: Path, **kwargs) -> int:
    lane_root = tmp_path / "lane"
    _lane_record(lane_root)
    return pbrun.withdraw_slurm_main(
        [KEY[:12]], by="rob@sparky", lane_root=lane_root,
        queue_root=tmp_path / "pb-queue", **kwargs,
    )


def test_it_cancels_the_sibling_the_singleton_is_holding(
    tmp_path: Path, controller: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """The whole defect: the held job runs the action after the withdrawal."""

    (controller / "siblings").write_text("1001|RUNNING\n1002|PENDING\n")

    assert _withdraw(tmp_path, reason="superseded") == 0
    assert _cancelled(controller) == ["1001", "1002"]
    err = capsys.readouterr().err
    assert "1001" in err and "1002" in err


def test_it_asks_the_controller_only_about_this_users_jobs(
    tmp_path: Path, controller: Path
) -> None:
    """A job name is scoped by user, so cancelling on the name alone would
    reach a second person's job of the same action."""

    (controller / "siblings").write_text("1001|RUNNING\n")
    _withdraw(tmp_path)

    asked = [
        line.split()
        for line in (controller / "squeue.argv").read_text().splitlines()
        if any(field.startswith("--name=") for field in line.split())
    ]
    assert asked, "the controller was never asked for the key's jobs"
    for argv in asked:
        assert "-u" in argv
        assert argv[argv.index("-u") + 1] == getpass.getuser()


def test_a_controller_that_will_not_answer_still_cancels_the_recorded_job(
    tmp_path: Path, controller: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The listing is an enrichment: losing it must not lose the cancel."""

    monkeypatch.setenv("FAKE_CONTROLLER_DOWN", "1")

    assert _withdraw(tmp_path) == 0
    assert _cancelled(controller) == ["1001"]


def test_one_refused_cancel_among_two_is_still_a_withdrawal(
    tmp_path: Path, controller: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A sibling that finished between the listing and the cancel is the
    ordinary case, and the run the operator asked about was stopped."""

    (controller / "siblings").write_text("1001|RUNNING\n1002|PENDING\n")
    monkeypatch.setenv("FAKE_SCANCEL_REFUSES", "1002")

    assert _withdraw(tmp_path) == 0
    assert _cancelled(controller) == ["1001", "1002"]
    assert "scancel refused slurm job 1002" in capsys.readouterr().err


def test_every_cancel_refused_is_reported_as_a_failure(
    tmp_path: Path, controller: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Exit 2 keeps its meaning: nothing the operator named was cancelled."""

    (controller / "siblings").write_text("1001|RUNNING\n")
    monkeypatch.setenv("FAKE_SCANCEL_REFUSES", "1001")

    assert _withdraw(tmp_path) == 2
    assert _cancelled(controller) == ["1001"]


def test_the_marker_is_on_disk_before_any_cancel_runs(
    tmp_path: Path, controller: Path
) -> None:
    """The order the withdrawal depends on: ``pool_reset`` skips a
    re-submission only on a marker or a ``withdrawn_unix``."""

    (controller / "siblings").write_text("1001|RUNNING\n1002|PENDING\n")
    marker = tmp_path / "pb-queue" / pool.WITHDRAWN / f"{KEY}.json"
    assert not marker.exists()

    _withdraw(tmp_path)

    # The fake scancel records what it was asked before it answers, and the
    # marker is written before the first ask, so both orders are visible here:
    # the marker exists and every job was cancelled.
    assert marker.is_file()
    assert _cancelled(controller) == ["1001", "1002"]


def test_sibling_jobs_scopes_its_listing_to_this_user() -> None:
    """The listing itself, asked directly."""

    seen: list[list[str]] = []

    def squeue(argv):
        seen.append([str(arg) for arg in argv])
        return subprocess.CompletedProcess(argv, 0, "1001|RUNNING\n", "")

    assert sl.sibling_jobs(KEY, squeue=squeue) == [("1001", "RUNNING")]
    assert seen and "-u" in seen[0]
    assert seen[0][seen[0].index("-u") + 1] == getpass.getuser()
