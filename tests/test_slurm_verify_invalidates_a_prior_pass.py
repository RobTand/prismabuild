"""A verification that fails retires the one that passed before it.

``verify.sh`` writes ``~/.prismabuild/slurm-verify-passed.json`` on a pass and
``cutover.sh`` reads it: the hash of the ``slurm.conf`` it verified must be
this checkout's, and the age is printed rather than judged.  All of that is
about the run that passed, and none of it is about the run that failed
afterwards.  So a fleet that verified on Monday and stopped executing jobs on
Tuesday still had Monday's marker on Tuesday, with the same configuration hash
and the same registered nodes, and an ordinary ``cutover.sh --yes`` retired a
working pull queue on the strength of it.

A failing run now removes the success marker and writes a failure marker in
its place, carrying the reason, the time and the commit; ``cutover.sh``
refuses while that file is there, and ``--verified`` stays the operator's
explicit override.

Nothing here runs a verification against a fleet.  ``finish`` is read out of
the script and run on its own, the way the audit reproduced it, and the
whole-script path under test is the one that exits before it reaches a box.
"""
from __future__ import annotations

import json
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import time

import pytest

ROOT = Path(__file__).resolve().parents[1]
VERIFY = ROOT / "fleet" / "slurm" / "verify.sh"

sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_slurm_install_scripts import (  # noqa: E402
    SUPERVISE_LINE,
    _cutover,
    _live_cutover,
    write_verify_marker,
)


def _finish_functions() -> str:
    """``finish`` and everything defined beside it, out of the real script.

    The audit's reproduction sliced exactly this region and ran it with the
    failure counters set, which is how it showed the success marker surviving.
    ``invalidate`` is defined in the same region on purpose, so the slice
    stays runnable.
    """

    source = VERIFY.read_text(encoding="utf-8")
    start = source.index("finish() {")
    end = source.index("# -- reading the configuration")
    return source[start:end]


def _run_finish(tmp_path: Path, *, failed: int, passed: int, result: str) -> None:
    marker_dir = tmp_path / ".prismabuild"
    marker_dir.mkdir(parents=True, exist_ok=True)
    script = "\n".join([
        f"FAILED={failed}",
        f"PASSED={passed}",
        f"RESULTS=({shlex.quote(result)})",
        f"MARKER_DIR={shlex.quote(str(marker_dir))}",
        f"MARKER={shlex.quote(str(marker_dir / 'slurm-verify-passed.json'))}",
        f"FAILURE_MARKER="
        f"{shlex.quote(str(marker_dir / 'slurm-verify-failed.json'))}",
        f"REPO={shlex.quote(str(ROOT))}",
        f"CONF={shlex.quote(str(ROOT / 'fleet' / 'slurm' / 'slurm.conf'))}",
        _finish_functions(),
        "finish",
    ])
    completed = subprocess.run(
        ["bash", "-c", script], capture_output=True, text=True, check=False,
    )
    assert completed.returncode == (1 if failed else 0), completed.stderr


def _markers(tmp_path: Path) -> tuple[Path, Path]:
    marker_dir = tmp_path / ".prismabuild"
    return (
        marker_dir / "slurm-verify-passed.json",
        marker_dir / "slurm-verify-failed.json",
    )


# -- verify.sh ---------------------------------------------------------------


def test_a_failing_finish_removes_the_success_marker(tmp_path: Path) -> None:
    """The success marker is what cutover.sh trusts, and it cannot tell
    "passed an hour ago" from "passed an hour ago and has failed since"."""

    passed_marker, failed_marker = _markers(tmp_path)
    passed_marker.parent.mkdir(parents=True, exist_ok=True)
    passed_marker.write_text(
        json.dumps({"verified_unix": 1, "rows": 20}), encoding="utf-8")

    _run_finish(tmp_path, failed=1, passed=6,
                result="FAIL 7 pbrun --transport slurm runs an action")

    assert not passed_marker.exists()
    assert failed_marker.exists()


def test_the_failure_marker_records_the_reason_the_time_and_the_commit(
    tmp_path: Path,
) -> None:
    """An operator reading it later has to be able to tell which run failed."""

    _, failed_marker = _markers(tmp_path)
    before = int(time.time())

    _run_finish(tmp_path, failed=2, passed=6,
                result="FAIL 7 pbrun --transport slurm runs an action")

    record = json.loads(failed_marker.read_text(encoding="utf-8"))
    assert record["schema"] == "prismaquant.prismabuild.slurm_verify_failed.v1"
    assert record["reason"] == "FAIL 7 pbrun --transport slurm runs an action"
    assert record["failed_unix"] >= before
    assert record["rows_passed"] == 6
    assert record["rows_failed"] == 2
    assert record["commit"]
    assert record["checkout"] == str(ROOT)


def test_a_passing_finish_clears_the_failure_marker(tmp_path: Path) -> None:
    """The two never coexist, so a pass is what lets a cutover proceed again."""

    passed_marker, failed_marker = _markers(tmp_path)
    failed_marker.parent.mkdir(parents=True, exist_ok=True)
    failed_marker.write_text(json.dumps({"reason": "FAIL 7"}), encoding="utf-8")

    _run_finish(tmp_path, failed=0, passed=21, result="PASS 0 something")

    assert passed_marker.exists()
    assert not failed_marker.exists()


@pytest.mark.skipif(
    shutil.which("sinfo") is not None,
    reason="the early exit under test is what happens when sinfo is missing",
)
def test_an_early_exit_invalidates_the_prior_pass_too(tmp_path: Path) -> None:
    """A box whose SLURM install has gone missing is exactly the box whose
    last pass says nothing about it now.

    This is the whole script, and it exits before it reads a configuration
    file or reaches another box: SLURM is installed on no box in this fleet,
    so ``sinfo`` is absent and the run stops at the check for it.
    """

    passed_marker, failed_marker = _markers(tmp_path)
    passed_marker.parent.mkdir(parents=True, exist_ok=True)
    passed_marker.write_text(json.dumps({"rows": 20}), encoding="utf-8")

    result = subprocess.run(
        ["bash", str(VERIFY)], capture_output=True, text=True, check=False,
        env={"HOME": str(tmp_path), "PATH": "/usr/bin:/bin"},
    )

    assert result.returncode == 2, result.stdout
    assert "sinfo is not on PATH" in result.stderr
    assert not passed_marker.exists()
    record = json.loads(failed_marker.read_text(encoding="utf-8"))
    assert "sinfo is not on PATH" in record["reason"]


# -- cutover.sh --------------------------------------------------------------


def _healthy(tmp_path: Path) -> dict[str, str]:
    """A live cutover whose publication fails, so only the gates decide."""

    return _live_cutover(tmp_path, crontab=SUPERVISE_LINE + "\n", publish_exit=1)


def _write_failure_marker(environment: dict[str, str], **overrides: object) -> Path:
    fields: dict[str, object] = {
        "schema": "prismaquant.prismabuild.slurm_verify_failed.v1",
        "host": "sparky",
        "failed_unix": int(time.time()) - 60,
        "checkout": str(ROOT),
        "commit": "0123456789abcdef0123456789abcdef01234567",
        "reason": "FAIL 7 pbrun --transport slurm runs an action end to end",
        "rows_passed": 6,
        "rows_failed": 1,
    }
    fields.update(overrides)
    path = Path(environment["PB_STATE_DIR"]) / "slurm-verify-failed.json"
    path.write_text(json.dumps(fields, indent=1) + "\n", encoding="utf-8")
    return path


def test_cutover_refuses_while_a_failed_verification_is_recorded(
    tmp_path: Path,
) -> None:
    """The success marker is still there, still names this checkout's
    slurm.conf, and the fake controller still reports the box idle.  None of
    that is evidence that the verification the operator just ran passed."""

    environment = _healthy(tmp_path)
    write_verify_marker(environment)
    failure = _write_failure_marker(environment)

    result = _cutover(environment, "--yes")

    assert result.returncode == 1, result.stdout
    assert "records a verification that did not pass" in result.stderr
    assert str(failure) in result.stderr
    assert "FAIL 7 pbrun" in result.stderr
    assert "verify.sh" in result.stderr
    # Refused before the first irreversible act, which is the state file.
    assert sorted(Path(environment["PB_STATE_DIR"]).glob("cutover-*.json")) == []
    assert not (Path(environment["PB_STATE_DIR"]) / "crontab.pre-cutover").exists()


def test_verified_is_still_the_operators_override(tmp_path: Path) -> None:
    """``--verified`` says the operator has verified the fleet from another
    box, and it has always been allowed to answer for a marker this box does
    not have.  A failure marker is the same kind of local evidence."""

    environment = _healthy(tmp_path)
    _write_failure_marker(environment)

    result = _cutover(environment, "--yes", "--verified")

    assert "did not pass" not in result.stderr
    # Past the verification gate: the refusal it hit is the publication's.
    assert "publication failed" in result.stderr
