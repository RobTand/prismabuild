"""Git-in-checkout runners share the core owner (issue #1307).

``core._git`` (mapped, fail factory) and ``core._git_run`` (raw result) own
every ``git -C`` invocation except ``pbsnapshot._git``, which stays a
``check_output`` bytes reader (bytes + DEVNULL + CalledProcessError is a
different primitive shape). Each site keeps its current timeout;
``seal_and_publish._git`` gains the 30 s core default (it had none).

Read-only: every test runs ``rev-parse`` against this repository itself.
"""

from __future__ import annotations

from pathlib import Path
import subprocess
import sys

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))

from prismabuild import core as pb  # noqa: E402


def test_owner_returns_stdout():
    out = pb._git(REPOSITORY, "rev-parse", "--verify", "HEAD")
    assert out.strip() == out.strip() and len(out.strip()) == 40


def test_owner_maps_failure_through_fail():
    seen = []
    try:
        pb._git(
            REPOSITORY, "rev-parse", "--verify", "refs/heads/does-not-exist-1307",
            fail=seen.append,
        )
    except Exception:
        raise AssertionError("custom fail factory was not used")
    assert seen and seen[0].startswith("Git rev-parse --verify")


def test_owner_default_fail_raises_contract(tmp_path):
    try:
        pb._git(tmp_path, "rev-parse", "HEAD")
    except pb.ActionContractError as exc:
        assert "Git rev-parse HEAD failed" in str(exc)
    else:
        raise AssertionError("non-repo read did not refuse")


def test_run_primitive_returns_completed_process():
    completed = pb._git_run(REPOSITORY, "rev-parse", "--verify", "HEAD")
    assert isinstance(completed, subprocess.CompletedProcess)
    assert completed.returncode == 0


def test_seal_git_returns_completed_process():
    import seal_and_publish as seal

    completed = seal._git(REPOSITORY, ["rev-parse", "--verify", "HEAD"])
    assert isinstance(completed, subprocess.CompletedProcess)
    assert completed.returncode == 0


def test_publish_git_result_returns_completed_process():
    import publish_runtime as pr

    result = pr._git_result("rev-parse", "--verify", "HEAD")
    assert isinstance(result, subprocess.CompletedProcess)
    assert result.returncode == 0


def test_shape_git_returns_str():
    import shape_gate as sg

    out = sg._git(REPOSITORY, "rev-parse", "--verify", "HEAD")
    assert isinstance(out, str) and len(out.strip()) == 40


def test_pbsnapshot_git_stays_bytes():
    import pbsnapshot as snap

    raw = snap._git(REPOSITORY, "rev-parse", "--verify", "HEAD")
    assert isinstance(raw, bytes) and len(raw.strip()) == 40


def test_pbrun_snapshot_git_returns_str():
    import pbrun

    out = pbrun._snapshot_git(REPOSITORY, ["rev-parse", "--verify", "HEAD"])
    assert isinstance(out, str) and len(out.strip()) == 40
