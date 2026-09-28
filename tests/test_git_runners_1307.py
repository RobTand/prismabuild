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
from typing import NoReturn

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))

import pytest  # noqa: E402

from prismabuild import core as pb  # noqa: E402


class _Refused(Exception):
    """Sentinel a fail factory raises: fail must not return."""


def _refuse(message: str) -> NoReturn:
    raise _Refused(message)


def test_owner_returns_stdout():
    out = pb._git(REPOSITORY, "rev-parse", "--verify", "HEAD")
    expected = subprocess.run(
        ["git", "-C", str(REPOSITORY), "rev-parse", "--verify", "HEAD"],
        capture_output=True, text=True, timeout=30,
    ).stdout
    assert out == expected and len(out.strip()) == 40


def test_owner_maps_failure_through_fail():
    with pytest.raises(_Refused, match=r"^Git rev-parse --verify"):
        pb._git(
            REPOSITORY, "rev-parse", "--verify",
            "refs/heads/does-not-exist-1307",
            fail=_refuse,
        )


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


def test_owner_maps_transport_error(monkeypatch):
    def _boom(*args, **kwargs):
        raise OSError("boom")

    monkeypatch.setattr(subprocess, "run", _boom)
    with pytest.raises(_Refused, match=r"^Git rev-parse HEAD failed: boom"):
        pb._git(REPOSITORY, "rev-parse", "HEAD", fail=_refuse)


def test_owner_maps_timeout(monkeypatch):
    def _hang(*args, **kwargs):
        raise subprocess.TimeoutExpired(cmd=args[0], timeout=kwargs["timeout"])

    monkeypatch.setattr(subprocess, "run", _hang)
    with pytest.raises(_Refused, match=r"timed out after 30"):
        pb._git(REPOSITORY, "rev-parse", "HEAD", fail=_refuse)


def test_owner_accepts_configured_returncode(monkeypatch):
    def _rc1(*args, **kwargs):
        return subprocess.CompletedProcess(args[0], 1, stdout="x", stderr="")

    monkeypatch.setattr(subprocess, "run", _rc1)
    out = pb._git(
        REPOSITORY, "diff", "--quiet",
        accepted_returncodes=(0, 1), fail=_refuse,
    )
    assert out == "x"


def test_seal_git_uses_core_default_timeout(monkeypatch):
    import seal_and_publish as seal

    captured = {}

    def _capture(*args, **kwargs):
        captured.update(kwargs)
        return subprocess.CompletedProcess(args[0], 0, stdout="", stderr="")

    monkeypatch.setattr(subprocess, "run", _capture)
    seal._git(REPOSITORY, ["rev-parse", "--verify", "HEAD"])
    assert captured["timeout"] == 30
    assert captured["env"]["GIT_COMMITTER_DATE"] == "2000-01-01T00:00:00+00:00"


def test_seal_git_maps_timeout_to_system_exit(monkeypatch):
    import seal_and_publish as seal

    def _hang_on_init(*args, **kwargs):
        if "init" in args[0]:
            raise subprocess.TimeoutExpired(
                cmd=args[0], timeout=kwargs["timeout"])
        return subprocess.CompletedProcess(args[0], 1, stdout="", stderr="")

    monkeypatch.setattr(subprocess, "run", _hang_on_init)
    with pytest.raises(SystemExit, match="seal_and_publish: cannot"):
        seal.ensure_snapshottable_checkout(REPOSITORY / "no-such-dir-1307")


def test_seal_probe_timeout_maps_to_system_exit(monkeypatch):
    # An NFS stall on the very first probe must give the SystemExit text,
    # not a raw traceback.
    import seal_and_publish as seal

    def _hang(*args, **kwargs):
        raise subprocess.TimeoutExpired(
            cmd=args[0], timeout=kwargs["timeout"])

    monkeypatch.setattr(subprocess, "run", _hang)
    with pytest.raises(SystemExit, match=r"cannot run Git rev-parse"):
        seal.ensure_snapshottable_checkout(REPOSITORY / "no-such-dir-1307")


def test_seal_add_timeout_maps_to_system_exit(monkeypatch):
    import seal_and_publish as seal

    def _hang_on_add(*args, **kwargs):
        argv = args[0]
        if argv[3:4] == ["add"]:
            raise subprocess.TimeoutExpired(
                cmd=argv, timeout=kwargs["timeout"])
        if argv[3:5] == ["rev-parse", "--verify"]:
            return subprocess.CompletedProcess(argv, 1, stdout="", stderr="")
        return subprocess.CompletedProcess(
            argv, 0, stdout="true\n", stderr="")

    monkeypatch.setattr(subprocess, "run", _hang_on_add)
    with pytest.raises(SystemExit, match=r"cannot run Git add"):
        seal.ensure_snapshottable_checkout(REPOSITORY / "no-such-dir-1307")


def test_identity_git_falls_back_on_plain_directory(monkeypatch, tmp_path):
    # Not a repository: every Git read fails, identity stays representable
    # as no-git instead of raising.
    def _norc(*args, **kwargs):
        return subprocess.CompletedProcess(args[0], 1, stdout="", stderr="")

    monkeypatch.setattr(subprocess, "run", _norc)
    identity = pb.git_checkout_identity(tmp_path)
    assert identity["head"] == "no-git"
