"""A CPU slot must not be able to run GPU work.

The pool's claim is that its ledger knows what is on each accelerator.  That
claim had a hole in one direction: an action submitted without ``--gpu``
inherited a visible device.  A pytest suite queued as a 4 GB CPU action on
2026-09-03 ran its ``skipif(not torch.cuda.is_available())`` tests on a box
whose GPU slots were held by another action -- work the ledger could not see,
contending with work it had promised exclusivity to.

The enforcement is the kernel's, not the caller's: an empty
``CUDA_VISIBLE_DEVICES`` means the child sees no device, so a mis-declared
action fails rather than steals.  That is the same mechanism ``require_pool``
already trusts for its own no-device escape hatch.
"""
from __future__ import annotations

from pathlib import Path
import subprocess
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

import pbrun


def _variables(argv, monkeypatch, tmp_path):
    """Build one submission's environment, stopping before it is published."""

    captured = []
    assert subprocess.run(
        ["git", "init", "-q", str(tmp_path)], check=False
    ).returncode == 0
    assert subprocess.run(
        [
            "git", "-C", str(tmp_path),
            "-c", "user.name=PrismaBuild test",
            "-c", "user.email=test@example.invalid",
            "commit", "--allow-empty", "-qm", "fixture",
        ],
        check=False,
    ).returncode == 0

    def _stop(body, *_a, **_kw):
        captured.append(body)
        raise _Stop()

    class _Stop(Exception):
        pass

    monkeypatch.setattr(pbrun.pb, "seal_action", _stop)
    monkeypatch.setattr(pbrun, "SH", tmp_path / "fleet")
    monkeypatch.setattr(pbrun, "git_repository_root", lambda _cwd: tmp_path)
    monkeypatch.setattr(
        pbrun,
        "build_git_checkout_snapshot",
        lambda *_args, **_kwargs: {"input": {"id": "test"}},
    )
    monkeypatch.setattr(sys, "argv", ["pbrun.py", "--cwd", str(tmp_path), *argv])
    with pytest.raises(_Stop):
        pbrun.main()
    body = captured[0]
    return body["environment"]["variables"], body["params"]["demand"]


def test_a_cpu_action_sees_no_device(monkeypatch, tmp_path):
    variables, demand = _variables(["--", "true"], monkeypatch, tmp_path)
    assert variables["CUDA_VISIBLE_DEVICES"] == ""
    assert not demand.get("gpu")


def test_a_gpu_action_is_left_alone(monkeypatch, tmp_path):
    """The mask is about slots that reserved nothing, not about the variable."""
    variables, demand = _variables(["--gpu", "--", "true"], monkeypatch, tmp_path)
    assert "CUDA_VISIBLE_DEVICES" not in variables
    assert demand.get("gpu")


def test_declaring_a_device_without_reserving_one_is_refused(monkeypatch, tmp_path):
    """The mis-declaration is refused, not silently honoured or silently masked.

    Masking it would hide the mistake; honouring it is the hole.  Refusing
    names the fix, which is ``--gpu``.
    """
    with pytest.raises(SystemExit) as exc:
        _variables(["--env", "CUDA_VISIBLE_DEVICES=0", "--", "true"],
                   monkeypatch, tmp_path)
    assert "--gpu" in str(exc.value)


def test_an_explicitly_empty_device_list_is_accepted(monkeypatch, tmp_path):
    """Saying the same thing the mask says is agreement, not conflict."""
    variables, _ = _variables(["--env", "CUDA_VISIBLE_DEVICES=", "--", "true"],
                              monkeypatch, tmp_path)
    assert variables["CUDA_VISIBLE_DEVICES"] == ""


def test_the_mask_survives_no_default_env(monkeypatch, tmp_path):
    """An empty environment makes every device visible -- the case this is for."""
    variables, _ = _variables(["--no-default-env", "--", "true"],
                              monkeypatch, tmp_path)
    assert variables["CUDA_VISIBLE_DEVICES"] == ""
