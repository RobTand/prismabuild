"""The historical install comparison must run with pip 24.0."""
from pathlib import Path
import runpy
import subprocess

import pytest


@pytest.fixture
def recipe():
    path = Path(__file__).resolve().parents[1] / "docs/evidence/pb1485/reproduce_pack_install.py"
    return runpy.run_path(str(path))


@pytest.mark.parametrize("version", ["24.0", "24.1", "26.2.1"])
def test_bootstrap_requires_pip24(recipe, monkeypatch, tmp_path, version):
    monkeypatch.setattr(recipe["venv"], "create", lambda *args, **kwargs: None)

    def pip_version(argv, **kwargs):
        output = version if "-c" in argv else f"pip {version} from disposable-overlay"
        return subprocess.CompletedProcess(argv, 0, output + "\n", "")

    monkeypatch.setattr(recipe["subprocess"], "run", pip_version)
    overlay = tmp_path / "overlay"
    if version == "24.0":
        assert recipe["bootstrap_overlay"](overlay, tmp_path, cwd=tmp_path, env={}) == str(
            overlay / "bin/python"
        )
    else:
        with pytest.raises(SystemExit, match="requires pip 24.0"):
            recipe["bootstrap_overlay"](overlay, tmp_path, cwd=tmp_path, env={})


@pytest.mark.parametrize("failure", ["ensurepip", "pip"])
def test_bootstrap_refuses_unavailable_pip(recipe, monkeypatch, tmp_path, failure):
    def create(*args, **kwargs):
        if failure == "ensurepip":
            raise subprocess.CalledProcessError(1, ["ensurepip"])

    monkeypatch.setattr(recipe["venv"], "create", create)
    monkeypatch.setattr(
        recipe["subprocess"], "run",
        lambda argv, **kwargs: subprocess.CompletedProcess(argv, 1, "", "pip unavailable"),
    )
    with pytest.raises(SystemExit, match="requires pip 24.0"):
        recipe["bootstrap_overlay"](tmp_path / "overlay", tmp_path, cwd=tmp_path, env={})
