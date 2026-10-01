"""Exercise profiler selection without attaching to a process."""
from __future__ import annotations

import importlib.util
from pathlib import Path
import sys

import pytest


class _ProfilerVersionRequested(RuntimeError):
    """Stop the actual CLI before any profiler is launched."""


@pytest.fixture
def profiler_path_observer():
    source = (Path(__file__).resolve().parents[1]
              / "tools/maintenance/diag940_contended_observer.py")
    spec = importlib.util.spec_from_file_location("diag940_profiler_binding", source)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _profiler_cli_args():
    return ["observer", "--pid", "1", "--start-ticks", "7",
            "--generation-root", "/generation", "--holder-key", "c" * 64,
            "--duration", "1", "--netdata-url", "http://unused.invalid",
            "--out", "capture"]


@pytest.mark.parametrize("explicit", [False, True], ids=["legacy-default", "explicit-installed-path"])
def test_cli_uses_declared_profiler_or_preserves_default(
        profiler_path_observer, tmp_path, monkeypatch, explicit):
    observer = profiler_path_observer
    selected = tmp_path / "installed-py-spy" if explicit else Path("/usr/local/bin/py-spy")
    argv = _profiler_cli_args()
    if explicit:
        argv += ["--py-spy", str(selected)]
    monkeypatch.setattr(sys, "argv", argv)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(observer, "identity", lambda pid: {
        "start_ticks": 7, "generation_root": "/generation"})
    monkeypatch.setattr(observer, "holder", lambda *args: {"present": True})
    requested = []

    def version_request(command, **kwargs):
        requested.append(command)
        raise _ProfilerVersionRequested("profiler selection reached before capture")

    monkeypatch.setattr(observer.subprocess, "check_output", version_request)
    with pytest.raises(_ProfilerVersionRequested):
        observer.main()
    assert requested == [[str(selected), "--version"]]
    assert not (tmp_path / "capture/worker.speedscope.json").exists()


def test_cli_refuses_relative_profiler_before_identity(
        profiler_path_observer, tmp_path, monkeypatch):
    observer = profiler_path_observer
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", _profiler_cli_args() + ["--py-spy", "py-spy"])

    def unexpected_identity(pid):
        raise AssertionError("relative profiler reached target identity")

    monkeypatch.setattr(observer, "identity", unexpected_identity)
    with pytest.raises(ValueError, match="observer py-spy path must be absolute"):
        observer.main()
    assert not (tmp_path / "capture").exists()
