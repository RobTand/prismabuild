"""The materializer's git runs ride core._git_run (#1318)."""
from __future__ import annotations

import subprocess

import pytest

from prismabuild import core as pb
from prismabuild import materialize


def test_materializer_git_delegates_to_the_shared_runner(monkeypatch):
    seen = {}

    def fake(root, *args, **kwargs):
        seen.update(root=root, args=args, kwargs=kwargs)
        return subprocess.CompletedProcess(["git"], 0, stdout="ok\n", stderr="")

    monkeypatch.setattr(pb, "_git_run", fake)
    out = materialize._run_materializer_git(
        ["git", "-C", "/r", "status"], where="w", environment={"A": "1"}
    )
    assert out == "ok\n"
    assert seen["args"] == ("-C", "/r", "status")
    assert seen["kwargs"] == {"timeout": 120, "env": {"A": "1"}}


def test_failure_text_keeps_the_where_prefix(monkeypatch):
    monkeypatch.setattr(
        pb, "_git_run",
        lambda *a, **k: subprocess.CompletedProcess(["git"], 1, stdout="", stderr="boom\n"),
    )
    with pytest.raises(materialize.MaterializationError, match="^w failed: boom$"):
        materialize._run_materializer_git(["git", "status"], where="w")


def test_timeout_maps_to_materialization_error(monkeypatch):
    def slow(*a, **k):
        raise subprocess.TimeoutExpired("git", 120)

    monkeypatch.setattr(pb, "_git_run", slow)
    with pytest.raises(materialize.MaterializationError, match="^w failed: "):
        materialize._run_materializer_git(["git", "status"], where="w")


def test_real_git_init_and_read(tmp_path):
    repo = tmp_path / "r"
    materialize._run_materializer_git(["git", "init", "-q", str(repo)], where="init")
    assert (repo / ".git").exists()
    assert materialize._run_materializer_git(
        ["git", "-C", str(repo), "rev-parse", "--is-inside-work-tree"], where="rp"
    ).strip() == "true"
