"""Automatic duration history follows the repository across worktrees and populations."""
from __future__ import annotations

import importlib
import json
from pathlib import Path
import subprocess

import pytest


@pytest.fixture
def history(tmp_path, monkeypatch):
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))
    return importlib.import_module("prismabuild.pbtest_history")


def completed(seconds):
    return [{"returncode": 0, "ran": True, "timed_out": False,
             "verdict": {"green": True}, "files": ["tests/test_one.py"],
             "output": json.dumps({"file_durations": {"tests/test_one.py": seconds}})}]


def test_completed_report_is_available_without_an_explicit_report_path(history, tmp_path):
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    assert history.cached_report(checkout, ["tests"]) is None

    results = completed(12)
    history.remember_report(checkout, ["tests"], results)
    report = history.cached_report(checkout, ["tests"])

    assert report is not None
    assert json.loads(report.read_text()) == results
    assert report.is_relative_to(tmp_path / "state" / "prismabuild")
    assert list(checkout.iterdir()) == []


def test_repository_history_does_not_launch_a_shard_client(history, tmp_path, monkeypatch):
    checkout = tmp_path / "checkout"
    (checkout / ".git").mkdir(parents=True)

    def unexpected_process(*args, **kwargs):
        pytest.fail("history discovery launched a process")

    monkeypatch.setattr(subprocess, "Popen", unexpected_process)
    history.remember_report(checkout, ["tests"], completed(12))

    assert history.cached_report(checkout, ["tests"]) is not None


def test_repository_history_survives_worktree_and_requested_population_changes(history, tmp_path):
    checkout = tmp_path / "checkout"
    other = tmp_path / "other"
    for repository in (checkout, other):
        subprocess.run(["git", "init", "--quiet", str(repository)], check=True)
    subprocess.run(["git", "-C", str(checkout), "-c", "user.name=Test",
                    "-c", "user.email=test@example.invalid", "commit", "--quiet",
                    "--allow-empty", "-m", "initial"], check=True)
    worktree = tmp_path / "second-worktree"
    # pb-history-ok: Creates a linked worktree from the generated temporary repository's current HEAD.
    subprocess.run(["git", "-C", str(checkout), "worktree", "add", "--quiet",
                    "--detach", str(worktree), "HEAD"], check=True)
    results = completed(12)
    history.remember_report(checkout, ["tests/test_one.py", "tests/test_old.py"], results)

    expected = history.cached_report(checkout, ["tests/test_one.py", "tests/test_old.py"])
    assert expected is not None
    assert history.cached_report(worktree, ["tests/test_one.py"]) == expected
    assert history.cached_report(worktree / "tests", ["test_one.py"]) == expected
    assert history.cached_report(worktree, ["tests/test_one.py", "tests/test_old.py",
                                             "tests/test_new.py"]) == expected
    assert json.loads(expected.read_text()) == results
    assert history.cached_report(other, ["tests/test_one.py"]) is None


def test_new_completed_report_replaces_previous_report(history, tmp_path):
    history.remember_report(tmp_path, ["tests"], completed(12))
    report = history.cached_report(tmp_path, ["tests"])
    history.remember_report(tmp_path, ["tests"], completed(8))

    assert history.cached_report(tmp_path, ["tests"]) == report
    assert json.loads(report.read_text()) == completed(8)
    assert list(report.parent.iterdir()) == [report]


@pytest.mark.parametrize("bad", [
    [], [{"returncode": 0}],
    [{**completed(1)[0], "returncode": 1}],
    [{**completed(1)[0], "ran": False}],
    [{**completed(1)[0], "verdict": {"green": False}}],
    [completed(1)[0], {**completed(1)[0], "returncode": 1}],
])
def test_failed_or_incomplete_report_preserves_completed_history(history, tmp_path, bad):
    history.remember_report(tmp_path, ["tests"], completed(12))
    report = history.cached_report(tmp_path, ["tests"])
    before = report.read_bytes()

    history.remember_report(tmp_path, ["tests"], bad)

    assert report.read_bytes() == before


def test_interrupted_replacement_preserves_prior_report_and_cleans_temporary(
        history, tmp_path, monkeypatch):
    history.remember_report(tmp_path, ["tests"], completed(12))
    report = history.cached_report(tmp_path, ["tests"])
    before = report.read_bytes()

    def failed_replace(source, target):
        assert Path(target) == report
        assert json.loads(Path(source).read_text()) == completed(8)
        assert report.read_bytes() == before
        raise OSError("injected replacement failure")

    monkeypatch.setattr(history.os, "replace", failed_replace)
    with pytest.raises(OSError, match="injected replacement failure"):
        history.remember_report(tmp_path, ["tests"], completed(8))

    assert report.read_bytes() == before
    assert list(report.parent.iterdir()) == [report]
