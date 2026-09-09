"""The maintenance poll must not lose work or launch overlapping repair agents."""
import fcntl
import importlib.util
import json
from pathlib import Path
import subprocess
from types import SimpleNamespace

import pytest


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("watch_issues", ROOT / "tools/maintenance/watch_issues.py")
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


@pytest.fixture
def runner(tmp_path, monkeypatch):
    prompt = tmp_path / "prompt.md"
    prompt.write_text("Use PB for every test and GPU job.")
    state_dir = tmp_path / "state"
    calls = []

    def agent(argv, **kwargs):
        calls.append((argv, kwargs))
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(MODULE.subprocess, "run", agent)
    return SimpleNamespace(
        run=lambda: MODULE.run_once(tmp_path, state_dir, prompt),
        state_dir=state_dir, calls=calls,
        state=lambda: json.loads((state_dir / "state.json").read_text()),
    )


def test_empty_queue_never_starts_agent(runner, monkeypatch):
    monkeypatch.setattr(MODULE, "issues", lambda: {})
    assert runner.run() == 0
    assert runner.calls == []
    assert runner.state()["open_issues"] == []


def test_new_issue_starts_agent_and_checks_actual_remaining_queue(runner, monkeypatch):
    snapshots = iter([{"196": "first"}, {}])
    monkeypatch.setattr(MODULE, "issues", lambda: next(snapshots))
    assert runner.run() == 0
    argv, kwargs = runner.calls[0]
    assert argv[:2] == ["codex", "exec"]
    assert "--json" in argv and argv[-1] == "-"
    assert "#196" in kwargs["input"]
    assert "Use PB" in kwargs["input"]
    assert runner.state()["open_issues"] == []


def test_unresolved_issue_is_not_reported_closed_or_repeated_immediately(runner, monkeypatch):
    monkeypatch.setattr(MODULE, "issues", lambda: {"196": "first"})
    assert runner.run() == 0
    assert runner.state()["open_issues"] == ["196"]
    assert runner.run() == 0
    assert len(runner.calls) == 1


def test_new_issue_during_run_remains_eligible_without_repeating_visited_issue(runner, monkeypatch):
    snapshots = iter([{"196": "first"}, {"196": "updated", "200": "new"},
                      {"196": "updated", "200": "new"}, {}])
    monkeypatch.setattr(MODULE, "issues", lambda: next(snapshots))
    runner.run()
    runner.run()
    assert len(runner.calls) == 2
    assert "#200" in runner.calls[1][1]["input"]
    assert "#196" not in runner.calls[1][1]["input"]


def test_own_update_does_not_rearm_next_poll(runner, monkeypatch):
    snapshots = iter([{"196": "first"}, {"196": "agent-comment"},
                      {"196": "agent-comment"}, {"196": "agent-comment"}])
    monkeypatch.setattr(MODULE, "issues", lambda: next(snapshots))
    assert runner.run() == 0
    assert runner.run() == 0
    assert len(runner.calls) == 1
    assert runner.state()["open_issues"] == ["196"]


def test_update_after_completed_run_remains_immediately_eligible(runner, monkeypatch):
    snapshots = iter([{"196": "first"}, {"196": "agent-comment"},
                      {"196": "later-update"}, {"196": "later-update"}])
    monkeypatch.setattr(MODULE, "issues", lambda: next(snapshots))
    runner.run()
    runner.run()
    assert len(runner.calls) == 2
    assert "#196" in runner.calls[1][1]["input"]


def test_unchanged_blocker_is_revisited_after_six_hours(runner, monkeypatch):
    clock = [100000]
    monkeypatch.setattr(MODULE.time, "time", lambda: clock[0])
    monkeypatch.setattr(MODULE, "issues", lambda: {"196": "first"})
    runner.run()
    clock[0] += 21601
    runner.run()
    assert len(runner.calls) == 2


def test_api_error_does_not_claim_queue_is_empty(runner, monkeypatch):
    def unavailable():
        raise subprocess.CalledProcessError(1, "gh")
    monkeypatch.setattr(MODULE, "issues", unavailable)
    with pytest.raises(subprocess.CalledProcessError):
        runner.run()
    assert runner.calls == []
    assert not (runner.state_dir / "state.json").exists()


def test_failed_agent_retries_on_next_poll(runner, monkeypatch):
    monkeypatch.setattr(MODULE, "issues", lambda: {"196": "first"})
    calls = []
    def failure(*args, **kwargs):
        calls.append(args)
        return SimpleNamespace(returncode=1)
    monkeypatch.setattr(MODULE.subprocess, "run", failure)
    assert runner.run() == 1
    assert runner.run() == 1
    assert len(calls) == 2
    assert runner.state()["open_issues"] == ["196"]


def test_lock_prevents_overlapping_agent_and_poll(runner, monkeypatch):
    def unexpected():
        raise AssertionError("A second run must not even poll GitHub")
    monkeypatch.setattr(MODULE, "issues", unexpected)
    runner.state_dir.mkdir()
    with (runner.state_dir / "lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert runner.run() == 0
    assert runner.calls == []


def test_github_pagination_excludes_prs_but_not_agent_authors(monkeypatch):
    def api(argv, **kwargs):
        assert "--paginate" in argv and "--slurp" in argv
        assert "repos/RobTand/prismabuild/issues?state=open&per_page=100" in argv
        return SimpleNamespace(stdout=json.dumps([
            [{"number": 1, "updated_at": "a", "user": {"login": "RobTand"}},
             {"number": 2, "updated_at": "b", "pull_request": {}}],
            [{"number": 3, "updated_at": "c", "user": {"login": "bot"}}],
        ]))
    monkeypatch.setattr(MODULE.subprocess, "run", api)
    assert MODULE.issues() == {"1": "a", "3": "c"}
