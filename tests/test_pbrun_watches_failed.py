"""A failed action must reach the person who submitted it.

An action whose argv exits non-zero is retried and then filed under ``failed``,
never under ``done``.  pbrun's wait loop watched ``done`` alone, so a caller
whose suite legitimately failed sat in the loop until ``--wait-s`` expired --
a day, by default -- and was then told "gave up waiting" with exit 75.  The
work had run, three times, and said why each time; none of it reached the
person waiting, and the failure looked exactly like a pool that never
scheduled anything.  Sixty-six items were sitting in ``failed`` when this was
found.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

import pbrun


class _Queue:
    """Just enough PoolQueue for the wait loop: two terminal directories."""

    def __init__(self, root: Path):
        self.root = root
        for name in ("ready", "claimed", "done", "failed"):
            (root / name).mkdir(parents=True, exist_ok=True)

    def item_path(self, state: str, key: str) -> Path:
        return self.root / state / f"{key}.json"


def _file(queue: _Queue, state: str, key: str, payload: dict) -> None:
    queue.item_path(state, key).write_text(json.dumps(payload), encoding="utf-8")


OUTCOME = {
    "status": "failed",
    "attempts": 3,
    "finished_host": "sparky",
    "detail": {"returncode": 4, "elapsed_s": 1.0,
               "stdout": "3 failed, 1 passed\n", "stderr": "pytest said so\n"},
}


def _wait(monkeypatch, capsys, queue: _Queue, key: str, wait_s: float = 5.0):
    """Drive only pbrun's terminal-outcome half, with the queue already filed."""
    monkeypatch.setattr(pbrun, "POLL_S", 0.001, raising=False)
    return pbrun.await_outcome(queue, key, wait_s=wait_s)


def test_a_failed_action_is_reported_not_waited_out(monkeypatch, capsys, tmp_path):
    q = _Queue(tmp_path)
    _file(q, "failed", "abc", OUTCOME)
    rc = _wait(monkeypatch, capsys, q, "abc")
    out = capsys.readouterr()
    assert rc == 4, "the action's own exit status is the caller's"
    assert "3 failed, 1 passed" in out.out
    assert "pytest said so" in out.err
    assert "gave up waiting" not in out.err


def test_a_done_action_still_works(monkeypatch, capsys, tmp_path):
    q = _Queue(tmp_path)
    _file(q, "done", "abc", {"status": "executed", "finished_host": "sparky",
                             "detail": {"returncode": 0, "stdout": "ok\n"}})
    assert _wait(monkeypatch, capsys, q, "abc") == 0


def test_a_worker_exception_still_exits_non_zero(monkeypatch, capsys, tmp_path):
    """No returncode in the record is not evidence the action succeeded."""
    q = _Queue(tmp_path)
    _file(q, "failed", "abc", {"status": "failed", "attempts": 3,
                               "detail": {"exception": "LocalActionError(...)"}})
    rc = _wait(monkeypatch, capsys, q, "abc")
    assert rc == 1
    assert "LocalActionError" in capsys.readouterr().err


def test_nothing_filed_still_times_out(monkeypatch, capsys, tmp_path):
    q = _Queue(tmp_path)
    assert _wait(monkeypatch, capsys, q, "abc", wait_s=0.01) == 75
    assert "gave up waiting" in capsys.readouterr().err


def test_the_actions_own_status_is_named_when_it_is_not_the_runs(
    monkeypatch, capsys, tmp_path
):
    """The launcher exits 1 for every failure, so 1 alone is not the story.

    ``pbrun`` still returns the run's status -- that is what a caller's shell
    tests -- and says the action's beside it.
    """

    q = _Queue(tmp_path)
    _file(q, "failed", "abc", {
        "status": "failed", "attempts": 1, "finished_host": "sparky",
        "detail": {"returncode": 1, "action_returncode": 7, "elapsed_s": 1.0},
    })
    assert _wait(monkeypatch, capsys, q, "abc") == 1
    assert "rc=1 (action exited 7)" in capsys.readouterr().err


def test_a_signalled_action_is_named_by_its_signal(monkeypatch, capsys, tmp_path):
    q = _Queue(tmp_path)
    _file(q, "failed", "abc", {
        "status": "failed", "attempts": 1, "finished_host": "sparky",
        "detail": {"returncode": 1, "action_returncode": -9,
                   "action_signal": 9, "elapsed_s": 1.0},
    })
    assert _wait(monkeypatch, capsys, q, "abc") == 1
    assert "rc=1 (action killed by signal 9)" in capsys.readouterr().err


def test_an_agreeing_status_is_not_said_twice(monkeypatch, capsys, tmp_path):
    """A transport that reports the action's own status says one number."""

    q = _Queue(tmp_path)
    _file(q, "failed", "abc", {
        "status": "failed", "attempts": 1, "finished_host": "sparky",
        "detail": {"returncode": 7, "action_returncode": 7, "elapsed_s": 1.0},
    })
    assert _wait(monkeypatch, capsys, q, "abc") == 7
    assert "action exited" not in capsys.readouterr().err
