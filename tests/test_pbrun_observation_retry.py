"""A patient ``pbrun`` wait outlasts one slow terminal-record read (#558).

The FIFO is a private stand-in for a hard-NFS terminal-record read: the
directory listing succeeds, then opening the named record blocks. The tests use
the real forked reader boundary and never point at a fleet queue.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import signal
import sys
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import pbrun  # noqa: E402


KEY = "c" * 64


class Queue:
    def __init__(self, root: Path) -> None:
        self.root = root
        for state in ("done", "failed", "withdrawn"):
            (root / state).mkdir(parents=True)

    def item_path(self, state: str, key: str) -> Path:
        return self.root / state / f"{key}.json"

    def withdrawal_decisions(self, key: str, **_kwargs) -> list:
        return []


def _ending(status: str, returncode: int) -> dict:
    return {
        "action_key": KEY,
        "status": status,
        "finished_host": "worker",
        "attempts": 1,
        "detail": {"returncode": returncode, "stdout": "ok\\n", "stderr": ""},
    }


def _fast_reads(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pbrun, "OUTCOME_READ_TIMEOUT_S", 0.05)
    monkeypatch.setattr(pbrun, "POLL_S", 0.01)


def _reap_exact(children: list[int]) -> None:
    """Collect only the reader PIDs this test forked; never a name scan."""

    for pid in children:
        try:
            done, _ = os.waitpid(pid, os.WNOHANG)
            if not done:
                os.kill(pid, signal.SIGKILL)
                os.waitpid(pid, 0)
        except (ChildProcessError, ProcessLookupError):
            pass


@pytest.mark.parametrize(
    ("state", "status", "returncode", "expected"),
    [
        ("done", "executed", 0, 0),
        ("failed", "failed", 9, 9),
    ],
)
def test_patient_wait_returns_the_verdict_of_a_record_that_becomes_readable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    state: str, status: str, returncode: int, expected: int,
) -> None:
    """The first read blocks past its budget; the record is readable later."""

    queue = Queue(tmp_path / "queue")
    path = queue.item_path(state, KEY)
    os.mkfifo(path)
    readable = tmp_path / "readable.json"
    readable.write_text(json.dumps(_ending(status, returncode)), encoding="utf-8")
    stops: list[str] = []
    original_stop = pbrun.pbstatus._stop_reader

    def stop_then_land(pid, section, started, abandoned):
        # Kill and reap the blocked reader first, exactly as production does,
        # then make the record readable for the next poll.
        original_stop(pid, section, started, abandoned)
        stops.append(section)
        if len(stops) == 1:
            os.replace(readable, path)

    _fast_reads(monkeypatch)
    monkeypatch.setattr(pbrun.pbstatus, "_stop_reader", stop_then_land)
    started = time.monotonic()
    assert pbrun.await_outcome(queue, KEY, wait_s=5) == expected
    assert stops == ["pool outcome observation"]
    assert time.monotonic() - started < 5


def test_patient_wait_starts_each_reader_only_after_the_last_was_reaped(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retries are serial: no reader is ever forked beside a live one."""

    queue = Queue(tmp_path / "queue")
    os.mkfifo(queue.item_path("done", KEY))
    children: list[int] = []
    unreaped_at_fork: list[int] = []
    fork = pbrun.pbstatus.os.fork

    def tracked_fork():
        for pid in children:
            try:
                os.waitpid(pid, os.WNOHANG)
            except ChildProcessError:
                continue  # Already reaped by the reader boundary.
            unreaped_at_fork.append(pid)
        pid = fork()
        if pid:
            children.append(pid)
        return pid

    _fast_reads(monkeypatch)
    monkeypatch.setattr(pbrun.pbstatus.os, "fork", tracked_fork)
    try:
        assert pbrun.await_outcome(queue, KEY, wait_s=0.5) == \
            pbrun.RECORD_WRITE_FAILED_EXIT
        assert len(children) >= 2
        assert unreaped_at_fork == []
        for pid in children:
            with pytest.raises(ChildProcessError):
                os.waitpid(pid, os.WNOHANG)
    finally:
        _reap_exact(children)


def test_wait_that_ends_while_the_outcome_is_unavailable_says_so(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    queue = Queue(tmp_path / "queue")
    os.mkfifo(queue.item_path("done", KEY))
    _fast_reads(monkeypatch)
    assert pbrun.await_outcome(queue, KEY, wait_s=0.3) == \
        pbrun.RECORD_WRITE_FAILED_EXIT
    err = capsys.readouterr().err
    assert f"pbrun: unavailable pool outcome for {KEY[:12]} when the wait ended" in err
    assert "may already have landed" in err
    assert "pool outcome observation timed out" in err
    assert "gave up waiting" not in err


def test_unreaped_reader_gets_one_terminal_reread_then_ends_74(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """#630 narrows the guard: patience never polls beside a retained reader,
    but the wait spends one last bounded snapshot before reporting
    unobserved.  A FIFO that answers neither still ends 74 -- after exactly
    two readers and no polling loop, not one."""

    queue = Queue(tmp_path / "queue")
    os.mkfifo(queue.item_path("done", KEY))
    retained: list[int] = []
    forks: list[int] = []
    fork = pbrun.pbstatus.os.fork

    def tracked_fork():
        pid = fork()
        if pid:
            forks.append(pid)
        return pid

    def retain(pid, section, _started, abandoned):
        retained.append(pid)
        abandoned.append({"pid": pid, "section": section,
                          "starttime_ticks": pbrun.pbstatus._starttime_ticks(pid)})

    _fast_reads(monkeypatch)
    monkeypatch.setattr(pbrun.pbstatus.os, "fork", tracked_fork)
    monkeypatch.setattr(pbrun.pbstatus, "_stop_reader", retain)
    started = time.monotonic()
    try:
        assert pbrun.await_outcome(queue, KEY, wait_s=30) == \
            pbrun.RECORD_WRITE_FAILED_EXIT
        assert time.monotonic() - started < 5
        assert len(forks) == 2 and retained == forks
        err = capsys.readouterr().err
        assert "could not be reaped" in err
        assert "when the terminal re-read ended" in err
        assert "when the wait ended" not in err
    finally:
        _reap_exact(forks)


def test_zero_patience_keeps_two_bounded_reads_and_exit_74(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Guard: ``wait_s=0`` makes no polling loop; #630 adds one re-read.

    The probe is still a single immediate observation plus, only when that
    observation is unavailable, the same terminal re-read every other
    unavailable wait spends.  A FIFO that answers neither still ends 74,
    after exactly two bounded readers and no wait.
    """

    queue = Queue(tmp_path / "queue")
    os.mkfifo(queue.item_path("done", KEY))
    forks: list[int] = []
    fork = pbrun.pbstatus.os.fork

    def tracked_fork():
        pid = fork()
        if pid:
            forks.append(pid)
        return pid

    _fast_reads(monkeypatch)
    monkeypatch.setattr(pbrun.pbstatus.os, "fork", tracked_fork)
    try:
        assert pbrun.await_outcome(queue, KEY, wait_s=0) == \
            pbrun.RECORD_WRITE_FAILED_EXIT
        assert len(forks) == 2
        err = capsys.readouterr().err
        assert f"pbrun: unavailable pool outcome for {KEY[:12]}: " \
               "pool outcome observation timed out" in err
        assert "when the wait ended" not in err
    finally:
        _reap_exact(forks)
