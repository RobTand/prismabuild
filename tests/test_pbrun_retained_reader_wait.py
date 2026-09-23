"""A patient wait outlasts an outcome reader stuck in the kernel (#1033).

On 2026-09-23 four shards of one broad ``pbtest`` run read red with exit 74
while their actions had landed rc 0: each shard's outcome reader timed out,
could not be reaped because it was still inside an NFS read, and the wait
ended at once with hours of ``--wait-s`` left. These tests stand in for that
reader with a FIFO, which blocks the read, and a reap that fails until the
"kernel" lets go. They use the real forked reader and never touch a fleet
queue.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
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


def _ending() -> dict:
    return {
        "action_key": KEY,
        "status": "executed",
        "finished_host": "worker",
        "attempts": 1,
        "detail": {"returncode": 0, "stdout": "7 passed in 0.1s\\n", "stderr": ""},
    }


class Readers:
    """Tracks this test's outcome readers: forked, reaped and the peak alive."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.forked: list[int] = []
        self.reaped: set[int] = set()
        self.peak = 0
        fork = pbrun.pbstatus.os.fork
        waitpid = pbrun.pbstatus.os.waitpid

        def tracked_fork():
            pid = fork()
            if pid:
                self.forked.append(pid)
                self.peak = max(self.peak, len(self.alive()))
            return pid

        def tracked_waitpid(pid, options):
            done, status = waitpid(pid, options)
            if done:
                self.reaped.add(done)
            return done, status

        monkeypatch.setattr(pbrun.pbstatus.os, "fork", tracked_fork)
        monkeypatch.setattr(pbrun.pbstatus.os, "waitpid", tracked_waitpid)

    def alive(self) -> list[int]:
        return [pid for pid in self.forked if pid not in self.reaped]

    def cleanup(self) -> None:
        for pid in self.alive():
            try:
                os.kill(pid, 9)
                os.waitpid(pid, 0)
            except (ChildProcessError, ProcessLookupError):
                pass


def _kernel_holds_the_reader_until(monkeypatch, *, release_at: float | None,
                                   on_release=None) -> None:
    """``_reap_within`` fails until ``release_at``, as for a reader in ``D``.

    The killed reader is really dead (a FIFO open is interruptible), so after
    ``release_at`` the real ``_reap_within`` reaps it; ``on_release`` runs once
    at that moment. ``release_at=None`` never releases it.
    """

    real = pbrun.pbstatus._reap_within
    released = []

    def reap_within(pid, grace_s):
        if release_at is None or time.monotonic() < release_at:
            return False
        if not released:
            released.append(True)
            if on_release is not None:
                on_release()
        return real(pid, grace_s)

    monkeypatch.setattr(pbrun.pbstatus, "_reap_within", reap_within)


def test_a_patient_wait_outlasts_a_reader_stuck_in_the_kernel(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The ending lands while the reader is stuck; the wait reports it (0).

    Before #1033 the wait ended with 74 at the first retained reader: its
    terminal re-read met the same stuck mount and was retained too.
    """

    queue = Queue(tmp_path / "queue")
    record = queue.item_path("done", KEY)
    os.mkfifo(record)

    def land():
        # The mount recovers: the FIFO that blocked every read is replaced by
        # the action's real ending, as the worker had written it all along.
        record.unlink()
        record.write_text(json.dumps(_ending()), encoding="utf-8")

    monkeypatch.setattr(pbrun, "OUTCOME_READ_TIMEOUT_S", 0.05)
    readers = Readers(monkeypatch)
    _kernel_holds_the_reader_until(
        monkeypatch, release_at=time.monotonic() + 2.0, on_release=land)
    try:
        assert pbrun.await_outcome(queue, KEY, wait_s=60) == 0
        out, err = capsys.readouterr()
        assert "7 passed" in out
        assert "reader has exited and was reaped" in err
        # One stuck reader, then the read that found the ending, then its
        # verification: never two at once.
        assert readers.peak == 1
        assert len(readers.forked) >= 2
    finally:
        readers.cleanup()


def test_a_long_wait_on_a_retained_reader_is_not_silent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """While the reader stays in the kernel, the wait says so at its interval."""

    queue = Queue(tmp_path / "queue")
    record = queue.item_path("done", KEY)
    os.mkfifo(record)

    def land():
        record.unlink()
        record.write_text(json.dumps(_ending()), encoding="utf-8")

    monkeypatch.setattr(pbrun, "OUTCOME_READ_TIMEOUT_S", 0.05)
    monkeypatch.setattr(pbrun, "UNAVAILABLE_NOTICE_INTERVAL_S", 0.3)
    readers = Readers(monkeypatch)
    _kernel_holds_the_reader_until(
        monkeypatch, release_at=time.monotonic() + 1.5, on_release=land)
    try:
        assert pbrun.await_outcome(queue, KEY, wait_s=60) == 0
        err = capsys.readouterr().err
        assert "its reader is still retained after" in err
        assert f'"pid": {readers.forked[0]}' in err
        assert readers.peak == 1
    finally:
        readers.cleanup()


def test_a_reader_still_retained_at_the_deadline_ends_the_wait_naming_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The deadline comes first: exit 74, the reader named, no second reader."""

    queue = Queue(tmp_path / "queue")
    os.mkfifo(queue.item_path("done", KEY))
    monkeypatch.setattr(pbrun, "OUTCOME_READ_TIMEOUT_S", 0.05)
    readers = Readers(monkeypatch)
    _kernel_holds_the_reader_until(monkeypatch, release_at=None)
    try:
        started = time.monotonic()
        assert pbrun.await_outcome(queue, KEY, wait_s=1.0) == pbrun.RECORD_WRITE_FAILED_EXIT
        assert time.monotonic() - started < 1.0 + pbrun.POLL_S
        err = capsys.readouterr().err
        assert "still retained when --wait-s ran out" in err
        assert f'"pid": {readers.forked[0]}' in err
        assert "no second reader was started" in err
        assert len(readers.forked) == 1
        assert readers.peak == 1
    finally:
        readers.cleanup()


def test_a_retained_reader_is_its_own_exception_and_still_unavailable() -> None:
    """Every ``except OutcomeReadUnavailable`` keeps catching it (exit 74)."""

    exc = pbrun.OutcomeReaderRetained("stuck", [{"pid": 1, "starttime_ticks": 2}])
    assert isinstance(exc, pbrun.OutcomeReadUnavailable)
    assert not isinstance(exc, pbrun.OutcomeObservationTimedOut)
    assert exc.retained == [{"pid": 1, "starttime_ticks": 2}]
