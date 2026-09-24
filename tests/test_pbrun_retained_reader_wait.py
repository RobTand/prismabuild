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
import re
import socket
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
        self.forked_at: list[float] = []
        self.reaped: set[int] = set()
        self.peak = 0
        fork = pbrun.pbstatus.os.fork
        waitpid = pbrun.pbstatus.os.waitpid

        def tracked_fork():
            pid = fork()
            if pid:
                self.forked.append(pid)
                self.forked_at.append(time.monotonic())
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


# --------------------------------------------------------------------------
# #1048: the paths the review of #1039 found unpinned, and its nits
# --------------------------------------------------------------------------

def _hold_one_reader(monkeypatch, readers: Readers, *, index: int,
                     release_at: float | None = None, calls: int | None = None,
                     on_release=None) -> None:
    """``_reap_within`` fails for the ``index``-th forked reader only.

    It holds that reader until ``release_at``, or for its first ``calls``
    reap attempts; every other reader is reaped as usual, so the high-water
    mark counts only what the wait itself started.
    """

    real = pbrun.pbstatus._reap_within
    seen = {"calls": 0, "released": False}

    def reap_within(pid, grace_s):
        if len(readers.forked) > index and pid == readers.forked[index]:
            seen["calls"] += 1
            held = ((release_at is not None and time.monotonic() < release_at)
                    or (calls is not None and seen["calls"] <= calls)
                    or (release_at is None and calls is None))
            if held:
                return False
            if not seen["released"]:
                seen["released"] = True
                if on_release is not None:
                    on_release()
        return real(pid, grace_s)

    monkeypatch.setattr(pbrun.pbstatus, "_reap_within", reap_within)


def test_a_failed_reader_that_cannot_be_reaped_still_ends_at_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The failed-and-unreaped split: not a retained timeout, so no wait.

    A reader that raised has delivered its EOF, so it is exiting, not stuck
    in a read. It stays plain ``OutcomeReadUnavailable``: one terminal re-read,
    then 74, however much ``--wait-s`` is left.
    """

    queue = Queue(tmp_path / "queue")

    def broken(*_args, **_kwargs):
        raise RuntimeError("the mount answered EIO")

    monkeypatch.setattr(pbrun, "outcome_poll", broken)
    readers = Readers(monkeypatch)
    monkeypatch.setattr(pbrun.pbstatus, "_reap_within", lambda pid, grace_s: False)
    try:
        started = time.monotonic()
        assert pbrun.await_outcome(queue, KEY, wait_s=5) == \
            pbrun.RECORD_WRITE_FAILED_EXIT
        assert time.monotonic() - started < 3
        err = capsys.readouterr().err
        assert "failed (RuntimeError: the mount answered EIO) and its reader " \
            "could not be reaped" in err
        assert "when the terminal re-read ended" in err
        assert "still retained" not in err
        assert len(readers.forked) == 2
    finally:
        readers.cleanup()


def test_a_retained_verification_reader_is_waited_out_too(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The ending has landed; its verification read is what gets stuck."""

    queue = Queue(tmp_path / "queue")
    queue.item_path("done", KEY).write_text(json.dumps(_ending()), encoding="utf-8")
    stuck = tmp_path / "attempt.fifo"
    os.mkfifo(stuck)
    readable = tmp_path / "readable"
    real_render = pbrun._outcome_render_value

    def render(q, outcome_path, outcome):
        # Runs in the forked reader: blocks as an NFS read of the attempt
        # record would, until the "mount" recovers.
        if not readable.exists():
            stuck.read_bytes()
        return real_render(q, outcome_path, outcome)

    monkeypatch.setattr(pbrun, "_outcome_render_value", render)
    monkeypatch.setattr(pbrun, "OUTCOME_READ_TIMEOUT_S", 0.05)
    monkeypatch.setattr(pbrun, "POLL_S", 0.5)
    readers = Readers(monkeypatch)
    _hold_one_reader(monkeypatch, readers, index=1,
                     release_at=time.monotonic() + 1.0,
                     on_release=readable.touch)
    try:
        assert pbrun.await_outcome(queue, KEY, wait_s=60) == 0
        out, err = capsys.readouterr()
        assert "7 passed" in out
        assert "pool outcome verification timed out" in err
        assert readers.peak == 1
        assert len(readers.forked) >= 4
    finally:
        readers.cleanup()


def test_a_reader_reaped_at_the_deadline_starts_no_new_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Reaped at (or just before) the deadline: 74 "when the wait ended".

    Whether the last reap lands just before the deadline or at it, the wait
    has no time left for a read, so it starts none.
    """

    queue = Queue(tmp_path / "queue")
    os.mkfifo(queue.item_path("done", KEY))
    monkeypatch.setattr(pbrun, "OUTCOME_READ_TIMEOUT_S", 0.05)
    readers = Readers(monkeypatch)
    started = time.monotonic()
    _hold_one_reader(monkeypatch, readers, index=0, release_at=started + 0.9)
    try:
        assert pbrun.await_outcome(queue, KEY, wait_s=1.0) == \
            pbrun.RECORD_WRITE_FAILED_EXIT
        err = capsys.readouterr().err
        assert "when the wait ended" in err
        assert "still retained when --wait-s ran out" not in err
        assert len(readers.forked) == 1
        assert readers.forked[0] in readers.reaped
    finally:
        readers.cleanup()


def test_the_retained_reader_notice_keeps_its_interval(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The in-wait notice is throttled, however often the reap is tried."""

    queue = Queue(tmp_path / "queue")
    record = queue.item_path("done", KEY)
    os.mkfifo(record)

    def land():
        record.unlink()
        record.write_text(json.dumps(_ending()), encoding="utf-8")

    interval = 0.3
    monkeypatch.setattr(pbrun, "OUTCOME_READ_TIMEOUT_S", 0.05)
    monkeypatch.setattr(pbrun, "POLL_S", 0.02)
    monkeypatch.setattr(pbrun, "UNAVAILABLE_NOTICE_INTERVAL_S", interval)
    readers = Readers(monkeypatch)
    started = time.monotonic()
    _kernel_holds_the_reader_until(
        monkeypatch, release_at=started + 1.5, on_release=land)
    try:
        assert pbrun.await_outcome(queue, KEY, wait_s=60) == 0
        elapsed = time.monotonic() - started
        err = capsys.readouterr().err
        notices = err.count("its reader is still retained after")
        assert 1 <= notices <= elapsed / interval + 1
    finally:
        readers.cleanup()


def test_the_read_after_a_reap_waits_out_the_poll_interval(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """N1: a reaped reader's retry keeps the reaped-timeout path's cadence."""

    queue = Queue(tmp_path / "queue")
    record = queue.item_path("done", KEY)
    os.mkfifo(record)

    def land():
        record.unlink()
        record.write_text(json.dumps(_ending()), encoding="utf-8")

    monkeypatch.setattr(pbrun, "OUTCOME_READ_TIMEOUT_S", 0.05)
    monkeypatch.setattr(pbrun, "POLL_S", 1.0)
    readers = Readers(monkeypatch)
    _hold_one_reader(monkeypatch, readers, index=0,
                     release_at=time.monotonic() + 0.1, on_release=land)
    try:
        assert pbrun.await_outcome(queue, KEY, wait_s=60) == 0
        assert len(readers.forked) >= 2
        assert readers.forked_at[1] - readers.forked_at[0] >= 0.9
    finally:
        readers.cleanup()


def test_the_final_74_states_the_seconds_and_the_host_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """N2: the last line says how long the reader was waited on."""

    queue = Queue(tmp_path / "queue")
    os.mkfifo(queue.item_path("done", KEY))
    monkeypatch.setattr(pbrun, "OUTCOME_READ_TIMEOUT_S", 0.05)
    readers = Readers(monkeypatch)
    _kernel_holds_the_reader_until(monkeypatch, release_at=None)
    try:
        assert pbrun.await_outcome(queue, KEY, wait_s=0.6) == \
            pbrun.RECORD_WRITE_FAILED_EXIT
        err = capsys.readouterr().err
        final = [line for line in err.splitlines()
                 if "still retained when --wait-s ran out" in line]
        assert len(final) == 1
        assert re.search(r"still retained when --wait-s ran out, "
                         r"\d+\.\d+s after its read timed out", final[0])
        assert final[0].count(socket.gethostname()) == 1
        assert final[0].startswith(f"pbrun: unavailable pool outcome for {KEY[:12]}")
    finally:
        readers.cleanup()


def test_a_delivered_snapshot_reader_is_reaped_later_not_left_a_zombie(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """N7: the #630 snapshot reader missed its reap; the wait collects it."""

    queue = Queue(tmp_path / "queue")
    queue.item_path("done", KEY).write_text(json.dumps(_ending()), encoding="utf-8")
    readers = Readers(monkeypatch)
    # Its two reap attempts inside ``pbstatus`` miss (a starved parent).
    _hold_one_reader(monkeypatch, readers, index=0, calls=2)
    try:
        assert pbrun.await_outcome(queue, KEY, wait_s=60) == 0
        err = capsys.readouterr().err
        assert "using the delivered snapshot" in err
        assert readers.forked[0] in readers.reaped
    finally:
        readers.cleanup()


def test_a_waiting_client_names_its_retained_reader_once_not_per_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """L1: a pool read leaves the naming to its caller's exception.

    ``pbstatus``'s own line was not throttled, and a waiting client now meets
    a retained reader once per turn. The exception carries the same JSON,
    and the client prints it at its own interval.
    """

    fifo = tmp_path / "stuck.fifo"
    os.mkfifo(fifo)
    readers = Readers(monkeypatch)
    _kernel_holds_the_reader_until(monkeypatch, release_at=None)
    try:
        with pytest.raises(pbrun.OutcomeReaderRetained) as raised:
            pbrun._bounded_pool_read("pool outcome observation",
                                     fifo.read_bytes, budget_s=0.05)
        assert f'"pid": {readers.forked[0]}' in str(raised.value)
        assert "pbstatus: retained reader" not in capsys.readouterr().err
    finally:
        readers.cleanup()


def test_pbstatus_itself_still_names_a_retained_reader(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Guard: ``pbstatus``'s own sections keep the line (its default)."""

    fifo = tmp_path / "stuck.fifo"
    os.mkfifo(fifo)
    readers = Readers(monkeypatch)
    _kernel_holds_the_reader_until(monkeypatch, release_at=None)
    abandoned: list = []
    try:
        result = pbrun.pbstatus.bounded(
            "queue root", fifo.read_bytes,
            deadline=pbrun.pbstatus.Deadline(0.05), abandoned=abandoned)
        assert result["status"] == "timed_out"
        assert len(abandoned) == 1
        assert "pbstatus: retained reader" in capsys.readouterr().err
    finally:
        readers.cleanup()
