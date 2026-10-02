"""#1214: private reader failures must explain their cause, never a blank EOF.

No fleet queue, live mount, container, or other process is touched. Signals
below target only the reader child forked by the admitted test process.
"""
from __future__ import annotations

import errno
import os
import signal

import pytest

import pbstatus


def observe(read):
    abandoned = []
    result = pbstatus.bounded(
        "queue-discovery", read, deadline=pbstatus.Deadline(2.0),
        abandoned=abandoned,
    )
    assert not abandoned
    return result


@pytest.mark.parametrize("exc", [SystemExit(73), KeyboardInterrupt("interrupted")])
def test_base_exception_retains_cause_and_exit_status(exc):
    def fail():
        raise exc

    result = observe(fail)
    assert result["status"] == "error"
    assert result["type"] == type(exc).__name__
    assert str(exc) in result["error"]
    assert "stage=read" in result["error"]
    assert "exit code 1" in result["error"]


def test_setup_failure_retains_cause(monkeypatch):
    def fail(_fd):
        raise OSError(errno.EMFILE, "fixture descriptor limit")

    monkeypatch.setattr(pbstatus._reader, "_isolate_child_fds", fail)
    result = observe(lambda: "must not run")
    assert result["status"] == "error"
    assert result["type"] == "OSError"
    assert "fixture descriptor limit" in result["error"]
    assert "stage=isolate_fds" in result["error"]
    assert "exit code 1" in result["error"]


def test_signal_death_is_not_an_anonymous_empty_payload():
    def die():
        os.kill(os.getpid(), signal.SIGKILL)

    result = observe(die)
    assert result["status"] == "error"
    assert "without a payload" in result["error"]
    assert "signal 9 (SIGKILL)" in result["error"]


def test_bare_exit_retains_exit_code():
    result = observe(lambda: os._exit(37))
    assert result["status"] == "error"
    assert "exit code 37" in result["error"]


def test_ordinary_exception_keeps_type_and_message():
    def fail():
        raise ValueError("fixture invalid queue record")

    result = observe(fail)
    assert result["status"] == "error"
    assert result["type"] == "ValueError"
    assert "fixture invalid queue record" in result["error"]
    assert "exit code 1" in result["error"]


def test_failed_write_can_report_its_cause(monkeypatch):
    real_write = os.write
    calls = 0

    def fail_once(fd, data):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OSError(errno.EIO, "fixture IPC write failure")
        return real_write(fd, data)

    monkeypatch.setattr(pbstatus.os, "write", fail_once)
    result = observe(lambda: "payload")
    assert result["status"] == "error"
    assert "fixture IPC write failure" in result["error"]
    assert "stage=write" in result["error"]
    assert "exit code 1" in result["error"]


def test_permanently_broken_writer_is_nonzero(monkeypatch):
    def fail(*_args):
        raise OSError(errno.EIO, "fixture permanent IPC failure")

    monkeypatch.setattr(pbstatus.os, "write", fail)
    result = observe(lambda: "payload")
    assert result["status"] == "error"
    assert "exit code 1" in result["error"]


def test_short_writes_deliver_the_whole_payload(monkeypatch):
    real_write = os.write

    def short_write(fd, data):
        return real_write(fd, data[:4096])

    monkeypatch.setattr(pbstatus.os, "write", short_write)
    payload = "x" * 700_000
    assert observe(lambda: payload) == {"status": "ok", "value": payload}


def test_interrupted_write_retries(monkeypatch):
    real_write = os.write
    calls = 0

    def interrupt_once(fd, data):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise InterruptedError()
        return real_write(fd, data)

    monkeypatch.setattr(pbstatus.os, "write", interrupt_once)
    assert observe(lambda: ["ready"]) == {"status": "ok", "value": ["ready"]}


def test_zero_byte_write_refuses_without_spinning(monkeypatch):
    real_write = os.write
    calls = 0

    def zero_once(fd, data):
        nonlocal calls
        calls += 1
        return 0 if calls == 1 else real_write(fd, data)

    monkeypatch.setattr(pbstatus.os, "write", zero_once)
    result = observe(lambda: "payload")
    assert result["status"] == "error"
    assert "made no progress" in result["error"]
    assert "stage=write" in result["error"]


def test_serialization_failure_is_distinguished_from_read_failure():
    result = observe(object)
    assert result["status"] == "error"
    assert result["type"] == "TypeError"
    assert "stage=serialize" in result["error"]
    assert "exit code 1" in result["error"]


def test_broken_exception_string_does_not_erase_its_type():
    class BrokenText(BaseException):
        def __str__(self):
            raise RuntimeError("cannot stringify")

    def fail():
        raise BrokenText()

    result = observe(fail)
    assert result["status"] == "error"
    assert result["type"] == "BrokenText"
    assert "exception message unavailable" in result["error"]
    assert "exit code 1" in result["error"]


def test_already_reaped_exit_is_unknown_not_success(monkeypatch):
    def missing(*_args):
        raise ChildProcessError()

    monkeypatch.setattr(pbstatus.os, "waitpid", missing)
    assert pbstatus._reap_status_within(123, 0.01) == (True, None)
    assert pbstatus._reap_within(123, 0.01) is True
    assert "status unavailable" in pbstatus._reader_exit_detail(123, None)


def test_success_shape_is_unchanged():
    assert observe(lambda: ["ready"]) == {"status": "ok", "value": ["ready"]}
