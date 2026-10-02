"""Parent-fenced, read-released readers; all processes/files are private fixtures.

Ownership metadata is written only by the parent. A FIFO read, not a child
metadata write, witnesses whether the child crossed the release boundary.
"""
from __future__ import annotations

import errno
import fcntl
import json
import os
import select
import signal
import subprocess
import sys
import time
from contextlib import suppress
from dataclasses import replace

import pytest

import prismabuild._bounded_reader as reader

SECTION = "ownership-fixture"
BOOT = "12345678-1234-1234-1234-123456789abc"


def _persist(path, ownership):
    # Model the caller's protected LOCAL storage obligation, including its
    # directory entry. The core neither writes nor deletes this fence.
    with path.open("xb") as stream:
        stream.write(json.dumps(ownership.to_record()).encode())
        stream.flush()
        os.fsync(stream.fileno())
    descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _scope(tmp_path):
    return str(tmp_path / "private-pool-identity")


def _live(ownership, **kwargs):
    return reader.reader_liveness(ownership, pool_identity=ownership.pool_identity,
                                  section=ownership.section, **kwargs)


def _read_fifo(path):
    with path.open("rb", buffering=0) as stream:
        return stream.read().decode()


def _no_fifo_reader(path):
    with pytest.raises(OSError) as caught:
        descriptor = os.open(path, os.O_WRONLY | os.O_NONBLOCK)
        os.close(descriptor)
    assert caught.value.errno == errno.ENXIO


def _wait_settled(ownership):
    until = time.monotonic() + 3
    while time.monotonic() < until:
        if _live(ownership) == "settled":
            return
        time.sleep(0.01)
    pytest.fail("exact fixture reader did not settle")


def test_default_reader_has_no_ownership_hook_or_extra_pipe(monkeypatch):
    pipes = []
    original = os.pipe

    def pipe():
        pair = original()
        pipes.append(pair)
        return pair

    def forbidden(*_args, **_kwargs):
        raise AssertionError("default readers must not capture ownership")

    monkeypatch.setattr(reader.os, "pipe", pipe)
    monkeypatch.setattr(reader, "_capture_ownership", forbidden)
    result = reader.bounded("default", lambda: "unchanged",
                            deadline=reader.Deadline(5), abandoned=[])
    assert result == {"status": "ok", "value": "unchanged"}
    assert len(pipes) == 1
    for descriptor in pipes[0]:
        with pytest.raises(OSError):
            os.fstat(descriptor)


def test_parent_persists_exact_identity_before_child_read(tmp_path):
    marker = tmp_path / "ownership.json"
    source = tmp_path / "read-source.json"
    pool = _scope(tmp_path)
    seen = []
    parent = os.getpid()

    def persist(ownership):
        assert os.getpid() == parent
        assert _live(ownership) == "running"
        seen.append(ownership)
        _persist(marker, ownership)
        # This input exists only after ownership is durable. The child must
        # not enter its read while persistence is in progress.
        source.write_text(marker.read_text())

    result = reader.bounded(SECTION, lambda: source.read_text(),
                            deadline=reader.Deadline(5), abandoned=[],
                            on_spawn=persist, pool_identity=pool)
    assert result["status"] == "ok"
    ownership = reader.ReaderOwnership.from_record(json.loads(result["value"]))
    assert ownership == seen[0]
    assert ownership.pid != parent and ownership.pool_identity == pool
    assert ownership.section == SECTION
    assert json.loads(marker.read_text()) == ownership.to_record()
    _wait_settled(ownership)
    assert marker.exists(), "only the fence consumer may retire durable ownership"


@pytest.mark.parametrize("failure", [RuntimeError("partial persist"), KeyboardInterrupt()])
def test_callback_failure_never_releases_read_or_discards_partial_marker(tmp_path, failure):
    marker = tmp_path / "partial.json"
    fifo = tmp_path / "read.fifo"
    os.mkfifo(fifo)
    seen = []

    def fail(ownership):
        seen.append(ownership)
        marker.write_text("{")
        raise failure

    with pytest.raises(type(failure)):
        reader.bounded(SECTION, lambda: _read_fifo(fifo),
                       deadline=reader.Deadline(5), abandoned=[],
                       on_spawn=fail, pool_identity=_scope(tmp_path))
    assert marker.read_text() == "{"
    _no_fifo_reader(fifo)
    _wait_settled(seen[0])


@pytest.mark.parametrize("signum", [signal.SIGINT, signal.SIGTERM])
def test_parent_signal_during_persistence_reaps_without_releasing_read(tmp_path, signum):
    marker = tmp_path / "ownership.json"
    fifo = tmp_path / "read.fifo"
    os.mkfifo(fifo)
    seen = []

    def cancel(ownership):
        seen.append(ownership)
        _persist(marker, ownership)
        os.kill(os.getpid(), signum)

    expected = KeyboardInterrupt if signum == signal.SIGINT else SystemExit
    with pytest.raises(expected) as caught:
        reader.bounded(SECTION, lambda: _read_fifo(fifo),
                       deadline=reader.Deadline(5), abandoned=[],
                       on_spawn=cancel, pool_identity=_scope(tmp_path))
    if signum == signal.SIGTERM:
        assert caught.value.code == 128 + signum
    assert marker.exists()
    _no_fifo_reader(fifo)
    _wait_settled(seen[0])


def test_unprovable_spawn_identity_cannot_run_callback_or_read(tmp_path, monkeypatch):
    fifo = tmp_path / "read.fifo"
    os.mkfifo(fifo)

    def unavailable(*_args, **_kwargs):
        raise reader.ReaderOwnershipUnavailable("fixture starttime unavailable")

    def forbidden(_ownership):
        raise AssertionError("unproved identity cannot authorize persistence")

    monkeypatch.setattr(reader, "_capture_ownership", unavailable)
    abandoned = []
    try:
        with pytest.raises(reader.ReaderOwnershipUnavailable, match="starttime unavailable"):
            reader.bounded(SECTION, lambda: _read_fifo(fifo),
                           deadline=reader.Deadline(5), abandoned=abandoned,
                           on_spawn=forbidden, pool_identity=_scope(tmp_path))
        _no_fifo_reader(fifo)
        assert len(abandoned) == 1 and abandoned[0]["ownership_unsettled"] is True
        assert abandoned[0]["starttime_ticks"] is None
    finally:
        for child in abandoned:
            # The fixture directly owns these forked children. Production
            # deliberately has no PID-only reap fallback on uncertainty.
            os.waitpid(child["pid"], 0)


def test_callback_cannot_extend_deadline_or_release_after_expiry(tmp_path):
    marker = tmp_path / "ownership.json"
    fifo = tmp_path / "read.fifo"
    os.mkfifo(fifo)
    seen = []

    def expire(ownership):
        seen.append(ownership)
        _persist(marker, ownership)
        time.sleep(2.1)

    result = reader.bounded(SECTION, lambda: _read_fifo(fifo),
                            deadline=reader.Deadline(2), abandoned=[],
                            on_spawn=expire, pool_identity=_scope(tmp_path))
    assert result["status"] == "timed_out" and result["started"] is True
    assert marker.exists()
    _no_fifo_reader(fifo)
    _wait_settled(seen[0])


def test_partial_callback_failure_retains_original_proved_identity(tmp_path, monkeypatch):
    marker = tmp_path / "partial.json"
    seen = []
    abandoned = []
    def fail(ownership):
        seen.append(ownership)
        marker.write_text("{")
        raise RuntimeError("partial persistence")

    # Model an unreapable cleanup, not an actual fleet/NFS fault. Its original
    # pre-release identity must survive even if a later identity read is lost.
    monkeypatch.setattr(reader, "_reap_pidfd_within", lambda *_args: (False, None))
    monkeypatch.setattr(reader, "_starttime_ticks", lambda _pid: None)
    try:
        with pytest.raises(RuntimeError, match="partial persistence"):
            reader.bounded(SECTION, lambda: "must not read",
                           deadline=reader.Deadline(5), abandoned=abandoned,
                           on_spawn=fail, pool_identity=_scope(tmp_path))
        assert len(abandoned) == 1
        assert abandoned[0]["ownership"] == seen[0].to_record()
        assert abandoned[0]["starttime_ticks"] == seen[0].starttime_ticks
        assert marker.read_text() == "{"
    finally:
        for child in abandoned:
            pid, _ = os.waitpid(child["pid"], 0)
            assert pid == child["pid"], "collect only this fixture's owned child"


def test_pidfd_support_failure_refuses_before_fork_and_default_still_works(monkeypatch):
    with monkeypatch.context() as owned:
        owned.delattr(reader.signal, "pidfd_send_signal")

        def forbidden():
            raise AssertionError("unsupported ownership must not fork")

        owned.setattr(reader.os, "fork", forbidden)
        with pytest.raises(reader.ReaderOwnershipUnavailable, match="Linux pidfd/waitid support"):
            reader.bounded(SECTION, lambda: "must not read", deadline=reader.Deadline(5),
                           abandoned=[], on_spawn=lambda _identity: None, pool_identity="private-pool")
    with monkeypatch.context() as default:
        default.delattr(reader.signal, "pidfd_send_signal")
        assert reader.bounded("default", lambda: "unchanged", deadline=reader.Deadline(5),
                              abandoned=[]) == {"status": "ok", "value": "unchanged"}


@pytest.mark.parametrize("fault", ["emfile", "echild", "identity_changed"])
def test_pidfd_failure_never_releases_or_falls_back_to_pid_only_operations(tmp_path, monkeypatch,
                                                                        fault):
    fifo = tmp_path / "read.fifo"
    os.mkfifo(fifo)
    abandoned = []
    calls = []
    real_capture = reader._capture_ownership

    def capture(*args, **kwargs):
        result = real_capture(*args, **kwargs)
        calls.append(result)
        # Initial capture, before-open capture, then after-open capture.
        if fault == "identity_changed" and len(calls) == 3:
            return replace(result, starttime_ticks=result.starttime_ticks + 1)
        return result

    def forbidden(*_args, **_kwargs):
        raise AssertionError("unproved ownership must not use a PID-only operation or callback")

    def failed_open(*_args):
        raise OSError(errno.EMFILE, "fixture pidfd descriptor limit")

    def nonchild(*_args):
        raise ChildProcessError(errno.ECHILD, "fixture relationship unavailable")

    with monkeypatch.context() as proof:
        proof.setattr(reader, "_capture_ownership", capture)
        proof.setattr(reader.os, "kill", forbidden)
        proof.setattr(reader.os, "waitpid", forbidden)
        if fault == "emfile":
            proof.setattr(reader.os, "pidfd_open", failed_open)
        elif fault == "echild":
            proof.setattr(reader.os, "waitid", nonchild)
        try:
            with pytest.raises(reader.ReaderOwnershipUnavailable):
                reader.bounded(SECTION, lambda: _read_fifo(fifo), deadline=reader.Deadline(5),
                               abandoned=abandoned, on_spawn=forbidden,
                               pool_identity=_scope(tmp_path))
            _no_fifo_reader(fifo)
            if fault in ("emfile", "echild"):
                assert len(abandoned) == 1 and abandoned[0]["ownership_unsettled"] is True
                assert abandoned[0]["ownership"] == calls[0].to_record()
        finally:
            # Restore only the fixture's PID-only cleanup functions, never
            # use them to infer production ownership or admit another reader.
            proof.undo()
            for child in abandoned:
                os.waitpid(child["pid"], 0)


def test_foreign_pidfd_cannot_authorize_or_signal_a_different_child(tmp_path, monkeypatch):
    foreign_read, foreign_write = os.pipe()
    foreign = os.fork()
    if foreign == 0:
        os.close(foreign_write)
        try:
            os.read(foreign_read, 1)
        finally:
            os._exit(0)
    os.close(foreign_read)
    actual_open = os.pidfd_open
    abandoned = []
    callback_calls = []

    def wrong_pidfd(_pid, _flags):
        return actual_open(foreign, 0)

    try:
        with monkeypatch.context() as fault:
            fault.setattr(reader.os, "pidfd_open", wrong_pidfd)
            with pytest.raises(reader.ReaderOwnershipUnavailable, match="expected reader PID"):
                reader.bounded(SECTION, lambda: "must not read", deadline=reader.Deadline(5),
                               abandoned=abandoned, on_spawn=callback_calls.append,
                               pool_identity=_scope(tmp_path))
        assert callback_calls == []
        assert os.waitpid(foreign, os.WNOHANG) == (0, 0), "foreign child was signalled or reaped"
        for child in abandoned:
            os.waitpid(child["pid"], 0)
    finally:
        os.close(foreign_write)
        with suppress(ChildProcessError):
            os.waitpid(foreign, 0)


def test_owned_reader_isolates_lock_fds_and_closes_release_pipe_before_read(tmp_path):
    marker = tmp_path / "ownership.json"
    lock = tmp_path / "parent.lock"
    descriptor = os.open(lock, os.O_CREAT | os.O_RDWR, 0o600)
    fcntl.flock(descriptor, fcntl.LOCK_EX)
    seen = []

    def persist(ownership):
        seen.append(ownership)
        _persist(marker, ownership)

    def read():
        descriptors = {}
        for name in os.listdir("/proc/self/fd"):
            with suppress(OSError):
                descriptors[name] = os.readlink(f"/proc/self/fd/{name}")
        return descriptors

    try:
        result = reader.bounded(SECTION, read, deadline=reader.Deadline(5),
                                abandoned=[], on_spawn=persist, pool_identity=_scope(tmp_path))
        assert result["status"] == "ok"
        descriptors = result["value"]
        assert all(descriptors[str(fd)] == os.devnull for fd in (0, 1, 2))
        extra = [value for key, value in descriptors.items() if int(key) > 2]
        assert len(extra) == 1 and extra[0].startswith("pipe:")
        assert str(lock) not in descriptors.values()
        _wait_settled(seen[0])
    finally:
        os.close(descriptor)


def _read_report(descriptor):
    raw = b""
    until = time.monotonic() + 5
    while b"\n" not in raw:
        left = until - time.monotonic()
        assert left > 0 and select.select([descriptor], [], [], left)[0]
        chunk = os.read(descriptor, 4096)
        assert chunk, "fixture owner died without reporting its exact reader"
        raw += chunk
    return reader.ReaderOwnership.from_record(json.loads(raw.split(b"\n")[0]))


@pytest.mark.parametrize("phase", ["before_persist", "before_release", "after_release"])
def test_parent_death_read_boundary_and_orphan_echild_is_not_death(tmp_path, phase):
    marker = tmp_path / "ownership.json"
    fifo = tmp_path / "read.fifo"
    os.mkfifo(fifo)
    report_read, report_write = os.pipe()
    parent = os.fork()
    if parent == 0:
        os.close(report_read)

        def persist(ownership):
            os.write(report_write, json.dumps(ownership.to_record()).encode() + b"\n")
            if phase == "before_persist":
                os._exit(31)
            _persist(marker, ownership)
            if phase == "before_release":
                os._exit(32)

        try:
            reader.bounded(SECTION, lambda: _read_fifo(fifo),
                           deadline=reader.Deadline(30), abandoned=[],
                           on_spawn=persist, pool_identity=_scope(tmp_path))
        finally:
            os._exit(33)
    os.close(report_write)
    ownership = None
    writer = None
    try:
        ownership = _read_report(report_read)
        if phase == "after_release":
            until = time.monotonic() + 5
            while writer is None and time.monotonic() < until:
                try:
                    writer = os.open(fifo, os.O_WRONLY | os.O_NONBLOCK)
                except OSError as exc:
                    assert exc.errno == errno.ENXIO
                    time.sleep(0.01)
            assert writer is not None, "reader never crossed the durable release boundary"
            assert reader.ReaderOwnership.from_record(json.loads(marker.read_text())) == ownership
            os.kill(parent, signal.SIGKILL)
            os.waitpid(parent, 0)
            with pytest.raises(ChildProcessError):
                os.waitpid(ownership.pid, os.WNOHANG)
            # It is an actual live orphan, not our waitable child. A restarted
            # observer must keep its durable fence despite ECHILD.
            assert _live(ownership) == "running"
        else:
            os.waitpid(parent, 0)
            _wait_settled(ownership)
            _no_fifo_reader(fifo)
            assert marker.exists() is (phase == "before_release")
    finally:
        os.close(report_read)
        with suppress(ChildProcessError, ProcessLookupError):
            os.kill(parent, signal.SIGKILL)
            os.waitpid(parent, 0)
        if ownership is not None and _live(ownership) == "running":
            # The writer stays open so this exact fixture orphan cannot finish
            # its FIFO read and recycle its PID between proof and cleanup.
            os.kill(ownership.pid, signal.SIGKILL)
            _wait_settled(ownership)
        if writer is not None:
            os.close(writer)


def _fixture_proc(tmp_path, state="S", ticks=123):
    proc = tmp_path / "private-proc"
    boot = proc / "sys/kernel/random/boot_id"
    boot.parent.mkdir(parents=True)
    boot.write_text(BOOT + "\n")
    process = proc / "8765"
    process.mkdir()
    fields = [state] + ["0"] * 19
    fields[19] = str(ticks)
    (process / "stat").write_text("8765 (fixture (reader)) " + " ".join(fields))
    ownership = reader.ReaderOwnership(pid=8765, starttime_ticks=123,
                                       host=reader.socket.gethostname(), boot_id=BOOT,
                                       pool_identity="private-pool", section=SECTION)
    return proc, ownership


@pytest.mark.parametrize("case,expected", [
    ("same", "running"), ("d_state", "running"), ("pid_reuse", "settled"),
    ("zombie", "settled"), ("new_boot", "settled"), ("bad_boot", "unknown"),
    ("bad_stat", "unknown"), ("negative_ticks", "unknown"), ("over_limit", "unknown"),
    ("wrong_host", "unknown"), ("wrong_pool", "unknown"), ("wrong_section", "unknown"),
    ("unreadable", "unknown"), ("missing_boot", "unknown"),
])
def test_persistent_fence_liveness_never_uses_waitpid_as_authority(tmp_path, monkeypatch,
                                                                case, expected):
    proc, ownership = _fixture_proc(tmp_path,
                                    state="D" if case == "d_state" else "Z" if case == "zombie" else "S",
                                    ticks=124 if case == "pid_reuse" else -1 if case == "negative_ticks" else 123)
    pool, section = ownership.pool_identity, ownership.section
    if case == "new_boot":
        (proc / "sys/kernel/random/boot_id").write_text("12345678-1234-1234-1234-123456789abd")
    elif case == "bad_boot":
        (proc / "sys/kernel/random/boot_id").write_text("unknown")
    elif case == "bad_stat":
        (proc / "8765/stat").write_text("incomplete")
    elif case == "over_limit":
        (proc / "8765/stat").write_text("x" * 4097)
    elif case == "wrong_host":
        monkeypatch.setattr(reader.socket, "gethostname", lambda: "another-host")
    elif case == "wrong_pool":
        pool = "another-pool"
    elif case == "wrong_section":
        section = "another-read"
    elif case == "unreadable":
        def unavailable(*_args):
            raise PermissionError("private fixture identity unreadable")
        monkeypatch.setattr(reader, "_local_identity_bytes", unavailable)
    elif case == "missing_boot":
        def missing(_proc):
            raise FileNotFoundError("private fixture boot identity missing")
        monkeypatch.setattr(reader, "_boot_id", missing)

    def forbidden(*_args):
        raise AssertionError("ECHILD cannot prove a persisted orphan has exited")

    monkeypatch.setattr(reader.os, "waitpid", forbidden)
    assert reader.reader_liveness(ownership, pool_identity=pool, section=section, proc=proc) == expected


def test_owned_release_pipe_survives_low_standard_descriptors(tmp_path):
    program = r'''
import json, os, sys
from pathlib import Path
from prismabuild import _bounded_reader as reader
marker = Path(sys.argv[1])
def persist(ownership):
    with marker.open("xb") as stream:
        stream.write(json.dumps(ownership.to_record()).encode())
        stream.flush()
        os.fsync(stream.fileno())
    directory = os.open(marker.parent, os.O_RDONLY | os.O_DIRECTORY)
    os.fsync(directory)
    os.close(directory)
os.close(0)
os.close(1)
result = reader.bounded("low-fds", lambda: "released", deadline=reader.Deadline(5),
                        abandoned=[], on_spawn=persist, pool_identity="private-pool")
sys.stderr.write(json.dumps(result))
'''
    completed = subprocess.run([sys.executable, "-c", program, str(tmp_path / "ownership.json")],
                               capture_output=True, text=True, timeout=15)
    assert completed.returncode == 0, completed.stderr
    assert json.loads(completed.stderr) == {"status": "ok", "value": "released"}


@pytest.mark.parametrize("budget,pool", [(0, "private-pool"), (5, None), (5, "")])
def test_owned_read_requires_a_finite_budget_and_exact_pool_scope(budget, pool):
    with pytest.raises(ValueError, match="finite budget and exact pool/read scope"):
        reader.bounded(SECTION, lambda: "must not read", deadline=reader.Deadline(budget),
                       abandoned=[], on_spawn=lambda _ownership: None, pool_identity=pool)
