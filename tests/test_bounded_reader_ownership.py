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


def test_callback_cannot_extend_deadline_or_release_after_expiry(tmp_path, monkeypatch):
    marker = tmp_path / "ownership.json"
    fifo = tmp_path / "read.fifo"
    os.mkfifo(fifo)
    seen = []
    pidfds = []
    released = []
    deadline = reader.Deadline(60)
    write_payload = reader._write_reader_payload

    def observe_release(descriptor, payload):
        if payload == b"R":
            released.append(payload)
        return write_payload(descriptor, payload)

    monkeypatch.setattr(reader, "_write_reader_payload", observe_release)

    def expire(ownership):
        seen.append(ownership)
        pidfds.append(os.pidfd_open(ownership.pid))
        _persist(marker, ownership)
        monkeypatch.setattr(deadline, "remaining", lambda: 0.0)

    try:
        result = reader.bounded(SECTION, lambda: _read_fifo(fifo),
                                deadline=deadline, abandoned=[],
                                on_spawn=expire, pool_identity=_scope(tmp_path))
        assert result["status"] == "timed_out" and result["started"] is True
        assert released == []
        assert marker.exists()
        _no_fifo_reader(fifo)
        exited = select.poll()
        exited.register(pidfds[0], select.POLLIN)
        assert exited.poll(60_000), "exact fixture reader did not exit"
        with suppress(ChildProcessError):
            os.waitpid(seen[0].pid, 0)
        assert _live(seen[0]) == "settled"
    finally:
        for descriptor in pidfds:
            os.close(descriptor)


def test_expired_callback_settles_without_the_sleeping_wait_helper(
        tmp_path, monkeypatch):
    def forbid_sleeping_wait(_ownership):
        raise AssertionError("callback expiry must synchronize with child exit")

    monkeypatch.setitem(globals(), "_wait_settled", forbid_sleeping_wait)
    test_callback_cannot_extend_deadline_or_release_after_expiry(
        tmp_path, monkeypatch)


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


def _hide_pidfd_apis(patch):
    patch.delattr(reader.os, "pidfd_open", raising=False)
    patch.delattr(reader.os, "P_PIDFD", raising=False)
    patch.delattr(reader.signal, "pidfd_send_signal", raising=False)


def _fallback_host():
    import platform

    if (sys.platform != "linux" or platform.machine() not in ("x86_64", "aarch64")
            or sys.byteorder != "little"):
        pytest.skip("real syscall proof requires supported 64-bit little-endian Linux")


def test_pidfd_support_failure_refuses_before_fork_and_default_still_works(monkeypatch):
    _fallback_host()
    with monkeypatch.context() as available:
        _hide_pidfd_apis(available)
        # Missing interpreter exports alone are no longer grounds for refusal.
        try:
            reader._require_pidfd_support()
        except reader.ReaderOwnershipUnavailable as exc:
            raise AssertionError("missing exports must use the Linux fallback") from exc
    with monkeypatch.context() as owned:
        _hide_pidfd_apis(owned)
        owned.setattr(reader.sys, "platform", "unsupported-fixture")

        def forbidden():
            raise AssertionError("unsupported ownership must not fork")

        owned.setattr(reader.os, "fork", forbidden)
        with pytest.raises(reader.ReaderOwnershipUnavailable, match="Linux pidfd/waitid support"):
            reader.bounded(SECTION, lambda: "must not read", deadline=reader.Deadline(5),
                           abandoned=[], on_spawn=lambda _identity: None, pool_identity="private-pool")
    with monkeypatch.context() as default:
        _hide_pidfd_apis(default)
        default.setattr(reader.sys, "platform", "unsupported-fixture")
        assert reader.bounded("default", lambda: "unchanged", deadline=reader.Deadline(5),
                              abandoned=[]) == {"status": "ok", "value": "unchanged"}


def test_pidfd_static_abi_table_per_architecture():
    import ctypes

    table = getattr(reader, "_PIDFD_ABIS", None)
    assert table is not None, "missing explicit Linux syscall/wait ABI table"
    assert set(table) == {"x86_64", "aarch64"}
    for arch, waitid in (("x86_64", 247), ("aarch64", 95)):
        abi = table[arch]
        assert (abi.pidfd_open, abi.pidfd_send_signal, abi.waitid, abi.p_pidfd) == (
            434, 424, waitid, 3)
        assert (abi.siginfo_size, abi.si_code_offset, abi.si_pid_offset,
                abi.si_status_offset) == (128, 8, 16, 24)
    # The shared LP64 wait payload is explicitly aligned; it is not libc's
    # opaque siginfo_t interpreted using an architecture-independent guess.
    assert ctypes.sizeof(reader._WaitSiginfo) == 128
    assert ctypes.alignment(reader._WaitSiginfo) == 8
    for name, offset in (("si_code", 8), ("si_pid", 16), ("si_status", 24)):
        assert getattr(reader._WaitSiginfo, name).offset == offset


@pytest.mark.parametrize("backend", ["native", "fallback"])
def test_pidfd_real_owned_child_native_and_fallback(monkeypatch, backend):
    _fallback_host()
    if backend == "native" and not all((hasattr(os, "pidfd_open"), hasattr(os, "waitid"),
                                        hasattr(os, "P_PIDFD"),
                                        hasattr(signal, "pidfd_send_signal"))):
        pytest.skip("interpreter lacks native pidfd APIs")
    assert callable(getattr(reader, "_pidfd_open", None)), "missing pidfd adapter"
    native_calls = []
    if backend == "fallback":
        _hide_pidfd_apis(monkeypatch)
    else:
        # Patch after import: the adapter must resolve native APIs at call
        # time, and must not enter libc while native exports are available.
        def track(name, function):
            def call(*args):
                native_calls.append(name)
                return function(*args)
            return call

        monkeypatch.setattr(reader.os, "pidfd_open", track("open", os.pidfd_open))
        monkeypatch.setattr(reader.os, "waitid", track("waitid", os.waitid))
        monkeypatch.setattr(reader.signal, "pidfd_send_signal",
                            track("signal", signal.pidfd_send_signal))

        def forbidden(*_args, **_kwargs):
            raise AssertionError("native APIs must be preferred over ctypes")

        monkeypatch.setattr(reader.ctypes, "CDLL", forbidden)
    # Only our child can be signalled/reaped. A pipe keeps it alive regardless
    # of scheduler timing, and pidfd readiness waits for its exit without reap.
    read_fd, write_fd = os.pipe()
    pid = os.fork()
    if pid == 0:
        os.close(write_fd)
        os.read(read_fd, 1)
        os._exit(0)
    os.close(read_fd)
    descriptor = None
    reaped = False
    try:
        descriptor = reader._pidfd_open(pid, 0)
        reader._check_pidfd_pid(descriptor, pid)
        flags = os.WEXITED | os.WNOHANG | os.WNOWAIT
        assert reader._pidfd_waitid(descriptor, flags) is None
        reader._pidfd_send_signal(descriptor, signal.SIGTERM)
        assert select.select([descriptor], [], [], 5)[0], "child did not exit"
        for _ in range(2):
            observed = reader._pidfd_waitid(descriptor, flags)
            assert (observed.si_pid, observed.si_code, observed.si_status) == (
                pid, os.CLD_KILLED, signal.SIGTERM)
        collected = reader._pidfd_waitid(descriptor, os.WEXITED | os.WNOHANG)
        reaped = True
        assert collected == observed
        with pytest.raises(ChildProcessError) as caught:
            reader._pidfd_waitid(descriptor, os.WEXITED | os.WNOHANG)
        assert caught.value.errno == errno.ECHILD
        if backend == "native":
            assert native_calls == ["open", "waitid", "signal", "waitid", "waitid",
                                    "waitid", "waitid"]
    finally:
        os.close(write_fd)
        if not reaped:
            # Closing our pipe releases only our fixture child, even on failure.
            with suppress(ChildProcessError):
                os.waitpid(pid, 0)
        if descriptor is not None:
            os.close(descriptor)


@pytest.mark.parametrize("missing_waitid", [False, True])
def test_pidfd_fallback_owned_reader_admission_liveness_and_cleanup(tmp_path, monkeypatch,
                                                                  missing_waitid):
    _fallback_host()
    _hide_pidfd_apis(monkeypatch)
    if missing_waitid:
        monkeypatch.delattr(reader.os, "waitid", raising=False)
    seen = []
    abandoned = []
    marker = tmp_path / "fallback-ownership.json"

    def persist(ownership):
        assert _live(ownership) == "running"
        _persist(marker, ownership)
        seen.append(ownership)

    try:
        result = reader.bounded(SECTION, lambda: marker.read_text(), deadline=reader.Deadline(5),
                                abandoned=abandoned, on_spawn=persist,
                                pool_identity=_scope(tmp_path))
    except reader.ReaderOwnershipUnavailable as exc:
        raise AssertionError("missing exports must not refuse a supported owned reader") from exc
    assert result["status"] == "ok"
    assert reader.ReaderOwnership.from_record(json.loads(result["value"])) == seen[0]
    _wait_settled(seen[0])
    assert abandoned == []

    # A deterministic callback failure keeps the read gated and exercises
    # descriptor signalling/reap, not merely the successful exit path.
    fifo = tmp_path / "fallback.fifo"
    os.mkfifo(fifo)

    def fail(ownership):
        persist(ownership)
        raise RuntimeError("fallback persist failure")

    marker.unlink()
    with pytest.raises(RuntimeError, match="fallback persist failure"):
        reader.bounded(SECTION, lambda: _read_fifo(fifo), deadline=reader.Deadline(5),
                       abandoned=abandoned, on_spawn=fail, pool_identity=_scope(tmp_path))
    _no_fifo_reader(fifo)
    _wait_settled(seen[1])
    assert abandoned == []

    # Exercise release followed by a blocked read and the unchanged timeout.
    marker.unlink()
    result = reader.bounded(SECTION, lambda: _read_fifo(fifo), deadline=reader.Deadline(0.3),
                            abandoned=abandoned, on_spawn=persist, pool_identity=_scope(tmp_path))
    assert result["status"] == "timed_out"
    _no_fifo_reader(fifo)
    _wait_settled(seen[2])
    assert abandoned == []


@pytest.mark.parametrize("operation", ["open", "send_signal", "waitid"])
@pytest.mark.parametrize("error", [errno.ENOSYS, errno.EPERM, errno.ESRCH, errno.ECHILD])
def test_pidfd_fallback_preserves_syscall_errno(monkeypatch, operation, error):
    import ctypes
    from types import SimpleNamespace

    _fallback_host()
    assert callable(getattr(reader, "_pidfd_open", None)), "missing pidfd adapter"
    _hide_pidfd_apis(monkeypatch)
    calls = []

    def syscall(number, *args):
        calls.append((number, args))
        ctypes.set_errno(error)
        return -1

    def libc(name, *, use_errno):
        assert name is None and use_errno is True
        return SimpleNamespace(syscall=syscall)

    monkeypatch.setattr(reader.ctypes, "CDLL", libc)
    invoke = {"open": lambda: reader._pidfd_open(123, 0),
              "send_signal": lambda: reader._pidfd_send_signal(42, signal.SIGKILL),
              "waitid": lambda: reader._pidfd_waitid(42, os.WEXITED | os.WNOHANG | os.WNOWAIT)}
    with pytest.raises(OSError) as caught:
        invoke[operation]()
    assert caught.value.errno == error
    assert isinstance(caught.value, ChildProcessError) == (error == errno.ECHILD)
    assert len(calls) == 1
    assert syscall.restype is ctypes.c_long
    assert syscall.argtypes == [ctypes.c_long]
    assert all(isinstance(arg, (ctypes.c_int, ctypes.c_uint, ctypes.c_void_p))
               for arg in calls[0][1])


@pytest.mark.parametrize("fault", ["platform", "architecture", "enosys", "eperm", "esrch",
                                   "waitid_enosys", "waitid_eperm", "signal_eperm"])
def test_pidfd_fallback_unsupported_platform_or_architecture_refuses_before_fork(monkeypatch,
                                                                               fault):
    import ctypes

    _fallback_host()
    assert callable(getattr(reader, "_pidfd_open", None)), "missing pidfd adapter"
    _hide_pidfd_apis(monkeypatch)
    if fault == "platform":
        monkeypatch.setattr(reader.sys, "platform", "unsupported-fixture")
    elif fault == "architecture":
        monkeypatch.setattr(reader.platform, "machine", lambda: "unsupported-fixture")
    else:
        real_libc = ctypes.CDLL
        real_syscall = real_libc(None, use_errno=True).syscall
        real_syscall.restype = ctypes.c_long
        real_syscall.argtypes = [ctypes.c_long]
        abi = reader._PIDFD_ABIS[reader.platform.machine()]
        number = (abi.waitid if fault.startswith("waitid_") else abi.pidfd_send_signal
                  if fault.startswith("signal_") else abi.pidfd_open)
        error = {"enosys": errno.ENOSYS, "eperm": errno.EPERM, "esrch": errno.ESRCH}[
            fault.rsplit("_", 1)[-1]]

        def denied(call_number, *args):
            if call_number == number:
                ctypes.set_errno(error)
                return -1
            return real_syscall(call_number, *args)

        from types import SimpleNamespace
        monkeypatch.setattr(reader.ctypes, "CDLL", lambda *_args, **_kwargs:
                            SimpleNamespace(syscall=denied))

    def forbidden(*_args, **_kwargs):
        raise AssertionError("unsupported admission must not fork or run callback")

    monkeypatch.setattr(reader.os, "fork", forbidden)
    with pytest.raises(reader.ReaderOwnershipUnavailable, match="Linux pidfd/waitid support") as caught:
        reader.bounded(SECTION, forbidden, deadline=reader.Deadline(5), abandoned=[],
                       on_spawn=forbidden, pool_identity="fixture")
    if fault not in ("platform", "architecture"):
        assert caught.value.__cause__.errno == error


@pytest.mark.parametrize("operation", ["signal", "waitid"])
@pytest.mark.parametrize("error", [errno.ENOSYS, errno.EPERM, errno.ESRCH, errno.ECHILD])
def test_pidfd_fallback_denied_kill_retains_unsettled_owner(tmp_path, monkeypatch, error,
                                                         operation):
    import ctypes
    from types import SimpleNamespace

    _fallback_host()
    assert callable(getattr(reader, "_pidfd_open", None)), "missing pidfd adapter"
    _hide_pidfd_apis(monkeypatch)
    read_fd, write_fd = os.pipe()
    pid = os.fork()
    if pid == 0:
        os.close(write_fd)
        os.read(read_fd, 1)
        os._exit(0)
    os.close(read_fd)
    descriptor = None
    abandoned = []
    try:
        ownership = reader._capture_ownership(pid, pool_identity=_scope(tmp_path), section=SECTION)
        descriptor = reader._pin_reader(ownership)
        real_libc = ctypes.CDLL
        real_syscall = real_libc(None, use_errno=True).syscall
        real_syscall.restype = ctypes.c_long
        real_syscall.argtypes = [ctypes.c_long]
        abi = reader._PIDFD_ABIS[reader.platform.machine()]
        denied_calls = []

        denied_number = abi.pidfd_send_signal if operation == "signal" else abi.waitid

        def denied(number, *args):
            if number == denied_number:
                denied_calls.append(args)
                ctypes.set_errno(error)
                return -1
            return real_syscall(number, *args)

        def forbidden(*_args, **_kwargs):
            raise AssertionError("uncertain cleanup must not use PID-only kill/waitpid")

        with monkeypatch.context() as fault:
            fault.setattr(reader.ctypes, "CDLL", lambda *_args, **_kwargs:
                          SimpleNamespace(syscall=denied))
            fault.setattr(reader.os, "kill", forbidden)
            fault.setattr(reader.os, "waitpid", forbidden)
            reader._stop_owned_reader(pid, descriptor, ownership, SECTION, time.monotonic(),
                                      abandoned, _scope(tmp_path))
        assert len(denied_calls) == 1
        assert len(abandoned) == 1
        assert abandoned[0]["ownership_unsettled"] is True
        assert abandoned[0]["ownership"] == ownership.to_record()
        assert _live(ownership) == "running", "denied signal must not be treated as death"
    finally:
        os.close(write_fd)
        with suppress(ChildProcessError):
            os.waitpid(pid, 0)
        if descriptor is not None:
            os.close(descriptor)


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
