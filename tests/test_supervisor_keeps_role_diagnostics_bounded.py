"""#1396: regular owning ticks retain diagnostic tails, not action evidence.

These are deliberately old-source-compatible consumer tests: no retention helper
or future policy constant is imported. The real role launcher hands a private,
finite child its append descriptors. Only unrelated queue/runtime/process facts
are substituted. Run this file inside an admitted PrismaBuild action.

64 MiB is a maintenance trigger, not an all-times cap. Successful quiescent
maintenance keeps the newest 8 MiB on the same inode. Concurrent diagnostic loss,
inter-tick overshoot and partial prefix modification on late failure are allowed;
these tests do not claim atomic retention, rollback or deployment qualification.
"""
from __future__ import annotations

import errno
import fcntl
import hashlib
import json
import os
import signal
import stat
import sys
import textwrap
import time
from contextlib import contextmanager, suppress
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))
import supervise  # noqa: E402

# Pin the approved production contract independently of future implementation.
TRIGGER_BYTES = 64 * 1024 * 1024
KEEP_BYTES = 8 * 1024 * 1024
HOST = "private-role-log-box"
APPEND_MARKER = b"after-maintenance stdout\nafter-maintenance stderr\n"
SLEEP = time.sleep


class CycleComplete(Exception):
    """Leave the real regular cycle at its existing sleep boundary."""


def _wait_for(predicate, seconds=10):
    deadline = time.monotonic() + seconds
    while not predicate():
        if time.monotonic() >= deadline:
            raise AssertionError("private role child did not reach its barrier")
        SLEEP(0.01)


def _identity(path):
    info = path.stat()
    return info.st_dev, info.st_ino


def _fingerprint(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return path.stat().st_size, digest.hexdigest(), _identity(path)


# Executed only by _spawn_role during a later parent-submitted PB test action.
# Private file barriers avoid writes overlapping the successful-maintenance
# assertion; both inherited descriptors are exercised with unbuffered os.write.
CHILD = textwrap.dedent("""\
    import fcntl, json, os, sys, time
    from pathlib import Path
    control = Path(sys.argv[1])
    requested_size = int(sys.argv[2])
    tail = bytes(range(256)) * (8 * 1024 * 1024 // 256)
    remaining = max(0, requested_size - os.fstat(1).st_size)
    suffix = tail[-min(remaining, len(tail)): ] if remaining else b''
    prefix_bytes = remaining - len(suffix)
    def write_all(fd, data):
        view = memoryview(data)
        while view:
            count = os.write(fd, view)
            if count <= 0:
                raise RuntimeError('zero child write')
            view = view[count:]
    block = b'old-diagnostic-prefix\\n' * 4096
    turn = 0
    while prefix_bytes:
        piece = block[:min(prefix_bytes, len(block))]
        write_all(1 + turn % 2, piece)
        prefix_bytes -= len(piece)
        turn += 1
    if suffix:
        write_all(2, suffix)
    metadata = {}
    for fd in (1, 2):
        info = os.fstat(fd)
        metadata[str(fd)] = {'dev': info.st_dev, 'ino': info.st_ino,
                             'flags': fcntl.fcntl(fd, fcntl.F_GETFL)}
    (control / 'ready.pending').write_text(json.dumps(metadata))
    (control / 'ready.pending').rename(control / 'ready.json')
    deadline = time.monotonic() + 30
    while not (control / 'append').exists() and not (control / 'finish').exists():
        if time.monotonic() >= deadline:
            raise SystemExit(2)
        time.sleep(0.01)
    if (control / 'append').exists():
        write_all(1, b'after-maintenance stdout\\n')
        write_all(2, b'after-maintenance stderr\\n')
        (control / 'appended').write_text('done')
        while not (control / 'finish').exists():
            if time.monotonic() >= deadline:
                raise SystemExit(2)
            time.sleep(0.01)
""")


@pytest.fixture
def box(tmp_path, monkeypatch):
    """Private real config/claim/log namespace, with no live fleet queries."""
    mirror = tmp_path / "fleet"
    scripts = mirror / "repo" / "tools"
    scripts.mkdir(parents=True)
    for name in supervise.ROLE_SCRIPTS.values():
        (scripts / name).write_text(CHILD)
    config = tmp_path / "fleet_boxes.json"
    logs = tmp_path / "logs"
    logs.mkdir(mode=0o700)
    children = {}
    cycles = []

    def declare(roles):
        config.write_text(json.dumps({"boxes": {HOST: {
            "loops": 1, "args": [], "roles": roles,
        }}}))

    declare({})
    monkeypatch.setattr(supervise, "CONFIG", config)
    monkeypatch.setattr(supervise, "MIRROR", mirror)
    monkeypatch.setattr(supervise, "LOG_DIR", logs)
    monkeypatch.setattr(supervise, "CLAIM", tmp_path / "supervisor.claim")
    monkeypatch.setattr(supervise, "SYSTEMD_UNIT", tmp_path / "absent.service")
    monkeypatch.setattr(supervise.socket, "gethostname", lambda: HOST)
    monkeypatch.delenv(supervise.INHERITED_CLAIM_FD_ENV, raising=False)
    monkeypatch.setattr(supervise, "_ROLE_CHILDREN", {})
    monkeypatch.setattr(supervise, "_ROLE_REFUSALS", {})
    # Kernel/queue census facts only. No broad waitpid(-1), pgrep, shared queue
    # reads or signals against unrelated action/pytest/fleet children.
    monkeypatch.setattr(supervise, "_reap_children", lambda: 0)
    monkeypatch.setattr(supervise, "_live_loops", lambda: [os.getpid()])
    monkeypatch.setattr(supervise, "loop_args_of", lambda _pid: [])
    monkeypatch.setattr(supervise, "_has_children", lambda _pid: False)
    monkeypatch.setattr(supervise, "_claim_holders", lambda: frozenset())
    monkeypatch.setattr(supervise, "_ready_backlog", lambda: False)

    def private_census(script, proc_root=None, roots=None):
        roots = supervise._proven_roots() if roots is None else roots
        pids = [pid for role, pid in children.items()
                if supervise.ROLE_SCRIPTS[role] == script]
        for pid in pids:
            assert supervise._is_fleet_loop(pid, roots, proc_root, script)
        return pids

    monkeypatch.setattr(supervise, "_live_role_loops", private_census)

    def boundary(_seconds):
        # A real owning cycle must exclude another open-file description.
        assert supervise.CLAIM.read_text() == f"{os.getpid()}\n"
        with (supervise.CLAIM.open("r+") as contender,
              pytest.raises(BlockingIOError)):
            fcntl.flock(contender, fcntl.LOCK_EX | fcntl.LOCK_NB)
        cycles.append(True)
        raise CycleComplete

    def run_cycle():
        before = len(cycles)
        monkeypatch.setattr(sys, "argv", ["supervise", "--loops", "1"])
        monkeypatch.setattr(supervise.time, "sleep", boundary)
        with suppress(CycleComplete):
            supervise._run_supervisor(lambda: False)
        assert len(cycles) == before + 1, "the actual owner cycle must finish"

    @contextmanager
    def child(role="storage", size=TRIGGER_BYTES + 1, *, precreate=True):
        control = tmp_path / f"control-{role}"
        control.mkdir()
        args = [str(control), str(size)]
        declare({role: args})
        path = logs / f"pb-role-{role}.log"
        if precreate:
            path.touch(mode=0o600)
        pid = supervise._spawn_role(role, args)
        children[role] = pid
        reaped = False
        status = None

        def reap():
            nonlocal reaped, status
            if reaped:
                return True
            found, observed = os.waitpid(pid, os.WNOHANG)
            if found:
                reaped, status = True, observed
            return reaped

        try:
            _wait_for(lambda: (control / "ready.json").exists())
            ready = json.loads((control / "ready.json").read_text())
            for fd in ("1", "2"):
                assert (ready[fd]["dev"], ready[fd]["ino"]) == _identity(path)
                assert ready[fd]["flags"] & os.O_APPEND
            assert stat.S_ISREG(path.stat().st_mode)
            assert path.stat().st_uid == os.getuid()
            assert path.stat().st_nlink == 1
            assert not path.is_symlink()
            assert not path.stat().st_mode & 0o022
            if size:
                assert path.stat().st_size == size
            yield path, control, pid
        finally:
            (control / "finish").touch()
            try:
                _wait_for(reap, seconds=5)
            except AssertionError:
                # Exact still-unreaped private child only; never role census
                # pids, process names or fleet processes. No group signals.
                os.kill(pid, signal.SIGKILL)
                _wait_for(reap, seconds=5)
            children.pop(role, None)
            declare({})
            assert status == 0, f"private finite child exited with {status}"

    return logs, run_cycle, child


@pytest.mark.parametrize("role", ["storage", "tiers", "metrics"])
@pytest.mark.parametrize("size", [TRIGGER_BYTES, TRIGGER_BYTES + 1],
                         ids=["at-trigger", "above-trigger"])
def test_an_owning_cycle_keeps_the_newest_tail_and_inherited_append_eof(
    box, role, size,
):
    logs, run_cycle, child = box
    with child(role, size) as (path, control, _pid):
        inode = _identity(path)
        with path.open("rb") as stream:
            stream.seek(-KEEP_BYTES, os.SEEK_END)
            newest = stream.read(KEEP_BYTES)
        run_cycle()
        # Genuine old-code RED: real completed cycle leaves the real file
        # oversized, not a missing helper/import/constant or fabricated result.
        assert path.stat().st_size == KEEP_BYTES, (
            "eligible owning cycle must retain exactly the newest 8 MiB")
        assert _identity(path) == inode
        assert path.read_bytes() == newest
        (control / "append").touch()
        _wait_for(lambda: (control / "appended").exists())
        assert path.stat().st_size == KEEP_BYTES + len(APPEND_MARKER)
        assert path.read_bytes() == newest + APPEND_MARKER
        assert _identity(path) == inode
        # A restart's ordinary append reopen also reaches the compacted EOF.
        with path.open("ab") as reopened:
            reopened.write(b"reopened\n")
        expected = newest + APPEND_MARKER + b"reopened\n"
        run_cycle()
        assert path.read_bytes() == expected, "hysteresis must not recopy"
        assert _identity(path) == inode
        assert sorted(p.name for p in logs.iterdir()) == [path.name]


@pytest.mark.parametrize("size", [0, KEEP_BYTES, TRIGGER_BYTES - 1],
                         ids=["spawn-marker-only", "retained-size", "below-trigger"])
def test_in_bounds_role_diagnostics_are_unchanged(box, size):
    _logs, run_cycle, child = box
    with child(size=size) as (path, _control, _pid):
        before = _fingerprint(path)
        run_cycle()
        assert _fingerprint(path) == before


def test_missing_and_empty_role_leaves_are_not_created_or_replaced(box):
    logs, run_cycle, _child = box
    empty = logs / "pb-role-storage.log"
    empty.touch(mode=0o600)
    before = _fingerprint(empty)
    run_cycle()
    assert _fingerprint(empty) == before
    assert sorted(p.name for p in logs.iterdir()) == [empty.name]


def test_nonrole_and_action_shaped_outputs_are_outside_the_budget(box):
    logs, run_cycle, _child = box
    names = ["pb-role-other.log", "pb-role-storage.log.gz", "pb-worker-0.log",
             "pb-supervisor.log", "attempt.stdout.digest.log", "storage-custom.log"]
    paths = [logs / name for name in names]
    for path in paths:
        with path.open("wb") as stream:
            stream.write(b"private nonrole control\n")
            stream.truncate(TRIGGER_BYTES + 1)
    before = [_fingerprint(path) for path in paths]
    run_cycle()
    assert [_fingerprint(path) for path in paths] == before
    assert sorted(p.name for p in logs.iterdir()) == sorted(names)


@pytest.mark.parametrize("unsafe", ["symlink", "hardlink", "fifo", "directory",
                                    "group-writable", "world-writable", "rename"])
def test_unsafe_or_renamed_role_leaf_is_not_compacted(box, unsafe):
    logs, run_cycle, child = box
    with child() as (path, _control, _pid):
        original = path
        if unsafe in {"symlink", "fifo", "directory", "rename"}:
            original = logs / "held-writer.log"
            path.rename(original)
            if unsafe == "symlink":
                path.symlink_to(original)
            elif unsafe == "fifo":
                os.mkfifo(path, mode=0o600)
            elif unsafe == "directory":
                path.mkdir(mode=0o700)
            else:
                # A named replacement is not the inode the active role holds.
                with path.open("wb") as stream:
                    stream.write(b"replacement is not the current writer\n")
                    stream.truncate(TRIGGER_BYTES + 1)
        elif unsafe == "hardlink":
            os.link(path, logs / "second-name.log")
        else:
            path.chmod(0o620 if unsafe == "group-writable" else 0o602)
        before = _fingerprint(original)
        replacement = _fingerprint(path) if unsafe == "rename" else None
        run_cycle()
        assert _fingerprint(original) == before
        if replacement is not None:
            assert _fingerprint(path) == replacement
        if unsafe == "symlink":
            assert path.is_symlink()
        if unsafe == "fifo":
            assert stat.S_ISFIFO(path.lstat().st_mode)
        if unsafe == "directory":
            assert path.is_dir()


def test_a_symlinked_log_directory_is_not_a_maintenance_namespace(box, monkeypatch):
    logs, run_cycle, child = box
    with child() as (path, _control, _pid):
        alias = logs.parent / "log-alias"
        alias.symlink_to(logs, target_is_directory=True)
        monkeypatch.setattr(supervise, "LOG_DIR", alias)
        before = _fingerprint(path)
        run_cycle()
        assert _fingerprint(path) == before


def test_foreign_uid_metadata_refuses_only_the_candidate(box, monkeypatch):
    _logs, run_cycle, child = box
    with child() as (path, _control, _pid):
        inode = _identity(path)
        before = _fingerprint(path)
        real_stat, real_fstat = os.stat, os.fstat

        class ForeignOwner:
            def __init__(self, info):
                self.info = info
                self.st_uid = os.getuid() + 1

            def __getattr__(self, name):
                return getattr(self.info, name)

        def foreign(info):
            if (info.st_dev, info.st_ino) != inode:
                return info
            return ForeignOwner(info)  # All other real metadata unchanged.

        with monkeypatch.context() as metadata:
            metadata.setattr(supervise.os, "stat", lambda *a, **kw: foreign(real_stat(*a, **kw)))
            metadata.setattr(supervise.os, "fstat", lambda fd: foreign(real_fstat(fd)))
            run_cycle()
        assert _fingerprint(path) == before


@pytest.mark.parametrize("mode", ["claimed-elsewhere", "systemd-nonowner", "cycle-stale-once"])
def test_nonowning_invocations_do_not_trim_role_logs(box, monkeypatch, mode):
    _logs, _run_cycle, child = box
    with child() as (path, _control, _pid):
        before = _fingerprint(path)
        monkeypatch.setattr(sys, "argv", ["supervise", "--ensure"])
        if mode == "systemd-nonowner":
            supervise.SYSTEMD_UNIT.write_text(supervise.SYSTEMD_EXEC + "\n")
        elif mode == "cycle-stale-once":
            monkeypatch.setattr(sys, "argv", ["supervise", "--cycle-stale", "--once"])
            monkeypatch.setattr(supervise, "cycle_stale", lambda _published: [])
        if mode == "claimed-elsewhere":
            with supervise.CLAIM.open("a+") as owner:
                fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
                assert supervise._run_supervisor(lambda: False) == 0
        else:
            assert supervise._run_supervisor(lambda: False) == 0
        assert _fingerprint(path) == before


@pytest.mark.parametrize("fault", ["read-error", "short-read", "write-error",
                                   "partial-write-error", "truncate-error"])
def test_low_level_failure_does_not_claim_a_successful_bound(
    box, monkeypatch, capsys, fault,
):
    _logs, run_cycle, child = box
    with child() as (path, _control, _pid):
        before = _fingerprint(path)
        inode = _identity(path)
        real_pread, real_pwrite, real_truncate = os.pread, os.pwrite, os.ftruncate
        real_fstat = os.fstat
        calls = []
        truncated = []

        def candidate(fd):
            info = real_fstat(fd)
            return (info.st_dev, info.st_ino) == inode

        def pread(fd, count, offset):
            if candidate(fd) and fault in {"read-error", "short-read"}:
                calls.append("read")
                if fault == "read-error":
                    raise OSError(errno.EIO, "private injected read failure")
                return b""  # Premature EOF before the captured tail is read.
            return real_pread(fd, count, offset)

        def pwrite(fd, data, offset):
            if candidate(fd) and fault in {"write-error", "partial-write-error"}:
                calls.append("write")
                if fault == "partial-write-error" and calls.count("write") == 1:
                    return real_pwrite(fd, data[:13], offset)
                raise OSError(errno.EIO, "private injected write failure")
            return real_pwrite(fd, data, offset)

        def truncate(fd, size):
            if candidate(fd):
                truncated.append(size)
                if fault == "truncate-error":
                    calls.append("truncate")
                    raise OSError(errno.EIO, "private injected truncate failure")
            return real_truncate(fd, size)

        with monkeypatch.context() as io:
            io.setattr(supervise.os, "pread", pread)
            io.setattr(supervise.os, "pwrite", pwrite)
            io.setattr(supervise.os, "ftruncate", truncate)
            capsys.readouterr()
            run_cycle()
        assert calls, "the real owning consumer must reach the injected syscall"
        assert _identity(path) == inode
        assert path.stat().st_size == before[0], "failed copy must not truncate"
        if fault in {"read-error", "short-read", "write-error"}:
            assert _fingerprint(path) == before
        if fault != "truncate-error":
            assert truncated == [], "truncate only after the entire tail copy"
        # No rollback assertion for a partial copy or failed final truncate.
        diagnostics = capsys.readouterr()
        output = diagnostics.out + diagnostics.err
        stage = "read" if fault in {"read-error", "short-read"} else (
            "truncate" if fault == "truncate-error" else "write")
        assert "storage" in output and stage in output.lower() and (
            "fail" in output.lower() or "refus" in output.lower()
        ), "the supervisor must name the real failed maintenance stage"


def test_an_oversized_role_leaf_without_a_proven_writer_is_refused(box, capsys):
    logs, run_cycle, _child = box
    path = logs / "pb-role-storage.log"
    path.touch(mode=0o600)
    with path.open("wb") as stream:
        stream.write(b"private diagnostic with no current role PID\n")
        stream.truncate(TRIGGER_BYTES + 1)
    before = _fingerprint(path)
    capsys.readouterr()
    run_cycle()
    assert _fingerprint(path) == before
    output = capsys.readouterr().out
    assert "role storage diagnostic log failed at writer" in output
    assert "no current proven role writer" in output


def test_an_owning_once_invocation_does_not_maintain_role_logs(box, monkeypatch):
    _logs, _run_cycle, child = box
    with child() as (path, _control, _pid):
        before = _fingerprint(path)
        monkeypatch.setattr(sys, "argv", ["supervise", "--loops", "1", "--once"])
        assert supervise._run_supervisor(lambda: False) == 0
        assert _fingerprint(path) == before


def test_new_role_log_is_private_even_with_group_writable_umask(box):
    _logs, _run_cycle, child = box
    previous = os.umask(0o002)
    try:
        with child(size=0, precreate=False) as (path, _control, _pid):
            assert stat.S_IMODE(path.stat().st_mode) == 0o600
    finally:
        os.umask(previous)


def test_normal_owned_group_writable_log_directory_allows_retention(box):
    logs, run_cycle, child = box
    logs.chmod(0o775)
    with child() as (path, _control, _pid):
        inode = _identity(path)
        with path.open("rb") as stream:
            stream.seek(-KEEP_BYTES, os.SEEK_END)
            newest = stream.read(KEEP_BYTES)
        run_cycle()
        assert path.stat().st_size == KEEP_BYTES
        assert _identity(path) == inode
        assert path.read_bytes() == newest
        assert stat.S_IMODE(logs.stat().st_mode) == 0o775
