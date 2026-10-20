"""Holder records name only a live process with this exact file open."""
from __future__ import annotations

import fcntl
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import adaptive_cpu, flock_owner, role_session


@pytest.fixture
def held(tmp_path):
    descriptor = os.open(tmp_path / "admission.lock", os.O_CREAT | os.O_RDWR, 0o600)
    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    try:
        yield descriptor
    finally:
        os.close(descriptor)


def _record(descriptor):
    return json.loads(os.pread(descriptor, 1024, 0))


def _replace(descriptor, record):
    payload = json.dumps(record).encode()
    os.ftruncate(descriptor, 0)
    os.pwrite(descriptor, payload, 0)


def test_the_real_holder_is_named_without_moving_the_file_offset(held):
    flock_owner.record_holder(held)
    os.lseek(held, 3, os.SEEK_SET)
    assert flock_owner.holder_pid(held) == os.getpid()
    assert os.lseek(held, 0, os.SEEK_CUR) == 3


def test_holder_wire_identity_and_fields_are_pinned(held):
    flock_owner.record_holder(held)
    record = _record(held)
    assert record["schema"] == "prismabuild.flock_holder.v1"
    assert set(record) == {
        "schema", "pid", "starttime", "boot_id", "pid_namespace",
        "device", "inode", "fd",
    }
    record["future_diagnostic_field"] = True
    _replace(held, record)
    assert flock_owner.holder_pid(held) == os.getpid()


def test_a_live_child_holder_is_named_and_its_exit_invalidates_the_record(tmp_path):
    path = tmp_path / "child.lock"
    source = """
import fcntl, os, sys
from prismabuild import flock_owner
fd = os.open(sys.argv[1], os.O_CREAT | os.O_RDWR, 0o600)
fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
flock_owner.record_holder(fd)
print('ready', flush=True)
sys.stdin.readline()
"""
    env = {**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[1] / "src")}
    with subprocess.Popen([sys.executable, "-c", source, str(path)], env=env,
                          stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                          text=True) as child:
        try:
            import select
            assert select.select([child.stdout], [], [], 10)[0], "holder did not start"
            assert child.stdout.readline().strip() == "ready"
            with path.open("rb") as contender:
                with pytest.raises(BlockingIOError):
                    fcntl.flock(contender, fcntl.LOCK_EX | fcntl.LOCK_NB)
                assert flock_owner.holder_pid(contender.fileno()) == child.pid
                child.communicate("exit\n", timeout=10)
                assert child.returncode == 0
                assert flock_owner.holder_pid(contender.fileno()) is None
        finally:
            if child.poll() is None:
                child.kill()
                child.wait(timeout=10)


@pytest.mark.parametrize("field", ["pid", "starttime", "boot_id", "device", "inode", "fd", "schema", "pid_namespace"])
def test_planted_stale_or_wrong_identity_names_nobody(held, field):
    flock_owner.record_holder(held)
    record = _record(held)
    record[field] = record[field] + 1 if isinstance(record[field], int) else "wrong"
    _replace(held, record)
    assert flock_owner.holder_pid(held) is None


def test_a_recycled_pid_with_the_same_file_open_names_nobody(held):
    flock_owner.record_holder(held)
    record = _record(held)
    record["starttime"] += 1
    _replace(held, record)
    assert flock_owner.holder_pid(held) is None


def test_a_descriptor_on_another_file_is_not_this_holder(held, tmp_path):
    flock_owner.record_holder(held)
    with (tmp_path / "other.lock").open("w+b") as other:
        record = _record(held)
        record["fd"] = other.fileno()
        _replace(held, record)
        assert flock_owner.holder_pid(held) is None


@pytest.mark.parametrize("payload", [b"", b"data belongs to caller", b"{}", b"null", b"[]", b"{", b"x" * 2048])
def test_unknown_or_malformed_records_never_raise_or_change_data(held, payload):
    os.pwrite(held, payload, 0)
    assert flock_owner.holder_pid(held) is None
    assert os.pread(held, 4096, 0) == payload


@pytest.mark.parametrize("descriptor", [-1, None, "closed"])
def test_bad_descriptors_are_unknown_without_raising(descriptor):
    assert flock_owner.holder_pid(descriptor) is None


@pytest.mark.parametrize("operation", ["pread", "stat", "readlink"])
def test_unreadable_identity_is_unknown(held, monkeypatch, operation):
    flock_owner.record_holder(held)
    original = getattr(flock_owner.os, operation)
    def denied(*args, **kwargs):
        target = args[0]
        identity_read = (target == held if operation == "pread" else
                         str(target).startswith(f"/proc/{os.getpid()}/")
                         or str(target) == "/proc/self/ns/pid")
        if identity_read:
            raise PermissionError("identity unavailable")
        return original(*args, **kwargs)
    monkeypatch.setattr(flock_owner.os, operation, denied)
    assert flock_owner.holder_pid(held) is None


def test_record_change_during_verification_is_unknown(held, monkeypatch):
    flock_owner.record_holder(held)
    pread = os.pread
    calls = []
    def changed(*args):
        calls.append(True)
        return pread(*args) if len(calls) == 1 else b"{}"
    monkeypatch.setattr(flock_owner.os, "pread", changed)
    assert flock_owner.holder_pid(held) is None


def test_process_identity_change_during_verification_is_unknown(held, monkeypatch):
    flock_owner.record_holder(held)
    original = flock_owner._process_starttime
    calls = []
    def changed(pid):
        calls.append(True)
        return original(pid) + (len(calls) > 1)
    monkeypatch.setattr(flock_owner, "_process_starttime", changed)
    assert flock_owner.holder_pid(held) is None


def test_diagnostic_write_failure_does_not_break_acquisition(held, monkeypatch):
    def denied(*args):
        raise OSError("read-only filesystem")
    monkeypatch.setattr(flock_owner.os, "pwrite", denied)
    flock_owner.record_holder(held)
    assert flock_owner.holder_pid(held) is None


def test_role_acquisition_publishes_the_holder_only_after_success(tmp_path, monkeypatch):
    monkeypatch.setattr(role_session, "ROLE_LOCK_ROOT", tmp_path / "roles")
    with role_session.role_singleton("test-role.py") as descriptor:
        before = os.pread(descriptor, 1024, 0)
        assert before
        assert role_session.role_singleton_holder("test-role.py") == (True, os.getpid())
        assert os.pread(descriptor, 1024, 0) == before


def test_admission_acquisition_publishes_the_holder_only_after_success(tmp_path, monkeypatch):
    monkeypatch.setattr(adaptive_cpu, "BOX_STATE_ROOT", tmp_path / "admission")
    class Ledger:
        base = tmp_path / "ledger"
    gate = adaptive_cpu.AdmissionGate(Ledger())
    with gate.locked():
        directory, digest = adaptive_cpu.box_state(Ledger.base)
        path = directory / (digest + ".lock")
        before = path.read_bytes()
        assert before
        with pytest.raises(adaptive_cpu.AdmissionBusy) as refused:
            with gate.locked():
                pytest.fail("contended admission granted")
        assert refused.value.holder == os.getpid()
        assert path.read_bytes() == before


def test_an_unrecorded_holder_still_refuses_as_unknown(held, tmp_path, monkeypatch):
    monkeypatch.setattr(role_session, "ROLE_LOCK_ROOT", tmp_path)
    with pytest.raises(role_session.RoleLockHeld) as refused:
        role_session.take_role_singleton("admission.py")
    assert refused.value.holder is None
