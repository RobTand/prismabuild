"""A passive gate census must prove a device alias, not guess from an inode."""

from __future__ import annotations

import io
import fcntl
import os
from pathlib import Path
import sys
import select
import subprocess
import time
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import mount_latency


@pytest.fixture
def proc_files(monkeypatch):
    """Kernel-format fdinfo/mountinfo, without accessing another process."""
    files = {}
    real_open = Path.open
    real_scan = os.scandir
    real_link = os.readlink

    def open_path(path, *args, **kwargs):
        if str(path).startswith("/proc/424"):
            value = files.get(str(path))
            if isinstance(value, Exception):
                raise value
            if value is None:
                raise FileNotFoundError(str(path))
            return (io.BytesIO(value.encode()) if args and "b" in args[0]
                    else io.StringIO(value))
        return real_open(path, *args, **kwargs)

    class Entries:
        def __init__(self, names):
            self.names = names
        def __enter__(self):
            return iter(SimpleNamespace(name=name) for name in self.names)
        def __exit__(self, *args):
            pass

    def scan(path):
        if str(path).startswith("/proc/424"):
            prefix = str(path).rstrip("/") + "/"
            return Entries([p[len(prefix):] for p in files if p.startswith(prefix)])
        return real_scan(path)

    monkeypatch.setattr(Path, "open", open_path)
    monkeypatch.setattr(os, "scandir", scan)

    def readlink(path):
        if str(path).startswith("/proc/424") or str(path) == "/proc/self/ns/mnt":
            value = files.get(str(path))
            if value is None:
                raise FileNotFoundError(str(path))
            return value
        return real_link(path)

    monkeypatch.setattr(os, "readlink", readlink)
    return files


def _descriptor(files, pid, fd, stat_device="0:31", kernel_key="00:1e:123",
                inode=123, mount_id=19, target="/home/worker/gate.lock"):
    files[f"/proc/{pid}/mountinfo"] = (
        f"{mount_id} 1 {stat_device} /home /home rw - btrfs /dev/test rw\n")
    files[f"/proc/{pid}/fdinfo/{fd}"] = (
        f"pos:\t0\nflags:\t0100002\nmnt_id:\t{mount_id}\nino:\t{inode}\n"
        f"lock:\t1: FLOCK ADVISORY WRITE {pid} {kernel_key} 0 EOF\n")
    files["/proc/self/ns/mnt"] = "mnt:[100]"
    files[f"/proc/{pid}/ns/mnt"] = "mnt:[100]"
    files[f"/proc/{pid}/fd/{fd}"] = target


def _read(rows):
    return mount_latency.read_proc_locks(
        {"00:1f:123"}, text=rows,
        paths={"00:1f:123": Path("/home/worker/gate.lock")})


def test_device_alias_recovers_holder_and_its_waiters(proc_files):
    _descriptor(proc_files, 4242, 7)
    rows = ("17: FLOCK ADVISORY WRITE 4242 00:1e:123 0 EOF\n"
            "17: -> FLOCK ADVISORY WRITE 4243 00:1e:123 0 EOF\n"
            "17: -> FLOCK ADVISORY WRITE 4244 00:1e:123 0 EOF\n")
    assert _read(rows) == {
        "00:1f:123": {"holders": [4242], "waiters": [4243, 4244]}}


def test_an_unheld_same_inode_target_does_not_adopt_another_filesystem(proc_files):
    _descriptor(proc_files, 4242, 7, stat_device="0:32",
                target="/home/other/gate.lock")
    rows = ("17: FLOCK ADVISORY WRITE 4242 00:1e:123 0 EOF\n"
            "17: -> FLOCK ADVISORY WRITE 4243 00:1e:123 0 EOF\n")
    assert _read(rows) == {
        "00:1f:123": {"holders": [], "waiters": [], "identity_unverified": True}}


def test_alias_waiters_follow_their_lock_group_not_a_colliding_key(proc_files):
    _descriptor(proc_files, 4242, 7)
    # Nested subvolumes can share mountinfo device AND inode. The fd's path
    # must still prove this is the watched subvolume's file.
    _descriptor(proc_files, 4245, 8, target="/home/other/gate.lock")
    rows = ("17: FLOCK ADVISORY WRITE 4242 00:1e:123 0 EOF\n"
            "17: -> FLOCK ADVISORY WRITE 4243 00:1e:123 0 EOF\n"
            "18: FLOCK ADVISORY WRITE 4245 00:1e:123 0 EOF\n"
            "18: -> FLOCK ADVISORY WRITE 4246 00:1e:123 0 EOF\n")
    assert _read(rows) == {
        "00:1f:123": {"holders": [4242], "waiters": [4243],
                       "identity_unverified": True}}


@pytest.mark.parametrize("unreadable", [True, False])
def test_alias_with_missing_or_unreadable_proof_names_nobody(proc_files, unreadable):
    _descriptor(proc_files, 4242, 7)
    proc_files["/proc/4242/fdinfo/7"] = (
        PermissionError("reader cannot inspect holder") if unreadable else
        "pos: 0\nflags: 0100002\nmnt_id: 999\nino: 123\n"
        "lock: 1: FLOCK ADVISORY WRITE 4242 00:1e:123 0 EOF\n")
    rows = "17: FLOCK ADVISORY WRITE 4242 00:1e:123 0 EOF\n"
    assert _read(rows) == {
        "00:1f:123": {"holders": [], "waiters": [], "identity_unverified": True}}


def test_alias_requires_the_descriptor_to_carry_this_lock(proc_files):
    _descriptor(proc_files, 4242, 7, kernel_key="00:1e:456", inode=456)
    rows = "17: FLOCK ADVISORY WRITE 4242 00:1e:123 0 EOF\n"
    assert _read(rows) == {
        "00:1f:123": {"holders": [], "waiters": [], "identity_unverified": True}}


def test_exact_device_and_inode_match_ignores_padding():
    rows = ("17: FLOCK ADVISORY WRITE 4242 0:1f:123 0 EOF\n"
            "17: -> FLOCK ADVISORY WRITE 4243 0:1f:123 0 EOF\n")
    assert mount_latency.read_proc_locks({"00:1f:123"}, text=rows) == {
        "00:1f:123": {"holders": [4242], "waiters": [4243]}}


@pytest.mark.parametrize("change", ["namespace", "deleted", "hidden_mount"])
def test_alias_rejects_a_path_that_is_not_visible_in_this_namespace(proc_files, change):
    _descriptor(proc_files, 4242, 7)
    if change == "namespace":
        proc_files["/proc/4242/ns/mnt"] = "mnt:[200]"
    elif change == "deleted":
        proc_files["/proc/4242/fd/7"] += " (deleted)"
    else:
        proc_files["/proc/4242/mountinfo"] += (
            "20 19 0:32 / /home/worker rw - btrfs /dev/other rw\n")
    assert _read("17: FLOCK ADVISORY WRITE 4242 00:1e:123 0 EOF\n") == {
        "00:1f:123": {"holders": [], "waiters": [], "identity_unverified": True}}


def test_two_holder_descriptors_with_one_kernel_key_are_ambiguous(proc_files):
    _descriptor(proc_files, 4242, 7)
    _descriptor(proc_files, 4242, 8, target="/home/other/gate.lock")
    assert _read("17: FLOCK ADVISORY WRITE 4242 00:1e:123 0 EOF\n") == {
        "00:1f:123": {"holders": [], "waiters": [], "identity_unverified": True}}


def test_duplicate_descriptors_of_the_same_lock_do_not_count_twice(proc_files):
    _descriptor(proc_files, 4242, 7)
    _descriptor(proc_files, 4242, 8)
    assert _read("17: FLOCK ADVISORY WRITE 4242 00:1e:123 0 EOF\n") == {
        "00:1f:123": {"holders": [4242], "waiters": []}}


@pytest.mark.parametrize("bound", ["MAX_ALIAS_HOLDERS", "MAX_ALIAS_FDS",
                                   "MAX_PROC_IDENTITY_BYTES"])
def test_alias_evidence_exhausting_a_scan_bound_is_unverified(proc_files, monkeypatch, bound):
    _descriptor(proc_files, 4242, 7)
    monkeypatch.setattr(mount_latency, bound, 0)
    assert _read("17: FLOCK ADVISORY WRITE 4242 00:1e:123 0 EOF\n") == {
        "00:1f:123": {"holders": [], "waiters": [], "identity_unverified": True}}


def test_unverified_alias_does_not_emit_a_healthy_zero_gate_sample():
    record = {"locks": {"present": True, "identity_complete": False,
                        "holders": 0, "waiters": 0}, "probe": {"status": "ok"}}
    out = io.StringIO()
    mount_latency._emit(record, out)
    assert "BEGIN prismabuild.admission_gate" not in out.getvalue()
    assert "BEGIN prismabuild.admission_hold" not in out.getvalue()
    assert "BEGIN prismabuild.mount_probe_state" in out.getvalue()
    assert "gate=(identity unverified)" in mount_latency.one_line(record)
    assert "0held" not in mount_latency.one_line(record)


def test_a_different_inode_never_triggers_alias_attribution(monkeypatch):
    def unexpected(pid):
        pytest.fail("unrelated inode triggered holder inspection")
    monkeypatch.setattr(mount_latency, "_holder_lock_paths", unexpected)
    assert _read("17: FLOCK ADVISORY WRITE 4242 00:1e:999 0 EOF\n") == {
        "00:1f:123": {"holders": [], "waiters": []}}


def test_unreadable_kernel_lock_table_is_not_a_complete_zero(monkeypatch):
    original = Path.read_text
    def read(path, *args, **kwargs):
        if str(path) == "/proc/locks":
            raise PermissionError("lock table unavailable")
        return original(path, *args, **kwargs)
    monkeypatch.setattr(Path, "read_text", read)
    assert mount_latency.read_proc_locks({"00:1f:123"}) == {
        "00:1f:123": {"holders": [], "waiters": [], "identity_unverified": True}}


def test_real_held_gate_and_blocked_waiter_are_counted(tmp_path):
    """Exercise real kernel rows; dl380g10's tmp_path is a btrfs subvolume."""
    gate = tmp_path / "admission.lock"
    with gate.open("w") as holder:
        fcntl.flock(holder, fcntl.LOCK_EX)
        command = [sys.executable, "-c",
                   "import fcntl,sys; f=open(sys.argv[1], 'r'); "
                   "print('ready', flush=True); fcntl.flock(f, fcntl.LOCK_EX)",
                   str(gate)]
        with subprocess.Popen(command, stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE, text=True) as waiter:
            try:
                assert select.select([waiter.stdout], [], [], 5)[0], "waiter did not start"
                assert waiter.stdout.readline().strip() == "ready"
                deadline = time.monotonic() + 5
                while True:
                    rows = Path("/proc/locks").read_text()
                    if any(" -> " in line and f" {waiter.pid} " in line
                           for line in rows.splitlines()):
                        break
                    assert time.monotonic() < deadline, "waiter did not reach flock"
                    time.sleep(0.01)
                record = mount_latency.lock_contention(tmp_path)
                print({"stat_key": mount_latency.lock_key(gate),
                       "kernel_rows": [line for line in rows.splitlines()
                                       if f" {waiter.pid} " in line or f" {os.getpid()} " in line],
                       "record": record})
                assert record["identity_complete"] is True
                assert record["holders"] == 1
                assert record["waiters"] == 1
                assert record["holder_detail"][0]["pid"] == os.getpid()
            finally:
                waiter.terminate()
                _, stderr = waiter.communicate(timeout=5)
                assert not stderr
