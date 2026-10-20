"""Mount diagnostics retain names but never present unknown waiters as health."""
from __future__ import annotations

import fcntl
import io
import os
import select
import subprocess
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))
from prismabuild import flock_owner
import mount_latency


@pytest.fixture
def gate(tmp_path):
    with (tmp_path / "gate.lock").open("w+b") as holder:
        fcntl.flock(holder, fcntl.LOCK_EX | fcntl.LOCK_NB)
        flock_owner.record_holder(holder.fileno())
        yield holder


def test_live_record_names_the_holder_but_waiter_identity_is_incomplete(tmp_path, gate):
    record = mount_latency.lock_contention(tmp_path)
    assert record["present"] is True
    assert record["identity_complete"] is False
    assert record["holders"] == 1
    assert record["holder_detail"][0]["pid"] == os.getpid()
    assert record["waiters"] == 0
    assert record["waiter_states"] == {}
    assert record["waiter_wchans"] == {}
    assert record["attributed_waiters"] == 0
    assert record["waiters_invisible_to_load"] == 0


def test_exited_holder_does_not_leave_a_named_holder(tmp_path):
    source = """
import fcntl, os, sys
from prismabuild import flock_owner
fd = os.open(sys.argv[1], os.O_CREAT | os.O_RDWR, 0o600)
fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
flock_owner.record_holder(fd)
print('ready', flush=True)
sys.stdin.readline()
"""
    env = {**os.environ, "PYTHONPATH": str(ROOT / "src")}
    with subprocess.Popen([sys.executable, "-c", source, str(tmp_path / "gate.lock")],
                          env=env, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                          text=True) as child:
        try:
            assert select.select([child.stdout], [], [], 10)[0], "holder did not start"
            assert child.stdout.readline().strip() == "ready"
            assert mount_latency.lock_contention(tmp_path)["holders"] == 1
            child.communicate("exit\n", timeout=10)
            assert child.returncode == 0
            record = mount_latency.lock_contention(tmp_path)
            assert record["identity_complete"] is False
            assert record["holders"] == 0
        finally:
            if child.poll() is None:
                child.kill()
                child.wait(timeout=10)


def test_unknown_data_is_not_rewritten_by_a_passive_census(tmp_path):
    path = tmp_path / "gate.lock"
    path.write_bytes(b"data belonging to an older writer")
    record = mount_latency.lock_contention(tmp_path)
    assert record["identity_complete"] is False
    assert record["holders"] == 0
    assert path.read_bytes() == b"data belonging to an older writer"


def _assert_no_healthy_gate_sample(record):
    out = io.StringIO()
    mount_latency._emit(record, out)
    assert "BEGIN prismabuild.admission_gate" not in out.getvalue()
    assert "BEGIN prismabuild.admission_hold" not in out.getvalue()
    assert "BEGIN prismabuild.mount_probe_state" in out.getvalue()
    assert "gate=(identity unverified)" in mount_latency.one_line(record)
    assert "0held" not in mount_latency.one_line(record)


def test_unverified_identity_does_not_emit_a_healthy_zero_gate_sample():
    record = {"locks": {"present": True, "identity_complete": False,
                        "holders": 0, "waiters": 0}, "probe": {"status": "ok"}}
    _assert_no_healthy_gate_sample(record)


def test_planted_complete_zero_fails_the_incomplete_identity_guard():
    record = {"locks": {"present": True, "identity_complete": True,
                        "holders": 0, "waiters": 0}, "probe": {"status": "ok"}}
    with pytest.raises(AssertionError):
        _assert_no_healthy_gate_sample(record)
