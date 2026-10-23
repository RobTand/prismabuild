"""Stuck-child diagnostics are fresh procfs observations, not health readings.

Real fork/pipe/waitpid throughout. Suppressing only the parent's SIGKILL
stands in for an uninterruptible mount wait. SIGSTOP and an empty pipe keep
the child in synchronized, stable states without touching a real mount.
"""
from __future__ import annotations

import io
import os
from pathlib import Path
import signal
import sys
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import mount_latency  # noqa: E402


def _child_proc_snapshot(pid):
    # Independent oracle: production parses /proc/PID/stat, not status.
    status = Path(f"/proc/{pid}/status").read_text()
    state = next(line.split()[1] for line in status.splitlines()
                 if line.startswith("State:"))
    return state, Path(f"/proc/{pid}/wchan").read_text().strip()


def _await_child_state(pid, state):
    deadline = time.monotonic() + 10
    previous = None
    while time.monotonic() < deadline:
        current = _child_proc_snapshot(pid)
        if current[0] == state and current == previous:
            return current
        previous = current
        time.sleep(0.01)
    pytest.fail(f"child {pid} did not settle in {state}: last procfs={previous}")


@pytest.fixture
def stuck_probe(tmp_path, monkeypatch):
    read_fd, write_fd = os.pipe()
    real_kill = os.kill
    children = set()

    def controlled_probe(directory, repeats, setup):
        os.close(write_fd)
        real_kill(os.getpid(), signal.SIGSTOP)
        os.read(read_fd, 1)  # SIGCONT changes T to S, blocked on this pipe.
        return {"status": "ok"}

    def suppress_probe_kill(pid, sig):
        if sig == signal.SIGKILL:
            children.add(pid)
        else:
            real_kill(pid, sig)

    monkeypatch.setattr(os, "kill", suppress_probe_kill)
    sampler = mount_latency.MountSampler(
        tmp_path, host="stuck-child-fixture", probe=controlled_probe,
        deadline_s=0.05, repeats=1, lock_dir=tmp_path / "locks",
    )
    real_reap = sampler._reap_or_remember

    def reap_stopped_child(pid, started):
        children.add(pid)
        # Synchronize even if the child wasn't scheduled before the deadline.
        # The real bounded reaping/ownership check still runs unchanged.
        _await_child_state(pid, "T")
        real_reap(pid, started)

    monkeypatch.setattr(sampler, "_reap_or_remember", reap_stopped_child)
    try:
        yield sampler, real_kill
    finally:
        for pid in children:
            # A disappearance case already reaped this PID. Check ownership
            # before signalling, so a reused numeric PID cannot be targeted.
            try:
                if os.waitpid(pid, os.WNOHANG)[0]:
                    continue
            except ChildProcessError:
                continue
            try:
                real_kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            try:
                os.waitpid(pid, 0)
            except ChildProcessError:
                pass
        os.close(read_fd)
        os.close(write_fd)


def _assert_failed_probe_output(probe, status):
    record = {"host": "stuck-child-fixture", "probe": probe,
              "mount": {"transport": "test"}}
    line = mount_latency.one_line(record)
    assert f"probe={status}" in line.split()
    sink = io.StringIO()
    mount_latency._emit(record, sink)
    lines = sink.getvalue().splitlines()
    # Pin the full state vector/dimensions, not just the presence of a failure.
    assert [line for line in lines if line.startswith("DIMENSION ")] == [
        f"DIMENSION {name} '' absolute 1 1"
        for name in ("ok", "timed_out", "wedged", "error")
    ]
    assert [line for line in lines if line.startswith("SET ")] == [
        "SET ok = 0",
        f"SET timed_out = {int(status == 'timed_out')}",
        f"SET wedged = {int(status == 'wedged')}",
        "SET error = 0",
    ]
    assert "BEGIN prismabuild.mount_latency" not in lines
    assert "ok" not in probe  # Diagnostics must not synthesize a health value.
    return line, lines


def _assert_child_attribution(probe, status, pid, expected):
    assert probe["status"] == status
    assert probe["outstanding_pid"] == pid
    assert (probe["child_state"], probe["child_wchan"]) == expected
    diagnostic = ":".join(expected)
    line, lines = _assert_failed_probe_output(probe, status)
    assert [word for word in line.split() if word.startswith("child=")] == [
        f"child={diagnostic}"
    ]
    assert [line for line in lines if line.startswith("CLABEL stuck_child ")] == [
        f"CLABEL stuck_child '{diagnostic}' 1"
    ]


def test_timed_out_sample_names_its_stuck_child_when_still_outstanding(stuck_probe):
    sampler, _ = stuck_probe
    first = sampler._run_probe()
    pid = sampler._outstanding_pid
    assert pid is not None and pid > 0
    expected = _await_child_state(pid, "T")
    _assert_child_attribution(first, "timed_out", pid, expected)


def test_wedged_sample_names_the_stuck_child_state_and_wchan(stuck_probe):
    sampler, real_kill = stuck_probe
    first = sampler._run_probe()
    pid = sampler._outstanding_pid
    assert pid is not None and pid > 0
    stopped = _await_child_state(pid, "T")
    _assert_child_attribution(first, "timed_out", pid, stopped)
    _assert_child_attribution(sampler._run_probe(), "wedged", pid, stopped)
    assert _child_proc_snapshot(pid) == stopped

    real_kill(pid, signal.SIGCONT)
    sleeping = _await_child_state(pid, "S")
    assert sleeping != stopped
    _assert_child_attribution(sampler._run_probe(), "wedged", pid, sleeping)
    assert _child_proc_snapshot(pid) == sleeping
    # A further transition also catches reuse of the first wedged reading.
    real_kill(pid, signal.SIGSTOP)
    stopped_again = _await_child_state(pid, "T")
    _assert_child_attribution(sampler._run_probe(), "wedged", pid, stopped_again)
    assert _child_proc_snapshot(pid) == stopped_again


@pytest.mark.parametrize("status", ["timed_out", "wedged"])
def test_child_disappearing_before_diagnostic_read_is_unknown(
        stuck_probe, monkeypatch, status):
    sampler, real_kill = stuck_probe
    if status == "wedged":
        first = sampler._run_probe()
        assert first["status"] == "timed_out"
    real_status = mount_latency.process_status
    observed_pids = []

    def disappear_before_read(pid, want_wchan=True):
        # The sampler has verified outstanding ownership, but the child can
        # disappear before procfs is read. Do not fake the procfs result.
        assert pid == sampler._outstanding_pid
        assert _child_proc_snapshot(pid)[0] == "T"
        real_kill(pid, signal.SIGKILL)
        assert os.waitpid(pid, 0)[0] == pid
        assert not Path(f"/proc/{pid}").exists()
        observed_pids.append(pid)
        return real_status(pid, want_wchan=want_wchan)

    monkeypatch.setattr(mount_latency, "process_status", disappear_before_read)
    sample = sampler._run_probe()
    pid = sampler._outstanding_pid
    assert observed_pids == [pid]
    _assert_child_attribution(sample, status, pid, ("?", "?"))


@pytest.mark.parametrize("status", ["timed_out", "wedged"])
@pytest.mark.parametrize("has_pid", [False, True])
def test_legacy_stuck_records_without_diagnostics_stay_unhealthy(status, has_pid):
    probe = {"status": status}
    if has_pid:
        probe["outstanding_pid"] = 123
    # Older sampler records have no child_state/child_wchan. They must still
    # emit the failing one-hot vector, never an ok or zero-latency reading.
    _assert_failed_probe_output(probe, status)
