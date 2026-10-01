"""Completed syscall output does not establish that its child has exited.

This isolates a collector lifetime boundary, not the NFS incident's cause.
Actual fork/pipe/probe/waitpid remain real. The child's final exit is gated
only after the real private-filesystem probe and completed payload/pipe EOF.
"""
from __future__ import annotations

import os
from pathlib import Path
import select
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import mount_latency  # noqa: E402


@pytest.mark.parametrize("payload_status", ["ok", "error"])
def test_completed_payload_keeps_unreaped_child_until_actual_exit(tmp_path, monkeypatch, payload_status):
    ready_read, ready_write = os.pipe()
    release_read, release_write = os.pipe()
    original_exit = os._exit
    original_reap = mount_latency.MountSampler._reap_within
    waits = []
    pid = None

    def probe_then_gate_exit(directory, repeats, setup):
        # Runs in the actual collector child. Changing os._exit here cannot
        # change the parent interpreter or another worker's process.
        result = mount_latency.timed_probe(directory, repeats, setup)
        assert result["status"] == "ok"
        if payload_status == "error":
            result = {"status": "error", "error": "completed controlled error payload"}

        def gated_exit(code):
            os.write(ready_write, b"x")
            try:
                os.read(release_read, 1)
            finally:
                original_exit(code)

        os._exit = gated_exit
        return result

    def observed_reap(child_pid, grace):
        reaped = original_reap(child_pid, grace)
        waits.append((child_pid, reaped))
        return reaped

    monkeypatch.setattr(mount_latency.MountSampler, "_reap_within", staticmethod(observed_reap))
    sampler = mount_latency.MountSampler(
        str(tmp_path), host="private-probe-fixture", probe=probe_then_gate_exit,
        deadline_s=2.0, repeats=1, lock_dir=tmp_path / "private-locks",
    )
    try:
        first = sampler._run_probe()
        assert first["status"] == payload_status
        assert len(waits) == 1
        pid, reaped = waits[0]
        assert pid > 0 and reaped == 0, "premise: successful payload, actual child not exited"
        assert select.select([ready_read], [], [], 2.0)[0]
        assert os.read(ready_read, 1) == b"x"
        assert os.waitpid(pid, os.WNOHANG) == (0, 0)

        assert sampler._outstanding_pid == pid, (
            "completed payload was returned but the actual unreaped child was forgotten", first, pid
        )
        assert sampler._outstanding_since is not None
        second = sampler._run_probe()
        assert second["status"] == "wedged"
        assert second["outstanding_pid"] == pid
        assert len(waits) == 1, "no second probe/fork while the actual first child remains alive"

        os.write(release_write, b"x")
        assert original_reap(pid, 2.0) == pid
        sampler._reap()  # ECHILD is actual proof of prior completed reaping.
        assert sampler._outstanding_pid is None
        assert sampler._outstanding_since is None
        pid = None
    finally:
        if pid is None and waits:
            pid = waits[0][0]
        if pid is not None:
            try:
                os.write(release_write, b"x")
            except BrokenPipeError:
                pass
            # Reap only the child this exact test actually created. No live
            # daemon, unknown PID, or other agent's process is signalled.
            original_reap(pid, 2.0)
        for fd in (ready_read, ready_write, release_read, release_write):
            os.close(fd)
