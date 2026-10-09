"""A wedged sample names the syscall its stuck child waits in.

Refs #1398: the 2026-09-30 sparklina mount timeouts needed a manual procfs
read (``rpc_wait_bit_killable`` in the NFS open path) to separate a genuine
mount stall from a producer fault. The wedged record already names the
outstanding pid; it must also carry the kernel's state and wchan for that
pid, read at the wedged sample's own timestamp.

Real fork/pipe/waitpid throughout. The child sleeps instead of touching the
mount, and the test suppresses only the parent's SIGKILL, which stands in
for the uninterruptible wait a real NFS stall puts the child in.
"""
from __future__ import annotations

import io
import os
import time
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import mount_latency  # noqa: E402


def _sleeping_probe(directory, repeats, setup):
    time.sleep(30)
    return {"status": "ok"}


def test_wedged_sample_names_the_stuck_child_state_and_wchan(tmp_path, monkeypatch):
    real_kill = os.kill
    monkeypatch.setattr(os, "kill", lambda pid, sig: None)
    sampler = mount_latency.MountSampler(
        str(tmp_path), host="wedged-child-fixture", probe=_sleeping_probe,
        deadline_s=0.3, repeats=1, lock_dir=tmp_path / "locks",
    )
    pid = None
    try:
        first = sampler._run_probe()
        assert first["status"] == "timed_out"
        pid = sampler._outstanding_pid
        assert pid is not None and pid > 0

        second = sampler._run_probe()
        assert second["status"] == "wedged"
        assert second["outstanding_pid"] == pid
        # Red before the fix: the wedged record names the pid but not what
        # the kernel says that pid waits in.
        assert isinstance(second["child_state"], str) and second["child_state"]
        assert isinstance(second["child_wchan"], str) and second["child_wchan"]
        assert second["child_state"] in ("S", "D", "R", "T", "t")

        line = mount_latency.one_line({
            "host": "wedged-child-fixture",
            "probe": second,
            "mount": {"transport": "test"},
        })
        assert "child=" in line

        sink = io.StringIO()
        mount_latency._emit({"probe": second}, sink)
        assert "SET wedged = 1" in sink.getvalue()
    finally:
        if pid is not None:
            try:
                real_kill(pid, 9)
            except OSError:
                pass
            try:
                os.waitpid(pid, 0)
            except (ChildProcessError, OSError):
                pass
