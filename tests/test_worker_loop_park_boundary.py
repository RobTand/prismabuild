"""The real idle loop records its stop before sleep and never touches admission."""
from __future__ import annotations

import importlib.util
import os
from pathlib import Path
import sys

import pytest


@pytest.mark.parametrize("contents,stamp,make_root", [
    ('{"draining": true, "changed_unix": 100.5}', "100.5", True),
    ('{}', "unknown", True),
    ('{broken', "unknown", True),
    (None, "unknown", True),  # read_text on a directory raises OSError
    ('absent', "unknown", True),
    ('{"draining": true, "changed_unix": 100.5}', "100.5", False),
])
def test_park_precedes_sleep_and_queue_access(tmp_path, monkeypatch,
                                             contents, stamp, make_root):
    gate = tmp_path / "maintenance.json"
    if contents == 'absent':
        pass
    elif contents is None:
        gate.mkdir()
    else:
        gate.write_text(contents)
    parked = tmp_path / "rollout" / "parked"
    if make_root:
        parked.mkdir(parents=True)
    monkeypatch.setenv("PRISMABUILD_MAINTENANCE_GATE", str(gate))
    source = Path(__file__).resolve().parents[1] / "tools/fleet/worker_loop.py"
    spec = importlib.util.spec_from_file_location("park_boundary_loop", source)
    loop = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(loop)
    # Also point the pre-feature baseline at the private gate, so its failure
    # proves missing evidence, rather than a missing environment override.
    monkeypatch.setattr(loop, "MAINTENANCE_GATE", gate)
    monkeypatch.setattr(loop, "SH", tmp_path)
    monkeypatch.setattr(loop, "loaded_runtime_commit", lambda: "test")
    monkeypatch.setattr(loop, "published_commit", lambda: "test")
    monkeypatch.setattr(loop, "_generation_at", lambda path: "test")
    monkeypatch.setattr(loop.cpu_topology, "inherited_tiers", lambda: None)

    class UntouchableQueue:
        def __init__(self, root):
            assert root == tmp_path / "pb-queue"

        def __getattr__(self, name):
            pytest.fail(f"parked loop reached queue operation {name}")

    monkeypatch.setattr(loop.pool, "PoolQueue", UntouchableQueue)
    starttime = Path("/proc/self/stat").read_text().rpartition(")")[2].split()[19]
    expected = parked / f"{os.getpid()}-{starttime}-{stamp}"
    polls = []

    def sleeping(seconds):
        if make_root:
            assert expected.is_file(), "loop slept without leaving park evidence"
            assert expected.read_bytes() == b""
            assert list(parked.iterdir()) == [expected]
            polls.append(expected.stat().st_mtime_ns)
        else:
            assert not parked.exists()
            polls.append(None)

    monkeypatch.setattr(loop.time, "sleep", sleeping)
    monkeypatch.setattr(sys, "argv", ["worker_loop.py", "--all-cores",
                                     "--cpu-slots", "1", "--poll-s", "0"])
    assert loop._run_loop(lambda: len(polls) == 2) == 0
    assert len(polls) == 2
    assert polls[0] == polls[1], "the second poll rewrote the marker"
