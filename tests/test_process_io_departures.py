"""Observed I/O must survive departures without duplicating inherited work."""
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import resource_scope


@pytest.mark.parametrize("frames,totals", [
    # The whole three-generation tree departs between reads.
    ([[("10:100", 1, 10), ("11:101", 10, 20), ("12:102", 11, 30)], []], [60, 60]),
    # A surviving grandparent inherits both departed generations exactly once.
    ([[("10:100", 1, 10), ("11:101", 10, 20), ("12:102", 11, 30)],
      [("10:100", 1, 60)], []], [60, 60, 60]),
    # Two roots: one departs wholly, the other reaps its child and survives.
    ([[("10:100", 1, 10), ("11:101", 10, 20),
       ("20:100", 1, 30), ("21:101", 20, 40)],
      [("20:100", 1, 70)], []], [100, 100, 100]),
    # A new process reusing the parent's PID cannot inherit the old child.
    ([[("10:100", 1, 10), ("11:101", 10, 20)],
      [("10:200", 1, 5)], []], [30, 35, 35]),
    # A child survives its parent, is reparented, then exits.
    ([[("10:100", 1, 10), ("11:101", 10, 20)],
      [("11:101", 1, 25)], []], [30, 35, 35]),
    # Historical membership of an absent parent is not evidence of a reap.
    ([[("11:101", 99, 20)], []], [20, 20]),
])
def test_departed_subtrees_preserve_observed_counters(tmp_path, monkeypatch, frames, totals):
    scope = resource_scope.ResourceScope("a" * 64, "b" * 32, 1 << 30,
                                         tmp_path / "telemetry.json")
    scope.cgroup_path = tmp_path
    scope._process_io = {"members": [99]}
    current = {}
    monkeypatch.setattr(resource_scope, "scope_pids", lambda *a, **k: list(current))
    monkeypatch.setattr(resource_scope, "_process_in_scope", lambda *a: False)
    monkeypatch.setattr(resource_scope, "read_process_io", lambda pid: current[pid])
    for frame, expected in zip(frames, totals):
        current.clear()
        for identity, parent, value in frame:
            current[int(identity.split(":")[0])] = (
                identity, parent, {name: value for name in resource_scope.IO_COUNTERS})
        record = scope.sample_process_io()
        for name in resource_scope.IO_COUNTERS:
            assert record[name] == expected, record
        assert not record.get("errors"), record
    # Sampling the same empty scope again must not retire a subtree twice.
    assert not current
    assert scope.sample_process_io()["wchar"] == totals[-1]
