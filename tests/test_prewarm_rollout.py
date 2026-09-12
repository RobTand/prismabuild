"""Storage readers must leave the queue before maintenance or runtime rotation."""
from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools/fleet"))
import worker_loop


class CycleBoundary(Exception):
    pass


def load(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / "tools/fleet" / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def role(tmp_path, monkeypatch):
    loop = load("prewarm_loop")
    gate = tmp_path / "maintenance.json"
    gate.write_text(json.dumps({"draining": False}))
    parked = tmp_path / "rollout/parked"
    parked.mkdir(parents=True)
    monkeypatch.setattr(worker_loop, "MAINTENANCE_GATE", gate)
    monkeypatch.setattr(worker_loop, "PARKED_ROOT", parked)
    loaded, current = tmp_path / "loaded.json", tmp_path / "current.json"
    for path in (loaded, current):
        path.write_text(json.dumps({"generation": "old", "commit": "a" * 40}))
    monkeypatch.setattr(worker_loop, "GENERATION_VERSION", loaded)
    monkeypatch.setattr(worker_loop, "RUNTIME_VERSION", current)
    calls = []
    monkeypatch.setattr(loop, "cycle", lambda *a, **k: calls.append(True) or {})
    monkeypatch.setattr(loop.time, "sleep", lambda _: (_ for _ in ()).throw(CycleBoundary))
    # Private queue and explicit disks: this exercises the non-dry entrypoint
    # boundary without warming production files or relying on the host's gate.
    argv = ["--mount-map", f"{tmp_path}={tmp_path}", "--pool-root", str(tmp_path / "queue"),
            "--cas-root", str(tmp_path / "cas"), "--disks", "sdb"]
    return loop, gate, parked, current, calls, argv


@pytest.mark.parametrize("contents", [None, "{broken", '{"draining":true,"changed_unix":458.0}'])
@pytest.mark.parametrize("once", [False, True])
def test_gate_stops_every_storage_invocation(role, contents, once):
    loop, gate, parked, current, calls, argv = role
    if contents is None:
        gate.unlink()
    else:
        gate.write_text(contents)
    if once:
        assert loop.main(argv + ["--once"]) == 75
    else:
        with pytest.raises(CycleBoundary):
            loop.main(argv)
    assert calls == []
    markers = list(parked.iterdir())
    assert len(markers) == 1
    assert markers[0].name.startswith(f"{os.getpid()}-{worker_loop._proc_starttime(os.getpid())}-")
    assert markers[0].name.endswith("-458.0" if contents and "458.0" in contents else "-unknown")


@pytest.mark.parametrize("once", [False, True])
def test_same_commit_new_generation_exits_before_reading_queue(role, once):
    loop, gate, parked, current, calls, argv = role
    current.write_text(json.dumps({"generation": "republished", "commit": "a" * 40}))
    assert loop.main(argv + (["--once"] if once else [])) == (75 if once else 0)
    assert calls == []
    assert list(parked.iterdir()) == []


def test_a_parked_role_rotates_when_generation_moves(role, monkeypatch):
    loop, gate, parked, current, calls, argv = role
    gate.write_text(json.dumps({"draining": True, "changed_unix": 458.0}))
    def move(_):
        current.write_text(json.dumps({"generation": "new", "commit": "b" * 40}))
        # A second sleep would mean the role failed to leave old imported code.
        monkeypatch.setattr(loop.time, "sleep", lambda _: (_ for _ in ()).throw(AssertionError("did not rotate")))
    monkeypatch.setattr(loop.time, "sleep", move)
    assert loop.main(argv) == 0
    assert calls == []
    assert len(list(parked.iterdir())) == 1


def test_open_gate_runs_one_cycle(role):
    loop, gate, parked, current, calls, argv = role
    assert loop.main(argv + ["--once"]) == 0
    assert calls == [True]
    assert list(parked.iterdir()) == []


def test_a_closed_gate_precedes_queue_and_disk_discovery(role, monkeypatch):
    loop, gate, parked, current, calls, argv = role
    gate.write_text(json.dumps({"draining": True, "changed_unix": 458.0}))
    def forbidden(*args, **kwargs):
        raise AssertionError("closed gate reached queue or disk discovery")
    monkeypatch.setattr(loop.pool, "PoolQueue", forbidden)
    monkeypatch.setattr(loop, "pacer_from_args", forbidden)
    assert loop.main(argv + ["--once"]) == 75


def test_cycle_finishes_before_maintenance_park(role, monkeypatch):
    loop, gate, parked, current, calls, argv = role
    def cycle(*args, **kwargs):
        assert list(parked.iterdir()) == []
        calls.append(True)
        gate.write_text(json.dumps({"draining": True, "changed_unix": 458.0}))
        return {}
    sleeps = []
    def sleep(_):
        sleeps.append(True)
        if len(sleeps) == 2:
            raise CycleBoundary
    monkeypatch.setattr(loop, "cycle", cycle)
    monkeypatch.setattr(loop.time, "sleep", sleep)
    with pytest.raises(CycleBoundary):
        loop.main(argv)
    assert calls == [True]
    assert len(list(parked.iterdir())) == 1


@pytest.mark.parametrize("transition", ["maintenance", "generation"])
def test_runtime_boundary_is_rechecked_after_disk_setup(role, monkeypatch, transition):
    loop, gate, parked, current, calls, argv = role
    build = loop.pacer_from_args
    def setup(args):
        if transition == "maintenance":
            gate.write_text(json.dumps({"draining": True, "changed_unix": 458.0}))
        else:
            current.write_text(json.dumps({"generation": "new", "commit": "b" * 40}))
        return build(args)
    monkeypatch.setattr(loop, "pacer_from_args", setup)
    assert loop.main(argv + ["--once"]) == 75
    assert calls == []
