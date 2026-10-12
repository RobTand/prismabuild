"""The scratch-lifetime qualifier refuses unsafe input before any effect (#1360)."""
from __future__ import annotations

import sys
import json
import signal
import time
from argparse import Namespace
from dataclasses import dataclass, field
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import qualify_scratch_lifetime as qualification


def test_unknown_scenario_exits_without_effect():
    with pytest.raises(SystemExit) as caught:
        qualification.main(["--scenario", "no-such-path",
                            "--temp-root-env", "TEMP_ROOT", "--temp-name", "row-temp",
                            "--cache-root-env", "CACHE_ROOT", "--cache-name", "compile"])
    assert caught.value.code == 2


def test_missing_action_identity_refuses(tmp_path, monkeypatch):
    monkeypatch.delenv("PRISMABUILD_ACTION_KEY", raising=False)
    with pytest.raises(SystemExit, match="PRISMABUILD_ACTION_KEY is absent"):
        qualification.main(["--scenario", "normal",
                            "--temp-root-env", "TEMP_ROOT", "--temp-name", "row-temp",
                            "--cache-root-env", "CACHE_ROOT", "--cache-name", "compile",
                            "--queue-root", str(tmp_path)])


def test_launcher_roles_require_a_rendezvous_id(tmp_path, monkeypatch):
    from argparse import Namespace
    args = Namespace(rendezvous_id="", rendezvous_root="/mnt/shared/pb-qualification")
    with pytest.raises(SystemExit, match="rendezvous-id"):
        qualification._rendezvous_dir(args)
    args = Namespace(rendezvous_id="../escape", rendezvous_root="/mnt/shared/pb-qualification")
    with pytest.raises(SystemExit, match="rendezvous-id"):
        qualification._rendezvous_dir(args)


def test_rendezvous_root_cannot_escape_qualification(tmp_path):
    from argparse import Namespace
    args = Namespace(rendezvous_id="case-1", rendezvous_root=str(tmp_path))
    with pytest.raises(SystemExit, match="beneath /mnt/shared/pb-qualification"):
        qualification._rendezvous_dir(args)


def test_killer_requires_the_victim_key():
    from argparse import Namespace
    args = Namespace(rendezvous_id="case-1", rendezvous_root="/mnt/shared/pb-qualification",
                     victim_key="short")
    with pytest.raises(SystemExit, match="victim action key"):
        qualification._launcher_killer(args, "k" * 64, {"host": "sparky"})


@dataclass
class _Launcher:
    pid: int
    start_ticks: int
    argv: tuple[bytes, ...]
    alive: bool = True
    readable: bool = True
    signals: list[int] = field(default_factory=list)


class _LauncherTarget:
    def __init__(self, directory):
        self.directory = directory
        self.args = Namespace(victim_key="a" * 64)
        self.original = _Launcher(101, 11, (b"python", b"run-local",
                                          self.args.victim_key.encode()))
        self.processes = {self.original.pid: self.original}
        self.pids = [self.original.pid]
        self.handles = {}
        self.acquired = []
        self.closed = []
        self.events = []
        self.after_open = lambda launcher: None
        self.on_delay = lambda: None

    def discover(self, key):
        assert key == self.args.victim_key
        return list(self.pids)

    def start_ticks(self, pid):
        self.events.append(("ticks", pid))
        launcher = self.processes.get(pid)
        return launcher.start_ticks if launcher and launcher.alive else None

    def cmdline(self, pid):
        self.events.append(("argv", pid))
        launcher = self.processes.get(pid)
        if launcher is None or not launcher.alive:
            return []
        if not launcher.readable:
            raise PermissionError("fixture metadata unavailable")
        return list(launcher.argv)

    def open_pidfd(self, pid, flags):
        assert flags == 0
        launcher = self.processes[pid]
        assert launcher.alive
        descriptor = 1000 + len(self.acquired)
        self.handles[descriptor] = launcher
        self.acquired.append(descriptor)
        self.events.append(("open", pid, descriptor))
        self.after_open(launcher)
        return descriptor

    def send_signal(self, descriptor, sig):
        self.events.append(("signal", descriptor, sig))
        launcher = self.handles[descriptor]
        if not launcher.alive:
            self.events.append(("lookup-error", descriptor))
            raise ProcessLookupError("pinned launcher exited")
        launcher.signals.append(sig)
        launcher.alive = False

    def close(self, descriptor):
        self.handles.pop(descriptor)
        self.closed.append(descriptor)
        self.events.append(("close", descriptor))

    def delay(self, _seconds):
        self.events.append(("delay",))
        self.on_delay()

    def forbid_numeric_signal(self, *args):
        raise AssertionError("qualifier used a numeric PID signal")


@pytest.fixture
def launcher_target(tmp_path, monkeypatch):
    target = _LauncherTarget(tmp_path)
    (tmp_path / "victim-ready.json").write_text(json.dumps({
        "action_key": target.args.victim_key, "host": "fixture", "leaf": "/unused",
    }), encoding="utf-8")
    monkeypatch.setattr(qualification, "_rendezvous_dir", lambda args: tmp_path)
    monkeypatch.setattr(qualification.pool, "find_launcher_pids", target.discover)
    monkeypatch.setattr(qualification.pool, "_contained_worker_start_ticks", target.start_ticks)
    monkeypatch.setattr(qualification.pool, "_process_cmdline", target.cmdline)
    monkeypatch.setattr(qualification, "os", Namespace(
        pidfd_open=target.open_pidfd, close=target.close, kill=target.forbid_numeric_signal))
    monkeypatch.setattr(signal, "pidfd_send_signal", target.send_signal, raising=False)
    monkeypatch.setattr(time, "sleep", target.delay)
    monkeypatch.setattr(time, "monotonic", lambda: 0)
    yield target
    assert sorted(target.closed) == target.acquired
    assert not target.handles


def test_launcher_killer_signals_verified_handle(launcher_target):
    target = launcher_target
    launcher = target.original
    assert qualification._launcher_killer(target.args, "b" * 64, {"host": "fixture"}) == 0
    descriptor, = target.acquired
    assert target.events == [
        ("ticks", launcher.pid), ("argv", launcher.pid),
        ("open", launcher.pid, descriptor),
        ("ticks", launcher.pid), ("argv", launcher.pid),
        ("delay",), ("signal", descriptor, signal.SIGKILL), ("close", descriptor),
    ]
    assert launcher.signals == [signal.SIGKILL]
    assert target.closed == [descriptor]
    assert not target.handles
    assert json.loads((target.directory / "kill-done.json").read_text()) == {
        "victim": target.args.victim_key, "killed": [launcher.pid], "killer": "b" * 64,
    }


def test_launcher_killer_pins_all_targets_before_any_signal(launcher_target):
    target = launcher_target
    second = _Launcher(202, 22, target.original.argv)
    target.processes[second.pid] = second
    target.pids.append(second.pid)
    assert qualification._launcher_killer(target.args, "b" * 64, {"host": "fixture"}) == 0
    first_fd, second_fd = target.acquired
    expected_pins = []
    for launcher, descriptor in [(target.original, first_fd), (second, second_fd)]:
        expected_pins.extend([
            ("ticks", launcher.pid), ("argv", launcher.pid),
            ("open", launcher.pid, descriptor),
            ("ticks", launcher.pid), ("argv", launcher.pid),
        ])
    assert target.events == expected_pins + [
        ("delay",), ("signal", first_fd, signal.SIGKILL),
        ("signal", second_fd, signal.SIGKILL),
        ("close", second_fd), ("close", first_fd),
    ]
    assert target.original.signals == second.signals == [signal.SIGKILL]
    assert sorted(target.closed) == target.acquired
    assert not target.handles
    assert json.loads((target.directory / "kill-done.json").read_text())["killed"] == target.pids


def test_launcher_refusal_never_signals_already_pinned_target(launcher_target):
    target = launcher_target
    target.pids.append(-1)
    with pytest.raises(SystemExit, match="refusing launcher"):
        qualification._launcher_killer(target.args, "b" * 64, {"host": "fixture"})
    assert target.original.alive
    assert target.original.signals == []
    assert not any(event[0] == "signal" for event in target.events)
    assert not (target.directory / "kill-intent.json").exists()
    assert not (target.directory / "kill-done.json").exists()
    descriptor, = target.acquired
    assert target.closed == [descriptor]
    assert not target.handles


def test_launcher_exit_after_pin_never_signals_replacement(launcher_target):
    target = launcher_target
    original = target.original
    replacement = _Launcher(original.pid, original.start_ticks + 1, original.argv)

    def exit_and_replace():
        descriptor, = target.acquired
        assert target.handles[descriptor] is original
        assert target.events == [
            ("ticks", original.pid), ("argv", original.pid),
            ("open", original.pid, descriptor),
            ("ticks", original.pid), ("argv", original.pid), ("delay",),
        ]
        assert (target.directory / "kill-intent.json").exists()
        original.alive = False
        target.processes[original.pid] = replacement

    target.on_delay = exit_and_replace
    assert qualification._launcher_killer(target.args, "b" * 64, {"host": "fixture"}) == 0
    descriptor, = target.acquired
    assert target.events[-3:] == [
        ("signal", descriptor, signal.SIGKILL),
        ("lookup-error", descriptor), ("close", descriptor),
    ]
    assert not original.alive
    assert original.signals == []
    assert target.processes[original.pid] is replacement
    assert replacement.alive
    assert replacement.signals == []
    assert target.closed == [descriptor]
    assert not target.handles
    assert json.loads((target.directory / "kill-done.json").read_text())["killed"] == []


@pytest.mark.parametrize("fault", ["reuse", "unreadable", "exited", "argv"])
def test_launcher_identity_refusal_closes_handle(launcher_target, fault):
    target = launcher_target
    second = _Launcher(202, 22, target.original.argv)
    target.processes[second.pid] = second
    target.pids.append(second.pid)

    def change_identity(launcher):
        if launcher is not second:
            return
        if fault == "reuse":
            launcher.start_ticks += 1
        elif fault == "unreadable":
            launcher.readable = False
        elif fault == "exited":
            launcher.alive = False
        else:
            launcher.argv = (b"python", b"unrelated")

    target.after_open = change_identity
    with pytest.raises(SystemExit, match="refusing launcher"):
        qualification._launcher_killer(target.args, "b" * 64, {"host": "fixture"})
    assert not (target.directory / "kill-intent.json").exists()
    assert not (target.directory / "kill-done.json").exists()
    assert target.original.alive
    assert second.alive == (fault != "exited")
    assert target.original.signals == second.signals == []
    assert not any(event[0] == "signal" for event in target.events)
    assert len(target.acquired) == 2
    assert sorted(target.closed) == target.acquired
    assert not target.handles


@pytest.mark.parametrize("missing", ["pidfd_open", "pidfd_send_signal"])
def test_launcher_killer_refuses_without_pidfd_support(launcher_target, monkeypatch, missing):
    target = launcher_target
    if missing == "pidfd_open":
        monkeypatch.setattr(qualification.os, missing, None)
    else:
        monkeypatch.setattr(signal, missing, None)
    with pytest.raises(SystemExit, match="pidfd support"):
        qualification._launcher_killer(target.args, "b" * 64, {"host": "fixture"})
    assert target.original.alive
    assert target.original.signals == []
    assert not any(event[0] == "signal" for event in target.events)
    assert target.acquired == target.closed == []
    assert not target.handles
    assert not (target.directory / "kill-intent.json").exists()
    assert not (target.directory / "kill-done.json").exists()
