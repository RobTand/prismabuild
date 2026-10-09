"""The scratch-lifetime qualifier refuses unsafe input before any effect (#1360)."""
from __future__ import annotations

import sys
import json
import os
import signal
import subprocess
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


@pytest.fixture
def launcher_target(tmp_path, monkeypatch):
    from argparse import Namespace
    key = "a" * 64
    open_pidfd = os.pidfd_open
    child = subprocess.Popen([
        sys.executable, "-c",
        "import signal; print('ready', flush=True); signal.pause()", "run-local", key],
        stdout=subprocess.PIPE, text=True)
    assert child.stdout.readline() == "ready\n"
    (tmp_path / "victim-ready.json").write_text(json.dumps({
        "action_key": key, "host": "fixture", "leaf": "/unused",
    }), encoding="utf-8")
    monkeypatch.setattr(qualification, "_rendezvous_dir", lambda args: tmp_path)
    monkeypatch.setattr(qualification.pool, "find_launcher_pids", lambda key: [child.pid])

    def forbid_numeric_signal(*args):
        raise AssertionError("qualifier used a numeric PID signal")

    monkeypatch.setattr(qualification.os, "kill", forbid_numeric_signal)
    args = Namespace(victim_key=key)
    try:
        yield args, child, tmp_path
    finally:
        if child.poll() is None:
            # Reap the fixture through an owned kernel handle, not the patched API.
            descriptor = open_pidfd(child.pid)
            try:
                signal.pidfd_send_signal(descriptor, signal.SIGKILL)
            finally:
                os.close(descriptor)
        child.wait(timeout=10)


def test_launcher_killer_signals_verified_handle(launcher_target, monkeypatch):
    import time
    args, child, directory = launcher_target
    monkeypatch.setattr(time, "sleep", lambda seconds: None)
    assert qualification._launcher_killer(args, "b" * 64, {"host": "fixture"}) == 0
    assert child.wait(timeout=10) == -signal.SIGKILL
    assert json.loads((directory / "kill-done.json").read_text())["killed"] == [child.pid]


def test_launcher_refusal_never_signals_already_pinned_target(launcher_target, monkeypatch):
    args, child, directory = launcher_target
    monkeypatch.setattr(qualification.pool, "find_launcher_pids",
                        lambda key: [child.pid, -1])
    descriptors = []
    open_pidfd = os.pidfd_open

    def pin(pid, flags=0):
        descriptor = open_pidfd(pid, flags)
        descriptors.append(descriptor)
        return descriptor

    monkeypatch.setattr(qualification.os, "pidfd_open", pin)
    with pytest.raises(SystemExit, match="refusing launcher"):
        qualification._launcher_killer(args, "b" * 64, {"host": "fixture"})
    assert child.poll() is None
    assert not (directory / "kill-intent.json").exists()
    assert len(descriptors) == 1
    with pytest.raises(OSError):
        os.fstat(descriptors[0])


def test_launcher_exit_after_pin_never_signals_replacement(launcher_target, monkeypatch):
    import time
    args, child, directory = launcher_target

    def exit_target(seconds):
        descriptor = os.pidfd_open(child.pid)
        try:
            signal.pidfd_send_signal(descriptor, signal.SIGTERM)
        finally:
            os.close(descriptor)
        child.wait(timeout=10)

    monkeypatch.setattr(time, "sleep", exit_target)
    assert qualification._launcher_killer(args, "b" * 64, {"host": "fixture"}) == 0
    assert child.returncode == -signal.SIGTERM
    assert json.loads((directory / "kill-done.json").read_text())["killed"] == []


@pytest.mark.parametrize("fault", ["reuse", "unreadable", "exited"])
def test_launcher_identity_refusal_closes_handle(launcher_target, monkeypatch, fault):
    import time
    args, child, directory = launcher_target
    descriptors = []
    open_pidfd = os.pidfd_open

    def pin(pid, flags=0):
        descriptor = open_pidfd(pid, flags)
        descriptors.append(descriptor)
        if fault == "reuse":
            ticks = qualification.pool._contained_worker_start_ticks(pid)
            monkeypatch.setattr(qualification.pool, "_contained_worker_start_ticks",
                                lambda pid: ticks + 1)
        elif fault == "unreadable":
            def unreadable(pid):
                raise PermissionError("fixture metadata unavailable")
            monkeypatch.setattr(qualification.pool, "_process_cmdline", unreadable)
        else:
            signal.pidfd_send_signal(descriptor, signal.SIGTERM)
            child.wait(timeout=10)
        return descriptor

    monkeypatch.setattr(qualification.os, "pidfd_open", pin)
    monkeypatch.setattr(time, "sleep", lambda seconds: None)
    with pytest.raises(SystemExit, match="refusing launcher"):
        qualification._launcher_killer(args, "b" * 64, {"host": "fixture"})
    assert not (directory / "kill-intent.json").exists()
    if fault != "exited":
        assert child.poll() is None
    assert descriptors
    for descriptor in descriptors:
        with pytest.raises(OSError):
            os.fstat(descriptor)


def test_launcher_killer_refuses_without_pidfd_support(launcher_target, monkeypatch):
    import time
    args, child, directory = launcher_target
    monkeypatch.setattr(qualification.os, "pidfd_open", None)
    monkeypatch.setattr(time, "sleep", lambda seconds: None)
    with pytest.raises(SystemExit, match="pidfd support"):
        qualification._launcher_killer(args, "b" * 64, {"host": "fixture"})
    assert child.poll() is None
    assert not (directory / "kill-intent.json").exists()
