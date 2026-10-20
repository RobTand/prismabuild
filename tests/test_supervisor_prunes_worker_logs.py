"""The supervisor prunes the worker logs nobody will append to again.

Every spawn opens a new ``pb-worker-<index>.log`` and indices are never
reused, so before this the files only accumulated: 18,121 on dl380g10,
25,862 on sparky and 66,268 on sparklina on 2026-10-15. A regular owning
cycle now removes a bounded number of logs that are older than the
retention, are not the highest index, and are held as stdout or stderr by
no live supervised loop. Nothing else in the directory is touched.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

import supervise  # noqa: E402

EIGHT_DAYS_S = 8 * 24 * 3600


class CycleComplete(Exception):
    """Stop a long-lived supervisor after its first real cycle."""


@pytest.fixture
def logs(tmp_path, monkeypatch):
    config = tmp_path / "fleet_boxes.json"
    config.write_text(json.dumps({"boxes": {"boxa": {
        "loops": 1, "args": ["--class", "x86"],
    }}}))
    directory = tmp_path / "logs"
    directory.mkdir()
    monkeypatch.setattr(supervise, "CONFIG", config)
    monkeypatch.setattr(supervise, "MIRROR", tmp_path / "absent")
    monkeypatch.setattr(supervise, "CLAIM", tmp_path / "supervisor.claim")
    monkeypatch.setattr(supervise, "LOG_DIR", directory)
    monkeypatch.setattr(supervise.socket, "gethostname", lambda: "boxa")
    monkeypatch.setattr(supervise, "loop_args_of", lambda _pid: ["--class", "x86"])
    monkeypatch.setattr(supervise, "_is_fleet_loop", lambda *_a, **_k: True)
    monkeypatch.setattr(supervise.os, "kill", lambda *_a, **_k: None)
    monkeypatch.setattr(supervise, "_ready_backlog", lambda: False)
    monkeypatch.setattr(supervise, "_has_children", lambda _pid: False)

    def spawn(_args, index):
        (directory / f"pb-worker-{index}.log").write_text("spawned\n")
        return 9000 + index

    monkeypatch.setattr(supervise, "_spawn", spawn)

    def sleep(seconds):
        if seconds >= supervise.BUSY_INTERVAL_S:
            raise CycleComplete

    monkeypatch.setattr(supervise.time, "sleep", sleep)
    return directory


def _log(directory: Path, index: int, age_s: float = EIGHT_DAYS_S) -> Path:
    path = directory / f"pb-worker-{index}.log"
    path.write_text(f"loop {index}\n")
    then = time.time() - age_s
    os.utime(path, (then, then))
    return path


def _cycle(monkeypatch, argv, live):
    monkeypatch.setattr(sys, "argv", ["supervise", *argv])
    monkeypatch.setattr(supervise, "_live_loops", lambda: list(live))
    monkeypatch.setattr(supervise, "_claim_holders", lambda: frozenset(live))
    try:
        supervise.main()
    except CycleComplete:
        pass


def _names(directory: Path) -> set[str]:
    return {path.name for path in directory.iterdir()}


def test_old_unheld_logs_go_and_everything_else_stays(logs, tmp_path, monkeypatch):
    for index in range(10):
        if index not in (6, 7):
            _log(logs, index, age_s=60 if index == 8 else EIGHT_DAYS_S)
    outside = tmp_path / "outside.log"
    outside.write_text("someone else's\n")
    (logs / "pb-worker-6.log").symlink_to(outside)
    shared = tmp_path / "shared.log"
    shared.write_text("hard-linked\n")
    os.link(shared, logs / "pb-worker-7.log")
    look_alikes = ["pb-worker-01.log", "pb-worker-x.log", "pb-worker-2.log.gz",
                   "pb-worker-+4.log", "pb-worker-5_0.log", "pb-role-storage.log",
                   "pb-supervisor.log"]
    for name in look_alikes:
        (logs / name).write_text("not a worker log\n")
    then = time.time() - EIGHT_DAYS_S
    for path in logs.iterdir():
        if path.name != "pb-worker-8.log":
            os.utime(path, (then, then), follow_symlinks=False)

    held = (logs / "pb-worker-3.log").open("a")
    holder = subprocess.Popen([sys.executable, "-c", "import sys; sys.stdin.read()"],
                              stdin=subprocess.PIPE, stdout=held, stderr=held)
    held.close()
    try:
        _cycle(monkeypatch, ["--loops", "1"], [holder.pid])
    finally:
        holder.stdin.close()
        holder.wait(timeout=30)

    assert _names(logs) == {
        "pb-worker-3.log",   # a live loop's stdout and stderr
        "pb-worker-6.log",   # a symlink is never PB's log
        "pb-worker-7.log",   # a second link is someone else's name too
        "pb-worker-8.log",   # younger than the retention
        "pb-worker-9.log",   # the highest index keeps the next one unused
        *look_alikes,
    }
    assert outside.read_text() == "someone else's\n"
    assert shared.read_text() == "hard-linked\n"


def test_one_cycle_prunes_a_bounded_number(logs, monkeypatch):
    for index in range(20):
        _log(logs, index)
    monkeypatch.setattr(supervise, "WORKER_LOG_PRUNES_PER_TICK", 5)
    _cycle(monkeypatch, ["--loops", "1"], [])
    remaining = sorted(int(name[10:-4]) for name in _names(logs))
    assert len(remaining) == 21 - 5, remaining
    assert 20 in remaining, "the loop spawned this cycle keeps its log"


def test_once_prunes_nothing(logs, monkeypatch):
    for index in range(5):
        _log(logs, index)
    monkeypatch.setattr(sys, "argv", ["supervise", "--once"])
    monkeypatch.setattr(supervise, "_live_loops", lambda: [])
    monkeypatch.setattr(supervise, "_claim_holders", lambda: frozenset())
    assert supervise.main() == 0
    assert {f"pb-worker-{index}.log" for index in range(5)} <= _names(logs)


def test_an_unreadable_live_loop_prunes_nothing_that_cycle(logs, tmp_path, monkeypatch):
    for index in range(5):
        _log(logs, index)
    # A live loop whose descriptors cannot be read might hold any of them.
    proc = tmp_path / "proc"
    (proc / "4242" / "fd").mkdir(parents=True)
    (proc / "4242" / "fd").chmod(0)
    monkeypatch.setattr(supervise, "PROC", proc)
    try:
        _cycle(monkeypatch, ["--loops", "1"], [4242])
    finally:
        (proc / "4242" / "fd").chmod(0o700)
    assert {f"pb-worker-{index}.log" for index in range(5)} <= _names(logs)


def test_a_loop_that_has_exited_holds_nothing(logs, tmp_path, monkeypatch):
    for index in range(5):
        _log(logs, index)
    monkeypatch.setattr(supervise, "PROC", tmp_path / "proc")
    _cycle(monkeypatch, ["--loops", "1"], [4242])
    assert _names(logs) == {"pb-worker-4.log"}


def test_the_census_names_only_worker_logs_and_the_next_slot_is_unused(logs):
    for name in ("pb-worker-2.log", "pb-worker-19.log", "pb-worker-nope.log",
                 "pb-worker-07.log", "pb-worker-3.log.gz"):
        (logs / name).write_text("evidence\n")
    assert supervise._worker_log_census() == [2, 19]
