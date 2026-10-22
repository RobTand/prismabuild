"""Deadline-driven merge-queue child waits (#1992), without wall-clock races.

The clock and kernel readiness seam model pidfds for controlled child exit times.
Both select(2) and poll(2) are supported: the tests prescribe the observable
contract, not a helper name or one choice of readiness API.
"""
from __future__ import annotations

import json
import select
import signal
from pathlib import Path
from types import SimpleNamespace

import pytest

from test_pbmergeq import config, mq, shard


class ChildSchedule:
    def __init__(self, monkeypatch, tmp_path, specs, heartbeat=5.0):
        self.now = 0.0
        self.specs = specs  # launch order: (name, exit time, returncode)
        self.children = []
        self.opened = {}
        self.closed = []
        self.waits = []
        self.polls = []
        self.sleeps = []
        self.blocks = []
        self.events = []
        self.status = []
        self.progress_reads = []
        self.monkeypatch = monkeypatch
        cfg = config(tmp_path, inconclusive_retries=0)
        store = SimpleNamespace(
            event=lambda batch, message: self.events.append((self.now, message)),
            write_status=lambda: self.status.append(self.now),
        )
        self.runner = mq.Runner(cfg, store)
        monkeypatch.setattr(self.runner, "history", lambda runtime: [])
        monkeypatch.setattr(mq, "checkout_runtime", lambda cfg, checkout: {
            "python": cfg.test_python, "checkout": str(checkout),
            "source": ["a" * 40, "b" * 40], "pbtest": str(cfg.pbtest),
        })
        monkeypatch.setattr(mq, "HEARTBEAT_S", heartbeat)
        monkeypatch.setattr(mq, "time", SimpleNamespace(
            monotonic=lambda: self.now, sleep=self.sleep,
        ))
        monkeypatch.setattr(mq.subprocess, "Popen", self.launch)
        monkeypatch.setattr(mq.os, "pidfd_open", self.pidfd_open)
        original_close = mq.os.close

        def close(fd):
            if fd in self.opened:
                assert fd not in self.closed, "pidfd closed more than once"
                self.closed.append(fd)
            else:
                original_close(fd)

        monkeypatch.setattr(mq.os, "close", close)
        monkeypatch.setattr(select, "select", self.select)
        schedule = self

        class Poll:
            def __init__(self):
                self.registered = {}

            def register(self, fd, eventmask=select.POLLIN | select.POLLOUT):
                self.registered[fd] = eventmask

            def unregister(self, fd):
                del self.registered[fd]

            def modify(self, fd, eventmask):
                self.registered[fd] = eventmask

            def poll(self, timeout=None):
                ready = schedule.ready(list(self.registered),
                                       None if timeout is None else timeout / 1000)
                return [(fd, select.POLLIN) for fd in ready]

        monkeypatch.setattr(select, "poll", Poll)
        self.jobs = [(name, tmp_path / name, [f"tests/test_{name}.py"])
                     for name, _, _ in specs]
        self.outdir = tmp_path / "out"

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds

    def launch(self, command, **kwargs):
        name, ends, code = self.specs[len(self.children)]
        report = Path(command[command.index("--json") + 1])
        assert report.stem == name
        # A killed client never produced a terminal report.
        if code >= 0:
            report.write_text(json.dumps([shard([f"tests/test_{name}.py"])]))
        schedule = self

        class Child:
            pid = 70000 + len(schedule.children)
            returncode = None
            exit_at = ends
            reaped = False

            def poll(self):
                schedule.polls.append((name, schedule.now))
                if schedule.now >= ends:
                    self.returncode = code
                return self.returncode

            def wait(self):
                assert not self.reaped, "each child must be reaped only once"
                # A premature wait would serialize jobs; model its blocking time.
                schedule.now = max(schedule.now, ends)
                self.returncode = code
                self.reaped = True
                schedule.waits.append((name, schedule.now, code))
                return code

        child = Child()
        self.children.append(child)
        return child

    def pidfd_open(self, pid, flags=0):
        child = next(child for child in self.children if child.pid == pid)
        assert child not in self.opened.values(), "open one pidfd per child"
        fd = 90000 + len(self.opened)
        self.opened[fd] = child
        return fd

    def ready(self, readers, timeout):
        assert readers, "a live child must be represented in the wait set"
        assert len(self.blocks) < 100, "readable pidfd was not consumed/removed"
        assert all(fd in self.opened and fd not in self.closed for fd in readers)
        self.blocks.append((self.now, timeout, tuple(readers)))
        next_exit = min(self.opened[fd].exit_at for fd in readers)
        deadline = float("inf") if timeout is None else self.now + max(0, timeout)
        self.now = max(self.now, min(next_exit, deadline))
        return [fd for fd in readers if self.opened[fd].exit_at <= self.now]

    def select(self, readers, writers, errors, timeout=None):
        assert not writers
        return self.ready(readers, timeout), [], []

    def run(self):
        return self.runner._wait("pidfd-test", self.outdir, self.jobs)

    def assert_event_wait(self, expected_waits):
        assert self.waits == expected_waits, (
            "children must be reaped at readiness, not at a two-second quantum", self.waits)
        assert not self.sleeps, "child exit must not be discovered by sleep polling"
        assert not self.polls, "the pidfd ready set owns child liveness"
        assert len(self.opened) == len(self.children), "wait on every child's pidfd"
        assert set(self.closed) == set(self.opened), "close consumed pidfds"


def test_staggered_children_reap_on_readiness_but_report_in_launch_order(monkeypatch, tmp_path):
    schedule = ChildSchedule(monkeypatch, tmp_path,
                             [("slow", 3.5, 0), ("fast", 0.25, 0)])
    results = schedule.run()
    schedule.assert_event_wait([("fast", 0.25, 0), ("slow", 3.5, 0)])
    assert list(results) == ["slow", "fast"]
    assert all(not r.failed and not r.inconclusive for r in results.values())
    ended = [(at, message.split()[1]) for at, message in schedule.events if " ended " in message]
    assert ended == [(3.5, "slow"), (3.5, "fast")], "retain the final launch-order result pass"
    assert not schedule.status


@pytest.mark.parametrize("early_exit", [False, True], ids=["no-exits", "exit-before-deadline"])
def test_heartbeat_deadline_and_progress_survive_child_notifications(monkeypatch, tmp_path, early_exit):
    specs = ([("brief", 2.0, 0)] if early_exit else []) + [("long", 16.25, 0)]
    schedule = ChildSchedule(monkeypatch, tmp_path, specs)
    snapshots = iter([
        {"sequence": 2, "completed_shards": 4},
        {"sequence": 1, "completed_shards": 1},  # stale: must not regress progress
        {"sequence": 3, "completed_shards": 5},
    ])

    def read_progress(path, **invocation):
        schedule.progress_reads.append((schedule.now, Path(path), invocation))
        return next(snapshots)

    monkeypatch.setattr(schedule.runner.outcomes, "read_progress", read_progress)
    results = schedule.run()
    assert schedule.status == [5.0, 10.0, 15.0], (
        "heartbeat is a deadline, not rounded to the next two-second poll", schedule.status)
    waiting = [(at, message) for at, message in schedule.events if " waiting " in message]
    assert waiting == [
        (5.0, "run long waiting 5s: 4 shard(s) reported"),
        (10.0, "run long waiting 10s: 4 shard(s) reported"),
        (15.0, "run long waiting 15s: 5 shard(s) reported"),
    ]
    assert len(schedule.progress_reads) == 3
    assert all(path == schedule.runner.outcomes.progress_path(schedule.outdir / "long.json")
               and invocation["run_id"]
               and invocation["launch_entrypoint"] == str(schedule.runner.cfg.pbtest)
               and invocation["generation"]
               for _, path, invocation in schedule.progress_reads)
    # Completion notifications must not move the heartbeat deadline. Each block
    # receives the remaining time, rather than a fresh interval or short tick.
    for started, timeout, _ in schedule.blocks:
        expected = 5.0 - (started % 5.0)
        assert timeout == pytest.approx(expected, abs=0.0011)
    expected_waits = ([("brief", 2.0, 0)] if early_exit else []) + [("long", 16.25, 0)]
    schedule.assert_event_wait(expected_waits)
    assert all(not result.failed and not result.inconclusive for result in results.values())


def test_killed_child_is_reaped_once_and_remains_inconclusive(monkeypatch, tmp_path):
    schedule = ChildSchedule(monkeypatch, tmp_path,
                             [("healthy", 3.25, 0), ("killed", 0.75, -signal.SIGKILL)])
    results = schedule.run()
    schedule.assert_event_wait([("killed", 0.75, -signal.SIGKILL), ("healthy", 3.25, 0)])
    assert list(results) == ["healthy", "killed"]
    assert not results["healthy"].failed and not results["healthy"].inconclusive
    assert results["killed"].inconclusive == ["tests/test_killed.py"]
    assert not results["killed"].runtime_refusal and not results["killed"].failed
    assert any(f"run killed ended rc={-signal.SIGKILL}" in message
               for _, message in schedule.events)


def test_real_killed_child_and_healthy_child_use_native_pidfds(monkeypatch, tmp_path):
    """Complement the deterministic deadline cases with real Linux exit readiness."""
    import os
    import subprocess
    import sys
    import time

    cfg = config(tmp_path, inconclusive_retries=0)
    store = SimpleNamespace(event=lambda *args: None, write_status=lambda: None)
    runner = mq.Runner(cfg, store)
    monkeypatch.setattr(runner, "history", lambda runtime: [])
    monkeypatch.setattr(mq, "checkout_runtime", lambda cfg, checkout: {
        "python": sys.executable, "checkout": str(checkout), "source": ["a" * 40, "b" * 40],
    })
    real_popen, real_pidfd_open = subprocess.Popen, os.pidfd_open
    children, waited, opened = [], [], []

    def launch(command, **kwargs):
        report = Path(command[command.index("--json") + 1])
        name = report.stem
        if name == "killed":
            script = "import os, signal; os.kill(os.getpid(), signal.SIGKILL)"
        else:
            rows = json.dumps([shard(["tests/test_healthy.py"])])
            script = f"from pathlib import Path; Path({str(report)!r}).write_text({rows!r})"
        process = real_popen([sys.executable, "-c", script], **kwargs)
        original_wait = process.wait
        children.append((process, original_wait))

        def wait(*args, **kwargs):
            waited.append(name)
            return original_wait(*args, **kwargs)

        monkeypatch.setattr(process, "wait", wait)
        return process

    def pidfd_open(pid, flags=0):
        fd = real_pidfd_open(pid, flags)
        opened.append((pid, fd))
        return fd

    monkeypatch.setattr(mq.subprocess, "Popen", launch)
    monkeypatch.setattr(mq.os, "pidfd_open", pidfd_open)
    monkeypatch.setattr(mq, "time", SimpleNamespace(
        monotonic=time.monotonic,
        sleep=lambda seconds: pytest.fail("native child exits must not use sleep polling"),
    ))
    try:
        results = runner._wait("native-pidfds", tmp_path / "out", [
            (name, tmp_path, [f"tests/test_{name}.py"]) for name in ("killed", "healthy")
        ])
        assert sorted(waited) == ["healthy", "killed"], "one wait per real child"
        assert sorted(pid for pid, _ in opened) == sorted(p.pid for p, _ in children)
        assert children[0][0].returncode == -signal.SIGKILL
        assert children[1][0].returncode == 0
        assert results["killed"].inconclusive == ["tests/test_killed.py"]
        assert not results["healthy"].failed and not results["healthy"].inconclusive
        for _, fd in opened:
            with pytest.raises(OSError):
                os.fstat(fd)
    finally:
        # Bounded cleanup also applies when the baseline fails at its first sleep.
        for process, original_wait in children:
            if process.returncode is None:
                process.kill()
            original_wait(timeout=10)
        for _, fd in opened:
            try:
                os.close(fd)
            except OSError:
                pass
