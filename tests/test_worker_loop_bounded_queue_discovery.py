"""The worker's claim scan must not park the loop in the queue read.

Issue #16: the NFS client on one box stalled 69 s reading frequently-rewritten
queue files while the pool kept placing on it. The worker read those files
in-process -- ``serve_once -> claim -> ready_items -> passes -> read_bytes`` --
so the stall parked the loop in D-state: it stopped polling, stopped
announcing, and held a claim it could not progress.

The fix bounds the pure-read half of the scan. The loop discovers the
candidate list in an abandonable child (``pbstatus.bounded``) and hands the
snapshot to ``serve_once``/``claim`` as ``ready``; anything but a completed
scan skips publication and admission for that poll. The rename that IS the
claim, and the lease/token writes after it, stay synchronous: a timed-out
mutation may still finish later and needs ownership-safe reconciliation,
which a disposable child cannot give (#266).
"""
from __future__ import annotations

import importlib.util
import os
from pathlib import Path
import sys
import time
from unittest import mock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import pool  # noqa: E402

WORKER = Path(__file__).resolve().parents[1] / "tools/fleet/worker_loop.py"
KEY = "d" * 64


@pytest.fixture()
def worker():
    spec = importlib.util.spec_from_file_location("discovery_adapter_test", WORKER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    return pool.PoolQueue(tmp_path / "queue")


def _publish(q: pool.PoolQueue) -> None:
    q.publish(
        action_key=KEY, cas_root=q.root / "cas", checkout_root=q.root / "co",
        worker_script=q.root / "worker.py", resources={"cpu": 1},
    )


def reap(pid, timeout=5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            done, _ = os.waitpid(pid, os.WNOHANG)
        except ChildProcessError:
            return True
        if done == pid:
            return True
        time.sleep(0.01)
    return False


def test_provided_snapshot_is_consumed_without_rescanning(
    queue: pool.PoolQueue, monkeypatch,
) -> None:
    """A prefetched snapshot feeds the claim; the scan is not repeated."""

    _publish(queue)
    snapshot = queue.ready_items()
    assert [r.get("action_key") for r in snapshot] == [KEY]

    def exploded():
        raise AssertionError("ready_items must not run when ready= is given")

    monkeypatch.setattr(queue, "ready_items", exploded)
    claimed = queue.claim(owner="sparky:1:abcd1234", capacity={"cpu": 1},
                          ready=snapshot)
    assert claimed is not None
    assert claimed["action_key"] == KEY


def test_empty_snapshot_is_an_empty_queue_not_a_rescan(
    queue: pool.PoolQueue, monkeypatch,
) -> None:
    """``ready=[]`` answers None without touching the mount."""

    def exploded():
        raise AssertionError("ready_items must not run when ready= is given")

    monkeypatch.setattr(queue, "ready_items", exploded)
    assert queue.claim(owner="sparky:1:abcd1234", capacity={"cpu": 1},
                       ready=[]) is None


def test_serve_once_forwards_the_snapshot_to_claim(
    queue: pool.PoolQueue,
) -> None:
    """The loop's discovery snapshot reaches the claim it prefetched for."""

    _publish(queue)
    snapshot = queue.ready_items()
    seen: dict = {}
    real_claim = pool.PoolQueue.claim

    def capture(self, **kwargs):
        seen.update(kwargs)
        return None

    with mock.patch.object(pool.PoolQueue, "claim", capture):
        assert queue.serve_once(capacity={"cpu": 1}, ready=snapshot) is None
    assert seen.get("ready") is snapshot


def test_healthy_discovery_returns_a_claimable_snapshot(
    worker, queue: pool.PoolQueue,
) -> None:
    """The child scan reads what the in-process scan reads."""

    _publish(queue)
    abandoned: list = []
    result = worker.discover_ready_snapshot(queue, budget_s=5.0,
                                            abandoned=abandoned)
    assert result.status == "ready", result
    assert [r.get("action_key") for r in result.snapshot] == [KEY]
    assert result.retained is None and abandoned == []


def test_blocked_discovery_times_out_without_parking_the_caller(
    worker, tmp_path: Path,
) -> None:
    """A wedged mount parks the child; the parent is back at the deadline."""

    fifo = tmp_path / "hold-discovery"
    os.mkfifo(fifo)
    entered = tmp_path / "entered"

    class WedgedQueue:
        def ready_items(self):
            entered.write_text("entered")
            fd = os.open(fifo, os.O_RDONLY)
            os.close(fd)
            return []

    abandoned: list = []
    started = time.monotonic()
    try:
        result = worker.discover_ready_snapshot(WedgedQueue(), budget_s=1.0,
                                                abandoned=abandoned)
    finally:
        # Release a child SIGKILL could not reap, if it is still waiting.
        try:
            fd = os.open(fifo, os.O_WRONLY | os.O_NONBLOCK)
        except OSError:
            pass
        else:
            os.close(fd)
    elapsed = time.monotonic() - started
    assert entered.exists(), result
    assert result.status == "unavailable", result
    assert result.snapshot is None, result
    assert "timed out" in result.error, result
    assert elapsed < 20.0, f"the parent waited {elapsed:g}s on a dead scan"


def test_discovery_error_is_a_skip_not_an_empty_queue(worker) -> None:
    """A scan that raises is reported failed, never as 'nothing queued'."""

    class BrokenQueue:
        def ready_items(self):
            raise OSError("simulated wedged queue read")

    result = worker.discover_ready_snapshot(BrokenQueue(), budget_s=5.0,
                                            abandoned=[])
    assert result.status == "failed", result
    assert result.snapshot is None, result
    assert "OSError" in result.error, result


def test_retained_reader_fences_the_next_poll_and_recovers(
    worker, tmp_path: Path, monkeypatch,
) -> None:
    """One unreaped reader blocks a second launch; its exit reopens discovery."""

    bounded_module = sys.modules[worker.bounded.__module__]
    original_stop = bounded_module._stop_reader
    fifo = tmp_path / "hold-reader"
    os.mkfifo(fifo)
    entered = tmp_path / "entered"
    abandoned: list = []

    def retain(pid, section, started, records):
        records.append({"pid": pid,
                        "starttime_ticks": bounded_module._starttime_ticks(pid),
                        "section": section, "since_unix": time.time()})

    class WedgedQueue:
        def ready_items(self):
            entered.write_text("entered")
            fd = os.open(fifo, os.O_RDONLY)
            os.close(fd)
            return []

    pid = None
    monkeypatch.setattr(bounded_module, "_stop_reader", retain)
    try:
        first = worker.discover_ready_snapshot(WedgedQueue(), budget_s=1.0,
                                               abandoned=abandoned)
        assert entered.exists(), first
        assert first.status == "unavailable" and len(abandoned) == 1, first
        pid = abandoned[0]["pid"]
        second = worker.discover_ready_snapshot(WedgedQueue(), budget_s=1.0,
                                                abandoned=abandoned)
        assert second.status == "busy", second
        assert second.snapshot is None, second
        assert len(abandoned) == 1, "a fenced poll must not abandon another reader"
    finally:
        monkeypatch.setattr(bounded_module, "_stop_reader", original_stop)
        # Release the retained child; its open completes and it exits.
        try:
            fd = os.open(fifo, os.O_WRONLY | os.O_NONBLOCK)
        except OSError:
            pass
        else:
            os.close(fd)
    assert pid is not None
    assert reap(pid), "retained discovery reader leaked a process"

    class HealthyQueue:
        def ready_items(self):
            return []

    recovered = worker.discover_ready_snapshot(HealthyQueue(), budget_s=5.0,
                                               abandoned=abandoned)
    # The exited child is reaped by waitpid, the fence clears on its own, and
    # the next poll discovers again.
    assert abandoned == [], abandoned
    assert recovered.status == "ready", recovered
    assert recovered.snapshot == [], recovered
