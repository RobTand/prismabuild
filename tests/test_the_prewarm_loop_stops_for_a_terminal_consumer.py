"""A warm selected for a live row stops when the row's generation ends (#571).

The production shape: the storage loop selects a row, starts a multi-entry
warm, and the consumer fails mid-read.  The loop must stop issuing new reads
-- including while parked in a pacing hold -- while keeping the bytes already
read accounted exactly.  Generation-scoped: a successor under the same key is
a different request, and an unreadable queue is a reason to carry on, never
to cancel another action's scope.
"""
from __future__ import annotations

import json
import threading
from pathlib import Path
import sys
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import prewarm_loop  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import pool  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))
from prewarm_fixture import Fleet, phase_table  # noqa: E402
from test_the_prewarm_reader_is_paced_by_the_disks import (  # noqa: E402
    LOADED, FakeDisk, accumulate,
)


def _manifest_key(fleet: Fleet, seed: str, files) -> str:
    return fleet.action(
        seed, files,
        annotations={"phases": phase_table(
            [(f"phase-{i}", size) for i, (_path, size) in enumerate(files)])},
    )


def _partial_receipt(fleet: Fleet, key: str, warmed_bytes: int) -> None:
    request = prewarm_loop.sealed_request(fleet.cas_root, key)
    entry = prewarm_loop.manifest_input_of(request)
    assert entry is not None
    fleet.queue.record_prewarm(key, {
        "host": "test", "manifest_sha256": str(entry["sha256"]),
        "manifest_bytes": 16384, "entry_count": 2,
        "warmed_bytes": warmed_bytes, "bytes_warmed": warmed_bytes,
        "entries_warmed": 1, "status": "partial",
        "seconds": 0.1, "mb_per_s": 80.0, "phased": True,
        "warmed_through_phase": "phase-0",
    })


class _Gate:
    """Every liveness check blocks until the test's main thread acts.

    All reader threads pile up here before reading a byte, so the state flip
    under test -- finish, claim, republish -- always lands mid-warm rather
    than before it or after it.  Deterministic by construction: no sleep
    tunes the overlap.
    """

    def __init__(self, queue: pool.PoolQueue) -> None:
        self._queue = queue
        self._lock = threading.Lock()
        self.entered = threading.Event()
        self.proceed = threading.Event()
        self.calls = 0
        self._real = pool.PoolQueue.selection_live.__get__(queue)

    def __call__(self, action_key: str, *, published_unix) -> str:
        with self._lock:
            self.calls += 1
        self.entered.set()
        assert self.proceed.wait(timeout=60), "the main thread never acted"
        return self._real(action_key, published_unix=published_unix)

    def install(self) -> None:
        self._queue.selection_live = self.__call__  # type: ignore[method-assign]


def _run_cycle(fleet: Fleet, args) -> dict:
    box: dict = {}

    def target() -> None:
        try:
            box["event"] = fleet.cycle(args)
        except Exception as exc:  # noqa: BLE001 -- reported, not hidden
            box["error"] = exc

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    return {"thread": thread, "box": box}


def _join(run: dict) -> dict:
    run["thread"].join(timeout=60)
    assert not run["thread"].is_alive(), "the cycle did not come back"
    assert "error" not in run["box"], f"cycle raised: {run['box']['error']!r}"
    return run["box"]["event"]


def test_a_claimed_window_stops_when_its_consumer_fails(
        tmp_path: Path) -> None:
    """The #571 shape: terminal mid-warm ends new reads, keeps exact bytes."""

    fleet = Fleet(tmp_path)
    key = _manifest_key(fleet, "doomed", [fleet.file("a.pt", 8192),
                                          fleet.file("b.pt", 8192)])
    _partial_receipt(fleet, key, 8192)
    # One attempt: the failure below must retire the generation to ``failed``
    # rather than requeue it to ``ready`` with the same stamp, which the
    # watch correctly reads as still live.  The claim copies the ready item,
    # so the bound goes on before it.
    ready_path = fleet.queue.item_path(pool.READY, key)
    ready_record = pool._read_json(ready_path)
    assert ready_record is not None
    ready_record["max_attempts"] = 1
    pool._write_json_atomic(ready_path, ready_record)
    claimed = fleet.queue.claim()
    assert claimed is not None and claimed["action_key"] == key

    gate = _Gate(fleet.queue)
    gate.install()
    run = _run_cycle(fleet, fleet.args())
    assert gate.entered.wait(timeout=60)

    fleet.queue.finish(key, status="failed", detail={"status": "failed"},
                       claim_snapshot=claimed)
    gate.proceed.set()
    event = _join(run)

    assert gate.calls >= 1
    record = fleet.queue.prewarm(key)
    assert record is not None
    assert record["stopped_by"] == "terminal", record
    assert record["bytes_warmed"] < 16384
    assert record["status"] == "partial"
    assert record["errors"] == []
    advanced = [entry for entry in event["advanced"]
                if entry["action_key"] == key]
    assert advanced and advanced[0].get("stopped_by") == "terminal"
    assert "terminal x1" in event["summary"]


def test_a_ready_row_claimed_mid_warm_warms_to_the_end(
        tmp_path: Path) -> None:
    """A claim of the same generation is liveness, not an ending (#571).

    The point of prewarm is the window between publication and claim; a warm
    that stopped when its row was claimed would never serve anybody.
    """

    fleet = Fleet(tmp_path)
    key = _manifest_key(fleet, "claimed-mid-warm",
                        [fleet.file("c.pt", 8192), fleet.file("d.pt", 8192)])
    gate = _Gate(fleet.queue)
    gate.install()
    run = _run_cycle(fleet, fleet.args())
    assert gate.entered.wait(timeout=60)

    claimed = fleet.queue.claim()
    assert claimed is not None and claimed["action_key"] == key
    gate.proceed.set()
    event = _join(run)

    record = fleet.queue.prewarm(key)
    assert record is not None
    assert "stopped_by" not in record, record
    assert record["status"] == "complete"
    assert record["bytes_warmed"] == 16384
    assert event["summary"].startswith("prewarm cycle:")
    assert "\n" not in event["summary"]


def test_a_same_key_successor_stops_the_old_window(tmp_path: Path) -> None:
    """A new stamp under the same key is superseded, never live (#571)."""

    fleet = Fleet(tmp_path)
    key = _manifest_key(fleet, "superseded", [fleet.file("e.pt", 8192),
                                              fleet.file("f.pt", 8192)])
    _partial_receipt(fleet, key, 8192)
    claimed = fleet.queue.claim()
    assert claimed is not None

    gate = _Gate(fleet.queue)
    gate.install()
    run = _run_cycle(fleet, fleet.args())
    assert gate.entered.wait(timeout=60)

    successor = dict(pool._read_json(fleet.queue.item_path(pool.CLAIMED, key)))
    successor["published_unix"] = float(successor["published_unix"]) + 1000.0
    fleet.queue.item_path(pool.CLAIMED, key).write_text(json.dumps(successor))
    gate.proceed.set()
    _join(run)

    record = fleet.queue.prewarm(key)
    assert record is not None
    assert record["stopped_by"] == "superseded", record


def test_an_unknown_row_warms_to_the_end(tmp_path: Path) -> None:
    """Absent everywhere is a requeue in flight: carry on (#571)."""

    fleet = Fleet(tmp_path)
    key = _manifest_key(fleet, "unknown", [fleet.file("g.pt", 8192),
                                           fleet.file("h.pt", 8192)])
    gate = _Gate(fleet.queue)
    gate.install()
    run = _run_cycle(fleet, fleet.args())
    assert gate.entered.wait(timeout=60)

    # Selected while ready, then gone from every state directory: a requeue
    # or a reaper holds it between two atomic renames.
    fleet.queue.item_path(pool.READY, key).unlink()
    gate.proceed.set()
    event = _join(run)

    record = fleet.queue.prewarm(key)
    assert record is not None
    assert "stopped_by" not in record, record
    assert record["status"] == "complete"
    assert record["bytes_warmed"] == 16384
    assert event["summary"].startswith("prewarm cycle:")


def test_a_live_ready_row_warms_to_the_end(tmp_path: Path) -> None:
    """The ordinary warm still runs: the watch must not break what it guards."""

    fleet = Fleet(tmp_path)
    key = _manifest_key(fleet, "live", [fleet.file("i.pt", 8192),
                                        fleet.file("j.pt", 8192)])

    event = fleet.cycle(fleet.args())

    record = fleet.queue.prewarm(key)
    assert record is not None
    assert "stopped_by" not in record, record
    assert record["status"] == "complete"
    assert record["bytes_warmed"] == 16384
    assert event["summary"].startswith("prewarm cycle:")


def test_a_later_selection_warms_after_an_earlier_stop(
        tmp_path: Path) -> None:
    """A stop latches one watch, never the queue: eligible work proceeds."""

    fleet = Fleet(tmp_path)
    first = _manifest_key(fleet, "first", [fleet.file("k.pt", 8192),
                                           fleet.file("l.pt", 8192)])
    _partial_receipt(fleet, first, 8192)
    claimed = fleet.queue.claim()
    assert claimed is not None
    fleet.queue.finish(first, status="failed", detail={"status": "failed"},
                       claim_snapshot=claimed)

    second = _manifest_key(fleet, "second", [fleet.file("m.pt", 8192),
                                             fleet.file("n.pt", 8192)])
    event = fleet.cycle(fleet.args())

    # The retired row's receipt is untouched -- no warm selected it -- and the
    # eligible row warmed whole: a stop latches one watch, never the queue.
    retired = fleet.queue.prewarm(first)
    assert retired is not None
    assert "stopped_by" not in retired, retired
    record = fleet.queue.prewarm(second)
    assert record is not None
    assert "stopped_by" not in record, record
    assert record["status"] == "complete"
    assert record["bytes_warmed"] == 16384
    assert event["summary"].startswith("prewarm cycle:")


def test_a_reader_stops_mid_entries_when_its_watch_trips(
        tmp_path: Path) -> None:
    """Partial accounting stays exact: whole entries count, the rest does not."""

    fleet = Fleet(tmp_path)
    paths = [fleet.file(f"stop-{i}.pt", 1 << 16) for i in range(8)]
    entries = [{"path": path, "offset": 0, "bytes": 1 << 16}
               for path, _size in paths]
    mounts = prewarm_loop.MountMap([f"{fleet.mount}={fleet.mount}"])
    reader = prewarm_loop.Reader(1, mounts)
    calls = {"n": 0}

    def trip() -> bool:
        calls["n"] += 1
        return calls["n"] < 10
    trip.verdict = "terminal"  # type: ignore[attr-defined]

    result = reader.read(entries, budget_bytes=1 << 23,
                         stop=threading.Event(), is_live=trip)

    assert result["stopped_by"] == "terminal"
    assert 0 < int(result["bytes_warmed"]) < 8 * (1 << 16)
    assert int(result["contiguous_bytes"]) <= int(result["bytes_warmed"])
    assert result["errors"] == []
    assert calls["n"] >= 10


def test_a_pacing_hold_ends_when_the_row_does() -> None:
    """A hold entered for a live row must not outlive it (#571).

    The disk stays over every cap for the whole test -- the verdict never
    clears -- so only the abort ends the wait.  Fake clock throughout: the
    hold lengths are decisions, not races.
    """

    disk = FakeDisk(accumulate([LOADED] * 4))
    paced = disk.pacer()
    assert paced._verdict() is True
    disk.tick()
    assert paced._verdict() is True

    calls: list[int] = []

    def abort() -> bool:
        calls.append(1)
        return len(calls) <= 3

    paced.wait(threading.Event(), abort=abort)

    assert paced._verdict() is True, "the pool never cleared: the abort ended it"
    assert disk.slept == pytest.approx(3 * 0.25), disk.slept
    assert paced.report()["holds"] == 1


def test_a_warm_parked_in_a_hold_stops_with_the_row(
        tmp_path: Path) -> None:
    """Reader plus holding pacer plus terminal row: the #571 composition."""

    fleet = Fleet(tmp_path)
    paths = [fleet.file(f"held-{i}.pt", 1 << 16) for i in range(4)]
    entries = [{"path": path, "offset": 0, "bytes": 1 << 16}
               for path, _size in paths]
    disk = FakeDisk(accumulate([LOADED] * 64), step_s=0.001)
    paced = disk.pacer()
    mounts = prewarm_loop.MountMap([f"{fleet.mount}={fleet.mount}"])
    reader = prewarm_loop.Reader(1, mounts, pacer=paced, max_readers=1)
    calls = {"n": 0}

    def trip() -> bool:
        calls["n"] += 1
        return calls["n"] < 10
    trip.verdict = "terminal"  # type: ignore[attr-defined]

    started = time.monotonic()
    result = reader.read(entries, budget_bytes=1 << 22,
                         stop=threading.Event(), is_live=trip)
    elapsed = time.monotonic() - started

    assert result["stopped_by"] == "terminal"
    assert int(result["bytes_warmed"]) < 4 * (1 << 16)
    assert elapsed < 30, f"the hold outlived the row by {elapsed:.1f}s"
