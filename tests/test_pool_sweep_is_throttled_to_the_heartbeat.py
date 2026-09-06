"""The reaper runs once per heartbeat per BOX, not once per poll per loop.

Issue #205: an 80-CPU box went invisible to placement while idle.  The offer
in ``workers/dl380g10.json`` aged past ``OFFER_TIMEOUT_S`` and ``offers()``
dropped the box from ``live``, so nine READY items were placeable only on the
one box that was already saturated.  ``announce`` sits at the top of the
worker's poll cycle (``tools/fleet/worker_loop.py``), so the offer lands only
as often as the cycle turns -- and the cycle was turning slower than 120 s on
a box with nothing to do.

What the cycle was spending its time on, measured on ``dl380g10``
2026-09-06 16:5x UTC, read-only:

* ``/proc/loadavg`` 0.44 across 80 CPUs, zero processes in ``D``, ZFS idle.
  There is no load story available for this box.
* Fifteen of its eighteen ``worker_loop.py`` processes were in
  ``__break_lease`` at the same instant.
* Reading twenty ``pb-queue`` records from the box itself took 86.4 s in
  total: seventeen under a millisecond, and three at 11.9 s, 34.2 s and
  40.3 s.
* ``/proc/locks`` showed the box's admission ``flock`` with twelve waiters
  behind one holder, and the holder listed as ``DELEG BREAKER WRITE`` on
  ``claimed/<key>.lease``.

The box exports the pool: ``/mnt/shared`` is its own ZFS dataset and it runs
``nfsd``.  Its local reads of a lease another box is writing must recall that
box's NFSv4 delegation, and the recall blocks on the remote client.  So the
poll cost was not throughput, CPU, or the network -- it was one delegation
recall per lease read, multiplied by the number of loops, multiplied by their
poll rate.  ``serve_once`` reaped on every poll, and ``reap_stale`` reads
every claimed record and every lease.

The fix is not to read faster or lock less: it is to not re-read, tens of
times a second, files that change once per ``HEARTBEAT_S``.  These tests pin
the throttle's three load-bearing properties -- it is per box (which is what
makes it worth anything, since the storm is loops x polls), it is not per
pool (a second box sweeps on its own clock), and it does not change what the
reaper concludes.
"""

from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from prismabuild import adaptive_cpu, pool  # noqa: E402


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch):
    """A clock ``pool`` reads and the sweep marker is stamped with.

    ``_sweep_due`` compares ``_now()`` against the marker's mtime and writes
    that same value back, so one fake clock governs both halves and the test
    never waits on the wall.
    """

    at = [1_788_700_000.0]
    monkeypatch.setattr(pool, "_now", lambda: at[0])
    return at


def _queue(tmp_path: Path, name: str) -> pool.PoolQueue:
    queue = pool.PoolQueue(tmp_path / name)
    queue.ensure_layout()
    return queue


def _sweeps(queue: pool.PoolQueue, monkeypatch: pytest.MonkeyPatch) -> list[int]:
    """Count reaper runs without changing what the reaper does."""

    calls = [0]
    real = queue.reap_stale

    def counted(*args, **kwargs):
        calls[0] += 1
        return real(*args, **kwargs)

    monkeypatch.setattr(queue, "reap_stale", counted)
    return calls


def test_the_first_poll_sweeps_and_the_rest_of_the_heartbeat_does_not(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, clock) -> None:
    """Once per ``HEARTBEAT_S``, not once per poll.

    Twenty-nine polls a second apart is what a loop does while the queue is
    non-empty (``worker_loop.PRESSURE_POLL_S`` is 1 s).  Before the throttle
    that was twenty-nine full passes over ``claimed/`` and every lease in it.
    """

    queue = _queue(tmp_path, "q")
    calls = _sweeps(queue, monkeypatch)

    for _ in range(30):
        assert queue.serve_once() is None
        clock[0] += 1.0

    assert calls[0] == 1, "one sweep in the first 30 s"
    clock[0] += pool.HEARTBEAT_S
    assert queue.serve_once() is None
    assert calls[0] == 2, "and one more once the heartbeat has passed"


def test_the_throttle_is_shared_by_every_loop_on_the_box(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, clock) -> None:
    """The load-bearing property: it is per box, not per process.

    A box runs one loop per class and ``supervise`` grows more of them;
    ``dl380g10`` was running eighteen.  A per-process throttle would have
    divided the storm by the poll rate and multiplied it back by the loop
    count.  Two ``PoolQueue`` objects here stand in for two sibling loops:
    they share nothing but the pool and the host, which is exactly what the
    real loops share.
    """

    first = _queue(tmp_path, "q")
    second = pool.PoolQueue(first.root)
    a = _sweeps(first, monkeypatch)
    b = _sweeps(second, monkeypatch)

    for _ in range(9):
        first.serve_once()
        second.serve_once()
        clock[0] += 1.0

    assert a[0] + b[0] == 1, "eighteen polls across two loops, one sweep"
    assert a[0] == 1 and b[0] == 0, "the loop that got there first did it"


def test_a_second_box_serving_the_same_pool_sweeps_on_its_own_clock(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, clock) -> None:
    """The identity is the box, so one box cannot silence another.

    ``reap_stale``'s ``finish_pending`` branch is owner-only, so a sweep that
    covered the whole fleet from whichever box swept first would be wrong as
    well as fragile.
    """

    queue = _queue(tmp_path, "q")
    calls = _sweeps(queue, monkeypatch)
    queue.serve_once()
    assert calls[0] == 1

    monkeypatch.setattr(adaptive_cpu.socket, "gethostname", lambda: "another-box")
    queue.serve_once()
    assert calls[0] == 2, "a different host hashes to a different marker"


def test_an_expired_lease_is_still_requeued_within_one_heartbeat(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, clock) -> None:
    """Detection is preserved, which is the whole safety argument.

    A lease expires at ``LEASE_TIMEOUT_S``; the throttle can delay noticing
    it by at most ``HEARTBEAT_S``, the interval at which its own writer
    refreshes it.  Nothing the reaper reads can move faster than that, so the
    sweep it skipped had nothing to find.
    """

    queue = _queue(tmp_path, "q")
    key = "d" * 64
    queue.publish(action_key=key, cas_root=str(tmp_path / "cas"),
                  checkout_root=None, worker_script="worker.py",
                  resources={"cpu": 1, "mem_gb": 1})
    claimed = queue.item_path(pool.READY, key).rename(queue.item_path(pool.CLAIMED, key))
    record = json.loads(claimed.read_text())
    record["claimed_unix"] = clock[0] - (pool.LEASE_TIMEOUT_S + pool.HEARTBEAT_S)
    record["claimed_by"] = "gone:1:deadbeef"
    record["claimed_host"] = "gone"
    claimed.write_text(json.dumps(record))
    pool._write_json_atomic(queue.lease_path(key), {
        "action_key": key, "owner": "gone:1:deadbeef",
        "heartbeat_unix": clock[0] - (pool.LEASE_TIMEOUT_S + pool.HEARTBEAT_S),
    })

    # The sweep that runs is the first one, and it finds it.
    assert queue.serve_once() is None
    assert queue.item_path(pool.READY, key).exists(), "requeued by the reaper"
