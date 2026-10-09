"""Alternating claim counts stop and respawn no loops (#1571).

The supervisor sizes loops to claims held plus a reserve. A claim
count that alternates between ticks -- a loop finishes one action and
claims the next between two censuses -- sized the box down and back up
every tick: sparky ran 10 to 11 loops while it held the same work
throughout. The repair: a shrink in the tick right after a shrink stops
only loops the earlier tick already proved idle and spared. Active
claims and unknown workers stay protected either way; the first shrink
still stops every proven-idle loop above the target.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

import supervise  # noqa: E402


class TicksDone(Exception):
    """Stop the supervisor after the planned ticks."""


@pytest.fixture
def box(tmp_path, monkeypatch):
    config = tmp_path / "fleet_boxes.json"
    import json
    config.write_text(json.dumps({"boxes": {"boxa": {
        "loops": 2, "args": ["--class", "x86"],
    }}}))
    monkeypatch.setattr(supervise, "CONFIG", config)
    monkeypatch.setattr(supervise, "MIRROR", tmp_path / "absent")
    monkeypatch.setattr(supervise, "CLAIM", tmp_path / "supervisor.claim")
    monkeypatch.setattr(supervise, "LOG_DIR", tmp_path / "logs")
    monkeypatch.setattr(supervise.socket, "gethostname", lambda: "boxa")
    monkeypatch.setattr(supervise, "loop_args_of",
                        lambda _pid: ["--class", "x86"])
    monkeypatch.setattr(supervise, "housekeeping_ceiling", lambda _floor: 8)
    monkeypatch.setattr(supervise, "_is_fleet_loop", lambda *_a, **_k: True)
    monkeypatch.setattr(supervise.os, "kill", lambda *_a, **_k: None)
    return monkeypatch


def test_an_alternating_claim_count_starts_and_stops_no_loop(
        box, monkeypatch):
    """Four ticks: settle at 3 loops, then 1 claim, a dip, 1 again."""
    monkeypatch.setattr(sys, "argv", ["supervise", "--interval-s", "30"])
    live = [11, 22, 33, 44]
    killed: list[int] = []
    monkeypatch.setattr(supervise, "_live_loops",
                        lambda: [pid for pid in live if pid not in killed])
    counts = [frozenset({11}), frozenset({11}), frozenset(),
              frozenset({11}), frozenset(), frozenset()]
    monkeypatch.setattr(supervise, "_claim_holders",
                        lambda: counts.pop(0) if counts else frozenset())
    monkeypatch.setattr(supervise, "_has_children", lambda _pid: False)
    monkeypatch.setattr(supervise, "_ready_backlog", lambda: False)
    monkeypatch.setattr(supervise.os, "kill",
                        lambda pid, _sig: killed.append(pid))
    spawned: list = []
    monkeypatch.setattr(supervise, "_spawn",
                        lambda *_a: spawned.append(True) or 9000)
    ticks = iter([None, None, None, None])

    def sleep(seconds):
        try:
            next(ticks)
        except StopIteration:
            raise TicksDone

    monkeypatch.setattr(supervise.time, "sleep", sleep)

    with pytest.raises(TicksDone):
        supervise.main()

    # Tick 1 settles 4 loops to the 1 claim plus reserve: loop 44 goes.
    # Tick 2 holds the claim: nothing moves. Tick 3 dips to 0, but the
    # two-tick maximum still sizes to 3, so nothing stops. Tick 4 is
    # back at 1 claim: still 3, so nothing spawns either.
    assert killed == [44]
    assert spawned == []


def test_a_steady_low_claim_count_still_shrinks(
        box, monkeypatch):
    """The maximum delays a steady shrink by one tick, never strands it."""
    monkeypatch.setattr(sys, "argv", ["supervise", "--interval-s", "30"])
    live = [11, 22, 33, 44]
    monkeypatch.setattr(supervise, "_live_loops",
                        lambda: [pid for pid in live if pid not in killed])
    monkeypatch.setattr(supervise, "_claim_holders", lambda: frozenset())
    monkeypatch.setattr(supervise, "_has_children", lambda _pid: False)
    monkeypatch.setattr(supervise, "_ready_backlog", lambda: False)
    killed: list[int] = []
    monkeypatch.setattr(supervise.os, "kill",
                        lambda pid, _sig: killed.append(pid))
    monkeypatch.setattr(supervise, "_spawn",
                        lambda *_a: pytest.fail("shrink must not respawn"))
    ticks = iter([None, None, None])

    def sleep(seconds):
        try:
            next(ticks)
        except StopIteration:
            raise TicksDone

    monkeypatch.setattr(supervise.time, "sleep", sleep)

    with pytest.raises(TicksDone):
        supervise.main()

    # Tick 1 already sizes to the floor of 2 and stops one loop; tick
    # 2 learns the steady 0 and stops the other; tick 3 holds the count.
    assert sorted(killed) == [33, 44]


def test_unknown_workers_are_never_stopped(box, monkeypatch):
    """A loop whose state the census cannot read survives every shrink."""
    monkeypatch.setattr(sys, "argv", ["supervise", "--interval-s", "30"])
    monkeypatch.setattr(supervise, "_live_loops", lambda: [11, 22, 33, 44])
    monkeypatch.setattr(supervise, "_claim_holders", lambda: frozenset({11}))
    states = {22: False, 33: None, 44: False}

    def children(pid):
        state = states[pid]
        if state is None:
            return None
        return state

    monkeypatch.setattr(supervise, "_has_children", children)
    monkeypatch.setattr(supervise, "_ready_backlog", lambda: True)
    killed: list[int] = []
    monkeypatch.setattr(supervise.os, "kill",
                        lambda pid, _sig: killed.append(pid))
    monkeypatch.setattr(supervise, "_spawn",
                        lambda *_a: pytest.fail("shrink must not respawn"))
    ticks = iter([None])

    def sleep(seconds):
        try:
            next(ticks)
        except StopIteration:
            raise TicksDone

    monkeypatch.setattr(supervise.time, "sleep", sleep)

    with pytest.raises(TicksDone):
        supervise.main()

    assert 33 not in killed
    assert 11 not in killed
