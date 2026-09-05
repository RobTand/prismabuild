"""Elastic worker-loop behavior through the supervisor's real main loop."""

from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

import supervise  # noqa: E402


class CycleComplete(Exception):
    """Stop a long-lived supervisor after its first real cycle."""


@pytest.fixture
def supervisor(tmp_path, monkeypatch):
    config = tmp_path / "fleet_boxes.json"
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
    monkeypatch.setattr(supervise, "_growth_batch", lambda _ceiling: 2)
    monkeypatch.setattr(supervise, "_is_fleet_loop", lambda *_a, **_k: True)
    return monkeypatch


def _stop_after_cycle(monkeypatch, sleeps):
    def sleep(seconds):
        sleeps.append(seconds)
        if seconds >= supervise.BUSY_INTERVAL_S:
            raise CycleComplete
    monkeypatch.setattr(supervise.time, "sleep", sleep)


def test_busy_loops_and_ready_work_grow_in_one_bounded_batch(
    supervisor, monkeypatch,
):
    monkeypatch.setattr(sys, "argv", ["supervise", "--interval-s", "30"])
    monkeypatch.setattr(supervise, "_live_loops", lambda: [11, 22])
    claims = 0

    def holders():
        nonlocal claims
        claims += 1
        return frozenset({11, 22})

    monkeypatch.setattr(supervise, "_claim_holders", holders)
    monkeypatch.setattr(supervise, "_ready_backlog", lambda: True)
    spawned = []
    monkeypatch.setattr(supervise, "_spawn",
                        lambda args, slot: spawned.append((list(args), slot)) or 9000 + slot)
    sleeps = []
    _stop_after_cycle(monkeypatch, sleeps)

    with pytest.raises(CycleComplete):
        supervise.main()

    assert spawned == [(["--class", "x86"], 0),
                       (["--class", "x86"], 1)]
    assert claims == 1, "one cycle performs one shared claim census"
    assert sleeps == [0.04, 5.0], "spawn staggering is amortized; busy ticks are fast"


def test_idle_pollers_show_admission_refusal_and_stop_further_growth(
    supervisor, monkeypatch,
):
    monkeypatch.setattr(sys, "argv", ["supervise"])
    monkeypatch.setattr(supervise, "_live_loops", lambda: [11, 22, 33])
    monkeypatch.setattr(supervise, "_claim_holders", lambda: frozenset())
    monkeypatch.setattr(supervise, "_has_children", lambda _pid: False)
    monkeypatch.setattr(supervise, "_ready_backlog", lambda: True)
    spawned = []
    monkeypatch.setattr(supervise, "_spawn",
                        lambda *_a: spawned.append(True) or 9000)
    sleeps = []
    _stop_after_cycle(monkeypatch, sleeps)

    with pytest.raises(CycleComplete):
        supervise.main()

    assert spawned == []
    assert sleeps == [5.0]


def test_cleared_backlog_retires_only_proven_idle_excess(
    supervisor, monkeypatch,
):
    monkeypatch.setattr(sys, "argv", ["supervise"])
    monkeypatch.setattr(supervise, "_live_loops", lambda: [11, 22, 33, 44])
    monkeypatch.setattr(supervise, "_claim_holders", lambda: frozenset({11}))
    monkeypatch.setattr(supervise, "_has_children", lambda _pid: False)
    monkeypatch.setattr(supervise, "_ready_backlog", lambda: False)
    killed = []
    monkeypatch.setattr(supervise.os, "kill",
                        lambda pid, _sig: killed.append(pid))
    monkeypatch.setattr(supervise, "_spawn",
                        lambda *_a: pytest.fail("scale-down must not respawn"))
    sleeps = []
    _stop_after_cycle(monkeypatch, sleeps)

    with pytest.raises(CycleComplete):
        supervise.main()

    assert killed == [33, 44]
    assert 11 not in killed
    assert sleeps == [5.0], "an active claim keeps the supervisor on a fast tick"


def test_once_keeps_the_configured_floor_for_deterministic_maintenance(
    supervisor, monkeypatch,
):
    monkeypatch.setattr(sys, "argv", ["supervise", "--once"])
    monkeypatch.setattr(supervise, "_live_loops", lambda: [])
    monkeypatch.setattr(supervise, "_claim_holders", lambda: frozenset())
    monkeypatch.setattr(supervise, "_ready_backlog", lambda: True)
    spawned = []
    monkeypatch.setattr(supervise, "_spawn",
                        lambda _args, slot: spawned.append(slot) or 9000 + slot)
    monkeypatch.setattr(supervise.time, "sleep", lambda _seconds: None)

    assert supervise.main() == 0
    assert spawned == [0, 1]


def test_explicit_loops_is_a_fixed_operator_opt_out(supervisor, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["supervise", "--loops", "2"])
    monkeypatch.setattr(supervise, "_live_loops", lambda: [11, 22])
    monkeypatch.setattr(supervise, "_claim_holders",
                        lambda: frozenset({11, 22}))
    monkeypatch.setattr(supervise, "_ready_backlog", lambda: True)
    spawned = []
    monkeypatch.setattr(supervise, "_spawn",
                        lambda *_a: spawned.append(True) or 9000)
    sleeps = []
    _stop_after_cycle(monkeypatch, sleeps)

    with pytest.raises(CycleComplete):
        supervise.main()

    assert spawned == []
    assert sleeps == [5.0]


def test_unknown_claim_ownership_freezes_elastic_changes(
    supervisor, monkeypatch,
):
    monkeypatch.setattr(sys, "argv", ["supervise"])
    monkeypatch.setattr(supervise, "_live_loops", lambda: [11, 22, 33])
    monkeypatch.setattr(supervise, "_claim_holders", lambda: None)
    monkeypatch.setattr(supervise, "_ready_backlog", lambda: True)
    monkeypatch.setattr(supervise, "_spawn",
                        lambda *_a: pytest.fail("unknown ownership cannot grow"))
    killed = []
    monkeypatch.setattr(supervise.os, "kill", lambda pid, _sig: killed.append(pid))
    sleeps = []
    _stop_after_cycle(monkeypatch, sleeps)

    with pytest.raises(CycleComplete):
        supervise.main()

    assert killed == []
    assert sleeps == [5.0]


def test_housekeeping_ceiling_uses_visible_cpu_and_memory(monkeypatch):
    monkeypatch.setattr(supervise.os, "sched_getaffinity", lambda _pid: set(range(12)))
    monkeypatch.setattr(supervise, "_visible_memory_bytes",
                        lambda: 6 * 1024 * 1024 * 1024)

    assert supervise.housekeeping_ceiling(3) == 24
    assert supervise.housekeeping_ceiling(30) == 30


def test_log_slots_never_reuse_append_targets(tmp_path, monkeypatch):
    monkeypatch.setattr(supervise, "LOG_DIR", tmp_path)
    (tmp_path / "pb-worker-2.log").write_text("old evidence\n")
    (tmp_path / "pb-worker-19.log").write_text("newer evidence\n")
    (tmp_path / "pb-worker-nope.log").write_text("ignore\n")

    assert supervise._next_log_index() == 20
