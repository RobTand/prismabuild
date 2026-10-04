"""Real signal/epoch/file controls for tier startup and finished-cycle parking.

Private namespace fixtures exercise the production serving loop and signal
handlers. They are source/component controls, not live c437 quiescence proof.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import signal
import sys
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))
import tier_loop  # noqa: E402


@pytest.fixture
def scope(tmp_path, monkeypatch):
    gate = tmp_path / "maintenance.json"
    parked = tmp_path / "parked"
    parked.mkdir()
    gate.write_text(json.dumps({"draining": False}))
    runtime = tier_loop.runtime_gate
    monkeypatch.setattr(runtime, "MAINTENANCE_GATE", gate)
    monkeypatch.setattr(runtime, "PARKED_ROOT", parked)
    monkeypatch.setattr(runtime, "loaded_runtime_commit", lambda: "fixed")
    monkeypatch.setattr(runtime, "published_commit", lambda: "fixed")
    monkeypatch.setattr(runtime, "_generation_at", lambda path: "fixed")
    args = SimpleNamespace(pool_root=str(tmp_path / "queue"), source_pool=None,
                           interval_s=1.0, once=True)
    events = []

    class Queue:
        def __init__(self, root):
            self.root = root
            events.append("construct")

        def ensure_layout(self):
            events.append("layout")
            self.root.mkdir()

    def start(queue, **kwargs):
        events.append("adopt")
        (queue.root / "adoption").write_text("finished")
        return object(), object()

    monkeypatch.setattr(tier_loop.pool, "PoolQueue", Queue)
    monkeypatch.setattr(tier_loop, "_start", start)
    monkeypatch.setattr(tier_loop, "tier_cycle_line", lambda *args: {"event": "cycle-final"})
    monkeypatch.setattr(tier_loop, "cycle", lambda *args, **kwargs: events.append("cycle") or [])
    return SimpleNamespace(gate=gate, parked=parked, args=args, events=events,
                           root=tmp_path, Queue=Queue)


def close(scope, epoch=101.5):
    scope.gate.write_text(json.dumps({"draining": True, "changed_unix": epoch,
                                    "owner": "controlled-test"}))


def markers(scope):
    return sorted(scope.parked.iterdir())


def test_closed_before_start_has_zero_queue_or_adoption_mutations(scope):
    close(scope)
    assert tier_loop._serve(scope.args) == 75
    assert scope.events == []
    assert not (scope.root / "queue").exists()
    assert len(markers(scope)) == 1
    assert markers(scope)[0].name.endswith("-101.5")


@pytest.mark.parametrize("contents", [None, "{broken", "[]", "{}"])
def test_unknown_gate_parks_without_starting_or_claiming_epoch(scope, contents):
    if contents is None:
        scope.gate.unlink()
    else:
        scope.gate.write_text(contents)
    assert tier_loop._serve(scope.args) == 75
    assert scope.events == []
    assert markers(scope)[0].name.endswith("-unknown")


def test_gate_closes_during_layout_and_blocks_adoption(scope, monkeypatch):
    original = scope.Queue.ensure_layout

    def layout(queue):
        original(queue)
        close(scope)

    monkeypatch.setattr(scope.Queue, "ensure_layout", layout)
    assert tier_loop._serve(scope.args) == 75
    assert scope.events == ["construct", "layout"]
    assert not (scope.root / "queue" / "adoption").exists()
    assert markers(scope)[0].name.endswith("-101.5")


def test_gate_closes_during_adoption_and_blocks_the_first_cycle(scope, monkeypatch):
    def start(queue, **kwargs):
        scope.events.append("adopt")
        (queue.root / "adoption").write_text("finished")
        close(scope)
        return object(), object()

    monkeypatch.setattr(tier_loop, "_start", start)
    assert tier_loop._serve(scope.args) == 75
    assert scope.events == ["construct", "layout", "adopt"]
    assert (scope.root / "queue" / "adoption").read_text() == "finished"
    assert markers(scope)[0].name.endswith("-101.5")


@pytest.mark.parametrize("signum", [signal.SIGTERM, signal.SIGINT])
def test_signal_during_cycle_finishes_mutations_then_posts_current_epoch(scope, monkeypatch, signum):
    before = {s: signal.getsignal(s) for s in (signal.SIGTERM, signal.SIGINT)}
    scope.args.once = False

    def cycle(queue, **kwargs):
        scope.events.append("cycle-begin")
        (queue.root / "first-write").write_text("owned")
        close(scope, 202.5)
        os.kill(os.getpid(), signum)
        assert markers(scope) == [], "signal must not acknowledge an unfinished cycle"
        (queue.root / "final-write").write_text("committed")
        scope.events.append("cycle-end")
        return []

    real_post = tier_loop.runtime_gate.post_park_marker

    def post(gate):
        assert scope.events[-1] == "cycle-end"
        assert (scope.root / "queue" / "final-write").read_text() == "committed"
        return real_post(gate)

    monkeypatch.setattr(tier_loop, "cycle", cycle)
    monkeypatch.setattr(tier_loop.runtime_gate, "post_park_marker", post)
    assert tier_loop._serve(scope.args) == 0
    assert scope.events.count("cycle-begin") == 1
    assert markers(scope)[0].name.endswith("-202.5")
    assert {s: signal.getsignal(s) for s in before} == before


def test_signal_during_final_log_waits_for_final_write_before_park(scope, monkeypatch):
    scope.args.once = False

    def line(*args):
        close(scope, 303.5)
        os.kill(os.getpid(), signal.SIGTERM)
        assert markers(scope) == []
        (scope.root / "queue" / "final-log").write_text("complete")
        return {"event": "cycle-final"}

    real_post = tier_loop.runtime_gate.post_park_marker

    def post(gate):
        assert (scope.root / "queue" / "final-log").read_text() == "complete"
        return real_post(gate)

    monkeypatch.setattr(tier_loop, "tier_cycle_line", line)
    monkeypatch.setattr(tier_loop.runtime_gate, "post_park_marker", post)
    assert tier_loop._serve(scope.args) == 0
    assert markers(scope)[0].name.endswith("-303.5")


def test_rotated_epoch_during_post_does_not_certify_cooperative_stop(scope, monkeypatch):
    scope.args.once = False

    def cycle(queue, **kwargs):
        close(scope, 404.5)
        os.kill(os.getpid(), signal.SIGTERM)
        return []

    real_post = tier_loop.runtime_gate.post_park_marker

    def post(gate):
        marker = real_post(gate)
        close(scope, 405.5)
        return marker

    monkeypatch.setattr(tier_loop, "cycle", cycle)
    monkeypatch.setattr(tier_loop.runtime_gate, "post_park_marker", post)
    assert tier_loop._serve(scope.args) == 75
    assert all(not p.name.endswith("-405.5") for p in markers(scope))


def test_failed_cycle_and_signal_never_posts_a_completed_boundary(scope, monkeypatch):
    scope.args.once = False

    def cycle(queue, **kwargs):
        close(scope)
        os.kill(os.getpid(), signal.SIGTERM)
        raise OSError("unfinished final mutation")

    monkeypatch.setattr(tier_loop, "cycle", cycle)
    assert tier_loop._serve(scope.args) == 75
    assert markers(scope) == []


def test_marker_failure_refuses_current_epoch_proof(scope, monkeypatch):
    scope.args.once = False

    def cycle(queue, **kwargs):
        close(scope)
        os.kill(os.getpid(), signal.SIGTERM)
        return []

    monkeypatch.setattr(tier_loop, "cycle", cycle)
    monkeypatch.setattr(tier_loop.runtime_gate, "post_park_marker", lambda gate: None)
    assert tier_loop._serve(scope.args) == 75
    assert markers(scope) == []


def test_startup_exception_restores_both_signal_handlers(scope, monkeypatch):
    previous = {s: signal.getsignal(s) for s in (signal.SIGTERM, signal.SIGINT)}

    def layout(queue):
        raise OSError("startup namespace unavailable")

    monkeypatch.setattr(scope.Queue, "ensure_layout", layout)
    with pytest.raises(OSError, match="startup namespace"):
        tier_loop._serve(scope.args)
    assert {s: signal.getsignal(s) for s in previous} == previous
    assert markers(scope) == []


def test_stop_during_initial_identity_reads_precedes_all_startup_writes(scope, monkeypatch):
    def loaded():
        os.kill(os.getpid(), signal.SIGTERM)
        return "fixed"

    monkeypatch.setattr(tier_loop.runtime_gate, "loaded_runtime_commit", loaded)
    assert tier_loop._serve(scope.args) == 0
    assert scope.events == []
    assert markers(scope) == []


def test_one_shot_cycle_closure_parks_only_after_final_output(scope, monkeypatch):
    def cycle(queue, **kwargs):
        close(scope, 606.5)
        scope.events.append("cycle-complete")
        (queue.root / "cycle-complete").write_text("committed")
        return []

    real_post = tier_loop.runtime_gate.post_park_marker

    def post(gate):
        assert scope.events[-1] == "cycle-complete"
        assert (scope.root / "queue" / "cycle-complete").read_text() == "committed"
        return real_post(gate)

    monkeypatch.setattr(tier_loop, "cycle", cycle)
    monkeypatch.setattr(tier_loop.runtime_gate, "post_park_marker", post)
    assert tier_loop._serve(scope.args) == 75
    assert markers(scope)[0].name.endswith("-606.5")


def test_runtime_rotation_precedes_layout_even_when_gate_is_open(scope, monkeypatch):
    monkeypatch.setattr(tier_loop.runtime_gate, "published_commit", lambda: "successor")
    assert tier_loop._serve(scope.args) == 75
    assert scope.events == []
    assert markers(scope) == []


@pytest.mark.parametrize("stamp", [True, "epoch", -1, float("nan"), float("inf")])
def test_malformed_epoch_cannot_certify_a_cooperative_stop(scope, monkeypatch, stamp):
    scope.args.once = False

    def cycle(queue, **kwargs):
        scope.gate.write_text(json.dumps({"draining": True, "changed_unix": stamp}))
        os.kill(os.getpid(), signal.SIGTERM)
        return []

    monkeypatch.setattr(tier_loop, "cycle", cycle)
    assert tier_loop._serve(scope.args) == 75
