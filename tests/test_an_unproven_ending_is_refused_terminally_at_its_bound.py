"""An owner whose ending can never be proven ends its mover's loop (#1004 item 2).

#966 left one refusal retryable: a divergent staged name whose owner's ending
cannot be proven.  Some such endings settle by themselves -- a queued or
leased key, a queue that did not read -- but some never do: a legacy
consumer with no outcome record (#798), or a fragment that stays unreadable.
There the mover refused before any copy, exited incomplete, and the window
republished it every cycle, forever.

Each run now files the evidence it refused on and how many consecutive runs
refused on the same evidence (``stage_move.unproven_streak``).  At
``UNPROVEN_ENDING_RUNS`` runs spanning ``UNPROVEN_ENDING_MIN_SPAN_S`` the run
refuses ``staged_destination_unproven``, names the path and the unproven
owner, exits 1 and retires its own window with the #708 record, so the loop
ends.  A cause that settles by itself is never counted.  Nothing is replaced
in any case.

Driven through the real ``stage_move.main``, ``PoolQueue.claim``/``finish``
and ``tier_loop.residency_window`` on the #966 fixtures.
"""
from __future__ import annotations

import os
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

import test_a_divergent_staged_copy_never_loops_its_mover as loop  # noqa: E402
import test_dead_owner_fragment_blocks_then_retires as base  # noqa: E402
from test_dead_owner_fragment_blocks_then_retires import fleet  # noqa: E402,F401
from prismabuild import pool, residency_map, residency_plan  # noqa: E402
import stage_move  # noqa: E402

BOUND = stage_move.UNPROVEN_ENDING_RUNS


def _unprovable(fleet, tmp_path, monkeypatch, cause: str):
    """An owner whose ending no wait will prove, and the successor's world."""

    queue, stage, _ = fleet
    consumer, mover = loop._old_owner(
        fleet, "absent" if cause == "no-outcome" else "failed")
    # The divergent name goes first where a fragment is unreadable: that
    # fails every proof closed, the adoptable name's too.
    order = "first" if cause == "unreadable-fragment" else "last"
    world = loop._World(fleet, tmp_path, monkeypatch, order)
    if cause == "unreadable-fragment":
        path = residency_map.fragment_path(
            queue.residency_fragment_root(), base._key(), base._key())
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("{not json")
    return world, consumer, mover, loop._staged(stage, loop._divergent(order))


@pytest.mark.parametrize("cause", ["no-outcome", "unreadable-fragment"])
def test_an_unprovable_ending_is_terminal_at_the_bound(
        fleet, tmp_path, monkeypatch, cause) -> None:
    """RED on the base source: the window republishes the mover forever."""

    queue, _stage, _ = fleet
    monkeypatch.setattr(stage_move, "UNPROVEN_ENDING_MIN_SPAN_S", 0.0)
    world, consumer, mover, path = _unprovable(fleet, tmp_path, monkeypatch,
                                               cause)
    before = os.stat(path)

    receipts = []
    for run in range(1, BOUND + 1):
        rc, receipt = world.run()
        receipts.append((rc, receipt))
        if run < BOUND:
            # Retryable below the bound: the count is filed, nothing else.
            assert receipt.get("refusal") in (None, "residency_moved_nothing")
            assert receipt["unproven"]["runs"] == run, receipt["unproven"]
            assert receipt["unproven"]["terminal"] is False
            assert residency_plan.superseded(queue, world.plan) is None

    rc, receipt = receipts[-1]
    assert rc == 1, (
        f"#1004 item 2 loop: run {BOUND} of an unprovable ending exited rc "
        f"{rc} with refusal={receipt.get('refusal')!r}, "
        f"unproven={receipt.get('unproven')}")
    assert receipt["refusal"] == stage_move.STAGED_DESTINATION_UNPROVEN
    assert receipt["complete"] is False
    block = receipt["unproven"]
    assert block["runs"] == BOUND and block["terminal"] is True
    assert block["settles"] is False
    conflict = receipt["conflict"]
    assert conflict["stage_path"] == str(path)
    assert conflict["consumer_action_key"] == world.successor
    assert conflict["mover_action_key"] == world.copier
    if cause == "no-outcome":
        assert conflict["owners"] == [{
            "consumer_action_key": consumer, "mover_action_key": mover,
            "state": "uncertain",
            "why": f"its consumer {consumer[:12]} has no outcome record",
            "settles": False}]
    else:
        # Every owner ended; what is unproven is the fragment nobody reads.
        assert conflict["owners"] == []
        assert "could not be read" in conflict["why"]
    # The window is retired by name, so the loop ends.
    assert receipt["plan_superseded"] is True
    marker = residency_plan.superseded(queue, world.plan)
    assert marker is not None and marker["movers"] == [world.copier]
    assert marker["reason"].startswith(stage_move.STAGED_DESTINATION_UNPROVEN)
    for key in (world.successor, world.copier):
        assert key in marker["reason"], (key, marker["reason"])
    if cause == "no-outcome":
        assert consumer in marker["reason"] and mover in marker["reason"]
    assert world.copier not in world.window(), "#1004 item 2 loop"
    # Nothing was ever replaced.
    after = os.stat(path)
    assert (after.st_ino, after.st_mtime_ns) == (before.st_ino,
                                                 before.st_mtime_ns)
    assert path.read_bytes() == loop.OLD
    assert queue.tier_ledger(loop.TIER).holder_tokens(world.copier) == {}


def test_the_bound_waits_out_the_mounts_visibility_span(
        fleet, tmp_path, monkeypatch) -> None:
    """Runs republished faster than the span keep counting, and wait."""

    world, _consumer, _mover, _path = _unprovable(
        fleet, tmp_path, monkeypatch, "no-outcome")
    for _run in range(BOUND):
        rc, receipt = world.run()
    assert receipt["unproven"]["runs"] == BOUND
    assert receipt["unproven"]["terminal"] is False
    assert rc == 0 and receipt.get("refusal") is None, receipt.get("refusal")
    assert world.copier in world.window()


@pytest.mark.parametrize("cause", ["mover-queued", "consumer-lease"])
def test_an_ending_that_settles_by_itself_is_never_counted(
        fleet, tmp_path, monkeypatch, cause) -> None:
    """A queued mover or a lease outliving its record ends by itself."""

    queue, _stage, _ = fleet
    monkeypatch.setattr(stage_move, "UNPROVEN_ENDING_MIN_SPAN_S", 0.0)
    consumer, mover = loop._old_owner(fleet, "failed")
    world = loop._World(fleet, tmp_path, monkeypatch, "last")
    if cause == "mover-queued":
        queue.publish(action_key=mover, cas_root="/cas", checkout_root="/co",
                      worker_script="/w.py", resources={"cpu": 1},
                      max_attempts=1, recompute=True, tags=["nowhere"])
    else:
        queue.lease_path(consumer).write_text("{}")
    for _run in range(BOUND + 1):
        rc, receipt = world.run()
        assert rc == 0 and receipt.get("refusal") is None, (
            receipt.get("refusal"), receipt.get("errors"))
        block = receipt["unproven"]
        assert block["settles"] is True and block["runs"] == 0, block
        assert block["terminal"] is False
    assert residency_plan.superseded(queue, world.plan) is None
    assert world.copier in world.window()


def test_changed_evidence_starts_the_count_again() -> None:
    """Only the same unprovable evidence continues a count."""

    owner = {"consumer_action_key": "c" * 64, "mover_action_key": "m" * 64,
             "state": "uncertain", "why": "its consumer ccc has no outcome "
             "record", "settles": False}
    seen = [{"stage_path": "/stage/a", "declared_sha256": None,
             "why": owner["why"], "owners": [owner], "settles": False}]
    first = stage_move.unproven_streak(None, seen, now=1000.0)
    assert first["runs"] == 1 and first["first_unix"] == 1000.0
    second = stage_move.unproven_streak({"unproven": first}, seen, now=1100.0)
    assert second["runs"] == 2 and second["first_unix"] == 1000.0
    # Another name of the same owner is the same evidence.
    moved = [dict(seen[0], stage_path="/stage/b")]
    assert stage_move.unproven_streak(
        {"unproven": second}, moved, now=1200.0)["runs"] == 3
    other = [dict(seen[0], owners=[dict(owner, why="two outcome records")])]
    assert stage_move.unproven_streak(
        {"unproven": second}, other, now=1200.0)["runs"] == 1
    # A run whose cause settles counts zero, and so restarts the next count.
    settling = [dict(seen[0], settles=True,
                     owners=[dict(owner, settles=True)])]
    zero = stage_move.unproven_streak({"unproven": second}, settling,
                                      now=1200.0)
    assert zero["runs"] == 0 and zero["evidence"] is None
    assert stage_move.unproven_streak({"unproven": zero}, seen,
                                      now=1300.0)["runs"] == 1
    assert stage_move.unproven_streak(None, [], now=1.0) is None
