"""A mover's typed refusal reaches the owner through `materialization_state`.

#804: an owner waiting for a staged copy polled a `failed` mover whose
receipt it could not read as a cause, and waited out its own staging
timeout. `materialization_state` now carries the mover's own refusal beside
the completeness answer, so `origin_unreachable` -- a produced-output
prefix the tier host cannot read -- is distinguishable from a mover defect
on the first attempt, and it is returned through the failure contract the
owner already reads (`ok: False` with `refusal`) so no consumer patch is
needed. Only the ACTIVE materialization's receipt can answer: a retired
predecessor's refusal is history. Silence stays silence: an absent or
unreadable receipt answers None, never a named failure, and never readiness.

These tests drive the real `Pool.execute` worker seam over the real sealed
mover `stage_move`, exactly as the restage suite does; the fixture
concessions are that suite's and are not repeated here.
"""
from __future__ import annotations

import shutil
from pathlib import Path
import sys

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO / "tools" / "fleet"))

import prismabuild.pool as pool  # noqa: E402
import prismabuild.produced_output as po  # noqa: E402
from test_produced_output_restage import (  # noqa: E402
    TIER, _World, _claim_mover, _descriptors, _prewrite, _two_real_generations,
)

PAYLOAD = b"R" * 700


def _publish(world: _World) -> str:
    descs = _descriptors(world.template, world.inst, "p1", PAYLOAD)
    _prewrite(world.q, world.inst, world.template, "b1", descs)
    published = world.first_publish("b1", descs)
    return str(published["mover_key"])


def _run_failed_mover(world: _World, mover: str, worker: str) -> dict:
    """Claim and execute a mover that must fail, WITHOUT ending its row.

    The owner must stop on the FIRST failed attempt, so the state read
    happens while the row is still claimed and no retry has been filed.
    """

    claimed = _claim_mover(world.q, worker)
    assert claimed["action_key"] == mover, claimed["action_key"]
    row = pool._read_json(world.q.item_path(pool.CLAIMED, mover))
    outcome = world.q.execute(row, timeout_s=240)
    assert outcome.get("returncode") not in (None, 0), outcome
    return outcome


def test_an_unreachable_origin_refusal_is_visible_to_the_owner(
        tmp_path: Path) -> None:
    """The full #804 path: a real committed batch, a real sealed mover, the
    owner's prefix removed as the tier host sees it, and the typed refusal
    through the failure contract the owner already raises on.

    ``ok: False`` with ``refusal: origin_unreachable`` is what the Stage A
    owner's `await_materialized` reads, so it fails fast on the receipt of
    the FIRST attempt -- the row here is still claimed and no retry has run.
    The mover/materialization identity is retained beside it.
    """

    world = _World(tmp_path, maxima=4096)
    mover = _publish(world)

    # The prefix exists only where it was produced; the mover runs where it
    # is not.
    shutil.rmtree(world.template["output_prefix"])
    _run_failed_mover(world, mover, "w-mover-804")

    state = po.materialization_state(world.q, world.inst, world.template,
                                     batch_id="b1")
    assert state["ok"] is False, state
    assert state["refusal"] == "origin_unreachable", state
    assert state["mover_refusal"] == "origin_unreachable", state
    assert state["mover_receipt_complete"] is False, state
    assert state["mover_key"] == mover
    assert state["mover_queue_state"] == "claimed", state
    assert state["generation"] == 0
    assert state["batch_id"] == "b1"
    assert state["tier"]

    world.q.finish(mover, status="failed")


def test_a_retired_predecessors_refusal_does_not_poison_the_successor(
        tmp_path: Path) -> None:
    """Only the ACTIVE materialization answers the state.

    A retired generation whose receipt refused says nothing about the
    successor that replaced it: its refusal is history, and the active
    state must stay ``ok`` while the successor's own receipt is silent or
    clean.
    """

    world, first, second = _two_real_generations(tmp_path)
    world.q.record_move(first, {
        "tier_id": TIER,
        "bytes_staged": 0, "entries_declared": 0, "entries_staged": 0,
        "complete": False, "refusal": "origin_unreachable"})

    state = po.materialization_state(world.q, world.inst, world.template,
                                     batch_id="b1")
    assert state["mover_key"] == second, state
    assert state["generation"] == 2, state
    assert state["ok"] is True, state
    assert "refusal" not in state, state
    assert state["mover_refusal"] is None, state
    assert state["mover_receipt_complete"] is True, state


def test_an_unfiled_receipt_is_unknown_and_never_a_refusal(
        tmp_path: Path) -> None:
    """A mover that never filed a receipt leaves both answers unknown.

    Absence of a receipt is silence, not a report: the owner must not read
    it as a named origin failure, and must not read it as readiness either.
    """

    world = _World(tmp_path, maxima=4096)
    mover = _publish(world)

    _claim_mover(world.q, "w-mover-804-silent")
    world.q.finish(mover, status="failed")

    state = po.materialization_state(world.q, world.inst, world.template,
                                     batch_id="b1")
    assert state["ok"] is True, state
    assert state["mover_refusal"] is None, state
    assert state["mover_receipt_complete"] is None, state


def test_a_successful_mover_files_no_refusal(tmp_path: Path) -> None:
    """The positive control: a mover that landed the batch answers complete
    and carries no refusal."""

    world = _World(tmp_path, maxima=4096)
    mover = _publish(world)
    world.run_mover(mover, "w-mover-804-clean")

    state = po.materialization_state(world.q, world.inst, world.template,
                                     batch_id="b1")
    assert state["ok"] is True, state
    assert state["mover_receipt_complete"] is True, state
    assert state["mover_refusal"] is None, state
