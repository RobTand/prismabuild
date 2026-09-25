"""A consumer whose window a mover's conflict retired says why it waits (#1004 item 3).

After a ``staged_destination_conflict`` the mover marks its own window
superseded (the #708 record, #966), so nothing republishes the consumer's
lead and the consumer is never admitted.  It sits READY until an operator
resubmits it.  Before this change its claim denial read
``residency_lead_terminal`` naming a failed lead, and ``pbstatus
--starvation`` reported the plan with no word of the retirement: a stall
with no record, #983's pattern 4.

Now the retirement marker carries the conflict, structured, and both readers
name it: the reason, the staged path and both owners.  The same holds for
#1004 item 2's terminal ``staged_destination_unproven``.

Driven through the real ``stage_move.main``, ``PoolQueue.claim`` and the
starvation census on the #966 fixtures.
"""
from __future__ import annotations

from pathlib import Path
import socket
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

import test_a_divergent_staged_copy_never_loops_its_mover as loop  # noqa: E402
from test_dead_owner_fragment_blocks_then_retires import fleet  # noqa: E402,F401
from prismabuild import adaptive_cpu, pool, residency_plan  # noqa: E402
import pbstatus  # noqa: E402
import stage_move  # noqa: E402


def _successor_row(world) -> dict:
    [row] = [row for row in world.queue.ready_items()
             if row.get("action_key") == world.successor]
    return row


def _denial(queue: pool.PoolQueue, key: str) -> dict:
    path = adaptive_cpu.local_state_base(queue.ledger().base) / pool.CLAIM_DENIALS
    records = adaptive_cpu.read_json(path).get("records", {})
    return next(value for value in records.values()
                if value["action_key"] == key)


def _census(world) -> dict:
    queue = world.queue
    ready = {path.stem for path in queue.dir(pool.READY).glob("*.json")}
    claimed = {path.stem for path in queue.dir(pool.CLAIMED).glob("*.json")}
    notes: list[str] = []
    unreadable: list[str] = []
    entry = pbstatus._starvation_plan_entry(
        queue, world.successor,
        residency_plan.read(queue, world.successor),
        ready=ready, claimed=claimed, notes=notes, unreadable=unreadable,
        now=1700000000.0)
    assert entry["valid"] is True and not unreadable, notes
    return entry


def _retired(fleet, tmp_path, monkeypatch, refusal: str):
    """Run the successor's mover into the terminal refusal; return what it named."""

    if refusal == stage_move.STAGED_DESTINATION_CONFLICT:
        consumer, mover = loop._old_owner(fleet, "claimed")
        world = loop._World(fleet, tmp_path, monkeypatch, "last")
        # Before the refusal, a lead that is only coming names no retirement,
        # and the census says the window is not retired.
        assert "plan_superseded" not in fleet[0].residency_verdict(
            _successor_row(world))
        assert _census(world)["superseded"] is None
        rc, receipt = world.run()
        state = "live"
    else:
        monkeypatch.setattr(stage_move, "UNPROVEN_ENDING_MIN_SPAN_S", 0.0)
        consumer, mover = loop._old_owner(fleet, "absent")
        world = loop._World(fleet, tmp_path, monkeypatch, "last")
        for _run in range(stage_move.UNPROVEN_ENDING_RUNS):
            rc, receipt = world.run()
        state = "uncertain"
    assert rc == 1 and receipt["refusal"] == refusal, receipt.get("refusal")
    assert receipt["plan_superseded"] is True
    path = str(loop._staged(fleet[1], loop.NAMES[1]))
    return world, consumer, mover, state, path


def _assert_names_it(summary, *, refusal, path, consumer, mover, state,
                     world) -> None:
    assert isinstance(summary, dict), summary
    assert summary["refusal"] == refusal
    assert summary["stage_path"] == path
    assert summary["movers"] == [world.copier]
    assert summary["marked_by"] == "stage-move"
    assert summary["reason"].startswith(refusal)
    [owner] = summary["owners"]
    assert (owner["consumer_action_key"], owner["mover_action_key"],
            owner["state"]) == (consumer, mover, state)


@pytest.mark.parametrize("refusal", [stage_move.STAGED_DESTINATION_CONFLICT,
                                     stage_move.STAGED_DESTINATION_UNPROVEN])
def test_the_denial_and_the_census_name_the_retirement(
        fleet, tmp_path, monkeypatch, refusal) -> None:
    """RED on the base source: neither reader names the supersession."""

    queue, _stage, _ = fleet
    world, consumer, mover, state, path = _retired(
        fleet, tmp_path, monkeypatch, refusal)
    names = dict(refusal=refusal, path=path, consumer=consumer, mover=mover,
                 state=state, world=world)

    # The marker carries the conflict itself.
    marker = residency_plan.superseded(queue, world.plan)
    assert marker is not None
    assert marker["conflict"]["stage_path"] == path
    assert marker["conflict"]["refusal"] == refusal

    # The consumer is not admitted, and its denial says why.
    row = _successor_row(world)
    claimed = queue.claim(
        tags=["x86"], owner=f"{socket.gethostname()}:1:consumer",
        capacity={"cpu": 4, "mem_gb": 16}, ready=[row])
    assert claimed is None
    denial = _denial(queue, world.successor)
    assert denial["reason"] == "residency_lead_terminal", denial
    residency = denial["evidence"]["residency"]
    _assert_names_it(residency.get("plan_superseded"), **names)

    # So does the starvation census, for the READY consumer.
    entry = _census(world)
    assert entry["state"] == "ready"
    _assert_names_it(entry["superseded"], **names)


def test_an_unreadable_marker_is_named_not_absent(
        fleet, tmp_path, monkeypatch) -> None:
    """A retirement that cannot be read never reads as no retirement."""

    queue, _stage, _ = fleet
    world, *_ = _retired(fleet, tmp_path, monkeypatch,
                         stage_move.STAGED_DESTINATION_CONFLICT)
    path = residency_plan._superseded_path(
        queue, world.successor, residency_plan.plan_sha256(world.plan))
    mode = path.stat().st_mode
    path.chmod(0o644)          # the marker is published read-only
    path.write_text("{torn")
    path.chmod(mode)

    verdict = queue.residency_verdict(_successor_row(world))

    assert verdict["plan_superseded"]["unreadable"] is True, verdict
