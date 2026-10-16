"""Prelaunch journals exercised by real reservation and turn decisions."""
from __future__ import annotations

import base64
import json

UNIT, MOVER, PLAN = "7" * 64, "b" * 64, "d" * 64
TIER = "prismabuild-stage:compat"


def operate(request, store, load):
    family, variant = request["case"].split("/", 1)
    pg, pool = load("prismabuild.prelaunch_group"), load("prismabuild.pool")
    queue = pool.PoolQueue(store / "queue")
    queue.ensure_layout()
    holder = pg.holder_name(UNIT, TIER, [])
    group = pg.group_dir(queue, UNIT, TIER)
    ticket = {"unit": UNIT, "priority": 1, "published_unix": 99.0, "tier_demands": {TIER: 1}}
    write = request["op"] == "write"

    def intent(demand=1):
        pg.file_intent(queue, UNIT, holder, TIER, demand, [])

    def acquire(count):
        handle = queue.tier_ledger(TIER).begin_acquire(holder, {"stage_gib": count})
        assert handle
        queue.tier_ledger(TIER).commit_acquire(holder, handle)

    def committed():
        intent()
        acquire(1)
        assert pg.reconcile(queue, TIER, UNIT, holder, 1, [], writer_is_me=True).state == "reserved"

    def turn():
        assert pg.take_turn(queue, [ticket]).epoch == 0
        if variant == "next":
            pg.finish_turn(queue, 0, "complete")
            assert pg.take_turn(queue, [ticket]).epoch == 1
            return 1
        return 0

    queue.mint_tier_capacity(TIER, {"stage_gib": 2})
    demand = 2 if family == "pg-rollback" else 1
    epoch = 0
    if family == "pg-intent":
        path = group / "intent.json"
        if write:
            chunks = ([{"mover_action_key": MOVER, "start_bytes": 0, "end_bytes": 8,
                        "stage_gib": 1, "plan_sha256": PLAN, "consumer_action_key": UNIT}]
                      if variant == "chunk" else [])
            pg.file_intent(queue, UNIT, holder, TIER, 1, chunks,
                           epoch=None if variant == "without-epoch" else 0)
    elif family == "pg-decline":
        path = group / "declined.json"
        if write:
            pg.record_decline(queue, UNIT, TIER, 1, "compat-shortage")
    elif family in ("pg-commit", "pg-rollback", "pg-release"):
        if family == "pg-commit":
            path = group / "committed.json"
            if write:
                committed()
            else:
                intent()
                acquire(1)
        elif family == "pg-rollback":
            intent(demand)
            path = group / ("rolled-back-1.json" if variant == "second" else "rolled-back-0.json")
            if write:
                for _ in range(2 if variant == "second" else 1):
                    acquire(1)
                    result = pg.reconcile(queue, TIER, UNIT, holder, demand, [], writer_is_me=True)
                    assert "prelaunch-group-rolled-back" in result.events
        else:
            committed()
            path = group / "released.json"
            if write:
                for _ in range(2 if variant == "repeated" else 1):
                    assert "prelaunch-group-released" in pg.release_unit(queue, TIER, UNIT, holder, [], terminal=True)
            else:
                queue.tier_ledger(TIER).release(holder)
    elif family == "pg-ticket":
        unit = UNIT if variant == "plain" else "compat/escaped-unit"
        path = pg._ticket_file(queue, unit)
        if write:
            assert pg.turn_ticket(queue, unit, 1, 99.0, {TIER: 1})
    elif family in ("pg-turn", "pg-reserved", "pg-done"):
        if family == "pg-turn":
            if write:
                epoch = turn()
            elif variant == "next":
                pg.take_turn(queue, [ticket])
                pg.finish_turn(queue, 0, "complete")
                epoch = 1
            path = queue.root / pg.TURN_DIR / f"turn-{epoch}.json"
        else:
            turn()
            if family == "pg-reserved":
                path = queue.root / pg.TURN_DIR / f"turn-0.reserved-{pg.tier_digest(TIER)}.json"
                if variant == "multiple":
                    pg.record_reserved(queue, 0, "prismabuild-stage:other")
                if write:
                    pg.record_reserved(queue, 0, TIER)
            else:
                path = queue.root / pg.TURN_DIR / "turn-0.done"
                if write:
                    pg.finish_turn(queue, 0, variant)
    else:
        raise ValueError(f"unknown prelaunch adapter: {family}")
    if write:
        return {"raw": base64.b64encode(path.read_bytes()).decode()}
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(base64.b64decode(request["raw"]))
    request["_consumer_boundary"]()
    if family == "pg-decline":
        result = pg.latest_decline_reason(queue, UNIT, TIER)
        assert result == "compat-shortage", "decline consumer lost the shortage cause"
    elif family == "pg-ticket":
        result = pg.turn_ticket(queue, unit, 1, 99.0, {TIER: 1})
        assert result, "standing ticket was not adopted"
    elif family in ("pg-turn", "pg-reserved", "pg-done"):
        result = pg.current_turn(queue)
        assert result["status"] == ("done" if family == "pg-done" else "open"), result
        assert result["unit"] == UNIT and result["epoch"] == epoch
        if family == "pg-reserved":
            assert TIER in result["reserved"], "reserved tier did not govern current turn"
        if family == "pg-done":
            assert result["reason"] == variant, "done marker did not close the turn with its cause"
    else:
        result = pg.reconcile(queue, TIER, UNIT, holder, demand, [], writer_is_me=False)
        expected = "reserved" if family == "pg-commit" else "done" if family == "pg-release" else "short"
        assert result.state == expected, result
        assert result.authority is (family in ("pg-commit", "pg-release"))
        result = {"state": result.state, "authority": result.authority}
    return result
