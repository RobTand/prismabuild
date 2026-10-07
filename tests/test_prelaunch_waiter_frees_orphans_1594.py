"""A declared prelaunch waiter turns orphans into room (#1594, #598, #901).

On 2026-10-07 the stage tier of dl380g10 had minted 178 GiB, and 171 of them
were held by landed copies of consumers that had already ended.  The two PACT
prelaunch captures needed 88 and 102 GiB and the tier had 7 free.  Orphans are
a cache that the sweep gives back only under pressure.  So the question is
whether a declared waiter, which publishes nothing until its whole group is
held, creates that pressure.

Everything runs on a ``tmp_path`` queue and stage root, through the real tier
cycle (nothing here touches a live queue or a real stage mount).
"""
from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from prismabuild import prelaunch_group, residency_plan  # noqa: E402
import stage_release  # noqa: E402
from test_a_resident_range_is_adopted_rather_than_recopied import (  # noqa: E402
    PHASE_GIB, TIER, _cycle, _row, _stage_range)
from test_prelaunch_group_reconcile_1594 import _hexkey, _queue  # noqa: E402
from test_prelaunch_tier_gate_1594 import _live  # noqa: E402
from test_prelaunch_tier_module_1594 import _declared_plan  # noqa: E402

CAPACITY = 20
WITHDRAWN = "1" * 64


def _orphans(queue, stage, count: int) -> list[str]:
    """``count`` done movers of a withdrawn consumer, each holding PHASE_GIB."""
    queue.publish(**_row(queue, WITHDRAWN, {"cpu": 1, "mem_gb": 1}))
    queue.withdraw(WITHDRAWN, reason="orphan fixture", by="test")
    movers = []
    for ordinal in range(count):
        mover = _hexkey(f"orphan-mover-{ordinal}")
        _stage_range(queue, mover=mover, consumer=WITHDRAWN, stage=stage,
                     ordinal=ordinal, manifest="e" * 64)
        movers.append(mover)
    return movers


def _held_orphans(queue, movers: list[str]) -> int:
    ledger = queue.tier_ledger(TIER)
    return sum(1 for mover in movers if ledger.holder_tokens(mover))


def _prefix_tokens(queue, plan) -> int:
    """Tokens held for the declared prefix, wherever the reservation sits.

    The group first holds them as one holder; once the group splits, the
    leg movers hold them (the tokens move, none is freed in between).
    """
    ledger = queue.tier_ledger(TIER)
    held = sum(int(tokens.get("stage_gib", 0)) for holder, tokens in
               ((h, ledger.holder_tokens(h)) for h in ledger.held_keys())
               if str(holder).startswith(prelaunch_group.HOLDER_PREFIX))
    return held + sum(int(ledger.holder_tokens(lead).get("stage_gib", 0))
                      for lead in residency_plan.leads_for(plan))


def test_a_declared_waiter_that_fits_the_tier_gets_room_from_orphans(
        tmp_path) -> None:
    queue = _queue(tmp_path, stage_gib=CAPACITY)
    stage = tmp_path / "stage"
    stage.mkdir()
    stage_release.register_stage_root(queue, tier_id=TIER, stage_root=stage)
    movers = _orphans(queue, stage, 7)       # 14 of 20 held, 6 free
    consumer = _hexkey("waiter")
    plan = _declared_plan(queue, consumer,
                          [("p0", 10, True, 1), ("p1", 1, False, 1)], tag="w")
    _live(queue, plan, consumer)             # peak 11: above free, within 20
    assert queue.tier_ledger(TIER).available()["stage_gib"] == (
        CAPACITY - 7 * PHASE_GIB)
    assert _prefix_tokens(queue, plan) == 0
    for _ in range(6):
        _cycle(queue, stage, gib=CAPACITY)
    assert _held_orphans(queue, movers) < 7, "no orphan was given back"
    assert _held_orphans(queue, movers) >= 4, "more orphans left than needed"
    assert _prefix_tokens(queue, plan) == 10, "the declared prefix never reserved"


def test_a_declared_waiter_above_the_tier_takes_nothing(tmp_path) -> None:
    queue = _queue(tmp_path, stage_gib=CAPACITY)
    stage = tmp_path / "stage"
    stage.mkdir()
    stage_release.register_stage_root(queue, tier_id=TIER, stage_root=stage)
    movers = _orphans(queue, stage, 7)
    consumer = _hexkey("too-big")
    plan = _declared_plan(queue, consumer,
                          [("p0", 25, True, 1), ("p1", 1, False, 1)], tag="b")
    _live(queue, plan, consumer)             # peak 26: above the tier
    for _ in range(6):
        _cycle(queue, stage, gib=CAPACITY)
    assert _held_orphans(queue, movers) == 7, "futile eviction (#632)"
    assert _prefix_tokens(queue, plan) == 0


def test_control_an_undeclared_waiter_of_the_same_size_gets_room_from_orphans(
        tmp_path) -> None:
    """The fixture is valid: the streaming path already frees orphans (#598)."""
    queue = _queue(tmp_path, stage_gib=CAPACITY)
    stage = tmp_path / "stage"
    stage.mkdir()
    stage_release.register_stage_root(queue, tier_id=TIER, stage_root=stage)
    movers = _orphans(queue, stage, 7)
    consumer = _hexkey("streaming-waiter")
    plan = _declared_plan(queue, consumer,
                          [("p0", 10, False, 1), ("p1", 1, False, 1)], tag="s")
    _live(queue, plan, consumer)
    for _ in range(6):
        _cycle(queue, stage, gib=CAPACITY)
    assert _held_orphans(queue, movers) < 7, "the streaming path gave nothing"
