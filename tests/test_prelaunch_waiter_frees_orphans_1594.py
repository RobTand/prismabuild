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
    PHASE_GIB, STAGE_KIND, TIER, _cycle, _row, _stage_range, _tier_record)
from test_prelaunch_group_reconcile_1594 import _hexkey, _queue  # noqa: E402
from test_prelaunch_tier_gate_1594 import _live  # noqa: E402
from test_prelaunch_tier_module_1594 import _declared_plan  # noqa: E402
from test_prelaunch_tier_module_1594 import _mover_row as _leg_row  # noqa: E402
from test_prelaunch_group_reconcile_1594 import GIB  # noqa: E402

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


def _prefix_tokens(queue, consumer, plan) -> int:
    """Tokens held for ONE consumer's declared prefix, wherever they sit.

    The group first holds them as one holder, named by its unit and phases;
    once the group splits, the leg movers hold them (the tokens move, none is
    freed in between).  Another unit's group is not this prefix.
    """
    ledger = queue.tier_ledger(TIER)
    holder = prelaunch_group.holder_name(
        consumer, TIER, residency_plan.prelaunch_phase_names(plan))
    return (int(ledger.holder_tokens(holder).get("stage_gib", 0))
            + sum(int(ledger.holder_tokens(lead).get("stage_gib", 0))
                  for lead in residency_plan.leads_for(plan)))


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
    assert _prefix_tokens(queue, consumer, plan) == 0
    for _ in range(6):
        _cycle(queue, stage, gib=CAPACITY)
    assert _held_orphans(queue, movers) < 7, "no orphan was given back"
    assert _held_orphans(queue, movers) >= 4, "more orphans left than needed"
    assert _prefix_tokens(queue, consumer, plan) == 10, "the declared prefix never reserved"


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
    assert _prefix_tokens(queue, consumer, plan) == 0


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


# -- review of PR 1615: the probe asks what the gate checks, and spares prefixes --


def _queued_row(queue, gib: int) -> None:
    """A ready stage mover row of another manifest: queued new money."""
    queue.publish(**_leg_row(queue, _hexkey("queued-demand"), 0, gib * GIB,
                             gib, TIER, "f" * 64, gib * GIB))


def test_the_relief_counts_queued_demand_the_gate_counts(tmp_path) -> None:
    """P1: held 14 + queued 4 + footprint 9 = 27 on a tier of 20.

    A probe that leaves the queued row out asks for 3 and evicts two orphans.
    The gate then still refuses (10 + 4 + 9 = 23), and the waiter stalls beside
    five orphans.  The relief must cover what the gate checks: four orphans go.
    """
    queue = _queue(tmp_path, stage_gib=CAPACITY)
    stage = tmp_path / "stage"
    stage.mkdir()
    stage_release.register_stage_root(queue, tier_id=TIER, stage_root=stage)
    movers = _orphans(queue, stage, 7)       # 14 of 20 held, 6 free
    _queued_row(queue, 4)
    consumer = _hexkey("waiter-with-queue")
    plan = _declared_plan(queue, consumer,
                          [("p0", 8, True, 1), ("p1", 1, False, 1)], tag="q")
    _live(queue, plan, consumer)             # peak 9
    for _ in range(8):
        _cycle(queue, stage, gib=CAPACITY)
    assert _held_orphans(queue, movers) <= 3, "relief stopped before the gate"
    assert _prefix_tokens(queue, consumer, plan) == 8, "the declared prefix never reserved"


def test_a_live_declared_prefix_is_not_counted_as_evictable(tmp_path) -> None:
    """P2: a waiter that fits only if a live prefix were evictable asks nothing.

    Tier 20: two orphans (4), a live declared consumer's landed prefix (2),
    free 14.  The waiter needs 19.  Free plus orphans is 18; the live prefix is
    protected, so it can never be given back.  The orphans must stay.

    A control, not the red test for P2: this fixture yields no horizon
    candidate, so it also passes on the head that omitted ``declared_keys``.
    The test that fails there is the wiring test below.
    """
    queue = _queue(tmp_path, stage_gib=CAPACITY)
    stage = tmp_path / "stage"
    stage.mkdir()
    stage_release.register_stage_root(queue, tier_id=TIER, stage_root=stage)
    movers = _orphans(queue, stage, 2)
    live = _hexkey("live-declared")
    live_plan = _declared_plan(queue, live,
                               [("a0", PHASE_GIB, True, 1),
                                ("a1", 1, False, 1)], tag="a")
    lead = residency_plan.leads_for(live_plan)[0]
    _stage_range(queue, mover=lead, consumer=live, stage=stage, ordinal=5,
                 manifest="d" * 64)
    _live(queue, live_plan, live)
    waiter = _hexkey("waiter-too-big-for-the-rest")
    plan = _declared_plan(queue, waiter, [("b0", 19, True, 1)], tag="b")
    _live(queue, plan, waiter)
    for _ in range(6):
        _cycle(queue, stage, gib=CAPACITY)
    assert _held_orphans(queue, movers) == 2, "futile eviction of orphans"
    assert _prefix_tokens(queue, waiter, plan) == 0


def test_the_pressure_census_receives_the_declared_keys(
        tmp_path, monkeypatch) -> None:
    """P2, wiring: the horizon candidates are asked with the protected keys."""
    import tier_loop
    queue = _queue(tmp_path, stage_gib=CAPACITY)
    stage = tmp_path / "stage"
    stage.mkdir()
    stage_release.register_stage_root(queue, tier_id=TIER, stage_root=stage)
    _orphans(queue, stage, 2)
    live = _hexkey("live-declared-2")
    live_plan = _declared_plan(queue, live,
                               [("a0", PHASE_GIB, True, 1),
                                ("a1", 1, False, 1)], tag="a2")
    _live(queue, live_plan, live)
    waiter = _hexkey("waiter-2")
    _live(queue, _declared_plan(queue, waiter, [("b0", 19, True, 1)], tag="b2"),
          waiter)
    seen: list[frozenset] = []
    real = tier_loop._beyond_horizon_candidates

    def _spy(*args, **kwargs):
        seen.append(frozenset(kwargs.get("declared_keys") or ()))
        return real(*args, **kwargs)

    monkeypatch.setattr(tier_loop, "_beyond_horizon_candidates", _spy)
    tier_loop.window_pressure(queue, tiers={TIER: _tier_record(stage, gib=CAPACITY)})
    lead = residency_plan.leads_for(live_plan)[0]
    assert seen and all(lead in keys for keys in seen), seen
