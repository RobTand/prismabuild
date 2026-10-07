"""Regressions for the source review of PR 1593 (#1594).

Each test drives tier_loop.residency_window through real transitions, in the
style of test_prelaunch_tier_publish_1594.py.  The integrator runs them.
"""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from prismabuild import pool  # noqa: E402
import tier_loop  # noqa: E402
from test_prelaunch_group_reconcile_1594 import (  # noqa: E402
    _hexkey, _queue, MANIFEST)
from test_prelaunch_tier_module_1594 import _declared_plan, TIER  # noqa: E402
from test_prelaunch_tier_publish_1594 import (  # noqa: E402
    _declared_movers, _live, _stage)


def _published(queue, movers):
    return all(queue.item_path(pool.READY, mover).exists()
               or queue.item_path(pool.CLAIMED, mover).exists()
               for mover in movers)


def test_a_prefix_only_plan_begins_its_group_and_publishes(tmp_path) -> None:
    """One declared phase with no streaming suffix has no unpublished lead.

    Its newcomer flag must come from the declared unit, not from the lead
    mover, or no permit reaches ``prelaunch_open`` and the reservation waits
    forever (the original single-phase artifact).
    """
    queue = _queue(tmp_path, stage_gib=300)
    tiers = {TIER: _stage(queue, TIER)}
    consumer = _hexkey("prefix-only-consumer")
    specs = [("phase-a", 4, True, 2)]
    plan = _declared_plan(queue, consumer, specs, tag="prefixonly")
    _live(queue, plan, consumer, specs)
    unit, movers = _declared_movers(queue, consumer)
    seen: list[dict] = []
    for _ in range(10):
        seen.extend(tier_loop.residency_window(queue, tiers=tiers))
        if _published(queue, movers):
            break
    assert _published(queue, movers), [
        (event.get("event"), event.get("reason")) for event in seen]
    for mover in movers:
        record = queue.read_funding(mover, TIER)
        assert record is not None and record["state"] == "transferring"


def _register_shared(queue, plan) -> None:
    """Register every declared range in the real shared registry, as pbrun does."""
    from prismabuild import residency_plan
    for phase in plan["phases"]:
        for chunk in phase.get("stage_chunks") or [phase]:
            row = chunk["mover_row"]
            residency_plan.register_shared_range(
                queue, manifest_sha256=MANIFEST, tier_id=TIER,
                start=int(chunk["start_bytes"]), end=int(chunk["end_bytes"]),
                seal=lambda row=row: (dict(row), {}),
                registered_by="test",
                sealed_against={"mover_python": "/usr/bin/python3",
                                "mover_tools_root": "/tools"})


def test_the_filed_group_demand_survives_its_own_publication(tmp_path) -> None:
    """A unit's own live mover is not 'owned by others'.

    The ranges are registered in the real shared registry, as pbrun registers
    them at submission.  The first publication then made the unit's own
    movers live, the unit's retained demand dropped to zero, and the next
    cycle tried to file a different intent for the same group.  The demand
    must stay the filed one through reservation, publication and the next
    census.
    """
    from prismabuild import prelaunch_group as pg
    queue = _queue(tmp_path, stage_gib=300)
    tiers = {TIER: _stage(queue, TIER)}
    consumer = _hexkey("stable-demand-consumer")
    specs = [("phase-a", 4, True, 2), ("phase-b", 4, False, 1)]
    plan = _declared_plan(queue, consumer, specs, tag="stable")
    _register_shared(queue, plan)
    _live(queue, plan, consumer, specs)
    unit, movers = _declared_movers(queue, consumer)
    before = unit.demand_gib
    assert before > 0
    seen: list[dict] = []
    for _ in range(10):
        seen.extend(tier_loop.residency_window(queue, tiers=tiers))
        if _published(queue, movers):
            break
    assert _published(queue, movers)
    for _ in range(3):
        seen.extend(tier_loop.residency_window(queue, tiers=tiers))
    after, _movers = _declared_movers(queue, consumer)
    assert after.demand_gib == before
    standing = pg.standing_intent(queue, after.unit, TIER)
    assert standing is not None and standing["demand_gib"] == before
    assert not [event for event in seen
                if event.get("event") == "window-unknown"], seen


def test_a_row_published_before_its_funding_is_funded_on_a_later_cycle(
        tmp_path, monkeypatch) -> None:
    """A crash between a mover row and its funding must not strand the row.

    The group holds the whole tier, so an unfunded READY mover has no free
    tokens and can never claim.  Later cycles skip a row that is already
    published, so the repair has to fund every live declared leg.
    """
    import pytest
    from prismabuild import prelaunch_group as pg
    queue = _queue(tmp_path, stage_gib=8)
    tiers = {TIER: _stage(queue, TIER)}
    consumer = _hexkey("interrupted-funding-consumer")
    specs = [("phase-a", 8, True, 2)]
    plan = _declared_plan(queue, consumer, specs, tag="interrupted")
    _live(queue, plan, consumer, specs)
    unit, movers = _declared_movers(queue, consumer)
    assert unit.demand_gib == 8
    real = pg.publish_chunk
    calls: list[str] = []

    def crash(*args, **kwargs):
        calls.append("crash")
        raise OSError("crash between the row and its funding")

    monkeypatch.setattr(pg, "publish_chunk", crash)
    for _ in range(5):
        try:
            tier_loop.residency_window(queue, tiers=tiers)
        except OSError:
            pass
        if calls:
            break
    assert calls, "the window never reached the funding step"
    monkeypatch.setattr(pg, "publish_chunk", real)
    assert any(queue.item_path(pool.READY, mover).exists() for mover in movers)
    for _ in range(5):
        tier_loop.residency_window(queue, tiers=tiers)
    assert _published(queue, movers)
    for mover in movers:
        record = queue.read_funding(mover, TIER)
        assert record is not None and record["state"] == "transferring", mover


def test_a_sole_consumer_withdrawn_before_publication_returns_its_group(
        tmp_path) -> None:
    """The group holder is released even when no consumer names the tier.

    The per-tier loop visits only tiers with a live want, so the dangling
    cleanup never ran for a tier whose only consumer was withdrawn between
    its reservation and its first publication.
    """
    import prelaunch_tier as pt
    from test_prelaunch_tier_module_1594 import TIERS, _consumer
    queue = _queue(tmp_path, stage_gib=300)
    tiers = {TIER: _stage(queue, TIER)}
    consumer = _hexkey("withdrawn-sole-consumer")
    specs = [("phase-a", 4, True, 2)]
    plan = _declared_plan(queue, consumer, specs, tag="withdrawn")
    _live(queue, plan, consumer, specs)
    units = pt.declared_units(queue, TIERS, [_consumer(consumer)])
    assert len(units) == 1
    unit = units[0]
    events: list[dict] = []
    for _ in range(5):
        found, _authority = pt.reserve_pass(queue, TIER, units,
                                            admitted=lambda found: True)
        events.extend(found)
        ledger = queue.tier_ledger(TIER)
        if ledger.holder_tokens(unit.holder).get("stage_gib", 0) > 0:
            break
    assert ledger.holder_tokens(unit.holder).get("stage_gib", 0) > 0, events
    queue.withdraw(consumer, reason="test: withdrawn before publication",
                   by="test")
    seen: list[dict] = []
    for _ in range(3):
        seen.extend(tier_loop.residency_window(queue, tiers=tiers))
    ledger = queue.tier_ledger(TIER)
    assert ledger.holder_tokens(unit.holder).get("stage_gib", 0) == 0, [
        (event.get("event"), event.get("reason")) for event in seen]
    assert ledger.available().get("stage_gib") == 300


def test_prefix_cuts_follow_phase_names_when_a_declared_phase_is_empty() -> None:
    """An empty declared phase has no range, so no cut list entry.

    Slicing by the declared count took the first suffix phase's cuts into the
    prefix and made the capacity check refuse or pass the wrong peak.
    """
    import pbrun
    ranges = [{"name": "kept"}, {"name": "streamed"}]
    cuts = [[(0, 4)], [(4, 8)]]
    prefix, suffix = pbrun.split_prelaunch_cuts(
        ranges, cuts, {"empty-declared", "kept"})
    assert prefix == [[(0, 4)]]
    assert suffix == [[(4, 8)]]


import pytest  # noqa: E402


@pytest.mark.parametrize("mode", ["crash", "deferred"])
def test_an_interruption_after_the_token_transfer_is_repaired(
        tmp_path, monkeypatch, mode) -> None:
    """The tokens are the mover's before its funding record is closed.

    One mover whose demand equals the tier: the group holds nothing after
    the transfer.  A stop between the transfer and the update to
    ``transferring`` (a crash, or a deferred update with no crash) must not
    read as a short group, or the reconcile drops authority and the record
    stays ``reserved`` while the mover holds every token and cannot claim.
    """
    from prismabuild import prelaunch_group as pg
    queue = _queue(tmp_path, stage_gib=8)
    tiers = {TIER: _stage(queue, TIER)}
    consumer = _hexkey(f"xfer-{mode}")
    specs = [("phase-a", 8, True, 1)]
    plan = _declared_plan(queue, consumer, specs, tag=f"x{mode[:2]}")
    _live(queue, plan, consumer, specs)
    unit, movers = _declared_movers(queue, consumer)
    assert unit.demand_gib == 8 and len(movers) == 1
    mover = movers[0]
    real = pg._advance_record
    calls: list[str] = []

    def interrupted(*args, **kwargs):
        calls.append(mode)
        if mode == "crash":
            raise OSError("stopped after the token transfer")
        return False

    monkeypatch.setattr(pg, "_advance_record", interrupted)
    for _ in range(6):
        try:
            tier_loop.residency_window(queue, tiers=tiers)
        except OSError:
            pass
        if calls:
            break
    assert calls, "the window never reached the funding update"
    ledger = queue.tier_ledger(TIER)
    assert len(pool.held_names_visible(ledger, mover)) == 8
    assert queue.read_funding(mover, TIER)["state"] == "reserved"
    monkeypatch.setattr(pg, "_advance_record", real)
    for _ in range(5):
        tier_loop.residency_window(queue, tiers=tiers)
    record = queue.read_funding(mover, TIER)
    assert record is not None and record["state"] == "transferring"
    assert len(pool.held_names_visible(queue.tier_ledger(TIER), mover)) == 8
