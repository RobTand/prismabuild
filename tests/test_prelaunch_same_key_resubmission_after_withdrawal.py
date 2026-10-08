"""A declared consumer withdrawn after its group committed, then resubmitted under the same key (#1594).

2026-10-08, dl380g10: the PACT band source (one declared phase, 121 GiB, four
chunks) committed its prelaunch group at 13:45:38Z, then its owner withdrew it.
The group's tokens came back.  The owner resubmitted the unchanged action, which
has the same key, so it is the same unit.  The renewed row stayed READY: the
stage had 129 GiB free for a 120 GiB obligation and the commitment did not
refuse it, but the loop filed no ``prelaunch-group-begun`` and its only events
were ``ram-window-stalled`` for the RAM leg that waits on the stage group.  The
group's ``committed.json`` still stood and its holder held nothing.

Everything runs on a ``tmp_path`` queue through the real tier cycle.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from prismabuild import pool, prelaunch_group, residency_plan  # noqa: E402
import tier_loop  # noqa: E402
from test_prelaunch_group_reconcile_1594 import _hexkey, _queue  # noqa: E402
from test_prelaunch_tier_module_1594 import (  # noqa: E402
    _declared_plan, MANIFEST, TIER)
from test_prelaunch_tier_publish_1594 import (  # noqa: E402
    _declared_movers, _live, _stage)
import prelaunch_tier  # noqa: E402
from test_prelaunch_tier_gate_1594 import (  # noqa: E402
    _held, _units, _two_consumer_queue)

SPECS = [("source", 4, True, 4)]


def _group_files(queue, consumer: str) -> list[str]:
    root = queue.root / prelaunch_group.GROUP_DIR
    return sorted(path.relative_to(root).as_posix()[-40:]
                  for path in root.rglob("*")
                  if path.is_file() and consumer[:12] in path.as_posix())


def _holder_tokens(queue, holder: str) -> int:
    return int(queue.tier_ledger(TIER).holder_tokens(holder).get("stage_gib", 0))


def _cycles(queue, tiers, count: int) -> list[dict]:
    seen: list[dict] = []
    for _ in range(count):
        seen.extend(tier_loop.residency_window(queue, tiers=tiers))
    return seen


def _names(events) -> list[str]:
    return [str(event.get("event")) for event in events]


def _committed_then_withdrawn(tmp_path):
    queue = _queue(tmp_path, stage_gib=300)
    tiers = {TIER: _stage(queue, TIER)}
    consumer = _hexkey("band-source")
    plan = _declared_plan(queue, consumer, SPECS, tag="band")
    _live(queue, plan, consumer, SPECS)
    unit, movers = _declared_movers(queue, consumer)
    first = _cycles(queue, tiers, 10)
    assert _holder_tokens(queue, unit.holder) > 0 or any(
        queue.item_path(pool.READY, mover).exists() for mover in movers), _names(first)
    assert "committed.json" in " ".join(_group_files(queue, consumer)), _names(first)
    queue.withdraw(consumer, reason="test: the owner withdrew the band", by="test")
    ended = _cycles(queue, tiers, 8)
    return queue, tiers, consumer, plan, unit, movers, ended


def test_the_withdrawal_returns_the_group_tokens(tmp_path: Path) -> None:
    """The control: this much is already true on main, and the test says so."""
    queue, _tiers, _consumer, _plan, unit, _movers, ended = _committed_then_withdrawn(
        tmp_path)
    assert _holder_tokens(queue, unit.holder) == 0, _names(ended)


def test_the_same_key_resubmitted_after_a_withdrawal_begins_its_group_again(
        tmp_path: Path) -> None:
    queue, tiers, consumer, plan, unit, movers, _ended = _committed_then_withdrawn(
        tmp_path)
    before = _group_files(queue, consumer)
    _live(queue, plan, consumer, SPECS)             # the renewed publication, same key
    seen = _cycles(queue, tiers, 15)
    state = {
        "group files before the resubmission": before,
        "group files after": _group_files(queue, consumer),
        "holder tokens": _holder_tokens(queue, unit.holder),
        "mover tokens": {mover[:8]: int(queue.tier_ledger(TIER).holder_tokens(mover)
                                        .get("stage_gib", 0)) for mover in movers},
        "events": sorted(set(_names(seen))),
    }
    published = any(queue.item_path(pool.READY, mover).exists()
                    or queue.item_path(pool.CLAIMED, mover).exists()
                    for mover in movers)
    assert _holder_tokens(queue, unit.holder) > 0 or published, json.dumps(state, indent=1)


def test_a_committed_group_whose_tokens_left_without_a_release_begins_again(
        tmp_path: Path) -> None:
    """The live state: committed.json, no released.json, an empty holder.

    The mover tokens left through another path than the group's own release
    (the first three here; the last chunk keeps its one token), so the census
    reads short while the receipt still says committed.
    """
    queue = _queue(tmp_path, stage_gib=300)
    tiers = {TIER: _stage(queue, TIER)}
    consumer = _hexkey("band-source")
    plan = _declared_plan(queue, consumer, SPECS, tag="band")
    _live(queue, plan, consumer, SPECS)
    unit, movers = _declared_movers(queue, consumer)
    _cycles(queue, tiers, 10)
    ledger = queue.tier_ledger(TIER)
    for mover in movers[:-1]:
        ledger.release(mover)
    ledger.release(unit.holder)
    before = {
        "group files": _group_files(queue, consumer),
        "holder tokens": _holder_tokens(queue, unit.holder),
        "mover tokens": {m[:8]: int(ledger.holder_tokens(m).get("stage_gib", 0))
                         for m in movers},
    }
    seen = _cycles(queue, tiers, 15)
    state = {"before": before, "group files after": _group_files(queue, consumer),
             "holder tokens after": _holder_tokens(queue, unit.holder),
             "mover tokens after": {m[:8]: int(ledger.holder_tokens(m).get("stage_gib", 0))
                                    for m in movers},
             "events": sorted(set(_names(seen)))}
    found = prelaunch_group.census(queue, TIER, unit.unit, unit.holder,
                                   int(unit.demand_gib), movers)
    state["census"] = {"h": found.h, "p": found.p, "m": found.m, "r": found.r}
    accounted = prelaunch_group._accounted(found, int(unit.demand_gib))
    assert accounted[1] is True, json.dumps(state, indent=1)
    gib_of = {m: 1 for m in movers}
    gib_of.update({leg["mover_key"]: int(leg["stage_gib"]) for leg in unit.legs})
    state["uncovered movers"] = _uncovered(queue, movers, gib_of)
    assert state["uncovered movers"] == {}, json.dumps(state, indent=1)
    # The claim spends the restored fence and takes nothing more from the tier.
    ready = [m for m in movers if queue.item_path(pool.READY, m).exists()]
    free_before = ledger.available().get("stage_gib")
    got = queue.claim(tags=["dl380g10"], owner="w-mover")
    state["claimed"] = None if got is None else str(got["action_key"])[-8:]
    assert got is not None and got["action_key"] in ready, json.dumps(state, indent=1)
    assert ledger.available().get("stage_gib") == free_before, json.dumps(
        dict(state, free_before=free_before,
             free_after=ledger.available().get("stage_gib")), indent=1)
    assert int(ledger.holder_tokens(got["action_key"]).get("stage_gib", 0)) == \
        gib_of[got["action_key"]], json.dumps(state, indent=1)


def _row(queue, mover: str) -> dict:
    return json.loads(queue.item_path(pool.READY, mover).read_text(encoding="utf-8"))


def _uncovered(queue, movers, gib_of) -> dict[str, tuple]:
    """Every READY mover whose funding record does not cover its whole row."""
    out = {}
    for mover in movers:
        if not queue.item_path(pool.READY, mover).exists():
            continue
        covered = queue.funded_cover(TIER, _row(queue, mover), "stage_gib",
                                     gib_of[mover])
        if covered[0] != gib_of[mover]:
            out[mover[-8:]] = covered
    return out


def test_a_committed_unit_that_owns_nothing_still_obliges_its_peak(
        tmp_path: Path) -> None:
    """Review of f1c9694e, point 1: a receipt alone must keep the peak."""
    queue, tiers, first, _second, _pf, _ps = _two_consumer_queue(tmp_path)
    _cycles(queue, tiers, 10)
    unit = _units(queue, first)[0]
    assert "committed.json" in " ".join(_group_files(queue, first))
    ledger = queue.tier_ledger(TIER)
    for holder in list(ledger.held_keys()):          # complete token loss
        ledger.release(holder)
    assert _held(queue) == {} or not any(_held(queue).values())
    totals, detail = prelaunch_tier.obligations(_units(queue, first), _held(queue),
                                                "stage_gib")
    assert totals.get(TIER) == unit.peak_gib, json.dumps(
        {"totals": totals, "detail": detail, "peak": unit.peak_gib}, indent=1)


def _publish_ranked(queue, plan, consumer: str) -> None:
    """Freeze one plan and publish its consumer ahead of every priority-0 unit."""
    residency_plan.freeze(queue, plan)
    span = sum(int(phase["end_bytes"]) - int(phase["start_bytes"])
               for phase in plan["phases"])
    queue.publish(
        action_key=consumer, cas_root=str(queue.root / "cas"),
        checkout_root=str(queue.root / "co"),
        worker_script=str(queue.root / "worker.py"),
        resources={"cpu": 1, "mem_gb": 1}, priority=10,
        priority_reason="test: the rival outranks the unit that lost its tokens",
        residency={"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                   "manifest_sha256": MANIFEST, "manifest_bytes": span,
                   "leads": residency_plan.leads_for(plan)})


def test_a_rival_waits_while_a_committed_group_recovers_its_tokens(
        tmp_path: Path) -> None:
    """Review of f1c9694e, point 1: complete loss, a suffix, an outranking rival.

    The rival is ranked first in the reserve pass, so it meets the gate before
    the lost unit's top-up runs.  If the lost unit obliges nothing, the gate
    admits it, the top-up then takes its prefix from the rest, and the joint
    peaks exceed the tier.
    """
    queue, tiers, first, _second, _pf, _ps = _two_consumer_queue(tmp_path)
    _cycles(queue, tiers, 10)
    unit = _units(queue, first)[0]
    gib_of = {leg["mover_key"]: int(leg["stage_gib"]) for leg in unit.legs}
    ledger = queue.tier_ledger(TIER)
    for holder in [unit.holder] + list(gib_of):
        ledger.release(holder)
    rival_key = _hexkey("gate-rival")
    specs = [("p0", 40, True, 1), ("p1", 10, False, 1), ("p2", 10, False, 1)]
    _publish_ranked(queue, _declared_plan(queue, rival_key, specs, tag="rival"),
                    rival_key)
    # Small enough to clear the gate beside the lost unit's unfunded 90 GiB
    # row (new money) alone, so only the lost unit's own obligation can hold
    # it back; the two peaks together are more than the tier.
    assert _units(queue, rival_key)[0].peak_gib + 90 <= 210
    assert _units(queue, rival_key)[0].peak_gib + unit.peak_gib > 210
    seen = _cycles(queue, tiers, 20)
    rival = _units(queue, rival_key)[0]
    found = prelaunch_group.census(queue, TIER, unit.unit, unit.holder,
                                   int(unit.demand_gib), list(gib_of))
    state = {"census": {"h": found.h, "p": found.p, "m": found.m, "r": found.r},
             "rival holder": _holder_tokens(queue, rival.holder),
             "events": sorted(set(_names(seen))),
             "rival gating": [{k: v for k, v in event.items() if k != "unix"}
                              for event in seen
                              if str(event.get("consumer")) == rival_key
                              or str(event.get("unit")) == rival.unit][:3]}
    assert _holder_tokens(queue, rival.holder) == 0, json.dumps(state, indent=1)
    assert prelaunch_group._accounted(found, int(unit.demand_gib))[1] is True, \
        json.dumps(state, indent=1)
    assert _uncovered(queue, gib_of, gib_of) == {}, json.dumps(state, indent=1)
