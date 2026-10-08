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

from prismabuild import pool, prelaunch_group  # noqa: E402
import tier_loop  # noqa: E402
from test_prelaunch_group_reconcile_1594 import _hexkey, _queue  # noqa: E402
from test_prelaunch_tier_module_1594 import _declared_plan, TIER  # noqa: E402
from test_prelaunch_tier_publish_1594 import (  # noqa: E402
    _declared_movers, _live, _stage)

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
