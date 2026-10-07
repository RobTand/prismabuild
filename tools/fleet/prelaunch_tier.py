#!/usr/bin/env python3
"""One prelaunch-resident pass per tier over live declared units (#1594).

The tier loop drives these functions once per tier per cycle. They turn
live declared consumers into reservation units, reserve one atomic group
per unit, bind published chunks to their movers, and release what no live
unit owns. Each function takes the queue, plain values or a snapshot.
No function imports the tier loop.
"""
from __future__ import annotations

import math
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping, Sequence

sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
from runtime_paths import generation_root  # noqa: E402

sys.path.insert(0, str(generation_root(__file__) / "src"))

from prismabuild import pool  # noqa: E402
from prismabuild import prelaunch_group  # noqa: E402
from prismabuild import residency_plan  # noqa: E402
from prismabuild import window_credit  # noqa: E402

__all__ = [
    "Unit",
    "declared_units",
    "reserve_pass",
    "publish_declared",
    "obligations",
    "is_admitted",
    "declared_leg_keys",
    "is_prelaunch_leg",
    "dangling",
    "release_terminal",
]

#: A gang group that names more than one tier reserves on none of them.
UNSUPPORTED_MULTI_TIER = "multi-tier"


@dataclass
class Unit:
    """One prelaunch reservation unit: a consumer or one gang group."""

    #: The representative consumer action key, the first member's.
    key: str = ""
    #: Every member consumer key, in queue read order.
    keys: list[str] = field(default_factory=list)
    #: The consumer key, or the gang group id for a merged unit.
    unit: str = ""
    #: The one stage tier this unit reserves on.
    tier_id: str = ""
    #: The deterministic group holder for this unit and tier.
    holder: str = ""
    #: The representative filed plan, the first member's.
    plan: dict = field(default_factory=dict)
    #: The declared prelaunch phase names, in plan order.
    phase_names: list[str] = field(default_factory=list)
    #: The retained prefix tokens still to reserve.
    demand_gib: int = 0
    #: The retained prefix plus the largest suffix overlap.
    peak_gib: int = 0
    #: The declared stage legs in read order, with their plan rows.
    legs: list[dict] = field(default_factory=list)
    #: Each member key with its ready or claimed state.
    consumer_state: dict = field(default_factory=dict)
    #: The representative member's priority, for the unit order.
    priority: int = 0
    #: The representative member's publish time, for the unit order.
    published_unix: float = 0.0
    #: Where this unit's group receipts live.
    receipt_dir: Path | None = None
    #: Why this unit reserves nothing, or None when it reserves.
    unsupported: str | None = None


def _member(queue, tiers, consumer) -> dict | None:
    """One consumer's declared plan and rank, or None with no claim."""
    if not isinstance(consumer, Mapping):
        return None
    key = consumer.get("action_key")
    if not isinstance(key, str) or not key:
        return None
    names = residency_plan.filed_prelaunch_phases(queue, key)
    if not names:
        return None
    plan, _ = residency_plan.read_filed(queue, key)
    if not isinstance(plan, Mapping):
        return None
    tier_id = plan.get("tier_id")
    record = tiers.get(tier_id) if isinstance(tiers, Mapping) else None
    if not isinstance(record, Mapping) or str(record.get("tier")) != "stage":
        return None
    item = consumer.get("item")
    group = None
    if isinstance(item, Mapping):
        gang = item.get("gang")
        if isinstance(gang, Mapping):
            group = gang.get("group")
    unit = group if isinstance(group, str) and group else key
    return {"key": key, "unit": unit, "tier_id": str(tier_id),
            "plan": dict(plan), "phase_names": [str(name) for name in names],
            "state": consumer.get("state"), "item": item}


def _leg_rows(phase: Mapping[str, object], mover_key: str) -> tuple:
    """One leg's mover and egress rows, chunked or whole."""
    chunks = phase.get("stage_chunks")
    if isinstance(chunks, list):
        for chunk in chunks:
            if not isinstance(chunk, Mapping):
                continue
            mover_row = chunk.get("mover_row")
            if (isinstance(mover_row, Mapping)
                    and str(mover_row.get("action_key")) == mover_key):
                return (mover_row, chunk.get("egress_row"))
    return (phase.get("mover_row"), phase.get("egress_row"))


def _declared_legs(plan: Mapping[str, object]) -> list[dict]:
    """The declared stage legs in read order, with their plan rows."""
    legs = []
    phases = plan.get("phases")
    if not isinstance(phases, list):
        return legs
    for phase in phases:
        if not isinstance(phase, Mapping):
            continue
        if phase.get("resident_before_launch") is not True:
            continue
        for leg in residency_plan._prelaunch_stage_legs(phase):
            mover_row, egress_row = _leg_rows(phase, leg["mover_key"])
            legs.append({**leg, "mover_row": mover_row,
                         "egress_row": egress_row})
    return legs


def _mover_live(queue, mover_key: str) -> bool:
    """True when one mover row stands ready or claimed now."""
    try:
        return (queue.item_path(pool.READY, mover_key).exists()
                or queue.item_path(pool.CLAIMED, mover_key).exists())
    except OSError:
        return False


def _owned_by_others(queue, mover_keys: Sequence[str]) -> frozenset:
    """Declared leg movers whose range a live shared mover already owns.

    The check reads the shared registry through
    ``share_namespace_of`` (which parses the mover index beside the
    registry) and ``read_shared_range`` (which names the registered
    mover). A leg counts owned only when the registered mover stands
    live. A leg the registry cannot answer counts unowned, so the
    runtime peak never drops below what the evidence proves. No
    registry means no sharing, so the set stays empty.
    """
    try:
        registry = (Path(queue.root) / pool.RESIDENCY_PLANS
                    / residency_plan.SHARED)
        if not registry.is_dir():
            return frozenset()
    except OSError:
        return frozenset()
    owned: set[str] = set()
    for mover_key in set(mover_keys):
        try:
            namespace = residency_plan.share_namespace_of(queue, mover_key)
        except (OSError, ValueError, pool.PoolContractError,
                residency_plan.ResidencyPlanError):
            continue
        if namespace is None:
            continue
        try:
            record = residency_plan.read_shared_range(queue, namespace)
        except (OSError, ValueError, pool.PoolContractError,
                residency_plan.ResidencyPlanError):
            continue
        if not isinstance(record, Mapping):
            continue
        shared = record.get("mover_action_key")
        if (isinstance(shared, str) and shared
                and _mover_live(queue, shared)):
            owned.add(mover_key)
    return frozenset(owned)


def _union(entries: Sequence[dict], owned: frozenset,
           ) -> tuple[list[dict], int, int]:
    """Merged legs and peak over members; shared ranges count once.

    The merge follows ``gang_prelaunch_demand``: legs deduplicate by
    share namespace, and the suffix step sums one adjacent-pair peak
    per member. Legs a live shared mover owns never count, so the
    runtime peak stays under the submission peak.
    """
    seen: set[str] = set()
    legs: list[dict] = []
    retained = 0
    step = 0
    for entry in entries:
        plan = entry["plan"]
        member_tier = entry["tier_id"]
        manifest = str(plan.get("manifest_sha256") or "")
        kept: list[dict] = []
        for leg in _declared_legs(plan):
            if leg["mover_key"] in owned:
                continue
            namespace = residency_plan.share_namespace(
                manifest, member_tier, int(leg["start_bytes"]),
                int(leg["end_bytes"]))
            if namespace in seen:
                continue
            seen.add(namespace)
            kept.append(leg)
            legs.append(leg)
        retained += sum(int(leg["stage_gib"]) for leg in kept)
        suffix_sizes: list[int] = []
        phases = plan.get("phases")
        if isinstance(phases, list):
            for phase in phases:
                if (not isinstance(phase, Mapping)
                        or phase.get("resident_before_launch") is True):
                    continue
                size = 0
                for leg in residency_plan._prelaunch_stage_legs(phase):
                    if leg["mover_key"] in owned:
                        continue
                    namespace = residency_plan.share_namespace(
                        manifest, member_tier, int(leg["start_bytes"]),
                        int(leg["end_bytes"]))
                    if namespace in seen:
                        continue
                    seen.add(namespace)
                    size += int(leg["stage_gib"])
                suffix_sizes.append(size)
        step += residency_plan.prelaunch_peak_gib([], suffix_sizes)[
            "suffix_gib"]
    return (legs, retained, retained + step)


def _rank(item: object) -> tuple[int, float]:
    """One member's priority and publish time, defaulted, finite."""
    priority = 0
    published = 0.0
    if isinstance(item, Mapping):
        try:
            priority = int(item.get("priority", 0))  # type: ignore[arg-type]
        except (TypeError, ValueError):
            priority = 0
        try:
            published = float(item.get("published_unix", 0.0))  # type: ignore[arg-type]
        except (TypeError, ValueError):
            published = 0.0
    if not math.isfinite(published):
        published = 0.0
    return (priority, published)


def _merge(queue, unit_id: str, entries: Sequence[dict],
           owned: frozenset) -> Unit:
    """One unit from its member consumers, merged on a gang group."""
    first = entries[0]
    tier_id = first["tier_id"]
    unsupported = (None if all(entry["tier_id"] == tier_id for entry in entries)
                   else UNSUPPORTED_MULTI_TIER)
    phase_names = list(first["phase_names"])
    holder = prelaunch_group.holder_name(unit_id, tier_id, phase_names)
    if len(entries) == 1 and unit_id == first["key"]:
        legs = _declared_legs(first["plan"])
        bound = residency_plan.prelaunch_bound(first["plan"], owned)
        assert bound is not None
        demand, peak = bound["retained_gib"], bound["peak_gib"]
    else:
        legs, demand, peak = _union(entries, owned)
    priority, published = _rank(first["item"])
    return Unit(
        key=first["key"], keys=[entry["key"] for entry in entries],
        unit=unit_id, tier_id=tier_id, holder=holder, plan=first["plan"],
        phase_names=phase_names, demand_gib=int(demand),
        peak_gib=int(peak), legs=legs,
        consumer_state={entry["key"]: entry["state"] for entry in entries},
        priority=priority, published_unix=published,
        receipt_dir=prelaunch_group.group_dir(queue, unit_id, tier_id),
        unsupported=unsupported)


def declared_units(queue, tiers, consumers) -> list[Unit]:
    """The live declared consumers as reservation units, best-ranked first.

    A consumer joins when its filed plan declares a prefix and names a
    stage tier in ``tiers``. Members of one gang group merge into one
    unit with the union of their legs. A group across tiers keeps one
    tier and returns ``unsupported`` as ``multi-tier``. Units sort by
    priority, then publish time, then unit id.
    """
    members: dict[str, list[dict]] = {}
    for consumer in consumers or []:
        entry = _member(queue, tiers, consumer)
        if entry is None:
            continue
        members.setdefault(entry["unit"], []).append(entry)
    if not members:
        return []
    owned = _owned_by_others(
        queue, [leg["mover_key"] for entries in members.values()
                for entry in entries
                for leg in _declared_legs(entry["plan"])])
    units = [_merge(queue, unit_id, entries, owned)
             for unit_id, entries in members.items()]
    units.sort(key=lambda found: (-found.priority, found.published_unix,
                                  found.unit))
    return units


def _intent_chunks(unit: Unit) -> list[dict]:
    """The write-ahead chunk entries for one unit's declared legs."""
    digest = residency_plan.plan_sha256(unit.plan)
    consumer = str(unit.plan.get("consumer_action_key"))
    return [{"mover_action_key": leg["mover_key"],
             "start_bytes": int(leg["start_bytes"]),
             "end_bytes": int(leg["end_bytes"]),
             "stage_gib": int(leg["stage_gib"]),
             "plan_sha256": digest, "consumer_action_key": consumer}
            for leg in unit.legs]


def _unit_event(unit: Unit, tier_id: str, name: str, **fields) -> dict:
    """One tier-loop event naming the unit it belongs to."""
    event = {"event": name, "unit": unit.unit, "consumer": unit.key,
             "tier_id": tier_id}
    event.update(fields)
    return event


def reserve_pass(queue, tier_id: str, units: Sequence[Unit], *, admitted,
                 writer_is_me: bool = True) -> tuple[list[dict], dict]:
    """File each unit's intent, then reconcile the admitted ones.

    ``admitted`` is the caller's gate: it takes a unit and answers true
    when the unit may reserve now. A unit the gate refuses still files
    its intent, then waits with no ledger change unless it already
    holds tokens or receipts. Units on another tier and unsupported
    units stay untouched. Returns the events with the authority map.
    """
    events: list[dict] = []
    authority: dict[str, bool] = {}
    for unit in units or []:
        if unit.tier_id != tier_id or unit.unsupported is not None:
            continue
        movers = [leg["mover_key"] for leg in unit.legs]
        prelaunch_group.file_intent(queue, unit.unit, unit.holder, tier_id,
                                    unit.demand_gib, _intent_chunks(unit))
        if not admitted(unit) and not prelaunch_group.has_holdings(
                queue, unit.unit):
            events.append(_unit_event(unit, tier_id, "prelaunch-waiting"))
            authority[unit.unit] = False
            continue
        outcome = prelaunch_group.reconcile(
            queue, tier_id, unit.unit, unit.holder, unit.demand_gib, movers,
            writer_is_me=writer_is_me)
        for name in outcome.events:
            events.append(_unit_event(unit, tier_id, name,
                                      state=outcome.state))
        authority[unit.unit] = bool(outcome.authority)
    return (events, authority)


def _live_mover_row(queue, mover_key: str) -> tuple[dict | None, bool]:
    """One published mover row, preferring ready over claimed.

    Returns the row with False, or (None, True) when the row stands
    but will not read. A missing row is (None, False): the window has
    not published this leg yet, and a later pass binds it.
    """
    for state in (pool.READY, pool.CLAIMED):
        path = queue.item_path(state, mover_key)
        try:
            present = path.exists()
        except OSError:
            return (None, True)
        if not present:
            continue
        try:
            row = pool.read_queue_record(path)
        except (OSError, ValueError, pool.PoolContractError):
            return (None, True)
        if row is None:
            continue
        return (row, False)
    return (None, False)


def publish_declared(queue, tier_id: str, unit: Unit,
                     publish_rows: Sequence[Mapping[str, object]]) -> list[dict]:
    """Bind the group fence behind the rows the window just published.

    ``publish_rows`` names the window rows to fund this pass. Each
    named leg whose mover row stands calls ``publish_chunk`` with the
    row's own publish time. A leg with no row yet skips with no error.
    Returns one event per chunk handoff.
    """
    wanted: set[str] = set()
    for row in publish_rows or []:
        if isinstance(row, Mapping):
            mover = row.get("mover_action_key")
            if isinstance(mover, str) and mover:
                wanted.add(mover)
    events: list[dict] = []
    for leg in unit.legs:
        mover = leg["mover_key"]
        if mover not in wanted:
            continue
        row, unreadable = _live_mover_row(queue, mover)
        if unreadable:
            events.append(_unit_event(unit, tier_id,
                                      "prelaunch-unknown-evidence",
                                      mover=mover))
            continue
        if row is None:
            continue
        try:
            published = float(row.get("published_unix"))  # type: ignore[arg-type]
        except (TypeError, ValueError):
            events.append(_unit_event(unit, tier_id,
                                      "prelaunch-unknown-evidence",
                                      mover=mover))
            continue
        funding = {"mover_action_key": mover,
                   "start_bytes": int(leg["start_bytes"]),
                   "end_bytes": int(leg["end_bytes"]),
                   "stage_gib": int(leg["stage_gib"])}
        outcome = prelaunch_group.publish_chunk(
            queue, tier_id, unit.unit, unit.holder, unit.plan, funding,
            published)
        for name in outcome.events:
            events.append(_unit_event(unit, tier_id, name, mover=mover,
                                      status=outcome.status,
                                      moved=outcome.moved))
    return events


def _held_count(held: Mapping[str, Mapping[str, int]], holder: str,
                kind: str) -> int:
    """Tokens one holder owns of one kind, or zero when unknown."""
    per_kind = held.get(holder) if isinstance(held, Mapping) else None
    if not isinstance(per_kind, Mapping):
        return 0
    try:
        return max(0, int(per_kind.get(kind, 0)))  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0


def obligations(units: Sequence[Unit], held: Mapping[str, Mapping[str, int]],
                kind: str) -> tuple[dict[str, int], dict[str, dict]]:
    """The admitted declared windows' peak obligations per tier.

    A unit obliges when its holder or one of its leg movers owns
    tokens now: its peak minus all it owns, never below zero. Units
    that own nothing oblige nothing yet. Returns the per-tier sums
    with one detail record per obliging unit for the logs.
    """
    totals: dict[str, int] = {}
    detail: dict[str, dict] = {}
    for unit in units or []:
        if unit.unsupported is not None:
            continue
        owns_holder = _held_count(held, unit.holder, kind) > 0
        owns_chunk = any(_held_count(held, leg["mover_key"], kind) > 0
                         for leg in unit.legs)
        if not owns_holder and not owns_chunk:
            continue
        owned = residency_plan.prelaunch_owned_gib(unit.plan, held,
                                                   unit.holder, kind)
        due = window_credit.prelaunch_obligation_gib(unit.peak_gib, owned)
        totals[unit.tier_id] = totals.get(unit.tier_id, 0) + due
        detail[unit.unit] = {"unit": unit.unit, "consumer": unit.key,
                             "tier_id": unit.tier_id, "holder": unit.holder,
                             "peak_gib": unit.peak_gib, "owned_gib": owned,
                             "obligation_gib": due}
    return (totals, detail)
