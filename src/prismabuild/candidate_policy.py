"""Pure candidate refusals, scan fairness and ordered room protection."""
from __future__ import annotations

from collections.abc import Container, Mapping, Sequence
from dataclasses import dataclass, replace
from enum import Enum
from types import MappingProxyType
from typing import cast


#: Residency states the claim pass refuses a row on before any token is taken
#: (``residency_<state>``): its bytes are not there, or cannot be reached.  A
#: ready GPU row in one of them keeps no room on a free GPU (#1169).
#: ``ram_epoch_stale`` belongs here with ``map_not_composed``: the map names
#: ram paths from the epoch before a reboot, so the bytes are gone and the
#: tier loop's next recomposition is what makes the row admissible (#640).
#: ``prelaunch_undeclared`` belongs here as a shape refusal: the
#: sealed manifest declares a prelaunch prefix the filed plan does not carry,
#: so no token moves until the plan matches the manifest (#1594).
RESIDENCY_REFUSAL_STATES = ("lead_not_resident", "lead_unpinned", "map_not_composed",
                            "map_stale", "map_unreadable", "plan_unreadable",
                            "ram_epoch_stale", "prelaunch_undeclared",
                            "map_incomplete")

#: Pending-lead statuses (``residency_verdict``'s ``pending``) that a later
#: poll can still see land: a lead with no ending yet (``absent``), and a lead
#: queued or running again under a later generation than the ending on file
#: (``ready``, ``claimed``, #1186).  Every other status is an ending, and a
#: denial whose pending leads all ended is ``residency_lead_terminal`` (#595).
RESIDENCY_LEAD_UNFINISHED = ("absent", "ready", "claimed")


def residency_denial(verdict: Mapping[str, object]) -> str | None:
    if verdict["state"] in RESIDENCY_REFUSAL_STATES:
        reason = f"residency_{verdict['state']}"
        if verdict["state"] in ("lead_not_resident", "lead_unpinned"):
            pending = verdict.get("pending")
            if (isinstance(pending, list) and pending
                    and all(isinstance(entry, Mapping)
                            and entry.get("status") is not None
                            and entry.get("status")
                            not in RESIDENCY_LEAD_UNFINISHED
                            for entry in pending)):
                reason = "residency_lead_terminal"
        return reason
    return None


def admission_denial(host_demand: Mapping[str, int], *, capacity_present: bool) -> str | None:
    if not capacity_present:
        return "admission_capacity_required"
    if not host_demand:
        return "admission_demand_required"
    return None


def never_fits(reservation_demand: Mapping[str, int], total: Mapping[str, int]) -> bool:
    return any(total.get(kind, 0) < need for kind, need in reservation_demand.items())


def funded_remainder(
    host_demand: Mapping[str, int], adaptive: Mapping[str, object],
) -> dict[str, int]:
    funded = adaptive["funded"]
    covered = {"cpu": len(funded["cpus"]),
               "mem_gb": int(funded["mem_gb"])}
    return {kind: int(need) - covered.get(kind, 0)
              for kind, need in host_demand.items()
              if int(need) - covered.get(kind, 0) > 0}


WITHHOLD_KINDS: dict[str, frozenset[str]] = {"gpu": frozenset({"gpu"})}


@dataclass(frozen=True)
class KeptRoom:
    action_key: str
    room: Mapping[str, int]
    reason: str | None
    binds_all: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "room", MappingProxyType(dict(self.room)))


@dataclass(frozen=True)
class ScanOrder:
    withheld_for: str | None = None
    withheld_kinds: frozenset[str] | None = None
    measurement_withholds: frozenset[str] = frozenset()
    carried_measurement_withholds: frozenset[str] = frozenset()
    rooms: tuple[KeptRoom, ...] = ()

    @property
    def whole_box_held(self) -> bool:
        return self.withheld_for is not None and self.withheld_kinds is None

    def row_held_back(
        self, *, takes_kind: bool, reserving_priority: int | None = None,
        withheld_priority: int = 0,
    ) -> bool:
        held_back = self.withheld_for is not None and (
            self.withheld_kinds is None or takes_kind)
        if reserving_priority is not None and withheld_priority <= reserving_priority:
            held_back = False
        return held_back

    def busy_room_needed(
        self, key: str, *, carried: Mapping[str, object] | None,
        gpu_first: Container[str], has_ledger: bool,
    ) -> bool:
        return has_ledger and (
            key in gpu_first or (carried is None and not self.whole_box_held))

    def withhold(
        self, key: str, kinds: frozenset[str] | None, *, measurement: bool = False,
    ) -> ScanOrder:
        owner, scope = self.withheld_for, self.withheld_kinds
        if not self.whole_box_held:
            if owner is None or kinds is None:
                owner, scope = key, kinds
            else:
                scope = scope | kinds
        return replace(
            self, withheld_for=owner, withheld_kinds=scope,
            measurement_withholds=(self.measurement_withholds | {key}
                                   if measurement else self.measurement_withholds))

    def carry(self, key: str, episode: Mapping[str, object] | None) -> ScanOrder:
        if episode is None:
            return self
        order = self.withhold(key, WITHHOLD_KINDS.get(str(episode["mode"])))
        if "drain_until_unix" in episode:
            order = replace(order, carried_measurement_withholds=(
                order.carried_measurement_withholds | {key}))
        return order

    def keep(self, room: KeptRoom) -> ScanOrder:
        return replace(self, rooms=(*self.rooms, room))

    def keep_refused_room(
        self, key: str, reservation: Mapping[str, object], reason: str,
        free: Mapping[str, int] | None,
    ) -> tuple[ScanOrder, dict[str, object]]:
        if free is None:
            return self, {}
        try:
            room = {str(kind): int(need) for kind, need in reservation.items()}
        except (TypeError, ValueError):
            return self, {}
        if not room.get("gpu"):
            return self, {}
        if room_fits(free, room):
            return self, {"gpu_room_fit_at_refusal": True}
        return self.keep(KeptRoom(key, room, reason, binds_all=True)), {
            "gpu_room_kept": dict(room), "gpu_room_binds_all": True}


def room_fits(available: Mapping[str, int], room: Mapping[str, object]) -> bool:
    return all(available.get(kind, 0) >= int(need) for kind, need in room.items())


def room_taken(
    available: Mapping[str, int], rooms: Sequence[KeptRoom], demand: Mapping[str, int],
) -> dict[str, object] | None:
    for kept in rooms:
        room = kept.room
        if not room_fits(available, room):
            continue
        if all(available.get(kind, 0) - int(demand.get(kind, 0)) >= int(need)
               for kind, need in room.items()):
            continue
        return {"gpu_row": str(kept.action_key)[:12], "room": dict(room),
                "kept_for": kept.reason, "available": available, "demand": dict(demand)}
    return None


def starved_suffix(verdict: Mapping[str, object]) -> str:
    """The denial suffix of an eligible item that is not withholding (#924).

    ``_past_ceiling`` keeps its pre-#924 meaning -- the item's own clock ran
    out -- and now also covers a veto that expired while work ahead of it kept
    refilling the box.  ``_starved`` is every holder or load that draining
    will not clear soon.  ``pbstatus --starvation`` lists both.
    """

    return ("_past_ceiling" if verdict.get("why") in ("unknown_past_ceiling",
                                                     "refilled_past_ceiling")
            else "_starved")


@dataclass(frozen=True)
class WithholdAfterDenial:
    key: str
    kinds: frozenset[str] | None

    def apply(
        self, order: ScanOrder, *,
        identity: Sequence[object] | Mapping[object, object] | None,
        measurement_suspended: bool,
    ) -> ScanOrder:
        if not (measurement_suspended and identity and identity[1]):
            return order.withhold(
                self.key, self.kinds, measurement=bool(identity and identity[1]))
        return order


@dataclass(frozen=True)
class CandidateDenial:
    reason: str
    evidence: Mapping[str, object] | None
    order_before: ScanOrder
    withhold_after: WithholdAfterDenial | None = None


@dataclass(frozen=True)
class CountAdaptive:
    reason: str
    evidence: dict[str, object]
    decision: Mapping[str, object] | None
    verdict: Mapping[str, object] | None


@dataclass(frozen=True)
class Uncounted:
    denial: CandidateDenial


@dataclass(frozen=True)
class Silent:
    pass


def prepare_adaptive_refusal(
    source: str | None, decision: Mapping[str, object] | None,
    verdict: Mapping[str, object] | None, *, order: ScanOrder,
    dependent_owner: object, room_taken: Mapping[str, object] | None,
) -> CountAdaptive | Uncounted | Silent:
    if (verdict is not None and verdict["withhold"]
            and isinstance(decision, Mapping)
            and decision.get("reason") == "measurement_holder"
            and decision.get("isolated_by")):
        # The measurement admits only its own dependents. Holding those back
        # would stop the holder from making the progress its drain waits on.
        verdict = dict(verdict, withhold=False,
                       why="measurement_admits_only_its_dependents")
    reason = source or "adaptive_refused"
    evidence: dict[str, object] = {"decision": decision or {}}
    if source == "deferred_behind_withholding":
        if isinstance(dependent_owner, str):
            evidence["withheld_for"] = order.withheld_for
            if order.withheld_kinds is not None:
                evidence["withheld_kinds"] = sorted(order.withheld_kinds)
            return Uncounted(CandidateDenial(reason, evidence, order))
        return Silent()
    if source == "deferred_for_ready_gpu_row":
        evidence.update(room_taken or {})
        return Uncounted(CandidateDenial(reason, evidence, order))
    return CountAdaptive(reason, evidence, decision, verdict)


def counted_adaptive_refusal(
    prefix: CountAdaptive, *, key: str, order: ScanOrder,
    demand: Mapping[str, int], reservation_demand: Mapping[str, object],
    mode: str | None, foreign: bool, free_at_refusal: Mapping[str, int] | None,
) -> CandidateDenial:
    reason, evidence = prefix.reason, dict(prefix.evidence)
    decision, verdict = prefix.decision, prefix.verdict
    drain_resolves = (mode is not None and not foreign
                      and (verdict is None
                           or verdict.get("drain_resolves") is not False))
    if (demand.get("gpu") and isinstance(decision, Mapping)
            and isinstance(decision.get("reason"), str) and decision["reason"]):
        evidence["drain_resolves"] = drain_resolves
    if verdict is not None and verdict["eligible"]:
        evidence["withhold"] = verdict
        if verdict["withhold"]:
            kinds = WITHHOLD_KINDS.get(str(verdict["mode"]))
            if kinds is not None:
                evidence["withheld_kinds"] = sorted(kinds)
            reason = f"{reason}_withholding"
            if drain_resolves:
                order, room_evidence = order.keep_refused_room(
                    key, reservation_demand, reason, free_at_refusal)
                evidence.update(room_evidence)
            return CandidateDenial(reason, evidence, order, WithholdAfterDenial(key, kinds))
        reason = f"{reason}{starved_suffix(verdict)}"
        evidence["starved"] = {"why": verdict["why"], "holders": verdict.get("holders")}
    if drain_resolves:
        order, room_evidence = order.keep_refused_room(
            key, reservation_demand, reason, free_at_refusal)
        evidence.update(room_evidence)
    return CandidateDenial(reason, evidence, order)


def counted_token_refusal(
    evidence: Mapping[str, object], *, key: str, order: ScanOrder, eligible: bool,
    asked: Mapping[str, object], free_at_refusal: Mapping[str, int] | None,
) -> CandidateDenial:
    evidence = dict(evidence)
    verdict = evidence["withhold"]
    if eligible and verdict["withhold"]:
        order, room_evidence = order.keep_refused_room(
            key, asked, "reservation_unavailable_withholding", free_at_refusal)
        evidence.update(room_evidence)
        return CandidateDenial("reservation_unavailable_withholding", evidence, order,
                               WithholdAfterDenial(key, None))
    if eligible:
        evidence["starved"] = {"why": verdict["why"], "holders": verdict.get("holders")}
    reason = "reservation_unavailable" + (starved_suffix(verdict) if eligible else "")
    order, room_evidence = order.keep_refused_room(key, asked, reason, free_at_refusal)
    evidence.update(room_evidence)
    return CandidateDenial(reason, evidence, order)


@dataclass(frozen=True)
class RowOpening:
    held_back: bool
    producer: bool


class RowProbe(Enum):
    GPU_SCOPE = "gpu_scope"


def row_before_placement(
    order: ScanOrder, *, held_back: bool, producer: bool,
) -> CandidateDenial | RowOpening:
    if held_back and order.withheld_kinds is None and not producer:
        return CandidateDenial("deferred_behind_withheld_row", {
            "withheld_for": order.withheld_for, "withheld_kinds": None}, order)
    return RowOpening(held_back, producer)


def _row_after_scope(order: ScanOrder, opening: RowOpening) -> CandidateDenial | RowOpening:
    if opening.held_back and not opening.producer:
        return CandidateDenial("deferred_behind_withheld_row", {
            "withheld_for": order.withheld_for,
            "withheld_kinds": sorted(order.withheld_kinds or ())}, order)
    return opening


def row_after_placement(
    item: Mapping[str, object], opening: RowOpening, *, order: ScanOrder,
    key: str, released: Container[str], matches: bool | None,
    worker_tags: frozenset[str], has_gpu: bool,
    ready_gpu: Mapping[str, object] | None, host: str,
) -> CandidateDenial | RowProbe | RowOpening:
    if key and key not in released and not matches:
        return CandidateDenial("placement_mismatch", {
            "worker_tags": sorted(worker_tags), "worker_has_gpu": has_gpu,
            "required_tags": item.get("tags"), "needs_gpu": item.get("needs_gpu")}, order)
    if (ready_gpu is not None and key not in released
            and host not in (item.get("tags") or []) and not item.get("needs_gpu")):
        return RowProbe.GPU_SCOPE
    return _row_after_scope(order, opening)


def row_after_gpu_scope(
    opening: RowOpening, *, order: ScanOrder, takes_gpu: bool,
    ready_gpu: Mapping[str, object], total: Mapping[str, int],
) -> CandidateDenial | RowOpening:
    if not takes_gpu:
        return CandidateDenial("deferred_for_ready_gpu", {
            "gpu_row": ready_gpu.get("action_key"),
            "gpu_published_unix": ready_gpu.get("published_unix"),
            "capacity_total": total}, order)
    return _row_after_scope(order, opening)


def busy_denial(
    order: ScanOrder, key: str, *, carried: Mapping[str, object] | None,
    room: Mapping[str, object] | None,
) -> CandidateDenial:
    if room is not None:
        kept = KeptRoom(
            cast(str, room["action_key"]), cast(Mapping[str, int], room["room"]),
            "transition_busy", binds_all=carried is None and not order.whole_box_held)
        order = order.keep(kept)
    evidence: dict[str, object] = {}
    if carried is not None:
        evidence["withhold_carried"] = carried
    if room is not None:
        evidence["gpu_room_kept"] = room["room"]
        if kept.binds_all:
            evidence["gpu_room_binds_all"] = True
    return CandidateDenial("transition_busy", evidence or None, order)
