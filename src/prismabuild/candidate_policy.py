"""Pure candidate refusals, scan fairness and ordered room protection."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from types import MappingProxyType


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
