"""Pure scan fairness and ordered room protection for claim candidates."""
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
