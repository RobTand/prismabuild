"""Advisory fleet observations and deductions for one claim pass."""
from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
import time


class TimedPreference(Enum):
    PREFERRED_CPU = "preferred_cpu"
    CROSS_RESOURCE = "cross_resource"


class PlacementHistory:
    def __init__(self) -> None:
        self._deferrals: dict[tuple[TimedPreference, tuple[str, str]], float] = {}
        self._cpu_host_yields: dict[tuple[str, str], float] = {}

    def clear(self) -> None:
        self._deferrals.clear()
        self._cpu_host_yields.clear()

    def retain(self, generations: set[tuple[str, str]]) -> None:
        for preference, generation in list(self._deferrals):
            if generation not in generations:
                self._deferrals.pop((preference, generation), None)
        for generation in list(self._cpu_host_yields):
            if generation not in generations:
                self._cpu_host_yields.pop(generation, None)

    def discard(self, generation: tuple[str, str]) -> None:
        for preference in TimedPreference:
            self._deferrals.pop((preference, generation), None)
        self._cpu_host_yields.pop(generation, None)


@dataclass
class PeerCapacity:
    available: dict[str, int]
    observed: dict[str, int]
    free_preferred: int

    def fits_measured(self, demand: Mapping[str, int]) -> bool:
        return (self.free_preferred >= demand.get("cpu", 0)
                and all(min(self.available.get(kind, 0), self.observed.get(kind, 0)) >= need
                        for kind, need in demand.items()))

    def fits_or_ledger(self, demand: Mapping[str, int]) -> bool:
        return (self.free_preferred >= demand.get("cpu", 0)
                and all(self.available.get(kind, 0) >= need
                        and self.observed.get(kind, self.available[kind]) >= need
                        for kind, need in demand.items()))


@dataclass
class PeerObservation:
    remaining: PeerCapacity
    denials: Mapping[str, object]
    scratch_isolated: bool | None = None

    def charge(self, demand: Mapping[str, int]) -> None:
        for kind, need in demand.items():
            self.remaining.available[kind] = int(self.remaining.available.get(kind, 0)) - int(need)
            if kind in self.remaining.observed:
                self.remaining.observed[kind] = int(self.remaining.observed[kind]) - int(need)
        self.remaining.free_preferred -= int(demand.get("cpu", 0))


class PlacementPass:
    def __init__(
        self, offers: Callable[[], Sequence[Mapping]],
        peer: Callable[[str, Mapping], PeerObservation | None],
        *, history: PlacementHistory, wall_clock: Callable[[], float],
    ) -> None:
        self._read_offers = offers
        self._read_peer = peer
        self._offers: Sequence[Mapping] | None = None
        self._peers: dict[str, PeerObservation | Exception | None] = {}
        self._history = history
        self._wall_clock = wall_clock

    def timer_expired(self, preference: TimedPreference, generation: tuple[str, str]) -> bool:
        started = self._history._deferrals.setdefault((preference, generation), time.monotonic())
        return time.monotonic() - started >= 20.0

    def cpu_host_evidence(
        self, generation: tuple[str, str], host: str, capacity: PeerCapacity,
        demand: Mapping[str, int],
    ) -> dict[str, object]:
        since = self._history._cpu_host_yields.setdefault(generation, self._wall_clock())
        return {"host": host, "available": dict(capacity.available),
                "free_preferred": capacity.free_preferred,
                "observed_capacity": dict(capacity.observed),
                "demand": dict(demand), "yielding_s": round(self._wall_clock() - since, 3)}

    def offers(self) -> Sequence[Mapping]:
        if self._offers is None:
            self._offers = self._read_offers()
        return self._offers

    def peer(self, host: str, offer: Mapping) -> PeerObservation | None:
        """The first offer fixes this host's observation for the pass.

        Callers reuse that snapshot offer, including scratch's original offer.
        """
        if host not in self._peers:
            try:
                self._peers[host] = self._read_peer(host, offer)
            except (OSError, ValueError, TypeError) as exc:
                self._peers[host] = exc
        peer = self._peers[host]
        if isinstance(peer, Exception):
            raise peer
        return peer
