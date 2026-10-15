"""Advisory fleet observations and deductions for one claim pass."""
from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass


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
    ) -> None:
        self._read_offers = offers
        self._read_peer = peer
        self._offers: Sequence[Mapping] | None = None
        self._peers: dict[str, PeerObservation | Exception | None] = {}

    def offers(self) -> Sequence[Mapping]:
        if self._offers is None:
            self._offers = self._read_offers()
        return self._offers

    def peer(self, host: str, offer: Mapping) -> PeerObservation | None:
        if host not in self._peers:
            try:
                self._peers[host] = self._read_peer(host, offer)
            except (OSError, ValueError, TypeError) as exc:
                self._peers[host] = exc
        peer = self._peers[host]
        if isinstance(peer, Exception):
            raise peer
        return peer
