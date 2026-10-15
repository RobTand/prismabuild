"""Read-only questions about records filed under a queue key.

The leaf JSON reader remains pool.read_queue_record until the wider queue-I/O
caller migration. Key standing owns its layout, ordering, and consecutive
observations here. These reads acquire no lock and impose no deadline.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal


type QueueState = Literal["claimed", "ready", "done", "failed", "withdrawn"]
type JsonRecord = dict[str, object]


@dataclass(frozen=True)
class Present[T]:
    value: T


@dataclass(frozen=True)
class Absent:
    pass


@dataclass(frozen=True)
class Unknown:
    reason: str
    path: Path | None = None
    error: Exception | None = field(default=None, compare=False, repr=False)


type ReadResult[T] = Present[T] | Absent | Unknown


@dataclass(frozen=True)
class QueueRow:
    state: QueueState
    record: JsonRecord


@dataclass(frozen=True)
class Moving:
    pass


type KeyStanding = ReadResult[QueueRow | Moving]


class RecordStore:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    def standing(self, key: str) -> KeyStanding:
        """Read a key twice; disagreement leaves its standing unknown.

        Each observation visits claimed, ready, transition sidecars, and
        terminals in that order. It does not refresh a cached missing name.
        Equal observations compare both the queue state and record contents.
        A tombstone or late-finish sidecar means moving; an intent or lease
        alone does not. Otherwise the newest published terminal wins, and
        equal newest publication stamps leave the answer unknown.
        """

        first = self._standing_once(key)
        second = self._standing_once(key)
        if isinstance(first, Unknown) and isinstance(second, Unknown):
            return first
        if first != second:
            return Unknown("observation-changed")
        return first

    def _standing_once(self, key: str) -> KeyStanding:
        from . import pool

        path = self.root
        try:
            for state in ("claimed", "ready"):
                path = self.root / state / f"{key}.json"
                record = pool.read_queue_record(path)
                if record is not None:
                    return Present(QueueRow(state, record))
            path = self.root / "claimed"
            try:
                beside = [entry.name for entry in pool._glob_visible(
                    path, f"{key}.*")]
            except FileNotFoundError:
                beside = []
            if any(name.endswith((pool.TOMBSTONE_SUFFIX,
                                  pool.LATE_FINISH_SUFFIX))
                   for name in beside):
                return Present(Moving())
            terminals: list[tuple[float, QueueState, JsonRecord]] = []
            for state in ("done", "failed", "withdrawn"):
                path = self.root / state / f"{key}.json"
                record = pool.read_queue_record(path)
                if record is None:
                    continue
                stamp = record.get("published_unix")
                if isinstance(stamp, bool) or not isinstance(stamp, (int, float)):
                    return Unknown("generation-missing-or-nonnumeric", path)
                terminals.append((float(stamp), state, record))
        except (OSError, pool.PoolContractError) as exc:
            return Unknown("record-unreadable", path, exc)
        if not terminals:
            return Absent()
        terminals.sort(key=lambda item: item[0])
        if len(terminals) > 1 and terminals[-1][0] == terminals[-2][0]:
            return Unknown("latest-generation-tied")
        _stamp, state, record = terminals[-1]
        return Present(QueueRow(state, record))
