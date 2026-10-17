"""Queue-key record observations and scratch owner commits.

The leaf JSON readers remain in pool until the wider queue-I/O caller
migration. Standing, mover polling, and claim questions own their layout and
observation policies here. These reads acquire no lock and impose no deadline.
"""

from __future__ import annotations

from collections.abc import Mapping
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


class ScratchOwnerConflict(ValueError):
    """A recovery path cannot address the active lifetime owner."""


class RecordStore:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    def claimed(self, key: str) -> ReadResult[JsonRecord]:
        """Read one claimed record without refreshing a missing name."""
        return self._plain("claimed", key)

    def queued(self, key: str) -> ReadResult[JsonRecord]:
        """Read ready only when the claimed record is missing or empty."""
        claimed = self._plain("claimed", key)
        if not isinstance(claimed, Absent):
            return claimed
        return self._plain("ready", key)

    def _plain(self, state: QueueState, key: str) -> ReadResult[JsonRecord]:
        from . import pool

        path = self.root
        try:
            path = self.root / state / f"{key}.json"
            record = pool.read_queue_record(path)
        except Exception as exc:
            return Unknown("record-unreadable", path, exc)
        return Absent() if record is None else Present(record)

    def mover(self, key: str) -> ReadResult[QueueRow]:
        """Poll claimed, ready, done, failed, then withdrawn for one key.

        Each missing or empty record gets the existing best-effort parent
        refresh and one retry. A hit needs no refresh. The first readable
        state wins, and any read exception leaves the answer unknown.
        """

        from . import pool

        path = self.root
        for state in ("claimed", "ready"):
            try:
                path = self.root / state / f"{key}.json"
                record = pool._read_json_fresh(path)
            except Exception as exc:
                return Unknown("record-unreadable", path, exc)
            if record is not None:
                return Present(QueueRow(state, record))
        for state in ("done", "failed", "withdrawn"):
            try:
                path = self.root / state / f"{key}.json"
                record = pool._read_json_fresh(path)
            except Exception as exc:
                return Unknown("record-unreadable", path, exc)
            if isinstance(record, Mapping):
                return Present(QueueRow(state, record))
        return Absent()

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

    def lease(self, key: str) -> ReadResult[JsonRecord]:
        """Read the existing lease once, without renewal or missing-name retry."""
        from . import pool

        path = self.root / "claimed" / f"{key}.lease"
        try:
            record = pool.read_queue_record(path)
        except Exception as exc:
            return Unknown("record-unreadable", path, exc)
        return Absent() if record is None else Present(record)

    def is_scratch_lease(self, path: Path, key: str) -> bool:
        return path == self.root / "claimed" / f"{key}.lease"

    def scratch_owner(self, record: Mapping[str, object], *, scope_only=False,
                      owner_path: Path | None = None) -> tuple[Path, JsonRecord | None]:
        """Locate one live, late, tombstone or widowed-lease owner without refresh.

        The lifetime owner validates the record's authority and monotone audit.
        The existing key transition lock must cover mutations that use this read.
        """
        from . import core, pool

        key = str(record["action_key"])
        claimed = self.root / "claimed"
        live_path = claimed / f"{key}.json"
        path = live_path
        if scope_only:
            identity = core.canonical_sha256({field: record.get(field) for field in pool._CLAIM_IDENTITY})
            path = claimed / f"{key}.{identity}{pool.LATE_FINISH_SUFFIX}"
        if owner_path is not None:
            if (scope_only or owner_path.parent != claimed
                    or not (self.is_scratch_lease(owner_path, key)
                            or (owner_path.name.startswith(key + ".")
                                and owner_path.name.endswith(pool.TOMBSTONE_SUFFIX)))
                    or live_path.exists()):
                raise ScratchOwnerConflict("scratch recovery owner conflicts with a live claim")
            path = owner_path
        return path, pool.read_queue_record(path)

    def write_scratch_owner(self, path: Path, record: Mapping[str, object]) -> None:
        """Publish an already validated owner through the existing atomic writer."""
        from . import pool

        pool._write_json_atomic(path, record)

    def write_scratch_lease(self, key: str, record: Mapping[str, object]) -> None:
        """Mirror audit fields only; the caller preserves heartbeat authority."""
        from . import pool

        pool._write_json_atomic(self.root / "claimed" / f"{key}.lease", record)
