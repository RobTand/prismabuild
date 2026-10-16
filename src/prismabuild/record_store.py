"""Read-only questions about records filed under a queue key.

Queue names, JSON intake and named observations live here. Domain callers
retain scheduling, lifecycle and retention authority. Reads take no lock
and impose no deadline.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
import errno
import hashlib
import json
import math
import os
from pathlib import Path

from . import digest_primitives, immutable_files
from typing import Literal


POOL_ATTEMPT_SCHEMA_V1 = "prismaquant.prismabuild.pool_attempt.v1"


READY = "ready"


CLAIMED = "claimed"


DONE = "done"


FAILED = "failed"


INTENT = "intent"


WITHDRAWN = "withdrawn"


_STATES = (READY, CLAIMED, DONE, FAILED, INTENT, WITHDRAWN)


TOMBSTONE_SUFFIX = ".tombstone"


LATE_FINISH_SUFFIX = ".late-finish"


ATTEMPTS = "attempts"


TIER_RESERVATIONS = "tier-reservations"


TIER_FUNDING = "tier-funding"


TIER_FUNDING_RETIRED = "retired"


TIERS = "tiers"


TIER_COMMITMENTS = "tier-commitments"


PASSES = "passes"


DENIAL_TRANSITIONS = "denial-transitions"


RESIDENCY_EVENTS = "residency-events"


RESIDENCY_EVENTS_HOST_DIR = "_host"


WORKERS = "workers"


PREWARM = "prewarm"


MOVERS = "movers"


MOVERS_RETIRED = "movers-retired"


MOVERS_RETIREMENT_CHECKPOINT = "checkpoint.json"


MOVERS_RETIREMENT_STATE = "state.json"


MOVERS_ARCHIVE = "movers-archive"


MOVERS_PRICING = "movers-pricing"


MOVERS_PRICING_LOG = "receipts.jsonl"


EXPORTS = "exports"


STAGE_OWNERSHIP_HOLDERS = "stage-ownership-holders"


RESIDENCY = "residency"


RESIDENCY_PLANS = "residency-plans"


RELEASED_ORIGIN_CONSUMERS = "released-origin-consumers"


MOVER_LANDING_REPORT = "landing.json"


_CLAIM_IDENTITY = ("claimed_by", "claimed_unix", "published_unix", "attempts")


def residency_action_key(value: object) -> str:
    """One action key, checked, because these two names index shared files.

    The residency map and the frozen plan are named after the consumer; a
    caller that passed a path fragment instead of a key would name a file
    somewhere else under the shared mount.
    """

    if (not isinstance(value, str) or len(value) != 64
            or any(character not in "0123456789abcdef" for character in value)):
        raise PoolContractError("a residency file is named by a 64-character action key")
    return value


class PoolError(digest_primitives.PrismaBuildError):
    """A queue-level failure, distinct from an action-level one."""


class PoolContractError(PoolError, ValueError):
    """A queue record does not satisfy its schema."""


class StaleAbsenceError(FileNotFoundError):
    """A linked attempt file its directory lists that this client cannot open yet.

    ``PoolQueue.attempt_outcomes`` raises it only after a listing of every
    directory on the canonical path showed each name, so the file exists on
    the server and this client still answers ``ENOENT`` for it from a lookup
    it cached before the file was created (#1100).  A waiter treats it as a
    read to repeat later, not as a verdict.  It is a ``FileNotFoundError``,
    so a caller that does not know it keeps its old handling.
    """


def stage_ownership_name(stage_root) -> str:
    """The name one stage root's ownership lock and holder record share."""

    root = str(Path(stage_root).absolute())
    return hashlib.sha256(f"stage-ownership:{root}".encode()).hexdigest()


def _read_json(
    path: Path, *, tolerate_stale: bool = False
) -> dict[str, object] | None:
    """The record at ``path``, or ``None`` if it is not there to read.

    ``tolerate_stale`` extends "not there" to cover ``ESTALE``, and belongs
    only to a caller enumerating a directory whose entries come and go.  A
    worker offer is exactly that: it appears when a loop starts and is gone
    when the box leaves, so a file that vanishes between the ``glob`` and the
    read is ordinary.  ``ENOENT`` has always been read that way here; on NFS
    the same event arrives as ``ESTALE`` through a directory handle the client
    had already cached, and untreated it left this function, ``offers()`` and
    ``placeable_hosts()`` and killed a submission before it queued anything --
    a whole ``pbtest`` shard, 74 tests never run, for two offer files an
    operator had tidied away (#208).

    It is opt-in because ``ESTALE`` is not only "this file vanished": it is
    also what a dead mount or a stale parent handle returns.  An enumerator
    has already had its ``glob`` succeed, so it holds the evidence that the
    directory is live and the entry is not.  A caller addressing one record by
    key holds no such evidence -- ``reclaim`` asserting exactly one terminal
    record, a lease read, a pass record -- and answering "absent" there would
    turn a broken mount into a confident wrong verdict.  Those callers keep
    the default and stay loud.
    """

    try:
        raw = path.read_bytes()
    except FileNotFoundError:
        return None
    except OSError as exc:
        if tolerate_stale and exc.errno == errno.ESTALE:
            return None
        raise
    if not raw:
        return None
    try:
        value = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise PoolContractError(f"queue record is not valid JSON: {path}") from exc
    if not isinstance(value, dict):
        raise PoolContractError(f"queue record is not an object: {path}")
    return value


def _read_json_fresh(path: Path) -> dict[str, object] | None:
    """``_read_json`` for a record another box files under a key this one polls.

    The queue is on NFS with default attribute caching.  A lookup of a name
    that does not exist yet is cached as a negative entry, and the client keeps
    answering ``ENOENT`` from it until the parent directory's attributes are
    revalidated -- up to ``acdirmin``, 30 s here.  A poller always looks first,
    so it always caches the miss.  On sparky, ``move_record`` saw a receipt
    dl380g10 had already filed 26.3 s to 26.6 s late with a plain read, and
    0.04 s to 0.25 s late with the parent opened first
    (``tools/fleet/qualify_record_visibility.py``, #808).  A produced-output
    owner waited that out on every staged group, after a copy that took 4 s.

    ``slurm_lane._read_json_object`` and ``pbrun.terminal_record`` follow the
    same rule by listing the parent.  Opening it is used here because these
    reads are polled several times a second and a listing costs what the
    directory holds (``done`` held 17,117 names when this was written): an
    open is one round trip, and close-to-open consistency makes it the
    revalidation.

    A stale "absent" is not only slow.  ``produced_output`` republishes a
    mover row it reads as absent, and an egress whose row it does not read as
    live and whose receipt it does not see.  In the 2026-09-21 live cycle
    those stale misses republished each egress three times, so each ran four
    times and each retirement took more than 20 s for a 4 s action.  The
    re-runs were no-ops (``stage_release.evict`` is idempotent), and the last
    one overwrote the receipt the retirement had been filed from.

    Only a miss pays for the revalidation: a record that is there is one
    read, as before.  It is best effort.  A parent that cannot be opened
    leaves the answer what the first read said, which is what this function
    answered before it revalidated anything, so no caller meets a failure it
    did not meet before; that read can still be stale.
    """

    record = _read_json(path)
    if record is not None:
        return record
    try:
        descriptor = os.open(
            path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    except OSError:
        return None
    os.close(descriptor)
    return _read_json(path)


def read_bounded_document(
    path: Path, *, max_bytes: int | None
) -> dict[str, object] | None:
    """``_read_json`` through Core's bounded, no-follow replaced-leaf reader.

    ``max_bytes is None`` is exactly :func:`_read_json`, so every uncapped
    caller keeps its behaviour.  With a cap the record is read as a stable,
    non-symlink regular file no larger than the bound (the writer replaces
    the name with ``os.replace``, so the replaced-leaf retry is the correct
    one) and decoded by Core's strict JSON reader, which refuses duplicate
    keys and non-finite numbers.  The mutable queue rows are writable by
    design, so no read-only requirement is imposed here; the immutable
    attempt logs and CAS objects keep their own stricter checks (#1446).

    A present leaf that is empty, oversized or invalid is refused, never read
    as absent: a terminal slot silently skipped here would let a readable
    success outrank a newer failure or withdrawal the census never parsed.
    Only a genuinely missing name is ``None``.
    """

    if max_bytes is None:
        return _read_json(path)
    try:
        raw = immutable_files.read_replaced_record(path, where="queue record", max_bytes=max_bytes)
    except FileNotFoundError:
        return None
    if not raw:
        raise PoolContractError(f"queue record is present but empty: {path}")
    try:
        value = digest_primitives.decode_strict_json(raw, where="queue record")
    except digest_primitives.PrismaBuildError as exc:
        raise PoolContractError(str(exc)) from exc
    if not isinstance(value, dict):
        raise PoolContractError(f"queue record is not an object: {path}")
    return value


def read_confirmed_document(path: Path, *, max_bytes: int | None = None) -> dict[str, object] | None:
    """Read after an error-visible directory refresh; never trust a cached miss."""
    names = os.listdir(path.parent)
    if path.name not in names:
        return None
    # A listed name that cannot be read is uncertain, not absent. Reopen
    # positive records too; the best-effort poll reader is not commit proof.
    raw = immutable_files.read_replaced_record(
        path,
        where="terminal proof record",
        max_bytes=max_bytes,
    )
    value = digest_primitives.decode_strict_json(raw, where="terminal proof record")
    if not isinstance(value, dict):
        raise PoolContractError(f"terminal proof record is not an object: {path}")
    return value


def _read_optional_document(path: Path
                            ) -> tuple[bool, dict[str, object] | None]:
    """``(present, document)``; invalid JSON raises, an empty file is present/None."""

    try:
        os.lstat(path)
    except FileNotFoundError:
        return False, None
    return True, _read_json(path)


TIER_DEMAND_SEPARATOR = "@"


def residency_map_address(root: Path, key: str) -> Path:
    return root / f"{key}.map.json"


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



@dataclass(frozen=True)
class QueueAddress:
    path: Path
    state: str
    action_key: str | None


@dataclass(frozen=True)
class RecordObservation:
    address: QueueAddress
    result: ReadResult[JsonRecord]


@dataclass(frozen=True)
class QueueCensus:
    rows: tuple[RecordObservation, ...]
    issues: tuple[Unknown, ...] = ()


@dataclass(frozen=True)
class TerminalCapture:
    candidates: tuple[RecordObservation, ...]


@dataclass(frozen=True)
class GenerationIdentity:
    action_key: str
    published_unix: float


class RecordStore:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self._layout_ensured = False

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
        path = self.root
        try:
            path = self.item_path(state, key)
            record = _read_json(path)
        except Exception as exc:
            return Unknown("record-unreadable", path, exc)
        return Absent() if record is None else Present(record)

    def mover(self, key: str) -> ReadResult[QueueRow]:
        """Poll claimed, ready, done, failed, then withdrawn for one key.

        Each missing or empty record gets the existing best-effort parent
        refresh and one retry. A hit needs no refresh. The first readable
        state wins, and any read exception leaves the answer unknown.
        """

        path = self.root
        for state in ("claimed", "ready"):
            try:
                path = self.root / state / f"{key}.json"
                record = _read_json_fresh(path)
            except Exception as exc:
                return Unknown("record-unreadable", path, exc)
            if record is not None:
                return Present(QueueRow(state, record))
        for state in ("done", "failed", "withdrawn"):
            try:
                path = self.root / state / f"{key}.json"
                record = _read_json_fresh(path)
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
        path = self.root
        try:
            for state in ("claimed", "ready"):
                path = self.root / state / f"{key}.json"
                record = _read_json(path)
                if record is not None:
                    return Present(QueueRow(state, record))
            path = self.root / "claimed"
            try:
                beside = [entry.name for entry in immutable_files.match_required(
                    path, f"{key}.*")]
            except FileNotFoundError:
                beside = []
            if any(name.endswith((TOMBSTONE_SUFFIX,
                                  LATE_FINISH_SUFFIX))
                   for name in beside):
                return Present(Moving())
            terminals: list[tuple[float, QueueState, JsonRecord]] = []
            for state in ("done", "failed", "withdrawn"):
                path = self.root / state / f"{key}.json"
                record = _read_json(path)
                if record is None:
                    continue
                stamp = record.get("published_unix")
                if isinstance(stamp, bool) or not isinstance(stamp, (int, float)):
                    return Unknown("generation-missing-or-nonnumeric", path)
                terminals.append((float(stamp), state, record))
        except (OSError, PoolContractError) as exc:
            return Unknown("record-unreadable", path, exc)
        if not terminals:
            return Absent()
        terminals.sort(key=lambda item: item[0])
        if len(terminals) > 1 and terminals[-1][0] == terminals[-2][0]:
            return Unknown("latest-generation-tied")
        _stamp, state, record = terminals[-1]
        return Present(QueueRow(state, record))
