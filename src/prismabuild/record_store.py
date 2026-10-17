"""Queue names, publication profiles and questions about filed records.

Queue names, JSON intake and named observations live here. Domain callers
retain scheduling, lifecycle and retention authority. Reads take no lock
and impose no deadline.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
import errno
import re
import heapq
import fnmatch
import json
import math
import os
import socket
import stat as statmod
import uuid
from pathlib import Path

from . import digest_primitives, immutable_files, kept_reads
from typing import Literal


MOVE_PRICING_LOG_TAG = b"pricing.v2"


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


from .claim_identity import CLAIM_IDENTITY_FIELDS


def residency_action_key(value: object) -> str:
    """One action key, checked, because these two names index shared files.

    The residency map and the frozen plan are named after the consumer; a
    caller that passed a path fragment instead of a key would name a file
    somewhere else under the shared mount.
    """

    if not key_shaped(value):
        raise PoolContractError("a residency file is named by a 64-character action key")
    return value


class PoolError(digest_primitives.PrismaBuildError):
    """A queue-level failure, distinct from an action-level one."""


class PoolContractError(PoolError, ValueError):
    """A queue record does not satisfy its schema."""


class AmbiguousClaimHolder(PoolContractError):
    """Contradictory committed reservations forbid concluding a claim."""


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
    return digest_primitives.raw_sha256(f"stage-ownership:{root}".encode())


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
    return _document_from_bytes(path, raw)


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

    ``RecordStore.wait_terminal_at`` follows the
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
    """``_read_json`` through the native bounded, no-follow replaced-leaf reader.

    ``max_bytes is None`` is exactly :func:`_read_json`, so every uncapped
    caller keeps its behaviour.  With a cap the record is read as a stable,
    non-symlink regular file no larger than the bound (the writer replaces
    the name with ``os.replace``, so the replaced-leaf retry is the correct
    one) and decoded by the digest owner's strict JSON reader, which refuses duplicate
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


def read_confirmed_document(
    path: Path,
    *,
    max_bytes: int | None = None,
) -> dict[str, object] | None:
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


class ScratchOwnerConflict(ValueError):
    """A recovery path cannot address the active lifetime owner."""


class RecordStore:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self._layout_ensured = False
        self._records: kept_reads.DirectoryRecords | None = None
        self._history: kept_reads.DirectoryCaptures | None = None
        self._history_once: dict[str, tuple[object, object]] | None = None
        self.history_listed = 0
        self.history_kept = 0

    def claimed(self, key: str) -> ReadResult[JsonRecord]:
        """Read one claimed record without refreshing a missing name."""
        return self._plain("claimed", key)

    def lease(self, key: str) -> ReadResult[JsonRecord]:
        """Read the existing lease once, without renewal or missing-name retry."""
        path = self.lease_path(key)
        try:
            record = _read_json(path)
        except Exception as exc:
            return Unknown("record-unreadable", path, exc)
        return Absent() if record is None else Present(record)

    def scratch_owner(self, record: Mapping[str, object], *, scope_only=False,
                      owner_path: Path | None = None) -> tuple[Path, JsonRecord | None]:
        """Read one live, late, tombstone or widowed-lease scratch owner."""
        key = str(record["action_key"])
        claimed = self.dir(CLAIMED)
        live_path = self.item_path(CLAIMED, key)
        path = self.late_finish_path(record) if scope_only else live_path
        if owner_path is not None:
            if (scope_only or owner_path.parent != claimed
                    or not (owner_path == self.lease_path(key)
                            or (owner_path.name.startswith(key + ".")
                                and owner_path.name.endswith(TOMBSTONE_SUFFIX)))
                    or live_path.exists()):
                raise ScratchOwnerConflict("scratch recovery owner conflicts with a live claim")
            path = owner_path
        return path, _read_json(path)

    def queued(self, key: str) -> ReadResult[JsonRecord]:
        """Read ready only when the claimed record is missing or empty."""
        claimed = self._plain("claimed", key)
        if not isinstance(claimed, Absent):
            return claimed
        return self._plain("ready", key)

    def _plain(self, state: str, key: str) -> ReadResult[JsonRecord]:
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
                try:
                    generation = float(stamp)
                except (OverflowError, ValueError) as exc:
                    return Unknown("generation-unorderable", path, exc)
                if not math.isfinite(generation):
                    return Unknown("generation-nonfinite", path)
                terminals.append((generation, state, record))
        except (OSError, PoolContractError) as exc:
            return Unknown("record-unreadable", path, exc)
        if not terminals:
            return Absent()
        terminals.sort(key=lambda item: item[0])
        if len(terminals) > 1 and terminals[-1][0] == terminals[-2][0]:
            return Unknown("latest-generation-tied")
        _stamp, state, record = terminals[-1]
        return Present(QueueRow(state, record))

    def dir(self, state: str) -> Path:
        if state not in _STATES:
            raise PoolContractError(f"unknown queue state: {state!r}")
        return self.root / state

    def item_path(self, state: str, action_key: str) -> Path:
        return self.dir(state) / f"{action_key}.json"

    def lease_path(self, action_key: str) -> Path:
        return self.dir(CLAIMED) / f"{action_key}.lease"

    def action_status_path(self, action_key: str) -> Path:
        """Where the launcher may leave facts its exit status cannot carry.

        The launcher exits 1 for every failed action, so ``detail.returncode``
        on this lane is the *launcher's* status and the action's own was simply
        unavailable here -- ``core`` has written it to this file since the
        status contract was added, and nothing on this lane set the variable
        that names the file.  A killed run's partial profile comes back the
        same way, because the launcher is being killed and cannot print it.

        Beside the lease, with a suffix that is neither ``.json`` nor
        ``.lease``: every reader of ``claimed/`` addresses those two names, so
        a third is invisible to all of them.  It is unlinked as it is read.
        """

        return self.dir(CLAIMED) / f"{action_key}.status"

    def action_progress_path(self, action_key: str) -> Path:
        """Where an action reports advancement while it is still running.

        A file of its own, beside the lease and the status sidecar and equally
        invisible to every reader of ``claimed/``, because it is the one thing
        here written repeatedly *during* execution by a process this loop does
        not own.  Merging it into the status sidecar would put a ticking writer
        into an unlocked read-merge-write shared with the launcher's own ending
        facts, and would ask a heartbeat to read a file whose reader unlinks it.

        The read costs one open of a small file in a directory this loop
        already writes to every heartbeat, at the heartbeat's own cadence.
        """

        return self.dir(CLAIMED) / f"{action_key}.progress"

    def attempt_generation(self, record: Mapping[str, object]) -> str:
        """Stable directory name for one submission of a content-addressed key."""

        key = str(record.get("action_key") or "")
        published = record.get("published_unix")
        if len(key) != 64 or any(ch not in "0123456789abcdef" for ch in key):
            raise PoolContractError("attempt history requires a full action key")
        if type(published) not in (int, float):
            raise PoolContractError("attempt history requires published_unix")
        try:
            generation = float(published)
        except OverflowError as exc:
            raise PoolContractError("attempt history requires finite published_unix") from exc
        if not math.isfinite(generation):
            raise PoolContractError("attempt history requires published_unix")
        return digest_primitives.canonical_sha256(
            {"action_key": key, "published_unix": generation}
        )

    def publication_ending_path(self, record: Mapping[str, object]) -> Path:
        """The immutable ending address for an exact publication identity."""
        generation = self.attempt_generation(record)
        return self.root / "publication-endings" / str(record["action_key"]) / f"{generation}.json"

    def attempt_path(self, record: Mapping[str, object], attempt: int) -> Path:
        """Immutable outcome path for one numbered attempt of one generation."""

        if type(attempt) is not int or attempt < 1:
            raise PoolContractError("attempt number must be a positive integer")
        key = str(record.get("action_key") or "")
        return (
            self.root
            / ATTEMPTS
            / key
            / self.attempt_generation(record)
            / f"{attempt:08d}.json"
        )

    def attempt_log_path(
        self,
        record: Mapping[str, object],
        attempt: int,
        stream: str,
        sha256: str,
    ) -> Path:
        """Immutable stdout/stderr path beside an attempt outcome."""

        if stream not in {"stdout", "stderr"}:
            raise PoolContractError(f"unknown attempt log stream: {stream!r}")
        if (
            not isinstance(sha256, str)
            or len(sha256) != 64
            or any(ch not in "0123456789abcdef" for ch in sha256)
        ):
            raise PoolContractError("attempt log requires a full SHA-256 digest")
        outcome = self.attempt_path(record, attempt)
        return outcome.parent / f"{attempt:08d}.{stream}.{sha256}.log"

    def ensure_layout(self) -> None:
        """Create the queue directories, once per process.

        Fifteen ``mkdir`` calls: one per directory in ``_STATES`` plus one
        per auxiliary root.  On a starved NFS mount each costs RPCs (a ``MKDIR``
        plus a stat on ``FileExistsError``), so running this on every claim
        poll billed every worker a directory walk per poll for directories
        nothing ever deletes (#595).  The first call in this process creates
        them; later calls are a flag check.  A directory an operator deletes
        by hand returns on the next loop restart, which is the same window
        any other queue repair already needs.
        """

        if self._layout_ensured:
            return
        for state in _STATES:
            self.dir(state).mkdir(parents=True, exist_ok=True)
        (self.root / WORKERS).mkdir(parents=True, exist_ok=True)
        (self.root / ATTEMPTS).mkdir(parents=True, exist_ok=True)
        (self.root / PREWARM).mkdir(parents=True, exist_ok=True)
        (self.root / MOVERS).mkdir(parents=True, exist_ok=True)
        (self.root / RESIDENCY).mkdir(parents=True, exist_ok=True)
        (self.root / RESIDENCY_PLANS).mkdir(parents=True, exist_ok=True)
        (self.root / TIER_RESERVATIONS).mkdir(parents=True, exist_ok=True)
        (self.root / TIERS).mkdir(parents=True, exist_ok=True)
        # Created here so the claim's listing never meets a directory that did
        # not exist yet, which NFS may keep answering as absent (#954).
        self.released_origin_consumers_dir().mkdir(parents=True, exist_ok=True)
        self._layout_ensured = True

    def passes_path(self, action_key: str) -> Path:
        return self.root / PASSES / f"{action_key}.json"

    def prewarm_path(self, action_key: str) -> Path:
        return self.root / PREWARM / f"{action_key}.json"

    def move_path(self, action_key: str) -> Path:
        return self.root / MOVERS / f"{action_key}.json"

    def mover_retirement_checkpoint_path(self) -> Path:
        """The atomically replaced compact checkpoint of retired projections."""

        return self.root / MOVERS_RETIRED / MOVERS_RETIREMENT_CHECKPOINT

    def mover_retirement_state_path(self) -> Path:
        """The persistent marker proving at least one retirement committed."""

        return self.root / MOVERS_RETIRED / MOVERS_RETIREMENT_STATE

    def mover_archive_dir(self) -> Path:
        """Where retired receipts' full bodies live, named by content digest."""

        return self.root / MOVERS_ARCHIVE

    def move_pricing_log_path(self) -> Path:
        """The pricing log :meth:`move_records` reads (#1044)."""

        return self.root / MOVERS_PRICING / MOVERS_PRICING_LOG

    def export_path(self, export_key: str) -> Path:
        """The file one finished export's pacing record keeps (#1319).

        ``exports/<export_key>.json``: one small immutable file per
        export.  The key is the export action key (a 64-hex seal, so it
        needs no escaping); a record without one is corrupt and refused
        loudly here, never filed.
        """

        key = str(export_key)
        if len(key) != 64 or any(ch not in "0123456789abcdef" for ch in key):
            raise PoolContractError(
                "export receipt names no 64-hex export key; "
                "record_export files one file per export")
        return self.root / EXPORTS / f"{key}.json"

    def residency_fragment_root(self) -> Path:
        """Where movement nodes file their fragments, one directory per consumer."""

        return self.root / RESIDENCY

    def residency_map_path(self, consumer_action_key: str) -> Path:
        """The composed map an action reads its staged inputs through.

        One definition, because two parties depend on this name being the same
        string: the ``tiers`` loop is its single writer, and this launcher puts
        it in the action's environment.  A second spelling would be a consumer
        reading a map nobody writes, which fails by falling back to the pool --
        that is, silently, at full cost.
        """

        return residency_map_address(
            self.residency_fragment_root(),
            residency_action_key(consumer_action_key))

    def residency_plan_path(self, consumer_action_key: str) -> Path:
        """The frozen window plan: every mover this consumer will ever have."""

        return (self.root / RESIDENCY_PLANS
                / f"{residency_action_key(consumer_action_key)}.json")

    def denial_transitions_path(self, action_key: str) -> Path:
        """The key's reason ring: ``denial-transitions/<key>.json``."""

        return self.root / DENIAL_TRANSITIONS / f"{residency_action_key(action_key)}.json"

    def consumer_event_path(self, key: str, host: str) -> Path:
        return self.consumer_events_dir(key) / f"{host}.jsonl"

    def host_event_path(self, host: str) -> Path:
        return self.host_events_dir() / f"{host}.jsonl"

    def consumer_events_dir(self, action_key: str) -> Path:
        """``residency-events/<consumer>/``: one ``<host>.jsonl`` per tier loop."""

        return self.root / RESIDENCY_EVENTS / residency_action_key(action_key)

    def host_events_dir(self) -> Path:
        """``residency-events/_host/``: one ``<host>.jsonl`` per tier loop (#1006)."""

        return self.root / RESIDENCY_EVENTS / RESIDENCY_EVENTS_HOST_DIR

    def mover_landing_path(self, mover: str) -> Path:
        """Where ``mover`` files its own landing report (#1090)."""

        return self.consumer_events_dir(mover) / MOVER_LANDING_REPORT

    @staticmethod
    def check_tier_id(tier_id: str) -> str:
        if (not isinstance(tier_id, str) or not tier_id or tier_id.startswith(".")
                or "/" in tier_id or TIER_DEMAND_SEPARATOR in tier_id):
            raise PoolContractError(f"invalid tier id {tier_id!r}")
        return tier_id

    def stage_ownership_holder_path(self, stage_root) -> Path:
        """Where the holder of one stage root's ownership lock names itself."""

        return (self.root / STAGE_OWNERSHIP_HOLDERS
                / f"{stage_ownership_name(stage_root)}.json")

    def funding_path(self, mover_action_key: str, tier_id: str) -> Path:
        """One mover's funding record on one tier (newest generation wins)."""

        return (self.root / TIER_FUNDING
                / f"{mover_action_key}.{tier_id}.funding.json")

    def funding_output_path(self, mover_action_key: str, tier_id: str) -> Path:
        """One output mover's funding record on one tier."""

        return (self.root / TIER_FUNDING
                / f"{mover_action_key}.{tier_id}.output-funding.json")

    def funding_output_retired_path(self, mover_action_key: str,
                                    tier_id: str) -> Path:
        """Where :meth:`retire_terminal_output_funding` moves that record."""

        return (self.root / TIER_FUNDING / TIER_FUNDING_RETIRED
                / f"{mover_action_key}.{tier_id}.output-funding.json")

    def tier_directory(self) -> Path:
        return self.root / TIERS

    def tier_record_path(self, tier_id: str) -> Path:
        return self.tier_directory() / f"{self.check_tier_id(tier_id)}.json"

    def tier_commitment_path(self, tier_id: str) -> Path:
        return self.root / TIER_COMMITMENTS / f"{self.check_tier_id(tier_id)}.json"

    def attempt_bytes(self, path: Path, *, where: str) -> bytes:
        """Read one linked immutable attempt file, past a stale "absent" (#1100).

        A landed ending links its attempt records, so a miss here is either a
        record that is really gone or a name this NFS client cached as absent.
        The second is the ordinary case for a waiter on another box: its poll
        walks ``attempts/<key>/<generation>`` before the ending lands
        (``archived_generation_outcomes``), the worker's box then creates it,
        and the client keeps answering ``ENOENT`` from the lookup it cached.
        On sparky that outlived a fresh open of the parent for 25 s to more
        than 180 s, so the close-to-open revalidation ``_read_json_fresh``
        relies on (#808) is not enough here.

        A miss is revalidated by listing each directory on the canonical
        path, the rule ``pbrun.terminal_record`` follows for ``done/``, and
        the file is read again only when every name is listed.  Only a miss
        pays for the listings, and ``attempts/`` holds one name per action
        key, about as many as the ``done/`` a waiter already lists on every
        poll.  A name that no listing shows raises the original
        ``FileNotFoundError``: a missing record is never read around.  A name
        that is listed but still cannot be opened raises
        ``StaleAbsenceError``, which a waiter repeats inside its own patience.
        """

        try:
            return immutable_files.read_immutable_file(path, where=where)
        except FileNotFoundError:
            if not self._attempt_path_listed(path):
                raise
        try:
            return immutable_files.read_immutable_file(path, where=where)
        except FileNotFoundError as exc:
            raise StaleAbsenceError(
                errno.ENOENT,
                f"{where} is listed by its directory but is not visible here "
                f"yet (a lookup this client cached before it was created, "
                f"#1100)",
                str(path),
            ) from exc

    def _attempt_path_listed(self, path: Path) -> bool:
        """Whether each directory from ``attempts/`` down lists the next name.

        Listing a directory is also what refreshes this client's view of its
        names.  A directory whose own name its parent just listed, and that
        still cannot be opened for its listing, is the same stale lookup one
        level down, so it counts as listed.  Any other failure to list
        answers ``False``, and the caller keeps its original miss.
        """

        base = self.root / ATTEMPTS
        try:
            parts = path.relative_to(base).parts
        except ValueError:
            return False
        directory = base
        for depth, name in enumerate(parts):
            try:
                names = os.listdir(directory)
            except FileNotFoundError:
                if depth == 0:
                    return False
                return True
            except OSError:
                return False
            if name not in names:
                return False
            directory = directory / name
        return True

    def archived_attempt_identity(
        self,
        path: Path,
        value: Mapping[str, object],
        *,
        action_key: str,
        generation: object,
    ) -> int:
        """The attempt number this immutable outcome canonically addresses.

        ``archive_attempt`` publishes one file per numbered attempt under
        ``(action_key, published_unix)``.  A reader that reconstructs an
        ending from that evidence must not be redirected to another action,
        another generation or another attempt number by the file's own
        claims, so the file's identity is checked against the directory it
        was found in and against the canonical path its number derives.  The
        number must also lie within the budget the attempt itself records: an
        attempt above its own ``max_attempts`` is inconsistent identity, not
        an ending, whatever its status says.  A legacy ``retry_safe`` of
        ``None`` remains valid; only the handoff context requires it.
        """

        if (
            type(generation) not in (int, float)
            or isinstance(generation, bool)
            or not math.isfinite(float(generation))
        ):
            raise PoolContractError("archived attempt names no generation")
        attempt = value.get("attempt")
        published = value.get("published_unix")
        limit = value.get("max_attempts")
        retry_safe = value.get("retry_safe")
        if (
            value.get("schema") != POOL_ATTEMPT_SCHEMA_V1
            or value.get("action_key") != action_key
            or type(published) not in (int, float)
            or isinstance(published, bool)
            or not math.isfinite(float(published))
            or float(published) != float(generation)
            or type(attempt) is not int
            or attempt < 1
            or type(limit) is not int
            or limit < 1
            or attempt > limit
            or (retry_safe is not None and type(retry_safe) is not bool)
            or path != self.attempt_path(
                {"action_key": action_key, "published_unix": float(generation)},
                attempt,
            )
        ):
            raise PoolContractError(
                f"archived attempt does not own its path and generation: {path}"
            )
        return attempt

    def late_finish_path(self, record: Mapping[str, object]) -> Path:
        identity = digest_primitives.canonical_sha256(
            {field: record.get(field) for field in CLAIM_IDENTITY_FIELDS},
        )
        return self.dir(CLAIMED) / f"{record['action_key']}.{identity}{LATE_FINISH_SUFFIX}"

    def released_origin_consumers_dir(self) -> Path:
        """Where the index of released origin-batch consumers lives (#954)."""

        return self.root / RELEASED_ORIGIN_CONSUMERS

    def superseded_dir(self) -> Path:
        """Where records go once a generation decision makes them non-live.

        A subdirectory rather than a timestamped sibling, because every reader
        of ``withdrawn/`` addresses it by ``<key>.json``: ``withdrawn_keys``
        lists it, ``find_key`` globs it, ``item_path`` builds the name,
        ``pbrun``'s wait loop lists it and client status screens count ``*.json``
        in it.  A sibling named ``<key>.<unix>.json`` would look to all five
        like an action whose key is nonsense; a subdirectory is invisible to
        every one of them.  It keeps both retired withdrawals and ready/claimed
        copies dropped because a terminal record already owns their generation,
        so no queue record disappears without evidence.
        """

        return self.dir(WITHDRAWN) / "superseded"

    def withdrawal_decision_path(self, record: Mapping[str, object]) -> Path | None:
        """The durable cancellation address for records with known generation."""
        try:
            generation = self.attempt_generation(record)
        except PoolContractError:
            return None  # legacy marker without a publication identity
        return self.dir(WITHDRAWN) / "decisions" / str(record["action_key"]) / f"{generation}.json"

    def withdrawal_decision(self, path: Path) -> dict[str, object]:
        try:
            raw = immutable_files.read_immutable_file(path, where="withdrawal decision")
            decision = json.loads(raw)
            if (not isinstance(decision, dict) or decision.get("status") != "withdrawn"
                    or self.withdrawal_decision_path(decision) != path):
                raise ValueError("decision identity disagrees with its address")
        except Exception as exc:
            raise PoolContractError(f"invalid withdrawal decision: {path}: {exc}") from exc
        return decision


    def item(self, state: str, key: str) -> ReadResult[JsonRecord]:
        return self._plain(state, key)

    def claimed_for_action(self, key: str) -> ReadResult[JsonRecord]:
        path = self.item_path(CLAIMED, key)
        try:
            try:
                present = path.exists()
            except OSError:
                present = True
            if not present:
                return Absent()
            record = _read_json(path)
            if record is None:
                try:
                    if not path.exists():
                        return Absent()
                except OSError:
                    pass
                raise PoolContractError(
                    f"claimed record is empty (torn write or broken mount?): {path}")
            return Present(record)
        except Exception as exc:
            return Unknown("record-unreadable", path, exc)

    def terminal_candidates(self, key: str, *, max_bytes: int | None = None) -> TerminalCapture:
        return self._terminal_capture(key, max_bytes=max_bytes, confirmed=False)

    def confirmed_terminals(self, key: str, *, max_bytes: int | None = None) -> TerminalCapture:
        return self._terminal_capture(key, max_bytes=max_bytes, confirmed=True)

    def _terminal_capture(self, key: str, *, max_bytes: int | None, confirmed: bool) -> TerminalCapture:
        candidates = []
        for state in (DONE, FAILED, WITHDRAWN):
            path = self.item_path(state, str(key))
            try:
                record = (read_confirmed_document(path, max_bytes=max_bytes) if confirmed
                          else read_bounded_document(path, max_bytes=max_bytes))
            except (OSError, digest_primitives.PrismaBuildError) as exc:
                result = Unknown("record-unreadable", path, exc)
            else:
                result = Absent() if record is None else Present(record)
            candidates.append(RecordObservation(QueueAddress(path, state, str(key)), result))
        return TerminalCapture(tuple(candidates))


    def item_for_publish(self, state: str, key: str) -> ReadResult[JsonRecord]:
        path = self.item_path(state, key)
        try:
            value = _read_json_fresh(path)
        except Exception as exc:
            return Unknown("record-unreadable", path, exc)
        return Absent() if value is None else Present(value)

    def confirmed_item(
        self,
        state: str,
        key: str,
        *,
        max_bytes: int | None = None,
    ) -> ReadResult[JsonRecord]:
        path = self.item_path(state, key)
        try:
            value = read_confirmed_document(path, max_bytes=max_bytes)
        except Exception as exc:
            return Unknown("record-unreadable", path, exc)
        return Absent() if value is None else Present(value)

    def movement_for_poll(self, key: str) -> ReadResult[JsonRecord]:
        path = self.move_path(key)
        try:
            value = _read_json_fresh(path)
        except Exception as exc:
            return Unknown("record-unreadable", path, exc)
        return Absent() if value is None else Present(value)

    def confirmed_movement(self, key: str) -> ReadResult[JsonRecord]:
        path = self.move_path(key)
        try:
            value = read_confirmed_document(path)
        except Exception as exc:
            return Unknown("record-unreadable", path, exc)
        return Absent() if value is None else Present(value)

    def mover_retirement_checkpoint(self) -> tuple[bool, dict[str, object] | None]:
        return _read_optional_document(self.mover_retirement_checkpoint_path())

    def mover_retirement_state(self) -> tuple[bool, dict[str, object] | None]:
        return _read_optional_document(self.mover_retirement_state_path())


    def listed_record(self, path: Path) -> ReadResult[JsonRecord]:
        """Read an already-enumerated row with the existing ESTALE absence rule."""
        try:
            value = _read_json(path, tolerate_stale=True)
        except Exception as exc:
            return Unknown("record-unreadable", path, exc)
        return Absent() if value is None else Present(value)


    def ready_publication(self, key: str) -> dict[str, object]:
        raw = immutable_files.read_replaced_record(
            self.item_path(READY, key),
            where="successor publication",
        )
        record = digest_primitives.decode_strict_json(raw, where="successor publication")
        if not isinstance(record, dict):
            raise PoolContractError("successor publication must be an object")
        return record


    def publication_ending(
        self, key: str, *, generation: float,
    ) -> tuple[Path, JsonRecord] | None:
        """Read one exact immutable decision; absence never infers a successor."""
        path = self.publication_ending_path({"action_key": key, "published_unix": generation})
        try:
            names = os.listdir(path.parent)
        except FileNotFoundError:
            return None
        if path.name not in names:
            return None
        raw = immutable_files.read_immutable_file(path, where="publication ending")
        record = digest_primitives.decode_strict_json(raw, where="publication ending")
        return path, record


    def attempt_document(self, path: Path) -> JsonRecord:
        raw = self.attempt_bytes(path, where="pool attempt outcome")
        try:
            value = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise PoolContractError(
                f"pool attempt outcome is not valid JSON: {path}"
            ) from exc
        if not isinstance(value, dict):
            raise PoolContractError(
                f"pool attempt outcome is not an object: {path}"
            )
        return value


    def archived_generation_records(
        self, action_key: str, *, generation: float,
    ) -> dict[int, tuple[Path, JsonRecord]]:
        key = str(action_key)
        if (
            type(generation) not in (int, float)
            or isinstance(generation, bool)
            or not math.isfinite(float(generation))
        ):
            raise PoolContractError("archived generation must be a finite timestamp")
        identity = {"action_key": key, "published_unix": float(generation)}
        generation_name = self.attempt_generation(identity)  # validates key and timestamp
        base = self.root / ATTEMPTS / key / generation_name
        try:
            entries = immutable_files.scan_required(base)
        except (FileNotFoundError, NotADirectoryError):
            # A generation that has not archived an attempt is an ordinary
            # wait.  Every other enumeration failure stays loud: a reader
            # that cannot see the directory must not answer "no ending".
            return {}
        present: dict[int, tuple[Path, dict[str, object]]] = {}
        for path in entries:
            name = path.name
            stem = name[:-5] if name.endswith(".json") else ""
            if not stem or not stem.isascii() or not stem.isdigit():
                # ``pool_reconcile`` writes ``<number>.receipt-reconciliation
                # .json`` beside the attempt it supplements; a numbered
                # attempt is exactly ``<digits>.json`` and nothing else here
                # is one.
                continue
            value = read_document(path)
            if value is None:
                raise PoolContractError(f"archived attempt is unreadable: {path}")
            attempt = self.archived_attempt_identity(
                path, value, action_key=key, generation=identity["published_unix"])
            present[attempt] = (path, value)
        return present


    def withdrawal_decisions(
        self, key: str, *, generation: float | None = None,
    ) -> list[tuple[Path, dict[str, object]]]:
        """Immutable cancellations for this key, optionally one generation."""

        directory = self.dir(WITHDRAWN) / "decisions" / key
        if generation is None:
            paths = immutable_files.match_optional(directory, "*.json")
        else:
            path = self.withdrawal_decision_path({
                "action_key": key, "published_unix": generation,
            })
            paths = [] if path is None else immutable_files.match_optional(directory, path.name)
        return [(path, self.withdrawal_decision(path)) for path in paths]


    def attempt_documents(
        self, action_key: str, *, generation: float | None = None,
    ) -> Iterator[tuple[Path, JsonRecord | None]]:
        """Read an optional history namespace in its original file order.

        Documents are yielded as read, so a caller's refusal still stops
        before touching the next record.
        """
        identity = {"action_key": action_key,
                    "published_unix": generation if generation is not None else 0.0}
        generation_name = self.attempt_generation(identity)
        base = self.root / ATTEMPTS / action_key
        if generation is not None:
            base /= generation_name
        pattern = "*.json" if generation is not None else "*/*.json"
        for path in immutable_files.match_optional(base, pattern):
            yield path, read_document(path)


    def prepare_lane_state(self, state: str) -> Path:
        directory = self.root / state
        directory.mkdir(parents=True, exist_ok=True)
        return directory

    def lane_item_address(self, state: str, key: str) -> Path:
        return self.prepare_lane_state(state) / f"{key}.json"

    def released_origin_consumer_path(self, consumer: str, checked_ref: Mapping[str, str]) -> Path:
        raw = digest_primitives.compact_ascii_json_bytes(dict(checked_ref))
        digest = digest_primitives.raw_sha256(raw)
        return self.released_origin_consumers_dir() / f"{consumer}.{digest}.json"

    def residency_supersession_path(self, consumer: str, digest: str) -> Path:
        return self.residency_plan_path(
            consumer,
        ).parent / "superseded" / f"{consumer}.{digest}.superseded.json"

    @staticmethod
    def lane_unreadable_name(path: Path, stamp: int, token: str) -> str:
        return f"{path.stem}.{stamp}.{token}.json"

    def worker_directory(self) -> Path:
        return self.root / WORKERS

    def ready_transition_directory(self) -> Path:
        return self.root / "ready-transitions"

    @staticmethod
    def claim_tombstone_name(key: str, when: float, host: str, pid: int, nonce: str) -> str:
        return f"{key}.{int(when * 1_000_000)}.{host}.{pid}.{nonce}{TOMBSTONE_SUFFIX}"

    @staticmethod
    def ready_transition_name(key: str, when: float, nonce: str, kind: str) -> str:
        return f"{key}.{int(when * 1_000_000)}.{nonce}.{kind}.json"

    def unreadable_capture_path(self, key: str) -> Path:
        return self.superseded_dir() / f"{key}.{uuid.uuid4().hex}.unreadable.raw"

    def mover_archive_path(self, digest: str) -> Path:
        return self.mover_archive_dir() / f"{digest}.json"

    def withdrawn_finish_path(self, key: str, record: Mapping[str, object]) -> Path:
        return self.superseded_dir(
        ) / f"{key}.{self.attempt_generation(
            record,
        )}.withdrawn-finish.json"

    @staticmethod
    def summary_lock_path(path: Path) -> Path:
        return path.parent.parent / ".summary-locks" / f"{path.stem}.lock"

    def lane_withdrawal_archive(self, key: str, when: float) -> Path:
        directory = self.prepare_lane_state(WITHDRAWN) / "superseded"
        directory.mkdir(parents=True, exist_ok=True)
        return directory / f"{key}.{when:.6f}.withdrawal.json"

    def worker_offer_path(self, host: str) -> Path:
        return self.root / WORKERS / f"{host}.json"

    def file_superseded(
        self, record: Mapping[str, object] | None, *, key: str, kind: str,
        when: float, **stamps: object,
    ) -> Path:
        payload = dict(record or {})
        payload["action_key"] = key
        payload.update(stamps)
        path = self.superseded_dir() / f"{key}.{when:.6f}.{kind}.json"
        self.replace_record_at(path, payload)
        return path

    def withdrawal_capture_path(self, key: str) -> Path:
        return self.superseded_dir() / f"{key}.{uuid.uuid4().hex}.withdrawal-source"

    def movement_pricing_records(self) -> tuple[dict[str, dict[str, object]], int | None]:
        """What the pricing log says about each receipt name, and its size.

        One read of one file.  The last line about a name wins, because a
        receipt re-filed under the same key replaces the file and appends a
        new line.  A line whose body does not parse -- a write cut off by a
        crash, or bytes an operator mangled -- makes its name *unknown*
        rather than leaving an older line standing, so the reader reads that
        receipt itself.  A missing log reads as empty; an unreadable one too,
        with ``None`` as its size so nothing is appended to it.
        """

        path = self.move_pricing_log_path()
        try:
            raw = path.read_bytes()
        except FileNotFoundError:
            return {}, 0
        except OSError:
            return {}, None
        kept: dict[str, dict[str, object]] = {}
        lines = raw.split(b"\n")
        # The last piece has no newline after it: empty when the log ends
        # cleanly, a line still being written (or cut off) otherwise.
        partial = lines.pop()
        for line in lines:
            name = pricing_line_name(line)
            if name is None:
                continue
            try:
                record = json.loads(line.split(b"\t", 2)[2])
            except (IndexError, ValueError):
                record = None
            if isinstance(record, dict):
                kept[name] = record
            else:
                kept.pop(name, None)
        name = pricing_line_name(partial)
        if name is not None:
            kept.pop(name, None)
        return kept, len(raw)

    @staticmethod
    def event_line(record: Mapping[str, object]) -> str:
        return json.dumps(dict(record), default=str)

    def append_stall_event(self, key: str, line: str) -> None:
        directory = self.consumer_events_dir(key)
        directory.mkdir(parents=True, exist_ok=True)
        with open(directory / f"{socket.gethostname()}-stall-watch.jsonl", "a") as stream:
            stream.write(line + "\n")

    def replace_record_at(self, path: Path, payload: Mapping[str, object]) -> None:
        """Replace a canonical store address or a path held by a domain transition."""
        immutable_files.write_json_atomic(path, payload)


    def replace_stage_holder_at(self, path: Path, payload: Mapping[str, object]) -> None:
        immutable_files.write_json_atomic(path, payload, text="sorted_lf", tmp="pid", fsync=False)

    def replace_reader_scope_at(self, path: Path, payload: Mapping[str, object]) -> None:
        immutable_files.write_json_atomic(path, payload, text="sorted_lf", tmp="pid")


    def replace_record_bytes_at(self, path: Path, raw: bytes) -> None:
        immutable_files.write_bytes_atomic(path, raw)


    def file_publication_ending_at(self, path: Path, payload: Mapping[str, object]) -> None:
        immutable_files.publish_exact(
            path, digest_primitives._canonical_bytes(payload),
            where="publication ending", conflict_error=PoolContractError)

    def file_attempt_log_at(self, path: Path, raw: bytes, *, stream: str) -> None:
        immutable_files.publish_exact(
            path, raw, where=f"pool attempt {stream}", conflict_error=PoolContractError)

    def file_record_if_absent_at(self, path: Path, payload: Mapping[str, object]) -> None:
        immutable_files.publish_immutable(path, digest_primitives._canonical_bytes(payload))


    def file_receipt_reconciliation_at(self, path: Path, payload: Mapping[str, object]) -> None:
        immutable_files.publish_exact(
            path, digest_primitives._canonical_bytes(payload) + b"\n",
            where="pool receipt reconciliation", conflict_error=PoolContractError)

    def replace_lane_item_at(self, path: Path, payload: Mapping[str, object]) -> None:
        immutable_files.write_json_atomic(path, payload, trailing_newline=True)

    def file_lane_item_if_absent(self, path: Path, payload: Mapping[str, object]) -> bool:
        return immutable_files.publish_mutable(
            path, digest_primitives._canonical_bytes(dict(payload)) + b"\n")

    @staticmethod
    def replace_cycle_at(path: Path, record: Mapping[str, object]) -> None:
        """Publish a record a loop rewrites every cycle, making its directory once.

        The directory is made only when the write finds it missing, so the
        steady state costs no ``mkdir`` per call (#960): on a starved NFS
        mount each one is an RPC, the reason ``ensure_layout`` runs once per
        process (#595).  A directory an operator deletes comes back on the
        next write.
        """

        try:
            immutable_files.write_json_atomic(path, record, make_parent=False)
        except FileNotFoundError:
            path.parent.mkdir(parents=True, exist_ok=True)
            immutable_files.write_json_atomic(path, record, make_parent=False)


    def file_withdrawal_decision(self, record: Mapping[str, object]) -> dict[str, object]:
        path = self.withdrawal_decision_path(record)
        if path is None:
            return dict(record)
        immutable_files.publish_immutable(path, digest_primitives._canonical_bytes(record))
        return self.withdrawal_decision(path)


    def file_residency_plan_at(self, path: Path, payload: Mapping[str, object]) -> None:
        immutable_files.publish_exact(
            path, digest_primitives._canonical_bytes(payload),
            where="residency plan", conflict_error=PoolContractError)

    def file_residency_supersession_at(self, path: Path, payload: Mapping[str, object]) -> None:
        immutable_files.publish_exact(
            path, digest_primitives._canonical_bytes(payload),
            where="residency plan supersession", conflict_error=PoolContractError)

    def file_released_origin_index_at(self, path: Path, raw: bytes) -> None:
        immutable_files.publish_exact(
            path, raw, where="produced-output origin consumer release index",
            conflict_error=PoolContractError)


    def item_presence(self, state: str, key: str) -> ReadResult[bool]:
        return self._presence_at(self.item_path(state, key), "slot-unreadable")

    def lease_presence(self, key: str) -> ReadResult[bool]:
        return self._presence_at(self.lease_path(key), "lease-unreadable")

    def plan_presence(self, key: str) -> ReadResult[bool]:
        return self._presence_at(self.residency_plan_path(key), "plan-unreadable")

    def funding_presence(self, mover: str, tier: str) -> ReadResult[bool]:
        return self._presence_at(self.funding_path(mover, tier), "funding-unreadable")

    def attempt_presence(self, record: JsonRecord, attempt: int) -> ReadResult[bool]:
        return self._presence_at(self.attempt_path(record, attempt), "attempt-unreadable")

    @staticmethod
    def _presence_at(path: Path, reason: str) -> ReadResult[bool]:
        try:
            path.stat()
        except (FileNotFoundError, NotADirectoryError):
            return Absent()
        except OSError as exc:
            return Unknown(reason, path, exc)
        return Present(True)

    def ready_first_live_row(self, key: str) -> ReadResult[JsonRecord]:
        for state in (READY, CLAIMED):
            result = self.item(state, key)
            if not isinstance(result, Absent):
                return result
        return Absent()

    def bindable_mover(self, key: str) -> ReadResult[JsonRecord]:
        for state in (READY, CLAIMED):
            presence = self.item_presence(state, key)
            if isinstance(presence, Unknown):
                return presence
            if isinstance(presence, Absent):
                continue
            path = self.item_path(state, key)
            try:
                record = read_document(path)
            except (OSError, ValueError, PoolContractError) as exc:
                return Unknown("binding-row-unreadable", path, exc)
            if record is not None:
                return Present(record)
        return Absent()

    def advisory_live_rows(self) -> tuple[dict[str, tuple[str, JsonRecord]], list[str]]:
        """Best-effort rows, keyed by embedded key, including hidden JSON names."""
        live: dict[str, tuple[str, JsonRecord]] = {}
        errors: list[str] = []
        for state in (READY, CLAIMED):
            try:
                names = sorted(os.listdir(self.dir(state)))
            except OSError as exc:
                errors.append(f"{state}: {exc!r}")
                continue
            for name in names:
                if not name.endswith(".json"):
                    continue
                try:
                    record = value_or_raise(self.listed_record(self.dir(state) / name))
                except (OSError, PoolContractError):
                    continue
                if isinstance(record, dict):
                    live[str(record.get("action_key", ""))] = (state, record)
        return live, errors

    def retention_live_rows(self) -> QueueCensus:
        """Visible READY then CLAIMED rows; stop before reading past an unknown."""
        rows: list[RecordObservation] = []
        for state in (READY, CLAIMED):
            directory = self.dir(state)
            try:
                names = sorted(os.listdir(directory))
            except OSError as exc:
                return QueueCensus(tuple(rows), (Unknown(f"{state} rows", directory, exc),))
            for name in names:
                if not name.endswith(".json") or name.startswith("."):
                    continue
                path = directory / name
                try:
                    record = read_document(path)
                except (OSError, ValueError, PoolContractError) as exc:
                    return QueueCensus(tuple(rows), (Unknown(f"{state}/{name}", path, exc),))
                if isinstance(record, dict):
                    rows.append(RecordObservation(
                        QueueAddress(path, state, name[:-len(".json")]), Present(record)))
        return QueueCensus(tuple(rows))

    def active_pair(self, key: str) -> ReadResult[str]:
        """CLAIMED then READY stat; refresh both missing names in that order."""
        for state in (CLAIMED, READY):
            path = self.item_path(state, key)
            try:
                path.stat()
            except FileNotFoundError:
                continue
            except OSError as exc:
                return Unknown(f"its {state} record could not be read: {exc}", path, exc)
            return Present(state)
        try:
            for state in (CLAIMED, READY):
                names = {path.stem for path in immutable_files.scan_optional(self.dir(state))}
                if key in names:
                    return Present(state)
        except OSError as exc:
            return Unknown(f"the queue could not be listed: {exc}", self.dir(state), exc)
        return Absent()

    def gang_member_slots(self, key: str) -> Iterator[RecordObservation]:
        """Stable strict 64 KiB slots in gang membership's original priority."""
        for state in (CLAIMED, DONE, FAILED, WITHDRAWN, READY):
            path = self.item_path(state, key)
            address = QueueAddress(path, state, key)
            try:
                raw = immutable_files.read_stable_file(
                    path, where="gang record", max_bytes=64 * 1024)
            except FileNotFoundError:
                yield RecordObservation(address, Absent())
                continue
            value = digest_primitives.decode_strict_json(raw, where="gang record")
            if not isinstance(value, dict):
                yield RecordObservation(address, Unknown("non-object-gang-record", path))
                continue
            yield RecordObservation(address, Present(value))

    def reconciliation_slots(self, key: str) -> Iterator[tuple[str, ReadResult[bool]]]:
        """Fresh parent enumeration before each conflicting slot's lstat."""
        for state in (READY, CLAIMED, DONE, WITHDRAWN):
            path = self.item_path(state, key)
            try:
                absent = immutable_files.refreshed_absence(path)
            except OSError as exc:
                result = Unknown("slot-unreadable", path, exc)
            else:
                result = Absent() if absent else Present(True)
            yield f"conflicting {state} record for this action", result
        path = self.lease_path(key)
        try:
            absent = immutable_files.refreshed_absence(path)
        except OSError as exc:
            result = Unknown("lease-unreadable", path, exc)
        else:
            result = Absent() if absent else Present(True)
        yield "action still has a lease", result

    def reconciliation_intent(self, key: str) -> tuple[JsonRecord, bytes]:
        path = self.item_path(INTENT, key)
        raw = immutable_files.read_stable_file(path, where="pool reconciliation evidence")
        value = digest_primitives.decode_strict_json(raw, where="pool reconciliation evidence")
        if not isinstance(value, dict):
            raise PoolContractError(f"evidence is not an object: {path}")
        return value, raw

    @staticmethod
    def maintenance_live_key(name: str) -> str | None:
        key = name[:64]
        return key if len(key) == 64 and set(key) <= set("0123456789abcdef") else None

    def maintenance_key_live(self, key: str, *, error_type) -> bool:
        return any(self.maintenance_live_key(entry.name) == key
                   for state in (READY, CLAIMED)
                   for entry in immutable_files.scan_custodied(
                       self.root / state,
                       error_type=error_type,
                   ))

    def maintenance_key_states(self, *, error_type) -> tuple[set[str], dict[str, float]]:
        live: set[str] = set()
        for state in (READY, CLAIMED):
            for entry in immutable_files.scan_custodied(self.root / state, error_type=error_type):
                key = self.maintenance_live_key(entry.name)
                if key is not None:
                    live.add(key)
        terminal: dict[str, float] = {}
        for state in (DONE, FAILED, WITHDRAWN):
            for entry in immutable_files.scan_custodied(self.root / state, error_type=error_type):
                key = entry.name[:-5]
                if not (entry.name.endswith(".json") and len(key) == 64
                        and set(key) <= set("0123456789abcdef")):
                    continue
                try:
                    mtime = entry.stat(follow_symlinks=False).st_mtime
                except OSError:
                    continue
                terminal[key] = max(terminal.get(key, 0.0), mtime)
        return live, terminal


    def retirement_presence_at(self, path: Path) -> ReadResult[bool]:
        """Refreshed nofollow metadata at a held canonical retirement address."""
        try:
            absent = immutable_files.confirmed_metadata_absence(path)
        except OSError as exc:
            return Unknown("retirement-slot-unreadable", path, exc)
        return Absent() if absent else Present(True)

    def retirement_candidate(self, state: str, key: str) -> object:
        raw = immutable_files.read_stable_file(
            self.item_path(state, key), where="dead-owner outcome")
        return json.loads(raw)

    def reconciliation_row_at(self, path: Path) -> tuple[JsonRecord, bytes]:
        raw = immutable_files.read_stable_file(path, where="pool reconciliation evidence")
        return _reconciliation_document(path, raw), raw

    def reconciliation_history_at(self, path: Path) -> tuple[JsonRecord, bytes]:
        raw = immutable_files.read_immutable_file(path, where="pool reconciliation evidence")
        return _reconciliation_document(path, raw), raw


    def mover_archive_coverage(self) -> ReadResult[bool]:
        """Whether every archived body is an exact active receipt's bytes.

        The only writer that may see archives without a checkpoint is one
        whose own crash landed an archive before the commit; its active
        receipt is still filed, byte for byte.  Any archive that no active
        receipt reproduces may be already-retired history, and starting from
        an empty checkpoint would silently reset it, so it refuses.  The
        listing is an explicit ``os.scandir``: ``Path.glob`` suppresses some
        listing failures, and an archive directory that cannot be read is
        unknown history, never empty history.  An entry that is not a
        plain ``<sha256>.json`` regular file is refused conservatively too.
        """

        root = self.mover_archive_dir()
        try:
            with os.scandir(root) as listing:
                entries = sorted(listing, key=lambda entry: entry.name)
        except FileNotFoundError:
            return Present(True)
        except OSError as exc:
            return Unknown(f"cannot list the retirement archives: {exc}", root)
        for entry in entries:
            if (not entry.name.endswith(".json")
                    or not key_shaped(entry.name[:-len(".json")])):
                return Unknown(f"unexpected entry in the retirement archives: {entry.name}", root)
            try:
                if not entry.is_file(follow_symlinks=False):
                    return Unknown(("an archived entry is not a regular file: "
                            f"{entry.name}"), root)
                body = Path(entry.path).read_bytes()
            except OSError as exc:
                return Unknown(f"an archived body is unreadable: {exc}", root)
            try:
                record = json.loads(body)
            except (json.JSONDecodeError, UnicodeDecodeError):
                return Unknown(f"an archived body is not JSON: {entry.name}", root)
            key = record.get("action_key") if isinstance(record, dict) else None
            if not isinstance(key, str) or not key:
                return Unknown(f"an archived body names no key: {entry.name}", root)
            try:
                active = self.move_path(key).read_bytes()
            except OSError:
                return Unknown(("an archived body is not covered by an active "
                        "receipt; retired history may be missing"), root)
            if active != body:
                return Unknown(("an archived body differs from its active receipt; "
                        "retired history may be missing"), root)
        return Present(True)


    def reader_release_event_path(self, consumer: str, host: str) -> Path:
        return self.consumer_events_dir(consumer) / f"{host}{RELEASE_EVENTS_SUFFIX}"

    @staticmethod
    def append_reader_release_line_at(path: Path, line: str) -> list[str]:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "a") as stream:
            stream.write(line + "\n")
        with open(path) as stream:
            return stream.readlines()


    def cancellation_prefix(self, wanted: str) -> list[str]:
        seen: set[str] = set()
        for state in (READY, CLAIMED, DONE, FAILED, WITHDRAWN):
            try:
                listed = immutable_files.match_required(self.dir(state), f"{wanted}*.json")
            except (FileNotFoundError, NotADirectoryError):
                continue
            seen.update(path.stem for path in listed)
        try:
            decisions = immutable_files.match_required(
                self.dir(WITHDRAWN) / "decisions", f"{wanted}*")
        except (FileNotFoundError, NotADirectoryError):
            decisions = []
        for directory in decisions:
            try:
                info = directory.stat()
            except (FileNotFoundError, NotADirectoryError):
                continue
            if not statmod.S_ISDIR(info.st_mode):
                continue
            try:
                entries = immutable_files.match_required(directory, "*.json")
            except (FileNotFoundError, NotADirectoryError):
                continue
            if entries:
                seen.add(directory.name)
        return sorted(seen)

    def wait_prefix(self, text: str) -> list[str]:
        found: set[str] = set()
        for state in (READY, CLAIMED, DONE, FAILED,
                      WITHDRAWN):
            try:
                names = os.listdir(self.dir(state))
            except FileNotFoundError:
                continue
            found.update(
                entry[: -len(".json")] for entry in names
                if entry.startswith(text) and entry.endswith(".json")
            )
        decisions = self.dir(WITHDRAWN) / "decisions"
        try:
            decision_names = os.listdir(decisions)
        except FileNotFoundError:
            decision_names = []
        for name in decision_names:
            if not name.startswith(text):
                continue
            try:
                entries = os.listdir(decisions / name)
            except FileNotFoundError:
                continue
            if any(entry.endswith(".json") for entry in entries):
                found.add(name)
        return sorted(found)

    def inspection_keys(self, state: str) -> list[str]:
        return _inspection_json_keys_at(self.dir(state))

    def inspection_prefix(self, prefix: str) -> list[str]:
        """Every action key under this prefix, across live and terminal states.

        A prefix rather than a key because that is what an operator and an agent
        both hold: ``pbstatus`` prints twelve characters and ``pbrun`` says
        ``pb-<key12>``.  An ambiguous prefix is answered with its candidates
        rather than with the first match: picking one would be a guess, and the
        caller can disambiguate with two more characters.
        """

        found: set[str] = set()
        for state in (READY, CLAIMED, DONE, FAILED, WITHDRAWN):
            found.update(key for key in self.inspection_keys(state)
                         if key.startswith(prefix))
        decisions = self.root / WITHDRAWN / "decisions"
        try:
            with os.scandir(decisions) as entries:
                found.update(entry.name for entry in entries
                             if entry.is_dir() and entry.name.startswith(prefix))
        except OSError as error:
            if not isinstance(error, (FileNotFoundError, NotADirectoryError)):
                raise
        try:
            with os.scandir(self.root / ATTEMPTS) as entries:
                found.update(entry.name for entry in entries
                             if entry.is_dir() and entry.name.startswith(prefix))
        except OSError as error:
            if not isinstance(error, (FileNotFoundError, NotADirectoryError)):
                raise
        return sorted(found)

    def recent_ending_paths(self, limit: int) -> list[os.DirEntry | HistoryEntry]:
        """The ``limit`` newest terminal records by modification time.

        Only the directory entries are stat'ed here.  Reading every record on the
        shared mount to print twenty rows is the slow path this exists to avoid,
        and an absent directory is a fleet that has not run under that transport
        rather than an error.
        """

        if self._history is not None:
            return self._kept_recent_ending_paths(limit)
        entries: list[tuple[float, os.DirEntry]] = []
        withdrawal_mtimes: dict[str, float] = {}
        for state in (DONE, FAILED, WITHDRAWN):
            directory = self.root / state
            try:
                with os.scandir(directory) as scan:
                    for entry in scan:
                        if not entry.name.endswith(".json") or not entry.is_file():
                            continue
                        entries.append((entry.stat().st_mtime, entry))
                        if state == WITHDRAWN:
                            withdrawal_mtimes[entry.name[:-5]] = entry.stat().st_mtime
            except FileNotFoundError:
                try:
                    directory.stat()
                except FileNotFoundError:
                    continue
                # A predicate would also return False on EACCES/EIO in Python 3.14.
                # Only a second ENOENT proves this optional directory is absent.
                raise  # The directory exists, but a selected entry was not readable.
        # A cancellation is durable before its visible summary is written. Keep
        # that ending visible if the operator crashed between the two writes.
        decisions = self.root / WITHDRAWN / "decisions"
        try:
            with os.scandir(decisions) as scan:
                directories = [entry for entry in scan if entry.is_dir()]
        except FileNotFoundError:
            directories = []
        for directory in directories:
            with os.scandir(directory.path) as scan:
                candidates = [(entry.stat().st_mtime, entry) for entry in scan
                              if entry.name.endswith(".json")]
            if candidates:
                newest = max(candidates, key=lambda pair: pair[0])
                if newest[0] > withdrawal_mtimes.get(directory.name, float("-inf")):
                    entries.append(newest)
        entries.sort(key=lambda pair: pair[0], reverse=True)
        return [entry for _, entry in entries[: max(0, int(limit))]]

    def selected_ending(self, entry: os.DirEntry, *, max_bytes: int) -> object:
        """Read one selected terminal record without an unbounded allocation."""

        if entry.stat().st_size > max_bytes:
            raise EndingRecordTooLarge
        path = Path(entry.path)
        if path.parent.parent.name == "decisions":
            return self.withdrawal_decision(path)
        with path.open("rb") as handle:
            raw = handle.read(max_bytes + 1)
        if len(raw) > max_bytes:
            raise EndingRecordTooLarge
        return json.loads(raw)

    def inspection_records(self, key: str) -> dict[str, JsonRecord]:
        records: dict[str, JsonRecord] = {}
        for state in (READY, CLAIMED, DONE, FAILED, WITHDRAWN):
            record = inspection_document(self.item_path(state, key))
            if record is not None:
                records[state] = record
        return records


    def inspection_slot(self, state: str, key: str) -> JsonRecord | None:
        return inspection_document(self.item_path(state, key))

    def inspection_tier_records(self) -> Iterator[tuple[str, ReadResult[JsonRecord]]]:
        directory = self.tier_directory()
        for key in _inspection_json_keys_at(directory):
            path = directory / f"{key}.json"
            try:
                record = read_document(path)
            except Exception as exc:
                result = Unknown("record-unreadable", path, exc)
            else:
                result = Absent() if record is None else Present(record)
            yield key, result

    def inspection_live_records(self, key: str) -> dict[str, JsonRecord]:
        records: dict[str, JsonRecord] = {}
        for state in (READY, CLAIMED):
            record = value_or_raise(self.item(state, key))
            if record is not None:
                records[state] = record
        return records


    @staticmethod
    def wait_terminal_at(path: Path) -> ReadResult[JsonRecord]:
        try:
            if path.name not in os.listdir(path.parent):
                return Absent()
        except OSError:
            return Absent()
        try:
            record = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return Absent()
        except (OSError, ValueError) as exc:
            reason = "unreadable" if isinstance(exc, OSError) else "not valid JSON"
            return Unknown(reason, path, exc)
        if not isinstance(record, dict):
            return Unknown("not a JSON object", path)
        return Present(record)

    def wait_ending_addresses(self, key: str) -> list[Path]:
        return [self.item_path(state, key) for state in (DONE, FAILED, WITHDRAWN)]

    @staticmethod
    def wait_notice_at(path: Path) -> object:
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return None

    def published_records(self, key: str, published_at: Path) -> Iterator[JsonRecord]:
        candidates = [published_at]
        for state in (CLAIMED, DONE, FAILED):
            candidates.append(self.item_path(state, key))
        for path in candidates:
            value = self.wait_notice_at(path)
            if isinstance(value, dict):
                yield value

    def successor_records(self, key: str) -> Iterator[JsonRecord]:
        for state in (READY, CLAIMED, DONE, FAILED, WITHDRAWN):
            value = self.wait_notice_at(self.item_path(state, key))
            if isinstance(value, dict):
                yield value

    def live_submission_records(self, key: str) -> Iterator[JsonRecord]:
        for state in (READY, CLAIMED):
            value = self.wait_notice_at(self.item_path(state, key))
            if isinstance(value, dict):
                yield value

    def finishing_submission_records(self, key: str) -> Iterator[JsonRecord]:
        try:
            names = os.listdir(self.dir(CLAIMED))
        except OSError:
            names = []
        for name in names:
            if not (name.startswith(f"{key}.")
                    and name.endswith((TOMBSTONE_SUFFIX, LATE_FINISH_SUFFIX))):
                continue
            value = self.wait_notice_at(self.dir(CLAIMED) / name)
            if isinstance(value, dict) and value.get("action_key") == key:
                yield value


    @staticmethod
    def polled_object_at(path: Path) -> JsonRecord | None:
        observation = RecordStore.wait_terminal_at(path)
        return observation.value if isinstance(observation, Present) else None

    @staticmethod
    def listed_slot(path: Path) -> bool:
        return path.name in os.listdir(path.parent)

    def sibling_ending_records(self, path: Path) -> Iterator[JsonRecord]:
        for state in (DONE, FAILED, WITHDRAWN):
            sibling = self.dir(state) / path.name
            try:
                present = self.listed_slot(sibling)
            except FileNotFoundError:
                present = False
            if not present or sibling == path:
                continue
            value = self.polled_object_at(sibling)
            if value is not None:
                yield value

    def filed_ending_index(self) -> dict[str, list[tuple[str, Path]]]:
        index: dict[str, list[tuple[str, Path]]] = {}
        for state in (DONE, FAILED, WITHDRAWN):
            directory = self.dir(state)
            try:
                names = os.listdir(directory)
            except OSError:
                continue
            for name in names:
                if name.endswith(".json"):
                    index.setdefault(name[:-len(".json")], []).append(
                        (state, directory / name))
        return index

    @staticmethod
    def lane_unreadable_dir(path: Path) -> Path:
        return path.parent / "unreadable"


    @staticmethod
    def text_document_at(path: Path) -> object:
        """Required default-text JSON; preserve its BOM/Unicode/error profile."""
        return json.loads(path.read_text())

    def text_item(self, state: str, key: str) -> object:
        return self.text_document_at(self.item_path(state, key))

    def text_lease(self, key: str) -> object:
        return self.text_document_at(self.lease_path(key))

    def item_metadata(self, state: str, key: str) -> os.stat_result:
        return self.item_path(state, key).stat()

    def lease_file_metadata(self, key: str) -> os.stat_result:
        return self.lease_path(key).lstat()

    def claim_occupancy(self, key: str) -> ReadResult[bool]:
        for state in (READY, CLAIMED):
            path = self.item_path(state, key)
            try:
                path.lstat()
            except FileNotFoundError:
                continue
            except OSError as exc:
                return Unknown("occupancy-unreadable", path, exc)
            return Present(True)
        directory = self.dir(CLAIMED)
        try:
            names = os.listdir(directory)
        except FileNotFoundError:
            return Absent()
        except OSError as exc:
            return Unknown("occupancy-unreadable", directory, exc)
        occupied = any(name.startswith(f"{key}.")
                       and name.endswith((TOMBSTONE_SUFFIX, LATE_FINISH_SUFFIX))
                       for name in names)
        return Present(True) if occupied else Absent()

    def successor_document(self, key: str) -> ReadResult[object]:
        path = self.item_path(READY, key)
        try:
            raw = path.read_text()
        except FileNotFoundError:
            return Absent()
        except OSError as exc:
            return Unknown("successor-unreadable", path, exc)
        try:
            return Present(json.loads(raw))
        except ValueError as exc:
            return Unknown("successor-corrupt", path, exc)

    def membership_claims(self) -> Iterator[RecordObservation]:
        directory = self.dir(CLAIMED)
        if not directory.is_dir():
            yield RecordObservation(QueueAddress(directory, CLAIMED, None),
                                    Unknown("claimed-directory-unreadable", directory))
            return
        try:
            with os.scandir(directory) as entries:
                names = sorted(entry.name for entry in entries
                               if entry.name.endswith(".json"))
        except OSError as exc:
            yield RecordObservation(QueueAddress(directory, CLAIMED, None),
                                    Unknown("claimed-directory-unlistable", directory, exc))
            return
        for name in names:
            path = directory / name
            try:
                value = self.text_document_at(path)
            except (OSError, ValueError) as exc:
                result = Unknown("record-unreadable", path, exc)
            else:
                result = (Present(value) if isinstance(value, dict)
                          else Unknown("non-object-record", path))
            yield RecordObservation(QueueAddress(path, CLAIMED, name[:-5]), result)


    def claimed_names(self) -> ClaimedNames:
        names = os.listdir(self.dir(CLAIMED))
        return ClaimedNames(frozenset(names), tuple(
            name for name in names
            if name.endswith((TOMBSTONE_SUFFIX, LATE_FINISH_SUFFIX))))

    def claim_blocked(self, key: str) -> bool:
        return _claimed_name_blocks(os.listdir(self.dir(CLAIMED)), key)

    def require_terminal_directory(self, state: str) -> None:
        directory = self.dir(state)
        if not statmod.S_ISDIR(os.stat(directory).st_mode):
            raise NotADirectoryError(
                errno.ENOTDIR, "terminal directory is not a directory", str(directory))

    def item_entry_present(self, state: str, key: str) -> bool:
        try:
            os.lstat(self.item_path(state, key))
        except FileNotFoundError:
            return False
        return True

    def required_slot_paths(self, state: str) -> list[Path]:
        with os.scandir(self.dir(state)) as entries:
            return sorted(Path(entry.path) for entry in entries
                          if entry.name.endswith(".json"))

    def host_event_writers(self) -> list[str]:
        return [name[:-len(".jsonl")]
                for name in sorted(os.listdir(self.host_events_dir()))
                if name.endswith(".jsonl")]

    def movement_metadata(self, key: str) -> os.stat_result:
        return os.stat(self.move_path(key))

    def superseded_records(self, action_key: str, *, fresh: bool = False
                           ) -> Iterator[tuple[Path, object]]:
        directory = self.superseded_dir()
        if fresh:
            paths = [directory / name for name in os.listdir(directory)
                     if fnmatch.fnmatchcase(name, f"{action_key}.*.json")]
        else:
            paths = immutable_files.match_optional(directory, f"{action_key}.*.json")
        read_record = read_confirmed_document if fresh else read_document
        for path in paths:
            yield path, read_record(path)

    def consumer_event_records(self, action_key: str) -> list[JsonRecord]:
        events: list[dict[str, object]] = []
        try:
            names = sorted(os.listdir(self.consumer_events_dir(action_key)))
        except OSError:
            return events
        for name in names:
            if not name.endswith(".jsonl"):
                continue
            try:
                text = (self.consumer_events_dir(action_key) / name).read_text()
            except OSError:
                continue
            for line in text.splitlines():
                try:
                    value = json.loads(line)
                except ValueError:
                    continue
                if isinstance(value, dict):
                    events.append(value)
        return events

    def host_event_records(self, host: str | None = None) -> list[JsonRecord]:
        events: list[dict[str, object]] = []
        directory = self.host_events_dir()
        if host is None:
            try:
                names = sorted(os.listdir(directory))
            except OSError:
                return events
        else:
            names = [f"{host}.jsonl"]
        for name in names:
            if not name.endswith(".jsonl"):
                continue
            try:
                text = (directory / name).read_text()
            except OSError:
                continue
            for line in text.splitlines():
                try:
                    value = json.loads(line)
                except ValueError:
                    continue
                if isinstance(value, dict):
                    events.append(value)
        return events

    def claimed_finish_marked(self, child_key: str) -> bool:
        """Whether a finish mark holds this key's claim aside (#1666)."""

        prefix = f"{child_key}."
        try:
            names = os.listdir(self.dir(CLAIMED))
        except OSError:
            return False
        return any(name.startswith(prefix)
                   and name.endswith((TOMBSTONE_SUFFIX, LATE_FINISH_SUFFIX))
                   for name in names)


    def state_directory_present(self, state: str) -> bool:
        return self.dir(state).is_dir()

    def json_slot_paths(self, state: str) -> list[Path]:
        return sorted(self.dir(state).glob("*.json"))

    def optional_claim_paths(self) -> list[Path]:
        return immutable_files.match_optional(self.dir(CLAIMED), "*.json")

    def claimed_lease_paths(self) -> list[Path]:
        return sorted(self.dir(CLAIMED).glob("*.lease"))

    def finishing_paths(self) -> list[Path]:
        directory = self.dir(CLAIMED)
        return sorted([*directory.glob(f"*{TOMBSTONE_SUFFIX}"),
                       *directory.glob(f"*{LATE_FINISH_SUFFIX}")])

    def listed_item_present(self, state: str, key: str) -> bool:
        return f"{key}.json" in os.listdir(self.dir(state))

    @staticmethod
    def raw_record_at(path: Path) -> bytes:
        """Whole historical bytes at a captured or canonical Store address."""
        return path.read_bytes()

    def attachment_item_address(self, key: str) -> Path:
        """Best-effort display address, preserving its historical optional hint."""
        ready = self.item_path(READY, key)
        return ready if ready.exists() else self.item_path(CLAIMED, key)

    def released_origin_index_names(self, consumer: str) -> list[str]:
        return sorted(name for name in os.listdir(self.released_origin_consumers_dir())
                      if name.startswith(f"{consumer}.") and name.endswith(".json"))

    def required_plan_paths(self) -> list[Path]:
        with os.scandir(self.root / RESIDENCY_PLANS) as entries:
            return sorted(Path(entry.path) for entry in entries
                          if entry.name.endswith(".json"))

    def terminal_keys(self) -> frozenset[str]:
        """Every action key with a worker-filed outcome.

        List rather than ``stat`` for the same NFS reason as
        :meth:`withdrawn_keys`: a negatively cached absence must not make a
        worker execute a ready copy after another box has filed its outcome.
        The record's generation is checked separately, so an old outcome does
        not blacklist this content-addressed name.

        **Loud on anything but absence.**  This set is the evidence
        ``terminal_outcome_covers`` decides on, and every ``OSError`` used to
        answer it the same way an empty directory does.  ``ESTALE`` on a
        cached directory handle is the ordinary way a listing fails on this
        mount -- ``record_store.read_document`` treats it as a first-class event (#208) and
        ``quarantine_orphans`` re-raises every errno that is not ``ESTALE``
        rather than swallowing the class -- so one stale handle reported "no
        outcomes have been filed" for a queue full of them.  ``reap_stale``
        then found no filed outcome for a generation that had one and put it
        back in ``ready``, which is the one thing this method's own caller
        says a CAS hit does not license.  ``record_store.read_document`` states the rule:
        answering "absent" without the evidence that the directory is live
        "would turn a broken mount into a confident wrong verdict".
        """

        keys: set[str] = set()
        for state in (DONE, FAILED):
            try:
                names = os.listdir(self.dir(state))
            except FileNotFoundError:
                # Absence only.  A queue whose layout has not been created yet
                # legitimately has no ``done`` and no ``failed``, and that is
                # the one reading of "no names" this method may make.
                continue
            keys.update(
                name[: -len(".json")] for name in names if name.endswith(".json")
            )
        return frozenset(keys)
    def released_origin_consumer_keys(self) -> frozenset[str]:
        """Every consumer key with an entry in the release index (#954).

        The claim's placement bypass asks this once per scan, so a released
        key reaches the lock on a box that cannot place the row and any box
        may file the refusal.  It is deliberately **not** the refusal's
        authority: a release that lands after this listing would be missed,
        so :meth:`_refuse_released_origin_consumer` lists the directory
        fresh under the key's transition lock (#964).  An entry is a lead,
        never a release: ``produced_output.origin_consumer_release``
        confirms it against the release record it names, and an unreadable
        listing or entry is unknown, not absence.
        """

        try:
            names = os.listdir(self.released_origin_consumers_dir())
        except FileNotFoundError:
            return frozenset()
        return frozenset(name[:64] for name in names
                         if name.endswith(".json") and name[64:65] == ".")
    def output_funding_bytes(
            self, mover_action_key: str, tier_id: str, *,
            max_bytes: int | None) -> bytes | None:
        """Read live or ENOENT-selected retired bytes; None proves absence."""

        try:
            try:
                handle = open(self.funding_output_path(
                    mover_action_key, tier_id), "rb")
            except FileNotFoundError:
                # Retirement moves terminal funding out of the census, not
                # out of the address used by per-mover decisions.
                handle = open(self.funding_output_retired_path(
                    mover_action_key, tier_id), "rb")
            with handle:
                return (handle.read() if max_bytes is None
                        else handle.read(max_bytes + 1))
        except FileNotFoundError:
            return None


    @classmethod
    def kept(cls, root: str | Path, *, records: kept_reads.DirectoryRecords,
             history: kept_reads.DirectoryCaptures | None = None):
        store = cls(root)
        store._records = records
        store._history = (kept_reads.DirectoryCaptures(records.trust)
                          if history is None else history)
        return store

    def state_records(self, state: str) -> list[tuple[Path, JsonRecord | None]]:
        """Every state entry in name order, with independent mutable records."""
        directory = self.dir(state)
        if self._records is None:
            return [(path, read_document(path))
                    for path in immutable_files.scan_optional(directory)]
        return self._records.read(directory, select=_all_entries,
                                  parse=_document_bytes, thaw=_document_from_bytes)

    def receipt_records_at(self, directory: Path, *, checkpoint=None,
                           readers: int = 1) -> list[tuple[Path, JsonRecord | None]]:
        """Visible receipt names at a domain-held receipt directory."""
        if self._records is None:
            return [(path, read_document(path))
                    for path in immutable_files.scan_optional(directory)
                    if _visible_json_entry(path)]
        return self._records.read(
            directory, select=_visible_json_entry, parse=_frozen_document,
            thaw=_thawed_document, checkpoint=checkpoint, readers=readers)

    def retirement_documents(self) -> list[tuple[Path, JsonRecord | None]]:
        directory = self.root / MOVERS_RETIRED
        if self._records is None:
            return [(path, read_document(path))
                    for path in immutable_files.scan_optional(directory)
                    if _retirement_entry(path)]
        return self._records.read(directory, select=_retirement_entry,
                                  parse=_frozen_document, thaw=_thawed_document)

    def diagnostic_records_at(self, directory: Path
                              ) -> list[tuple[Path, DiagnosticRecord]]:
        """Every .json slot, retaining each original read failure."""
        if self._records is None:
            return [(path, _diagnostic_queue_record(path))
                    for path in immutable_files.scan_optional(directory)
                    if _json_entry(path)]
        return self._records.read(
            directory, select=_json_entry, parse=_diagnostic_queue_record,
            keep=_diagnostic_kept)

    def diagnostic_json_at(self, directory: Path
                           ) -> list[tuple[Path, DiagnosticRecord]]:
        """Visible queue metadata objects, retaining original read failures."""
        if self._records is None:
            return [(path, _diagnostic_json_record(path))
                    for path in immutable_files.scan_optional(directory)
                    if _visible_json_entry(path)]
        return self._records.read(
            directory, select=_visible_json_entry, parse=_diagnostic_json_record,
            keep=_diagnostic_kept)

    def _residency_supersession_names(self) -> frozenset[str]:
        directory = self.root / RESIDENCY_PLANS / "superseded"
        if self._records is None:
            return frozenset(os.listdir(directory))
        return self._records.names(directory, select=_all_entries)


    def withdrawn_keys(self) -> frozenset[str]:
        directory = self.dir(WITHDRAWN)
        if self._records is None:
            try:
                names = os.listdir(directory)
            except FileNotFoundError:
                return frozenset()
        else:
            names = self._records.names(directory, select=_json_name)
        return frozenset(name[:-5] for name in names if name.endswith(".json"))


    def _take_history(self, directory: Path, select, *, absent_ok: bool = True):
        history = (kept_reads.DirectoryCaptures() if self._history is None
                   else self._history)
        with history.acquire_files(directory) as capture:
            if capture.hit:
                self.history_kept += 1
                return capture.value.values()
            previous = capture.previous
            previous = {} if previous is kept_reads.NoCapture else previous
            complete = True
            entries: dict[str, HistoryEntry] = {}
            stat = os.stat
            try:
                with os.scandir(directory) as scan:
                    for entry in scan:
                        if not select(entry):
                            continue
                        info = stat(entry.path)
                        version = capture.file_version(info, path=entry.path)
                        prior = previous.get(entry.name)
                        if version is None or prior is None or prior.version != version:
                            prior = HistoryEntry.observed(entry.path, entry.name, info, version)
                            complete = complete and version is not None
                        entries[entry.name] = prior
            except FileNotFoundError:
                try:
                    os.stat(directory)
                except FileNotFoundError:
                    if absent_ok:
                        return ()
                raise
            self.history_listed += 1
            capture.commit(entries, complete=complete)
            return entries.values()

    def ending_entries(self, state: str):
        return self._history_at(self.dir(state), _json_file_entry)

    def withdrawal_directories(self):
        return self._history_at(self.dir(WITHDRAWN) / 'decisions', _directory_entry)

    def withdrawal_entries(self, directory: HistoryEntry):
        return self._history_at(Path(directory.path), _json_entry, absent_ok=False)

    def mover_entries(self):
        return self._history_at(self.root / MOVERS, _json_entry)

    def plan_entries(self):
        records = (kept_reads.DirectoryRecords() if self._records is None
                   else self._records)
        return records.read(
            self.root / RESIDENCY_PLANS, select=_json_entry, parse=_history_entry,
            keep=_history_entry_kept, stat_parse=True)

    def tier_directories(self):
        records = (kept_reads.DirectoryRecords() if self._records is None
                   else self._records)
        return [path for path, _ in records.read(
            self.root / TIER_RESERVATIONS, select=_visible_directory_entry,
            parse=_listed)]

    @staticmethod
    def diagnostic_sidecar_at(path: Path) -> DiagnosticRecord:
        return _diagnostic_json_record(path)

    def released_claim_filings(self) -> list[tuple[float, str]]:
        directory = self.superseded_dir()
        records = (kept_reads.DirectoryRecords() if self._records is None
                   else self._records)
        names = records.names(directory, select=_RELEASE_FILING.match)
        return [(float(_RELEASE_FILING.match(name).group('when')),
                 os.path.join(directory, name)) for name in names]


    def begin_read(self) -> None:
        self._history_once = {}

    def end_read(self) -> None:
        self._history_once = None

    def history_directories(self) -> set[str]:
        return set(self._history_once or ())

    def _history_at(self, directory: Path, select, *, absent_ok: bool = True):
        name = os.fspath(directory)
        done = None if self._history_once is None else self._history_once.get(name)
        if done is not None:
            if done[0] is not select:
                raise AssertionError(f"{name} read under two meanings")
            return done[1]
        out = self._take_history(directory, select, absent_ok=absent_ok)
        if self._history_once is not None:
            self._history_once[name] = (select, out)
        return out

    def ending_fences(self) -> kept_reads.EvidenceSet | None:
        directories = [self.dir(state) for state in (DONE, FAILED, WITHDRAWN)]
        decisions = self.dir(WITHDRAWN) / 'decisions'
        trust = kept_reads.filesystem_trust if self._records is None else self._records.trust
        taken = tuple(trust.ancestor_evidence(path) for path in [*directories, decisions])
        if any(fence is None for fence in taken):
            return None
        subdirectories = tuple(trust.ancestor_evidence(Path(entry.path))
                               for entry in self.withdrawal_directories())
        if any(fence is None for fence in subdirectories):
            return None
        return kept_reads.EvidenceSet((*taken, *subdirectories))

    def _kept_recent_ending_paths(self, limit: int) -> list[HistoryEntry]:
        entries: list[HistoryEntry] = []
        withdrawal_mtimes: dict[str, float] = {}
        for state in (DONE, FAILED, WITHDRAWN):
            listed = self.ending_entries(state)
            entries.extend(listed)
            if state == WITHDRAWN:
                for entry in listed:
                    withdrawal_mtimes[entry.name[:-5]] = entry.mtime
        for directory in list(self.withdrawal_directories()):
            candidates = list(self.withdrawal_entries(directory))
            if candidates:
                newest = max(candidates, key=_mtime)
                if newest.mtime > withdrawal_mtimes.get(directory.name, float('-inf')):
                    entries.append(newest)
        return heapq.nlargest(max(0, int(limit)), entries, key=_mtime)

    def required_diagnostic_records_at(self, directory: Path):
        with os.scandir(directory) as entries:
            paths = sorted(Path(entry.path) for entry in entries if entry.name.endswith('.json'))
        return [(path, _diagnostic_queue_record(path)) for path in paths]


    def residency_supersession_keys(self) -> frozenset[str] | None:
        try:
            names = self._residency_supersession_names()
        except FileNotFoundError:
            return frozenset()
        except OSError:
            return None
        keys: set[str] = set()
        for name in names:
            key = _active_marker_key(name)
            if key is not None:
                keys.add(key)
            elif not _archived_supersession_name(name):
                return None
        return frozenset(keys)


    def claim_denial_hosts_at(self, directory: Path) -> list[Path]:
        """Host directories at the domain-held reservation-ledger root."""
        records = (kept_reads.DirectoryRecords() if self._records is None
                   else self._records)
        return [path for path, _ in records.read(
            directory, select=_directory_entry, parse=_listed)]


def read_document(path: Path) -> dict[str, object] | None:
    """Read a permissive JSON object; missing/empty is absent and errors escape."""
    return _read_json(path)


def value_or_raise[T](result: ReadResult[T]) -> T | None:
    """Adapt an observation to an existing value-or-error public contract."""
    if isinstance(result, Unknown):
        if result.error is not None:
            raise result.error
        raise PoolContractError(result.reason)
    return None if isinstance(result, Absent) else result.value


def pricing_line_name(line: bytes) -> str | None:
    """The receipt name a log line is about, even when its body is torn."""

    parts = line.split(b"\t", 2)
    if len(parts) < 2 or parts[0] != MOVE_PRICING_LOG_TAG:
        return None
    try:
        return parts[1].decode()
    except UnicodeDecodeError:
        return None


def pricing_unknown_line(name: str) -> bytes:
    """A line that makes ``name`` unknown to the log until a later line.

    Appended before a receipt is re-filed under its own name, so a log that
    loses the new receipt's line -- a crash between the rename and the
    append, a failed append -- reads that name from the receipt rather than
    from its older line.
    """

    return b"\t".join((MOVE_PRICING_LOG_TAG, name.encode(), b"")) + b"\n"


def pricing_line(name: str, record: Mapping[str, object]) -> bytes:
    body = digest_primitives._canonical_bytes(record)
    return b"\t".join((MOVE_PRICING_LOG_TAG, name.encode(), body)) + b"\n"


def event_line_count(path: Path) -> int:
    try:
        with open(path) as stream:
            return sum(1 for _line in stream)
    except FileNotFoundError:
        return 0


def append_event_record(path: Path, record: Mapping[str, object]) -> None:
    with open(path, "a") as stream:
        stream.write(json.dumps(record, default=str) + "\n")


def replace_event_lines(path: Path, lines: list[str]) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with open(temporary, "w") as stream:
        stream.writelines(lines)
    os.replace(temporary, path)


def replace_event_tail(path: Path, limit: int) -> int:
    with open(path) as stream:
        kept = stream.readlines()[-limit:]
    replace_event_lines(path, kept)
    return len(kept)


def _reconciliation_document(path: Path, raw: bytes) -> JsonRecord:
    value = digest_primitives.decode_strict_json(raw, where="pool reconciliation evidence")
    if not isinstance(value, dict):
        raise PoolContractError(f"evidence is not an object: {path}")
    return value


def key_shaped(value: object) -> bool:
    return (isinstance(value, str) and len(value) == 64
            and all(character in "0123456789abcdef" for character in value))


#: That event's file, ``residency-events/<consumer>/<host>`` + this: beside
#: the tier loop's ``<host>.jsonl`` rather than in it, because that file has
#: one writer, the host's tier loop, and a reader is not it.
RELEASE_EVENTS_SUFFIX = "-reader-release.jsonl"


class EndingRecordTooLarge(ValueError):
    pass


def inspection_document(path: Path) -> dict | None:
    try:
        return read_document(path)
    except OSError as error:
        if isinstance(error, (FileNotFoundError, NotADirectoryError)):
            return None
        raise
    except (ValueError, PoolContractError):
        return None


def _claimed_name_blocks(names: Iterable[str], key: str) -> bool:
    """Whether a listing of ``claimed/`` refuses a claim of ``key``.

    Its claim record, or a finish mark for it: a tombstone or a late-finish
    record, whose names start ``<key>.`` and end in their suffix.
    """

    record = f"{key}.json"
    prefix = f"{key}."
    return any(name == record
               or (name.startswith(prefix)
                   and name.endswith((TOMBSTONE_SUFFIX, LATE_FINISH_SUFFIX)))
               for name in names)


@dataclass(frozen=True)
class ClaimedNames:
    names: frozenset[str]
    finishes: tuple[str, ...]

    def blocks(self, key: str) -> bool:
        return f"{key}.json" in self.names or _claimed_name_blocks(self.finishes, key)


def _inspection_json_keys_at(directory: Path) -> list[str]:
    try:
        with os.scandir(directory) as entries:
            return sorted(entry.name[:-5] for entry in entries
                          if entry.name.endswith(".json") and entry.is_file())
    except OSError as error:
        if isinstance(error, (FileNotFoundError, NotADirectoryError)):
            return []
        raise


# Source proposal. No product imports or runtime execution on coordinator.
@dataclass(frozen=True)
class _JsonObject:
    members: tuple[tuple[str, object], ...]


@dataclass(frozen=True)
class _JsonArray:
    members: tuple[object, ...]


def _freeze_json(value: object) -> object:
    """Freeze parsed JSON without adding a lower recursion limit."""
    frozen: dict[int, object] = {}
    pending = [(value, False)]
    while pending:
        current, ready = pending.pop()
        if not isinstance(current, (dict, list)):
            continue
        if ready:
            if isinstance(current, dict):
                frozen[id(current)] = _JsonObject(tuple(
                    (key, frozen.get(id(child), child))
                    for key, child in current.items()))
            else:
                frozen[id(current)] = _JsonArray(tuple(
                    frozen.get(id(child), child) for child in current))
        else:
            pending.append((current, True))
            children = current.values() if isinstance(current, dict) else current
            pending.extend((child, False) for child in reversed(list(children)))
    return frozen.get(id(value), value)


def _thaw_json(value: object) -> object:
    """Copy frozen or parsed JSON containers without a lower recursion limit."""
    objects = (_JsonObject, dict)
    arrays = (_JsonArray, list)
    if isinstance(value, objects):
        root = {}
    elif isinstance(value, arrays):
        root = []
    else:
        return value
    pending = [(value, root)]
    while pending:
        current, result = pending.pop()
        if isinstance(current, _JsonObject):
            children = current.members
        elif isinstance(current, dict):
            children = current.items()
        else:
            children = enumerate(current.members if isinstance(current, _JsonArray) else current)
        is_array = isinstance(result, list)
        for key, child in children:
            if isinstance(child, objects):
                out = {}
            elif isinstance(child, arrays):
                out = []
            else:
                out = child
            if is_array:
                result.append(out)
            else:
                result[key] = out
            if out is not child:
                pending.append((child, out))
    return root


def independent_document(record: JsonRecord) -> JsonRecord:
    """Detach every JSON container without imposing a lower recursion limit."""
    return _thaw_json(record)


@dataclass(frozen=True)
class DiagnosticRecord:
    _value: object
    error: Exception | None = None

    @property
    def record(self) -> JsonRecord | None:
        return _thaw_json(self._value)


def _diagnostic_queue_record(path: Path) -> DiagnosticRecord:
    try:
        if not statmod.S_ISREG(path.lstat().st_mode):
            raise ValueError("not a regular queue record")
        record = read_document(path)
        if record is None:
            raise ValueError("record disappeared or is empty")
        return DiagnosticRecord(_freeze_json(record))
    except (OSError, ValueError) as exc:
        return DiagnosticRecord(None, exc)


def _diagnostic_json_record(path: Path) -> DiagnosticRecord:
    try:
        return DiagnosticRecord(_freeze_json(read_document(path)))
    except (OSError, ValueError) as exc:
        return DiagnosticRecord(None, exc)


def _diagnostic_kept(parsed: DiagnosticRecord) -> bool:
    return parsed.error is None


def _document_bytes(path: Path) -> bytes | None:
    try:
        return path.read_bytes()
    except FileNotFoundError:
        return None


def _document_from_bytes(path: Path, raw: bytes | None) -> JsonRecord | None:
    if not raw:
        return None
    try:
        value = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise PoolContractError(f"queue record is not valid JSON: {path}") from exc
    if not isinstance(value, dict):
        raise PoolContractError(f"queue record is not an object: {path}")
    return value


def _frozen_document(path: Path) -> object:
    return _freeze_json(read_document(path))


def _thawed_document(_path: Path, value: object) -> JsonRecord | None:
    return _thaw_json(value)


def _all_entries(_entry) -> bool:
    return True


def _json_entry(entry) -> bool:
    return entry.name.endswith(".json")


def _visible_json_entry(entry) -> bool:
    return entry.name.endswith(".json") and not entry.name.startswith(".")


def _retirement_entry(entry) -> bool:
    return entry.name in (MOVERS_RETIREMENT_CHECKPOINT, MOVERS_RETIREMENT_STATE)


def _json_name(name: str) -> bool:
    return name.endswith(".json")


@dataclass(frozen=True)
class _EntryStat:
    st_mtime: float
    st_size: int


@dataclass(frozen=True)
class HistoryEntry:
    path: str
    name: str
    mtime: float
    version: tuple | None
    _stat: _EntryStat

    @classmethod
    def observed(cls, path: str, name: str, info: os.stat_result, version: tuple | None):
        return cls(path, name, info.st_mtime, version,
                   _EntryStat(info.st_mtime, info.st_size))

    def stat(self) -> _EntryStat:
        return self._stat


def _history_entry(path: Path, info: os.stat_result | None) -> HistoryEntry | None:
    if info is None:
        return None
    return HistoryEntry.observed(str(path), path.name, info,
                                 kept_reads.metadata_version(info))


def _history_entry_kept(entry: HistoryEntry | None) -> bool:
    return entry is not None


def _json_file_entry(entry) -> bool:
    return entry.name.endswith('.json') and entry.is_file()


def _directory_entry(entry) -> bool:
    return entry.is_dir()


def _visible_directory_entry(entry) -> bool:
    return entry.is_dir() and not entry.name.startswith('.')


def _mtime(entry: HistoryEntry) -> float:
    return entry.mtime


def _listed(_path: Path) -> bool:
    return True


_RELEASE_FILING = re.compile(
    r'\A(?P<key>[0-9a-f]{64})\.(?P<when>[0-9]+\.[0-9]{6})\.unstarted-claim\.json\Z')


_SUPERSEDED_MARKER_SUFFIX = ".superseded.json"

#: The characters a reaped plan body's reason slug is built from
#: (``residency_plan._retired_slug``).
_SLUG_CHARACTERS = frozenset(
    "abcdefghijklmnopqrstuvwxyz0123456789-")


def _active_marker_key(name: str) -> str | None:
    """The consumer key an active supersession marker name carries, or ``None``.

    The address is ``{key}.{plan_sha256}.superseded.json``, both 64 lowercase
    hex characters.  Any other name is not an active marker; the caller tells
    a known archive name from an unrecognized one.
    """

    if not name.endswith(_SUPERSEDED_MARKER_SUFFIX):
        return None
    stem = name[:-len(_SUPERSEDED_MARKER_SUFFIX)]
    key, separator, digest = stem.partition(".")
    if not separator or "." in digest:
        return None
    if not key_shaped(key) or not key_shaped(digest):
        return None
    return key


def _archived_supersession_name(name: str) -> bool:
    """Whether one marker-directory name is a known archive entry.

    ``residency_plan`` archives a reaped plan body as
    ``{key}.{seconds}.{micros}.{reason-slug}.json`` and retires an active
    marker beside it as
    ``{key}.{digest}.{filing}.{seconds}.{micros}.marker.json``; a
    publication temporary is ``.{name}.{random}.tmp``.  None of these can be
    the address an active marker is read at, so a directory holding them
    still answers which keys have one.  Anything else is unrecognized, and
    the caller treats the whole listing as unknown.
    """

    if name.startswith("."):
        return True
    if name.endswith(".marker.json"):
        parts = name[:-len(".marker.json")].split(".")
        return (len(parts) == 5 and key_shaped(parts[0]) and key_shaped(parts[1])
                and all(part.isdigit() for part in parts[2:]))
    if name.endswith(".json"):
        parts = name[:-len(".json")].split(".")
        return (len(parts) == 4 and key_shaped(parts[0])
                and parts[1].isdigit() and parts[2].isdigit()
                and bool(parts[3])
                and all(character in _SLUG_CHARACTERS
                        for character in parts[3]))
    return False
