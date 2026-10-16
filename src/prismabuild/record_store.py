"""Read-only questions about records filed under a queue key.

Queue names, JSON intake and named observations live here. Domain callers
retain scheduling, lifecycle and retention authority. Reads take no lock
and impose no deadline.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
import errno
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

    def tier_record_path(self, tier_id: str) -> Path:
        return self.root / TIERS / f"{self.check_tier_id(tier_id)}.json"

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
        identity = digest_primitives.canonical_sha256({field: record.get(field) for field in _CLAIM_IDENTITY})
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

    def confirmed_item(self, state: str, key: str, *, max_bytes: int | None = None) -> ReadResult[JsonRecord]:
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
