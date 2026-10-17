"""Captured action facts and the owner of durable conclusion transitions.

Facts describe observations, never an atomic key-wide state. Existing records
are the recovery protocol. No lifecycle journal or on-disk schema is added.
"""
from __future__ import annotations

from collections.abc import Mapping
from contextlib import suppress
from dataclasses import dataclass
from enum import Enum
import errno
import os
from pathlib import Path
import re
import socket
import stat
import sys
import uuid

from . import claim_identity, core as pb, endings, immutable_files, local_scratch, record_store, scratch_lifetime
from .record_store import (
    Absent, AmbiguousClaimHolder, CLAIMED, DONE, FAILED, LATE_FINISH_SUFFIX,
    PoolContractError, Present, READY, TOMBSTONE_SUFFIX, Unknown, WITHDRAWN,
)


class NotCaptured(Enum):
    INPUT = "not_captured"


class ClaimRelation(Enum):
    UNOBSERVED = "unobserved"
    UNKNOWN = "unknown"
    ABSENT = "absent"
    HELD = "held"
    EXACT = "exact"
    SUCCESSOR = "successor"


class ReservationPhase(Enum):
    ACQUIRING = "acquiring"
    COMMITTED = "committed"


@dataclass(frozen=True)
class ReservationFact:
    host: str
    phase: ReservationPhase
    claim: Mapping[str, object] | None = None


@dataclass(frozen=True)
class RecordFact:
    kind: str
    record: Mapping[str, object]
    path: Path | None = None

    @property
    def generation(self) -> object:
        return self.record.get("published_unix")


@dataclass(frozen=True)
class ActionCapture:
    publication: Present | Absent | Unknown | NotCaptured = NotCaptured.INPUT
    claim: Present | Absent | Unknown | NotCaptured = NotCaptured.INPUT
    lease: Present | Absent | Unknown | NotCaptured = NotCaptured.INPUT
    reservations: tuple[ReservationFact, ...] | NotCaptured = NotCaptured.INPUT
    terminals: record_store.TerminalCapture | NotCaptured = NotCaptured.INPUT
    artifacts: tuple[RecordFact, ...] = ()
    expected_claim: Mapping[str, object] | None = None


@dataclass(frozen=True)
class ActionFacts:
    records: tuple[RecordFact, ...]
    claim: ClaimRelation
    reservations: tuple[ReservationFact, ...] | NotCaptured
    terminal: Mapping[str, object] | NotCaptured
    unknown: tuple[str, ...]
    uncaptured: tuple[str, ...]

    def for_generation(self, generation: object) -> tuple[RecordFact, ...]:
        return tuple(fact for fact in self.records if fact.generation == generation)


def classify(capture: ActionCapture) -> ActionFacts:
    """Classify supplied facts without reading or deciding a terminal twice."""
    records = list(capture.artifacts)
    unknown, uncaptured = [], []
    relation = ClaimRelation.UNOBSERVED
    for kind in ("publication", "claim", "lease"):
        observed = getattr(capture, kind)
        if isinstance(observed, NotCaptured):
            uncaptured.append(kind)
        elif isinstance(observed, Unknown):
            unknown.append(kind)
            if kind == "claim":
                relation = ClaimRelation.UNKNOWN
        elif isinstance(observed, Present):
            records.append(RecordFact(kind, observed.value))
            if kind == "claim":
                relation = (ClaimRelation.HELD if capture.expected_claim is None else
                            ClaimRelation.EXACT if claim_identity.same_claim(
                                observed.value, capture.expected_claim) else ClaimRelation.SUCCESSOR)
        elif kind == "claim":
            relation = ClaimRelation.ABSENT
    terminal = NotCaptured.INPUT
    if isinstance(capture.terminals, NotCaptured):
        uncaptured.append("terminals")
    else:
        terminal = endings.resolve_current(capture.terminals)
        for candidate in capture.terminals.candidates:
            if isinstance(candidate.result, Present):
                records.append(RecordFact(candidate.address.state, candidate.result.value, candidate.address.path))
            elif isinstance(candidate.result, Unknown):
                unknown.append(candidate.address.state)
    if isinstance(capture.reservations, NotCaptured):
        uncaptured.append("reservations")
    return ActionFacts(tuple(records), relation, capture.reservations, terminal,
                       tuple(unknown), tuple(uncaptured))


class Transition(Enum):
    FINISH = "finish"
    EXPIRE = "expire"
    RELEASE_UNSTARTED = "release_unstarted"
    CANCEL = "cancel"
    LATE = "late"
    ADOPT_TERMINAL = "adopt_terminal"
    RESTORE_TOMBSTONE = "restore_tombstone"
    RETIRE_TOMBSTONE = "retire_tombstone"
    QUARANTINE = "quarantine"
    DEFER = "defer"


LEGAL_DESTINATIONS = {
    Transition.FINISH: frozenset({DONE, FAILED, READY}),
    Transition.EXPIRE: frozenset({DONE, FAILED, READY}),
    Transition.RELEASE_UNSTARTED: frozenset({READY}),
    Transition.CANCEL: frozenset({WITHDRAWN}),
    Transition.LATE: frozenset({"attempt", "evidence"}),
    Transition.ADOPT_TERMINAL: frozenset({"existing"}),
    Transition.RESTORE_TOMBSTONE: frozenset({CLAIMED}),
    Transition.RETIRE_TOMBSTONE: frozenset({"evidence"}),
    Transition.QUARANTINE: frozenset({FAILED, "existing"}),
    Transition.DEFER: frozenset({READY}),
}


@dataclass(frozen=True)
class WorkerFinish:
    action_key: str
    status: str
    detail: Mapping[str, object] | None = None
    claim_snapshot: Mapping[str, object] | None = None


@dataclass(frozen=True)
class ExpiredClaim:
    action_key: str
    record: dict[str, object]
    original: Mapping[str, object]
    holder: str | None
    lease_age: float | None
    terminal_keys: frozenset[str]


@dataclass(frozen=True)
class ClaimCommit:
    transition: Transition
    action_key: str
    record: dict[str, object]
    original: Mapping[str, object]
    holder: str | None
    destination: Path
    disposition: str
    attempt: int = 0
    adopted: Mapping[str, object] | None = None


def _legal_destination(transition: Transition, destination: str) -> None:
    if destination not in LEGAL_DESTINATIONS[transition]:
        raise PoolContractError(f"illegal {transition.value} destination: {destination}")


def _finish_late(
    queue,
    action_key: str,
    *,
    status: str,
    detail: Mapping[str, object] | None,
    snapshot: Mapping[str, object],
    live: Mapping[str, object],
) -> Path:
    from . import pool
    if snapshot.get("resource_scope") is None and snapshot.get("resource_scope_intent") is None:
        return _archive_late_finish(queue, 
            action_key, status=status, detail=detail, snapshot=snapshot, live=live)
    record = {**dict(snapshot), "action_key": action_key}
    path = queue.records.late_finish_path(record)
    saved = record_store.read_document(path)
    if saved is None:
        record["finish_pending"] = {"status": status, "detail": dict(detail or {})}
        record["late_finish"] = {field: live.get(field) for field in claim_identity.CLAIM_IDENTITY_FIELDS}
        # Persist authority and the completed payload's result before any
        # broker operation. A crash can then be retried by the same sweep
        # that already recovers interrupted finish tombstones.
        queue.records.replace_record_at(path, record)
    else:
        record = saved
    return _retry_late_finish(queue, path, record)


def _retry_late_finish(queue, path: Path, record: dict[str, object]) -> Path:
    from . import pool
    pending = record.get("finish_pending")
    live = record.get("late_finish")
    if (path != queue.records.late_finish_path(record)
            or not isinstance(pending, dict) or not isinstance(pending.get("status"), str)
            or not isinstance(pending.get("detail"), dict) or not isinstance(live, dict)
            or (record.get("resource_scope") is None
                and record.get("resource_scope_intent") is None)):
        raise PoolContractError("invalid late-finish recovery authority or result")
    status, detail = pending["status"], dict(pending["detail"])
    cleanup = queue.cleanup_action_containers(
        record, reason=str(detail.get("termination_reason") or status), scope_only=True)
    if not cleanup["complete"]:
        queue._note_cleanup_attempt(record, record, cleanup)
        queue.records.replace_record_at(path, record)
        return path
    scope_cleanup = cleanup.get("resource_scope") or {}
    detail["resource_scope_cleanup"] = scope_cleanup
    telemetry = scope_cleanup.get("telemetry") or {}
    resource_failure = queue._resource_failure(telemetry)
    if resource_failure:
        status = "failed"
        detail.update(status="failed", returncode=137,
                      termination_reason=resource_failure, resource_telemetry=telemetry,
                      termination_evidence=telemetry.get("termination_evidence"))
    # Keep completed cleanup durable until both the immutable attempt and
    # the recovery evidence are filed. Neither can overwrite a successor.
    queue.records.replace_record_at(path, record)
    result = _archive_late_finish(queue, 
        str(record["action_key"]), status=status, detail=detail, snapshot=record, live=live)
    evidence = dict(record)
    evidence.pop("finish_pending", None)
    evidence.pop("container_cleanup_pending", None)
    queue.records.file_superseded(
        evidence, key=str(record["action_key"]), kind="late-finish",
        status=status, detail=detail, finished_unix=pool._now(),
        finished_host=socket.gethostname(
        ), outcome=str(
            result.relative_to(queue.root),
        ), when=pool._now(
        ))
    path.unlink(missing_ok=True)
    return result


def _archive_late_finish(
    queue,
    action_key: str,
    *,
    status: str,
    detail: Mapping[str, object] | None,
    snapshot: Mapping[str, object],
    live: Mapping[str, object],
) -> Path:
    """File the result of an attempt that a newer one has already replaced.

    ``finish`` used to read whichever record occupied
    ``claimed/<key>.json`` and prefer it over ``claim_snapshot`` without
    comparing identity.  When a lease expired while its launcher was still
    alive, the reaper requeued the action and a second worker claimed the
    retry, the first worker's ``finish`` then advanced *that* record's
    attempt counter, archived its own result under the second worker's
    identity, filed the generation terminal, released the second worker's
    tokens and removed its claim.  A ``done`` record and an immutable
    attempt both described a result the named attempt never produced, and
    the running retry lost its reservation.

    A worker may conclude only the attempt it executed.  The live claim,
    its lease and its reservation are left exactly as they are, and this
    attempt's result goes where it belongs: its own numbered attempt under
    its own generation, first-writer-wins like every other immutable
    outcome, so a reaper that already filed a lease loss for this attempt
    keeps that record and this one does not overwrite it.

    An unstarted release has not charged that number. If the replacement
    is still at the same number, retain this report as superseded evidence
    instead; the numbered slot belongs to the replacement's execution.

    The caller cleans up any exact broker scope before this archive.
    Action-wide Docker ownership cannot distinguish attempts and is never
    used by this path. A legacy attempt without scope authority can only
    archive its result here.
    """
    from . import pool

    attempt = int(snapshot.get("attempts", 0)) + 1
    if (queue.records.attempt_generation({**snapshot, "action_key": action_key})
            == queue.records.attempt_generation({**live, "action_key": action_key})
            and int(live.get("attempts", 0)) < attempt):
        # An unstarted release keeps the publication and attempt count.
        # First-writer-wins is unsafe here: filing the predecessor in that
        # slot makes the successor adopt an outcome it never produced.
        # Keep the report attributable without charging an execution or
        # creating a terminal that a waiter could mistake for completion.
        return queue.records.file_superseded(
            snapshot, key=action_key, kind="uncharged-late-finish",
            status=status, detail=dict(detail or {}),
            finished_unix=pool._now(), finished_host=socket.gethostname(),
            reason="replaced claim has no charged attempt; its execution "
                   "slot belongs to the same-generation successor", when=pool._now(),
        )
    archived = dict(snapshot)
    archived["action_key"] = action_key
    archived["finished_unix"] = pool._now()
    archived["finished_host"] = socket.gethostname()
    succeeded = status in {"executed", "cache_hit"}
    limit = int(snapshot.get("max_attempts", pool.DEFAULT_MAX_ATTEMPTS))
    # The same rule ``attempt_ending`` applies, so this outcome can
    # never be the one that makes a reader refuse the record. A withdrawn
    # status requires the withdrawn disposition: a cancelled attempt is
    # not a failure and must not descend the retry ladder.
    disposition = (
        WITHDRAWN if status == WITHDRAWN else
        DONE if succeeded else FAILED if attempt >= limit else "requeued"
    )
    queue.archive_attempt(
        archived,
        attempt=attempt,
        status=status,
        disposition=disposition,
        detail={
            **dict(detail or {}),
            "late_finisher": {
                "reason": "this claim was requeued and re-claimed while "
                          "this worker was still running it, so its "
                          "result is filed under its own attempt and the "
                          "live claim, lease and reservation were left "
                          "untouched",
                "live_claimed_by": live.get("claimed_by"),
                "live_claimed_unix": live.get("claimed_unix"),
                "live_attempts": live.get("attempts"),
                "live_published_unix": live.get("published_unix"),
            },
        },
    )
    return queue.records.attempt_path(archived, attempt)


def _archive_earlier_terminal(
    queue, record: Mapping[str, object], disposition: str,
) -> tuple[Path, dict[str, object]] | None:
    """Archive the other terminal record an earlier generation left.

    A key is content-addressed, so resubmitting a concluded key publishes
    a new generation of it.  When that generation ends in the *other*
    terminal state -- a failed key republished and executed, or the
    reverse -- the earlier record stayed beside the new one and the key
    read as both (#1117).  Only a readable record of a different
    generation (``published_unix``) is archived; a same-generation pair
    is not this method's to judge. Called by ``finish`` after it owns the
    exact claim's tombstone, before the new terminal is written. Ordinary
    ``finish`` does not hold the key's transition lock across this call.

    The record is copied whole to ``withdrawn/superseded/``, a name no
    terminal-state lookup reads.  Returns its live path, which the caller
    unlinks once the new terminal is filed, and the summary the new
    terminal carries as ``supersedes_terminal``.  An archive that cannot
    be written, or a record that cannot be read, returns ``None`` and
    leaves the record in place: the ambiguity is then the pre-#1117
    state, never a lost record and never a failed conclusion.
    """
    from . import pool

    key = str(record["action_key"])
    other = FAILED if disposition == DONE else DONE
    path = queue.records.item_path(other, key)
    try:
        prior = record_store.read_document(path)
    except (OSError, PoolContractError):
        # Unreadable is not this conclusion's to repair, and raising here
        # would strand the claim this call has already entombed.
        return None
    if prior is None or prior.get("published_unix") == record.get(
            "published_unix"):
        return None
    try:
        archived = queue.records.file_superseded(
            prior, key=key, kind=f"{other}-terminal",
            superseded_unix=pool._now(), superseded_host=socket.gethostname(),
            superseded_by_published_unix=record.get("published_unix"),
            superseded_by_state=disposition, when=pool._now())
    except OSError:
        return None
    summary: dict[str, object] = {"state": other}
    for field in ("status", "published_unix", "published_by",
                  "finished_unix", "finished_host", "attempts",
                  "attempt_history"):
        if field in prior:
            summary[field] = prior[field]
    summary["superseded_path"] = str(archived)
    return path, summary

