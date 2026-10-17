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



def _worker_finish(
    queue,
    action_key: str,
    *,
    status: str,
    detail: Mapping[str, object] | None = None,
    claim_snapshot: Mapping[str, object] | None = None,
) -> Path:
    """File an outcome and return the claim's capacity.

    ``claim_snapshot`` is the record this worker actually executed.  The
    live claimed path can disappear under a finishing worker when a reaper
    wins the terminal-file race; the snapshot keeps the reservation's host
    available even then.  It is not used to reconstruct the queue item --
    the lost-race outcome remains deliberately terminal.
    """
    from . import pool

    succeeded = status in {"executed", "cache_hit"}
    src = queue.records.item_path(CLAIMED, action_key)
    record = record_store.read_document(src)
    facts = classify(ActionCapture(
        claim=Absent() if record is None else Present(record), expected_claim=claim_snapshot))
    if not succeeded and (record or claim_snapshot or {}).get("gang") is not None:
        # One member ending unsuccessfully ends the gang (#1517).
        queue._gang_teardown(record or claim_snapshot, by=action_key,
                            reason=f"member {action_key[:12]} ended {status}")
    if facts.claim is ClaimRelation.ABSENT and claim_snapshot is not None:
        ready = record_store.value_or_raise(queue.records.item(READY, action_key))
        if ready is not None:
            # A queued successor is as live as a claimed successor. This
            # includes charged retries and replacement publications, not
            # only unstarted releases. The late archive distinguishes their
            # numbered history from an uncharged slot; the missing-claim
            # terminal path would end work still waiting in READY (#234).
            return lifecycle._finish_late(queue, 
                action_key, status=status, detail=detail,
                snapshot=claim_snapshot, live=ready,
            )
    if facts.claim is ClaimRelation.SUCCESSOR:
        # Whatever is at ``claimed/<key>.json`` now is not the claim this
        # worker executed, so none of the code below may touch it.
        return lifecycle._finish_late(queue, 
            action_key, status=status, detail=detail,
            snapshot=claim_snapshot, live=record,
        )
    if record is None and claim_snapshot is not None and (
            claim_snapshot.get("resource_scope") is not None
            or claim_snapshot.get("resource_scope_intent") is not None):
        late_path = queue.records.late_finish_path({**claim_snapshot, "action_key": action_key})
        pending_late = record_store.read_document(late_path)
        if pending_late is not None:
            # Its successor may have finished since the first refusal.
            # The saved exact-scope transition still owns this retry.
            return lifecycle._retry_late_finish(queue, late_path, pending_late)
    if record is not None:
        # Host-level token ownership cannot distinguish successive attempts
        # on one box. A stale claim read can agree with this caller while
        # the lease names its successor. Refuse before action-wide Docker
        # cleanup or any claim/telemetry write; the later entomb comparison
        # cannot undo payload removal. Missing legacy fields add no proof.
        claim_identity.check_lease_identity(
            action_key,
            record,
            record_store.read_document(queue.records.lease_path(action_key)),
           invalid=AmbiguousClaimHolder,
       )
    read_claim = dict(record) if record is not None else None
    # Cleanup annotates this mapping with the completed scope evidence.
    # Keep the live record itself so the terminal retains that annotation;
    # only a caller-owned fallback snapshot needs a private copy.
    effective_record = record if record is not None else dict(claim_snapshot or {})
    holder = queue.resolve_claim_holder(action_key, effective_record)
    if holder is not None:
        effective_record["claimed_host"] = holder
    container_cleanup = queue.cleanup_action_containers(
        effective_record, reason=str((detail or {}).get("termination_reason") or status))
    if not container_cleanup["complete"]:
        # A detached container is still the action even after its launcher
        # has returned.  Keep the claim as the durable owner of both the
        # work and its tokens; a local reaper retries cleanup, while a
        # remote one sees the claimed host and leaves it alone.
        pending = dict(effective_record)
        pending["action_key"] = action_key
        queue._note_cleanup_attempt(pending, effective_record, container_cleanup)
        pending["finish_pending"] = {
            "status": status, "detail": dict(detail or {}),
        }
        live = record_store.read_document(src)
        if live is not None and claim_identity.same_claim(live, effective_record):
            queue.records.replace_record_at(src, pending)
        return src
    scope_cleanup = container_cleanup.get("resource_scope") or {}
    telemetry = scope_cleanup.get("telemetry") or {}
    resource_failure = queue._resource_failure(telemetry)
    if resource_failure:
        status, succeeded = "failed", False
        detail = {**dict(detail or {}), "status": "failed", "returncode": 137,
                  "termination_reason": resource_failure, "resource_telemetry": telemetry,
                  "termination_evidence": telemetry.get("termination_evidence")}
    if effective_record.get("tier_fill_borrowed"):
        # Before the export's own tokens go back (#999): which lenders
        # left while it still wrote on what they lent.
        with suppress(Exception):
            queue.note_fill_borrow_end(effective_record)
    if queue.withdrawal_covers(record, action_key=action_key) is not None:
        # Cancellation remains the ending, but the worker owns the proof
        # that its exact attempt stopped. Preserve that proof before giving
        # up the claim; never overwrite an operator's decision or a newer
        # generation's visible marker.
        dst = queue.records.item_path(WITHDRAWN, action_key)
        if read_claim is None:
            return dst
        with queue.action_transition_lock(action_key):
            live = record_store.read_document(src)
            if live is None or not claim_identity.same_claim(live, read_claim):
                return dst
            decision = queue.withdrawal_covers(record, action_key=action_key)
            marker = record_store.read_document(dst)
            current = (marker is not None and
                       marker.get("published_unix") == record.get("published_unix"))
            archive = (None if current else queue.records.withdrawn_finish_path(
                action_key,
                record,
            ))
            filed = dict(marker if current else record_store.read_document(archive) or decision)
            if "withdrawn_attempt" not in filed:
                retained = dict(record)
                retained.pop("finish_pending", None)
                retained.pop("container_cleanup_pending", None)
                prior_attempts = int(retained.get("attempts", 0))
                if (prior_attempts and "attempt_history" not in retained
                        and "attempt_history_missing_before" not in retained):
                    retained["attempt_history_missing_before"] = prior_attempts
                finished_detail = dict(detail or {})
                for field in ("resource_scope", "resource_scope_intent", "resource_scope_cleanup"):
                    if field in record:
                        finished_detail[field] = record[field]
                finished_detail["container_cleanup"] = container_cleanup
                retained.update(status=WITHDRAWN, attempts=prior_attempts + 1,
                                finished_unix=pool._now(), finished_host=socket.gethostname(),
                                detail=finished_detail)
                retained["attempt_history"] = queue.archive_attempt(
                    retained, attempt=retained["attempts"], status=WITHDRAWN,
                    disposition=WITHDRAWN, detail=finished_detail)
                adopted = queue.attempt_ending(retained)
                if adopted["status"] != WITHDRAWN:
                    raise PoolContractError("withdrawn finish conflicts with an archived attempt")
                for field in ("status", "finished_unix", "finished_host", "detail"):
                    retained[field] = adopted[field]
                # Nested history keeps the decision's attempt count and
                # inherited evidence unchanged. Readers never adopt an
                # execution result in place of the cancellation.
                filed["withdrawn_attempt"] = retained
                for field in ("resource_scope", "resource_scope_intent", "resource_scope_cleanup"):
                    if field in record:
                        filed.setdefault(field, record[field])
                filed.setdefault("container_cleanup", adopted["detail"]["container_cleanup"])
                filed.setdefault("finished_unix", adopted["finished_unix"])
                filed.setdefault("finished_host", adopted["finished_host"])
                if current:
                    queue.records.replace_record_at(dst, filed)
                else:
                    queue.records.file_record_if_absent_at(archive, filed)
            if ("scratch_declaration_record" in record
                    or scratch_lifetime.SCRATCH_LIFETIME_FIELD in record):
                queue.records.file_superseded(record, key=action_key,
                                      kind="scratch-declarations-after-withdrawal", when=pool._now())
            return _commit_claim(queue, CancelledClaim(
                action_key, record, read_claim, holder, dst, decision, filed,
                dst if current else archive))
    if record is None:
        # A reaper concluded this claim while the work was still running,
        # so the claim file is gone and the item has already been filed
        # somewhere by the winner.  Synthesising ``{"action_key": key}``
        # here and letting the code below requeue it publishes a record
        # that has the key and nothing else -- no ``worker_script``, no
        # ``cas_root``, no ``checkout_root`` -- *over* the full record the
        # reaper just wrote.  The action can then never run again: every
        # subsequent claim dies on ``KeyError('worker_script')``, and the
        # only copy of where the work lived is gone.  Six actions in the
        # live queue are unrecoverable for exactly this reason.
        #
        # This is the twin of the ``reap_stale`` race fixed in 8b32569 --
        # the same missing-read-treated-as-empty-record on the other side
        # of the same window; that fix's own comment names ``finish()``
        # and only the loop was repaired.  File the outcome terminally so
        # it is countable, and never route it back to ``ready``.
        #
        # Ask what this generation has already been filed as before
        # choosing a directory.  The winner's conclusion is the terminal:
        # a reaper that filed ``failed/`` and a launcher that then
        # succeeded are one attempt with one ending, and writing the
        # launcher's opinion into ``done/`` beside it gives one key two
        # terminals.  ``pbrun`` then answers with whichever record scores
        # higher, ``pool_reset`` offers to re-run work whose receipt is in
        # the CAS, and ``reclaim_terminal_reservation`` refuses the key as
        # ambiguous.  The snapshot carries ``published_unix``, which is
        # what makes the question askable here at all.
        snapshot = dict(effective_record)
        queue._release_reservation(action_key, host=holder)
        queue.records.lease_path(action_key).unlink(missing_ok=True)
        try:
            covered = queue.terminal_outcome_covers(
                snapshot, action_key=action_key)
        except PoolContractError:
            # A terminal for this key exists and cannot be read.  Raising
            # here ends the whole ``serve_once`` call over one bad file,
            # and PR #52 introduced that on a branch which used to write
            # unconditionally, so ask what is actually left to do.
            #
            # Nothing, is the answer.  This branch is reached only because
            # a reaper already concluded the claim, so the key HAS an
            # ending; the unreadable record is it.  Writing a second one
            # beside it is the two-terminals defect PR #52 removed, and a
            # generation this read cannot supply is no basis for deciding
            # that this is a different run.  So report the terminal that
            # is there and write nothing: the submitter's own reader
            # reports an unreadable record at once (PR #50), which is
            # where a corrupted queue record has to surface, and repairing
            # it from here would be inventing an ending for an attempt
            # this worker did not archive.
            for state in (DONE, FAILED):
                unreadable = queue.records.item_path(state, action_key)
                if unreadable.exists():
                    return unreadable
            raise
        if covered is not None:
            return queue.records.item_path(str(covered[0]), action_key)
        lost = queue.records.item_path(
            DONE if succeeded else FAILED, action_key)
        if not record_store.value_or_raise(queue.records.item_presence(
                DONE if succeeded else FAILED, action_key)):
            filed = {
                "schema": pool.POOL_OUTCOME_SCHEMA_V1,
                "action_key": action_key,
                "status": status if succeeded else "finish_lost_race",
                "finished_unix": pool._now(),
                "finished_host": socket.gethostname(),
                "detail": {
                    "reason": "the claim was concluded by a reaper while "
                              "this worker was still running it; the "
                              "item's own record was not available to "
                              "carry forward",
                    "worker_detail": dict(detail or {}),
                },
            }
            # Generation-scoped like every other terminal, from the only
            # copy of the item this branch has.  Identity fields only: the
            # snapshot's ``attempts`` predates this attempt, and its
            # ``attempt_history`` links an attempt somebody else archived,
            # which a reader would adopt against the wrong disposition.
            for field in ("published_unix", "published_by", "claimed_by",
                          "claimed_unix", "claimed_host", "max_attempts",
                          "retry_safe"):
                if field in snapshot:
                    filed[field] = snapshot[field]
            # Every end record names a repository (#1565): the
            # submission's own tag, backfilled to explicit unknown.
            filed["tested_repository"] = pool.tested_repository_of(snapshot)
            queue.records.replace_record_at(lost, filed)
        return lost
    record.pop("finish_pending", None)
    record.pop("container_cleanup_pending", None)
    prior_attempts = int(record.get("attempts", 0))
    attempts = prior_attempts + 1
    limit = int(record.get("max_attempts", pool.DEFAULT_MAX_ATTEMPTS))
    if (
        prior_attempts
        and "attempt_history" not in record
        and "attempt_history_missing_before" not in record
    ):
        record["attempt_history_missing_before"] = prior_attempts
    # The prewarm receipt was referenced from the claim; resolve it into
    # the terminal record's ``detail`` so the done row answers "was this
    # row's data resident when it ran" without a reader having to join
    # against a sidecar the next campaign may have pruned.
    # ``setdefault``: a worker that measured its own residency outranks
    # the loop's prediction.  A reference that no longer resolves -- the
    # sidecar pruned early, or rewritten for a same-key successor whose
    # digest differs -- resolves to nothing rather than to a dangling
    # pointer: an immutable record must not cite a receipt it cannot
    # produce.  A legacy claim that still carries the full receipt (filed
    # before the reference) is copied as before.
    finished_detail = dict(detail or {})
    placement = record.get(local_scratch.PLACEMENT)
    if isinstance(placement, Mapping):
        # Always use the actual winner's claim, not caller-supplied detail.
        finished_detail[local_scratch.PLACEMENT] = dict(placement)
    warmed = record.get("prewarm")
    if isinstance(warmed, Mapping):
        resolved = queue.resolve_prewarm_reference(warmed)
        if resolved is not None:
            finished_detail.setdefault("prewarm", resolved)
    record.update(
        {
            "schema": pool.POOL_OUTCOME_SCHEMA_V1,
            "status": status,
            "attempts": attempts,
            "finished_unix": pool._now(),
            "finished_host": socket.gethostname(),
            "detail": finished_detail,
        }
    )
    terminal = succeeded or attempts >= limit
    disposition = (
        DONE if succeeded else FAILED if terminal else "requeued"
    )
    # Publish the evidence before the mutable queue pointer moves.  A
    # retry rewrites ``detail`` with its own result, so the history link is
    # the only place the causal attempt can survive that transition.
    record["attempt_history"] = queue.archive_attempt(
        record,
        attempt=attempts,
        status=status,
        disposition=disposition,
        # The merged detail, not the caller's: the attempt archive is the
        # immutable evidence and ``attempt_ending`` reads the
        # terminal's ``detail`` back out of it, so a receipt added only to
        # the record is overwritten two statements below.
        detail=finished_detail,
    )
    adopted = queue.attempt_ending(record)
    record.update(
        {
            "status": adopted["status"],
            "finished_unix": adopted["finished_unix"],
            "finished_host": adopted["finished_host"],
            "detail": adopted["detail"],
        }
    )
    disposition = adopted["disposition"]
    if disposition in {DONE, FAILED}:
        dst = queue.records.item_path(str(disposition), action_key)
    else:
        # Reaching this branch is the producer's explicit retry contract,
        # not an inference from deterministic bytes: an argv may mutate
        # external state before failing even when its CAS result would be
        # reproducible.  ``fleet/pbrun`` reaches it only with
        # ``--retry-safe`` and a bound above one.
        dst = queue._shape_as_ready_item(record, action_key=action_key)
    # Everything this worker owns goes before the item's next home becomes
    # visible: the claim to a tombstone, then its own lease.  A retry
    # published while either still stood was claimed by the next poll, and
    # the unlinks below then deleted that new claim and its lease.
    # Another cleanup retry can finish and re-claim this key during the
    # archive write. Compare the original identity at the atomic move,
    # not just at entry, before touching the lease or reservation.
    return _commit_claim(queue, ClaimCommit(
        Transition.FINISH, action_key, record, read_claim, holder, dst,
        str(disposition), attempts, adopted))


def conclude(queue, policy: WorkerFinish | ExpiredClaim) -> Path | None:
    """Run the complete conclusion selected by a named policy over this queue."""
    if isinstance(policy, WorkerFinish):
        return _worker_finish(queue, policy.action_key, status=policy.status,
                              detail=policy.detail, claim_snapshot=policy.claim_snapshot)
    if isinstance(policy, ExpiredClaim):
        return _expired_claim(queue, policy)
    if isinstance(policy, FinishRecovery):
        return _sweep_finish_tombstones(queue, grace_s=policy.grace_s)
    raise TypeError(f"unsupported conclusion policy: {type(policy).__name__}")


@dataclass(frozen=True)
class CancelledClaim:
    action_key: str
    record: dict[str, object]
    original: Mapping[str, object]
    holder: str | None
    destination: Path
    decision: Mapping[str, object]
    filed: dict[str, object]
    proof_path: Path


def _commit_claim(queue, plan: ClaimCommit | CancelledClaim) -> Path | None:
    """Retire the exact owner before publishing its chosen next home.

    Policies retain their conditional reads and historical retention rules.
    The original claim, not the projected outcome, is crash recovery authority.
    """
    from . import pool

    key, record, destination = plan.action_key, plan.record, plan.destination
    cancelled = isinstance(plan, CancelledClaim)
    transition = Transition.CANCEL if cancelled else plan.transition
    disposition = WITHDRAWN if cancelled else plan.disposition
    target = disposition if disposition in {DONE, FAILED, WITHDRAWN} else READY
    _legal_destination(transition, target)
    tombstone, mine = queue._entomb_claim(key, expect=plan.original)
    if not mine or tombstone is None:
        if cancelled:
            return destination
        return (queue.records.attempt_path(record, plan.attempt)
                if transition is Transition.FINISH else None)
    queue.records.lease_path(key).unlink(missing_ok=True)
    if transition is Transition.EXPIRE and disposition in {DONE, FAILED} and record.get("tier_fill_borrowed"):
        with suppress(Exception):
            queue.note_fill_borrow_end(record)
    keep_tier = False
    if transition is Transition.FINISH:
        keep_tier = queue.pin_holds_tier_tokens(record, key)
    elif transition is Transition.EXPIRE:
        keep_tier = (plan.adopted.get("output_retry_stopped") is True
                     and queue.pin_holds_tier_tokens(record, key))
    holder = (plan.holder if isinstance(plan.holder, str) else socket.gethostname()
              ) if transition in {Transition.EXPIRE, Transition.RELEASE_UNSTARTED} else plan.holder
    if transition in {Transition.FINISH, Transition.EXPIRE}:
        queue._release_reservation(key, host=holder, keep_tier=keep_tier)
    else:
        queue._release_reservation(key, host=holder)
    if cancelled:
        if isinstance(plan.decision.get("gang_backfill_preemption"), Mapping):
            from . import _gang
            timing = {**plan.decision["gang_backfill_preemption"], "tokens_returned_unix": pool._now()}
            plan.filed["gang_backfill_release"] = timing
            with suppress(OSError, ValueError, pb.PrismaBuildError):
                queue.records.replace_record_at(plan.proof_path, plan.filed)
            election = timing["election"]
            with queue.action_transition_lock(election["action_key"], blocking=False) as acquired:
                if acquired:
                    with suppress(OSError, ValueError, pb.PrismaBuildError):
                        _gang.note_backfill_preemption(queue, election, timing)
        tombstone.unlink(missing_ok=True)
        return destination
    retired = (_archive_earlier_terminal(queue, record, str(disposition))
               if transition is Transition.FINISH and disposition in {DONE, FAILED} else None)
    if retired is not None:
        record["supersedes_terminal"] = retired[1]
    try:
        if disposition in {DONE, FAILED}:
            queue.records.replace_record_at(destination, record)
        else:
            if transition is Transition.DEFER:
                queue._shape_as_ready_item(record, action_key=key)
                record["maintenance_deferred_unix"] = pool._now()
            kind, reason = {
                Transition.RELEASE_UNSTARTED: (
                    "unstarted-requeue", "a newer publication already owns ready after unstarted claim release"),
                Transition.DEFER: (
                    "maintenance-deferred", "a newer publication already owns ready after maintenance deferral"),
            }.get(transition, ("retry-superseded", "a newer publication already owns ready after an earlier claim ended"))
            destination = queue._publish_requeued_claim(
                record, action_key=key, tombstone=tombstone,
                superseded_kind=kind, superseded_reason=reason)
    except (OSError, pb.PrismaBuildError):
        if transition not in {Transition.EXPIRE, Transition.RELEASE_UNSTARTED}:
            raise
        try:
            os.link(tombstone, queue.records.item_path(CLAIMED, key))
        except OSError:
            pass
        else:
            tombstone.unlink(missing_ok=True)
        return None
    if retired is not None:
        with suppress(FileNotFoundError):
            retired[0].unlink()
    tombstone.unlink(missing_ok=True)
    if transition is Transition.FINISH and disposition in {DONE, FAILED}:
        queue.retire_residency_plan(key)
    return destination


def _expired_claim(queue, policy: ExpiredClaim) -> Path | None:
    from . import pool

    key, record, read_claim = policy.action_key, policy.record, policy.original
    holder, age, terminal_keys = policy.holder, policy.lease_age, policy.terminal_keys
    path = queue.records.item_path(CLAIMED, key)
    container_cleanup = queue.cleanup_action_containers(record, reason="lease_lost")
    if not container_cleanup["complete"]:
        pending = dict(record)
        queue._note_cleanup_attempt(pending, record, container_cleanup)
        queue.records.replace_record_at(path, pending)
        return None
    terminal = queue.terminal_outcome_covers(
        record, action_key=key, terminal=terminal_keys
    )
    if terminal is not None:
        # The worker already filed this exact generation.  A stale
        # directory view or a cycle racing the final unlink may still
        # expose its old claim, but that copy is cleanup, not a retry.
        state, outcome = terminal
        queue.records.file_superseded(
            record, key=key, kind="terminal-claim", status="dropped",
            dropped_unix=pool._now(), dropped_host=socket.gethostname(),
            reason="stale claim belongs to a generation already filed "
                   f"under {state}",
            terminal_status=outcome.get("status"), when=pool._now(),
        )
        queue._release_reservation(
            key, host=holder if isinstance(holder, str) else socket.gethostname(),
            # Judged on the record that was *filed*, not on this
            # stale copy: the ending already concluded, and if it
            # kept its tier tokens the bytes are on the stage.
            # Releasing them here would leave occupancy nothing can
            # attribute -- every reclaim path (an egress, the orphan
            # sweep) walks the tier's held keys, so a key released
            # while its files remain is capacity no mechanism can
            # ever take back.
            keep_tier=queue.pin_holds_tier_tokens(outcome, key))
        path.unlink(missing_ok=True)
        queue.records.lease_path(key).unlink(missing_ok=True)
        return None
    if queue.withdrawal_covers(record, action_key=key) is not None:
        if ("scratch_declaration_record" in record
                or scratch_lifetime.SCRATCH_LIFETIME_FIELD in record):
            queue.records.file_superseded(record, key=key,
                                  kind="scratch-declarations-after-withdrawal", when=pool._now())
        # A withdrawal that could not finish its own cleanup -- the
        # operator's box died mid-verb, say -- leaves a claimed record
        # whose lease nobody refreshes.  Requeueing that is the one
        # thing withdrawal exists to prevent, so conclude it here
        # instead: capacity back, records gone, nothing counted as
        # reaped because nothing was returned to the pool.
        tombstone, mine = queue._entomb_claim(key, expect=read_claim)
        if not mine or tombstone is None:
            return None
        queue.records.lease_path(key).unlink(missing_ok=True)
        queue._release_reservation(
            key, host=holder if isinstance(holder, str) else socket.gethostname())
        tombstone.unlink(missing_ok=True)
        return None
    # The filename is the identity; a record that disagrees with it, or
    # has lost it, must not be written back to a queue directory where
    # every consumer addresses items by key.
    record["action_key"] = key
    prior_attempts = int(record.get("attempts", 0))
    if (age is None
            and record.get("withdrawn_unix") is None
            and not bool(
                record_store.value_or_raise(
                    queue.records.attempt_presence(record, prior_attempts + 1),
                ),
            )
            # Consumed output funding proves this claim already
            # persisted its lease; a subsequently missing lease
            # cannot restore its one-use entitlement (#848).
            and queue._spent_output_retry_stop(record) is None):
        # Nothing ever ran under this claim, so nothing failed under
        # it.  ``claim`` writes the lease before it returns and
        # ``execute`` writes the child pid into it before the payload
        # is launched, so a claim with no lease at all never reached a
        # launch; and no attempt is published under the number this
        # claim would take, so no other writer recorded one either.
        #
        # Both halves are needed.  A lease is also absent from a claim
        # a *finisher* archived and then died holding:
        # ``sweep_finish_tombstones`` restores exactly that record, and
        # relies on this path archiving at the same attempt number so
        # first-writer-wins hands the item the finisher's real outcome.
        # ``finish`` publishes the attempt before it entombs the claim,
        # so the attempt on disk is what tells the two apart.
        #
        # Charging an attempt here is what turned a stalled claimant
        # into lost work: measured on this fleet, a single pool-record
        # operation took 45 s against a 30 s grace, and
        # ``0a44f2e0f62c`` came back ``lease_lost_max_attempts`` with
        # empty stdout and stderr -- an action that never started,
        # unrunnable, out of a queue another box could have taken it
        # from.  Widening the grace only moves the guess; the window
        # has no upper bound on this filesystem.  Releasing does not
        # need one, because it asks what happened rather than how long
        # it took.
        #
        # A withdrawn claim is the one thing a release must not
        # touch. ``withdraw`` closes the retry with an immutable,
        # generation-scoped decision, and ``withdrawal_covers`` reads
        # that decision even after a later publication retires the
        # visible summary. A release does not charge an attempt, but
        # it would put work the operator cancelled straight back in
        # the queue, so this branch explicitly excludes any covered
        # generation.
        #
        # This does not stop a reaper taking a live-but-blocked
        # claimant's claim -- that is not knowable across boxes.  It
        # stops the taking from destroying the work. A resumed
        # claimant must pass the exact-claim launch/heartbeat guards.
        # Its late report cannot use the uncharged attempt number:
        # that slot now belongs to the successor's first execution.
        queue.records.file_superseded(
            record, key=key, kind="unstarted-claim", status="released",
            released_unix=pool._now(), released_host=socket.gethostname(),
            reason="claim released without an attempt: no lease was "
                   "written and no attempt was published", when=pool._now(),
        )
        # Counted, not bounded.  A bound would be the constant this
        # issue exists to avoid, and a release costs an execution
        # nothing; a key whose count climbs is a box that cannot start
        # work, which is the thing to go and look at.
        record["unstarted_releases"] = int(
            record.get("unstarted_releases", 0) or 0) + 1
        destination = queue._shape_as_ready_item(record, action_key=key)
        published = _commit_claim(queue, ClaimCommit(
            Transition.RELEASE_UNSTARTED, key, record, read_claim, holder,
            destination, "requeued"))
        return destination if published == destination else None
    attempts = prior_attempts + 1
    limit = int(record.get("max_attempts", pool.DEFAULT_MAX_ATTEMPTS))
    if (
        prior_attempts
        and "attempt_history" not in record
        and "attempt_history_missing_before" not in record
    ):
        # Runtime rollout can meet a record already requeued by older
        # bytes.  State the irrecoverable prefix honestly and archive
        # from this attempt onward; inventing links would be worse,
        # while refusing the record would strand live work.
        record["attempt_history_missing_before"] = prior_attempts
    terminal = attempts >= limit
    finished_unix = pool._now()
    finished_host = socket.gethostname()
    attempt_record = dict(record)
    attempt_record.update(
        {
            "finished_unix": finished_unix,
            "finished_host": finished_host,
        }
    )
    telemetry = (container_cleanup.get("resource_scope") or {}).get("telemetry") or {}
    resource_failure = queue._resource_failure(telemetry)
    recovery_detail = {
        "reason": "claim lease expired before an outcome was filed",
        "lease_age_s": age,
    }
    if telemetry:
        recovery_detail["resource_telemetry"] = telemetry
    if resource_failure:
        recovery_detail.update(termination_reason=resource_failure,
                               termination_evidence=telemetry.get("termination_evidence"),
                               returncode=137)
    record["attempt_history"] = queue.archive_attempt(
        attempt_record,
        attempt=attempts,
        status="failed" if resource_failure else (
            "lease_lost_max_attempts" if terminal else "lease_lost"),
        disposition=FAILED if terminal else "requeued",
        detail=recovery_detail,
    )
    record["attempts"] = attempts
    adopted = queue.attempt_ending(record)
    record.update(
        {
            "status": adopted["status"],
            "finished_unix": adopted["finished_unix"],
            "finished_host": adopted["finished_host"],
            "detail": adopted["detail"],
        }
    )
    disposition = adopted["disposition"]
    if disposition in {DONE, FAILED}:
        # The immutable winner may be the finisher, not this reaper.
        # File the exact transition it proved rather than the local
        # lease observation that lost the first-writer race.
        record["schema"] = pool.POOL_OUTCOME_SCHEMA_V1
        destination = queue.records.item_path(str(disposition), key)
    else:
        destination = queue._shape_as_ready_item(record, action_key=key)
    published = _commit_claim(queue, ClaimCommit(
        Transition.EXPIRE, key, record, read_claim, holder, destination,
        str(disposition), attempts, adopted))
    if published is not None and (disposition in {DONE, FAILED}
                                 or published == queue.records.item_path(READY, key)):
        return published
    return None


def _sweep_finish_tombstones(
    queue, *, grace_s: float
) -> list[str]:
    """Recover a claim whose finisher died with it moved out of the way.

    ``finish`` and ``reap_stale`` move a claim to a tombstone, publish the
    item's next home, then delete the tombstone.  A process killed inside
    that window leaves a record that no consumer addresses: the key is in
    neither ``ready`` nor ``claimed``, so nothing claims it, nothing reaps
    it, and its waiter never sees an outcome.  This is the only thing that
    looks.

    Three dispositions, and which one applies is decided by what else the
    key has, never by what the tombstone says about itself:

    *   A record in ``ready`` or ``claimed``, **of any generation**, means
        the key has moved on.  Re-injecting these bytes could only start a
        fight with a live record, so the tombstone is filed as evidence and
        removed.  Any generation, not just this one: a crash in this window
        leaves the key addressable nowhere, so a submitter re-publishes it,
        and restoring the old generation over that would have the reaper
        requeue it straight over the new one.
    *   A terminal record of the *same* generation means the publish landed
        and the tombstone is redundant cleanup.  Filed and removed.
    *   Otherwise the publish did not land: link the record back to
        ``claimed/<key>.json`` and let the ordinary reaper conclude it.
        Its lease is already gone, so the missing-lease path applies one
        grace later, and it charges the same attempt number the finisher
        archived -- ``archive_attempt`` is first-writer-wins, so the
        finisher's real outcome is what the record adopts, not this
        reaper's lease observation.

    A record whose attempt links no longer verify is filed rather than
    restored.  Restoring it would hand ``reap_stale`` a record that raises
    from ``archive_attempt``, and that exception stops reaping on every box
    for as long as the record exists.  Verification therefore has to cover
    every way a link can fail to resolve, not only the ones the queue
    itself judges: a missing or tampered immutable outcome raises out of
    ``core``, not out of the pool's contract error, and an escape here
    causes the exact stall this paragraph is about -- ``reap_stale`` calls
    this sweep unguarded, and ``serve_once`` calls ``reap_stale`` before it
    claims.  Being *unable to look* is a third answer and takes neither
    disposition: the tombstone is left for the next sweep, because filing
    it would hide it in ``withdrawn/superseded/`` on the strength of a
    stale directory handle.

    The grace is the lease timeout: nothing is blocked behind this except
    the action's own visibility, and a sweep that fires while a finisher is
    mid-publish would put a claim beside a ready record of the same
    generation.

    Exact-scope late finishes use a separate suffix. They are already
    awaiting cleanup, never restored as claims, and retried immediately
    on their owning host under the same key lock.

    A killed requeue publisher can also leave its hidden staging sibling.
    Aged regular siblings are removed only after their exact tombstone is
    gone. This cleanup does not add keys to the recovered-claim result.
    """
    from . import pool

    swept: list[str] = []
    claimed = queue.records.dir(CLAIMED)
    if not queue.records.state_directory_present(CLAIMED):
        return swept
    queue._sweep_orphan_requeue_staging(claimed, grace_s=grace_s)
    now = pool._now()
    for tombstone in queue.records.finishing_paths():
        parts = tombstone.name.split(".", 2)
        key = parts[0]
        if len(key) != 64 or any(ch not in "0123456789abcdef" for ch in key):
            continue
        with queue.action_transition_lock(key, blocking=False) as acquired:
            if not acquired:
                continue
            if tombstone.name.endswith(LATE_FINISH_SUFFIX):
                # This record never owns the action's current claim or
                # tokens. Retry only its saved scope, even beside a live
                # successor, without the ordinary tombstone dispositions.
                try:
                    record = record_store.read_document(tombstone)
                    if record is None or record.get("action_key") != key:
                        raise PoolContractError("invalid late-finish recovery record")
                    if record.get("claimed_host") != socket.gethostname():
                        continue
                    result = lifecycle._retry_late_finish(queue, tombstone, record)
                except Exception as exc:                         # noqa: BLE001
                    print(f"pool: late finish {key} retained: {type(exc).__name__}: {exc}",
                          file=sys.stderr)
                    continue
                if result != tombstone:
                    swept.append(key)
                continue
            when: float | None = None
            if len(parts) >= 2:
                try:
                    when = int(parts[1]) / 1_000_000.0
                except ValueError:
                    when = None
            if when is None:
                try:
                    when = tombstone.stat().st_mtime
                except OSError:
                    continue
            if now - when <= grace_s:
                continue
            successor = None
            try:
                record = record_store.read_document(tombstone)
                live = any(
                    bool(record_store.value_or_raise(queue.records.item_presence(state, key)))
                    for state in (READY, CLAIMED)
                )
                covered = (
                    queue.terminal_outcome_covers(record, action_key=key) is not None
                    or queue.withdrawal_covers(record, action_key=key) is not None
                )
                if record is not None and type(record.get("published_unix")) in (int, float):
                    generation = record["published_unix"]
                    decision = queue.publication_ending(key, generation=generation)
                    covered = covered or decision is not None
                    if (not covered and not queue.records.listed_item_present(CLAIMED, key)
                            and queue.records.listed_item_present(READY, key)):
                        archived = queue.archived_generation_outcomes(key, generation=generation)
                        if archived:
                            covered = True
                        else:
                            candidate = queue.records.ready_publication(key)
                            candidate_generation = queue._successor_generation(record, candidate)
                            live = True
                            if candidate_generation > generation:
                                successor = candidate
            except (OSError, pb.PrismaBuildError):
                continue
            restorable = record is not None and not live and not covered
            if restorable and "attempt_history" in record:
                try:
                    queue.attempt_outcomes(record)
                except (PoolContractError, FileNotFoundError, pb.CASTamperError):
                    # ``attempt_outcomes`` decides most of this by reading the
                    # record and raises ``PoolContractError``, but the link it
                    # checks last is a *file*: ``open_regular_nofollow``
                    # raises a bare ``FileNotFoundError`` when the immutable
                    # outcome is absent and ``CASTamperError`` when the entry
                    # is not a readonly regular file, and neither derives from
                    # ``PoolContractError``.  Both are positive evidence that
                    # the link does not verify, which is this branch's whole
                    # question, so both file the record rather than restore it.
                    restorable = False
                except pb.CASUnavailableError:
                    # "Could not look" is not evidence, and it must not be
                    # answered either way.  Restoring risks the reaping stall
                    # this guard exists to prevent; filing moves the record
                    # into ``withdrawn/superseded/``, which every reader is
                    # documented to ignore, so a stale handle would lose the
                    # action permanently.  Leave the tombstone for the next
                    # sweep -- the only disposition that keeps the evidence.
                    continue
            if restorable:
                try:
                    os.link(tombstone, queue.records.item_path(CLAIMED, key))
                except OSError:
                    pass
                else:
                    tombstone.unlink(missing_ok=True)
                    swept.append(key)
                    continue
            # A cancellation can be durable before its finisher returns
            # tokens. If that finisher dies after removing the lease,
            # this tombstone is the only remaining cleanup authority.
            # A queued successor has not acquired anything yet: retain its
            # READY bytes while completing the predecessor's cleanup.
            # A claimed successor, however, owns the key now; none of its
            # resources or lease belongs to this old tombstone.
            if not queue.records.listed_item_present(CLAIMED, key):
                try:
                    holders = queue.claim_reservation_hosts(key)
                    if len(holders) > 1 or (record is None and holders):
                        continue
                    if record is not None:
                        holder = record.get("claimed_host")
                        if holders:
                            if holder and holder != holders[0]:
                                continue
                            record["claimed_host"] = holders[0]
                        cleanup = queue.cleanup_action_containers(
                            record, reason="interrupted finish cleanup", scratch_owner_path=tombstone)
                        if not cleanup["complete"]:
                            continue
                    if holders:
                        # The ending is already filed -- that is what made
                        # this tombstone unrestorable -- so if it pinned a
                        # staged range, its tier tokens are not this
                        # cleanup's to return.  Releasing them would leave
                        # the files on the stage with nothing holding them.
                        queue._release_reservation(
                            key, host=holders[0],
                            keep_tier=queue._filed_pin_holds(key))
                        if queue.claim_reservation_hosts(key):
                            continue  # a partial return still needs this owner
                    lease = record_store.read_document(queue.records.lease_path(key))
                    if (record is not None and lease is not None
                            and lease.get("owner") == record.get("claimed_by")):
                        queue.records.lease_path(key).unlink(missing_ok=True)
                except (OSError, PoolContractError, pb.CASUnavailableError):
                    continue
            if successor is not None:
                try:
                    queue._persist_publication_supersession(record, successor=successor)
                except (OSError, pb.PrismaBuildError):
                    continue
            queue.records.file_superseded(
                record, key=key, kind="finish-tombstone", status="dropped",
                dropped_unix=pool._now(), dropped_host=socket.gethostname(),
                reason="a finisher was interrupted between moving its claim "
                       "aside and publishing the item's next home; the key "
                       "already has a live or terminal record, so these bytes "
                       "are evidence rather than work", when=pool._now(),
            )
            tombstone.unlink(missing_ok=True)
            swept.append(key)
    return swept



@dataclass(frozen=True)
class FinishRecovery:
    grace_s: float
