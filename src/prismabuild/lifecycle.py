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
            tombstone, mine = queue._entomb_claim(action_key, expect=read_claim)
            if not mine or tombstone is None:
                return dst
            queue.records.lease_path(action_key).unlink(missing_ok=True)
            queue._release_reservation(action_key, host=holder)
            if isinstance(decision.get("gang_backfill_preemption"), Mapping):
                from . import _gang
                timing = {**decision["gang_backfill_preemption"], "tokens_returned_unix": pool._now()}
                filed["gang_backfill_release"] = timing
                # The decision stays immutable; the worker's conclusion
                # records when its exact released reservation came back.
                with suppress(OSError, ValueError, pb.PrismaBuildError):
                    queue.records.replace_record_at(dst if current else archive, filed)
                election = timing["election"]
                with queue.action_transition_lock(election["action_key"], blocking=False) as acquired:
                    if acquired:
                        with suppress(OSError, ValueError, pb.PrismaBuildError):
                            _gang.note_backfill_preemption(queue, election, timing)
            tombstone.unlink(missing_ok=True)
            return dst
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
    tombstone, mine = queue._entomb_claim(action_key, expect=read_claim)
    if not mine or tombstone is None:
        return queue.records.attempt_path(record, attempts)
    queue.records.lease_path(action_key).unlink(missing_ok=True)
    # Capacity is released before the item is filed, so the next worker to
    # look sees the tokens free rather than racing this rename.  A mover
    # that staged what it declared keeps its *tier* tokens: they stand for
    # bytes that are still on the stage, and only an egress that deletes
    # them may give them back.
    queue._release_reservation(
        action_key, host=holder,
        keep_tier=queue.pin_holds_tier_tokens(record, action_key))
    # A republished key ends with one terminal record (#1117): an earlier
    # generation's record in the other terminal state is archived first,
    # named on the new record, and leaves its state directory only after
    # the new record is filed, so the key is never without an ending.
    retired = (lifecycle._archive_earlier_terminal(queue, record, str(disposition))
               if disposition in {DONE, FAILED} else None)
    if retired is not None:
        record["supersedes_terminal"] = retired[1]
    if disposition in {DONE, FAILED}:
        queue.records.replace_record_at(dst, record)
    else:
        dst = queue._publish_requeued_claim(
            record, action_key=action_key, tombstone=tombstone,
            superseded_kind="retry-superseded",
            superseded_reason="a newer publication already owns ready "
                              "after an earlier claim ended",
        )
    if retired is not None:
        with suppress(FileNotFoundError):
            retired[0].unlink()
    if tombstone is None:
        src.unlink(missing_ok=True)
    else:
        tombstone.unlink(missing_ok=True)
    if disposition in {DONE, FAILED}:
        queue.retire_residency_plan(action_key)
    return dst



def conclude(queue, policy: WorkerFinish) -> Path:
    """Run the complete conclusion selected by a named policy over this queue."""
    if isinstance(policy, WorkerFinish):
        return _worker_finish(queue, policy.action_key, status=policy.status,
                              detail=policy.detail, claim_snapshot=policy.claim_snapshot)
    raise TypeError(f"unsupported conclusion policy: {type(policy).__name__}")
