"""Synchronous outcome observation, immutable verification and reporting."""
from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from enum import Enum
import math
from pathlib import Path
import re
import socket
import sys
from typing import assert_never

from . import action_result, action_wait, core as pb, endings, pool
from .digest_primitives import sorted_json

#: What ``pbrun`` exits with when it stopped waiting before the work finished.
#: The pool path already spells it this way; the SLURM path means the same
#: thing by it, and in both cases the work is still running.
GAVE_UP_EXIT = 75


PUBLICATION_SUPERSEDED_EXIT = 76


#: What ``pbrun`` exits with when the action it was waiting for was withdrawn.
#: 128+SIGTERM, which is the shell's own word for "this was stopped on purpose",
#: and it is literally the signal a withdrawal sends to the action's process
#: group -- ``core.run_local_action`` reports the same event as status ``-15``.
#: Non-zero because the command did not run; distinct from a real failure
#: because nothing about it was a defect.
WITHDRAWN_EXIT = 143


#: Filesystem/record persistence failure (sysexits.h ``EX_IOERR``). The legacy
#: name remains for callers; diagnostics distinguish an accepted job's failed
#: record write from other filesystem access failures and withdrawal stages.
#: Neither a timeout nor a withdrawal verdict: recovery requires reading the
#: reported path, reason and known job id.
RECORD_WRITE_FAILED_EXIT = 74


#: The exit codes ``pbrun`` decides for itself, and therefore the codes a run's
#: own status must never be allowed to impersonate.  A terminal record carries
#: the far side's launcher status as a plain integer, and both report paths
#: returned it verbatim: a launcher that exited 143 reached the caller as
#: ``WITHDRAWN_EXIT``, which claims an operator made a decision that nothing on
#: disk records.  That is reachable rather than theoretical --
#: ``core._sigterm_unwinds_this_process`` raises ``SystemExit(128 + signum)``,
#: so every SIGTERM that is not a withdrawal leaves 143 in the record -- and 2
#: (a refusal), 74 and 75 are the same hole with different remedies attached.
#: Zero is deliberately absent: it is ``pbrun``'s word for success, no producer
#: files it under a status that is not one, and the SLURM site below already
#: excludes it by truthiness.
RESERVED_EXITS = frozenset(
    {2, RECORD_WRITE_FAILED_EXIT, GAVE_UP_EXIT, PUBLICATION_SUPERSEDED_EXIT, WITHDRAWN_EXIT}
)


class ClientArrival(Enum):
    SUBMITTED = 'submitted'
    ATTACHED = 'attached'


class ClientTransport(Enum):
    POOL = 'pool'
    SLURM = 'slurm'


class ReplayDiagnostic(Enum):
    NOT_A_CACHE_HIT = 'result_was_not_cached'
    NO_UNIQUE_ANNOUNCEMENT = 'no_unique_result_announcement'
    INVALID_ANNOUNCEMENT = 'invalid_cached_result_announcement'
    INVALID_IPC = 'invalid_cached_replay_metadata'


@dataclass(frozen=True)
class CachedResult:
    sha256: str


@dataclass(frozen=True)
class ReplayUnavailable:
    reason: ReplayDiagnostic


CachedReplay = CachedResult | ReplayUnavailable | None


@dataclass(frozen=True)
class CachedOutput:
    text: str


@dataclass(frozen=True)
class CachedOutputUnavailable:
    reason: str


@dataclass(frozen=True)
class PoolClientIdentity:
    action_key: str
    published_unix: float | None
    queue_root: Path
    cas_root: Path
    submission: Path | None
    arrival: ClientArrival


@dataclass(frozen=True)
class SlurmClientIdentity:
    action_key: str
    published_unix: float | None
    queue_root: Path
    cas_root: Path
    submission: Path | None
    job_id: str
    arrival: ClientArrival


@dataclass(frozen=True)
class CachedClientIdentity:
    action_key: str
    queue_root: Path
    cas_root: Path
    transport: ClientTransport
    result_sha256: str


ClientIdentity = PoolClientIdentity | SlurmClientIdentity | CachedClientIdentity


@dataclass(frozen=True)
class ClientExecution:
    action_key: str
    published_unix: float | None
    status: str
    transport_returncode: int | None
    action_returncode: int | None
    action_signal: int | None
    execution_timeout: bool
    reported_returncode: int
    replay: CachedReplay = None
    receipt_published: bool | None = None
    transport_state: str | None = None
    transport_signal: int | None = None

    @property
    def cached_result_sha256(self) -> str | None:
        return self.replay.sha256 if isinstance(self.replay, CachedResult) else None

    @property
    def cached_result_diagnostic(self) -> ReplayDiagnostic | None:
        return self.replay.reason if isinstance(self.replay, ReplayUnavailable) else None


@dataclass(frozen=True)
class ClientUnreadable:
    action_key: str
    published_unix: float | None = None


@dataclass(frozen=True)
class ClientUnavailable:
    action_key: str | None
    published_unix: float | None
    wait_end: action_wait.WaitEnd


@dataclass(frozen=True)
class ClientPatienceExpired:
    action_key: str
    published_unix: float | None


@dataclass(frozen=True)
class ClientSuperseded:
    action_key: str
    published_unix: float
    successor_published_unix: float


@dataclass(frozen=True)
class ClientOfferTimeout:
    pass


@dataclass(frozen=True)
class ClientRefused:
    returncode: int
    action_key: str | None = None
    published_unix: float | None = None


@dataclass(frozen=True)
class ClientDeferredTimeout:
    pending_id: str


@dataclass(frozen=True)
class ClientSubmitted:
    identity: ClientIdentity
    submission: Path | None = None


@dataclass(frozen=True)
class ClientDeferredSubmitted:
    pending_id: str
    submission: Path
    release: Path


class UnsettledReason(Enum):
    SUBMISSION_FATE = 'submission_fate_unknown'
    SCHEDULER_UNKNOWN = 'scheduler_cannot_describe_job'


@dataclass(frozen=True)
class ClientUnsettled:
    action_key: str
    published_unix: float | None
    reason: UnsettledReason


@dataclass(frozen=True)
class ClientCommand:
    returncode: int


ClientVerdict = (ClientExecution | ClientUnavailable | ClientPatienceExpired
                 | ClientSuperseded | ClientOfferTimeout | ClientUnreadable | ClientRefused
                 | ClientDeferredTimeout | ClientSubmitted | ClientDeferredSubmitted
                 | ClientUnsettled | ClientCommand)


def _execution_exit_code(returncode: int) -> int:
    return 1 if returncode in RESERVED_EXITS else returncode


def client_exit(verdict: ClientVerdict) -> int:
    if isinstance(verdict, ClientExecution):
        return verdict.reported_returncode
    if isinstance(verdict, ClientUnreadable):
        return 1
    if isinstance(verdict, ClientUnavailable):
        return RECORD_WRITE_FAILED_EXIT
    if isinstance(verdict, (ClientPatienceExpired, ClientDeferredTimeout, ClientUnsettled)):
        return GAVE_UP_EXIT
    if isinstance(verdict, ClientSuperseded):
        return PUBLICATION_SUPERSEDED_EXIT
    if isinstance(verdict, ClientOfferTimeout):
        return 1
    if isinstance(verdict, (ClientSubmitted, ClientDeferredSubmitted)):
        return 0
    if isinstance(verdict, (ClientRefused, ClientCommand)):
        return verdict.returncode
    assert_never(verdict)


@dataclass(frozen=True)
class ClientEnding:
    verdict: ClientVerdict
    readers: action_wait.ReaderHistory = action_wait.ReaderHistory()

    @property
    def exit_code(self) -> int:
        return client_exit(self.verdict)


def recovery_permitted(identity: ClientIdentity | None, ending: ClientEnding) -> bool:
    return (isinstance(identity, PoolClientIdentity)
            and identity.published_unix is not None
            and math.isfinite(identity.published_unix)
            and isinstance(ending.verdict, ClientUnavailable)
            and ending.verdict.action_key == identity.action_key
            and ending.readers.abandoned_count == 0
            and ending.readers.complete)


@dataclass
class ClientReport:
    identity: ClientIdentity | None = None
    ending: ClientEnding | None = None
    on_event: Callable[[ClientIdentity | ClientEnding], None] | None = field(
        default=None, repr=False)

    def publish_identity(self, identity: ClientIdentity) -> None:
        if self.identity is not None or self.ending is not None:
            raise RuntimeError('client identity must precede its one ending')
        self.identity = identity
        if self.on_event is not None:
            self.on_event(identity)

    def conclude(self, ending: ClientEnding) -> None:
        if self.ending is not None:
            raise RuntimeError('client already has an ending')
        self.ending = ending
        if self.on_event is not None:
            self.on_event(ending)

    def finish(self, verdict: ClientVerdict) -> int:
        ending = ClientEnding(verdict, action_wait.client_reader_history())
        self.conclude(ending)
        return ending.exit_code


#: Public detach records can be read by another published runtime generation.
DETACH_SCHEMA_V1 = 'prismaquant.prismabuild.pbrun_detach.v1'


def detached_submission_record(
    key: str, *, transport: str, status: str, queue_root,
    published_unix: float | None = None, job_id: str | None = None, submission=None,
) -> dict:
    """Build the public record rendered as one JSON line by pbrun.

    Everything ``pbrun`` prints for a person goes to stderr, so stdout carries
    this and nothing else: a caller reads one line of JSON instead of parsing
    prose written to be read aloud.

    ``status`` is one of ``submitted`` (this call put the work somewhere),
    ``cache_hit`` (it was already in the CAS, so nothing was submitted) or
    ``attached`` (it was already running under an earlier submission, which
    this call joined rather than duplicated).  All three exit 0, and the last
    two name a run this process did not start.

    The generation travels in it because the terminal record a later wait looks
    for is identified by generation and not by key.  An action key is a content
    hash, so one key accumulates the records of every earlier run of the same
    work, and a waiter given only the key cannot tell this run's ending from
    the ending of a run that finished last week."""
    queue = Path(queue_root)
    return dict(schema=DETACH_SCHEMA_V1, action_key=str(key), transport=str(transport),
        status=str(status), published_unix=published_unix,
        job_id=str(job_id) if job_id is not None else None,
        submission=str(submission) if submission is not None else None,
        done=str(queue / pool.DONE / f'{key}.json'),
        failed=str(queue / pool.FAILED / f'{key}.json'),
        withdrawn=str(queue / pool.WITHDRAWN / f'{key}.json'))


def reported_submission(report: ClientReport) -> dict | None:
    ending = report.ending
    if ending is None:
        return None
    verdict = ending.verdict
    if isinstance(verdict, ClientDeferredSubmitted):
        return dict(schema=DETACH_SCHEMA_V1, action_key=None, pending_id=verdict.pending_id,
            transport='pool', status='deferred', published_unix=None, job_id=None,
            submission=str(verdict.submission), release=str(verdict.release))
    if not isinstance(verdict, ClientSubmitted):
        return None
    identity = verdict.identity
    if isinstance(identity, CachedClientIdentity):
        return detached_submission_record(identity.action_key, transport=identity.transport.value,
            status='cache_hit', queue_root=identity.queue_root)
    return detached_submission_record(identity.action_key,
        transport='pool' if isinstance(identity, PoolClientIdentity) else 'slurm',
        status='submitted' if identity.arrival is ClientArrival.SUBMITTED else 'attached',
        queue_root=identity.queue_root, published_unix=identity.published_unix,
        job_id=identity.job_id if isinstance(identity, SlurmClientIdentity) else None,
        submission=verdict.submission if verdict.submission is not None else identity.submission)


def read_cached_output(identity: ClientIdentity | None, ending: ClientEnding) -> (
        CachedOutput | CachedOutputUnavailable | None):
    if identity is None or not isinstance(ending.verdict, (ClientExecution, ClientSubmitted)):
        return None
    if isinstance(identity, CachedClientIdentity):
        digest = identity.result_sha256
    elif isinstance(ending.verdict, ClientExecution):
        replay = ending.verdict.replay
        if isinstance(replay, ReplayUnavailable):
            return (None if replay.reason is ReplayDiagnostic.NOT_A_CACHE_HIT
                    else CachedOutputUnavailable(replay.reason.value))
        if not isinstance(replay, CachedResult):
            return None
        digest = replay.sha256
    else:
        return None
    try:
        cas = pb.PrismaBuildCAS(identity.cas_root)
        action = cas.read_action_request(identity.action_key)
        if action is None:
            return CachedOutputUnavailable('the sealed action request is unavailable')
        receipt = cas.lookup(action)
        if receipt is None:
            return CachedOutputUnavailable('the verified result receipt is unavailable')
        if receipt['result']['sha256'] != digest:
            return CachedOutputUnavailable('the verified result differs from the announced digest')
        return CachedOutput(cas.result_path(receipt, action).read_text(errors='replace'))
    except (OSError, pb.PrismaBuildError) as exc:
        return CachedOutputUnavailable(str(exc))


def bounded_outcome_observation(
    q, key: str, generation: float | None, *, budget_s: float,
) -> tuple[endings.ObservedEnding | None, float | None]:
    """Read and select one pool outcome snapshot in an abandonable child.

    The child returns only JSON values.  In particular, paths become strings
    and the generation selected after a preemption is returned to the parent;
    the next poll must not rediscover a different generation from mutable
    queue rows.

    The synchronous profile is ``report_pool_outcome``'s opt-in (#630): a
    complete payload from a reader that could not be reaped is used, because
    what follows it is verification and a printout, not a scheduler query.
    ``pbwait`` and the other readers keep the strict refusal.
    """

    value = action_wait.read_observation(
        "pool outcome observation",
        lambda: _outcome_observation_value(q, key, generation),
        budget_s=budget_s,
    )
    if not isinstance(value, dict):
        raise action_wait.OutcomeReadUnavailable("pool outcome observation returned an invalid payload")
    unreadable = value.get("unreadable")
    if unreadable is not None:
        if (not isinstance(unreadable, dict)
                or not isinstance(unreadable.get("path"), str)
                or not isinstance(unreadable.get("reason"), str)):
            raise action_wait.OutcomeReadUnavailable(
                "pool outcome observation returned an invalid unreadable ending")
        raise action_wait.UnreadableTerminal(Path(unreadable["path"]), unreadable["reason"])
    next_generation = value.get("generation")
    if (next_generation is not None
            and (not isinstance(next_generation, (int, float))
                 or isinstance(next_generation, bool))):
        raise action_wait.OutcomeReadUnavailable("pool outcome observation returned an invalid generation")
    landed = decode_outcome_ending(q, key, next_generation, value)
    if isinstance(landed, endings.PublicationSupersession) and next_generation != generation:
        raise action_wait.OutcomeReadUnavailable("publication supersession changed the watched generation")
    return landed, float(next_generation) if next_generation is not None else None


def encode_outcome_ending(ending: endings.ObservedEnding | None) -> dict:
    """Encode the observed-ending union for both bounded waiter protocols."""
    if isinstance(ending, endings.PublicationSupersession):
        return {"landed": None,
                "superseded": {"path": str(ending.path), "record": ending.record}}
    return {"landed": None if ending is None else {
        "path": str(ending[0]), "record": ending[1],
    }}


def decode_outcome_ending(q, key: str, generation: float | None, value: dict
                          ) -> endings.ObservedEnding | None:
    """Decode wire branches; the queue owns publication-proof validation."""
    superseded = value.get("superseded")
    landed = value.get("landed")
    raw = superseded if superseded is not None else landed
    if raw is None:
        return None
    if (not isinstance(raw, dict) or not isinstance(raw.get("path"), str)
            or not isinstance(raw.get("record"), dict)
            or (superseded is not None and landed is not None)):
        raise action_wait.OutcomeReadUnavailable("pool outcome observation returned an invalid ending")
    if superseded is None:
        action_wait.require_execution_ending(raw["record"], where="outcome execution IPC branch")
        return Path(raw["path"]), raw["record"]
    try:
        ending = q.decode_publication_supersession(Path(raw["path"]), raw["record"])
    except (OSError, pb.PrismaBuildError) as exc:
        raise action_wait.OutcomeReadUnavailable(f"invalid observed publication ending: {exc}") from exc
    if ending.action_key != key or ending.published_unix != generation:
        raise action_wait.OutcomeReadUnavailable("observed publication ending differs from the watched identity")
    return ending


def _outcome_observation_value(q, key: str, generation: float | None) -> dict:
    try:
        landed, next_generation = action_wait.outcome_poll(
            q, key, generation, report_unreadable=True)
    except action_wait.UnreadableTerminal as exc:
        return {"unreadable": {"path": str(exc.path), "reason": exc.reason}}
    return {
        "generation": next_generation,
        **encode_outcome_ending(landed),
    }


class _TerminalSummaryConflict(pool.PoolContractError):
    """A verified immutable attempt disagrees with its mutable summary."""

    def __init__(self, message, *, adopted, fields):
        super().__init__(message)
        self.adopted = adopted
        self.fields = fields


def bounded_outcome_render(q, outcome_path: Path, outcome: dict, *, budget_s: float,
                                              diagnose_conflicts: bool = False) -> dict:
    """Verify and expand one landed ending in a separate bounded read.

    Immutable attempt records and their logs are part of the outcome verdict.
    This read occurs after the caller has received an ending, so it gets its
    own finite verification budget rather than borrowing or resetting the
    deadline that governed how long the caller waited for an ending to land.

    The synchronous profile is ``report_pool_outcome``'s opt-in, as with
    observation: the verified summary feeds a printout and an exit status,
    so a complete payload from an unreaped reader is used (#630).
    ``diagnose_conflicts`` reports a verified-attempt/summary disagreement as
    inspection evidence, never as an ordinary successful outcome (#1351).
    """

    action_wait.require_execution_ending(outcome, where=str(outcome_path))
    value = action_wait.read_observation(
        "pool outcome verification",
        lambda: (_diagnosed_outcome_render_value if diagnose_conflicts
                 else _outcome_render_value)(q, str(outcome_path), outcome),
        budget_s=budget_s,
    )
    if not isinstance(value, dict):
        raise action_wait.OutcomeReadUnavailable("pool outcome verification returned an invalid payload")
    if diagnose_conflicts and isinstance(value.get("integrity_error"), dict):
        return value
    if not isinstance(value.get("summary"), dict) or not isinstance(value.get("attempts"), list):
        raise action_wait.OutcomeReadUnavailable("pool outcome verification returned an invalid summary")
    action_wait.require_execution_ending(value["summary"], where="execution verification IPC summary")
    value['cached_replay'] = decode_replay_metadata(value, action_key=str(outcome.get('action_key') or ''))
    return value


def decode_replay_metadata(value: Mapping[str, object], *, action_key: str) -> CachedReplay:
    digest = value.get('cached_result_sha256')
    announced_key = value.get('cached_result_action_key')
    diagnostic = value.get('cached_result_diagnostic')
    if digest is None and announced_key is None:
        if diagnostic is None:
            return None
        try:
            return ReplayUnavailable(ReplayDiagnostic(diagnostic))
        except (TypeError, ValueError):
            return ReplayUnavailable(ReplayDiagnostic.INVALID_IPC)
    if (isinstance(digest, str) and re.fullmatch('[a-f0-9]{64}', digest)
            and announced_key == action_key and diagnostic is None):
        return CachedResult(digest)
    return ReplayUnavailable(ReplayDiagnostic.INVALID_IPC)


def _cached_replay_value(summary: dict, attempts: list[dict]) -> dict:
    def no_replay_value(reason):
        return dict(cached_result_sha256=None, cached_result_action_key=None,
                    cached_result_diagnostic=reason.value if reason is not None else None)

    adopted = summary['adopted']
    if adopted is not None:
        selected = [attempt for attempt in attempts if attempt.get('attempt') == adopted['attempt']]
        if len(selected) != 1:
            return no_replay_value(ReplayDiagnostic.NO_UNIQUE_ANNOUNCEMENT)
        stdout = selected[0].get('stdout')
    else:
        stdout = summary['detail'].get('stdout')
    if not isinstance(stdout, str):
        return no_replay_value(ReplayDiagnostic.NO_UNIQUE_ANNOUNCEMENT)
    try:
        announcement = action_result.decode_result_announcement(stdout.encode('utf-8', errors='replace'))
    except action_result.ActionResultError:
        return no_replay_value(ReplayDiagnostic.NO_UNIQUE_ANNOUNCEMENT)
    if announcement.get('status') != 'cache_hit':
        return no_replay_value(ReplayDiagnostic.NOT_A_CACHE_HIT)
    receipt = announcement['receipt']
    if not isinstance(receipt, Mapping):
        return no_replay_value(ReplayDiagnostic.INVALID_ANNOUNCEMENT)
    result = receipt.get('result')
    digest = result.get('sha256') if isinstance(result, Mapping) else None
    if (receipt.get('action_key') != summary['action_key'] or not isinstance(digest, str)
            or re.fullmatch('[a-f0-9]{64}', digest) is None):
        return no_replay_value(ReplayDiagnostic.INVALID_ANNOUNCEMENT)
    return dict(cached_result_sha256=digest, cached_result_action_key=summary['action_key'],
                cached_result_diagnostic=None)


def _diagnosed_outcome_render_value(q, outcome_path: str, outcome: dict) -> dict:
    """Diagnose conflicts inside the same bounded, read-only verifier."""
    try:
        return _outcome_render_value(q, outcome_path, outcome)
    except _TerminalSummaryConflict as exc:
        adopted = exc.adopted
        attempt = q.attempt_outcomes(outcome)[-1]
        queued_detail = outcome.get("detail")
        immutable_detail = adopted["detail"]
        if isinstance(queued_detail, dict):
            absent = object()
            fields = sorted(name for name in queued_detail.keys() | immutable_detail.keys()
                            if queued_detail.get(name, absent) != immutable_detail.get(name, absent))
        else:
            fields = ["<not-an-object>"]
        # No field values, argv, or streams are included. Bound even malicious
        # field names/counts; the digests identify the full compared objects.
        diagnostic = {
            "kind": "terminal_summary_conflict",
            "action_key": outcome["action_key"],
            "generation": q.records.attempt_generation(outcome),
            "published_unix": outcome["published_unix"],
            "attempt": adopted["attempt"],
            "summary_path": outcome_path,
            "attempt_path": str(q.records.attempt_path(outcome, adopted["attempt"])),
            "mismatched_fields": exc.fields,
            "detail_fields": [name[:128] for name in fields[:32]],
            "detail_field_count": len(fields),
            "queue_detail_sha256": pb.canonical_sha256(queued_detail),
            "adopted_detail_sha256": pb.canonical_sha256(immutable_detail),
            "immutable_attempt": {
                "status": str(adopted["status"])[:128],
                "disposition": str(adopted["disposition"])[:128],
                "returncode": (immutable_detail.get("returncode")
                               if type(immutable_detail.get("returncode")) is int else None),
                "logs": {stream: {field: metadata[field]
                                  for field in ("path", "bytes", "sha256")}
                         for stream, metadata in attempt["logs"].items()
                         if stream in {"stdout", "stderr"}},
            },
        }
        return {"integrity_error": diagnostic}


def _outcome_render_value(q, outcome_path: str, outcome: dict) -> dict:
    summary = outcome_summary(q, Path(outcome_path), outcome)
    attempts = q.attempt_outcomes(outcome) if summary["adopted"] is not None else []
    missing = outcome.get("attempt_history_missing_before", 0)
    return {
        "summary": summary,
        "attempts": attempts,
        "missing": missing,
        "total_attempts": outcome.get("attempts", len(attempts)),
        **_cached_replay_value(summary, attempts),
    }


def outcome_summary(q, outcome_path, outcome, *, verify_attempts=True) -> dict:
    """One ending reduced to the fields a caller reports, after verification.

    The mutable terminal summary is state-machine output.  Where the record
    links immutable attempt records, the first-writer-published attempt decides
    what this returns, so a later stale-output refusal cannot replace the
    causal failure on the submitter's screen.  ``attempt_ending``
    verifies every canonical path, digest, byte count and generation on the way
    through, and refuses a summary that disagrees with the attempt it adopted.

    Consumer projection may defer attempt verification to the bounded streaming
    ending reader with verify_attempts=False. It must not claim verification
    from this summary alone. Legacy callers retain full verification here.

    ``transport`` defaults to the pull queue: the SLURM lane stamps its own
    records and the pool's predate the field.
    """

    action_wait.require_execution_ending(outcome, where=str(outcome_path))
    detail = outcome.get("detail") or {}
    status = str(outcome.get("status"))
    adopted = None
    if verify_attempts and (
        "attempt_history" in outcome
        or "attempt_history_missing_before" in outcome
    ):
        adopted = q.attempt_ending(outcome)
        disposition = adopted["disposition"]
        # An ending recovered from its own immutable attempt is not filed in a
        # disposition directory at all -- the attempt path is its source, for
        # an ordinary generation and a preemption successor alike.  A mutable
        # terminal row must still sit in the directory its adopted
        # disposition names.
        archived_source = (
            isinstance(outcome.get("attempts"), int)
            and Path(outcome_path) == q.records.attempt_path(
                outcome, outcome["attempts"]))
        if not archived_source and disposition != Path(outcome_path).parent.name:
            raise _TerminalSummaryConflict(
                "terminal queue directory disagrees with the adopted immutable "
                f"attempt: {Path(outcome_path).parent.name!r} != {disposition!r}",
                adopted=adopted, fields=["disposition"],
            )
        conflicts = [field for field in ("status", "finished_unix", "finished_host", "detail")
                     if outcome.get(field) != adopted[field]]
        if conflicts:
            raise _TerminalSummaryConflict(
                "terminal queue summary disagrees with the adopted "
                f"immutable attempt field {conflicts[0]!r}",
                adopted=adopted, fields=conflicts,
            )
        detail = adopted["detail"]
        status = str(adopted["status"])
    claimed_host = outcome.get("claimed_host")
    if not isinstance(claimed_host, str) or not claimed_host:
        # A terminal record filed by a path that did not carry the field can
        # still have it in the immutable attempt, which ``archive_attempt``
        # stamps from the claim itself. Read the newest attempt that names a
        # box; never invent one, because "unknown" is the answer that sends
        # nobody anywhere.
        claimed_host = None
        if adopted is not None:
            for attempt in reversed(q.attempt_outcomes(outcome)):
                candidate = attempt.get("claimed_host")
                if isinstance(candidate, str) and candidate:
                    claimed_host = candidate
                    break
    withdrawn_attempt = None
    if "withdrawn_attempt" in outcome:
        retained = outcome["withdrawn_attempt"]
        if (not isinstance(retained, dict) or status != pool.WITHDRAWN
                or any(retained.get(field) != outcome.get(field) for field in
                       ("action_key", "published_unix", "max_attempts", "retry_safe"))):
            raise pool.PoolContractError("withdrawn attempt differs from its decision identity")
        stopped = (q.attempt_ending(retained) if verify_attempts else
                   {**retained, "attempt": retained.get("attempts"),
                    "disposition": pool.WITHDRAWN})
        if stopped["status"] != pool.WITHDRAWN or stopped["disposition"] != pool.WITHDRAWN:
            raise pool.PoolContractError("withdrawn attempt is not a concluded cancellation")
        for field in ("resource_scope_cleanup", "container_cleanup"):
            if stopped["detail"].get(field) != outcome.get(field):
                raise pool.PoolContractError("withdrawn cleanup differs from its immutable attempt")
        withdrawn_attempt = {field: stopped[field] for field in
                             ("attempt", "status", "disposition", "finished_unix", "finished_host")}
        withdrawn_attempt["outcome"] = retained["attempt_history"][-1]["outcome"]
    return {
        "action_key": str(outcome.get("action_key") or ""),
        "status": status,
        "detail": detail,
        "adopted": adopted,
        "withdrawn_attempt": withdrawn_attempt,
        "resource_scope_cleanup": outcome.get("resource_scope_cleanup"),
        "container_cleanup": outcome.get("container_cleanup"),
        # ``executed`` and ``cache_hit`` both mean the work is done; that is
        # the pull queue's own rule, in ``attempt_ending``, which
        # routes both to ``done/``.
        "succeeded": status in {"executed", "cache_hit"},
        "transport": str(outcome.get("transport") or "pool"),
        "finished_host": outcome.get("finished_host"),
        # The box the action was ON. ``finished_host`` is the box that
        # concluded it, and for a reaped claim those are different machines.
        "claimed_host": claimed_host,
        "elapsed_s": detail.get("elapsed_s"),
        # What the run cost and how loaded its box was (#372 Tier 0).  Carried
        # whole rather than flattened, because the record is what a reader
        # comes back to and the summary is only what fits on a line.
        "resource_profile": detail.get("resource_profile"),
        "returncode": detail.get("returncode"),
        # The action's own ending, where the transport recorded one.  The
        # launcher's status above is 1 for every failure, so an action that
        # exited 7 reads as 1 without this.
        "action_returncode": detail.get("action_returncode"),
        "action_signal": detail.get("action_signal"),
        "receipt_published": detail.get("receipt_published"),
        # Present as ``None`` on every unprofiled run so a reader tests one
        # field rather than the absence of one.
        "profile": detail.get("profile"),
        "attempts": outcome.get("attempts"),
        "withdrawn_by": outcome.get("withdrawn_by"),
        "reason": outcome.get("reason"),
    }


@dataclass(frozen=True)
class PoolClientOutcome:
    action_key: str
    observed: (action_wait.Ready[endings.VerifiedExecution | endings.PublicationSupersession]
               | action_wait.Unavailable | action_wait.Unreadable | action_wait.PatienceExpired)
    ending: ClientEnding
    submitter_host: str


def _pool_client_verdict(key: str, result) -> ClientVerdict:
    if isinstance(result, action_wait.Unreadable):
        return ClientUnreadable(key)
    if isinstance(result, action_wait.Unavailable):
        return ClientUnavailable(key, result.fault.generation, result.ending)
    if isinstance(result, action_wait.PatienceExpired):
        return ClientPatienceExpired(key, result.generation)
    completed = result.value
    if isinstance(completed, endings.PublicationSupersession):
        return ClientSuperseded(key, completed.published_unix, completed.successor_published_unix)
    rendered = completed.rendered
    summary = rendered['summary']
    detail = summary['detail']

    def numeric_fact(name):
        value = detail.get(name)
        return value if type(value) is int else None

    receipt_published = detail.get('receipt_published')
    return ClientExecution(key, result.generation, summary['status'],
        numeric_fact('returncode'), numeric_fact('action_returncode'), numeric_fact('action_signal'),
        summary['status'] == 'timeout',
        _pool_execution_returncode(summary['status'], detail.get('returncode')),
        rendered.get('cached_replay'), receipt_published=(
            receipt_published if type(receipt_published) is bool else None))


def _pool_execution_returncode(status: str, returncode: int | None) -> int:
    if status == 'withdrawn':
        return WITHDRAWN_EXIT
    if status == 'cache_hit':
        return 0
    if isinstance(returncode, int):
        return int(_execution_exit_code(returncode))
    return 0 if status == 'executed' else 1


def await_pool_outcome(
    q, key: str, *, patience: action_wait.WaitUntil | action_wait.ProbeOnce,
    generation: float | None = None,
    notice: Callable | None = None,
) -> PoolClientOutcome:
    """Return the observed ending and client verdict without rendering them."""
    host = socket.gethostname()

    def observe(*, budget_s, generation, remaining_s):
        watched = generation
        selected = watched
        stage = action_wait.ReadStage.OBSERVATION
        try:
            landed, selected = bounded_outcome_observation(
                q, key, watched, budget_s=budget_s,
                )
            if landed is None:
                return action_wait.Pending(selected, selected != watched)
            if isinstance(landed, endings.PublicationSupersession):
                return action_wait.Ready(landed, selected)
            stage = action_wait.ReadStage.VERIFICATION
            rendered = bounded_outcome_render(
                q, landed[0], landed[1], budget_s=action_wait.OUTCOME_READ_TIMEOUT_S,
                )
            return action_wait.Ready(endings.VerifiedExecution(landed, rendered), selected)
        except action_wait.OutcomeReadUnavailable as exc:
            return action_wait.ReadFault(exc, selected, stage)

    with action_wait.client_readers(join=True) as history:
        result = action_wait.await_action(
            observe, generation=generation, patience=patience,
            profile=action_wait.AwaitProfile.SYNCHRONOUS,
            tool='pbrun', subject=f'pool outcome for {key[:12]}',
            notice=notice if notice is not None else lambda _event: None)
        ending = ClientEnding(_pool_client_verdict(key, result), history.snapshot())
    return PoolClientOutcome(key, result, ending, host)


def report_pool_notice(key: str, event) -> None:
    error = event.fault.error
    if isinstance(event, action_wait.FinalReadNotice):
        print(f"pbrun: unavailable pool outcome for {key[:12]}: {error}",
              file=sys.stderr)
        return
    reaped = ('the reader has exited and was reaped, '
              if event.reader_reaped else '')
    print(f"pbrun: pool outcome for {key[:12]} not observed yet "
          f"({error}); {reaped}{event.count} consecutive unavailable "
          "read(s), retrying inside --wait-s", file=sys.stderr, flush=True)


def render_pool_outcome(outcome: PoolClientOutcome) -> None:
    """Render an already observed pool outcome with the existing CLI text."""
    key, result, host = outcome.action_key, outcome.observed, outcome.submitter_host
    if isinstance(result, action_wait.Unreadable):
        print(f"pbrun: unreadable ending for {key[:12]}: {result.error}", file=sys.stderr)
        return
    if isinstance(result, action_wait.Unavailable):
        error = result.fault.error
        if result.ending is action_wait.WaitEnd.RETAINED:
            still = [child.to_record() for child in result.retained]
            print(f"pbrun: unavailable pool outcome for {key[:12]}: "
                  f"{error}. That reader was still retained when "
                  f"--wait-s ran out, {result.waited_s:.1f}s after its read "
                  f"timed out (retained reader="
                  f"{sorted_json(still)}), so no second "
                  f"reader was started beside it. Nothing was cancelled. "
                  f"Read pb-queue/{{done,failed,withdrawn}}/{key[:12]}"
                  f"*.json or run pbwait.py {key[:12]}", file=sys.stderr, flush=True)
        elif result.ending is action_wait.WaitEnd.FINAL_OBSERVATION:
            print(f"pbrun: unavailable pool outcome for {key[:12]} on {host} "
                  f"when the terminal re-read ended: {error}. The action may "
                  f"still be running or may already have landed; nothing was "
                  f"cancelled. Read pb-queue/{{done,failed,withdrawn}}/"
                  f"{key[:12]}*.json or run pbwait.py {key[:12]}", file=sys.stderr)
        elif result.ending is action_wait.WaitEnd.FINAL_VERIFICATION:
            print(f"pbrun: unavailable pool outcome for {key[:12]} on "
                  f"{host} when its verification ended: {error}", file=sys.stderr)
        elif result.ending is action_wait.WaitEnd.FINAL_EMPTY:
            print(f"pbrun: unavailable pool outcome for {key[:12]} on {host}; "
                  f"the terminal re-read saw no ending either. The action may "
                  f"still be running or may already have landed; nothing was "
                  f"cancelled. Read pb-queue/{{done,failed,withdrawn}}/"
                  f"{key[:12]}*.json or run pbwait.py {key[:12]}", file=sys.stderr)
        elif result.ending is action_wait.WaitEnd.DEADLINE:
            print(f"pbrun: unavailable pool outcome for {key[:12]} when the wait "
                  f"ended: {result.count} consecutive unavailable read(s), "
                  f"the last: {error}. The action may still be running or "
                  "may already have landed; nothing was cancelled. Read "
                  f"pb-queue/{{done,failed,withdrawn}}/{key[:12]}*.json or run "
                  f"pbwait.py {key[:12]}", file=sys.stderr)
        else:
            print(f"pbrun: unavailable pool outcome for {key[:12]}: {error}", file=sys.stderr)
        return
    if isinstance(result, action_wait.PatienceExpired):
        print(f"pbrun: gave up waiting for {key[:12]}", file=sys.stderr)
        return
    completed = result.value
    if isinstance(completed, endings.PublicationSupersession):
        print(f"pbrun: {key[:12]} publication {completed.published_unix!r} superseded by "
              f"publication {completed.successor_published_unix!r}; no execution verdict",
              file=sys.stderr)
        return
    outcome_path, outcome = completed.ending
    rendered = completed.rendered
    summary = rendered["summary"]
    detail = summary["detail"]
    status = summary["status"]
    if summary["adopted"] is not None:
        attempts = rendered["attempts"]
        missing = rendered["missing"]
        if isinstance(missing, int) and not isinstance(missing, bool) and missing:
            noun = "attempt" if missing == 1 else "attempts"
            print(
                f"pbrun: {missing} earlier {noun} "
                "predates immutable history",
                file=sys.stderr,
            )
        total_attempts = int(rendered["total_attempts"])
        for attempt in attempts:
            print(
                f"pbrun: attempt {attempt['attempt']}/{total_attempts} "
                f"{attempt.get('status')} ({attempt.get('disposition')})",
                file=sys.stderr,
            )
            sys.stdout.write(str(attempt.get("stdout") or ""))
            sys.stderr.write(str(attempt.get("stderr") or ""))
    else:
        # Backward compatibility for outcomes filed by a pre-history runtime.
        sys.stdout.write(str(detail.get("stdout") or ""))
        sys.stderr.write(str(detail.get("stderr") or ""))
    if status == "withdrawn":
        who = summary.get("withdrawn_by") or "an operator"
        why = str(summary.get("reason") or "").strip()
        print(f"pbrun: withdrawn by {who}"
              f"{' -- ' + why if why else ''}", file=sys.stderr)
        return
    print(f"pbrun: {outcome_headline(summary)}", file=sys.stderr)
    if status == "cache_hit":
        return
    rc = detail.get("returncode")
    if isinstance(rc, int):
        reported_exit(rc, key=key)
        return
    if status == "executed":
        return
    # A failure the worker itself raised carries no returncode -- the argv's
    # status is inside the exception text.  Surface the text; the caller gets a
    # non-zero exit either way, but the text is what makes it actionable.
    error = str(detail.get("error") or detail.get("exception") or "").strip()
    if error:
        print(f"pbrun: {error}", file=sys.stderr)
    print(f"pbrun: outcome filed under {outcome_path.parent.name} after "
          f"{rendered.get('total_attempts', '?')} attempt(s)", file=sys.stderr)
    return


def report_pool_outcome(q, key: str, *, wait_s: float, generation: float | None = None,
                        report: ClientReport | None = None) -> int:
    """Report one typed pool result at the shell boundary."""
    deadline = action_wait.time.monotonic() + wait_s
    patience = (action_wait.ProbeOnce(deadline) if wait_s <= 0
                else action_wait.WaitUntil(deadline))
    outcome = await_pool_outcome(q, key, patience=patience, generation=generation,
        notice=lambda event: report_pool_notice(key, event))
    render_pool_outcome(outcome)
    if report is not None:
        report.conclude(outcome.ending)
    return outcome.ending.exit_code


def action_status_suffix(detail: Mapping[str, object]) -> str:
    """What to add to an outcome line when the action's status is not the run's.

    Nothing at all when the two agree, which is the ordinary case: an action
    that exited 3 under a transport that reports its own launcher's status
    would say the same number twice. When they differ -- the launcher exits 1
    for every failure -- the run's number stays first, because that is the one
    ``pbrun`` returns as its own exit status, and the action's is named as the
    action's.
    """

    action = detail.get("action_returncode")
    if not isinstance(action, int) or isinstance(action, bool):
        return ""
    if action == detail.get("returncode"):
        return ""
    signal = detail.get("action_signal")
    if isinstance(signal, int) and not isinstance(signal, bool):
        return f"; rc={detail.get('returncode')} (action killed by signal {signal})"
    return f"; rc={detail.get('returncode')} (action exited {action})"


def resource_suffix(detail: Mapping[str, object]) -> str:
    """What the run cost, on the line that says it finished, or nothing.

    Nothing rather than a row of dashes: a run whose box had no recorder should
    not spend a line saying so every time, and the record still carries the
    reason under ``detail.resource_profile.box_window``.
    """

    described = pool.describe_resource_profile(
        pool.resource_profile_summary(detail))
    return f"; {described}" if described != "-" else ""


def profile_suffix(detail: Mapping[str, object]) -> str:
    """Where a profiled run left its profile, said on the line that reports it.

    Only on a run that asked for one, so an ordinary ending is unchanged.  The
    phrasing itself lives in ``core.describe_profile`` so this and
    ``pbstatus`` abbreviate one digest the same way.
    """

    described = pb.describe_profile(detail.get("profile"))
    return f"; {described}" if described else ""


def outcome_headline(summary: Mapping[str, object]) -> str:
    """One line saying how a run ended, and where -- and who "where" is.

    ``finished_host`` is the box that *filed* the record, which for anything a
    worker ran is also the box that ran it.  For a claim the reaper concluded
    it is not: ``reap_stale`` stamps its own hostname there, correctly, and
    rendering that as the place the action failed sends an investigation at
    the machine that merely noticed.  It did, on 2026-09-06.

    The bias outlives the one wrong trip.  The box that runs most of the
    fleet's work reaps most of it too, so misattributed failures accumulate on
    the box that already looks busiest and the fleet's failure profile leans
    toward whichever box reaps.  That is a measurement defect: it corrupts the
    evidence used to decide which box is unhealthy.

    ``detail["lease_age_s"]`` is the discriminator, not the status text.  The
    reaper is its only producer, it is on every record the reaper files
    whatever status it chose, and a gate can read it.  Its value carries a
    second distinction the operator needs: a number is a lease that stopped
    being refreshed, while ``None`` means no lease was ever written -- the
    claim was lost before the worker launched anything.
    """

    detail = summary.get("detail") or {}
    status = str(summary.get("status"))
    finished = summary.get("finished_host") or "(not recorded)"
    if "lease_age_s" not in detail:
        # ``elapsed_s`` is present and null on a SLURM record whose scheduler
        # provenance was purged, so the key's presence must not defeat the
        # default.
        return (f"{status} on {finished} "
                f"in {(detail.get('elapsed_s') or 0):.0f}s"
                f"{action_status_suffix(detail)}"
                f"{resource_suffix(detail)}"
                f"{profile_suffix(detail)}")
    held = summary.get("claimed_host")
    age = detail.get("lease_age_s")
    if isinstance(age, (int, float)) and not isinstance(age, bool):
        lease = f"lease {float(age):.1f}s stale"
    else:
        lease = "no lease was ever written"
    return (f"{status} -- held by {held or '(not recorded)'}, "
            f"reaped by {finished}, {lease}")


def reported_exit(returncode: int, *, key: str) -> int:
    """One run's recorded status as ``pbrun``'s own exit status.

    Every status but ``pbrun``'s own reserved words passes through unchanged,
    which is the contract callers already have: an action that exits 7 exits
    7.  One that lands on a reserved word is reported as 1, an ordinary
    failure, with the real number said on a line of its own so nothing is
    hidden.  The record keeps it under ``detail.returncode`` either way, and
    ``pbstatus`` and ``pbwait`` print it from there.

    Clamping rather than renumbering, because the codes have readers this
    cannot see: ``pbcampaign``, the harness, and whatever an operator wrapped
    ``pbrun`` in.  Every existing condition keeps the code it had, and the one
    case that was ambiguous stops being ambiguous.

    Args:
        returncode: The status the terminal record recorded for the run.
        key: The action key, for the prefix every fleet tool takes.

    Returns:
        ``returncode``, or 1 when returning it would impersonate a verdict
        ``pbrun`` did not reach.
    """

    selected = _execution_exit_code(returncode)
    if selected == returncode:
        return selected
    print(f"pbrun: {key[:12]} exited {returncode}, which is one of pbrun's own "
          f"exit codes; reporting it as 1 so it is not read as pbrun's "
          f"verdict. The run's own status is detail.returncode in the record.",
          file=sys.stderr)
    return selected
