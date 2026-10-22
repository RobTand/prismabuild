"""Bounded action observations and the lifetime of their retained readers."""
from __future__ import annotations

import contextlib
import contextvars
from dataclasses import dataclass, field, replace
from enum import Enum
import json
import math
import os
from pathlib import Path
import socket
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from typing import Generic, Protocol, TypeVar

from . import _bounded_reader as _reader, core as pb, endings, pool, queue_bell, record_store
from .digest_primitives import sorted_json

POLL_S = 5.0
OUTCOME_READ_TIMEOUT_S = 5.0
UNAVAILABLE_NOTICE_INTERVAL_S = 60.0

T = TypeVar('T')


class AwaitProfile(Enum):
    SYNCHRONOUS = 'synchronous'
    DETACHED = 'detached'


@dataclass(frozen=True)
class WaitUntil:
    deadline: float


@dataclass(frozen=True)
class ProbeOnce:
    deadline: float


@dataclass(frozen=True)
class RetainedReader:
    section: str
    pid: int
    starttime_ticks: int | None
    since_unix: int | float

    @classmethod
    def from_record(cls, record: Mapping[str, object]) -> RetainedReader:
        section = record.get('section')
        pid = record.get('pid')
        ticks = record.get('starttime_ticks')
        since = record.get('since_unix')
        if (set(record) != {'section', 'pid', 'starttime_ticks', 'since_unix'}
                or not isinstance(section, str) or not section.strip()
                or type(pid) is not int or pid <= 0
                or (ticks is not None and type(ticks) is not int)
                or type(since) not in (int, float)):
            raise OutcomeReadUnavailable('invalid retained reader identity')
        try:
            finite = math.isfinite(since)
        except OverflowError:
            finite = False
        if not finite:
            raise OutcomeReadUnavailable('invalid retained reader timestamp')
        return cls(section, pid, ticks, since)

    def to_record(self) -> dict:
        return {'section': self.section, 'pid': self.pid,
                'starttime_ticks': self.starttime_ticks, 'since_unix': self.since_unix}


@dataclass(frozen=True)
class ReaderHistory:
    abandoned_count: int = 0
    identities: tuple[RetainedReader, ...] = ()
    complete: bool = True


@dataclass
class ClientReaderHistory:
    abandoned_count: int = 0
    identities: list[RetainedReader] = field(default_factory=list)
    complete: bool = True

    def record_abandoned(self, records: Sequence[Mapping[str, object]]) -> None:
        for record in records:
            self.abandoned_count += 1
            if len(self.identities) >= 8:
                self.complete = False
                continue
            try:
                identity = RetainedReader.from_record(record)
            except OutcomeReadUnavailable:
                self.complete = False
            else:
                self.identities.append(identity)

    def snapshot(self) -> ReaderHistory:
        return ReaderHistory(self.abandoned_count, tuple(self.identities), self.complete)


_CLIENT_READERS: contextvars.ContextVar[ClientReaderHistory | None] = contextvars.ContextVar(
    'action_client_reader_history', default=None)


@contextlib.contextmanager
def client_readers(*, join: bool = False):
    """Keep abandonment for the whole client, even after a child is reaped."""
    existing = _CLIENT_READERS.get()
    if join and existing is not None:
        yield existing
        return
    history = ClientReaderHistory()
    token = _CLIENT_READERS.set(history)
    try:
        yield history
    finally:
        _CLIENT_READERS.reset(token)


def client_reader_history() -> ReaderHistory:
    history = _CLIENT_READERS.get()
    return history.snapshot() if history is not None else ReaderHistory(complete=False)


@dataclass
class AwaitState:
    generation: float | None
    profile: AwaitProfile
    retained: list[RetainedReader] = field(default_factory=list)
    lingering: list[RetainedReader] = field(default_factory=list)


@dataclass(frozen=True)
class Pending(Generic[T]):
    generation: float | None
    handoff: bool = False
    diagnostic: T | None = None


@dataclass(frozen=True)
class Ready(Generic[T]):
    value: T
    generation: float | None


class ReadStage(Enum):
    OBSERVATION = 'observation'
    VERIFICATION = 'verification'


@dataclass(frozen=True)
class ReadFault(Generic[T]):
    error: OutcomeReadUnavailable
    generation: float | None
    stage: ReadStage = ReadStage.OBSERVATION
    diagnostic: T | None = None


@dataclass(frozen=True)
class Unreadable:
    error: UnreadableTerminal


class WaitEnd(Enum):
    UNRETRIED = 'unretried'
    DEADLINE = 'deadline'
    RETAINED = 'retained'
    FINAL_OBSERVATION = 'final_observation'
    FINAL_VERIFICATION = 'final_verification'
    FINAL_EMPTY = 'final_empty'


@dataclass(frozen=True)
class Unavailable(Generic[T]):
    fault: ReadFault[T]
    count: int
    ending: WaitEnd
    retained: tuple[RetainedReader, ...] = ()
    waited_s: float = 0.0
    had_patience: bool = False


@dataclass(frozen=True)
class PatienceExpired(Generic[T]):
    generation: float | None
    diagnostic: T | None = None


@dataclass(frozen=True)
class RetryNotice(Generic[T]):
    fault: ReadFault[T]
    count: int
    reader_reaped: bool


@dataclass(frozen=True)
class FinalReadNotice(Generic[T]):
    fault: ReadFault[T]


class UnreadableTerminal(ValueError):
    """A published ending exists but cannot supply a verdict."""

    def __init__(self, path: Path, reason: str):
        self.path = path
        self.reason = reason
        super().__init__(f"{reason}: {path}")


class OutcomeReadUnavailable(RuntimeError):
    """A bounded pool outcome read did not return a trustworthy result."""


class RetainedIdentityUnavailable(OutcomeReadUnavailable):
    """An invalid reader record still blocks recovery and retains its raw identity."""

    def __init__(self, message: str, records: list[dict]):
        self.records = [dict(record) for record in records]
        super().__init__(message + '; retained reader=' + sorted_json(self.records))


class OutcomeObservationTimedOut(OutcomeReadUnavailable):
    """A bounded read ran out of budget, and its reader was killed and reaped.

    This is the one unavailable read that a caller with patience left may
    repeat at once. No reader is left behind, so the next reader cannot race an
    earlier one. A reader that failed and an invalid reply stay plain
    ``OutcomeReadUnavailable`` and end the wait; a timed-out reader that could
    not be reaped yet is :class:`OutcomeReaderRetained`. Every existing
    ``except OutcomeReadUnavailable`` also catches both subclasses, so a caller
    that does not retry keeps its exit 74.
    """


class OutcomeNotYetVisible(OutcomeObservationTimedOut):
    """A landed ending's attempt record is listed but not visible here yet.

    The reader raised ``pool.StaleAbsenceError``: every directory on the
    record's canonical path lists it, and this box's NFS client still answers
    ``ENOENT`` from a lookup it cached before the record was created (#1100).
    The reader exited and was reaped, so a caller with patience left repeats
    the read exactly as it repeats a reaped timeout; at zero patience, or at
    the deadline, it stays exit 74.  A record that no listing shows is not
    this error, and it still ends the wait at once.
    """


class OutcomeReaderRetained(OutcomeReadUnavailable):
    """A bounded read ran out of budget, and its reader is still in the kernel.

    The reader was sent SIGKILL and has not exited: a read on a hard NFS mount
    finishes its RPC before the signal lands. It is this process's own child,
    so ``waitpid`` reaps it the moment that read returns. ``retained`` names
    it by PID and ``starttime`` exactly as ``pbstatus`` recorded it.

    A caller with patience left waits for that exit and then reads again
    (#1033), so no second reader ever starts beside it. Before #1033 this ended
    the wait with 74 even with hours of ``--wait-s`` left: four shards of one
    broad ``pbtest`` run read red while their actions had landed rc 0.
    """

    def __init__(self, message: str,
                 retained: Sequence[RetainedReader | Mapping[str, object]]) -> None:
        super().__init__(message)
        try:
            self.readers = tuple(child if isinstance(child, RetainedReader)
                                 else RetainedReader.from_record(child) for child in retained)
        except OutcomeReadUnavailable as exc:
            records = [child.to_record() if isinstance(child, RetainedReader)
                       else dict(child) for child in retained]
            raise RetainedIdentityUnavailable(str(exc), records) from exc

    @property
    def retained(self) -> list[dict]:
        return [child.to_record() for child in self.readers]


def read_observation(section: str, read, *, budget_s: float):
    """Return one pool reader's value or raise without continuing a wait.

    A reader retained without delivering a payload is unsafe to ignore: its
    exit and resources are no longer known, and it is the exact reader a later
    poll would otherwise race with. Refuse before launching another reader,
    naming its PID/starttime pair in the error so an operator can identify it.

    A reader retained *after* delivering its payload is a different event, and
    only the outcome path opts into treating it that way
    (the synchronous profile). EOF on the pipe is the proof the payload is
    whole, and the child holds nothing but that pipe (``_isolate_child_fds``
    closed everything else before the section ran, so no lock or descriptor
    survives it).  The reap grace is 0.25 s, and a parent starved of CPU under
    load misses it with the data already in hand -- which is #630, three
    finished shards reported unobserved for actions already in ``done/``.
    Discarding a complete, valid payload over a scheduling artifact is the
    bug; the outcome path returns it.  A later poll then runs a new
    independent read-only child, which is safe here because the retained one
    can deliver nothing more: its pipe is closed and ``outcome_poll`` mutates
    no queue state.  Every other reader -- attachment discovery, prefix scans,
    ``pbwait`` -- keeps the strict refusal, because what follows their payload
    is a scheduler query or a documented ``record_error``, not a printout.
    """

    abandoned: list[dict] = []
    # Every refusal below names the retained reader, so ``pbstatus`` need not:
    # its own line is not throttled, and a waiting client meets a retained
    # reader once per turn of its wait (#1048).
    try:
        result = _reader.bounded(
            section, read, deadline=_reader.Deadline(budget_s), abandoned=abandoned,
            announce_retained=False)
    finally:
        history = _CLIENT_READERS.get()
        if history is not None:
            history.record_abandoned(abandoned)
    try:
        readers = [RetainedReader.from_record(child) for child in abandoned]
    except OutcomeReadUnavailable as exc:
        raise RetainedIdentityUnavailable(str(exc), abandoned) from exc
    state = _AWAIT_STATE.get()
    retained = ("; retained reader=" + sorted_json(abandoned)
                if abandoned else "")
    if result.get("status") == "ok":
        if abandoned and (state is None or state.profile is not AwaitProfile.SYNCHRONOUS):
            raise OutcomeReadUnavailable(
                f"{section} reader could not be reaped{retained}")
        if abandoned:
            print(f"pbrun: {section} payload delivered but its reader could "
                  f"not be reaped on {socket.gethostname()}{retained}; using "
                  f"the delivered snapshot", file=sys.stderr, flush=True)
            if state is not None:
                # The waiting caller reaps it later, without blocking, so a
                # long wait does not collect a zombie per poll (#1048).
                state.lingering.extend(readers)
        return result.get("value")
    if abandoned:
        if result.get("status") == "timed_out":
            # Transient, like a reaped timeout, but only once this reader is
            # gone: the caller that retries waits for it first (#1033).
            raise OutcomeReaderRetained(
                f"{section} timed out after {result.get('elapsed_s', budget_s)}s "
                f"and its reader could not be reaped on "
                f"{socket.gethostname()}{retained}", readers)
        kind = str(result.get("type") or "RuntimeError")
        message = str(result.get("error") or "reader failed")
        raise OutcomeReadUnavailable(
            f"{section} failed ({kind}: {message}) and its reader could not be "
            f"reaped on {socket.gethostname()}{retained}")
    if result.get("status") == "timed_out":
        # ``abandoned`` is empty here: ``_reader._stop_reader`` killed and
        # reaped this reader (or never started one), so a later read cannot
        # race it. Only this case is transient.
        raise OutcomeObservationTimedOut(
            f"{section} timed out after {result.get('elapsed_s', budget_s)}s")
    kind = str(result.get("type") or "RuntimeError")
    message = str(result.get("error") or "reader failed")
    # These two historical errors remain meaningful to callers of
    # ``report_pool_outcome`` even though their originating read now lived in a child.
    if kind in {"PoolContractError", "_TerminalSummaryConflict"}:
        raise pool.PoolContractError(message)
    if kind == pool.StaleAbsenceError.__name__:
        # Listed, not yet visible on this box (#1100); the reader is reaped.
        raise OutcomeNotYetVisible(f"{section}: {message}")
    raise OutcomeReadUnavailable(f"{section} failed: {kind}: {message}")


def reap_retained(retained: Sequence[RetainedReader]) -> list[RetainedReader]:
    """Make exactly one nonblocking own-child reap pass."""
    return [child for child in retained if not _reader._reap_within(child.pid, 0)]


def wait_out_retained_readers(retained: Sequence[RetainedReader], deadline: float, *,
                              tool: str, subject: str
                              ) -> tuple[list[RetainedReader], float]:
    """Settle a timed-out reader before a retry, inside the original deadline.

    Kernel-blocked NFS readers can outlive SIGKILL. Own-child waitpid is the
    authority. The reap grace doubles to the poll interval; an exited reader
    still consumes the rest of that interval before the caller retries.
    """
    clock = time
    started = clock.monotonic()
    last_notice = clock.monotonic()
    delay = _reader.KILL_GRACE_S
    alive = list(retained)
    while True:
        alive = reap_retained(alive)
        now = clock.monotonic()
        remaining = deadline - now
        if not alive or remaining <= 0:
            break
        if now - last_notice >= UNAVAILABLE_NOTICE_INTERVAL_S:
            last_notice = now
            waited_s = now - started
            records = [child.to_record() for child in alive]
            print(f"{tool}: {subject} not observed yet: its reader is still "
                  f"retained after {waited_s:.0f}s (retained reader="
                  f"{sorted_json(records)}); waiting for it to exit "
                  "before reading again, inside --wait-s", file=sys.stderr, flush=True)
        clock.sleep(min(delay, remaining))
        delay = min(delay * 2, POLL_S)
    waited_s = clock.monotonic() - started
    if not alive:
        clock.sleep(max(0.0, min(POLL_S - waited_s, deadline - clock.monotonic())))
    return alive, waited_s


_AWAIT_STATE: contextvars.ContextVar[AwaitState | None] = contextvars.ContextVar(
    'action_wait_state', default=None)


@contextlib.contextmanager
def reader_session(state: AwaitState):
    """Keep delivered readers local to one wait and reap without delaying it."""
    token = _AWAIT_STATE.set(state)
    try:
        yield state
    finally:
        try:
            reap_retained(state.lingering)
        finally:
            _AWAIT_STATE.reset(token)


class Observer(Protocol[T]):
    def __call__(self, *, budget_s: float, generation: float | None,
                 remaining_s: Callable[[], float]
                 ) -> Ready[T] | Pending[T] | ReadFault[T]: ...


def remaining_until(deadline: float) -> float:
    """Remaining caller patience at the instant a parent-side effect starts."""
    return max(0.0, deadline - time.monotonic())


def await_action(
    observe: Observer[T],
    *, generation: float | None, patience: WaitUntil | ProbeOnce,
    profile: AwaitProfile, tool: str, subject: str,
    notice: Callable[[RetryNotice[T] | FinalReadNotice[T]], None],
    bell: queue_bell.Client | None = None, bell_key: str = "",
    use_bell: Callable[[], bool] = lambda: True,
) -> Ready[T] | Unavailable[T] | Unreadable | PatienceExpired:
    """Await one identity with its original patience and one reader lifetime.

    Ordinary readers retain the existing own-child waitpid contract. Persistent
    scope-owned readers keep their separate durable fence protocol. Pool
    integrity failures and BaseException cancellation propagate through cleanup.

    A positive synchronous deadline starts no read after expiry (#938). Its
    zero-patience probe keeps one final bounded reread on failure. A detached
    waiter always makes its first read and follows a causal handoff at expiry.
    Verification has its own finite budget after an ending is observed.

    Hard NFS reads can outlive SIGKILL (#1033). Patient callers wait for that
    exact child before another read and report progress while it remains
    retained (#1048). Listed but not yet visible attempt records retry inside
    the same deadline (#1100). Only synchronous reporting accepts a complete
    delivered payload whose reader missed its reap grace (#630).
    """
    clock = time
    if not isinstance(profile, AwaitProfile):
        raise ValueError('unknown await profile')
    if isinstance(patience, ProbeOnce) and profile is not AwaitProfile.SYNCHRONOUS:
        raise ValueError('ProbeOnce belongs to the synchronous await profile')
    state = AwaitState(generation, profile)
    first = True
    unavailable = None
    count = 0
    last_notice = None
    deadline = patience.deadline
    had_patience = deadline > clock.monotonic()
    diagnostic = None
    fallback_s = min(0.25, POLL_S)
    unusable_replies = 0
    cursor = None

    def fail(fault, count, ending, retained=(), waited_s=0.0):
        return Unavailable(fault, count, ending, retained, waited_s, had_patience)

    def remaining_s():
        return remaining_until(deadline)

    def poll(budget_s):
        try:
            return observe(budget_s=budget_s, generation=state.generation, remaining_s=remaining_s)
        except UnreadableTerminal as exc:
            return Unreadable(exc)
        except OutcomeReadUnavailable as exc:
            return ReadFault(exc, state.generation)

    def retry(fault, *, reader_reaped):
        nonlocal last_notice
        now = clock.monotonic()
        if last_notice is None or now - last_notice >= UNAVAILABLE_NOTICE_INTERVAL_S:
            last_notice = now
            notice(RetryNotice(fault, count, reader_reaped))

    def final_read(fault):
        notice(FinalReadNotice(fault))
        result = poll(OUTCOME_READ_TIMEOUT_S)
        if isinstance(result, ReadFault):
            ending = (WaitEnd.FINAL_VERIFICATION
                      if result.stage is ReadStage.VERIFICATION else WaitEnd.FINAL_OBSERVATION)
            return fail(result, count, ending)
        if isinstance(result, Pending):
            return fail(fault, count, WaitEnd.FINAL_EMPTY)
        return result

    with reader_session(state):
        while True:
            state.lingering[:] = reap_retained(state.lingering)
            if (profile is AwaitProfile.SYNCHRONOUS
                    and not (first and isinstance(patience, ProbeOnce))):
                remaining = deadline - clock.monotonic()
                if remaining <= 0:
                    return (fail(unavailable, count, WaitEnd.DEADLINE)
                            if unavailable is not None else PatienceExpired(state.generation, diagnostic))
                budget_s = min(OUTCOME_READ_TIMEOUT_S, remaining)
            else:
                budget_s = OUTCOME_READ_TIMEOUT_S
            result = poll(budget_s)
            first = False
            if isinstance(result, (Ready, Unreadable)):
                return result
            state.generation = result.generation
            if isinstance(result, ReadFault):
                unavailable, count = result, count + 1
                patient = isinstance(patience, WaitUntil)
                error = result.error
                if isinstance(error, RetainedIdentityUnavailable):
                    return fail(result, count, WaitEnd.UNRETRIED)
                if isinstance(error, OutcomeObservationTimedOut) and patient:
                    if clock.monotonic() >= deadline:
                        return fail(result, count, WaitEnd.DEADLINE)
                    retry(result, reader_reaped=False)
                    clock.sleep(max(0.0, min(POLL_S, deadline - clock.monotonic())))
                    continue
                if isinstance(error, OutcomeReaderRetained) and patient:
                    state.retained[:] = error.readers
                    state.retained[:], waited_s = wait_out_retained_readers(
                        state.retained, deadline, tool=tool, subject=subject)
                    if state.retained or (profile is AwaitProfile.DETACHED
                                          and clock.monotonic() >= deadline):
                        return fail(result, count, WaitEnd.RETAINED,
                                           tuple(state.retained), waited_s)
                    retry(result, reader_reaped=True)
                    continue
                if profile is AwaitProfile.SYNCHRONOUS:
                    return final_read(result)
                return fail(result, count, WaitEnd.UNRETRIED)
            unavailable, count = None, 0
            diagnostic = result.diagnostic
            if result.handoff and profile is AwaitProfile.DETACHED:
                continue
            if isinstance(patience, ProbeOnce):
                return PatienceExpired(state.generation, diagnostic)
            if clock.monotonic() >= deadline:
                return PatienceExpired(state.generation, diagnostic)
            if result.handoff:
                continue
            if bell is not None and use_bell():
                try:
                    if cursor is None:
                        # Optional cursor I/O must not precede the first record
                        # read. Confirm after initialization: a publication
                        # between that read and now() is behind the new cursor.
                        cursor = bell.now(budget_s=remaining_s())
                        continue
                    started_wait = clock.monotonic()
                    reply = bell.wait(cursor, queue_bell.TERMINAL_DIRS, (bell_key,), remaining_s())
                    # Each reply supplies the cursor for the next observation
                    # and wait, including resets and empty timeouts. Reread
                    # directly on wake; another now() could spend the last of
                    # the caller's patience before that authoritative read.
                    cursor = queue_bell.Cursor.parse(f"{reply['epoch']}:{reply['seq']}")
                    if not reply.get("events") and clock.monotonic() - started_wait < 0.25:
                        unusable_replies += 1
                        if unusable_replies > 1:
                            raise queue_bell.Unavailable("repeated immediate empty/reset replies")
                    else:
                        unusable_replies = 0
                        fallback_s = min(0.25, POLL_S)
                    continue
                except queue_bell.Unavailable:
                    pass
            # Unavailable-callback backoff: an unreachable or repeatedly unusable
            # bell, or SLURM submission discovery, is a service
            # that cannot call back. Recheck with bounded backoff up to POLL_S.
            clock.sleep(max(0.0, min(fallback_s, remaining_s())))
            fallback_s = min(fallback_s * 2, POLL_S)


def terminal_record(
    path: Path, generation: float | None, *, report_unreadable: bool = False,
):
    """The record filed at ``path``, when it belongs to ``generation``.

    An action key is a content hash, so one key accumulates the endings of
    every run of the same work.  ``published_unix`` equality is the queue's own
    generation rule -- ``PoolQueue.terminal_outcome_covers`` states it, and
    ``slurm_lane._same_generation`` applies it on the other transport -- so a
    reader waiting for one run must skip the record of another.

    A record carrying no generation stands.  ``PoolQueue.finish`` writes one
    with no ``published_unix`` when a reaper concluded the claim underneath it,
    and records filed before generations were stamped have none either;
    staleness cannot be proved of those, and refusing them would hang a caller
    on the outcome that is the only account of what happened.

    With ``report_unreadable=True``, a present but unreadable record raises
    ``UnreadableTerminal``. Nonblocking compatibility probes keep returning
    ``None``; waiters opt in so corruption is not reported as a timeout.
    """

    observation = record_store.RecordStore.wait_terminal_at(path)
    if isinstance(observation, record_store.Absent):
        return None
    if isinstance(observation, record_store.Unknown):
        if report_unreadable:
            exc = observation.error
            if exc is None:
                raise UnreadableTerminal(path, observation.reason)
            reason = (str(exc.strerror or type(exc).__name__).lower()
                      if isinstance(exc, OSError) else "not valid JSON")
            raise UnreadableTerminal(path, reason) from exc
        return None
    record = observation.value
    if generation is None:
        return record
    theirs = record.get("published_unix")
    if isinstance(theirs, (int, float)) and not isinstance(theirs, bool):
        return record if float(theirs) == float(generation) else None
    return record


def _read_preemption_successor(q, key: str, ending, generation) -> float | None:
    """The generation a preemption requeued this one as, or ``None``.

    ``PoolQueue`` stops an admitted background holder to admit foreground work
    and immediately re-publishes it (#364).  The requeue is a NEW generation --
    it has to be, because the cancellation it revives is generation-scoped and
    would otherwise cover its own retry -- so a waiter watching the stopped
    generation sees a withdrawal and would report exit 143 for work the queue
    is about to run again.  That would make "retried, not lost" true of the
    queue and false of everyone waiting on it.

    Deliberately narrow.  It follows only an ending stamped ``preempted_by``,
    which nothing but admission writes, and only to a generation the queue
    actually holds -- waiting to run, running, or already ended.  An operator's
    withdrawal still ends the wait; so does a preemption whose requeue was
    never published, because then there is no newer generation to find and the
    cancellation is the whole account of what happened.

    The terminal directories are searched as well as the live ones, because a
    caller that arrives after the requeued run finished would otherwise be told
    about the stop and never about the ending that followed it.
    """

    if not endings.preempted(ending, generation):
        return None
    # Admission holds this lock across withdrawal and replacement publication.
    # Reading the marker before the replacement exists is an intermediate
    # transition, not evidence that the preempted action was abandoned.
    with q.action_transition_lock(key):
        records = [record for _, record in q.records.withdrawal_decisions(key)]
        records.extend(record for _, record in q.archived_preemption_outcomes(key))
        for state in (pool.READY, pool.CLAIMED, pool.DONE, pool.FAILED,
                      pool.WITHDRAWN):
            record = q.records.wait_notice_at(q.records.item_path(state, key))
            if isinstance(record, dict):
                records.append(record)
        # Preserve this reader's historical JSON-object intake. The pure
        # handoff policy also serves inspection's broader Mapping capture.
        candidates = [record for record in records
                      if isinstance(record.get("supersedes_withdrawal"), dict)]
        return endings.preemption_successor(ending, generation, candidates)


def outcome_poll(
    q, key: str, generation: float | None, *, report_unreadable: bool = False,
) -> tuple[endings.ObservedEnding | None, float | None]:
    """Capture this poll's facts and fulfill the pure run query's needed reads.

    The query preserves exact-generation, legacy fallback and preemption
    ordering. Archives and publication proofs are read only when requested.
    A new poll always starts with a fresh capture. Handoff reads retain the
    existing transition lock; no reader mutates or republishes queue state.
    """
    watched = q.records.wait_ending_addresses(key)
    found = []
    broken = []
    publication_errors = []
    for path in watched:
        try:
            record = terminal_record(
                path, generation, report_unreadable=report_unreadable)
        except UnreadableTerminal as exc:
            broken.append(exc)
            continue
        if record is not None:
            try:
                require_execution_ending(record, where=str(path))
            except OutcomeReadUnavailable as exc:
                publication_errors.append(exc)
            else:
                found.append((path, record))
    capture = endings.RunCapture(tuple(found), tuple(broken), tuple(publication_errors))
    while True:
        answer = endings.resolve_run(capture, generation)
        if isinstance(answer, endings.RunAnswer):
            return answer.ending, answer.generation
        if isinstance(answer, endings.WithdrawalInputs):
            decisions = (q.records.withdrawal_decisions(key) if answer.generation is None
                         else q.records.withdrawal_decisions(key, generation=answer.generation))
            capture = replace(capture, withdrawals=tuple(decisions))
        elif isinstance(answer, endings.ArchiveInputs):
            capture = replace(capture, archives=tuple(q.archived_generation_outcomes(
                key, generation=answer.generation)))
        elif isinstance(answer, endings.PublicationInputs):
            try:
                publication = q.publication_ending(key, generation=answer.generation)
            except (OSError, pb.PrismaBuildError) as exc:
                raise OutcomeReadUnavailable(f"unavailable publication ending: {exc}") from exc
            capture = replace(capture, publication=publication)
        elif isinstance(answer, endings.PreemptionInputs):
            capture = replace(capture, successor=_read_preemption_successor(
                q, key, answer.ending[1], answer.generation))
        else:
            raise AssertionError(f"unknown run input request: {answer!r}")


def require_execution_ending(record: Mapping[str, object], *, where: str) -> None:
    if (record.get("status") == "superseded"
            or record.get("schema") == pool.PUBLICATION_SUPERSEDED_SCHEMA_V1):
        raise OutcomeReadUnavailable(
            f"publication evidence cannot supply an execution ending: {where}")


def await_ending(
    q, key: str, *, wait_s: float, generation: float | None = None,
    report_unreadable: bool = False,
) -> endings.ObservedEnding | None:
    """Await a raw ending inside the caller's existing observation boundary."""
    def observe(*, budget_s, generation, remaining_s):
        landed, selected = outcome_poll(
            q, key, generation, report_unreadable=report_unreadable)
        return (Ready(landed, selected) if landed is not None
                else Pending(selected, selected != generation))

    deadline = time.monotonic() + wait_s
    result = await_action(
        observe, generation=generation, patience=WaitUntil(deadline),
        profile=AwaitProfile.DETACHED, tool='pbrun', subject=key[:12],
        notice=lambda _event: None, bell=queue_bell.Client(q.records.root), bell_key=key)
    if isinstance(result, Ready):
        return result.value
    if isinstance(result, Unreadable):
        raise result.error
    if isinstance(result, Unavailable):
        raise result.fault.error
    return None
