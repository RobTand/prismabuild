"""Pure ending questions over captured queue evidence."""
from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
import math
from pathlib import Path

from . import record_store, core as pb

DONE = "done"
FAILED = "failed"
WITHDRAWN = "withdrawn"


@dataclass(frozen=True)
class PublicationSupersession:
    """An exact publication ended; its successor supplies no execution verdict."""

    path: Path
    action_key: str
    published_unix: float
    successor_published_unix: float
    record: Mapping[str, object]


RecordedEnding = tuple[Path, dict[str, object]]
ObservedEnding = RecordedEnding | PublicationSupersession


@dataclass(frozen=True)
class VerifiedExecution:
    ending: RecordedEnding
    rendered: dict[str, object]


class _Uncaptured(Enum):
    INPUT = "not_captured"


@dataclass(frozen=True)
class RunCapture:
    """One run's observed facts; uncaptured inputs request their existing reads.

    This marker describes query progress, not a file's presence or readability.
    A captured absent publication or successor is ``None``; a captured empty
    set of records is ``()``. Filesystem failures retain their original errors.
    """

    terminals: tuple[RecordedEnding, ...]
    broken: tuple[Exception, ...] = ()
    publication_errors: tuple[Exception, ...] = ()
    withdrawals: tuple[RecordedEnding, ...] | _Uncaptured = _Uncaptured.INPUT
    archives: tuple[RecordedEnding, ...] | _Uncaptured = _Uncaptured.INPUT
    publication: PublicationSupersession | None | _Uncaptured = _Uncaptured.INPUT
    successor: float | None | _Uncaptured = _Uncaptured.INPUT


@dataclass(frozen=True)
class RunAnswer:
    ending: ObservedEnding | None
    generation: float | None


@dataclass(frozen=True)
class WithdrawalInputs:
    generation: float | None


@dataclass(frozen=True)
class ArchiveInputs:
    generation: float


@dataclass(frozen=True)
class PublicationInputs:
    generation: float


@dataclass(frozen=True)
class PreemptionInputs:
    generation: float
    ending: RecordedEnding


NeedsRunInputs = WithdrawalInputs | ArchiveInputs | PublicationInputs | PreemptionInputs


def preempted(ending: Mapping[str, object], generation: float | None) -> bool:
    return (generation is not None and ending.get("preempted_by") is not None
            and str(ending.get("status") or "") == WITHDRAWN)


def preemption_successor(
    ending: Mapping[str, object], generation: float,
    records: Sequence[Mapping[str, object]],
) -> float | None:
    """The earliest actual handoff from this stop, never an unrelated rerun."""
    successors = []
    for record in records:
        theirs = record.get("published_unix")
        parent = record.get("supersedes_withdrawal")
        if (type(theirs) not in (int, float)
                or not isinstance(parent, Mapping)
                or parent.get("published_unix") != generation
                or parent.get("preempted_by") != ending.get("preempted_by")):
            continue
        if float(theirs) > float(generation):
            successors.append(float(theirs))
    return min(successors) if successors else None


def resolve_run(capture: RunCapture, generation: float | None) -> RunAnswer | NeedsRunInputs:
    """Resolve the watched run, requesting only its conditionally needed inputs.

    Run equality and legacy fallback retain the waiter's historical rules.
    They differ from the finite ordering used by point-in-time status below.
    A request performs no I/O and never grants mutation or a fresh deadline.
    """
    if isinstance(capture.withdrawals, _Uncaptured):
        return WithdrawalInputs(generation)
    found = list(capture.terminals)
    for path, record in capture.withdrawals:
        if generation is None or float(record["published_unix"]) == float(generation):
            if not any(existing.get("status") == WITHDRAWN
                       and existing.get("published_unix") == record.get("published_unix")
                       for _, existing in found):
                found.append((path, record))
    if generation is not None and not any(
            record.get("published_unix") == generation for _, record in found):
        if isinstance(capture.archives, _Uncaptured):
            return ArchiveInputs(generation)
        found.extend(capture.archives)
    if generation is not None:
        exact = [entry for entry in found
                 if isinstance(entry[1].get("published_unix"), (int, float))
                 and not isinstance(entry[1].get("published_unix"), bool)
                 and float(entry[1]["published_unix"]) == float(generation)]
        if not exact:
            if isinstance(capture.publication, _Uncaptured):
                return PublicationInputs(generation)
            if capture.publication is not None:
                return RunAnswer(capture.publication, generation)
            if capture.publication_errors:
                raise capture.publication_errors[0]
        if not found:
            if capture.broken:
                raise capture.broken[0]
            return RunAnswer(None, generation)
        landed = (exact or found)[0]
        if preempted(landed[1], generation):
            if isinstance(capture.successor, _Uncaptured):
                return PreemptionInputs(generation, landed)
            if capture.successor is not None:
                return RunAnswer(None, capture.successor)
        return RunAnswer(landed, generation)
    if capture.publication_errors:
        raise capture.publication_errors[0]
    if len(found) == 1:
        return RunAnswer(found[0], generation)
    if found:
        def stamp(entry: RecordedEnding) -> float:
            value = entry[1].get("published_unix")
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                return float(value)
            return float("-inf")
        return RunAnswer(max(found, key=stamp), generation)
    if capture.broken:
        raise capture.broken[0]
    return RunAnswer(None, generation)


def publication_generation(value: object) -> float | None:
    """A record's generation as a finite float, or ``None``.

    A huge integer is ``None``, never an ``OverflowError``: hostile durable
    input stays in the pool's fail-closed error vocabulary (#1446).
    """

    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    try:
        number = float(value)
    except OverflowError:
        return None
    return number if math.isfinite(number) else None


def resolve_current(
    capture: record_store.TerminalCapture,
) -> dict[str, object]:
    """How this key ended, by generation, over one captured record set.

    Pure: no directory or record is read here, so the result can only
    describe the capture it was handed.  ``done/`` and ``failed/`` are
    one slot each and a key is a content hash, so two generations'
    records can stand together: records filed before #1117 retired the
    earlier row at conclusion, a crash between the new terminal's write
    and the retired row's unlink, an archive write that failed, or a
    terminal filed by a path that never archived.  Readers that each
    walked the directories in their own order disagreed in exactly that
    state (GLM Stage B layer 037: ``pbwait`` read ``executed``, a
    ``failed/``-first census read dead).

    The rule is ordering over validated generations.  The unique newest
    finite ``published_unix`` among the readable candidates answers, and
    the other readable candidates are ``history``.  ``supersedes_terminal``
    is historical evidence the writer filed, not current authority: it
    names the generation a later record retired, and a slot it points
    into may since have been replaced by a newer one, so it never
    decides.  A CONTESTED key generations cannot order -- a missing or
    malformed generation, a tie at the maximum, a third unorderable
    candidate, or a record that could not be read -- is never read as
    success: a readable failure or cancellation stands, or ``state`` is
    ``None`` when even that cannot be named, and ``ambiguous`` is true.

    Nothing is moved, deleted or rewritten.  A lone record is the key's
    ending even when its generation is malformed or absent:
    ``action_wait.terminal_record`` already states that a record carrying no
    generation stands, because staleness cannot be proved of it and
    refusing it would hang a reader on the only account of what
    happened.  The conservative rule above applies to a contest, not to
    a sole record.  This is a point-in-time
    status rule; a waiter bound to one generation keeps
    ``action_wait.outcome_poll`` and ``archived_generation_outcomes``, which
    are exact to that generation.
    """

    readable = {}
    unreadable = []
    for observation in capture.candidates:
        address, result = observation.address, observation.result
        if isinstance(result, record_store.Present):
            readable[address.state] = (address.path, result.value)
        elif isinstance(result, record_store.Unknown):
            exc = result.error
            unreadable.append({
                "state": address.state, "path": address.path,
                "reason": (str(exc) or type(exc).__name__) if exc is not None else result.reason})

    def generation(state: str) -> float | None:
        # The one finite-generation owner: an integer past the binary64
        # range is an unorderable generation (``None``), never an escaped
        # ``OverflowError`` out of a status census (#1446).
        return publication_generation(readable[state][1].get("published_unix"))

    present = [state for state in (DONE, FAILED, WITHDRAWN)
               if state in readable]
    unreadable_states = [str(entry["state"]) for entry in unreadable]
    coexisting = present + unreadable_states

    def result(state: str | None,
               *, ambiguous: bool = False) -> dict[str, object]:
        history = [
            {"state": other, "path": readable[other][0],
             "record": readable[other][1], "generation": generation(other)}
            for other in present if other != state
        ]
        if state is None:
            return {
                "state": None, "path": None, "record": None,
                "generation": None, "history": history,
                "unreadable": list(unreadable), "ambiguous": ambiguous,
                "coexisting": coexisting,
            }
        path, record = readable[state]
        return {
            "state": state, "path": path, "record": record,
            "generation": generation(state), "history": history,
            "unreadable": list(unreadable), "ambiguous": ambiguous,
            "coexisting": coexisting,
        }

    def failure() -> str | None:
        return next((state for state in (FAILED, WITHDRAWN)
                     if state in readable), None)

    if not coexisting:
        return result(None)
    if len(readable) == 1 and not unreadable:
        return result(present[0])

    orderable = [state for state in present if generation(state) is not None]
    if len(orderable) >= 2:
        generations = {state: generation(state) for state in orderable}
        newest = max(
            value for value in generations.values() if value is not None)
        winners = [state for state, value in generations.items()
                   if value == newest]
        if len(winners) == 1:
            winner = winners[0]
            if winner == DONE and len(orderable) != len(coexisting):
                # An unorderable or unreadable sibling stands against
                # the success; a unique maximum over every candidate
                # does not.
                return result(failure(), ambiguous=True)
            return result(winner, ambiguous=bool(unreadable))
        failure_winner = next(
            (state for state in winners if state != DONE), winners[0])
        return result(failure_winner, ambiguous=True)
    if len(orderable) == 1:
        only = orderable[0]
        if only == DONE and len(coexisting) > 1:
            return result(failure(), ambiguous=True)
        return result(only, ambiguous=len(coexisting) > 1)
    return result(failure() or (present[0] if present else None),
                  ambiguous=len(coexisting) > 1)


#: Inline cap on an action stream inside a mutable queue record (#1203).  The
#: immutable attempt log keeps the full stream and the record names it, so a
#: reader that needs the bytes follows the reference instead of the row.
TERMINAL_STREAM_TAIL_BYTES = 64 * 1024


def _terminal_stream_inline(raw: str) -> tuple[str, int, bool]:
    """Bounded inline copy of an action stream for a mutable record.

    Returns ``(inline, byte_count, cut)``.  The inline copy is the stream's
    last ``TERMINAL_STREAM_TAIL_BYTES`` bytes, decoded permissively so a code
    point the cut split still renders; the byte count and whether the copy
    was cut travel beside it so a reader can tell a tail from a whole stream
    (#1203).
    """
    data = raw.encode("utf-8")
    if len(data) <= TERMINAL_STREAM_TAIL_BYTES:
        return raw, len(data), False
    tail = data[-TERMINAL_STREAM_TAIL_BYTES:]
    return tail.decode("utf-8", errors="replace"), len(data), True


def resolve_attempt(
    attempts: Sequence[Mapping[str, object]], *,
    invalid: Callable[[str], Exception],
    validate_output_retry_stop: Callable[[object, Mapping[str, object]], None],
) -> dict[str, object]:
    """Return the one mutable transition the immutable winner permits.

    A finisher and a stale reaper can both observe the same claim and race
    to publish one attempt number.  ``archive_attempt`` makes that evidence
    first-writer-wins; this method makes its status, disposition, detail,
    and provenance first-writer-wins too.  Both queue writers and readers
    use this rule so a mutable summary cannot route one cause while
    reporting or returning another.
    """

    if not attempts:
        raise invalid(
            "an attempt-backed queue record has no immutable outcome"
        )
    adopted = attempts[-1]
    status = adopted.get("status")
    disposition = adopted.get("disposition")
    if not isinstance(status, str) or not status:
        raise invalid("pool attempt status must be nonempty text")
    if not isinstance(disposition, str) or not disposition:
        raise invalid(
            "pool attempt disposition must be nonempty text"
        )
    attempt = adopted.get("attempt")
    max_attempts = adopted.get("max_attempts")
    if type(attempt) is not int or type(max_attempts) is not int:
        raise invalid("pool attempt transition has invalid bounds")
    succeeded = status in {"executed", "cache_hit"}
    stopped = "output_retry_stop" in adopted
    if stopped:
        if succeeded:
            raise invalid("successful attempt cannot stop an output retry")
        validate_output_retry_stop(adopted["output_retry_stop"], adopted)
    worker_failure_final = (adopted.get("retry_scope") == "interruption"
                            and adopted.get("worker_reported") is True)
    raw_detail = adopted.get("detail")
    if not isinstance(raw_detail, Mapping):
        raise invalid("pool attempt detail must be an object")
    observation = raw_detail.get("resource_telemetry")
    gpu_judgment = None
    if isinstance(observation, Mapping) and ("gpu_record" in observation or "gpu_use" in observation):
        gpu_judgment = pb.GpuUseJudgment.require(pb.gpu_execution_judgment(observation.get("gpu_record"),
            nonce=observation.get("nonce"), scope_id=observation.get("scope_id")))
    gpu_refusal_final = (raw_detail.get("termination_reason") == "zero_gpu_devices_used"
                         and gpu_judgment is not None and gpu_judgment["gpu_use"] == "zero")
    expected = (
        WITHDRAWN if status == WITHDRAWN else
        DONE if succeeded else FAILED if (stopped or worker_failure_final or gpu_refusal_final
                                         or attempt >= max_attempts) else "requeued"
    )
    if disposition != expected:
        raise invalid(
            f"pool attempt {attempt} status {status!r} requires "
            f"disposition {expected!r}, not {disposition!r}"
        )
    finished_unix = adopted.get("finished_unix")
    if (
        isinstance(finished_unix, bool)
        or not isinstance(finished_unix, (int, float))
        or not math.isfinite(float(finished_unix))
    ):
        raise invalid("pool attempt finished_unix must be finite")
    finished_host = adopted.get("finished_host")
    if not isinstance(finished_host, str) or not finished_host:
        raise invalid("pool attempt finished_host must be nonempty text")
    detail = dict(raw_detail)
    if gpu_judgment is not None:
        detail.update(resource_telemetry=gpu_judgment.as_dict(), gpu_use=gpu_judgment["gpu_use"],
                      gpu_use_reason=gpu_judgment["gpu_use_reason"])
    logs = adopted.get("logs")
    for stream in ("stdout", "stderr"):
        # Only the bounded tail lives in the mutable record; the immutable
        # attempt log beside it keeps the whole stream and is named so a
        # reader can still fetch every byte (#1203).
        inline, full_bytes, cut = _terminal_stream_inline(
            str(adopted.get(stream) or ""))
        detail[stream] = inline
        detail[f"{stream}_bytes"] = full_bytes
        detail[f"{stream}_truncated"] = cut
        metadata = logs.get(stream) if isinstance(logs, Mapping) else None
        if isinstance(metadata, Mapping):
            detail[f"{stream}_log"] = dict(metadata)
    return {
        "attempt": attempt,
        "status": status,
        "disposition": disposition,
        "output_retry_stopped": stopped,
        "finished_unix": finished_unix,
        "finished_host": finished_host,
        **({"served_from": adopted["served_from"], "resident_set": adopted.get("resident_set")}
           if "served_from" in adopted else {}),
        "detail": detail,
    }
