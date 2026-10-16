"""Bounded action observations and the lifetime of their retained readers."""
from __future__ import annotations

import contextlib
import contextvars
import json
from pathlib import Path
import socket
import sys
import time

from . import _bounded_reader as _reader, pool

POLL_S = 5.0
OUTCOME_READ_TIMEOUT_S = 5.0
UNAVAILABLE_NOTICE_INTERVAL_S = 60.0


class UnreadableTerminal(ValueError):
    """A published ending exists but cannot supply a verdict."""

    def __init__(self, path: Path, reason: str):
        self.path = path
        self.reason = reason
        super().__init__(f"{reason}: {path}")


class OutcomeReadUnavailable(RuntimeError):
    """A bounded pool outcome read did not return a trustworthy result."""


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

    def __init__(self, message: str, retained: list[dict]) -> None:
        super().__init__(message)
        self.retained = [dict(child) for child in retained]


def bounded_read(section: str, read, *, budget_s: float,
                 use_delivered_snapshot: bool = False):
    """Return one pool reader's value or raise without continuing a wait.

    A reader retained without delivering a payload is unsafe to ignore: its
    exit and resources are no longer known, and it is the exact reader a later
    poll would otherwise race with. Refuse before launching another reader,
    naming its PID/starttime pair in the error so an operator can identify it.

    A reader retained *after* delivering its payload is a different event, and
    only the outcome path opts into treating it that way
    (``use_delivered_snapshot``).  EOF on the pipe is the proof the payload is
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
    result = _reader.bounded(
        section, read, deadline=_reader.Deadline(budget_s), abandoned=abandoned,
        announce_retained=False)
    retained = ("; retained reader=" + json.dumps(abandoned, sort_keys=True)
                if abandoned else "")
    if result.get("status") == "ok":
        if abandoned and not use_delivered_snapshot:
            raise OutcomeReadUnavailable(
                f"{section} reader could not be reaped{retained}")
        if abandoned:
            print(f"pbrun: {section} payload delivered but its reader could "
                  f"not be reaped on {socket.gethostname()}{retained}; using "
                  f"the delivered snapshot", file=sys.stderr, flush=True)
            lingering = _DELIVERED_READERS.get()
            if lingering is not None:
                # The waiting caller reaps it later, without blocking, so a
                # long wait does not collect a zombie per poll (#1048).
                lingering.extend(abandoned)
        return result.get("value")
    if abandoned:
        if result.get("status") == "timed_out":
            # Transient, like a reaped timeout, but only once this reader is
            # gone: the caller that retries waits for it first (#1033).
            raise OutcomeReaderRetained(
                f"{section} timed out after {result.get('elapsed_s', budget_s)}s "
                f"and its reader could not be reaped on "
                f"{socket.gethostname()}{retained}", abandoned)
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
    # ``await_outcome`` even though their originating read now lived in a child.
    if kind in {"PoolContractError", "_TerminalSummaryConflict"}:
        raise pool.PoolContractError(message)
    if kind == pool.StaleAbsenceError.__name__:
        # Listed, not yet visible on this box (#1100); the reader is reaped.
        raise OutcomeNotYetVisible(f"{section}: {message}")
    raise OutcomeReadUnavailable(f"{section} failed: {kind}: {message}")


def reap_retained(retained: list[dict], deadline: float, *,
                  on_wait=None) -> list[dict]:
    """Reap retained readers as they exit, until the deadline. Return the rest.

    Each one is this process's own child (``pbstatus`` forked it), so
    ``waitpid`` names it exactly and reaps it; a PID that is no longer this
    process's child is already gone. Nothing new starts meanwhile, which is
    what keeps a wait to one reader at a time (#1033). The cadence starts at
    the reap grace ``pbstatus`` already gave the reader and doubles up to the
    wait's own poll interval; the only bound is the caller's deadline.

    A hung mount can hold a reader for as long as the deadline allows, so the
    wait is not silent: ``on_wait(alive, waited_s)`` runs once every
    ``UNAVAILABLE_NOTICE_INTERVAL_S`` while a reader is still retained.
    """

    alive = [dict(child) for child in retained]
    delay = _reader.KILL_GRACE_S
    started = last_notice = time.monotonic()
    while True:
        alive = [child for child in alive
                 if not _reader._reap_within(int(child["pid"]), 0)]
        now = time.monotonic()
        remaining = deadline - now
        if not alive or remaining <= 0:
            return alive
        if on_wait is not None and now - last_notice >= UNAVAILABLE_NOTICE_INTERVAL_S:
            last_notice = now
            on_wait(alive, now - started)
        time.sleep(min(delay, remaining))
        delay = min(delay * 2, POLL_S)


def wait_out_retained_readers(retained: list[dict], deadline: float, *,
                              tool: str, subject: str) -> tuple[list[dict], float]:
    """Wait out a timed-out reader that could not be reaped, as every client does.

    ``await_outcome``, ``pbwait.wait_one`` and ``pbcampaign``'s window all
    meet :class:`OutcomeReaderRetained` and must not start a second reader
    beside the first (#1033, #1048). This is the one wait they share: reap
    the reader as it exits (:func:`_await_retained_readers`), saying so every
    ``UNAVAILABLE_NOTICE_INTERVAL_S`` as ``{tool}: {subject} not observed
    yet``, until the deadline.

    Returns ``(still_retained, waited_s)``. When the reader was reaped, the
    rest of one ``POLL_S`` is slept first, bounded by the deadline, so a
    retry after a reap reads no more often than a retry after a reaped
    timeout does. The caller then reads again only while time is left; a
    non-empty ``still_retained`` means the deadline came first, and nothing
    new may start.
    """

    started = time.monotonic()

    def waiting(alive, waited_s):
        print(f"{tool}: {subject} not observed yet: its reader is still "
              f"retained after {waited_s:.0f}s (retained reader="
              f"{json.dumps(alive, sort_keys=True)}); waiting for it to exit "
              "before reading again, inside --wait-s", file=sys.stderr, flush=True)

    still = reap_retained(retained, deadline, on_wait=waiting)
    waited_s = time.monotonic() - started
    if not still:
        time.sleep(max(0.0, min(POLL_S - waited_s, deadline - time.monotonic())))
    return still, waited_s


_DELIVERED_READERS: contextvars.ContextVar[list | None] = contextvars.ContextVar(
    "action_wait_delivered_readers", default=None)


@contextlib.contextmanager
def reader_session():
    """Keep delivered readers local to one wait and reap without delaying it."""
    lingering = []
    token = _DELIVERED_READERS.set(lingering)
    try:
        yield lingering
    finally:
        reap_retained(lingering, time.monotonic())
        _DELIVERED_READERS.reset(token)
