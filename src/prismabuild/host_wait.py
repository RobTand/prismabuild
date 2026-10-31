"""Latched wake hints and named deadlines; record readers remain authoritative."""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
import math
from pathlib import Path
import threading
import time

from . import queue_bell
from .queue_bell_http import RequestCancellation


class WakeKind(Enum):
    ACTION = "action"
    PUBLICATION = "publication"
    PEER = "peer"
    TIER = "tier"
    GROUP = "group"
    CAPABILITY = "capability"
    RESOURCE = "resource"
    CONTROL = "control"


@dataclass(frozen=True)
class WakeInput:
    kind: WakeKind
    identity: str = ""


class DeadlineReason(Enum):
    POLICY_EXPIRY = "policy_expiry"
    EVIDENCE_EXPIRY = "evidence_expiry"
    RENEWAL = "renewal"
    UNAVAILABLE_SOURCE_RETRY = "unavailable_source_retry"
    NFS_VISIBILITY = "nfs_visibility"


@dataclass(frozen=True)
class RetryDeadline:
    reason: DeadlineReason
    monotonic_at: float

    def __post_init__(self):
        if not math.isfinite(self.monotonic_at):
            raise ValueError("deadline must be finite")


@dataclass(frozen=True)
class ClaimWait:
    inputs: frozenset[WakeInput] = frozenset()
    deadlines: tuple[RetryDeadline, ...] = ()


@dataclass(frozen=True)
class Coverage:
    complete: bool
    topics: frozenset[str] = frozenset()
    version: int | None = None
    diagnostic: str | None = None


@dataclass(frozen=True)
class BellSnapshot:
    cursor: queue_bell.Cursor
    coverage: Coverage


@dataclass(frozen=True)
class SourceGeneration:
    owner: object
    local: tuple[tuple[WakeInput, int], ...]
    reset_generation: int
    bell: BellSnapshot | None
    failure: str | None = None


class WakeReasonKind(Enum):
    CHANGED = "changed"
    DEADLINE = "deadline"
    STOP = "stop"
    RESET = "reset"
    UNAVAILABLE = "unavailable"


@dataclass(frozen=True)
class WakeReason:
    kind: WakeReasonKind
    inputs: frozenset[WakeInput] = frozenset()
    deadlines: tuple[RetryDeadline, ...] = ()
    hints: tuple[dict, ...] = ()
    coverage: Coverage = Coverage(True)
    diagnostic: str | None = None


class RecoveryBackoff:
    """Named retry deadlines clipped to the original operation's budget."""

    def __init__(self, reason: DeadlineReason, minimum_s: float, maximum_s: float):
        if not 0 < minimum_s <= maximum_s or not math.isfinite(maximum_s):
            raise ValueError("invalid recovery backoff")
        self.reason, self.minimum_s, self.maximum_s = reason, minimum_s, maximum_s
        self._delay = minimum_s

    @classmethod
    def nfs_visibility(cls):
        return cls(DeadlineReason.NFS_VISIBILITY, 0.05, 1.0)

    def next(self, now: float, *, operation_deadline: float | None = None) -> RetryDeadline:
        if self.reason is DeadlineReason.NFS_VISIBILITY and operation_deadline is None:
            raise ValueError("NFS visibility recovery requires its original read deadline")
        if operation_deadline is not None and not math.isfinite(operation_deadline):
            raise ValueError("operation deadline must be finite")
        when = now + self._delay
        self._delay = min(self.maximum_s, self._delay * 2)
        if operation_deadline is not None:
            when = min(when, operation_deadline)
        return RetryDeadline(self.reason, when)

    def reset(self):
        self._delay = self.minimum_s


class HostWaitHub:
    """One observation owner combines callbacks with one owned remote reader.

    Source adapters call notify when meaningful dependencies change. Consumers
    capture arm before their reads and acknowledge reconstructed indexes with
    reconciled. The later supervisor hub owns scanner fairness and IPC fanout.
    """

    def __init__(self, queue_root: Path | None = None, *, topics: tuple[str, ...] = (),
                 ids: tuple[str, ...] = (), client_factory=None,
                 clock: Callable[[], float] = time.monotonic, wait=None):
        if client_factory is None and queue_root is not None:
            client_factory = lambda cancellation: queue_bell.Client(
                queue_root, cancellation=cancellation)
        self._client_factory = client_factory
        self._topics, self._ids = topics, ids
        self._clock, self._wait = clock, wait
        self._condition = threading.Condition()
        self._owner = threading.Lock()
        self._identity = object()
        self._generations: dict[WakeInput, int] = {}
        self._stopped = False
        self._active: RequestCancellation | None = None
        self._reset_generation = 0
        self._reconciled_generation = -1 if client_factory is not None else 0
        self._coverage = Coverage(client_factory is None, diagnostic=(
            "initial reconstruction required" if client_factory is not None else None))
        self._recovery = RecoveryBackoff(DeadlineReason.UNAVAILABLE_SOURCE_RETRY, 0.25, 5.0)
        self._retry: RetryDeadline | None = None
        self._reset_seen = False
        self._stream_identity = None
        self._bell_snapshot: BellSnapshot | None = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    @property
    def coverage(self) -> Coverage:
        with self._condition:
            return self._coverage

    def notify(self, source: WakeInput):
        with self._condition:
            self._generations[source] = self._generations.get(source, 0) + 1
            self._condition.notify_all()

    def stop(self):
        with self._condition:
            self._stopped = True
            if self._active is not None:
                self._active.cancel()
            self._condition.notify_all()

    def close(self):
        self.stop()
        # The observation owner joins its remote reader before releasing this.
        with self._owner:
            pass

    def _check_owner(self, before: SourceGeneration):
        if before.owner is not self._identity:
            raise ValueError("source generation belongs to another hub")

    def reconciled(self, before: SourceGeneration) -> bool:
        self._check_owner(before)
        with self._condition:
            if (before.bell is None or not before.bell.coverage.complete
                    or before.reset_generation != self._reset_generation):
                return False
            self._reconciled_generation = before.reset_generation
            self._coverage = before.bell.coverage
            self._retry = None
            return True

    def arm(self) -> SourceGeneration:
        with self._owner:
            with self._condition:
                local = tuple(self._generations.items())
                reset = self._reset_generation
                if self._stopped or self._client_factory is None:
                    return SourceGeneration(self._identity, local, reset, None)
                if self._retry is not None and self._retry.monotonic_at > self._clock():
                    return SourceGeneration(self._identity, local, reset, None,
                                            self._coverage.diagnostic)
                if self._bell_snapshot is not None:
                    return SourceGeneration(self._identity, local, reset, self._bell_snapshot)
                cancellation = RequestCancellation()
                self._active = cancellation
            try:
                body = self._client_factory(cancellation).now_v2(self._topics)
                coverage = Coverage(True, frozenset(body["coverage"]["topics"]),
                                    body["coverage"]["version"])
                bell = BellSnapshot(queue_bell.Cursor(body["epoch"], body["seq"]), coverage)
                identity = (bell.cursor.epoch, coverage.version, coverage.topics)
                failure = None
                with self._condition:
                    if self._stream_identity is not None and identity != self._stream_identity:
                        failure = "bell epoch or coverage changed; reconstruction required"
                        self._degrade(failure)
                    self._stream_identity = identity
                    self._bell_snapshot = bell
                    reset = self._reset_generation
                return SourceGeneration(self._identity, local, reset, bell, failure)
            except queue_bell.Unavailable as exc:
                with self._condition:
                    if cancellation.cancelled:
                        return SourceGeneration(self._identity, local, reset, None)
                    self._degrade(str(exc))
                    reset = self._reset_generation
                return SourceGeneration(self._identity, local, reset, None, str(exc))
            finally:
                with self._condition:
                    self._active = None

    def _degrade(self, diagnostic: str):
        self._reset_generation += 1
        self._coverage = Coverage(False, diagnostic=diagnostic)
        if self._bell_snapshot is not None:
            old = self._bell_snapshot
            self._bell_snapshot = BellSnapshot(old.cursor, Coverage(
                False, old.coverage.topics, old.coverage.version, diagnostic))
        if self._retry is None:
            self._retry = self._recovery.next(self._clock())

    def _pause(self, delay):
        if self._wait is None:
            self._condition.wait(delay)
        else:
            self._wait(self._condition, delay)

    def wait_since(self, before: SourceGeneration, need: ClaimWait) -> WakeReason:
        self._check_owner(before)
        if not self._owner.acquire(blocking=False):
            raise RuntimeError("hub already has an observation owner")
        cancellation = RequestCancellation()
        reader = None
        result = []
        completed_bell = []
        error = []

        def read_bell():
            bell = before.bell
            try:
                client = self._client_factory(cancellation)
                while True:
                    with self._condition:
                        if self._stopped:
                            return
                        due = min((d.monotonic_at for d in need.deadlines), default=math.inf)
                        budget = min(queue_bell.MAX_WAIT_S, due - self._clock())
                        if budget <= 0:
                            return
                    started = self._clock()
                    body = client.wait_v2(bell.cursor, bell.coverage.version,
                                          self._topics, self._ids, budget)
                    with self._condition:
                        if body.get("reset") is True:
                            self._degrade("bell reset requires reconstruction")
                            self._bell_snapshot = None
                            if not self._reset_seen:
                                self._retry = None
                                self._recovery.reset()
                            self._reset_seen = True
                            result.append((WakeReasonKind.RESET, (), "bell reset requires reconstruction"))
                            return
                        hints = tuple(body["events"])
                        server_budget = budget - min(0.05, budget / 10)
                        if not hints and self._clock() - started < server_budget:
                            raise queue_bell.Unavailable("bell returned an immediate empty renewal")
                        self._recovery.reset()
                        self._retry = None
                        self._reset_seen = False
                        fresh = BellSnapshot(queue_bell.Cursor(body["epoch"], body["seq"]),
                            Coverage(True, bell.coverage.topics, bell.coverage.version))
                        if hints:
                            completed_bell.append(fresh)
                            result.append((WakeReasonKind.CHANGED, hints, None))
                            return
                        self._bell_snapshot = fresh
                        if not bell.coverage.complete:
                            result.append((WakeReasonKind.CHANGED, (),
                                           "bell callback recovered; reconstruction required"))
                            return
                        bell = fresh
            except queue_bell.Unavailable as exc:
                with self._condition:
                    if cancellation.cancelled:
                        return
                    self._degrade(str(exc))
                    result.append((WakeReasonKind.UNAVAILABLE, (), str(exc)))
            except BaseException as exc:
                with self._condition:
                    error.append(exc)
            finally:
                with self._condition:
                    self._condition.notify_all()

        try:
            with self._condition:
                self._active = cancellation
                captured = dict(before.local)
                pending_failure = before.failure
                if before.bell is not None and self._reconciled_generation == before.reset_generation:
                    pending_failure = None
                while True:
                    now = self._clock()
                    due = tuple(d for d in need.deadlines if d.monotonic_at <= now)
                    changed = frozenset(source for source in need.inputs
                        if self._generations.get(source, 0) != captured.get(source, 0))
                    hints = result[0][1] if result else ()
                    if completed_bell:
                        self._bell_snapshot = completed_bell[0]
                    if self._stopped:
                        return WakeReason(WakeReasonKind.STOP, deadlines=due, hints=hints,
                                          coverage=self._coverage)
                    if changed:
                        return WakeReason(WakeReasonKind.CHANGED, changed, due, hints=hints,
                                          coverage=self._coverage)
                    if due:
                        return WakeReason(WakeReasonKind.DEADLINE, deadlines=due, hints=hints,
                                          coverage=self._coverage)
                    if before.reset_generation != self._reset_generation and not result:
                        return WakeReason(WakeReasonKind.RESET, coverage=self._coverage,
                                          diagnostic=self._coverage.diagnostic)
                    if error:
                        raise error[0]
                    if result and (self._retry is None or self._retry.monotonic_at <= now):
                        kind, hints, diagnostic = result[0]
                        self._retry = None
                        return WakeReason(kind, hints=hints, coverage=self._coverage, diagnostic=diagnostic)
                    if pending_failure and (self._retry is None or self._retry.monotonic_at <= now):
                        retry = self._retry
                        self._retry = None
                        kind = WakeReasonKind.RESET if before.bell is not None else WakeReasonKind.UNAVAILABLE
                        return WakeReason(kind, deadlines=(retry,) if retry is not None else (),
                                          coverage=self._coverage, diagnostic=pending_failure)
                    if reader is None and before.bell is not None and not pending_failure:
                        reader = threading.Thread(target=read_bell, name="prismabuild-bell-wait")
                        reader.start()
                    deadlines = [d.monotonic_at for d in need.deadlines]
                    if self._retry is not None:
                        deadlines.append(self._retry.monotonic_at)
                    delay = max(0.0, min(deadlines) - now) if deadlines else None
                    # The callback service cannot notify during failure or immediate
                    # reset/empty loops. Its 0.25–5 s retry is cancelled by local
                    # changes, stop, or the caller's original named deadline.
                    self._pause(delay)
        finally:
            cancellation.cancel()
            if reader is not None:
                reader.join()
            with self._condition:
                self._active = None
            self._owner.release()
