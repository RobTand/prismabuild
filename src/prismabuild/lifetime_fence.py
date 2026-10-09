"""Opt-in sealed lifetime contract v1: an enforced, evidenced release bound (#1429).

The sealed ``execution_timeout_s`` is a payload execution budget only.
Checkout runs before its deadline starts, checkpoint I/O and other waits
credit it, and cleanup settles after it. It never proves that capacity
returns by any wall time, and this contract does not change it.

A submitter opts in with ``params.lifetime``. Publication stamps one clock
before any resource moves, and nothing credits or extends it:

* ``deadline_unix`` is ``published_unix + fence_s``. Every phase of the
  attempt has ended and the host tokens are back by then.
* ``stop_unix`` is ``deadline_unix - RELEASE_RESERVE_S``. Nothing launches
  from this instant, and the payload is stopped here at the latest. The
  reserve is what termination, cleanup, scope settlement and release use.

Each phase has one enforcing mechanism (:data:`COMPONENTS`). The worker
records the phase end beside that mechanism as it runs
(:class:`AttemptLog`), and a phase counts as enforced only when it ends
before its bound. The audit (:func:`release_bound`) reads those records back.
Admission (:func:`prospective_bound`) is finite only when every phase is
bounded for the candidate's shape; a phase that is not names its reason and
the verdict is UNKNOWN.

What the bound is not. It is the bound of this control flow when the kernel,
the broker, the local disk and the shared mount answer promptly. The reserve
is sized for answers that take seconds, not for the sum of every worst-case
timeout. A system call that never returns is outside it. Such an attempt keeps its tokens, its audit
reads UNKNOWN, and a delay can still reach the measurement. Once the original
opportunity has passed, no later bound precedes it, so one overrun does not
admit further backfill. A timer never returns capacity; only the proved
settlement does. The payload stop kills the payload; it releases nothing.

Validation lives in :mod:`prismabuild.core` beside the other sealed params,
so ``validate_action`` refuses an unreadable fence at seal time. This module
owns the clock, the phase table, the evidence shape, the stop alarm and the
admission predicate.
"""
from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
import math
import threading
import time
from typing import NamedTuple

from . import core as pb

#: Sealed request key that carries the opt-in lifetime fence. Mirrored
#: from :mod:`prismabuild.core` so sealed bytes and reader agree by import.
LIFETIME_PARAM = pb.LIFETIME_PARAM
#: Versioned contract name. A future fence format is a new name.
#: Old workers refuse a sealed lifetime they do not read.
LIFETIME_SCHEMA_V1 = pb.LIFETIME_SCHEMA_V1
#: Capability tag a lifetime action requires of its claiming box. A box offers
#: it only when its loop enforces this contract.
LIFETIME_TAG = pb.LIFETIME_TAG
#: Versioned evidence record the worker files per attempt.
EVIDENCE_SCHEMA_V1 = "prismabuild.action_lifetime_evidence.v1"
#: Termination reason the fence stop files, distinct from the payload
#: budget's ``execution_deadline`` so a reader tells which bound fired.
FENCE_TERMINATION_REASON = "lifetime_fence"
#: Seconds between the stop instant and the deadline (contract v1).
RELEASE_RESERVE_S = pb.LIFETIME_RELEASE_RESERVE_S
#: Shortest fence the contract seals, in seconds: the reserve plus one minute.
MIN_FENCE_S = pb.LIFETIME_MIN_FENCE_S
#: Longest fence the contract seals, in seconds. A longer fence
#: is not an admission planning bound.
MAX_FENCE_S = pb.LIFETIME_MAX_FENCE_S
#: How often the stop alarm re-reads the clock, in seconds.
ALARM_POLL_S = 1.0

#: A phase that must end before ``stop_unix``: nothing launches after it.
STOP = "stop"
#: A phase that must end before ``deadline_unix``.
RELEASE = "release"


class Component(NamedTuple):
    """One applicable phase, its bound and the mechanism that enforces it."""

    phase: str
    bound: str
    mechanism: str


#: The contract: every phase from admission to resource release, in order.
#: The audit accepts a phase only under the mechanism named here, so a record
#: from a path without that mechanism (an uncontained run has no scope to
#: stop or settle) can never read as enforced.
COMPONENTS = (
    Component("admission", STOP, "claim-gate"),
    Component("checkout", STOP, "deadline-bounded-checkout"),
    Component("readiness", STOP, "launch-gate"),
    Component("prelaunch", STOP, "launch-gate"),
    Component("payload", RELEASE, "stop-alarm"),
    Component("credited_waits", RELEASE, "absolute-stop-clock"),
    Component("termination", RELEASE, "scope-stop"),
    Component("cleanup", RELEASE, "fail-closed-cleanup"),
    Component("scope_settlement", RELEASE, "exact-scope-proof"),
    Component("resource_release", RELEASE, "release-after-proof"),
)
PHASES = tuple(component.phase for component in COMPONENTS)
#: Phases the execution supervisor records while the attempt runs.
EXECUTION_PHASES = PHASES[:7]
#: Phases ``finish`` records after the run, from settlement evidence.
SETTLEMENT_PHASES = PHASES[7:]
_COMPONENT = {component.phase: component for component in COMPONENTS}


def _finite_seconds(value: object) -> float | None:
    if (
        type(value) in (int, float)
        and not isinstance(value, bool)
        and math.isfinite(float(value))
    ):
        return float(value)
    return None


@dataclass(frozen=True)
class Clock:
    """The two absolute instants of one fenced publication."""

    published_unix: float
    fence_s: float
    deadline_unix: float
    stop_unix: float

    def launch_expired(self, now_unix: object) -> bool:
        """Whether no payload may start now. An unreadable clock refuses."""

        now = _finite_seconds(now_unix)
        return now is None or now >= self.stop_unix

    def bound(self, phase: str) -> float:
        """The instant by which ``phase`` must end."""

        return self.stop_unix if _COMPONENT[phase].bound == STOP else self.deadline_unix


def clock(*, published_unix: object, fence_s: object) -> Clock | None:
    """The fence clock, or ``None`` when UNKNOWN.

    The publication stamp exists before resource admission. Both ends must
    be finite numbers and the fence must leave room before the stop instant.
    """

    published = _finite_seconds(published_unix)
    fence = _finite_seconds(fence_s)
    if published is None or fence is None or fence <= RELEASE_RESERVE_S:
        return None
    deadline = published + fence
    stop = deadline - RELEASE_RESERVE_S
    if not (math.isfinite(deadline) and math.isfinite(stop)):
        return None
    return Clock(published, fence, deadline, stop)


def fence_deadline(*, published_unix: object, fence_s: object) -> float | None:
    """The absolute release deadline, or ``None`` when UNKNOWN."""

    fence_clock = clock(published_unix=published_unix, fence_s=fence_s)
    return None if fence_clock is None else fence_clock.deadline_unix


def required_tags(lifetime: Mapping[str, object] | None) -> list[str]:
    """The capability a lifetime action requires of its claiming box."""

    if lifetime is None:
        return []
    return [LIFETIME_TAG]


def fence_seconds(lifetime: Mapping[str, object] | None) -> float | None:
    """The sealed fence in seconds, or ``None`` when unfenced."""

    if not isinstance(lifetime, Mapping):
        return None
    fence = lifetime.get("fence_s")
    if (
        type(fence) not in (int, float)
        or isinstance(fence, bool)
        or not math.isfinite(float(fence))
        or float(fence) <= 0
    ):
        return None
    return float(fence)


def attempt_identity(record: Mapping[str, object]) -> dict[str, object]:
    """Bind evidence to the publication, owner, and exact resource scope."""

    scope = record.get("resource_scope") or record.get("resource_scope_intent")
    scope = scope if isinstance(scope, Mapping) else {}
    return {
        **{field: record.get(field) for field in (
            "action_key", "published_unix", "claimed_by",
            "claimed_unix", "claimed_host")},
        "nonce": scope.get("nonce"),
        "scope_id": scope.get("scope_id"),
    }


def components_support(
    *, gang: bool = False, scratch: bool = False,
) -> dict[str, str | None]:
    """Per phase: ``None`` when the shape is bounded, else why it is UNKNOWN.

    A shape the contract does not cover names the phase it leaves unfenced.
    A shape not listed here runs the same bounded path as any other.
    """

    support: dict[str, str | None] = {phase: None for phase in PHASES}
    if gang:
        support["admission"] = (
            "a gang member waits on sibling claims that no fence bounds")
    if scratch:
        support["cleanup"] = "declared scratch is removed without a deadline"
    return support


def prospective_bound(
    *,
    published_unix: object,
    fence_s: object,
    components: Mapping[str, object] | None,
    now_unix: object,
) -> float | None:
    """The prospective release bound for backfill, or ``None`` when UNKNOWN.

    Finite only when the clock is readable, the stop instant is still ahead,
    and every phase of the contract is bounded (:func:`components_support`).
    A phase missing from ``components`` or carrying a reason is UNKNOWN, so
    one unfenced component makes the whole verdict UNKNOWN.
    """

    fence_clock = clock(published_unix=published_unix, fence_s=fence_s)
    if fence_clock is None or not isinstance(components, Mapping):
        return None
    if fence_clock.launch_expired(now_unix):
        return None
    if any(phase not in components or components[phase] is not None
           for phase in PHASES):
        return None
    return fence_clock.deadline_unix


class AttemptLog:
    """One attempt's per-phase record: the audit's only input.

    ``end`` files a phase with the mechanism that bounded it and the instant
    it ended. ``enforced`` is derived, never asserted: the mechanism must be
    the one the contract names for that phase, and the end must fall before
    the phase's bound. The launch gate refuses at the stop instant itself, so
    a phase that ends exactly there did not end in time.
    """

    def __init__(
        self,
        fence_clock: Clock,
        claim: Mapping[str, object] | None = None,
        *,
        phases: Mapping[str, Mapping[str, object]] | None = None,
        expired_phase: str | None = None,
    ) -> None:
        self.clock = fence_clock
        #: The attempt's identity (:func:`attempt_identity`). It is bound just
        #: before the record is filed: the scope nonce exists only after prelaunch.
        self.claim: dict[str, object] = dict(claim or {})
        self.phases: dict[str, dict[str, object]] = {
            phase: dict(entry) for phase, entry in (phases or {}).items()}
        self.expired_phase = expired_phase

    @classmethod
    def from_record(cls, record: object) -> "AttemptLog | None":
        """Re-open a filed record, or ``None`` when it is not v1 or disagrees."""

        if not isinstance(record, Mapping) or record.get("schema") != EVIDENCE_SCHEMA_V1:
            return None
        fence_clock = clock(published_unix=record.get("published_unix"),
                            fence_s=record.get("fence_s"))
        phases = record.get("phases")
        claim = record.get("claim")
        if (fence_clock is None or not isinstance(phases, Mapping)
                or not isinstance(claim, Mapping)
                or record.get("deadline_unix") != fence_clock.deadline_unix
                or record.get("stop_unix") != fence_clock.stop_unix
                or record.get("reserve_s") != RELEASE_RESERVE_S
                or not all(isinstance(entry, Mapping) for entry in phases.values())):
            return None
        expired = record.get("expired_phase")
        return cls(fence_clock, claim, phases=phases,
                   expired_phase=expired if isinstance(expired, str) else None)

    def end(self, phase: str, *, mechanism: str | None, evidence: str,
            ended_unix: object) -> bool:
        """File one phase. Returns whether it counts as enforced."""

        ended = _finite_seconds(ended_unix)
        bound = self.clock.bound(phase)
        enforced = (mechanism == _COMPONENT[phase].mechanism
                    and ended is not None and ended < bound)
        self.phases[phase] = {
            "enforced": enforced,
            "mechanism": mechanism,
            "evidence": evidence,
            "bound_unix": bound,
            "ended_unix": ended,
        }
        return enforced

    def refuse(self, phase: str, *, now_unix: object, evidence: str) -> None:
        """File a phase that ran out of time before the launch and refused it.

        ``expired_phase`` names only such a refusal. A payload stopped at the
        stop instant kept the contract, and its record says so on ``payload``.
        """

        self.end(phase, mechanism=_COMPONENT[phase].mechanism,
                 evidence=evidence, ended_unix=now_unix)
        self.expired_phase = phase

    def as_record(self) -> dict[str, object]:
        record: dict[str, object] = {
            "schema": EVIDENCE_SCHEMA_V1,
            "fence_s": self.clock.fence_s,
            "published_unix": self.clock.published_unix,
            "deadline_unix": self.clock.deadline_unix,
            "stop_unix": self.clock.stop_unix,
            "reserve_s": RELEASE_RESERVE_S,
            "claim": dict(self.claim),
            "phases": {phase: dict(entry) for phase, entry in self.phases.items()},
        }
        if self.expired_phase is not None:
            record["expired_phase"] = self.expired_phase
        return record


def release_bound(
    *,
    published_unix: object,
    fence_s: object,
    evidence: Mapping[str, object] | None,
) -> float | None:
    """The audited release bound of one finished attempt, or ``None``.

    Every phase of the contract must be present, enforced under the mechanism
    the contract names, backed by an evidence string, and ended before its
    bound. The record's own clock must equal the sealed clock. A missing
    phase, a foreign mechanism, an end at or past its bound or absent
    evidence answers ``None``. This audit never becomes a prospective bound for
    another attempt.
    """

    fence_clock = clock(published_unix=published_unix, fence_s=fence_s)
    log = AttemptLog.from_record(evidence)
    if fence_clock is None or log is None or log.clock != fence_clock:
        return None
    for component in COMPONENTS:
        entry = log.phases.get(component.phase)
        if entry is None:
            return None
        ended = _finite_seconds(entry.get("ended_unix"))
        bound = fence_clock.bound(component.phase)
        if (entry.get("enforced") is not True
                or entry.get("mechanism") != component.mechanism
                or not isinstance(entry.get("evidence"), str)
                or not entry["evidence"]
                or entry.get("bound_unix") != bound
                or ended is None or ended >= bound):
            return None
    return fence_clock.deadline_unix


def timed_backfill_allowed(
    *,
    candidate_bound: object,
    original_opportunity: object,
) -> bool:
    """Whether a timed backfill may run before the original opportunity.

    The candidate bound must be finite and strictly before the original.
    Equality and later bounds refuse. UNKNOWN on either side refuses.
    Capacity and isolation gates stay in force beside this answer.
    """

    for value in (candidate_bound, original_opportunity):
        if (
            type(value) not in (int, float)
            or isinstance(value, bool)
            or not math.isfinite(float(value))
        ):
            return False
    return float(candidate_bound) < float(original_opportunity)


class StopAlarm:
    """Deliver the fence stop at ``stop_unix`` from a thread of its own.

    The supervisor reads the clock between synchronous calls, and one blocked
    call (a stalled shared mount) can hold it past the stop instant. The
    alarm waits on its own thread, so the stop does not depend on the
    supervisor's I/O. It only delivers the stop: the supervisor still
    concludes the attempt, and tokens still return only on settlement proof.

    ``stop`` runs at most once, under a lock, from whichever caller reaches
    :meth:`fire` first. A payload that has already ended is never stopped.
    """

    def __init__(
        self,
        stop_unix: float,
        stop: Callable[[], None],
        *,
        alive: Callable[[], bool],
        now: Callable[[], float] = time.time,
        poll_s: float | None = None,
    ) -> None:
        self._stop_unix = float(stop_unix)
        self._stop = stop
        self._alive = alive
        self._now = now
        self._poll_s = ALARM_POLL_S if poll_s is None else float(poll_s)
        self._lock = threading.Lock()
        self._cancel = threading.Event()
        self._thread = threading.Thread(
            target=self._run, name="lifetime-stop-alarm", daemon=True)
        #: When the stop was delivered, or ``None``.
        self.fired_unix: float | None = None
        #: What the stop raised, or ``None``.
        self.error: Exception | None = None

    def arm(self) -> None:
        self._thread.start()

    def _run(self) -> None:
        while not self._cancel.is_set():
            remaining = self._stop_unix - self._now()
            if remaining <= 0:
                self.fire()
                return
            self._cancel.wait(min(self._poll_s, remaining))

    def fire(self) -> bool:
        """Stop the payload now. True when the fence stopped it (now or earlier)."""

        with self._lock:
            if self.fired_unix is not None:
                return True
            if self._cancel.is_set() or not self._alive():
                return False
            self.fired_unix = self._now()
            try:
                self._stop()
            except Exception as exc:                          # noqa: BLE001
                self.error = exc
            return True

    def disarm(self) -> bool:
        """Cancel the alarm. True when it never stopped the payload."""

        self._cancel.set()
        with self._lock:
            quiet = self.fired_unix is None
        if self._thread.is_alive() and self._thread is not threading.current_thread():
            self._thread.join(timeout=5.0)
        return quiet
