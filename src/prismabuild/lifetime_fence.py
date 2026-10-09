"""Opt-in sealed lifetime fence, separate from the payload budget (#1429).

The sealed ``execution_timeout_s`` is a payload execution budget only.
Checkout runs before its deadline starts, checkpoint I/O and other waits
credit it, and cleanup settles after it. It never proves that capacity
returns by any wall time.

This module seals one absolute, non-creditable, versioned bound instead.
A submitter opts in with ``params.lifetime``. ``fence_deadline`` is the
single enforcement clock: the queue stamps it at publication from
``published_unix`` plus the sealed fence, before any worker claims the
row and before any resource moves. Every phase the worker runs --
checkout, readiness, prelaunch, payload including credited waits,
termination, cleanup, scope settlement, resource release -- must
conclude before it. Nothing credits it, pauses it, or extends it.
Admission grants timed backfill only on a verified prospective bound
strictly before the original opportunity. Every unfenced component
reads ``UNKNOWN``. Expired timers never return capacity; only proved
settlement does.

The prospective predicate (:func:`prospective_bound`) is what timed
backfill reads: support is a sealed, versioned declaration plus
claim-time enforcement, not a completion record. A finished attempt's
filed evidence audits whether the fence held; it never becomes a new
attempt's guarantee. A successor reads the same sealed bound from its
own publication stamp.

Validation lives in :mod:`prismabuild.core` beside the other sealed
params, so ``validate_action`` refuses an unreadable fence at seal time.
This module owns the enforcement clock, the evidence shape, and the
admission predicate.
"""
from __future__ import annotations

from collections.abc import Mapping
import math

from . import core as pb

#: Sealed request key that carries the opt-in lifetime fence. Mirrored
#: from :mod:`prismabuild.core` so sealed bytes and reader agree by import.
LIFETIME_PARAM = pb.LIFETIME_PARAM
#: Versioned contract name. A future fence format is a new name.
#: Old workers refuse a sealed lifetime they do not read.
LIFETIME_SCHEMA_V1 = pb.LIFETIME_SCHEMA_V1
#: Versioned fence clock. The v1 clock starts at publication; a future
#: clock is a new name and a new admission predicate.
FENCE_CLOCK_V1 = "prismabuild.action_lifetime_clock.v1"
#: Capability tag a lifetime action requires of its claiming box.
LIFETIME_TAG = pb.LIFETIME_TAG
#: Versioned evidence record the worker files per attempt.
EVIDENCE_SCHEMA_V1 = "prismabuild.action_lifetime_evidence.v1"
#: Termination reason the fence kill files, distinct from the payload
#: budget's ``execution_deadline`` so a reader tells which bound fired.
FENCE_TERMINATION_REASON = "lifetime_fence"

#: Phases the fence enforces with a bounded stop. A component outside
#: this list is unfenced by definition and answers ``UNKNOWN``.
#: Cleanup, scope settlement and resource release are not here: their
#: proof is filed by ``finish`` after the worker runs, so they audit
#: the attempt instead of gating the prospective bound.
ENFORCED_PHASES = (
    "admission",
    "checkout",
    "readiness",
    "prelaunch",
    "payload",
    "credited_waits",
    "termination",
)
#: Phases whose settlement proof ``finish`` files after the run. Each
#: record cites its own cleanup or release evidence; a bound backed by
#: these reads only from the attempt archive, never from a live claim.
AUDITED_PHASES = (
    "cleanup",
    "scope_settlement",
    "resource_release",
)
#: Every phase the contract names, enforced or audited.
PHASES = ENFORCED_PHASES + AUDITED_PHASES

#: Shortest fence the contract seals, in seconds. A shorter fence
#: cannot cover checkout, termination and settlement evidence.
MIN_FENCE_S = pb.LIFETIME_MIN_FENCE_S
#: Longest fence the contract seals, in seconds. A longer fence
#: is not an admission planning bound.
MAX_FENCE_S = pb.LIFETIME_MAX_FENCE_S


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


def _finite_number(value: object) -> float | None:
    if (
        type(value) in (int, float)
        and not isinstance(value, bool)
        and math.isfinite(float(value))
    ):
        return float(value)
    return None


def fence_deadline(*, published_unix: object, fence_s: object) -> float | None:
    """The absolute wall-clock bound, or ``None`` when UNKNOWN.

    One addition, never credited: ``published_unix + fence_s``. The
    publication stamp exists before any claim, so admission reads a
    prospective bound from the READY row itself. Both ends must be
    finite numbers. Anything else answers ``None``.
    """

    published = _finite_number(published_unix)
    fence = _finite_number(fence_s)
    if published is None or fence is None or fence <= 0:
        return None
    return published + fence


def prospective_bound(
    *,
    published_unix: object,
    fence_s: object,
    supported: object,
    now_unix: object,
) -> float | None:
    """The verified prospective release bound, or ``None`` when UNKNOWN.

    A finite bound needs a finite publication stamp, a sealed fence, a
    supported enforcement declaration for this exact clock, and a fence
    that has not expired yet. ``supported`` is the queue's own answer
    that the claiming box enforces the versioned contract; a missing
    tag, an unreadable row, a fenced gang member, or any unsupported
    shape answers ``None``. Callers render that as ``UNKNOWN`` and
    hold resources.
    """

    bound = fence_deadline(published_unix=published_unix, fence_s=fence_s)
    if bound is None or supported is not True:
        return None
    now = _finite_number(now_unix)
    if now is None or now >= bound:
        return None
    return bound


def release_bound(
    *,
    claimed_unix: object = None,
    published_unix: object = None,
    fence_s: object,
    evidence: Mapping[str, object] | None,
) -> float | None:
    """The audited release bound of one finished attempt, or ``None``.

    A finished attempt proves the fence held only when every named
    phase -- enforced and audited -- carries a record that names the
    evidence schema, carries ``enforced`` true, cites its enforcement
    proof, and records the phase end at or before the fence deadline.
    The deadline is publication-anchored; ``claimed_unix`` is accepted
    only as a legacy alias and never moves it. A missing phase, a
    failed phase, an end past the deadline, or absent evidence answers
    ``None``. Admission never calls this for a new attempt: it audits
    a finished one from the attempt archive.
    """

    anchor = published_unix if published_unix is not None else claimed_unix
    bound = fence_deadline(published_unix=anchor, fence_s=fence_s)
    if bound is None or not isinstance(evidence, Mapping):
        return None
    if evidence.get("schema") != EVIDENCE_SCHEMA_V1:
        return None
    phases = evidence.get("phases")
    if not isinstance(phases, Mapping):
        return None
    for phase in PHASES:
        record = phases.get(phase)
        if not isinstance(record, Mapping):
            return None
        if record.get("enforced") is not True:
            return None
        if record.get("evidence") in (None, "", [], {}):
            return None
        ended = _finite_number(record.get("ended_unix"))
        if ended is None or ended > bound:
            return None
    return bound


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
