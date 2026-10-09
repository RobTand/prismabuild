"""Opt-in sealed lifetime fence, separate from the payload budget (#1429).

The sealed ``execution_timeout_s`` is a payload execution budget only.
Checkout runs before its deadline starts, checkpoint I/O and other waits
credit it, and cleanup settles after it. It never proves that capacity
returns by any wall time.

This module seals one absolute, non-creditable, versioned bound instead.
A submitter opts in with ``params.lifetime``. ``fence_deadline`` is the
single enforcement clock: the worker stamps it at claim from ``claimed_unix``
plus the sealed fence, and every phase the worker runs -- checkout,
readiness, prelaunch, payload including credited waits, termination,
cleanup, scope settlement, resource release -- must conclude before it.
Nothing credits it, pauses it, or extends it. Admission grants timed
backfill only on a verified release bound strictly before the original
opportunity. Every unfenced component reads ``UNKNOWN``. Expired timers
never return capacity; only proved settlement does.

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
#: Capability tag a lifetime action requires of its claiming box.
LIFETIME_TAG = pb.LIFETIME_TAG
#: Versioned evidence record the worker files per attempt.
EVIDENCE_SCHEMA_V1 = "prismabuild.action_lifetime_evidence.v1"
#: Termination reason the fence kill files, distinct from the payload
#: budget's ``execution_deadline`` so a reader tells which bound fired.
FENCE_TERMINATION_REASON = "lifetime_fence"

#: Every phase the fence covers. A component outside this list is
#: unfenced by definition and answers ``UNKNOWN``.
PHASES = (
    "admission",
    "checkout",
    "readiness",
    "prelaunch",
    "payload",
    "credited_waits",
    "termination",
    "cleanup",
    "scope_settlement",
    "resource_release",
)

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


def fence_deadline(*, claimed_unix: object, fence_s: object) -> float | None:
    """The absolute wall-clock bound, or ``None`` when UNKNOWN.

    One addition, never credited: ``claimed_unix + fence_s``. Both ends
    must be finite numbers. Anything else answers ``None``.
    """

    if (
        type(claimed_unix) in (int, float)
        and not isinstance(claimed_unix, bool)
        and math.isfinite(float(claimed_unix))
        and type(fence_s) in (int, float)
        and not isinstance(fence_s, bool)
        and math.isfinite(float(fence_s))
        and float(fence_s) > 0
    ):
        return float(claimed_unix) + float(fence_s)
    return None


def release_bound(
    *,
    claimed_unix: object,
    fence_s: object,
    evidence: Mapping[str, object] | None,
) -> float | None:
    """The verified release bound, or ``None`` when it is UNKNOWN.

    A finite bound needs all three: a finite claim stamp, a sealed
    fence, and enforcement evidence for every applicable phase.
    Each phase record must name the fence schema, carry ``enforced``
    true, cite its enforcement proof, and record the phase end at or
    before the fence deadline. A missing phase, a failed phase, an
    end past the deadline, or absent evidence answers ``None``.
    Callers render that as ``UNKNOWN`` and hold resources.
    """

    bound = fence_deadline(claimed_unix=claimed_unix, fence_s=fence_s)
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
        ended = record.get("ended_unix")
        if (
            type(ended) not in (int, float)
            or isinstance(ended, bool)
            or not math.isfinite(float(ended))
            or float(ended) > bound
        ):
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
