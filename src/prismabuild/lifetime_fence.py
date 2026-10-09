"""Opt-in sealed lifetime fence, separate from the payload budget (#1429).

The sealed ``execution_timeout_s`` is a payload execution budget only.
Checkout runs before its deadline starts, checkpoint I/O and other waits
credit it, and cleanup settles after it. It never proves that capacity
returns by any wall time.

This module seals one absolute, non-creditable, versioned bound instead.
A submitter opts in with ``params.lifetime``. The worker enforces the
bound across every phase it runs. Admission grants timed backfill only
on a verified release bound strictly before the original opportunity.
Every unfenced component reads ``UNKNOWN``. Expired timers never return
capacity; only proved settlement does.

Validation lives in :mod:`prismabuild.core` beside the other sealed
params, so ``validate_action`` refuses an unreadable fence at seal time.
This module owns the admission predicate and its evidence shape.
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


def release_bound(
    *,
    claimed_unix: object,
    fence_s: object,
    evidence: Mapping[str, object] | None,
) -> float | None:
    """The verified release bound, or ``None`` when it is UNKNOWN.

    A finite bound needs all three: a finite claim stamp, a sealed
    fence, and enforcement evidence for every applicable phase.
    A missing phase, a failed phase, or absent evidence answers
    ``None``. Callers render that as ``UNKNOWN`` and hold resources.
    """

    if (
        type(claimed_unix) in (int, float)
        and not isinstance(claimed_unix, bool)
        and math.isfinite(float(claimed_unix))
        and type(fence_s) in (int, float)
        and not isinstance(fence_s, bool)
        and math.isfinite(float(fence_s))
        and float(fence_s) > 0
        and isinstance(evidence, Mapping)
    ):
        phases = evidence.get("phases")
        if isinstance(phases, Mapping):
            for phase in PHASES:
                record = phases.get(phase)
                if not isinstance(record, Mapping):
                    return None
                if record.get("enforced") is not True:
                    return None
                if record.get("evidence") in (None, "", [], {}):
                    return None
            return float(claimed_unix) + float(fence_s)
    return None


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
