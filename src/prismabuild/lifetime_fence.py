"""Opt-in sealed lifetime fence, separate from the payload budget (#1429).

The sealed ``execution_timeout_s`` is a payload execution budget only.
Checkout runs before its deadline starts, checkpoint I/O and other waits
credit it, and cleanup settles after it. It never proves that capacity
returns by any wall time.

The queue stamps one absolute, non-creditable deadline at publication.
The worker checks this deadline without changing the separate payload budget.
These checks do not bound synchronous lifecycle calls or physical reclamation.
The component records name these limits and return UNKNOWN.

Timed backfill requires prospective enforcement for every applicable phase.
A capability tag or a completed attempt cannot supply this proof.
Expired timers never return capacity; only proved settlement does.

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

#: Phases whose completion the execution supervisor observes.
#: A deadline check after a synchronous call does not bound that call.
EXECUTION_PHASES = (
    "admission",
    "checkout",
    "readiness",
    "prelaunch",
    "payload",
    "credited_waits",
    "termination",
)
#: Phases whose completion the existing finalization path observes.
SETTLEMENT_PHASES = (
    "cleanup",
    "scope_settlement",
    "resource_release",
)
PHASES = EXECUTION_PHASES + SETTLEMENT_PHASES

#: No current lifecycle path proves prospective release by the deadline.
#: Keep the reason beside each phase, not in a scheduler heuristic.
_UNFENCED_COMPONENTS = {
    "admission": "queue and reservation I/O has no absolute deadline",
    "checkout": "Git and tree removal have no shared absolute deadline",
    "readiness": "readiness and allocation I/O has no absolute deadline",
    "prelaunch": "scope creation and launch I/O has no absolute deadline",
    "payload": "synchronous supervisor I/O can delay the deadline check",
    "credited_waits": "checkpoint I/O has no absolute deadline",
    "termination": "a stop request cannot bound kernel process settlement",
    "cleanup": "container and export cleanup has no absolute deadline",
    "scope_settlement": "kernel and broker settlement can remain unknown",
    "resource_release": "ledger return I/O has no absolute deadline",
}


def phase_enforcement() -> dict[str, dict[str, object]]:
    """Report current enforcement limits for each applicable component."""

    return {
        phase: {"verdict": "UNKNOWN", "enforced": False,
                "reason": reason, "evidence": None}
        for phase, reason in _UNFENCED_COMPONENTS.items()
    }


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

    The publication stamp exists before resource admission.
    This arithmetic defines the clock, not a verified release guarantee.
    Invalid or non-finite endpoints return UNKNOWN.
    """

    published = _finite_number(published_unix)
    fence = _finite_number(fence_s)
    if published is None or fence is None or fence <= 0:
        return None
    deadline = published + fence
    return deadline if math.isfinite(deadline) else None


def prospective_bound(
    *,
    published_unix: object,
    fence_s: object,
    supported: object,
    now_unix: object,
) -> float | None:
    """Return UNKNOWN unless every applicable phase has prospective enforcement.

    ``supported`` establishes clock support only. It does not prove release.
    The current lifecycle has unfenced synchronous calls in every phase.
    Completion records cannot turn those calls into prospective enforcement.
    """

    bound = fence_deadline(published_unix=published_unix, fence_s=fence_s)
    components = phase_enforcement()
    if (bound is None or supported is not True
            or any(components[phase]["enforced"] is not True for phase in PHASES)):
        return None
    now = _finite_number(now_unix)
    if now is None or now >= bound:
        return None
    return bound


def release_bound(
    *,
    published_unix: object,
    fence_s: object,
    evidence: Mapping[str, object] | None,
) -> float | None:
    """The audited release bound of one finished attempt, or ``None``.

    Every phase must cite enforcement and a measured end within the deadline.
    A completion observation alone does not establish enforcement.
    This audit does not supply a prospective bound for another attempt.
    """

    bound = fence_deadline(published_unix=published_unix, fence_s=fence_s)
    if bound is None or not isinstance(evidence, Mapping):
        return None
    if evidence.get("schema") != EVIDENCE_SCHEMA_V1:
        return None
    if (evidence.get("published_unix") != published_unix
            or evidence.get("fence_s") != fence_s
            or evidence.get("deadline_unix") != bound):
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
