"""Advance credits: fenced room for one admissible next step per window.

A window holds its current ranges (pinned) and waits on its next ranges.  Two
windows that each fit alone can wedge jointly: the second current lands in the
room the first advance needed, and no eviction, claim, or sweep relieves it
(see the hold-and-wait fixture).  Ordinary admission control cannot fix this
at publish time alone either: a publish reserves nothing, so unrelated claims
can steal the room a protected advance was counted on between its retire and
its claim.

An advance credit is that room made real: tokens for one next step, held in
the EXISTING tier ``ResourceLedger`` under a deterministic grant key, then
transferred to the next mover with the existing ``transfer`` when its row
publishes.  No second scheduler, no capacity database, no new ledger: holds,
ready grants, and pins are all read off the one ledger every cycle, and the
single writer per tier (the tier loop owning that ``tier_id``) is the only
boundary -- per-consumer transition locks keep their existing file-transition
role and never fence shared obligations.

Lifecycle (see the design note's state table): UNFUNDED (gate alone) ->
RESERVED (``acquire`` under the grant key, bound to tier/consumer/plan/mover/
range/generation/publication) -> TRANSFERRING (fence moved under the mover
without a free interval) -> CONSUMED (the mover's claim counted it toward its
demand and fused the binding shut) or RELEASED.  A funded claim takes only
the remainder from free, so exact-fit advances fence and stay protected from
stealers -- there is no gate-only exception and no double charge.  Replenish
reads safe-retire evidence (staged-held or egress-done predecessors), never
the progress counter.  Unknown ledger/ready/pin evidence defers new grants
with the unreadable record named.

Explicit funding-record state table (``tier-funding/<mover>.<tier>.json``,
one generation per file, binding immutable per generation -- only ``state``
advances, via compare-and-swap on ``generation``):

================  ============================================================
state             meaning / who may move it
================  ============================================================
``reserved``      Fence held under the ``advance-`` grant key; coordinator
                  owns it.  May advance to ``transferring`` (fence moved
                  under the mover, no free interval) or ``released``.  A
                  fresh reservation never edits one: it mints a new
                  generation instead, so a verifying claim's pin cannot be
                  pulled out from under it.
``transferring``  Fence held under the mover key, awaiting its claim.  Only
                  this state authorizes the claim path to subtract named
                  tokens (verified by name, kind prefix, tier, mover,
                  ``published_unix``, sealed residency range, and live
                  consumer/plan digest).  May advance to ``consumed`` (the
                  claim persisted, then fused the binding shut) or
                  ``released``.
``consumed``      Terminal: the claim counted this fence.  Never advances,
                  never authorizes a second subtraction.  A later need
                  fences afresh under a new generation.
``released``      Terminal: the fence went home (cancel, supersede,
                  terminal mover, stale binding).  Never advances.
================  ============================================================

Claim durability ordering (the partial-failure rule): the claim verifies
every funding pre-persistence (read-only, against the renamed record) and
marks ``consumed`` only after the claim and lease are durable.  Marking
tier1 then failing tier2 -- or failing persistence itself -- leaves every
record ``transferring`` for the next attempt to re-verify: a recoverable
entitlement, never consumed-but-no-executable-claim stranded credit.  A
mark left ``transferring`` under an already-executable claim is closed by
the mover-terminal settle on finish, never by a second subtraction.

Fairness reuses claim-poll order (the queue's own scan order); the key
tiebreak is determinism only.  No unbounded starvation-freedom claim.
"""

from __future__ import annotations

from collections.abc import Mapping
import hashlib

#: Grant holder prefix.  Never ``claiming.*`` (transfer refuses those) and
#: never 64-hex (cannot collide with content-hashed action keys).
GRANT_PREFIX = "advance-"

#: Typed reasons on gated/stalled decisions.  ``joint-fit-oversize`` is
#: permanent (``unsupported-workset`` semantics); the rest are transient waits
#: naming what must retire.
REASON_STALL = "joint-fit-stall"
REASON_OVERSIZE = "joint-fit-oversize"
REASON_UNFUNDED = "advance-credit-unfunded"
REASON_DEFER_UNKNOWN = "advance-deferred-unknown-evidence"

#: The produced-output obligation until the PB contract lands: counted as zero
#: with this note on every gated decision.  Never an estimate.
OUTPUT_UNENFORCED_NOTE = "output-scope-unenforced"


def grant_key(consumer_action_key: str, tier_id: str, leg: str,
              phase: str, chunk_index: int | None = None) -> str:
    """Deterministic fence holder for one consumer's next advance on one tier.

    ``leg`` is the mover role (``mover_row``/``ram_mover_row``).  The phase
    digest keeps arbitrary manifest phase names out of holder directory names.
    """

    digest = hashlib.sha256(str(phase).encode("utf-8")).hexdigest()[:12]
    key = (f"{GRANT_PREFIX}{str(consumer_action_key)[:16]}-"
           f"{str(tier_id).split(':')[0][:24]}-{str(leg)}-{digest}")
    if chunk_index is not None:
        key += f"-c{int(chunk_index)}"
    return key


def gate_newcomer(*, held_gib: int, ready_gib: int, output_gib: int,
                  capacity_gib: int | None, cur_min_gib: int,
                  next_min_gib: int | None,
                  existing_min_next_gib: int) -> dict[str, object]:
    """Whether a window's first step may publish against shared room.

    ``held_gib`` counts every held token on the tier (movers, grants, pins --
    one ledger, no exceptions); ``ready_gib`` counts queued-but-unclaimed
    tier demands that will commit on claim.  Minimum admission covers the
    lead (``cur_min_gib``) AND the protected next (``next_min_gib``) under
    their actual overlapping lifetime, plus the already-admitted minimum
    next: a second current may not land in the room the first advance was
    promised.  A final window (``next_min_gib=None``) needs no future credit.
    ``capacity_gib=None`` is unknown capacity and always defers.
    """

    if capacity_gib is None:
        return {"admit": False, "reason": REASON_DEFER_UNKNOWN,
                "permanent": False,
                "output_note": OUTPUT_UNENFORCED_NOTE}
    if cur_min_gib > capacity_gib:
        return {"admit": False, "reason": REASON_OVERSIZE, "permanent": True,
                "output_note": OUTPUT_UNENFORCED_NOTE}
    total = (held_gib + ready_gib + output_gib + cur_min_gib
             + (next_min_gib or 0) + existing_min_next_gib)
    if total <= capacity_gib:
        return {"admit": True, "reason": "",
                "output_note": OUTPUT_UNENFORCED_NOTE}
    return {"admit": False, "reason": REASON_STALL, "permanent": False,
            "output_note": OUTPUT_UNENFORCED_NOTE}


def fence_fits(*, held_gib: int, ready_gib: int, output_gib: int,
               capacity_gib: int | None) -> bool:
    """Whether one more fence keeps every future take fundable.

    New-money peak accounting: everything held plus everything queued that
    will commit new capacity plus unenforced output must leave the tier
    within capacity.  ``ready_gib`` carries new money only (funded rows ride
    at zero); the fence's own take from free is enforced atomically by the
    reserve itself.  Every claim, copy, pin, and temporary overlap is counted
    exactly once, never twice.
    """

    if capacity_gib is None:
        return False
    return held_gib + ready_gib + output_gib <= capacity_gib


def replenish_ok(*, grant_outstanding: bool, need_gib: int | None,
                 prior_retired: bool) -> bool:
    """Whether a fresh fence may be reserved for the following next step.

    One outstanding RESERVED grant per consumer/tier/leg; a new one needs a
    real need, safe-retire evidence for every earlier leg (staged-held or
    egress-done -- never the progress counter alone), and no live fence.
    Handed-off (fused) grants do not count: they already moved.
    """

    if grant_outstanding:
        return False
    if need_gib is None or need_gib <= 0:
        return False
    return bool(prior_retired)


def cancel_due(*, consumer_live: bool, superseded: bool,
               need_gib: int | None, mover_holds_need: bool) -> str | None:
    """Why a held fence must be released, or ``None`` to keep holding it."""

    if not consumer_live:
        return "consumer-terminal"
    if superseded:
        return "plan-superseded"
    if need_gib is None:
        return "need-gone-final"
    if mover_holds_need:
        return "need-landed"
    return None


def cancel(ledger, grant: str) -> dict[str, int]:
    """Release a fence: safe in every direction, twice included.

    A pure ledger release, never a funding-record effect: it moves no
    generations and edits no bindings, so it needs no mover lock of its
    own.  Every caller in the frozen wiring either holds the mover's
    transition lock already (settle, non-blocking) or names a grant key no
    claim ever verifies against (pre-pass cancel of an unhanded fence).
    Releasing only frees capacity (under-protection stalls, never
    over-admits) and a second release of an absent holder returns ``0``.
    Funding-record closure beside a release always goes through
    ``PoolQueue.advance_funding_state``, which serializes on the lock.
    """

    try:
        return {"released": int(ledger.release(grant))}
    except (OSError, ValueError):
        return {"released": 0}


def held_grants(ledger) -> list[str]:
    """Every fence holder on this ledger.  Stateless recovery reads this.

    Read-only listing: no mutation, no lock needed.  Callers reconcile what
    they find against funding records (which carry the generations) rather
    than acting on names alone.
    """

    try:
        keys = ledger.held_keys()
    except (OSError, ValueError):
        return []
    return sorted(key for key in keys if str(key).startswith(GRANT_PREFIX))


def obligations(*, held_gib: int, ready_gib: int, output_gib: int = 0,
                output_enforced: bool = False) -> dict[str, object]:
    """One tier's joint obligations, with the output scope explicit.

    Until the PB produced-output contract lands, ``output_enforced`` is False
    and the value counts as zero with the standing note -- never an estimate.
    """

    return {"held_gib": int(held_gib), "ready_gib": int(ready_gib),
            "output_gib": int(output_gib) if output_enforced else 0,
            "output_enforced": bool(output_enforced),
            "output_note": "" if output_enforced else OUTPUT_UNENFORCED_NOTE,
            "total_gib": int(held_gib) + int(ready_gib)
            + (int(output_gib) if output_enforced else 0)}


def decision_needs(plan: Mapping[str, object], accepted_phase: str | None,
                   published: object, *, mover_role: str = "mover_row",
                   ) -> dict[str, object]:
    """Thin pure wrapper so the loops share one needs spelling.

    Lives here (not on the plan module) to keep the plan module's surface to
    range/window truth; the import runs one way only.
    """

    from prismabuild import residency_plan as plan_mod

    return plan_mod.advance_needs(plan, accepted_phase, published=published,
                                  mover_role=mover_role)
