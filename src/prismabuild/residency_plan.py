"""A rolling window of movement nodes, frozen once and published as it fits (#583).

The campaign's `run` stage reads 3.33 TB and its `prepare` 6.84 TB, in phases,
against a 721 GB stage.  "Admitted only when every lead is executed" cannot
hold for a read set five times the size of the stage, so the DAG has to express
a **window**: the consumer depends on the movers for the phase it is about to
read, later movers are published as its accepted progress advances, and the
ranges it has finished with are evicted.  Staging for phase k+N then overlaps
compute on phase k and the stage never overfills.

**The plan is frozen before the first publish; only publication is deferred.**
Every mover and every egress node is *sealed* by the submitter -- an action key
is ``canonical_sha256`` of an action body, so it is never something this module
may derive -- and the whole queue row each one will be published with is
written into the plan.  Membership is fixed at submission; a restart resumes
the same decomposition instead of cutting a new one, which is what
``docs/design_work_decomposition_2026-09-11.md`` requires of a decomposer.  The
only thing the window changes is *when* a frozen child is published, and that
same contract already puts publication in the coordinator's hands: an admitted
child never publishes work.  Here the coordinator is the ``tiers`` role loop,
which already reads the queue and the tier ledger every cycle.

**The window is bounded by tokens, not by a number.**  How many movers may be
in flight is how many the tier's free capacity can hold, which is discovered
from the device and changes when the hardware does.  Nothing here is a
capacity constant.

**What gates a later mover is publication, not admission.**  A published mover
is admitted on its tokens like any other action; one whose phase the consumer
has not reached yet is simply not in ``ready/`` for anyone to scan.  That keeps
the claim path free of any phase comparison, and keeps ``ready/`` small:
publishing every mover and egress node of a 223-phase manifest at once would
put 446 items in front of every loop of every box, at two reads each per scan.

**The consumer depends only on its first phase.**  Anything later is an
optimization the window keeps ahead of it, and a range the map does not name is
read from the pool exactly as it is today.  That is what removes the deadlock:
the consumer never waits on a mover that is waiting on the consumer.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
import json
from pathlib import Path

from . import core as pb
from . import pool as _pool
from . import storage_tiers

RESIDENCY_PLAN_SCHEMA_V1 = "prismaquant.prismabuild.residency_plan.v1"

_HEX = frozenset("0123456789abcdef")
#: What a phase says, and nothing else.  Unknown keys refuse, for the reason
#: every other schema here refuses them: a field the writer meant and the
#: reader ignores is the quiet half of a disagreement.
_PHASE_KEYS = frozenset({
    "name", "start_bytes", "end_bytes", "stage_gib", "mover_row", "egress_row"})
_PLAN_KEYS = frozenset({
    "schema", "consumer_action_key", "tier_id", "stage_root", "manifest_sha256",
    "manifest_bytes", "phases"})


class ResidencyPlanError(ValueError):
    """A plan that does not say what it must, or one that already says otherwise."""


def _action_key(value: object, *, where: str) -> str:
    if (not isinstance(value, str) or len(value) != 64
            or any(character not in _HEX for character in value)):
        raise ResidencyPlanError(f"{where} must be a 64-character action key")
    return value


def build_plan(*, consumer_action_key: str, tier_id: str, stage_root: str,
               manifest_sha256: str, manifest_bytes: int,
               phases: Sequence[Mapping[str, object]]) -> dict[str, object]:
    """Assemble one consumer's plan from ranges the submitter has already sealed.

    ``phases`` are the manifest's own, in read order, each with the queue row
    its mover and its egress node will be published with.  The ranges are not
    invented here and must not be: a boundary that cut a manifest entry would
    hand a mover more bytes than its tokens reserved, and
    ``storage_tiers.manifest_phase_ranges`` already refuses a phase table that
    does not describe its own manifest.
    """

    built = []
    for phase in phases:
        start, end = int(phase["start_bytes"]), int(phase["end_bytes"])
        built.append({
            "name": str(phase["name"]),
            "start_bytes": start,
            "end_bytes": end,
            "stage_gib": storage_tiers.stage_tokens_for_bytes(end - start),
            "mover_row": dict(phase["mover_row"]),      # type: ignore[arg-type]
            "egress_row": dict(phase["egress_row"]),    # type: ignore[arg-type]
        })
    return validate_plan({
        "schema": RESIDENCY_PLAN_SCHEMA_V1,
        "consumer_action_key": consumer_action_key,
        "tier_id": tier_id,
        "stage_root": stage_root,
        "manifest_sha256": manifest_sha256,
        "manifest_bytes": int(manifest_bytes),
        "phases": built,
    })


def validate_plan(value: object) -> dict[str, object]:
    """Refuse a plan that is not a cover of one manifest's read order.

    The three things checked are the three a coordinator cannot check later:
    that the phases tile the read order with no gap and no overlap (a gap is
    bytes nobody stages, an overlap is bytes two movers both publish under one
    name), that each phase's demand is at least what its range occupies, and
    that no two phases name one action.
    """

    if not isinstance(value, Mapping):
        raise ResidencyPlanError("a residency plan must be an object")
    unknown = sorted(set(value) - _PLAN_KEYS)
    if unknown:
        raise ResidencyPlanError(f"unknown residency-plan fields: {unknown}")
    if value.get("schema") != RESIDENCY_PLAN_SCHEMA_V1:
        raise ResidencyPlanError(f"plan schema must be {RESIDENCY_PLAN_SCHEMA_V1!r}")
    _action_key(value.get("consumer_action_key"), where="consumer_action_key")
    digest = value.get("manifest_sha256")
    if (not isinstance(digest, str) or len(digest) != 64
            or any(character not in _HEX for character in digest)):
        raise ResidencyPlanError("manifest_sha256 must be a 64-character digest")
    size = value.get("manifest_bytes")
    if isinstance(size, bool) or not isinstance(size, int) or size <= 0:
        raise ResidencyPlanError("manifest_bytes must be a positive integer")
    tier_id = value.get("tier_id")
    if not isinstance(tier_id, str) or not tier_id:
        raise ResidencyPlanError("a residency plan must name its tier")
    stage_root = value.get("stage_root")
    if not isinstance(stage_root, str) or not stage_root.startswith("/"):
        raise ResidencyPlanError("stage_root must be an absolute path")
    phases = value.get("phases")
    if not isinstance(phases, list) or not phases:
        raise ResidencyPlanError("a residency plan needs at least one phase")

    position = 0
    keys: set[str] = set()
    names: set[str] = set()
    checked: list[dict[str, object]] = []
    demand_kind = (f"{storage_tiers.capacity_kind_of(tier_id)}"
                   f"{storage_tiers.TIER_DEMAND_SEPARATOR}{tier_id}")
    for phase in phases:
        if not isinstance(phase, Mapping):
            raise ResidencyPlanError("each plan phase must be an object")
        stray = sorted(set(phase) - _PHASE_KEYS)
        if stray:
            raise ResidencyPlanError(f"unknown plan-phase fields: {stray}")
        name = phase.get("name")
        if not isinstance(name, str) or not name or name in names:
            raise ResidencyPlanError("each plan phase needs a distinct name")
        names.add(name)
        start, end = phase.get("start_bytes"), phase.get("end_bytes")
        for field, number in (("start_bytes", start), ("end_bytes", end)):
            if isinstance(number, bool) or not isinstance(number, int) or number < 0:
                raise ResidencyPlanError(f"plan phase {name!r} needs a whole {field}")
        assert isinstance(start, int) and isinstance(end, int)
        if end <= start:
            raise ResidencyPlanError(
                f"plan phase {name!r} must be non-empty and half-open")
        if start != position:
            raise ResidencyPlanError(
                f"plan phase {name!r} starts at {start}, not where the previous "
                f"one ended ({position})")
        position = end
        floor = storage_tiers.stage_tokens_for_bytes(end - start)
        declared = phase.get("stage_gib")
        if isinstance(declared, bool) or not isinstance(declared, int) or declared < floor:
            raise ResidencyPlanError(
                f"plan phase {name!r} claims {declared} GiB for a range that "
                f"occupies {floor}")
        rows: dict[str, object] = {}
        for role in ("mover_row", "egress_row"):
            row = phase.get(role)
            if not isinstance(row, Mapping):
                raise ResidencyPlanError(f"plan phase {name!r} needs a {role}")
            key = _action_key(row.get("action_key"), where=f"{role}.action_key")
            if key in keys:
                raise ResidencyPlanError("two plan rows share an action key")
            keys.add(key)
            rows[role] = dict(row)
        mover = rows["mover_row"]
        assert isinstance(mover, dict)
        resources = mover.get("resources")
        if not isinstance(resources, Mapping) or int(resources.get(demand_kind, 0)) < floor:
            # The range is the measurement and the demand is a claim about it.
            # A row that asked the tier for less than its range occupies would
            # pin bytes the ledger never counted -- the accounting #583 closes
            # -- and ``publish`` would refuse it one phase into the campaign
            # rather than here, where nothing is queued yet.
            raise ResidencyPlanError(
                f"plan phase {name!r} asks the tier for "
                f"{None if not isinstance(resources, Mapping) else resources.get(demand_kind)}"
                f", below the {floor} GiB its range occupies")
        checked.append({**dict(phase), "mover_row": mover,
                        "egress_row": rows["egress_row"]})
    return {**{key: value[key] for key in _PLAN_KEYS if key in value},
            "phases": checked}


def freeze(queue, plan: Mapping[str, object]) -> dict[str, object]:
    """Write the plan once; a second attempt verifies rather than replaces.

    First-writer, because repartitioning on a retry is exactly what the
    decomposition contract forbids: the children are already named and may
    already be queued, and a second plan would name different ones.  The same
    immutable publish the attempt records use, so a conflicting body is a
    refusal with both in hand rather than a silent overwrite.
    """

    checked = validate_plan(plan)
    path = queue.residency_plan_path(str(checked["consumer_action_key"]))
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        _pool._publish_immutable(
            path, pb._canonical_bytes(checked), where="residency plan")
    except _pool.PoolContractError as exc:
        raise ResidencyPlanError(
            f"a different residency plan is already filed for "
            f"{checked['consumer_action_key']}: {exc}") from None
    return checked


def read(queue, consumer_action_key: str) -> dict[str, object] | None:
    """One consumer's frozen plan, or ``None`` when it has none.

    ``None`` is the ordinary answer -- almost no action is staged -- and a
    plan that cannot be read or does not validate answers the same way, so a
    corrupt file leaves the consumer reading the pool rather than stopping the
    loop that was going to stage for somebody else.
    """

    try:
        with open(queue.residency_plan_path(consumer_action_key)) as stream:
            return validate_plan(json.load(stream))
    except (OSError, ValueError):
        return None


def leads_for(plan: Mapping[str, object]) -> list[str]:
    """The movers the consumer's admission depends on: its first phase, only.

    Depending on more than the first phase is what would let the consumer wait
    on a mover the window has not published yet, while the window waits on the
    consumer's progress to publish it.
    """

    phases = plan["phases"]
    assert isinstance(phases, list)
    return [str(phases[0]["mover_row"]["action_key"])]


def mover_keys(plan: Mapping[str, object]) -> list[str]:
    """Every movement node this plan will ever have, published or not.

    The orphan sweep needs this and not just the leads: a pinned mover for a
    phase the consumer has not reached is named by nothing in the queue, and a
    sweep that tested only live items' ``leads`` would evict the window it is
    there to protect.
    """

    phases = plan["phases"]
    assert isinstance(phases, list)
    return [str(phase["mover_row"]["action_key"]) for phase in phases]


def window(plan: Mapping[str, object], *, accepted_phase: str | None,
           free_gib: int, published: Sequence[str] = (),
           staged: Sequence[str] = ()) -> dict[str, list[dict[str, object]]]:
    """What the coordinator should publish and evict on this cycle.

    ``accepted_phase`` is the phase the consumer's progress record says it is
    reading *now*, so the phases before it are finished with and may be
    evicted; counting the named phase itself would take back a window the
    consumer is still inside, the same rule ``prewarm_loop.consumed_through``
    keeps.  ``published`` names the movers already queued, claimed or terminal
    **and still holding their tokens**; ``staged`` those whose bytes are on the
    tier now.

    A mover that is terminal but no longer pinned is deliberately absent from
    ``published``: its key is a content hash, so a second campaign over the
    same manifest seals the same key, and a ``done`` record left from an
    already-evicted range would otherwise satisfy nothing and be republished by
    nobody.  The window is what fits: phases are published in read order while
    the tier's free capacity covers the next one, so a starved mover is rarely
    in ``ready/`` at all and the tokens stay the safety net rather than the
    schedule.
    """

    phases = list(plan["phases"])                                # type: ignore[arg-type]
    names = [str(phase["name"]) for phase in phases]
    current = names.index(accepted_phase) if accepted_phase in names else 0
    already = set(published)
    resident = set(staged)

    evict = [
        {"phase": phase["name"],
         "mover_action_key": str(phase["mover_row"]["action_key"]),
         "egress_row": phase["egress_row"],
         "stage_gib": phase["stage_gib"]}
        for phase in phases[:current]
        if str(phase["mover_row"]["action_key"]) in resident
    ]

    publish: list[dict[str, object]] = []
    room = int(free_gib)
    for phase in phases[current:]:
        key = str(phase["mover_row"]["action_key"])
        if key in already:
            continue
        need = int(phase["stage_gib"])
        if need > room:
            break
        room -= need
        publish.append({
            "phase": phase["name"], "mover_action_key": key,
            "start_bytes": phase["start_bytes"], "end_bytes": phase["end_bytes"],
            "stage_gib": need, "mover_row": phase["mover_row"],
        })
    return {"publish": publish, "evict": evict}


__all__ = [
    "RESIDENCY_PLAN_SCHEMA_V1",
    "ResidencyPlanError",
    "build_plan",
    "freeze",
    "leads_for",
    "mover_keys",
    "read",
    "validate_plan",
    "window",
]
