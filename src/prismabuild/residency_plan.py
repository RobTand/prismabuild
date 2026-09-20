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

**...and by the consumer, because free capacity alone brakes at 0 B (#632).**
Free capacity is the only brake that acts on the admit side, and the release
side is driven by the consumer's accepted progress, so a consumer that
publishes none stages its whole plan up to the last phase that fits.  The
GLM-5.3-Flash run ``ad8803aa`` did exactly that on 2026-09-18: 46 phases and
3.60 TB against a 744 GB stage, 19 movers done, 1 egress done, and
``prismabuild-stage/prewarm`` at 0 B -- at which point the #628 ownership
marker could not be rewritten and every sweep and egress refused with
``stage_root_unregistered`` (#631).  So ``window`` also bounds *run-ahead*:
the tokens it holds for phases **strictly after** the one the consumer is
reading.  Two quantities, both read off the plan and the tier and neither of
them a count of phases:

* a consumer that has accepted **nothing** gets ``step`` -- the largest
  ``stage_gib`` still ahead of it.  ``N = 1`` is the smallest N for which this
  module's own overlap claim ("staging for phase k+N overlaps compute on phase
  k") is satisfiable, and a consumer that has published nothing has given no
  evidence it consumes at all, so anything deeper is speculation on a rate
  nobody has measured.
* a consumer that **has** accepted a phase is rolling, and its bound is the
  tier: ``capacity - step``, so the stage keeps room to stage one more phase
  and never reaches 0 B.  Free capacity still binds first whenever it is
  smaller, exactly as before.

A consumer that reported some phases and then went quiet gets the second
bound, not the first, and stalls there.  Telling "quiet" from "slow" needs a
clock, and a clock is the thing #598 took out of this subsystem: an orphan is
evicted when the tier needs its tokens, never because a clock said so.  The
only progress-free fact available without one is whether the consumer has ever
accepted anything, so that is the fact the two regimes turn on.

**A stall is reported, never inferred.**  ``window`` returns a ``stall``
descriptor naming the phase it declined, how far ahead it already is and what
it is waiting for, and ``tier_loop`` files it as a ``window-stalled`` event.
A silent stall would reproduce the incident in the other direction.

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

from collections.abc import Callable, Mapping, Sequence
import hashlib
import json
import os
from pathlib import Path
import re
import time

from . import core as pb
from . import pool as _pool
from . import storage_tiers

RESIDENCY_PLAN_SCHEMA_V1 = "prismaquant.prismabuild.residency_plan.v1"

#: The marker filed beside a plan whose window an operator has withdrawn.
#: The plan body stays readable; the marker is what stops publication and
#: what a resubmission consults before sealing a fresh plan (#708).
RESIDENCY_PLAN_SUPERSEDED_SCHEMA_V1 = (
    "prismaquant.prismabuild.residency_plan_superseded.v1")

#: Where a retired plan goes: a subdirectory of the live plan directory, for
#: the reason ``withdrawn/superseded`` is one.  Every reader of the live
#: directory addresses a plan as ``<consumer_action_key>.json`` -- ``read``
#: below, ``pbstatus``'s starvation census, the MCP cursor join, ``pbmetrics``
#: -- so a subdirectory is invisible to all of them while the retirement
#: marker and the reaped body stay on disk as evidence.
SUPERSEDED = "superseded"

_HEX = frozenset("0123456789abcdef")
#: What a phase says, and nothing else.  Unknown keys refuse, for the reason
#: every other schema here refuses them: a field the writer meant and the
#: reader ignores is the quiet half of a disagreement.
_PHASE_KEYS = frozenset({
    "name", "start_bytes", "end_bytes", "stage_gib", "mover_row", "egress_row",
    # The phase's promotion onto the RAM tier (#640), optional: a promotion
    # node and an egress node, sealed with the plan like the stage's own so
    # the same frozen decomposition governs both tiers.  A phase without
    # them predates the ram tier and is staged exactly as it always was.
    "ram_mover_row", "ram_egress_row",
    # The same leg cut into chunks (#673): one promotion node plus one
    # egress node per chunk, in read order, each carrying its chunk index
    # and its chunk range.  A phase carries either the pair or the chunks,
    # never both: chunking is a sealing-time property, and a node whose
    # range is its phase's whole range follows the whole-phase rules.
    "ram_chunks",
    # The stage's own leg cut into chunks the same way (#675): one movement
    # node plus one egress node per chunk, in read order, each carrying its
    # chunk index and its chunk range under the stage's own role names.  A
    # phase carries either the pair or the chunks, never both, and either
    # leg chunks independently of the other: a phase may carry
    # ``stage_chunks`` and/or ``ram_chunks``, and validation tiles each
    # against the phase on its own.
    "stage_chunks"})
#: What one chunk of a chunked ram leg says, and nothing else.
_CHUNK_KEYS = frozenset({
    "chunk_index", "start_bytes", "end_bytes", "stage_gib",
    "ram_mover_row", "ram_egress_row"})
#: What one chunk of a chunked stage leg says, and nothing else: the same
#: shape under the stage's own role names (#675).
_STAGE_CHUNK_KEYS = frozenset({
    "chunk_index", "start_bytes", "end_bytes", "stage_gib",
    "mover_row", "egress_row"})
_PLAN_KEYS = frozenset({
    "schema", "consumer_action_key", "tier_id", "stage_root", "manifest_sha256",
    "manifest_bytes", "phases",
    # Optional, and deliberately on the plan rather than on a row: the movers
    # of one window are priced by one read of the mover receipts, and
    # ``tier_loop`` publishes a row as ``queue.publish(**row)``, so a row key
    # ``publish`` has no parameter for would take the whole window down.
    "demand_source",
    # The ram tier this plan's ``ram_mover_row`` blocks name; required the
    # moment any phase carries one, and refused as a non-ram id otherwise.
    "ram_tier_id"})
#: Which movement leg a window decision is about, and the egress row that
#: frees it.  The stage window decides for ``mover_row``; the ram window
#: decides for ``ram_mover_row`` (#640).  A role this table does not name is
#: refused rather than guessed at, because a decision run against the wrong
#: leg's keys finds nothing resident and quietly never evicts.
_MOVEMENT_ROLES = {
    "mover_row": "egress_row",
    "ram_mover_row": "ram_egress_row",
}
#: Which rows one chunk of a chunked leg carries, by leg: the tmpfs leg
#: under its own role names (#673), the SSD leg under the stage's (#675).
_MOVEMENT_ROLES_BY_LEG = {
    "ram": ("ram_mover_row", "ram_egress_row"),
    "stage": ("mover_row", "egress_row"),
}


class ResidencyPlanError(ValueError):
    """A plan that does not say what it must, or one that already says otherwise."""


def _action_key(value: object, *, where: str) -> str:
    if (not isinstance(value, str) or len(value) != 64
            or any(character not in _HEX for character in value)):
        raise ResidencyPlanError(f"{where} must be a 64-character action key")
    return value


def build_plan(*, consumer_action_key: str, tier_id: str, stage_root: str,
               manifest_sha256: str, manifest_bytes: int,
               phases: Sequence[Mapping[str, object]],
               demand_source: Mapping[str, object] | None = None,
               ram_tier_id: str | None = None,
               ) -> dict[str, object]:
    """Assemble one consumer's plan from ranges the submitter has already sealed.

    ``phases`` are the manifest's own, in read order, each with the queue row
    its mover and its egress node will be published with.  The ranges are not
    invented here and must not be: a boundary that cut a manifest entry would
    hand a mover more bytes than its tokens reserved, and
    ``storage_tiers.manifest_phase_ranges`` already refuses a phase table that
    does not describe its own manifest.

    ``ram_tier_id`` names the tier the phases' ``ram_mover_row`` entries
    promote onto, when the submitter sealed a ram leg at all.
    """

    built = []
    for phase in phases:
        start, end = int(phase["start_bytes"]), int(phase["end_bytes"])
        entry: dict[str, object] = {
            "name": str(phase["name"]),
            "start_bytes": start,
            "end_bytes": end,
            "stage_gib": storage_tiers.stage_tokens_for_bytes(end - start),
        }
        if phase.get("ram_mover_row") is not None:
            entry["ram_mover_row"] = dict(phase["ram_mover_row"])  # type: ignore[arg-type]
        if phase.get("ram_egress_row") is not None:
            entry["ram_egress_row"] = dict(phase["ram_egress_row"])  # type: ignore[arg-type]
        if phase.get("ram_chunks") is not None:
            entry["ram_chunks"] = [  # type: ignore[arg-type]
                {**chunk} for chunk in phase["ram_chunks"]]
        # The stage leg arrives in either shape (#675); the validator
        # refuses the mixture, so copying what is there copies one shape.
        if phase.get("mover_row") is not None:
            entry["mover_row"] = dict(phase["mover_row"])      # type: ignore[arg-type]
        if phase.get("egress_row") is not None:
            entry["egress_row"] = dict(phase["egress_row"])    # type: ignore[arg-type]
        if phase.get("stage_chunks") is not None:
            entry["stage_chunks"] = [  # type: ignore[arg-type]
                {**chunk} for chunk in phase["stage_chunks"]]
        built.append(entry)
    body: dict[str, object] = {
        "schema": RESIDENCY_PLAN_SCHEMA_V1,
        "consumer_action_key": consumer_action_key,
        "tier_id": tier_id,
        "stage_root": stage_root,
        "manifest_sha256": manifest_sha256,
        "manifest_bytes": int(manifest_bytes),
        "phases": built,
    }
    if ram_tier_id is not None:
        body["ram_tier_id"] = str(ram_tier_id)
    if demand_source is not None:
        body["demand_source"] = dict(demand_source)
    return validate_plan(body)


def _checked_leg_chunk(chunk: object, *, leg: str, phase_name: str,
                       chunk_index: int, phase_start: int, digest: str,
                       tier_id: str, demand_kind: str,
                       keys: set[str]) -> dict[str, object]:
    """One chunk of a chunked movement leg, checked the way the whole-phase leg is.

    ``leg`` is ``"ram"`` (#673) or ``"stage"`` (#675): it selects the chunk
    key set and the mover and egress roles the chunk carries them under --
    ``ram_mover_row``/``ram_egress_row`` on the tmpfs leg, the stage's own
    ``mover_row``/``egress_row`` on the SSD leg.  ``phase_start`` is where
    the previous chunk ended (or the phase began): chunks tile their phase
    with no gap and no overlap, because a gap is bytes nobody moves and an
    overlap is bytes two nodes both publish under one name -- the same cover
    rule the phases themselves answer to.  ``keys`` is the plan's shared
    action-key set, so a chunk node never shares a key with anything else
    sealed.
    """

    mover_role, egress_role = _MOVEMENT_ROLES_BY_LEG[leg]
    chunk_keys = _CHUNK_KEYS if leg == "ram" else _STAGE_CHUNK_KEYS
    where = f"plan phase {phase_name!r} chunk {chunk_index}"
    if not isinstance(chunk, Mapping):
        raise ResidencyPlanError(f"{where} must be an object")
    stray = sorted(set(chunk) - chunk_keys)
    if stray:
        raise ResidencyPlanError(f"unknown {leg}-chunk fields: {stray}")
    if (isinstance(chunk.get("chunk_index"), bool)
            or chunk.get("chunk_index") != chunk_index):
        raise ResidencyPlanError(
            f"{where} names chunk {chunk.get('chunk_index')!r}, "
            f"not its position {chunk_index}")
    cstart, cend = chunk.get("start_bytes"), chunk.get("end_bytes")
    for field, number in (("start_bytes", cstart), ("end_bytes", cend)):
        if isinstance(number, bool) or not isinstance(number, int) or number < 0:
            raise ResidencyPlanError(f"{where} needs a whole {field}")
    assert isinstance(cstart, int) and isinstance(cend, int)
    if cend <= cstart:
        raise ResidencyPlanError(f"{where} must be non-empty and half-open")
    if cstart != phase_start:
        raise ResidencyPlanError(
            f"{where} starts at {cstart}, not where the previous one "
            f"ended ({phase_start})")
    floor = storage_tiers.stage_tokens_for_bytes(cend - cstart)
    declared = chunk.get("stage_gib")
    if isinstance(declared, bool) or not isinstance(declared, int) or declared < floor:
        raise ResidencyPlanError(
            f"{where} claims {declared} GiB for a range that occupies {floor}")
    mover = chunk.get(mover_role)
    if not isinstance(mover, Mapping):
        raise ResidencyPlanError(f"{where} needs a {mover_role}")
    mover_key = _action_key(mover.get("action_key"),
                            where=f"{mover_role}.action_key")
    if mover_key in keys:
        raise ResidencyPlanError("two plan rows share an action key")
    keys.add(mover_key)
    pin = mover.get("residency")
    if not isinstance(pin, Mapping):
        raise ResidencyPlanError(
            f"{where} has a {leg} mover row with no residency block; its "
            f"occupancy would be released the moment it finished")
    if (pin.get("tier_id") != tier_id
            or pin.get("manifest_sha256") != digest
            or pin.get("range_start_bytes") != cstart
            or pin.get("range_end_bytes") != cend):
        raise ResidencyPlanError(
            f"{where} names bytes {cstart}..{cend} of {digest[:12]} for the "
            f"{leg} tier {tier_id}, and its {leg} mover row pins "
            f"{pin.get('range_start_bytes')}.."
            f"{pin.get('range_end_bytes')} of "
            f"{str(pin.get('manifest_sha256'))[:12]} on "
            f"{pin.get('tier_id')}")
    resources = mover.get("resources")
    if (not isinstance(resources, Mapping)
            or int(resources.get(demand_kind, 0)) < floor):
        raise ResidencyPlanError(
            f"{where} asks the {leg} tier for "
            f"{None if not isinstance(resources, Mapping) else resources.get(demand_kind)}"
            f", below the {floor} GiB its range occupies")
    egress = chunk.get(egress_role)
    if not isinstance(egress, Mapping):
        raise ResidencyPlanError(f"{where} needs a {egress_role}")
    egress_key = _action_key(egress.get("action_key"),
                             where=f"{egress_role}.action_key")
    if egress_key in keys:
        raise ResidencyPlanError("two plan rows share an action key")
    keys.add(egress_key)
    return {**dict(chunk), mover_role: dict(mover),
            egress_role: dict(egress)}


def validate_plan(value: object) -> dict[str, object]:
    """Refuse a plan that is not a cover of one manifest's read order.

    The four things checked are the four a coordinator cannot check later:
    that the phases tile the read order with no gap and no overlap (a gap is
    bytes nobody stages, an overlap is bytes two movers both publish under one
    name), that each phase's demand is at least what its range occupies, that
    no two phases name one action, and that every mover row carries the
    residency block its pin is read from -- a row without one stages its range
    and then gives the tokens back, which nothing but the ledger can see.
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
    ram_tier_id = value.get("ram_tier_id")
    if ram_tier_id is not None:
        if not isinstance(ram_tier_id, str) or not ram_tier_id:
            raise ResidencyPlanError("ram_tier_id must be a tier id")
        if storage_tiers.tier_kind_of(ram_tier_id) != "ram":
            raise ResidencyPlanError(
                f"ram_tier_id {ram_tier_id!r} does not name a ram tier")
    ram_demand_kind: str | None = None
    if ram_tier_id is not None:
        ram_demand_kind = (f"{storage_tiers.capacity_kind_of(ram_tier_id)}"
                           f"{storage_tiers.TIER_DEMAND_SEPARATOR}{ram_tier_id}")
    stage_root = value.get("stage_root")
    if not isinstance(stage_root, str) or not stage_root.startswith("/"):
        raise ResidencyPlanError("stage_root must be an absolute path")
    if "demand_source" in value and not isinstance(value["demand_source"], Mapping):
        raise ResidencyPlanError("demand_source must be an object")
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
        # The stage leg, in either sealed shape (#675): one movement node
        # plus one egress node over the whole range, or one pair per chunk.
        # Either shape or neither -- a phase with no stage leg at all is a
        # plan no version of the submitter ever sealed, and a phase with
        # both seals one shape twice.
        mover = phase.get("mover_row")
        egress = phase.get("egress_row")
        stage_chunks = phase.get("stage_chunks")
        if stage_chunks is not None and (
                mover is not None or egress is not None):
            raise ResidencyPlanError(
                f"plan phase {name!r} carries both a mover and stage "
                f"chunks; chunking is a sealing-time property, and one "
                f"phase seals one shape")
        if stage_chunks is not None:
            if not isinstance(stage_chunks, list) or not stage_chunks:
                raise ResidencyPlanError(
                    f"plan phase {name!r} needs a non-empty stage_chunks list")
            checked_chunks: list[dict[str, object]] = []
            chunk_position = start
            for index, chunk in enumerate(stage_chunks):
                checked_chunk = _checked_leg_chunk(
                    chunk, leg="stage", phase_name=name, chunk_index=index,
                    phase_start=chunk_position, digest=digest,
                    tier_id=tier_id, demand_kind=demand_kind, keys=keys)
                checked_chunks.append(checked_chunk)
                chunk_position = int(checked_chunk["end_bytes"])
            if chunk_position != end:
                raise ResidencyPlanError(
                    f"plan phase {name!r} ends at {end}, not where its last "
                    f"chunk ended ({chunk_position})")
            rows["stage_chunks"] = checked_chunks
        else:
            for role in ("mover_row", "egress_row"):
                row = phase.get(role)
                if not isinstance(row, Mapping):
                    raise ResidencyPlanError(
                        f"plan phase {name!r} needs a {role}")
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
            # ...and it has to carry the pin the row is read for.  A mover row
            # without a residency block publishes, claims, stages its 34 GB and
            # then releases its tier tokens at ``finish``, because
            # ``residency_pin_holds`` reads the queue record and finds no range to
            # check the receipt against.  Nothing downstream can see that: the
            # mover is ``executed``, the files are on the stage, and only the
            # ledger disagrees -- so it is checked here, where the plan is frozen
            # and nothing is queued yet, against the range the phase already
            # declares rather than against itself.
            pin = mover.get("residency")
            if not isinstance(pin, Mapping):
                raise ResidencyPlanError(
                    f"plan phase {name!r} has a mover row with no residency block; "
                    f"its tier tokens would be released the moment it finished")
            if (pin.get("tier_id") != tier_id
                    or pin.get("manifest_sha256") != digest
                    or pin.get("range_start_bytes") != start
                    or pin.get("range_end_bytes") != end):
                raise ResidencyPlanError(
                    f"plan phase {name!r} names bytes {start}..{end} of {digest[:12]} "
                    f"on {tier_id}, and its mover row pins "
                    f"{pin.get('range_start_bytes')}..{pin.get('range_end_bytes')} of "
                    f"{str(pin.get('manifest_sha256'))[:12]} on {pin.get('tier_id')}")
        # The ram leg, when the plan carries one: the same two checks the
        # stage's own row just passed, aimed at the tier the promotion lands
        # on.  A ram mover row without a pin releases its occupancy the
        # moment it finishes -- bytes on a roof-limited tmpfs that no token
        # stands for are ENOSPC waiting to happen (#640).
        ram_mover = phase.get("ram_mover_row")
        ram_egress = phase.get("ram_egress_row")
        ram_chunks = phase.get("ram_chunks")
        if ram_chunks is not None and (
                ram_mover is not None or ram_egress is not None):
            raise ResidencyPlanError(
                f"plan phase {name!r} carries both a ram mover and ram "
                f"chunks; chunking is a sealing-time property, and one "
                f"phase seals one shape")
        if ram_chunks is not None:
            if ram_tier_id is None or ram_demand_kind is None:
                raise ResidencyPlanError(
                    f"plan phase {name!r} carries ram chunks, but the plan "
                    f"names no ram tier for them to promote onto")
            if not isinstance(ram_chunks, list) or not ram_chunks:
                raise ResidencyPlanError(
                    f"plan phase {name!r} needs a non-empty ram_chunks list")
            checked_chunks: list[dict[str, object]] = []
            position = start
            for index, chunk in enumerate(ram_chunks):
                checked_chunk = _checked_leg_chunk(
                    chunk, leg="ram", phase_name=name, chunk_index=index,
                    phase_start=position, digest=digest,
                    tier_id=ram_tier_id,
                    demand_kind=ram_demand_kind, keys=keys)
                checked_chunks.append(checked_chunk)
                position = int(checked_chunk["end_bytes"])
            if position != end:
                raise ResidencyPlanError(
                    f"plan phase {name!r} ends at {end}, not where its last "
                    f"chunk ended ({position})")
            rows["ram_chunks"] = checked_chunks
        if ram_egress is not None and ram_mover is None:
            raise ResidencyPlanError(
                f"plan phase {name!r} has a ram egress with no ram mover to free")
        if ram_mover is not None:
            if not isinstance(ram_mover, Mapping):
                raise ResidencyPlanError(
                    f"plan phase {name!r} needs an object as ram_mover_row")
            if ram_tier_id is None or ram_demand_kind is None:
                raise ResidencyPlanError(
                    f"plan phase {name!r} carries a ram mover, but the plan "
                    f"names no ram tier for it to promote onto")
            ram_key = _action_key(ram_mover.get("action_key"),
                                  where="ram_mover_row.action_key")
            if ram_key in keys:
                raise ResidencyPlanError("two plan rows share an action key")
            keys.add(ram_key)
            ram_pin = ram_mover.get("residency")
            if not isinstance(ram_pin, Mapping):
                raise ResidencyPlanError(
                    f"plan phase {name!r} has a ram mover row with no residency "
                    f"block; its occupancy would be released the moment it "
                    f"finished")
            if (ram_pin.get("tier_id") != ram_tier_id
                    or ram_pin.get("manifest_sha256") != digest
                    or ram_pin.get("range_start_bytes") != start
                    or ram_pin.get("range_end_bytes") != end):
                raise ResidencyPlanError(
                    f"plan phase {name!r} names bytes {start}..{end} of "
                    f"{digest[:12]} for the ram tier {ram_tier_id}, and its ram "
                    f"mover row pins "
                    f"{ram_pin.get('range_start_bytes')}.."
                    f"{ram_pin.get('range_end_bytes')} of "
                    f"{str(ram_pin.get('manifest_sha256'))[:12]} on "
                    f"{ram_pin.get('tier_id')}")
            ram_resources = ram_mover.get("resources")
            if (not isinstance(ram_resources, Mapping)
                    or int(ram_resources.get(ram_demand_kind, 0)) < floor):
                raise ResidencyPlanError(
                    f"plan phase {name!r} asks the ram tier for "
                    f"{None if not isinstance(ram_resources, Mapping) else ram_resources.get(ram_demand_kind)}"
                    f", below the {floor} GiB its range occupies")
            rows["ram_mover_row"] = dict(ram_mover)
        if ram_egress is not None:
            if not isinstance(ram_egress, Mapping):
                raise ResidencyPlanError(
                    f"plan phase {name!r} needs an object as ram_egress_row")
            ram_egress_key = _action_key(ram_egress.get("action_key"),
                                         where="ram_egress_row.action_key")
            if ram_egress_key in keys:
                raise ResidencyPlanError("two plan rows share an action key")
            keys.add(ram_egress_key)
            rows["ram_egress_row"] = dict(ram_egress)
        checked.append({**dict(phase), **rows})
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
    key = str(checked["consumer_action_key"])
    path = queue.residency_plan_path(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    # The consumer's own transition lock, the one ``withdraw`` and the queue's
    # publication use: a plan is filed, marked and reaped exclusively, so a
    # reap cannot archive a body filed between its read and its rename, and a
    # deliberate reseal cannot interleave with the retirement of its
    # predecessor (#708).
    with queue._transition_locked(key):
        try:
            _pool._publish_immutable(
                path, pb._canonical_bytes(checked), where="residency plan")
        except _pool.PoolContractError as exc:
            raise ResidencyPlanError(
                f"a different residency plan is already filed for "
                f"{key}: {exc}") from None
    return checked


def read(queue, consumer_action_key: str, *,
         on_unreadable: Callable[[Exception], None] | None = None,
         ) -> dict[str, object] | None:
    """One consumer's frozen plan, or ``None`` when it has none.

    ``None`` stays the ordinary answer -- almost no action is staged -- and a
    plan that cannot be read or does not validate still answers the same way,
    so a corrupt file leaves the consumer reading the pool rather than
    stopping the loop that was going to stage for somebody else.

    ``on_unreadable`` is how a caller tells those two apart.  It was one
    answer for both until #615: a plan written by a generation that knows one
    more key than this reader does is refused by ``validate_plan``, and the
    coordinator then skipped that consumer every cycle with nothing in its
    log -- 25 minutes of an idle GPU behind a staged head window.  *No plan*
    is nobody's work; *a plan this reader refuses* is a consumer that will
    never be staged for, which is a denial and belongs in a record.  The
    callback is given the refusal, and the answer is still ``None`` so no
    caller has to grow a second branch to stay safe.
    """

    def refused(error: Exception) -> None:
        if on_unreadable is not None:
            on_unreadable(error)

    try:
        raw = Path(queue.residency_plan_path(consumer_action_key)).read_text()
    except FileNotFoundError:
        return None                       # the ordinary answer: none was filed
    except OSError as error:
        # A plan that is there and unreadable -- a torn write, or this mount's
        # quarter-hourly ESTALE (#575).  Not the same as absent.
        refused(error)
        return None
    except ValueError as error:
        # ``residency_plan_path`` refuses a key that is not an action key.
        refused(error)
        return None
    try:
        return validate_plan(json.loads(raw))
    except ValueError as error:
        refused(error)
        return None


def incarnation(path: Path) -> tuple[int, int, int] | None:
    """A filed file's incarnation: the inode, mtime and size it is now.

    Identity for the retirement machinery, not a content hash.  Two seals of
    one body are two files with different inodes, which is exactly what tells
    a deliberate same-body resubmission from the filing a marker was written
    for (#708 review); a content digest cannot.  ``None`` means the file is
    not there (or not statable), never "unchanged".
    """

    try:
        info = path.stat()
    except OSError:
        return None
    return (int(info.st_ino), int(info.st_mtime_ns), int(info.st_size))


def read_filed(queue, consumer_action_key: str, *,
               on_unreadable: Callable[[Exception], None] | None = None,
               ) -> tuple[dict[str, object] | None, tuple[int, int, int] | None]:
    """One filed plan and the incarnation it was read from, both consistent.

    ``read`` alone answers "what does the plan say"; retirement needs "which
    *filing* of it did the caller decide against", and a replacement can land
    between a read and a later stat.  So the stat wraps the read and is
    repeated until it brackets one unchanged file -- the same filing the
    marker's incarnation check is later made against.
    """

    key = _action_key(consumer_action_key, where="consumer_action_key")
    path = queue.residency_plan_path(key)
    for _attempt in range(3):
        before = incarnation(path)
        if before is None:
            return None, None
        plan = read(queue, key, on_unreadable=on_unreadable)
        if plan is None:
            return None, None
        if incarnation(path) == before:
            return plan, before
    return None, None      # churning under us: defer to the next cycle


def _retired_slug(value: object) -> str:
    """A short, filesystem-safe word for why a plan was retired."""

    cleaned = re.sub(r"[^a-z0-9]+", "-", str(value or "").lower()).strip("-")
    return cleaned[:48] or "retired"


def superseded_path(queue, plan: Mapping[str, object]) -> Path:
    """The identity-bound address of one plan filing's retirement marker."""

    key = _action_key(plan.get("consumer_action_key"),
                      where="consumer_action_key")
    return _superseded_path(queue, key, plan_sha256(plan))


def _superseded_path(queue, consumer_action_key: str,
                     plan_sha256: str) -> Path:
    """One plan *identity*'s retirement marker: a sibling of the live plan.

    Keyed by the plan body's digest as well as the consumer, so a marker
    cannot cover a later plan of the same consumer -- a stale cancellation
    must never retire a concurrent replacement.
    """

    return (queue.residency_plan_path(consumer_action_key).parent / SUPERSEDED
            / f"{consumer_action_key}.{plan_sha256}.superseded.json")


def plan_sha256(plan: Mapping[str, object]) -> str:
    """The identity of one frozen plan body, the way ``freeze`` wrote it."""

    return hashlib.sha256(
        pb._canonical_bytes(validate_plan(plan))).hexdigest()


def _read_marker(path: Path) -> tuple[str, dict[str, object] | None, str | None]:
    """``(state, marker, error)``: absent, ok, or unreadable with its reason.

    Enumerated before reading, because this mount's negative cache can hide a
    marker another box just wrote; and a marker this reader cannot parse is
    *unreadable*, a state no caller may read as "no marker".
    """

    try:
        paths = _pool._glob(path.parent, path.name)
    except OSError as exc:                                    # pragma: no cover
        return "unreadable", None, f"{type(exc).__name__}: {exc}"
    if not paths:
        return "absent", None, None
    try:
        raw = json.loads(path.read_text())
    except (OSError, ValueError) as exc:
        return "unreadable", None, f"{type(exc).__name__}: {exc}"
    if not isinstance(raw, Mapping):
        return "unreadable", None, "marker is not a JSON object"
    return "ok", dict(raw), None


def superseded(queue, plan: Mapping[str, object]) -> dict[str, object] | None:
    """The retirement marker covering *this filing* of one plan, or ``None``.

    The canonical plan body stays where it was filed: ``read`` still answers
    with it, which is what keeps a withdrawn consumer's queued children
    attributable for ``withdraw_dead_consumer_movers`` and what keeps a
    running consumer's resident ranges named for the orphan sweep and
    adoption.  What the marker changes is *publication*: the coordinator and
    the planner consult it before staging anything from the plan, and a
    resubmission seals a fresh plan once the old window's ownership has
    ended.

    Two identities are checked, because neither alone is enough.  The digest
    keys the marker to the body it retired; the file incarnation ties it to
    the *filing*, so a deliberate same-body resubmission after a reap is not
    covered by the marker of the body it replaced.

    Unknown retirement is not "not retired": an unreadable or malformed
    marker answers with a record carrying ``unreadable`` (and the reason),
    which callers must refuse or defer on.  ``None`` means an absent marker,
    or one written for another filing of the same body.
    """

    key = _action_key(plan.get("consumer_action_key"),
                      where="consumer_action_key")
    digest = plan_sha256(plan)
    state, marker, error = _read_marker(_superseded_path(queue, key, digest))
    if state == "absent":
        return None
    if state == "unreadable" or marker is None:
        return {
            "schema": RESIDENCY_PLAN_SUPERSEDED_SCHEMA_V1,
            "consumer_action_key": key, "plan_sha256": digest,
            "reason": "unreadable-marker", "unreadable": True,
            "error": error or "unreadable marker",
        }
    if (marker.get("schema") != RESIDENCY_PLAN_SUPERSEDED_SCHEMA_V1
            or marker.get("consumer_action_key") != key
            or marker.get("plan_sha256") != digest):
        return {
            "schema": RESIDENCY_PLAN_SUPERSEDED_SCHEMA_V1,
            "consumer_action_key": key, "plan_sha256": digest,
            "reason": "corrupt-marker", "unreadable": True,
            "error": "marker identity disagrees with its address",
        }
    stamped = marker.get("plan_incarnation")
    current = incarnation(queue.residency_plan_path(key))
    if (not isinstance(stamped, list) or len(stamped) != 3
            or current is None
            or tuple(int(value) for value in stamped) != current):
        # A marker for another filing of the same body.  A deliberate
        # same-body resubmission after a reap is not covered by it.
        return None
    return marker


def mark_superseded(queue, consumer_action_key: str, *,
                    plan: Mapping[str, object] | None = None,
                    filing: tuple[int, int, int] | None = None,
                    reason: str = "", movers: Sequence[str] = (),
                    by: str = "") -> dict[str, object] | None:
    """Mark one *filing* of a frozen plan superseded, under its own lock.

    A withdrawal is a decision about the *window* that minted the withdrawn
    action, not about one row.  Rows cannot be edited or repriced in place --
    a mover's action key hashes the resources and argv it was sealed with
    (#710) -- and a window whose mover an operator cancelled cannot be
    published again without overriding that decision (#708).  The marker is
    the supported answer: publication from the plan stops, its bytes and
    fragments stay attributable until the work ends, and a deliberate
    resubmission can seal a fresh plan at the current price.

    ``plan`` and ``filing`` are the caller's decision inputs, from
    :func:`read_filed`.  Under the consumer's transition lock the current
    file is re-read and its incarnation compared, so a body that changed
    after the caller decided is never marked -- a stale cancellation does not
    retire a concurrent replacement.  With neither given, the current filing
    is marked, which is what withdrawing an action by its own key means.

    Idempotent and first-writer; the marker names the filing it covers.
    """

    key = _action_key(consumer_action_key, where="consumer_action_key")
    with queue._transition_locked(key):
        current = incarnation(queue.residency_plan_path(key))
        if current is None:
            return None
        if filing is not None and tuple(filing) != current:
            return None       # the filing changed under the caller: defer
        current_plan = read(queue, key)
        if current_plan is None:
            return None
        digest = plan_sha256(current_plan)
        if plan is not None and plan_sha256(plan) != digest:
            return None
        path = _superseded_path(queue, key, digest)
        state, existing, _error = _read_marker(path)
        if state == "unreadable":
            return None       # unknown state: an operator resolves it
        if state == "ok" and existing is not None:
            stamped = existing.get("plan_incarnation")
            if (isinstance(stamped, list) and len(stamped) == 3
                    and tuple(int(value) for value in stamped) == current):
                return existing
            # A marker for an earlier filing of the same body that a failed
            # reap left at the active address: evidence now, not authority.
            _retire_marker(queue, key, digest, current)
        marker: dict[str, object] = {
            "schema": RESIDENCY_PLAN_SUPERSEDED_SCHEMA_V1,
            "consumer_action_key": key,
            "plan_sha256": digest,
            "plan_incarnation": list(current),
            "reason": str(reason),
            "marked_unix": time.time(),
            "marked_by": str(by),
            "movers": [str(mover) for mover in movers],
        }
        try:
            _pool._publish_immutable(
                path, pb._canonical_bytes(marker),
                where="residency plan supersession")
        except _pool.PoolContractError:
            # A concurrent mark of this exact filing won; its marker is the
            # decision, not this one.
            state, existing, _error = _read_marker(path)
            return existing if state == "ok" else None
        return marker


def child_keys(plan: Mapping[str, object]) -> list[str]:
    """Every movement and egress key the plan will ever publish, both legs.

    ``mover_keys`` answers what may hold tier occupancy; this answers what may
    be *live work* -- a queued or claimed row of either kind -- which is the
    question a handoff has to ask before a fresh plan replaces a frozen one.
    """

    phases = plan["phases"]
    assert isinstance(phases, list)
    out: list[str] = []
    for phase in phases:
        for mover_role, egress_role, chunk_table in (
                ("mover_row", "egress_row", "stage_chunks"),
                ("ram_mover_row", "ram_egress_row", "ram_chunks")):
            chunks = phase.get(chunk_table)
            if isinstance(chunks, list):
                for chunk in chunks:
                    out.append(str(chunk[mover_role]["action_key"]))
                    out.append(str(chunk[egress_role]["action_key"]))
                continue
            if mover_role in phase:
                out.append(str(phase[mover_role]["action_key"]))
            if egress_role in phase:
                out.append(str(phase[egress_role]["action_key"]))
    return out


def handoff_safe(queue, consumer_action_key: str,
                 plan: Mapping[str, object]) -> tuple[bool, str]:
    """Whether a superseded window's ownership has ended.

    A fresh plan is a different decomposition: its mover keys differ, so
    replacing the old one while any of the old window's work is still live
    would strand a queued or running row nobody can publish, egress or
    attribute.  The test is the queue's own state, never a clock:

    * the consumer itself must not be in ``ready/`` or ``claimed/`` -- a live
      window is not handed off from underneath, it is withdrawn first;
    * no movement or egress row the plan sealed may be queued or claimed.

    Resident ranges are deliberately not part of the test: their tokens are
    held by their own keys, a successor adopts them by descriptor, and
    nothing about a handoff releases them.
    """

    for state, label in ((_pool.CLAIMED, "claimed"), (_pool.READY, "queued")):
        if queue.item_path(state, consumer_action_key).exists():
            return False, f"the consumer is still {label}"
    for key in child_keys(plan):
        if queue.item_path(_pool.CLAIMED, key).exists():
            return False, f"its row {key[:12]} is claimed"
        if queue.item_path(_pool.READY, key).exists():
            return False, f"its row {key[:12]} is queued"
    return True, "no live consumer and no queued or claimed child"


def _retire_marker(queue, consumer_action_key: str, digest: str,
                   filing: tuple[int, int, int]) -> None:
    """Move a reaped filing's active marker to evidence, under its lock.

    The active marker means "the filed body is superseded".  Once the body is
    archived, a later seal of even the identical body is a new decision and
    must start without it; the marker is retained beside the archived body
    rather than deleted.
    """

    path = _superseded_path(queue, consumer_action_key, digest)
    try:
        if not path.is_file():
            return
        evidence = path.parent / (
            f"{consumer_action_key}.{digest}.{filing[1]}.{time.time():.6f}"
            f".marker.json")
        os.replace(path, evidence)
    except OSError:                                           # pragma: no cover
        return


def reap(queue, consumer_action_key: str, *,
         reason: str = "",
         plan: Mapping[str, object] | None = None,
         filing: tuple[int, int, int] | None = None,
         ) -> dict[str, object] | None:
    """Archive a frozen plan whose ownership has ended; ``None`` while it has not.

    This is the physical half of retirement, and it is deliberately delayed:
    the marker stops publication at once, and the body moves out of the live
    directory only when :func:`handoff_safe` says no consumer and no queued or
    claimed child still names it.  Until then every reader -- the dead
    consumer's mover withdrawal, the running consumer's orphan protection,
    the planner's reuse -- sees the filed body exactly as it was sealed.

    Serialized on the consumer's transition lock against ``freeze``,
    ``mark_superseded`` and every other reaper, and the exact filing the
    caller decided against is rechecked inside that lock: a concurrent
    resubmission cannot have its new plan archived by a stale reaper, even
    when the new body is byte-identical.  The filing's active marker is
    retired with it, so the next seal starts clean.

    ``None`` means "nothing moved": there was no plan, its body no longer
    validates (no cover, so no handoff can be shown safe), its work is still
    live, or the filing changed since the caller read it.
    """

    key = _action_key(consumer_action_key, where="consumer_action_key")
    with queue._transition_locked(key):
        filed, current = read_filed(queue, key)
        if filed is None or current is None:
            return None
        if filing is not None and tuple(filing) != current:
            return None       # a different filing is filed now
        if plan is not None and plan_sha256(plan) != plan_sha256(filed):
            return None
        safe, _why = handoff_safe(queue, key, filed)
        if not safe:
            return None
        path = queue.residency_plan_path(key)
        target = (path.parent / SUPERSEDED
                  / f"{path.stem}.{time.time():.6f}.{_retired_slug(reason)}.json")
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.replace(path, target)
        except FileNotFoundError:
            return None           # something else reaped it first
        _retire_marker(queue, key, plan_sha256(filed), current)
        return filed


def lead_mover_row(plan: Mapping[str, object]) -> dict[str, object]:
    """The row the submitter publishes at once: the first chunk's, or the mover's.

    The consumer depends only on its first phase, and a first phase sealed
    chunked (#675) starts with its first chunk: the rest is the tiers loop's
    to publish as accepted progress advances.
    """

    phases = plan["phases"]
    assert isinstance(phases, list)
    first = phases[0]
    chunks = first.get("stage_chunks")
    if isinstance(chunks, list):
        assert isinstance(chunks[0], Mapping)
        return dict(chunks[0]["mover_row"])  # type: ignore[index]
    return dict(first["mover_row"])


def leads_for(plan: Mapping[str, object]) -> list[str]:
    """The movers the consumer's admission depends on: its first phase, only.

    Depending on more than the first phase is what would let the consumer wait
    on a mover the window has not published yet, while the window waits on the
    consumer's progress to publish it.
    """

    return [str(lead_mover_row(plan)["action_key"])]


def mover_keys(plan: Mapping[str, object]) -> list[str]:
    """Every movement node this plan will ever have, published or not.

    The orphan sweep needs this and not just the leads: a pinned mover for a
    phase the consumer has not reached is named by nothing in the queue, and a
    sweep that tested only live items' ``leads`` would evict the window it is
    there to protect.  A plan's ram promotions are movement nodes of the same
    plan (#640), so their keys are here too -- a pinned promotion no live item
    names is an orphan on the ram tier exactly as its stage sibling is.
    """

    phases = plan["phases"]
    assert isinstance(phases, list)
    out = stage_mover_keys(plan)
    out += ram_mover_keys(plan)
    return out


def stage_mover_keys(plan: Mapping[str, object]) -> list[str]:
    """Every stage movement node this plan will ever have, published or not.

    A chunked phase contributes one key per chunk, in chunk order (#675);
    a whole-phase leg contributes its mover's key, exactly as before.
    """

    phases = plan["phases"]
    assert isinstance(phases, list)
    out = []
    for phase in phases:
        chunks = phase.get("stage_chunks")
        if isinstance(chunks, list):
            out += [str(chunk["mover_row"]["action_key"])
                    for chunk in chunks]
        elif "mover_row" in phase:
            out.append(str(phase["mover_row"]["action_key"]))
    return out


def ram_mover_keys(plan: Mapping[str, object]) -> list[str]:
    """Every promotion node this plan will ever have, published or not.

    Empty for a plan whose submitter sealed no ram leg, which is the answer
    that leaves those consumers staged exactly as they were before the tier
    existed.  A chunked phase contributes one key per chunk, in chunk order.
    """

    phases = plan["phases"]
    assert isinstance(phases, list)
    out = []
    for phase in phases:
        chunks = phase.get("ram_chunks")
        if isinstance(chunks, list):
            out += [str(chunk["ram_mover_row"]["action_key"])
                    for chunk in chunks]
        elif "ram_mover_row" in phase:
            out.append(str(phase["ram_mover_row"]["action_key"]))
    return out


def remaining(plan: Mapping[str, object],
              accepted_phase: str | None) -> list[dict[str, object]]:
    """The phases the consumer is reading now or has not reached yet.

    The phase its progress names is *included*: counting it as finished would
    take back a window the consumer is still inside, which is the rule
    ``prewarm_loop.consumed_through`` keeps.  A name the plan does not carry --
    no progress yet, or a phase from another plan -- reads as the beginning,
    because a consumer that has not said where it is has not passed anything.
    """

    phases = list(plan["phases"])                                # type: ignore[arg-type]
    names = [str(phase["name"]) for phase in phases]
    return phases[names.index(accepted_phase) if accepted_phase in names else 0:]


def accepted(plan: Mapping[str, object], accepted_phase: str | None) -> bool:
    """Whether the consumer has accepted a phase of *this* plan.

    ``remaining`` already reads a name the plan does not carry as the
    beginning, for the reason it says: a consumer that has not said where it
    is has not passed anything.  The run-ahead bound has to read it the same
    way, or a stale name -- another plan's phase, a phase renamed by a
    resubmission -- would buy the deeper budget that only demonstrated
    progress earns.
    """

    if accepted_phase is None:
        return False
    phases = plan["phases"]
    assert isinstance(phases, list)
    return any(str(phase["name"]) == accepted_phase for phase in phases)


def runahead_step_gib(plan: Mapping[str, object],
                      accepted_phase: str | None) -> int:
    """The largest single phase still ahead of the consumer, in tier tokens.

    One quantity, used twice and in opposite directions: it is the whole
    run-ahead budget of a consumer that has accepted nothing, and it is the
    room a rolling window must leave the tier so the stage never reaches 0 B.
    Both readings say the same thing -- *one more phase* -- which is what
    ``tier_loop.window_pressure`` already calls "what the tier must be able to
    offer".  The largest rather than the next, so the answer does not depend
    on which phase happens to come first in a plan whose phases differ by 65%.
    """

    ahead = remaining(plan, accepted_phase)[1:]
    return max((int(phase["stage_gib"]) for phase in ahead), default=0)


def _budget_from_step(step: int, *, has_accepted: bool,
                      capacity_gib: int | None,
                      runahead_cap_gib: int | None = None) -> int | None:
    """The two-regime run-ahead bound for one step size, or ``None``.

    ``None`` is "the tier's free capacity is the only bound", which is what
    a caller that cannot say what the tier's capacity is gets.  The regimes
    are ``runahead_budget_gib``'s: a consumer that has accepted nothing gets
    one step, a rolling one gets the tier less one step of room.
    """

    if (runahead_cap_gib is not None and (
            isinstance(runahead_cap_gib, bool)
            or not isinstance(runahead_cap_gib, int) or runahead_cap_gib <= 0)):
        raise ResidencyPlanError("runahead_cap_gib must be a positive whole GiB")
    if not has_accepted:
        budget = step
    elif capacity_gib is None:
        budget = None
    else:
        budget = max(0, int(capacity_gib) - step)
    if budget is None or runahead_cap_gib is None:
        return budget
    return min(budget, runahead_cap_gib)


def runahead_budget_gib(plan: Mapping[str, object], accepted_phase: str | None,
                        *, capacity_gib: int | None,
                        runahead_cap_gib: int | None = None) -> int | None:
    """How many tokens the window may hold ahead of the consumer, or ``None``.

    ``None`` is "the tier's free capacity is the only bound", which is what a
    caller that cannot say what the tier's capacity is gets: this module will
    not invent a capacity, and a bound derived from a number nobody minted
    would be the heuristic the explicit exists to replace.

    ``runahead_cap_gib`` is a declared ceiling on the same budget -- the
    policy's ``prefill_depth``, when one is set.  It tightens the bound; it
    never loosens it, and ``None`` keeps the two-regime semantics above.
    """

    return _budget_from_step(
        runahead_step_gib(plan, accepted_phase),
        has_accepted=accepted(plan, accepted_phase),
        capacity_gib=capacity_gib, runahead_cap_gib=runahead_cap_gib)


#: Which chunk table a window decision reads, by mover role: the stage
#: window reads ``stage_chunks`` (#675), the ram window ``ram_chunks`` (#673).
_CHUNK_TABLES = {
    "mover_row": "stage_chunks",
    "ram_mover_row": "ram_chunks",
}


def _legs(plan: Mapping[str, object], *, mover_role: str) -> list[dict[str, object]]:
    """One publishable unit per phase, or per chunk of a chunked phase.

    A chunked leg stages and frees per chunk: each chunk is a leg carrying
    its phase, its chunk index and its own mover and egress rows, in read
    order -- ``stage_chunks`` for the stage window, ``ram_chunks`` for the
    ram window.  Every other leg is one leg over the phase's whole range
    with no chunk index, which is what keeps those decisions byte-identical
    to today.
    """

    egress_role = _MOVEMENT_ROLES[mover_role]
    legs = []
    for phase in plan["phases"]:
        assert isinstance(phase, Mapping)
        chunks = phase.get(_CHUNK_TABLES[mover_role])
        if isinstance(chunks, list):
            for chunk in chunks:
                assert isinstance(chunk, Mapping)
                legs.append({
                    "phase": str(phase["name"]),
                    "chunk_index": int(chunk["chunk_index"]),
                    "start_bytes": int(chunk["start_bytes"]),
                    "end_bytes": int(chunk["end_bytes"]),
                    "stage_gib": int(chunk["stage_gib"]),
                    "mover_row": chunk[mover_role],
                    "egress_row": chunk[egress_role],
                })
        elif mover_role in phase:
            legs.append({
                "phase": str(phase["name"]),
                "chunk_index": None,
                "start_bytes": int(phase["start_bytes"]),
                "end_bytes": int(phase["end_bytes"]),
                "stage_gib": int(phase["stage_gib"]),
                "mover_row": phase[mover_role],
                "egress_row": phase[egress_role],
            })
        # Else sealed without this tier's leg: nothing here to publish,
        # hold or evict, and the stage window owns whatever it carries.
    return legs


def window(plan: Mapping[str, object], *, accepted_phase: str | None,
           free_gib: int, capacity_gib: int | None = None,
           published: Sequence[str] = (),
           staged: Sequence[str] = (),
           runahead_cap_gib: int | None = None,
           mover_role: str = "mover_row",
           withdrawn: Sequence[str] = ()) -> dict[str, object]:
    """What the coordinator should publish and evict on this cycle.

    ``accepted_phase`` is the phase the consumer's progress record says it is
    reading *now*, so the phases before it are finished with and may be
    evicted; counting the named phase itself would take back a window the
    consumer is still inside, the same rule ``prewarm_loop.consumed_through``
    keeps.  ``published`` names the movers already queued, claimed or terminal
    **and still holding their tokens**; ``staged`` those whose bytes are on the
    tier now.

    The decision is about one movement leg, and ``mover_role`` names it:
    ``"mover_row"`` for the stage window, ``"ram_mover_row"`` for the ram
    window (#640).  ``published`` and ``staged`` hold that leg's own action
    keys, so every membership test here runs against the leg's keys -- a ram
    window fed promotion keys would find no stage key resident, never evict
    and never recognise its own published promotions, which is the hole the
    parameter closes.  A phase the submitter sealed without this leg has
    nothing on the tier: published by nobody, evicted by nobody.  Each
    entry's ``mover_row`` and ``egress_row`` are the pair the role names.

    ``withdrawn`` names the leg's action keys that carry a live withdrawal
    marker.  Such a leg is never published: its key is a content hash, so
    publishing it would retire the operator's marker and run the cancelled
    copy again at the price it was cancelled for, which is the state #708
    was filed about.  It is not a stall and it takes no room -- the next
    publishable leg is what the consumer can still be staged with -- and the
    coordinator retires the plan that names it, so this is the guard against
    a marker filed between that pass and this decision rather than the
    decision itself.  Eviction is untouched: an egress frees bytes nobody
    has cancelled.

    A phase the submitter sealed chunked decides per chunk: each chunk is
    a leg with its own ``chunk_index``, its own range and its own rows, in
    read order -- ``stage_chunks`` on the stage leg (#675), ``ram_chunks``
    on the ram leg (#673).  Chunks of the phase being read are the reader's
    near-term food -- they publish as their turn comes, outside the
    run-ahead budget -- while later chunks spend it, so the budget buys
    several chunks instead of zero phases.  A chunk of a passed phase
    evicts through its own egress node; a leg sealed whole carries no
    ``chunk_index`` and decides exactly as it always did.

    A mover that is terminal but no longer pinned is deliberately absent from
    ``published``: its key is a content hash, so a second campaign over the
    same manifest seals the same key, and a ``done`` record left from an
    already-evicted range would otherwise satisfy nothing and be republished by
    nobody.  The window is what fits: phases are published in read order while
    the tier's free capacity covers the next one, so a starved mover is rarely
    in ``ready/`` at all and the tokens stay the safety net rather than the
    schedule.

    ...and while the run-ahead bound covers it, which is the second half #632
    added: free capacity brakes at 0 B, which is too late for a stage that has
    to keep its own ownership marker writable.  ``capacity_gib`` is the tier's
    minted total (``ResourceLedger.capacity``); leaving it out keeps free
    capacity as the only bound for a consumer that is reporting.  The answer
    carries ``stall``: ``None``, or what the window declined to publish and
    what it is waiting for.
    """

    if mover_role not in _MOVEMENT_ROLES:
        raise ResidencyPlanError(
            f"mover_role must be one of {sorted(_MOVEMENT_ROLES)}, "
            f"not {mover_role!r}")
    phases = list(plan["phases"])                                # type: ignore[arg-type]
    ahead = remaining(plan, accepted_phase)
    ahead_names = [str(phase["name"]) for phase in ahead]
    current_name = ahead_names[0] if ahead_names else None
    passed = {str(phase["name"]) for phase in phases[:len(phases) - len(ahead)]}
    already = set(published)
    resident = set(staged)
    cancelled = set(withdrawn)
    legs = _legs(plan, mover_role=mover_role)

    evict = []
    for leg in legs:
        if leg["phase"] not in passed:
            continue
        key = str(leg["mover_row"]["action_key"])  # type: ignore[index]
        if key not in resident:
            continue
        entry: dict[str, object] = {"phase": leg["phase"]}
        if leg["chunk_index"] is not None:
            entry["chunk_index"] = leg["chunk_index"]
        entry.update({"mover_action_key": key, "egress_row": leg["egress_row"],
                      "stage_gib": leg["stage_gib"]})
        evict.append(entry)

    publish: list[dict[str, object]] = []
    room = int(free_gib)
    has_accepted = accepted(plan, accepted_phase)
    future = [leg for leg in legs if leg["phase"] in ahead_names[1:]]
    step = max((int(leg["stage_gib"]) for leg in future), default=0)
    budget = _budget_from_step(
        step, has_accepted=has_accepted, capacity_gib=capacity_gib,
        runahead_cap_gib=runahead_cap_gib)
    # Everything the window already holds beyond the phase being read.  The
    # phase the consumer is inside is not run-ahead: it is the work -- and
    # for a chunked leg that means the current phase's chunks promote as
    # their turn comes, while later chunks spend the budget (#673).
    runahead = sum(int(leg["stage_gib"]) for leg in future
                   if str(leg["mover_row"]["action_key"]) in already)  # type: ignore[index]
    stall: dict[str, object] | None = None
    for leg in legs:
        if leg["phase"] not in ahead_names:
            continue
        key = str(leg["mover_row"]["action_key"])  # type: ignore[index]
        if key in already or key in cancelled:
            continue
        need = int(leg["stage_gib"])
        is_current = leg["phase"] == current_name
        if not is_current and budget is not None and runahead + need > budget:
            stall = {
                "consumer_action_key": plan["consumer_action_key"],
                "tier_id": plan["tier_id"],
                "accepted_phase": accepted_phase,
                "reading_phase": current_name,
                "blocked_phase": str(leg["phase"]),
                "blocked_gib": need,
                "runahead_gib": runahead,
                "runahead_budget_gib": budget,
                "free_gib": int(free_gib),
                "capacity_gib": None if capacity_gib is None else int(capacity_gib),
                "reason": ("runahead_budget" if has_accepted
                           else "no_accepted_progress"),
                "waiting_for": (
                    f"accepted progress past {accepted_phase}" if has_accepted
                    else "the consumer's first accepted progress record"),
            }
            if leg["chunk_index"] is not None:
                stall["chunk_index"] = leg["chunk_index"]
            break
        if need > room:
            break
        room -= need
        if not is_current:
            runahead += need
        row: dict[str, object] = {"phase": leg["phase"]}
        if leg["chunk_index"] is not None:
            row["chunk_index"] = leg["chunk_index"]
        row.update({
            "mover_action_key": key,
            "start_bytes": leg["start_bytes"], "end_bytes": leg["end_bytes"],
            "stage_gib": need, "mover_row": leg["mover_row"],
        })
        publish.append(row)
    return {"publish": publish, "evict": evict, "stall": stall}


__all__ = [
    "RESIDENCY_PLAN_SCHEMA_V1",
    "ResidencyPlanError",
    "accepted",
    "build_plan",
    "freeze",
    "lead_mover_row",
    "leads_for",
    "mover_keys",
    "ram_mover_keys",
    "read",
    "remaining",
    "runahead_budget_gib",
    "runahead_step_gib",
    "stage_mover_keys",
    "validate_plan",
    "window",
]
