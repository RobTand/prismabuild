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

from collections.abc import Callable, Collection, Mapping, Sequence
import hashlib
import json
import math
import os
from pathlib import Path
import re
import time

from . import core as pb
from . import pool as _pool
from . import residency_map
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
    "ram_tier_id",
    # Optional: what the consumer declares about its own reading (#909), so
    # that nothing has to stand in for it before it is measured.  Absent on
    # every plan sealed without a declaration, which is then byte-identical
    # to one sealed before the field existed.
    "reader"})
#: What a reader declaration says, and nothing else (#909).  Either or both:
#: ``prefetch_depth_bytes`` is how many bytes the consumer holds ahead of the
#: phase it is reading, and ``read_mb_s`` is how fast it reads, in the
#: decimal MB/s the fill ledger counts in.
_READER_KEYS = frozenset({"prefetch_depth_bytes", "read_mb_s"})
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
               reader: Mapping[str, object] | None = None,
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

    ``reader`` is the consumer's declaration about its own reading (#909);
    see :func:`declared_prefetch_bytes` and :func:`declared_read_bytes_per_s`.
    An empty or absent declaration adds nothing to the plan.
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
    if reader:
        body["reader"] = dict(reader)
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


def _check_reader(reader: object) -> None:
    """Refuse a reader declaration that is not whole, positive numbers (#909).

    Whole numbers because both are quotations a gate compares with ledger
    arithmetic in bytes and in the whole MB/s the fill ledger counts, and a
    declaration nothing can read is refused here, where the plan is frozen,
    rather than priced as absent later.
    """

    if not isinstance(reader, Mapping) or not reader:
        raise ResidencyPlanError(
            "reader must be an object declaring prefetch_depth_bytes, "
            "read_mb_s or both")
    stray = sorted(set(reader) - _READER_KEYS)
    if stray:
        raise ResidencyPlanError(f"unknown reader fields: {stray}")
    depth = reader.get("prefetch_depth_bytes")
    if "prefetch_depth_bytes" in reader and (
            isinstance(depth, bool) or not isinstance(depth, int) or depth < 0):
        raise ResidencyPlanError(
            "reader.prefetch_depth_bytes must be a whole number of bytes, 0 or more")
    rate = reader.get("read_mb_s")
    if "read_mb_s" in reader and (
            isinstance(rate, bool) or not isinstance(rate, int) or rate <= 0):
        raise ResidencyPlanError("reader.read_mb_s must be a positive whole MB/s")


def declared_prefetch_bytes(plan: Mapping[str, object]) -> int | None:
    """The bytes the consumer declares it holds ahead of its read, or ``None`` (#909)."""

    reader = plan.get("reader")
    depth = reader.get("prefetch_depth_bytes") if isinstance(reader, Mapping) else None
    return depth if isinstance(depth, int) and not isinstance(depth, bool) else None


def declared_read_bytes_per_s(plan: Mapping[str, object]) -> float | None:
    """The consumer's declared read rate in bytes per second, or ``None`` (#909)."""

    reader = plan.get("reader")
    rate = reader.get("read_mb_s") if isinstance(reader, Mapping) else None
    if isinstance(rate, int) and not isinstance(rate, bool) and rate > 0:
        return float(rate) * storage_tiers.MB
    return None


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
    if "reader" in value:
        _check_reader(value["reader"])
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


def _stat_incarnation(path: Path) -> tuple[tuple[int, int, int] | None, OSError | None]:
    """One filing's incarnation, keeping absence apart from unavailable.

    ``incarnation`` answers ``None`` for both because a caller that only asks
    "is a plan there" may treat them alike.  A caller deciding against a
    filing may not: a stat that failed is unknown state, not evidence the
    file is gone, and a fresh seal over unknown state is a guess (#708
    review).  Returns ``(incarnation, None)``, ``(None, None)`` for an absent
    file, or ``(None, error)`` for a stat that failed.
    """

    try:
        info = path.stat()
    except FileNotFoundError:
        return None, None
    except OSError as error:
        return None, error
    return (int(info.st_ino), int(info.st_mtime_ns), int(info.st_size)), None


def incarnation(path: Path) -> tuple[int, int, int] | None:
    """A filed file's incarnation: the inode, mtime and size it is now.

    Identity for the retirement machinery, not a content hash.  Two seals of
    one body are two files with different inodes, which is exactly what tells
    a deliberate same-body resubmission from the filing a marker was written
    for (#708 review); a content digest cannot.  ``None`` means the file is
    not there (or not statable), never "unchanged"; a caller that must tell
    those apart reads through :func:`read_filed`, which retains the
    diagnostic.
    """

    return _stat_incarnation(path)[0]


def read_filed(queue, consumer_action_key: str, *,
               on_unreadable: Callable[[Exception], None] | None = None,
               ) -> tuple[dict[str, object] | None, tuple[int, int, int] | None]:
    """One filed plan and the incarnation it was read from, both consistent.

    ``read`` alone answers "what does the plan say"; retirement needs "which
    *filing* of it did the caller decide against", and a replacement can land
    between a read and a later stat.  So the stat wraps the read and is
    repeated until it brackets one unchanged file -- the same filing the
    marker's incarnation check is later made against.

    An incarnation that could not be read at all -- a stat that failed, as
    opposed to a file that is absent -- is *not* answered as "no plan filed":
    it is reported through ``on_unreadable``, because a caller about to seal
    or reap over it holds unknown state, and the only safe reading of unknown
    state is deferral (#708 review).  ``read``'s own refusals arrive through
    the same callback.
    """

    key = _action_key(consumer_action_key, where="consumer_action_key")

    def refused(error: Exception) -> None:
        if on_unreadable is not None:
            on_unreadable(error)

    path = queue.residency_plan_path(key)
    for _attempt in range(3):
        before, error = _stat_incarnation(path)
        if error is not None:
            refused(error)
            return None, None
        if before is None:
            return None, None
        plan = read(queue, key, on_unreadable=on_unreadable)
        if plan is None:
            return None, None
        after, error = _stat_incarnation(path)
        if error is not None:
            refused(error)
            return None, None
        if after == before:
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


def find_mover_leg(plan: Mapping[str, object], mover_action_key: str,
                   ) -> dict[str, object] | None:
    """The sealed leg this mover was cut for, or ``None`` when it has none.

    Searched over every movement role (stage and ram legs, whole-phase and
    chunked): a mover the frozen decomposition never named -- a foreign key,
    a stale member of a replaced plan, a row hand-typed beside the plan --
    has no leg here.  The answer carries the leg's own ``mover_role``,
    ``phase``, ``start_bytes``, ``end_bytes`` and ``stage_gib``, quoted from
    the sealed plan rather than from any caller-supplied row, so a funding
    check can bind credit to the plan's range instead of trusting the row's.
    Never raises for plan-shape reasons; unknown is ``None``.
    """

    try:
        validated = validate_plan(plan)
    except (ValueError, TypeError, AttributeError):
        return None
    key = str(mover_action_key)
    try:
        for role in sorted(_MOVEMENT_ROLES):
            for leg in _legs(validated, mover_role=role):
                row = leg.get("mover_row")
                if (isinstance(row, Mapping)
                        and str(row.get("action_key")) == key):
                    return {"mover_role": role,
                            "phase": str(leg.get("phase")),
                            "start_bytes": int(leg.get("start_bytes")),  # type: ignore[arg-type]
                            "end_bytes": int(leg.get("end_bytes")),  # type: ignore[arg-type]
                            "stage_gib": int(leg.get("stage_gib")),  # type: ignore[arg-type]
                            "chunk_index": leg.get("chunk_index")}
    except (ValueError, TypeError, AttributeError, KeyError):
        return None
    return None


def _stamped_incarnation(value: object) -> tuple[int, int, int] | None:
    """A marker's filing stamp, only when it is three whole integers.

    The marker is JSON written by another process on a shared mount.  The one
    answer that may never be read as "a different filing" -- which authorizes
    reuse and republication -- is a stamp this reader could not parse, so a
    missing, wrongly shaped, non-integer or boolean stamp answers ``None``
    here and every caller turns that into a deferral (#708 review).
    """

    if not isinstance(value, list) or len(value) != 3:
        return None
    if any(isinstance(item, bool) or not isinstance(item, int) for item in value):
        return None
    return (int(value[0]), int(value[1]), int(value[2]))


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
    stamped = _stamped_incarnation(marker.get("plan_incarnation"))
    if stamped is None:
        # A stamp this reader cannot parse says nothing about which filing
        # the marker covers, and unknown retirement is not "not retired".
        return {
            "schema": RESIDENCY_PLAN_SUPERSEDED_SCHEMA_V1,
            "consumer_action_key": key, "plan_sha256": digest,
            "reason": "malformed-incarnation", "unreadable": True,
            "error": ("the marker's plan_incarnation is not three whole "
                      f"integers: {marker.get('plan_incarnation')!r}"),
        }
    current = incarnation(queue.residency_plan_path(key))
    if current is None:
        # The marker names a filing this reader cannot stat.  Only a
        # successfully read current identity may prove the marker covers a
        # different filing; an unavailable one defers (#708 review).
        return {
            "schema": RESIDENCY_PLAN_SUPERSEDED_SCHEMA_V1,
            "consumer_action_key": key, "plan_sha256": digest,
            "reason": "plan-stat-unavailable", "unreadable": True,
            "error": "the filed plan's incarnation could not be read",
        }
    if stamped != current:
        # A valid stamp for another filing of the same body.  A deliberate
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
            # ``current`` was read inside the lock, so a valid stamp that
            # equals it is this filing's own marker.  A malformed stamp is
            # never authority -- it is moved to evidence and replaced.
            if _stamped_incarnation(existing.get("plan_incarnation")) == current:
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


#: How one key's live queue state is said in a handoff refusal.
_LIVE_STATE_LABELS = {_pool.CLAIMED: "claimed", _pool.READY: "queued"}


def live_state(queue, action_key: str) -> tuple[str | None, str]:
    """Where one key is queued right now: ``CLAIMED``, ``READY``, or nothing.

    The caller holds the key's transition lock, so a claim cannot be in
    progress and both states are one snapshot.  A stat that fails is *not*
    absence -- this mount's negative cache answers a just-written record as
    missing, and a reader that turned an I/O error into "empty" would infer a
    safety nobody proved -- so an absent pair is confirmed by listing the two
    directories before it is believed.  ``(None, why)`` is that uncertainty,
    and every caller must defer on it: ``handoff_safe`` refuses on it, and the
    dead-consumer sweep skips the key for this cycle.
    """

    for state in (_pool.CLAIMED, _pool.READY):
        try:
            queue.item_path(state, action_key).stat()
        except FileNotFoundError:
            continue
        except OSError as exc:
            return None, f"its {state} record could not be read: {exc}"
        return state, ""
    try:
        for state in (_pool.CLAIMED, _pool.READY):
            names = {path.stem for path in _pool._scan(queue.dir(state))}
            if action_key in names:
                return state, ""
    except OSError as exc:
        return None, f"the queue could not be listed: {exc}"
    return None, ""


def handoff_safe(queue, consumer_action_key: str,
                 plan: Mapping[str, object]) -> tuple[bool, str]:
    """Whether a superseded window's ownership has ended.

    A fresh plan is a different decomposition: its mover keys differ, so
    replacing the old one while any of the old window's work is still live
    would strand a queued or running row nobody can publish, egress or
    attribute.  The test is the queue's own state, never a clock:

    * the consumer itself must not be in ``ready/`` or ``claimed/`` -- a live
      window is not handed off from underneath it, it is withdrawn first;
    * no movement or egress row the plan sealed may be queued or claimed.

    Resident ranges are deliberately not part of the test: their tokens are
    held by their own keys, a successor adopts them by descriptor, and
    nothing about a handoff releases them.

    The consumer's transition lock is held across the whole scan, and each
    child's across its own states -- parent before child, the one order every
    writer here keeps, and same-thread nesting is supported.  Without the
    child's lock a READY->CLAIMED claim lands between the CLAIMED and READY
    reads and looks like a child none of whose states is live, which is the
    exact window in which a reaper archives a plan a worker is still
    fulfilling.  A state that cannot be read is uncertainty and answers
    ``(False, why)``: this function proves safety, and only a complete,
    current scan proves it.
    """

    key = _action_key(consumer_action_key, where="consumer_action_key")
    with queue._transition_locked(key):
        state, why = live_state(queue, key)
        if why:
            return False, f"the consumer: {why}"
        if state is not None:
            return False, f"the consumer is still {_LIVE_STATE_LABELS[state]}"
        for child in child_keys(plan):
            try:
                with queue._transition_locked(child):
                    state, why = live_state(queue, child)
            except OSError as exc:                            # pragma: no cover
                return False, f"its row {child[:12]} could not be locked: {exc}"
            if why:
                return False, f"its row {child[:12]}: {why}"
            if state is not None:
                return False, f"its row {child[:12]} is {_LIVE_STATE_LABELS[state]}"
    return True, "no live consumer and no queued or claimed child"


def window_owned(queue, consumer_action_key: str, *,
                 filing: tuple[int, int, int] | None = None,
                 generation: object | None = None) -> tuple[bool, str]:
    """Whether a captured plan filing and consumer generation still stand.

    The automatic publication sites call this while holding the consumer's
    transition lock, immediately before the child ``publish`` it authorizes.
    The lock is what makes the answer mean anything: the plan cannot be
    reaped, replaced or marked, and the consumer cannot be withdrawn or
    resubmitted, between it and that publication.  ``filing`` is from
    :func:`read_filed` and ``generation`` is the consumer record's
    ``published_unix`` from the same cycle; ``(False, reason)`` defers to the
    next cycle, which reads what is actually there (#708 review).
    """

    key = _action_key(consumer_action_key, where="consumer_action_key")
    current = incarnation(queue.residency_plan_path(key))
    if current is None:
        return False, "its plan filing is gone"
    if filing is not None and tuple(filing) != current:
        return False, "its plan filing was replaced"
    for state in (_pool.CLAIMED, _pool.READY):
        path = queue.item_path(state, key)
        try:
            path.stat()
        except FileNotFoundError:
            continue
        except OSError as exc:
            return False, f"the consumer's {state} record could not be read: {exc}"
        if generation is not None:
            try:
                item = _pool._read_json(path)
            except (OSError, ValueError) as exc:
                return False, f"the consumer's record could not be read: {exc}"
            if not isinstance(item, Mapping):
                return False, "the consumer's record could not be read"
            if item.get("published_unix") != generation:
                return False, "the consumer was resubmitted"
        return True, ""
    return False, "the consumer is no longer live"


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


def retire_predecessor_cancellations(
        queue, consumer_action_key: str,
        plan: Mapping[str, object]) -> dict[str, object]:
    """Retire a reaped window's visible child cancellations for a fresh seal.

    A submission publishes its consumer and nothing else: every phase is the
    window's to publish, the first included, as the consumer advances.  An
    action key is a content hash, so a resubmission of the same consumer,
    price and tool seals the same child keys -- and the *visible* withdrawal
    markers the predecessor generation left on those keys (an operator's
    cancellation, or the dead-consumer pass stopping its movers) outlive the
    plan's own retirement.  Read as live, they supersede the fresh plan before
    its second phase ever published (#708 review).

    A deliberate seal is a new generation of that consumer's window, so it
    retires those predecessor markers: the keys are its own sealed children,
    and the same act that re-submits the consumer is how a person asks for the
    work again.  Three boundaries keep it honest:

    * :func:`handoff_safe` must prove the predecessor's ownership ended -- no
      live consumer and no queued or claimed child -- under the consumer's
      transition lock.  This is the same proof ``reap`` uses; a window whose
      work is still live refuses by name rather than being replaced.
    * only the *visible* marker is moved, under each child's own transition
      lock and in the parent-before-child order every writer here keeps; the
      immutable decision under ``withdrawn/decisions/`` stays, and the marker
      itself is filed under ``withdrawn/superseded/`` as evidence, never
      deleted.
    * a marker that cannot be read is unknown state, not "no cancellation":
      the whole renewal refuses while any hit is unreadable, and nothing is
      retired before that refusal -- an operator resolves it.

    The boundary for a later child is *that child's own locked retirement* in
    this transaction -- not this call, and not the freeze that follows it.  The
    marker is moved while the child's transition lock is held, so a
    cancellation filed for that child after that instant survives, and the
    window's next cycle reads it as live: it refuses to publish the child and
    marks the fresh plan superseded.  That is the rule for the first lead
    too, with no special case: ``child_keys`` names every phase, so the
    lead's marker is retired here under its own transition lock, on the same
    boundary as any later child.  The automatic
    publisher never retires a cancellation.  ``None`` from
    ``withdrawn_keys``-backed reads is the ordinary first seal, which is not a
    renewal at all and returns an empty answer without taking a lock.
    """

    key = _action_key(consumer_action_key, where="consumer_action_key")
    try:
        cancelled = queue.withdrawn_keys()
    except OSError as exc:
        raise ResidencyPlanError(
            f"the live withdrawal markers could not be listed: {exc}") from None
    hits = sorted(child for child in child_keys(plan) if child in cancelled)
    if not hits:
        return {"consumer_action_key": key, "retired": []}
    with queue._transition_locked(key):
        safe, why = handoff_safe(queue, key, plan)
        if not safe:
            raise ResidencyPlanError(
                f"the window filed for {key[:12]} still owns live work "
                f"({why}); a fresh seal cannot retire its cancellations. Stop "
                f"the old work and resubmit")
        # Readability first, retirement second: an unreadable marker refuses
        # the whole renewal before any decision is moved.
        readable: list[str] = []
        for child in hits:
            with queue._transition_locked(child):
                marker = queue.live_withdrawal(child)
                if marker is None:
                    continue        # already retired by another writer
                if marker.get("unreadable"):
                    raise ResidencyPlanError(
                        f"the cancellation marker for {child[:12]} cannot be "
                        f"read ({marker.get('reason') or 'unknown state'}); an "
                        f"operator must resolve it under withdrawn/superseded/ "
                        f"before this window can be sealed")
                readable.append(child)
        retired: list[str] = []
        for child in readable:
            with queue._transition_locked(child):
                try:
                    if queue._supersede_withdrawal(child) is not None:
                        retired.append(child)
                except (_pool.PoolContractError, OSError) as exc:
                    raise ResidencyPlanError(
                        f"the cancellation marker for {child[:12]} could not "
                        f"be retired: {exc}") from None
        return {"consumer_action_key": key, "retired": retired}


def lead_mover_row(plan: Mapping[str, object]) -> dict[str, object]:
    """The row the consumer's admission waits on: the first chunk's, or the mover's.

    The consumer depends only on its first phase, and a first phase sealed
    chunked (#675) starts with its first chunk.  Naming the row is all this
    does: the tiers loop publishes it, and the rest as accepted progress
    advances.
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


class ResidencyEvidenceUnreadable(OSError):
    """The filed publication evidence for one consumer could not be read.

    Distinct from "nothing is published": a fragment directory that is simply
    absent is a consumer nobody has staged for yet, which is a known-empty
    answer.  A directory that exists and cannot be listed is an *unknown*
    answer, and a caller that flattened it to empty would report a stalled
    mount as a clean unstaged window.  Raised so every caller has to decide;
    the tier loop fails that consumer's cycle closed and says so, the status
    census reports the readiness as unknown rather than false.
    """


def _sealed_spans(plan: Mapping[str, object], *,
                  ram: bool) -> dict[str, tuple[int, int]]:
    """The byte span this plan seals per mover key on one leg.

    Plan arithmetic only -- no payload is opened and nothing is stat-ed.  A
    whole-phase leg seals the phase's range; a chunked leg seals each chunk's
    own range under that chunk's mover key, and the span widens to the union
    if one key is ever sealed over several chunks.  The span is what a
    receipt has to *cover* before the key counts as resident, so a copy that
    finished a narrower range than the plan sealed is not this range.
    """

    row_key = "ram_mover_row" if ram else "mover_row"
    chunks_key = "ram_chunks" if ram else "stage_chunks"
    spans: dict[str, tuple[int, int]] = {}

    def _note(key: str, start: object, end: object) -> None:
        if isinstance(start, bool) or not isinstance(start, int):
            return
        if isinstance(end, bool) or not isinstance(end, int):
            return
        if not key or end < start:
            return
        prior = spans.get(key)
        spans[key] = ((start, end) if prior is None
                      else (min(prior[0], start), max(prior[1], end)))

    for phase in plan.get("phases", []):                 # type: ignore[union-attr]
        if not isinstance(phase, Mapping):
            continue
        chunks = phase.get(chunks_key)
        if isinstance(chunks, list):
            for chunk in chunks:
                if not isinstance(chunk, Mapping):
                    continue
                row = chunk.get(row_key)
                if isinstance(row, Mapping):
                    _note(str(row.get("action_key") or ""),
                          chunk.get("start_bytes"), chunk.get("end_bytes"))
            continue
        row = phase.get(row_key)
        if isinstance(row, Mapping):
            _note(str(row.get("action_key") or ""),
                  phase.get("start_bytes"), phase.get("end_bytes"))
    return spans


def _plan_fragments(queue: _pool.PoolQueue, consumer_action_key: str,
                    keys: Collection[str]) -> list[dict[str, object]]:
    """Every filed fragment for one consumer, read strictly for ``keys``.

    ``residency_map.read_fragments`` deliberately *skips* a file it cannot
    read or validate, and that is right where it lives: a consumer composing
    its map must still find the copies its other movers really did make, and
    one bad file must not cost it the rest.

    It is wrong for readiness.  Skipping turns "I could not tell" into "not
    staged", and ``staged: false`` asserts a fact a caller acts on --
    exactly the unproven-reported-as-known error #759 was.  So a file whose
    name is one of *this plan's* mover keys is read through the same
    validator and, if it cannot be read or does not validate, raises
    :class:`ResidencyEvidenceUnreadable` rather than vanishing.

    A file no leg of this plan names is still skipped: it cannot change what
    this plan's movers published, so refusing on it would be a stall with no
    reason.  Names are authoritative because ``residency_map.fragment_path``
    writes exactly ``<consumer>/<mover>.json`` and nothing else does.  No
    payload byte is opened here.
    """

    directory = (Path(queue.residency_fragment_root())
                 / str(consumer_action_key))
    try:
        names = sorted(entry.name for entry in os.scandir(directory)
                       if entry.is_file() and entry.name.endswith(".json"))
    except FileNotFoundError:
        return []           # never published: known empty, not unknown
    except OSError as exc:
        raise ResidencyEvidenceUnreadable(
            f"residency fragments for {str(consumer_action_key)[:12]} "
            f"unreadable: {exc!r}") from exc
    wanted = set(keys)
    out: list[dict[str, object]] = []
    for name in names:
        try:
            with open(directory / name) as stream:
                out.append(residency_map.validate_fragment(json.load(stream)))
        except (OSError, ValueError) as exc:
            if name[:-len(".json")] in wanted:
                raise ResidencyEvidenceUnreadable(
                    f"residency fragment {name[:12]} for "
                    f"{str(consumer_action_key)[:12]} unreadable: {exc!r}"
                ) from exc
            continue        # a file no leg of this plan names
    return out


def resident_movers(queue: _pool.PoolQueue, plan: Mapping[str, object],
                    tier_id: str, *,
                    tier_record: Mapping[str, object] | None = None,
                    ) -> set[str]:
    """Which of a plan's movers this tier both accounts for and has published.

    The one shared readiness predicate.  The tier window gates RAM
    publication on it and the status census reports it, so a gate and a
    cursor can never again disagree about what is resident -- which is
    exactly how issue #759 stayed invisible: two predicates, one premature
    trigger.

    A tier token is a *reservation*.  It is taken at claim, before a byte
    moves, and it is what stops the tier being over-committed; it says the
    room is booked, never that the bytes arrived.  Residency needs both, and
    this asks for both:

    1. **The tier still accounts for the range.**  ``holder_tokens`` is
       non-empty for the key.  This is the guard the old docstring was
       written for and it is kept exactly: an egress that released the
       tokens but failed before unlinking the fragment leaves a vouch for
       bytes that are going, and a reboot leaves a ram ledger with no
       tokens at all.
    2. **A current fragment vouches for the bytes, under this plan's
       identity.**  The consumer's own fragment for this mover, naming this
       ``tier_id`` and this ``manifest_sha256``, with a non-empty entry set.
       Relevance is checked per leg: a stage fragment must name the plan's
       stage root, and a ram fragment must name the *announced* ram root
       under the *announced* epoch -- never the plan's stage root, which is
       where the promotion read from, not where it landed.
    3. **The key is not CLAIMED.**  ``stage_move`` republishes its fragment
       as entries land, on purpose, so a running copy's fragment is a
       prefix and whatever receipt is on disk belongs to a previous run.
    4. **A complete receipt covers the sealed span.**  ``complete is True``,
       the same manifest, and a filed range that contains the span this plan
       sealed for the key.
    5. **That receipt describes *this* fragment.**  ``entries_staged ==
       entries_declared`` and the current fragment holds exactly that many
       entries.  Entries are keyed and a mover stages exactly its window, so
       a short count is a hole and never a straddle.  This is what stops a
       historical complete receipt from resurrecting a new partial copy:
       after a crash or a requeue the predecessor's receipt is still on
       disk, the pin is still holding its tokens because of it, and only the
       count tie notices that the fragment beside it is a prefix.

    Adoption passes unchanged: ``tier_loop.adopt`` re-issues the donor's
    fragment under the successor's name and files a receipt whose declared
    and staged counts are that fragment's own, with the tokens transferred
    rather than released.

    Bounded by design: one listing of this consumer's fragment directory,
    each of its (small) fragment documents, and one small receipt read per
    fragment-backed key this plan names.  No payload byte is read, nothing
    is hashed, and no model is stat-ed.

    Absent evidence and unreadable evidence are different answers and stay
    different.  A fragment directory that is simply not there is
    known-empty.  A directory that cannot be listed, or a fragment named for
    one of this plan's movers that cannot be read or does not validate,
    raises :class:`ResidencyEvidenceUnreadable` -- because reporting that as
    "not staged" would assert a fact nothing supports, which is #759's own
    error in another costume.  See :func:`_plan_fragments`.

    ``tier_record`` is the announced record for ``tier_id`` when the caller
    already has the cycle's census in hand; left out, it is read back from
    the queue.  It is consulted for the ram leg only.
    """

    consumer = str(plan["consumer_action_key"])
    keys = set(ram_mover_keys(plan) if _is_ram_tier(tier_id)
               else mover_keys(plan))
    if not keys:
        return set()
    fragments = _plan_fragments(queue, consumer, keys)
    ram = _is_ram_tier(tier_id)
    manifest = str(plan.get("manifest_sha256") or "")
    if ram:
        if tier_record is None:
            tier_record = next(
                (record for record in queue.tiers()
                 if str(record.get("tier_id") or "") == str(tier_id)), None)
        epoch = str((tier_record or {}).get("epoch") or "")
        root_wanted = str((tier_record or {}).get("mountpoint") or "")
        if not epoch or not root_wanted:
            # An undated or unannounced ram tier is not resident, ever: the
            # tmpfs empties on reboot and only the announced epoch says which
            # incarnation the fragments on the shared mount belong to.
            return set()
    else:
        epoch = ""
        root_wanted = str(plan.get("stage_root") or "")
    spans = _sealed_spans(plan, ram=ram)
    ledger = queue.tier_ledger(tier_id)
    out: set[str] = set()
    for fragment in fragments:
        mover = fragment.get("mover_action_key")
        if not isinstance(mover, str) or mover not in keys or mover in out:
            continue
        if str(fragment.get("tier_id") or "") != str(tier_id):
            continue
        if manifest and str(fragment.get("manifest_sha256") or "") != manifest:
            continue
        if root_wanted and os.path.normpath(
                str(fragment.get("stage_root") or "")) != os.path.normpath(
                    root_wanted):
            continue
        if ram and str(fragment.get("epoch") or "") != epoch:
            continue
        entries = fragment.get("entries")
        if not isinstance(entries, Mapping) or not entries:
            continue
        span = spans.get(mover)
        if span is None:
            continue            # no leg of this plan seals that key
        if not ledger.holder_tokens(mover):
            continue            # the tier no longer accounts for the range
        if queue.item_path(_pool.CLAIMED, mover).exists():
            continue            # a running copy; its receipt is a prior run's
        receipt = queue.move_record(mover)
        if not isinstance(receipt, Mapping) or receipt.get("complete") is not True:
            continue
        if manifest and str(receipt.get("manifest_sha256") or "") != manifest:
            continue
        try:
            filed = (int(receipt["range_start_bytes"]),
                     int(receipt["range_end_bytes"]))
        except (KeyError, TypeError, ValueError):
            continue
        if filed[0] > span[0] or filed[1] < span[1]:
            continue
        declared, landed = receipt.get("entries_declared"), receipt.get(
            "entries_staged")
        if (isinstance(declared, bool) or not isinstance(declared, int)
                or isinstance(landed, bool) or not isinstance(landed, int)):
            continue            # a receipt that cannot be tied to a fragment
        if landed != declared or len(entries) != landed:
            continue
        out.add(mover)
    return out


def _is_ram_tier(tier_id: object) -> bool:
    """Whether a tier id names a ram tier, by the one prefix that defines it."""

    return str(tier_id).startswith(storage_tiers.RAM_TIER_PREFIX)


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


def refill_horizon(plan: Mapping[str, object], accepted_phase: str | None, *,
                   claimed_unix: object, reported_unix: object,
                   readahead_bytes: int | None,
                   landing_bytes_per_s: float | None,
                   report_latency_s: float,
                   declared_bytes_per_s: float | None = None,
                   mover_role: str = "mover_row",
                   consumption_bytes_per_s: float | None = None,
                   declared_wait_end_bytes: int | None = None,
                   ) -> dict[str, object] | None:
    """How far ahead of a reading consumer its window must be staged (#903).

    The horizon is three spans of the plan's read order, which is its byte
    order (``validate_plan`` requires each phase to start where the last one
    ended):

    * the phase the consumer's accepted progress names, which it is reading;
    * ``readahead_bytes`` past that phase's end, the bytes the consumer can
      hold ahead of what it is reading -- its memory reservations, which are
      what bounds a prefetch that has to keep what it reads;
    * the refill: legs past that reach until they cover what the consumer
      reads while a copy published now lands, and never fewer than one leg,
      so the next range is staged before the current reach is exhausted.

    The refill's time is ``report_latency_s`` (how stale an accepted phase
    can be when a cycle reads it, plus the wait for the next cycle) plus the
    landing time: the largest leg still ahead at ``landing_bytes_per_s``, the
    slowest rate a copy of this plan has landed at.  Priced by throughput
    rather than by a receipt's duration, so a plan whose landed copies were
    small legs does not under-price its large ones.  Its rate is the
    consumer's measured consumption: the bytes up to the end of the accepted
    phase over the time from its claim to that phase's report.  Counting the
    whole accepted phase as read over-estimates the rate while the consumer
    is inside it, which errs toward a longer horizon.

    ``declared_bytes_per_s`` is the rate the consumer's plan declares
    (:func:`declared_read_bytes_per_s`, #909).  Before a rate can be
    measured -- no report time after the claim -- it is the rate; beside a
    measurement the larger of the two is.  Both are lower bounds on how fast
    the consumer reads, and a faster rate only lengthens the horizon.  With
    neither, the horizon is undefined.  The tier's announced fill supply
    stood in here before #909 and no longer can: it moves as the tier loop
    probes the pool, so the same consumer's horizon, and the admission
    verdict priced from it, moved with it.

    ``consumption_bytes_per_s``, when given, is the rate, and neither of the
    two above is computed: :func:`read_footprint` asks for the horizon at
    phases the consumer has not reached, where the bytes through that phase
    over the time to *its* report would be a rate nobody measured (#907).

    ``declared_wait_end_bytes`` is where the furthest leg the consumer has
    declared itself blocked on ends (#1018): its reader's staged-wait record
    (#989) names that leg's mover.  The read-ahead above is the consumer's
    own statement of how far it reads, and a reader blocked past it has
    measured a longer one; a leg past the horizon is published only once the
    consumer's progress brings it inside, and that progress waits on the
    leg.  So a horizon that ends before the declared leg ends is taken
    through that leg, and ``declared_wait_end_bytes`` says why.  A declared
    leg already inside the horizon changes nothing.

    Returns ``None`` when the horizon is undefined: no accepted progress (the
    window's own no-progress regime already publishes one step), no
    read-ahead or landing rate, or no consumption rate.  ``None`` keeps the window's
    decisions exactly what they were before the horizon existed.  Otherwise
    ``horizon_end_bytes`` is where the first leg outside the horizon starts
    (``None`` when every remaining leg is inside it), ``advance`` names that
    leg -- the next one the window will publish -- and ``beyond`` names every
    leg after it, in read order.
    """

    if mover_role not in _MOVEMENT_ROLES:
        raise ResidencyPlanError(
            f"mover_role must be one of {sorted(_MOVEMENT_ROLES)}, "
            f"not {mover_role!r}")
    if not accepted(plan, accepted_phase):
        return None
    if (readahead_bytes is None or isinstance(readahead_bytes, bool)
            or readahead_bytes < 0):
        return None
    if not (_finite_number(landing_bytes_per_s)
            and float(landing_bytes_per_s) > 0):             # type: ignore[arg-type]
        return None
    ahead = remaining(plan, accepted_phase)
    ahead_names = [str(phase["name"]) for phase in ahead]
    reading = ahead[0]
    first_start = int(plan["phases"][0]["start_bytes"])      # type: ignore[index]
    read_through = int(reading["end_bytes"])
    rate: float | None = None
    basis = ""
    if consumption_bytes_per_s is not None:
        if (_finite_number(consumption_bytes_per_s)
                and float(consumption_bytes_per_s) > 0):     # type: ignore[arg-type]
            rate = float(consumption_bytes_per_s)            # type: ignore[arg-type]
            basis = "given"
    else:
        if (_finite_number(claimed_unix) and _finite_number(reported_unix)
                and float(reported_unix) > float(claimed_unix)):   # type: ignore[arg-type]
            rate = (read_through - first_start) / (
                float(reported_unix) - float(claimed_unix))        # type: ignore[arg-type]
            basis = "measured"
        if (_finite_number(declared_bytes_per_s)
                and float(declared_bytes_per_s) > 0               # type: ignore[arg-type]
                and (rate is None or float(declared_bytes_per_s) > rate)):  # type: ignore[arg-type]
            rate = float(declared_bytes_per_s)                    # type: ignore[arg-type]
            basis = "declared"
    if rate is None or rate <= 0:
        return None
    future = [leg for leg in _legs(plan, mover_role=mover_role)
              if leg["phase"] in ahead_names[1:]]
    largest = max((int(leg["end_bytes"]) - int(leg["start_bytes"])
                   for leg in future), default=0)
    landing_s = landing_seconds(largest, landing_bytes_per_s)   # type: ignore[arg-type]
    latency = float(report_latency_s) + landing_s
    reach_end = read_through + int(readahead_bytes)
    refill_bytes = rate * latency
    horizon_end: int | None = None
    refilled = 0
    for leg in future:
        start = int(leg["start_bytes"])
        if start < reach_end:
            continue
        if refilled and refilled >= refill_bytes:
            horizon_end = start
            break
        refilled += int(leg["end_bytes"]) - start
    extended = False
    if (declared_wait_end_bytes is not None and horizon_end is not None
            and int(declared_wait_end_bytes) > horizon_end):
        # The reader is blocked on a leg past the horizon: take the horizon
        # through the end of that leg, to the first leg that starts after it.
        horizon_end = next((int(leg["start_bytes"]) for leg in future
                            if int(leg["start_bytes"])
                            >= int(declared_wait_end_bytes)), None)
        extended = True
    outside = ([] if horizon_end is None else
               [leg for leg in future if int(leg["start_bytes"]) >= horizon_end])
    return {
        "consumer_action_key": plan["consumer_action_key"],
        "accepted_phase": accepted_phase,
        "consumption_bytes_per_s": rate,
        "consumption_basis": basis,
        "read_through_bytes": read_through,
        "readahead_bytes": int(readahead_bytes),
        "reach_end_bytes": reach_end,
        "landing_s": landing_s,
        # The rate ``landing_s`` was priced at, so a caller pricing another
        # range on this tier uses the same number (#1011).
        "landing_bytes_per_s": float(landing_bytes_per_s),
        "latency_s": latency,
        "refill_bytes": refill_bytes,
        "horizon_end_bytes": horizon_end,
        # The end of the furthest leg the consumer declared itself blocked
        # on (#1018), and whether it took the horizon past the refill.
        "declared_wait_end_bytes": (None if declared_wait_end_bytes is None
                                    else int(declared_wait_end_bytes)),
        "extended_by_declared_wait": extended,
        "advance": (str(outside[0]["mover_row"]["action_key"])  # type: ignore[index]
                    if outside else None),
        "beyond": [{
            "phase": str(leg["phase"]),
            "chunk_index": leg["chunk_index"],
            "mover_action_key": str(leg["mover_row"]["action_key"]),  # type: ignore[index]
            "egress_row": leg["egress_row"],
            "start_bytes": int(leg["start_bytes"]),
            "end_bytes": int(leg["end_bytes"]),
            "stage_gib": int(leg["stage_gib"]),
            # Belady's order: the leg the consumer reaches last is the one to
            # give back first.  Seconds from the end of the accepted phase at
            # the measured rate, so two consumers' legs compare in one unit.
            "seconds_until_needed": (int(leg["start_bytes"]) - read_through) / rate,
        } for leg in outside[1:]],
    }


def landing_seconds(range_bytes: float, landing_bytes_per_s: float) -> float:
    """How long a copy of ``range_bytes`` takes to land, at the landing rate.

    The one landing model: :func:`refill_horizon` prices its refill with it,
    and :func:`expected_landings` prices every queued range with it (#989).
    ``landing_bytes_per_s`` is the slowest complete copy of the plan
    (``tier_loop._plan_landing``), so the answer errs long.
    """

    return float(range_bytes) / float(landing_bytes_per_s)


def expected_landings(tier_queue: Sequence[Mapping[str, object]], *,
                      now: float, landing_bytes_per_s: float,
                      ) -> dict[str, dict[str, object]]:
    """When each stage mover queued on one tier is expected to land (#989).

    ``tier_queue`` is every ``ready`` or ``claimed`` stage mover of the
    tier's live plans, claimed ones first (oldest claim first) and then the
    ready ones in the queue's own claim order, each with
    ``mover_action_key``, ``state``, ``range_bytes`` and, when claimed,
    ``claimed_unix``.

    :func:`refill_horizon`'s landing term, extended over the queue: a
    claimed copy lands :func:`landing_seconds` of its own bytes after its
    claim.  A ready one lands after every copy ahead of it -- the claimed
    copies' bytes still to land at that rate, then each ready range before
    it -- and its own.  Serial, at the plan's slowest measured rate, so it
    errs long, as the horizon does.  It is an expectation for records and
    readers, never a deadline.

    A claimed copy that has reported its landed bytes (#1010) carries
    ``landed_bytes``, ``reported_unix`` and ``landed_phase``, the mover's own
    last progress report.  It is priced from them instead: the rest of its
    range at the rate it has landed at since its claim
    (``live_bytes_per_s``), from the time of the report.  A report in the
    ``warm`` phase, or one that covers the range, has landed.  A claimed
    copy with no report, or none that prices a rate, keeps the claim-time
    expectation.  ``basis`` says which: ``reported`` or ``claim``; a queued
    range's is ``queue``.  On a resumed attempt, ``reported`` overstates the
    live rate: the entries it adopted from an earlier attempt count as
    landed at once.  That is harmless because the price is informational
    and gates nothing.

    Returns, per mover, ``queue_position`` (its place in that order),
    ``bytes_ahead``, ``expected_landing_unix`` and ``basis``, and for a
    ``reported`` copy ``landed_bytes``, ``reported_unix`` and
    ``live_bytes_per_s``.
    """

    rate = float(landing_bytes_per_s)
    out: dict[str, dict[str, object]] = {}
    ahead = 0.0
    for position, mover in enumerate(tier_queue):
        own = int(mover["range_bytes"])                       # type: ignore[call-overload]
        key = str(mover["mover_action_key"])
        if mover.get("state") == "claimed" and _finite_number(mover.get("claimed_unix")):
            claimed = float(mover["claimed_unix"])            # type: ignore[arg-type]
            reported = _reported_landing(mover, own=own, claimed=claimed)
            if reported is None:
                expected = claimed + landing_seconds(own, rate)
                out[key] = {"queue_position": position, "bytes_ahead": 0,
                            "expected_landing_unix": expected, "basis": "claim"}
            else:
                expected = float(reported["expected_landing_unix"])  # type: ignore[arg-type]
                out[key] = {"queue_position": position, "bytes_ahead": 0,
                            **reported, "basis": "reported"}
            ahead += max(0.0, expected - float(now)) * rate
            continue
        out[key] = {"queue_position": position, "bytes_ahead": int(round(ahead)),
                    "expected_landing_unix": float(now) + landing_seconds(
                        ahead + own, rate), "basis": "queue"}
        ahead += own
    return out


def _reported_landing(mover: Mapping[str, object], *, own: int,
                      claimed: float) -> dict[str, object] | None:
    """A claimed copy's expectation from its own progress report, or ``None``.

    ``None`` when the entry carries no report, or one that prices nothing:
    no bytes landed yet, or a report no later than the claim.
    """

    landed = mover.get("landed_bytes")
    at = mover.get("reported_unix")
    if (isinstance(landed, bool) or not isinstance(landed, int) or landed < 0
            or not _finite_number(at)):
        return None
    at = float(at)                                            # type: ignore[arg-type]
    if mover.get("landed_phase") == "warm" or landed >= own:
        return {"expected_landing_unix": at, "landed_bytes": min(landed, own),
                "reported_unix": at, "live_bytes_per_s": None}
    if landed == 0 or at <= claimed:
        return None
    live = landed / (at - claimed)
    return {"expected_landing_unix": at + landing_seconds(own - landed, live),
            "landed_bytes": landed, "reported_unix": at,
            "live_bytes_per_s": live}


def _finite_number(value: object) -> bool:
    return (not isinstance(value, bool) and isinstance(value, (int, float))
            and math.isfinite(float(value)))


def read_footprint(plan: Mapping[str, object], accepted_phase: str | None, *,
                   capacity_gib: int,
                   readahead_bytes: int | None,
                   landing_bytes_per_s: float | None,
                   consumption_bytes_per_s: float | None,
                   report_latency_s: float,
                   mover_role: str = "mover_row") -> int:
    """The most tier GiB this consumer's window will publish at once (#907).

    The window's own answer, asked at every phase the consumer has still to
    read: at each, the stage GiB :func:`window` publishes from an empty tier
    of ``capacity_gib`` with the refill horizon recomputed at that phase, and
    the largest of them.  So it is exactly the window's rule -- the phase
    being read, the in-horizon legs, and the #633 run-ahead bound, which
    keeps it at or under ``capacity_gib`` -- summed over what the window can
    hold at once, not over the plan.

    The horizon at each phase is priced at one ``consumption_bytes_per_s``
    and one ``landing_bytes_per_s``: rates measured now stand for the rest
    of the plan.  Where the horizon is undefined -- no read-ahead, landing or
    consumption rate -- the window has only the run-ahead bound, and the
    footprint is what that bound lets it publish.

    A consumer that has not accepted a phase is asked from its first phase
    as though it had: the footprint is what its window grows to once it
    reads, not the one step it publishes before (#632).
    """

    if mover_role not in _MOVEMENT_ROLES:
        raise ResidencyPlanError(
            f"mover_role must be one of {sorted(_MOVEMENT_ROLES)}, "
            f"not {mover_role!r}")
    capacity = int(capacity_gib)
    largest = 0
    for phase in remaining(plan, accepted_phase):
        name = str(phase["name"])
        horizon = refill_horizon(
            plan, name, claimed_unix=None, reported_unix=None,
            readahead_bytes=readahead_bytes,
            landing_bytes_per_s=landing_bytes_per_s,
            report_latency_s=report_latency_s, mover_role=mover_role,
            consumption_bytes_per_s=consumption_bytes_per_s)
        end = None if horizon is None else horizon["horizon_end_bytes"]
        decision = window(plan, accepted_phase=name, free_gib=capacity,
                          capacity_gib=capacity, mover_role=mover_role,
                          horizon_end_bytes=end)             # type: ignore[arg-type]
        publish = decision["publish"]
        assert isinstance(publish, list)
        largest = max(largest, sum(int(row["stage_gib"]) for row in publish))
    return largest


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


def legs_over(plan: Mapping[str, object], start_bytes: int, end_bytes: int,
              *, mover_role: str) -> list[dict[str, object]]:
    """The legs of one tier that overlap ``[start_bytes, end_bytes)``, in read order.

    Asked across tiers: which promotions sit over a stage range's bytes
    (#906), when the two legs may be chunked differently.
    """

    if mover_role not in _MOVEMENT_ROLES:
        raise ResidencyPlanError(
            f"mover_role must be one of {sorted(_MOVEMENT_ROLES)}, "
            f"not {mover_role!r}")
    return [leg for leg in _legs(plan, mover_role=mover_role)
            if int(leg["start_bytes"]) < int(end_bytes)
            and int(leg["end_bytes"]) > int(start_bytes)]


def window(plan: Mapping[str, object], *, accepted_phase: str | None,
           free_gib: int, capacity_gib: int | None = None,
           published: Sequence[str] = (),
           staged: Sequence[str] = (),
           runahead_cap_gib: int | None = None,
           mover_role: str = "mover_row",
           withdrawn: Sequence[str] = (),
           horizon_end_bytes: int | None = None) -> dict[str, object]:
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

    ``horizon_end_bytes`` is the third bound, and the one that answers "how
    far ahead does this consumer need its bytes" rather than "how much room
    is there" (#903): :func:`refill_horizon`'s ``horizon_end_bytes``.  A leg
    of a later phase that starts at or past it is not published this cycle,
    however much room the tier has; it is published on the cycle the
    consumer's progress brings it inside.  That is the normal state of a
    rolling window, not a stall, so it files no ``stall``.  ``None`` -- no
    horizon, or every remaining leg inside it -- leaves the decision exactly
    as it was.  The phase being read is never held back by it.
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
        if (not is_current and horizon_end_bytes is not None
                and int(leg["start_bytes"]) >= int(horizon_end_bytes)):
            break     # past the refill horizon: published as progress arrives
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


def _advance_entry(leg: Mapping[str, object]) -> dict[str, object]:
    """One leg in the shape the fence binder reads (plan-quoted, not row-quoted)."""

    return {
        "phase": str(leg["phase"]),
        "mover_action_key": str(leg["mover_row"]["action_key"]),  # type: ignore[index]
        "egress_action_key": str(leg["egress_row"]["action_key"]),  # type: ignore[index]
        "stage_gib": int(leg["stage_gib"]),
        "chunk_index": leg["chunk_index"],
        "start_bytes": int(leg["start_bytes"]),
        "end_bytes": int(leg["end_bytes"]),
    }


def _advance_prior(legs: Sequence[Mapping[str, object]]) -> list[dict[str, object]]:
    """Every earlier leg listed for the safe-retire check replenish requires."""

    return [{
        "mover_action_key": str(leg["mover_row"]["action_key"]),  # type: ignore[index]
        "egress_action_key": str(leg["egress_row"]["action_key"]),  # type: ignore[index]
        "stage_gib": int(leg["stage_gib"]),
    } for leg in legs]


def _entry_list(legs: Sequence[Mapping[str, object]]) -> list[dict[str, object]]:
    """Entry shape for a leg list, shared by waiting and queued answers."""

    return [_advance_entry(leg) for leg in legs]


def advance_needs(plan: Mapping[str, object], accepted_phase: str | None, *,
                  published: Sequence[str] = (),
                  rowed: Sequence[str] = (),
                  staged: Sequence[str] = (),
                  mover_role: str = "mover_row",
                  horizon_end_bytes: int | None = None) -> dict[str, object]:
    """The minimum simultaneous current-plus-next needs of one window leg.

    The first two unpublished legs in read order: ``current_min_gib`` is what
    admission must stage first (the lead), ``next_min_gib`` the advance that
    must fit beside it for guaranteed progress (``None`` on the final leg --
    a final phase needs no future credit).  Chunked legs decide per chunk, so
    a sliding window's minimum is two chunks, never two phases.  A leg the
    submitter sealed without (``mover_role`` absent) contributes nothing.
    Raises :class:`ResidencyPlanError` for an unknown mover role, like
    :func:`window`.

    ``rowed`` names the movers with a queued ready row; ``staged`` the ones
    holding tokens for landed bytes; ``queued`` returns those ahead legs in
    read order, with ``queued_prior`` the full-ordered legs before the first
    of them.  ``fence_target`` names the one advance the fence protects --
    exactly the leg after the frontier (the earliest unstaged ahead leg),
    which pays from free under the gate's count -- with ``fence_prior``
    the legs before it: one fence per window, and the phase the blind
    pre-publish take and the post-publish bind agree on.  The gate reads
    ``waiting``/``prior`` exactly as before.

    ``horizon_end_bytes`` is :func:`window`'s refill horizon (#903), and it
    bounds ``waiting`` the way it bounds the window's publication: a leg of
    a later phase that starts at or past it is not something this window
    will publish this cycle, so it is neither the current nor the next the
    gate reserves room for.  Without the bound a window asks the joint gate
    for legs it will not publish -- a newcomer its current and next, an
    admitted window the next the gate reserves for it -- and that phantom
    footprint can hold another consumer's in-horizon leg out.  (Before #908
    a rolling window whose original lead retired was also re-gated as a
    newcomer; a claimed window no longer is.)  The fence is bounded the
    same way: its frontier is still the earliest unstaged leg of the whole
    plan, but a ``fence_target`` past the horizon is no advance yet -- the
    window will not publish it until progress brings it inside -- so it is
    ``None`` this cycle, and a fence held for it would be room reserved for
    a leg nobody asked for.  ``queued`` stays on the whole plan.  ``None``
    changes nothing.
    """

    if mover_role not in _MOVEMENT_ROLES:
        raise ResidencyPlanError(
            f"mover_role must be one of {sorted(_MOVEMENT_ROLES)}, "
            f"not {mover_role!r}")
    ahead = remaining(plan, accepted_phase)
    ahead_names = [str(phase["name"]) for phase in ahead]
    done = set(published)
    rowed_set = set(rowed)
    staged_set = set(staged)
    full = _legs(plan, mover_role=mover_role)
    current_name = ahead_names[0] if ahead_names else None
    waiting = [leg for leg in full
               if leg["phase"] in ahead_names
               and str(leg["mover_row"]["action_key"]) not in done  # type: ignore[index]
               and (horizon_end_bytes is None or leg["phase"] == current_name
                    or int(leg["start_bytes"]) < int(horizon_end_bytes))]
    queued = [leg for leg in full
              if leg["phase"] in ahead_names
              and str(leg["mover_row"]["action_key"]) in rowed_set]  # type: ignore[index]
    # Frontier-first fencing: one fence per window per pass.  The frontier
    # (the earliest unstaged ahead leg) pays from free under the gate's
    # count; exactly the leg after it is the advance the fence protects.
    # Blind (unpublished advance) and bind (published advance) target the
    # same phase, so the pre-publish take and the post-publish bind agree;
    # legs before the target excluding the frontier are staged by
    # construction, so the retired check over ``fence_prior`` is a sanity
    # rail rather than a discovery.
    frontier_index: int | None = None
    for index, leg in enumerate(full):
        if (leg["phase"] in ahead_names
                and str(leg["mover_row"]["action_key"])  # type: ignore[index]
                not in staged_set):
            frontier_index = index
            break
    fence_target: dict[str, object] | None = None
    fence_prior: list[dict[str, object]] = []
    if frontier_index is not None and frontier_index + 1 < len(full):
        candidate = full[frontier_index + 1]
        if candidate["phase"] in ahead_names and (
                horizon_end_bytes is None or candidate["phase"] == current_name
                or int(candidate["start_bytes"]) < int(horizon_end_bytes)):
            fence_target = _advance_entry(candidate)
            fence_prior = _advance_prior(full[:frontier_index])
    if not waiting:
        return {"current_min_gib": 0, "next_min_gib": None, "final": True,
                "reading_phase": ahead_names[0] if ahead_names else None,
                "next_phase": None, "next_mover_action_key": None,
                "next_chunk_index": None, "waiting": [], "prior": [],
                "queued": _entry_list(queued), "queued_prior": [],
                "fence_target": fence_target, "fence_prior": fence_prior,
                "lead_mover_action_key": None}
    first, rest = waiting[0], waiting[1:]
    order = [str(leg["mover_row"]["action_key"]) for leg in full]  # type: ignore[index]
    prior = full[:order.index(str(first["mover_row"]["action_key"]))]  # type: ignore[index]
    queued_prior: list[dict[str, object]] = []
    if queued:
        queued_prior = full[:order.index(
            str(queued[0]["mover_row"]["action_key"]))]  # type: ignore[index]
    out: dict[str, object] = {
        "current_min_gib": int(first["stage_gib"]),
        "next_min_gib": int(rest[0]["stage_gib"]) if rest else None,
        "final": not rest,
        "reading_phase": ahead_names[0] if ahead_names else None,
        "next_phase": str(rest[0]["phase"]) if rest else None,
        "next_mover_action_key": (str(rest[0]["mover_row"]["action_key"])  # type: ignore[index]
                                  if rest else None),
        "next_chunk_index": rest[0]["chunk_index"] if rest else None,
        "lead_mover_action_key": (str(full[0]["mover_row"]["action_key"])  # type: ignore[index]
                                  if full else None),
        # The advance itself: the first queued leg is what the fence
        # protects (its claim), with every earlier leg listed for the
        # safe-retire check replenish requires.  ``waiting`` stays the
        # unpublished future the gate decides on.
        "waiting": _entry_list(waiting),
        "prior": _advance_prior(prior),
        "queued": _entry_list(queued),
        "queued_prior": _advance_prior(queued_prior),
        "fence_target": fence_target,
        "fence_prior": fence_prior,
    }
    return out



# -- Shared staged ranges (#1026) -------------------------------------------
#
# N consumers that read one staged range name one mover.  A mover's action key
# is the hash of its whole sealed body -- the argv, the checkout snapshot, the
# pricing read off receipts at submission, the log name -- so two submitters
# cannot derive the same key independently, and "drop the consumer from the
# preimage" is not enough to make them agree.  The agreement is filed instead:
# the first submitter to seal a range registers its mover under the range's
# identity, and every later submitter of the same range puts that registered
# row in its own plan.  Everything downstream then sees one key: the window's
# ``_mover_state``, the ledger's one holder, the pool's staged-wait verdict and
# ``expected_landings`` need no change to agree across consumers.
#
# The registry lives under ``residency-plans/shared/``.  Every reader of the
# plan directory lists ``<consumer>.json`` names only, so a subdirectory is
# invisible to all of them, as ``superseded/`` already is.

#: The schema of one registered shared range.
SHARED_RANGE_SCHEMA_V1 = "prismaquant.prismabuild.shared_range.v1"

#: The schema of the mover -> range index beside it.
SHARED_MOVER_SCHEMA_V1 = "prismaquant.prismabuild.shared_mover.v1"

#: The subdirectory of the plan directory the registry lives in.
SHARED = "shared"

_SHARED_RANGES = "ranges"
_SHARED_MOVERS = "movers"

#: The fields of a stage tier's announcement a shared mover's argv is sealed
#: with (``movement_actions.movement_tools``).  A registration records them,
#: and it is reused only by a submission that reads the same announcement.
SEALED_AGAINST_FIELDS = ("mover_python", "mover_tools_root")

#: :func:`share_namespace_of`'s answers within one tier cycle (#1026):
#: ``{(queue root, mover key): namespace or None}``, or ``None`` outside a
#: cycle, where every ask reads the index.  ``tier_loop.cycle`` arms it.  A
#: mover's index is published once and never rewritten or removed, so no
#: answer changes under a cycle for a mover it read a plan for: a submitter
#: files the index before it freezes the plan that names the mover.
_SHARE_NAMESPACE_MEMO: list[dict[tuple[str, str], str | None] | None] = [None]


def share_namespace(manifest_sha256: str, tier_id: str, start: int,
                    end: int) -> str:
    """The namespace one staged range is shared under.

    The digest of the four fields ``core.residency_descriptor`` binds, which
    are the same four ``tier_loop._descriptor`` matches ranges on: what makes
    two consumers' ranges the same bytes on the same tier.  A shared mover's
    fragment and material are filed under this namespace rather than under a
    consumer, because no one consumer's death may retire bytes the others
    still read.  It is a 64-character digest like an action key, and no queue
    record ever carries it, so it is never taken for a live or ended consumer.
    """

    return pb.canonical_sha256({
        "schema": SHARED_RANGE_SCHEMA_V1,
        "manifest_sha256": str(manifest_sha256),
        "tier_id": str(tier_id),
        "range_start_bytes": int(start),
        "range_end_bytes": int(end),
    })


def _shared_root(queue) -> Path:
    return Path(queue.root) / _pool.RESIDENCY_PLANS / SHARED


def shared_range_path(queue, namespace: str) -> Path:
    """Where the mover registered for one shared range is filed."""

    return (_shared_root(queue) / _SHARED_RANGES
            / f"{_action_key(namespace, where='share namespace')}.json")


def shared_mover_path(queue, mover_action_key: str) -> Path:
    """Where a shared mover's range is indexed by the mover's own key."""

    return (_shared_root(queue) / _SHARED_MOVERS
            / f"{_action_key(mover_action_key, where='shared mover')}.json")


def _validate_shared_range(value: object, *, namespace: str) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise ResidencyPlanError("a shared range record must be an object")
    if value.get("schema") != SHARED_RANGE_SCHEMA_V1:
        raise ResidencyPlanError(
            f"shared range schema must be {SHARED_RANGE_SCHEMA_V1!r}")
    descriptor = value.get("descriptor")
    if not isinstance(descriptor, Mapping):
        raise ResidencyPlanError("a shared range record needs a descriptor")
    try:
        derived = share_namespace(
            str(descriptor["manifest_sha256"]), str(descriptor["tier_id"]),
            int(descriptor["range_start_bytes"]),
            int(descriptor["range_end_bytes"]))
    except (KeyError, TypeError, ValueError) as exc:
        raise ResidencyPlanError(
            f"a shared range descriptor does not read: {exc!r}") from None
    if derived != namespace or value.get("share_namespace") != namespace:
        raise ResidencyPlanError(
            f"shared range {namespace[:12]} is filed under a namespace its "
            f"descriptor does not derive")
    mover = _action_key(value.get("mover_action_key"),
                        where="shared range mover_action_key")
    row = value.get("mover_row")
    if not isinstance(row, Mapping) or str(row.get("action_key")) != mover:
        raise ResidencyPlanError(
            f"shared range {namespace[:12]} names mover {mover[:12]} but "
            f"files a row for another key")
    sealed_against = value.get("sealed_against")
    if (not isinstance(sealed_against, Mapping)
            or sorted(sealed_against) != sorted(SEALED_AGAINST_FIELDS)
            or not all(isinstance(sealed_against[field], str)
                       and sealed_against[field]
                       for field in SEALED_AGAINST_FIELDS)):
        raise ResidencyPlanError(
            f"shared range {namespace[:12]} names no tier announcement its "
            f"mover was sealed against ({', '.join(SEALED_AGAINST_FIELDS)})")
    residency = row.get("residency")
    if (not isinstance(residency, Mapping)
            or str(residency.get("manifest_sha256")) != str(
                descriptor["manifest_sha256"])
            or str(residency.get("tier_id")) != str(descriptor["tier_id"])
            or residency.get("range_start_bytes")
            != int(descriptor["range_start_bytes"])
            or residency.get("range_end_bytes")
            != int(descriptor["range_end_bytes"])):
        raise ResidencyPlanError(
            f"shared range {namespace[:12]}'s mover row stages another range")
    return dict(value)


def read_shared_range(queue, namespace: str) -> dict[str, object] | None:
    """The mover registered for one shared range, or ``None`` when none is.

    A record that is there and does not read or validate raises
    :class:`ResidencyPlanError`: a submitter that took it for absent would
    seal a second mover for bytes the first already stages, which is the
    double charge this registry exists to prevent.
    """

    path = shared_range_path(queue, namespace)
    try:
        raw = path.read_bytes()
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise ResidencyPlanError(
            f"shared range {namespace[:12]} unreadable: {exc!r}") from None
    try:
        return _validate_shared_range(json.loads(raw), namespace=namespace)
    except ValueError as exc:
        raise ResidencyPlanError(
            f"shared range {namespace[:12]} does not validate: {exc}") from None


def read_shared_mover(queue, mover_action_key: str) -> dict[str, object] | None:
    """The range a mover stages for sharing, or ``None`` for a per-consumer one.

    ``None`` is the ordinary answer: every mover sealed before #1026, and
    every mover a submitter sealed with sharing off, has no index record.
    One that is there and does not read raises :class:`ResidencyPlanError`,
    so a caller deciding whose bytes these are fails closed rather than
    treating a shared range as one consumer's.
    """

    path = shared_mover_path(queue, mover_action_key)
    try:
        raw = path.read_bytes()
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise ResidencyPlanError(
            f"shared mover {str(mover_action_key)[:12]} unreadable: "
            f"{exc!r}") from None
    try:
        value = json.loads(raw)
    except ValueError as exc:
        raise ResidencyPlanError(
            f"shared mover {str(mover_action_key)[:12]} does not read: "
            f"{exc}") from None
    if (not isinstance(value, Mapping)
            or value.get("schema") != SHARED_MOVER_SCHEMA_V1
            or value.get("mover_action_key") != str(mover_action_key)):
        raise ResidencyPlanError(
            f"shared mover {str(mover_action_key)[:12]} does not validate")
    _action_key(value.get("share_namespace"), where="shared mover namespace")
    return dict(value)


def share_namespace_of(queue, mover_action_key: str) -> str | None:
    """The namespace a shared mover files under, or ``None`` if it is not one.

    Within a tier cycle each mover's index is read once
    (:data:`_SHARE_NAMESPACE_MEMO`): the cycle asks per plan leg, and every
    sharer's plan names the same movers, so the same index was read once
    per sharer.  An index that does not read is never remembered, so the
    next ask raises again.
    """

    memo = _SHARE_NAMESPACE_MEMO[0]
    cache_key = (str(queue.root), str(mover_action_key))
    if memo is not None and cache_key in memo:
        return memo[cache_key]
    record = read_shared_mover(queue, mover_action_key)
    namespace = None if record is None else str(record["share_namespace"])
    if memo is not None:
        memo[cache_key] = namespace
    return namespace


def mover_announcement(tier: Mapping[str, object]) -> dict[str, str]:
    """The fields of ``tier``'s announcement a stage mover is sealed with.

    What :func:`register_shared_range` compares a registration against.
    The same two fields ``movement_actions.movement_tools`` builds a mover's
    argv from, read the same way.
    """

    return {field: str(tier.get(field) or "") for field in SEALED_AGAINST_FIELDS}


def _operator_withdrawn(queue, mover_action_key: str) -> bool:
    """Whether a live withdrawal of this mover is an operator's decision.

    The rule ``tier_loop._operator_withdrawal`` applies to a plan's movers
    (#708): a preemption carries ``preempted_by`` and a membership handoff
    its own proof, and both requeue the same work, so neither retires the
    registration.  An operator's cancellation has no successor: a range
    registered to a cancelled mover could never be staged again by anyone
    who reused it, because the window never republishes a withdrawn key.
    """

    marker = queue.live_withdrawal(mover_action_key)
    if not isinstance(marker, Mapping) or marker.get("preempted_by"):
        return False
    return not _pool.membership_handoff_authorized(marker)


def _reuse_despite_stale(queue, mover_action_key: str) -> tuple[bool, str]:
    """Whether a stale registration's mover must still be reused, and why.

    A mover in ``ready/`` or ``claimed/`` is about to copy, or copying, the
    range: sealing a second mover for it now is the double charge the
    registry exists to prevent, and the next submission after it ends
    replaces the registration.  A state that cannot be read is treated the
    same way, because reuse is what the registry did before it compared
    announcements, and a later submission asks again.

    Read under the namespace's lock, not the mover's: a window that
    publishes the old mover for a plan sealed against it just after this
    read makes the old mover live beside the new one.  That costs what two
    per-consumer movers of one range cost before #1026, once: two token
    bookings, and each copy adopts the names the other already published
    instead of copying them (``stage_move._StagedPublisher.try_adopt``).
    """

    state, why = live_state(queue, mover_action_key)
    if state in (_pool.READY, _pool.CLAIMED):
        return True, f"live ({state})"
    if state is None and why:
        return True, f"live state unknown: {why}"
    return False, ""


def register_shared_range(queue, *, manifest_sha256: str, tier_id: str,
                          start: int, end: int, seal,
                          registered_by: str,
                          sealed_against: Mapping[str, str],
                          ) -> tuple[dict[str, object], bool]:
    """The mover every consumer of one staged range names; seal it if none.

    ``seal`` is called only when no usable registration exists, and returns
    ``(mover_row, derivation)`` for a mover sealed with ``--consumer-action-key``
    set to this range's :func:`share_namespace`, against the tier
    announcement ``sealed_against`` names (:func:`mover_announcement`).  Returns
    ``(record, sealed_here)``.

    First writer wins, under the namespace's transition lock, so two
    submitters of one range registering at once agree on one mover and the
    loser's sealed request is never published.  A registration is kept for
    as long as its mover can be published under the tier announcement it was
    sealed against: a ``done`` mover whose range was evicted is published
    again by the window (``tier_loop._mover_state``), so reuse needs no
    retirement.  Two things replace it, so that a later submitter stages
    the range under a fresh mover:

    * an operator's live withdrawal of the registered mover (#708), which
      the window refuses to republish;
    * a new tier announcement.  A mover's argv names the interpreter and
      the tool root the tier announced when it was sealed, which is one
      runtime generation's directory, so a registration reused across a
      publish would run the old generation's mover for every later
      consumer.  A registration whose ``sealed_against`` differs from this
      submission's is stale, and it is still reused while its mover is live
      (:func:`_reuse_despite_stale`).

    The replaced record moves to ``<namespace>.<mover>.withdrawn`` or
    ``.stale``, so the new one is a first write rather than an overwrite.
    The old mover's index stays: plans sealed against it still name it, and
    their fan-out, interest and egress read its namespace through it.

    The mover index (:func:`shared_mover_path`) is filed before the range
    record, both immutable.  A crash between the two leaves an index for a
    mover no plan names, which nothing reads.
    """

    wanted = {field: str(sealed_against.get(field) or "")
              for field in SEALED_AGAINST_FIELDS}
    if not all(value.startswith("/") for value in wanted.values()):
        raise ResidencyPlanError(
            f"a shared range must be registered against a tier announcement "
            f"that names absolute {' and '.join(SEALED_AGAINST_FIELDS)}; "
            f"got {wanted!r}")
    namespace = share_namespace(manifest_sha256, tier_id, start, end)
    path = shared_range_path(queue, namespace)
    path.parent.mkdir(parents=True, exist_ok=True)
    with queue._transition_locked(namespace):
        current = read_shared_range(queue, namespace)
        retire_as = ""
        if current is not None:
            mover = str(current["mover_action_key"])
            if _operator_withdrawn(queue, mover):
                retire_as = "withdrawn"
            elif dict(current["sealed_against"]) == wanted:   # type: ignore[call-overload]
                return current, False
            else:
                reuse, _why = _reuse_despite_stale(queue, mover)
                if reuse:
                    return current, False
                retire_as = "stale"
        mover_row, derivation = seal()
        mover = _action_key(mover_row.get("action_key"),
                            where="shared mover row action_key")
        record: dict[str, object] = {
            "schema": SHARED_RANGE_SCHEMA_V1,
            "share_namespace": namespace,
            "descriptor": {"manifest_sha256": str(manifest_sha256),
                           "tier_id": str(tier_id),
                           "range_start_bytes": int(start),
                           "range_end_bytes": int(end)},
            "mover_action_key": mover,
            "mover_row": dict(mover_row),
            "sealed_against": dict(wanted),
            "derivation": dict(derivation or {}),
            "registered_by": str(registered_by),
            "registered_unix": time.time(),
        }
        if current is not None:
            record["replaces"] = {
                "mover_action_key": str(current["mover_action_key"]),
                "reason": retire_as}
        _validate_shared_range(record, namespace=namespace)
        index = shared_mover_path(queue, mover)
        index.parent.mkdir(parents=True, exist_ok=True)
        try:
            _pool._publish_immutable(index, pb._canonical_bytes({
                "schema": SHARED_MOVER_SCHEMA_V1,
                "mover_action_key": mover,
                "share_namespace": namespace}), where="shared mover index")
        except _pool.PoolContractError as exc:
            raise ResidencyPlanError(
                f"mover {mover[:12]} is already indexed under another "
                f"range: {exc}") from None
        if current is not None:
            # The replaced registration goes to a retired name, so the
            # replacement is a first write rather than an overwrite.
            retired = path.with_name(
                f"{namespace}.{current['mover_action_key']}.{retire_as}")
            os.replace(path, retired)
        try:
            _pool._publish_immutable(path, pb._canonical_bytes(record),
                                     where="shared range")
        except _pool.PoolContractError as exc:
            raise ResidencyPlanError(
                f"a different mover is already registered for shared range "
                f"{namespace[:12]}: {exc}") from None
        return record, True


__all__ = [
    "RESIDENCY_PLAN_SCHEMA_V1",
    "ResidencyPlanError",
    "SEALED_AGAINST_FIELDS",
    "SHARED_MOVER_SCHEMA_V1",
    "SHARED_RANGE_SCHEMA_V1",
    "accepted",
    "advance_needs",
    "build_plan",
    "expected_landings",
    "find_mover_leg",
    "freeze",
    "landing_seconds",
    "lead_mover_row",
    "leads_for",
    "legs_over",
    "mover_announcement",
    "mover_keys",
    "ram_mover_keys",
    "read",
    "read_footprint",
    "read_shared_mover",
    "read_shared_range",
    "refill_horizon",
    "register_shared_range",
    "remaining",
    "runahead_budget_gib",
    "runahead_step_gib",
    "share_namespace",
    "share_namespace_of",
    "shared_mover_path",
    "shared_range_path",
    "stage_mover_keys",
    "validate_plan",
    "window",
]
