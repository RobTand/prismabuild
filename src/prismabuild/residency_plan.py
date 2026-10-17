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
import bisect
import contextlib
from dataclasses import dataclass
from functools import cached_property
import hashlib
import json
import math
import os
from pathlib import Path
import re
import time

from . import core as pb
from . import immutable_files
from . import manifest_layout
from . import pool as _pool
from . import residency_map
from . import storage_tiers, movement_actions, contracts
from . import data_manifest as manifest_codec
from .submission_types import SubmissionRefused
from .queue_publication import RowPublication

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
SUPERSEDED = _pool.record_store.RESIDENCY_SUPERSEDED

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
    "stage_chunks",
    # A declared prelaunch-resident phase (#1594).  Admission waits on all
    # chunks of all declared phases (see ``leads_for``).  The key is present
    # only when declared.  A plan with no declaration stays byte-identical.
    "resident_before_launch"})
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
    ``manifest_layout.manifest_phase_ranges`` already refuses a phase table that
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
        # Copy the prelaunch declaration (#1594) only when the submitter
        # phase carries it.  A plan with no declaration omits the key and
        # stays byte-identical.  ``validate_plan`` refuses a non-true value
        # and a non-prefix set.
        if "resident_before_launch" in phase:
            entry["resident_before_launch"] = phase["resident_before_launch"]
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
    # Check the prelaunch declaration (#1594).  Only a literal true
    # declares.  The declared phases form a contiguous prefix from the
    # first phase.  A prelaunch phase behind an unread phase has no staging
    # order.  Refuse it here, where nothing is queued, not at admission.
    for phase in checked:
        if ("resident_before_launch" in phase
                and phase["resident_before_launch"] is not True):
            raise ResidencyPlanError(
                f"plan phase {str(phase.get('name'))!r} resident_before_launch "
                f"must be true when present, got "
                f"{phase['resident_before_launch']!r}")
    declared = [str(phase["name"]) for phase in checked
                if phase.get("resident_before_launch") is True]
    ordered = [str(phase["name"]) for phase in checked]
    if declared != ordered[:len(declared)]:
        raise ResidencyPlanError(
            f"prelaunch phases {declared} are not a contiguous prefix of "
            f"the plan order {ordered}")
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
    path = queue.records.residency_plan_path(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    # The consumer's own transition lock, the one ``withdraw`` and the queue's
    # publication use: a plan is filed, marked and reaped exclusively, so a
    # reap cannot archive a body filed between its read and its rename, and a
    # deliberate reseal cannot interleave with the retirement of its
    # predecessor (#708).
    with queue.action_transition_lock(key):
        try:
            queue.records.file_residency_plan_at(path, checked)
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
        raw = queue.records.text_document_at(
            Path(queue.records.residency_plan_path(consumer_action_key)),
        )
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
        return validate_plan(raw)
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

    path = queue.records.residency_plan_path(key)
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
    return queue.records.residency_supersession_path(key, plan_sha256(plan))


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
        paths = immutable_files.match_optional(path.parent, path.name)
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
    state, marker, error = _read_marker(queue.records.residency_supersession_path(key, digest))
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
    current = incarnation(queue.records.residency_plan_path(key))
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
                    by: str = "",
                    conflict: Mapping[str, object] | None = None,
                    ) -> dict[str, object] | None:
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

    ``conflict`` is the structured cause of a mover's terminal refusal
    (#966, #1004): the refusal, the staged path and every owner with its
    state.  It rides on the marker so whoever reads the stall -- the
    consumer's claim denial, ``pbstatus --starvation`` -- names the path and
    both owners without parsing ``reason`` (:func:`supersession_summary`).

    Idempotent and first-writer; the marker names the filing it covers.
    """

    key = _action_key(consumer_action_key, where="consumer_action_key")
    with queue.action_transition_lock(key):
        current = incarnation(queue.records.residency_plan_path(key))
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
        path = queue.records.residency_supersession_path(key, digest)
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
        if conflict is not None:
            marker["conflict"] = _conflict_record(conflict)
        try:
            queue.records.file_residency_supersession_at(path, marker)
        except _pool.PoolContractError:
            # A concurrent mark of this exact filing won; its marker is the
            # decision, not this one.
            state, existing, _error = _read_marker(path)
            return existing if state == "ok" else None
        return marker


def _conflict_record(conflict: Mapping[str, object]) -> dict[str, object]:
    """The structured cause a supersession marker carries, JSON-safe."""

    owners = []
    for row in conflict.get("owners") or ():
        if not isinstance(row, Mapping):
            continue
        owner = {name: str(row[name]) for name in
                 ("consumer_action_key", "mover_action_key", "state", "why")
                 if row.get(name) is not None}
        owners.append(owner)
    record: dict[str, object] = {"owners": owners}
    for name in ("refusal", "stage_path", "consumer_action_key",
                 "mover_action_key", "declared_sha256", "why"):
        if conflict.get(name) is not None:
            record[name] = str(conflict[name])
    return record


def supersession_summary(marker: Mapping[str, object] | None,
                         ) -> dict[str, object] | None:
    """What a stalled consumer's readers say about its retired window.

    ``None`` for no marker.  Otherwise the marker's reason, who marked it and
    which movers, and -- when a mover's terminal refusal retired it (#966,
    #1004) -- the refusal, the staged path and every owner with its state,
    lifted out of the marker's ``conflict``.  An unreadable marker says so;
    it is never read as absent.
    """

    if marker is None:
        return None
    if marker.get("unreadable"):
        return {"unreadable": True,
                "error": str(marker.get("error") or "unreadable marker")}
    summary: dict[str, object] = {
        "reason": str(marker.get("reason") or ""),
        "marked_by": str(marker.get("marked_by") or ""),
        "marked_unix": marker.get("marked_unix"),
        "movers": [str(mover) for mover in marker.get("movers") or ()
                   if isinstance(mover, str)],
    }
    conflict = marker.get("conflict")
    if isinstance(conflict, Mapping):
        for name in ("refusal", "stage_path", "why"):
            if conflict.get(name) is not None:
                summary[name] = conflict[name]
        summary["owners"] = [dict(row) for row in conflict.get("owners") or ()
                             if isinstance(row, Mapping)]
    return summary


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
_LIVE_STATE_LABELS = {_pool.record_store.CLAIMED: "claimed", _pool.record_store.READY: "queued"}


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

    answer = queue.records.active_pair(action_key)
    if isinstance(answer, _pool.record_store.Unknown):
        return None, answer.reason
    return _pool.record_store.value_or_raise(answer), ""


def handoff_safe(queue, consumer_action_key: str,
                 plan: Mapping[str, object]) -> tuple[bool, str]:
    """Whether a superseded window's ownership has ended.

    A fresh plan is a new decomposition.  Its keys may differ from the old
    one's, or match them -- a same-body reseal seals the same keys, and a
    shared range's mover (#1026) is named by every consumer of the range --
    so replacing the old window while any of its work is still live would
    strand a queued or running row nobody can publish, egress or attribute,
    or retire a cancellation that row still answers to.  The test is the
    queue's own state, never a clock:

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
    with queue.action_transition_lock(key):
        state, why = live_state(queue, key)
        if why:
            return False, f"the consumer: {why}"
        if state is not None:
            return False, f"the consumer is still {_LIVE_STATE_LABELS[state]}"
        for child in child_keys(plan):
            try:
                with queue.action_transition_lock(child):
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
    current = incarnation(queue.records.residency_plan_path(key))
    if current is None:
        return False, "its plan filing is gone"
    if filing is not None and tuple(filing) != current:
        return False, "its plan filing was replaced"
    for state in (_pool.record_store.CLAIMED, _pool.record_store.READY):
        path = queue.records.item_path(state, key)
        try:
            queue.records.item_metadata(state, key)
        except FileNotFoundError:
            continue
        except OSError as exc:
            return False, f"the consumer's {state} record could not be read: {exc}"
        if generation is not None:
            try:
                item = _pool.record_store.read_document(path)
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

    path = queue.records.residency_supersession_path(consumer_action_key, digest)
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
    with queue.action_transition_lock(key):
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
        path = queue.records.residency_plan_path(key)
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
    """Retire a predecessor window's visible child cancellations for a seal.

    A submission publishes its consumer and nothing else: every phase is the
    window's to publish, the first included, as the consumer advances.  An
    action key is a content hash, so a resubmission of the same consumer,
    price and tool seals the same child keys, and a shared range's mover
    (#1026) is named by every consumer of that range.  The *visible*
    withdrawal markers a predecessor generation left on those keys (an
    operator's cancellation, or the dead-consumer pass stopping a dead
    consumer's movers) outlive the ownership they were made against.  Read as
    live, they supersede the new window before its second phase ever
    published (#708 review, #1114).

    A deliberate submission is a new generation of that consumer's window, so
    it retires those predecessor markers: the keys are its own sealed
    children, and the same act that re-submits the consumer is how a person
    asks for the work again.  Four boundaries keep it honest:

    * the plan is filed first (:func:`seal_window`).  From that moment the
      dead-consumer pass counts this consumer's interest in every mover the
      plan names (:func:`plan_interest`), so it cannot withdraw one for
      someone else afterwards;
    * the renewal decides while it holds the transition lock of every mover
      the plan names, taken in key order after the consumer's own.  The pass
      decides and withdraws under the same mover lock, so each of its
      withdrawals either landed before this listing, and is retired here, or
      runs after it and sees the filed plan.  Without that, a pass that read
      interest before the filing and withdrew after the listing superseded
      the fresh window (#1114);
    * :func:`handoff_safe` must prove the predecessor's ownership ended -- no
      live consumer and no queued or claimed child -- before anything is
      retired.  This is the same proof ``reap`` uses; a window whose work is
      still live refuses by name rather than being replaced;
    * only the *visible* marker is moved; the immutable decision under
      ``withdrawn/decisions/`` stays, and the marker itself is filed under
      ``withdrawn/superseded/`` as evidence, never deleted.  A marker that
      cannot be read is unknown state, not "no cancellation": the whole
      renewal refuses while any hit is unreadable, and nothing is retired
      before that refusal.

    The boundary for every child is this listing, made under the locks.  A
    cancellation filed for a child after the locks are released is the new
    window's own decision and survives: the window's next cycle reads it as
    live, refuses to publish the child and marks the plan superseded.  The
    automatic publisher never retires a cancellation.  With no marker on any
    child the answer is empty and nothing is proven or moved: a live child
    is then the same work this window asks for, adopted as it stands.
    """

    key = _action_key(consumer_action_key, where="consumer_action_key")
    with contextlib.ExitStack() as held:
        held.enter_context(queue.action_transition_lock(key))
        for mover in sorted(set(mover_keys(plan))):
            held.enter_context(queue.action_transition_lock(mover))
        try:
            cancelled = queue.withdrawn_keys()
        except OSError as exc:
            raise ResidencyPlanError(
                f"the live withdrawal markers could not be listed: {exc}") from None
        hits = sorted(child for child in set(child_keys(plan))
                      if child in cancelled)
        if not hits:
            return {"consumer_action_key": key, "retired": []}
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
            with queue.action_transition_lock(child):
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
            with queue.action_transition_lock(child):
                try:
                    if queue._supersede_withdrawal(child) is not None:
                        retired.append(child)
                except (_pool.PoolContractError, OSError) as exc:
                    raise ResidencyPlanError(
                        f"the cancellation marker for {child[:12]} could not "
                        f"be retired: {exc}") from None
        return {"consumer_action_key": key, "retired": retired}


def seal_window(queue, plan: Mapping[str, object], *,
                renew: bool = True) -> dict[str, object]:
    """File a window's plan, then take its renewal boundary (#1114).

    One transaction under the consumer's transition lock, and the order is
    the point: :func:`freeze` first, :func:`retire_predecessor_cancellations`
    second.  Filing the plan is how this consumer's interest in the movers it
    names becomes visible to the dead-consumer pass (:func:`plan_interest`),
    and the renewal's locked listing is the instant after which a
    predecessor's cleanup can no longer cancel them.  Renewed the other way
    round, a pass that read interest just before the filing could withdraw a
    mover just after the listing, and the fresh window's first cycle would
    supersede it.

    ``freeze`` stays first-writer, so a plan already filed for this consumer
    (a resubmission reusing its frozen plan) is verified rather than
    replaced, and renewed on the same boundary: its children carry the same
    predecessor cancellations a fresh seal's would.  ``renew`` is ``False``
    only for a caller re-attaching to its own live publication.

    A renewal that refuses or fails leaves nothing behind: a plan this call
    filed is archived under ``superseded/`` (reason ``seal-refused``) before
    the error is raised, so no plan this call filed outlives it unpublished.
    A plan that was already filed stays as it was.  What this cannot cover
    is a process killed after the seal, or a publication that fails after
    it: that plan stays filed with no row, and until it is reaped it keeps
    any dead consumer's mover it names until the mover ends.  Returns the
    renewal's answer (``retired`` lists the markers it retired).
    """

    checked = validate_plan(plan)
    key = str(checked["consumer_action_key"])
    path = queue.records.residency_plan_path(key)
    with queue.action_transition_lock(key):
        before, error = _stat_incarnation(path)
        if error is not None:
            raise ResidencyPlanError(
                f"the filed plan for {key[:12]} could not be read before "
                f"sealing: {error}")
        freeze(queue, checked)
        if not renew:
            return {"consumer_action_key": key, "retired": []}
        try:
            return retire_predecessor_cancellations(queue, key, checked)
        except Exception:
            # Any failure, not only a refusal: an ``OSError`` out of the
            # handoff proof or the withdrawn listing would otherwise leave a
            # filed plan with no row and no ending, which the dead-consumer
            # pass reads as a live seal's interest in every mover it names.
            filed = incarnation(path)
            if before is None and filed is not None:
                target = (path.parent / SUPERSEDED
                          / f"{path.stem}.{time.time():.6f}.seal-refused.json")
                target.parent.mkdir(parents=True, exist_ok=True)
                try:
                    os.replace(path, target)
                except FileNotFoundError:            # pragma: no cover
                    pass
            raise


#: Per queue root and consumer, every action key its filed plan's bytes
#: carry, keyed by the filing's incarnation.  A filing is immutable, so the
#: answer holds until the incarnation moves; entries for plans no longer
#: filed are dropped.
_FILED_KEYS: dict[tuple[str, str],
                  tuple[tuple[int, int, int], frozenset[str]]] = {}

#: One action key in a plan's bytes: 64 lowercase hex digits, not part of a
#: longer run.
_KEY_IN_BYTES = re.compile(rb"(?<![0-9a-f])[0-9a-f]{64}(?![0-9a-f])")


def _filed_keys(queue, consumer_action_key: str,
                ) -> tuple[frozenset[str] | None, str]:
    """``(keys, "")`` for one filed plan, or ``(None, why)`` when unknown.

    The keys are read off the plan's bytes, not its validated body, so the
    answer does not depend on this reader knowing the plan's schema: a plan
    sealed by a newer generation than this reader (#615) names its movers
    all the same, and one this reader refuses must not read as "names
    nothing".  Every mover a plan names appears in its bytes as its key, so
    a key absent from the bytes is a mover the plan does not name; the other
    keys a plan carries (its consumer, its egress rows, its manifest digest)
    are never a mover's, so the set over-states nothing a caller asks.
    """

    root = str(queue.root)
    path = queue.records.residency_plan_path(consumer_action_key)
    for _attempt in range(3):
        before, error = _stat_incarnation(path)
        if error is not None:
            return None, f"its plan could not be stat-ed: {error}"
        if before is None:
            return frozenset(), ""             # reaped since the listing
        kept = _FILED_KEYS.get((root, consumer_action_key))
        if kept is not None and kept[0] == before:
            return kept[1], ""
        try:
            raw = queue.records.raw_record_at(path)
        except FileNotFoundError:
            return frozenset(), ""
        except OSError as exc:
            return None, f"its plan could not be read: {exc}"
        after, error = _stat_incarnation(path)
        if error is not None:
            return None, f"its plan could not be stat-ed: {error}"
        if after == before:
            keys = frozenset(match.decode()
                             for match in _KEY_IN_BYTES.findall(raw))
            _FILED_KEYS[(root, consumer_action_key)] = (before, keys)
            return keys, ""
    return None, "its plan kept changing while it was read"


def plan_interest(queue, mover_action_key: str, *,
                  exclude: Collection[str] = ()) -> dict[str, list[str]]:
    """The consumers whose filed plan names a mover and who are not dead.

    What the dead-consumer pass asks before it withdraws a dead consumer's
    queued or claimed mover (#1114).  A mover key is a content hash, and a
    shared range's mover (#1026) is named by every consumer of that range,
    so the same key is often another consumer's work too: a retry of the
    failed consumer over the same manifest names exactly the movers the
    failed attempt left queued.  Withdrawing one of those for the dead
    consumer cancels the live one's window.

    Read from the *filed plans*, not from live queue rows, because a plan is
    filed before its consumer's row is published (:func:`seal_window`), and
    that row can take a while to become visible: a sealed consumer with no
    row yet is interested.  A plan is read as bytes (:func:`_filed_keys`)
    rather than through :func:`validate_plan`, so a plan a newer generation
    sealed (#615) still counts by the keys it names.  A consumer counts as
    dead only when all of this holds, read under its own transition lock taken without blocking: it is
    in no live state, and it has a ``failed``, ``withdrawn`` or ``done``
    record.  So a consumer being sealed or resubmitted right now (its lock
    is held) is never taken for the dead generation it replaces.

    ``unknown`` names the consumers whose plan or state did not read, or
    whose lock was busy; the caller counts them as interested, because
    cancelling another consumer's work on a question nobody could answer is
    the direction that cannot be taken back.  A plan listing that fails is
    unknown too (``"<plans>"``).

    The caller holds the mover's transition lock.  That is what makes the
    answer good until the withdrawal it authorizes: a seal renews its
    children under the same lock (:func:`retire_predecessor_cancellations`),
    so a plan filed after this read is renewed after the withdrawal.
    """

    mover = _action_key(mover_action_key, where="mover_action_key")
    skip = {str(key) for key in exclude}
    interested: list[str] = []
    unknown: list[str] = []
    root = str(queue.root)
    try:
        filed = filed_consumers(queue)
    except OSError:
        return {"interested": [], "unknown": ["<plans>"]}
    for stale in [entry for entry in _FILED_KEYS
                  if entry[0] == root and entry[1] not in filed]:
        del _FILED_KEYS[stale]
    for key in filed:
        if key in skip:
            continue
        keys, _why = _filed_keys(queue, key)
        if keys is None:
            unknown.append(key)
            continue
        if mover not in keys:
            continue
        with queue.action_transition_lock(key, blocking=False) as got:
            if not got:
                unknown.append(key)          # mid-transition: a seal, say
                continue
            state, why = live_state(queue, key)
            if why:
                unknown.append(key)
                continue
            if state is not None:
                interested.append(key)
                continue
            ended, unreadable = False, False
            for terminal in (_pool.record_store.FAILED, _pool.record_store.WITHDRAWN, _pool.record_store.DONE):
                try:
                    if not queue.records.item_entry_present(terminal, key):
                        continue
                except OSError:
                    unreadable = True
                    break
                ended = True
                break
            if unreadable:
                unknown.append(key)
            elif not ended:
                # Filed, never published and never ended: a seal whose row
                # is not visible yet, or one that stopped between its plan
                # and its row.  Either way not dead.
                interested.append(key)
    return {"interested": sorted(set(interested)),
            "unknown": sorted(set(unknown))}


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


def prelaunch_phase_names(plan: Mapping[str, object]) -> list[str]:
    """The declared prelaunch-resident phases of the plan, in order, or ``[]``.

    A phase declares with a literal ``resident_before_launch: true``
    (#1594).  ``validate_plan`` refuses other values and non-prefix sets.
    A frozen plan always declares a prefix from the first phase.
    """

    phases = plan["phases"]
    assert isinstance(phases, list)
    return [str(phase["name"]) for phase in phases
            if isinstance(phase, Mapping)
            and phase.get("resident_before_launch") is True]


def leads_for(plan: Mapping[str, object]) -> list[str]:
    """The movers on which consumer admission waits.

    With no declaration, only the first-phase mover.  A wait on a later
    mover can deadlock the window.  With a declaration (#1594), all
    stage-leg chunk movers of all declared phases, in read order, chunked
    or whole.  The declared prefix stages whole before launch.  The rest
    stays lazy.
    """

    phases = plan["phases"]
    assert isinstance(phases, list)
    declared = [phase for phase in phases
                if isinstance(phase, Mapping)
                and phase.get("resident_before_launch") is True]
    if not declared:
        return [str(lead_mover_row(plan)["action_key"])]
    leads: list[str] = []
    for phase in declared:
        assert isinstance(phase, Mapping)
        chunks = phase.get("stage_chunks")
        if isinstance(chunks, list):
            for chunk in chunks:
                assert isinstance(chunk, Mapping)
                mover = chunk["mover_row"]
                assert isinstance(mover, Mapping)
                leads.append(str(mover["action_key"]))
        else:
            mover = phase["mover_row"]
            assert isinstance(mover, Mapping)
            leads.append(str(mover["action_key"]))
    return leads


def prelaunch_stage_legs(
        phase: Mapping[str, object]) -> list[dict[str, object]]:
    """One row per stage leg of a plan phase: chunked or whole (#1594).

    Each row gives the mover key, the ledger demand (``stage_gib``) and
    the range.  The peak bound and the gang demand read one shape.
    """

    chunks = phase.get("stage_chunks")
    if isinstance(chunks, list):
        legs = []
        for chunk in chunks:
            assert isinstance(chunk, Mapping)
            mover = chunk["mover_row"]
            assert isinstance(mover, Mapping)
            legs.append({"mover_key": str(mover["action_key"]),
                         "stage_gib": int(chunk["stage_gib"]),  # type: ignore[arg-type]
                         "start_bytes": int(chunk["start_bytes"]),  # type: ignore[arg-type]
                         "end_bytes": int(chunk["end_bytes"])})  # type: ignore[arg-type]
        return legs
    mover = phase["mover_row"]
    assert isinstance(mover, Mapping)
    return [{"mover_key": str(mover["action_key"]),
             "stage_gib": int(phase["stage_gib"]),  # type: ignore[arg-type]
             "start_bytes": int(phase["start_bytes"]),  # type: ignore[arg-type]
             "end_bytes": int(phase["end_bytes"])}]  # type: ignore[arg-type]


def prelaunch_peak_gib(prefix_sizes: Sequence[int],
                       suffix_sizes: Sequence[int]) -> dict[str, int]:
    """The retained-plus-overlap peak of a declared prefix (#1594).

    ``B = T + max over i of (size(pi) + size(p(i+1)))``, with
    ``size(p(n+1)) = 0``.  Submission, ``pbgang`` and the gate share this
    function.  They never disagree.
    """

    retained = sum(prefix_sizes)
    sizes = list(suffix_sizes)
    step = max([first + second
                for first, second in zip(sizes, [*sizes[1:], 0])],
               default=0)
    return {"retained_gib": retained, "suffix_gib": step,
            "peak_gib": retained + step}


def prelaunch_bound(plan: Mapping[str, object],
                    owned_by_others=frozenset()) -> dict[str, int] | None:
    """A declared plan peak tier demand, or ``None`` with no declaration (#1594).

    ``T`` sums the declared prefix ledger demands.  The suffix term covers
    the largest two adjacent suffix phases.  A phase size sums the
    ``stage_gib`` of its stage legs.  Legs in ``owned_by_others`` do not
    count.  Submission counts all legs, so the runtime peak never exceeds
    the submission peak.
    """

    phases = plan["phases"]
    assert isinstance(phases, list)
    owned = set(owned_by_others)
    prefix = [phase for phase in phases
              if isinstance(phase, Mapping)
              and phase.get("resident_before_launch") is True]
    if not prefix:
        return None
    suffix = [phase for phase in phases
              if not (isinstance(phase, Mapping)
                      and phase.get("resident_before_launch") is True)]

    def size(phase: Mapping[str, object]) -> int:
        return sum(leg["stage_gib"] for leg in prelaunch_stage_legs(phase)
                   if leg["mover_key"] not in owned)

    return prelaunch_peak_gib(
        [size(phase) for phase in prefix],
        [size(phase) for phase in suffix])


def gang_prelaunch_demand(members: Sequence[Mapping[str, object]],
                          tier_capacity: Mapping[str, int]
                          ) -> dict[str, dict[str, object]]:
    """A gang joint prelaunch demand per stage tier (#1594).

    ``members`` holds the member frozen plans.  ``tier_capacity`` maps each
    tier to minted GiB.  Each tier sums distinct movers.  Shared ranges
    count once through ``share_namespace``.  Members with no declaration
    add nothing.  Unknown capacity never refuses.
    """

    demand: dict[str, dict[str, object]] = {}

    def tier_entry(tier_id: str) -> dict[str, object]:
        entry = demand.get(tier_id)
        if entry is None:
            entry = {"tier_id": tier_id, "retained_gib": 0,
                     "suffix_gib": 0, "peak_gib": 0,
                     "members": [], "namespaces": set()}
            demand[tier_id] = entry
        return entry

    for member in members:
        if not isinstance(member, Mapping):
            continue
        phases = member.get("phases")
        if not isinstance(phases, list):
            continue
        names = [str(phase["name"]) for phase in phases
                 if isinstance(phase, Mapping)
                 and phase.get("resident_before_launch") is True]
        if not names:
            continue
        tier_id = str(member.get("tier_id"))
        digest = str(member.get("manifest_sha256"))
        entry = tier_entry(tier_id)
        seen = entry["namespaces"]
        assert isinstance(seen, set)
        key = member.get("consumer_action_key")
        if isinstance(key, str) and key not in entry["members"]:
            entry["members"].append(key)  # type: ignore[attr-defined]
        for phase in phases:
            if not isinstance(phase, Mapping):
                continue
            if phase.get("resident_before_launch") is not True:
                continue
            for leg in prelaunch_stage_legs(phase):
                namespace = share_namespace(
                    digest, tier_id, int(leg["start_bytes"]),
                    int(leg["end_bytes"]))
                if namespace in seen:
                    continue
                seen.add(namespace)
                entry["retained_gib"] = int(entry["retained_gib"]) + int(leg["stage_gib"])
        suffix_sizes: list[int] = []
        for phase in phases:
            if not isinstance(phase, Mapping):
                continue
            if phase.get("resident_before_launch") is True:
                continue
            size = 0
            for leg in prelaunch_stage_legs(phase):
                namespace = share_namespace(
                    digest, tier_id, int(leg["start_bytes"]),
                    int(leg["end_bytes"]))
                if namespace in seen:
                    continue
                seen.add(namespace)
                size += int(leg["stage_gib"])
            suffix_sizes.append(size)
        step = prelaunch_peak_gib([], suffix_sizes)["suffix_gib"]
        entry["suffix_gib"] = int(entry["suffix_gib"]) + step

    out: dict[str, dict[str, object]] = {}
    for tier_id, entry in demand.items():
        capacity = tier_capacity.get(tier_id)
        peak = int(entry["retained_gib"]) + int(entry["suffix_gib"])
        out[tier_id] = {
            "tier_id": tier_id,
            "retained_gib": int(entry["retained_gib"]),
            "suffix_gib": int(entry["suffix_gib"]),
            "peak_gib": peak,
            "members": sorted(entry["members"]),  # type: ignore[arg-type]
            "capacity_gib": capacity,
            "over_capacity": (capacity is not None and peak > int(capacity)),
        }
    return out


#: One live row's filed plan, reduced to what discovery needs (#1332), keyed
#: by ``(queue root, consumer)`` and re-read only when the filing's
#: incarnation changes.  A plan runs to 775 KB on the GLM rows, and the tier
#: loop asks about every live row several times a cycle, so the steady cost
#: is one ``stat`` per row per question, not a parse.
_FILED: dict[tuple[str, str], tuple[tuple[int, int, int], dict[str, object]]] = {}

#: A row's sealed progress order, keyed by ``(cas root, action key)``.  A
#: sealed request is content-addressed and immutable, so the answer never
#: changes; an unreadable request is not remembered and is asked again.
_PROGRESS_ORDERS: dict[tuple[str, str], tuple[str, ...] | None] = {}


def _filed_entry(queue, consumer_action_key: str, *,
                 on_unreadable: Callable[[Exception], None] | None = None,
                 ) -> dict[str, object] | None:
    """The discovery facts of one filed plan, or ``None`` when none is filed."""

    def refused(error: Exception) -> None:
        if on_unreadable is not None:
            on_unreadable(error)

    try:
        path = Path(queue.records.residency_plan_path(consumer_action_key))
    except ValueError as error:
        refused(error)
        return None
    memo_key = (str(queue.root), consumer_action_key)
    incarnation, error = _stat_incarnation(path)
    if error is not None:
        refused(error)
        return None
    if incarnation is None:
        _FILED.pop(memo_key, None)
        return None
    cached = _FILED.get(memo_key)
    if cached is not None and cached[0] == incarnation:
        return cached[1]
    plan, read_incarnation = read_filed(queue, consumer_action_key,
                                        on_unreadable=on_unreadable)
    if plan is None or read_incarnation is None:
        _FILED.pop(memo_key, None)
        return None
    entry: dict[str, object] = {
        "residency": {
            # The block ``pbrun``'s staged lane and ``pbcampaign`` write onto
            # a consumer row, field for field, so no reader can tell a
            # derived block from a sealed one by its shape.
            "schema": _pool.RESIDENCY_SCHEMA_V1,
            "manifest_sha256": plan["manifest_sha256"],
            "manifest_bytes": plan["manifest_bytes"],
            "tier_id": plan["tier_id"],
            "leads": leads_for(plan),
        },
        "phase_names": tuple(str(phase["name"])
                             for phase in plan["phases"]),  # type: ignore[union-attr]
        "mover_keys": tuple(mover_keys(plan)),
        # The declared prelaunch prefix (#1594), so a verdict reads one cached
        # tuple instead of parsing the whole plan on every claim scan.
        "prelaunch_phases": tuple(prelaunch_phase_names(plan)),
    }
    _FILED[memo_key] = (read_incarnation, entry)
    return entry


def filed_prelaunch_phases(queue, consumer_action_key: str, *,
                           on_unreadable: Callable[[Exception], None] | None = None,
                           ) -> tuple[str, ...] | None:
    """The declared prelaunch phases of a filed plan, or ``None`` when none is filed.

    ``None`` is "no readable plan": the caller keeps today's verdict.  An empty
    tuple is a readable plan that declares nothing (#1594).
    """

    entry = _filed_entry(queue, consumer_action_key, on_unreadable=on_unreadable)
    if entry is None:
        return None
    return tuple(entry["prelaunch_phases"])  # type: ignore[arg-type]


def retain_filed(queue, live_keys: Collection[str]) -> None:
    """Forget the filed-plan memo of every consumer no longer live.

    Bounded by the backlog, not by the loop's lifetime: the tier loop calls
    this with the keys its own census found ready or claimed.
    """

    root = str(queue.root)
    keep = set(live_keys)
    for stale in [memo_key for memo_key in _FILED
                  if memo_key[0] == root and memo_key[1] not in keep]:
        del _FILED[stale]
    # The progress-order memo is keyed by CAS root, not queue root, so it is
    # pruned by action key alone: forgetting another queue's live key costs
    # that queue one re-read of an immutable request, never a wrong answer.
    for stale in [memo_key for memo_key in _PROGRESS_ORDERS
                  if memo_key[1] not in keep]:
        del _PROGRESS_ORDERS[stale]


def consumer_residency(queue, item: Mapping[str, object], *,
                       on_unreadable: Callable[[Exception], None] | None = None,
                       ) -> tuple[dict[str, object] | None, str | None]:
    """The residency block the tier machinery reads for one live row (#1332).

    ``(block, "row")`` for a row that carries its own block naming leads --
    the ``--residency stage`` submitter's and ``pbcampaign``'s rows, exactly
    as before.  ``(block, "filed_plan")`` for a bare row with a filed plan:
    the #1247 planner files a plan and deliberately never rewrites the
    consumer's row (rewriting a ready item races its claim), so the block is
    derived from the filing, in the shape the submitter would have written.
    ``(None, None)`` for everything else, including a mover's or egress's
    own block, which names a range rather than leads.

    Before #1332 every reader tested the row's block alone, so a planner
    row's plan was filed and then staged by nobody: no mover, no map, and a
    GLM row read 51 GB cold at every row boundary.
    """

    residency = item.get("residency")
    if isinstance(residency, Mapping):
        if residency.get("leads"):
            return dict(residency), "row"
        return None, None
    # Only a queue row is a consumer.  ``claimed/`` also holds each claim's
    # ``<key>.lease``, which carries the row's action key but is not the row:
    # read as one, every claimed planner row would be counted twice.
    if item.get("schema") != _pool.POOL_ITEM_SCHEMA_V1:
        return None, None
    key = item.get("action_key")
    if not isinstance(key, str) or not key:
        return None, None
    entry = _filed_entry(queue, key, on_unreadable=on_unreadable)
    if entry is None:
        return None, None
    return dict(entry["residency"]), "filed_plan"  # type: ignore[arg-type]


def filed_mover_keys(queue, consumer_action_key: str) -> list[str]:
    """Every movement node the consumer's filed plan names, from the memo."""

    entry = _filed_entry(queue, consumer_action_key)
    return [] if entry is None else list(entry["mover_keys"])  # type: ignore[arg-type]


def progress_contract(phase_names: Sequence[str],
                      progress_order: Sequence[str] | None) -> str:
    """Whether a row's progress phases can say where it is in its read plan.

    ``linear`` when every read phase is a declared progress phase, in read
    order -- the contract ``pbrun`` has always required of a ``--residency
    stage`` submitter (``require_linear_read_plan_progress``).  Otherwise
    ``undeclared`` (no linear progress phases at all: none, or cyclic),
    ``unnamed`` (a read phase is not a progress phase), or ``misordered``.
    """

    names = [str(name) for name in phase_names]
    if not progress_order:
        return "undeclared"
    order = [str(name) for name in progress_order]
    if not set(names).issubset(order):
        return "unnamed"
    wanted = set(names)
    if [name for name in order if name in wanted] != names:
        return "misordered"
    return "linear"


def plan_phase(phase_names: Sequence[str], reported: str | None,
               progress_order: Sequence[str] | None) -> str | None:
    """The plan phase a reported progress phase places the consumer in.

    A read phase the consumer reports is itself.  A progress phase that is
    not a read phase -- ``pricing`` after the row has read its weights --
    places the consumer in the last read phase declared at or before it, but
    only under the ``linear`` contract, where the sealed progress order says
    which read phases come first.  That keeps ``remaining``'s rule of
    including the phase being read: the last read phase is never evicted
    under a running consumer, only the ones before it.  Without the
    contract, or for a phase declared before any read phase (``startup``),
    the name is returned unchanged and ``remaining`` reads it as the
    beginning, as it always has.

    Why the mapping must exist before a producer names its read phases:
    once a consumer has reported a read phase, the window evicts the phases
    before it; a later non-read report read as "the beginning" would find
    every evicted range ahead and unpublished and copy it again (#1332).
    """

    if reported is None:
        return None
    names = [str(name) for name in phase_names]
    if reported in names:
        return reported
    if (progress_order is None
            or progress_contract(names, progress_order) != "linear"):
        return reported
    order = [str(name) for name in progress_order]
    if reported not in order:
        return reported
    at = order.index(reported)
    passed = [name for name in names if order.index(name) <= at]
    return passed[-1] if passed else reported


def sealed_progress_order(item: Mapping[str, object]) -> tuple[str, ...] | None:
    """The row's sealed linear progress order, or ``None`` when it has none.

    Read from the sealed request (``pool.progress_policy``), the same source
    the worker enforces.  A cyclic policy has no order to place a read phase
    in, so it answers ``None``; so does a request that does not read, and
    that answer is not remembered.
    """

    key = item.get("action_key")
    cas_root = item.get("cas_root")
    if not isinstance(key, str) or not isinstance(cas_root, str):
        return None
    memo_key = (cas_root, key)
    if memo_key in _PROGRESS_ORDERS:
        return _PROGRESS_ORDERS[memo_key]
    try:
        policy = _pool.progress_policy(item, None)
    except (OSError, ValueError, KeyError, pb.ActionContractError,
            pb.CASTamperError, pb.CASUnavailableError,
            _pool.PoolContractError):
        return None
    order = (None if policy is None or policy.cycle
             else tuple(phase.name for phase in policy.phases))
    _PROGRESS_ORDERS[memo_key] = order
    return order


def accepted_plan_phase(queue, item: Mapping[str, object],
                        reported: str | None) -> str | None:
    """``plan_phase`` for one live row, from its filed plan and sealed order.

    ``reported`` unchanged when the row has no filed plan or the phase is
    already one of the plan's, which costs no request read.
    """

    if reported is None:
        return None
    key = item.get("action_key")
    if not isinstance(key, str):
        return reported
    entry = _filed_entry(queue, key)
    if entry is None:
        return reported
    names = entry["phase_names"]
    assert isinstance(names, tuple)
    if reported in names:
        return reported
    return plan_phase(names, reported, sealed_progress_order(item))


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

    directory = (Path(queue.records.residency_fragment_root())
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
    ledger = queue.tier(tier_id)
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
        if not ledger.holdings(mover):
            continue            # the tier no longer accounts for the range
        if bool(
            _pool.record_store.value_or_raise(
                queue.records.item_presence(_pool.record_store.CLAIMED, mover),
            ),
        ):
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
    ``manifest_layout.consumed_through`` keeps.  A name the plan does not carry --
    no progress yet, or a phase from another plan -- reads as the beginning,
    because a consumer that has not said where it is has not passed anything.
    """

    phases = list(plan["phases"])                                # type: ignore[arg-type]
    index = manifest_layout.phase_index(
        (str(phase["name"]) for phase in phases), accepted_phase)
    return phases[0 if index is None else index:]


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
    return manifest_layout.phase_index(
        (str(phase["name"]) for phase in phases), accepted_phase) is not None


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
    (``live_bytes_per_s``), from the time of the report.  A mover's own
    landing report (#1090) also carries ``copied_bytes``, the bytes read so
    far whether or not their entry has landed, and ``started_unix``, when
    the copy began after its start gate: the rate is then the copied bytes
    since the start, so a copy of a few large entries is priced from its
    first second rather than from its first landed entry.  A report in the
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
    # The mover's own report (#1090): bytes read, landed or not, since the
    # copy started.  Without it, the landed bytes since the claim.
    copied = mover.get("copied_bytes")
    started = mover.get("started_unix")
    own_report = (not isinstance(copied, bool) and isinstance(copied, int)
                  and copied >= 0 and _finite_number(started))
    progress = max(landed, int(copied)) if own_report else landed  # type: ignore[call-overload]
    origin = (max(claimed, float(started)) if own_report      # type: ignore[arg-type]
              else claimed)
    extra: dict[str, object] = ({"copied_bytes": min(progress, own),
                                 "started_unix": float(started)}  # type: ignore[arg-type]
                                if own_report else {})
    if mover.get("landed_phase") == "warm" or landed >= own:
        return {"expected_landing_unix": at, "landed_bytes": min(landed, own),
                "reported_unix": at, "live_bytes_per_s": None, **extra}
    if progress == 0 or at <= origin:
        return None
    live = progress / (at - origin)
    return {"expected_landing_unix": at + landing_seconds(own - min(progress, own), live),
            "landed_bytes": landed, "reported_unix": at,
            "live_bytes_per_s": live, **extra}


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
    read: at each, the stage GiB :attr:`ConsumerWindow.decision` publishes from an empty tier
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
        decision = ConsumerWindow(plan, name,
            LegObservation(mover_role=mover_role, horizon_end_bytes=end),
            WindowBudget(free_gib=capacity, capacity_gib=capacity)).decision             # type: ignore[arg-type]
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



def prelaunch_owned_gib(plan: Mapping[str, object],
                        held_by_holder: Mapping[str, Mapping[str, int]],
                        group_holder: str, kind: str) -> int:
    """Tokens one unit owns now on one tier: group holder plus stage movers (#1594).

    The tier loop passes a snapshot of the ledger: each holder maps to its
    per-kind counts.  Only ``kind`` counts.  Unknown holders count zero.
    """

    total = 0
    for holder in (group_holder, *stage_mover_keys(plan)):
        per_kind = held_by_holder.get(holder)
        if isinstance(per_kind, Mapping):
            total += int(per_kind.get(kind, 0) or 0)
    return total


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
SHARED = _pool.record_store.RESIDENCY_SHARED

_SHARED_RANGES = "ranges"
_SHARED_MOVERS = _pool.record_store.RESIDENCY_SHARED_MOVERS

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

    Hash ``manifest_sha256``, ``tier_id``, ``range_start_bytes`` and
    ``range_end_bytes``, the same fields ``tier_loop._descriptor`` compares
    to identify the same bytes on the same tier. A shared mover's
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
    return Path(queue.root) / _pool.record_store.RESIDENCY_PLANS / SHARED


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
    if state in (_pool.record_store.READY, _pool.record_store.CLAIMED):
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
    with queue.action_transition_lock(namespace):
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
            immutable_files.publish_exact(index, pb._canonical_bytes({
                "schema": SHARED_MOVER_SCHEMA_V1,
                "mover_action_key": mover,
                "share_namespace": namespace}), where="shared mover index", conflict_error=_pool.PoolContractError)
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
            immutable_files.publish_exact(path, pb._canonical_bytes(record),
                                     where="shared range", conflict_error=_pool.PoolContractError)
        except _pool.PoolContractError as exc:
            raise ResidencyPlanError(
                f"a different mover is already registered for shared range "
                f"{namespace[:12]}: {exc}") from None
        return record, True


__all__ = [
    "RESIDENCY_PLAN_SCHEMA_V1",
    "FilingOptions", "FiledWindow", "ConsumerWindow", "LegObservation", "WindowBudget",
    "prepare_window", "consumer_filing", "file_manifest_row",
    "resolve_stage_tier", "resolve_ram_tier", "reader_declaration",
    "prelaunch_capacity_refusal", "split_prelaunch_cuts", "residency_leg_cuts",
    "active_supersessions", "retired_consumers", "registry_cycle", "registry_root",
    "filed_consumers", "filed_consumer_candidates", "shared_movers", "shared_ranges_present",
    "inspection_paths", "inspection_names", "inspection_entries", "concluded_consumers",
    "prelaunch_stage_legs", "stage_leg_rows", "ram_leg_rows", "stage_source_resident",
    "retirement_presence", "filed_name",
    "ResidencyPlanError",
    "SEALED_AGAINST_FIELDS",
    "SHARED_MOVER_SCHEMA_V1",
    "SHARED_RANGE_SCHEMA_V1",
    "accepted",
    "accepted_plan_phase",
    "build_plan",
    "consumer_residency",
    "filed_mover_keys",
    "expected_landings",
    "filed_prelaunch_phases",
    "find_mover_leg",
    "freeze",
    "gang_prelaunch_demand",
    "landing_seconds",
    "lead_mover_row",
    "leads_for",
    "legs_over",
    "mover_announcement",
    "mover_keys",
    "plan_phase",
    "prelaunch_bound",
    "prelaunch_owned_gib",
    "prelaunch_peak_gib",
    "prelaunch_phase_names",
    "progress_contract",
    "ram_mover_keys",
    "read",
    "read_footprint",
    "read_shared_mover",
    "read_shared_range",
    "refill_horizon",
    "register_shared_range",
    "remaining",
    "retain_filed",
    "runahead_budget_gib",
    "runahead_step_gib",
    "sealed_progress_order",
    "share_namespace",
    "share_namespace_of",
    "shared_mover_path",
    "shared_range_path",
    "stage_mover_keys",
    "validate_plan",
]


@dataclass(frozen=True, kw_only=True)
class FilingOptions:
    ram: str = "auto"
    share: str = "auto"
    mover_readers: int = 4
    mover_mem_gb: int = 1
    mover_max_attempts: int = 3
    prefetch_depth_gib: int | None = None
    read_mb_s: int | None = None


@dataclass(frozen=True)
class FiledWindow:
    plan: Mapping[str, object]
    residency: Mapping[str, object]
    retired: tuple[str, ...]
    reused: bool


def resolve_stage_tier(queue, declared: str | None) -> dict[str, object]:
    """Which stage tier this submission's bytes land on, from what the fleet says.

    Discovered rather than configured, because the tiers are: ``tier_loop``
    announces what each box actually has, so a submitter that hard-coded a
    pool name would keep asking for a dataset after Rob moved it.  One stage
    tier is the unambiguous case and needs no flag; none is a refusal naming
    the loop that would have announced it; more than one is a refusal that
    lists them, because picking for the operator is picking which box the
    campaign reads from.
    """

    stages = [record for record in queue.tiers()
              if record.get("tier") == "stage" and not record.get("retired")]
    if declared is not None:
        for record in stages:
            if str(record.get("tier_id")) == declared:
                return record
        raise SubmissionRefused(
            f"pbrun: no live stage tier {declared!r}; the fleet announces "
            f"{sorted(str(r.get('tier_id')) for r in stages) or 'none'}")
    if not stages:
        raise SubmissionRefused(
            "pbrun: --residency stage needs a stage tier, and no box announces "
            "one.  A storage box mints it by running the tiers role "
            "(tier_loop.py); until it does, nothing can reserve stage capacity")
    if len(stages) > 1:
        raise SubmissionRefused(
            "pbrun: more than one stage tier is live; name one with "
            f"--residency-tier: {sorted(str(r.get('tier_id')) for r in stages)}")
    return stages[0]



def resolve_ram_tier(queue, stage_tier: Mapping[str, object],
                     ) -> dict[str, object] | None:
    """The ram tier that sits in front of this stage, from what the fleet says.

    Discovered rather than configured, because the tiers are: the promotion
    node runs on the box that owns the stage, so the ram tier that feeds the
    same consumers is that box's, and a ram tier on another host is another
    stage's leg.  None is the ordinary answer -- no tmpfs mounted, or a
    generation predating the policy -- and it seals no ram leg, exactly as a
    submission before the tier existed did.
    """

    host = str(stage_tier.get("host") or "")
    rams = [record for record in queue.tiers()
            if record.get("tier") == "ram" and not record.get("retired")
            and str(record.get("host") or "") == host]
    if not rams:
        return None
    if len(rams) > 1:
        raise SubmissionRefused(
            "pbrun: more than one ram tier is live on "
            f"{host or 'the stage host'}: the policy declares one mountpoint, "
            f"and the fleet announces {sorted(str(r.get('tier_id')) for r in rams)}")
    return rams[0]



def reader_declaration(options: FilingOptions) -> dict[str, int]:
    """What the submitter declares about the consumer's reading (#909).

    ``--residency-prefetch-depth-gib`` and ``--residency-read-mb-s``, as the
    residency plan's ``reader`` block: the tier prices the window from them
    instead of from the consumer's memory reservation and the tier's own fill
    offer.  Either may be absent; an empty declaration adds nothing to the
    plan.  Read with ``getattr`` because a deferred submission filed before
    #909 carries neither.
    """

    reader: dict[str, int] = {}
    depth = getattr(options, "prefetch_depth_gib", None)
    if depth is not None:
        if isinstance(depth, bool) or not isinstance(depth, int) or depth < 0:
            raise SubmissionRefused(
                "pbrun: --residency-prefetch-depth-gib must be a whole number "
                "of GiB, 0 or more")
        reader["prefetch_depth_bytes"] = int(depth) * storage_tiers.GIB
    rate = getattr(options, "read_mb_s", None)
    if rate is not None:
        if isinstance(rate, bool) or not isinstance(rate, int) or rate <= 0:
            raise SubmissionRefused("pbrun: --residency-read-mb-s must be a positive "
                             "whole MB/s")
        reader["read_mb_s"] = int(rate)
    return reader



def prelaunch_capacity_refusal(
    tier: Mapping[str, object], tier_id: str, bound: Mapping[str, int],
    prelaunch: object, manifest_sha256: str,
) -> str | None:
    """Why a declared prefix cannot fit this stage tier, or ``None`` (#1594).

    Compares the peak with the tier's minted capacity.  Headroom is not
    checked: a fit prefix with no free room waits.  Unknown capacity never
    refuses.
    """

    minted = storage_tiers.minted_tokens(tier).get(
        storage_tiers.capacity_kind_of(tier_id))
    if minted is None or bound["peak_gib"] <= minted:
        return None
    return (
        f"pbrun: prelaunch prefix {prelaunch} of "
        f"{manifest_sha256[:12]} needs peak "
        f"{bound['peak_gib']} GiB (retained "
        f"{bound['retained_gib']} GiB + suffix "
        f"{bound['suffix_gib']} GiB) on stage tier {tier_id}, "
        f"above the tier's minted capacity of {minted} GiB.  "
        f"Nothing was sealed or published.")



def split_prelaunch_cuts(
    ranges: Sequence[Mapping[str, object]],
    cuts: Sequence[list[tuple[int, int]]],
    prelaunch_names: frozenset[str] | set[str],
) -> tuple[list[list[tuple[int, int]]], list[list[tuple[int, int]]]]:
    """The declared prefix's cuts and the streaming suffix's, chosen by name.

    ``cuts`` holds one entry per range, and ``ranges`` omits an empty phase,
    so the manifest's declared count and the cut list can disagree: slicing
    by count would take a suffix phase's cuts into the prefix.  The names
    are what the manifest declared, so the split follows them (#1594).
    """

    if len(ranges) != len(cuts):
        raise SubmissionRefused("pbrun: prelaunch cuts and ranges disagree; "
                         "nothing was sealed or published.")
    prefix = [chunks for span, chunks in zip(ranges, cuts)
              if str(span["name"]) in prelaunch_names]
    suffix = [chunks for span, chunks in zip(ranges, cuts)
              if str(span["name"]) not in prelaunch_names]
    return (prefix, suffix)



def residency_leg_cuts(
    record: Mapping[str, object],
    *,
    leg: str,
    ranges: Sequence[Mapping[str, object]],
    read_entries: Sequence[Mapping[str, object]],
) -> list[list[tuple[int, int]]]:
    """Every phase's chunk ranges on one leg, in read order.

    The chunk size is the tier's announced sizing, never this box's: the pin
    when the loop mints one, else the same window-quarter derivation the
    loop announces with.  A tier that announces no sizing seals whole-phase
    pairs -- chunking is a sealing-time property, and an unchunkable phase
    keeps the shape it always had.

    Cuts fall only on the manifest's entry boundaries (#965).  A mover
    stages every entry its range touches, so a cut inside an entry made both
    neighbouring movers overrun their reservations and refuse
    ``residency_overran_reservation`` on every attempt.  Each chunk is
    therefore whole entries, reserved at their bytes; an entry larger than
    the chunk is a chunk of its own.  An entry larger than the window the
    tier announces can never be admitted, so it refuses here, by name,
    before anything is sealed.  A record that announces no window (the
    stage's, which carries only its host's chunk) has nothing to refuse on.
    """

    def whole_gib(value: object) -> int | None:
        if isinstance(value, int) and not isinstance(value, bool) and value > 0:
            return value
        return None

    announced_chunk = whole_gib(record.get("promotion_chunk_gib"))
    announced_window = whole_gib(record.get("window_gib"))
    chunk_gib = announced_chunk
    if chunk_gib is None and announced_window is not None:
        chunk_gib = storage_tiers.promotion_chunk_gib_for_window(announced_window)
    spans = [(int(span["start_bytes"]), int(span["end_bytes"])) for span in ranges]
    if chunk_gib is None:
        return [[span] for span in spans]
    sizes = [int(entry.get("bytes", 0) or 0) for entry in read_entries]
    window_bytes = (None if announced_window is None
                    else announced_window * storage_tiers.GIB)
    cuts: list[list[tuple[int, int]]] = []
    for span, (start, end) in zip(ranges, spans):
        try:
            cuts.append(manifest_layout.split_range_into_chunks(
                start, end, chunk_gib * storage_tiers.GIB,
                entry_bytes=sizes, window_bytes=window_bytes))
        except manifest_layout.EntryExceedsWindow as exc:
            entry = read_entries[exc.entry_index]
            raise SubmissionRefused(
                f"pbrun: residency_entry_exceeds_window: phase "
                f"{span['name']!r} reads {entry.get('path')!r} at offset "
                f"{entry.get('offset')}, {exc.end_bytes - exc.start_bytes} "
                f"bytes, which is larger than the {announced_window} GiB "
                f"window of the {leg} tier {record.get('tier_id')}.  A mover "
                f"stages whole entries, so no chunk can hold this one and "
                f"its mover could never be admitted.  Nothing was sealed or "
                f"published.") from None
    return cuts



def prepare_window(
    template: Mapping[str, object],
    *,
    consumer_action_key: str,
    tier: Mapping[str, object],
    options: FilingOptions,
    queue,
    cas,
    movement_receipts: Sequence[Mapping[str, object]] | None = None,
    publication: RowPublication,
) -> dict[str, object]:
    """Seal every movement and egress node this submission will ever have.

    All of them, now, before anything is published: an action key is a hash of
    an action body, so the only way a coordinator can publish the window's
    later phases is for the submitter to have sealed them and written the rows
    down.  That is also what freezes the decomposition -- a restart republishes
    these same children rather than cutting a new partition of the read order.

    The ranges are the manifest's own phases.  Nothing here chooses a boundary:
    a cut through an entry would hand a mover more bytes than its tokens
    reserved, and ``manifest_phase_ranges`` refuses a phase table that does not
    describe its own manifest.  Chunks inside a phase are cut at the same
    entries' boundaries (``residency_leg_cuts``, #965).
    """

    manifest_input = template["params"].get("data_manifest")   # type: ignore[union-attr]
    if not isinstance(manifest_input, Mapping):
        raise SubmissionRefused(
            "pbrun: --residency stage needs --data-manifest: the byte ranges a "
            "mover stages are read-order offsets into that list, and there is "
            "no other way to say which bytes a range means")
    entry = manifest_input["input"]
    manifest, _ = manifest_codec.read_data_manifest(cas.input_path(entry))
    ranges = manifest_layout.manifest_phase_ranges(manifest)
    if not ranges:
        raise SubmissionRefused(
            "pbrun: --residency stage needs a manifest that declares its read "
            "order in phases; this one declares none, so there is no boundary "
            "to stage up to that is not invented here")
    # Read the declared prelaunch-resident prefix (#1594).  Mark its phases
    # on the sealed plan.  Refuse its peak when it exceeds the tier.  A v1
    # manifest reaches this reader unchecked by ``core``.  Refuse a
    # non-boolean or a non-prefix declaration here.
    try:
        prelaunch = manifest_layout.manifest_prelaunch_phases(manifest)
    except ValueError as exc:
        raise SubmissionRefused(
            f"pbrun: data manifest {str(entry['sha256'])[:12]} declares an "
            f"unusable prelaunch prefix: {exc}; nothing was sealed or "
            f"published.") from None
    prelaunch_names = frozenset(prelaunch)
    tier_id = str(tier["tier_id"])
    # A consumer that is already staged keeps the window it was frozen with.
    # Receipts price a *new* window; they must never repartition a frozen one.
    # The demand now depends on which receipts exist at submission, so a
    # resubmission of the same consumer after one more mover filed would seal
    # different mover keys and a different plan -- and ``freeze``
    # is first-writer, so it would refuse the whole submission with both bodies
    # in hand.  The frozen plan is the answer to that question, already agreed.
    #
    # The one exception is a plan a withdrawal marked superseded (#708): the
    # request is new, the old window must not be resurrected at the price it
    # was cancelled for, and the only supported reprice is this sealing path
    # run again -- after the old window's work has ended.
    #
    # The reading and the filing are one captured identity, never a key read
    # and a later "whatever is filed": a replacement can land between an
    # advisory check and a reap, and a caller that never saw it must not
    # archive it.  ``reap`` rechecks the exact ``(plan, filing)`` inside the
    # consumer's lock; when it removes nothing, this loop reads what is
    # actually filed now and decides again from that, rather than treating a
    # refused reap as "the old filing is gone" (#708 review).
    refusals: list[Exception] = []

    def filed_now():
        seen = len(refusals)
        plan, identity = read_filed(
            queue, consumer_action_key, on_unreadable=refusals.append)
        if plan is None and len(refusals) > seen:
            # A plan body this reader refuses, or a stat that failed: unknown
            # state, and a fresh seal over it would be a guess (#708 review).
            raise SubmissionRefused(
                f"pbrun: the residency plan filed for "
                f"{consumer_action_key[:12]} cannot be read "
                f"({refusals[-1]!r}); refusing to reuse or replace it. An "
                f"operator must resolve it under {SUPERSEDED}/.")
        return plan, identity

    frozen, filing = filed_now()
    for _attempt in range(3):
        if frozen is None:
            break
        marker = superseded(queue, frozen)
        if marker is None:
            return {
                "plan": frozen,
                "residency": {
                    "schema": _pool.RESIDENCY_SCHEMA_V1,
                    "manifest_sha256": str(frozen["manifest_sha256"]),
                    "manifest_bytes": int(frozen["manifest_bytes"]),
                    "tier_id": str(frozen["tier_id"]),
                    "leads": leads_for(frozen),
                },
                "reused_frozen_plan": True,
            }
        if marker.get("unreadable"):
            # Unknown retirement is not "not retired": without a readable
            # marker nobody can say which window the operator stopped, so a
            # fresh seal would be a guess and is refused (#708 review).
            raise SubmissionRefused(
                f"pbrun: the supersession marker for {consumer_action_key[:12]} "
                f"is unreadable ({marker.get('error') or 'unknown state'}); "
                f"refusing to reuse or replace its frozen plan. An operator "
                f"must resolve the marker under {SUPERSEDED}/.")
        # A withdrawal marked this window superseded (#708).  Rows are sealed
        # with the resources their keys hash, so the only supported way to
        # request the range again at today's price is a fresh plan through
        # this same sealing path -- and only once the old window's ownership
        # has ended.  A live consumer or a claimed child is not replaced from
        # under itself: the old plan is not rewritten, and this refuses.
        safe, why = handoff_safe(queue, consumer_action_key, frozen)
        if not safe:
            raise SubmissionRefused(
                f"pbrun: the residency plan for {consumer_action_key[:12]} was "
                f"superseded ({marker.get('reason') or 'withdrawn'}), and its "
                f"work has not ended: {why}.  Withdraw the consumer and let "
                f"the tiers loop reap the old window, then resubmit; sealed "
                f"rows and a frozen plan are never rewritten in place.")
        reaped = reap(
            queue, consumer_action_key, reason="superseded-reseal",
            plan=frozen, filing=filing)
        if reaped is not None:
            frozen = None
            break
        # The locked reap removed nothing: the handoff went live, or another
        # handoff changed the filing, after the advisory check above.  Read
        # the filing that actually stands and decide again from it; a stable
        # refusal follows on the next pass.
        frozen, filing = filed_now()
    else:
        raise SubmissionRefused(
            f"pbrun: the residency plan for {consumer_action_key[:12]} kept "
            f"changing while this submission decided against it; nothing was "
            f"sealed and nothing published. Retry the submission.")
    stage_root = str(tier.get("mountpoint") or "")
    if not stage_root.startswith("/"):
        raise SubmissionRefused(
            f"pbrun: stage tier {tier_id} announces no mountpoint to write into")
    digest = str(entry["sha256"])
    tags = [str(tier["host"])]
    mover_python, mover_tool, egress_tool = movement_actions.movement_tools(tier)
    announced = mover_announcement(tier)
    pool_root = str(publication.queue_root)
    # The ram leg, when a ram tier sits in front of this stage (#640): a
    # promotion node and an egress node per phase, sealed here with the rest
    # of the plan, because an action key is a hash and the coordinator cannot
    # publish children the submitter never sealed.  ``auto`` is the default so
    # the tier turns on with the mount; ``off`` is the A/B's other arm.
    ram_tier = None
    if str(getattr(options, "ram", "auto") or "auto") == "auto":
        ram_tier = resolve_ram_tier(queue, tier)
    ram_tier_id = None
    ram_root = ""
    ram_python = ram_tool = ram_egress_tool = ""
    if ram_tier is not None:
        ram_tier_id = str(ram_tier["tier_id"])
        ram_identity = ram_tier.get("pool_identity")
        if not isinstance(ram_identity, Mapping):
            ram_identity = None
        ram_root = str(ram_tier.get("mountpoint") or "")
        if not ram_root.startswith("/"):
            raise SubmissionRefused(
                f"pbrun: ram tier {ram_tier_id} announces no mountpoint to "
                f"promote into")
        ram_python, ram_tool, ram_egress_tool = movement_actions.movement_tools(
            ram_tier, mover=movement_actions.RAM_PROMOTE_SCRIPT)

    # Every chunk of both legs is cut here, before a single node is sealed or
    # published: an entry that no chunk of a tier can hold refuses the whole
    # submission now, rather than after half its nodes reached the CAS.
    read_entries = manifest_layout.manifest_read_entries(manifest)
    stage_cuts = residency_leg_cuts(tier, leg="stage", ranges=ranges,
                                    read_entries=read_entries)
    ram_cuts = (None if ram_tier is None else
                residency_leg_cuts(ram_tier, leg="ram", ranges=ranges,
                                   read_entries=read_entries))
    if prelaunch:
        # Fit the declared prefix peak to the minted tier capacity (#1594).
        # The sizes are the same chunk demands the sealed legs will carry,
        # cut at the same boundaries.  This refuses what the plan demands.
        # Headroom is not checked: a fit prefix with no free room waits.
        # Unknown capacity never refuses.
        prefix_cuts, suffix_cuts = split_prelaunch_cuts(
            ranges, stage_cuts, prelaunch_names)

        def _cut_gib(cuts: list[list[tuple[int, int]]]) -> list[int]:
            return [sum(storage_tiers.stage_tokens_for_bytes(cend - cstart)
                        for cstart, cend in chunks) for chunks in cuts]

        bound = prelaunch_peak_gib(
            _cut_gib(prefix_cuts), _cut_gib(suffix_cuts))
        refusal = prelaunch_capacity_refusal(
            tier, tier_id, bound, prelaunch, str(entry["sha256"]))
        if refusal is not None:
            raise SubmissionRefused(refusal)

    # One read of the live receipts for the whole window: every mover in it has
    # the same structure and reads the same pool, so they price alike, and a
    # per-phase read would give two phases of one plan different demands
    # because a mover finished between them.
    readers = int(options.mover_readers)
    # The consumer's own reading, as its submitter declares it (#909).  A
    # frozen plan returned above keeps the declaration it was frozen with.
    reader = reader_declaration(options)
    # A logical freeze supplies one invocation-local observation for all its
    # siblings. None retains the standalone submission's fresh observation;
    # an explicitly empty snapshot must not trigger another live census.
    # Mover receipts and egress receipts share the movers directory, and one
    # pass reads both (#1021): the movers price the copy, the egresses price
    # the release (``movement_actions.egress_price``).  A caller's snapshot
    # that holds no egress receipt seals every egress as before.
    observed = (queue.move_records(schemas=(_pool.POOL_MOVE_SCHEMA_V1,
                                            _pool.POOL_EGRESS_SCHEMA_V1))
                if movement_receipts is None else movement_receipts)
    receipts = [record for record in observed
                if not (isinstance(record, Mapping) and record.get("schema")
                        == _pool.POOL_EGRESS_SCHEMA_V1)]
    # Which pools the receipts must have measured to price this window
    # (#611): the tier's current identity, as the tier loop announced it.
    # ``None`` -- a tier last announced by an older generation -- prices off
    # every usable receipt, exactly as before.
    tier_identity = tier.get("pool_identity")
    if not isinstance(tier_identity, Mapping):
        tier_identity = None
    priced = storage_tiers.mover_demand_from_receipts(
        receipts, tier_id=tier_id, readers=readers,
        fallback_mem_gb=int(options.mover_mem_gb),
        pool_identity=tier_identity)
    # The pool bandwidth a mover reserves, once anything has measured it.  It
    # is what makes concurrency a ledger decision rather than an accident: the
    # tier mints what the disks delivered plus one probe mover's worth, and a
    # mover that reserves nothing can never be rationed against another.  One
    # copy's measured rate (#909): the slowest landing of this manifest's
    # latest window onto this tier when that window holds two copies or more,
    # else the median single-reader share of the tier's latest window (#958),
    # never the tier's whole offer.  The tier's *current* offer caps it: a fresh seal
    # asks no more than admission will honour on this cycle (#708), and with
    # nothing measured the offer is the stated bound, named in demand_source.
    fill_price = storage_tiers.mover_fill_price(
        receipts, tier_id=tier_id, pool_identity=tier_identity,
        manifest_sha256=digest)
    fill, offered_fill, fill_basis = storage_tiers.current_fill_offer(tier, fill_price["mb_s"])
    mover_retry_policy = {
        "max_attempts": int(options.mover_max_attempts),
        # True by construction, not by the operator's say-so: ``stage_move``
        # copies to a mover-keyed ``.<name>.partial``, verifies the digest,
        # then ``os.replace``s, and files its fragment only for entries it
        # verified.
        "retry_safe": True,
    }
    # A stage mover's stall grace (#1010), from what the tier has measured
    # (``movement_actions.mover_progress_policy``): its next landing at the
    # slowest measured landing of this manifest on this tier, capped by the
    # fill it reserves, plus the time a report takes to reach the stall
    # check.  Measured means a landing of this manifest only (``landing`` on
    # the price, whatever basis sealed the fill): a median share of other
    # manifests' copies is not a landing of this one, while one copy is --
    # a window of one prices no fill seal (#958) but still bounds how long a
    # copy of this manifest has gone between landings.  Time the pool is
    # the bottleneck is not priced here: the worker credits it on its own
    # sample of the pool's members (``core.POOL_CONTENTION_PARAM``), so a
    # policy is sealed only with the members the storage role announced.
    # With either unmeasured the mover is sealed as before, with no stall
    # grace, and ``demand_source.mover_progress`` says which was missing.
    landing_bytes_per_s: float | None = None
    landing = fill_price.get("landing")
    if isinstance(landing, Mapping) and landing.get("mb_s"):
        slowest = int(landing["mb_s"])                     # type: ignore[arg-type]
        if fill is not None:
            slowest = min(slowest, int(fill))
        landing_bytes_per_s = float(slowest * storage_tiers.MB)
    source_members = tier.get("source_members")
    pool_members = ([str(member) for member in source_members if member]
                    if isinstance(source_members, Sequence)
                    and not isinstance(source_members, (str, bytes)) else [])
    # How many movers were copying on the tier while the pricing window
    # landed, at most: the rate above was measured under that load, and a
    # reviewer comparing it with the load now needs both numbers.
    window_concurrency = None
    if isinstance(landing, Mapping):
        counts = [record.get(storage_tiers.MOVER_CONCURRENCY_FIELD)
                  for record in receipts
                  if isinstance(record, Mapping)
                  and str(record.get("manifest_sha256") or "") == digest
                  and str(record.get("consumer_action_key") or "")
                  == str(landing.get("window_consumer") or "")]
        counts = [int(count) for count in counts
                  if isinstance(count, int) and not isinstance(count, bool)]
        window_concurrency = max(counts) if counts else None
    entry_sizes = [int(read_entry.get("bytes", 0) or 0)
                   for read_entry in read_entries]
    entry_starts: list[int] = []
    position = 0
    for size in entry_sizes:
        entry_starts.append(position)
        position += size
    mover_progress: dict[str, object] = {}
    # A stage egress's stall grace (#1021), from the egresses this stage has
    # receipted (``movement_actions.egress_progress_policy``): the census,
    # this chunk's unlinks and the settle at the slowest measured terms, plus
    # the time a report takes to reach the stall check.  Sealed only with the
    # pool members the worker judges contention by, as a mover's is: the
    # census reads the queue on the source pool, and time that pool is the
    # bottleneck is credited, not priced.  Unmeasured, the egress is sealed
    # as before, with no stall grace, and ``demand_source.egress_progress``
    # says which term was missing.
    egress_terms = movement_actions.egress_price(observed, stage_root=stage_root)
    egress_progress: dict[str, object] = {}

    def chunk_entry_bytes(cstart: int, cend: int) -> list[int]:
        """The sizes of the read-order entries a chunk stages.

        Chunks are cut on entry boundaries (#965), so this is exactly the
        window ``stage_move`` copies (``manifest_layout.entries_between``).
        """

        low = bisect.bisect_right(entry_starts, cstart) - 1
        return [entry_sizes[index]
                for index in range(max(0, low), len(entry_sizes))
                if entry_starts[index] < cend
                and entry_starts[index] + entry_sizes[index] > cstart]

    phases: list[dict[str, object]] = []
    for ordinal, span in enumerate(ranges):
        start, end = int(span["start_bytes"]), int(span["end_bytes"])
        # The stage leg is cut into chunks (#675) the way the ram leg is
        # (#673): one movement node plus one egress node per chunk, in read
        # order, so the SSD refills as it frees instead of sawtoothing a
        # whole phase at a time.  The cuts were made above, at entry
        # boundaries, before anything was sealed (#965).  A phase that fits
        # in one chunk seals today's whole-phase pair, and a tier that
        # announces no sizing seals it too.
        chunk_ranges = stage_cuts[ordinal]

        def seal_stage_mover(cstart: int, cend: int, csuffix: str,
                             namespace: str) -> tuple[dict, dict]:
            """Seal one chunk's mover, filing its fragment under ``namespace``.

            ``namespace`` is the consumer's own key for a per-consumer mover,
            and the range's :func:`share_namespace` for a
            shared one (#1026): no one consumer's fragment directory may hold
            bytes the others read, or that consumer's death would retire
            them.  Returns the row and its stall-grace derivation.
            """

            chunk_demand = storage_tiers.residency_demand(
                tier_id=tier_id, range_start_bytes=cstart,
                range_end_bytes=cend, fill_mb_s_pool_side=fill)
            # Measured, not habitual, and above all *present*: a row without a
            # ``cpu`` key is read by ``adaptive_cpu`` as unknown CPU use and
            # refused whenever the box already holds anything
            # (``unbounded_cpu_not_exclusive``), which is what ran the first live
            # window one large mover at a time with 17 idle worker loops (#607,
            # #603).  Both numbers come off ``pb-queue/movers/`` receipts when
            # there are any, and ``demand_source`` on the plan says which receipts
            # were read and which field fell back to a declared bound.
            chunk_demand["cpu"] = int(priced["cpu"])
            chunk_demand["mem_gb"] = int(priced["mem_gb"])
            progress, derivation = movement_actions.mover_progress_policy(
                chunk_entry_bytes(cstart, cend),
                landing_bytes_per_s=landing_bytes_per_s)
            contention: dict[str, object] | None = None
            if progress is not None and not pool_members:
                # No members to judge the pool by: a pacer hold would be
                # charged to the copy, so no grace at all, as before.
                progress = None
                derivation = {**derivation, "basis": "unmeasured",
                              "unmeasured": "pool members", "grace_s": None}
            elif progress is not None:
                contention = movement_actions.pool_contention_spec(
                    members=pool_members, stage_root=stage_root,
                    priced_bytes_per_s=float(landing_bytes_per_s))  # type: ignore[arg-type]
            elif derivation["basis"] == "unmeasured":
                derivation = {**derivation, "unmeasured": "landing rate"}
            derivation = {**derivation,
                          "landing_window_movers": window_concurrency,
                          "pool_contention": contention}
            chunk_mover = movement_actions.seal_movement_action(
                template,
                container_owner_fn=movement_actions.submission_container_owner,
                command=[mover_python, mover_tool,
                         "--pool-root", pool_root,
                         "--cas-root", str(publication.cas_root),
                         "--consumer-action-key", namespace,
                         "--tier-id", tier_id,
                         "--stage-root", stage_root,
                         "--manifest-sha256", digest,
                         "--range-start-bytes", str(cstart),
                         "--range-end-bytes", str(cend),
                         # Stated on the command, so the width the row reserves and
                         # the width the copy runs at cannot drift apart.
                         "--readers", str(readers)]
                        + ([] if fill is None else
                           # Carried so the receipt can say what the ledger had
                           # promised this copy; a later cycle compares that with
                           # what the copy achieved, and a shortfall is the
                           # measured ceiling on how many movers the pool feeds.
                           ["--fill-mb-s-pool-side", str(fill)]),
                demand=chunk_demand,
                # A progress-governed action is placed only on a worker
                # that enforces the contract and exports the helper
                # environment the mover reports through.
                tags=[*tags, *contracts.declared_contract_tags(contracts.Publication(
                    progress=progress, pool_contention=progress is not None))],
                retry_policy=mover_retry_policy,
                log_name=f"stage-move-{ordinal:04d}-{span['name']}{csuffix}.log",
                extra_params=(None if progress is None
                              else {pb.PROGRESS_PARAM: progress,
                                    pb.POOL_CONTENTION_PARAM: contention}))
            cas.publish_action_request(chunk_mover)
            mover_row = {
                **publication.row(chunk_mover, queue=queue, max_attempts=int(options.mover_max_attempts), retry_safe=True),
                # The row, not only the sealed body.  ``residency_pin_holds``
                # reads the *queue record* to decide whether a concluding
                # mover keeps its tier tokens, so a row without this block
                # ends ``executed`` and hands its tokens straight back -- the
                # ledger reads its full supply free while 34 GB sit on the
                # stage, which is the one invariant #583 rests on.  The
                # consumer's block names leads; a mover's names the range it
                # makes resident, which is what the pin is checked against.
                "residency": {
                    "schema": _pool.RESIDENCY_SCHEMA_V1,
                    "manifest_sha256": digest,
                    "manifest_bytes": int(entry["bytes"]),
                    "tier_id": tier_id,
                    "range_start_bytes": cstart,
                    "range_end_bytes": cend,
                },
            }
            return mover_row, derivation

        def seal_stage_chunk(cstart: int, cend: int,
                             csuffix: str) -> tuple[dict, dict]:
            """One chunk's mover row and this consumer's egress row for it.

            With sharing on (the default, #1026) the mover is the one every
            consumer of this exact range names: the first submitter of the
            range seals and registers it, and every later one puts that
            registered row in its own plan.  A mover's key is the hash of a
            body that carries its submission's checkout, pricing and log
            name, so a second submitter cannot derive the same key; the
            registration is how two submitters agree on it
            (:func:`register_shared_range`).  A registration
            sealed against another tier announcement -- another runtime
            generation's mover -- is reused only while its mover is live;
            otherwise this submission seals the range's mover afresh and
            replaces it.  The egress is
            always this consumer's own: for a shared range it drops this
            consumer's interest, and the last interest to go deletes.
            """

            if str(getattr(options, "share", "auto") or "auto") == "auto":
                namespace = share_namespace(
                    digest, tier_id, cstart, cend)
                try:
                    record, sealed_here = register_shared_range(
                        queue, manifest_sha256=digest, tier_id=tier_id,
                        start=cstart, end=cend,
                        seal=lambda: seal_stage_mover(
                            cstart, cend, csuffix, namespace),
                        registered_by=consumer_action_key,
                        # Sealed against the announcement
                        # ``movement_tools`` read ``mover_python`` and
                        # ``mover_tool`` from.  A registration sealed
                        # against another one is reused only while its
                        # mover is live.
                        sealed_against=announced)
                except (ResidencyPlanError, OSError,
                        _pool.record_store.PoolContractError) as exc:
                    raise SubmissionRefused(
                        f"pbrun: the shared range [{cstart}, {cend}) of "
                        f"{digest[:12]} on {tier_id} cannot be registered "
                        f"or read ({exc}); nothing was published. "
                        f"--residency-share off stages it per consumer."
                    ) from None
                mover_row = dict(record["mover_row"])        # type: ignore[arg-type]
                mover_key = str(record["mover_action_key"])
                derivation = {
                    **dict(record.get("derivation") or {}),  # type: ignore[arg-type]
                    "share_namespace": namespace,
                    "registered_by": str(record.get("registered_by") or ""),
                    "sealed_here": bool(sealed_here)}
            else:
                mover_row, derivation = seal_stage_mover(
                    cstart, cend, csuffix, consumer_action_key)
                mover_key = str(mover_row["action_key"])
            mover_progress[mover_key] = derivation
            egress_policy, egress_derivation = (
                movement_actions.egress_progress_policy(
                    chunk_entry_bytes(cstart, cend), price=egress_terms))
            egress_contention: dict[str, object] | None = None
            if egress_policy is not None and not pool_members:
                # No members to judge the pool by: a census slowed by
                # another reader would be charged to the egress, so no
                # grace at all, as before.
                egress_policy = None
                egress_derivation = {**egress_derivation, "basis": "unmeasured",
                                     "unmeasured": "pool members",
                                     "grace_s": None}
            elif egress_policy is not None:
                egress_contention = movement_actions.pool_contention_spec(
                    members=pool_members, stage_root=stage_root,
                    priced_bytes_per_s=float(
                        egress_derivation["priced_bytes_per_s"]))  # type: ignore[arg-type]
            else:
                egress_derivation = {**egress_derivation,
                                     "unmeasured": "egress receipts"}
            egress_derivation = {**egress_derivation,
                                 "mover_action_key": mover_key,
                                 "pool_contention": egress_contention}
            chunk_egress = movement_actions.seal_movement_action(
                template,
                container_owner_fn=movement_actions.submission_container_owner,
                command=[mover_python, egress_tool,
                         "--pool-root", pool_root,
                         "--mover-action-key", mover_key,
                         "--consumer-action-key", consumer_action_key,
                         "--stage-root", stage_root],
                # No tier demand: an egress *returns* capacity, and one that had to
                # reserve some before it could give any back would deadlock exactly
                # when the stage is full -- which is the only moment it matters.
                # CPU and memory it must still declare, and bounded: a row without
                # a ``cpu`` key is *unknown* CPU use to ``adaptive_cpu``, refused
                # whenever the box holds anything (#603, #607) -- and the box an
                # egress runs on is the stage's own file server, whose resident
                # loops mean it always holds something.  A release that cannot
                # claim there deadlocks the tier through the same door the comment
                # above closes: the concluding movers pin their ranges' tokens, the
                # egress is the only node that returns them, and one waiting for an
                # empty box waits forever.  One CPU is a declared bound, the width
                # of the single-process unlink-and-record an egress is -- not a
                # measurement, because an egress files no receipts of its own, and
                # pricing it off the movers' copy receipts would measure the wrong
                # node entirely (the #655 lesson).
                demand={"cpu": 1, "mem_gb": 1},
                # A progress-governed egress is placed only on a worker that
                # enforces the contract, credits the pool on its own sample,
                # and never credits the egress's own hold of the stage's
                # ownership lock as a wait (``contracts.EGRESS_PROGRESS_TAG``).
                tags=[*tags, *contracts.declared_contract_tags(contracts.Publication(
                    progress=egress_policy, pool_contention=egress_policy is not None,
                    egress=egress_policy is not None))],
                # The egress's retry policy is a movement node's, as the
                # mover's is (#950): with the consumer's single attempt, one
                # transient unlink error left the range's bytes holding their
                # tokens until pressure eviction.  An egress's second attempt
                # finds released what the first released.
                retry_policy=mover_retry_policy,
                log_name=f"stage-release-{ordinal:04d}-{span['name']}{csuffix}.log",
                extra_params=(None if egress_policy is None
                              else {pb.PROGRESS_PARAM: egress_policy,
                                    pb.POOL_CONTENTION_PARAM: egress_contention}))
            cas.publish_action_request(chunk_egress)
            egress_progress[str(chunk_egress["action_key"])] = egress_derivation
            # No block on the egress: it reserves no tier capacity, and
            # ``validate_residency`` refuses a range whose stage demand is
            # below the range's own floor.  An egress finds its mover by
            # ``--mover-action-key``, not by a range of its own.
            return mover_row, publication.row(
                chunk_egress,
                queue=queue,
                max_attempts=int(options.mover_max_attempts),
                retry_safe=True,
            )

        stage_chunks = None
        stage_mover_row: dict[str, object] | None = None
        stage_egress_row: dict[str, object] | None = None
        if len(chunk_ranges) == 1:
            stage_mover_row, stage_egress_row = seal_stage_chunk(start, end, "")
        else:
            stage_chunks = []
            for cindex, (cstart, cend) in enumerate(chunk_ranges):
                chunk_mover_row, chunk_egress_row = seal_stage_chunk(
                    cstart, cend, f"-c{cindex:02d}")
                stage_chunks.append({
                    "chunk_index": cindex,
                    "start_bytes": cstart, "end_bytes": cend,
                    "stage_gib": storage_tiers.stage_tokens_for_bytes(
                        cend - cstart),
                    "mover_row": chunk_mover_row,
                    "egress_row": chunk_egress_row,
                })
        ram_mover_row = None
        ram_egress_row = None
        ram_chunks = None
        if ram_tier_id is not None:
            # The withdrawn #639 part 3's plumbing, aimed at the right actor:
            # one occupancy leg per movement node, ``ram_gib`` on the ram tier,
            # priced off the promotion receipts exactly the way a stage
            # mover's demand is priced off its own.
            #
            # The leg is cut into chunks (#673): one promotion node plus one
            # egress node per chunk, in read order, so the tmpfs refills as
            # it frees instead of sawtoothing a whole phase at a time.  The
            # cuts were made above, at entry boundaries, before anything was
            # sealed (#965).  A phase that fits in one chunk seals today's
            # whole-phase pair, and a tier that announces no sizing seals it
            # too.
            assert ram_cuts is not None
            chunk_ranges = ram_cuts[ordinal]

            def seal_ram_chunk(cstart: int, cend: int,
                               csuffix: str) -> tuple[dict, dict]:
                ram_demand = storage_tiers.residency_demand(
                    tier_id=ram_tier_id, range_start_bytes=cstart,
                    range_end_bytes=cend)
                ram_priced = storage_tiers.mover_demand_from_receipts(
                    receipts, tier_id=ram_tier_id, readers=readers,
                    fallback_mem_gb=int(options.mover_mem_gb),
                    pool_identity=ram_identity)
                ram_demand["cpu"] = int(ram_priced["cpu"])
                # The runtime term only bounds the copier's own buffers and
                # metadata.  The destination is a tmpfs, whose pages stay
                # charged to the writing cgroup (writeback never reclaims
                # them), so the row must reserve the range it will write as
                # well -- otherwise the promotion OOMs at its own cap partway
                # through, which is how four live 4-11 GiB promotions died at
                # exactly 1 GiB on 2026-09-19.  ``ram_gib`` for the range's
                # retention on the tier is untouched; this only makes the
                # action's memory demand honest.
                ram_demand["mem_gb"] = storage_tiers.ram_promotion_mem_gb(
                    runtime_mem_gb=int(ram_priced["mem_gb"]),
                    range_bytes=cend - cstart)
                ram_mover = movement_actions.seal_movement_action(
                    template,
                    container_owner_fn=movement_actions.submission_container_owner,
                    command=[ram_python, ram_tool,
                             "--pool-root", pool_root,
                             "--cas-root", str(publication.cas_root),
                             "--consumer-action-key", consumer_action_key,
                             "--tier-id", ram_tier_id,
                             "--ram-root", ram_root,
                             "--source-stage-root", stage_root,
                             "--manifest-sha256", digest,
                             "--range-start-bytes", str(cstart),
                             "--range-end-bytes", str(cend),
                             "--readers", str(readers)],
                    demand=ram_demand, tags=tags,
                    retry_policy=mover_retry_policy,
                    log_name=f"ram-promote-{ordinal:04d}-{span['name']}{csuffix}.log")
                ram_egress = movement_actions.seal_movement_action(
                    template,
                    container_owner_fn=movement_actions.submission_container_owner,
                    command=[ram_python, ram_egress_tool,
                             "--pool-root", pool_root,
                             "--mover-action-key", str(ram_mover["action_key"]),
                             "--consumer-action-key", consumer_action_key,
                             "--stage-root", ram_root],
                    # No tier demand, for the same reason as the stage's egress,
                    # and one declared CPU for the same reason as its cpu: a
                    # tmpfs promotion's release runs on the same never-empty file
                    # server, and unknown CPU use would refuse to run beside the
                    # loops that make it never-empty.
                    demand={"cpu": 1, "mem_gb": 1}, tags=tags,
                    retry_policy=mover_retry_policy,
                    log_name=f"ram-release-{ordinal:04d}-{span['name']}{csuffix}.log")
                cas.publish_action_request(ram_mover)
                cas.publish_action_request(ram_egress)
                mover_row = {
                    **publication.row(ram_mover, queue=queue, max_attempts=int(options.mover_max_attempts), retry_safe=True),
                    # The pin the ram window and the pin check read: a promotion
                    # row without it releases its occupancy the moment it
                    # finishes -- bytes on a roof-limited tmpfs that no token
                    # stands for are ENOSPC waiting to happen (#640).
                    "residency": {
                        "schema": _pool.RESIDENCY_SCHEMA_V1,
                        "manifest_sha256": digest,
                        "manifest_bytes": int(entry["bytes"]),
                        "tier_id": ram_tier_id,
                        "range_start_bytes": cstart,
                        "range_end_bytes": cend,
                    },
                }
                return mover_row, publication.row(
                    ram_egress,
                    queue=queue,
                    max_attempts=int(options.mover_max_attempts),
                    retry_safe=True,
                )

            if len(chunk_ranges) == 1:
                ram_mover_row, ram_egress_row = seal_ram_chunk(
                    start, end, "")
            else:
                ram_chunks = []
                for cindex, (cstart, cend) in enumerate(chunk_ranges):
                    mover_row, egress_row = seal_ram_chunk(
                        cstart, cend, f"-c{cindex:02d}")
                    ram_chunks.append({
                        "chunk_index": cindex,
                        "start_bytes": cstart, "end_bytes": cend,
                        "stage_gib": storage_tiers.stage_tokens_for_bytes(
                            cend - cstart),
                        "ram_mover_row": mover_row,
                        "ram_egress_row": egress_row,
                    })
        phase_record: dict[str, object] = {
            "name": str(span["name"]),
            "start_bytes": start, "end_bytes": end,
            "stage_gib": storage_tiers.stage_tokens_for_bytes(end - start),
        }
        if stage_chunks is not None:
            phase_record["stage_chunks"] = stage_chunks
        else:
            assert stage_mover_row is not None and stage_egress_row is not None
            phase_record["mover_row"] = stage_mover_row
            # No block on the egress: it reserves no tier capacity, and
            # ``validate_residency`` refuses a range whose stage demand is
            # below the range's own floor.  An egress finds its mover by
            # ``--mover-action-key``, not by a range of its own.
            phase_record["egress_row"] = stage_egress_row
        if ram_mover_row is not None:
            phase_record["ram_mover_row"] = ram_mover_row
            phase_record["ram_egress_row"] = ram_egress_row
        if ram_chunks is not None:
            phase_record["ram_chunks"] = ram_chunks
        # Carry the declared prefix on the frozen plan (#1594).  Discovery,
        # admission and reuse derive leads from ``leads_for(plan)`` with no
        # manifest in hand.  Only declared phases carry the key.  Undeclared
        # plans stay byte-identical.
        if str(span["name"]) in prelaunch_names:
            phase_record["resident_before_launch"] = True
        phases.append(phase_record)
    plan = build_plan(
        consumer_action_key=consumer_action_key, tier_id=tier_id,
        stage_root=stage_root, manifest_sha256=digest,
        manifest_bytes=int(entry["bytes"]), phases=phases,
        ram_tier_id=ram_tier_id, reader=reader,
        # Which receipts priced these movers' cpu and mem_gb, so a demand in
        # the queue traces back to a measurement rather than to a habit.  On
        # the plan, not on a row: ``tier_loop`` publishes a row as
        # ``queue.publish(**row)``, whose parameters are a closed set.  The
        # fill entry names the basis: which tier offer capped it, or that
        # only receipts priced it.
        demand_source={**priced["demand_source"],
                       "fill_mb_s_pool_side": fill,
                       "fill": fill_basis,
                       "fill_measured": dict(fill_price),
                       "tier_offer_mb_s": offered_fill,
                       # Each stage mover's stall grace and every term of
                       # it (#1010), by mover key: ``basis`` is ``landing``
                       # when a measured landing priced it and the tier
                       # named the pool members the worker judges contention
                       # by (``pool_contention``, as sealed); ``unmeasured``
                       # (naming the missing term) when the mover was sealed
                       # with no grace.
                       "mover_progress": mover_progress,
                       # Each stage egress's stall grace and every term of
                       # it (#1021), by egress key: ``basis`` is ``egress``
                       # when the stage's receipted egresses priced it and
                       # the tier named the pool members; ``unmeasured``
                       # (naming the missing term) when the egress was
                       # sealed with no grace.
                       "egress_progress": egress_progress})
    return {
        "plan": plan,
        "residency": {
            "schema": _pool.RESIDENCY_SCHEMA_V1,
            "manifest_sha256": digest,
            "manifest_bytes": int(entry["bytes"]),
            "tier_id": tier_id,
            "leads": leads_for(plan),
        },
    }



@contextlib.contextmanager
def consumer_filing(queue, consumer_action_key: str, *,
                    template: Mapping[str, object], options: FilingOptions,
                    publication: RowPublication, cas, tier_id: str | None = None,
                    attach: bool = False,
                    immutable_plan: Mapping[str, object] | None = None):
    """Hold this consumer's filing authority through its row publication.

    The yield stays inside the existing transition lock. Reapers, cancellation
    and a replacement publication therefore cannot split filing from its row.
    """
    with queue.action_transition_lock(consumer_action_key):
        if immutable_plan is None:
            staged = prepare_window(template, consumer_action_key=consumer_action_key,
                tier=resolve_stage_tier(queue, tier_id), options=options,
                queue=queue, cas=cas, publication=publication)
        else:
            if superseded(queue, immutable_plan) is not None:
                raise SubmissionRefused("decomposed child's immutable data plan was superseded; refusing automatic revival")
            staged = {"plan": immutable_plan, "residency": {
                "schema": _pool.RESIDENCY_SCHEMA_V1,
                "manifest_sha256": immutable_plan["manifest_sha256"],
                "manifest_bytes": immutable_plan["manifest_bytes"],
                "tier_id": immutable_plan["tier_id"],
                "leads": leads_for(immutable_plan)}}
        renew = not (attach and staged.get("reused_frozen_plan"))
        try:
            renewal = seal_window(queue, staged["plan"], renew=renew)
        except ResidencyPlanError as exc:
            raise SubmissionRefused(f"pbrun: {exc}") from None
        yield FiledWindow(staged["plan"], staged["residency"],
                          tuple(renewal["retired"]),
                          bool(staged.get("reused_frozen_plan")))


def file_manifest_row(queue, consumer_action_key: str, *,
                      template: Mapping[str, object], tier: Mapping[str, object],
                      options: FilingOptions, publication: RowPublication, cas
                      ) -> FiledWindow:
    """File for an already-published bare row; reuse never renews its window."""
    with queue.action_transition_lock(consumer_action_key):
        staged = prepare_window(template, consumer_action_key=consumer_action_key,
            tier=tier, options=options, queue=queue, cas=cas,
            publication=publication)
        renewal = ({"retired": []} if staged.get("reused_frozen_plan") else
                   seal_window(queue, staged["plan"], renew=True))
        return FiledWindow(staged["plan"], staged["residency"],
                           tuple(renewal["retired"]),
                           bool(staged.get("reused_frozen_plan")))


def _active_marker_key(name: str) -> str | None:
    """The consumer key an active supersession marker name carries, or ``None``.

    The address is ``{key}.{plan_sha256}.superseded.json``, both 64 lowercase
    hex characters.  Any other name is not an active marker; the caller tells
    a known archive name from an unrecognized one.
    """

    if not name.endswith(_SUPERSEDED_MARKER_SUFFIX):
        return None
    stem = name[:-len(_SUPERSEDED_MARKER_SUFFIX)]
    key, separator, digest = stem.partition(".")
    if not separator or "." in digest:
        return None
    if not _pool.record_store.key_shaped(key) or not _pool.record_store.key_shaped(digest):
        return None
    return key



def _archived_supersession_name(name: str) -> bool:
    """Whether one marker-directory name is a known archive entry.

    ``residency_plan`` archives a reaped plan body as
    ``{key}.{seconds}.{micros}.{reason-slug}.json`` and retires an active
    marker beside it as
    ``{key}.{digest}.{filing}.{seconds}.{micros}.marker.json``; a
    publication temporary is ``.{name}.{random}.tmp``.  None of these can be
    the address an active marker is read at, so a directory holding them
    still answers which keys have one.  Anything else is unrecognized, and
    the caller treats the whole listing as unknown.
    """

    if name.startswith("."):
        return True
    if name.endswith(".marker.json"):
        parts = name[:-len(".marker.json")].split(".")
        return (len(parts) == 5 and _pool.record_store.key_shaped(parts[0]) and _pool.record_store.key_shaped(parts[1])
                and all(part.isdigit() for part in parts[2:]))
    if name.endswith(".json"):
        parts = name[:-len(".json")].split(".")
        return (len(parts) == 4 and _pool.record_store.key_shaped(parts[0])
                and parts[1].isdigit() and parts[2].isdigit()
                and bool(parts[3])
                and all(character in _SLUG_CHARACTERS
                        for character in parts[3]))
    return False



_SUPERSEDED_MARKER_SUFFIX = ".superseded.json"



_SLUG_CHARACTERS = frozenset(
    "abcdefghijklmnopqrstuvwxyz0123456789-")



def active_supersessions(queue, *, store=None) -> frozenset[str] | None:
    store = queue.records if store is None else store
    try:
        names = store.residency_supersession_names()
    except FileNotFoundError:
        return frozenset()
    except OSError:
        return None
    keys: set[str] = set()
    for name in names:
        key = _active_marker_key(name)
        if key is not None:
            keys.add(key)
        elif not _archived_supersession_name(name):
            return None
    return frozenset(keys)



def shared_movers(queue: _pool.PoolQueue) -> frozenset[str] | None:
    """The movers registered as shared (#1026), or ``None`` if unreadable.

    One listing of ``residency-plans/shared/movers/`` per cycle.  An absent
    directory is the ordinary answer before any range was shared: none.
    """

    memo = _SHARED_MOVER_NAMES[0]
    root = str(queue.root)
    if memo is not None and root in memo:
        return memo[root]
    try:
        found: frozenset[str] | None = frozenset(
            name[:-len(".json")] for name in queue.records.shared_mover_names())
    except OSError:
        found = None
    if memo is not None:
        memo[root] = found
    return found



_SHARED_MOVER_NAMES: list[dict[str, frozenset[str] | None] | None] = [None]


@contextlib.contextmanager
def registry_cycle():
    """The tier cycle's existing shared-index and shared-name observation lifetime."""
    _SHARED_MOVER_NAMES[0] = {}
    _SHARE_NAMESPACE_MEMO[0] = {}
    try:
        yield
    finally:
        _SHARED_MOVER_NAMES[0] = None
        _SHARE_NAMESPACE_MEMO[0] = None


def registry_root(queue) -> Path:
    return queue.records.residency_registry_root()


def filed_consumers(queue) -> list[str]:
    """Filed names shaped as complete consumer keys, sorted for attribution."""
    return sorted(path.stem for path in queue.records.optional_plan_paths()
                  if path.suffix == ".json" and len(path.stem) == 64
                  and set(path.stem) <= _HEX)


def filed_consumer_candidates(queue) -> list[str]:
    """The sweep's permissive positive candidates; locked reads decide ownership."""
    return [path.stem for path in queue.records.optional_plan_paths()
            if path.suffix == ".json" and len(path.stem) == 64]


def retired_consumers(queue) -> set[str] | None:
    """Any retirement history stands down the automatic bare-row planner."""
    try:
        return {marker.name.split(".", 1)[0]
                for marker in queue.records.supersession_paths()}
    except (OSError, ValueError):
        return None


def shared_ranges_present(queue) -> bool:
    return queue.records.shared_registry_present()


def inspection_paths(queue) -> list[Path]:
    """Required listing with the census's original visible-JSON selector."""
    return queue.records.required_plan_paths()


def inspection_names(queue) -> list[str]:
    return queue.records.inspection_plan_names()


def inspection_entries(store):
    return store.plan_entries()


def retirement_presence(queue, consumer: str):
    return queue.records.retirement_presence_at(queue.records.residency_plan_path(consumer))


def concluded_consumers(queue: _pool.PoolQueue) -> list[str]:
    """Consumer keys with a filed plan and no ready or claimed consumer row."""

    keys = []
    for path in sorted(
        queue.records.optional_plan_paths(),
    ):
        key = path.stem
        if path.suffix != ".json" or not path.is_file():
            continue
        if bool(
            _pool.record_store.value_or_raise(
                queue.records.item_presence(_pool.record_store.READY, key),
            ),
        ):
            continue
        if bool(
            _pool.record_store.value_or_raise(
                queue.records.item_presence(_pool.record_store.CLAIMED, key),
            ),
        ):
            continue
        keys.append(key)
    return keys



@dataclass(frozen=True, kw_only=True)
class LegObservation:
    published: Sequence[str] = ()
    staged: Sequence[str] = ()
    rowed: Sequence[str] = ()
    mover_role: str = "mover_row"
    horizon_end_bytes: int | None = None


@dataclass(frozen=True, kw_only=True)
class WindowBudget:
    free_gib: int
    capacity_gib: int | None = None
    runahead_cap_gib: int | None = None
    withdrawn: Sequence[str] = ()
    prelaunch_held: bool | None = None
    prelaunch_need_gib: int | None = None


@dataclass(frozen=True)
class ConsumerWindow:
    """One captured consumer/leg observation and its cycle-local decisions.

    Budget probes use their own observations. This value performs no I/O and
    never changes a later freshness or publication boundary into a cached read.
    """
    plan: Mapping[str, object]
    accepted_phase: str | None
    leg: LegObservation
    budget: WindowBudget | None = None

    @cached_property
    def decision(self) -> dict[str, object]:
        """What the coordinator should publish and evict on this cycle.

        ``accepted_phase`` is the phase the consumer's progress record says it is
        reading *now*, so the phases before it are finished with and may be
        evicted; counting the named phase itself would take back a window the
        consumer is still inside, the same rule ``manifest_layout.consumed_through``
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
        minted total (``TierLedger.mint_supply``); leaving it out keeps free
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

        ``prelaunch_held`` says whether the consumer's prelaunch group holds its
        declared prefix now (#1594).  It changes nothing for a plan with no
        declared phase.  For a declared plan the declared legs are the phase
        being read: they never evict, never count as run-ahead, and never wait
        on the budget or the horizon.  Without the held group (``None`` counts
        as not held) no declared leg publishes and the answer carries a
        ``prelaunch_waiting_for_room`` stall; with it every unpublished declared
        leg publishes in read order, outside free room, and the streaming legs
        follow the rules above as if the declared phases were absent.
        """
        if self.budget is None:
            raise TypeError("window decision requires a captured free_gib budget")
        plan, accepted_phase = self.plan, self.accepted_phase
        published, staged = self.leg.published, self.leg.staged
        mover_role, withdrawn = self.leg.mover_role, self.budget.withdrawn
        horizon_end_bytes = self.leg.horizon_end_bytes
        free_gib, capacity_gib = self.budget.free_gib, self.budget.capacity_gib
        runahead_cap_gib = self.budget.runahead_cap_gib
        prelaunch_held = self.budget.prelaunch_held
        prelaunch_need_gib = self.budget.prelaunch_need_gib

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
        declared = frozenset(prelaunch_phase_names(plan))

        evict = []
        for leg in legs:
            if leg["phase"] in declared:
                continue
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
        future = [leg for leg in legs if leg["phase"] in ahead_names[1:]
                  and leg["phase"] not in declared]
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
        # The first streaming phase is the budget regime's current: the declared
        # legs never count as the phase being read here.  Without a declaration
        # this names the same phase as ``current_name``.
        stream_names = [name for name in ahead_names if name not in declared]
        stream_current = stream_names[0] if stream_names else None
        effective_current = stream_current if declared else current_name
        stall: dict[str, object] | None = None
        if declared and prelaunch_held is not True:
            pending = [leg for leg in legs
                       if leg["phase"] in declared
                       and leg["phase"] in ahead_names
                       and str(leg["mover_row"]["action_key"]) not in already]  # type: ignore[index]
            if pending:
                from . import window_credit as _credit
                # One computation serves the window and the begin (#1690): the
                # stall's blocked amount is the group's own deficit when known,
                # never a second sum beside it. Without the need the pending
                # sum stands, so callers that pass no need read as before.
                blocked = (int(prelaunch_need_gib)
                           if prelaunch_need_gib is not None
                           else sum(int(leg["stage_gib"]) for leg in pending))
                stall = {
                    "consumer_action_key": plan["consumer_action_key"],
                    "tier_id": plan["tier_id"],
                    "accepted_phase": accepted_phase,
                    "reading_phase": current_name,
                    "blocked_phase": str(pending[0]["phase"]),
                    "blocked_gib": blocked,
                    "runahead_gib": runahead,
                    "runahead_budget_gib": budget,
                    "free_gib": int(free_gib),
                    "capacity_gib": None if capacity_gib is None else int(capacity_gib),
                    "reason": _credit.REASON_PRELAUNCH_WAIT,
                    "waiting_for": "the prelaunch group reservation",
                }
                if prelaunch_need_gib is not None:
                    stall["need_gib"] = int(prelaunch_need_gib)
                return {"publish": publish, "evict": evict, "stall": stall}
        for leg in legs:
            if leg["phase"] not in ahead_names:
                continue
            key = str(leg["mover_row"]["action_key"])  # type: ignore[index]
            if key in already or key in cancelled:
                continue
            need = int(leg["stage_gib"])
            if leg["phase"] in declared:
                # Only a held group reaches this branch with an unpublished
                # declared leg: the wait above returns first.  The group already
                # holds these tokens, so they take no room and spend no budget.
                row: dict[str, object] = {"phase": leg["phase"]}
                if leg["chunk_index"] is not None:
                    row["chunk_index"] = leg["chunk_index"]
                row.update({
                    "mover_action_key": key,
                    "start_bytes": leg["start_bytes"], "end_bytes": leg["end_bytes"],
                    "stage_gib": need, "mover_row": leg["mover_row"],
                })
                publish.append(row)
                continue
            is_current = leg["phase"] == effective_current
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
            row = {"phase": leg["phase"]}
            if leg["chunk_index"] is not None:
                row["chunk_index"] = leg["chunk_index"]
            row.update({
                "mover_action_key": key,
                "start_bytes": leg["start_bytes"], "end_bytes": leg["end_bytes"],
                "stage_gib": need, "mover_row": leg["mover_row"],
            })
            publish.append(row)
        return {"publish": publish, "evict": evict, "stall": stall}


    @cached_property
    def advance(self) -> dict[str, object]:
        """The minimum simultaneous current-plus-next needs of one window leg.

        The first two unpublished legs in read order: ``current_min_gib`` is what
        admission must stage first (the lead), ``next_min_gib`` the advance that
        must fit beside it for guaranteed progress (``None`` on the final leg --
        a final phase needs no future credit).  Chunked legs decide per chunk, so
        a sliding window's minimum is two chunks, never two phases.  A leg the
        submitter sealed without (``mover_role`` absent) contributes nothing.
        Raises :class:`ResidencyPlanError` for an unknown mover role, like
        :attr:`ConsumerWindow.decision`.

        ``rowed`` names the movers with a queued ready row; ``staged`` the ones
        holding tokens for landed bytes; ``queued`` returns those ahead legs in
        read order, with ``queued_prior`` the full-ordered legs before the first
        of them.  ``fence_target`` names the one advance the fence protects --
        exactly the leg after the frontier (the earliest unstaged ahead leg),
        which pays from free under the gate's count -- with ``fence_prior``
        the legs before it: one fence per window, and the phase the blind
        pre-publish take and the post-publish bind agree on.  The gate reads
        ``waiting``/``prior`` exactly as before.

        ``horizon_end_bytes`` is :attr:`ConsumerWindow.decision`'s refill horizon (#903), and it
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

        A declared plan (#1594) reports only its streaming legs here: ``waiting``,
        ``queued``, the frontier and ``fence_target`` skip the declared prefix,
        and ``lead_mover_action_key`` is the first streaming leg's mover.  Two
        extra keys carry the prefix itself: ``prelaunch_legs`` lists every
        declared leg in read order and ``prelaunch_gib`` sums their demands.
        A plan with no declaration returns neither key.
        """
        plan, accepted_phase = self.plan, self.accepted_phase
        published, rowed, staged = self.leg.published, self.leg.rowed, self.leg.staged
        mover_role, horizon_end_bytes = self.leg.mover_role, self.leg.horizon_end_bytes

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
        declared = frozenset(prelaunch_phase_names(plan))
        stream_full = ([leg for leg in full if leg["phase"] not in declared]
                       if declared else full)
        prelaunch = [leg for leg in full if leg["phase"] in declared]
        current_name = ahead_names[0] if ahead_names else None
        waiting = [leg for leg in stream_full
                   if leg["phase"] in ahead_names
                   and str(leg["mover_row"]["action_key"]) not in done  # type: ignore[index]
                   and (horizon_end_bytes is None or leg["phase"] == current_name
                        or int(leg["start_bytes"]) < int(horizon_end_bytes))]
        queued = [leg for leg in stream_full
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
        for index, leg in enumerate(stream_full):
            if (leg["phase"] in ahead_names
                    and str(leg["mover_row"]["action_key"])  # type: ignore[index]
                    not in staged_set):
                frontier_index = index
                break
        fence_target: dict[str, object] | None = None
        fence_prior: list[dict[str, object]] = []
        if frontier_index is not None and frontier_index + 1 < len(stream_full):
            candidate = stream_full[frontier_index + 1]
            if candidate["phase"] in ahead_names and (
                    horizon_end_bytes is None or candidate["phase"] == current_name
                    or int(candidate["start_bytes"]) < int(horizon_end_bytes)):
                fence_target = _advance_entry(candidate)
                fence_prior = _advance_prior(stream_full[:frontier_index])
        if not waiting:
            idle: dict[str, object] = {
                "current_min_gib": 0, "next_min_gib": None, "final": True,
                "reading_phase": ahead_names[0] if ahead_names else None,
                "next_phase": None, "next_mover_action_key": None,
                "next_chunk_index": None, "waiting": [], "prior": [],
                "queued": _entry_list(queued), "queued_prior": [],
                "fence_target": fence_target, "fence_prior": fence_prior,
                "lead_mover_action_key": None}
            if declared:
                idle["prelaunch_legs"] = _entry_list(prelaunch)
                idle["prelaunch_gib"] = sum(
                    int(leg["stage_gib"]) for leg in prelaunch)
            return idle
        first, rest = waiting[0], waiting[1:]
        order = [str(leg["mover_row"]["action_key"])  # type: ignore[index]
                 for leg in stream_full]
        prior = stream_full[:order.index(  # type: ignore[index]
            str(first["mover_row"]["action_key"]))]
        queued_prior: list[dict[str, object]] = []
        if queued:
            queued_prior = stream_full[:order.index(
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
            "lead_mover_action_key": (str(stream_full[0]["mover_row"]["action_key"])  # type: ignore[index]
                                      if stream_full else None),
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
        if declared:
            out["prelaunch_legs"] = _entry_list(prelaunch)
            out["prelaunch_gib"] = sum(int(leg["stage_gib"]) for leg in prelaunch)
        return out


    def ram_publications(self, stage_resident: Collection[str]) -> list[tuple[object, object]]:
        phases = {str(phase["name"]): phase for phase in self.plan["phases"]}
        publishable = []
        for entry in self.decision["publish"]:
            phase = phases.get(str(entry["phase"]))
            if phase is None:
                continue
            mover_row, _egress_row = ram_leg_rows(phase, entry.get("chunk_index"))
            if mover_row is None:
                continue
            if not stage_source_resident(
                    phase, int(entry["start_bytes"]), int(entry["end_bytes"]),
                    stage_resident):
                continue
            publishable.append((mover_row, entry))
        return publishable




def pre_progress_stage_reach(plan: Mapping[str, object],
                             accepted_phase: object) -> set[str]:
    """Later stage legs inside the first-progress run-ahead bound.

    A withdrawn leg still counts against this geometric bound, so
    the answer can name fewer legs than the window publishes.
    """

    ahead = [str(phase["name"]) for phase in remaining(
        plan, accepted_phase)][1:]                               # type: ignore[arg-type]
    later = [leg for leg in legs_over(
        plan, 0, 1 << 62, mover_role="mover_row") if leg["phase"] in ahead]
    budget = _budget_from_step(
        max((int(leg["stage_gib"]) for leg in later), default=0),
        has_accepted=False, capacity_gib=None)
    reach: set[str] = set()
    spent = 0
    for leg in later:
        spent += int(leg["stage_gib"])
        if budget is not None and spent > budget:
            break
        reach.add(str(leg["mover_row"]["action_key"]))          # type: ignore[index]
    return reach


def stage_leg_rows(phase: Mapping[str, object],
                     chunk_index: object) -> tuple[object, object]:
    """The ``(mover_row, egress_row)`` one window entry's stage leg sealed.

    The stage mirror of :func:`_ram_leg_rows`: ``(None, None)`` when the
    entry and the phase disagree about the shape -- a chunk entry for a
    whole-phase leg, or the reverse.  Publishing a row the plan never sealed
    would put an unsealed key in the queue, so a mismatch publishes nothing
    rather than guessing which leg was meant.
    """

    if chunk_index is not None:
        chunks = phase.get("stage_chunks")
        if not isinstance(chunks, list):
            return None, None
        matches = [chunk for chunk in chunks
                   if isinstance(chunk, Mapping)
                   and chunk.get("chunk_index") == chunk_index
                   and isinstance(chunk.get("mover_row"), Mapping)
                   and isinstance(chunk.get("egress_row"), Mapping)]
        if len(matches) != 1:
            return None, None
        return matches[0]["mover_row"], matches[0]["egress_row"]
    mover = phase.get("mover_row")
    egress = phase.get("egress_row")
    return (mover if isinstance(mover, Mapping) else None,
            egress if isinstance(egress, Mapping) else None)



def ram_leg_rows(phase: Mapping[str, object],
                   chunk_index: object) -> tuple[object, object]:
    """The ``(mover_row, egress_row)`` one window entry's leg sealed.

    ``(None, None)`` when the entry and the phase disagree about the shape --
    a chunk entry for a whole-phase leg, or the reverse.  Publishing a row
    the plan never sealed would put an unsealed key in the queue, so a
    mismatch publishes nothing rather than guessing which leg was meant.
    """

    if chunk_index is not None:
        chunks = phase.get("ram_chunks")
        if not isinstance(chunks, list):
            return None, None
        matches = [chunk for chunk in chunks
                   if isinstance(chunk, Mapping)
                   and chunk.get("chunk_index") == chunk_index
                   and isinstance(chunk.get("ram_mover_row"), Mapping)
                   and isinstance(chunk.get("ram_egress_row"), Mapping)]
        if len(matches) != 1:
            return None, None
        return matches[0]["ram_mover_row"], matches[0]["ram_egress_row"]
    # A promotion leg with no egress row still promotes: the plan validator
    # refuses an egress without a mover, not a mover without an egress, so
    # the publish side asks for the mover and the evict side asks for both.
    mover = phase.get("ram_mover_row")
    egress = phase.get("ram_egress_row")
    return (mover if isinstance(mover, Mapping) else None,
            egress if isinstance(egress, Mapping) else None)



def stage_source_resident(phase: Mapping[str, object], start: int, end: int,
                         stage_staged: set[str]) -> bool:
    """Whether the stage already holds the bytes one promotion copies.

    ``stage_staged`` is publication state -- :func:`_resident_movers`, never
    bare token holdings.  The distinction is the whole of #759: a booking
    exists from claim, while the evidence that predicate reads exists only
    once a copy has landed and vouched for itself.

    A promotion's source is the stage and nothing else: with a whole-phase
    stage leg that is the phase's mover, and with a chunked one (#675) it is
    every stage chunk the promoted range overlaps -- a later chunk the
    promotion does not touch is not its precondition, so the current phase's
    promotions stage as their turn comes while the stage slides behind them.
    """

    chunks = phase.get("stage_chunks")
    if not isinstance(chunks, list):
        mover = phase.get("mover_row")
        return (isinstance(mover, Mapping)
                and str(mover.get("action_key")) in stage_staged)
    overlapped = []
    for chunk in chunks:
        if not isinstance(chunk, Mapping):
            continue
        cstart, cend = chunk.get("start_bytes"), chunk.get("end_bytes")
        if (isinstance(cstart, bool) or not isinstance(cstart, int)
                or isinstance(cend, bool) or not isinstance(cend, int)):
            continue
        if cstart < end and cend > start:
            overlapped.append(chunk)
    if not overlapped:
        return False
    return all(
        isinstance(chunk.get("mover_row"), Mapping)
        and str(chunk["mover_row"]["action_key"]) in stage_staged  # type: ignore[index]
        for chunk in overlapped)


def filed_name(queue, consumer_action_key: str) -> str:
    """The canonical current filename, retaining Store's consumer-key check."""
    return queue.records.residency_plan_path(consumer_action_key).name
