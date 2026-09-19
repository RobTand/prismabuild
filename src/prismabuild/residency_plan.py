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
    "ram_chunks"})
#: What one chunk of a chunked ram leg says, and nothing else.
_CHUNK_KEYS = frozenset({
    "chunk_index", "start_bytes", "end_bytes", "stage_gib",
    "ram_mover_row", "ram_egress_row"})
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
            "mover_row": dict(phase["mover_row"]),      # type: ignore[arg-type]
            "egress_row": dict(phase["egress_row"]),    # type: ignore[arg-type]
        }
        if phase.get("ram_mover_row") is not None:
            entry["ram_mover_row"] = dict(phase["ram_mover_row"])  # type: ignore[arg-type]
        if phase.get("ram_egress_row") is not None:
            entry["ram_egress_row"] = dict(phase["ram_egress_row"])  # type: ignore[arg-type]
        if phase.get("ram_chunks") is not None:
            entry["ram_chunks"] = [  # type: ignore[arg-type]
                {**chunk} for chunk in phase["ram_chunks"]]
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


def _checked_ram_chunk(chunk: object, *, phase_name: str, chunk_index: int,
                       phase_start: int, digest: str, ram_tier_id: str,
                       ram_demand_kind: str,
                       keys: set[str]) -> dict[str, object]:
    """One chunk of a chunked ram leg, checked the way the whole-phase leg is.

    ``phase_start`` is where the previous chunk ended (or the phase began):
    chunks tile their phase with no gap and no overlap, because a gap is
    bytes nobody promotes and an overlap is bytes two promotions both
    publish under one name -- the same cover rule the phases themselves
    answer to.  ``keys`` is the plan's shared action-key set, so a chunk
    node never shares a key with anything else sealed.
    """

    where = f"plan phase {phase_name!r} chunk {chunk_index}"
    if not isinstance(chunk, Mapping):
        raise ResidencyPlanError(f"{where} must be an object")
    stray = sorted(set(chunk) - _CHUNK_KEYS)
    if stray:
        raise ResidencyPlanError(f"unknown ram-chunk fields: {stray}")
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
    ram_mover = chunk.get("ram_mover_row")
    if not isinstance(ram_mover, Mapping):
        raise ResidencyPlanError(f"{where} needs a ram_mover_row")
    ram_key = _action_key(ram_mover.get("action_key"),
                          where="ram_mover_row.action_key")
    if ram_key in keys:
        raise ResidencyPlanError("two plan rows share an action key")
    keys.add(ram_key)
    ram_pin = ram_mover.get("residency")
    if not isinstance(ram_pin, Mapping):
        raise ResidencyPlanError(
            f"{where} has a ram mover row with no residency block; its "
            f"occupancy would be released the moment it finished")
    if (ram_pin.get("tier_id") != ram_tier_id
            or ram_pin.get("manifest_sha256") != digest
            or ram_pin.get("range_start_bytes") != cstart
            or ram_pin.get("range_end_bytes") != cend):
        raise ResidencyPlanError(
            f"{where} names bytes {cstart}..{cend} of {digest[:12]} for the "
            f"ram tier {ram_tier_id}, and its ram mover row pins "
            f"{ram_pin.get('range_start_bytes')}.."
            f"{ram_pin.get('range_end_bytes')} of "
            f"{str(ram_pin.get('manifest_sha256'))[:12]} on "
            f"{ram_pin.get('tier_id')}")
    ram_resources = ram_mover.get("resources")
    if (not isinstance(ram_resources, Mapping)
            or int(ram_resources.get(ram_demand_kind, 0)) < floor):
        raise ResidencyPlanError(
            f"{where} asks the ram tier for "
            f"{None if not isinstance(ram_resources, Mapping) else ram_resources.get(ram_demand_kind)}"
            f", below the {floor} GiB its range occupies")
    ram_egress = chunk.get("ram_egress_row")
    if not isinstance(ram_egress, Mapping):
        raise ResidencyPlanError(f"{where} needs a ram_egress_row")
    ram_egress_key = _action_key(ram_egress.get("action_key"),
                                 where="ram_egress_row.action_key")
    if ram_egress_key in keys:
        raise ResidencyPlanError("two plan rows share an action key")
    keys.add(ram_egress_key)
    return {**dict(chunk), "ram_mover_row": dict(ram_mover),
            "ram_egress_row": dict(ram_egress)}


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
                checked = _checked_ram_chunk(
                    chunk, phase_name=name, chunk_index=index,
                    phase_start=position, digest=digest,
                    ram_tier_id=ram_tier_id,
                    ram_demand_kind=ram_demand_kind, keys=keys)
                checked_chunks.append(checked)
                position = int(checked["end_bytes"])
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
        checked.append({**dict(phase), "mover_row": mover,
                        "egress_row": rows["egress_row"],
                        **{role: rows[role] for role in rows
                           if role not in ("mover_row", "egress_row")}})
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
    there to protect.  A plan's ram promotions are movement nodes of the same
    plan (#640), so their keys are here too -- a pinned promotion no live item
    names is an orphan on the ram tier exactly as its stage sibling is.
    """

    phases = plan["phases"]
    assert isinstance(phases, list)
    out = [str(phase["mover_row"]["action_key"]) for phase in phases]
    out += ram_mover_keys(plan)
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


def _legs(plan: Mapping[str, object], *, mover_role: str) -> list[dict[str, object]]:
    """One publishable unit per phase, or per chunk of a chunked phase.

    A chunked ram leg (#673) promotes and frees per chunk: each chunk is a
    leg carrying its phase, its chunk index and its own mover and egress
    rows, in read order.  Every other leg -- the stage's, and a ram leg
    sealed whole -- is one leg over the phase's whole range with no chunk
    index, which is what keeps those decisions byte-identical to today.
    """

    egress_role = _MOVEMENT_ROLES[mover_role]
    legs = []
    for phase in plan["phases"]:
        assert isinstance(phase, Mapping)
        chunks = (phase.get("ram_chunks")
                  if mover_role == "ram_mover_row" else None)
        if isinstance(chunks, list):
            for chunk in chunks:
                assert isinstance(chunk, Mapping)
                legs.append({
                    "phase": str(phase["name"]),
                    "chunk_index": int(chunk["chunk_index"]),
                    "start_bytes": int(chunk["start_bytes"]),
                    "end_bytes": int(chunk["end_bytes"]),
                    "stage_gib": int(chunk["stage_gib"]),
                    "mover_row": chunk["ram_mover_row"],
                    "egress_row": chunk["ram_egress_row"],
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
           mover_role: str = "mover_row") -> dict[str, object]:
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

    A phase the submitter sealed chunked (#673) decides per chunk: each
    chunk is a leg with its own ``chunk_index``, its own range and its own
    rows, in read order.  Chunks of the phase being read are the reader's
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
        if key in already:
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
    "leads_for",
    "mover_keys",
    "ram_mover_keys",
    "read",
    "remaining",
    "runahead_budget_gib",
    "runahead_step_gib",
    "validate_plan",
    "window",
]
