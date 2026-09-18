#!/usr/bin/env python3
"""Mint and announce this box's storage tiers from what the box says (#583, #582).

One loop per file-serving box, spawned by ``supervise.py`` under the
``tiers`` role.  Every cycle it discovers the tiers again -- the ARC from
``arcstats``, stage pools by the ``prismabuild-stage`` name prefix, the
source pool's members from ``zpool status`` -- learns the pool's fill
bandwidth from the receipts of reads off it, and makes each tier's ledger
say exactly that: ``mint_tier_capacity`` grows and shrinks the token supply
to the discovered number, so adding a device to the stage pool offers more
on the next cycle and exporting the pool offers nothing.  Rob, 2026-09-17:
*"as the topology of my drives changes, prismabuild will be able to
automatically adapt."*

Nothing here is a capacity constant.  The two arguments name *which* pool
the export is served from and how often to look; every quantity is read.

**The probe rule.**  Fill tokens come from receipts that carry the pacer's
pool-side delivery (``disk_pacing.mean_pool_read_mb_s``: the sum over the
pool's members of sectors read during that reader's window).  Until
one exists nothing has measured the pool, and a ledger with no fill tokens
admits no mover at all.  So while no attributed receipt exists, the loop
mints exactly the fill demand of the oldest ready item that asks this tier
for fill: one mover fits, it runs, its receipt is the first measurement,
and the next cycle mints from that.  The same idiom the adaptive GPU
controller uses for an unknown consumer -- admit one, measure it, price
the rest from the measurement.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from collections.abc import Mapping
import socket
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
from runtime_paths import generation_root  # noqa: E402

sys.path.insert(0, str(generation_root(__file__) / "src"))

from prismabuild import pool  # noqa: E402
from prismabuild import residency_map  # noqa: E402
from prismabuild import residency_plan  # noqa: E402
from prismabuild import storage_tiers  # noqa: E402

import prewarm_loop  # noqa: E402
import stage_release  # noqa: E402
#: The same generation gate ``prewarm_loop`` reads, under the same name, for
#: the same reason: a loop holds the modules it imported for its whole life,
#: so a fix published under a running fleet reaches none of it (#615).
import worker_loop as runtime_gate  # noqa: E402

#: One definition, in the pool: ``stage_move.py`` writes these through
#: ``record_move`` and this loop reads them for the fill measurement.
MOVER_RECEIPTS = pool.MOVERS

#: The interpreter this loop runs under, and the directory the fleet scripts
#: were published into beside it.  Read here rather than passed in: the loop is
#: spawned on the storage box by the supervisor, from the published generation,
#: so these two values are that box's own answer to "what runs a mover".
#: ``publish_runtime`` writes every fleet script to ``tools/<name>`` *and*
#: ``tools/fleet/<name>``, and a checkout keeps them only under
#: ``tools/fleet/``, so the loop's own directory holds ``stage_move.py`` in
#: either layout.
MOVER_PYTHON = sys.executable
MOVER_TOOLS_ROOT = str(Path(__file__).resolve().parent)


class ReceiptCache:
    """Receipts read once per (path, mtime); the pool's fill history is append-only."""

    def __init__(self) -> None:
        self._records: dict[str, tuple[float, dict[str, object]]] = {}

    def read(self, directories: list[Path]) -> list[dict[str, object]]:
        seen: set[str] = set()
        for directory in directories:
            try:
                entries = sorted(directory.glob("*.json"))
            except OSError:
                continue
            for path in entries:
                try:
                    mtime = path.stat().st_mtime
                except OSError:
                    continue
                name = str(path)
                seen.add(name)
                cached = self._records.get(name)
                if cached is not None and cached[0] == mtime:
                    continue
                record = pool._read_json(path)
                if isinstance(record, dict):
                    self._records[name] = (mtime, record)
        for name in list(self._records):
            if name not in seen:
                self._records.pop(name)
        return [record for _, record in self._records.values()]


def probe_fill_demand(ready: list[dict[str, object]], tier_id: str) -> int | None:
    """The fill the oldest ready item asks of this tier, or ``None`` when none does."""

    kind = f"{storage_tiers.FILL_KIND}{storage_tiers.TIER_DEMAND_SEPARATOR}{tier_id}"
    candidates: list[tuple[float, int]] = []
    for item in ready:
        resources = item.get("resources")
        if not isinstance(resources, dict) or kind not in resources:
            continue
        try:
            need = int(resources[kind])
        except (TypeError, ValueError):
            continue
        if need > 0:
            candidates.append((float(item.get("published_unix", 0.0) or 0.0), need))
    if not candidates:
        return None
    return min(candidates)[1]


def live_consumers(queue: pool.PoolQueue) -> list[dict[str, object]]:
    """Every ready or claimed item that declares leads, with what it has accepted.

    Ready and claimed both, because the window has work to do on either: a
    ready consumer needs its first phase staged before anything can admit it,
    and a claimed one needs the next phase staged while it reads this one.  A
    terminal consumer is deliberately absent -- its ranges are nobody's to keep
    resident, and the sweep takes them back.
    """

    out: list[dict[str, object]] = []
    for state in (pool.READY, pool.CLAIMED):
        for path in pool._scan(queue.dir(state)):
            item = pool._read_json(path)
            if not isinstance(item, dict):
                continue
            residency = item.get("residency")
            if not isinstance(residency, dict) or not residency.get("leads"):
                continue
            key = item.get("action_key")
            if not isinstance(key, str):
                continue
            accepted = None
            if state == pool.CLAIMED:
                claimed_unix = item.get("claimed_unix")
                if isinstance(claimed_unix, (int, float)):
                    observation = prewarm_loop.progress_phase(
                        queue, key, float(claimed_unix))
                    if observation is not None:
                        accepted = str(observation["phase"])
            # The record itself travels beside the key: ``record_denial`` is
            # keyed by an item's own ``published_unix`` generation, so a
            # coordinator that carried only the key could not file one.
            out.append({"action_key": key, "state": state,
                        "accepted_phase": accepted, "item": item})
    return out


def _mover_state(queue: pool.PoolQueue, plan: Mapping[str, object],
                 tier_id: str) -> tuple[set[str], set[str]]:
    """Which of a plan's movers count as published, and which are on the stage.

    A mover holding tier tokens is resident by definition -- that is the
    invariant the pin exists to keep -- so the ledger answers "what is on the
    stage" without walking the device.  ``published`` deliberately excludes a
    terminal mover that holds nothing: its key is a content hash, so the same
    manifest seals the same key on a second campaign, and a ``done`` record
    left over from an evicted range would otherwise be mistaken for a window
    that is already staged and never republished by anyone.
    """

    ledger = queue.tier_ledger(tier_id)
    published: set[str] = set()
    staged: set[str] = set()
    for key in residency_plan.mover_keys(plan):
        pinned = bool(ledger.holder_tokens(key))
        if pinned:
            staged.add(key)
        if (queue.item_path(pool.READY, key).exists()
                or queue.item_path(pool.CLAIMED, key).exists()
                or pinned):
            published.add(key)
    return published, staged


def compose_map(queue: pool.PoolQueue, consumer_action_key: str) -> Path | None:
    """Write one consumer's residency map from its movers' fragments.

    **The single writer.**  Movers write one fragment each, into a file only
    they name; nothing merges them but this loop, on one box, in one thread.
    A shared document with many writers cannot be merged by a rename, which is
    the only concurrency primitive this mount gives us, so the alternative was
    not a lock -- it was lost entries.

    Recomposed every cycle rather than on a trigger, because the fragments move
    in both directions: a mover adds one when it finishes, an egress removes
    one when it deletes the bytes, and a map that still named an evicted range
    would send the consumer to a path that is gone.
    """

    fragments = residency_map.read_fragments(
        queue.residency_fragment_root(), consumer_action_key)
    path = queue.residency_map_path(consumer_action_key)
    if not fragments:
        # Nothing staged (yet, or any more).  Removing the map is what puts the
        # consumer back on the pool; leaving a stale one would point it at
        # deleted files, which reads as corruption rather than as a cache miss.
        path.unlink(missing_ok=True)
        return None
    return residency_map.write_map(path, residency_map.compose(fragments))


def _planned_consumers(
    queue: pool.PoolQueue, tiers: Mapping[str, Mapping[str, object]],
) -> list[tuple[str, dict[str, object], dict[str, object], str]]:
    """Live consumers whose frozen plan stages onto a tier this box announced.

    Quietly: a plan this reader refuses is reported once, by
    :func:`residency_window`, which is the step that has a denial to file.  A
    second report from each of the steps below would say the same thing three
    times per cycle.
    """

    out: list[tuple[str, dict[str, object], dict[str, object], str]] = []
    for consumer in live_consumers(queue):
        key = str(consumer["action_key"])
        plan = residency_plan.read(queue, key)
        if plan is None:
            continue
        tier_id = str(plan["tier_id"])
        if tier_id not in tiers:
            continue      # another box's stage; its own loop owns that ledger
        out.append((key, consumer, plan, tier_id))
    return out


def _descriptor(manifest_sha256: str, tier_id: str, start: int, end: int) -> tuple:
    """The identity two movers of one range share however they were sealed.

    The four fields ``core.residency_descriptor`` binds.  It is deterministic,
    which is what lets a consumer bind a mover's result before the mover runs
    -- and is equally what lets a *later* consumer recognise its own range in a
    copy somebody else already made.  The mover's action key cannot do this
    job: it hashes an argv carrying ``--consumer-action-key``, so two consumers
    of one manifest seal two different keys for the same bytes.
    """

    return (str(manifest_sha256), str(tier_id), int(start), int(end))


def adoptable_ranges(queue: pool.PoolQueue, *, tier_id: str,
                     reserved: set[str]) -> dict[tuple, str]:
    """Descriptor -> mover key, for resident ranges no live item still names.

    Exactly the set the orphan sweep would take back: a range whose consumer
    has finished, failed or been withdrawn, still pinned because its bytes are
    still there.  ``reserved`` is what keeps a *running* consumer's window out
    of it -- that is the distinction #598 said was missing, and it is read off
    the queue's own live state rather than from a clock.

    First key wins when two finished movers left the same range, so the answer
    does not depend on directory order.
    """

    index: dict[tuple, str] = {}
    try:
        held = queue.tier_ledger(tier_id).held_keys()
    except (OSError, pool.PoolContractError):
        return index
    for key in sorted(held):
        if key in reserved:
            continue
        staged = queue.staged_range_of(key)
        if staged is None or str(staged["tier_id"]) != tier_id:
            continue
        index.setdefault(
            _descriptor(str(staged["manifest_sha256"]), tier_id,
                        int(staged["range_start_bytes"]),
                        int(staged["range_end_bytes"])), key)
    return index


def adopt(queue: pool.PoolQueue, *, old_key: str, new_key: str,
          consumer_action_key: str, tier_id: str, phase: str,
          range_start_bytes: int, range_end_bytes: int,
          residency_root: Path) -> dict[str, object]:
    """Hand one resident range from a finished mover to a live consumer's (#598).

    No byte is copied and no instant has bytes on the stage that no key holds.
    The order is the whole argument:

    1. **The successor vouches for the same files under its own name.**  Two
       fragments then name one range, which every reader already tolerates:
       ``compose`` is per consumer, and the reconciliation unions them.
    2. **The tokens change owner.**  ``ResourceLedger.transfer`` renames each
       token between two directories under ``held/``, so the tier's occupancy
       is the same number throughout and a crash part-way splits the
       attribution without changing the sum.
    3. **Only then does the old name stop accounting for the bytes.**  Dropping
       the old fragment before the transfer would leave an egress able to
       release tokens for bytes that are still there; dropping it after means
       the worst an interrupted adoption leaves is a range named twice.
    4. **The receipt the pin and the gate read.**  An adopted mover never runs,
       so it files no terminal record; ``adopted_from`` plus the ledger is what
       ``residency_verdict`` reads instead.

    Under the old mover's transition lock, non-blocking, because the one party
    that could be acting on the same range at the same time is its egress.
    Declining costs a copy; proceeding against an egress mid-delete would cost
    the consumer its bytes.
    """

    outcome: dict[str, object] = {
        "event": "range-adoption-declined", "adopted": False,
        "consumer": consumer_action_key, "phase": phase,
        "tier_id": tier_id, "mover": new_key, "adopted_from": old_key,
    }
    ledger = queue.tier_ledger(tier_id)
    with queue.mover_transition_lock(old_key, blocking=False) as acquired:
        if not acquired:
            # Its egress holds the lock, which means its bytes are going.
            return {**outcome, "reason": "range_busy"}
        before = ledger.holder_tokens(old_key)
        if not before:
            return {**outcome, "reason": "no_longer_resident"}
        receipt = queue.move_record(old_key)
        old_consumer = (str(receipt.get("consumer_action_key") or "")
                        if isinstance(receipt, Mapping) else "")
        if len(old_consumer) != 64:
            return {**outcome, "reason": "unattributed_range"}
        source_path = residency_map.fragment_path(
            residency_root, old_consumer, old_key)
        try:
            with open(source_path) as stream:
                source = residency_map.validate_fragment(json.load(stream))
        except (OSError, ValueError) as exc:
            # A fragment that is absent or unreadable cannot say which files
            # this range is, and a range nobody can name is not one to take
            # over.  The same refusal ``evict`` makes, for the same reason.
            return {**outcome, "reason": "range_not_named", "error": repr(exc)}
        residency_map.write_fragment(residency_root, residency_map.reissue(
            source, consumer_action_key=consumer_action_key,
            mover_action_key=new_key))
        expected = sum(before.values())
        moved = queue.transfer_tier_reservation(tier_id, old_key, new_key)
        if moved != expected:
            # The reservation is split across the two keys and the sum is
            # unchanged, so nothing is over-admitted; the old key is still
            # resident, so the next cycle asks again and finishes the move.
            return {**outcome, "reason": "partial_transfer",
                    "tokens_moved": moved, "tokens_expected": expected}
        source_path.unlink(missing_ok=True)
        entries = dict(source["entries"])                # type: ignore[arg-type]
        # The successor's own phase boundaries, which the descriptor match
        # already proved equal to the range this copy made resident.  Taken
        # from the plan rather than re-parsed out of the old receipt, so the
        # range the new receipt declares is the one its pin is checked against.
        start, end = int(range_start_bytes), int(range_end_bytes)
        queue.record_move(new_key, {
            "consumer_action_key": consumer_action_key,
            "tier_id": tier_id,
            "stage_root": str(source["stage_root"]),
            "manifest_sha256": str(source["manifest_sha256"]),
            "range_start_bytes": start,
            "range_end_bytes": end,
            "range_bytes": end - start,
            # The bytes are staged; they were staged by somebody else.  Both
            # halves are said, because ``residency_pin_holds`` reads the first
            # and an operator pricing the next window reads the second -- and
            # a receipt with no ``seconds`` prices nothing, which is what an
            # adoption should contribute to a bandwidth measurement.
            "bytes_staged": end - start,
            "bytes_copied": 0,
            "entries_declared": len(entries),
            "entries_staged": len(entries),
            "complete": True,
            pool.MOVE_ADOPTED_FROM_FIELD: old_key,
            "adopted_from_consumer": old_consumer,
            "phase": phase,
            "host": socket.gethostname(),
            "unix": time.time(),
        })
        return {**outcome, "event": "range-adopted", "adopted": True,
                "tokens_moved": moved, "entries": len(entries),
                "bytes_staged": end - start}


def adopt_resident_ranges(
    queue: pool.PoolQueue, *, tiers: Mapping[str, Mapping[str, object]],
    consumers: list | None = None,
) -> list[dict[str, object]]:
    """Take over every resident range a live consumer's window still needs (#598).

    Before the orphan sweep, deliberately: the ranges this can take are exactly
    the ones the sweep would delete, and the campaign's shape is one probe and
    many artifacts of one model, so the next artifact reads the same shards.
    Rob, 2026-09-18: *"We should not be rerunning anything in bulk if
    avoidable."*

    Adoption is free of capacity: the tokens move, they are not acquired, so a
    range is taken over on a stage with nothing free -- which is the only state
    that matters, because a stage with room would simply have staged the copy.
    Phases the consumer has already read past are left alone; taking those over
    would pin bytes it will never open again.
    """

    events: list[dict[str, object]] = []
    wanted, owners = stage_release.live_claims(queue)
    reserved = set(wanted) | set(owners)
    root = queue.residency_fragment_root()
    index_by_tier: dict[str, dict[tuple, str]] = {}
    if consumers is None:
        consumers = _planned_consumers(queue, tiers)
    for consumer_key, consumer, plan, tier_id in consumers:
        if tier_id not in index_by_tier:
            index_by_tier[tier_id] = adoptable_ranges(
                queue, tier_id=tier_id, reserved=reserved)
        index = index_by_tier[tier_id]
        if not index:
            continue
        ledger = queue.tier_ledger(tier_id)
        digest = str(plan["manifest_sha256"])
        accepted = consumer["accepted_phase"]
        for phase in residency_plan.remaining(plan, accepted):  # type: ignore[arg-type]
            new_key = str(phase["mover_row"]["action_key"])     # type: ignore[index]
            descriptor = _descriptor(digest, tier_id, int(phase["start_bytes"]),
                                     int(phase["end_bytes"]))
            old_key = index.get(descriptor)
            if old_key is None or old_key == new_key:
                continue
            if ledger.holder_tokens(new_key):
                continue      # this phase already holds tokens of its own
            if (queue.item_path(pool.READY, new_key).exists()
                    or queue.item_path(pool.CLAIMED, new_key).exists()):
                continue      # its own copy is queued or running; let it finish
            event = adopt(queue, old_key=old_key, new_key=new_key,
                          consumer_action_key=consumer_key, tier_id=tier_id,
                          phase=str(phase["name"]),
                          range_start_bytes=int(phase["start_bytes"]),
                          range_end_bytes=int(phase["end_bytes"]),
                          residency_root=root)
            events.append(event)
            if event.get("adopted"):
                index.pop(descriptor, None)
    return events


def window_pressure(
    queue: pool.PoolQueue, *, tiers: Mapping[str, Mapping[str, object]],
    consumers: list | None = None,
) -> dict[str, int]:
    """Per tier, the GiB a live window needs and the tier does not have free.

    "The tier needs the tokens", measured rather than timed: the next phase a
    live consumer's window would publish is the next thing that will ask for
    capacity, and its ``stage_gib`` is what the tier must be able to offer.
    The maximum across consumers rather than the sum, because they are served
    one at a time and evicting for the sum would take back more than anything
    is waiting for.  *Would publish*, not *has not staged*: since #632 the
    window also declines phases its run-ahead bound covers, and an orphan
    evicted for one of those would be evicted for room nobody asks for.

    A tier no live window is waiting on is absent from the answer, and an
    orphan there stays resident -- held, counted, and ready for the next
    artifact that names it.
    """

    need: dict[str, int] = {}
    if consumers is None:
        consumers = _planned_consumers(queue, tiers)
    for _key, consumer, plan, tier_id in consumers:
        already, staged = _mover_state(queue, plan, tier_id)
        accepted = consumer["accepted_phase"]
        # Asked of the window rather than of the plan (#632).  A phase the
        # run-ahead bound has already declined is not something the tier needs
        # tokens for, and reporting it as pressure would evict a resident range
        # to make room nobody is going to use -- a stall no eviction can
        # relieve, which is a deadlock of a different shape.  So the question
        # is "would the window publish this if the room existed", and the way
        # to ask it is to run the same decision with the room.
        capacity = queue.tier_ledger(tier_id).capacity().get(
            storage_tiers.capacity_kind_of(tier_id), 0)
        ahead = residency_plan.remaining(plan, accepted)          # type: ignore[arg-type]
        waiting = [phase for phase in ahead
                   if str(phase["mover_row"]["action_key"]) in already - staged]
        if waiting:
            # A mover already in ``ready/`` or ``claimed/`` that holds no
            # tokens is the plainest form of "the tier needs the tokens": it
            # is queued and cannot be admitted.  The window will not offer it
            # again -- it counts as published -- so asking the window what it
            # would publish next would step straight over it.
            need[tier_id] = max(need.get(tier_id, 0),
                                int(waiting[0]["stage_gib"]))
            continue
        unbounded = sum(int(phase["stage_gib"]) for phase in ahead)
        decision = residency_plan.window(
            plan, accepted_phase=accepted,                       # type: ignore[arg-type]
            free_gib=unbounded, capacity_gib=int(capacity),
            published=sorted(already), staged=sorted(staged))
        wanted = decision["publish"]
        assert isinstance(wanted, list)
        if not wanted:
            continue
        need[tier_id] = max(need.get(tier_id, 0), int(wanted[0]["stage_gib"]))
    return need


def residency_window(queue: pool.PoolQueue, *, tiers: Mapping[str, Mapping[str, object]],
                     now: float | None = None) -> list[dict[str, object]]:
    """Publish the next movers, retire the consumed ones, recompose the maps.

    This is the coordinator half of the decomposition contract: the submitter
    froze every child and this publishes them, so no admitted action ever
    publishes work.  It runs here because the tier loop already holds the two
    things the decision needs -- the queue and the tier ledger -- and adding a
    second loop would mean two boxes deciding one stage's occupancy.
    """

    published: list[dict[str, object]] = []
    for consumer in live_consumers(queue):
        key = str(consumer["action_key"])
        refusals: list[Exception] = []
        plan = residency_plan.read(queue, key, on_unreadable=refusals.append)
        if plan is None:
            if not refusals:
                continue      # no plan filed: this consumer is nobody's to stage
            # A plan this reader refuses is a denial, and it used to be a
            # ``continue`` with nothing behind it.  #609 added ``demand_source``
            # to the plan's key set; a tier loop two generations old refused
            # every plan carrying it and skipped its consumer every cycle, so
            # the GLM run stage sat ready for 25 minutes behind a staged head
            # window with an idle GPU and a log that said only ``tier-cycle``.
            # Said twice on purpose, because two different people read them:
            # the event for whoever is watching this box, and the claim denial
            # for whoever runs ``pbstatus`` from anywhere.
            error = repr(refusals[0])
            published.append({"event": "plan-unreadable", "consumer": key,
                              "error": error})
            item = consumer.get("item")
            if isinstance(item, Mapping):
                queue.record_denial(item, "residency_plan_unreadable",
                                    {"error": error})
            continue
        tier_id = str(plan["tier_id"])
        if tier_id not in tiers:
            # Another box's stage.  Its own tier loop owns that ledger and will
            # publish this window; two loops minting one tier's occupancy is
            # the thing the tier id exists to prevent.
            continue
        already, staged = _mover_state(queue, plan, tier_id)
        ledger = queue.tier_ledger(tier_id)
        kind = storage_tiers.capacity_kind_of(tier_id)
        free = ledger.available().get(kind, 0)
        # The minted total, not the free remainder: the run-ahead bound of a
        # rolling window is a fraction of the tier, and a bound read off what
        # is free would shrink as the window it is bounding fills it.
        capacity = ledger.capacity().get(kind, 0)
        decision = residency_plan.window(
            plan, accepted_phase=consumer["accepted_phase"],  # type: ignore[arg-type]
            free_gib=int(free), capacity_gib=int(capacity),
            published=sorted(already), staged=sorted(staged))
        stall = decision["stall"]
        if isinstance(stall, Mapping):
            # Said here rather than nowhere: the incident this bound exists to
            # prevent was invisible for hours because the only thing a stalled
            # window printed was ``tier-cycle``.  Not a claim denial -- the
            # consumer is not denied, it is running and reporting nothing.
            published.append({"event": "window-stalled", "consumer": key,
                              **{field: stall[field] for field in (
                                  "accepted_phase", "reading_phase",
                                  "blocked_phase", "blocked_gib", "runahead_gib",
                                  "runahead_budget_gib", "free_gib",
                                  "capacity_gib", "reason", "waiting_for")}})
        for phase in decision["publish"]:
            row = dict(phase["mover_row"])                 # type: ignore[arg-type]
            try:
                # A copy has no result to replay: published with recompute,
                # or a republished range is a cache hit that stages nothing.
                queue.publish(**row, recompute=True)
            except (pool.PoolContractError, OSError) as exc:
                published.append({"event": "mover-publish-failed", "consumer": key,
                                  "phase": phase["phase"], "error": repr(exc)})
                continue
            published.append({"event": "mover-published", "consumer": key,
                              "phase": phase["phase"],
                              "action_key": phase["mover_action_key"],
                              "stage_gib": phase["stage_gib"]})
        for phase in decision["evict"]:
            row = dict(phase["egress_row"])                # type: ignore[arg-type]
            egress_key = str(row["action_key"])
            if (queue.item_path(pool.READY, egress_key).exists()
                    or queue.item_path(pool.CLAIMED, egress_key).exists()):
                continue      # already asked; asking again would double the row
            try:
                queue.publish(**row, recompute=True)   # a deletion, likewise
            except (pool.PoolContractError, OSError) as exc:
                published.append({"event": "egress-publish-failed", "consumer": key,
                                  "phase": phase["phase"], "error": repr(exc)})
                continue
            published.append({"event": "egress-published", "consumer": key,
                              "phase": phase["phase"], "action_key": egress_key,
                              "mover": phase["mover_action_key"]})
        try:
            compose_map(queue, key)
        except (residency_map.ResidencyMapError, OSError) as exc:
            # A map that cannot be composed leaves the previous one in place
            # and the consumer on the pool: slower, never wrong.
            published.append({"event": "map-compose-failed", "consumer": key,
                              "error": repr(exc)})
    return published


def sweep_orphans(queue: pool.PoolQueue,
                  tiers: Mapping[str, Mapping[str, object]],
                  *, pressure: Mapping[str, int] | None = None,
                  ) -> list[dict[str, object]]:
    """Take back the stage from movers no live consumer still plans to read.

    A consumer withdrawn between its movers finishing and its own claim would
    otherwise hold its ranges for the life of the fleet, because nothing
    publishes an egress for work nobody is waiting on.

    ``pressure`` is what turns "for the life of the fleet" into "until somebody
    needs the room" (#598).  An orphan's tokens are held the whole time it
    waits, so the ledger still counts every resident byte and nothing is
    admitted onto capacity that is not there -- the deferral is a *cache*, not
    the retained-but-unpinned state #598 refused.  The reconciliation inside
    the sweep is not deferred: bytes no key holds are not a cache, they are the
    accounting hole #608 closed.
    """

    stage_roots = {
        tier_id: str(record.get("mountpoint") or "")
        for tier_id, record in tiers.items()
        if record.get("tier") == "stage" and record.get("mountpoint")
    }
    if not stage_roots:
        return []
    return stage_release.sweep(queue, stage_roots=stage_roots, pressure=pressure)


def landed_and_in_flight(queue: pool.PoolQueue, tier_id: str, kind: str) -> tuple[int, int]:
    """Split a stage tier's held tokens into bytes on the dataset and bytes still coming.

    A stage tier's ``capacity_bytes`` is ZFS ``available``: what may still be
    written, net of every byte already on the dataset.  A mover takes its
    tokens at claim, before it has written anything, and keeps them past
    ``finish`` only once its receipt says the whole range landed
    (``residency_pin_holds``).  So held tokens are two different things:

    * **landed** -- a holder with a complete, unrefused receipt.  Its bytes are
      already subtracted from ``available``; adding them back is what keeps
      the supply from counting staged GiB twice (#621).
    * **in flight** -- a holder still copying, or one whose copy fell short and
      is about to release.  Its bytes are *not* yet in ``available``, and its
      tokens are exactly the room the window must not hand out again.

    Minting ``available + held`` treated both as landed and published one more
    window every cycle while the first was still copying: 2026-09-18, gen
    ``772d269c2164``, ten 82 GiB movers admitted against 275 GiB writable, all
    ten ENOSPC (#623).  ``available + landed`` is the supply; in-flight tokens
    stay a deduction until their bytes are on the dataset.
    """
    ledger = queue.tier_ledger(tier_id)
    landed = 0
    in_flight = 0
    for key in ledger.held_keys():
        tokens = int(ledger.holder_tokens(key).get(kind, 0))
        if tokens <= 0:
            continue
        receipt = queue.move_record(key)
        if (isinstance(receipt, Mapping) and receipt.get("complete") is True
                and not receipt.get("refusal")):
            landed += tokens
        else:
            in_flight += tokens
    return landed, in_flight


def cycle(
    queue: pool.PoolQueue,
    *,
    host: str,
    source_pool: str,
    receipts: ReceiptCache,
    now: float | None = None,
    discover=storage_tiers.discover_tiers,
) -> list[dict[str, object]]:
    """Discover, mint, announce; returns the records it announced."""

    fill_records = receipts.read([queue.root / pool.PREWARM, queue.root / MOVER_RECEIPTS])
    tiers = discover(host=host, source_pool=source_pool, fill_records=fill_records, now=now)
    ready: list[dict[str, object]] | None = None
    announced: list[dict[str, object]] = []
    for tier_id, record in sorted(tiers.items()):
        tokens = storage_tiers.tier_tokens(record)
        kind = storage_tiers.capacity_kind_of(tier_id)
        if (record.get("tier") == "stage" and kind in tokens
                and record.get("capacity_source") == storage_tiers.WRITABLE_CAPACITY_SOURCE):
            # ``capacity_bytes`` is what ZFS will still let a writer write,
            # net of the bytes already on the dataset.  Minting from
            # ``available`` alone counted every staged GiB twice and starved
            # the window at half the pool (#621); minting ``available + held``
            # counted a claimed mover's unlanded bytes as free and admitted ten
            # windows against one (#623).  The supply is what is writable plus
            # what has *landed*; ``landed_and_in_flight`` draws the line.
            landed, in_flight = landed_and_in_flight(queue, tier_id, kind)
            record["writable_gib"] = tokens[kind]
            record["held_gib"] = landed + in_flight
            record["landed_gib"] = landed
            record["in_flight_gib"] = in_flight
            record["capacity_basis"] = "zfs available + landed"
            tokens[kind] = tokens[kind] + landed
        record["fill_source"] = "measured" if storage_tiers.FILL_KIND in tokens else "none"
        record["fill_records"] = len(fill_records)
        # The probe rule, generalised from "nothing measured yet" to "nothing
        # has measured a ceiling yet".  ``max`` over receipts cannot by itself
        # let a second mover run -- a supply equal to the best single delivery
        # admits exactly the reader that produced it -- so while no receipt has
        # fallen short of the fill it reserved, the tier offers what the pool
        # has delivered *plus one more ready mover's own demand*, and the next
        # receipt decides.  If the pool kept up, the delivery observed rises
        # and so does the supply.  If it did not, that receipt is the measured
        # ceiling and the growth stops there.  Nothing here is a number: the
        # increment is a queued mover's sealed demand and the base is a
        # measurement.
        supply = storage_tiers.fill_supply_from_records(fill_records)
        record["fill_supply"] = {key: value for key, value in supply.items()
                                 if key != "ceiling_receipt"}
        if supply["ceiling_receipt"]:
            record["fill_ceiling_receipt"] = supply["ceiling_receipt"]
        if ready is None:
            ready = queue.ready_items()
        probe = probe_fill_demand(ready, tier_id)
        ceiling, best = supply["ceiling_mb_s"], supply["best_mb_s"]
        if ceiling is not None and int(ceiling) > 0:
            tokens[storage_tiers.FILL_KIND] = int(ceiling)
            record["fill_source"] = "measured-ceiling"
        elif best is not None and int(best) > 0:
            grown = int(best) + (probe or 0)
            tokens[storage_tiers.FILL_KIND] = grown
            record["fill_source"] = ("measured-growing" if probe
                                     else "measured")
            if probe:
                record["fill_probe_mb_s"] = probe
        elif probe is not None:
            tokens[storage_tiers.FILL_KIND] = probe
            record["fill_source"] = "probe"
            record["fill_probe_mb_s"] = probe
        else:
            tokens.pop(storage_tiers.FILL_KIND, None)
            record["fill_source"] = "none"
        record["tokens"] = tokens
        # What a movement node on this tier is run *with*, discovered on the
        # box that will run it.  A mover for the dl380g10 stage is sealed by a
        # submitter on an aarch64 Spark, whose ``sys.executable`` names a venv
        # that does not exist here and whose generation root is reached by a
        # path this box need not share; sealing either of them into the
        # mover's argv produces an action that cannot start on the only box it
        # can be placed on.  Announced beside ``mountpoint`` because it is the
        # same kind of fact: something about this box that a submitter would
        # otherwise have to guess.
        record["mover_python"] = MOVER_PYTHON
        record["mover_tools_root"] = MOVER_TOOLS_ROOT
        if (record.get("tier") == "stage" and record.get("mountpoint")
                and str(record.get("host") or "") == host):
            # This box's own stage is marked as this queue's before the tier
            # is announced, so the sweep below and every egress row sealed
            # against the announcement find the root owned (#628).  A stage
            # another box announces is that box's loop's to mark; a root that
            # is read-only here or already another queue's is announced with
            # the refusal on the record, and the sweep refuses on the same
            # fact rather than deleting under it.
            record["stage_root_owner"] = stage_release.register_stage_root(
                queue, tier_id=tier_id, stage_root=str(record["mountpoint"]))
        record["ledger"] = queue.mint_tier_capacity(tier_id, tokens)
        queue.announce_tier(record)
        announced.append(record)
    # Minting first, windowing second, on purpose: the window publishes what
    # the tier's *current* free capacity covers, so it must see this cycle's
    # supply rather than the last one's.
    announced_tiers = {str(record["tier_id"]): record for record in announced}
    # Adopt, then evict under pressure, then publish.  The order is the policy
    # (#598): a range a live consumer's window names is taken over rather than
    # deleted and re-copied, what is left over is deleted only when a window
    # cannot be placed without the room, and the window is published last so it
    # sees both -- the ranges it no longer has to stage and the capacity the
    # eviction just returned.
    # One walk of ``ready/`` and ``claimed/`` for both steps.  The plan is
    # frozen and an accepted phase moves in minutes, so the second reader of
    # this list is not reading anything stale; what changes between them is the
    # ledger, and both re-read that.
    planned = _planned_consumers(queue, announced_tiers)
    for event in adopt_resident_ranges(queue, tiers=announced_tiers,
                                       consumers=planned):
        print(json.dumps({"unix": time.time(), **event}), flush=True)
    pressure = window_pressure(queue, tiers=announced_tiers, consumers=planned)
    for event in sweep_orphans(queue, announced_tiers, pressure=pressure):
        print(json.dumps({"event": "stage-orphan-evicted", **event}), flush=True)
    for event in residency_window(queue, tiers=announced_tiers, now=now):
        print(json.dumps({"unix": time.time(), **event}), flush=True)
    # A tier this box announced before and no longer discovers is retired:
    # its free tokens go now, its held ones as their holders finish, and its
    # record says why it is empty rather than vanishing.
    suffix = f":{host}"
    for tier_id in queue.tier_ids():
        if tier_id in tiers or not tier_id.endswith(suffix):
            continue
        ledger = queue.mint_tier_capacity(tier_id, {})
        queue.announce_tier({
            "schema": storage_tiers.TIER_RECORD_SCHEMA_V1, "tier_id": tier_id, "host": host,
            "tier": "stage" if tier_id.startswith(storage_tiers.STAGE_POOL_PREFIX) else "arc",
            "capacity_bytes": 0, "retired": True, "ledger": ledger,
            "sampled_unix": time.time() if now is None else now,
        })
    return announced


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="mint and announce this box's storage tiers from what the box says",
    )
    parser.add_argument("--pool-root", default=str(prewarm_loop.SH / "pb-queue"),
                        help="the pull queue root this box's loops serve")
    parser.add_argument("--source-pool", default="storage_pool",
                        help="the ZFS pool the fleet export is served from; its "
                             "members and the receipts of reads off it are what "
                             "fill bandwidth is learned from")
    parser.add_argument("--interval-s", type=float, default=60.0,
                        help="seconds between discovery cycles; every quantity is "
                             "re-read each cycle")
    parser.add_argument("--once", action="store_true",
                        help="run one cycle, print the records as JSON and exit")
    args = parser.parse_args(argv)
    if args.interval_s <= 0:
        raise SystemExit("--interval-s must be positive")
    queue = pool.PoolQueue(Path(args.pool_root))
    queue.ensure_layout()
    host = socket.gethostname()
    receipts = ReceiptCache()
    loaded_commit = runtime_gate.loaded_runtime_commit()
    loaded_generation = runtime_gate._generation_at(runtime_gate.GENERATION_VERSION)

    def runtime_moved() -> bool:
        current = runtime_gate.published_commit()
        current_generation = runtime_gate._generation_at(runtime_gate.RUNTIME_VERSION)
        return bool((current and current != loaded_commit) or (
            loaded_generation and current_generation
            and loaded_generation != current_generation))

    while True:
        # Read at the top of the cycle, never inside one: every mutation this
        # loop makes is a single atomic rename, and the one composite -- the
        # map -- is recomposed from the fragments on disk each cycle, so the
        # boundary between two cycles is the only place there is nothing to
        # finish.  The supervisor's ``ensure_roles`` puts the replacement back
        # on the published generation on its next tick.
        if runtime_moved():
            print(json.dumps({
                "event": "tier-runtime-moved", "unix": time.time(), "host": host,
                "loaded": loaded_commit[:12] or "(unversioned)",
                "published": runtime_gate.published_commit()[:12],
            }), flush=True)
            return 75 if args.once else 0
        started = time.monotonic()
        try:
            records = cycle(queue, host=host, source_pool=args.source_pool, receipts=receipts)
        except (OSError, pool.PoolContractError) as exc:
            print(json.dumps({"event": "tier-cycle-failed", "error": repr(exc),
                              "unix": time.time()}), flush=True)
            records = []
        else:
            print(json.dumps({
                "event": "tier-cycle", "unix": time.time(), "host": host,
                "tiers": [{k: r.get(k) for k in ("tier_id", "tier", "capacity_bytes",
                                                    storage_tiers.FILL_RECORD_FIELD, "fill_source",
                                                    "fill_supply", "tokens")}
                          for r in records],
            }), flush=True)
        if args.once:
            print(json.dumps(records, indent=1, default=str))
            return 0
        time.sleep(max(0.0, args.interval_s - (time.monotonic() - started)))


if __name__ == "__main__":
    raise SystemExit(main())
