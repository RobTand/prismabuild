#!/usr/bin/env python3
"""Mint and announce this box's storage tiers from what the box says (#583, #582).

One loop per file-serving box, spawned by ``supervise.py`` under the
``tiers`` role.  Every cycle it discovers the tiers again -- the ARC from
``arcstats``, stage pools by the ``prismabuild-stage`` name prefix, the
ram tier by the tmpfs mounted at the policy's mountpoint (#640) -- learns
the pool's fill bandwidth from the receipts of reads off it, and makes each
tier's ledger say exactly that: ``mint_tier_capacity`` grows and shrinks the
token supply to the discovered number, so adding a device to the stage pool
offers more on the next cycle and exporting the pool offers nothing.  Rob,
2026-09-17: *"as the topology of my drives changes, prismabuild will be able
to automatically adapt."*

Nothing here is a capacity constant.  The two arguments name *which* pool
the export is served from and how often to look; every quantity is read --
and the ram tier's one declared file is read fresh every cycle too, so a
published policy change is picked up between cycles without a remount.

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
import os
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


def load_ram_policy() -> dict[str, object] | None:
    """The ram tier's declared sizing, read fresh on every cycle.

    The policy is a file in this loop's own directory, published with the
    runtime the way ``fleet_boxes.json`` is, so a change to it is a publish
    rather than an ssh: the next cycle reads the new numbers, mints from the
    mount's own ``statvfs`` again, and the window follows.  ``None`` -- no
    file, or one this reader refuses -- discovers no ram tier, exactly as a
    box with no tmpfs does.
    """

    here = Path(__file__).resolve().parent
    for candidate in (here / storage_tiers.RAM_POLICY_FILE,
                      here.parent / storage_tiers.RAM_POLICY_FILE):
        policy = storage_tiers.read_ram_policy(candidate)
        if policy is not None:
            return policy
    return None


def _prefill_depth(policy: Mapping[str, object] | None) -> int | None:
    """The policy's declared run-ahead cap, or ``None`` for the #633 semantics."""

    depth = (policy or {}).get("prefill_depth")
    if (isinstance(depth, int) and not isinstance(depth, bool) and depth > 0):
        return depth
    return None


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


def _ram_mover_state(queue: pool.PoolQueue, plan: Mapping[str, object],
                     ram_tier_id: str) -> tuple[set[str], set[str]]:
    """Which of a plan's promotions count as published, and which are in the tmpfs.

    The same ledger-answered question :func:`_mover_state` asks of the stage,
    asked of the ram tier: a promotion holding ``ram_gib`` is resident by
    definition -- held tokens equal bytes on the tmpfs at every instant --
    and a terminal promotion holding nothing counts as unpublished, so a
    reboot's ghost is republished rather than read as staged.
    """

    ledger = queue.tier_ledger(ram_tier_id)
    published: set[str] = set()
    staged: set[str] = set()
    for key in residency_plan.ram_mover_keys(plan):
        pinned = bool(ledger.holder_tokens(key))
        if pinned:
            staged.add(key)
        if (queue.item_path(pool.READY, key).exists()
                or queue.item_path(pool.CLAIMED, key).exists()
                or pinned):
            published.add(key)
    return published, staged


def drop_prior_ram_epochs(
        queue: pool.PoolQueue,
        tiers: Mapping[str, Mapping[str, object]]) -> list[dict[str, object]]:
    """Drop every ram fragment and ghost token the current epoch does not cover.

    tmpfs empties on reboot; the fragments and the ledger on the shared mount
    survive it.  Without this step the first cycle after a reboot would
    compose a map still naming ram paths whose bytes are gone, and the ledger
    would count tokens for ranges that no longer exist -- the new window
    starved by ghosts, which is the one failure the direction names
    ("starvation is the failure to avoid").

    The epoch each announced ram tier carries is the only one that counts.  A
    fragment read raw rather than through ``validate_fragment``, so a fragment
    that no longer validates -- a corrupt epoch, a hand-edit -- is dropped with
    the rest rather than lingering invisibly.  A held key whose receipt carries
    a different epoch, and which nothing has claimed, has tokens standing for
    bytes the reboot deleted; they come back here, because an egress never
    will.  A ram tier that is not announced at all has no current epoch, so
    every fragment of it is a prior one and every unclaimed holder is a ghost
    (#640).
    """

    current = {
        str(tier_id): str(record.get("epoch") or "")
        for tier_id, record in tiers.items()
        if record.get("tier") == "ram"}
    events: list[dict[str, object]] = []
    root = queue.residency_fragment_root()
    try:
        consumers = sorted(entry.name for entry in os.scandir(root)
                           if entry.is_dir())
    except OSError:
        consumers = []
    for consumer in consumers:
        try:
            names = sorted(entry.name for entry in os.scandir(root / consumer)
                           if entry.is_file() and entry.name.endswith(".json"))
        except OSError:
            continue
        for name in names:
            path = root / consumer / name
            try:
                with open(path) as stream:
                    raw = json.load(stream)
            except (OSError, ValueError):
                continue      # not ours to interpret; compose already skips it
            if not isinstance(raw, Mapping):
                continue
            tier_id = str(raw.get("tier_id") or "")
            if not tier_id.startswith(storage_tiers.RAM_TIER_PREFIX):
                continue
            epoch = str(raw.get("epoch") or "")
            live = current.get(tier_id, "")
            if live and epoch == live:
                continue
            path.unlink(missing_ok=True)
            events.append({"event": "ram-fragment-dropped", "tier_id": tier_id,
                           "consumer": consumer, "epoch": epoch or None,
                           "current_epoch": live or None})
    for tier_id in queue.tier_ids():
        if not tier_id.startswith(storage_tiers.RAM_TIER_PREFIX):
            continue
        live = current.get(tier_id, "")
        try:
            ledger = queue.tier_ledger(tier_id)
            held = sorted(ledger.held_keys())
        except (OSError, pool.PoolContractError):
            continue
        for key in held:
            if queue.item_path(pool.CLAIMED, key).exists():
                # A promotion claimed right now holds tokens for a copy that
                # is running; its receipt will date it, and the next cycle
                # judges it then.
                continue
            receipt = queue.move_record(key)
            epoch = (str(receipt.get("epoch") or "")
                     if isinstance(receipt, Mapping) else "")
            if live and epoch == live:
                continue
            released = queue.release_tier_reservations(key)
            events.append({"event": "ram-ghost-tokens-released",
                           "tier_id": tier_id, "holder": key,
                           "epoch": epoch or None, "released": released})
    return events


def release_incomplete_ram_promotions(
        queue: pool.PoolQueue,
        tiers: Mapping[str, Mapping[str, object]]) -> list[dict[str, object]]:
    """Evict ram promotions whose terminal receipt says they did not land (#644).

    A promotion that lands partially files a fragment for the landed subset
    while the ledger holds ``ram_gib`` for the *declared* range -- and then
    :func:`_ram_mover_state` counts the pinned-but-failed key as
    staged/published, so the window never republishes it, no egress fires for
    a phase the consumer has not passed, and the orphan sweep (which takes
    only orphans, and a live consumer's promotion is not one) never takes it
    back.  The half-landed range squats on its full-range tokens until its
    phase passes or an operator intervenes, and the window reports
    ``ram-window-stalled`` for room that is held but unusable.

    Refuse-and-release, the issue's first preference: a promotion key that is
    terminal (neither queued nor running) yet still holds tokens, whose move
    receipt says anything but a clean landing -- ``complete`` is not True, or
    a refusal or errors ride with it -- is evicted through the same
    read-delete-release :func:`stage_release.evict` an egress uses, so the
    partial files go before the tokens come back and the next window
    republishes the whole range through the ordinary publish path (no retry
    row needed: a terminal, unpinned key counts as unpublished).  The event
    carries the receipt's errors, which is the alert the issue asks for at
    minimum.

    Fail closed throughout: a key still queued or claimed is the window's or
    the copy's, not this step's, and a receipt read now would race the mover
    filing it; a key with no readable move receipt names bytes this step
    cannot date, so its tokens stay held; an evict that refuses or errors
    retains them too, and says so on a ``ram-mover-incomplete-retained``
    event the next cycle retries.
    """

    events: list[dict[str, object]] = []
    ram_roots = {
        str(tier_id): str(record.get("mountpoint") or "")
        for tier_id, record in tiers.items()
        if record.get("tier") == "ram" and record.get("mountpoint")
    }
    if not ram_roots:
        return events
    for tier_id, ram_root in ram_roots.items():
        try:
            held = sorted(queue.tier_ledger(tier_id).held_keys())
        except (OSError, pool.PoolContractError):
            continue
        for key in held:
            if (queue.item_path(pool.READY, key).exists()
                    or queue.item_path(pool.CLAIMED, key).exists()):
                # Queued or running: the window or the copy owns this key, and
                # a receipt read now would race the mover filing it.
                continue
            receipt = queue.move_record(key)
            if not isinstance(receipt, Mapping):
                # No terminal promotion receipt: a crash before filing holds
                # nothing past its reap, and anything else holding tokens here
                # names bytes this step cannot date.  Either way, not released.
                continue
            if str(receipt.get("tier_id") or "") != tier_id:
                continue
            receipt_errors = receipt.get("errors")
            if receipt_errors is None:
                receipt_errors = []
            elif not isinstance(receipt_errors, list):
                receipt_errors = [receipt_errors]
            if (receipt.get("complete") is True and not receipt.get("refusal")
                    and not receipt_errors):
                continue      # a clean landing: occupancy, not stranding
            consumer = str(receipt.get("consumer_action_key") or "")
            try:
                outcome = stage_release.evict(
                    queue, key, consumer_action_key=consumer,
                    stage_root=ram_root, reason="ram-mover-incomplete")
            except (OSError, ValueError, pool.PoolContractError) as exc:
                events.append({"event": "ram-mover-incomplete-retained",
                               "tier_id": tier_id, "mover": key,
                               "consumer": consumer or None,
                               "error": repr(exc)})
                continue
            assert isinstance(outcome, dict)
            evict_errors = outcome.get("errors")
            base = {"tier_id": tier_id, "mover": key,
                    "consumer": consumer or None,
                    "range_start_bytes": receipt.get("range_start_bytes"),
                    "range_end_bytes": receipt.get("range_end_bytes"),
                    "bytes_staged": receipt.get("bytes_staged"),
                    "receipt_errors": list(receipt_errors),
                    "entries_deleted": outcome.get("entries_deleted"),
                    "tokens_released": outcome.get("tokens_released")}
            if outcome.get("complete") is True and not evict_errors:
                events.append({"event": "ram-mover-incomplete-released",
                               **base})
            else:
                events.append({"event": "ram-mover-incomplete-retained",
                               **base, "evict_errors": list(evict_errors or [])})
    return events


def _stage_leg_rows(phase: Mapping[str, object],
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


def _stage_source_staged(phase: Mapping[str, object], start: int, end: int,
                         stage_staged: set[str]) -> bool:
    """Whether the stage already holds the bytes one promotion copies.

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


def _ram_leg_rows(phase: Mapping[str, object],
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


def _ram_window_state(
        queue: pool.PoolQueue, consumer: Mapping[str, object],
        plan: Mapping[str, object], tiers: Mapping[str, Mapping[str, object]],
        *, prefill_depth: int | None) -> dict[str, object] | None:
    """One consumer's ram decision inputs, or ``None`` when it has no ram leg.

    The stage window's own question, asked of the ram ledger: what fits, what
    the run-ahead bound covers, and -- the one bound that is a dependency
    rather than a size -- which phases' stage ranges have landed, because a
    promotion's source is the stage and nothing else.
    """

    ram_tier_id = plan.get("ram_tier_id")
    if not isinstance(ram_tier_id, str) or not ram_tier_id:
        return None
    if not residency_plan.ram_mover_keys(plan):
        return None
    record = tiers.get(ram_tier_id)
    if record is None or record.get("tier") != "ram":
        # Another box's ram tier, or none announced: its own loop owns that
        # ledger and will publish this window.
        return None
    if str(plan["tier_id"]) not in tiers:
        # The stage tier is another box's, and so is the ram tier that sits
        # in front of it.
        return None
    already, staged = _ram_mover_state(queue, plan, ram_tier_id)
    _stage_published, stage_staged = _mover_state(
        queue, plan, str(plan["tier_id"]))
    ledger = queue.tier_ledger(ram_tier_id)
    kind = storage_tiers.capacity_kind_of(ram_tier_id)
    free = int(ledger.available().get(kind, 0))
    capacity = int(ledger.capacity().get(kind, 0))
    decision = residency_plan.window(
        plan, accepted_phase=consumer["accepted_phase"],
        free_gib=free, capacity_gib=capacity,
        published=sorted(already), staged=sorted(staged),
        runahead_cap_gib=prefill_depth,
        # The two sets name promotion keys, so the decision must test
        # promotion keys: against stage keys its evict side would never fire
        # and its already-published skip would never skip (#640).
        mover_role="ram_mover_row")
    phases = {str(phase["name"]): phase for phase in plan["phases"]}
    publishable = []
    for entry in decision["publish"]:
        phase = phases.get(str(entry["phase"]))
        if phase is None:
            continue
        mover_row, _egress_row = _ram_leg_rows(phase, entry.get("chunk_index"))
        if mover_row is None:
            continue
        if not _stage_source_staged(
                phase, int(entry["start_bytes"]), int(entry["end_bytes"]),
                stage_staged):
            continue
        publishable.append((mover_row, entry))
    return {"ram_tier_id": ram_tier_id, "decision": decision,
            "phases": phases, "publishable": publishable,
            "already": already, "staged": staged,
            "stage_staged": stage_staged, "free_gib": free}


def ram_residency_window(
        queue: pool.PoolQueue, *, tiers: Mapping[str, Mapping[str, object]],
        now: float | None = None) -> list[dict[str, object]]:
    """Publish the next ram promotions, retire the consumed ones, first.

    The stage window's own semantics, pointed at the ram ledger: admission
    needs free ``ram_gib`` -- Rob's instinct, "empty space in tmpfs", made
    exact through the ledger -- bounded by the #633 run-ahead budget on the
    consumer's accepted progress, in the plan's read order.  The ram egress
    of a phase the consumer has passed is published here, *before* the stage
    window publishes its own, so on a box that runs them in queue order the
    tokens that bound the smaller tier come back before the bytes that feed
    it leave (#640).
    """

    events: list[dict[str, object]] = []
    ram_tiers = {tier_id: record for tier_id, record in tiers.items()
                 if record.get("tier") == "ram"}
    if not ram_tiers:
        return events
    depth = _prefill_depth(load_ram_policy())
    for consumer in live_consumers(queue):
        key = str(consumer["action_key"])
        plan = residency_plan.read(queue, key)
        if plan is None:
            continue      # a plan this reader refuses is reported once, below
        state = _ram_window_state(queue, consumer, plan, tiers,
                                  prefill_depth=depth)
        if state is None:
            continue
        ram_tier_id = str(state["ram_tier_id"])
        decision = state["decision"]
        stall = decision["stall"]
        if isinstance(stall, Mapping):
            events.append({
                "event": "ram-window-stalled", "consumer": key,
                **{field: stall[field] for field in (
                    "accepted_phase", "reading_phase", "blocked_phase",
                    "blocked_gib", "runahead_gib", "runahead_budget_gib",
                    "free_gib", "capacity_gib", "reason", "waiting_for")},
                "chunk_index": stall.get("chunk_index"),
                "tier_id": ram_tier_id})
        publishable = state["publishable"]
        ram_tier_record = tiers.get(ram_tier_id)
        if (isinstance(ram_tier_record, Mapping)
                and not _tier_admits_movers(ram_tier_record)):
            # The stage window's own rule, pointed at the tmpfs (#631): a
            # present-but-unregistered ram root admits no new promotions, the
            # ram egress below still publishes, and the deferral names its
            # refusal.  The tokens stay minted either way.
            if publishable:
                events.append({
                    "event": "ram-mover-publish-deferred-unregistered-root",
                    "consumer": key, "tier_id": ram_tier_id,
                    "stage_root_owner": ram_tier_record.get("stage_root_owner"),
                    "phases": [str(entry["phase"]) for _, entry in publishable
                               if isinstance(entry, Mapping)]})
            publishable = []
        for mover_row, entry in publishable:
            row = dict(mover_row)
            try:
                # A copy has no result to replay, for the same reason the
                # stage's own rows carry it.
                queue.publish(**row, recompute=True)
            except (pool.PoolContractError, OSError) as exc:
                events.append({"event": "ram-mover-publish-failed",
                               "consumer": key, "phase": entry["phase"],
                               "chunk_index": entry.get("chunk_index"),
                               "error": repr(exc)})
                continue
            events.append({"event": "ram-mover-published", "consumer": key,
                           "phase": entry["phase"],
                           "chunk_index": entry.get("chunk_index"),
                           "action_key": str(row["action_key"]),
                           "tier_id": ram_tier_id,
                           "ram_gib": int(entry["stage_gib"])})
        for entry in decision["evict"]:
            phase = state["phases"].get(str(entry["phase"]))
            mover_row, egress_row = (
                _ram_leg_rows(phase, entry.get("chunk_index"))
                if isinstance(phase, Mapping) else (None, None))
            if egress_row is None or mover_row is None:
                continue
            row = dict(egress_row)
            egress_key = str(row["action_key"])
            if (queue.item_path(pool.READY, egress_key).exists()
                    or queue.item_path(pool.CLAIMED, egress_key).exists()):
                continue      # already asked; asking again would double the row
            try:
                queue.publish(**row, recompute=True)   # a deletion, likewise
            except (pool.PoolContractError, OSError) as exc:
                events.append({"event": "ram-egress-publish-failed",
                               "consumer": key, "phase": entry["phase"],
                               "chunk_index": entry.get("chunk_index"),
                               "error": repr(exc)})
                continue
            events.append({"event": "ram-egress-published", "consumer": key,
                           "phase": entry["phase"],
                           "chunk_index": entry.get("chunk_index"),
                           "action_key": egress_key,
                           "mover": str(mover_row["action_key"]),  # type: ignore[index]
                           "tier_id": ram_tier_id})
    return events


#: What :func:`compose_map` last wrote per consumer, so a cycle whose inputs
#: did not move does not pay a read of every fragment plus an ``os.replace``
#: on the shared mount (#604).  Keyed by queue root and consumer; the value is
#: the fingerprint the map was written from and whether a map resulted.  The
#: loop is single-threaded, so no lock guards it.
_COMPOSE_FINGERPRINTS: dict[tuple[str, str], tuple[tuple[object, ...], bool]] = {}


def _compose_fingerprint(queue: pool.PoolQueue, consumer_action_key: str,
                         ram_tiers: Mapping[str, Mapping[str, object]] | None,
                         ) -> tuple[object, ...] | None:
    """What a consumer's next map is a function of, cheaply (#604).

    The fragment set with their mtimes -- fragments are written once by rename
    and never mutated, so (name, mtime, size) pins the content -- joined with
    the ram overlay's inputs (tier, root, epoch), because a remount lays the
    same fragments under a different header.  ``None`` when the directory
    cannot be read: that is "compose, not skip".
    """

    root = queue.residency_fragment_root()
    try:
        names = sorted(entry.name for entry in os.scandir(
            root / consumer_action_key)
            if entry.is_file() and entry.name.endswith(".json"))
    except OSError:
        return None
    stamped: list[tuple[object, ...]] = []
    for name in names:
        try:
            status = os.stat(root / consumer_action_key / name)
        except OSError:
            return None
        stamped.append((name, status.st_mtime_ns, status.st_size))
    overlay = tuple(sorted(
        (str(tier_id), str(record.get("mountpoint") or ""),
         str(record.get("epoch") or ""))
        for tier_id, record in (ram_tiers or {}).items()
        if record.get("tier") == "ram"))
    return (tuple(stamped), overlay)


def compose_map(queue: pool.PoolQueue, consumer_action_key: str, *,
                ram_tiers: Mapping[str, Mapping[str, object]] | None = None,
                ) -> Path | None:
    """Write one consumer's residency map from its movers' fragments.

    **The single writer.**  Movers write one fragment each, into a file only
    they name; nothing merges them but this loop, on one box, in one thread.
    A shared document with many writers cannot be merged by a rename, which is
    the only concurrency primitive this mount gives us, so the alternative was
    not a lock -- it was lost entries.

    Recomposed when the inputs move rather than every cycle (#604): a mover
    adds a fragment when it finishes, an egress removes one when it deletes
    the bytes, and a map that still named an evicted range would send the
    consumer to a path that is gone.  A consumer whose fragment set and mtimes
    -- and ram overlay inputs -- are unchanged since the map last written
    from them keeps that map: the rewrite would be byte-identical, and at one
    loop every 60 s over every live consumer the avoided cost is a read of
    every fragment plus an ``os.replace`` on the shared mount per consumer
    per cycle.  Anything unreadable composes rather than skips, and a map
    that went missing under an unchanged fingerprint is rewritten.

    A consumer's ram fragments are laid **over** the stage map rather than
    composed into it (#640): ``compose`` refuses fragments that disagree
    about the tier, and the ram tier's job is to serve the entries the stage
    already vouched for.  Only fragments carrying the epoch the ram tier
    announces now are laid -- the drop below removed the others, and this is
    the belt to that braces.
    """

    root = queue.residency_fragment_root()
    path = queue.residency_map_path(consumer_action_key)
    cache_key = (str(queue.root), consumer_action_key)
    fingerprint = _compose_fingerprint(queue, consumer_action_key, ram_tiers)
    if fingerprint is None:
        # Unreadable inputs: compose rather than skip, and drop any memory of
        # what was last written -- skipping against it afterwards could serve
        # a decision made while the inputs could not be read.
        _COMPOSE_FINGERPRINTS.pop(cache_key, None)
    else:
        cached = _COMPOSE_FINGERPRINTS.get(cache_key)
        if cached is not None and cached[0] == fingerprint:
            present = path.exists()
            if cached[1] == present:
                # Same fragments, same overlay inputs, same map outcome as the
                # last write: the rewrite would be byte-identical.
                return path if present else None
            # The map appeared or vanished under an unchanged fingerprint;
            # fall through and recompose rather than trust the memory.
    fragments = residency_map.read_fragments(root, consumer_action_key)
    stage_fragments = [
        fragment for fragment in fragments
        if not str(fragment.get("tier_id", "")).startswith(
            storage_tiers.RAM_TIER_PREFIX)]
    ram_fragments = [
        fragment for fragment in fragments
        if str(fragment.get("tier_id", "")).startswith(
            storage_tiers.RAM_TIER_PREFIX)]
    if not stage_fragments:
        # Nothing staged (yet, or any more).  Removing the map is what puts the
        # consumer back on the pool; leaving a stale one would point it at
        # deleted files, which reads as corruption rather than as a cache miss.
        path.unlink(missing_ok=True)
        if fingerprint is not None:
            _COMPOSE_FINGERPRINTS[cache_key] = (fingerprint, False)
        return None
    mapping = residency_map.compose(stage_fragments)
    if ram_fragments:
        tier_id = str(ram_fragments[0]["tier_id"])
        record = (ram_tiers or {}).get(tier_id)
        if (record is not None and record.get("tier") == "ram"
                and isinstance(record.get("epoch"), str)):
            epoch = str(record["epoch"])
            live = [fragment for fragment in ram_fragments
                    if str(fragment.get("epoch") or "") == epoch]
            if live:
                mapping = residency_map.overlay_ram(
                    mapping, live, ram_tier_id=tier_id,
                    ram_root=str(record.get("mountpoint") or ""),
                    ram_epoch=epoch)
    result = residency_map.write_map(path, mapping)
    if fingerprint is not None:
        _COMPOSE_FINGERPRINTS[cache_key] = (fingerprint, True)
    return result


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


def withdraw_dead_consumer_movers(queue: pool.PoolQueue) -> list[dict[str, object]]:
    """Withdraw the still-queued movers of consumers that already failed (#620).

    A consumer that fails with movers published leaves them running for
    nobody: the egress evicts the completed ones, but the not-yet-run ones
    stage hundreds of GiB for a consumer already in ``failed/`` -- and on
    stage paths shared with the successor's movers, their writes collide.
    So in the same egress cycle that evicts the dead consumer's resident
    ranges, its movers still in ``ready/`` or ``claimed/`` are withdrawn:
    the queued ones never start, the claimed ones are stopped, and both
    leave whatever partials they landed to the sweep and the reconciliation.

    Three things are never touched.  A mover with a complete receipt is a
    resident range, which the successor adopts rather than recopies.  An
    egress row is cleanup, not staging, and still has to run.  And a consumer
    key also present in ``ready/`` or ``claimed/`` was resubmitted: the
    withdrawal names a generation, not a key for all time, and the new
    generation's movers are live work.  A plan this reader refuses is hands
    off for the same reason -- without it no mover can be attributed -- and
    a withdrawal the queue refuses is reported, never forced: ambiguous
    state fails closed and the bytes stay bounded by the sweep.
    """

    events: list[dict[str, object]] = []
    for state in (pool.FAILED, pool.WITHDRAWN):
        try:
            paths = list(pool._scan(queue.dir(state)))
        except OSError:
            continue
        for path in paths:
            name = path.name
            key = name[:-len(".json")] if name.endswith(".json") else name
            if len(key) != 64:
                continue
            item = pool._read_json(path)
            if not isinstance(item, dict):
                continue
            if (queue.item_path(pool.READY, key).exists()
                    or queue.item_path(pool.CLAIMED, key).exists()):
                # Resubmitted under the same key: a new generation, live work.
                continue
            plan = residency_plan.read(queue, key)
            if plan is None:
                continue      # not a staged consumer, or an unreadable plan
            for mover_key in residency_plan.mover_keys(plan):
                receipt = queue.move_record(mover_key)
                if (isinstance(receipt, Mapping)
                        and receipt.get("complete") is True):
                    continue  # a resident range: adoption's, not withdrawal's
                if queue.item_path(pool.READY, mover_key).exists():
                    origin = pool.READY
                elif queue.item_path(pool.CLAIMED, mover_key).exists():
                    origin = pool.CLAIMED
                else:
                    continue  # finished or never published: nothing to stop
                try:
                    outcome = queue.withdraw(
                        mover_key, reason=f"consumer-{state}", by="tier-loop")
                except (pool.PoolContractError, OSError) as exc:
                    events.append({
                        "event": "dead-consumer-mover-withdraw-failed",
                        "consumer": key, "mover": mover_key, "state": origin,
                        "withdrawn": False, "error": repr(exc)})
                    continue
                done = outcome.get("status") in ("withdrawn", "already_withdrawn")
                events.append({
                    "event": "dead-consumer-mover-withdrawn",
                    "consumer": key, "mover": mover_key, "state": origin,
                    "withdrawn": bool(done), "status": outcome.get("status")})
    return events


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
          residency_root: Path,
          chunk_index: int | None = None) -> dict[str, object]:
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
        "chunk_index": chunk_index,
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
            # One adoption candidate per leg: a chunked phase's chunks are
            # adopted under their own ranges and keys (#675), because the
            # descriptor match proves the taken range equal to the range the
            # copy made resident -- a whole-phase range under a chunk key
            # would misattribute bytes the pin is checked against.
            legs = []
            chunks = phase.get("stage_chunks")
            if isinstance(chunks, list):
                for chunk in chunks:
                    if not isinstance(chunk, Mapping):
                        continue
                    mover = chunk.get("mover_row")
                    if not isinstance(mover, Mapping):
                        continue
                    legs.append((chunk.get("chunk_index"),
                                 str(mover.get("action_key")),
                                 chunk.get("start_bytes"),
                                 chunk.get("end_bytes")))
            elif isinstance(phase.get("mover_row"), Mapping):
                legs.append((None, str(phase["mover_row"]["action_key"]),  # type: ignore[index]
                             phase.get("start_bytes"), phase.get("end_bytes")))
            for chunk_index, new_key, cstart, cend in legs:
                if (isinstance(cstart, bool) or not isinstance(cstart, int)
                        or isinstance(cend, bool) or not isinstance(cend, int)):
                    continue
                if not (chunk_index is None
                        or (isinstance(chunk_index, int)
                            and not isinstance(chunk_index, bool))):
                    continue
                descriptor = _descriptor(digest, tier_id, cstart, cend)
                old_key = index.get(descriptor)
                if old_key is None or old_key == new_key:
                    continue
                if ledger.holder_tokens(new_key):
                    continue      # this leg already holds tokens of its own
                if (queue.item_path(pool.READY, new_key).exists()
                        or queue.item_path(pool.CLAIMED, new_key).exists()):
                    continue      # its own copy is queued or running; let it finish
                event = adopt(queue, old_key=old_key, new_key=new_key,
                              consumer_action_key=consumer_key, tier_id=tier_id,
                              phase=str(phase["name"]),
                              range_start_bytes=cstart,
                              range_end_bytes=cend,
                              residency_root=root,
                              chunk_index=chunk_index)
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
    depth = _prefill_depth(load_ram_policy())
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
        # The probe asks in legs, the way the window decides: a chunked
        # phase's chunks are what its movers will ask for one by one (#675).
        # A whole-phase leg is one leg over the phase's whole range, which
        # is what keeps this probe byte-identical to today beside chunks.
        legs: list[tuple[str, int]] = []
        for phase in ahead:
            chunks = phase.get("stage_chunks")
            if isinstance(chunks, list):
                for chunk in chunks:
                    if not isinstance(chunk, Mapping):
                        continue
                    mover = chunk.get("mover_row")
                    if not isinstance(mover, Mapping):
                        continue
                    legs.append((str(mover.get("action_key")),
                                 int(chunk.get("stage_gib", 0))))
            elif isinstance(phase.get("mover_row"), Mapping):
                legs.append((str(phase["mover_row"]["action_key"]),  # type: ignore[index]
                             int(phase.get("stage_gib", 0))))
        waiting = [key for key, _gib in legs if key in already - staged]
        if waiting:
            # A mover already in ``ready/`` or ``claimed/`` that holds no
            # tokens is the plainest form of "the tier needs the tokens": it
            # is queued and cannot be admitted.  The window will not offer it
            # again -- it counts as published -- so asking the window what it
            # would publish next would step straight over it.
            need[tier_id] = max(need.get(tier_id, 0),
                                next(gib for key, gib in legs
                                     if key == waiting[0]))
            continue
        unbounded = sum(gib for _key, gib in legs)
        decision = residency_plan.window(
            plan, accepted_phase=accepted,                       # type: ignore[arg-type]
            free_gib=unbounded, capacity_gib=int(capacity),
            published=sorted(already), staged=sorted(staged))
        wanted = decision["publish"]
        assert isinstance(wanted, list)
        if wanted:
            need[tier_id] = max(need.get(tier_id, 0), int(wanted[0]["stage_gib"]))
        # The ram leg asks the same question of the ram ledger (#640): the
        # next promotion the ram window would publish is the next thing that
        # will ask the tmpfs for room, and its GiB is what the sweep on that
        # tier must be able to offer.  Asked of the window rather than of
        # the plan (#642): a promotion the run-ahead bound has already
        # declined is not something the tier needs tokens for, and reporting
        # it as pressure would evict a resident range to make room nobody is
        # going to use -- the same deadlock shape #632 closed on the stage
        # side, one tier up.  So the question is "would the ram window
        # publish this if the room existed", and the way to ask it is to run
        # the same decision with the room, for this leg's own mover role,
        # joined with the stage ranges that have landed (a promotion's
        # source is the stage and nothing else, #640).
        # Held-by-nobody bytes on a roof-limited tmpfs are ENOSPC waiting to
        # happen, so an orphan there becomes an eviction candidate the
        # moment this need exists.
        state = _ram_window_state(queue, consumer, plan, tiers,
                                  prefill_depth=depth)
        if state is None:
            continue
        ram_tier_id = str(state["ram_tier_id"])
        ram_kind = storage_tiers.capacity_kind_of(ram_tier_id)
        ram_capacity = int(queue.tier_ledger(ram_tier_id).capacity().get(
            ram_kind, 0))
        ram_ahead = [
            phase for phase in residency_plan.remaining(plan, accepted)  # type: ignore[arg-type]
            if "ram_mover_row" in phase or "ram_chunks" in phase]
        # The probe asks in legs, the way the window decides: a chunked
        # phase's chunks are what its promotions will ask for one by one.
        ram_room = 0
        for phase in ram_ahead:
            chunks = phase.get("ram_chunks")
            if isinstance(chunks, list):
                ram_room += sum(
                    int(chunk.get("stage_gib", 0))
                    for chunk in chunks if isinstance(chunk, Mapping))
            else:
                ram_room += int(phase.get("stage_gib", 0))
        ram_decision = residency_plan.window(
            plan, accepted_phase=accepted,                       # type: ignore[arg-type]
            free_gib=ram_room,
            capacity_gib=ram_capacity,
            published=sorted(state["already"]),                  # type: ignore[arg-type]
            staged=sorted(state["staged"]),                      # type: ignore[arg-type]
            runahead_cap_gib=depth, mover_role="ram_mover_row")
        ram_published = ram_decision["publish"]
        assert isinstance(ram_published, list)
        ram_phases = {str(phase["name"]): phase for phase in plan["phases"]}  # type: ignore[union-attr]
        ram_wanted = [
            entry for entry in ram_published
            if str(entry["phase"]) in ram_phases
            and _stage_source_staged(
                ram_phases[str(entry["phase"])],
                int(entry["start_bytes"]), int(entry["end_bytes"]),
                state["stage_staged"])]
        if ram_wanted:
            need[ram_tier_id] = max(need.get(ram_tier_id, 0),
                                    int(ram_wanted[0]["stage_gib"]))
    return need


def reclaim_failed_mover_partials(
        queue: pool.PoolQueue, consumers: list,
        pressure: Mapping[str, int]) -> list[dict[str, object]]:
    """Publish the egress row of a failed mover whose partials block the window (#627).

    A mover that ends without a complete receipt releases its tokens at
    ``finish`` -- but every entry it renamed into place before it failed is
    still on the dataset, still named by its fragment, and counted by no
    ledger token.  The window evicts only phases the consumer has read past
    and the sweep takes back only movers no live plan names, so nothing
    publishes an egress for these bytes -- while a republished recopy cannot
    land for want of the room they occupy.

    A terminal, unpinned mover that still names bytes in a fragment is
    therefore an eviction candidate whenever the window has no room for the
    next phase: its own egress row is published, which already handles "an
    earlier egress removed it" and returns no tokens when none are held.  No
    pressure, no reclaim -- the partials are a cache until a window cannot
    be placed -- and never from under a queued recopy, a complete receipt,
    or a concluded egress: the copy already running is the owner, a complete
    copy is adoption's or the sweep's, and a concluded egress that left the
    fragment behind refused rather than raced, which republishing would only
    repeat.  Each of those declines silently; only publications are events.
    """

    events: list[dict[str, object]] = []
    root = queue.residency_fragment_root()
    for consumer_key, _consumer, plan, tier_id in consumers:
        if int((pressure or {}).get(tier_id, 0) or 0) <= 0:
            continue
        try:
            ledger = queue.tier_ledger(tier_id)
        except (OSError, pool.PoolContractError):
            continue
        for phase in plan["phases"]:
            # One reclaim candidate per leg: a chunked phase's chunks fail
            # and free independently (#675), each through its own egress
            # node, while a whole-phase leg reclaims through the phase's.
            legs = []
            chunks = phase.get("stage_chunks")
            if isinstance(chunks, list):
                for chunk in chunks:
                    if not isinstance(chunk, Mapping):
                        continue
                    legs.append((chunk.get("chunk_index"),
                                 chunk.get("mover_row"),
                                 chunk.get("egress_row")))
            else:
                legs.append((None, phase.get("mover_row"),
                             phase.get("egress_row")))
            for chunk_index, mover_row, egress_row in legs:
                if not isinstance(mover_row, Mapping) or not isinstance(
                        egress_row, Mapping):
                    continue
                mover_key = str(mover_row.get("action_key") or "")
                egress_key = str(egress_row.get("action_key") or "")
                if not mover_key or not egress_key:
                    continue
                try:
                    pinned = bool(ledger.holder_tokens(mover_key))
                except (OSError, pool.PoolContractError):
                    continue
                if pinned:
                    continue
                if (queue.item_path(pool.READY, mover_key).exists()
                        or queue.item_path(pool.CLAIMED, mover_key).exists()):
                    continue      # its own recopy is queued or running; let it finish
                receipt = queue.move_record(mover_key)
                if receipt is not None and (
                        not isinstance(receipt, Mapping)
                        or receipt.get("complete") is True):
                    continue
                try:
                    with open(residency_map.fragment_path(
                            root, consumer_key, mover_key)) as stream:
                        fragment = residency_map.validate_fragment(
                            json.load(stream))
                except (OSError, ValueError):
                    continue      # names nothing readable: nothing to evict
                if not fragment["entries"]:
                    continue
                if (queue.item_path(pool.READY, egress_key).exists()
                        or queue.item_path(pool.CLAIMED, egress_key).exists()):
                    continue      # already asked; asking again would double the row
                if (queue.item_path(pool.DONE, egress_key).exists()
                        or queue.item_path(pool.FAILED, egress_key).exists()
                        or queue.item_path(pool.WITHDRAWN, egress_key).exists()):
                    continue      # concluded and the fragment is still there:
                                  # it refused rather than raced; do not spin
                try:
                    queue.publish(**dict(egress_row), recompute=True)
                except (pool.PoolContractError, OSError) as exc:
                    events.append({
                        "event": "failed-mover-egress-publish-failed",
                        "consumer": consumer_key, "phase": phase.get("name"),
                        "chunk_index": chunk_index,
                        "mover": mover_key, "action_key": egress_key,
                        "error": repr(exc)})
                    continue
                events.append({
                    "event": "failed-mover-egress-published",
                    "consumer": consumer_key, "phase": phase.get("name"),
                    "chunk_index": chunk_index,
                    "mover": mover_key, "action_key": egress_key,
                    "tier_id": tier_id})
    return events


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
        tier_record = tiers[tier_id]
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
                                  "capacity_gib", "reason", "waiting_for")},
                              "chunk_index": stall.get("chunk_index")})
        by_name = {str(entry["name"]): entry for entry in plan["phases"]
                   if isinstance(entry, Mapping)}
        publishable = decision["publish"]
        if not _tier_admits_movers(tier_record):
            # A root that is present but unregistered admits nothing more
            # (#631): the movers wait while the evict loop below still
            # publishes egress, and the deferral is said out loud with the
            # refusal that caused it.  The tokens stay minted, so the
            # window's own bound still reads the tier's real supply when
            # the root registers.
            if publishable:
                published.append({
                    "event": "mover-publish-deferred-unregistered-root",
                    "consumer": key, "tier_id": tier_id,
                    "stage_root_owner": tier_record.get("stage_root_owner"),
                    "phases": [str(entry["phase"]) for entry in publishable
                               if isinstance(entry, Mapping)]})
            publishable = []
        for entry in publishable:
            # The leg's own egress row, resolved off the plan rather than
            # the entry: publish entries carry their mover, evict entries
            # their egress, and a chunked phase's egress lives on its chunk.
            _mover, egress_row = _stage_leg_rows(
                by_name.get(str(entry["phase"]), {}),
                entry.get("chunk_index"))
            if isinstance(egress_row, Mapping):
                egress_key = str(egress_row["action_key"])  # type: ignore[index]
                if (queue.item_path(pool.READY, egress_key).exists()
                        or queue.item_path(pool.CLAIMED, egress_key).exists()):
                    # A reclaim published this leg's egress and it has not
                    # run yet (#627).  The egress frees device bytes, not
                    # ledger tokens, so the window would otherwise republish
                    # the recopy into a stage that is still full -- and the
                    # recopy would ENOSPC into the very room being made.  The
                    # mover waits; the egress deletes; the next cycle stages.
                    published.append({
                        "event": "mover-publish-deferred-for-egress",
                        "consumer": key, "phase": entry["phase"],
                        "chunk_index": entry.get("chunk_index"),
                        "mover": entry["mover_action_key"],
                        "egress": egress_key})
                    continue
            row = dict(entry["mover_row"])                   # type: ignore[arg-type]
            # A republished mover carries the fill price its dispatch sealed,
            # and the tier's supply may have sunk under it since (#706): a
            # claim above the minted total is ``never_fits_tier_capacity``,
            # the one denial no amount of waiting repairs, so an adopted
            # mover priced before a sink wedges forever.  Republished at the
            # tier's current offer instead.  Only the fill is repriced -- the
            # range's own occupancy is the manifest's arithmetic, not this
            # loop's to shrink -- and the copy's sealed argv still carries
            # the original number, which the fold reads as the reservation it
            # compares deliveries against; a shortfall against it re-sets the
            # ceiling at the pool's own delivery, which is the measurement
            # that ceiling exists to take.
            fill_kind = (f"{storage_tiers.FILL_KIND}"
                         f"{storage_tiers.TIER_DEMAND_SEPARATOR}{tier_id}")
            resources = row.get("resources")
            sealed_fill = (resources.get(fill_kind)
                           if isinstance(resources, dict) else None)
            if isinstance(sealed_fill, bool) or not isinstance(
                    sealed_fill, (int, float)):
                sealed_fill = None
            if sealed_fill is not None:
                offered = int(ledger.capacity().get(storage_tiers.FILL_KIND, 0))
                if int(sealed_fill) > offered:
                    row["resources"] = {**resources, fill_kind: offered}
                    published.append({
                        "event": "mover-repriced-to-tier-offer",
                        "consumer": key, "phase": entry["phase"],
                        "chunk_index": entry.get("chunk_index"),
                        "tier_id": tier_id,
                        "sealed_fill_mb_s": int(sealed_fill),
                        "offer_mb_s": offered})
            try:
                # A copy has no result to replay: published with recompute,
                # or a republished range is a cache hit that stages nothing.
                queue.publish(**row, recompute=True)
            except (pool.PoolContractError, OSError) as exc:
                published.append({"event": "mover-publish-failed", "consumer": key,
                                  "phase": entry["phase"],
                                  "chunk_index": entry.get("chunk_index"),
                                  "error": repr(exc)})
                continue
            published.append({"event": "mover-published", "consumer": key,
                              "phase": entry["phase"],
                              "chunk_index": entry.get("chunk_index"),
                              "action_key": entry["mover_action_key"],
                              "stage_gib": entry["stage_gib"]})
        for entry in decision["evict"]:
            row = dict(entry["egress_row"])                  # type: ignore[arg-type]
            egress_key = str(row["action_key"])
            if (queue.item_path(pool.READY, egress_key).exists()
                    or queue.item_path(pool.CLAIMED, egress_key).exists()):
                continue      # already asked; asking again would double the row
            try:
                queue.publish(**row, recompute=True)   # a deletion, likewise
            except (pool.PoolContractError, OSError) as exc:
                published.append({"event": "egress-publish-failed", "consumer": key,
                                  "phase": entry["phase"],
                                  "chunk_index": entry.get("chunk_index"),
                                  "error": repr(exc)})
                continue
            published.append({"event": "egress-published", "consumer": key,
                              "phase": entry["phase"],
                              "chunk_index": entry.get("chunk_index"),
                              "action_key": egress_key,
                              "mover": entry["mover_action_key"]})
        try:
            compose_map(queue, key, ram_tiers={
                tier_id: record for tier_id, record in tiers.items()
                if record.get("tier") == "ram"})
        except (residency_map.ResidencyMapError, OSError) as exc:
            # A map that cannot be composed leaves the previous one in place
            # and the consumer on the pool: slower, never wrong.
            published.append({"event": "map-compose-failed", "consumer": key,
                              "error": repr(exc)})
    return published


def reclaim_idle_rates(queue: pool.PoolQueue) -> list[dict[str, object]]:
    """Return the fill tokens of every holder that is not copying right now.

    A rate is held for the duration of a transfer.  ``keep_tier`` returns it
    at the point a copy ends, but three things can still leave one held: a
    mover killed between its last byte and its outcome, an adoption, which
    takes over a range's whole reservation although it moves no bytes, and
    every holder already on the ledger from before that fix.  The symptom is
    not subtle -- on ``prismabuild-stage:dl380g10`` on 2026-09-18, 506 of 635
    fill units were held by seven terminal or never-published keys, leaving
    129 against a fresh mover's demand of 188, so no mover could be admitted
    and every stage-fed consumer waited on a lead that could not land (#636).

    Claimed keys are left alone: that is exactly the reader whose rate is
    real.  Occupancy is never touched here, whatever state its holder is in --
    those bytes are on the device and an egress or the orphan sweep is what
    takes them back.
    """

    events: list[dict[str, object]] = []
    for tier_id in queue.tier_ids():
        try:
            ledger = queue.tier_ledger(tier_id)
            keys = ledger.held_keys()
        except (OSError, pool.PoolContractError):
            continue
        for key in sorted(keys):
            try:
                if queue.item_path(pool.CLAIMED, key).exists():
                    continue
                rates = {kind: count
                         for kind, count in ledger.holder_tokens(key).items()
                         if kind in pool.TIER_RATE_KINDS}
                if not rates:
                    continue
                released = ledger.release_kinds(key, pool.TIER_RATE_KINDS)
            except (OSError, pool.PoolContractError):
                continue
            events.append({"event": "tier-rate-reclaimed", "tier_id": tier_id,
                           "holder": key, "released": released, "rates": rates})
    return events


def sweep_orphans(queue: pool.PoolQueue,
                  tiers: Mapping[str, Mapping[str, object]],
                  *, pressure: Mapping[str, int] | None = None,
                  ) -> list[dict[str, object]]:
    """Take back the stage from movers no live consumer still plans to read.

    A consumer withdrawn between its movers finishing and its own claim would
    otherwise hold its ranges for the life of the fleet, because nothing
    publishes an egress for work nobody is waiting on.  The ram tier's
    orphans are eviction candidates on the same terms (#640): held-by-nobody
    bytes on a roof-limited tmpfs are ENOSPC waiting to happen, and a failed
    promotion's landed partials and a dead consumer's unclaimed promotions
    are exactly the ownership discipline the stage's own sweep enforces.

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
        if record.get("tier") in ("stage", "ram") and record.get("mountpoint")
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


def _same_host_chunk(
        tiers: Mapping[str, Mapping[str, object]], host: str) -> int | None:
    """The effective promotion chunk the ram tier on this host announces.

    One chunk family across tiers (#675): the stage record carries the same
    sizing the submitter cuts both legs with.  ``None`` when no ram tier is
    announced on this host, when its record predates the announcement, or
    when the sizing is not a positive whole GiB -- all of which seal
    whole-phase pairs, exactly as before.
    """

    for record in tiers.values():
        if (isinstance(record, Mapping) and record.get("tier") == "ram"
                and str(record.get("host") or "") == host):
            chunk = record.get("promotion_chunk_gib")
            if (isinstance(chunk, int) and not isinstance(chunk, bool)
                    and chunk > 0):
                return chunk
    return None


def _stage_root_present(mountpoint: object) -> bool:
    """Whether a stage root exists to be registered (#631).

    Registration never got a chance when the mountpoint is not there at all:
    a discovered dataset's mountpoint always exists, so a missing one is a
    fixture path or a discover anomaly, not a root withholding capacity.
    Only a root that is present but unregistered refuses admission.
    """

    return bool(mountpoint) and Path(str(mountpoint)).is_dir()


def _tier_admits_movers(tier_record: Mapping[str, object]) -> bool:
    """Whether the window may publish movers against this tier's record (#631).

    ``cycle`` stamps ``stage_root_admits: False`` on a tier whose root is
    present but owned by nobody this queue may write; every other record --
    registered, pre-registration, or a tier with no root at all -- admits as
    before.  Absent means admissible, so older announcements stay readable.
    """

    return tier_record.get("stage_root_admits") is not False


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

    # Before minting, so this cycle's announced supply and this cycle's window
    # both see the bandwidth a finished copy is no longer drawing (#636).
    for event in reclaim_idle_rates(queue):
        print(json.dumps({"unix": time.time(), **event}), flush=True)
    fill_records = receipts.read([queue.root / pool.PREWARM, queue.root / MOVER_RECEIPTS])
    # The ram tier's declared sizing, read fresh: a published policy change is
    # picked up between cycles without a remount, and a rare operator remount
    # is picked up by the statvfs read inside the same cycle (#640).
    ram_policy = load_ram_policy()
    # The floor guard's worker demand, read fresh like the policy: what this
    # host's own loops offer under capacity.mem_gb (#645).  Absent when no
    # loop has announced under this name, which the admission refuses on
    # rather than admitting a window beside demand it cannot see.
    worker_mem_gb = storage_tiers.read_worker_mem_gb(
        queue.root / pool.WORKERS, host)
    tiers = discover(host=host, source_pool=source_pool, fill_records=fill_records,
                     now=now, ram_policy=ram_policy,
                     worker_mem_gb=worker_mem_gb)
    # The records this box announced last cycle, read before this cycle
    # overwrites them: the ram tier's epoch is compared against its own
    # previous announcement, so a change is said once rather than inferred.
    earlier: dict[str, dict[str, object]] = {
        str(record.get("tier_id")): record for record in queue.tiers()}
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
        if record.get("tier") == "stage":
            # The second cache layer's precondition, announced with the tier
            # and refused out loud (#638).  A stage dataset whose
            # ``primarycache`` forbids data caching serves every consumer read
            # off the SSD -- 2402 MB/s against 10045 on the same file at the
            # same concurrency, measured 2026-09-18 -- so the warm is refused
            # on the record a submitter and a mover both read, and the refusal
            # is logged because a rebuilt pool inherits the default silently.
            # The tier is still announced: layer 1 works without layer 2, and
            # taking staging down to fix a cache setting would cost the
            # campaign the thing that does work.
            verdict = storage_tiers.stage_arc_eligibility(record)
            record["arc_warm"] = verdict
            if verdict["primarycache"] is not None and not verdict["eligible"]:
                print(json.dumps({
                    "event": "stage-primarycache-refused",
                    "unix": time.time(), "host": host, "tier_id": tier_id,
                    "dataset": record.get("dataset"),
                    "primarycache": verdict["primarycache"],
                    "reason": verdict["reason"],
                }), flush=True)
            # One chunk family across tiers (#675): the stage announces the
            # same effective promotion chunk the ram tier on this host
            # announces, so the submitter cuts both legs at the same size.
            # The sealer reads it off this record, never off its own box.
            chunk = _same_host_chunk(tiers, host)
            if chunk is not None:
                record["promotion_chunk_gib"] = chunk
        if record.get("tier") == "ram":
            # The tmpfs's own arithmetic, mirroring the stage's (#640):
            # ``capacity_bytes`` is statvfs ``f_bavail`` -- what the mount may
            # still hold -- so minting from it alone would count every landed
            # GiB twice exactly as ``available`` alone did (#621), and the
            # supply is what is writable plus what has *landed*, capped by the
            # policy's window.  In-flight tokens stay a deduction until their
            # bytes are in the tmpfs.  A refused mount mints nothing at all --
            # ``tier_tokens`` already withheld its ``ram_gib`` -- and the
            # refusal is logged here with every number it named, because a
            # mis-sized tmpfs is an operator's decision to change and a silent
            # one is a decision nobody can see.
            window = record.get("window_gib")
            if (kind in tokens and isinstance(window, int)
                    and not isinstance(window, bool) and window > 0):
                landed, in_flight = landed_and_in_flight(queue, tier_id, kind)
                record["writable_gib"] = tokens[kind]
                record["held_gib"] = landed + in_flight
                record["landed_gib"] = landed
                record["in_flight_gib"] = in_flight
                record["capacity_basis"] = (
                    "statvfs f_bavail + landed, capped by the policy window")
                tokens[kind] = min(tokens[kind] + landed, window)
            admission = record.get("ram_admission")
            if isinstance(admission, Mapping) and not admission.get("admissible"):
                print(json.dumps({
                    "event": "ram-admission-refused",
                    "unix": time.time(), "host": host, "tier_id": tier_id,
                    "ram_admission": admission,
                }), flush=True)
            previous = earlier.get(tier_id)
            previous_epoch = (str(previous.get("epoch") or "")
                              if isinstance(previous, Mapping) else "")
            if (previous is not None
                    and previous_epoch != str(record.get("epoch") or "")):
                # The one event an operator must never miss: every prior-epoch
                # fragment is about to be dropped, and every ghost token is
                # about to come back.
                print(json.dumps({
                    "event": "ram-epoch-changed",
                    "unix": time.time(), "host": host, "tier_id": tier_id,
                    "epoch": record.get("epoch"),
                    "previous_epoch": previous_epoch or None,
                }), flush=True)
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
        # The receipts a gated fold may price this tier from (#611): the
        # identity this cycle just discovered.  ``None`` -- stamped by no
        # generation, which is every record until this one -- folds every
        # usable receipt, exactly as before.
        tier_identity = record.get("pool_identity")
        supply = storage_tiers.fill_supply_from_records(
            fill_records, pool_identity=(
                tier_identity if isinstance(tier_identity, Mapping) else None))
        record["fill_supply"] = {key: value for key, value in supply.items()
                                 if key != "ceiling_receipt"}
        if supply["ceiling_receipt"]:
            record["fill_ceiling_receipt"] = supply["ceiling_receipt"]
        if ready is None:
            ready = queue.ready_items()
        probe = probe_fill_demand(ready, tier_id)
        ceiling, best = supply["ceiling_mb_s"], supply["best_mb_s"]
        # The probe rule, extended to a standing ceiling (#706).  Minting
        # exactly the ceiling closes the fold's own escape: movers are
        # admitted against fill tokens, so a tier at a ceiling paces every
        # later reservation at or under it, no delivery can exceed it, and
        # the refutation clause never fires -- while each paced-at-ceiling
        # mover that falls short sinks the ceiling further (111 MB/s, then
        # 65, live 2026-09-19).  When the fold can measure one reader's
        # worth above the ceiling, the tokens mint from that offer instead,
        # so the next mover admits above the ceiling: a delivery refutes it
        # and growth resumes, a shortfall re-sets it with a fresh probe.
        offer = supply.get("probe_offer_mb_s")
        if ceiling is not None and int(ceiling) > 0:
            if (isinstance(offer, (int, float))
                    and not isinstance(offer, bool)
                    and int(offer) > int(ceiling)):
                tokens[storage_tiers.FILL_KIND] = int(offer)
                record["fill_source"] = "measured-probing"
                record["fill_probe_mb_s"] = int(offer) - int(ceiling)
            else:
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
        if (record.get("tier") in ("stage", "ram") and record.get("mountpoint")
                and str(record.get("host") or "") == host):
            # This box's own stage -- and its own tmpfs -- are marked as this
            # queue's before the tier is announced, so the sweep below and
            # every egress row sealed against the announcement find the root
            # owned (#628).  A tier another box announces is that box's loop's
            # to mark; a root that is read-only here or already another
            # queue's is announced with the refusal on the record, and the
            # sweep refuses on the same fact rather than deleting under it.
            # The ram root carries the epoch marker beside this one; both are
            # the root's own identity, and ``reconcile`` skips them by name.
            record["stage_root_owner"] = stage_release.register_stage_root(
                queue, tier_id=tier_id, stage_root=str(record["mountpoint"]))
            if (record["stage_root_owner"] != "registered"
                    and _stage_root_present(record.get("mountpoint"))):
                # A root that is there but is not this queue's refuses new
                # movers -- and only that (#631).  The tokens stay minted:
                # refuse-and-keep, not refuse-and-remove.  Popping the
                # occupancy kind retired it to zero and turned every ``[]``
                # read of the ledger into a ``KeyError``, while the held
                # reservations the refusal exists to protect kept working.
                # A mountpoint that is not there at all is pre-registration,
                # not refusal: registration never got a chance to mark, so
                # the cycle mints as before and the record carries only the
                # owner it reported.  The refusal is loud on the record as
                # ``stage_root_owner`` with ``stage_root_admits`` False, and
                # said once on the log; the sweep below refuses on the same
                # fact, as it always has.
                record["stage_root_admits"] = False
                print(json.dumps({
                    "event": "stage-root-refuses-movers",
                    "unix": time.time(), "host": host, "tier_id": tier_id,
                    "stage_root": str(record["mountpoint"]),
                    "stage_root_owner": record["stage_root_owner"],
                }), flush=True)
        record["ledger"] = queue.mint_tier_capacity(tier_id, tokens)
        queue.announce_tier(record)
        announced.append(record)
    # Minting first, windowing second, on purpose: the window publishes what
    # the tier's *current* free capacity covers, so it must see this cycle's
    # supply rather than the last one's.
    announced_tiers = {str(record["tier_id"]): record for record in announced}
    # The epoch drop before anything reads a fragment: a prior epoch's range
    # is not resident, so the adoption, the pressure, the sweep and the maps
    # below all see a world that no longer contains it (#640).  The drop is
    # idempotent -- a steady cycle finds nothing to drop -- and it holds:
    # nothing reads as ram-resident until a promotion lands under the current
    # epoch.
    for event in drop_prior_ram_epochs(queue, announced_tiers):
        print(json.dumps({"unix": time.time(), **event}), flush=True)
    # Incomplete promotions before anything prices the tier (#644): a
    # half-landed promotion squats on its full-range tokens until its phase
    # passes, so its tokens come back here -- partial files deleted first,
    # through the egress's own read-delete-release -- and the adoption, the
    # pressure, the sweep and both windows below see the room and republish
    # the whole range through the ordinary publish path.
    for event in release_incomplete_ram_promotions(queue, announced_tiers):
        print(json.dumps({"unix": time.time(), **event}), flush=True)
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
    # Dead consumers' movers first: a consumer that failed with movers
    # published would otherwise keep staging for nobody all cycle (#620).
    # Withdrawing only stops queued work, so adoption below still sees every
    # resident range it could take.
    for event in withdraw_dead_consumer_movers(queue):
        print(json.dumps({"unix": time.time(), **event}), flush=True)
    for event in adopt_resident_ranges(queue, tiers=announced_tiers,
                                       consumers=planned):
        print(json.dumps({"unix": time.time(), **event}), flush=True)
    pressure = window_pressure(queue, tiers=announced_tiers, consumers=planned)
    # Failed movers' partials next: a terminal, unpinned mover that still
    # names bytes is an eviction candidate when the window has no room (#627).
    # Its egress rows land in ``ready/`` before the sweep runs, so the window
    # below sees both the room being made and the recopy it must hold back.
    for event in reclaim_failed_mover_partials(queue, planned, pressure):
        print(json.dumps({"unix": time.time(), **event}), flush=True)
    for event in sweep_orphans(queue, announced_tiers, pressure=pressure):
        print(json.dumps({"event": "stage-orphan-evicted", **event}), flush=True)
    # The ram window before the stage's, so a phase's ram egress is published
    # before its stage egress: the tokens that bound the smaller tier come
    # back first, and a ram range never outlives the stage range that feeds
    # it (#640).
    for event in ram_residency_window(queue, tiers=announced_tiers, now=now):
        print(json.dumps({"unix": time.time(), **event}), flush=True)
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
            "tier": storage_tiers.tier_kind_of(tier_id),
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
