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
from contextlib import ExitStack
import json
import math
from pathlib import Path
from collections.abc import Iterable, Mapping, Sequence
import os
import socket
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
from runtime_paths import generation_root  # noqa: E402

sys.path.insert(0, str(generation_root(__file__) / "src"))

from prismabuild import pool  # noqa: E402
from prismabuild import produced_output  # noqa: E402
from prismabuild import reader_lease  # noqa: E402
from prismabuild import residency_map  # noqa: E402
from prismabuild import residency_plan  # noqa: E402
from prismabuild import storage_tiers  # noqa: E402
from prismabuild import window_credit  # noqa: E402

import prewarm_loop  # noqa: E402
import stage_release  # noqa: E402
#: The same generation gate ``prewarm_loop`` reads, under the same name, for
#: the same reason: a loop holds the modules it imported for its whole life,
#: so a fix published under a running fleet reaches none of it (#615).
import worker_loop as runtime_gate  # noqa: E402

#: One definition, in the pool: ``stage_move.py`` writes these through
#: ``record_move`` and this loop reads them for the fill measurement.
MOVER_RECEIPTS = pool.MOVERS

#: Seconds between cycles; ``_serve`` sets it from ``--interval-s``.  A
#: window's refill horizon counts one cycle of latency (#903), and a caller
#: that drives :func:`cycle` directly gets the parser's default.
CYCLE_INTERVAL_S = 60.0

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
            claimed_unix = None
            reported_unix = None
            if state == pool.CLAIMED:
                claimed_unix = item.get("claimed_unix")
                if isinstance(claimed_unix, (int, float)):
                    observation = prewarm_loop.progress_phase(
                        queue, key, float(claimed_unix))
                    if observation is not None:
                        accepted = str(observation["phase"])
                        reported_unix = observation.get("reported_unix")
            # The record itself travels beside the key: ``record_denial`` is
            # keyed by an item's own ``published_unix`` generation, so a
            # coordinator that carried only the key could not file one.  The
            # claim time and the accepted phase's report time are the two
            # ends of the consumer's measured consumption (#903).
            out.append({"action_key": key, "state": state,
                        "accepted_phase": accepted, "item": item,
                        "claimed_unix": claimed_unix,
                        "reported_unix": reported_unix})
    return out


def _mover_state(queue: pool.PoolQueue, plan: Mapping[str, object],
                 tier_id: str) -> tuple[set[str], set[str]]:
    """Which of a plan's movers count as published, and which hold the tier.

    Both sets are *accounting*.  ``held`` names the movers whose tokens the
    tier ledger is still carrying -- the booking, taken at claim, which is
    what bounds occupancy and what an egress gives back.  It is what the
    window evicts on and what the advance fence counts, and neither question
    is "have the bytes arrived".

    That last question has its own answer, and it is deliberately not here:
    :func:`residency_plan.resident_movers` reads the filed publication
    evidence.  Issue #759 is what the split is for -- this function's old
    docstring said a mover holding tokens "is resident by definition", the
    RAM publication gate believed it, and the head promotion published
    against a copy that was still running and refused ``source-coverage-gap``
    80 times.  A reservation says the room is booked, never that the bytes
    are in it.

    ``published`` deliberately excludes a terminal mover that holds nothing:
    its key is a content hash, so the same manifest seals the same key on a
    second campaign, and a ``done`` record left over from an evicted range
    would otherwise be mistaken for a window that is already staged and never
    republished by anyone.
    """

    ledger = queue.tier_ledger(tier_id)
    published: set[str] = set()
    staged: set[str] = set()      # held: see the docstring, not residency
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
    """Which of a plan's promotions count as published, and which hold the tmpfs.

    The same ledger-answered question :func:`_mover_state` asks of the stage,
    asked of the ram tier, and with the same #759 caveat: ``held`` is the
    ``ram_gib`` booking, not a claim that the tmpfs has the bytes.  A
    terminal promotion holding nothing counts as unpublished, so a reboot's
    ghost is republished rather than read as staged; what the bytes are
    doing is :func:`residency_plan.resident_movers`' question, asked of the
    announced ram root under the announced epoch.
    """

    ledger = queue.tier_ledger(ram_tier_id)
    published: set[str] = set()
    staged: set[str] = set()      # held: see the docstring, not residency
    for key in residency_plan.ram_mover_keys(plan):
        pinned = bool(ledger.holder_tokens(key))
        if pinned:
            staged.add(key)
        if (queue.item_path(pool.READY, key).exists()
                or queue.item_path(pool.CLAIMED, key).exists()
                or pinned):
            published.add(key)
    return published, staged


def _resident_movers(
        queue: pool.PoolQueue, plan: Mapping[str, object], tier_id: str,
        tiers: Mapping[str, Mapping[str, object]],
) -> tuple[set[str], bool]:
    """One tier's published, complete, relevant coverage for this plan.

    The tier loop's side of the one shared readiness predicate that
    ``pbstatus`` also reports through, so the gate and the cursor cannot
    disagree (#759).  Distinct from :func:`_mover_state`'s ``held``: that is
    the reservation, this is the publication.

    Returns ``(resident, known)``.  Evidence that cannot be read answers
    ``(set(), False)``, which gates closed *and* keeps the distinction the
    cycle has to report: unknown readiness is not the same answer as a
    range that is honestly not there yet.  The cycle's own tier census is
    passed through, so the ram leg's announced root and epoch cost no extra
    read.
    """

    try:
        return residency_plan.resident_movers(
            queue, plan, tier_id, tier_record=tiers.get(tier_id)), True
    except (OSError, pool.PoolContractError, ValueError):
        return set(), False


def _withdrawn_keys(queue: pool.PoolQueue,
                    withdrawn: frozenset[str] | None = None) -> frozenset[str]:
    """The live withdrawal markers, read once per cycle unless the caller has.

    Enumerated rather than stat-ed, for ``withdrawn_keys``' own NFS reason; a
    cycle passes one snapshot to every step that needs it so the steps cannot
    disagree about which keys were cancelled while it ran.
    """

    return queue.withdrawn_keys() if withdrawn is None else frozenset(withdrawn)


def _operator_withdrawal(queue: pool.PoolQueue, key: str) -> bool:
    """Whether one live marker is an operator's decision, not a handoff's.

    Admission preempts a background holder through the same withdrawal
    ladder, then republishes it immediately with ``supersedes_withdrawal``
    naming the cancellation.  That plan must survive -- the requeue is the
    same work -- while an operator's decision has no successor and retires
    the window it was made against (#708).  The immutable decision carries
    ``preempted_by`` when admission made it, so the marker record answers.

    A membership handoff answers the same way through its own durable
    identity: ``pool.withdraw`` persists ``membership_handoff`` only after
    proving the live claim still is the planned attempt with restart
    permission, remaining budget and existing lineage.  The window reads
    that exact decision back here through the one shared carrier check
    (owner, attempt, budget and generation all typed and all equal to
    the marker's own), so the plan's later phases keep staging through
    the settlement interval.  A supervisor-shaped ``withdrawn_by`` with
    no (or a mismatched) proof is an ordinary cancellation and still
    retires the window.
    """

    try:
        marker = pool._read_json(queue.item_path(pool.WITHDRAWN, key))
    except (OSError, pool.PoolContractError):
        return False      # unreadable: not a decision this cycle acts on
    if not isinstance(marker, Mapping) or marker.get("preempted_by"):
        return False
    return not pool.membership_handoff_authorized(marker)


def _ram_credit_state(queue, tier_id, holder, *, current_epoch, locks):
    """Positive unconsumed-credit proof; locks live through caller's decision.

    Blind holder names only discover possible consumers. A fresh frozen plan,
    live queue row and exact current advance authorize retention. Bound credit
    additionally proves the mover publication and existing funding binding.
    """
    blind = holder.startswith(window_credit.GRANT_PREFIX)
    funding = None
    if blind:
        prefix = holder[len(window_credit.GRANT_PREFIX):].split("-", 1)[0]
        if len(prefix) != 16 or any(c not in "0123456789abcdef" for c in prefix):
            return "none", "malformed blind-grant name"
        candidates = [p.stem for p in pool._scan(queue.root / pool.RESIDENCY_PLANS)
                      if p.suffix == ".json" and len(p.stem) == 64
                      and p.stem.startswith(prefix)]
        if len(candidates) > 1:
            return "unknown", "ambiguous blind-grant consumer"
        if not candidates:
            return "none", "no filed consumer plan"
        consumer = candidates[0]
    else:
        status, funding, reason = queue.read_funding_evidence(holder, tier_id)
        if status == "unknown":
            return "unknown", reason
        if funding is None or funding["state"] not in ("reserved", "transferring"):
            if not locks.enter_context(queue.mover_transition_lock(holder, blocking=False)):
                return "unknown", "mover transition busy"
            return "none", "not unconsumed credit"
        consumer = str(funding["consumer_action_key"])
    if not locks.enter_context(queue.mover_transition_lock(consumer, blocking=False)):
        return "unknown", "consumer transition busy"
    if not blind and not locks.enter_context(
            queue.mover_transition_lock(holder, blocking=False)):
        return "unknown", "mover transition busy"
    live, error = residency_plan.live_state(queue, consumer)
    if error:
        return "unknown", error
    if live is None:
        return "none", "consumer is terminal or vanished"
    refused = []
    plan, _incarnation = residency_plan.read_filed(
        queue, consumer, on_unreadable=refused.append)
    if refused:
        return "unknown", str(refused[0])
    if plan is None or plan.get("ram_tier_id") != tier_id:
        return "none", "no matching frozen RAM plan"
    marker = residency_plan.superseded(queue, plan)
    if marker:
        return ("unknown", "retirement marker unreadable") if marker.get("unreadable") else (
            "none", "plan superseded")
    if not current_epoch:
        return "none", "RAM tier has no current epoch"
    item = pool._read_json(queue.item_path(live, consumer))
    if not isinstance(item, dict) or item.get("action_key") != consumer:
        return "unknown", "live consumer row unreadable or divergent"
    if blind:
        accepted = None
        if live == pool.CLAIMED:
            observed = prewarm_loop.progress_phase(queue, consumer, float(item["claimed_unix"]))
            if observed is None:
                # The tolerant progress reader also returns None for a
                # corrupt/missing lease. Destruction must not mistake that
                # uncertainty for proof of the initial frontier.
                return "unknown", "claimed progress is not positively available"
            accepted = str(observed["phase"])
        already, held = _ram_mover_state(queue, plan, tier_id)
        rowed = [key for key in residency_plan.ram_mover_keys(plan)
                 if queue.item_path(pool.READY, key).exists()]
        needs = residency_plan.advance_needs(
            plan, accepted, published=sorted(already), staged=sorted(held),
            rowed=rowed, mover_role="ram_mover_row")
        target = needs.get("fence_target")
        if target is None or holder != window_credit.grant_key(
                consumer, tier_id, "ram_mover_row", target["phase"], target["chunk_index"]):
            return "none", "holder is not the current frozen advance"
        mover = str(target["mover_action_key"])
        demand = int(target["stage_gib"])
    else:
        mover = holder
        leg = residency_plan.find_mover_leg(plan, mover)
        if leg is None or leg["mover_role"] != "ram_mover_row":
            return "none", "funding mover is outside frozen RAM plan"
        demand = int(leg["stage_gib"])
    if not locks.enter_context(queue.mover_transition_lock(mover, blocking=False)):
        return "unknown", "mover transition busy"
    status, record, reason = queue.read_funding_evidence(mover, tier_id)
    if status == "unknown":
        return "unknown", reason
    if funding is not None and (record is None or
            (record["consumer_action_key"], record["generation"]) !=
            (funding["consumer_action_key"], funding["generation"])):
        return "unknown", "funding rotated during qualification"
    if blind and record is None:
        tokens = queue.tier_ledger(tier_id).holder_tokens(holder)
        if tokens == {"ram_gib": demand}:
            return "live", "exact blind advance"
        return "unknown", "partial or divergent blind holdings"
    if record is None:
        return "unknown", "funding disappeared during qualification"
    if (record["state"] not in ("reserved", "transferring")
            or record["consumer_action_key"] != consumer
            or record["plan_sha256"] != residency_plan.plan_sha256(plan)
            or record["mover_action_key"] != mover
            or record["tier_id"] != tier_id or record["kind"] != "ram_gib"):
        return "none", "funding is not this unconsumed plan credit"
    leg = residency_plan.find_mover_leg(plan, mover)
    if (leg is None or leg["mover_role"] != "ram_mover_row"
            or (record["range_start_bytes"], record["range_end_bytes"]) != (
                leg["start_bytes"], leg["end_bytes"])):
        return "none", "funding range differs from frozen leg"
    row = pool._read_json(queue.item_path(pool.READY, mover))
    if not isinstance(row, dict):
        return "unknown", "funded mover publication unavailable"
    if row.get("published_unix") != record["published_unix"]:
        return "none", "funding publication is stale"
    if blind:
        names = {p.name for p in pool._glob(
            queue.tier_ledger(tier_id).held_dir / holder, "*-*")}
        if (record["state"] == "reserved" and len(record["tokens"]) == demand
                and all(name.startswith("ram_gib-") and name in names
                        for name in record["tokens"])):
            return "live", "bound grant before transfer"
    else:
        covered, _generation = queue.funded_cover(tier_id, row, "ram_gib", demand)
        if covered == demand:
            return "live", "bound mover before claim"
    return "unknown", "unconsumed funding has incomplete token evidence"


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
                continue  # Positive current material needs no credit census.
            with ExitStack() as locks:
                try:
                    # The current advance depends on real holdings. Keep
                    # those stable until retaining/releasing this holder.
                    # Every transition lock below is nonblocking, so a
                    # claim holding mover->mint makes us defer, never wait
                    # in the reverse order. Only this tier is released
                    # under the guard, reentrantly on the mint already
                    # held; other tiers are swept after it drops.
                    if not locks.enter_context(
                            queue.tier_mint_lock(tier_id, blocking=False)):
                        credit, reason = "unknown", "tier mint busy"
                    else:
                        credit, reason = _ram_credit_state(
                            queue, tier_id, key, current_epoch=live, locks=locks)
                except (OSError, pool.PoolContractError, ValueError, KeyError) as exc:
                    credit, reason = "unknown", str(exc)
                if credit != "none":
                    if credit == "unknown":
                        events.append({"event": "ram-credit-cleanup-deferred",
                                       "tier_id": tier_id, "holder": key,
                                       "reason": reason})
                    continue
                if queue.item_path(pool.CLAIMED, key).exists():
                    continue
                receipt = queue.move_record(key)
                epoch = (str(receipt.get("epoch") or "")
                         if isinstance(receipt, Mapping) else "")
                if live and epoch == live:
                    continue
                # This tier only, and under the guard that proved the
                # holder reclaimable: a tier ledger's mutation guard *is*
                # that tier's mint lock, so releasing every tier here
                # would block on another tier's mint while holding this
                # one -- an order nothing else in the tree takes, and one
                # the "mint is a leaf" analysis does not cover.
                try:
                    released = ledger.release(key)
                except (OSError, pool.PoolContractError):
                    released = 0
            # Guard dropped.  A holder reaches one tier, so the rest is
            # normally a no-op; sweep it as a leaf anyway, exactly as
            # ``stage_release._evict_owned`` does after its own mint
            # section, so a holder that somehow reached two tiers still
            # leaves neither behind.
            for other_tier in queue.tier_ids():
                if other_tier == tier_id:
                    continue
                try:
                    released += queue.tier_ledger(other_tier).release(key)
                except (OSError, pool.PoolContractError, ValueError):
                    continue
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
                    "tokens_released": outcome.get("tokens_released"),
                    "tokens_decharged": outcome.get("tokens_decharged")}
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
        *, prefill_depth: int | None,
        withdrawn: Sequence[str] = (),
        own_fence_gib: int = 0) -> dict[str, object] | None:
    """One consumer's ram decision inputs, or ``None`` when it has no ram leg.

    The stage window's own question, asked of the ram ledger: what fits, what
    the run-ahead bound covers, and -- the one bound that is a dependency
    rather than a size -- which phases' stage ranges have landed, because a
    promotion's source is the stage and nothing else.

    ``own_fence_gib`` reads this consumer's held fence back into free for
    this decision alone, exactly like the stage loop does: a fence never
    blocks its own window.
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
    # The promotion's precondition is the stage's *publication*, never its
    # booking: the tokens the stage mover holds were taken at claim, and
    # reading them as bytes is what published the head promotion against a
    # copy that was still running (#759).
    stage_resident, stage_known = _resident_movers(
        queue, plan, str(plan["tier_id"]), tiers)
    ledger = queue.tier_ledger(ram_tier_id)
    kind = storage_tiers.capacity_kind_of(ram_tier_id)
    # The consumer's own fence is read back into free for this decision
    # alone, as the stage window reads its grant back: the room the fence
    # holds is the room its advance publishes into (#906).
    free = int(ledger.available().get(kind, 0)) + int(own_fence_gib)
    capacity = int(ledger.capacity().get(kind, 0))
    # Bounded by the consumer's refill horizon on the tmpfs, as the stage
    # window is on the stage (#906): a promotion past it publishes on the
    # cycle the consumer's progress brings it inside.  ``prefill_depth`` is
    # still honoured as a declared ceiling; ``None`` for either changes
    # nothing.
    horizon_end = _horizon_end(_ram_horizon(queue, consumer, plan, tiers))
    decision = residency_plan.window(
        plan, accepted_phase=consumer["accepted_phase"],
        free_gib=free, capacity_gib=capacity,
        published=sorted(already), staged=sorted(staged),
        runahead_cap_gib=prefill_depth,
        # The two sets name promotion keys, so the decision must test
        # promotion keys: against stage keys its evict side would never fire
        # and its already-published skip would never skip (#640).
        mover_role="ram_mover_row", withdrawn=withdrawn,
        horizon_end_bytes=horizon_end)
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
                stage_resident):
            continue
        publishable.append((mover_row, entry))
    return {"ram_tier_id": ram_tier_id, "decision": decision,
            "phases": phases, "publishable": publishable,
            "already": already, "staged": staged,
            "stage_resident": stage_resident,
            "stage_resident_known": stage_known, "free_gib": free,
            "horizon_end_bytes": horizon_end}


def ram_residency_window(
        queue: pool.PoolQueue, *, tiers: Mapping[str, Mapping[str, object]],
        now: float | None = None,
        withdrawn: frozenset[str] | None = None) -> list[dict[str, object]]:
    """Publish the next ram promotions, retire the consumed ones, first.

    The stage window's own semantics, pointed at the ram ledger: admission
    needs free ``ram_gib`` -- Rob's instinct, "empty space in tmpfs", made
    exact through the ledger -- bounded by the #633 run-ahead budget on the
    consumer's accepted progress, in the plan's read order.  The ram egress
    of a phase the consumer has passed is published here, *before* the stage
    window publishes its own, so on a box that runs them in queue order the
    tokens that bound the smaller tier come back before the bytes that feed
    it leave (#640).

    ``withdrawn`` is the cycle's snapshot of live withdrawal markers; a
    promotion key in it is never published here, and the plan that names it
    is marked superseded by the stage window in the same cycle (#708).
    """

    events: list[dict[str, object]] = []
    ram_tiers = {tier_id: record for tier_id, record in tiers.items()
                 if record.get("tier") == "ram"}
    if not ram_tiers:
        return events
    cancelled = _withdrawn_keys(queue, withdrawn)
    depth = _prefill_depth(load_ram_policy())

    def ram_horizon_of(consumer, plan, _tier_id):
        # One horizon per window for the gate and the publication alike, as
        # on the stage (#903, #906): the gate reserves room for exactly the
        # promotions the window would publish.
        return _horizon_end(_ram_horizon(queue, consumer, plan, tiers))

    ram_protection = _protect_tier_advances(
        queue, tiers, mover_role="ram_mover_row",
        tier_of=lambda plan: (str(plan.get("ram_tier_id") or "")
                              if residency_plan.ram_mover_keys(plan) else ""),
        state_of=_ram_mover_state, horizon_of=ram_horizon_of)
    ram_gated = ram_protection["gated"]
    assert isinstance(ram_gated, dict)
    ram_grants = ram_protection["grants"]
    assert isinstance(ram_grants, dict)
    ram_permitted = ram_protection.get("permitted")
    assert isinstance(ram_permitted, dict)
    ram_unknown_ready = bool(ram_protection.get("unknown_ready"))
    ram_unknown_tiers = set(ram_protection.get("unknown_tiers") or ())
    ram_unknown_consumers = set(ram_protection.get("unknown_consumers") or ())
    events.extend(ram_protection["events"])  # type: ignore[arg-type]
    try:
        ram_cycle_consumers = live_consumers(queue)
    except (OSError, pool.PoolContractError) as exc:
        events.append({"event": "ram-window-unknown", "consumer": None,
                       "tier_id": None,
                       "reason": f"live census unreadable: {exc!r}"})
        ram_cycle_consumers = []
    for consumer in ram_cycle_consumers:
        key = str(consumer["action_key"])
        plan, incarnation = residency_plan.read_filed(queue, key)
        if plan is None:
            continue      # a plan this reader refuses is reported once, below
        # A fence never blocks its own window (stage loop reads it back the
        # same way); sum this consumer's held ram grants here.
        own_fence_gib = 0
        for (grant_key, grant_tier), grant_name in ram_grants.items():
            if grant_key != key or not isinstance(grant_name, str):
                continue
            try:
                own_fence_gib += int(queue.tier_ledger(
                    grant_tier).holder_tokens(grant_name).get(
                        storage_tiers.capacity_kind_of(grant_tier), 0))
            except (OSError, pool.PoolContractError, ValueError):
                continue
        state = _ram_window_state(queue, consumer, plan, tiers,
                                  prefill_depth=depth,
                                  withdrawn=sorted(cancelled),
                                  own_fence_gib=own_fence_gib)
        if state is None:
            continue
        ram_tier_id = str(state["ram_tier_id"])
        if not state["stage_resident_known"]:
            # Said, not swallowed: the window has already gated closed on it
            # (an unknown source is not a staged one), and an operator
            # reading a quiet cycle would otherwise see a consumer that
            # simply never promotes.  The next cycle asks again.
            events.append({
                "event": "ram-window-unknown", "consumer": key,
                "tier_id": ram_tier_id,
                "reason": "stage publication evidence unreadable"})
        decision = state["decision"]
        superseded = residency_plan.superseded(queue, plan)
        stall = decision["stall"]
        # A superseded window promotes nothing -- at any price -- while its
        # ram egress below still frees the phases the consumer has passed.
        if isinstance(stall, Mapping) and superseded is None:
            events.append({
                "event": "ram-window-stalled", "consumer": key,
                **{field: stall[field] for field in (
                    "accepted_phase", "reading_phase", "blocked_phase",
                    "blocked_gib", "runahead_gib", "runahead_budget_gib",
                    "free_gib", "capacity_gib", "reason", "waiting_for")},
                "chunk_index": stall.get("chunk_index"),
                "tier_id": ram_tier_id})
        publishable = ([] if (superseded is not None
                              or (key, ram_tier_id) in ram_gated
                              or ram_unknown_ready
                              or ram_tier_id in ram_unknown_tiers
                              or (key, ram_tier_id) in ram_unknown_consumers
                              or (key, "") in ram_unknown_consumers
                              or ("", "") in ram_unknown_consumers
                              or (key, ram_tier_id) not in ram_permitted)
                         else state["publishable"])
        gate = ram_gated.get((key, ram_tier_id))
        if gate is not None and superseded is None:
            assert isinstance(gate, dict)
            events.append({
                "event": "ram-window-gated", "consumer": key,
                "tier_id": ram_tier_id, "reason": str(gate.get("reason")),
                "permanent": bool(gate.get("permanent")),
                "need_gib": gate.get("need_gib"),
                "output_note": str(gate.get("output_note") or ""),
            })
        if (gate is None and superseded is None and (
                ram_unknown_ready or ram_tier_id in ram_unknown_tiers
                or (key, ram_tier_id) in ram_unknown_consumers
                or (key, "") in ram_unknown_consumers
                or ("", "") in ram_unknown_consumers)):
            events.append({
                "event": "ram-window-unknown", "consumer": key,
                "tier_id": ram_tier_id, "reason": "unknown-evidence"})
        if (gate is None and superseded is None
                and not (ram_unknown_ready
                         or ram_tier_id in ram_unknown_tiers
                         or (key, ram_tier_id) in ram_unknown_consumers
                         or (key, "") in ram_unknown_consumers
                         or ("", "") in ram_unknown_consumers)
                and (key, ram_tier_id) not in ram_permitted
                and state["publishable"]):
            # Required advance unproved and ungated (should not happen:
            # protection denies every such path) -- fail closed loudly.
            events.append({
                "event": "ram-window-unfunded", "consumer": key,
                "tier_id": ram_tier_id, "reason": "advance-unproved"})
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
        generation = None
        item = consumer.get("item")
        if isinstance(item, Mapping):
            generation = item.get("published_unix")
        for mover_row, entry in publishable:
            row = dict(mover_row)
            try:
                # A copy has no result to replay, for the same reason the
                # stage's own rows carry it -- and an automatic promotion
                # refuses a live cancellation under publish's own lock rather
                # than retiring the operator's marker (#708).
                #
                # The consumer's lock is the parent boundary, held across the
                # recheck and the publish exactly as the stage window holds
                # it: a promotion is never published after the plan that
                # minted it was reaped or replaced, or after the consumer
                # itself was withdrawn (#708 review).
                with queue._transition_locked(key):
                    owned, why = residency_plan.window_owned(
                        queue, key, filing=incarnation, generation=generation)
                    if not owned:
                        events.append({
                            "event": "ram-mover-publish-deferred-stale-window",
                            "consumer": key, "phase": entry["phase"],
                            "chunk_index": entry.get("chunk_index"),
                            "action_key": str(row["action_key"]),
                            "reason": why})
                        break
                    queue.publish(**row, recompute=True, refuse_withdrawn=True)
            except pool.WithdrawnActionError as exc:
                marked = residency_plan.mark_superseded(
                    queue, key, plan=plan, filing=incarnation,
                    reason="mover-withdrawn",
                    movers=[str(entry.get("mover_action_key") or
                                row.get("action_key") or "")],
                    by="tier-loop")
                events.append({"event": "ram-mover-publish-refused-withdrawn",
                               "consumer": key, "phase": entry["phase"],
                               "chunk_index": entry.get("chunk_index"),
                               "action_key": str(row["action_key"]),
                               "tier_id": ram_tier_id,
                               "error": str(exc),
                               "plan_superseded": marked is not None})
                break     # the plan is superseded now: no more promotions
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
                # And the same question again under the queue's lock, so the
                # look above and this publication are one decision (#810).
                queue.publish(**row, recompute=True,   # a deletion, likewise
                              refuse_if_live=True)
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
    events.extend(_settle_protected(queue, ram_protection))
    # Second pass binds what this cycle published: the blind pre-publish
    # take already holds the room, so the bind commits no new capacity and
    # no published row leaves this cycle unfunded.  The pass re-gates from
    # the fresh census (newcomers are members now) and never publishes.
    ram_protection_again = _protect_tier_advances(
        queue, tiers, mover_role="ram_mover_row",
        tier_of=lambda plan: (str(plan.get("ram_tier_id") or "")
                              if residency_plan.ram_mover_keys(plan) else ""),
        state_of=_ram_mover_state, horizon_of=ram_horizon_of)
    events.extend(ram_protection_again["events"])  # type: ignore[arg-type]
    events.extend(_settle_protected(queue, ram_protection_again))
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


def _emptied_map(path: Path) -> dict[str, object] | bool | None:
    """A running consumer's map with nothing staged (#908).

    The header the reader already adopted -- tier, stage root, manifest --
    with no entries, no ram overlay and no leads, because ``leads`` names
    the movers whose fragments were composed and there are none.
    ``generation`` is carried over, so the document does not count
    backwards.  ``True`` when the map on disk is already that document, so
    an unchanged gap costs no rewrite; ``None`` when there is no readable
    map to keep, which leaves the old rule (remove it) in charge.
    """

    try:
        current = residency_map.read_map(path)
    except (OSError, ValueError):
        return None
    emptied: dict[str, object] = {
        "schema": current["schema"], "tier_id": current["tier_id"],
        "stage_root": current["stage_root"],
        "manifest_sha256": current["manifest_sha256"],
        "leads": [], "generation": current["generation"], "entries": {}}
    return True if current == emptied else emptied


def compose_map(queue: pool.PoolQueue, consumer_action_key: str, *,
                ram_tiers: Mapping[str, Mapping[str, object]] | None = None,
                running: bool = False,
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

    **A running consumer keeps its map (#908).**  Between an egress of the
    last range it read and the landing of the next one, a claimed consumer
    has nothing staged.  With ``running`` its map is kept rather than
    removed: the header the reader adopted, naming no range and no mover.
    The reader answers "not staged yet" from an empty map exactly as from a
    missing one -- a declared span waits either way -- but a missing map is
    refused whole and logged as ``residency map is unreadable``, which on
    2026-09-22 read as the cause of capture ``a92f62783e8f``'s stall when
    the cause was a range nobody published (#903).  A map stays for as long
    as its consumer runs, and is composed from fragments again the moment
    one lands.  A consumer that is not running -- a newcomer, or one
    requeued into ``ready`` -- still loses its map when nothing is staged,
    so the claim's residency gate keeps reading ``map_not_composed``.
    """

    root = queue.residency_fragment_root()
    path = queue.residency_map_path(consumer_action_key)
    cache_key = (str(queue.root), consumer_action_key)
    fingerprint = _compose_fingerprint(queue, consumer_action_key, ram_tiers)
    if fingerprint is not None:
        # Whether the consumer runs decides what an empty fragment set
        # composes to (#908), so it is an input like the fragments are.
        fingerprint = (*fingerprint, bool(running))
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
        # Nothing staged (yet, or any more).  A consumer that is not running
        # loses its map, which is what the claim gate reads as
        # ``map_not_composed``.  A running one keeps the header it adopted
        # and no entries (#908): an entry naming an egressed range would send
        # the reader to a deleted file, and no map at all is refused whole.
        kept = _emptied_map(path) if running else None
        if kept is None:
            path.unlink(missing_ok=True)
            if fingerprint is not None:
                _COMPOSE_FINGERPRINTS[cache_key] = (fingerprint, False)
            return None
        if kept is not True:
            residency_map.write_map(path, kept)
        if fingerprint is not None:
            _COMPOSE_FINGERPRINTS[cache_key] = (fingerprint, True)
        return path
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
    queue: pool.PoolQueue, tiers: Mapping[str, Mapping[str, object]], *,
    unknown: list[dict[str, object]] | None = None,
) -> list[tuple[str, dict[str, object], dict[str, object], str]]:
    """Live consumers whose frozen plan stages onto a tier this box announced.

    Quietly: a plan this reader refuses is reported once, by
    :func:`residency_window`, which is the step that has a denial to file.  A
    second report from each of the steps below would say the same thing three
    times per cycle.  ``unknown`` collects those consumers all the same, for
    the admission commitment (#907), which must not count a live consumer's
    room as free because its plan did not read.
    """

    out: list[tuple[str, dict[str, object], dict[str, object], str]] = []
    for consumer in live_consumers(queue):
        key = str(consumer["action_key"])
        refusals: list[Exception] = []
        plan = residency_plan.read(queue, key, on_unreadable=refusals.append)
        if plan is None:
            if refusals and unknown is not None:
                unknown.append({"consumer": key, "tier_id": "",
                                "error": f"plan unreadable: {refusals[0]!r}"})
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

    Once the consumer's queued work is stopped, the plan that minted it is
    archived too (#708) -- but only once :func:`residency_plan.handoff_safe`
    says nothing still names it, because the next cycle uses the plan to find
    a mover whose stop is still pending.  A plan whose window an operator
    withdrew was already marked superseded at withdrawal, so its body stays
    readable while its claimed children conclude and no publication can come
    from it in the meantime.

    The sweep of one terminal is one consumer-lock transaction.  The scan
    observes the terminal and checks the consumer from outside that lock, so
    both are re-read inside it, and the plan attribution, every child
    withdrawal and the reap happen there too.  A resubmission of the same key
    publishes its consumer under the same lock, and the window publishes the
    lead under it too, so a stale pass cannot reach the new generation's rows; a live or unreadable consumer
    defers the sweep to the next cycle (#708 review).
    """

    events: list[dict[str, object]] = []
    # Only a filed plan can attribute work to a dead consumer. Discover these
    # positive candidates before inspecting terminal history: live_state's
    # safe absence check lists the live queue, so doing it for every historical
    # action creates history-by-live-queue work (#870). This is only a filter;
    # _sweep_dead_consumer still rechecks terminal, live state and current plan
    # under the consumer lock before withdrawing or reaping anything. A plan
    # filed after this observation is conservatively deferred to the next pass.
    try:
        filed_keys = {path.stem for path in pool._scan(queue.root / pool.RESIDENCY_PLANS)
                      if path.suffix == ".json" and len(path.stem) == 64}
    except OSError:
        return events
    if not filed_keys:
        return events
    for state in (pool.FAILED, pool.WITHDRAWN, pool.DONE):
        try:
            paths = list(pool._scan(queue.dir(state)))
        except OSError:
            continue
        for path in paths:
            name = path.name
            key = name[:-len(".json")] if name.endswith(".json") else name
            if key not in filed_keys:
                continue
            _sweep_dead_consumer(queue, key=key, state=state, path=path,
                                 events=events)
    return events


def _sweep_dead_consumer(queue: pool.PoolQueue, *, key: str, state: str,
                         path: Path, events: list[dict[str, object]]) -> None:
    """One terminal record's sweep, inside the consumer's transition lock.

    The terminal and the no-live-parent observation are made outside the
    lock by the directory scan, so both are re-read here before anything acts
    on them.  A resubmission publishes the same consumer key, and its lead,
    under this same transition lock: holding it across the re-read, the plan
    attribution, every child withdrawal and the reap means a stale pass
    either runs wholly before the new generation exists or observes its live
    rows here and leaves them alone.  Without it, a pass that read the old
    terminal can withdraw the NEW generation's lead, and ``reap``'s locked
    recheck is far too late to undo a cancellation (#708 review).

    The lock order is the one every writer keeps, parent before child:
    ``queue.withdraw`` and ``reap`` take the mover keys' own locks while this
    consumer's is held, and nothing here waits on a child a parent does not
    already hold.
    """

    with queue._transition_locked(key):
        item = pool._read_json(path)
        if not isinstance(item, dict):
            return
        live, _why = residency_plan.live_state(queue, key)
        if live is not None or _why:
            # Resubmitted under the same key: a new generation, live work.
            # A queue that cannot be read is uncertainty, not absence, and
            # defers the sweep exactly as it defers a handoff.
            return
        plan, incarnation = residency_plan.read_filed(queue, key)
        if plan is None:
            return      # not a staged consumer, or an unreadable plan
        if (state == pool.DONE
                and residency_plan.superseded(queue, plan) is None):
            # A finished consumer that was never superseded keeps its
            # frozen plan: a retry republishes the same children.
            return
        failed = False
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
                failed = True
                continue
            done = outcome.get("status") in ("withdrawn", "already_withdrawn")
            if not done:
                failed = True
            events.append({
                "event": "dead-consumer-mover-withdrawn",
                "consumer": key, "mover": mover_key, "state": origin,
                "withdrawn": bool(done), "status": outcome.get("status")})
        if failed:
            return      # the plan is how the next cycle retries
        reaped = residency_plan.reap(
            queue, key, reason=f"consumer-{state}",
            plan=plan, filing=incarnation)
        if reaped is not None:
            events.append({
                "event": "residency-plan-reaped", "consumer": key,
                "tier_id": str(plan["tier_id"]),
                "reason": f"consumer-{state}",
                "phases": len(reaped.get("phases") or []),
                "movers": len(residency_plan.mover_keys(reaped))})


def adoptable_ranges(queue: pool.PoolQueue, *, tier_id: str,
                     reserved: set[str]) -> dict[tuple, list[str]]:
    """Descriptor -> candidate donor mover keys, resident, no live item naming.

    Exactly the set the orphan sweep would take back: a range whose consumer
    has finished, failed or been withdrawn, still pinned because its bytes are
    still there.  ``reserved`` is what keeps a *running* consumer's window out
    of it -- that is the distinction #598 said was missing, and it is read off
    the queue's own live state rather than from a clock.

    Candidates are listed in sorted key order, so the answer does not depend
    on directory order.  ``adopt`` verifies each candidate's dated material
    against the files that are actually there and falls through to the next
    on a stale one, so a donor whose sidecar names a superseded incarnation
    no longer shadows the donor that dates the current one (#755).
    """

    index: dict[tuple, list[str]] = {}
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
                        int(staged["range_end_bytes"])), []).append(key)
    return index


def adopt(queue: pool.PoolQueue, *, old_key: str, new_key: str,
          consumer_action_key: str, tier_id: str, phase: str,
          range_start_bytes: int, range_end_bytes: int,
          residency_root: Path,
          chunk_index: int | None = None) -> dict[str, object]:
    """Hand one resident range from a finished mover to a live consumer's (#598).

    No byte is copied and no instant has bytes on the stage that no key holds.
    The order is the whole argument:

    1. **The donor's material still dates the files that are there.**  Under
       the stage ownership lock, every dated entry's ``file_id`` is compared
       against a live stat of its staged path -- the same comparison the
       strict reader will make.  A donor whose sidecar names a superseded
       incarnation (or bytes that are gone) publishes nothing: no successor,
       no material, no transfer (#755).  A donor with no sidecar, or one
       dating only some of the files its fragment names, is declined for the
       same reason: what it would hand on is a range the reader cannot prove.
    2. **The successor vouches for the same files under its own name.**  Two
       fragments then name one range, which every reader already tolerates:
       ``compose`` is per consumer, and the reconciliation unions them.
       Published under the ownership lock, never before it, so no egress
       scan can interleave between the vouch and the transfer.
    3. **The tokens change owner.**  ``ResourceLedger.transfer`` renames each
       token between two directories under ``held/``, so the tier's occupancy
       is the same number throughout and a crash part-way splits the
       attribution without changing the sum.
    4. **Only then does the old name stop accounting for the bytes.**  Dropping
       the old fragment before the transfer would leave an egress able to
       release tokens for bytes that are still there; dropping it after means
       the worst an interrupted adoption leaves is a range named twice.
    5. **The receipt the pin and the gate read.**  An adopted mover never runs,
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
        # Same bytes, same generation: the successor dates its vouching with
        # the publish it took over, never a new one (a new generation is for
        # new bytes).
        old_material = reader_lease.read_material(
            residency_root, old_consumer, old_key)
        if isinstance(old_material, Exception):
            return {**outcome, "reason": "range_not_named",
                    "error": repr(old_material)}
        # The dated material is the only proof that survives into the
        # successor, so a donor that cannot supply it is declined rather than
        # adopted: the reader takes material as proof, and a successor vouched
        # by a fragment alone is a range it refuses.  The next candidate for
        # the descriptor is tried; if none qualifies, the copy runs.
        named = source.get("entries")
        if not isinstance(old_material, dict):
            return {**outcome, "reason": "donor_undated"}
        dated = old_material.get("entries")
        if (not isinstance(dated, dict) or not isinstance(named, dict)
                or not named or not set(named) <= set(dated)):
            # Entries the sidecar does not date are unproven, and the reader
            # needs every one of them.
            return {**outcome, "reason": "donor_material_partial"}
        # Covering the same keys is not describing the same object: a
        # sidecar can pass live ``file_id`` validation against a different
        # valid file.  ``reader_lease.covers_for_keys`` requires tier,
        # manifest and epoch of the material itself to match before it will
        # take a cover, and adoption qualifies its donor the same way.
        if any(str(old_material.get(field) or "") != str(source.get(field) or "")
               for field in ("tier_id", "manifest_sha256", "epoch")):
            return {**outcome, "reason": "donor_material_mismatch"}
        # Per entry, the same comparison the reader makes: the sidecar dates
        # the fragment's vouching, so same path, same length, same digest or
        # the two are about different bytes.
        for key, vouched in named.items():
            mention = dated.get(key)
            if (not isinstance(vouched, Mapping) or not isinstance(mention, Mapping)
                    or str(vouched.get("stage_path") or "")
                    != str(mention.get("stage_path") or "")
                    or vouched.get("bytes") != mention.get("bytes")
                    or str(vouched.get("sha256") or "")
                    != str(mention.get("sha256") or "")):
                return {**outcome, "reason": "donor_material_mismatch",
                        "entry": str(key)}
        with queue.stage_ownership_lock(str(source["stage_root"]),
                                        blocking=False) as owned:
            if not owned:
                # An egress is mid-scan on this stage root; its snapshot
                # predates this adoption, so proceeding could interleave the
                # transfer between its scan and its unlink.  Declining costs
                # a copy, the same price as a busy transition lock.  Nothing
                # has been published yet: the successor's fragment and
                # material land only under this lock.
                return {**outcome, "reason": "ownership_busy"}
            if isinstance(old_material, dict):
                # A partial prune can leave the fragment a strict subset of
                # the complete range its historical receipt declares (#853).
                # Whole-range adoption transfers one reservation and files
                # one complete receipt, so it may only take over the exact
                # range the donor's own complete receipt staged: the receipt
                # must be complete, its declared and staged entry counts must
                # equal the fragment's validated entries, their byte sum must
                # equal the receipt's actual staged bytes and the requested
                # leg's span, and the receipt's range must be that leg.  A
                # shortened fragment is a per-path cache for the publisher,
                # never a whole-range donor.
                #
                # This is metadata only -- three dict reads and a sum -- so
                # it runs before the per-file stat walk below: only a donor
                # that can still stand for the whole range is worth
                # qualifying file by file.
                declared = receipt.get("entries_declared")
                staged = receipt.get("entries_staged")
                receipt_start = receipt.get("range_start_bytes")
                receipt_end = receipt.get("range_end_bytes")
                staged_bytes = receipt.get("bytes_staged")
                if (receipt.get("complete") is not True
                        or receipt.get("refusal")
                        or any(isinstance(value, bool) or not isinstance(value, int)
                               for value in (declared, staged, receipt_start,
                                             receipt_end, staged_bytes))
                        # The counts are one shortening witness: a partial
                        # prune leaves fewer fragment entries than the
                        # complete receipt declared and staged.
                        or declared != staged
                        or declared != len(source["entries"])
                        or int(receipt_start) != int(range_start_bytes)
                        or int(receipt_end) != int(range_end_bytes)
                        # The receipt's own byte accounting must still equal
                        # its range...
                        or int(staged_bytes)
                        != int(receipt_end) - int(receipt_start)
                        # ...and the fragment must cover that range exactly.
                        # ``stage_move`` adds each landed entry's own
                        # ``written`` to both the entry's ``bytes`` and
                        # ``bytes_staged`` (an adopted incarnation returns
                        # ``want`` the same way), and a run that did not land
                        # every entry files an incomplete receipt -- so for a
                        # complete receipt the two are equal, and equal entry
                        # counts do not prove equal bytes.  Anything else is a
                        # certificate for a range the fragment cannot cover.
                        or sum(int(entry["bytes"])
                               for entry in dict(source["entries"]).values())
                        != int(staged_bytes)):
                    return {**outcome, "reason": "donor_range_shortened",
                            "declared": declared, "staged": staged,
                            "entries": len(source["entries"])}
                # The dated vouch must still describe the incarnation that
                # is there: the strict reader takes the successor's material
                # as proof, so adopting a donor that names a superseded
                # incarnation would publish a refusal into the consumer
                # (#755).  Verified under the ownership lock, so no egress
                # or publisher can be changing the same name at the same
                # time; a file that is gone is the same answer.
                stale: list[str] = []
                missing: list[str] = []
                for key, mention in dict(
                        old_material["entries"]).items():
                    mention = mention if isinstance(mention, Mapping) else {}
                    live = reader_lease.stat_identity(
                        str(mention.get("stage_path") or ""))
                    if live is None:
                        missing.append(str(key))
                    elif not reader_lease.file_id_matches(
                            mention.get("file_id"), live):
                        stale.append(str(key))
                if missing:
                    return {**outcome, "reason": "donor_file_missing",
                            "missing": missing}
                if stale:
                    return {**outcome, "reason": "donor_file_changed",
                            "stale": stale}
            residency_map.write_fragment(residency_root, residency_map.reissue(
                source, consumer_action_key=consumer_action_key,
                mover_action_key=new_key))
            if isinstance(old_material, dict):
                material_entries = old_material.get("entries")
                assert isinstance(material_entries, dict)
                reader_lease.write_material(
                    residency_root, consumer_action_key=consumer_action_key,
                    mover_action_key=new_key,
                    tier_id=str(source.get("tier_id") or ""),
                    stage_root=str(source.get("stage_root") or ""),
                    manifest_sha256=str(source.get("manifest_sha256") or ""),
                    generation=reader_lease.adopted_generation(old_material),
                    entries=material_entries,  # type: ignore[arg-type]
                    epoch=(str(source.get("epoch") or "")
                           if source.get("epoch") is not None else None))
            expected = sum(before.values())
            moved = queue.transfer_tier_reservation(tier_id, old_key, new_key)
            if moved != expected:
                # The reservation is split across the two keys and the sum is
                # unchanged, so nothing is over-admitted; the old key is still
                # resident, so the next cycle asks again and finishes the move.
                return {**outcome, "reason": "partial_transfer",
                        "tokens_moved": moved, "tokens_expected": expected}
            source_path.unlink(missing_ok=True)
            try:
                reader_lease.material_path(
                    residency_root, old_consumer,
                    old_key).unlink(missing_ok=True)
            except OSError:
                pass
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
    withdrawn: frozenset[str] | None = None,
    unknown: list[dict[str, object]] | None = None,
) -> list[dict[str, object]]:
    """Take over the resident prefix before a live window's first gap (#864).

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

    A missing near range stops adoption of farther ranges. Those bytes stay
    charged under their historical donors, reusable without another copy, but
    remain eligible for the existing pressure-driven orphan sweep. Adopting
    beyond a hole would put all those distant bytes under live-plan protection
    while the missing frontier and its advance have no room to stage. Existing
    live owners and reader pins keep their ordinary protection.

    A leg whose key carries a live withdrawal marker is not adopted: the plan
    that names it is marked superseded in this cycle (#708), and moving
    occupancy onto a key nobody will publish leaves tokens holding bytes the
    sweep then has to take back.

    A donor whose dated material no longer describes the incarnation on the
    stage is declined rather than taken over, and the next candidate for the
    same descriptor is tried (#755): publishing superseded identity into a
    live consumer is how a resident range turns into a refusal.
    """

    events: list[dict[str, object]] = []
    cancelled = _withdrawn_keys(queue, withdrawn)
    wanted, owners = stage_release.live_claims(queue)
    reserved = set(wanted) | set(owners)
    root = queue.residency_fragment_root()
    index_by_tier: dict[str, dict[tuple, list[str]]] = {}
    if consumers is None:
        unknown = []
        consumers = _planned_consumers(queue, tiers, unknown=unknown)
    # Asked once, and only when a ready consumer is about to take a range
    # over: most cycles adopt nothing.
    admissions: dict[tuple[str, str], dict[str, object]] | None = None
    for consumer_key, consumer, plan, tier_id in consumers:
        if residency_plan.superseded(queue, plan) is not None:
            continue      # superseded: no new occupancy under its keys, and
                          # its resident ranges stay named by live_claims
        if tier_id not in index_by_tier:
            index_by_tier[tier_id] = adoptable_ranges(
                queue, tier_id=tier_id, reserved=reserved)
        index = index_by_tier[tier_id]
        if not index:
            continue
        ledger = queue.tier_ledger(tier_id)
        digest = str(plan["manifest_sha256"])
        accepted = consumer["accepted_phase"]
        try:
            resident = residency_plan.resident_movers(queue, plan, tier_id=tier_id)
        except residency_plan.ResidencyEvidenceUnreadable as exc:
            events.append({"event": "adoption-prefix-deferred", "consumer": consumer_key,
                           "reason": "residency-evidence-unreadable", "error": str(exc)})
            continue
        prefix_blocked = False
        admission_asked = consumer.get("state") != pool.READY
        for phase in residency_plan.remaining(plan, accepted):  # type: ignore[arg-type]
            if prefix_blocked:
                break
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
                if new_key in cancelled:
                    prefix_blocked = True
                    break         # never adopt past a withdrawn frontier
                if new_key in resident:
                    continue      # this exact leg already has qualified bytes
                candidates = index.get(_descriptor(digest, tier_id, cstart, cend))
                if not candidates:
                    prefix_blocked = True
                    break
                if ledger.holder_tokens(new_key):
                    prefix_blocked = True
                    break         # booked room is not qualified residency
                if (queue.item_path(pool.READY, new_key).exists()
                        or queue.item_path(pool.CLAIMED, new_key).exists()):
                    prefix_blocked = True
                    break         # let its own copy finish before adopting ahead
                if not admission_asked:
                    # Taking a newcomer's lead over admits it: its lead then
                    # holds tokens, and a consumer whose lead holds tokens is
                    # an admitted window.  Adoption moves tokens rather than
                    # acquiring them, so no joint-fit gate sees it; the
                    # commitment must (#907), or a successor over its
                    # predecessor's ranges -- R13 over R12's -- is admitted
                    # without it.
                    admission_asked = True
                    if admissions is None:
                        admissions = _commitment_admissions(_commitment_census(
                            queue, tiers, consumers=consumers,
                            unknown=unknown or ()))
                    refusal = _commitment_refusal(admissions, consumer_key,
                                                  str(tier_id))
                    if refusal is not None:
                        events.append({
                            "event": "adoption-deferred",
                            "consumer": consumer_key, "tier_id": tier_id,
                            "phase": str(phase["name"]),
                            "reason": str(refusal["reason"]),
                            "commitment": refusal.get("commitment")})
                        prefix_blocked = True
                        break
                leg_adopted = False
                for old_key in list(candidates):
                    if old_key == new_key:
                        continue
                    event = adopt(queue, old_key=old_key, new_key=new_key,
                                  consumer_action_key=consumer_key,
                                  tier_id=tier_id,
                                  phase=str(phase["name"]),
                                  range_start_bytes=cstart,
                                  range_end_bytes=cend,
                                  residency_root=root,
                                  chunk_index=chunk_index)
                    events.append(event)
                    if event.get("adopted"):
                        leg_adopted = True
                        index.pop(_descriptor(digest, tier_id, cstart, cend),
                                  None)
                        break
                    reason = str(event.get("reason") or "")
                    if reason in ("donor_file_changed", "donor_file_missing",
                                  "donor_undated", "donor_material_partial",
                                  "donor_material_mismatch",
                                  "donor_range_shortened"):
                        # The donor's dated material no longer describes the
                        # incarnation on the stage, and it stays that way
                        # until somebody republishes: not this cycle, not a
                        # later one.  The next candidate for the same
                        # descriptor is the range the consumer can actually
                        # take over (#755).
                        candidates.remove(old_key)
                        continue
                    break          # busy or unnamed: this cycle's answer stands
                if not leg_adopted:
                    prefix_blocked = True
                    break
    return events


#: Landing rates of complete stage copies, in bytes per second, by queue and
#: mover, each beside the identity of the receipt it was read from (#903).  A
#: range evicted past its horizon and copied again files a new receipt under
#: the same mover key; the identity is what notices it, so the rate is always
#: the last copy's.  A remembered first rate would price a slower second copy
#: too fast and make the horizon short.  One stat per call, one read per new
#: receipt.
_LANDING_RATES: dict[tuple[str, str], tuple[tuple[int, int, int], float]] = {}


def _landing_rate(queue: pool.PoolQueue, mover_action_key: str) -> float | None:
    """The rate the mover's last complete stage copy landed at, or ``None`` (#903).

    ``bytes_staged`` over ``seconds`` from the mover's own receipt, which is
    what ``stage_move`` measured while it copied.  ``None`` for no receipt,
    an incomplete one, or one that does not read: an unlanded copy measured
    nothing.  The receipt is read again whenever its file changed, because
    a re-staged range replaces it (``record_move`` writes atomically).
    """

    cache_key = (str(queue.root), str(mover_action_key))
    try:
        status = os.stat(queue.move_path(str(mover_action_key)))
    except (OSError, pool.PoolContractError, ValueError):
        status = None
    identity = ((int(status.st_ino), int(status.st_mtime_ns),
                 int(status.st_size)) if status is not None else None)
    cached = _LANDING_RATES.get(cache_key)
    if identity is not None and cached is not None and cached[0] == identity:
        return cached[1]
    _LANDING_RATES.pop(cache_key, None)
    try:
        record = queue.move_record(str(mover_action_key))
    except (OSError, pool.PoolContractError, ValueError):
        return None
    if not isinstance(record, Mapping) or record.get("complete") is not True:
        return None
    try:
        staged = int(record.get("bytes_staged") or 0)
        seconds = float(record.get("seconds") or 0.0)
    except (TypeError, ValueError):
        return None
    if staged <= 0 or not math.isfinite(seconds) or seconds <= 0:
        return None
    rate = staged / seconds
    if identity is not None:
        _LANDING_RATES[cache_key] = (identity, rate)
    return rate


def _readahead_bytes(item: object) -> int | None:
    """The bytes a claimed consumer can hold ahead of what it reads (#903).

    Its memory reservations: ``mem_gb`` of host memory plus the GPU memory
    its admission budgeted.  A consumer that keeps what it prefetches cannot
    hold more than it reserved.  The two are summed even where they share
    one physical pool (a GB10's unified memory), which over-states the
    reach, so the horizon errs long, never short.  ``None`` when the item's
    resources do not read.
    """

    if not isinstance(item, Mapping):
        return None
    resources = item.get("resources")
    if not isinstance(resources, Mapping):
        return None
    mem = resources.get("mem_gb", 0)
    if (isinstance(mem, bool) or not isinstance(mem, (int, float))
            or not math.isfinite(float(mem)) or mem < 0):
        return None
    total = int(float(mem) * storage_tiers.GIB)
    admission = item.get("gpu_admission")
    if isinstance(admission, Mapping):
        budget = admission.get("gpu_memory_budget_bytes")
        if isinstance(budget, int) and not isinstance(budget, bool) and budget > 0:
            total += budget
    return total


def _sealed_fill_bytes_per_s(plan: Mapping[str, object],
                             tier_id: str) -> float | None:
    """The smallest fill any of the plan's stage copies was sealed with.

    The pre-measurement landing rate (#903), used only until the plan's
    first copy lands and its receipt replaces it.  Each copy is admitted
    with the fill reservation its row was sealed with, so the smallest of
    them is the slowest rate a copy of this plan was provisioned for.  It is
    not a guaranteed floor: on 2026-09-22 R12's 24 complete copies, all
    sealed at 144 MB/s, landed at 134 to 626 MB/s (median 154), so the
    slowest ran 7% under it.
    """

    kind = f"{storage_tiers.FILL_KIND}{storage_tiers.TIER_DEMAND_SEPARATOR}{tier_id}"
    demands: list[float] = []
    phases = plan.get("phases")
    if not isinstance(phases, list):
        return None
    for phase in phases:
        if not isinstance(phase, Mapping):
            continue
        rows = [phase.get("mover_row")]
        chunks = phase.get("stage_chunks")
        if isinstance(chunks, list):
            rows = [chunk.get("mover_row") for chunk in chunks
                    if isinstance(chunk, Mapping)]
        for row in rows:
            if not isinstance(row, Mapping):
                continue
            resources = row.get("resources")
            value = (resources.get(kind) if isinstance(resources, Mapping)
                     else None)
            if (isinstance(value, (int, float)) and not isinstance(value, bool)
                    and value > 0):
                demands.append(float(value) * storage_tiers.MB)
    return min(demands) if demands else None


def _announced_fill_supply(tier_record: Mapping[str, object] | None
                           ) -> float | None:
    """The fill supply a tier record announces, in MB/s, or ``None``."""

    if not isinstance(tier_record, Mapping):
        return None
    tokens = tier_record.get("tokens")
    if not isinstance(tokens, Mapping):
        return None
    value = tokens.get(storage_tiers.FILL_KIND)
    if (isinstance(value, (int, float)) and not isinstance(value, bool)
            and value > 0):
        return float(value)
    return None


def _consumer_horizon(queue: pool.PoolQueue, consumer: Mapping[str, object],
                      plan: Mapping[str, object],
                      tier_record: Mapping[str, object] | None, *,
                      mover_role: str) -> dict[str, object] | None:
    """A claimed consumer's refill horizon on one of its tiers, or ``None``.

    :func:`residency_plan.refill_horizon` with this box's measurements: the
    consumer's reservations for its read-ahead, the slowest complete copy of
    this leg of its plan for the landing rate, the tier's announced fill
    supply for its consumption before that is measured, and one heartbeat
    plus one cycle for the time an accepted phase takes to reach a decision.
    ``None`` -- a ready consumer, no accepted progress, nothing measured --
    leaves every decision what it was before the horizon existed.

    The two legs differ only in what stands in before their first copy
    lands.  A stage leg (#903) is priced at the smallest fill its copies
    were sealed with, then at the tier's fill supply.  A ram leg (#906) has
    no stand-in: a promotion copies the stage into the tmpfs, a path neither
    number measures, so until a promotion of this plan lands the ram horizon
    is undefined and the ram window keeps its #633 bound.  A ram horizon
    that comes out short costs less than a short stage horizon: the consumer
    reads a range from the stage while its ram copy is not there yet, which
    is slower but never a stall.  So the ram leg is priced at the promotion's
    own landing rate alone, not at the stage copy plus the promotion.
    """

    if consumer.get("state") != pool.CLAIMED:
        return None
    accepted = consumer.get("accepted_phase")
    if not residency_plan.accepted(plan, accepted):              # type: ignore[arg-type]
        return None
    readahead = _readahead_bytes(consumer.get("item"))
    if readahead is None:
        return None
    ram = mover_role == "ram_mover_row"
    keys = (residency_plan.ram_mover_keys(plan) if ram
            else residency_plan.stage_mover_keys(plan))
    rates = [rate for rate in (_landing_rate(queue, key) for key in keys)
             if rate is not None]
    supply = _announced_fill_supply(tier_record)
    landing: float | None = min(rates) if rates else None
    if landing is None and not ram:
        landing = _sealed_fill_bytes_per_s(plan, str(plan.get("tier_id") or ""))
        if landing is None and supply is not None:
            landing = supply * storage_tiers.MB
    try:
        return residency_plan.refill_horizon(
            plan, accepted,                                      # type: ignore[arg-type]
            claimed_unix=consumer.get("claimed_unix"),
            reported_unix=consumer.get("reported_unix"),
            readahead_bytes=readahead, landing_bytes_per_s=landing,
            report_latency_s=pool.HEARTBEAT_S + CYCLE_INTERVAL_S,
            fill_supply_mb_s=supply, mover_role=mover_role)
    except (residency_plan.ResidencyPlanError, KeyError, TypeError,
            ValueError):
        return None


def _stage_horizon(queue: pool.PoolQueue, consumer: Mapping[str, object],
                   plan: Mapping[str, object],
                   tier_record: Mapping[str, object] | None,
                   ) -> dict[str, object] | None:
    """A claimed consumer's refill horizon on its stage, or ``None`` (#903)."""

    return _consumer_horizon(queue, consumer, plan, tier_record,
                             mover_role="mover_row")


def _ram_horizon(queue: pool.PoolQueue, consumer: Mapping[str, object],
                 plan: Mapping[str, object],
                 tiers: Mapping[str, Mapping[str, object]],
                 ) -> dict[str, object] | None:
    """A claimed consumer's refill horizon on its ram tier, or ``None`` (#906).

    ``None`` as well for a plan with no ram leg, or whose ram tier this box
    did not announce: another box's loop owns that window.
    """

    ram_tier_id = plan.get("ram_tier_id")
    if not isinstance(ram_tier_id, str) or not ram_tier_id:
        return None
    record = tiers.get(ram_tier_id)
    if not isinstance(record, Mapping) or record.get("tier") != "ram":
        return None
    if not residency_plan.ram_mover_keys(plan):
        return None
    return _consumer_horizon(queue, consumer, plan, record,
                             mover_role="ram_mover_row")


def _horizon_end(horizon: Mapping[str, object] | None) -> int | None:
    """Where the first leg outside a horizon starts, or ``None`` for no bound."""

    if not isinstance(horizon, Mapping):
        return None
    end = horizon.get("horizon_end_bytes")
    return int(end) if isinstance(end, int) and not isinstance(end, bool) else None


def _landed_past(queue: pool.PoolQueue, key: str,
                 horizon: Mapping[str, object], tier_id: str,
                 ) -> list[dict[str, object]]:
    """One consumer's landed legs on one tier past its horizon, as candidates.

    A leg is a candidate when its copy is complete and holds the tier's
    tokens and nothing is queued or running on it -- not its mover (a copy
    in flight is not a landed range) and not its egress (already being given
    back).  The advance, the first leg past the horizon, is never in
    ``beyond``.
    """

    out: list[dict[str, object]] = []
    beyond = horizon.get("beyond")
    if not isinstance(beyond, list):
        return out
    try:
        ledger = queue.tier_ledger(tier_id)
    except (OSError, pool.PoolContractError, ValueError):
        return out
    kind = storage_tiers.capacity_kind_of(tier_id)
    for leg in beyond:
        mover = str(leg["mover_action_key"])
        egress = leg.get("egress_row")
        egress_key = (str(egress.get("action_key"))
                      if isinstance(egress, Mapping) else "")
        try:
            held = int(ledger.holder_tokens(mover).get(kind, 0))
            busy = any(
                queue.item_path(state, name).exists()
                for state in (pool.READY, pool.CLAIMED)
                for name in (mover, egress_key) if name)
        except (OSError, pool.PoolContractError, ValueError):
            continue
        if held <= 0 or busy or _landing_rate(queue, mover) is None:
            continue
        out.append({
            "tier_id": tier_id, "consumer_action_key": key,
            "mover_action_key": mover, "phase": leg["phase"],
            "chunk_index": leg.get("chunk_index"), "stage_gib": held,
            "start_bytes": int(leg["start_bytes"]),
            "end_bytes": int(leg["end_bytes"]),
            "seconds_until_needed": float(leg["seconds_until_needed"]),
        })
    return out


def _held_ram_copies(queue: pool.PoolQueue, plan: Mapping[str, object],
                     start: int, end: int) -> list[str] | None:
    """The plan's promotions over ``[start, end)`` that hold ram tokens.

    ``None`` when the ram ledger cannot be read: a stage range whose ram
    copies are unknown is not a range this pass may give back (#640).
    """

    ram_tier_id = plan.get("ram_tier_id")
    if not isinstance(ram_tier_id, str) or not ram_tier_id:
        return []
    kind = storage_tiers.capacity_kind_of(ram_tier_id)
    held: list[str] = []
    try:
        ledger = queue.tier_ledger(ram_tier_id)
        for leg in residency_plan.legs_over(plan, start, end,
                                            mover_role="ram_mover_row"):
            mover = str(leg["mover_row"]["action_key"])  # type: ignore[index]
            if int(ledger.holder_tokens(mover).get(kind, 0)) > 0:
                held.append(mover)
    except (OSError, pool.PoolContractError, ValueError, KeyError,
            residency_plan.ResidencyPlanError):
        return None
    return held


def _beyond_horizon_candidates(
    queue: pool.PoolQueue, tiers: Mapping[str, Mapping[str, object]],
    consumers: list, cancelled: frozenset[str],
) -> dict[str, list[dict[str, object]]]:
    """Per tier, the landed ranges no reader needs before a refill (#903, #906).

    A range is a candidate when its consumer is claimed and reporting, the
    range lies past that consumer's refill horizon on the range's own tier
    (the stage horizon for a stage range, the ram horizon for a promotion),
    its copy is complete and holds the tier's tokens, and nothing is queued
    or running on it.  Ranges inside a horizon never appear, nor do a
    superseded or withdrawn window's (the orphan sweep and the successor's
    adoption own those).  Farthest first: the range whose reader reaches it
    last is the one to give back first, measured in seconds at each reader's
    own consumption rate so two readers' ranges compare in one unit.

    A stage range whose promotion still holds the tmpfs goes only with that
    ram copy, ram first (#640): a ram range whose stage source is gone is one
    the consumer's map can no longer read (``overlay_ram`` reads a ram entry
    only beside its stage entry), so it would hold the tmpfs for nothing.  A
    stage row therefore names the ram copies it takes with it in
    ``ram_first``, and a stage range whose ram copy is not itself a candidate
    -- inside the ram horizon, busy, on a tier this box does not own, or
    unknown -- is not a candidate either.
    """

    out: dict[str, list[dict[str, object]]] = {}
    for key, consumer, plan, tier_id in consumers:
        if key in cancelled:
            continue
        try:
            if residency_plan.superseded(queue, plan) is not None:
                continue
        except (OSError, pool.PoolContractError, ValueError):
            continue
        ram_rows: dict[str, dict[str, object]] = {}
        ram_tier_id = str(plan.get("ram_tier_id") or "")
        ram_record = tiers.get(ram_tier_id) if ram_tier_id else None
        if isinstance(ram_record, Mapping) and ram_record.get("tier") == "ram":
            try:
                ram_horizon = _ram_horizon(queue, consumer, plan, tiers)
            except (OSError, pool.PoolContractError, ValueError):
                ram_horizon = None
            if ram_horizon is not None:
                for row in _landed_past(queue, key, ram_horizon, ram_tier_id):
                    ram_rows[str(row["mover_action_key"])] = row
                    out.setdefault(ram_tier_id, []).append(row)
        if tier_id not in tiers or tiers[tier_id].get("tier") != "stage":
            continue
        try:
            horizon = _stage_horizon(queue, consumer, plan, tiers.get(tier_id))
        except (OSError, pool.PoolContractError, ValueError):
            continue
        if horizon is None:
            continue
        ram_root = (str(ram_record.get("mountpoint") or "")
                    if isinstance(ram_record, Mapping) else "")
        for row in _landed_past(queue, key, horizon, tier_id):
            copies = _held_ram_copies(queue, plan, int(row["start_bytes"]),  # type: ignore[arg-type]
                                      int(row["end_bytes"]))  # type: ignore[arg-type]
            if copies is None or any(copy not in ram_rows for copy in copies):
                continue
            row["ram_first"] = [
                {"tier_id": ram_tier_id, "mover_action_key": copy,
                 "stage_root": ram_root,
                 "stage_gib": ram_rows[copy]["stage_gib"]}
                for copy in copies]
            out.setdefault(tier_id, []).append(row)
    for rows in out.values():
        rows.sort(key=lambda row: (-float(row["seconds_until_needed"]),  # type: ignore[arg-type]
                                   str(row["mover_action_key"])))
    return out



def _unpublished_lead(needs: Mapping[str, object], published: set[str]) -> bool:
    """The publication gate's newcomer boundary, including adopted tails."""

    lead = needs.get("lead_mover_action_key")
    return bool(lead) and str(lead) not in published


def _is_newcomer(consumer: Mapping[str, object], needs: Mapping[str, object],
                 published: set[str]) -> bool:
    """Whether the joint gate admits this window as a newcomer (#908).

    A newcomer is a consumer that has not been admitted: it is in ``ready``
    and its lead is unpublished.  The lead is the plan's first range
    (:func:`residency_plan.advance_needs`), and an egressed range holds no
    tokens, so it reads as unpublished again once the consumer has passed
    it.  A claimed consumer passed the claim's residency gate on those
    leads, so its window is admitted for the rest of its run: its next range
    is an admitted window's advance, fenced and counted in
    ``existing_min_next``, never a newcomer's current asking the gate for
    held + queued + current + next.  On 2026-09-22 that re-check gated the
    running capture ``a92f62783e8f`` for 60 cycles after its ``head`` egress.
    """

    if consumer.get("state") == pool.CLAIMED:
        return False
    return _unpublished_lead(needs, published)


def _unpublished_current_gib(needs: Mapping[str, object], *,
                             held: Mapping[str, Mapping[str, object]],
                             rowed: Mapping[str, object], kind: str) -> int:
    """The GiB an admitted window's current will take from free, or 0 (#908).

    Its first waiting leg, when that leg is in the phase the consumer is
    reading and is neither queued (queued demand is counted in full) nor
    holding its tokens (held is counted by the holder scan).  Only a
    running window's current can be unpublished: a ready consumer's lead is
    published, or the consumer is a newcomer.
    """

    waiting = needs.get("waiting")
    if not isinstance(waiting, list) or not waiting:
        return 0
    first = waiting[0]
    if (not isinstance(first, Mapping)
            or first.get("phase") != needs.get("reading_phase")):
        return 0
    mover = str(first.get("mover_action_key") or "")
    gib = int(first.get("stage_gib") or 0)
    if mover in rowed or int(held.get(mover, {}).get(kind, 0)) >= gib:
        return 0
    return gib


#: Opt-in switch for the produced-output obligation (#747).  ``1`` counts
#: each tier's unheld producer window (``produced_output.unheld_window_gib``)
#: in the joint-fit gate, the fence check and the newcomer relief; unset or
#: ``0`` counts it as zero, as before.  ``--output-windows`` sets it.
OUTPUT_WINDOWS_ENV = "PRISMABUILD_TIER_OUTPUT_WINDOWS"


def output_windows_enabled() -> bool:
    """Whether this loop counts produced-output windows; refuses any other value."""

    value = os.environ.get(OUTPUT_WINDOWS_ENV, "")
    if value in ("", "0"):
        return False
    if value == "1":
        return True
    raise ValueError(
        f"{OUTPUT_WINDOWS_ENV} must be unset, 0 or 1, not {value!r}")


def output_obligation(queue: pool.PoolQueue, tier_id: str
                      ) -> tuple[int, bool, str, str]:
    """``(output_gib, enforced, output_note, error)`` for one tier's gate.

    Off (the default), the obligation is zero with the standing
    ``output-scope-unenforced`` note, exactly as before #747.  On, it is
    the GiB of live producer windows that nobody holds: the room a
    producer's next ``refill_window`` takes back from free.  An obligation
    the census cannot read is an error, never zero, and the caller defers
    the tier as it does for an unreadable ledger.
    """

    if not output_windows_enabled():
        return (0, False, window_credit.OUTPUT_UNENFORCED_NOTE, "")
    try:
        owed = produced_output.unheld_window_gib(queue, tier_id)
    except (OSError, pool.PoolContractError, ValueError) as exc:
        return (0, True, "", f"output windows unreadable: {exc}")
    if owed["gib"] is None:
        return (0, True, "", "output windows unknown: " + "; ".join(
            f"{str(entry.get('owner', ''))[:12] or '(scopes)'}: "
            f"{entry.get('error', '')}" for entry in owed["unknown"]))
    return (int(owed["gib"]), True, "", "")


def _claim_tier_demands(item: object,
                        tiers: Mapping[str, Mapping[str, object]]
                        ) -> list[tuple[str, int]]:
    """``(tier_id, gib)`` for each announced tier a ready row's claim takes.

    Read from the row's own ``resources``, the demand its claim passes to
    the tier ledger (#901).  A row whose demand does not parse asks for
    nothing: the claim refuses it on the same parse.
    """

    if not isinstance(item, Mapping):
        return []
    try:
        _host, demands = storage_tiers.split_demand(item.get("resources") or {})
    except (ValueError, TypeError, AttributeError):
        return []
    out: list[tuple[str, int]] = []
    for tier_id, needs in sorted(demands.items()):
        if tier_id not in tiers or not isinstance(needs, Mapping):
            continue
        gib = int(needs.get(storage_tiers.capacity_kind_of(tier_id), 0) or 0)
        if gib > 0:
            out.append((str(tier_id), gib))
    return out


def _admission_relief(*, held_gib: int, ready_gib: int, output_gib: int,
                      output_enforced: bool, capacity_gib: int,
                      cur_min_gib: int, next_min_gib: int | None,
                      existing_min_next_gib: int, free_gib: int,
                      evictable_gib: int) -> int | None:
    """The free a sweep must reach so ``gate_newcomer`` admits, or ``None``.

    One arithmetic for every relief term (#orphan-pressure, #901): the gate
    decides, and its own terms give the shortfall.  ``None`` when the gate
    admits already, when its answer is permanent or unknown (no eviction
    could admit it), or when the shortfall exceeds what the tier can give
    back (#632: a demand that cannot fit even after everything evictable
    returns asks for nothing).  ``evictable_gib`` is the tier's orphans plus
    the landed ranges past their readers' refill horizons (#903).
    Otherwise the answer is stated as the free the sweeps must reach; their
    stop-at-needed order keeps the eviction to the shortfall.
    """

    decision = window_credit.gate_newcomer(
        held_gib=held_gib, ready_gib=ready_gib, output_gib=output_gib,
        output_enforced=output_enforced, capacity_gib=capacity_gib,
        cur_min_gib=cur_min_gib, next_min_gib=next_min_gib,
        existing_min_next_gib=existing_min_next_gib)
    if decision.get("admit"):
        return None
    if str(decision.get("reason")) != window_credit.REASON_STALL:
        return None
    shortfall = (held_gib + ready_gib + output_gib + cur_min_gib
                 + (next_min_gib or 0) + existing_min_next_gib - capacity_gib)
    if 0 < shortfall <= evictable_gib:
        return free_gib + shortfall
    return None


#: The fastest consumption each claim has provably attained, in bytes per
#: second, by queue, consumer and claim time (#907).  A reader that slowed
#: can speed up again, and its window grows back into the room it gave up; a
#: newcomer admitted into that room in between would be squeezed by it.  So
#: the read footprint of an admitted window is priced at no less than the
#: fastest rate its claim has attained.  Attained, not measured: the
#: horizon's rate counts the whole accepted phase as read, which over-states
#: the rate while the reader is inside it -- by a whole phase over a few
#: seconds when the first report lands just after the claim -- and a peak of
#: that estimate kept for the claim's lifetime would refuse every newcomer
#: beside it.  Held by this process: a restart forgets it, and the first
#: cycle after one prices each claim at its current rate, which is what every
#: window was priced at before #907.  Entries of consumers that are no longer
#: live are dropped each census.
_FASTEST_CONSUMPTION: dict[tuple[str, str, float], float] = {}


def _footprint_consumption(queue: pool.PoolQueue,
                           consumer: Mapping[str, object],
                           plan: Mapping[str, object],
                           tier_record: Mapping[str, object] | None,
                           ) -> tuple[float | None, str]:
    """The consumption rate a read footprint is priced at, and its basis (#907).

    A claimed consumer with accepted progress: the horizon's own measurement
    (bytes through the accepted phase over the time from the claim to its
    report), which is the rate its window is bounded by now, or the fastest
    rate its claim has attained if that is higher.  The attained rate counts
    only the phases before the accepted one, which the reader has certainly
    read by its report, so it is a lower bound on how fast it went and never
    the horizon's in-phase over-estimate.  Anything else --
    a newcomer, a ready consumer, a claim that has reported nothing -- has
    measured no rate, and the tier's announced fill supply stands in, as it
    does in :func:`residency_plan.refill_horizon`: a consumer that reads
    staged bytes cannot keep up a rate above what the tier refills them at.
    ``None`` when neither exists.
    """

    claimed = consumer.get("claimed_unix")
    reported = consumer.get("reported_unix")
    accepted = consumer.get("accepted_phase")
    if (consumer.get("state") == pool.CLAIMED
            and residency_plan.accepted(plan, accepted)      # type: ignore[arg-type]
            and isinstance(claimed, (int, float)) and not isinstance(claimed, bool)
            and isinstance(reported, (int, float)) and not isinstance(reported, bool)
            and math.isfinite(float(claimed)) and math.isfinite(float(reported))
            and float(reported) > float(claimed)):
        phases = list(plan["phases"])                         # type: ignore[arg-type]
        names = [str(phase["name"]) for phase in phases]
        entered = phases[names.index(str(accepted))]
        first = int(phases[0]["start_bytes"])
        elapsed = float(reported) - float(claimed)
        rate = (int(entered["end_bytes"]) - first) / elapsed
        attained = (int(entered["start_bytes"]) - first) / elapsed
        if rate > 0:
            key = (str(queue.root), str(consumer.get("action_key")), float(claimed))
            fastest = max(attained, _FASTEST_CONSUMPTION.get(key, 0.0))
            _FASTEST_CONSUMPTION[key] = fastest
            return max(rate, fastest), "measured"
    supply = _announced_fill_supply(tier_record)
    if supply is not None:
        return supply * storage_tiers.MB, "fill-supply"
    return None, "none"


def _footprint_landing(queue: pool.PoolQueue, plan: Mapping[str, object],
                       tier_record: Mapping[str, object] | None,
                       ) -> float | None:
    """The stage leg's landing rate, exactly as :func:`_consumer_horizon` prices it."""

    rates = [rate for rate in (_landing_rate(queue, key)
                               for key in residency_plan.stage_mover_keys(plan))
             if rate is not None]
    if rates:
        return min(rates)
    sealed = _sealed_fill_bytes_per_s(plan, str(plan.get("tier_id") or ""))
    if sealed is not None:
        return sealed
    supply = _announced_fill_supply(tier_record)
    return None if supply is None else supply * storage_tiers.MB


def _commitment_census(queue: pool.PoolQueue,
                       tiers: Mapping[str, Mapping[str, object]], *,
                       consumers: list,
                       unknown: Iterable[Mapping[str, object]] = (),
                       ) -> dict[str, dict[str, object]]:
    """Per stage tier, what admission has promised and to which window (#907).

    #903 bounded each window by its refill horizon but left admission on
    each newcomer's minimum, so two admitted windows could between them want
    more of the tier than it has, and one reader then waited on the other's
    reading.  This is the ledger of promises the newcomer gate reads instead:

    * every held token nothing can evict -- held, less the tier's orphans,
      less each window's passed legs and legs past its refill horizon;
    * queued new money: ready rows' tier demand no funding record covers;
    * the unheld produced-output windows (``produced_output.
      unheld_window_gib``), whether or not ``--output-windows`` counts them
      in the joint-fit gate (#905): the producer takes that room back from
      free;
    * each window's **read footprint** (:func:`residency_plan.read_footprint`)
      and what it already holds toward it -- its in-horizon legs held or
      queued, and its fence grants.  The difference is its growth.

    Every token is in exactly one of those: a leg is passed, past the
    horizon or ahead of it; an orphan is in no live plan; a queued row is
    new money once, whether the window's holding names it or not.  Stage
    tiers only: a ram miss is a read from the stage, slower but never a
    stall (#906).

    ``unknown`` is every live consumer the caller could not census -- an
    unreadable plan (``tier_id`` empty) or unreadable state on a named tier.
    Its ranges are still counted, as a live item's.  If it may be an
    admitted window (:func:`_uncensused_tier`), its growth is not known and
    the next pass that reads its plan may find it wants its footprint back,
    so its tier is not censused either.  A certain newcomer commits nothing.

    Returns ``{tier_id: {...}}``; a tier whose ledger, queue or output
    census does not read, or that a live consumer may be on uncensused,
    carries ``error`` instead, and its newcomers wait.
    """

    out: dict[str, dict[str, object]] = {}
    kind = storage_tiers.STAGE_CAPACITY_KIND
    tier_ids = sorted(str(tier_id) for tier_id in tiers
                      if storage_tiers.capacity_kind_of(str(tier_id)) == kind)
    if not tier_ids:
        return out
    try:
        ready_items = queue.ready_items()
        wanted, owners = stage_release.live_claims(queue)
    except (OSError, pool.PoolContractError, ValueError) as exc:
        return {tier_id: {"error": f"queue census unreadable: {exc}"}
                for tier_id in tier_ids}
    live = {str(key) for key, *_rest in consumers}
    root = str(queue.root)
    for stale in [entry for entry in _FASTEST_CONSUMPTION
                  if entry[0] == root and entry[1] not in live]:
        _FASTEST_CONSUMPTION.pop(stale, None)
    uncensused: dict[str, str] = {}
    for entry in unknown:
        consumer_key = str(entry.get("consumer") or "")
        entry_tier: str | None = str(entry.get("tier_id") or "")
        if not entry_tier:
            entry_tier = _uncensused_tier(queue, consumer_key)
        if entry_tier is None:
            continue
        uncensused.setdefault(entry_tier, (
            f"{consumer_key[:12] or '(census)'}: {entry.get('error', '')}"))
    for tier_id in tier_ids:
        record = tiers.get(tier_id)
        blind = uncensused.get(tier_id) or uncensused.get("")
        if blind is not None:
            out[tier_id] = {"error": f"live consumer not censused: {blind}"}
            continue
        try:
            ledger = queue.tier_ledger(tier_id)
            held = {str(holder): int(ledger.holder_tokens(holder).get(kind, 0))
                    for holder in ledger.held_keys()}
            capacity = ledger.capacity().get(kind)
        except (OSError, pool.PoolContractError, ValueError) as exc:
            out[tier_id] = {"error": f"ledger unreadable: {exc}"}
            continue
        if capacity is None:
            out[tier_id] = {"error": "no minted capacity for tier kind"}
            continue
        capacity_gib = int(capacity)
        queued: dict[str, int] = {}
        for item in ready_items:
            if not isinstance(item, Mapping):
                continue
            action = item.get("action_key")
            try:
                _host, demands = storage_tiers.split_demand(
                    item.get("resources") or {})
                need = int((demands.get(tier_id) or {}).get(kind, 0) or 0)
            except (ValueError, TypeError, AttributeError):
                continue
            if need <= 0 or not isinstance(action, str):
                continue
            try:
                covered, _generation = queue.funded_cover(tier_id, item, kind, need)
            except (OSError, pool.PoolContractError, ValueError, KeyError):
                covered = 0
            if covered < need:
                queued[action] = need
        try:
            owed = produced_output.unheld_window_gib(queue, tier_id)
        except (OSError, pool.PoolContractError, ValueError) as exc:
            out[tier_id] = {"error": f"output windows unreadable: {exc}"}
            continue
        if owed.get("gib") is None:
            out[tier_id] = {"error": "output windows unknown: " + "; ".join(
                f"{str(entry.get('owner', ''))[:12] or '(scopes)'}: "
                f"{entry.get('error', '')}"
                for entry in owed.get("unknown") or [])}
            continue
        evictable = 0
        try:
            for holder, gib in held.items():
                if not gib or holder in wanted or holder in owners:
                    continue
                receipt = queue.move_record(holder)
                named = (receipt.get("consumer_action_key")
                         if isinstance(receipt, Mapping) else None)
                # A range a live item's receipt names is that item's, not an
                # orphan, even when its plan did not read this pass.
                if named and str(named) not in owners:
                    evictable += gib
        except (OSError, pool.PoolContractError, ValueError) as exc:
            out[tier_id] = {"error": f"orphan census unreadable: {exc}"}
            continue
        windows: dict[str, dict[str, object]] = {}
        error = ""
        for key, consumer, plan, plan_tier in consumers:
            if plan_tier != tier_id:
                continue
            key = str(key)
            try:
                if residency_plan.superseded(queue, plan) is not None:
                    continue      # publishes nothing more: its holdings are static
                accepted = consumer.get("accepted_phase")
                already, staged = _mover_state(queue, plan, tier_id)
                needs = residency_plan.advance_needs(
                    plan, accepted, published=sorted(already),   # type: ignore[arg-type]
                    staged=sorted(staged))
                horizon = _stage_horizon(queue, consumer, plan, record)
                beyond = ({str(row["mover_action_key"])      # type: ignore[index]
                           for row in horizon["beyond"]}     # type: ignore[union-attr]
                          if horizon else set())
                ahead = {str(phase["name"]) for phase in
                         residency_plan.remaining(plan, accepted)}  # type: ignore[arg-type]
                holding = 0
                for leg in residency_plan.legs_over(plan, 0, 1 << 62,
                                                    mover_role="mover_row"):
                    mover = str(leg["mover_row"]["action_key"])  # type: ignore[index]
                    if leg["phase"] not in ahead or mover in beyond:
                        evictable += held.get(mover, 0)
                        continue
                    holding += held.get(mover, 0) + queued.get(mover, 0)
                prefix = f"{window_credit.GRANT_PREFIX}{key[:16]}-"
                holding += sum(gib for holder, gib in held.items()
                               if holder.startswith(prefix))
                rate, basis = _footprint_consumption(queue, consumer, plan, record)
                footprint = residency_plan.read_footprint(
                    plan, accepted, capacity_gib=capacity_gib,   # type: ignore[arg-type]
                    readahead_bytes=_readahead_bytes(consumer.get("item")),
                    landing_bytes_per_s=_footprint_landing(queue, plan, record),
                    consumption_bytes_per_s=rate,
                    report_latency_s=pool.HEARTBEAT_S + CYCLE_INTERVAL_S)
            except (OSError, pool.PoolContractError, residency_plan.ResidencyPlanError,
                    KeyError, TypeError, ValueError) as exc:
                error = f"{key[:12]}: footprint unreadable: {exc!r}"
                break
            windows[key] = {
                "newcomer": _is_newcomer(consumer, needs, already),
                "priority": _priority(consumer),
                "footprint_gib": footprint, "holding_gib": holding,
                "growth_gib": max(0, footprint - holding),
                "own_queued_gib": queued.get(key, 0),
                "consumption_basis": basis,
            }
        if error:
            out[tier_id] = {"error": error}
            continue
        held_total = sum(held.values())
        out[tier_id] = {
            "capacity_gib": capacity_gib, "held_gib": held_total,
            "evictable_gib": evictable, "queued_gib": sum(queued.values()),
            "unheld_output_gib": int(owed["gib"]), "windows": windows,
        }
    return out


def _uncensused_tier(queue: pool.PoolQueue, key: str) -> str | None:
    """The tier a live consumer the census could not read may hold room on.

    Read from its queue item, which names the tier and the leads without
    the plan.  ``None`` when it is certainly a newcomer -- ready, and none
    of its leads published: it has been admitted nowhere, so it commits
    nothing, and it waits for its own plan anyway.  Otherwise the tier its
    residency declares, or ``""`` (every tier) when the item does not say
    or does not read: a claimed consumer, or a ready one whose lead is
    published, is an admitted window whose growth the census cannot know.
    """

    if not key:
        return ""
    for state in (pool.READY, pool.CLAIMED):
        try:
            item = json.loads(queue.item_path(state, key).read_text())
        except FileNotFoundError:
            continue
        except (OSError, ValueError, pool.PoolContractError):
            return ""
        residency = item.get("residency") if isinstance(item, dict) else None
        tier_id = residency.get("tier_id") if isinstance(residency, dict) else None
        if not isinstance(tier_id, str) or not tier_id:
            return ""
        if state == pool.CLAIMED:
            return tier_id
        leads = residency.get("leads")                     # type: ignore[union-attr]
        if not isinstance(leads, list):
            return ""
        try:
            ledger = queue.tier_ledger(tier_id)
            for lead in leads:
                lead = str(lead)
                if (queue.item_path(pool.READY, lead).exists()
                        or queue.item_path(pool.CLAIMED, lead).exists()
                        or ledger.holder_tokens(lead)):
                    return tier_id
        except (OSError, ValueError, pool.PoolContractError):
            return ""
        return None
    return None       # no longer live: nothing to wait for


def _priority(consumer: Mapping[str, object]) -> int:
    value = (consumer.get("item") or {}).get("priority", 0)   # type: ignore[union-attr]
    return value if type(value) is int else 0


def _commitment_decision(census: Mapping[str, object] | None, key: str, *,
                         admitted: set[str]) -> dict[str, object] | None:
    """Whether the commitment admits newcomer ``key`` on one tier (#907).

    ``admitted`` names the newcomers already admitted on this tier in the
    same pass: each is an admitted window from then on, and its growth is
    committed before the next newcomer is asked.  ``None`` for a tier the
    census does not cover (not a stage tier) or a consumer that is no
    newcomer there.  A census that did not read refuses, naming the record.
    """

    if census is None:
        return None
    if "error" in census:
        return {"admit": False, "reason": window_credit.REASON_DEFER_UNKNOWN,
                "permanent": False,
                "commitment": {"error": str(census["error"])}}
    windows = census["windows"]
    assert isinstance(windows, Mapping)
    mine = windows.get(key)
    if not isinstance(mine, Mapping) or not mine["newcomer"]:
        return None
    growth = 0
    others = False
    for other, window in windows.items():
        if other == key or (window["newcomer"] and other not in admitted):
            continue
        others = True
        growth += int(window["growth_gib"])
    committed = (int(census["held_gib"]) - int(census["evictable_gib"])  # type: ignore[arg-type]
                 + int(census["queued_gib"]) + int(census["unheld_output_gib"])  # type: ignore[arg-type]
                 + growth)
    other_queued = int(census["queued_gib"]) - int(mine["own_queued_gib"])  # type: ignore[arg-type]
    lone = (not others and int(census["unheld_output_gib"]) == 0   # type: ignore[arg-type]
            and other_queued == 0)
    decision = window_credit.gate_commitment(
        committed_gib=committed, growth_gib=int(mine["growth_gib"]),
        capacity_gib=int(census["capacity_gib"]), lone=lone)   # type: ignore[arg-type]
    decision["commitment"] = {
        "capacity_gib": census["capacity_gib"],
        "held_gib": census["held_gib"],
        "evictable_gib": census["evictable_gib"],
        "queued_gib": census["queued_gib"],
        "unheld_output_gib": census["unheld_output_gib"],
        "admitted_growth_gib": growth,
        "committed_gib": committed,
        "footprint_gib": mine["footprint_gib"],
        "holding_gib": mine["holding_gib"],
        "growth_gib": mine["growth_gib"],
        "consumption_basis": mine["consumption_basis"],
        "lone": lone,
    }
    return decision


def _commitment_admissions(census: Mapping[str, Mapping[str, object]],
                           ) -> dict[tuple[str, str], dict[str, object]]:
    """Every newcomer's commitment decision, in the joint-fit gate's order (#907).

    For the passes that do not run the joint-fit gate -- adoption and the
    eviction pressure -- but must agree with it: newcomers by priority,
    highest first and otherwise in queue scan order, each admitted one
    committed before the next is asked.
    """

    out: dict[tuple[str, str], dict[str, object]] = {}
    for tier_id, tier in census.items():
        if "error" in tier:
            out[(_ANY_NEWCOMER, tier_id)] = _commitment_decision(  # type: ignore[assignment]
                tier, _ANY_NEWCOMER, admitted=set())
            continue
        windows = tier["windows"]
        assert isinstance(windows, Mapping)
        admitted: set[str] = set()
        for key in sorted((key for key, window in windows.items()
                           if window["newcomer"]),
                          key=lambda key: -int(windows[key]["priority"])):
            decision = _commitment_decision(tier, key, admitted=admitted)
            if decision is None:
                continue
            out[(key, tier_id)] = decision
            if decision["admit"]:
                admitted.add(key)
    return out


#: The key :func:`_commitment_admissions` files a tier's census error under.
_ANY_NEWCOMER = ""


def _commitment_refusal(admissions: Mapping[tuple[str, str], Mapping[str, object]],
                        key: str, tier_id: str) -> Mapping[str, object] | None:
    """The commitment's refusal of newcomer ``key`` on ``tier_id``, or ``None``."""

    decision = admissions.get((key, tier_id)) or admissions.get(
        (_ANY_NEWCOMER, tier_id))
    if decision is None or decision.get("admit"):
        return None
    return decision


def window_pressure(
    queue: pool.PoolQueue, *, tiers: Mapping[str, Mapping[str, object]],
    consumers: list | None = None,
    withdrawn: frozenset[str] | None = None,
    unknown: list[dict[str, object]] | None = None,
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

    Beside that next-phase term, a NEWCOMER gated by a transient joint-fit
    stall contributes its ADMISSION shortfall, bounded to the tier's
    orphans (#orphan-pressure): the joint gate admits a window only when
    held + queued + cur + next fits, and a feasible newcomer waiting on
    room that only orphan reclamation can return would otherwise deadlock
    -- the next-phase term alone never covers cur+next.  The shortfall is
    asked through ``gate_newcomer`` itself with over-estimated
    obligations, and a window that cannot fit even after every orphan
    returns (permanently oversize, or held by live work) asks for
    nothing: no futile eviction.

    A tier no live window is waiting on is absent from the answer, and an
    orphan there stays resident -- held, counted, and ready for the next
    artifact that names it.

    A key with a live withdrawal marker is not pressure either (#708): the
    coordinator will not publish it, and evicting a resident range to make
    room for cancelled work is room nobody will use -- the same deadlock
    shape #632 and #642 closed on the other side.

    The third term is a READY consumer's own claim (#901).  Once its leads
    hold their tokens, the next thing it asks the tier for is its
    claim-time demand -- a produced-output window, say -- and the claim
    takes that from free.  Without this term a withdrawn consumer's landed
    movers held the room the successor's claim needed while nothing asked
    the sweep for it: 2026-09-22, R12 waited 25 minutes in ``ready/``
    beside 484 GiB of a withdrawn consumer's orphans.  The claim is probed
    through the same ``gate_newcomer`` path as a newcomer, as a final
    one-step window with the obligations the claim gate checks (none but
    held), bounded to the tier's orphans the same way.  A withdrawn ready
    key asks for nothing (#708).

    Every term is asked within the consumer's refill horizon (#903): a leg
    past it is not something the window will publish this cycle, so it is
    not pressure -- not as a probe, not as a row queued before the horizon
    existed, and not as a newcomer's current or next.  And the two relief
    terms are bounded by what the tier can give back, which since #903 is
    its orphans plus the landed ranges past their readers' horizons
    (:func:`evict_beyond_horizon` takes those after the orphan sweep).
    """

    need: dict[str, int] = {}
    if consumers is None:
        unknown = []
        consumers = _planned_consumers(queue, tiers, unknown=unknown)
    cancelled = _withdrawn_keys(queue, withdrawn)
    depth = _prefill_depth(load_ram_policy())
    # Newcomer admission probes (#orphan-pressure): collected during the
    # walk, probed once per tier after it. A newcomer has an unpublished
    # lead, even when later ranges were adopted (#829): use the publication
    # gate's identity and remaining needs on both movement legs.
    newcomers: dict[str, list[Mapping[str, object]]] = {}
    landed_next: dict[str, int] = {}
    # Claim-time tier demands of ready consumers whose leads are pinned
    # (#901), per tier: the next thing such a consumer asks the tier for.
    claimants: dict[str, list[int]] = {}
    # The commitment's decisions (#907), asked once and only when a
    # newcomer is found.
    admissions: dict[tuple[str, str], dict[str, object]] | None = None
    for _key, consumer, plan, tier_id in consumers:
        if residency_plan.superseded(queue, plan) is not None:
            # A superseded window publishes nothing (#708), so it is not
            # waiting on room: evicting for it would make room nobody uses.
            continue
        already, staged = _mover_state(queue, plan, tier_id)
        if (consumer.get("state") == pool.READY and _key not in cancelled
                and set(residency_plan.leads_for(plan)) <= staged):
            # A ready consumer whose leads hold their tokens is past the
            # claim's residency gate, which refuses before any token is
            # taken; what refuses it next is its own claim-time tier demand
            # (#901).  Asked of every tier this loop announced, since the
            # demand may name a tier other than the plan's own.  A claimed
            # consumer already holds its reservation, and a withdrawn key
            # will never claim (#708).
            for demand_tier, gib in _claim_tier_demands(
                    consumer.get("item"), tiers):
                claimants.setdefault(demand_tier, []).append(gib)
        accepted = consumer["accepted_phase"]
        # The refill horizon (#903): a leg past it is not something this
        # window will publish this cycle, however much room there is, so it
        # asks for no room either -- not as a probe, not as a queued row
        # published before the horizon existed, and not as a newcomer's
        # current or next.  ``None`` changes nothing.
        horizon_end = _horizon_end(_stage_horizon(
            queue, consumer, plan, tiers.get(tier_id)))
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
        legs: list[tuple[str, int, str, int]] = []
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
                                 int(chunk.get("stage_gib", 0)),
                                 str(phase["name"]),
                                 int(chunk.get("start_bytes", 0))))
            elif isinstance(phase.get("mover_row"), Mapping):
                legs.append((str(phase["mover_row"]["action_key"]),  # type: ignore[index]
                             int(phase.get("stage_gib", 0)),
                             str(phase["name"]),
                             int(phase.get("start_bytes", 0))))
        reading = str(ahead[0]["name"]) if ahead else None
        waiting = [key for key, _gib, phase_name, start in legs
                   if key in already - staged and key not in cancelled
                   and (horizon_end is None or phase_name == reading
                        or start < horizon_end)]
        stage_needs = residency_plan.advance_needs(
            plan, accepted, published=sorted(already), staged=sorted(staged),
            horizon_end_bytes=horizon_end)
        stage_newcomer = _is_newcomer(consumer, stage_needs, already)
        if stage_newcomer:
            if admissions is None:
                admissions = _commitment_admissions(_commitment_census(
                    queue, tiers, consumers=consumers, unknown=unknown or ()))
            if _commitment_refusal(admissions, _key, str(tier_id)) is not None:
                # The commitment refuses it (#907), and no eviction can
                # change that: its window publishes nothing this cycle, so
                # neither its lead nor its admission shortfall is pressure
                # (#632).
                continue
            newcomers.setdefault(str(tier_id), []).append(stage_needs)
        if waiting:
            # A mover already in ``ready/`` or ``claimed/`` that holds no
            # tokens is the plainest form of "the tier needs the tokens": it
            # is queued and cannot be admitted.  The window will not offer it
            # again -- it counts as published -- so asking the window what it
            # would publish next would step straight over it.
            need[tier_id] = max(need.get(tier_id, 0),
                                next(gib for key, gib, _phase, _start in legs
                                     if key == waiting[0]))
            continue
        unbounded = sum(gib for _key, gib, _phase, _start in legs)
        decision = residency_plan.window(
            plan, accepted_phase=accepted,                       # type: ignore[arg-type]
            free_gib=unbounded, capacity_gib=int(capacity),
            published=sorted(already), staged=sorted(staged),
            withdrawn=sorted(cancelled), horizon_end_bytes=horizon_end)
        wanted = decision["publish"]
        assert isinstance(wanted, list)
        if wanted:
            need[tier_id] = max(need.get(tier_id, 0), int(wanted[0]["stage_gib"]))
            if not stage_newcomer and str(wanted[0]["phase"]) != reading:
                # An admitted window's next is the joint gate's
                # ``existing_min_next`` term, minimum first, collected so
                # the newcomer probe asks with the same shape.  Counted
                # for every progressing window so the probe never asks
                # for less relief than the real gate will require.  An
                # unpublished current is not a next: the gate counts it
                # only for a window it permits this pass (#908), and once
                # published it is queued demand the probe counts in full.
                stage = int(wanted[0]["stage_gib"])
                landed_next[tier_id] = min(landed_next.get(tier_id, stage),
                                            stage)
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
                                  prefill_depth=depth,
                                  withdrawn=sorted(cancelled))
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
            runahead_cap_gib=depth, mover_role="ram_mover_row",
            withdrawn=sorted(cancelled),
            horizon_end_bytes=state["horizon_end_bytes"])        # type: ignore[arg-type]
        ram_published = ram_decision["publish"]
        assert isinstance(ram_published, list)
        ram_phases = {str(phase["name"]): phase for phase in plan["phases"]}  # type: ignore[union-attr]
        ram_wanted = [
            entry for entry in ram_published
            if str(entry["phase"]) in ram_phases
            and _stage_source_staged(
                ram_phases[str(entry["phase"])],
                int(entry["start_bytes"]), int(entry["end_bytes"]),
                state["stage_resident"])]
        if ram_wanted:
            need[ram_tier_id] = max(need.get(ram_tier_id, 0),
                                    int(ram_wanted[0]["stage_gib"]))
            ram_needs = residency_plan.advance_needs(
                plan, accepted, published=sorted(state["already"]),
                staged=sorted(state["staged"]), mover_role="ram_mover_row",
                horizon_end_bytes=state["horizon_end_bytes"])    # type: ignore[arg-type]
            if _is_newcomer(consumer, ram_needs, set(state["already"])):
                newcomers.setdefault(ram_tier_id, []).append(ram_needs)
            elif str(ram_wanted[0]["phase"]) != reading:
                # The stage leg's rule: an unpublished current is no next.
                ram_next = int(ram_wanted[0]["stage_gib"])
                landed_next[ram_tier_id] = min(
                    landed_next.get(ram_tier_id, ram_next), ram_next)
    # The owed output windows (#747), once per tier that asks for room.  Off
    # by default, every tier reads ``(0, False)`` and nothing below changes.
    # On, the fence check counts the owed window, so the next-phase relief
    # must leave it free as well, or an admitted window's advance waits
    # beside reclaimable orphans; the newcomer probe counts it too.  An
    # obligation the census cannot read defers the tier's publication, so
    # no relief could admit anything there and none is asked for.
    owed: dict[str, tuple[int, bool]] = {}
    for tier_id in sorted(set(need) | {t for t, w in newcomers.items() if w}):
        owed_gib, owed_enforced, _note, owed_error = (
            output_obligation(queue, tier_id))
        if owed_error:
            need.pop(tier_id, None)
            newcomers.pop(tier_id, None)
            continue
        owed[tier_id] = (owed_gib, owed_enforced)
        if owed_enforced and owed_gib and tier_id in need:
            need[tier_id] += owed_gib
    # Newcomer admission pressure (#orphan-pressure): the joint-fit gate's
    # own decision, asked here for the sweep.  A newcomer gated by a
    # TRANSIENT joint-fit stall is waiting on room that may exist as safe
    # orphans; without this term the sweep only ever relieves the next
    # phase's GiB, the gate keeps refusing on cur+next, and a feasible
    # window deadlocks beside reclaimable bytes.  The probe reuses
    # ``gate_newcomer`` itself -- no second admission arithmetic -- with
    # over-estimated obligations (full queued demand, every landed
    # window's protected next), so the relief it asks for always covers
    # what the real gate will check and never falls short of it; relief
    # is bounded to the tier's orphans, so a window that cannot fit even
    # after every orphan returns asks for nothing and evicts nothing.
    # What the tiers can give back beside their orphans (#903): the landed
    # ranges past their readers' refill horizons.  A live plan's ranges are
    # never orphans, so before #903 a relief bounded to orphans alone asked
    # nothing of a stage one far-ahead window had filled, and a newcomer's
    # gate -- or a running consumer re-gated after its lead retired --
    # waited for as long as that reader took to read it all.
    speculative: dict[str, int] = {}
    if newcomers or claimants:
        for tier_id, rows in _beyond_horizon_candidates(
                queue, tiers, consumers, cancelled).items():
            speculative[tier_id] = sum(int(row["stage_gib"]) for row in rows)  # type: ignore[arg-type]
    for tier_id in sorted({t for t, w in newcomers.items() if w}
                          | {t for t, c in claimants.items() if c}):
        # A tier whose owed output was unreadable lost its newcomers above.
        waiting_newcomers = newcomers.get(tier_id, [])
        waiting_claims = claimants.get(tier_id, [])
        kind = storage_tiers.capacity_kind_of(tier_id)
        try:
            ledger = queue.tier_ledger(tier_id)
            held_total = int(ledger.held().get(kind, 0))
            free_gib = int(ledger.available().get(kind, 0))
            capacity_gib = int(ledger.capacity().get(kind, 0))
        except (OSError, pool.PoolContractError, ValueError):
            continue
        if capacity_gib <= 0:
            continue
        try:
            wanted_claims, owners = stage_release.live_claims(queue)
            orphan_gib = 0
            for holder in ledger.held_keys():
                if holder in wanted_claims or holder in owners:
                    continue
                record = queue.move_record(holder)
                if (isinstance(record, Mapping)
                        and record.get("consumer_action_key")):
                    orphan_gib += int(
                        ledger.holder_tokens(holder).get(kind, 0))
        except (OSError, pool.PoolContractError, ValueError):
            continue
        evictable_gib = orphan_gib + speculative.get(tier_id, 0)
        if evictable_gib <= 0:
            continue
        # A ready consumer's claim (#901): its claim-time demand is a final
        # window of one step, asked of the same gate with the obligations
        # the claim's own tier gate checks.  ``begin_acquire`` takes the
        # demand from free and checks nothing else, so held, the demand and
        # capacity are the whole question: no queued demand, no owed output
        # (the demand may itself be that output window) and no protected
        # next.  Its oversize answer is the claim's ``never_fits``.
        for demand_gib in waiting_claims:
            relief = _admission_relief(
                held_gib=held_total, ready_gib=0, output_gib=0,
                output_enforced=False, capacity_gib=capacity_gib,
                cur_min_gib=demand_gib, next_min_gib=None,
                existing_min_next_gib=0, free_gib=free_gib,
                evictable_gib=evictable_gib)
            if relief is not None:
                need[tier_id] = max(need.get(tier_id, 0), relief)
        if not waiting_newcomers:
            continue
        try:
            ready_full = 0
            for item in queue.ready_items():
                if not isinstance(item, Mapping):
                    continue
                try:
                    _host, demands = storage_tiers.split_demand(
                        item.get("resources") or {})
                except (ValueError, TypeError, AttributeError):
                    continue
                tier_needs = demands.get(tier_id)
                if isinstance(tier_needs, Mapping):
                    ready_full += int(tier_needs.get(kind, 0) or 0)
        except (OSError, pool.PoolContractError, ValueError):
            ready_full = 0
        output_gib, output_enforced = owed[tier_id]
        existing_next = landed_next.get(tier_id, 0)
        for needs in waiting_newcomers:
            nxt = needs.get("next_min_gib")
            relief = _admission_relief(
                held_gib=held_total, ready_gib=ready_full,
                output_gib=output_gib, output_enforced=output_enforced,
                capacity_gib=capacity_gib,
                cur_min_gib=int(needs.get("current_min_gib") or 0),
                next_min_gib=nxt if isinstance(nxt, int) else None,
                existing_min_next_gib=existing_next, free_gib=free_gib,
                evictable_gib=evictable_gib)
            if relief is not None:
                need[tier_id] = max(need.get(tier_id, 0), relief)
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
                    # And the same question again under the queue's lock, so
                    # the look above and this publication are one decision
                    # (#810).
                    queue.publish(**dict(egress_row), recompute=True,
                                  refuse_if_live=True)
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


def _advance_wants(queue: pool.PoolQueue,
                   tiers: Mapping[str, Mapping[str, object]], *,
                   mover_role: str, tier_of, state_of, horizon_of=None,
                   ) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    """Per-consumer advance needs for one movement leg, in queue scan order.

    ``horizon_of(consumer, plan, tier_id)`` answers a window's refill
    horizon (#903) so its needs ask for no leg the window will not publish;
    ``None`` asks the whole plan, as before.

    Returns ``(wants, unknown)``.  One want per live consumer whose plan
    carries this leg on a local tier; one unknown entry per consumer this
    census could not decide -- an unreadable plan, unreadable mover state,
    or a needs check that refuses -- naming the record that stopped it.
    Unknown is never silence: the window caller must not publish for a
    consumer it cannot see, and absence of a gate is never permission.
    Order is the queue's own scan order -- the same fairness the claim poll
    already uses -- never a re-ranking invented here.
    """

    wants: list[dict[str, object]] = []
    unknown: list[dict[str, object]] = []
    try:
        consumers = live_consumers(queue)
    except (OSError, pool.PoolContractError) as exc:
        # No census at all: every consumer is unknown, and the empty key
        # below tells the protection pass the whole tier set is.  Wants are
        # discarded, not half-kept -- obligations on an incomplete census
        # stay exactly where they are.
        return [], [{"consumer": "", "tier_id": "",
                     "leg": mover_role,
                     "error": f"live census unreadable: {exc!r}"}]
    for consumer in consumers:
        key = str(consumer["action_key"])
        refusals: list[Exception] = []
        plan = residency_plan.read(
            queue, key, on_unreadable=refusals.append)
        if plan is None:
            if refusals:
                unknown.append({
                    "consumer": key, "leg": mover_role,
                    "error": f"plan unreadable: {refusals[0]!r}"})
            continue
        tier_id = tier_of(plan)
        if not isinstance(tier_id, str) or tier_id not in tiers:
            continue
        try:
            already, staged = state_of(queue, plan, tier_id)
        except (OSError, pool.PoolContractError) as exc:
            unknown.append({
                "consumer": key, "tier_id": tier_id, "leg": mover_role,
                "error": f"mover state unreadable: {exc!r}"})
            continue
        try:
            role_keys = (residency_plan.ram_mover_keys(plan)
                         if mover_role == "ram_mover_row"
                         else residency_plan.stage_mover_keys(plan))
            rowed = [key for key in role_keys
                     if queue.item_path(pool.READY, str(key)).exists()]
        except (OSError, pool.PoolContractError, ValueError) as exc:
            unknown.append({
                "consumer": key, "tier_id": tier_id, "leg": mover_role,
                "error": f"ready census unreadable: {exc!r}"})
            continue
        try:
            needs = residency_plan.advance_needs(
                plan, consumer["accepted_phase"],  # type: ignore[arg-type]
                published=sorted(already), rowed=rowed, staged=sorted(staged),
                mover_role=mover_role,
                horizon_end_bytes=(None if horizon_of is None
                                   else horizon_of(consumer, plan, tier_id)))
        except residency_plan.ResidencyPlanError as exc:
            unknown.append({
                "consumer": key, "tier_id": tier_id, "leg": mover_role,
                "error": f"advance needs refused: {exc!r}"})
            continue
        already_set = set(already)
        staged_set = set(staged)
        wants.append({
            "key": key, "consumer": consumer, "plan": plan,
            "tier_id": tier_id, "accepted": consumer["accepted_phase"],
            "needs": needs, "already": already_set, "staged": staged_set,
            # Newcomer while its lead is unpublished: the gate blocks the
            # formation event (publishing the lead), not the landing.  Once
            # the lead is queued, later cycles treat it as admitted, and a
            # claimed consumer is admitted for good (#908).
            "newcomer": _is_newcomer(consumer, needs, already_set),
        })
    return wants, unknown


def _bind_fence(queue: pool.PoolQueue, tier_id: str, kind: str,
                grant: str, key: str, plan: Mapping[str, object],
                leg: Mapping[str, object], demand: int) -> bool:
    """Acquire one fence under the grant key and bind its names to a record.

    Crash repair included: a grant left holding by a coordinator that died
    between ``acquire`` and ``write_funding`` is bound (not doubled) here,
    and a later top-up binds the union actually held.  The binding carries
    the mover row's own ``published_unix`` so the claim later verifies the
    exact publication it was fenced for -- a republished content-hash key
    never inherits older credit.  Never raises for queue-state reasons;
    unknown is ``False``.
    """

    try:
        plan_digest = residency_plan.plan_sha256(plan)
        start = int(leg["start_bytes"])
        stop = int(leg["end_bytes"])
        row = pool._read_json(queue.item_path(
            pool.READY, str(leg["mover_action_key"])))
        published_unix = float((row or {}).get("published_unix"))  # type: ignore[arg-type]
    except (ValueError, OSError, KeyError, TypeError, pool.PoolContractError):
        return False
    fields = {"consumer_action_key": str(key), "plan_sha256": plan_digest,
              "mover_action_key": str(leg["mover_action_key"]),
              "range_start_bytes": start, "range_end_bytes": stop,
              "kind": str(kind), "published_unix": published_unix}
    try:
        return bool(queue.reserve_fence(
            str(tier_id), grant, fields, int(demand)))
    except (OSError, pool.PoolContractError, ValueError, KeyError, TypeError):
        return False


def _protect_tier_advances(queue: pool.PoolQueue,
                           tiers: Mapping[str, Mapping[str, object]], *,
                           mover_role: str, tier_of, state_of,
                           horizon_of=None,
                           ) -> dict[str, object]:
    """Gate newcomers and fence protected nexts on every local tier, one pass.

    Returns ``{"gated": {(key, tier_id): {...}}, "protected":
    {(key, tier_id): {...}}, "grants": {(key, tier_id): grant_key},
    "permitted": {(key, tier_id): {...}},
    "unknown_ready": bool, "unknown_tiers": {...},
    "unknown_consumers": {(key, tier_id)}, "events": [...]}``.  Positive
    publication authority: a window may publish movers on a tier this cycle
    only when ``(key, tier_id)`` is in ``permitted`` -- either its advance
    is retained (bound fence, blind-held grant, or already-landed mover) or
    it explicitly needs none (final, ``fence_target`` None) -- and it is in
    neither ``gated`` nor any unknown set.  Every required-but-unproved
    advance lands in ``gated`` (fit/binding) or unknown (unreadable
    evidence); absence of an entry is never permission.  A held grant that
    already covers the whole demand and whose target row is not published
    yet is itself retained proof: it is permitted blind-held and its bind is
    deferred to the pass that sees the row, because a bound record carries
    the row's own ``published_unix`` and gating the row's publication behind
    that bind would be circular.  A partial grant is never such proof: it
    falls through to the bind path and fails closed while the row is
    unpublished, retaining what it holds.  A genuinely failed bind beside a
    published row, or any unreadable record or row, still denies.  The only
    mutations are fence ``acquire``/``release`` (reserve, cancel) plus
    funding-record writes; transfers run in the post-pass settle, queue rows
    are never written here.  Unreadable ledger, ready, or capacity evidence
    defers with the record named.  The dangling-grant cleanup runs only on
    a complete census, so a grant whose consumer went unreadable is
    preserved, never freed.

    Tallies are exact: ``running_extra`` carries admitted-but-unheld
    newcomer currents-plus-nexts; each actual take moves its next from
    planned to held (``running_extra -= next``, ``running_fence +=
    deficit``), already-held advances move out of planned with no new take,
    and failed newcomers roll their footprint back.  The newcomer gate
    counts both running tallies, so two independently fitting windows admit
    and a third does not.
    """

    gated: dict[tuple[str, str], dict[str, object]] = {}
    protected: dict[tuple[str, str], dict[str, object]] = {}
    grants: dict[tuple[str, str], str] = {}
    permitted: dict[tuple[str, str], dict[str, object]] = {}
    events: list[dict[str, object]] = []
    unknown_consumers: set[tuple[str, str]] = set()
    unknown_tiers: set[str] = set()
    unknown_ready = False
    wants, census_unknown = _advance_wants(
        queue, tiers, mover_role=mover_role,
        tier_of=tier_of, state_of=state_of, horizon_of=horizon_of)
    for entry in census_unknown:
        consumer_key = str(entry.get("consumer", ""))
        entry_tier = entry.get("tier_id")
        if consumer_key == "":
            unknown_ready = True
        unknown_consumers.add((consumer_key, str(entry_tier or "")))
        events.append({"event": "advance-deferred-unknown-evidence",
                       "consumer": consumer_key or None,
                       "tier_id": entry_tier, "leg": mover_role,
                       "error": str(entry.get("error", ""))})
    try:
        ready_items = queue.ready_items()
    except (OSError, pool.PoolContractError) as exc:
        events.append({"event": "advance-deferred-unknown-evidence",
                       "leg": mover_role,
                       "error": f"ready scan unreadable: {exc}"})
        return {"gated": gated, "protected": protected, "grants": grants,
                "permitted": permitted,
                "unknown_ready": True, "unknown_tiers": unknown_tiers,
                "unknown_consumers": unknown_consumers, "events": events}
    by_tier: dict[str, list[dict[str, object]]] = {}
    for want in wants:
        by_tier.setdefault(str(want["tier_id"]), []).append(want)
    # What each stage tier has promised its admitted windows (#907): the
    # newcomer gate below admits only a window whose read footprint fits
    # beside them.  Taken on the first newcomer, since most passes gate
    # none.  The stage leg's pass only -- a ram miss reads the stage, it
    # never stalls (#906).  A consumer this census could not read makes its
    # tier's newcomers wait: its growth is unknown, and the next pass that
    # reads it may find it wants its footprint back.
    census: dict[str, dict[str, object]] | None = (
        None if mover_role == "mover_row" else {})
    for tier_id, tier_wants in sorted(by_tier.items()):
        def priority_of(want):
            value = (want["consumer"].get("item") or {}).get("priority", 0)
            return value if type(value) is int else 0

        def priority_candidate(want):
            # Only an unstarted current publication participates in
            # admission priority; CLAIMED read windows neither create nor
            # receive it.  Since #908 a claimed window is never a newcomer
            # (its original lead retiring no longer re-gates it), so the
            # state check here is a second statement of that rule.
            return (bool(want["newcomer"])
                    and want["consumer"].get("state") == pool.READY)

        # Already-admitted windows keep their advancement authority before
        # new work. Among newcomers, honour priority before spending fresh
        # room; stable sorting preserves the existing order for equal ranks.
        # Every admitted window sorts before every newcomer, so its
        # unpublished current is counted before any newcomer is gated
        # (#908).
        tier_wants = sorted(tier_wants, key=lambda want: (
            bool(want["newcomer"]), priority_candidate(want),
            -priority_of(want) if priority_candidate(want) else 0))
        try:
            ledger = queue.tier_ledger(tier_id)
        except (OSError, pool.PoolContractError, ValueError) as exc:
            unknown_tiers.add(tier_id)
            events.append({"event": "advance-deferred-unknown-evidence",
                           "tier_id": tier_id, "leg": mover_role,
                           "error": f"ledger unreadable: {exc}"})
            continue
        kind = storage_tiers.capacity_kind_of(tier_id)
        try:
            held = {str(holder): dict(ledger.holder_tokens(holder))
                    for holder in ledger.held_keys()}
            capacity = ledger.capacity().get(kind)
        except (OSError, pool.PoolContractError, ValueError) as exc:
            unknown_tiers.add(tier_id)
            events.append({"event": "advance-deferred-unknown-evidence",
                           "tier_id": tier_id, "leg": mover_role,
                           "error": f"ledger unreadable: {exc}"})
            continue
        capacity_gib = None if capacity is None else int(capacity)
        if capacity_gib is None:
            unknown_tiers.add(tier_id)
            events.append({"event": "advance-deferred-unknown-evidence",
                           "tier_id": tier_id, "leg": mover_role,
                           "error": "no minted capacity for tier kind"})
            continue
        ready_rows: list[tuple[str, int]] = []
        ready_by_key: dict[str, Mapping[str, object]] = {}
        for item in ready_items:
            if not isinstance(item, Mapping):
                continue
            action = item.get("action_key")
            try:
                _host, demands = storage_tiers.split_demand(
                    item.get("resources") or {})
            except (ValueError, TypeError, AttributeError):
                continue
            tier_needs = demands.get(tier_id)
            if isinstance(tier_needs, Mapping):
                try:
                    row_gib = int(tier_needs.get(kind, 0))
                except (TypeError, ValueError):
                    continue
                if row_gib and isinstance(action, str):
                    ready_rows.append((action, row_gib))
                    ready_by_key[action] = item
        held_total = sum(int(tokens.get(kind, 0)) for tokens in held.values())
        output_gib, output_enforced, output_note, output_error = (
            output_obligation(queue, tier_id))
        if output_error:
            unknown_tiers.add(tier_id)
            events.append({"event": "advance-deferred-unknown-evidence",
                           "tier_id": tier_id, "leg": mover_role,
                           "error": output_error})
            continue
        # New money per queued row: a row fully covered by its funding
        # record will consume its fence instead of free, so it commits no
        # new capacity.  Funded rows are invisible to everyone but the
        # record; anything else counts in full.
        funded_rows: set[str] = set()
        for row_key, row_need in ready_rows:
            try:
                covered, _generation = queue.funded_cover(
                    tier_id, ready_by_key[row_key], kind, row_need)
            except (OSError, pool.PoolContractError, ValueError, KeyError):
                continue
            if covered >= row_need:
                funded_rows.add(row_key)
        ready_new_money = sum(
            need for row_key, need in ready_rows if row_key not in funded_rows)
        # The minimum protected next across landed windows, in queue scan
        # order: a second current may not land in the room the first advance
        # was promised.  Same-pass newcomers commit their unpublished
        # current-plus-next to the running footprint instead (see below), so
        # neither source is ever counted twice.  Funded nexts commit no new
        # money (their fence already holds it), so only unfunded ones reserve
        # room here.
        reserve_next: int | None = None
        for want in tier_wants:
            if want["newcomer"]:
                continue
            for candidate in (want["needs"].get("waiting") or []):
                if not isinstance(candidate, dict):
                    continue
                mover_name = str(candidate["mover_action_key"])
                if mover_name in want["already"]:
                    continue
                need_gib = int(candidate["stage_gib"])
                row_item = ready_by_key.get(mover_name)
                if row_item is None:
                    # Unpublished, or already claimed-holding (counted in
                    # held): either way no *new* room need be kept beyond
                    # what the holder scan already counts.
                    if int(held.get(mover_name, {}).get(kind, 0)) >= need_gib:
                        break
                    if candidate.get("phase") == want["needs"].get(
                            "reading_phase"):
                        # The window's own unpublished current is no
                        # landed window's next (#908).  It pays from free
                        # when the window publishes it, so it is counted
                        # below only for a window this pass permits;
                        # reserving it here would let a window that cannot
                        # publish at all -- gated on its own fence, say --
                        # hold every newcomer out (#881).
                        break
                    covered = 0
                else:
                    try:
                        covered, _generation = queue.funded_cover(
                            tier_id, row_item, kind, need_gib)
                    except (OSError, pool.PoolContractError, ValueError, KeyError):
                        covered = 0
                if covered < need_gib and (
                        reserve_next is None or need_gib < reserve_next):
                    reserve_next = need_gib
                break
        running_extra = 0
        running_fence = 0
        expected_grants: set[str] = set()
        waiting_priority: int | None = None
        waiting_consumer: str | None = None
        # Admitted windows' unpublished currents, counted once every
        # admitted window has been decided and before the first newcomer is
        # gated (#908): the newcomer gate then sees each current a
        # permitted window is about to publish from free, exactly as it
        # sees a same-pass newcomer's.  A window that is not permitted
        # publishes nothing this pass and counts nothing.
        admitted_currents: dict[str, int] = {}
        currents_counted = False
        admitted_newcomers: set[str] = set()
        for want in tier_wants:
            key = str(want["key"])
            needs = want["needs"]
            waiting = needs.get("waiting")
            assert isinstance(waiting, list)
            cur = int(needs.get("current_min_gib") or 0)
            nxt = needs.get("next_min_gib")
            next_gib = int(nxt) if isinstance(nxt, int) else 0
            added_extra = 0
            if want["newcomer"] and not currents_counted:
                running_extra += sum(
                    gib for other, gib in admitted_currents.items()
                    if (other, tier_id) in permitted)
                currents_counted = True
            if not want["newcomer"]:
                admitted_currents[key] = _unpublished_current_gib(
                    needs, held=held, rowed=ready_by_key, kind=kind)
            if want["newcomer"]:
                priority = priority_of(want)
                if (priority_candidate(want) and waiting_priority is not None
                        and priority < waiting_priority):
                    gated[(key, tier_id)] = {
                        "reason": "higher-priority-window-waiting", "permanent": False,
                        "need_gib": cur, "tier_id": tier_id,
                        "waiting_consumer": waiting_consumer,
                        "waiting_priority": waiting_priority,
                        "output_note": output_note,
                    }
                    continue
                decision = window_credit.gate_newcomer(
                    held_gib=held_total + running_extra + running_fence,
                    ready_gib=ready_new_money,
                    output_gib=output_gib, output_enforced=output_enforced,
                    capacity_gib=capacity_gib,
                    cur_min_gib=cur, next_min_gib=nxt if isinstance(nxt, int) else None,
                    existing_min_next_gib=reserve_next or 0)
                # The commitment (#907): the newcomer's read footprint beside
                # every admitted window's.  Asked unless the joint-fit gate
                # refused for good, and it names the refusal when it refuses
                # too, because no eviction can admit what it refuses: a
                # ``joint-fit-stall`` would promise room that eviction
                # cannot make.
                commitment: dict[str, object] | None = None
                if decision["admit"] or (
                        decision["reason"] == window_credit.REASON_STALL
                        and not decision.get("permanent")):
                    if census is None:
                        census = _commitment_census(queue, tiers, consumers=[
                            (other["key"], other["consumer"], other["plan"],
                             other["tier_id"]) for other in wants],
                            unknown=census_unknown)
                    verdict = _commitment_decision(
                        census.get(tier_id), key, admitted=admitted_newcomers)
                    if verdict is not None:
                        commitment = dict(verdict["commitment"])  # type: ignore[arg-type]
                        if not verdict["admit"]:
                            decision = {
                                "admit": False, "reason": verdict["reason"],
                                "permanent": False,
                                "output_note": decision.get("output_note", "")}
                if not decision["admit"]:
                    gated[(key, tier_id)] = {
                        "reason": str(decision["reason"]),
                        "permanent": bool(decision.get("permanent")),
                        "need_gib": cur, "tier_id": tier_id,
                        "output_note": str(decision.get("output_note") or ""),
                    }
                    if commitment is not None:
                        gated[(key, tier_id)]["commitment"] = commitment
                    # Do not perpetually refill smaller lower-priority
                    # windows while existing promises drain. Only a feasible
                    # transient wait establishes this barrier; oversized or
                    # unknown work cannot block otherwise useful newcomers.
                    # A commitment wait is feasible by construction: a read
                    # footprint never exceeds the tier (#907).
                    if (priority_candidate(want)
                            and decision["reason"] in (
                                window_credit.REASON_STALL,
                                window_credit.REASON_COMMITMENT)
                            and not decision.get("permanent")
                            and cur + next_gib <= capacity_gib
                            and (waiting_priority is None or priority > waiting_priority)):
                        waiting_priority, waiting_consumer = priority, key
                    continue
                running_extra += cur + next_gib
                added_extra = cur + next_gib
                admitted_newcomers.add(key)
            # One fence per window: the advance after the frontier.  The
            # frontier pays from free under the gate's count; exactly the
            # advance is fenced -- bound when its row is queued, taken blind
            # under the grant before its publish when it is not, so no
            # admitted current is exposed without its advance reservation
            # real.  Both spellings target the same phase, so the blind take
            # and the bind agree.  A blind grant that covers the whole demand
            # and whose target row has not published is itself the live
            # reservation: it permits the window (blind-held) and binds in
            # the later pass that sees the row, never the reverse; a partial
            # grant permits nothing and fails closed.  Positive authority:
            # only ``permitted`` publishes; every required-but-unproved
            # advance below lands in ``gated`` or unknown.
            target = needs.get("fence_target")
            if not isinstance(target, dict):
                # Final: explicitly needs no advance.  Still subject to the
                # newcomer gate above and the caller's unknown check.
                permitted[(key, tier_id)] = {
                    "advance": "final", "tier_id": tier_id,
                    "leg": mover_role,
                }
                continue
            first = target
            assert isinstance(first, dict)
            demand = int(first["stage_gib"])
            phase = str(first["phase"])
            chunk = first["chunk_index"]
            grant = window_credit.grant_key(key, tier_id, mover_role, phase,
                                            chunk if isinstance(chunk, int) else None)
            expected_grants.add(grant)
            grants[(key, tier_id)] = grant
            held_grant = int(held.get(grant, {}).get(kind, 0))
            mover = str(first["mover_action_key"])
            mover_holds = int(held.get(mover, {}).get(kind, 0)) >= demand
            # A mover holding its own live fence (reserved/transferring
            # record binding those tokens) is not landed: its holdings are
            # the advance reservation, and no cancel decision may read them
            # as bytes.  Only holdings without a live binding count as
            # landed for cancel purposes.
            target_fence_live = False
            if mover_holds:
                fence_status, fence_record, fence_reason = (
                    queue.read_funding_evidence(mover, tier_id))
                if fence_status == "unknown":
                    # The mover's holdings may be a live bound fence whose
                    # record cannot be read; reading them as landed bytes
                    # would cancel real reservation authority on a guess.
                    # Defer with the record named.
                    unknown_consumers.add((key, tier_id))
                    events.append({"event": "advance-deferred-unknown-evidence",
                                   "consumer": key, "tier_id": tier_id,
                                   "leg": mover_role,
                                   "error": fence_reason
                                   or "funding record unreadable"})
                    if added_extra:
                        running_extra -= added_extra
                        admitted_newcomers.discard(key)
                    continue
                if (fence_status == "record"
                        and fence_record.get("state") in (
                            "reserved", "transferring")):
                    target_fence_live = True
            try:
                mover_rowed = queue.item_path(pool.READY, mover).exists()
            except (OSError, pool.PoolContractError) as exc:
                unknown_consumers.add((key, tier_id))
                events.append({"event": "advance-deferred-unknown-evidence",
                               "consumer": key, "tier_id": tier_id,
                               "leg": mover_role,
                               "error": f"ready census unreadable: {exc!r}"})
                if added_extra:
                    running_extra -= added_extra
                    admitted_newcomers.discard(key)
                continue
            if held_grant > 0:
                record_status, record, record_reason = (
                    queue.read_funding_evidence(mover, tier_id))
                if record_status == "unknown":
                    # Unknown is not absent: a present-but-unreadable
                    # record may still bind the grant-held tokens, and
                    # binding beside it would double-fence.  Defer with
                    # the reason named; the reserve path defers the same
                    # way on its own strict read.
                    unknown_consumers.add((key, tier_id))
                    events.append({"event": "advance-deferred-unknown-evidence",
                                   "consumer": key, "tier_id": tier_id,
                                   "leg": mover_role,
                                   "error": record_reason
                                   or "funding record unreadable"})
                    if added_extra:
                        running_extra -= added_extra
                        admitted_newcomers.discard(key)
                    continue
                if record_status == "absent":
                    if (not mover_rowed and not mover_holds
                            and held_grant >= demand):
                        # The advance's row is not published yet and the
                        # grant already holds the *whole* demand: it is the
                        # live blind pre-publish reservation (the ``#832``
                        # nonfinal fence), and binding needs the row's own
                        # ``published_unix`` to stay immutable.  Permit
                        # blind-held and defer the bind to the pass after
                        # the row publishes; gating here would deny the very
                        # publication the bind waits for, wedging the window
                        # behind its own fence.  A partial grant is not the
                        # advance's reservation: it falls through to the
                        # bind path and fails closed while the row is
                        # unpublished (unsupported replenishment before
                        # publication), with its tokens retained.
                        if added_extra:
                            running_extra -= next_gib
                        permitted[(key, tier_id)] = {
                            "advance": "blind-held", "tier_id": tier_id,
                            "leg": mover_role, "mover": mover,
                            "grant": grant, "need_gib": demand,
                        }
                        continue
                    # Tokens held with no binding beside a published row (a
                    # crash between acquire and write): bind the names now
                    # rather than fence twice.  A failed bind proves nothing
                    # -- deny, do not publish.
                    bound = _bind_fence(
                        queue, tier_id, kind, grant, key, want["plan"],
                        first, demand)
                    if not bound:
                        gated[(key, tier_id)] = {
                            "reason": window_credit.REASON_STALL,
                            "permanent": False, "need_gib": cur,
                            "tier_id": tier_id,
                            "output_note": output_note,
                        }
                        if added_extra:
                            running_extra -= added_extra
                            admitted_newcomers.discard(key)
                        continue
                    _reread_status, record, _reread_reason = (
                        queue.read_funding_evidence(mover, tier_id))
                    if _reread_status != "record":
                        # The bind claims success but its record cannot be
                        # read back: deny publication and retain both the
                        # fence and the record for the next cycle rather
                        # than cancel on a guess.
                        gated[(key, tier_id)] = {
                            "reason": window_credit.REASON_STALL,
                            "permanent": False, "need_gib": cur,
                            "tier_id": tier_id,
                            "output_note": output_note,
                        }
                        if added_extra:
                            running_extra -= added_extra
                            admitted_newcomers.discard(key)
                        continue
                # Coordinator-side binding check: the fence belongs to this
                # live plan and consumer.  A replaced plan (same mover keys
                # under a new digest) or a foreign consumer never inherits
                # it -- the claim path cannot see these fields on the row,
                # so this comparison is the only place that can.
                try:
                    live_digest = residency_plan.plan_sha256(want["plan"])
                except (ValueError, OSError):
                    live_digest = ""
                if (record is None
                        or str(record.get("consumer_action_key")) != key
                        or str(record.get("plan_sha256")) != live_digest):
                    released = window_credit.cancel(ledger, grant)["released"]
                    if released:
                        held_total -= released
                    if record is not None and record.get("state") in (
                            "reserved", "transferring"):
                        queue.advance_funding_state(
                            mover, tier_id, expect=str(record.get("state")),
                            advance_to="released",
                            generation=(str(record.get("generation"))
                                        if isinstance(record.get("generation"),
                                                     str) else None))
                    if released:
                        events.append({"event": "advance-released",
                                       "consumer": key, "tier_id": tier_id,
                                       "leg": mover_role,
                                       "reason": "binding-stale",
                                       "released_gib": released})
                    # Stale binding released: the advance is required but
                    # unheld this cycle -- deny so the current is not
                    # published without its reservation.  Next cycle
                    # re-fences fresh.
                    gated[(key, tier_id)] = {
                        "reason": window_credit.REASON_STALL,
                        "permanent": False, "need_gib": cur,
                        "tier_id": tier_id,
                        "output_note": output_note,
                    }
                    if added_extra:
                        running_extra -= added_extra
                        admitted_newcomers.discard(key)
                    continue
                superseded = residency_plan.superseded(queue, want["plan"])
                due = window_credit.cancel_due(
                    consumer_live=True, superseded=superseded is not None,
                    need_gib=demand,
                    mover_holds_need=(mover_holds and not target_fence_live))
                if due is not None:
                    released = window_credit.cancel(ledger, grant)["released"]
                    if released:
                        held_total -= released
                    record = queue.read_funding(mover, tier_id)
                    if record is not None and record.get("state") in (
                            "reserved", "transferring"):
                        queue.advance_funding_state(
                            mover, tier_id, expect=str(record.get("state")),
                            advance_to="released",
                            generation=(str(record.get("generation"))
                                        if isinstance(record.get("generation"),
                                                     str) else None))
                    if released:
                        events.append({"event": "advance-released",
                                       "consumer": key, "tier_id": tier_id,
                                       "leg": mover_role, "reason": due,
                                       "released_gib": released})
                    if due == "need-landed":
                        # The mover already carries the advance's bytes:
                        # no fence needed, publication explicitly permitted.
                        # The planned next was already counted in held
                        # (mover holdings), so move it out of planned.
                        if added_extra:
                            running_extra -= next_gib
                        permitted[(key, tier_id)] = {
                            "advance": "landed", "tier_id": tier_id,
                            "leg": mover_role, "mover": mover,
                        }
                        continue
                    # Superseded callers block on superseded anyway; any
                    # other due leaves the required advance unheld -- deny.
                    if due != "plan-superseded":
                        gated[(key, tier_id)] = {
                            "reason": window_credit.REASON_STALL,
                            "permanent": False, "need_gib": cur,
                            "tier_id": tier_id,
                            "output_note": output_note,
                        }
                    if added_extra:
                        running_extra -= added_extra
                        admitted_newcomers.discard(key)
                    continue
                if added_extra:
                    # Already-held advance was planned: it sits in
                    # held_total, so move it out of planned with no new take.
                    running_extra -= next_gib
                permitted[(key, tier_id)] = {
                    "advance": "held", "tier_id": tier_id,
                    "leg": mover_role, "mover": mover,
                    "grant": grant, "need_gib": demand,
                }
                protected[(key, tier_id)] = {
                    "grant": grant, "mover": mover, "need_gib": demand,
                    "phase": phase, "tier_id": tier_id, "kind": kind,
                    "leg": mover_role,
                }
                continue
            prior = needs.get("fence_prior")
            assert isinstance(prior, list)
            retired = True
            prior_unknown: Exception | None = None
            for leg in prior:
                assert isinstance(leg, dict)
                if str(leg["mover_action_key"]) in want["staged"]:
                    continue
                try:
                    if queue.item_path(
                            pool.DONE, str(leg["egress_action_key"])).exists():
                        continue
                except (OSError, pool.PoolContractError) as exc:
                    prior_unknown = exc
                    retired = False
                    break
                retired = False
                break
            if prior_unknown is not None:
                unknown_consumers.add((key, tier_id))
                events.append({"event": "advance-deferred-unknown-evidence",
                               "consumer": key, "tier_id": tier_id,
                               "leg": mover_role,
                               "error": f"prior census unreadable: {prior_unknown!r}"})
                if added_extra:
                    running_extra -= added_extra
                    admitted_newcomers.discard(key)
                continue
            if not window_credit.replenish_ok(
                    grant_outstanding=False, need_gib=demand,
                    prior_retired=retired):
                gated[(key, tier_id)] = {
                    "reason": window_credit.REASON_STALL,
                    "permanent": False, "need_gib": cur,
                    "tier_id": tier_id,
                    "output_note": output_note,
                }
                if added_extra:
                    running_extra -= added_extra
                    admitted_newcomers.discard(key)
                continue
            if mover_holds:
                # Already holding under its own key: either the live fence
                # (reserved/transferring record) awaiting claim, or landed
                # bytes.  Either way the room is real and already counted
                # in held_total -- move it out of planned, take nothing.
                if added_extra:
                    running_extra -= next_gib
                if target_fence_live:
                    permitted[(key, tier_id)] = {
                        "advance": "held", "tier_id": tier_id,
                        "leg": mover_role, "mover": mover,
                        "grant": grant, "need_gib": demand,
                    }
                    protected[(key, tier_id)] = {
                        "grant": grant, "mover": mover, "need_gib": demand,
                        "phase": phase, "tier_id": tier_id, "kind": kind,
                        "leg": mover_role,
                    }
                else:
                    permitted[(key, tier_id)] = {
                        "advance": "landed", "tier_id": tier_id,
                        "leg": mover_role, "mover": mover,
                    }
                continue
            if not mover_rowed:
                # Blind take before the publish: the tokens are held under
                # the grant from this point on, so no unrelated claim can
                # take the advance's room between its publish and its bind
                # later this cycle.  A crash between take and bind leaves
                # grant-held tokens with no record, which binds (rowed by
                # then) or defers (dangling release if the window died)
                # next cycle.  Failing the take stalls this window instead
                # of exposing it: the publish below is skipped via gated.
                # Already-held grants bind with no new money, so no fit
                # check; only a fresh take from free needs one.
                try:
                    grant_have = int(ledger.holder_tokens(grant).get(kind, 0))
                except (OSError, pool.PoolContractError, ValueError) as exc:
                    unknown_consumers.add((key, tier_id))
                    events.append({"event": "advance-deferred-unknown-evidence",
                                   "consumer": key, "tier_id": tier_id,
                                   "leg": mover_role,
                                   "error": f"grant census unreadable: {exc!r}"})
                    if added_extra:
                        running_extra -= added_extra
                        admitted_newcomers.discard(key)
                    continue
                if int(demand) - grant_have <= 0:
                    # Grant already sufficient (held_total counts it):
                    # move planned out, take nothing.
                    if added_extra:
                        running_extra -= next_gib
                    permitted[(key, tier_id)] = {
                        "advance": "blind-held", "tier_id": tier_id,
                        "leg": mover_role, "mover": mover,
                        "grant": grant, "need_gib": demand,
                    }
                    continue
                if not window_credit.fence_fits(
                        held_gib=held_total + running_extra + running_fence,
                        ready_gib=ready_new_money, output_gib=output_gib,
                        capacity_gib=capacity_gib):
                    gated[(key, tier_id)] = {
                        "reason": window_credit.REASON_STALL,
                        "permanent": False, "need_gib": cur,
                        "tier_id": tier_id,
                        "output_note": output_note,
                    }
                    if added_extra:
                        running_extra -= added_extra
                        admitted_newcomers.discard(key)
                    continue
                deficit = int(demand) - grant_have
                try:
                    taken = bool(ledger.acquire(
                        grant, {kind: int(deficit)}))
                except (OSError, pool.PoolContractError, ValueError):
                    taken = False
                if not taken:
                    gated[(key, tier_id)] = {
                        "reason": window_credit.REASON_STALL,
                        "permanent": False, "need_gib": cur,
                        "tier_id": tier_id,
                        "output_note": output_note,
                    }
                    if added_extra:
                        running_extra -= added_extra
                        admitted_newcomers.discard(key)
                    continue
                # Planned next becomes held: exact, once.
                if added_extra:
                    running_extra -= next_gib
                running_fence += int(deficit)
                permitted[(key, tier_id)] = {
                    "advance": "blind-held", "tier_id": tier_id,
                    "leg": mover_role, "mover": mover,
                    "grant": grant, "need_gib": demand,
                }
                continue
            # Binding an already-held grant commits no new capacity, so no
            # fit check; only a fresh take from free needs one.  Tallies are
            # exact: held_total is the pass-start snapshot; running_extra
            # carries admitted currents plus still-unheld nexts;
            # running_fence carries same-pass takes.
            try:
                grant_have = int(ledger.holder_tokens(grant).get(kind, 0))
            except (OSError, pool.PoolContractError, ValueError) as exc:
                unknown_consumers.add((key, tier_id))
                events.append({"event": "advance-deferred-unknown-evidence",
                               "consumer": key, "tier_id": tier_id,
                               "leg": mover_role,
                               "error": f"grant census unreadable: {exc!r}"})
                if added_extra:
                    running_extra -= added_extra
                    admitted_newcomers.discard(key)
                continue
            if int(demand) - grant_have > 0:
                # Fresh take: the peak must fit before committing new money.
                if not window_credit.fence_fits(
                        held_gib=held_total + running_extra + running_fence,
                        ready_gib=ready_new_money, output_gib=output_gib,
                        capacity_gib=capacity_gib):
                    gated[(key, tier_id)] = {
                        "reason": window_credit.REASON_STALL,
                        "permanent": False, "need_gib": cur,
                        "tier_id": tier_id,
                        "output_note": output_note,
                    }
                    if added_extra:
                        running_extra -= added_extra
                        admitted_newcomers.discard(key)
                    continue
            try:
                kept = queue.read_funding(mover, tier_id)
                kept_generation = (str(kept.get("generation"))
                                   if isinstance(kept, dict)
                                   and isinstance(kept.get("generation"), str)
                                   else None)
            except (OSError, pool.PoolContractError, ValueError):
                kept_generation = None
            if not _bind_fence(
                    queue, tier_id, kind, grant, key, want["plan"],
                    first, demand):
                gated[(key, tier_id)] = {
                    "reason": window_credit.REASON_STALL,
                    "permanent": False, "need_gib": cur,
                    "tier_id": tier_id,
                    "output_note": output_note,
                }
                if added_extra:
                    running_extra -= added_extra
                    admitted_newcomers.discard(key)
                continue
            # Exact move: planned next becomes held.
            if added_extra:
                running_extra -= next_gib
            running_fence += max(0, int(demand) - grant_have)
            bound = queue.read_funding(mover, tier_id)
            if (kept_generation is None or bound is None
                    or str(bound.get("generation")) != kept_generation
                    or bound.get("state") != "reserved"):
                # Newly bound this pass; a kept fence re-reports nothing.
                events.append({"event": "advance-credit-held",
                               "consumer": key, "tier_id": tier_id,
                               "leg": mover_role, "phase": phase,
                               "held_gib": demand})
            permitted[(key, tier_id)] = {
                "advance": "bound", "tier_id": tier_id,
                "leg": mover_role, "mover": mover,
                "grant": grant, "need_gib": demand,
            }
            protected[(key, tier_id)] = {
                "grant": grant, "mover": mover, "need_gib": demand,
                "phase": phase, "tier_id": tier_id, "kind": kind,
                "leg": mover_role,
            }
        # Dangling-grant cleanup runs only on a complete census: a grant
        # whose consumer went unreadable this cycle is preserved, never
        # freed -- releasing on a partial view could return room a live
        # window still counts on.  A truly dead grant waits one cycle.
        if unknown_consumers:
            events.append({"event": "advance-deferred-unknown-evidence",
                           "tier_id": tier_id, "leg": mover_role,
                           "error": "consumer census incomplete: "
                                    "dangling cleanup withheld"})
            continue
        try:
            dangling = [holder for holder in window_credit.held_grants(ledger)
                        if holder not in expected_grants]
        except (OSError, pool.PoolContractError):
            events.append({"event": "advance-deferred-unknown-evidence",
                           "tier_id": tier_id, "leg": mover_role,
                           "error": "grant census unreadable: cleanup withheld"})
            continue
        for grant in dangling:
            released = window_credit.cancel(ledger, grant)["released"]
            if released:
                events.append({"event": "advance-released",
                               "consumer": None, "tier_id": tier_id,
                               "leg": mover_role, "reason": "dangling-grant",
                               "released_gib": released})
    return {"gated": gated, "protected": protected, "grants": grants,
            "permitted": permitted,
            "unknown_ready": unknown_ready, "unknown_tiers": unknown_tiers,
            "unknown_consumers": unknown_consumers, "events": events}


def _settle_terminal_fence(queue: pool.PoolQueue, ledger, *, tier_id: str,
                           mover_role: str, consumer: str | None,
                           mover: str) -> list[dict[str, object]]:
    """Release one terminal mover's exact unspent fence, never its physical.

    Shared by the protected loop (mover terminal with a live entry) and the
    funding scan below (mover terminal with no live entry -- a withdrawn or
    failed consumer's stranded fence, which no protected entry visits).
    ``consumed`` records are landed bytes: finish kept them on purpose and
    only an egress may return them.  A ``transferring`` record whose
    generation and token set match the terminal attempt's own
    ``tier_funding`` proof is that attempt's fence-or-bytes, never free
    credit.  Anything else releases only on an exact name match between the
    bound tokens and what the mover holds; the record always closes.
    Unreadable terminal, proof, or holder evidence defers with the record
    named and leaves recoverable authority intact -- absence of proof is
    never proof of absence.  Takes the mover's transition lock
    non-blocking (a live claim wins, this defers), so both call sites are
    safe locked or not.
    """

    events: list[dict[str, object]] = []
    try:
        with queue._transition_locked(mover, blocking=False) as acquired:
            if not acquired:
                return events
            status, record, funding_reason = queue.read_funding_evidence(
                mover, str(tier_id))
            if status == "unknown":
                # A present-but-unreadable record may still bind held
                # tokens: name it and retain, never settle beside it.
                events.append({"event": "advance-deferred-unknown-evidence",
                               "consumer": consumer, "tier_id": str(tier_id),
                               "leg": str(mover_role),
                               "error": funding_reason
                               or "funding record unreadable"})
                return events
            if status == "absent":
                return events
            state = str(record.get("state"))
            if state in ("consumed", "released"):
                return events
            try:
                terminal = (queue.item_path(pool.DONE, mover).exists()
                            or queue.item_path(pool.FAILED, mover).exists()
                            or queue.item_path(
                                pool.WITHDRAWN, mover).exists())
            except (OSError, pool.PoolContractError) as exc:
                events.append({"event": "advance-deferred-unknown-evidence",
                               "consumer": consumer, "tier_id": str(tier_id),
                               "leg": str(mover_role),
                               "error": f"terminal census unreadable: {exc!r}"})
                return events
            if not terminal:
                return events
            bound = record.get("tokens")
            bound_names = (set(str(name) for name in bound)
                           if isinstance(bound, list) and bound else set())
            bound_generation = (str(record.get("generation"))
                                if isinstance(record.get("generation"), str)
                                else None)
            if not bound_names or bound_generation is None:
                return events
            if state == "transferring":
                bound_to_attempt = False
                proof_unknown: Exception | None = None
                proof_reason: str | None = None
                for _state in (pool.DONE, pool.FAILED):
                    try:
                        ended = pool._read_json(queue.item_path(_state, mover))
                    except (OSError, pool.PoolContractError) as exc:
                        # A terminal file that exists but cannot be read
                        # may bind this fence: defer, do not free.
                        try:
                            if queue.item_path(_state, mover).exists():
                                proof_unknown = exc
                                break
                        except (OSError, pool.PoolContractError) as exc2:
                            proof_unknown = exc2
                            break
                        continue
                    if ended is None:
                        # ``_read_json`` answers ``None`` for ENOENT and
                        # for a present-but-empty file.  Only ENOENT is
                        # absence: a zero-byte proof is present and
                        # unproved -- it may be this attempt's own torn
                        # binding, so it defers like any unreadable one.
                        try:
                            if queue.item_path(_state, mover).exists():
                                proof_reason = (
                                    f"terminal proof present but empty: "
                                    f"{queue.item_path(_state, mover)}")
                                break
                        except (OSError, pool.PoolContractError) as exc2:
                            proof_unknown = exc2
                            break
                        continue
                    if not isinstance(ended, Mapping):
                        continue
                    proof = ended.get("tier_funding")
                    if not isinstance(proof, Mapping):
                        continue
                    tier_proof = proof.get(str(tier_id))
                    if not isinstance(tier_proof, Mapping):
                        continue
                    try:
                        proof_names = {
                            str(name) for name in
                            tier_proof.get("tokens") or []}
                    except (TypeError, ValueError) as exc:
                        # A malformed present proof is unknown evidence,
                        # not positive absence of the binding.
                        proof_unknown = exc
                        break
                    if (str(tier_proof.get("generation")) == bound_generation
                            and proof_names == bound_names
                            and proof_names):
                        bound_to_attempt = True
                        break
                if bound_to_attempt:
                    return events
                if proof_unknown is not None or proof_reason is not None:
                    events.append({"event": "advance-deferred-unknown-evidence",
                                   "consumer": consumer,
                                   "tier_id": str(tier_id),
                                   "leg": str(mover_role),
                                   "error": (
                                       f"terminal proof unreadable: "
                                       f"{proof_unknown!r}"
                                       if proof_unknown is not None
                                       else proof_reason)})
                    return events
            try:
                # Error-visible census (the accepted #742 semantics):
                # ``Path.glob`` hides an ``EACCES`` holder directory as an
                # empty listing, which would silently strand a held fence
                # while the record closes.  True absence (no holder
                # directory) reads empty; unreadable defers below.
                mover_names = pool.held_names_visible(ledger, mover)
            except (OSError, pool.PoolContractError, ValueError) as exc:
                events.append({"event": "advance-deferred-unknown-evidence",
                               "consumer": consumer, "tier_id": str(tier_id),
                               "leg": str(mover_role),
                               "error": f"holder census unreadable: {exc!r}"})
                return events
            if mover_names == bound_names:
                released = window_credit.cancel(ledger, mover)["released"]
                if released:
                    events.append({"event": "advance-released",
                                   "consumer": consumer,
                                   "tier_id": str(tier_id),
                                   "leg": str(mover_role),
                                   "reason": "mover-terminal-fused",
                                   "released_gib": released})
            queue.advance_funding_state(
                mover, str(tier_id), expect=state,
                advance_to="released", generation=bound_generation)
    except (OSError, pool.PoolContractError, ValueError):
        pass
    return events


def _settle_protected(queue: pool.PoolQueue,
                        protection: Mapping[str, object]) -> list[dict[str, object]]:
    """Move held fences onto their published movers; release the moot ones.

    Transfer runs under the mover's transition lock (never blocking: a busy
    mover belongs to a live claim, which wins), so a ready claim racing the
    coordinator cannot interleave mid-move.  ``transfer`` never lets a token
    touch free, and a crash part-way leaves the sum split across the two
    holders -- the next cycle completes the remainder by the same rule, which
    is the whole partial-transfer recovery proof.  The mover's claim then
    counts the fused fence toward its demand (funded-claim bookkeeping), so
    nothing is ever charged twice.

    Exact-set discipline throughout: a terminal mover releases only what
    matches its funding record's names (anything fused beyond that belongs to
    an owner path -- finish, withdrawal, egress -- which serves it, and
    ``consumed`` stays owned for that reason); an unpublished, nonterminal
    mover keeps its recoverable ``transferring`` record for a later
    publication or the terminal scan -- the accepted state machine has no
    transferring-to-reserved step.
    """

    events: list[dict[str, object]] = []
    protected = protection.get("protected")
    assert isinstance(protected, dict)
    seen: set[tuple[str, str]] = set()
    for (key, tier_id), entry in sorted(protected.items()):
        assert isinstance(entry, dict)
        if (key, tier_id) in seen:
            continue
        seen.add((key, tier_id))
        ledger = queue.tier_ledger(str(tier_id))
        grant = str(entry["grant"])
        mover = str(entry["mover"])
        kind = str(entry["kind"])
        try:
            locked = queue._transition_locked(mover, blocking=False)
        except (OSError, pool.PoolContractError, ValueError):
            continue
        try:
            with locked as acquired:
                if not acquired:
                    continue
                record = queue.read_funding(mover, str(tier_id))
                try:
                    held_grant = int(ledger.holder_tokens(grant).get(kind, 0))
                    held_mover = int(ledger.holder_tokens(mover).get(kind, 0))
                except (OSError, pool.PoolContractError, ValueError):
                    continue
                try:
                    terminal = (queue.item_path(pool.DONE, mover).exists()
                                or queue.item_path(pool.FAILED, mover).exists()
                                or queue.item_path(
                                    pool.WITHDRAWN, mover).exists())
                    rowed = (queue.item_path(pool.READY, mover).exists()
                             or queue.item_path(pool.CLAIMED, mover).exists())
                except (OSError, pool.PoolContractError):
                    continue
                bound = (record.get("tokens") if isinstance(record, dict)
                         else None)
                bound_names = (set(str(name) for name in bound)
                               if isinstance(bound, list) and bound else set())
                bound_generation = (str(record.get("generation"))
                                    if isinstance(record, dict)
                                    and isinstance(record.get("generation"),
                                                   str) else None)
                if terminal:
                    # Owner paths serve whatever a terminal mover holds beyond
                    # the fence; the exact unspent fence is decided in
                    # _settle_terminal_fence (unknown evidence defers there).
                    if held_grant > 0:
                        released = window_credit.cancel(ledger, grant)["released"]
                        if released:
                            events.append({"event": "advance-released",
                                           "consumer": key,
                                           "tier_id": str(tier_id),
                                           "leg": str(entry["leg"]),
                                           "reason": "mover-terminal",
                                           "released_gib": released})
                    events.extend(_settle_terminal_fence(
                        queue, ledger, tier_id=str(tier_id),
                        mover_role=str(entry["leg"]), consumer=key,
                        mover=mover))
                    continue
                if not rowed:
                    # No queue row will ever claim this, yet the fence sits
                    # transferred under its key: leave the record
                    # ``transferring`` exactly as is.  That state is itself
                    # the recoverable one -- a later publication claims
                    # against it through the normal cover, and a terminal
                    # mover is reaped by the terminal scan above -- because
                    # the accepted state machine has no transferring-to-
                    # reserved step, and inventing one here would strand a
                    # permanently transferring record beside returned tokens.
                    continue
                if held_grant > 0:
                    moved = queue.transfer_fence(str(tier_id), grant, mover)
                    if moved:
                        queue.advance_funding_state(
                            mover, str(tier_id), expect="reserved",
                            advance_to="transferring",
                            generation=bound_generation)
                        events.append({"event": "advance-handed-off",
                                       "consumer": key, "tier_id": str(tier_id),
                                       "leg": str(entry["leg"]), "mover": mover,
                                       "moved_gib": moved})
                    continue
                # No fence under the grant: either never reserved (nothing to
                # do) or the transfer landed without its mark (a crash between
                # the move and the record write).  The latter is exactly
                # recoverable: the bound names are all under the mover, so
                # complete the marking instead of moving anything twice.
                if (bound_names and record is not None
                        and record.get("state") == "reserved"):
                    try:
                        mover_names = {
                            path.name for path in pool._glob(
                                ledger.held_dir / mover, "*-*")}
                    except (OSError, pool.PoolContractError, ValueError):
                        mover_names = set()
                    if all(name in mover_names for name in bound_names):
                        queue.advance_funding_state(
                            mover, str(tier_id), expect="reserved",
                            advance_to="transferring",
                            generation=bound_generation)
        except (OSError, pool.PoolContractError, ValueError):
            continue
    # Terminal movers with no live protected entry: a withdrawn or failed
    # consumer's stranded fence is visited by nobody above, so its exact
    # unspent tokens would leak until a sweep that spares plan members.
    # Scan the funding records themselves; live and handled movers are
    # skipped, everything else goes through the same terminal discipline.
    handled: set[str] = set()
    for (_key, _tier), _entry in sorted(protected.items()):
        if isinstance(_entry, dict) and isinstance(_entry.get("mover"), str):
            handled.add(str(_entry["mover"]))
    try:
        funding_dir = queue.root / pool.TIER_FUNDING
        funding_files = sorted(funding_dir.glob("*.funding.json"))
    except (OSError, pool.PoolContractError, ValueError):
        funding_files = []
    for funding_path in funding_files:
        stem = funding_path.name
        if not stem.endswith(".funding.json"):
            continue
        stem = stem[: -len(".funding.json")]
        mover_name, dot, scan_tier = stem.rpartition(".")
        if not dot or len(mover_name) != 64 or not scan_tier:
            continue
        if mover_name in handled:
            continue
        try:
            scan_ledger = queue.tier_ledger(scan_tier)
        except (OSError, pool.PoolContractError, ValueError):
            continue
        scan_status, scan_record, scan_reason = queue.read_funding_evidence(
            mover_name, scan_tier)
        if scan_status == "unknown":
            # Unreadable record: nothing may settle against it.  Name it
            # and keep the scan moving; the fence and record both retain.
            events.append({"event": "advance-deferred-unknown-evidence",
                           "consumer": None, "tier_id": str(scan_tier),
                           "leg": "mover_row",
                           "error": scan_reason
                           or "funding record unreadable"})
            continue
        if (scan_status == "absent"
                or scan_record.get("state") in ("consumed", "released")):
            continue
        scan_leg: str | None = None
        for (_key, _tier), _entry in sorted(protected.items()):
            if (str(_tier) == scan_tier and isinstance(_entry, dict)
                    and isinstance(_entry.get("leg"), str)):
                scan_leg = str(_entry["leg"])
                break
        events.extend(_settle_terminal_fence(
            queue, scan_ledger, tier_id=scan_tier,
            mover_role=scan_leg or "mover_row", consumer=None,
            mover=mover_name))
    return events


def residency_window(queue: pool.PoolQueue, *, tiers: Mapping[str, Mapping[str, object]],
                     now: float | None = None,
                     withdrawn: frozenset[str] | None = None) -> list[dict[str, object]]:
    """Publish the next movers, retire the consumed ones, recompose the maps.

    This is the coordinator half of the decomposition contract: the submitter
    froze every child and this publishes them, so no admitted action ever
    publishes work.  It runs here because the tier loop already holds the two
    things the decision needs -- the queue and the tier ledger -- and adding a
    second loop would mean two boxes deciding one stage's occupancy.

    A plan one of whose movers was withdrawn stops being a schedule (#708):
    the withdrawal is marked against the plan's own identity, nothing more is
    published from it -- no mover and no promotion, at any price -- while its
    egress rows still run, because freeing bytes the consumer has read past
    is cleanup rather than staging.  The body stays filed, so the consumer's
    other resident ranges stay named for the sweep and the successor's
    adoption; the dead-consumer pass archives it once its work has ended.

    ``withdrawn`` is the cycle's snapshot of live withdrawal markers, so a
    cancellation filed between the mark pass and this decision cannot leak a
    publish either.
    """

    published: list[dict[str, object]] = []
    cancelled = _withdrawn_keys(queue, withdrawn)

    def horizon_of(consumer, plan, tier_id):
        # One horizon per window for the gate and the publication alike
        # (#903): the gate reserves room for exactly the legs the window
        # would publish.
        return _horizon_end(_stage_horizon(queue, consumer, plan,
                                           tiers.get(tier_id)))

    protection = _protect_tier_advances(
        queue, tiers, mover_role="mover_row",
        tier_of=lambda plan: plan.get("tier_id"),
        state_of=_mover_state, horizon_of=horizon_of)
    gated = protection["gated"]
    assert isinstance(gated, dict)
    grants = protection["grants"]
    assert isinstance(grants, dict)
    permitted = protection.get("permitted")
    assert isinstance(permitted, dict)
    unknown_ready = bool(protection.get("unknown_ready"))
    unknown_tiers = set(protection.get("unknown_tiers") or ())
    unknown_consumers = set(protection.get("unknown_consumers") or ())
    published.extend(protection["events"])  # type: ignore[arg-type]
    try:
        cycle_consumers = live_consumers(queue)
    except (OSError, pool.PoolContractError) as exc:
        published.append({"event": "window-unknown", "consumer": None,
                          "tier_id": None,
                          "reason": f"live census unreadable: {exc!r}"})
        cycle_consumers = []
    for consumer in cycle_consumers:
        key = str(consumer["action_key"])
        refusals: list[Exception] = []
        plan, incarnation = residency_plan.read_filed(
            queue, key, on_unreadable=refusals.append)
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
        superseded = residency_plan.superseded(queue, plan)
        if superseded is None:
            # An operator's withdrawal of one of the plan's movers retires
            # the whole window.  Rows are sealed with the resources their
            # keys hash and the plan is frozen, so there is no supported
            # in-place repair: publication stops here, and a deliberate
            # resubmission can seal a fresh window at the current price once
            # this one's work has ended.  An admission preemption is not an
            # operator's decision -- it requeues its holder immediately -- so
            # it does not retire the plan (``preempted_by`` on the marker).
            hits = [mover for mover in residency_plan.mover_keys(plan)
                    if mover in cancelled]
            if hits:
                operator = [mover for mover in hits
                            if _operator_withdrawal(queue, mover)]
                if operator:
                    superseded = residency_plan.mark_superseded(
                        queue, key, plan=plan, filing=incarnation,
                        reason="mover-withdrawn",
                        movers=operator, by="tier-loop")
                    if superseded is not None:
                        published.append({
                            "event": "residency-plan-superseded",
                            "consumer": key, "tier_id": tier_id,
                            "reason": "mover-withdrawn",
                            "movers": sorted(operator),
                            "phases": len(plan["phases"])})  # type: ignore[arg-type]
        try:
            already, staged = _mover_state(queue, plan, tier_id)
            ledger = queue.tier_ledger(tier_id)
            kind = storage_tiers.capacity_kind_of(tier_id)
            free = ledger.available().get(kind, 0)
        except (OSError, pool.PoolContractError, ValueError) as exc:
            # Fail closed for the whole consumer this cycle -- movers and
            # its window-driven egress alike, since both decide off ledger
            # state this read could not see.  Owner-path cleanup with exact
            # receipts (evict, sweep, dead-consumer pass) runs elsewhere and
            # is unaffected: it proves per range, never per census.
            published.append({"event": "window-unknown", "consumer": key,
                              "tier_id": tier_id,
                              "reason": f"ledger unreadable: {exc!r}"})
            continue
        # A fence never blocks its own window: the grant this consumer's next
        # advance holds is readable back into free for this decision alone.
        # Everyone else -- stealers, later windows, the pressure probe --
        # still sees the fenced ledger.  No grants means no change.
        assert isinstance(grants, dict)
        own_grant = grants.get((key, tier_id))
        if isinstance(own_grant, str):
            try:
                free = int(free) + int(
                    ledger.holder_tokens(own_grant).get(kind, 0))
            except (OSError, pool.PoolContractError, ValueError):
                pass
        # The minted total, not the free remainder: the run-ahead bound of a
        # rolling window is a fraction of the tier, and a bound read off what
        # is free would shrink as the window it is bounding fills it.
        try:
            capacity = ledger.capacity().get(kind, 0)
        except (OSError, pool.PoolContractError, ValueError) as exc:
            published.append({"event": "window-unknown", "consumer": key,
                              "tier_id": tier_id,
                              "reason": f"ledger unreadable: {exc!r}"})
            continue
        # Bounded by the consumer's refill horizon as well as by room and the
        # run-ahead budget (#903): a leg past it publishes on the cycle the
        # consumer's progress brings it inside, and not before.
        decision = residency_plan.window(
            plan, accepted_phase=consumer["accepted_phase"],  # type: ignore[arg-type]
            free_gib=int(free), capacity_gib=int(capacity),
            published=sorted(already), staged=sorted(staged),
            withdrawn=sorted(cancelled),
            horizon_end_bytes=horizon_of(consumer, plan, tier_id))
        stall = decision["stall"]
        if isinstance(stall, Mapping) and superseded is None:
            # Said here rather than nowhere: the incident this bound exists to
            # prevent was invisible for hours because the only thing a stalled
            # window printed was ``tier-cycle``.  Not a claim denial -- the
            # consumer is not denied, it is running and reporting nothing.  A
            # superseded window does not stall: nothing is waiting to publish.
            published.append({"event": "window-stalled", "consumer": key,
                              **{field: stall[field] for field in (
                                  "accepted_phase", "reading_phase",
                                  "blocked_phase", "blocked_gib", "runahead_gib",
                                  "runahead_budget_gib", "free_gib",
                                  "capacity_gib", "reason", "waiting_for")},
                              "chunk_index": stall.get("chunk_index")})
        by_name = {str(entry["name"]): entry for entry in plan["phases"]
                   if isinstance(entry, Mapping)}
        # A superseded plan publishes its egresses -- cleanup the consumer has
        # already paid for -- and nothing else.  A gated newcomer likewise
        # publishes no movers this cycle: its first step would consume the
        # room a live window's advance was promised, so it waits with the
        # reason named while its egresses and map still run.
        gate = gated.get((key, tier_id))
        # Positive publication authority: a current publishes only with its
        # advance retained (permitted: bound, blind-held, landed, or final)
        # and no gate/unknown.  Absence of a gate is never permission.
        unknown_hit = (unknown_ready or tier_id in unknown_tiers
                       or (key, tier_id) in unknown_consumers
                       or (key, "") in unknown_consumers
                       or ("", "") in unknown_consumers)
        have_permit = (key, tier_id) in permitted
        publishable = ([] if (superseded is not None or gate is not None
                              or unknown_hit or not have_permit)
                       else decision["publish"])
        if gate is not None and superseded is None:
            assert isinstance(gate, dict)
            published.append({
                "event": "window-gated", "consumer": key,
                "tier_id": tier_id, "reason": str(gate.get("reason")),
                "permanent": bool(gate.get("permanent")),
                "need_gib": gate.get("need_gib"),
                "output_note": str(gate.get("output_note") or ""),
                **({"commitment": gate["commitment"]}
                   if "commitment" in gate else {}),
            })
        if unknown_hit and superseded is None and gate is None:
            published.append({
                "event": "window-unknown", "consumer": key,
                "tier_id": tier_id, "reason": "unknown-evidence"})
        if (not unknown_hit and gate is None and superseded is None
                and not have_permit and decision["publish"]):
            # Required advance unproved and ungated (should not happen:
            # protection denies every such path) -- fail closed loudly.
            published.append({
                "event": "window-unfunded", "consumer": key,
                "tier_id": tier_id, "reason": "advance-unproved"})
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
        generation = None
        item = consumer.get("item")
        if isinstance(item, Mapping):
            generation = item.get("published_unix")
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
            # The row publishes with the resources its dispatch sealed.  A
            # mover priced above the tier's current offer is not rewritten
            # here (#706): the admission ledger is the authority, and the
            # sealed request and the copy's argv must agree about what was
            # reserved.  Once this row supplies the oldest ready demand, the
            # tier's next cycle raises the fill offer to that unchanged
            # demand; other admission gates still apply, so this is the
            # scoped liveness the probe floor can promise.
            try:
                # A copy has no result to replay: published with recompute,
                # or a republished range is a cache hit that stages nothing.
                # ``refuse_withdrawn`` closes the race the cycle's snapshot
                # cannot: a cancellation filed after the snapshot is seen
                # under publish's own transition lock and outranks this
                # automatic republication (#708 review).
                #
                # The consumer's lock is the parent boundary and it is held
                # across the recheck and the publish, so the captured filing
                # and generation cannot change in between: a child is never
                # published after its parent's plan was reaped or replaced,
                # or after the consumer itself was withdrawn (#708 review).
                with queue._transition_locked(key):
                    owned, why = residency_plan.window_owned(
                        queue, key, filing=incarnation, generation=generation)
                    if not owned:
                        published.append({
                            "event": "mover-publish-deferred-stale-window",
                            "consumer": key, "phase": entry["phase"],
                            "chunk_index": entry.get("chunk_index"),
                            "action_key": entry["mover_action_key"],
                            "reason": why})
                        break
                    queue.publish(**row, recompute=True, refuse_withdrawn=True)
            except pool.WithdrawnActionError as exc:
                marked = residency_plan.mark_superseded(
                    queue, key, plan=plan, filing=incarnation,
                    reason="mover-withdrawn",
                    movers=[str(entry["mover_action_key"])], by="tier-loop")
                published.append({
                    "event": "mover-publish-refused-withdrawn", "consumer": key,
                    "phase": entry["phase"],
                    "chunk_index": entry.get("chunk_index"),
                    "action_key": entry["mover_action_key"],
                    "error": str(exc),
                    "plan_superseded": marked is not None})
                break     # the plan is superseded now: no more movers
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
                # And the same question again under the queue's lock, so the
                # look above and this publication are one decision (#810).
                queue.publish(**row, recompute=True,   # a deletion, likewise
                              refuse_if_live=True)
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
                if record.get("tier") == "ram"},
                running=consumer.get("state") == pool.CLAIMED)
        except (residency_map.ResidencyMapError, OSError) as exc:
            # A map that cannot be composed leaves the previous one in place
            # and the consumer on the pool: slower, never wrong.
            published.append({"event": "map-compose-failed", "consumer": key,
                              "error": repr(exc)})
    published.extend(_settle_protected(queue, protection))
    # Second pass binds what this cycle published: the blind pre-publish
    # take already holds the room, so the bind commits no new capacity and
    # no published row leaves this cycle unfunded.  The pass re-gates from
    # the fresh census (newcomers are members now) and never publishes.
    protection_again = _protect_tier_advances(
        queue, tiers, mover_role="mover_row",
        tier_of=lambda plan: plan.get("tier_id"),
        state_of=_mover_state, horizon_of=horizon_of)
    published.extend(protection_again["events"])  # type: ignore[arg-type]
    published.extend(_settle_protected(queue, protection_again))
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


def evict_beyond_horizon(queue: pool.PoolQueue,
                         tiers: Mapping[str, Mapping[str, object]], *,
                         consumers: list,
                         pressure: Mapping[str, int] | None,
                         withdrawn: frozenset[str] | None = None,
                         ) -> list[dict[str, object]]:
    """Give back landed ranges past their readers' refill horizons (#903, #906).

    Runs after the orphan sweep, on the same ``pressure``: when a stage tier
    or a ram tier is still short of the free a live window needs -- a
    running consumer's in-horizon range, a newcomer's lead, a ready
    consumer's claim -- the ranges no reader needs before a refill could
    land are the room.  They go farthest-needed first (Belady's order across
    every reader on the tier), one at a time, re-reading the ledger after
    each, and stop when the tier has the room.  A tier that could not reach
    the room even after every candidate went evicts nothing (#632: no futile
    eviction).  Ram tiers go first, so the tokens of the smaller tier come
    back before the bytes that feed it leave (#640).

    Each eviction is all or nothing (``stage_release.evict`` with
    ``whole``): a range a reader has pinned, that a promotion is reading,
    or whose ownership this pass cannot prove is declined whole -- nothing
    unlinked, no retiring mark -- and the next candidate goes instead.  A
    range inside any reader's horizon is never a candidate.  An evicted
    range's mover holds no tokens afterwards, so it reads as unpublished and
    its window publishes it again, whole, on the cycle the reader's
    progress brings it back inside the horizon.

    A stage range goes with the ram copies it names in ``ram_first``, ram
    first.  A copy another eviction already took is skipped.  A copy whose
    eviction is declined keeps its stage range too, because deleting the
    stage source under a live ram copy is the state #640 forbids, and the
    next candidate goes instead.
    """

    events: list[dict[str, object]] = []
    if not pressure:
        return events
    cancelled = _withdrawn_keys(queue, withdrawn)
    short: dict[str, int] = {}
    for tier_id, needed in pressure.items():
        record = tiers.get(tier_id)
        if (int(needed) <= 0 or not isinstance(record, Mapping)
                or record.get("tier") not in ("stage", "ram")):
            continue
        try:
            free = int(queue.tier_ledger(tier_id).available().get(
                storage_tiers.capacity_kind_of(tier_id), 0))
        except (OSError, pool.PoolContractError, ValueError):
            continue
        if free < int(needed):
            short[tier_id] = int(needed)
    if not short:
        return events
    candidates = _beyond_horizon_candidates(queue, tiers, consumers, cancelled)

    def evicted(row: Mapping[str, object], tier_id: str, stage_root: str,
                needed: int) -> tuple[bool, dict[str, object]]:
        receipt = stage_release.evict(
            queue, str(row["mover_action_key"]),
            consumer_action_key=str(row["consumer_action_key"]),
            stage_root=stage_root, reason="beyond-horizon", whole=True)
        done = bool(receipt.get("complete"))
        return done, {
            "event": ("beyond-horizon-evicted" if done
                      else "beyond-horizon-eviction-declined"),
            "tier_id": tier_id,
            "consumer": row["consumer_action_key"],
            "mover": row["mover_action_key"],
            "phase": row.get("phase"), "chunk_index": row.get("chunk_index"),
            "stage_gib": row["stage_gib"],
            "seconds_until_needed": (
                round(float(row["seconds_until_needed"]), 1)  # type: ignore[arg-type]
                if row.get("seconds_until_needed") is not None else None),
            "needed_gib": needed,
            "tokens_released": receipt.get("tokens_released"),
            "tokens_decharged": receipt.get("tokens_decharged"),
            "declined": receipt.get("declined") or [],
            "live_pins": receipt.get("live_pins") or [],
            "errors": receipt.get("errors") or []}

    def ram_first(row: Mapping[str, object], needed: int) -> bool:
        """Evict a stage row's ram copies; ``False`` keeps the stage row."""

        for copy in row.get("ram_first") or []:        # type: ignore[union-attr]
            assert isinstance(copy, Mapping)
            copy_tier = str(copy["tier_id"])
            try:
                held = int(queue.tier_ledger(copy_tier).holder_tokens(
                    str(copy["mover_action_key"])).get(
                        storage_tiers.capacity_kind_of(copy_tier), 0))
            except (OSError, pool.PoolContractError, ValueError):
                return False
            if held <= 0:
                continue      # already given back, by the ram tier's own pass
            root = str(copy.get("stage_root") or "")
            refusal = (stage_release.stage_root_refusal(queue, root)
                       if root else "no ram root announced")
            if refusal is not None:
                events.append({"event": "beyond-horizon-eviction-refused",
                               "tier_id": copy_tier, "stage_root": root,
                               "refusal": refusal,
                               "stage_range": row["mover_action_key"]})
                return False
            done, event = evicted({**copy, "consumer_action_key":
                                   row["consumer_action_key"],
                                   "phase": row.get("phase"),
                                   "chunk_index": row.get("chunk_index"),
                                   "seconds_until_needed":
                                   row.get("seconds_until_needed")},
                                  copy_tier, root, needed)
            event["stage_range"] = row["mover_action_key"]
            events.append(event)
            if not done:
                return False
        return True

    for tier_id, needed in sorted(
            short.items(),
            key=lambda item: (tiers[item[0]].get("tier") != "ram", item[0])):
        rows = candidates.get(tier_id, [])
        if not rows:
            continue
        stage_root = str(tiers[tier_id].get("mountpoint") or "")
        refusal = (stage_release.stage_root_refusal(queue, stage_root)
                   if stage_root else "no stage root announced")
        if refusal is not None:
            events.append({"event": "beyond-horizon-eviction-refused",
                           "tier_id": tier_id, "stage_root": stage_root,
                           "refusal": refusal})
            continue
        kind = storage_tiers.capacity_kind_of(tier_id)
        ledger = queue.tier_ledger(tier_id)
        try:
            free = int(ledger.available().get(kind, 0))
        except (OSError, pool.PoolContractError, ValueError):
            continue
        offered = sum(int(row["stage_gib"]) for row in rows)  # type: ignore[arg-type]
        if free + offered < needed:
            events.append({"event": "beyond-horizon-eviction-futile",
                           "tier_id": tier_id, "needed_gib": needed,
                           "free_gib": free, "beyond_horizon_gib": offered})
            continue
        for row in rows:
            if free >= needed:
                break
            try:
                if int(ledger.holder_tokens(str(row["mover_action_key"])).get(
                        kind, 0)) <= 0:
                    continue      # given back since the candidates were read
            except (OSError, pool.PoolContractError, ValueError):
                continue
            if not ram_first(row, needed):
                continue
            _done, event = evicted(row, tier_id, stage_root, needed)
            events.append(event)
            try:
                free = int(ledger.available().get(kind, 0))
            except (OSError, pool.PoolContractError, ValueError):
                break
    return events


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


def mint_stage_supply(queue: pool.PoolQueue, *, tier_id: str, kind: str,
                      writable_tokens: int | None = None,
                      writable_reader=None,
                      cap: int | None = None,
                      extra_tokens: Mapping[str, int] | None = None,
                      ) -> dict[str, object]:
    """Mint one tier's supply as writable-plus-landed, atomically (#733).

    The supply is what the dataset may still hold plus what has *landed*;
    ``landed_and_in_flight`` draws the line (#621/#623).  Exactly one of
    ``writable_tokens`` (a fixed number, for tests) or ``writable_reader``
    (a zero-argument callable re-sampling the dataset's writable room,
    for production) supplies the writable side, with ``writable_tokens``
    doubling as the fallback when the reader fails mid-cycle (the
    discovery number; discovery itself offers nothing on unreadable, so
    the next cycle still converges).  The reader runs INSIDE
    the same tier mint lock as the landed snapshot and the ensure+retire
    apply (via :meth:`PoolQueue.mint_tier_capacity_guarded`): a copy
    landing between discovery and the mint can no longer combine
    discovery's writable with the newcomer's landed into free, because
    the writable number is re-read after every pre-lock completion, and
    completions file under the same lock (see :meth:`PoolQueue.record_move`).
    ``cap`` bounds the supply (the ram policy window); without it the
    supply is exactly ``writable + landed``.  ``extra_tokens`` carries the
    tier's other qualified kinds (fill rates and the like) into the same
    single apply, so no kind is ever retired to zero mid-cycle.  Returns
    ``{"landed", "in_flight", "supply", "ledger"}``; the caller stamps its
    own record fields.
    """

    if writable_reader is None and writable_tokens is None:
        raise ValueError(
            "mint_stage_supply needs writable_tokens xor writable_reader")
    seen: dict[str, int] = {}

    def wanted(ledger) -> dict[str, int]:
        landed, in_flight = landed_and_in_flight(queue, tier_id, kind)
        if writable_reader is not None:
            try:
                writable = int(writable_reader())
            except (OSError, ValueError, TypeError) as exc:
                if writable_tokens is None:
                    raise
                print(json.dumps({
                    "event": "tier-mint-writable-unreadable",
                    "unix": time.time(), "tier_id": tier_id,
                    "fallback_tokens": int(writable_tokens),
                    "reason": repr(exc)}), flush=True)
                writable = int(writable_tokens)
        else:
            writable = int(writable_tokens)  # type: ignore[arg-type]
        supply = writable + landed
        if cap is not None:
            supply = min(supply, int(cap))
        seen["landed"] = landed
        seen["in_flight"] = in_flight
        seen["supply"] = supply
        seen["writable"] = writable
        merged = {str(k): int(v) for k, v in dict(extra_tokens or {}).items()}
        merged[kind] = supply
        return merged

    result = queue.mint_tier_capacity_guarded(tier_id, wanted)
    return {"landed": seen["landed"], "in_flight": seen["in_flight"],
            "supply": seen["supply"], "writable": seen["writable"],
            "ledger": result}


def _supply_reader_for(record: Mapping[str, object], tier_id: str, *,
                       fallback_tokens: int):
    """Re-sample one tier's writable room for the mint critical section.

    The same source and units discovery used: the stage dataset's
    ``available`` for stage tiers, one ``statvfs`` for ram tiers.  A
    bounded metadata read (one subprocess / one syscall), never a copy.
    Returns a zero-argument callable suitable for
    :func:`mint_stage_supply`'s ``writable_reader``; ``None`` when the
    record names no samplable source (the mint then uses the discovery
    number via its fallback path).
    """

    tier = record.get("tier")
    if tier == "stage":
        pool_name = record.get("pool")
        if not isinstance(pool_name, str) or not pool_name:
            return None

        def read_stage(pool_name=pool_name):
            dataset = storage_tiers.stage_dataset(pool_name)
            if not isinstance(dataset, Mapping):
                raise OSError(f"stage dataset unreadable for {pool_name}")
            available = dataset.get("available_bytes")
            if (isinstance(available, bool)
                    or not isinstance(available, int)):
                raise OSError(f"stage available unreadable for {pool_name}")
            return max(0, available) // storage_tiers.GIB

        return read_stage
    if tier == "ram":
        mountpoint = record.get("mountpoint")
        if not isinstance(mountpoint, str) or not mountpoint:
            return None

        def read_ram(mountpoint=mountpoint):
            sampled = os.statvfs(mountpoint)
            return (max(0, int(sampled.f_bavail))
                    * max(0, int(sampled.f_frsize))) // storage_tiers.GIB

        return read_ram
    return None


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
        # The supply mint happens ONCE below, after every admission and
        # policy check, with the landed snapshot in the same critical
        # section (#733).  These sites only stash the qualified writable
        # number (and cap); minting here would publish capacity before the
        # tier is qualified, mint a {kind}-only supply that temporarily
        # retires every other kind to zero, and let a later final mint
        # re-apply a stale pre-egress count after the lock is released.
        supply_writable: int | None = None
        supply_cap: int | None = None
        supply_basis = ""
        if (record.get("tier") == "stage" and kind in tokens
                and record.get("capacity_source") == storage_tiers.WRITABLE_CAPACITY_SOURCE):
            # ``capacity_bytes`` is what ZFS will still let a writer write,
            # net of the bytes already on the dataset.  Minting from
            # ``available`` alone counted every staged GiB twice and starved
            # the window at half the pool (#621); minting ``available + held``
            # counted a claimed mover's unlanded bytes as free and admitted ten
            # windows against one (#623).  The supply is what is writable plus
            # what has *landed*; ``landed_and_in_flight`` draws the line.
            supply_writable = tokens[kind]
            supply_basis = "zfs available + landed"
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
                supply_writable = tokens[kind]
                supply_cap = window
                supply_basis = ("statvfs f_bavail + landed, capped by the "
                                "policy window")
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
        # The probe rule over a standing ceiling (#706).  Minting exactly the
        # ceiling admits nobody priced above it, so the tier never asks for
        # more and the refutation clause cannot fire from admitted work --
        # while each shortfall under it sinks it further (live 2026-09-19: an
        # offer of 171 against six already-ready movers reserving 259, every
        # one at ``never_fits_tier_capacity``).  The receipts fold prices one
        # historical reader's worth above the ceiling (#707), but it cannot
        # see the queue: movers sealed before a sink ask more than that offer.
        # The queued floor is the same rule every other branch already uses --
        # the oldest ready mover's own sealed demand -- and the selected offer
        # is the larger of the two, so already-ready work can claim on this
        # cycle without its sealed request changing.  With no ready demand the
        # historical offer stands; with neither, the offer is exactly the
        # measured ceiling.
        selected: int | None = None
        floor_wins = False
        basis = supply.get("probe_basis")
        historical = supply.get("probe_offer_mb_s")
        priced = (isinstance(historical, (int, float))
                  and not isinstance(historical, bool))
        if priced:
            selected = int(historical)
        if ceiling is not None and int(ceiling) > 0 and probe:
            floor = int(ceiling) + probe
            if selected is None or floor > selected:
                selected = floor
                floor_wins = True
                basis = {"basis": "oldest-ready-sealed-demand",
                         "demand_mb_s": probe}
                if priced:
                    basis["historical_offer_mb_s"] = int(historical)
        if ceiling is not None and int(ceiling) > 0:
            if selected is not None and selected > int(ceiling):
                tokens[storage_tiers.FILL_KIND] = selected
                record["fill_source"] = "measured-probing"
                record["fill_probe_mb_s"] = selected - int(ceiling)
                if floor_wins:
                    # The announced supply names the offer that was actually
                    # selected -- a queued demand, never measured delivery --
                    # so a reader (and the probe_offer metric) cannot mistake
                    # the smaller historical fold for the live offer (#706).
                    # A local name distinct from the cycle's ``announced``
                    # list: shadowing it broke the append below.
                    announced_supply = record["fill_supply"]
                    announced_supply["probing"] = True
                    announced_supply["probe_offer_mb_s"] = selected
                    announced_supply["probe_basis"] = basis
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
        if supply_writable is not None:
            # The ONE authoritative mint for this tier: after every
            # admission and policy check above, with a fresh writable
            # sample, the landed snapshot and the ensure+retire in the same
            # critical section, over the FULL qualified token dict (a
            # {kind}-only mint would retire every other kind to zero until
            # the final call repaired it).  Refuse-and-keep tiers (#631)
            # still mint: the tokens stay so the held reservations the
            # refusal protects keep working.  The reader re-samples what
            # discovery sampled (same source, same units); discovery's
            # number stays as the fallback and still drives the record and
            # admission assembly above.
            reader = _supply_reader_for(record, tier_id,
                                        fallback_tokens=supply_writable)
            minted = mint_stage_supply(
                queue, tier_id=tier_id, kind=kind,
                writable_tokens=supply_writable, writable_reader=reader,
                cap=supply_cap,
                extra_tokens={k: v for k, v in tokens.items()
                              if k != kind})
            record["writable_gib"] = minted.get("writable",
                                                supply_writable)
            record["held_gib"] = minted["landed"] + minted["in_flight"]
            record["landed_gib"] = minted["landed"]
            record["in_flight_gib"] = minted["in_flight"]
            record["capacity_basis"] = supply_basis
            tokens[kind] = minted["supply"]
            record["ledger"] = minted["ledger"]
        else:
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
    planned_unknown: list[dict[str, object]] = []
    planned = _planned_consumers(queue, announced_tiers, unknown=planned_unknown)
    # One snapshot of the live withdrawal markers for every step below, so a
    # cancellation filed mid-cycle cannot have the adoption, the pressure
    # probe and the two windows disagree about it (#708).
    withdrawn = queue.withdrawn_keys()
    # Dead consumers' movers first: a consumer that failed with movers
    # published would otherwise keep staging for nobody all cycle (#620).
    # Withdrawing only stops queued work, so adoption below still sees every
    # resident range it could take.
    for event in withdraw_dead_consumer_movers(queue):
        print(json.dumps({"unix": time.time(), **event}), flush=True)
    for event in adopt_resident_ranges(queue, tiers=announced_tiers,
                                       consumers=planned, withdrawn=withdrawn,
                                       unknown=planned_unknown):
        print(json.dumps({"unix": time.time(), **event}), flush=True)
    pressure = window_pressure(queue, tiers=announced_tiers, consumers=planned,
                               withdrawn=withdrawn, unknown=planned_unknown)
    # Failed movers' partials next: a terminal, unpinned mover that still
    # names bytes is an eviction candidate when the window has no room (#627).
    # Its egress rows land in ``ready/`` before the sweep runs, so the window
    # below sees both the room being made and the recopy it must hold back.
    for event in reclaim_failed_mover_partials(queue, planned, pressure):
        print(json.dumps({"unix": time.time(), **event}), flush=True)
    for event in sweep_orphans(queue, announced_tiers, pressure=pressure):
        print(json.dumps({"event": "stage-orphan-evicted", **event}), flush=True)
    # Orphans first, then the ranges past their readers' refill horizons
    # (#903): an orphan is nobody's, a range past a horizon is somebody's
    # later, so the sweep that returns what nobody will read goes first.
    for event in evict_beyond_horizon(queue, announced_tiers,
                                      consumers=planned, pressure=pressure,
                                      withdrawn=withdrawn):
        print(json.dumps({"unix": time.time(), **event}), flush=True)
    # The ram window before the stage's, so a phase's ram egress is published
    # before its stage egress: the tokens that bound the smaller tier come
    # back first, and a ram range never outlives the stage range that feeds
    # it (#640).
    for event in ram_residency_window(queue, tiers=announced_tiers, now=now,
                                      withdrawn=withdrawn):
        print(json.dumps({"unix": time.time(), **event}), flush=True)
    for event in residency_window(queue, tiers=announced_tiers, now=now,
                                  withdrawn=withdrawn):
        print(json.dumps({"unix": time.time(), **event}), flush=True)
    # Terminal output funding last, once every step above that could still
    # spend a produced batch's fence has run.  Every finish that holds a tier
    # token reads the census, and a record nothing can count again only makes
    # that read longer (#747).
    retired_funding = queue.retire_terminal_output_funding()
    if retired_funding.get("retired") or retired_funding.get("unreadable"):
        print(json.dumps({"unix": time.time(), "event": "output-funding-retired",
                          **retired_funding}), flush=True)
    # Consumed produced-output origins whose declared consumers have all
    # succeeded, and consumed ones a dead producer attempt left undeclared
    # (#914).  Silent when it retires nothing; a stalled or refused
    # retirement is reported once per change.
    for event in produced_output.origin_retirement_tick(queue):
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


def _parser() -> argparse.ArgumentParser:
    """The role's own parser, built before anything takes the singleton.

    Help and argument refusal must not depend on the lock: ``--help`` beside
    a running role has to print usage, and an unparseable invocation is not a
    second minter.
    """

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
    parser.add_argument("--output-windows", action="store_true",
                        help="count each tier's unheld produced-output window "
                             "in the joint-fit gate and the fence check (#747); "
                             f"sets {OUTPUT_WINDOWS_ENV}=1. Off by default")
    return parser


def _serve(args) -> int:
    global CYCLE_INTERVAL_S
    # The cadence this loop decides at is part of every horizon it prices
    # (#903): a range published now is first seen published a cycle later.
    CYCLE_INTERVAL_S = float(args.interval_s)
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


def main(argv: list[str] | None = None) -> int:
    """Mint the tiers, single-instance on this box.

    Every valid invocation takes the role's host-local singleton lock around
    the whole cycle, one-shot included: a ``--once`` mints and announces
    against the real queue just as the service does, so exempting it would be
    the bypass the guard exists to close (#709).  Two minters against one
    tier are outside what the per-tier mint lock was analysed for, and on
    2026-09-19 a duplicate supervisor's copy raced the primary's.  The lock
    is taken by the process that serves, so the launcher cannot matter, and
    the winner holds until the block's final close (never an unlink or an
    explicit unlock).

    Safety never depends on naming the holder: the holder pid is a
    /proc/locks diagnostic the ``RoleLockHeld`` message may carry, read only
    when the flock is refused, and an unreadable holder is still a refusal.
    """

    args = _parser().parse_args(argv)
    if args.interval_s <= 0:
        raise SystemExit("--interval-s must be positive")
    if args.output_windows:
        os.environ[OUTPUT_WINDOWS_ENV] = "1"
    try:
        output_windows_enabled()
    except ValueError as exc:
        raise SystemExit(str(exc)) from None
    try:
        with runtime_gate.role_singleton(Path(__file__)):
            return _serve(args)
    except runtime_gate.RoleLockHeld as held:
        print(f"tier_loop: refusing a second tiers role; "
              f"{runtime_gate.role_lock_path(Path(__file__))} is held by "
              + (f"pid {held.holder}" if held.holder is not None
                 else "an unreadable holder"),
              file=sys.stderr, flush=True)
        return runtime_gate.ROLE_SINGLETON_HELD_EXIT
    except runtime_gate.RoleLockUnavailable as exc:
        print(f"tier_loop: refusing to serve without the tiers role "
              f"singleton lock: {exc}", file=sys.stderr, flush=True)
        return runtime_gate.ROLE_SINGLETON_HELD_EXIT


if __name__ == "__main__":
    raise SystemExit(main())
