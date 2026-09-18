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
pool-side attribution (``disk_pacing.mean_self_read_mb_s``, #580).  Until
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
            out.append({"action_key": key, "state": state, "accepted_phase": accepted})
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
        plan = residency_plan.read(queue, key)
        if plan is None:
            continue
        tier_id = str(plan["tier_id"])
        if tier_id not in tiers:
            # Another box's stage.  Its own tier loop owns that ledger and will
            # publish this window; two loops minting one tier's occupancy is
            # the thing the tier id exists to prevent.
            continue
        already, staged = _mover_state(queue, plan, tier_id)
        free = queue.tier_ledger(tier_id).available().get(
            storage_tiers.capacity_kind_of(tier_id), 0)
        decision = residency_plan.window(
            plan, accepted_phase=consumer["accepted_phase"],  # type: ignore[arg-type]
            free_gib=int(free), published=sorted(already), staged=sorted(staged))
        for phase in decision["publish"]:
            row = dict(phase["mover_row"])                 # type: ignore[arg-type]
            try:
                queue.publish(**row)
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
                queue.publish(**row)
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
                  tiers: Mapping[str, Mapping[str, object]]) -> list[dict[str, object]]:
    """Take back the stage from movers no live consumer still plans to read.

    A consumer withdrawn between its movers finishing and its own claim would
    otherwise hold its ranges for the life of the fleet, because nothing
    publishes an egress for work nobody is waiting on.
    """

    stage_roots = {
        tier_id: str(record.get("mountpoint") or "")
        for tier_id, record in tiers.items()
        if record.get("tier") == "stage" and record.get("mountpoint")
    }
    if not stage_roots:
        return []
    return stage_release.sweep(queue, stage_roots=stage_roots)


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
        record["fill_source"] = "measured" if storage_tiers.FILL_KIND in tokens else "none"
        record["fill_records"] = len(fill_records)
        if storage_tiers.FILL_KIND not in tokens:
            if ready is None:
                ready = queue.ready_items()
            probe = probe_fill_demand(ready, tier_id)
            if probe is not None:
                tokens[storage_tiers.FILL_KIND] = probe
                record["fill_source"] = "probe"
                record["fill_probe_mb_s"] = probe
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
        record["ledger"] = queue.mint_tier_capacity(tier_id, tokens)
        queue.announce_tier(record)
        announced.append(record)
    # Minting first, windowing second, on purpose: the window publishes what
    # the tier's *current* free capacity covers, so it must see this cycle's
    # supply rather than the last one's.
    announced_tiers = {str(record["tier_id"]): record for record in announced}
    for event in sweep_orphans(queue, announced_tiers):
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
    while True:
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
                                                    storage_tiers.FILL_RECORD_FIELD, "fill_source", "tokens")}
                          for r in records],
            }), flush=True)
        if args.once:
            print(json.dumps(records, indent=1, default=str))
            return 0
        time.sleep(max(0.0, args.interval_s - (time.monotonic() - started)))


if __name__ == "__main__":
    raise SystemExit(main())
