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
import socket
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
from runtime_paths import generation_root  # noqa: E402

sys.path.insert(0, str(generation_root(__file__) / "src"))

from prismabuild import pool  # noqa: E402
from prismabuild import storage_tiers  # noqa: E402

MOVER_RECEIPTS = "movers"


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
        record["ledger"] = queue.mint_tier_capacity(tier_id, tokens)
        queue.announce_tier(record)
        announced.append(record)
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
    parser.add_argument("--pool-root", required=True,
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
                                                    "fill_mb_s", "fill_source", "tokens")}
                          for r in records],
            }), flush=True)
        if args.once:
            print(json.dumps(records, indent=1, default=str))
            return 0
        time.sleep(max(0.0, args.interval_s - (time.monotonic() - started)))


if __name__ == "__main__":
    raise SystemExit(main())
