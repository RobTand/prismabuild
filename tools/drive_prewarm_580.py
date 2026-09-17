#!/usr/bin/env python3
"""Run the #580 branch's prewarm cycle against the live queue, for a bounded window.

Calls ``prewarm_loop.cycle`` directly with the storage role's own arguments,
bypassing ``main()``'s generation gate (a checkout is not a published
generation).  The published daemon must be SIGSTOPped for the window so the
two do not warm the same window twice; the supervisor still sees it and does
not respawn.  One JSON cycle record per line on stdout, hold events on stderr.
"""
import argparse, json, sys, threading, time
sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent / "fleet"))
import prewarm_loop as pl
from prismabuild import pool

ap = argparse.ArgumentParser()
ap.add_argument("--minutes", type=float, default=25.0)
ap.add_argument("--max-readers", type=int, default=16)
ap.add_argument("--poll-s", type=float, default=20.0)
ap.add_argument("--log", required=True)
ap.add_argument("--served", action="append", default=[],
                help="HOST=ADDR,ADDR: stand in for the offer's addresses field, which "
                     "the published worker runtime does not announce yet; the values "
                     "are what `ip -4 -o addr show scope global` reports on that box")
opts = ap.parse_args()

overrides = {}
for spec in opts.served:
    host, _, addrs = spec.partition("=")
    overrides[host] = tuple(a for a in addrs.split(",") if a)
if overrides:
    real = pl.served_addresses
    def served_addresses(queue, host):
        if host in overrides:
            return {"served_host": host, "served_addresses": overrides[host],
                    "served_reason": "attributed (driver stand-in for the offer field)",
                    "offer_age_s": None}
        return real(queue, host)
    pl.served_addresses = served_addresses

args = argparse.Namespace(
    pool_root=str(pl.SH / "pb-queue"), cas_root=str(pl.SH / "cas"),
    mount_map=["/mnt/shared=/storage_pool/shared"],
    readers=1, max_readers=opts.max_readers, lookahead=1,
    pace_pool="storage_pool", disks="",
    client_active_mb_s=pl.CLIENT_ACTIVE_MB_S, nfsd_io=pl.NFSD_IO,
    export_stats=pl.EXPORT_STATS,
    max_util_pct=25.0, max_read_await_ms=10.0, max_backlog_ms=2000.0,
    pace_sample_s=0.25, pace_hold_s=0.25, min_manifest_bytes=1 << 30,
    poll_s=opts.poll_s, arc_reserve_fraction=0.8, arcstats=pl.ARCSTATS,
    claim_grace_min=20.0, once=False, dry_run=False, log=opts.log,
)
mounts = pl.MountMap(args.mount_map)
assert mounts.usable()
queue = pool.PoolQueue(pl.SH / "pb-queue")
ledger = pl.HoldLedger()
stop = threading.Event()
deadline = time.time() + opts.minutes * 60

def announce(payload):
    print(json.dumps({**payload, "unix": round(time.time(), 3)}), file=sys.stderr, flush=True)

def stopper():
    while time.time() < deadline:
        time.sleep(1.0)
    stop.set()
threading.Thread(target=stopper, daemon=True).start()

while not stop.is_set():
    pacer = pl.pacer_from_args(args)
    pacer.ledger = ledger
    pl.require_storage_pacing(pacer)
    pacer.notify = announce
    event = pl.cycle(args, queue, mounts, stop, pacer=pacer)
    line = json.dumps(event)
    print(line, flush=True)
    with open(args.log, "a") as h:
        h.write(line + "\n")
    if stop.is_set():
        break
    time.sleep(args.poll_s)
print(json.dumps({"event": "driver-done", "held_seconds_total": ledger.held_s,
                  "holds_total": ledger.holds, "unix": time.time()}), flush=True)
