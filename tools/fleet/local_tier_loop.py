#!/usr/bin/env python3
"""One supervised local-tier capacity/lifecycle pass per owning host."""
import argparse
import json
from pathlib import Path
import signal
import socket
import sys
import threading

sys.path.insert(0, str(Path(__file__).resolve().parent))
from runtime_paths import generation_root
sys.path.insert(0, str(generation_root(__file__) / "src"))
from prismabuild import local_tier, pool, resident_sets
import worker_loop as runtime_gate


def local_tier_cycle(queue, host, policy):
    spec = policy["hosts"].get(host)
    if spec is None:
        return queue.mint_tier_capacity(local_tier.local_resident_tier_id(host), {local_tier.KIND: 0})
    from prismabuild import local_resident
    lease_results = local_resident.lease_pass(resident_sets.ResidentSets(queue.root), host, spec)
    # Remaining trees and retained holders are still counted by the minter.
    result = local_tier.mint_local_tier_capacity(queue, host, spec)
    result["lease_pass"] = lease_results
    queue.announce_tier({"tier_id": local_tier.local_resident_tier_id(host), "tier": "local",
        "host": host, "mountpoint": spec["root"], "capacity_bytes": result["wanted_gib"] * local_tier.GIB,
        "mover_python": sys.executable, "mover_tools_root": str(Path(__file__).resolve().parent)})
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pool-root", required=True, help="PrismaBuild queue root (the pool directory) that holds the resident set records.")
    parser.add_argument("--policy", default=str(Path(__file__).with_name("local_tier_policy.json")),
                        help="local_tier_policy.json; a host missing from its host map mints nothing and runs no lease pass (the shipped map is empty).")
    parser.add_argument("--interval-s", type=float, default=5, help="Seconds between passes of lease expiry, eviction and capacity minting.")
    parser.add_argument("--once", action="store_true", help="Run one pass for this host and exit instead of looping.")
    args = parser.parse_args(argv)
    if args.interval_s <= 0:
        parser.error("interval must be positive")
    stop = threading.Event()
    handlers = {sig: signal.signal(sig, lambda *_: stop.set()) for sig in (signal.SIGTERM, signal.SIGINT)}
    loaded = runtime_gate.loaded_runtime_commit()
    try:
        with runtime_gate.role_singleton(Path(__file__)):
            while not stop.is_set():
                published = runtime_gate.published_commit()
                if loaded and published and loaded != published:
                    return 75 if args.once else 0
                gate = runtime_gate.read_maintenance_gate()
                if gate is not None:
                    runtime_gate.post_park_marker(gate)
                    if args.once:
                        return 75
                else:
                    try:
                        queue = pool.PoolQueue(args.pool_root)
                        queue.ensure_layout()
                        result = local_tier_cycle(queue, socket.gethostname(), resident_sets.read_policy(args.policy))
                    except Exception as error:
                        print(json.dumps({"event": "localtier-cycle-error",
                            "host": socket.gethostname(), "error": f"{type(error).__name__}: {error}"}), flush=True)
                        if args.once:
                            return 1
                    else:
                        print(json.dumps(result), flush=True)
                        if args.once:
                            return 1 if any(row.get("error") for row in result.get("lease_pass", [])) else 0
                stop.wait(args.interval_s)
    except (runtime_gate.RoleLockHeld, runtime_gate.RoleLockUnavailable) as exc:
        print(f"localtier: {exc}", file=sys.stderr)
        return runtime_gate.ROLE_SINGLETON_HELD_EXIT
    finally:
        for sig, handler in handlers.items():
            signal.signal(sig, handler)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
