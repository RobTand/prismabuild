#!/usr/bin/env python3
"""Inventory and explicitly adopt pre-contract holds while legacy writers stop.

Apply establishes custody without returning tokens. Explicit terminal reclaim
uses the ordinary certificate and ledger return. Keep writers stopped throughout.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
from prismabuild import pool  # noqa: E402
from prismabuild.legacy_custody import LegacyCustodyMigration  # noqa: E402


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--queue-root", type=Path)
    choice = parser.add_mutually_exclusive_group(required=True)
    choice.add_argument("--inventory", type=Path, help="write a new pre-deploy inventory, refusing overwrite")
    choice.add_argument("--apply", type=Path, help="apply the reviewed pre-deploy inventory")
    choice.add_argument("--reclaim-terminal", metavar="KEY", help="explicitly return certified-absent terminal holds through the ordinary certifier")
    choice.add_argument("--observe-host", action="store_true", help="verify local stopped writers and scan the roots requested on stdin")
    parser.add_argument("--local-host", action="store_true", help="inventory only this host's ledgers; foreign custody must be handled on its owner host")
    parser.add_argument("--unpin", action="store_true", help="reclaim terminal tier occupancy only after its adopted absence proof")
    parser.add_argument("--roots", type=Path, help="reviewed complete historical e9 root coverage, including separate occupancy roots")
    parser.add_argument("--writers-stopped", action="store_true", help="operator confirms legacy writers and new admissions are stopped")
    parser.add_argument("--report", type=Path, help="write adoption outcomes, refusing overwrite")
    args = parser.parse_args()
    if args.observe_host:
        from prismabuild.legacy_custody_host import observe_legacy_custody_host
        print(json.dumps(observe_legacy_custody_host(json.load(sys.stdin))))
        return 0
    if args.queue_root is None:
        parser.error("--queue-root is required for inventory or apply")
    migration = LegacyCustodyMigration(pool.PoolQueue(args.queue_root), roots=(
        None if args.roots is None else json.loads(args.roots.read_bytes())))
    if not args.writers_stopped:
        parser.error("--writers-stopped is required; see the operating guide deploy step")
    if args.reclaim_terminal:
        from prismabuild.legacy_custody_host import legacy_stopped_host
        legacy_stopped_host()
        result = migration.queue.reclaim_terminal_reservation(args.reclaim_terminal, unpin=args.unpin)
        print(f"Returned {result['released']} certified tokens for {args.reclaim_terminal}.")
        return 0
    if args.inventory:
        value = migration.inventory()
        if args.local_host:
            import socket
            from prismabuild import fleet_roster
            roster = fleet_roster.Roster.at(Path(__file__).resolve().parents[2])
            local = roster.canonical_box(socket.gethostname())
            if local is None:
                parser.error("local host is not in the fleet roster")
            value["holds"] = [row for row in value["holds"] if (
                (box := roster.canonical_box(row["ledger"] if row["kind"] == "host"
                                            else row["ledger"].rsplit(":", 1)[-1])) is not None
                and box.key == local.key)]
        with args.inventory.open("x") as f:
            json.dump(value, f, indent=2)
            f.write("\n")
        print(f"Inventoried {len(value['holds'])} legacy holds; no tokens released.")
        return 0
    value = migration.apply(json.loads(args.apply.read_bytes()), writers_stopped=True)
    if args.report:
        with args.report.open("x") as f:
            json.dump(value, f, indent=2)
            f.write("\n")
    retained = sum(row["status"] == "retained" for row in value)
    print(f"Adopted {len(value)-retained}; retained {retained}; no tokens released.")
    return 2 if retained else 0


if __name__ == "__main__":
    raise SystemExit(main())
