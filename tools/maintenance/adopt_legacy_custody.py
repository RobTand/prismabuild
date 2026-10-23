#!/usr/bin/env python3
"""Inventory and explicitly adopt pre-contract holds while legacy writers stop.

This never returns tokens. Normal lifecycle cleanup and egress remain the only
release owners. Do not prepare a new inventory after enabling the contract.
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
    parser.add_argument("--queue-root", required=True, type=Path)
    choice = parser.add_mutually_exclusive_group(required=True)
    choice.add_argument("--inventory", type=Path, help="write a new pre-deploy inventory, refusing overwrite")
    choice.add_argument("--apply", type=Path, help="apply the reviewed pre-deploy inventory")
    parser.add_argument("--writers-stopped", action="store_true", help="operator confirms legacy writers and new admissions are stopped")
    parser.add_argument("--report", type=Path, help="write adoption outcomes, refusing overwrite")
    args = parser.parse_args()
    migration = LegacyCustodyMigration(pool.PoolQueue(args.queue_root))
    if not args.writers_stopped:
        parser.error("--writers-stopped is required; see the operating guide deploy step")
    if args.inventory:
        value = migration.inventory()
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
