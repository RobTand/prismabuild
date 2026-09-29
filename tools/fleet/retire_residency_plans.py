#!/usr/bin/env python3
"""Retire residency plans whose consumer has concluded (#1041).

``PoolQueue.finish`` retires a plan at its consumer's terminal transition
going forward.  This tool clears the plans filed before that change, and the
ones whose children outlived their consumer.  It is a dry run unless you pass
``--apply``.

Each candidate goes through ``residency_plan.reap``, which takes the
consumer's transition lock and archives the plan under
``residency-plans/superseded/`` only when the consumer is neither ready nor
claimed and no child mover or egress row is still ready or claimed.  A plan
that fails that check is reported and left filed.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from prismabuild import pool, residency_plan  # noqa: E402


def candidates(queue: pool.PoolQueue) -> list[str]:
    """Consumer keys with a filed plan and no ready or claimed consumer row."""

    keys = []
    for path in sorted(pool._scan(queue.root / pool.RESIDENCY_PLANS)):
        key = path.stem
        if path.suffix != ".json" or not path.is_file():
            continue
        if queue.item_path(pool.READY, key).exists():
            continue
        if queue.item_path(pool.CLAIMED, key).exists():
            continue
        keys.append(key)
    return keys


def run(queue: pool.PoolQueue, *, apply: bool) -> dict[str, list[str]]:
    result: dict[str, list[str]] = {"retired": [], "kept": [], "would_retire": []}
    for key in candidates(queue):
        if not apply:
            result["would_retire"].append(key)
            continue
        moved = residency_plan.reap(queue, key, reason="consumer-concluded-backfill")
        result["retired" if moved is not None else "kept"].append(key)
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--queue", required=True, help="pool queue root")
    parser.add_argument("--apply", action="store_true",
                        help="archive the plans (default: dry run)")
    args = parser.parse_args(argv)
    result = run(pool.PoolQueue(Path(args.queue)), apply=args.apply)
    print(json.dumps({"apply": args.apply,
                      **{k: len(v) for k, v in result.items()},
                      "keys": result}, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
