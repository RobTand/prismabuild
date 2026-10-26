#!/usr/bin/env python3
"""Explicit bounded-step backfill of advisory publication candidates."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from prismabuild.record_store import RecordStore


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--queue-root", type=Path, required=True)
    parser.add_argument("--entries-per-step", type=int, default=256)
    parser.add_argument("--max-steps", type=int, default=32)
    args = parser.parse_args(argv)
    if args.entries_per_step < 1 or args.max_steps < 1:
        parser.error("step and entry bounds must be positive")
    with RecordStore(args.queue_root).begin_publication_reconciliation() as run:
        for _ in range(args.max_steps):
            progress = run.step(max_entries=args.entries_per_step)
            print(json.dumps(progress), flush=True)
            if progress["finished"]:
                return 0 if progress["complete"] else 1
    return 75


if __name__ == "__main__":
    raise SystemExit(main())
