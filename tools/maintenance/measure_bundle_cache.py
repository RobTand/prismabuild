#!/usr/bin/env python3
"""Compare repeated snapshot sealing CPU in one admitted, CPU-only PB action."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import resource
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
from prismabuild import core, submission  # noqa: E402


def _sealing_cpu_seconds():
    own = resource.getrusage(resource.RUSAGE_SELF)
    children = resource.getrusage(resource.RUSAGE_CHILDREN)
    return own.ru_utime + own.ru_stime + children.ru_utime + children.ru_stime


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--checkout", type=Path, default=Path.cwd())
    parser.add_argument("--repeats", type=int, default=6)
    args = parser.parse_args()
    if args.repeats < 2:
        parser.error("--repeats must include a cold seal and at least one repeat")
    native = subprocess.run
    packs = 0

    def counted(command, **kwargs):
        nonlocal packs
        if "pack-objects" in command:
            packs += 1
        return native(command, **kwargs)

    subprocess.run = counted
    arms = {}
    expected = None
    with tempfile.TemporaryDirectory(prefix="measure-bundle-cache-") as temporary:
        root = Path(temporary)
        cas = core.PrismaBuildCAS(root / "cas")
        for arm in ("before", "after"):
            os.environ["PRISMABUILD_BUNDLE_CACHE_ROOT"] = (
                "off" if arm == "before" else str(root / "cache"))
            samples = []
            initial_packs = packs
            for index in range(args.repeats):
                cpu_start, start = _sealing_cpu_seconds(), time.monotonic()
                snapshot = submission.snapshot_checkout(args.checkout.resolve(), cas=cas,
                    stamp_name=".pbrun-closure.measure", stamp_payload="PB #2171 fixed submission\n")
                elapsed = time.monotonic() - start
                cpu = _sealing_cpu_seconds() - cpu_start
                if expected is None:
                    expected = snapshot
                if snapshot != expected:
                    raise RuntimeError("cached and fresh snapshot identities differ")
                samples.append({"cpu_seconds": cpu, "wall_seconds": elapsed})
                print(json.dumps({"arm": arm, "submission": index + 1, **samples[-1]}), flush=True)
            arms[arm] = {"samples": samples, "pack_executions": packs - initial_packs,
                         "cpu_seconds": sum(sample["cpu_seconds"] for sample in samples)}
    if arms["before"]["pack_executions"] != args.repeats or arms["after"]["pack_executions"] != 1:
        raise RuntimeError("measurement did not exercise cold fill followed by verified hits")
    saving = arms["before"]["cpu_seconds"] - arms["after"]["cpu_seconds"]
    result = {"repeats": args.repeats, "arms": arms, "cpu_seconds_saved": saving,
              "cpu_percent_saved": 100 * saving / arms["before"]["cpu_seconds"],
              "bundle_sha256": expected["input"]["sha256"]}
    print(json.dumps(result), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
