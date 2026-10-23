#!/usr/bin/env python3
"""Interleave fresh and cached fixture seals inside an ordinary CPU PB action."""
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


def _bundle_fixture(directory: Path, files: int):
    checkout = directory / "checkout"
    checkout.mkdir()
    subprocess.run(["git", "init", "-q", str(checkout)], check=True, capture_output=True)
    body = "\n".join(f"line {index}: deterministic shared fixture content " * 2 for index in range(64))
    for index in range(files):
        (checkout / f"file-{index:05d}").write_text(f"{body}\nfile marker {index}\n")
    (checkout / ".pbrun-closure.measure").write_text("PB #2171 fixed closure\n")
    subprocess.run(["git", "-C", str(checkout), "add", "."], check=True, capture_output=True)
    subprocess.run(["git", "-C", str(checkout), "-c", "user.name=PrismaBuild",
                    "-c", "user.email=prismabuild@example.invalid", "commit", "-qm", "fixture"],
                   check=True, capture_output=True)
    tip = subprocess.check_output(["git", "-C", str(checkout), "rev-parse", "HEAD"], text=True).strip()
    return checkout, [("refs/heads/prismabuild-snapshot", tip)]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repeats", type=int, default=8)
    parser.add_argument("--files", type=int, default=2000)
    args = parser.parse_args()
    if args.repeats < 2 or args.files < 1:
        parser.error("require at least two pairs and one fixture file")
    native = subprocess.run
    packs = 0

    def counted(command, **kwargs):
        nonlocal packs
        if "pack-objects" in command:
            packs += 1
        return native(command, **kwargs)

    subprocess.run = counted
    arms = {arm: {"samples": [], "pack_executions": 0} for arm in ("before", "after")}
    with tempfile.TemporaryDirectory(prefix="measure-bundle-cache-") as temporary:
        root = Path(temporary)
        checkout, refs = _bundle_fixture(root, args.files)
        cache = root / "cache"

        def seal(arm):
            os.environ["PRISMABUILD_BUNDLE_CACHE_ROOT"] = "off" if arm == "before" else str(cache)
            output = root / f"{arm}.bundle"
            submission.write_deterministic_bundle(checkout, output, refs,
                git_dir=checkout / ".git", environment=dict(os.environ))
            return output

        expected = core.stream_sha256(seal("after"))
        if packs != 1:
            raise RuntimeError("cache warmup did not execute one fresh pack")
        for pair in range(args.repeats):
            # Reverse each pair to balance load drift without needing an idle host.
            for arm in (("before", "after") if pair % 2 == 0 else ("after", "before")):
                initial_packs = packs
                cpu_start, start = _sealing_cpu_seconds(), time.monotonic()
                output = seal(arm)
                elapsed = time.monotonic() - start
                cpu = _sealing_cpu_seconds() - cpu_start
                if core.stream_sha256(output) != expected:
                    raise RuntimeError("fresh and cached bundle digests differ")
                sample = {"pair": pair + 1, "cpu_seconds": cpu, "wall_seconds": elapsed}
                arms[arm]["samples"].append(sample)
                arms[arm]["pack_executions"] += packs - initial_packs
                print(json.dumps({"arm": arm, **sample}), flush=True)
    for arm in arms.values():
        arm["cpu_seconds"] = sum(sample["cpu_seconds"] for sample in arm["samples"])
    if arms["before"]["pack_executions"] != args.repeats or arms["after"]["pack_executions"] != 0:
        raise RuntimeError("comparison did not exercise N fresh seals against N cache hits")
    saving = arms["before"]["cpu_seconds"] - arms["after"]["cpu_seconds"]
    result = {"repeats": args.repeats, "fixture_files": args.files, "arms": arms,
              "cpu_seconds_saved": saving, "cpu_percent_saved": 100 * saving / arms["before"]["cpu_seconds"],
              "warmup_pack_executions": 1, "bundle_sha256": expected}
    print(json.dumps(result), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
