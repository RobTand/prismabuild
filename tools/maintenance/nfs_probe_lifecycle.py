#!/usr/bin/env python3
"""Run one admitted private-prefix NFS lifecycle arm, or inspect server states.

Run ``run`` through published pbrun. ``server`` only reads nfsd procfs and
requires the operator's read access; it does not change tracing or the mount.
"""

from __future__ import annotations

import argparse
import dataclasses
import importlib.util
import json
import os
from pathlib import Path
import socket
import sys
import time


ARMS = ("current", "unique", "no-rename", "overwrite", "replace", "empty",
        "probe-direct", "probe-fork")


def run_probe_lifecycle(arm: str, prefix: str, cycles: int) -> dict:
    path = Path(__file__).resolve().parents[1] / "fleet" / "mount_latency.py"
    spec = importlib.util.spec_from_file_location("mount_latency", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    root = module.DEFAULT_MOUNT / module.PROBE_DIRNAME / prefix
    root.mkdir()  # Refuse reuse: every arm must start in a fresh namespace.
    source = root / f"claim-{os.getpid()}.tmp"
    target = source.with_suffix(".done")
    if arm == "replace":
        target.write_text("probe\n", encoding="utf-8")
    before = module.read_mountstats(root)
    statuses = {}
    if arm.startswith("probe-"):
        sampler = module.MountSampler(module.DEFAULT_MOUNT, host=prefix,
                                      repeats=5)
        for index in range(cycles // 5):
            sample = (sampler._run_probe() if arm == "probe-fork" else
                      module.timed_probe(root, repeats=5, setup=index == 0))
            status = sample["status"]
            statuses[status] = statuses.get(status, 0) + 1
            if status != "ok":
                raise RuntimeError(sample)
    else:
        for index in range(cycles):
            if arm == "unique":
                source = root / f"claim-{os.getpid()}-{index}.tmp"
                target = source.with_suffix(".done")
            with source.open("w", encoding="utf-8") as handle:
                if arm != "empty":
                    handle.write("probe\n")
            if arm not in ("no-rename", "overwrite"):
                os.rename(source, target)
            if arm not in ("overwrite", "replace"):
                os.unlink(source if arm == "no-rename" else target)
    after = module.read_mountstats(root)
    return {
        "arm": arm, "cycles": cycles, "prefix": prefix,
        "host": socket.gethostname(), "kernel": os.uname().release,
        "pid": os.getpid(), "unix": time.time(),
        "sample_statuses": statuses,
        "mountstats_before": dataclasses.asdict(before) if before else None,
        "mountstats_after": dataclasses.asdict(after) if after else None,
        "files_after": sorted(p.name for p in root.iterdir()),
    }


def read_probe_server_states(prefix: str) -> dict:
    clients = {}
    for client in sorted(Path("/proc/fs/nfsd/clients").iterdir()):
        with (client / "states").open() as stream:
            raw = stream.read(8 * 1024 * 1024 + 1)
        if len(raw) > 8 * 1024 * 1024:
            raise ValueError(f"state read cap exceeded: {client}")
        opens = [row for row in raw.splitlines() if "type: open," in row]
        clients[client.name] = {
            "info": (client / "info").read_text(),
            "open_count": len(opens),
            "matching_states": [row for row in opens
                                if f'filename: "{prefix}/' in row],
        }
    return {"unix": time.time(), "host": socket.gethostname(),
            "kernel": os.uname().release, "prefix": prefix,
            "clients": clients}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    subs = parser.add_subparsers(dest="command", required=True)
    probe = subs.add_parser("run")
    probe.add_argument("arm", choices=ARMS)
    probe.add_argument("--cycles", type=int, default=1000)
    probe.add_argument("--prefix", required=True)
    observe = subs.add_parser("server")
    observe.add_argument("--prefix", required=True)
    args = parser.parse_args()
    if not args.prefix or Path(args.prefix).name != args.prefix:
        parser.error("prefix must be one nonempty directory name")
    if args.command == "run" and args.cycles < 1:
        parser.error("cycles must be positive")
    if args.command == "run" and args.arm.startswith("probe-") and args.cycles % 5:
        parser.error("full probe arms require a multiple of five cycles")
    result = (run_probe_lifecycle(args.arm, args.prefix, args.cycles)
              if args.command == "run" else read_probe_server_states(args.prefix))
    print(json.dumps(result), flush=True)


if __name__ == "__main__":
    main()
