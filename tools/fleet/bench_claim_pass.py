#!/usr/bin/env python3
"""Directory listings and per-name lookups of one worker poll (#993).

A record of one measurement, not an operator command: it builds its own
queue under ``--work`` and never touches the live queue or a stage root.

Builds a local queue shaped like the live one on 2026-09-23 (40 ready items
this box cannot place, 28 claimed records, 1,324 passes/ sidecars), then runs
two polls the way ``worker_loop`` does: discovery (``ready_items``, through
``pool.ready_placement`` where the tree has it) and one claim pass over that
snapshot.  Each poll is bracketed by two sentinel ``stat`` calls, so an
``strace`` of this process can count exactly that poll's syscalls.  The
first poll is cold; the second is the steady one.

Run under ``strace -f -e trace=%file,getdents64 -o TRACE``; then
``bench_claim_pass.py --count TRACE --work WORK`` prints per-directory counts:
directory opens (each is one listing: on NFS, one or more READDIR) and
path lookups of per-key names (on NFS, each is a LOOKUP unless the dentry is
cached and still valid).
"""

from __future__ import annotations

import argparse
import collections
import json
import os
from pathlib import Path
import re
import sys


def build_and_poll(checkout: Path, work: Path, ready: int, claimed: int,
                   passes: int) -> None:
    sys.path.insert(0, str(checkout / "src"))
    from prismabuild import pool

    queue = pool.PoolQueue(work / "pb-queue")
    queue.ensure_layout()
    for index in range(ready):
        key = f"{index + 1:064x}"
        queue.publish(action_key=key, cas_root=queue.root / "cas",
                      checkout_root=queue.root / "co",
                      worker_script=queue.root / "worker.py",
                      resources={"cpu": 1}, tags=["elsewhere"])
    claimed_dir = queue.dir(pool.CLAIMED)
    for index in range(claimed):
        key = f"{0xc0000 + index:064x}"
        (claimed_dir / f"{key}.json").write_text(json.dumps(
            {"action_key": key, "claimed_by": "another-box",
             "claimed_unix": 1.0}))
    passes_dir = queue.root / pool.PASSES
    passes_dir.mkdir(parents=True, exist_ok=True)
    for index in range(passes):
        key = f"{index + 1:064x}" if index < ready else f"{0xa0000 + index:064x}"
        (passes_dir / f"{key}.json").write_text(json.dumps(
            {"action_key": key, "passes": 3}))
    placement = getattr(pool, "ready_placement", None)
    for poll in (1, 2):
        # The first poll of a process is cold (no in-process memo); the second
        # is what a running worker loop pays on every poll.
        os.path.exists(work / f"POLL{poll}-BEGIN")
        if placement is None:
            snapshot = queue.ready_items()
        else:
            with placement(("here",), False):
                snapshot = queue.ready_items()
        result = queue.claim(tags=["here"], owner="worker", ready=snapshot)
        os.path.exists(work / f"POLL{poll}-END")
        print(json.dumps({"poll": poll,
                          "ready_placement": placement is not None,
                          "snapshot": len(snapshot), "claimed": result}))


_PATH = re.compile(r'"([^"]*)"')


def count(trace: Path, work: Path, poll: int) -> None:
    root = str(work / "pb-queue")
    inside = False
    opens: collections.Counter[str] = collections.Counter()
    lookups: collections.Counter[str] = collections.Counter()
    getdents = 0
    dir_fds: dict[tuple[str, str], str] = {}
    getdents_by: collections.Counter[str] = collections.Counter()
    key_name = re.compile(r"^\.?[0-9a-f]{64}(\.[^/]*)?$")
    for line in trace.read_text().splitlines():
        if f"POLL{poll}-BEGIN" in line:
            inside = True
            continue
        if f"POLL{poll}-END" in line:
            break
        if not inside:
            continue
        pid = line.split(None, 1)[0]
        if "getdents64(" in line:
            fd = line.split("getdents64(", 1)[1].split(",", 1)[0]
            directory = dir_fds.get((pid, fd), "?")
            if directory.startswith(root):
                getdents += 1
                getdents_by[os.path.relpath(directory, root)] += 1
            continue
        paths = [p for p in _PATH.findall(line) if p.startswith(root)]
        if not paths:
            continue
        if "O_DIRECTORY" in line and "openat(" in line:
            match = re.search(r"\)\s*=\s*(\d+)", line)
            if match:
                dir_fds[(pid, match.group(1))] = paths[0]
            opens[os.path.relpath(paths[0], root)] += 1
            continue
        for path in paths:
            name = os.path.basename(path)
            if key_name.match(name):
                lookups[os.path.relpath(os.path.dirname(path), root)] += 1
    print(json.dumps({"poll": poll, "directory_listings": dict(opens),
                      "listings_total": sum(opens.values()),
                      "getdents64": dict(getdents_by),
                      "per_key_path_lookups": dict(lookups),
                      "lookups_total": sum(lookups.values())}, indent=1))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--work", required=True, type=Path,
                        help="scratch directory the synthetic queue is built "
                             "in; must not exist yet; with --count, the "
                             "directory the traced run used")
    parser.add_argument("--checkout", type=Path,
                        default=Path(__file__).resolve().parents[2],
                        help="the tree whose src/prismabuild is measured")
    parser.add_argument("--count", type=Path,
                        help="an strace -f log of a run; prints its counts")
    parser.add_argument("--ready", type=int, default=40,
                        help="ready items this box cannot place (the live "
                             "queue held 40 on 2026-09-23)")
    parser.add_argument("--claimed", type=int, default=28,
                        help="claimed records (the live queue held 28)")
    parser.add_argument("--passes", type=int, default=1324,
                        help="passes/ sidecars (the live queue held 1,324)")
    args = parser.parse_args()
    if args.count is not None:
        for poll in (1, 2):
            count(args.count, args.work, poll)
        return 0
    args.work.mkdir(parents=True, exist_ok=False)
    build_and_poll(args.checkout, args.work, args.ready, args.claimed,
                   args.passes)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
