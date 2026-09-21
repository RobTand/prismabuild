#!/usr/bin/env python3
"""Measure how late this box sees a queue record that another box files (#808).

Run it inside an admitted action on an NFS client of the queue (a Spark):

    pbrun.py --cwd CHECKOUT --tag sparky --cpus 1 --demand mem_gb=2 -- \\
        python3 tools/fleet/qualify_record_visibility.py --reader fresh \\
        --client-root /mnt/shared/SCRATCH --server-root /storage_pool/shared/SCRATCH

It polls for a mover receipt under a scratch queue root on the shared mount,
four times a second, the way a produced-output owner polls for its mover.
Three seconds in, the file server writes the receipt on its local pool over
``ssh``.  The printed delay is the receipt's own timestamp to the first poll
that returned it.  A root that looks like a queue, or sits inside one, is
refused, so the live queue cannot be the scratch directory.

``--reader plain`` is the by-name read the pool used before #808 and
``--reader fresh`` is ``PoolQueue.move_record``; the two arms are the before
and after of that change on the real mount, which no unit test can reach.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import threading
import time
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))


def _plain_move_record(pool, queue, key):
    """``PoolQueue.move_record`` as it read before #808: no revalidation."""

    record = pool._read_json(queue.move_path(key))
    if not isinstance(record, dict) or record.get("schema") != pool.POOL_MOVE_SCHEMA_V1:
        return None
    return record


def _refuse_a_queue_root(root: Path, queue_states: tuple[str, ...]) -> None:
    """The scratch root must not be a queue, or a directory inside one."""

    for candidate in (root, *root.parents):
        held = [name for name in queue_states if (candidate / name).is_dir()]
        if held:
            raise SystemExit(
                f"refused: {candidate} holds {', '.join(held)} and looks like "
                "a queue root; give this probe a scratch directory")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--reader", choices=("plain", "fresh"), required=True,
        help="which read to measure: plain is the pre-#808 by-name read, "
             "fresh is PoolQueue.move_record")
    parser.add_argument(
        "--runs", type=int, default=3,
        help="how many receipts to file and poll for, one result line each")
    parser.add_argument(
        "--client-root", required=True,
        help="scratch directory on the shared mount, as this box sees it")
    parser.add_argument(
        "--server-root", required=True,
        help="the same directory as the file server sees it on its local pool")
    parser.add_argument(
        "--server", default="dl380g10",
        help="ssh host that writes each receipt on its local pool")
    parser.add_argument(
        "--budget-s", type=float, default=90.0,
        help="how long each run polls for its receipt before reporting none")
    args = parser.parse_args()

    from prismabuild import pool

    _refuse_a_queue_root(
        Path(args.client_root).resolve(),
        (pool.READY, pool.CLAIMED, pool.DONE, pool.MOVERS))
    results = []
    for index in range(args.runs):
        name = f"queue-{uuid.uuid4().hex[:12]}"
        queue = pool.PoolQueue(Path(args.client_root) / name)
        key = uuid.uuid4().hex * 2
        receipt = queue.move_path(key)
        receipt.parent.mkdir(parents=True, exist_ok=True)
        server_path = str(Path(args.server_root) / name / pool.MOVERS / receipt.name)
        # Written whole and renamed into place, as the pool files a receipt:
        # a poll must never land on half a record.
        writer = (
            "import json,os,time; t=%r+'.tmp'; open(t,'w').write(json.dumps("
            "{'schema': %r, 'action_key': %r, 'complete': True, "
            "'unix': time.time()})); os.replace(t, %r)"
            % (server_path, pool.POOL_MOVE_SCHEMA_V1, key, server_path))

        def file_it() -> None:
            time.sleep(3.0)
            subprocess.run(
                ["ssh", "-o", "BatchMode=yes", args.server,
                 "python3 -c " + shlex.quote(writer)],
                check=True, timeout=60)

        thread = threading.Thread(target=file_it, daemon=True)
        thread.start()
        polls, started, seen = 0, time.time(), None
        while time.time() - started < args.budget_s:
            polls += 1
            record = (queue.move_record(key) if args.reader == "fresh"
                      else _plain_move_record(pool, queue, key))
            if record is not None:
                seen = time.time() - float(record["unix"])
                break
            time.sleep(0.25)
        thread.join(timeout=70)
        results.append({"run": index, "polls": polls,
                        "seen_after_s": None if seen is None else round(seen, 2)})
        print(json.dumps(results[-1]), flush=True)
    print(json.dumps({
        "schema": "prismabuild.qualify_record_visibility.v1",
        "reader": args.reader, "host": os.uname().nodename,
        "pool_module": pool.__file__, "results": results}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
