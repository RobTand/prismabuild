#!/usr/bin/env python3
"""Two-host register -> refresh -> admit proof of the #1483 floor (private registry).

Run as two PrismaBuild CPU actions against one private queue directory on the
shared store, never the live queue:

* ``owner`` on the file server (dl380g10): registers the directory's ZFS pool
  with a ``filesystem_gib`` growth ledger and refreshes it every few seconds,
  as the owner's loop would, until the client's report appears (or
  ``--hold-s`` runs out).
* ``client`` on an NFS client (a Spark): with the floor enforced in-process,
  checks the NFS view (attributed by server address), holds a real growth
  allowance through ``filesystem_floor.operation`` against the owner's
  published sample, proves the boundary refusal, and writes its report.

Each prints one JSON report and exits 0 only when every step passed.  The
private directory is left for inspection; remove it afterwards.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import socket
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from prismabuild import filesystem_floor as ff  # noqa: E402
from prismabuild import pool  # noqa: E402

LEDGER = "proof-growth"
REPORT = "client-report.json"


def owner(root: Path, hold_s: float) -> dict:
    queue = root / "pb-queue"
    pool.PoolQueue(queue).ensure_layout()
    registered = ff.register(queue, root / "shared", filesystem_gib=4,
                             filesystem_ledger=LEDGER)
    report = {"role": "owner", "host": socket.gethostname(),
              "binding": registered["binding"], "first_refresh": registered["refresh"],
              "refreshes": 0}
    deadline = time.monotonic() + hold_s
    while time.monotonic() < deadline and not (root / REPORT).exists():
        result = ff.refresh_binding(queue, registered["binding"])
        report["refreshes"] += 1
        report["last_refresh"] = result
        time.sleep(5)
    report["client_report_seen"] = (root / REPORT).exists()
    report["ok"] = (registered["binding"]["fstype"] == "zfs"
                    and registered["refresh"]["allowed"] is True
                    and report.get("last_refresh", registered["refresh"])["allowed"] is True)
    return report


def client(root: Path, wait_s: float) -> dict:
    os.environ[ff.MODE_ENV] = "enforce"
    queue = root / "pb-queue"
    shared = root / "shared"
    report: dict = {"role": "client", "host": socket.gethostname(), "steps": {}}
    deadline = time.monotonic() + wait_s
    while not ff.bindings(queue) and time.monotonic() < deadline:
        time.sleep(5)
    view = ff.identify(shared)
    report["view"] = view
    verdicts = ff.check_paths(queue, [shared])
    report["steps"]["nfs_attributed"] = {
        "ok": view["fstype"] in ff.NFS_TYPES and all(v["allowed"] for v in verdicts)
        and any(v.get("sample") == "fresh-nfs-client" for v in verdicts),
        "verdicts": verdicts}
    ledger = pool.ResourceLedger(queue / ff.FILESYSTEM_RESERVATIONS, host=LEDGER)
    with ff.operation(queue, used_paths=[shared], growth_gib={shared: 2},
                      label="proof") as held:
        during = ledger.held().get(ff.FILESYSTEM_KIND, 0)
    report["steps"]["growth_admitted_and_released"] = {
        "ok": during == 2 and ledger.held().get(ff.FILESYSTEM_KIND, 0) == 0,
        "held_during": during, "verdicts": held}
    binding = ff.bindings(queue)[0]
    published = ff.published_verdict(queue, binding)
    headroom = (published["free_bytes"] - published["floor_bytes"]
                - published["charge_bytes"]) if "required_bytes" in published else None
    report["published"] = published
    refused = None
    if headroom is not None:
        # Leave exactly 1 GiB of headroom through the grant counter -- what
        # 1 GiB of outstanding grants since the sample looks like -- and ask
        # for 2.  The owner's next refresh resets it.
        directory = ff.fs_dir(queue, binding["key"])
        with ff.floor_locked(directory) as acquired:
            if acquired:
                ff._floor_write(directory / "granted.json", {
                    "granted_bytes": ff._granted(directory) + headroom - ff.GIB})
        try:
            with ff.operation(queue, growth_gib={shared: 2}, label="proof-boundary"):
                refused = False
        except ff.FloorRefused as exc:
            refused = exc.verdicts
    report["steps"]["boundary_refused"] = {
        "ok": bool(refused) and ledger.held().get(ff.FILESYSTEM_KIND, 0) == 0,
        "verdicts": refused}
    report["ok"] = all(step["ok"] for step in report["steps"].values())
    ff._floor_write(root / REPORT, report)
    return report


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("role", choices=("owner", "client"),
                    help="owner: run on the file server, register and keep "
                         "refreshing; client: run on an NFS client, check and admit")
    ap.add_argument("--root", required=True, type=Path,
                    help="a private directory on the shared store, the same for "
                         "both roles; never the live queue")
    ap.add_argument("--hold-s", type=float, default=1800.0,
                    help="owner: refresh until the client reports or this many "
                         "seconds pass; client: wait this long for the owner")
    args = ap.parse_args(argv)
    args.root.mkdir(parents=True, exist_ok=True)
    report = (owner(args.root, args.hold_s) if args.role == "owner"
              else client(args.root, args.hold_s))
    print(json.dumps(report, indent=1, default=str))
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
