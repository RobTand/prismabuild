#!/usr/bin/env python3
"""Profile the tier-release census a finish pays, against a real funding dir.

A diagnostic, not a gate. ``PoolQueue.finish`` concludes every claim through
``_release_reservation``, which asks the prepaid-output census which tier
tokens it must keep. This tool measures that step in-process (``cProfile``
plus wall time) on a throwaway queue whose ``tier-funding`` directory is a
read-only view of a live queue's, so the census reads the real record
population over the real filesystem without the live queue being touched.

Nothing is written under ``--queue``. Every mutating ``os`` call whose target
resolves under it raises before it runs, and the tool reads nothing else from
there except a read-only prune dry-run (``--prune-dry-run``) that classifies
each output funding record by the facts a terminal prune would require.

Scenarios, each repeated ``--repeat`` times:

* ``census-nonowner``: ``output_census_for_owner`` for a key owning nothing.
* ``release-nonholder``: ``_release_reservation`` for a key holding no tier
  token, the case of almost every finishing action.
* ``release-holder``: the same for a key holding one token on the first tier.
* ``local-copy``: the census over a local-disk copy of every record, then
  over the copy with the dry-run's terminal records moved aside.
"""
from __future__ import annotations

import argparse
import cProfile
import io
import json
import os
from pathlib import Path
import pstats
import shutil
import sys
import tempfile
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from prismabuild import pool  # noqa: E402

SUFFIX = ".output-funding.json"


def guard_writes(root: Path) -> None:
    """Refuse every mutating ``os`` call that would land under ``root``."""

    real = os.path.realpath(root)

    def inside(path) -> bool:
        try:
            target = os.path.realpath(os.fspath(path))
        except TypeError:
            return False
        return target == real or target.startswith(real + os.sep)

    def wrap(name, positions):
        original = getattr(os, name)

        def checked(*args, **kwargs):
            if any(inside(args[index]) for index in positions if index < len(args)):
                raise PermissionError(f"read-only profile refused os.{name}{args}")
            return original(*args, **kwargs)
        setattr(os, name, checked)

    # A symlink's target is only named, never written; its link path is.
    for name, positions in (("rename", (0, 1)), ("replace", (0, 1)), ("unlink", (0,)),
                            ("remove", (0,)), ("rmdir", (0,)), ("mkdir", (0,)),
                            ("link", (1,)), ("symlink", (1,))):
        wrap(name, positions)
    opener = os.open

    def open_checked(path, flags, *args, **kwargs):
        if flags & (os.O_WRONLY | os.O_RDWR | os.O_CREAT) and inside(path):
            raise PermissionError(f"read-only profile refused a write open of {path}")
        return opener(path, flags, *args, **kwargs)
    os.open = open_checked


def profiled(label, repeat, fn):
    walls = []
    profile = cProfile.Profile()
    for _ in range(repeat):
        started = time.perf_counter()
        profile.enable()
        fn()
        profile.disable()
        walls.append(time.perf_counter() - started)
    buffer = io.StringIO()
    stats = pstats.Stats(profile, stream=buffer).sort_stats("cumulative")
    stats.print_stats(12)
    calls = {}
    watched = ("_read_json", "output_census_for_owner", "validate_output_funding",
               "output_keep_names_for_owner", "_release_reservation", "scandir", "io.open")
    for (_filename, _line, name), row in stats.stats.items():
        for token in watched:
            if token in name:
                calls[token] = calls.get(token, 0) + row[1]
    walls.sort()
    return {"label": label, "repeat": repeat, "wall_s_min": walls[0],
            "wall_s_median": walls[len(walls) // 2], "wall_s_max": walls[-1],
            "calls_per_run": {name: count / repeat for name, count in sorted(calls.items())},
            "top_cumulative": buffer.getvalue().splitlines()[:40]}


def prune_dry_run(live: pool.PoolQueue, funding: Path) -> dict:
    """Classify each record by the facts a terminal prune requires (read-only)."""

    counts = {"records": 0, "terminal": 0, "live": 0, "unreadable": 0}
    reasons: dict[str, int] = {}
    terminal = []
    for entry in sorted(os.listdir(funding)):
        if not entry.endswith(SUFFIX):
            continue
        counts["records"] += 1
        stem = entry[: -len(SUFFIX)]
        mover, _, tier = stem.partition(".")
        record, state = live.output_funding_file_state(mover, tier)
        if state != "ok":
            counts["unreadable"] += 1
            continue
        why = None
        if record.get("state") not in ("consumed", "released"):
            why = f"state-{record.get('state')}"
        elif any(live.item_path(s, mover).exists() for s in (pool.READY, pool.CLAIMED)):
            why = "mover-queued-or-claimed"
        elif not any(live.item_path(s, mover).exists()
                     for s in (pool.DONE, pool.FAILED, pool.WITHDRAWN)):
            why = "mover-not-terminal"
        elif live.lease_path(mover).exists():
            why = "mover-lease"
        elif live.tier_ledger(tier).holder_tokens(mover):
            why = "mover-holds-tier-tokens"
        if why is None:
            counts["terminal"] += 1
            terminal.append(entry)
        else:
            counts["live"] += 1
            reasons[why] = reasons.get(why, 0) + 1
    return {**counts, "live_reasons": reasons, "terminal_names": terminal}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--queue", required=True, help="live queue root, read only")
    parser.add_argument("--scratch", required=True, help="throwaway directory")
    parser.add_argument("--out", help="also write the report here")
    parser.add_argument("--repeat", type=int, default=5,
                        help="runs per scenario; the report gives min, median and max")
    parser.add_argument("--prune-dry-run", action="store_true",
                        help="also classify each live record by the terminal-prune facts "
                             "and time the census on a local copy with and without them")
    args = parser.parse_args(argv)
    live_root = Path(args.queue)
    funding = live_root / pool.TIER_FUNDING
    tiers = sorted(p.name for p in (live_root / pool.TIER_RESERVATIONS).iterdir()
                   if p.is_dir() and not p.name.startswith("."))
    guard_writes(live_root)
    os.makedirs(args.scratch, exist_ok=True)
    scratch = Path(tempfile.mkdtemp(prefix="census-", dir=args.scratch))
    report = {"queue": str(live_root), "tiers": tiers, "host": os.uname().nodename,
              "records": sum(1 for n in os.listdir(funding) if n.endswith(SUFFIX)),
              "scenarios": []}
    try:
        view = pool.PoolQueue(scratch / "view")
        view.root.mkdir()
        (view.root / pool.TIER_FUNDING).symlink_to(funding, target_is_directory=True)
        for tier in tiers:
            view.mint_tier_capacity(tier, {"probe": 4})
        nobody = "0" * 63 + "1"
        holder = "0" * 63 + "2"
        report["scenarios"].append(profiled(
            "census-nonowner", args.repeat, lambda: view.output_census_for_owner(nobody)))
        report["scenarios"].append(profiled(
            "release-nonholder", args.repeat,
            lambda: view._release_reservation(nobody, host=None)))

        def holder_cycle():
            assert view.tier_ledger(tiers[0]).acquire(holder, {"probe": 1})
            view._release_reservation(holder, host=None)
            assert not view.tier_ledger(tiers[0]).holder_tokens(holder)
        report["scenarios"].append(profiled("release-holder", args.repeat, holder_cycle))
        if args.prune_dry_run:
            live = pool.PoolQueue(live_root)
            dry = prune_dry_run(live, funding)
            report["prune_dry_run"] = {k: v for k, v in dry.items() if k != "terminal_names"}
            copy = pool.PoolQueue(scratch / "copy")
            (copy.root / pool.TIER_FUNDING).mkdir(parents=True)
            for name in os.listdir(funding):
                if name.endswith(SUFFIX):
                    shutil.copy2(funding / name, copy.root / pool.TIER_FUNDING / name)
            report["scenarios"].append(profiled(
                "local-copy-all-records", args.repeat,
                lambda: copy.output_census_for_owner(nobody)))
            aside = copy.root / "aside"
            aside.mkdir()
            for name in dry["terminal_names"]:
                os.rename(copy.root / pool.TIER_FUNDING / name, aside / name)
            report["scenarios"].append(profiled(
                "local-copy-live-records-only", args.repeat,
                lambda: copy.output_census_for_owner(nobody)))
    finally:
        shutil.rmtree(scratch, ignore_errors=True)
    if args.out:
        Path(args.out).write_text(json.dumps(report, indent=1, sort_keys=True) + "\n")
    for scenario in report["scenarios"]:
        print(f"{scenario['label']:32s} median {scenario['wall_s_median']:.4f}s "
              f"calls {scenario['calls_per_run']}")
    if "prune_dry_run" in report:
        print("prune dry run", report["prune_dry_run"])
    print("REPORT " + json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
