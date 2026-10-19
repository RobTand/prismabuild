"""Replay the exact C27 owner/caller patch, or verify an idempotent rerun.

The patch includes embedded programs and assertion locator migrations.
Historical compatibility sources are outside the migration.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess

BASE = "8e8572ee324b8508b9a7dd6a6dcb7d3548b663a7"
OWNERS = {"pbstatus.read_pool": "census.action_rows.value",
          "pbstatus.read_starvation": "census.starvation",
          "pbstatus.read_blocked_origins": "census.blocked_origins.value",
          "pbstatus.read_endings": "census.endings",
          "pbmetrics.KeptReads": "census.KeptReads",
          "pbmetrics._promotion_metrics derivation": "census.promotion",
          "pbmcp._cursor_census": "census.plans.value",
          "pbmcp._tier_rows": "census.tiers.value",
          "pbmcp._mover_live_rows": "census.movers.value",
          "pbmcp._filed_move_receipts": "census.movers(view=filed).value",
          "pbstatus.read_rollout_summary": "upgrade_client.read_rollout_summary"}


def replay_census_patch(root, patch, expected_tree, patch_blob):
    def git(*args, data=None):
        return subprocess.check_output(["git", "-C", str(root), *args], input=data).decode().strip()

    raw = patch.read_bytes()
    if git("hash-object", "--stdin", data=raw) != patch_blob:
        raise ValueError("census patch differs from its pinned Git blob")
    before = git("write-tree")
    if before == expected_tree:
        return {"status": "already applied", "tree": before}
    if git("rev-parse", "HEAD") != BASE or git("status", "--porcelain"):
        raise ValueError("census replay requires the clean exact base or the completed index tree")
    subprocess.run(["git", "-C", str(root), "apply", "--index", "--binary", str(patch)], check=True)
    actual = git("write-tree")
    if actual != expected_tree:
        raise ValueError(f"replay tree {actual} differs from {expected_tree}")
    return {"status": "applied", "base": BASE, "tree": actual, "patch_blob": patch_blob,
            "paths": git("diff", "--cached", "--name-only").splitlines(), "owners": OWNERS}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--patch", type=Path, required=True)
    parser.add_argument("--patch-blob", required=True)
    parser.add_argument("--tree", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = replay_census_patch(args.root, args.patch, args.tree, args.patch_blob)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report))


if __name__ == "__main__":
    main()
