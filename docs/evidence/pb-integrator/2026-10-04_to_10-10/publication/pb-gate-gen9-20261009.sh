#!/bin/bash
# Full-suite gate for the generation commit c8daa1be41 (main 6e5aaee1f4 minus PR 1640). Base = gate1619b-cand (same tree as main). Author pb-integrator, 2026-10-09.
set -uo pipefail
G=c8daa1be416cd4d7fe8a8e55c49924ab33c4de39
cd /home/rob/wt/pb-gen9-prep && [ "$(git rev-parse HEAD)" = "$G" ] || { echo "worktree not at $G: stop"; exit 1; }
P=/home/rob/tmp/pb-submit-celestia-20261003/bin/python; R=/mnt/shared/prismabuild-fleet/repo/tools; O=/home/rob/tmp/pbfix
W=/home/rob/wt/pb-gate-gen9; git worktree remove --force $W 2>/dev/null; git worktree prune; git worktree add -q --detach $W $G || exit 1
echo "gate tree $(git -C $W rev-parse HEAD^{tree} | cut -c1-10)"
$P $R/pbtest.py --checkout $W --python /home/rob/venvs/pb-cpu/bin/python --tag x86 --shards 20 --json $O/gate-gen9-cand.json tests > $O/gate-gen9-cand.log 2>&1
echo "gate rc=$?"
