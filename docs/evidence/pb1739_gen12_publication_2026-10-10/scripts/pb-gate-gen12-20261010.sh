#!/bin/bash
# Full-suite gate for the gen12 commit 34c2228cb2 (main 561e3177be minus PR 1640), run with the published pbtest (a box-local candidate pbtest is refused by pbrun). Author pb-integrator, 2026-10-09.
set -uo pipefail
G=34c2228cb25e47e48d26781efc9fd6f1e378dd53
cd /home/rob/wt/pb-gen12-prep && [ "$(git rev-parse HEAD)" = "$G" ] || { echo "worktree not at $G: stop"; exit 1; }
P=/usr/bin/python3; R=/mnt/shared/prismabuild-fleet/repo/tools; O=/mnt/shared/fleet-ceo/pb-integrator-gates
W=/home/rob/wt/pb-gate-gen12; git worktree remove --force $W 2>/dev/null; git worktree prune; git worktree add -q --detach $W $G || exit 1
echo "gate tree $(git -C $W rev-parse HEAD^{tree} | cut -c1-10)"
$P $R/pbtest.py --checkout $W --python /home/rob/venvs/pb-cpu/bin/python --tag x86 --shards 20 --json $O/gate-gen12-cand.json tests > $O/gate-gen12-cand.log 2>&1
echo "gate rc=$?"
