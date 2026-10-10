#!/bin/bash
# Full-suite gate for the gen11 commit 281006ef63 (main c3c98642ad minus PR 1640), run with the published pbtest (a box-local candidate pbtest is refused by pbrun). Author pb-integrator, 2026-10-09.
set -uo pipefail
G=281006ef6374e4c058b84d0c306c2460a21771dd
cd /home/rob/wt/pb-gen11-prep && [ "$(git rev-parse HEAD)" = "$G" ] || { echo "worktree not at $G: stop"; exit 1; }
P=/home/rob/tmp/pb-submit-celestia-20261003/bin/python; R=/mnt/shared/prismabuild-fleet/repo/tools; O=/home/rob/tmp/pbfix
W=/home/rob/wt/pb-gate-gen11; git worktree remove --force $W 2>/dev/null; git worktree prune; git worktree add -q --detach $W $G || exit 1
echo "gate tree $(git -C $W rev-parse HEAD^{tree} | cut -c1-10)"
$P $R/pbtest.py --checkout $W --python /home/rob/venvs/pb-cpu/bin/python --tag x86 --shards 20 --json $O/gate-gen11-cand.json tests > $O/gate-gen11-cand.log 2>&1
echo "gate rc=$?"
