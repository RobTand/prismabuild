#!/bin/bash
# Full-suite gate for the gen10 commit cfe060ca72 (main 625f148f95 minus PR 1640). Author pb-integrator, 2026-10-09.
set -uo pipefail
G=cfe060ca720547273d2fdba125cdbffb1aaa8223
cd /home/rob/wt/pb-gen10-prep && [ "$(git rev-parse HEAD)" = "$G" ] || { echo "worktree not at $G: stop"; exit 1; }
P=/home/rob/tmp/pb-submit-celestia-20261003/bin/python; R=/mnt/shared/prismabuild-fleet/repo/tools; O=/home/rob/tmp/pbfix
W=/home/rob/wt/pb-gate-gen10; git worktree remove --force $W 2>/dev/null; git worktree prune; git worktree add -q --detach $W $G || exit 1
echo "gate tree $(git -C $W rev-parse HEAD^{tree} | cut -c1-10)"
$P $R/pbtest.py --checkout $W --python /home/rob/venvs/pb-cpu/bin/python --tag x86 --shards 20 --json $O/gate-gen10-cand.json tests > $O/gate-gen10-cand.log 2>&1
echo "gate rc=$?"
