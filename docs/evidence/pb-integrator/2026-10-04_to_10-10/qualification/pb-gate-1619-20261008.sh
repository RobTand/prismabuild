#!/bin/bash
# Fresh full-suite gate for PR 1619 under strict branch protection. Author pb-integrator, 2026-10-08.
set -uo pipefail
cd /home/rob/wt/pb-1642-merged
git fetch -q origin
M=$(git rev-parse origin/main); H=7bb7c8ffc15544f9743393ebf128c3af33819cdb
git fetch -q origin pull/1619/head:prx1619 -f
[ "$(git rev-parse prx1619)" = "$H" ] || { echo "HEAD CHANGED: stop"; exit 1; }
git merge-base --is-ancestor "$M" "$H" || { echo "MAIN MOVED past the PR head ($M): the head is behind; stop"; exit 1; }
echo "main ${M:0:10}; head ${H:0:10}; tree $(git rev-parse $H^{tree} | cut -c1-10)"
P=/home/rob/tmp/pb-submit-celestia-20261003/bin/python; R=/mnt/shared/prismabuild-fleet/repo/tools; O=/home/rob/tmp/pbfix
for arm in base cand; do
  W=/home/rob/wt/pb-gate1619-$arm; git worktree remove --force $W 2>/dev/null; git worktree prune
  if [ $arm = base ]; then git worktree add -q --detach $W $M; else git worktree add -q --detach $W $H; fi || exit 1
  echo "== $arm $(git -C $W rev-parse --short=10 HEAD) tree $(git -C $W rev-parse HEAD^{tree} | cut -c1-10)"
  $P $R/pbtest.py --checkout $W --python /home/rob/venvs/pb-cpu/bin/python --tag x86 --shards 20 --json $O/gate1619-$arm.json tests > $O/gate1619-$arm.log 2>&1
  echo "$arm pbtest rc=$?"
done
