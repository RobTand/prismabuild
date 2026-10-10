#!/bin/bash
# Gate for PR 1619 reduced head 76b49dde3a: candidate arm only (base = the 1619 run on the same main ba540d991b), then the shard-11 repeats. Author pb-integrator, 2026-10-08.
set -uo pipefail
cd /home/rob/wt/pb-1642-merged
git fetch -q origin main; git fetch -q origin pull/1619/head:prx1619 -f
H=76b49dde3a8d4ee5f5d403d55d2587f2273e59c9; M=$(git rev-parse origin/main)
[ "$(git rev-parse prx1619)" = "$H" ] || { echo "HEAD CHANGED: stop"; exit 1; }
git merge-base --is-ancestor "$M" "$H" || { echo "MAIN MOVED ($M): head is behind; stop"; exit 1; }
[ "${M:0:10}" = ba540d991b ] || { echo "main is ${M:0:10}, not ba540d991b: the base arm is stale; stop"; exit 1; }
echo "main ${M:0:10}; head ${H:0:10}; tree $(git rev-parse $H^{tree} | cut -c1-10)"
P=/home/rob/tmp/pb-submit-celestia-20261003/bin/python; R=/mnt/shared/prismabuild-fleet/repo/tools; O=/home/rob/tmp/pbfix
W=/home/rob/wt/pb-gate1619b-cand; git worktree remove --force $W 2>/dev/null; git worktree prune; git worktree add -q --detach $W $H || exit 1
$P $R/pbtest.py --checkout $W --python /home/rob/venvs/pb-cpu/bin/python --tag x86 --shards 20 --json $O/gate1619b-cand.json tests > $O/gate1619b-cand.log 2>&1
echo "full cand rc=$?"
mapfile -t FILES < $O/shard11-files.txt
for i in 1 2 3; do
  $P $R/pbtest.py --checkout $W --python /home/rob/venvs/pb-cpu/bin/python --tag x86 --shards 1 --timeout-s $((1820+i)) --json $O/shard11b-cand-$i.json "${FILES[@]}" > $O/shard11b-cand-$i.log 2>&1
  echo "shard11 round $i rc=$? $(grep -E ' passed| failed' $O/shard11b-cand-$i.log | tail -1 | cut -c1-100)"; grep -h -E '^FAILED' $O/shard11b-cand-$i.log | cut -c1-170
done
