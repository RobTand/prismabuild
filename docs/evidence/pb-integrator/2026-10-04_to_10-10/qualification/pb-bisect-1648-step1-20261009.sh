#!/bin/bash
# Issue 1648, step 1: does the pbgang entry still fail the race test with fewer files in the shard? Author pb-integrator, 2026-10-09.
set -uo pipefail
P=/home/rob/tmp/pb-submit-celestia-20261003/bin/python; R=/mnt/shared/prismabuild-fleet/repo/tools; O=/home/rob/tmp/pbfix
W=/home/rob/wt/pb-gate1619-gangonly
for i in 1 2 3; do for set in A B; do
  mapfile -t FILES < $O/bisect$set-files.txt
  $P $R/pbtest.py --checkout $W --python /home/rob/venvs/pb-cpu/bin/python --tag x86 --shards 1 --timeout-s $((1830+i*2+$([ $set = B ] && echo 1 || echo 0))) --json $O/b1648-$set-$i.json "${FILES[@]}" > $O/b1648-$set-$i.log 2>&1
  echo "round $i set $set (${#FILES[@]} files) rc=$? $(grep -E ' passed| failed' $O/b1648-$set-$i.log | tail -1 | cut -c1-90)"; grep -h -E '^FAILED' $O/b1648-$set-$i.log | cut -c1-150
done; done
