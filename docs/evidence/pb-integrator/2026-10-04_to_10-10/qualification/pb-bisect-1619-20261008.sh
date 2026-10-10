#!/bin/bash
# Which added conftest line makes the progress-reader race test fail in its shard? Author pb-integrator, 2026-10-08.
set -uo pipefail
P=/home/rob/tmp/pb-submit-celestia-20261003/bin/python; R=/mnt/shared/prismabuild-fleet/repo/tools; O=/home/rob/tmp/pbfix
mapfile -t FILES < $O/shard11-files.txt
for i in 1 2 3; do for v in gangonly watchonly; do
  W=/home/rob/wt/pb-gate1619-$v
  $P $R/pbtest.py --checkout $W --python /home/rob/venvs/pb-cpu/bin/python --tag x86 --shards 1 --timeout-s $((1810+i)) --json $O/bisect-$v-$i.json "${FILES[@]}" > $O/bisect-$v-$i.log 2>&1
  echo "round $i $v rc=$? $(grep -E ' passed| failed' $O/bisect-$v-$i.log | tail -1 | cut -c1-100)"
  grep -h -E '^FAILED' $O/bisect-$v-$i.log | cut -c1-170
done; done
