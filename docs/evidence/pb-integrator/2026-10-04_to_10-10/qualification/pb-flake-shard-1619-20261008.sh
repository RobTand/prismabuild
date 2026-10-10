#!/bin/bash
# Rerun cand shard 11's exact file set on both arms, alternating, with a distinct --timeout-s per round (1801..1803; --pytest-args replaces addopts, so not used)
# so each round has its own action key. Author pb-integrator, 2026-10-08.
set -uo pipefail
P=/home/rob/tmp/pb-submit-celestia-20261003/bin/python; R=/mnt/shared/prismabuild-fleet/repo/tools; O=/home/rob/tmp/pbfix
mapfile -t FILES < $O/shard11-files.txt
for i in 1 2 3; do for arm in cand base; do
  W=/home/rob/wt/pb-gate1619-$arm
  $P $R/pbtest.py --checkout $W --python /home/rob/venvs/pb-cpu/bin/python --tag x86 --shards 1 --timeout-s $((1800+i)) --json $O/shard11-$arm-$i.json "${FILES[@]}" > $O/shard11-$arm-$i.log 2>&1
  echo "round $i $arm rc=$? $(grep -E ' passed| failed' $O/shard11-$arm-$i.log | tail -1 | cut -c1-110)"
  grep -h -E '^FAILED' $O/shard11-$arm-$i.log | cut -c1-170
done; done
