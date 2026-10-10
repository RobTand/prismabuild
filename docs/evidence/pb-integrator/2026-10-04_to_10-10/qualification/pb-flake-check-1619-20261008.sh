#!/bin/bash
# Repeat the one candidate-only failure on both arms, alternating. Author pb-integrator, 2026-10-08.
set -uo pipefail
P=/home/rob/tmp/pb-submit-celestia-20261003/bin/python; R=/mnt/shared/prismabuild-fleet/repo/tools; O=/home/rob/tmp/pbfix
F=tests/test_a_replaced_progress_record_is_read_again.py
for i in 1 2 3 4; do for arm in base cand; do
  W=/home/rob/wt/pb-gate1619-$arm
  $P $R/pbtest.py --checkout $W --python /home/rob/venvs/pb-cpu/bin/python --tag x86 --shards 1 --json $O/flake1619-$arm-$i.json $F > $O/flake1619-$arm-$i.log 2>&1
  echo "round $i $arm rc=$? $(grep -E 'passed|failed' $O/flake1619-$arm-$i.log | tail -1 | cut -c1-80)"
done; done
