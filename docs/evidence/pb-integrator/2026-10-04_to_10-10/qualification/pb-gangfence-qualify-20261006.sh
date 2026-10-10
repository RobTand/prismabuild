#!/bin/bash
PY=/home/rob/tmp/pb-submit-celestia-20261003/bin/python
PBT=/mnt/shared/prismabuild-fleet/repo/tools/pbtest.py
rm -f /home/rob/tmp/pbgf.status
run() { n=$1; w=$2; c=$3; shift 3; cd "$w" || exit 9
  "$PY" "$PBT" --checkout . --python /home/rob/venvs/pb-cpu/bin/python --tag x86 --tmpdir /tmp \
   --shards $c --workers-per-shard 2 --threads-per-shard 1 --cpus-per-shard 4 --mem-gb 6 --max-clients 2 \
   --timeout-s 1800 --wait-s 3000 --json /home/rob/tmp/pbgf-$n.json "$@" > /home/rob/tmp/pbgf-$n.log 2>&1
  echo "$n rc=$?" >> /home/rob/tmp/pbgf.status; }

run green /home/rob/prismabuild-wt/pb-gangfence 1 tests/test_gang_reservation_1517.py tests/test_gang_fence_equal_priority_1517.py
run wide /home/rob/prismabuild-wt/pb-gangfence 3 $(cat /home/rob/tmp/pbgf-wide.txt) tests/test_duplication_baseline.py
