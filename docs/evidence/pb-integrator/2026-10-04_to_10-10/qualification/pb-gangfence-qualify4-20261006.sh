#!/bin/bash
PY=/home/rob/tmp/pb-submit-celestia-20261003/bin/python
PBT=/mnt/shared/prismabuild-fleet/repo/tools/pbtest.py
run() { n=$1; w=$2; shift 2; cd "$w" || exit 9
  "$PY" "$PBT" --checkout . --python /home/rob/venvs/pb-cpu/bin/python --tag x86 --tmpdir /tmp \
   --shards 1 --workers-per-shard 2 --threads-per-shard 1 --cpus-per-shard 4 --mem-gb 6 --max-clients 1 \
   --timeout-s 1800 --wait-s 3000 --json /home/rob/tmp/pbgf4-$n.json "$@" > /home/rob/tmp/pbgf4-$n.log 2>&1
  echo "$n rc=$?" >> /home/rob/tmp/pbgf4.status; }
run green /home/rob/prismabuild-wt/pb-gangfence tests/test_gang_residency_members.py tests/test_gang_reservation_1517.py tests/test_gang_fence_equal_priority_1517.py
run mutant /home/rob/prismabuild-wt/pb-gangfence-mut tests/test_gang_residency_members.py
