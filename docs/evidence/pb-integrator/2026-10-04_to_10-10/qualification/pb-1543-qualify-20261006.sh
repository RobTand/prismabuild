#!/bin/bash
PY=/home/rob/tmp/pb-submit-celestia-20261003/bin/python
PBT=/mnt/shared/prismabuild-fleet/repo/tools/pbtest.py
rm -f /home/rob/tmp/pb1543.status
run() { n=$1; w=$2; c=$3; shift 3; cd "$w" || exit 9
  "$PY" "$PBT" --checkout . --python /home/rob/venvs/pb-cpu/bin/python --tag x86 --tmpdir /tmp \
   --shards $c --workers-per-shard 2 --threads-per-shard 1 --cpus-per-shard 4 --mem-gb 6 --max-clients 2 \
   --timeout-s 1800 --wait-s 3000 --json /home/rob/tmp/pb1543-$n.json "$@" > /home/rob/tmp/pb1543-$n.log 2>&1
  echo "$n rc=$?" >> /home/rob/tmp/pb1543.status; }
run red /home/rob/prismabuild-wt/pb-1543-red 1 tests/test_gang_residency_members.py
run green /home/rob/prismabuild-wt/pb-1543 1 tests/test_gang_residency_members.py
run wide /home/rob/prismabuild-wt/pb-1543 3 $(cat /home/rob/tmp/pb1543-wide.txt) tests/test_duplication_baseline.py
