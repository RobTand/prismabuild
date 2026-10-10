#!/bin/bash
PY=/home/rob/tmp/pb-submit-celestia-20261003/bin/python
PBT=/mnt/shared/prismabuild-fleet/repo/tools/pbtest.py
F=tests/test_fleet_membership_busy_resign.py
rm -f /home/rob/tmp/pbresign.status
run() { n=$1; w=$2; cd "$w" || exit 9
  "$PY" "$PBT" --checkout . --python /home/rob/venvs/pb-cpu/bin/python --tag x86 --tmpdir /tmp \
   --shards 1 --workers-per-shard 2 --threads-per-shard 1 --cpus-per-shard 4 --mem-gb 6 --max-clients 1 \
   --timeout-s 900 --wait-s 1800 --json /home/rob/tmp/pbresign-$n.json $F > /home/rob/tmp/pbresign-$n.log 2>&1
  echo "$n rc=$?" >> /home/rob/tmp/pbresign.status; }
run red /home/rob/prismabuild-wt/pb-triage
run green /home/rob/prismabuild-wt/pb-probe
