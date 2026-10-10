#!/bin/bash
PY=/home/rob/tmp/pb-submit-celestia-20261003/bin/python
PBT=/mnt/shared/prismabuild-fleet/repo/tools/pbtest.py
rm -f /home/rob/tmp/pbattr.status
run() { n=$1; w=$2; c=$3; shift 3; cd "$w" || exit 9
  "$PY" "$PBT" --checkout . --python /home/rob/venvs/pb-cpu/bin/python --tag x86 --tmpdir /tmp \
   --shards $c --workers-per-shard 2 --threads-per-shard 1 --cpus-per-shard 4 --mem-gb 6 --max-clients 2 \
   --timeout-s 1800 --wait-s 3000 --json /home/rob/tmp/pbattr-$n.json "$@" > /home/rob/tmp/pbattr-$n.log 2>&1
  echo "$n rc=$?" >> /home/rob/tmp/pbattr.status; }
run red /home/rob/prismabuild-wt/pb-attrib-red 1 tests/test_control_attribution_born_in_interval.py
run green /home/rob/prismabuild-wt/pb-attrib 1 tests/test_control_attribution_born_in_interval.py
run wide /home/rob/prismabuild-wt/pb-attrib 3 $(cat /home/rob/tmp/pbattr-wide.txt) tests/test_control_attribution_born_in_interval.py tests/test_duplication_baseline.py
