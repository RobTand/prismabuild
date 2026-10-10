#!/bin/bash
PY=/home/rob/tmp/pb-submit-celestia-20261003/bin/python
PBT=/mnt/shared/prismabuild-fleet/repo/tools/pbtest.py
F="tests/test_generation_drift_refusals.py tests/test_worker_gpu_adaptive_contract.py tests/test_worker_loop_follows_a_rename.py"
rm -f /home/rob/tmp/pbroots.status
run() { n=$1; w=$2; cd "$w" || exit 9
  "$PY" "$PBT" --checkout . --python /home/rob/venvs/pb-cpu/bin/python --tag x86 --tmpdir /tmp \
   --shards 1 --workers-per-shard 2 --threads-per-shard 1 --cpus-per-shard 4 --mem-gb 6 --max-clients 1 \
   --timeout-s 900 --wait-s 1800 --json /home/rob/tmp/pbroots-$n.json $F > /home/rob/tmp/pbroots-$n.log 2>&1
  echo "$n rc=$?" >> /home/rob/tmp/pbroots.status; }
run red /home/rob/prismabuild-wt/pb-triage
run green /home/rob/prismabuild-wt/pb-roots
