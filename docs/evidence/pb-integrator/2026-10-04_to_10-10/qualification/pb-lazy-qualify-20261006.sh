#!/bin/bash
PY=/home/rob/tmp/pb-submit-celestia-20261003/bin/python
PBT=/mnt/shared/prismabuild-fleet/repo/tools/pbtest.py
rm -f /home/rob/tmp/pblazy.status
run() { n=$1; w=$2; c=$3; shift 3; cd "$w" || exit 9
  "$PY" "$PBT" --checkout . --python /home/rob/venvs/pb-cpu/bin/python --tag x86 --tmpdir /tmp \
   --shards $c --workers-per-shard 2 --threads-per-shard 1 --cpus-per-shard 4 --mem-gb 6 --max-clients 2 \
   --timeout-s 1800 --wait-s 3000 --json /home/rob/tmp/pblazy-$n.json "$@" > /home/rob/tmp/pblazy-$n.log 2>&1
  echo "$n rc=$?" >> /home/rob/tmp/pblazy.status; }
S="tests/test_tools_do_not_run_on_import.py tests/test_pbtest_outcomes_load_is_lazy_1554.py"
run red /home/rob/prismabuild-wt/pb-triage 1 $S
run green /home/rob/prismabuild-wt/pb-lazy 1 $S
run wide /home/rob/prismabuild-wt/pb-lazy 3 $(cat /home/rob/tmp/pblazy-files.txt) tests/test_duplication_baseline.py
