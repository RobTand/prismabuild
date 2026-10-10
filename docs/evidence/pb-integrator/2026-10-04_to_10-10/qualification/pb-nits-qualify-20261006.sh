#!/bin/bash
PY=/home/rob/tmp/pb-submit-celestia-20261003/bin/python
PBT=/mnt/shared/prismabuild-fleet/repo/tools/pbtest.py
run() { n=$1; w=$2; shift 2; cd "$w" || exit 9
  "$PY" "$PBT" --checkout . --python /home/rob/venvs/pb-cpu/bin/python --tag x86 --tmpdir /tmp \
   --shards 1 --workers-per-shard 2 --threads-per-shard 1 --cpus-per-shard 2 --mem-gb 4 --max-clients 1 \
   --timeout-s 900 --wait-s 1800 --json /home/rob/tmp/pbnits-$n.json "$@" > /home/rob/tmp/pbnits-$n.log 2>&1
  echo "$n rc=$?" >> /home/rob/tmp/pbnits.status; }
rm -f /home/rob/tmp/pbnits.status
F="tests/test_a_ram_tier_mints_what_the_tmpfs_says_it_may_hold.py tests/test_census_refusal_is_once_per_pass_1571.py"
run green /home/rob/prismabuild-wt/pb-nits $F
run mutA /home/rob/prismabuild-wt/pb-nits-mutA $F
run mutB /home/rob/prismabuild-wt/pb-nits-mutB $F
