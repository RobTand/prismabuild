#!/bin/bash
PY=/home/rob/tmp/pb-submit-celestia-20261003/bin/python
PBT=/mnt/shared/prismabuild-fleet/repo/tools/pbtest.py
run() { n=$1; w=$2; c=$3; shift 3; cd "$w" || exit 9
  "$PY" "$PBT" --checkout . --python /home/rob/venvs/pb-cpu/bin/python --tag x86 --tmpdir /tmp \
   --shards 1 --workers-per-shard 2 --threads-per-shard 1 --cpus-per-shard $c --mem-gb 4 --max-clients 1 \
   --timeout-s 900 --wait-s 1800 --json /home/rob/tmp/pbkill-$n.json "$@" > /home/rob/tmp/pbkill-$n.log 2>&1
  echo "$n rc=$?" >> /home/rob/tmp/pbkill.status; }
rm -f /home/rob/tmp/pbkill.status
F=tests/test_a_kill_names_what_it_waited_on.py
run red2cpu /home/rob/prismabuild-wt/pb-kill-red 2 $F
run green2cpu /home/rob/prismabuild-wt/pb-kill 2 $F
run green4cpu /home/rob/prismabuild-wt/pb-kill 4 $F
run base4cpu /home/rob/prismabuild-wt/pb-kill-red 4 $F
