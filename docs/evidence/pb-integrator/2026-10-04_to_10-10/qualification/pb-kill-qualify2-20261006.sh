#!/bin/bash
PY=/home/rob/tmp/pb-submit-celestia-20261003/bin/python
PBT=/mnt/shared/prismabuild-fleet/repo/tools/pbtest.py
PBR=/mnt/shared/prismabuild-fleet/repo/tools/pbrun.py
W=/home/rob/prismabuild-wt/pb-kill
rm -f /home/rob/tmp/pbkill2.status
run() { n=$1; c=$2; cd $W || exit 9
  "$PY" "$PBT" --checkout . --python /home/rob/venvs/pb-cpu/bin/python --tag x86 --tmpdir /tmp \
   --shards 1 --workers-per-shard 2 --threads-per-shard 1 --cpus-per-shard $c --mem-gb 4 --max-clients 1 \
   --timeout-s 900 --wait-s 1800 --json /home/rob/tmp/pbkill2-$n.json tests/test_a_kill_names_what_it_waited_on.py > /home/rob/tmp/pbkill2-$n.log 2>&1
  echo "$n rc=$?" >> /home/rob/tmp/pbkill2.status; }
run g2 2
run g4 4
# a mask that excludes CPUs 0-3: the last two CPUs of an 8-CPU allocation, asserted in the action itself
cd $W
"$PY" "$PBR" --tag x86 --cpus 8 --demand mem_gb=4 --timeout-s 900 --wait-s 1800 --cwd . -- /bin/bash -c '
set -e
M=$(/home/rob/venvs/pb-cpu/bin/python -c "import os;a=sorted(os.sched_getaffinity(0));print(\",\".join(map(str,a[-2:])))")
echo "allocation: $(/home/rob/venvs/pb-cpu/bin/python -c "import os;print(sorted(os.sched_getaffinity(0)))")  mask: $M"
LOW=$(/home/rob/venvs/pb-cpu/bin/python -c "import os;print(min(sorted(os.sched_getaffinity(0))[-2:]))")
[ "$LOW" -gt 3 ] || { echo "MASK DOES NOT EXCLUDE CPUS 0-3: $M"; exit 7; }
exec taskset -c "$M" /home/rob/venvs/pb-cpu/bin/python -m pytest -p no:cacheprovider -q tests/test_a_kill_names_what_it_waited_on.py' > /home/rob/tmp/pbkill2-masked.log 2>&1
echo "masked rc=$?" >> /home/rob/tmp/pbkill2.status
