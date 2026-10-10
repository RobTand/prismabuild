#!/bin/bash
# CEO decision on dec-1007-210518-efd1 (MITIGATE): 20 runs of test_pool_finish_cleanup_retry on main and on the carry.
# Each run is a distinct action (different --mem-gb), arms alternate, runs are sequential (no shared pytest temp base hazard).
P=/home/rob/tmp/pb-submit-celestia-20261003/bin/python; R=/mnt/shared/prismabuild-fleet/repo/tools
OUT=/home/rob/fleet/inventory/pb-flake-rate-20261007.tsv; : > "$OUT"
for i in $(seq 1 20); do
  mem=$((3+i))
  for arm in main:pb-main2e71 carry:pb-carry-gen3; do
    name=${arm%%:*}; d=${arm##*:}
    cd ~/prismabuild-wt/$d || exit 1
    line=$($P $R/pbtest.py --checkout . --python /home/rob/venvs/pb-cpu/bin/python --tag x86 --shards 1 --workers-per-shard 1 --threads-per-shard 3 --cpus-per-shard 3 --mem-gb $mem --timeout-s 600 --json ~/tmp/pbfix/rate-$name-$i.json tests/test_pool_finish_cleanup_retry.py 2>&1 | grep -E "^shard +[0-9]+ (ok|rc=)" | head -1)
    key=$(echo "$line" | grep -o "action [0-9a-f]\{12\}" | cut -c8-)
    res=$(echo "$line" | sed -E 's/^shard +[0-9]+ +//' | cut -c1-60)
    echo -e "$i\t$name\t$(git rev-parse --short=10 HEAD)\t$key\t$res" >> "$OUT"
  done
done
echo done
