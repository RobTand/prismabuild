#!/bin/bash
PY=/home/rob/tmp/pb-submit-celestia-20261003/bin/python
PBT=/mnt/shared/prismabuild-fleet/repo/tools/pbtest.py
W=/home/rob/tmp/pb-sdk4-carry5-20261006
rm -f /home/rob/tmp/pbcarryB.status
cd $W || exit 9
# 1) the touched gang surface on the carry head
"$PY" "$PBT" --checkout . --python /home/rob/venvs/pb-cpu/bin/python --tag x86 --tmpdir /tmp \
 --shards 1 --workers-per-shard 2 --threads-per-shard 1 --cpus-per-shard 4 --mem-gb 6 --max-clients 1 \
 --timeout-s 1800 --wait-s 3000 --json /home/rob/tmp/pbcarryB-gang.json \
 tests/test_gang_residency_members.py tests/test_pbgang_member_options_1517.py tests/test_gang_reservation_1517.py > /home/rob/tmp/pbcarryB-gang.log 2>&1
echo "gang rc=$?" >> /home/rob/tmp/pbcarryB.status
# 2) the shape gate of this head (same action as for earlier carries)
"$PY" "$PBT" --checkout . --python /home/rob/venvs/pb-cpu/bin/python --tag x86 --tmpdir /tmp \
 --shards 1 --workers-per-shard 1 --threads-per-shard 1 --cpus-per-shard 2 --mem-gb 8 --max-clients 1 \
 --timeout-s 1800 --wait-s 3600 --json /home/rob/tmp/pbcarryB-shape.json tests/gate_campaign_shape.py > /home/rob/tmp/pbcarryB-shape.log 2>&1
echo "shape rc=$?" >> /home/rob/tmp/pbcarryB.status
