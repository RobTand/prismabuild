#!/bin/bash
# Pre-publish shape gate (#987) for the gen9 commit c8daa1be41. Same command as pb-carryB-qualify-20261006.sh. Author pb-integrator, 2026-10-09.
set -uo pipefail
cd /home/rob/wt/pb-gen9-prep && [ "$(git rev-parse HEAD)" = c8daa1be416cd4d7fe8a8e55c49924ab33c4de39 ] && [ -z "$(git status --short)" ] || { echo "worktree not at the approved head or dirty: stop"; exit 1; }
PY=/home/rob/tmp/pb-submit-celestia-20261003/bin/python; PBT=/mnt/shared/prismabuild-fleet/repo/tools/pbtest.py
"$PY" "$PBT" --checkout . --python /home/rob/venvs/pb-cpu/bin/python --tag x86 --tmpdir /tmp \
 --shards 1 --workers-per-shard 1 --threads-per-shard 1 --cpus-per-shard 2 --mem-gb 8 --max-clients 1 \
 --timeout-s 1800 --wait-s 3600 --json /home/rob/fleet/inventory/pb-gen9-shape-20261009.json tests/gate_campaign_shape.py > /home/rob/fleet/inventory/pb-gen9-shape-20261009.log 2>&1
echo "shape rc=$?"
