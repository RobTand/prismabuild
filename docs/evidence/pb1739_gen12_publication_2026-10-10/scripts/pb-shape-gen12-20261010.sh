#!/bin/bash
# Pre-publish shape gate (#987) for the gen12 commit 34c2228cb2. Same command as pb-carryB-qualify-20261006.sh. Author pb-integrator, 2026-10-09.
set -uo pipefail
cd /home/rob/wt/pb-gen12-prep && [ "$(git rev-parse HEAD)" = 34c2228cb25e47e48d26781efc9fd6f1e378dd53 ] && [ -z "$(git status --short)" ] || { echo "worktree not at the approved head or dirty: stop"; exit 1; }
PY=/usr/bin/python3; PBT=/mnt/shared/prismabuild-fleet/repo/tools/pbtest.py
"$PY" "$PBT" --checkout /home/rob/wt/pb-gate-gen12 --python /home/rob/venvs/pb-cpu/bin/python --tag x86 --tmpdir /tmp \
 --shards 1 --workers-per-shard 1 --threads-per-shard 1 --cpus-per-shard 2 --mem-gb 8 --max-clients 1 \
 --timeout-s 1800 --wait-s 3600 --json /mnt/shared/fleet-ceo/pb-integrator-gates/pb-gen12-shape-20261010.json tests/gate_campaign_shape.py > /mnt/shared/fleet-ceo/pb-integrator-gates/pb-gen12-shape-20261010.log 2>&1
echo "shape rc=$?"
