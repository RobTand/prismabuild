#!/bin/bash
# The four-leg canary for generation 2399c0008ff7-1791450990-6fa074e867f3 (CEO dec-1008-080711-64fe). Author pb-integrator, 2026-10-08.
# From sparky, as the 10-07 window script does: leg 4 pins sparky and sparklina by tag. Exit 0 is the only pass.
set -uo pipefail
G=2399c0008ff7-1791450990-6fa074e867f3
GP=/mnt/shared/prismabuild-fleet/runtime-generations/$G
D=/mnt/shared/fleet-ceo/pb-publish-20261008/carry-clone-gen6
W=/mnt/shared/fleet-ceo/pb-publish-20261008
GPU_IMAGE='prismaquant-glm-derivative@sha256:c0e532d28a78b3bf425bbbc0d862e2840ba624249162aedfadd09748a6c68c37'
RID=$(date -u +%Y%m%dT%H%M%SZ)g6; echo "$RID" > $W/canary-id.txt
ssh -o BatchMode=yes sparky "cd $D && PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 tools/fleet/pbcanary.py \
  --legs leg-1,leg-2,leg-3,leg-4 --priority 0 --run-id $RID --generation $G --published-root $GP --checkout $D \
  --gpu-image '$GPU_IMAGE'" 2>&1 | tee "$W/canary-$RID.log"
rc=${PIPESTATUS[0]}
echo "canary driver exit: $rc (0 is the only pass) result: /mnt/shared/prismabuild-fleet/pb-canary/$RID/canary-result.json"
exit "$rc"
