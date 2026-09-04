#!/bin/bash
# Does a cgroup MemoryMax charge a CUDA allocation on GB10 unified memory?
#
# Three arms under one 4 GiB cap, each asking for 8 GiB in 512 MiB steps and
# touching every page it takes; only the allocator differs.  Kept in the tree
# because the finding in docs/memory_enforcement_2026-09-04.md is only as good
# as the method that produced it, and because the next box, driver or torch is
# not covered by the last answer.
#
# Submit it, never run it out of pool -- it wants a GPU:
#     tools/fleet/pbrun.py --gpu --demand mem_gb=16 -- #         bash tools/fleet/probes/run_cgroup_cuda_probe.sh
PY=/home/rob/dq-runs/venvs/prismaquant-cu130/bin/python
PROBE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/cgroup_cuda_probe.py"
CAP=4
TARGET=8192
STEP=512

echo "== box =="
hostname
grep MemAvailable /proc/meminfo
nvidia-smi --query-gpu=power.draw --format=csv,noheader 2>&1 | head -1

SETENV=""
for V in CUDA_VISIBLE_DEVICES TRITON_CACHE_DIR TMPDIR HOME PATH; do
  # Forward only what is actually SET.  Forwarding an unset name as an empty
  # value is not a no-op: CUDA_VISIBLE_DEVICES="" means "no devices", and it
  # cost the first run of this probe both GPU arms.
  if [ -n "${!V+x}" ]; then SETENV="$SETENV --setenv=$V=${!V}"; fi
done
echo "-- forwarding:$SETENV"
"$PY" -c "import torch; print('cuda_available_uncapped', torch.cuda.is_available())"

for ARM in host cuda pinned; do
  UNIT="pbcap-probe-$ARM-$$"
  echo
  echo "=================== arm=$ARM cap=${CAP}G target=${TARGET}MiB ==================="
  systemctl --user reset-failed "$UNIT" 2>/dev/null
  if [ "$ARM" = host ]; then INTERP=/usr/bin/python3; else INTERP=$PY; fi
  systemd-run --user --quiet --pipe --wait --unit="$UNIT" \
    -p MemoryMax=${CAP}G -p MemorySwapMax=0 -p MemoryAccounting=yes \
    ${SETENV} \
    -- "$INTERP" "$PROBE" "$ARM" "$TARGET" "$STEP"
  RC=$?
  echo "-- systemd-run rc=$RC"
  echo -n "-- unit: "
  systemctl --user show "$UNIT" -p Result -p ExecMainStatus -p ExecMainCode -p MemoryPeak 2>&1 | tr '\n' ' '
  echo
  systemctl --user reset-failed "$UNIT" 2>/dev/null
done
echo
echo "== after =="
grep MemAvailable /proc/meminfo
