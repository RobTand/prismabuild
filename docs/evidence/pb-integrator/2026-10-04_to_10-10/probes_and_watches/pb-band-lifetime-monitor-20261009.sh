#!/bin/bash
# Read-only timeline of action dee0bc15bfab on sparklina from claim to real cleanup. Starts nothing, signals nothing. Author pb-integrator, 2026-10-09.
K=dee0bc15bfab5b63e980083e1dae16186f48c7583cb96692842b5ff90c441c6a; Q=/mnt/shared/prismabuild-fleet/pb-queue
OUT=/home/rob/fleet/inventory/pb-band-dee0-lifetime-20261009.jsonl; : > $OUT
end_seen=""
for i in $(seq 1 480); do
  now=$(date -u +%s.%N)
  state=none; for s in claimed done failed withdrawn ready; do [ -e $Q/$s/$K.json ] && { state=$s; break; }; done
  obs=$(timeout 25 ssh -o BatchMode=yes -o ConnectTimeout=8 sparklina 'c=$(docker ps --filter name=pact-band-dee0bc15bfab --format "{{.Names}}|{{.RunningFor}}" 2>/dev/null | head -1); g=$(nvidia-smi --query-compute-apps=pid,used_gpu_memory --format=csv,noheader 2>/dev/null | tr "\n" ";"); s=/sys/fs/cgroup/prismabuild.slice/prismabuild-job8533dfd6b6064c96702b69e259b86933.slice; if [ -d $s ]; then sp=$(wc -l < $s/cgroup.procs); sm=$(cat $s/memory.current); else sp=gone; sm=gone; fi; echo "$c#$g#$sp#$sm"' 2>/dev/null)
  echo "{\"t\":$now,\"state\":\"$state\",\"obs\":\"$obs\"}" >> $OUT
  case "$obs" in "#"*"#gone#gone"|"##gone#gone") [ "$state" != claimed ] && end_seen=$((${end_seen:-0}+1));; esac
  [ -n "$end_seen" ] && [ "$end_seen" -ge 8 ] && break
  sleep 15
done
echo "monitor ended after $i samples; last: $(tail -1 $OUT)"
