#!/bin/bash
# Waits until one of the two running PACT actions leaves CLAIMED, then prints which and how it ended. Read only.
Q=/mnt/shared/prismabuild-fleet/pb-queue
end=$((SECONDS+3400))
while [ $SECONDS -lt $end ]; do
  for p in a7cf943c 21b0bb55; do
    for s in done failed withdrawn; do
      f=$(ls $Q/$s 2>/dev/null | grep "^$p" | head -1)
      [ -n "$f" ] && { echo "FIRST_END $p state=$s at $(date -u +%H:%M:%SZ) file=$f"; python3 - "$Q/$s/$f" <<'PYEOF'
import json,sys
d=json.load(open(sys.argv[1])); det=d.get('detail') or {}
print({k:d.get(k) for k in ('status','claimed_host','finished_host','claimed_unix','finished_unix')}, 'returncode',det.get('returncode'),'elapsed_s',det.get('elapsed_s'))
print((det.get('stderr') or '')[-400:])
PYEOF
      exit 0; }
    done
  done
  sleep 30
done
echo "NO_END_WITHIN_WINDOW $(date -u +%H:%M:%SZ)"
