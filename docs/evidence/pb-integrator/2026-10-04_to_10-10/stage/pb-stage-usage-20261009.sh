#!/bin/bash
# Read-only: who uses /stage/prewarm on dl380g10. Nothing is deleted or moved. Author pb-integrator, 2026-10-09.
OUT=/home/rob/fleet/inventory/pb-stage-usage-20261009.txt
timeout 900 ssh -o BatchMode=yes -o ConnectTimeout=8 dl380g10 'cd /stage/prewarm && echo "top level (GiB, apparent, newest mtime):"; for d in */; do d=${d%/}; s=$(nice -n 19 ionice -c3 du -sb "$d" 2>/dev/null | cut -f1); m=$(find "$d" -maxdepth 6 -type f -printf "%TY-%Tm-%Td %TH:%TM\n" 2>/dev/null | sort | tail -1); echo "$(( ${s:-0} / 1073741824 )) GiB  $d  newest=$m"; done | sort -rn' > $OUT 2>&1
echo "done rc=$? $(wc -l < $OUT) lines"
