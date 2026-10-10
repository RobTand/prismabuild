#!/bin/bash
# Submit the eng-serve-measure local-pair gang. Author pb-integrator, 2026-10-07. NOT RUN.
# Authority: CEO decision dec-1007-034122-babe (release at source head 9d3ed3f9...) and dec-1006-235656-2a25 (30-minute window).
# usage: MANIFEST=<path> PROOF="<who/when the CEO supplied the completed preparation proof>" ~/fleet/inventory/pb-submit-serve-gang-20261007.sh [--check-only]
set -euo pipefail
: "${MANIFEST:?set MANIFEST}"; : "${PROOF:?set PROOF to the CEO message that supplies the completed preparation proof}"
WANT_GEN=234357e7f55f
R=/mnt/shared/prismabuild-fleet/repo/tools; P=/home/rob/tmp/pb-submit-celestia-20261003/bin/python
[ -s "$MANIFEST" ] || { echo "manifest missing or empty: $MANIFEST"; exit 2; }
live=$(python3 -c "import json;print(json.load(open('/mnt/shared/prismabuild-fleet/repo/RUNTIME_VERSION.json'))['generation'])")
case "$live" in "$WANT_GEN"-*) ;; *) echo "live generation is $live, not $WANT_GEN. STOP and tell the CEO."; exit 3;; esac
python3 - "$MANIFEST" <<'PY'
import json,sys
m=json.load(open(sys.argv[1]))
members=m["members"] if isinstance(m,dict) else m
top=m if isinstance(m,dict) else {}
bad=[]
if len(members)!=2: bad.append(f"expected 2 members, found {len(members)}")
def _tag(x):
    t = x.get("tag") or x.get("tags")
    return str(t[0]) if isinstance(t, list) and len(t) == 1 else str(t)
tags=sorted(_tag(x) for x in members)
if tags!=sorted(["sparky","sparklina"]): bad.append(f"member tags are {tags}")
if int(top.get("priority",-99))!=0: bad.append(f"priority is {top.get('priority')}, want 0")
if int(top.get("timeout_s",0))!=1800: bad.append(f"timeout_s is {top.get('timeout_s')}, want 1800")
for x in members:
    a=x.get("argv") or []
    if "REPLACE" in json.dumps(x): bad.append("placeholder text in a member")
    if not any("rank_window.py" in str(t) for t in a): bad.append(f"{x.get('tag')}: argv has no rank_window.py")
    if x.get("data_manifest") or str(x.get("residency","none"))!="none": bad.append(f"{x.get('tag')}: a staged reader is declared")
if bad:
    print("MANIFEST REFUSED:", *bad, sep="\n  "); sys.exit(4)
print("manifest shape ok:", tags)
PY
echo "== state before submit"; timeout 100 $P $R/pbstatus.py 2>&1 | grep -E "^(sparky|sparklina)|CLAIMED" | cut -c1-200 || true
[ "${1:-}" = "--check-only" ] && { echo "check only; stopping"; exit 0; }
echo "== submit (authorized: $PROOF)"
$P $R/pbgang.py --manifest "$MANIFEST"
