#!/bin/bash
# Roster gate: refuse a publication whose tools/fleet/fleet_boxes.json drops, from any box, an arg the LIVE generation's roster declares.
# usage: pb-roster-gate-20261009.sh <release-worktree> <head-sha>   (exit 0 pass; 1 refuse; 2 usage)
# Reason (#1657): gen9 was main plus a revert, so it dropped --gang-admission that only the carry stack had; the supervisor adopts the
# new generation's roster on re-exec and spawned Spark loops without the flag. Override only with ROSTER_REMOVAL_OK="<who/why>".
# Author pb-integrator, 2026-10-09. Reads only.
set -uo pipefail
REL=${1:?release worktree}; HEAD=${2:?head sha}
LIVE=$(readlink -f /mnt/shared/prismabuild-fleet/repo)
python3 - "$LIVE/tools/fleet/fleet_boxes.json" "$REL" "$HEAD" <<'PY'
import json,subprocess,sys,os
live_path,rel,head=sys.argv[1:4]
live=json.load(open(live_path))
new=json.loads(subprocess.run(['git','-C',rel,'show',f'{head}:tools/fleet/fleet_boxes.json'],capture_output=True,text=True,check=True).stdout)
lost=[]
for box,cfg in live.get('boxes',{}).items():
    old=cfg.get('args') or []; cur=(new.get('boxes',{}).get(box) or {}).get('args')
    if cur is None: lost.append((box,'<box missing in the new roster>')); continue
    for a in old:
        if a.startswith('--') and a not in cur: lost.append((box,a))
if lost:
    print('roster gate: the new roster drops args the live generation declares:')
    for b,a in lost: print(f'  {b}: {a}')
    if os.environ.get('ROSTER_REMOVAL_OK'): print('roster gate: override ROSTER_REMOVAL_OK="%s"'%os.environ['ROSTER_REMOVAL_OK']); sys.exit(0)
    sys.exit(1)
print('roster gate: OK: no box loses an arg the live generation declares')
PY
