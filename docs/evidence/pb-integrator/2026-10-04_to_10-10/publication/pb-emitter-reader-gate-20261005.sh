#!/bin/bash
# Mechanical gate (CEO dec-1005-222738-0a37, option C). Author pb-integrator, 2026-10-05.
#
# REFUSES, loudly, to let a publication go ahead when the head contains the #1547 EMITTER (a worker runtime body that
# carries the optional digest_primitives key) while ANY pbmcp reader is older than the reader-first generation.
# An old reader raises CASTamperError (a RuntimeError) on a receipt that carries the new key, so every old reader must have
# restarted before such a generation is activated.
#
# usage: pb-emitter-reader-gate-20261005.sh REL_WORKTREE HEAD_SHA
# env:   READER_FIRST_UNIX  unix time at which the reader-first generation (the first with PR 1558) was activated.
#                           default 1791235724 = 2026-10-05T21:28:44Z, generation 4aa83ee71960-1791235724-011145925cda
#        GATE_HOSTS         hosts to inspect over ssh (default: sparky sparklina dl380g10); this host is always inspected too
# exit:  0 no emitter in the head, or no stale reader anywhere; 6 stale reader found; 7 a host could not be inspected
#        (an unverifiable host counts as a refusal: this gate fails closed); 2 bad usage.
set -uo pipefail
REL=${1:?usage: $0 REL_WORKTREE HEAD_SHA}; HEAD=${2:?usage: $0 REL_WORKTREE HEAD_SHA}
CUT=${READER_FIRST_UNIX:-1791235724}
HOSTS=${GATE_HOSTS:-"sparky sparklina dl380g10"}
[[ "$HEAD" =~ ^[0-9a-f]{40}$ ]] || { echo "emitter gate: HEAD must be 40 hex" >&2; exit 2; }

emitter=0
# NOTE: count matches instead of 'grep -q': under pipefail grep -q exits at the first match, git show dies of SIGPIPE and the
# pipeline reports failure, which silently turned an emitter head into "no emitter" (found by testing this gate, 2026-10-05).
if git -C "$REL" cat-file -e "$HEAD:src/prismabuild/digest_primitives.py" 2>/dev/null; then
  core_text=$(git -C "$REL" show "$HEAD:src/prismabuild/core.py" 2>/dev/null) || { echo "emitter gate: cannot read core.py at ${HEAD:0:12}: REFUSED (fails closed)" >&2; exit 7; }
  hits=$(printf '%s\n' "$core_text" | grep -c '"digest_primitives": digest_owner' || true)
  if [ "${hits:-0}" -gt 0 ]; then
    emitter=1
  else
    # the owner file exists but the emitter line was not found: do not assume safety, the layout may have changed
    echo "emitter gate: digest_primitives.py exists at ${HEAD:0:12} but the emitter line was not found in core.py: REFUSED until the detection is updated (fails closed)" >&2; exit 7
  fi
fi
if [ "$emitter" = 0 ]; then
  echo "emitter gate: ${HEAD:0:12} does not contain the digest_primitives emitter; the reader gate does not apply"
  exit 0
fi
echo "emitter gate: ${HEAD:0:12} CONTAINS the digest_primitives emitter; checking pbmcp readers older than $(date -u -d @"$CUT" +%FT%TZ)"

probe='python3 - <<"E"
import subprocess,time,re
now=time.time()
out=subprocess.run(["ps","-eo","pid,etimes,args"],capture_output=True,text=True).stdout.splitlines()[1:]
for l in out:
    p=l.split(None,2)
    # count only a python interpreter running pbmcp.py: the ssh tunnels on the client host carry the remote command text and
    # would double-count every session (found by testing this gate, 2026-10-05)
    argv0=p[2].split(None,1)[0].rsplit("/",1)[-1] if len(p)==3 else ""
    if len(p)==3 and argv0.startswith("python") and "tools/fleet/pbmcp.py" in p[2] and "python3 -" not in p[2]:
        print("%d %d" % (int(p[0]), int(now-int(p[1]))))
E'

stale=0; unknown=0; total=0
check() {   # $1 label, rest: command producing "pid start_unix" lines
  local label=$1; shift
  local out; out=$("$@" 2>&1); local rc=$?
  if [ $rc -ne 0 ]; then echo "emitter gate: CANNOT INSPECT $label (rc=$rc): $(echo "$out" | head -1 | cut -c1-120)" >&2; unknown=$((unknown+1)); return; fi
  while read -r pid start; do
    [[ "$pid" =~ ^[0-9]+$ && "$start" =~ ^[0-9]+$ ]] || continue
    total=$((total+1))
    if [ "$start" -lt "$CUT" ]; then
      stale=$((stale+1)); echo "emitter gate: STALE READER on $label: pbmcp pid $pid started $(date -u -d @"$start" +%FT%TZ) (before the reader-first activation)" >&2
    fi
  done <<< "$out"
}
for h in $HOSTS; do check "$h" ssh -o BatchMode=yes -o ConnectTimeout=10 "$h" "$probe"; done
check "$(hostname) (local)" bash -c "$probe"

echo "emitter gate: inspected pbmcp readers: $total, older than the cut-off: $stale, hosts not inspectable: $unknown"
if [ "$unknown" -gt 0 ]; then
  echo "emitter gate: REFUSED: a host could not be inspected, so zero stale readers is not proven. Do not publish." >&2; exit 7
fi
if [ "$stale" -gt 0 ]; then
  echo "emitter gate: REFUSED: $stale pbmcp reader(s) older than the reader-first generation. An old reader raises CASTamperError on a receipt with the new digest_primitives key. Restart those agent sessions, then re-run. Do not publish." >&2; exit 6
fi
echo "emitter gate: OK: no pbmcp reader older than the reader-first generation on any inspected host"
exit 0
