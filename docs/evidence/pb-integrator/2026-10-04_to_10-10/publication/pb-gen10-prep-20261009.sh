#!/bin/bash
# Build the gen10 release head: MAIN_WANT (a main commit) plus the revert of PR 1640 (merge ba540d991b), exactly one commit, as in gen9.
# usage: pb-gen10-prep-20261009.sh <MAIN_WANT 40 hex> [worktree]   Author pb-integrator, 2026-10-09 (CEO dec re rep-1009-094212-c5ab, step 1).
# Prints the head. Cut rule: MAIN_WANT = the first main commit that holds PRs for 1663, 1664, 1636 and 1690. Does not publish.
set -euo pipefail
M=${1:?MAIN_WANT}; REL=${2:-/home/rob/wt/pb-gen10-prep}; REPO=/home/rob/prismabuild
[[ "$M" =~ ^[0-9a-f]{40}$ ]] || { echo "MAIN_WANT must be 40 hex"; exit 2; }
git -C "$REPO" fetch -q origin
git -C "$REPO" merge-base --is-ancestor "$M" origin/main || { echo "$M is not on origin/main"; exit 3; }
[ -e "$REL" ] && { echo "$REL exists; remove it deliberately first"; exit 3; }
git -C "$REPO" worktree add -q --detach "$REL" "$M"
git -C "$REL" -c user.name="pb-integrator" -c user.email="pb-integrator@fleet.invalid" revert -m 1 --no-edit ba540d991bbd06c3cfc9c59d4b8c60052e6ad09b >/dev/null
[ "$(git -C "$REL" rev-list --count "$M..HEAD")" = 1 ] || { echo "not exactly one commit over main"; exit 4; }
echo "main $M"; echo "head $(git -C "$REL" rev-parse HEAD)"; git -C "$REL" diff --stat "$M" HEAD | tail -1
