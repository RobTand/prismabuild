# Handoff: demand-based gang fence for #1579 (PR 1584), to the D37 executive engineer
Author: pb-integrator, 2026-10-06 ~19:00Z. State: PR 1584 open, head 91828039, THREE reviews, last REQUEST_CHANGES. D45 (gangs at priority 0) stays in force. #1579 stays open.

## Goal
Two-Spark gang jobs starve at equal priority: PrismaBuild keeps admitting smaller same-priority singles on a gang member's host (2026-10-06 ~13:10Z, a -10 gang waited 14+ min). Make a gang that has waited 10 minutes start within 10 min plus the longest running job, with no cycle, without lists of recognised rows.

## Where everything is
- Branch `fix/gang-demand-fence-1579` (origin), worktree `/home/rob/prismabuild-wt/pb-gangdemand`, head `918280392caf`. RED worktree `/home/rob/prismabuild-wt/pb-gangdemand-red` (main a8dec2bf + new tests). Design note: `/home/rob/fleet/inventory/pb-gang-demand-fence-design-20261006.md`. Earlier review write-up: `pb-gang-demand-fence-review1-20261006.md`. Record: `~/fleet/records/pb-integrator-main.json` evidence.pr1584_gang_demand_fence.
- Run tests ONLY through PrismaBuild: `/home/rob/tmp/pbgd-r3.sh` (RED+GREEN on the two new test files), `/home/rob/tmp/pbgd-r6.sh` (wide 152 files + the ten files); results land in `/home/rob/tmp/pbgd-r3|r6/*.json`. From celestia (mount restored 17:38Z) or from a fresh clone on dl380g10 (`/home/rob/tmp/pbint-b`).
- Main baseline failures on the touched surface: 39 tests in ten files (dl380g10 environment), identical on main and the branch; list in `/home/rob/tmp/pbgd-r4/basefail.json`.

## What is solid (reviewers verified: precedence, starvation bound, claim cost)
- The rule: after `GANG_RESERVE_AFTER_S` (600 s) an elected host reserves the member's declared demand; a new equal-priority row needs `held + row + reserved <= capacity` per dimension; unknown/unreadable demand = whole host; unreadable ledger = deny; strictly lower priority fenced from election. `src/prismabuild/_measurement_reservation.py` (`gang_blocking`, `reservation_shortfall`, `reservation_priority_on`, election carries `demand`).
- Measurement precedence (CEO addition): reservation wins over a measurement of the gang's priority or lower (fence, withhold incl. carried withholds, election suppressed, gang member past the bound released from a measurement withhold: `PoolQueue._gang_member_past_reservation_bound`); a strictly higher-priority measurement keeps its place.
- Tests: `tests/test_gang_demand_fence_1579.py` (pure table, 60-seed timeline), `tests/test_gang_demand_fence_claims_1579.py` (real two-host claims, full-CPU member that starts through its own mover, carried withhold, priority-10 measurement).

## What is NOT solved: who may be exempt (the roles)
A gang member that takes every CPU leaves no slack for its own prerequisite movers (stage movers are priced up to `readers`=4 CPUs and run on the stage host, which can be a member host) and for capacity-returning releases/egress/exports. They must skip the arithmetic. CEO decisions: PrismaBuild itself assigns the roles `serves_residency` and `returns_capacity`; no caller declarations; publish refuses both names in a sealed action; a path anchor to a published generation. Current implementation: `movement_actions.capacity_role` + `resource_scope.published_generation_member`, called from `PoolQueue.publish`.

## Review history of the role derivation (all static reviews, GPT-6.1 Sol)
1. e9bc0f89: sealed `returns_capacity` in params, any action could set it; mover demand up to 4 CPUs held behind a full-CPU member.
2. 50846042: role read from `params.command`, not the executed `task.argv`; spool exports publish without `recompute`.
3. 91828039 (latest, comment 6023441919): four blocking findings
   a. an action whose `task.argv` is exactly the bash capture wrapper can still run arbitrary code through the sealed environment (PYTHONSTARTUP, PYTHONPATH, BASH_ENV, LD_PRELOAD...) or any executable whose name starts with "python";
   b. classification uses the UNRESOLVED basename, so a submitter-owned alias (a file or symlink named stage_release.py that points at another published tool, or is retargeted after publication) is misclassified;
   c. generation directories are owner-writable and receipts are self-written, so "sealed, receipt, sha256" does not establish publication authority or immutability; the documented residual is incomplete;
   d. duplicate `--operation` arguments classify a resident copy as an eviction.

## My assessment, for the takeover and the CEO's trust-boundary decision
Findings (a) (environment must equal `movement_environment(command)` exactly; interpreter must be the sealed mover python), (b) (classify by the resolved path's basename) and (d) (parse `--operation` once, reject duplicates) are cheap, mechanical fixes. Finding (c) is not: every fleet process and every submitter run as the same Unix user (rob), so no property of the store (mode bits, a receipt) can authenticate WHO sealed an action. Three honest ways out, each a decision, not a patch:
 1. Declare the threat model cooperative: the exemption prevents accidents (a normal submitted action cannot get it by mistake), not an adversary with the same UID. Then (a),(b),(d) plus a documented residual are enough, and the anchor stays a sanity check.
 2. Make PrismaBuild's own publishers the only source of a role: the exemption is carried by a pool-side record written by the movement publishers themselves (tier_loop/pbrun/produced_output/local_resident) at publish time in a pool-owned directory that submitters' tools never write, e.g. `publish(..., role=...)` accepted only when the call comes through those publishers' code path with a recorded publisher identity. Still not adversary-proof under one UID, but there is no derivation to forge. New pool contract; needs a design note and review.
 3. Do not exempt at all: drop the roles, and instead reserve less (the member's demand minus a fixed, configured mover allowance on the host, option B2 in the first note), accepting the bounded delay and documenting it; or stop and keep D45 permanently.
My recommendation: 1 if Rob agrees the fleet is cooperative (it has been so far: the review standard here is a security boundary, but the actors are Rob's own agents); otherwise 3 over 2, because 2 adds a contract that cannot be made adversary-proof without a different UID or signing.

## Counters and policy
Reviews of this design: 3 (first design review 1 + two failed corrections); a failed review counts per policy; takeover by an executive engineer under D37 is the CEO's ruling. PR 1580 (the earlier exemption-list drain, four reviews) is closed as superseded. #1583 (terminal-lead confirmation) is parked behind this; #1582's runtime generation 61616944b033-1791311474-69cd52cc357f is live and canaried (unrelated).
