# Design note: demand-based gang fence (B) for #1579

Status: proposal for CEO approval. No code until approved. PR 1580 is now a draft and is superseded by this.
Author: pb-integrator, 2026-10-06. Decision basis: dec-1006-141107-317d, review 4 of PR 1580.

## Why the exemption approach keeps failing

Rounds 1 to 4 each found a row the drain holds that the gang is waiting on. Every fix was a way of
recognising such rows: by type, by a plan read, by a transitive closure. Each recognition method has
a hole: the plan is gone when the consumer ends, the plan may not read, and reading plans for every
claim is unbounded work. B stops recognising rows. It reserves capacity instead.

## The rule

A gang whose election on host H has waited longer than 10 minutes (as today, from the first member's
publication time) RESERVES its elected member's declared demand D on H. New equal-priority work on H is
admitted only if the reservation survives it:

    for every dimension d the member declares:  held_H[d] + R[d] + D[d]  <=  capacity_H[d]

R is the new row's own declared demand; `held_H` and `capacity_H` come from H's resource ledger.
Otherwise the row is denied as `deferred_for_gang_reservation`, naming the dimension and the shortfall.
Strictly lower priority is fenced from election, as today. The gang's own members, higher priority work,
and rows on other hosts are untouched. Nothing is lent. Running work is never touched.

Everything the rule reads is already read in the claim pass: the member's `resources` from the census rows
(read strictly today), the row's sealed demand, and H's ledger under H's admission lock. No plan reads,
no closure walk, no new census work. Cost is one comparison per election per row.

## Returners and prerequisites

Work that gives capacity back (stage or RAM egress, produced export) must be admissible when the host is
exactly at the reservation line, or a running action waiting on it can hold the host forever. B handles
this by declaration, not recognition: the sealers of those rows (`pbrun` stage and RAM egress,
`produced_output`, `produced_spool` exports) set one sealed row field, `returns_capacity: true`. The
census already reads the row, so the field costs nothing and cannot disappear when a consumer goes
terminal. A declared returner skips the reservation test. A row without the field, including every row
published before the change, is treated as consuming. That is the fail-safe direction the CEO asked for:
unknown never means exempt.

Prerequisite movers (a lead, a lead's lead) are CPU and tier-token rows (cpu 1, mem 1, stage tokens on the
tier ledger, not the host). They fit in the slack the member leaves on H, so they need no list and no
closure; if running work has used the slack they wait, at most until that work ends.

The publication canary slot keeps its own contract (next free safe boundary) and stays exempt, as in
PR 1580. It is a sealed declaration (`publication_canary`), not an inference.

## Fail-safe cases

- Row demand missing a dimension the member declares, or unreadable: treated as taking the whole
  capacity in that dimension, so held.
- Member demand unknown or unreadable: the reservation is the whole host (everything not declared as a
  returner is held), never "no reservation".
- Host ledger unreadable in the pass: deny the row for the pass, as other ledger faults do.
- Election on another host, or no rank: no effect, as today.

## Invariants

I1 Bound. After the reservation starts, a row that is not a declared returner is admitted only if
`held + R + D <= capacity`. So `held` cannot rise above `capacity - D` through new admissions, and while
`held` is above that line it only falls. Each running job is capped at 30 minutes, so the gang's
hosts reach `held <= capacity - D` within 10 minutes plus the longest running job, after which the
member is admissible subject to its other gates (barrier, residency). Declared returners each hold
cpu 1 and mem 1 for a bounded time, so they add one such run at worst.

I2 No cycle. The gang waits on running work, which is never held; on returners, which are declared
and never held; and on prerequisite movers, which are held only while running work occupies the slack
and are released when it ends. No waited-on row depends on a held row except through running work that
finishes, so the wait-for graph has no cycle.

I3 Unknown is consuming. No unreadable, missing, or undeclared row is ever exempt.

I4 Bounded work. The census gains no plan or CAS reads; the added cost is O(elections) per claim.

## Known limits

- A GPU prerequisite on a member host cannot coexist with the member by definition. The planner never
  emits one (movers are CPU and tier rows). B holds such a row and says so in the denial; the gang
  then waits on it. This is documented, with a test that asserts the denial text.
- An old undeclared egress, in flight at rollout, is held only when running work has exhausted the
  slack, and only until that work ends. It is the one residual delay, and it is bounded.
- Mixed generations: an old worker ignores the reservation and the new field. The start barrier still
  prevents a lone start.

## Test plan

1. Pure rule table over the real function, on a whole-box member (cpu 20, mem 100, gpu 1 on a 20/128/1
   host): GPU single held; 8 cpu / 64 GB single held; 1/1 undeclared row admitted while slack remains
   and held when it is exhausted; declared returner always admitted; missing cpu key held; unreadable
   demand held; unknown member demand reserves the whole host; strictly lower priority still fenced from
   election; own members and higher priority never held.
2. Timeline simulation with the real rule as the admission predicate: seeded random running jobs
   (each at most 30 minutes) and arrival streams of every row class, many seeds. Assert I1: the gang is
   admissible by drain bound plus the longest remaining run, and `held` is non-increasing for
   non-returner admissions after the reservation starts.
3. No-cycle enumeration: every row class the gang can wait on (declared egress, stage mover, mover of a
   mover, export), with running work occupying all slack except its own. Assert each is admitted or is
   released when the running work ends.
4. Real claims on the two-host fixture: a drained sparky admits a declared egress, including after its
   consumer is withdrawn (the round 4 hole), admits a prerequisite mover, and denies a GPU single and a
   large CPU single with the shortfall named.
5. Bounded census: monkeypatch `residency_plan.read` to raise and assert the census and claims never
   call it; a malformed row demand is held and does not fail the census.
6. Sealer tests: each egress and export sealer sets `returns_capacity`; a row without it is held by
   demand.
7. Wide run on the gang, measurement, census, admission and preemption files, compared with main.

## What changes from PR 1580

Keep: the election rank and clock wiring, the ten-minute bound, the canary exemption, the design text
frame and the tests of the drain timing. Drop: `_member_leads` closure, `_capacity_returners`,
`returns_capacity` and `in_gang_closure` plan-reading predicates, the census field. Add: the reservation
predicate, the member `demand` in the election, the `returns_capacity` row field and its sealers.

## Decisions needed

1. Approve B as above.
2. Approve the sealed `returns_capacity` row field (touches four sealers, one new row key). The
   alternative, no marker, makes returners rely on slack only, which leaves a slack-exhaustion cycle for
   egress when running work is a consumer that waits on that egress. I recommend the field.
3. Confirm the canary exemption stays.
