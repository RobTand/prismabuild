# Handoff: #1589, class-scoped CPU work behind the READY-GPU guard (PR 1590), to the executive engineer
Author: pb-integrator, 2026-10-06 ~21:25Z. State: PR 1590 open at head d8acbfa2, THREE reviews, last REQUEST_CHANGES (second failed corrective revision). Do not publish this head. #1589 stays open. No code changes by me after the last review.

## The defect (confirmed in the published code)
`PoolQueue.claim` (pool.py ~20680, the guard after `placement_mismatch`) leaves every unpinned CPU-only row READY on a GPU host while any READY GPU row is eligible for that host (#1526 `deferred_for_ready_gpu`), across priority bands, with no timer. A genuine arm64 CPU row tagged `gb10` (no host without a GPU can run it) is deferred for good on both Sparks: kernels' smoke d93fd4bc4741 (record kernels-875-runtime-repair-sol-20261006; gates PR 1017 runtime proof and ten prepared GPU successor rows). #1262's own docstring describes this case and says it is spread across GPU hosts "subject to the same READY-GPU rule", which is the starvation. docs/design.md ~5101 said class-scoped tags bypass only the CPU-host deferral, not the READY-GPU rule.

## Where everything is
Branch `fix/ready-gpu-guard-class-scoped-1589`, worktree `/home/rob/prismabuild-wt/pb-1589`, head d8acbfa2. Main worktree for comparison `/home/rob/prismabuild-wt/pb-1589-main`; previous head `/home/rob/prismabuild-wt/pb-1590-prev`. Scripts (PrismaBuild only): `/home/rob/tmp/pb1589-r3.sh` (RED on main, previous head, GREEN, 15-file surface), surface list `/home/rob/tmp/pb1589-surface.txt`. Evidence: RED 29 failed/16 passed (08b6265b7198), GREEN 45 passed (316c9beb7a1f), surface 240 passed vs main 239 passed + 1 failed. Record: `~/fleet/records/pb-integrator-main.json` evidence.pb1589_ready_gpu_guard.

## What the code does now (and what is solid)
- Candidate: required tags carried only by GPU hosts (read from ALL retained offers incl. stale; needs evidence on both sides; a brief x86 outage does not turn portable work class-scoped); no gang member; plain bounded host demand (explicit cpu and mem_gb, nothing else).
- Room (`_class_scoped_room`): read once per pass BEFORE any admission lock; the eligible GPU row must tolerate a CPU holder (not a measurement row, explicit cpu+mem_gb, not a gang member); then `_ready_gpu_row_room`.
- Boundary (`_class_scoped_beside_room`): under host admission, reads only free tokens; passes only if the room still fits after the row's own tokens.
Reviewers verified: no new CAS read at the boundary, the cache structure, retained-offer classification, the portable and host-pinned behaviour.

## What is NOT solved: the review history
1. f48ff86f: fit against TOTAL capacity ignored incumbents, cumulative admissions, measurement/exclusivity, tier/export. -> rebuilt on the #1169 room machinery against FREE tokens.
2. 885793fd: a GPU measurement or unbounded-CPU row can still be kept from starting; gang elections survive refusal; `_declared_run_bound` was an unbounded CAS read under admission. -> GPU-row conditions, gang exclusion, run bound removed, reads moved before the lock.
3. d8acbfa2 (comment 6025714022): (a) FREE-TOKEN FIT DOES NOT PRESERVE ADAPTIVE ADMISSION: a 2-CPU holder costs 2.5 CPUs under the adaptive controller, so a 6-CPU GPU row on an 8-CPU host can be blocked although six free tokens remain; (b) an unreadable GPU request returns an unknown/non-measurement fallback (`adaptive_gpu.action_contract` returns `(None, measurement, True, budget)` instead of raising), which my exemption accepts and which can then block a GPU measurement; (c) my once-per-pass regression test never evaluates the second candidate.

## Assessment
(b) and (c) are mechanical (treat `shape is None` from `action_contract` as unreadable; make the read-once test actually evaluate both candidates). (a) is not: the cost the adaptive controller charges a holder is not its declared demand (foreign-load and sampling margins), so NO arithmetic on free tokens can prove the GPU row stays startable. Every round has been a different way of approximating the GPU row's admission. Options, each a decision:
 A. Ask the real controller: a dry-run of the GPU row's admission decision with the CPU row added as a hypothetical holder, under host admission. Exact, but needs a hypothetical-holder mode in `adaptive_cpu`/`adaptive_gpu` decisions (new contract, new tests, new review).
 B. Serialise: a class-scoped row passes only when the host otherwise holds nothing (idle), at most one at a time, and only when the eligible GPU row has been refused here or cannot start for reasons unrelated to tokens. Then the delay a CPU row can cause is bounded by that row's own run, which needs a total run bound (the read review 2 rejected under the lock; it can be taken before the lock once per candidate).
 C. Policy: accept that a small architecture-bound prerequisite may delay a GPU row for its own run (the downstream kernels GPU rows are themselves waiting on this smoke), state it in design.md, and keep only the cheap protections (gang, measurement, bounded demand, idle host).
 D. Leave the guard as is and have the CEO sequence the smoke when no eligible GPU row is READY (operational; no code).
My recommendation: C with B's idle-host condition, because it is the only one a reviewer can verify without a new admission contract, and the delay is bounded and stated; A if the engineer wants exactness.

## Interim for kernels
Nothing supported can admit d93fd4bc4741 until a generation carries a fix: no host pin, fake GPU demand or duplicate key was used and none should be. The row stays READY; nothing is lost. Publication needs an approved PR, a carry onto the live generation (61616944b033-1791311474-69cd52cc357f), a shape gate and the CEO's explicit yes; it would ride the prepared 1587 generation (D39) if approved in time.
