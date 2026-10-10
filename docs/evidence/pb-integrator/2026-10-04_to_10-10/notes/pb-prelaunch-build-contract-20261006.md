# Build contract for the prelaunch-resident change (issue #1594, PR 1593, design APPROVED at 5a5b70be43)
Design: docs/design_prelaunch_resident_phase_2026-10-06.md on branch feat/prelaunch-resident-phase-20261006 (read Revisions 1-4; later revisions replace earlier bullets as each says). CEO scope: (1) explicit per-phase declaration in the manifest; (2) leads cover every chunk of a declared phase, tier loop holds the whole prefix until the consumer ends; (3) submission refuses a declared phase/bound larger than the stage tier capacity; (4) no GPU claim until the declared phase is resident, headroom shortage is a wait; (5) undeclared consumers byte-identical. No generation/commit/digest identity refusal anywhere (D32: no seals).

## Names (shared across workers; do not rename)
- Manifest: literal boolean `resident_before_launch: true` on a phase entry. v1: `annotations.phases[i]`. v2: `read_plan.phases[i]` (core's exact phase key set gains this one optional key; a manifest without it normalizes byte-identically to today).
- `storage_tiers.manifest_prelaunch_phases(manifest) -> list[str]`: declared phase names in order; `[]` when none; raises ValueError on a non-bool value or a declared set that is not a contiguous prefix from phase 0.
- Plan: phase entry may carry `resident_before_launch: true` (only when declared; absent otherwise so undeclared plans stay byte-identical, same plan_sha256, same rows). `residency_plan._PHASE_KEYS` gains it; `validate_plan` refuses non-True and non-prefix.
- `residency_plan.prelaunch_phase_names(plan) -> list[str]`; `residency_plan.leads_for(plan)`: every stage-leg chunk mover (chunked or whole) of every declared phase in read order, else today's single lead; `residency_plan.prelaunch_bound(plan, owned_by_others=frozenset()) -> {"retained_gib": T, "suffix_gib": S, "peak_gib": B}` with B = T + max over suffix phases i of (size(pi)+size(p(i+1))), size(p(n+1)) = 0, sizes = sums of the legs' `stage_gib` (ledger chunk demands), legs whose mover key is in owned_by_others excluded. No declaration: the function returns None.
- Ledger (pool.py `ResourceLedger`): `acquisitions_of(holder) -> list[(handle, usec, host, pid, token_count)]` with the exact parse of design R1'''; `transfer_count(from_key, to_key, count) -> int` (per-token rename, never free in between, idempotent, moves up to `count` still-short tokens, refuses acquisition handles like `transfer`). PoolQueue gets `transfer_tier_reservation_count(tier_id, from_key, to_key, count)` mirroring `transfer_tier_reservation`.
- Admission: `residency_verdict` gains refusal state `residency_prelaunch_undeclared` (in RESIDENCY_REFUSAL_STATES); gang election deferral denial reason `deferred_for_gang_prelaunch`.
- Tier loop events: `window-stalled` reason `prelaunch_waiting_for_room`; `prelaunch-group-short`, `prelaunch-group-overfull`, `prelaunch-handle-unparsed`, `prelaunch-acquisition-in-flight`, `prelaunch-turn-unknown`, `prelaunch-turn-blocked`.
- Holder name: `prelaunch-<unit16>-<tier digest12>-<phase digest12>` (hex digests only; dot-free).

## Rules for every worker
- Work ONLY in your own git worktree and branch (command in your task); never touch ~/prismabuild or another worktree; commit on your branch; do not push; do not open PRs.
- Match the surrounding code's style, comment density and idioms. No new dependencies. Do not restyle unrelated code.
- Tests: write them first as new files named tests/test_<...>_1594.py under your worktree. Do NOT run build, lint or tests yourself (the integrator runs everything through PrismaBuild pbtest --tag x86 afterward). Read code carefully instead; if you cannot be sure a behavior holds, say so in your final report rather than guessing.
- Undeclared behavior must be byte-identical: add tests that pin canonical bytes / return values for inputs with no declaration.
- Final report: files changed, the new public names, which design requirement each test covers, anything you could not verify, anything in the design you think is unimplementable as written (say so, do not silently deviate).

## Writing standard (D48, Rob 2026-10-07): ASD-STE100 Simplified Technical English
Write all text in ASD-STE100. This covers reports, commit messages, test docstrings and comments you add, and documents.
It does not cover code, identifiers, logs or quoted text. Keep instructions to 20 words or fewer. Keep descriptive sentences to 25 words or fewer.
Use the active voice and simple tenses. Use one term for one thing. Read rule 11 in ~/.omp/agent/RULES.md.

## Wave 1 result (merged on feat/prelaunch-resident-phase-20261006 at 1be75106fb)
Ledger: ResourceLedger.acquisitions_of, unparsed_acquisitions_of, transfer_count; PoolQueue.transfer_tier_reservation_count. transfer_count skips cpu/gpu metadata files.
Contract: storage_tiers.manifest_prelaunch_phases; residency_plan.prelaunch_phase_names, leads_for (all declared chunk movers), prelaunch_peak_gib, prelaunch_bound, gang_prelaunch_demand; pbgang.gang_prelaunch_refusal; pbrun refusal before seal.
Plan phase key: resident_before_launch (only present when declared).

## Wave 2 split
- PrelaunchAdmission (pool.py): residency_prelaunch_undeclared verdict; election deferral deferred_for_gang_prelaunch.
- PrelaunchGroups (new module src/prismabuild/prelaunch_group.py, plus a funding-validation change in pool.py if needed): receipts, census, rule table, begin/commit/roll back, split to movers, release, turn protocol. No tier_loop.py edits.
- PrelaunchTierLoop (later, after PrelaunchGroups): wire the module into tier_loop.py and residency_plan.window.

## Wave 2 results merged so far (feature branch, local)
Admission: PoolQueue._prelaunch_shape_verdict (state prelaunch_undeclared, denial residency_prelaunch_undeclared), PoolQueue._gang_prelaunch_blocker (denial deferred_for_gang_prelaunch). Both read the declared phases through residency_plan.filed_prelaunch_phases (cached on the filed-plan memo).
Groups: src/prismabuild/prelaunch_group.py: holder_name, file_intent, census, reconcile (returns ReconcileOutcome with authority), publish_chunk, release_unit, has_holdings, turn_ticket, current_turn, take_turn, record_reserved, finish_turn.

## Tier loop worker notes (audit findings from the integrator)
- Every `held_keys()` user must be checked against a `prelaunch-` holder. Known list: tools/fleet/stage_release.py 5032 (sweep) and 5197; tools/fleet/tier_loop.py 1168, 1289, 2581, 3936, 5753, 6403, 8715, 9265; src/prismabuild/pool.py 11909, 12636, 12733, 16908, 19732, 20472, 20579, 21736. Decide for each: skip, count, or leave. Say which in the report.
- stage_release.sweep (5032 ff): a prelaunch holder has no move receipt and no fragment owner. Today that files stage-receiptless-holder-retained and puts it in `unresolved` every pass; it is retained, never released. Skip prelaunch holders there. The prelaunch pass owns their release.
- tier_loop.landed_and_in_flight (9265): a holder with no move record counts as in-flight. Keep that: the group tokens are reserved room.
- The dangling-grant cleanup (tier_loop 7226) only sees `advance-` holders (window_credit.held_grants). Prelaunch holders need their own expected-set census from live units (design R1'', R2).
