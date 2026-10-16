# Refactor integration follow-up

This records source changes after the complete configured-suite checkpoint
`8dacf78510df19bdb746f581b5b30a49e630625c`. It does not replace the frozen
111-requirement audits or complete the 31-card brief.

C26 is assembled with the accepted accumulation at behavioral source
`6f4de87cca5c4dff914bed053669aad9d011006e`. The documentation-only successor
`7aaac3e0c9f97afc78c0ab779974836a5e3a3922` explains the co-published tools'
rollout requirement.

| Requirement | Resulting ownership |
| --- | --- |
| c26.r01, shared shard verdict | `tools/fleet/pbtest_outcomes.py::judge_shards` owns per-shard and complete-population judgment. It receives an independent requested-file roster and preserves known failures beside unknown coverage. |
| c26.r02, structured refusal and progress | Preflight refusal is structured before pytest starts. Progress binds the invocation UUID, entrypoint and actual generation; human output remains presentation. |
| c26.r03, migrated consumers | `pbtest`, selected-generation `pbmergeq` and the verified-receipt shape gate use the same owner. The old reducers and prose classifiers are removed. |

The final isolated C26 source `4a38ae279618bfa418d599606d8b033996c5a1aa`
passed 14,713 cases with 23 skips across 1,003 files and twelve actions. Six
auxiliary subtests passed separately. The skips retain their actual capability,
NFS-availability and opt-in causes. Root and Claude independently checked those
primary chains and source bundles. Root also checked the per-node preservation
and skip causes, and the three retained native positive, foreign-record and
duplicate-record receipt chains.

The assembled source passed 2,177 cases across the complete 121-file consumer
closure, plus eight cases in the complete duplication module. These nine
actions had no skips or failures. Native collection, outcomes and completed
teardown agree, and the file assignments are disjoint. The earlier C26 closure
had 2,218 cases. The difference is exactly 43 retired rendezvous cases and the
two accepted replacement consumer proofs in `tests/test_core.py`; all other
nodes remain. All canonical retirement changes survive the merge. The sole
additional test-helper change removes a trailing blank line with an identical
AST.

Evidence and rerunnable data-only audits are under
`/mnt/shared/prismabuild-agent-evidence-rob/refactor-20261016/active/c26-root-review/`.
In particular, `integrated-primary/verified.json`, `integrated-population.json`
and `reconciliation.json` identify the actual records, population and source.
The isolated source's failed controls and interrupted attempts remain preserved
under the adjacent `c26-recovery/` directory.

The legacy SLURM/Dagster adapters, initial-miss rendezvous and experimental
prewarm stage are retired after their accepted checks. `slurm_lane` and the
established staging behavior remain. B14's final-object census found zero
objects and bytes; no final object was deleted and no role release was claimed.
Its assembled closure passed 488 cases, while the wider inherited
filesystem-dependent fixture failure remains recorded for correction.

C01's wave-4 source `b1c51e3e160b7672f54b40a85c49b5c6afc197e7` separately
passed its complete 381-file gate, with 6,464 passes and 19 skips. The completed
driver and its later assembled qualification are recorded below. Native C05
source `26d0c7532c6f` remains unqualified: a historical digest fixture called a
removed Core API, and the stopped run left 253 files unexecuted. Corrected
native N2 at `2c9886c0ec262a59b46fb54eac07cf5c9ae14f78` passed 1,066 cases with
18 existing capability skips across 26 files. Its twenty primary chains were
independently verified. N2 remains an isolated C05 step; the full RecordStore
and explicit-trust migration continues separately.

## Completed claim driver

C01's completed owned source is `dea353d156b487d2116fdb5e11cfb845c6447bee`.
The driver has 104 physical lines and 65 AST statements, with fourteen explicit
queue-phase protocols. Candidate T, canonical sorted M/H acquisition,
post-H accounting, tier reservation, rename, commit and publication retain
their original boundaries. `_ClaimReservation` remains the resource owner.
Its implementation and the four canonical census owners retain their accepted
ASTs. Publication stays outside the existing uncertain-acquisition catch.

That owned source passed its full 1,003-file suite: 14,723 passed, 23 skipped
and six separate auxiliary subtests. The skips retain their actual capability,
NFS and opt-in causes. This is the owned source's result, not a full-suite
claim for the later accumulation.

Assembly at `ad7497a9bcb1e6e94f5e99b2a07c5ba5af5f2849` preserves the canonical
retirements and C26. Two review corrections remove unused passive field reads
and bind two carried-denial fixtures explicitly to their claiming host. Its
complete 479-file C01, C26 and B14 consumer union has 7,700 unique cases:
7,681 passed, 19 existing capability skips and six separate auxiliary subtests.
All eight success chains, source bundles, native populations and skip causes
were independently verified. Three initially failed cohorts were rerun in
full after disk-capacity recovery, with identical file and node sets. Their
original failed records remain preserved; none supplies final pass credit.

Real sibling-held transition histories were added in
`test_candidate_scan_order.py`, `test_drain_boundary_order.py`,
and `test_a_refused_gpu_row_keeps_the_drain_boundary.py`.
`test_gang_demand_fence_claims_1579.py` now files real bounded carry denials
instead of patching the carry decoder. This does not describe every busy-row
test. `test_a_gpu_refused_row_is_not_overtaken_by_gpu_rows.py` and
`test_claim_denials.py` retain their earlier fixtures outside this C01 change.

## Completed roster ownership and integration corrections

C28 migrates all thirteen readers to the standard-library-only `Roster` and
`Box` owner. Each capture retains its path, exact bytes, digest and document.
Publication validates new typed fields before consumers rely on them.
Historical membership, rollback and role-log reads bind actual bytes to each
qualified generation's manifest. Canonical lookup, recorder ambiguity,
installer all-match role checks and historical rename unions retain their
separate policies. Publication's permissive alias-shape policy is unchanged.

The closed roster source `c23c26fbdb910d61432beb0f267c14c4a1ffac38` passed
2,521 cases. All 2,477 baseline nodes are mapped, with one deliberate
replacement for the now-refused `boxes: []` publication. All 3,578 existing
assertions remain. The supervisor review correction at
`e9817a681df2a39ee38517c035dfb60f502f08af` keeps the previous valid shape when
typed loop fields are malformed. Three new real consumer controls first
failed, then passed in its complete 109-file, 2,524-case qualification.

The first combined gate exposed two architecture failures that the isolated
roster population had omitted. The correction at
`dc91c4c3b064650ca34319848fe1015accb65ae0` shares ordered JSON object construction
through the existing digest owner across the roster, Core's strict decoder and
RAM mount installer. Each keeps its exception class, message and other parsing
policies. The maintenance generator's internal function is now
`roster_references`. The duplication baseline and exception registry are
unchanged. All 417 focused consumer and architecture cases passed. A production
mutant that discards duplicate keys fails five actual cases across all three
consumers. The installer now tests an otherwise valid duplicate policy, with
an exact refusal and no effects, replacing a non-discriminating invalid-schema
parameter.

Two separate fixture repairs preserve the production behavior. The synthetic
RAM-window fixture uses the existing supply-reader boundary instead of the
host's actual free disk space. Its controlled zero-supply reproduction fails
the original assertion; all nine corrected cases and all 51 assertions remain.
The promotion-chunk fixture now creates private stage and RAM roots, checks
actual discovered paths before registration and requires registered ownership.
Its five original cases and assertions remain. The guarded old fixture fails
before reaching the live roots. The bounded eleven-file census found no other
fixture that needed the RAM supply correction.

The final combined behavioral source is
`c7c663e00dda210fd4cb3ac48a45717ed1d6eda1`. Its complete 184-file consumer union
passed 4,477 cases with eighteen existing privileged `map_files` identity
skips. The 4,495 observed nodes exactly match the source-derived roster,
including the explicit installer-case replacement. All eight architecture
cases passed. Every one of the six action chains has verified terminal cleanup,
source, complete logs, request, producer, executable, runtime and CAS result.
Collection, outcomes and finished teardown agree. This is a scoped combined
qualification; the final repository suite remains open.

Evidence and rerunnable source, population and receipt audits are under
`/mnt/shared/prismabuild-agent-evidence-rob/refactor-20261016/active/` in
`c01-root-review/`, `c28-root-review/` and `c01-c28-root-integration/`.
The earlier failures and mutations remain preserved. Acceptance now requires
passing current-source architecture evidence whenever a production Python
unit changes. C03, C05 and C31 continue in isolated worktrees, alongside the
remaining original requirements.

## Completed role kit boundary

C17 is complete under D05's accepted shared-kit design. Each role keeps its
loop, idle waits, completion boundary and exit choices. The kit owns shared stop
requests, singleton fencing, receipt observation, drift stamps and parking.
`generation_receipt` owns identity comparison. At behavioral source
`69667698ed99fa80b5fd7d4a5732d4db4aa9c063`, the kit also names the shared
deferral value as `ROLE_DEFERRED_EXIT = 75`. All fourteen role return literals
use that owner. Singleton refusal stays 3; worker one-shot drift stays 0.

The original c17.r01 list includes sleep and exits. D05 assigns the choice and
timing to each role while sharing the operations and exit value. Worker paths
that use `time.sleep` still finish that interval after a stop request. This is
the explicit engineering reading used for completion; the frozen requirement
wording and historical assessments remain intact.

The ordinary package and role import closure contains eight package modules,
each depending only on the standard library and those siblings. Core and
residency-map exports load on explicit access. The existing boundary tests build
and install a wheel offline, remove the broad owners from one installation,
and check actual module paths and hashes in isolated child interpreters. They
exercise real signals, cross-process singleton contention, receipt refusals,
exact drift bytes and idempotent park markers. Complete-package tests preserve
public object identity and actual progress operations.

The baseline at `1f4000f8131492e68ed992edc5deb2b09ea4751e` passed 708 cases
across 49 whole files. The final source passed all 737 cases across 51 whole
files and four successful actions, with no skips. The additional two complete
tier-exit consumer files contain 29 existing native nodes. No test or assertion
changed. Resolving the new constant gives each role module's exact prior AST;
the kit adds only the integer constant. All eight architecture cases passed.
Source bundles, executable and producer identities, logs, CAS receipts, native
collection, outcomes and completed teardown were independently checked. This
is the affected C17 population, not a new full-repository qualification.

Rerunnable source and population checks are under
`/mnt/shared/prismabuild-agent-evidence-rob/refactor-20261016/active/c17-root-review/`.
`final-combined-primary.json`, `final-population.json`, `final-source-audit.json`
and `exit-source-equivalence.json` bind that result to its source. The privileged
drain profiles and ARM/NFS proof remain separate work.

The final integrated suite, remaining source requirements, native ARM/NFS and
paired-host drain proofs, compatibility corpus and deployment checks remain
open. No runtime publication or GitHub PR-state change is included.
