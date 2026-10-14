# PrismaBuild simplification review

Base: `6073c9d36555`, the fetched `origin/main`. The input is the
[root architecture review](../prismabuild-architecture-review-2026-10-13.html).
All 31 candidates and all 18 open pull requests were checked against source.

The useful direction is to give existing knowledge one owner. The proposal
sometimes goes further: it treats different authority boundaries as duplicate
implementations, or proposes a new service around an already complicated
coordinator. Those changes need a separate behavior decision. This branch
implements the concrete ownership moves and test repairs below.

The original dirty `/home/rob/prismabuild` checkout remains in place. Work
accumulates on `sol/refactor-simplify-prismabuild-20261014` in
`/home/rob/wt/prismabuild-refactor-20261014`. GPT-6.1-Sol workers used isolated
worktrees: xhigh effort for queue, trust and funding contracts; high effort
for PR inventory, test cleanup, roster work and independent review.

## Implemented ownership changes

- `kept_reads.py` owns filesystem trust, directory versions and retained census
  reads. Its 23 moved algorithms match the original syntax after owner renames.
  Produced-output memos default to fresh reads; tier cycles and blocked-origin
  observations enable retention through an explicit scope. The scope follows
  the actual synchronous callers and resets on exceptions.
- `manifest_layout.py` owns read order, phases, byte ranges, window frontiers
  and entry-aligned chunks. Direct movers retain support for unphased v1
  manifests. The public SDK retains its stricter phased reader. A comparison
  against the original Git revision checks 45 manifest cases and 1,575 operation
  results, including refusals and exception messages.
- `storage_tiers.py` supplies the prewarm role's seven formerly duplicated
  probes. The role passes its subprocess boundary explicitly. Its opt-in stage
  policy, capacity fallback and device-name presentation remain caller policy.
- `fleet_roster.py` moves from tools into the package. Supervisor and membership
  share canonical-host resolution; recorder evidence retains its stricter alias
  collision rule. Each caller still chooses its own generation or checkout
  path. Bootstrap hooks retain their own small parsing boundaries.
- `decomposition.py` owns record addressing, membership reads and durable child
  verification. Reporter reads stop after an invalid plan; queue diagnostics
  can observe both documents. Campaign closure retains its publication-only
  capture. Frozen plans, canonical document bytes and publication ordering keep
  their existing owners and contracts.
- `ProgressWatch` owns final stall sampling and record construction. The
  execution loop passes one clock for monotonic accounting and Unix reporting.
  Queue probes and termination ordering remain explicit in the execution loop.

- Funding records share identical write, generation-rotation and state-advance
  mechanics through an explicit window/output binding. Variant validators,
  evidence readers, retired-record refusal and lock order retain their existing
  contracts. Three zero-production-caller methods were removed; the publication
  refusal cases now exercise real sealed-request publication.

These are source changes on a local branch. Existing immutable generations and
sealed requests remain the deployment boundary. New source snapshots have new
identities. The v2 submission compatibility check now includes
`src/prismabuild/manifest_layout.py`, alongside core and the prewarm entrypoint.

## Candidate decisions

"Revise" identifies a useful concern whose proposed implementation is not
accepted as a mechanical refactor. It is separate from the completed code.

| Card | Decision | Reason or completed scope |
| --- | --- | --- |
| 01 | Revise | Acquisition has distinct uncommitted, committed, funded and persisted ownership phases. A universal abandon operation can release another attempt's capacity. |
| 02 | Revise | Share lazy peer observations later. Missing-capacity defaults, timers and charging differ intentionally; uniform fit would change placement. |
| 03 | Revise | Offer matching is advisory; live claim checks are authority. Share facts without treating those verdicts as interchangeable. |
| 04 | Revise | Holder classification and sample authority differ by consumer. A universal census would hide those proof boundaries. |
| 05 | Revise | Missing, unavailable and confirmed-absent reads have different authority. A generic queue store must preserve the distinctions. |
| 06 | Revise | Exact-generation waits differ from point-in-time status. MCP already projects `resolve_ending`; it is not a sixth independent selector. |
| 07 | Reject universal lifecycle | A key can have concurrent ready, terminal and historical attempts. One state per directory cannot represent those valid histories. |
| 08 | Implemented narrower owner | One execution clock and watch-owned stall accounting; no generic wait-adapter registry. |
| 09 | Revise | Staged-path authority belongs with complete publication and eviction transactions. Moving a census alone would expose the same lock coordination. |
| 10 | Revise | Tier holders include occupancy, rates, grants and windows. A generic release operation can decharge before physical egress. |
| 11 | Implemented | Flat retained-read owner, direct callers and explicit action-versus-fleet retention scope. |
| 12 | Implemented bounded step | Consolidate identical advancement mechanics and remove proven zero-caller APIs. Different validators, evidence meanings and retired fallbacks remain distinct. |
| 13 | Reject universal batch phase | Origin quota, staged retirement, consumer release and materialization succession are partly independent dimensions. Existing history already has validation. |
| 14 | Revise after integration train | Extract the common publication sequence, not a new giant submission service. Preserve pending safety fixes and review missing child gates separately. |
| 15 | Revise | Receipt parsing, selected-member verification and privileged authorization are different operations. Installed root clients need a deployment bridge. |
| 16 | Reject deletion | README and design explicitly retain the earlier adapter until Phase 3. Dagster remains a public optional adapter. The migration is paused. |
| 17 | Reject universal runner | Worker claim boundaries, failed tier cycles and HTTP serving have different stop semantics. A small shared role boundary remains a separate option. |
| 18 | Revise | Share marker spelling and posting first. Choosing which root and user roles must park changes the completion contract. |
| 19 | Reject runtime registry | Named capability derivations are useful; callbacks for arbitrary request fields add another framework and can enable disabled capabilities. |
| 20 | Reject rendezvous deletion | Public `run_local_action` and CLI reach it. Extracting manifest readers from core also requires a closure decision. |
| 21 | Revise after integration train | Sealing and filing can deepen, but attachment renewal and shared-tier scheduling have different rules. |
| 22 | Implemented | One manifest owner with distinct unphased mover and phased SDK entrypoints, plus old-versus-new comparison. |
| 23 | Implemented probes; retain stage | Seven shared probes. Retiring `--stage`, xattrs or temporary skips is a feature and lifecycle change. |
| 24 | Defer extraction | Scratch cleanup authority spans durable queue ownership, late finish and tombstones. Moving the file alone still changes measured profile identity. |
| 25 | Implemented | Decomposition owns addressing, capture and durable result verification while preserving each caller's read order. |
| 26 | Revise | Share evidence facts before verdict policy. Infrastructure failures must remain failures even when there are no failing test node IDs. |
| 27 | Revise | Use separately bounded census sections. An eager whole-fleet object would couple independent deadlines; denial-window differences need an explicit behavior fix. |
| 28 | Implemented narrower owner | Package roster facts and equivalent resolution, with caller-selected roots, parse policy and recorder authority. |
| 29 | Initial library migration implemented | Retained reads and roster libraries move into the package. Complete staged-path eviction and submission migration remain distinct transactions. |
| 30 | Implemented small test rigs | Controlled trust clocks and a shared terminal handshake replace copied waits. Existing admission and writer fixtures remain real production adapters. |
| 31 | Reject runtime catalogue | A fixture corpus can help, but new readers accepting old records does not prove old readers accept new writes. A registry alone adds no compatibility proof. |

## Test cleanup

The removed tests had an independently identified replacement or no production
behavior to protect:

- Two definitions of `test_parent_double_charge_gap_demo` included one shadowed
  function and one manually constructed obsolete ledger demonstration. Real
  fund/claim no-double-charge and crash cases remain.
- The duplicate readonly-submission import-guard check was removed; the same
  guard and actual readonly submission behavior remain exercised.
- The three parser-copy agreement tests and three chunk-order agreement cases
  were retired after the implementations shared one owner. Literal topology,
  invalid-manifest, byte-boundary and real stage/RAM mover cases remain.
- Two fixed three-second finisher delays became causal Event handshakes around
  the real terminal read and `queue.finish`.
- Trust tests control the coarse clock and observed metadata rather than wait
  for a favorable filesystem tick. Execution-boundary tests use controlled
  process steps and exact clock/credit assertions.
- Capacity fixtures now supply coherent synthetic disk/RAM measurements.
  Their low-headroom failures also reproduced on unchanged main. Real floor
  formulas, locks, ledgers, reclamation and live-holder assertions remain.
  Eight disk-headroom skips were removed without weakening enforcement.
- The mount-probe isolation check imports its module before the autouse guard,
  so it runs independently of which other files were collected.
- An existing export-owner fixture failed 26 cases on unchanged main because
  its global subprocess and sleep guards also blocked claim-time adaptive
  publication. The guards now apply to the owner-execution phase they test.

`tools/maintenance/test_inventory.py` reproduces the structural inventory. It
flags sleeps, reflexive assertions, source inspection and shadowed definitions
for review; it does not claim that every flag is a bad test. The scan reduced
detected sleep calls in test functions from 107 to 100 and shadowed definitions from one to zero.
The two reflexive-looking calls check repeated-operation stability and remain.
Parameterized
populations, corruption fixtures, policy guards and mutation tests were retained.

## Open pull requests

The inventory records exact heads and diffs. PR checks and body receipts were
not treated as independent approval or merge qualification. No PR was pushed,
closed or merged during this work.

| PRs | Decision | Required handling |
| --- | --- | --- |
| [1790](https://github.com/RobTand/prismabuild/pull/1790), [1779](https://github.com/RobTand/prismabuild/pull/1779), [1775](https://github.com/RobTand/prismabuild/pull/1775) | Superseded by main | Preserve any unique negative coverage, qualify current implementations, then close the obsolete harness alternatives. |
| [1764](https://github.com/RobTand/prismabuild/pull/1764) | Keep one integration vehicle | Keep it draft until exact-head independent approval and the integration test gate pass. |
| [1717](https://github.com/RobTand/prismabuild/pull/1717), [1752](https://github.com/RobTand/prismabuild/pull/1752), [1756](https://github.com/RobTand/prismabuild/pull/1756), [1678](https://github.com/RobTand/prismabuild/pull/1678), [1719](https://github.com/RobTand/prismabuild/pull/1719) | Members of 1764 | Their audited head OIDs are in the vehicle's retained commit list. Apply once through the train, then rebase affected refactor slices. |
| [1755](https://github.com/RobTand/prismabuild/pull/1755) | Preserve independent safety fix | It is not a member of 1764. Carry its child/fleet publication gates through a later submission-owner extraction and prove refusal before writes. |
| [1754](https://github.com/RobTand/prismabuild/pull/1754), [1802](https://github.com/RobTand/prismabuild/pull/1802) | Keep independent; rebase | Preserve GPU closure and class-scoped CPU behavior. These are behavior changes with fresh qualification obligations. |
| [1780](https://github.com/RobTand/prismabuild/pull/1780) | Revise guard | A large AST scanner and line-number allowlist do not prove timing safety. Its shrink-only comparison needs a trusted baseline. Keep this review inventory small and descriptive. |
| [1490](https://github.com/RobTand/prismabuild/pull/1490) | Reconcile dependencies, then rebase | Resolve actual ancestry and ownership of 1483/1487; remove guard-dependent skips only when their dependency is integrated. |
| [1750](https://github.com/RobTand/prismabuild/pull/1750) | Keep decision hold | Pending D38 audit policy requires the recorded human decision. A refactor cannot enact it. |
| [1683](https://github.com/RobTand/prismabuild/pull/1683) | Keep acceptance hold | Lifetime/phase fences remain opt-in and UNKNOWN until their exact acceptance and deployment requirements pass. |
| [1325](https://github.com/RobTand/prismabuild/pull/1325), [1326](https://github.com/RobTand/prismabuild/pull/1326) | Keep Phase 0 A/B hold | 1752 explicitly withdrew GO. Its partial proxy evidence does not authorize paced-export or output-window defaults. |

## Verification and decision trail

Every test and equivalence run used the published PrismaBuild entrypoints at
priority -10, with declared aggregate CPU/memory and bounded native threads.
Portable CPU work used eligible fleet placement. Successful results were checked
against terminal records, immutable logs and CAS receipts/payloads.

Six submissions were correctly refused when a baseline rewrite overlapped
checkout sealing. They published no actions and count as no test coverage.
Subsequent integrated checks use detached verification worktrees from candidate
commits. Historical failed or interrupted reports remain separate from current
passing evidence.

The complete 958-file run at `ffb353ab5bef` collected and reconciled 14,011
outcomes: 13,959 passed, 17 failed and 35 skipped. It remains recorded as a
failed run. Six failures came from a two-CPU fixture admitted with one CPU;
all 21 cases in that family passed when correctly admitted with two. The ten
filesystem-floor failures and one stalled-window failure reproduced on unchanged
main. Their fixtures mixed synthetic capacities with real scratch-disk free
space. The three subsequent repairs change only test files; production under
`src` and `tools` is identical to `ffb353ab5bef`.

The final candidate is `9d33cb6f2125`. A coherent 93-file recheck includes every
file in the three failed cohorts plus the probe, mount-latency and CPU-affinity
families. Each action reserves four CPUs for the floor fixture's four-process
contention peak, with one native thread per process. All six actions passed:
1,218 passed and two NFS capability skips across 1,220 collected cases.
The manifest comparison on the unchanged production code passed all 45 cases
and 1,575 operation results against `6073c9d36555`.

| Verified unit | Result |
| --- | --- |
| Shared storage probes | 239 passed |
| Membership handshakes and redundant-test removal | 96 passed |
| Manifest owner and source compatibility check | 616 passed |
| Package roster and caller policies | 743 passed |
| Decomposition membership and durable verification | 83 passed |
| Execution clock and stall accounting | 93 passed |
| Retained-read owner and controlled filesystem clocks | 583 passed, one filesystem-trust skip |
| Funding record write/rotation consolidation | 209 passed |
| Controlled filesystem-floor capacity | 37 passed, two NFS capability skips |
| Synthetic stalled-window supply | 23 passed |
| Probe isolation and real two-CPU affinity | 53 passed |

The final outcome reconciliation uses the actual immutable stdout records.
Every one of the 1,167 original node IDs in the failed cohorts was rechecked;
the extra three families bring the recheck to 1,220. All 17 original failures
now pass, and ten original skips now pass. Across the original 14,011-test
population, the result is **13,986 passed, 25 skipped, no unresolved failures**.
The six auxiliary passing subtest records have ordinary passing parent calls
and are not counted as six additional tests. This combines a complete run and
source-qualified rechecks; the original failed run is not relabeled green.

The remaining skips are 18 native `map_files` privilege checks, two NFS
capability cases, two opt-in SLURM smoke cases, one optional Dagster case
(package not installed), one opt-in claim-loop profile and one filesystem-version
trust case. No new skip or runtime admission exception was added.

The unit populations above overlap and must not be summed as distinct tests.
The retained-read skip is
`test_the_origin_retirement_tick_reads_each_scope_once.py::test_a_quiet_scope_parses_its_commitments_once`:
the worker filesystem cannot vouch for an info-only file version. The
conservative refusal remains; controlled positive trust cases pass.

The duplication baseline shrank from 14 to seven near-duplicate pairs and 93 to
83 same-name groups. Primitive digest sites remain at 185; no new exemption was
added.

Final integrated results are recorded in
[the verification evidence](refactor_evidence_2026-10-14.json) and
[the decision trail](refactor_decisions_2026-10-14.tsv).

Laziness Protocol rejected giant services and preferred deletion. Foundational
Thinking and Model the Domain kept binding and authority differences explicit.
Separate Before Serializing Shared State gave every writer its own worktree.
Sequence Verifiable Units and Prove It Works required baseline pins, independent
diff review, frozen candidate runs and the rerunnable manifest comparison.
Build the Lever produced the inventory and comparison scripts.

Attention: GPT-6.1-Sol independently reviewed the actual diffs, PR metadata,
terminal/CAS/log evidence and final outcome reconciliation. Earlier comment and
documentation findings are resolved; no correctness flag remains. The reviewer
used the requested model; no cross-family review was run.
