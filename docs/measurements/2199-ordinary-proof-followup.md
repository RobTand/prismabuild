# Ordinary admission residual repair for #2199 and #2273

Scope: source and component validation for PR #2221. Runtime publication,
service restart, deployment and campaign completion are outside this task.

## Reproductions

All commands used the published PrismaBuild clients at priority -10.
Tests used serial pytest, native thread limits of one, no GPU, and isolated
/tmp scratch. The residual checks declared 2 CPUs and 4 GiB per shard.

| Case | Action | Result before repair |
| --- | --- | --- |
| Ordinary admission behind a busy census fence on main 42f119c4f7 | e85f9ea8030d9989be5b657e4eec8893e38ed10dd460eff731d1930a49087d1a | 1 failed, 42 deselected; ordinary row was refused |
| Busy discovery followed by sibling omission | 35e6f10c837e949034509fc38bd53bd77536c05f7e7b7363fdb1434ff6f77316 | 1 failed, 1 passed; later row crossed a live fence |
| Ordinary observation lost between candidates | 81714c94251d6a8f705b027156d7ef3fbcf4c724b7e2a8502ff0693a2253bec7 | 2 failed; follower admitted after sidecar removal |
| Busy optional reader suppresses ordinary proof | 7641c2ce2aa2cfbcc9d7444dda33e1db41fb7353f9ce1643cdf48e0d374141a1 | 1 failed, 1 passed; safe ordinary admission refused |
| Foreign predecessor hides local successor | eeb59e5abe2f5fe91753157212b23e5de6d631a3d949de16ee313b23dad71e14 | 1 failed, 8 deselected; successor fence was absent |

The first ordinary-observation fixture's higher-priority row exceeded total
capacity and never reached the census. Its corrected control uses an existing
30 GiB holder and a 100 GiB request against 120 GiB total. The candidate reaches
its ordinary proof, then fails for available capacity. The higher-priority proof
must preserve the observed election for the lower-priority followers.

## Repair

The retained implementation and completed follow-ups at b8ea84e61b,
e3d94fc3fa and 61a986bf35 are reused. No archival captures or goldens were
recaptured. New changes normalize busy-discovery elections into selections and
keys before pass/shared reuse, separate ordinary source metadata from election
observations, and retain observations by their complete validated election identity, including publication generation, host, priority and election epoch.
Metadata is rechecked under H before observation; remembered local M keys are
acquired nonblocking before H. Ordinary proofs use the existing exact retirement
rule. Complete/shared fallback conservatively retains the strongest local fence
per key, so an older foreign election cannot hide a local successor.

Optional shared-source validation has its own failure boundary. Its refusal
allows the independently rechecked ordinary proof. Unsafe local state remains
outside that optional boundary. Caller acquisition exceptions remain outside the
setup handlers.

The fixtures control sidecar removal and lock ownership explicitly. They cover
live, missing, exactly retired, busy-M and retained-holder states, higher-priority
ordinary proofs, shared omission and subsequent complete-census fallback. They
make no wall-time performance claim.

## Validation status

The targeted final residual run passed 25 tests on the ARM fleet:

| Action | File | Result |
| --- | --- | --- |
| c284d2c1e82f1c35144cb5debc0dbed12d3550680606e4481c2f6d0e7095d936 | test_ordinary_observation_memory_2199.py | 9 passed |
| df6a7d1e5ed6c3477cb8b1cc2b2912eb032911f8fecd504b82d0db4f33672d0f | test_ordinary_shared_fallback_2199.py | 3 passed |
| 97d20f8de22901d2ea1bd6169c282adb8f4dfc127589ecf0da71183739d72679 | test_shared_census_safety_2199.py | 13 passed |

The initial 21-file ARM sweep passed all behavioral files. Its field-set pin
failed because the retained branch's generated inventory omitted two entries
added to main. The repository inventory export restored the missing main entries and all mandatory ARM guards passed. This is inventory maintenance, not golden adoption or a new wire contract.

The initial x86 sweep waited behind the deployed #2199 refusal. Rows
d477bfcd1901ffc008f9434c5b5c7dd1a6e3fd8522c2e4005a7eb5d7bdae50a2 and
342f8f80df9d7046bb4d6b6d32e71930d711b1545d53927227e69b2e31ebb19a
were READY with zero attempts. dl380g10's fresh worker offer coexisted with
measurement_census_unavailable / measurement census reader busy at
1792963978.8. No kernel lock census or live process descriptor inspection was
used. Runtime changes to unblock this lane remain the Principal Engineer's
release responsibility. Pending is not green.

One resubmission was refused because rebase changed the checkout during sealing.
It ran no tests. The separately owned source-generation audit CLI exited 2 with
source-review/unreviewed-builder findings; this does not qualify complete wire
audit coverage. The field-set export and pinned-field test are separate checks.

## Contract scope

SC-01's target keeps admission and placement in PrismaBuild. This source repair
implements that component in candidate_admission_census / locked_census and
exercises real fixture claims on workers. Deployment is not performed or claimed.
SC-02 movement authority and INV-07 settlement remain unchanged. No staged-read
requirement, deployment axis or campaign acceptance is declared complete here.
Principal review, x86 evidence and merge remain required for issue closure.

## Review validation gate

The earlier integrated 193-test run at 47f74a7259 is excluded from merge
acceptance. One action's canonical logs were restored by the submitter from
terminal-retained bytes. None of that run's ten actions is a merge evidence
source, including the repaired action. The original attachment remains history.
No further canonical logs, endings, or receipts will be repaired by this task.

The review continuation rebases the reviewed c40fbd3b9c tree onto current main
and starts fresh ARM and x86 admission/guard selections at the resulting clean
head. The report paths are `/mnt/shared/paperclip-runs/PRI-27/review2-arm.json`
and `/mnt/shared/paperclip-runs/PRI-27/review2-x86.json`. A report is acceptance
evidence only after its terminal status, unmodified canonical log hashes, and
CAS receipt and payload bindings are verified. The review handoff links the
reports and their exact checkout parent. Pending, timed-out, or unreadable
results remain gaps.

PRI-68's action 27ae66c919453d6d9a4c65326128bf3ae370ad63b0f1b4d8ebe9e11a6d7fe393
independently observed the deployed reader-busy refusal on dl380g10 at runtime
9dce28460e76-1792959923-b5904f02524e. Read-only observation on continuation
shows it has since reached a failed ending. This does not establish its payload
verdict or qualify the repaired source. The earlier x86 guard action
6257a9db9cbcd0a7f2af901224c558e152f8e90ddb7c115843cb3acb94ef3806 is now done.
Fresh validation still covers the rebased head.

The inline ordinary-proof I/O limitation has a tracking sub-issue,
[#2307](https://github.com/RobTand/prismabuild/issues/2307). It preserves the
boundary explicitly excluded by #2273 rather than widening this repair.
The two optional source-generation helpers also add work under H on full
censuses. They enable sibling reuse, but their hold cost is not assumed free.
A separate fixture-only paired experiment records that cost. Its evidence
cannot replace the admission or architecture guard results.

## Review continuation results

Fresh reports at published head `37b1597df1b8f230054c4bd8ed5eeee22c5d5757`
completed on ARM and x86. Each 19-file selection reported 300 passed, one
failed, and no skips. The sole failure on each architecture is
`test_primitive_digest_sites_only_shrink`, the existing main defect
[#2297](https://github.com/RobTand/prismabuild/issues/2297). Unchanged main
`2ca082a002` reproduced the same unexpected raw digest site in
`sealed_bundle_cache._reuse_bundle_cache`, action
`d989f936d5248afbdcdce8e66a2fb3961bc7d2e64f1aa762f5d353d114539ae4`.
Neither selection is green. This task does not weaken that guard or waive it.

Reports are retained under `/mnt/shared/paperclip-runs/PRI-27/`:

| Report | Result |
| --- | --- |
| review2-arm-37b1597df1.json | 300 passed, 1 failed |
| review2-x86-37b1597df1.json | 300 passed, 1 failed |
| review2-foreign-arm-37b1597df1.json | 5 passed |
| review2-foreign-x86-37b1597df1.json | 5 passed |

The separate foreign-load checks cover integration with merged #2267.
Including those five tests, each architecture has 305 passed and one failed.
Across these reports, 38 successful actions contain 298 passed tests per
architecture. Read-only verification checked their exact checkout parent,
terminal return code, canonical stdout and stderr lengths and SHA-256 hashes,
CAS claim and receipt binding, and payload hash. Attestation signatures were
not verified. No record, log, ending, receipt, or claim was repaired.
Failed guard actions have no successful CAS receipt and remain failures:
`8f9f0ad061ce98d50e65e643feca605e5ee4a21d49ffb0ef5c8f7c525a3cb362`
on ARM and
`e80efc9bcdd94b6664e78ca144eeed19df9c214a7e718dc6481b32894984027a`
on x86.

The current-main continuation rebases onto `58e9a4fa32`. Its new reports are
`review3-arm.json` and `review3-x86.json` in the same directory. Those reports
must establish the published head independently. Earlier results do not qualify
a later head. Principal review and green required checks remain merge gates.

## Full-census H-hold experiment

Action `dfb468c0fe0a64331afa2efbe585c4fd1a5340c0f3a9ab8e90233a2c52fe1869`
completed on sparklina as a PB measurement. It interleaved five before/after
pairs over private NFS fixture queues with 250 legacy READY rows and no
elections. Both arms used the same current `CensusReader`; the before arm used
`locked_census` from `91df700880`. The probe measured the interval inside H,
with no access to live queue records or kernel lock tables.

| Pair | Before H hold, seconds | After H hold, seconds |
| --- | ---: | ---: |
| 0 | 0.647583432 | 0.673510729 |
| 1 | 0.664075138 | 0.700007306 |
| 2 | 0.646095603 | 0.626455925 |
| 3 | 0.678177352 | 0.529363024 |
| 4 | 0.541280952 | 0.759402126 |

The effect is inconclusive. Paired changes range from -0.149 to +0.218 seconds,
and within-arm spread exceeds the apparent shift. The recorded one-minute
load average ranges from 0.957 to 1.121, with start/end load retained per arm.
This fixture does not establish live contention performance or zero helper
cost. The two source-generation helpers add owned child launches under H;
that cost remains a limitation. The probe and historical baseline are retained
with the evidence for rerun.

The earlier hand-restored log set remains excluded in full. Existing
[#2094](https://github.com/RobTand/prismabuild/issues/2094) tracks terminal
results that reference unavailable canonical logs. The retained evidence does
not identify which old action was repaired, so no action key is inferred.
Bounded ordinary-proof I/O remains tracked by native sub-issue #2307.
