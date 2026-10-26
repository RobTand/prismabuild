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

The initial x86 sweep is pending behind the deployed #2199 refusal. Rows
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

## Integrated result

The final integrated check on 47f74a7259 passed **193 tests in ten actions**,
with no skips. This includes ordinary admission, ordinary observation memory,
shared fallback, reused-census transitions, the slow-reader sweep, and all four
mandatory guards. The earlier broad sweep's other behavioral files passed;
its five failures were repaired and their complete files passed in this run.

A reused-complete-census counterexample was reproduced separately by action
75892dd7aa80528dadecb57119448c37b5b6bc9fcf359ba12303bbc22910690b
(1 failed, 9 deselected). Ordinary observations now join any already stored
census before fallback can reuse it. The updated memory file passes ten cases.

The broad check also caught retirement resurrection and a selection replacement
within one publication. Observations now distinguish the complete selection
identity. A matching complete-census retirement does not become a live election
again. The generation-check mutation control now asserts that the independent
ordinary observation still preserves the live fence when that check is removed.

All ten terminal records returned zero. Canonical stdout/stderr lengths and
SHA-256 digests, CAS claim addresses and digests, receipt bindings, payload
lengths and payload SHA-256 digests were independently checked. One action's
missing canonical log files were restored through RecordStore.file_attempt_log_at
using its complete terminal-retained bytes, after exact length and hash checks.
The client then reconciled all ten green shards. No ending or result was edited.
These checks do not verify worker attestation signatures.

| Action | Result |
| --- | --- |
| 48d44f87de72413c819cfc349bd80be33525c72d391083cc85c168f838caa1df | 43 passed in 9.11s |
| 5b71182eb9703061de4aa1155ddb7ca16bf4ea4ca45bd3579e15783fd1eb2649 | 10 passed in 0.81s |
| c71c5f8396d6b050374b48302a014fbe31477ca95b094d1420658dd446bd85a0 | 3 passed in 0.67s |
| 69ee2735f9e68f7a1e0d21dad104e8a9ccbe32e1fb52c46eb54a9b5aefb43d1b | 13 passed, 28 warnings in 3.96s |
| 49717bd392eebcf33d46c9ca626aaf34192260ae5b297e7b1bda2e513a9f48b7 | 10 passed in 13.92s |
| 3c0b656308f4b7bab55fb8e028867efc68e33ed599e2b1cf88e4f4c5fb814aa9 | 5 passed in 5.24s |
| 3afa40d39cbab06477f3f85255ac180e1b57d27c1dfb667fabd9c0e2cd9bb304 | 8 passed in 20.79s |
| 85e427e453173bfe55bf98c3ac5367cb26fa48f8e03e869975504c047efbddb4 | 92 passed in 0.91s |
| 464ecb334f693a2b6360a53b0211f1399c1575d49278ffc8a481fa171d24e25a | 8 passed in 9.86s |
| bc848d887345a5fdad44f7df8e5a48954546eb29ee3942ba0dfa855a41cd8373 | 1 passed in 0.09s |

The final rebase onto d0b92d267d adds main's sealed-bundle cache. Regenerate the field inventory after resolving its data-file conflict, then rerun the admission/guard selection against that integrated source. The x86 gate and Principal review remain pending.
