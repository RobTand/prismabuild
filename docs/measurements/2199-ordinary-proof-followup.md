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
observations, and retain observations by action key and publication generation.
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
added to main. Regenerate with the repository inventory tool and rerun the guards;
this is inventory maintenance, not golden adoption or a new wire contract.

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
