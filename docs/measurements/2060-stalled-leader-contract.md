# #2060 W1: stalled-leader evidence and joint-fit backfill boundary

## Disposition and identity

W1 delivers portable evidence and a proposed authorization contract, not a
production repair. The priority barrier is reproduced; the stronger claim of a
safely stalled leader starving safe backfill is **not established** (bounded
non-reproduction, not disproof). No existing repair or resolution of #1772 has
been verified. Preserve #1772 and P1. Existing W2 #2061 owns any subsequent
repair and any genuinely failing safe-backfill regression; do not merge that
red regression alone. #1953/#1942 are related infrastructure, not demonstrated
duplicates. Retired PrismaGraph routing is not a dependency.

Research inspected revision `e9e9d907721282f517e4e8feca40eda1aec79e5d`.
This delivery inspected checkout HEAD `82f97e8811d67940681eccdfba8515026ed0953d`.
No production source or acceptance tests are changed. The new
`tests/fixtures/stalled_leader_2060.json` selects declarations, event provenance,
unresolved categories and explicit synthetic assumptions from the unchanged
`tests/fixtures/stalled_leader_2060_observed.json`. The latter remains the
portable source inventory; its hashes identify recovered source bytes, not
independently verified CAS receipts. Tests need neither fleet reads nor
historical commits.

## What the executable probes establish

`tests/test_stalled_leader_2060_contract.py` invokes the real tier cycle in
temporary queues. The exact action keys and resource declarations are metadata:

- Leader `2bce5567a050816e9af0564c4b6f775564d668fefad4365f303aaa348ea6bd28`:
  priority 0, GPU1/CPU4/56 GiB consumer memory.
- Canary `cafd40ce45d818b7d894e8a6bd6cee0eff2a1ced6f496c257855cd7f05378507`:
  READY priority -10, CPU1/4 GiB consumer memory; historically ran on sparky.

Synthetic stage-token inputs are capacity 20, a never-started unused holding 5,
leader demand 18 and canary demand 3. They are not reconstructed history or RAM
requirements. Under those controlled inputs:

1. Leader receives `joint-fit-stall` (5 + 18 > 20). Canary receives
   `higher-priority-window-waiting` naming the leader before its own gate runs.
2. Canary's independent gate admits (5 + 3 <= 20). In an equivalent queue
   without the leader, its mover publishes. This proves a viable counterfactual,
   not safe interference with the leader's future rights.
3. Releasing only the test's own never-started unused holding allows leader
   publication. The resulting promise excludes the canary (18 + 3 > 20).
   This is a **protected feasible wait**, not a safe-backfill counterexample.
4. Unknown capacity refuses authorization.

No mover, consumer, foreign replay or GPU computation executes. Publication is
not execution. Neither idle hardware nor duration changes these conclusions.

## Historical linkage and limits

The unchanged observed fixture lists queue-relative paths, sizes and SHA-256s
for the leader's withdrawn record, canary's done record, filed plan, residency
map and event stream. In
`residency-events/cafd40ce45d818b7d894e8a6bd6cee0eff2a1ced6f496c257855cd7f05378507/dl380g10.jsonl`,
line 137 (source SHA-256
`744fa6646ac07f8a641cf67adeb299234b9c64ac59edf11749bd5f2bd4c25542`)
at unix `1791748669.3183753` explicitly records the canary's
`higher-priority-window-waiting` on tier `prismabuild-stage:dl380g10`, naming
that leader's `joint-fit-stall`. Its `need_gib` is 1 and its output note is
`output-scope-unenforced`. Each filed canary phase has stage demand 1 GiB;
substituting the 4-GiB consumer memory declaration would be wrong.

Leader withdrawal at `1791748674.449193` precedes canary stage publication at
line 139, `1791748680.9886343`. The canary later executes successfully on sparky
(return code 0; claimed `1791749267.541131`, finished `1791749442.9675417`).
Subsequent RAM stalls prevent attribution of its entire delay to the stage
barrier. These events establish the priority barrier, **not** the full leader
refusal arithmetic or historical safety of backfill.

All unresolved categories remain explicit in both fixtures:

| Category | Missing contemporaneous admission facts |
| --- | --- |
| held | Full holder ledger, owners, live fences, collectibility/evictability and valid release authority |
| queued | Complete READY mover/consumer census and unfunded versus funded demand |
| output | Owed unheld output windows and exact enforcement scope; the event explicitly says unenforced |
| protected_next | Aggregate next reservations, running/same-pass extras and leader current/next overlap |
| prelaunch | Group identities/order, ownership, peak, funding and reservations |
| sharing | Cross-consumer range identities, reachable ranges, deduplication and shared-fence bindings |
| commitment | Read footprints, growth, admitted set, static occupancy and exact commitment decision terms |
| leader_plan | Bound plan/publication identity, phase geometry and lifecycle state |

A missing canary prelaunch field is not proof that no other groups existed.
Capacity/free observations cannot reconstruct the missing snapshot;
capacity138/held40/growth21 is not a proved cause. Exact consumer-named lookup
negatives in the observed fixture are bounded current-filename negatives, not
historical absence. A limited recursive glob and timed-out literal search are
inconclusive. The previously denied audit was not retried or independently
verified. This delivery performs no recovery retry, credential request,
permission workaround or substitution of fresh execution for history.

## Proposed evidence-backed authorization boundary (requires review)

Existing owners are `src/prismabuild/tier_cycle.py::_protect_tier_advances` and
`src/prismabuild/window_credit.py`. The former rejects a lower-priority newcomer
before its gate and establishes the barrier only for eligible feasible transient
waits. The latter already includes current **plus next** in total admission,
distinguishes permanent oversize from transient obligations, defers unknown
capacity, and owns commitment, prelaunch remainder and shared-range joint-need
arithmetic. The priority-policy discussion in `docs/design.md` deliberately
protects feasible higher-priority waits while earlier promises drain. W1 does
not relax any of these rules.

A refusal that may clear as authorized earlier promises drain remains a
**protected feasible wait**. A transient reason string alone cannot establish
that a leader is safely stalled; neither can elapsed waiting, idle CPUs/GPUs,
consumer memory declarations, another scalar capacity comparison or removal of
the priority barrier. Permanent oversize and unknown evidence already have
separate handling and are not new bypass authority.

A positive stalled classification for W2 would require all of the following,
using the existing admission/accounting owners, not an independent capacity
oracle:

1. A coherent same-pass, publication-bound snapshot for the leader, candidate
   and every affected tier: exact READY/CLAIMED state, priority, plan/ranges,
   current-next overlapping lifetimes, capacity and complete terms in the table
   above. Unknown, stale, unreadable or incomplete state refuses authorization.
2. An identified **progress-independent obstruction**, supported by ownership,
   lifecycle and retirement/collectibility facts: why ordinary authorized
   draining cannot currently restore the leader's admissible next transition.
   Show the actual refusing predicate and its complete inputs, not guessed
   arithmetic or a delay threshold. If authorized draining can restore room,
   keep the feasible-wait barrier.
3. A non-interference proof for the candidate's entire overlapping lifetime,
   including queued publication/claim, current and protected next, output and
   retirement. Preserve every existing higher-priority future right and all
   held/queued/output obligations, prelaunch ownership and peak, sharing and
   commitment guarantees. Do not treat publication as reservation or promise
   eventual eviction of a pinned/noncollectible candidate. If the obstruction
   can clear during backfill, demonstrate that the leader's resulting rights
   remain funded and exercisable without waiting for unauthorized release.
4. Re-evaluation and actual funding through the existing tier single-writer
   ledger and fence-transfer lifecycle before publication. No free interval,
   double counting, plan rewrite, revocation of older promises or second
   accounting authority is permitted. Any changed binding or evidence loss
   invalidates the authorization; grant/claim verification remains intact.

These are necessary proof obligations, not an implemented new predicate or a
claim of general starvation freedom. This incident lacks the complete snapshot
needed even for classification. Therefore W1 withholds a safe-backfill
conclusion and a production patch. Only a fully evidenced failing reproducer
and reviewed non-interference contract should accompany W2 #2061.

## W2 #2061 addendum: positive remaining-movement witness for #1772

The W1 statements above describe its original delivery, not the following
production repair. W2 adopts W1's observations and protected feasible-wait
control without changing its fixtures or closing its evidence dependency
(#2125). `ConsumerWindow.remaining_movement_minimum_gib` now traverses the
remaining movement legs, retaining the named accepted/reading phase and
excluding declared prelaunch prefixes in the same way as the advance owner.
It measures each indivisible leg plus its immediate successor, including
chunk-to-chunk and cross-phase
frontiers. It does not sum whole chunked phases, cap the result at capacity,
or limit the traversal to the currently publishable horizon. Published legs
still consume capacity. `read_footprint()` is unchanged: its bounded
publication footprint can omit an impossible future transition.

Before publication, relief, or setting the newcomer priority barrier, the
cycle compares that minimum to authoritative minted tier capacity, not free
space or capacity minus selected holdings. A strictly larger minimum is a
positive progress-independent obstruction: draining other promises cannot
make the required overlap fit that capacity. The newcomer stays gated using
the existing oversize reason, and lower work still passes all its ordinary
admission/funding gates. This is not general backfill authorization for
joint-fit/commitment stalls. Unknown supply keeps its fail-closed gate;
feasible draining waits retain priority protection. Admitted consumers and
live tier holdings retain existing continuity and accounting. Exclusion is
cycle-local, so increased capacity restores ordinary consideration without
mutating plans or maintaining a blacklist.

The new acceptance fixture uses a pinned incumbent2, capacity5, a priority0
leader [1,1,4,6] (future indivisible6), or [1,1,4,4] (future overlap8), and a
READY priority-10 final window1. The leader's initial minimum2 and bounded
footprint4 conceal its future dead end. Tests exercise lower publication and
normal funded claim accounting, capacity increase and range-scoped next
funding, withdrawal, a live leader holding, unchanged sealed identity and
pinned incumbent. These are synthetic CPU-only queue/cycle counterexamples,
not reconstructions from GPU memory or manifest totals. The W1 20/5/18/3
control remains a protected feasible wait. Smaller unit checks additionally
cover chunk minima, accepted frontiers, absent movement roles and invalid
role refusal.

### Exact October 11 qualification disposition (unchanged)

The original audit directory listing was permission-denied, not an individual
object read or proof of evidence loss. W1 has no complete same-pass joint-fit
reconstruction. Every missing contemporaneous fact in the table above remains
unavailable: full held/fence/release ledger, queued funded/unfunded census,
output obligations and scope, protected-next/running/same-pass extras,
prelaunch groups and peaks, sharing identities and bindings, commitment
terms/admitted growth/static occupancy, and bound leader plan geometry and
lifecycle. Reported capacity138/held40/growth21 does not fill these gaps.
No denied recovery is repeated here.

Synthetic success does **not** prove the exact October 11 refusal cause or
safe historical backfill, successful live canary qualification, completion
of #2060/#2125 evidence work, or a causal link to October 8 stalls. The
existing unclaimed canary and its three verified siblings are retained; no
sibling replay, foreign work change, queue/priority/host workaround, sealed
unit rewrite, admission override, or change to the original 600-second
execution timeout is made. The 900-second observer expiry is not an execution
timeout. Independent fixed-head review and normal publication gates remain
with TrustLadder/the integrator; this package publishes no runtime.

## W2 verification

The unchanged #1772 acceptance file first reproduced all five intended
publication failures on x86 with:

```sh
python -m pytest tests/test_infeasible_future_leader_does_not_starve_window_1772.py -q --basetemp="$TMPDIR/pytest"
```

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"d92fcc4e31ada603c8c9effdd96028f80439e06217e65cd184eaf979f9fdf195","size":30658}},"operation":"620ea3c2c1b83c1b243f42f54933235ca110196674eb997b8a2bc61854754767","receipt":"d0ff2ef385badfc0e79a5c28cace46dd58129521748560b73d7033bc7a026d69"}]

Final acceptance, regression, pressure, chunk/frontier and repository guards:
**159 passed on x86 Python 3.14; 159 passed on ARM Python 3.12**. Each command
ran inside the admitted test environment, with no nested submission:

```sh
python -m pytest tests/test_infeasible_future_leader_does_not_starve_window_1772.py tests/test_stalled_leader_2060_contract.py tests/test_higher_priority_window_waits_for_existing_promises.py tests/test_claimed_window_does_not_set_newcomer_priority_barrier.py tests/test_the_nonfinal_window_fences_its_advance.py tests/test_advance_needs_names_its_fence_target.py tests/test_a_ready_claim_is_tier_pressure.py tests/test_a_waiter_with_no_pressure_says_why_1627.py tests/test_wire_inventory.py tests/test_fleet_tool_flags_have_help.py tests/test_duplication_baseline.py tests/test_design_doc_line_references.py -q --basetemp="$TMPDIR/pytest"
```

x86:

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"84b43109f2605be54cd7600605dc2dbf023eed85e58d175a775d04d7c6023daf","size":3822}},"operation":"df3a0aeab2e6970fc784b12ca0b4211d39de930f32670e96fb0e68975f4fb333","receipt":"7b76df475d133049870f7a3a97daea287e3551a5c6170a67ae0c0e54d0f07f0f"}]

ARM:

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"ce0f3f98de7dbb33c841b90b2672be0176d7f133771aa99e73f49f073fe20fb7","size":3823}},"operation":"4ace190a8de9b51c87512f07e1023cbea91609fcd64a803c9483da7bfc2bc890","receipt":"a265fe26513f196ea06c0f769dde60155cf8978840d4d5e40d3bf8c224da1854"}]

No record-field contract or compatibility golden was changed. The acceptance
file was not modified. No full-suite, live canary, historical replay or
publication qualification is claimed.

## W1 verification (retained)

Plain CPU-only command inside the supplied admitted actions, with no nested
submission or fleet retrieval:

```sh
python -m pytest tests/test_stalled_leader_2060_contract.py tests/test_wire_inventory.py tests/test_fleet_tool_flags_have_help.py tests/test_duplication_baseline.py tests/test_design_doc_line_references.py -q --basetemp="$TMPDIR/pytest"
```

Research receipts below are retained verbatim from
`tests/fixtures/stalled_leader_2060_evidence.md`: each reported **1 failed,
113 passed**, solely because the W1 deliverable was absent; the four probes and
four mandatory guards passed. An initial test-author API error was corrected
before those final research runs; it was not a scheduler failure.

### Delivery checks

The command above completed with **114 passed, exit 0 on both arms**: x86
Python 3.14.4 (83.75 seconds) and aarch64 Python 3.12.3 (31.12 seconds).
All four behavioral probes, the portable deliverable assertion and all four
mandatory guards pass. This validates W1 delivery, not historical safety or
production repair. No requested check was unavailable. Retained stdout was
verified through the test artifact reader. The only subsequent change is this
verification narrative and receipts.

#### Delivery x86

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"f771e56928d24361f0cf8afda719d11a4b33fd3d5a8dea38b828966d8104e823","size":1991}},"operation":"5bbcfedd9443071268a4904674c9668a430b893cf8c5d040677615313d65353f","receipt":"3eee7b0e47af4e0e9c4cb91b35e22fd0c30733b9b9365bc9241f9b3904225c92"}]

#### Delivery ARM

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"d10e8cc2bf0ff74be3afc719ec4059758c6a0fc79a499b2dd5ad7f93a0d81620","size":1993}},"operation":"636a404cb879d6dc503f975a91df192cac4ac9b1c2c00bb82b7d9cc229d83908","receipt":"ac2cd9698d0b91538c4d87093aeb91fcd42529147d7771e71c1bad9d791e1c13"}]

### Research x86 / Python 3.14

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"7e24bc2d811c710b2858463bd1fe27a3c90402fd456e136f7a68003071134abb","size":3569}},"operation":"e4094bfb6c16cea97b2a971de037bf8dacc16d302d315f7b3e611c20fbbaa7e2","receipt":"b5fe3a1c9c2a0cac8a2ed6c9203cea2f9eec83cf83ccbc7a36a636401185e7fe"}]

### Research ARM / Python 3.12

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"d1b729f4ea24d97c592e6e1c692a8ef1602ec6095a79b16c66b1481df512fa1d","size":3573}},"operation":"16f666937871108a1585308f0c560bd52b1cff370a38949ab826bf6dbb04d5d2","receipt":"28ac46fa34f93faed67b74607f6f6f5a6ac3a563f3b4a98eebc34de21dbc9101"}]
