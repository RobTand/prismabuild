# #2060 W1 research evidence and handoff

Base inspected: `e9e9d907721282f517e4e8feca40eda1aec79e5d`.
Only acceptance tests and test-support evidence were added. No production,
foreign replay, GPU work, live queue mutation, publication or policy change.

## Scope of the observation

`test_stalled_leader_2060_contract.py` runs the production tier cycle against a
temporary queue and discovered temporary stage root. The exact reported action
keys and CPU/GPU/memory declarations are metadata only. The 20-token capacity,
5-token receipt-less unspent holder, 18-token leader and 3-token canary staging
requirements are deliberately synthetic, unrelated to consumer RAM declarations.
The fixture is a protected feasible wait, NOT evidence that bypass is safe.

Observed on both arms:

- Leader receives `joint-fit-stall`; canary receives the named
  `higher-priority-window-waiting` barrier before its own `gate_newcomer` runs.
- The pure canary gate would admit with the same controlled held obligations.
- In an otherwise equivalent queue without the leader, canary mover publishes.
- Returning only the test's never-started, unspent holder permits leader
  publication, and its new 18-token promise still excludes the 3-token canary.
- Unknown capacity refuses admission.
- No consumer or mover executes; no GPU allocation is requested by the test.

The research's final expected failure is **missing portable W1 evidence/contract
artifact**, not a demonstrated scheduler defect. Creating
`tests/fixtures/stalled_leader_2060.json` with the schema checked by the acceptance
test and writing the reviewed report can satisfy W1 without changing production.
A green W1 does not resolve #1772 or prove historical safe backfill. Do not turn
this protected-wait example into a red backfill assertion. Any genuine safely
stalled red reproducer belongs with #2061/W2, not a standalone red merge.

## Historical recovery

Portable selected observations and source hashes are in
`stalled_leader_2060_observed.json`. Source paths there are queue-relative;
checks never access the fleet or retrieve historical commits.

A read-only bounded recovery found the leader's terminal withdrawn declaration,
the canary's executed terminal declaration, filed plan, map and consumer events.
Event line 137 names the exact leader and barrier. Leader withdrawal precedes
stage publication, and the canary eventually executes successfully on sparky.
The canary also suffered RAM-tier stalls after the leader was withdrawn, so its
entire delay cannot be attributed solely to the stage barrier.

The source-file hashes identify bytes read, not independently verified CAS
receipts. Full historical admission inputs were NOT recovered. The prior denied
audit path was not retried, independently verified or accessed by workaround.
Exact missing terms and bounded negative/inconclusive searches are recorded in
the observed fixture. No assumption of zero is justified for unresolved held,
queued/output, protected-next, prelaunch ownership/peak, sharing or commitment
terms. Capacity/free observations are not a same-pass admission snapshot.

## Contract/report work remaining

Write an evidence-backed report (suggested location:
`docs/measurements/2060-stalled-leader-contract.md`) separating proved mechanism,
synthetic assumptions, historical observations, and unresolved arithmetic. Use
`window_credit` and `_protect_tier_advances` as the admission/accounting owners.
The existing current-plus-next total-capacity test already exists; simply adding
another scalar comparison or removing the barrier is not a contract.

Distinguish feasible waits while existing promises drain from a stalled leader
using only known, complete admission/accounting/ownership facts. If those facts
do not prove a progress-independent obstruction and non-interference with future
higher-priority rights, refuse to authorize bypass. Preserve unknown-state
refusal, current/next, prelaunch and output rights, commitment and sharing terms.
State precisely what additional evidence is needed to apply any proposed stalled
classification to this incident. Do not infer permission from elapsed wait,
CPU/GPU idleness, the 56/4-GiB consumer declarations, or capacity138/held40/growth21.

No historical safe-backfill counterexample has been proved in this research.
Report bounded non-reproduction of that stronger claim, not verified repair or
resolution. No speculative patch is authorized. Existing work: #2061 is W2 under
#1772 (P1); #1953/#1942 are related infrastructure, not duplicate repair owners.

## Executed checks

All checks were plain `python -m pytest` inside admitted actions, with no nested
submission. Command on both arms:

```sh
python -m pytest tests/test_stalled_leader_2060_contract.py tests/test_wire_inventory.py tests/test_fleet_tool_flags_have_help.py tests/test_duplication_baseline.py tests/test_design_doc_line_references.py -q --basetemp="$TMPDIR/pytest"
```

An initial run exposed a test-author mistake (`TierLedger.give` does not exist).
That was corrected to the explicit `release_unspent(...,
authority=Unspent.NEVER_STARTED)` operation for the test's own unused grant. That
initial failure was not a scheduler result. Final test-source runs below each
reported **1 failed, 113 passed**. The only failure is the explicit assertion that
the W1 evidence/contract artifact has not yet been delivered; the four behavioral
probes and all four mandatory guards passed. Retained stdout was inspected.

### x86 / Python 3.14

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"7e24bc2d811c710b2858463bd1fe27a3c90402fd456e136f7a68003071134abb","size":3569}},"operation":"e4094bfb6c16cea97b2a971de037bf8dacc16d302d315f7b3e611c20fbbaa7e2","receipt":"b5fe3a1c9c2a0cac8a2ed6c9203cea2f9eec83cf83ccbc7a36a636401185e7fe"}]

### ARM / Python 3.12 (additional portability check)

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"d1b729f4ea24d97c592e6e1c692a8ef1602ec6095a79b16c66b1481df512fa1d","size":3573}},"operation":"16f666937871108a1585308f0c560bd52b1cff370a38949ab826bf6dbb04d5d2","receipt":"28ac46fa34f93faed67b74607f6f6f5a6ac3a563f3b4a98eebc34de21dbc9101"}]
