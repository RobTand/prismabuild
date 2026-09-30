# Local scratch placement validation

Status: the rebased code candidate passed 865 component tests without skips;
10-file syntax compilation is verified. This record-only update requires its
own final candidate check before PR acceptance. Full #1182 remains open.

This opt-in source slice seals an explicit directional traffic contract,
qualifies a configured profile through existing producer/CAS evidence, and
uses comparable measured service cost at the existing worker claim boundary.
It does not infer traffic from reserved capacity, benchmark during admission,
flip ordinary placement defaults, or establish representative hardware rates.

## Recorded behavioral evidence

The following are admitted CPU component tests. Synthetic competing-host
observations do not establish physical spill execution or deployed support.
Terminal records, immutable attempts, logs, reconciliation and cleanup were
verified; successful test receipts and results were also checked against CAS.
No listed test was skipped.

| Check | PB action key | Result and scope |
| --- | --- | --- |
| Original slower-host claim | `f6bdc4e615df1ceb43b78194cd89e63f9e67d76f2ce93a653ef42442c4cbc1b6` | 1 failed, 34 deselected. Old code actually claimed the slower host, including its 186 GiB reservation. Not a physical 186 GiB spill. |
| First independent review boundaries | `ca0c266938940955f8597605f55e6c62364862d1fee1dc1daaed3e02a7d95ed4` | 13 failed, 71 passed; 84 reconciled. Seven attributable witnesses and six fixture/contract failures, not 13 production bugs. |
| Replacement publication | `1b5a7a10d56c7270fe7a950f25c18cb16357dfafa23051a34a409bd72f6131de` | 1 failed, 83 deselected. Corrected setup reached the actual reentrant publication/reservation boundary before the desired unwind failed. |
| Earlier 84-case candidate | `6dc7d24c8c4f8f39a2447174a1aa9555bb44031fae695812a09a8a7dca966cb7` | 84 passed. Historical evidence only; predates subsequent retained-review findings. |
| Retained peer/shape boundaries | `ad54754f25028c9ec068dc6fa9a0abc0d2b211a3f435ba2bef674f6720118001` | 5 failed, 2 passed. Nested device shapes, unknown GPU refusal authority and original peer-price age failures; moderate-depth decoder controls passed. |
| Actual fairness-decorated refusal | `3d38f3b3118de83cc1dee5c75f5582b43ffe847b55eb61a27f9ee3ff47e6cad4` | 1 failed. Actual fresh, attributed, exact-generation `adaptive_gpu_refused_starved` was recorded; the slower host still yielded. |
| Maximum JSON input boundary | `7affeecc8d1d61ee3aeac7f0dd9f1b089a21259825e8d0247a2b94e4e699ce97` | 2 failed, 2 reconciled, 1.70 s, no skips. Actual 65,534-byte decoder `RecursionError` escaped receipted-result and configuration observation paths. |
| Earlier integrated candidate | `faf0ce80cb42cdb17f6b1a4efca220281e1d915a757a54a43fabb1b9a72934d6` | 2 failed, 377 passed; 379 reconciled, 27.64 s, no skips. Not integrated GREEN. |
| Core owner and qualification gate | `016a8be6aa216a7fec0dfb917edbad54e47b547c6492fd2757b582c528eb0245` | 398 passed; 398 collected/ran/outcomes reconciled, 55.20 s, no failures or skips. Includes 45 new byte/source/isolated-producer controls, 170 Core tests, guards and upstream namespace controls. |
| Current-main component suite | `afd8e00256bb7075d4c895e54b8438924cb3d025a52dbeebf7437f00736d2a4f` | 683 passed across 20 files; 683 collected/ran/outcomes reconciled, 82.16 s, no failures or skips. Includes placement, ordinary sealing, controllers, preemption, workers, source qualification and guards. |
| Rebased code candidate | `985118ee9668876a09c234ef229a64fd3e5585509e6d1a79ae534ef7bcf4d098` | 865 passed across 23 files; complete reconciliation, 63.14 s, no failures or skips. Includes upstream declaration-registration, boundary and SDK controls on commit `78b3a4efd4b4d2861441714cbd5b1259ab6f9cee`. |
| Syntax compilation | `5ca134b05522b7dc76ea326e86e8c4a7f9315a54c5b66843a6b0f593b0a27ca1` | `COMPILE_OK 10 files`; terminal, immutable attempt, log hashes, CAS result and released cleanup verified. Syntax only, not throughput or deployment. |

The six initial fixture/contract failures were corrected without weakening the
primary claim or reviewed outcomes. They were a noncanonical infinity fixture,
a same-thread reentrant lock mistaken for contention, lost keyword forwarding,
two stricter-than-existing ordinary CPU policy assumptions, and an omitted
zero-valued free kind before the publication witness. Ordinary reserved
preferred-CPU work retains its existing stale-sample behavior; this feature does
not invent a new CPU refusal policy.

## Current source boundaries

- Malformed observed shapes fail closed before nested lookup.
- Only the closed, sample-backed CPU/GPU policy subset can end a peer yield.
  Unknown or early/transient refusals are not scheduling authority.
- Existing fairness display suffixes preserve the underlying class. Unknown
  prefixes or suffixes do not. Identity and original sample-age checks remain.
- Peer offer, artifact, workload, root identity and fit are revalidated after
  peer I/O. Original measured price age is checked after local boundary reads.
- Actual local controllers, fallback, borrowing, preemption, token acquisition
  and generation rollback remain authoritative. #1360 lifetime work is separate.

Independent retained review found no issue with the scoped decorated-reason
repair. Source-only review is not current-source GREEN or runtime adoption.

## Decoder boundary is reproduced

Action `7343dc92bd4f4b0cd7812bfd193a98efe8839ef4b358be86225cf2e17416aee4`
completed with an empty result and receipt-only stdout. The helper invoked the
importable outcome-recorder library without calling its `main` API: **zero
tests ran**. Receipt integrity does not make this a decoder or test pass.
The admitted helper was corrected to call the existing API. Its next action
`7affeecc8d1d61ee3aeac7f0dd9f1b089a21259825e8d0247a2b94e4e699ce97`
ran both maximum-input controls on sparklina with Python 3.12.3. The real
65,534-byte JSON input had SHA-256
`f2093b7d76319a37630b90b291e452a76b9f1587923e5b06e20e717f11fdc64c`;
the direct decoder classified `RecursionError` before both real observation
paths failed with:

```text
RecursionError: maximum recursion depth exceeded while decoding a JSON array from a unicode string
```

The receipted-result path passed actual producer/receipt/CAS checks before
`ProfileInputs._load` decoding failed. The configuration path reached the real
worker observation helper. These are attributable runtime failures, unlike the
two earlier moderate-depth controls that passed.

The candidate adds `RecursionError` only to the existing configuration and
per-root observation containment tuples. Invalid configuration returns no
profiles; a broken reference removes older good profiles for the same root.
No parser mock, recursion-limit change, guessed depth threshold, producer
qualification weakening, or shared Core decoder change is introduced.
Retained independent producer review found no issue with the scoped repair.
Both maximum-input controls passed in the later integrated candidate, but that
candidate is not GREEN because two other controls failed.

## Integrated control correction

Both remaining failures reached the desired unknown-GPU refusal behavior:
the slower host did not claim. Their later `ready_unspent` helper incorrectly
required that no shared fairness-pass file exist after the faster host's real
controller refusal. The unchanged ordinary refusal path calls `record_pass`
before publishing that denial; this behavior is present in the branch base.

Only those two secondary controls were corrected. They now require the real
preexisting pass file, capture its bytes, and require exact equality after the
slower measured yield. The desired no-claim assertion, unchanged READY record,
leases, complete token availability, unchanged genuine denial and later winning
host/receipt/finish controls remain. No controller or fairness policy changed.
Those assertions passed in focused admitted validation and in the later
683-case current-main suite. The earlier failed result remains recorded above.

## Producer source-closure integration

The recorder now reuses Core's raw source-byte SHA-256 and canonical BODY+LF
writer, plus one fixed-purpose Core SHAKE-256 block recipe accepting only a
positive integer length. The scratch finite-positive predicate delegates to
Core and discards its normalized return, retaining accepted original types.
The isolated bootstrap loads only sibling Core from the scratch module's own
file; the CLI uses the already-bound writer. There is no injectable namespace,
payload callable, environment-selected source, `sys.path` mutation or fallback.
Core's self-source capture occurs during startup, outside I/O timing.

The producer closure is stdlib plus the exact recorder, scratch module and
Core source bytes. All three are checked against installed and materialized
sources and participate in the verification cache. Recipe Core's snapshot
identity remains separate from receipt worker-launcher Core; producer/consumer
interpreter bytes need not match. Even unrelated Core edits invalidate existing
profiles and require explicit remeasurement. This dependency transition is not
an old security defect: the previous recorder did not execute Core recipes.

New `tests/test_local_scratch_core_recipes.py` controls cover independent byte
recipes, predicate/type preservation, actual tiny isolated execution and
three-source qualification, harmless changed-Core snapshot refusal, and an
installed-Core change after cached verification invalidating all same-root
references. After a source-only review corrected three snapshot-versus-stamp
test preconditions, all 45 new controls passed in the admitted gates above.
The tests materialize the real sealed checkout snapshot; they do not fabricate
members in the single-stamp code closure. Independent producer and placement
reviews found no issues with the corrected source and controls. These receipts
prove component behavior and byte qualification, not representative throughput,
physical spill execution or deployment.

## Remaining gates

The rebased code component suite and syntax compilation are verified. This
documentation-only record update still needs the final candidate check and
issue-linked PR acceptance. Real paired profile production,
representative spill measurements, application uptake, pressure policy and
runtime adoption remain separate work. Do not close full #1182 or publish a
speedup, physical spill, deployment or globally optimal placement claim from
these component receipts.

## Artifacts and commands

The coordinator artifacts are under
`/home/rob/tmp/claude-campaign-20260926/tmp/p2p3/prismabuild/`:

- `1182-red.json`, `1182-review-red.json`, `1182-generation-red.json`, and their
  verification records
- `1182-green-first.json` and `1182-green-first-verification.json`
- `1182-retained-red.json` and `1182-retained-red-verification.json`
- `1182-decorated-red.json` and `1182-decorated-red-verification.json`
- `1182-decoder-noop-verification.json`, corrected `1182-decoder-boundary.sh`,
  `1182-decoder-red.json`, and `1182-decoder-red-verification.json`
- `1182-green-integrated.sh`, its failed result and verification (15 files)
- `1182-spending-control.json` and its verification (two focused controls passed)
- `1182-core-owner-gate.json` and its verification (398 passed)
- `1182-green-current-main.json` and its verification (683 passed)
- `1182-green-refreshed.json` and its verification (865 passed, before this record-only update)
- `1182-compile.sh` and `1182-compile-verification.json` (10 files; verified)
- `1182-producer-independent-review.md` and `1182-placement-independent-review.md`

Each helper checks the serve-window sentinel before a client launch. Parent
submissions are serial, priority -10, explicitly time-bounded, GB10 `pb-cpu`,
with declared aggregate resources, bounded native threads and assigned affinity.
