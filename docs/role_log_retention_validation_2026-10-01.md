# Role diagnostic retention validation, 2026-10-01

This record covers the source slice for #1396. It does not certify deployed retention or recovery of a pressured root disk.

## Contract

A regular claim-owning supervisor cycle maintains only its fixed storage, tiers and metrics diagnostic leaves. At 64 MiB or more, a successful nonconcurrent pass retains the newest 8 MiB of raw bytes on the same inode. Current role ownership and inherited writable-append FD identity must remain proven. New leaves use mode0600 even under umask002; existing group/world-writable leaves remain retention-ineligible. Owned nonsymlink mode0775 directories are supported.

This is periodic hysteresis, not an all-times disk roof. Inter-tick output, blocked supervision, unsafe or vanished writers, concurrent append loss, mixed lines, partial-copy failure and path races retain the limits documented in `docs/design.md`. Action PIPE/CAS streams, sidecars, worker/supervisor/custom logs and archives are not retention targets.

## Actual RED

On unchanged source `9b9854f9ad2ea5c24be7eef17cc5e3c5cde31334`, action `0efd3fcfd19a0da36256c82fb1d1a4a85f833747caa92bcd4de72791475b7ea4` ran on dl380g10:

-6 failed,22 deselected,0 skipped in1.02s;6 collected/ran/outcomes, reconciliation problems empty.
-All six real owning-cycle/Popen cases reached the desired64MiB/64MiB+1 versus8MiB assertion. Actual private-child cleanup exited zero; no import, collection or fixture-setup substitute.
-Authoritative terminal, immutable attempt, exact log hashes and scope release were verified in campaign `1396-red-verification.json`. A successful CAS receipt is absent as expected for a failed action.

The earlier resource-window guard and unqualified system-Python launcher attempts ran no tests. They are not RED evidence.

## Actual candidate GREEN

Action `a5ea358eca522a69b14b75525fcce5c8d5a891e6f807b037389fbda010d27a21`, dl380g10:

-180 passed,0 failed,0 skipped in18.83s across15 files;180 collected/ran/outcomes, no collection issues, extra phases, missing files or reconciliation problems.
-32 actual new controls, including the original28 and no-proven-PID refusal, owning-once exclusion, real new-leaf creation under umask002, and positive retention in an owned0775 directory. Case counts are recorded in `1396-green-case-counts.json`.
-Related publish/role-lock/health/refusal/spool-declaration/elastic-worker/stop/alias/signal/reap/reexec/shape/held-claim tests passed unchanged.
-Terminal, immutable attempt, exact log hashes, canonical v3 CAS receipt/result and released scope were independently verified in `1396-green-verification.json`.

Both actions used published PrismaBuild entrypoints, priority-10, explicit execution timeouts, one shard, CPU2/memory4GiB, native threads one and PB-assigned affinity. No local test, Docker, GPU or live-file maintenance was used. Ordinary portable CPU admission from dl380g10 is authorized during mirrored serving windows; this is not timing or GPU authorization.

## Source-only and static checks

Retained independent reviewer `eb82432f-9e54-4e23-987b-c7fc6df61f29` read the complete candidate and controls and found no issues. This is a source-review gate, not execution or rollout evidence.

Active primary LSP found five diagnostics. Three supervisor expressions were independently mapped unchanged to base (`declared`, `verdict[2]`, nullable `__doc__.splitlines()`). Two new-test import-resolution findings are the editor environment's absent pytest/dynamic fleet path; the admitted180-case execution proves those imports in the selected runtime. `1396-lsp-base-comparison.json` records that distinction. This is not an all-clean type-check claim.

Admitted two-file compilation passed on dl380g10: action `a7199ddc6d87178b209fc598be6f4c10da47bcecbd59fd32b02cadad04e82065`, witness `COMPILE_OK 2 files`, result19bytes/SHA-256 `876f6f6c79127b504fa99403738f7d6b4eebf55d32c60344e21eb6ce77e4e872`, receiptSHA-256 `782efd59821f7e11563044fcec3c508c6f5efcf207a5c3146ba6249bdc1be53a`. The read-only verifier checked authoritative terminal/immutable request/exact logs/canonical CAS/result/cleanup in `1396-compile-verification.json`. Syntax qualification is not broader runtime or static proof.

Final fresh-main validation remains pending at this record's creation. Earlier candidate passes must not be promoted to a later changed source head without a final gate.

## Existing deployment boundary

The passive snapshot `1396-passive-log-metadata-dl-move.json` found all three dl380g10 legacy leaves mode0664 and the owned directory0775. The named leaves were absent on both Sparks at that instant; absence is not bounded-log or deployment proof. No file or permission was changed.

The source does not chmod, replace or compact an unsafe existing leaf. Separately authorized coordinator permission qualification/migration and normal runtime adoption remain required, recorded in `1396-LEGACY-PERMISSION-DECISION.md`. Neither source rollout nor this fixture proves root-disk recovery or full #1396 closure.
