# Preemption review and integration — 2026-09-07

A foreground admission denial can interrupt one background holder only when its released tokens close the gap, the action explicitly permits retries, a known non-measurement action has remaining launch budget, and the exact claim still owns the reservation. Recorded failures remain in their generation; interrupted launches consume the existing attempt budget. Withdrawal and replacement publication share the transition lock.

The original proposal could stop a replacement claim, repeatedly restart work without consuming its budget, expose an intermediate cancellation, and follow an unrelated later generation. Demonstrated regressions precede separate fixes. Waiters now follow exact immutable withdrawal lineage. Existing immutable attempt evidence retains the handoff context and allows recovery when a later same-status generation overwrites the mutable terminal summary. Recovery validates history and log digests, writes no queue pointer, and returns the actual archived attempt path.

## Verification

Reviewer head `f1ec3c8a4744d88668f6509cd1ce21d36a2c12cd`: PB action `c23b2a49f00692c4cbaac2e0dfcea6c0de08595e9d7c45bc434789029335bf1b`, 3138 passed, 3 skipped, 4 subtests passed. The four same-status overwrite regressions failed before the fix (`c4707e564efbd533df0ce965e54e99360fed42421a46a7624093f7a162d214f5`); 56 targeted tests then passed (`66751acc9f874d9b6571acf83d5178a76c30d46fed087cdd1130b645b71bf68c`).

Integrated with main `276706d72` at `3555a5024eeee8f8ca80b125affd288b2d555c30`:

- Full suite: `56bc4bf39bb6f48b02b37e7b2a943d53d67a62f7ec2a0b773c84c009f7dd6057`, **3194 passed, 3 skipped, 4 subtests passed**, 33.66 seconds. Portable DL380 CPU12/memory12 GiB, xdist12, native threads one, CPU-only. 219 recorded Python fork deprecation warnings; no test failures. Skips: optional Dagster and two opt-in SLURM container smokes.
- Compile: `37b426f73be1c1e2de5ce28c77af855ac42f4cc1104a5199f30aa983d389f284`, pool.py/pbrun.py/pbstatus.py, exit zero on Sparklina, portable CPU1/memory1 GiB.
- Full-suite canonical receipt `aa300719922c0fa7c8f2f6cb239dc570ce757435f4309effe6c7fa53c5b6d466`; payload `21645a12bcbdb5e51d1454c4436d1a246cbf73cc64093fd208930dde57bfa2a9`, 6901 bytes. Snapshot bundle `eb1455064f10a8afc051f750610233e129fcc7aa2c18e10c3bc08eb2ad77ca58` was rehashed, imported and compared with the integration commit; only the generated closure stamp differs.

Root independently checked terminal and attempt exits, log hashes, source bundles, canonical CAS receipts/payloads, and completed resource cleanup. Audit files: `/home/rob/tmp/pb365-root-overwrite-audit.json`, `/home/rob/tmp/pb365-root-integration-audit.json`. Full reviewer evidence remains under `/home/rob/tmp/pr365-review/`, including negative and invalid-collection dispositions. No live model run was interrupted for validation; this establishes private-queue correctness, not a cross-host preemption performance measurement.

Two separate test-fixture fixes accompany the review: the cadence regression reads the actual local CPU authority, and a campaign race permits a concurrent worker to publish before submit returns while still checking its receipt. Baseline failures and corrected passes are retained in the reviewer report.
