# GPU container-runtime fence source evidence

PR #1982 implements the default-off #2039 slice of #1895. This record covers source qualification for Principal Engineer review. Live qualification and host enablement remain #1980; attempt-free requeue remains #1981.

The broker distinguishes a never-started container from a payload exit and independently proves CDI/NVIDIA host faults. Its host-local fence denies GPU claims by `gpu_container_runtime_unready` while CPU claims continue. A CDI event authorizes a pinned readiness probe; only successful current-token evidence clears the fence. PB never regenerates CDI.

## Before and after

Current-main baseline `9dce28460e76` failed `test_broker_fence_denies_gpu_claim_and_recovery_restores_it` on sparklina. PB action `d694613cb3ae577e691ec52bea925e88327a1648c7b3d6938a5316cd7d6664c0` claimed the GPU row while the broker mirror was latched:

```text
assert queue.claim(has_gpu=True)['action_key'] == cpu_key
AssertionError: 'aaaaaaaa...aaaa' != 'bbbbbbbb...bbbb'
1 failed in 0.13s
```

The unchanged regression passed on x86 action `b9f4343ec7cd4019359816dc9304189a5308d4a6ab45d21cf96ae47672c58be7` and ARM action `937276c501e87d0f4f0cc7685db50b0b978f1bbd314377bf09e0c7ffd57dc8bd`. It checks named sibling denials, CPU continuation and GPU admission after clearing.

| Selection | x86 / Python 3.14 | ARM / Python 3.12 |
| --- | ---: | ---: |
| Fence, recovery and repository guards, 17 files | 783 passed | 783 passed |
| Existing broker, scope, Docker, gang and measurement tests, 14 files | 448 passed | 448 passed |
| Total | 1,231 passed | 1,231 passed |

No failures or skips occurred in the fixed-source selections. Every shard reserved 2 CPUs and 4 GiB with two pytest workers, one native thread per worker and no GPU demand. PB selected the 31 actions per architecture and their placement.

The tested source is `c262c8cb5b6fb62343bce5d1aa4e0d17371a8ed2`. All four sealed snapshot bundles were hashed, imported and compared with that source. `src`, `tools` and `tests` match exactly. Each of the 62 successful terminal records has a matching immutable log and verified CAS claim, receipt and payload digest. PB MCP does not verify full worker attestation here. A fresh receipt-index read resolved an initial missing-receipt observation; no action was rerun for that observation.

The maintained `refresh_compat_field_sets.py` generated the original merge-conflict resolution: 572 to 575 entries, one removal and four additions. The final rebase described below also records two symbolic witnesses inherited from main. No golden bytes changed.

## Scope and records

[The merge acceptance record](issue1895_source_acceptance_2026-10-25.json) lists every action, immutable log, receipt digest, snapshot and per-requirement target/source/deployment status for SC-01, ID-02, ID-08 and ID-09. All staged-read targets and historical ledger axes remain unchanged.

The evidence archive attached to Paperclip PRI-29, attachment `0825ed25-38d1-469e-ab30-972d41bcbc8b`, contains exact PB command vectors, all five reports, their readback audits, the rerunnable audit script, snapshot comparisons and the maintained field-set report. The same files are retained under `/tmp/PRI-29` on the coordinator.

No runtime generation was published, service restarted, host enabled or live CDI file changed. This is injected source/component evidence. It claims no live repair, campaign completion, full-suite preservation or power-loss durability. Historical compatibility-support drift remains owned by PRI-30; this branch leaves that support unchanged.

## Final rebase onto current main

Main advanced to `b658e0912c` with #2284 during handoff. Merge-preserving rebase source `65dc42942f28607fd2a387c6aa9a38b29c208ce9` keeps the GPU production code and prior tests identical to the bulk-qualified source. Main adds only the test-runtime inventory tool, its tests and docs. The field scanner records two additional symbolic witnesses from that tool, a positional proc row and observed action keys. The reviewed refresh now contains 577 entries, one removal and six additions relative to main's 572. No new serialized record contract or golden bytes changed.

On this rebased source, the claim regression, wire inventory, duplication guards and new-main inventory tests passed **30 of 30 on x86 and 30 of 30 on ARM**, with no failures or skips. All eight additional terminal records, immutable logs and CAS claims/receipts/payload digests were verified. Both sealed snapshots match the rebased source in src/tools/tests. These 30 checks repeat a focused subset and add main's inventory tests; they are reported separately from the 1,231-test selections.

The final commands, audits, two snapshot comparisons and maintained inventory reports are attached to Paperclip PRI-29 as `2ab0fb6e-f041-4cb0-8f3e-44d34521c354`. Both evidence archives were downloaded and their stored byte counts and SHA-256 digests matched. Only evidence documents changed after this final source qualification.
