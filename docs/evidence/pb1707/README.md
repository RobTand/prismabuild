# Progress supervision, #1707

[PR #2069](https://github.com/RobTand/prismabuild/pull/2069) contains the source repair. It remains draft because final fleet qualification is blocked. No runtime was published or service restarted.

An ordinary payload now has no implicit total-duration deadline. Output growth or aggregate containment CPU renews a quiet allowance. Its default is 7200 seconds, the former worker ceiling, clamped by the executing worker. This preserves the former allowance for quiet daemon image loading. `--stall-s` seals an override and requires a worker offering `activity-stall-v1`. GPU activity, I/O counters and arbitrary progress-file changes do not renew this watch.

Semantic `ProgressWatch` keeps durable cumulative counters, phase grants and verified dependency-wait credits. D30, withdrawal and explicit deadlines retain precedence. Heartbeats record age, phase, counters and quiet time in bounded files for the exact claim. These log writes grant no activity credit.

## Verified evidence before the outage

| Check | Actual result |
| --- | --- |
| Original fake-clock reproduction | 2 failures on unchanged source, action `c72fdb88403279799cc0bf9bb0121a5f1dc93cd8efb8fdbe847361be36afc4d6` |
| Capability and running-log regressions before repair | 4 expected failures across `bc5b09b3dfc348010ba474d69eaaf4a369eaaaeb5ad5a7d4cf75f45e0d1c20cc` and `4efae6912a69b4730d93fbfc9160e11879c3645be4e92c4c5729cdb1c5870029` |
| Focused ARM checks after repair | 72 passed, zero failures/skips |
| 144-file x86 nearby suite | 2,543 passed; one baseline guard failure |
| Same guard on unchanged main `0f71df9164` | 7 passed; the same `test_closed_field_sets_remain_pinned` failure, action `e20895da5295c81770c8d1cca04dd19227a4e37441da191cd6e99c1e2f31d17c` |
| Core source capture and strict adoption | Six current and 12 historical records independently reproduced and checked in both consumer directions; five exported files authenticated before integration |
| Initial Core/guard suite | 304 passed; 41 archival-environment refusals because scratch differed from its retained qualification |

The focused green actions are `9f9f5df619064cfba4ab07cb30a429090b19ee5a5a9d39667d2e6c7f0ad8219d`, `3efc60d73c7f39cb8e4dc63c574b08f4a4b510ad0046d53f9c6b6e5b3597417f`, `0d048f35f829158680cff9447cfda56acd9d0f41a25911f9077cabdd92a817e1`, and `aaa3f4424c9d3cd70196d150eef9a75b2d3fd0bd780d55e4294f0bb8f64429dd`.

Terminal records, both log hashes and sizes, source bundles, cleanup and successful CAS receipts were independently audited. Raw reports and primary audits are retained under `/mnt/shared/paperclip-evidence/PRI-43/`; local client logs and continuation scripts are under `/tmp/pri-43-evidence/` on celestia. The baseline wire-inventory fixture is not weakened here. Counts above are separate runs and include repeated cases.

## Core identity

Capture `6903cfc9b9e83576ff855587c2e6aff96cd2b7dbe88a50781d67f2d46ce4e242` and strict adoption `4620a052170bb6b620cfe72c0b93d462fc98902cebab74b16b518c47cd57ae89` bind the source. Integration preserves 133 previous historical/control files and all unselected current records. No golden is edited by hand.

Core is 306962 bytes with SHA-256 `e6b9e79b40c45863d78c437237ea7388371a27842087f88dc8e9272217a2749e`. The admitted capture uses `record_compat.py --generate PATH --expect-machine x86_64 --kind core-receipt --kind core-attestation --kind core-runtime`; adoption uses `refresh_compat_runtime.py --profile runtime` inside an admitted action. Only Core source identity and its dependent digests change in the six current records.

## Source delivery after continuation

The integrated source is now on the existing PR branch at `a79f780f72cdf8183097cfd934dddb5758504c22`, with current main `aee1a4542078e92839a1de5c2d6d049c9dbb0499` as an ancestor. Its complete Git tree equals rebased local checkpoint `74c54dc892d2b9e4a5f3eccade3ebfd55d0c08a5`. All three compatibility conflicts are resolved. [source-delivery.json](source-delivery.json) records that comparison and the unchanged Core identity.

Delivery used the available GitHub connector because the CLI lacks a managed push capability. The original local series remains preserved. This resolves source delivery only. The PR remains draft; final qualification, independent Principal Opus review and merge remain owed. No additional tests or duplicate actions were submitted during this continuation because SSH/NFS still refuse connections.

## Blocker and resumption

At 2026-10-25T21:58:39Z, dl380g10 / `192.168.1.107` refused TCP 22 and 2049. SSH and the Hindsight tunnel refused. Coordinator processes waited in `rpc_wait_bit_killable`, and the final PB clients repeatedly reported bounded outcome-read timeouts. No successful outcome is inferred for the pending runs.

[pending-actions.json](pending-actions.json) retains the exact action keys. The full Core unit was resubmitted with its qualified `/home/rob/pbt17` environment and a 1200-second budget, based on the retained prior 582-second runtime. The final rebased suite covers the supervision tests, registry fence, semantic progress, budget, ordinary sealing and wire guard. Its final outcome remains unobserved.

After host recovery, read the existing terminal records, logs and CAS receipts before any further submission. Reconcile interruption/retry lineage through the published clients. Do not modify sealed requests, resubmit uncertain work, release held resources, or bypass admission. Finish the local handoff generator only after the final results and audits exist. Rebase before requesting review and verify any changed integration path.

## Requirement scope and gates

| Requirement | Target and source result | Deployment/workload result |
| --- | --- | --- |
| ID-08 | Runtime attribution; Core source capture/adoption validated. | Candidate not deployed. |
| SM-01 | Exact-attempt endings and cleanup; integrated deadline/D30 and existing withdrawal checks pass. | Whole lifecycle proof is not claimed. |
| INV-09 | Cumulative durable units; semantic watch retained and activity kept separate. | Full validation/workload proof remains owed. |
| PRG-03 | Typed waits and named stalls; existing wait-credit owner retained. | No staged-reader campaign was run. |
| LIVE-01 | Progress or bounded failure; source implementation and component checks are partial evidence. | No production, real GPU, daemon-load or NFS-fault qualification. |

[merge-acceptance.json](merge-acceptance.json) is a proposed record, not merge authorization. Final fleet qualification and Principal Opus review remain required. The Principal owns merge, issue closure and release. #2023 stays separate; a later Core change requires fresh identity evidence. CAS records prove recorded bytes and re-readability, not power-loss durability.
