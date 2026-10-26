# Progress supervision, #1707

[PR #2069](https://github.com/RobTand/prismabuild/pull/2069) contains the source repair. Source qualification is complete. Independent Principal Engineer review and merge remain pending. No runtime was published or service restarted.

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

## Recovery and current source

The earlier PR checkpoint included main `2ca082a0025ce1f0032eb13ed6f16f52b30efaf1` at source commit `93a10d8f4b3d99edc4789a336d3c20e51c57b2a1`. Its current Core bytes still match the verified capture. The original task worktree and its untracked helpers remain preserved; qualification uses `/home/rob/paperclip-worktrees/prismabuild/PRI-43-qualification`.

The fleet recovered. Core action `8eed6890445ed3285378ad45c79c01723aed77c5cb8dfb2eb549c80138f4cbbf` completed 73 cases after recovery on attempt 2. Attempt 1 lost its lease and was requeued under the immutable request. Both attempts' logs, the final terminal record, source bundle, native completion, CAS receipt and scope cleanup were audited. [qualification-correction.json](qualification-correction.json) records those checks and source comparisons. This is Core compatibility evidence on its retained snapshot, not final integration qualification.

All eight crash-era final-suite actions are excluded from merge and qualification evidence. They ran on dl380g10 within two minutes before its 2026-10-25 21:52Z panic. Their seven passes require replacement; the failure is a likely crash artifact. The correction supersedes evidence derived from those keys in the 22:00Z commit. [qualification-correction.json](qualification-correction.json) lists every excluded key, and [pending-actions.json](pending-actions.json) separates them from the recovered Core result.

The replacement used the same eight files in [prelaunch.json](prelaunch.json) on a new snapshot. It retained supervision, capability publication, semantic progress, budgets, ordinary sealing and the wire guard. [Fresh qualification](fresh-qualification.json) records its audited results. No excluded cached receipt supports the replacement.

## Requirement scope and gates

| Requirement | Target and source result | Deployment/workload result |
| --- | --- | --- |
| ID-08 | Runtime attribution; Core source capture/adoption validated. | Candidate not deployed. |
| SM-01 | Exact-attempt endings and cleanup; integrated deadline/D30 and existing withdrawal checks pass. | Whole lifecycle proof is not claimed. |
| INV-09 | Cumulative durable units; semantic watch retained and activity kept separate. | Full validation/workload proof remains owed. |
| PRG-03 | Typed waits and named stalls; existing wait-credit owner retained. | No staged-reader campaign was run. |
| LIVE-01 | Progress or bounded failure; source implementation and component checks are partial evidence. | No production, real GPU, daemon-load or NFS-fault qualification. |

[merge-acceptance.json](merge-acceptance.json) is a proposed record, not merge authorization. Independent Principal Engineer review and merge remain required. The Principal owns merge, issue closure and release. #2023 stays separate; a later Core change requires fresh identity evidence. CAS records prove recorded bytes and re-readability, not power-loss durability.

## Replacement qualification, 2026-10-26

The initial replacement client refused forwarded `-q` before publishing a shard. Its earlier running claim is retracted. Corrected `--pytest-args '[]'` preserves the eight-file selection and lets pbtest supply quiet output and cache suppression.

[Fresh qualification](fresh-qualification.json) records all eight current-head actions at `7a99877db04549daa7cd851e4ff44f2bb21c7c01`: 160 passed, one known wire-inventory fixture failure, zero skips. All eight terminal records, source bundles, both logs, native collected/finished/outcome populations and cleanup were audited. All seven successful actions have verified CAS receipts; the failing action has no success receipt. Each sealed snapshot has the exact declared source parent and no uncommitted path beyond PB's closure stamp. The excluded crash-era keys remain excluded.

Qualification base `58e9a4fa327fa15aeaf21a734baf0c23a191e32c` is integrated at qualified source `b530faad8d8218fe7e9bb805cc94f399a81dfda1`. `git rebase --rebase-merges origin/main` completed with the same source commit and tree. The PR branch also includes current main. No PR-owned supervision code or test changed after qualification.


## Current-main qualification

[Repeated qualification](repeated-qualification.json) records 95 distinct executed actions. All nine touched test files passed five times on x86 and five times on ARM. Rounds two and four used two pytest workers on each architecture. The repeated selection has 2,150 passes, no failures and no skips. Five integration files have 61 passes and the same unchanged wire-inventory fixture failure. The controller stopped on that integration failure and did not retry it.

The primary audit verified every terminal record, sealed request, clean source parent, source bundle, both log hashes and sizes, native collection/outcome/teardown populations, and complete scope cleanup. All 94 successful actions have verified CAS receipts and payloads. Failed action `04e0dd88fd4ebbfc0eb24c820f7a9da4bfdd86032dcaea391aa45b59be6ff3b3` has no success receipt. The manifest binds the full audit and frozen plan by SHA-256 and lists every key and receipt.

The launch used published `pbtest.py` from persistent user unit `pb-PRI-43-b530faad-repeats.service`, explicit 1200-second shard deadlines and default eight clients. Each shard declared two CPUs and four GiB, with one native thread per worker and no GPU. Maximum aggregate demand was 16 CPUs and 32 GiB. Architecture tags served the repository's repeated architecture gate.

The frozen plan and raw reports remain under `/mnt/shared/paperclip-runs/PRI-43/`. The full read-only audit is `repeated-audit-b530faad.json`. To repeat the audit, pass the manifest's action keys to `tests/compat/support/audit_compat_actions.py` with `--expect-head b530faad8d8218fe7e9bb805cc94f399a81dfda1 --require-clean`, a fresh `--out` and task-owned `--git-store`. `summarize_qualification.py` checks coverage, architecture, xdist, resource declarations and distinct executed keys against the frozen plan and full audit.

The known wire-inventory failure remains visible for Principal review. The fixture and assertion are unchanged. This qualification proves source components only. Deployment, production soak, real GPU behavior, Docker daemon loading, NFS faults and staged-read workload conformance remain unclaimed.

The earlier review integrated main `defc5bd8467f009c9ea7935678e31e282e46d7a1`. Its later #2309 merge changes seven canary-only paths. [Integration comparison](integration-comparison.json) verifies that every PR-owned product, test and compatibility blob equals qualified `b530faad`. No additional canary test execution is claimed here.


## Principal review 1 correction

Ordinary table and JSON status now show the cumulative semantic counter and optional unit. Activity samples keep their output and CPU meaning. The deterministic regression failed all four variants before the formatter repair in `92fc54d3cc7bf42c9496f56a53c0f92775194ee266f7b48d69c6d3eede643a38`; the corrected status file passed 28 tests in `2094f84e60e309d2a890c06d336b383c7227a4ceaf63f24b7e5d8de2bd84fc06`. Both raw audits are retained in the review evidence bundle.

[Counter qualification](counter-qualification.json) binds 54 successful actions and 1,500 passes, zero failures or skips. Each of the three changed test files passed five times per architecture, with two xdist rounds each. The final twelve-file integration scope passed 270 cases on x86 and 270 on ARM. The final frozen source is `5482fb5e2ee3887ca08eb0acded230a96361c3f8`, with complete tree equal to delivered `eca345a74ea4c7d5f83d6898d351e515d9cd31af`. The ordinary-status file and formatter equal the earlier ten-round source; the progress file equals its later ten-round source. The final integration uses the corrected census expectation.

The first correction integration had two stale display assertions, retained as failed evidence with no success receipts. The progress test now requires the committed count and unit in both row and table. The historical census test asserts the exact old and current strings, then compares every other value and its original read schedule. The original census oracle passed unchanged main and failed the candidate solely on the display; the revised oracle failed unchanged main. No fixture or unrelated assertion was removed.

The raw audits authenticate source bundles and Git parents, canonical requests, both logs, complete native outcome and teardown populations, cleanup, and CAS receipts and payloads. The three frozen plans declare two CPUs and four GiB per shard, one native thread per worker, zero GPUs, default eight clients, and 1200-second deadlines. Their documented aggregate upper bound during overlap was 20 CPUs and 44 GiB. No new tests were submitted during the final evidence continuation.

The three plans and raw audits are preserved in the Paperclip review bundle. To regenerate the committed manifest from them:

```bash
python3 docs/evidence/pb1707/summarize_counter_qualification.py \
  --evidence /mnt/shared/paperclip-runs/PRI-43 \
  --delivered-head eca345a74ea4c7d5f83d6898d351e515d9cd31af \
  --main 6185a6ee60f32cea85e4c45144493767a7477998 \
  --out /tmp/pb1707-counter-qualification.json
```

The bundle includes `audit-qualification-plan.py` to repeat the raw artifact audit for either follow-up plan. It imports the repository's read-only compatibility auditor, then checks the plan's resource, thread and architecture declarations. These commands inspect retained evidence and run no payloads.

Twenty of the 25 previously inspected blobs equal the Principal-authenticated qualification source. Census, pool, worker loop and the two corrected expectation files have fresh integrated component evidence. Earlier evidence retains its exact-source scope and all eight crash-era keys stay excluded. The historical wire-inventory failure remains separately recorded and was accepted by Principal review 1.

The branch includes main `954e14171565e849198cd0287abe24ff1ea7d076`. Later main `6185a6ee60f32cea85e4c45144493767a7477998` overlaps seven qualified paths, listed in the manifest. The merge train owns composition with current main under `merge-via-batch-20261026`; this continuation performs no additional rebase. The proposed acceptance record awaits independent Principal review 2. #1707 stays open until merged proof exists; PRI-69 remains downstream. Candidate deployment and workload proof remain absent.

The owner of the historical wire-inventory baseline, [#2296](https://github.com/RobTand/prismabuild/issues/2296), is now closed by [PR #2315](https://github.com/RobTand/prismabuild/pull/2315). Its earlier failure remains visible in retained source evidence. No new blocker or duplicate issue is created.
