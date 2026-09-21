# Produced-output read: dated evidence addendum (2026-09-21)

This file records the evidence behind the 2026-09-21 update to the staged-read
contract. The normative text is `staged_read_contract_2026-09-20.md` section 7
and the rows PO-01–PO-07, BUD-01–BUD-03, DUR-01, and ACC-07 in
`staged_read_requirements_2026-09-20.json`. This addendum proves no runtime
behavior, changes no endgame or worker join/resign semantics, and is not a
substitute for a scoped acceptance record.

Observations below were read on 2026-09-21 around 04:07 UTC unless a row says
otherwise.

## Deployment and source axes

- Published PB runtime: `/mnt/shared/prismabuild-fleet/repo` points at
  `runtime-generations/054d7f0b66c8-1789960683-23b36e18a4f6`, source
  `054d7f0b66c8960a74b4447e138c48ee51bde7a5`. The snapshot
  `/home/rob/tmp/cache-takeover-20260920/status-before783activation.json`
  records three live nodes on that runtime commit and one stale node on an
  older one.
- PB782 merged at `1d0b9c3e270f560954db6c1e13c4e12f610746dc`; the deployed
  generation above was staged from source commit
  `054d7f0b66c8960a74b4447e138c48ee51bde7a5`, whose parent is that merge.
  Source, receipt, and CAS verification:
  `/home/rob/tmp/cache-takeover-20260920/pb782-root-cas-source-verification.json`.
  That deployment is evidence for the PB782 repair only, not for any PO, BUD,
  or DUR row.
- PB783 merged at `f5b6bba0358a684f96a44251b21c77f0aabf1784` (PR783, issue780).
  The acceptance record
  `/home/rob/tmp/cache-takeover-20260920/pb783-root-acceptance.json` records
  the merge with deploy pending at acceptance. The stage action
  `a9afda9560e7b95d8a7d89564d8bab50ef0af408997e6c2bb3c3fb259373c0b6` produced
  generation `43b790cce88c-1789962578-9e60f8c7ea49` (receipt
  `de1d2f7807eede98e626dffd2734d122dfc61982e8fb732f14af9f55a8533fc6`, result
  CAS blob
  `c5dbf053593fb88332ac161c946b8e32f76a043a44a4177ce13bc7d9836c3afd`, log
  `/home/rob/tmp/cache-takeover-20260920/runtime-783-stage.log`); at 04:07 UTC
  it was staged, and its 725 publication members were root-verified against
  source `43b790cce88c8fc5a70610b5d3270185c829960f` with parent `f5b6bba0358`
  (`pb783-publication-root-verification.json`,
  `2026-09-21T04:07:13.444848+00:00`). The published symlink then switched to
  that generation (observed at 04:08 UTC), and role convergence at
  `2026-09-21T04:09:13.850343+00:00` shows all three live nodes on that source
  with the rollout idle (`pb783-runtime-convergence.json`). Merged, staged,
  and activated are three separate facts, and this activation proves the lower
  cross-root egress edge only.
- PB781 outer-caller lock ordering: the branch
  `/home/rob/tmp/pb-produced-output-current-20260921` at head
  `73a39d498b6a7e1568169baca18c3a7e1e1b638d` now has source that validates
  and selects the exact materialization under the output-prefix ownership
  lock, releases that lock before `stage_release.evict`, and reacquires it to
  revalidate the exact selection before filing the retirement record
  (`src/prismabuild/produced_output.py`, `retire_batch`). The branch is
  unaccepted and its tests are ongoing; this source inspection proves no
  behavior and no deployment.
- In-flight and unaccepted work: PQ881/issue880 tree
  `/home/rob/tmp/pq-stagea-produced-boundaries-20260921` head
  `9bcc65414536038b05f476da4090a2d40fdbb322`; PQ882 tree
  `/home/rob/tmp/pq-stagea-artifact-budget-20260921` head
  `ea65a52736a714c04e2a13859bdf4107a3d123ca` (PR883 executor-reported, marked
  tentative in the campaign handover); PB781 is described above. None of this
  work is merged, deployed, or accepted.
- Full upgraded Stage A is not proven. The last attempt, action
  `8ca8952cc651d09c5235e2c20b71f987bf396afa67848396f54fb5681166318c` on PQ
  snapshot `7638bf8bc864c0109a81a2c35e48428c30ce4cd6`, exited 1 after writing
  512 boundary-0 files and zero boundary-1 files, generation
  `cfe17deb6d18452da2d4e90545af49e3`, failure `staged-not-serving` before the
  SDK acquire (`stage-a-8ca-attempt-review.json`). Preserved inputs include
  plan digest
  `0b2cc0066bb612e32af6d0c8c809912d325b2975583297eedeee97851ee545da`,
  prepared `962207a3385e9531adaf951b823871a2fb7ff4684320e7a8e19a1d0aa85d8f16`,
  parent manifest
  `71fd8f5688c649d90d6645489cd298eeac7add761f216aab90902948f241e988`, and
  read manifest
  `43f40d18d82ab7489614335b6f1ca33f3d9ec422dcd264eaeb137330e252e3c1`. No
  new GPU launch belongs to this documentation update.
- Baseline whole-model capability is established:
  `/mnt/shared/models/GLM-5.3-Flash-Tessera-E2M1K2-20260901` carries 45
  layers, 38,770 tensors, 120 shards, and 162,658,026,517 bytes, and
  `/home/rob/tessera/docs/glm53_tessera_drain_runbook.md` records the export
  completion and 4.0005 bpp. Rob reports a successful vLLM serve of that
  artifact; the historical serving receipt has not been independently
  re-found. The unqualified piece is the strict produced-output read
  integration, not the whole-model pipeline.

## Strict canary evidence (ACC-07)

- Correction of record: `_pb_commit` in PB main `f5b6bba0358`
  (`tools/fleet/pbcanary_legs/leg3.py`) already includes
  `PRISMABUILD_ACTION_PROGRESS_TOKEN`. The earlier missing-token subclaim was
  wrong; the maintenance readback is
  `/home/rob/tmp/pb-maintenance-780-784-20260921/issue784-progress-correction.md`.
- Historical admitted action
  `932316b609b2256ab3b17be21f7a4b5648cd903a852b098d4335f7c5c57d7322` ended
  executed, rc 0, with `progress_observation.source=action-progress`,
  `accepted_count=1`, last phase `leg3-c2`, cumulative `units_completed=3`,
  and `rejected_count=0`
  (`/home/rob/tmp/pb-maintenance-780-784-20260921/old-leg3-progress.json`).
  The same action still read the declared origin paths, because main leg3
  hashes `entry["path"]` with an ordinary open. Exit 0 plus accepted progress
  alone is not a staged-read proof.
- Conservative source facts that remain true on main: the driver never checks
  accepted progress, and leg3 discards the `_pb_commit` return value. The
  strict staged-only plus independently read accepted-progress gate is
  unqualified.
- Historical canary CPU, CUDA, and cross-Spark payload evidence remains valid
  as scoped by `runtime-776-canary-root-verification.json` and
  `runtime-776-canary-actions.json`: CPU action `8d1b83eb98ca...`, CUDA
  action `63250b02683f...`, and cross-Spark leg-4 actions `b3186ba1d734...`
  and `e75567c90da4...` on runtime `5f878f03c62b`. The leg-3 payload
  `c42eba7bbaac...` in that set itself names origin chunk paths, so the set
  proves payload and cross-host execution, not strict staged reads.
- Repair lane: issue #784, branch `fix/784-canary-staged-reader-20260921`,
  tree `/home/rob/tmp/pb-maintenance-784-fix-20260921` (head
  `53afbdb74546397df47c46e98642014f70407224` at observation). It is unmerged
  and unaccepted; its tests are not qualified gates here.

## Budget example (scoped, not a universal bound)

PQ Stage A artifact budget lane (issue882; PR883 executor-reported; head
`ea65a52736a714c04e2a13859bdf4107a3d123ca`): sealed plan
446,676,598,784 B (416 GiB) retained as provenance; geometry-derived floor
627,065,225,216 B (584 GiB) for the stated geometry (45 retained input
boundary groups, four live probe planes, six retained checkpoint copies,
true-remainder last batch); planning estimate 642,441,609,216 B whose
allowances (shared state, manifest, temporary) are labeled assumptions; and a
proposed invocation budget of 687,194,767,360 B (640 GiB). The runtime byte
guard stays authoritative, and the override seam records the sealed limit,
effective budget, and override identity while the plan digest
`0b2cc0066bb612e32af6d0c8c809912d325b2975583297eedeee97851ee545da` stays
unchanged. Evidence:
`/home/rob/tmp/cache-takeover-20260920/pq-stagea-artifact-budget-result.json`
(executor-reported runs: actions `ad71976c7fa3...`, `0e62c1adb502...`,
`2ddcf168d2c6...`). These dimensions belong to one producer's geometry; the
contract sets no universal PB constant.

## Durability assumptions

The shared output dataset `storage_pool/shared` was observed with
`sync=disabled`; an earlier reading of the parent `storage_pool` dataset as
`sync=standard` was wrong and retracted. The read-only census is
`/home/rob/tmp/cache-takeover-20260920/zfs-output-path-root-review-20260921.json`.
Rename, fsync, digest, CAS receipt, and recovery do not confer physical
durability on volatile writes. No storage property was changed, and the
contract update authorizes no sealing work.

## What this update does not prove

- No produced-output API conformance: `src/prismabuild/produced_output.py` is
  absent from PB main `f5b6bba0358`; the PR781 lane is unmerged and
  unaccepted.
- No deployment of PO-01–PO-06, BUD-01–BUD-03, DUR-01, or ACC-07; PO-07 has
  only its lower cross-root egress edge deployed, in the activated 43b790
  generation. Activation of PB783 does not prove the unmerged produced-output
  stack or the unaccepted PB781 source fix.
- No workload proof: no completed upgraded Stage A, no both-Spark strict-read
  campaign, and no qualified accepted-progress gate.
- No change to worker join/resign support or to the endgame acceptance
  boundary; both remain as the contract states.
- No promotion of the historical canary, baseline export, or budget-lane
  executor evidence beyond the scoped statements above.
