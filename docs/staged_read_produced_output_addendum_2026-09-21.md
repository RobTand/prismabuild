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

## Snapshot 2026-09-21T06:18:11Z — PR781 merged and deployed; produced-output proof still open

Append-only current-status evidence, frozen at 2026-09-21T06:18:11Z. Every line below is
root-artifact- or read-only-inspection-backed; nothing here advances an axis by
itself. The runtime read used below was taken at 2026-09-21T06:12:05Z through
read-only `pbmcp pb_runtime`.

### Corrections of record (superseding earlier dated statements in this addendum)

- The earlier statements that PB781 is unmerged, that its tests are ongoing,
  and that the #784 canary repair is unmerged are dated observations at their
  write time. Current: PR781 is merged and its produced-output module and
  `pbrun --produced-output-template` are present in deployed generation
  `d794839c589d-1789966072-c66c7f0568d0`; PR788/#784 is merged at
  `c9af82333eeb8900b139e6c593fa6258b0421d63` and is in the same deployed
  generation. A 2026-09-21 executor was misled into reading the old wording as
  "the deployed pbrun lacks its actual `--produced-output-template` flag"; it
  does not.
- The earlier "driver never checks accepted progress" and "leg3 discards the
  `_pb_commit` return value" source facts were true at PB main `f5b6bba0358`
  and are superseded on main by PR788: `tools/fleet/pbcanary.py`
  `terminal_progress_observation` binds accepted progress to the terminal's
  adopted attempt, and `leg3._pb_commit`'s result gates each chunk.
- None of these corrections makes a produced-output requirement deployed or
  workload-proven. The static-input baseline is not the produced-output case.
- Root review retracted the GB10-local-tier blocker this snapshot recorded
  (parent mailbox `1789971289963-fea9cee8`, after source review of
  `pbrun.resolve_stage_tier` at `tools/fleet/pbrun.py:4949-4976`): a stage tier
  need not belong to the consumer's host. The existing
  `prismabuild-stage:dl380g10` tier is served over the shared NFS mount to the
  Sparks, and the original 8ca attempt and baseline canary action
  `e44c5a11db6a87dfa95c29425ef040e90f1c790370230e2a58cb5a2b3f74a299` are
  actual proof. No new tier, capacity, infrastructure or permission decision
  is required; the tiny global Docker cycle waits only on accepted/deployed
  PB792 and PB795 plus the bounded retirement adapter.
- Library and component produced-output cycles did run. What is missing is a
  successful global lifecycle and a global produced-retirement acceptance; the
  one global attempt failed at the mover preflight and is preserved as such.
- After this snapshot: PR795 (own-copy egress, issue 793) merged
  2026-09-21T06:33:58Z (head `83e8d502ad9305177ee6e0501ff7c88ee036579d`,
  merge `3641e29332bfd7a091f35b5bd3875b6de294c86e`); root acceptance and
  deployment remain pending. PR794 (withdrawn-attempt
  readback, refs issue 790) had already merged 2026-09-21T06:04:03Z, before
  this snapshot, and is not in the active `d794` runtime.

### Source and merge facts

- PR781: merged `dd5523dd3ff66025170f1e2248d8b32d180d7a78` at
  2026-09-21T04:46:51Z, head `42f2cfb874077da66c27401e718d3636903a8a40`,
  accepted for source merge only with 231 final focused checks and 30
  candidate-only failure identities matched to a same-harness baseline
  (`pb781-root-acceptance.json`). Remaining gaps recorded there: not deployed
  at acceptance, PQ production constructor/SDK/template/lifetime wiring
  incomplete, original 512 Stage A not completed, broad suite not green.
- PR792: merged `aa03c0bde1b52138b02be2b13083bf3e8274d9ac` at
  2026-09-21T05:59:13Z, head `444e9743c591855b18da68bcd236bb1f21d5e200`; 75
  targeted PB checks over five action keys plus an actual RED
  `1f10f71c7cd5f0d960ff77391c7df37ae5b94f883313105f04208a5264138bdb` with 4
  behavioral failures on base `dd5523dd` (`pb792-root-acceptance.json`,
  `pb792-admission-root-verification.json`). Scope: PO-03 component and
  ACC-07; root acceptance states it fixes child source addressing and does not
  independently close repeat materialization. Merged, not deployed.
- PR788 (#784): merged `c9af82333eeb8900b139e6c593fa6258b0421d63`; the
  staged-reader leg and the independent accepted-progress binding are on main
  and in the deployed generation below.

### Deployment facts

- Deployed runtime generation `d794839c589d-1789966072-c66c7f0568d0`, source
  `d794839c589dee8cc9427e10127f6381800eaa05`, parent `dd5523dd`. Publication
  verified 737 files compared with no mismatches
  (`pb781-publication-root-verification.json`; receipt
  `032202292b340a994027c6884d6f1f8388b586e5ccd398c004cacea0c6ad89d5`, result
  `d79a558b87afa062616ad58be78216501c9f54d417212537149f4bca2bd6bd5f`).
- Live at the 2026-09-21T06:12:05Z read (snapshot frozen 06:18:11Z): dl380g10 (16 loops), sparklina (3), sparky (5) on
  source `d794839c589dee8cc9427e10127f6381800eaa05`; DESKTOP-P5UOGNJ stale on
  `a80eea97fe8f9ced1f3f2776188ee1f51a029c11`; rollout idle. The root acceptance
  adds the direct dl380g10 role census (supervisor 1, prewarm 1, tier 1,
  worker 16; all `d794`).
- Baseline canary, root-verified: five actions executed rc 0 — four on Sparky
  (legs 1, 2, 3 and the Sparky arm of leg 4) and one on Sparklina (the other
  leg-4 arm). Leg 3 (`e44c5a11db6a87dfa95c29425ef040e90f1c790370230e2a58cb5a2b3f74a299`,
  Sparky) is the strict staged read: `reader-lease-v1`, tier
  `prismabuild-stage:dl380g10`, three pin ids, and accepted progress
  (`accepted_count` 2, phase `leg3-c2`, `units_completed` 3) read from the
  terminal attempt. The both-Spark claim is the separate leg-4 payload
  comparison (`859436101045acedd1a88173a96b0b5b6fd55d1873f9f37112044d916ece8f0e`
  on Sparky, `ac2f43ffdb749dd4176fa0e4326b893e5e5a72c29e02f3706f8f19dc0c095bd9`
  on Sparklina, bitwise equal envelopes), not the strict read
  (`runtime-781-canary-root-verification.json`).
- Limits, root-stated and unchanged: this is a static-input baseline. It is
  not produced-output or global-PQ-cycle acceptance; the new produced template,
  the PQ live cycle, and full Stage A are not claimed. No PO, BUD, DUR, or
  ACC-07 requirement is behavior-deployed or workload-proven by it.
- PR792 is not in the active runtime: the active generation stays `d794` until
  root publishes both repairs. The own-copy egress repair has no accepted
  commit or deployment.

### Produced-output live-cycle facts (all negative or scoped)

- The one actual global produced-output live attempt failed before any read:
  owner `0dedb066f8684fba56f388496089790b53a0b0658a4abe8d5945e75592178669`,
  mover `6fbc96301c6cc2ad245003a4866271288532dcde5ff37a324046e7004abd0846`.
  The mover was claimed and then failed in 0.759 s on core preflight
  (`live pbrun checkout identity differs from its sealed stamp`) because the
  child request lost the producer's `params.checkout_snapshot`; the owner
  failed closed on its 600 s boundary-staging budget. No full Stage A
  launched. Preserved pre-withdrawal attempt history shows one actual failed
  attempt; the withdrawn-row view omitted it (`PBMCP attempts_detail=[]`,
  issue 790; the readback surface was fixed in source by PR794, merged
  2026-09-21T06:04:03Z before this snapshot and not in the active `d794`
  runtime), so current ready/withdrawn state must not be read as
  "never claimed" (`produced-mover-snapshot-root-diagnosis.json`).
- The old live attempt's recorded capability is host-venv SDK bootstrap on
  x86 (`argv[0]` `/home/rob/venvs/pq881-pb461728e4/bin/python`, no
  docker/podman in argv), not Docker code-in-container loading; container
  loading remains an unproven launch hazard
  (`pq-stagea-produced-boundaries-result.json`).
- Own-copy egress defect: admitted action
  `83264d103a2aed2bc5b24b747bcf93655b45d3c15977fc7da1c19666ba50e92a`
  (source `fd9ae33fb32205f270c27fee43a212ca455af615`, parent
  `510b38a277a28e5d01f92cbfe5193fedb7028b4b`) ended rc 0 while its payload
  ran two iterations: iteration 0 ok, iteration 1 refused
  `tier-reservation-unavailable`; the wrapper exit 0 is not a full pass. Its
  result CAS is
  `f9256a52fa5c0a497408bee2a75f7063683a26d6996104038d82cf9426cb20b6`
  (receipt `68749592f8a541917c03cbb7a5638d19f22d1806e41101b4f324d0ca69fef0bb`).
  The failing retirement returned complete with `entries_deleted` 0,
  `entries_shared` 4, `shared_with` `["in-flight-copy"]`, `tokens_released` 0,
  `tokens_decharged` 1, `bytes_shared` 8356, and free/held 0 with nothing
  returning after a 30.03 s poll, against the healthy receipt
  `tokens_released` 1 / `tokens_decharged` 0 / `entries_deleted` 4. Root
  traced the cause to `stage_release._evict_owned` calling `_claimed_paths`
  without excluding its own mover, so the mover drops its only proof and
  destroys its own token (`pq-stagea-race-census-root-read.json`).
- Root decision of record (`claude-pq-own-copy-defer-contract.md`): an evicted
  mover's own still-live claimed copy is a deferred handoff, never a separate
  coowner; the fix preserves bytes, proof, and full credit and reports an
  incomplete egress with an own-copy-in-flight reason. After the CHILD mover
  reaches terminal, ordinary retry returns the token. Produced reads still
  start from published data-ready proof and must not wait for the whole
  producer terminal (PO-02); retirement waits are a different edge. No
  wall-clock timestamp is attempt-ownership proof; the PQ adapter waits boundedly
  on existing PB APIs under its declared budget; no new receipt version is
  created, because existing PO-05/PO-07/ACC-07 obligations cover this case. The
  conservative repair is in progress (issue 793, tree
  `pb-egress-own-copy-20260921`); its head is moving and is deliberately not
  pinned here.

### PQ lanes and capability, scoped

- PQ881 remains unmerged; its live lifecycle is pending and the original 512
  Stage A was not restarted or completed. Its prepared tiny staged input
  (4416 bytes, sha256
  `1013c15b67a2bae3b9fbe55b38d8c175f672e830bfe3d2c583ade2132d8db82e`) was
  accepted but not exercised. The earlier reading that no GB10 worker
  advertises a stage tier was retracted by root review (see the corrections of
  record): the tier need not belong to the consumer host, and the existing
  `prismabuild-stage:dl380g10` NFS tier serves the Sparks. The one-token
  acceptance is not closed at candidate `42f2cfb8` and must be re-run against
  the repaired PB candidate once PB792 and PB795 are accepted and deployed
  (`pq-stagea-produced-boundaries-result.json`).
- PQ885 is merged at `d5f844da9cddde3be70e328f0437f1ba40b068ae`
  (2026-09-21T05:33:19Z): metadata-generation seam with 127 PB checks plus two
  actual original-input check-only cases (45 quanta / 360 windows, no writes,
  identical scientific bindings). It preserves original scientific data roots
  and inputs and old immutable metadata; it does not prove full Stage A or
  strict quantum bound inputs (`pq885-root-acceptance.json`).
- Whole-model capability is preexisting and unchanged: the Tessera GLM-5.3
  artifact at `/mnt/shared/models/GLM-5.3-Flash-Tessera-E2M1K2-20260901`
  (45 layers, 38,770 tensors, 120 shards, 162,658,026,517 bytes). The open
  produced-output integration does not mean no model was ever quantized.

### Budget and storage, restated

- Proposed explicit Stage A invocation budget 687,194,767,360 B (640 GiB),
  against the geometry-derived raw floor 627,065,225,216 B (584 GiB) and the
  sealed plan 446,676,598,784 B (416 GiB) retained as provenance
  (`pq-stagea-artifact-budget-result.json`). Dimensions are producer-specific;
  no universal PB constant is set.
- Working tier geometry recorded in the plan and token audit: the 512-entry
  boundary-0 is eight 64-entry batches (~8 GiB payload) durable under the
  origin quota with zero tier tokens; per-batch `stage_tokens_for_bytes`
  rounding is 64 x ~16 MiB + headers -> 2 window tokens per group, and the
  working geometry carries 4 window tokens (a two-window budget), acceptable
  only after the actual overlap (pinned input + prefetched next + publishing
  next-boundary outputs + checkpoint working set) is proven to fit
  (`pq-stagea-produced-boundaries-plan.json`,
  `pq-stagea-artifact-budget-result.json`). The token audit exercised 1 GiB
  windows with four groups (`pq-stagea-race-census-root-read.json`). This is a
  bounded materialization geometry, never a requirement to retain the whole
  corpus on SSD, and it keeps the logical retained-artifact budget, the
  physical tier reservations, and process memory distinct (BUD-01).
- DEV mode adds no extra giant payload hash. Raw produced origins remain on
  the current shared ZFS path; physical staging is a separate window
  (PO-01/DEV-01/DEV-02). `storage_pool/shared` remains `sync=disabled`;
  commits, readbacks, and receipts are not power-loss durability (DUR-01).

### Open at this snapshot (pending, not claims)

- Root acceptance and deployment of PB792 (snapshot repair) and PB795
  (own-copy egress, merged 2026-09-21T06:33:58Z after this snapshot); the
  bounded retirement adapter; the one-token acceptance re-run on the repaired
  candidate.
- A successful global produced-output lifecycle, including a same-action
  self-read and the ACC-07 origin-refusal negative control in the global PQ
  cycle; the PQ live lifecycle; full upgraded Stage A. No new stage tier,
  capacity, infrastructure or permission decision is required for the tiny
  global Docker cycle.
- Whole-workload proof for every PO, BUD, and DUR row: all remain
  `unknown`/`partial`, never satisfied. This file is evidence, not acceptance.
