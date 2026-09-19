# Slice 1 recon: pool queue → slurmctld/slurmd (PB #657, row 1)

- **Scope:** leverage-map row 1 only — `PoolQueue` (ready/claimed/done) plus the
  worker-loop claim machinery → slurmctld queue + slurmd execution.
- **Verdict:** ADOPT — the transport seam already exists; the scheduler does not
  (no `sbatch`/`sinfo` on any fleet box; verified 2026-09-19).
- **Pin:** `origin/main` `f05e2434e3` (2026-09-19). All `file:line` refs below
  are against that commit.
- **Relation to prior work:** issue comment `IC_kwDOUKZwrc8AAAABVg7ayw`
  (muse-spark-recon1, main @ `b125b68c`) covered rows 1–4 read-only. Since then
  `pool.py` grew and every `PoolQueue` method moved; this slice re-verifies row 1
  at the new pin with current line numbers and a complete consumer/test
  inventory. No code change — this document is the slice's only change.

## 1. The bespoke mechanism

`src/prismabuild/pool.py` (10,322 lines). Queue-directory vocabulary
(`pool.py:183-192`): `READY="ready"`, `CLAIMED="claimed"`, `DONE="done"`,
`FAILED="failed"`, `WITHDRAWN="withdrawn"`. Claim-is-a-lease:
`LEASE_TIMEOUT_S = 300.0` (`pool.py:228`).

| Method | Ref | Role replaced by SLURM |
|---|---|---|
| `class ResourceLedger` | `pool.py:1615` | Node/core/memory admission (rows 2/6 own the policy; the *ledger substrate* retires here) |
| `acquire` | `pool.py:2203` | Admission check per action |
| `announce` | `pool.py:2623` | Worker offer publication (`pb-queue/workers/<host>.json`) |
| `class PoolQueue` | `pool.py:2405` | The queue itself |
| `publish` | `pool.py:3121` | `sbatch --parsable` (`slurm_lane.submit`, `slurm_lane.py:1038`) |
| `claim` / `_claim` | `pool.py:5561` / `pool.py:6121` | slurmctld dispatch + slurmd start (no `rename()`-race claim, no lease) |
| `reap_stale` | `pool.py:7058` | Nothing — leases cease to exist; node failure surfaces as `NODE_FAIL` (runbook-verified in containers) |
| `finish` | `pool.py:8634` | Terminal-record filing moves to the submitter (`slurm_lane` outcome → `done/`/`failed/`); see risk §6 |
| `withdraw` / `withdrawn_keys` | `pool.py:9376` / `pool.py:9099` | `scancel` + `withdrawn/` marker reads (`test_slurm_withdraw_siblings.py`, `test_slurm_withdrawal_generation.py`) |
| `execute` | `pool.py:9583` | slurmd + `tools/fleet/slurm_job.py` (same launch bytes — `slurm_job.py:18`, `pool.worker_argv` pinned byte-identical) |
| `serve_once` | `pool.py:10202` | One poll-loop iteration; dies with the loops |

Claim-side liveness that is transport-independent stays: `ProgressPhase` /
`progress_policy` (`pool.py:521-617`) and the heartbeat/lease fields travel
with the action, not the queue (rows 5–8 keep: progress contracts stay
application-attested; see §6).

## 2. Every call site (bespoke consumers of `PoolQueue` / `pb-queue`)

Submit / orchestrate:

- `tools/fleet/pbrun.py` — pool publish/withdraw paths; `TRANSPORTS =
  ("pool", "slurm")` (`pbrun.py:96`); `--transport` (`pbrun.py:5600`,
  default `fleet_submit.default_transport()`); `_OfferSnapshot(pool.PoolQueue)`
  (`pbrun.py:1229`); dual-transport record resolution (`pbrun.py:2791-2932`).
- `tools/fleet/pbcampaign.py` — `pool.PoolQueue(pbrun.SH / "pb-queue")`
  (`pbcampaign.py:822,1183,1472`); ledger-availability note (`pbcampaign.py:526`).
- `tools/fleet/fleet_submit.py` — `default_transport()` (`fleet_submit.py:97`,
  env `PRISMABUILD_TRANSPORT`); `queue = pool.PoolQueue(queue_root)`
  (`fleet_submit.py:387`).
- `tools/fleet/publish_runtime.py` — records `default_transport` for the
  cutover (`publish_runtime.py:1039,1228,1278,1410`); import smoke
  (`publish_runtime.py:448`).
- `tools/fleet/pool_reset.py` — already transport-aware: each record's own
  `transport` field decides, re-submits through `slurm_lane` unchanged
  (`pool_reset.py:31-57,137-150`); `queue = pool.PoolQueue(...)`
  (`pool_reset.py:794`).

Execute / poll:

- `tools/fleet/worker_loop.py` (1,154 lines) — `queue = pool.PoolQueue(SH /
  "pb-queue")` (`worker_loop.py:784`); poll+`serve_once` loop per module header.
- `tools/fleet/supervise.py` (1,192 lines) — spawns loops from
  `tools/fleet/fleet_boxes.json` (`supervise.py:66`); 5 sparky + 3 gx10-6b77 +
  16 dl380g10 = 24 loops.
- `tools/fleet/worker.py:60` — `pool.PoolQueue(SH / "pb-queue")`.

Observe / sweep / account:

- `tools/fleet/pbwait.py:800`, `tools/fleet/pbstatus.py:859`
  (+ `_pool_claim_denials` `:784`, starvation helpers `:1172-1404`),
  `tools/fleet/pbmetrics.py:521,879` (+ `_read_claim` `:143`),
  `tools/fleet/pbsweep.py:103`, `tools/fleet/pbtest.py:179,608`.
- Residency roles read the queue (policy stays, substrate moves — rows 6–7):
  `tools/fleet/tier_loop.py` (15 `queue: pool.PoolQueue` signatures,
  `:157-1495`; `--pool-root` `:1765-1779`),
  `tools/fleet/prewarm_loop.py` (`:2483-3453,4344`),
  `tools/fleet/stage_release.py` (`:109-764`),
  `tools/fleet/dispatch_tessera_model.py:105,179`,
  `tools/fleet/qualify_rollout.py`, `tools/fleet/tessera_status.py`,
  `tools/fleet/mount_latency.py`.

## 3. Tests covering the queue (retire with `pool.py` in Phase 3)

`tests/test_pool.py` plus ~60 `tests/test_pool_*.py` files
(`ls tests/ | grep -i pool` at the pin): claim race/lease/reaper
(`test_pool_claim_race_names_the_winner.py`,
`test_pool_reaper_lease_identity.py`, `test_pool_widowed_lease_*.py`),
finish/late-finisher/tombstone (`test_pool_finish_*.py`,
`test_pool_late_finisher.py`, `test_pool_ready_late_finisher.py`),
withdraw (`test_pool_withdraw*.py`), preempt/entomb/poison
(`test_pool_preempts_a_background_holder.py`,
`test_pool_entomb_refusal.py`, `test_pool_poison_record_is_a_denial.py`),
plus loop/supervisor shells (`test_worker_loop_*.py`,
`test_supervisor_sees_a_held_claim.py`, `test_pool_queue_default_root.py`).
Bridge tests that must survive the move: `test_pool_reset_transport.py`,
`test_slurm_withdrawal_*.py`, `test_slurm_outcome_*.py`,
`test_slurm_cutover_*.py`, `test_slurm_liveness.py`, `test_slurm_singleton.py`.

## 4. The exact SLURM feature replacing it

slurmctld queue + slurmd execution, via the existing lane:

- `slurm_lane.submit` (`slurm_lane.py:1038`): `sbatch --parsable`,
  `--dependency=singleton --job-name=pb-<key12>` (one-job-per-key),
  `--export=NIL`, `--comment=<nonce>` (hung-submit adoption),
  `--partition` (`partition_for`, `slurm_lane.py:666`),
  `--constraint` (tags→Features), `--gres`, `--mem`/`--cpus-per-task`
  (`LaneResources`, `slurm_lane.py:595`), `--time` only when the submitter
  asked (`format_time_limit`, `slurm_lane.py:569`), `--nice` from pool
  priority (`nice_for`, `slurm_lane.py:306`).
- `slurm_lane.job_script_text` (`slurm_lane.py:978`); `tools/fleet/slurm_job.py`
  (346 lines) materializes the sealed snapshot and execs canonical
  `pool.worker_argv` — same launch bytes either transport.
- Provenance/observability: `squeue`/`scontrol`; `sacct` via `slurm.py`
  (`_SACCT_ADOPTION_FORMAT :160`, `_SACCT_STATE_FORMAT :163`) — inert until
  `slurmdbd` exists (`AccountingStorageType=none`; lane falls back to
  `scontrol`+`MinJobAge`).
- Controller config already in tree: `fleet/slurm/slurm.conf`
  (`SelectType=select/cons_tres`, `CR_Core_Memory` `:80-81`,
  `GresTypes=gpu,shard` `:154`, partitions `all`/`gpu`/`cpu` `:253-255`,
  all `MaxTime=UNLIMITED`), `gres.conf`, `cgroup.conf`, `epilog.sh`;
  lifecycle scripts `install.sh` / `verify.sh` / `cutover.sh` (publishes a
  runtime generation with `default_transport: slurm`) / `rollback.sh`.
- Container-verified: `fleet/slurm/smoke/` (+ `multinode/` — three-box RPC,
  cross-box placement, partition/weight routing, node-fail/return,
  controller restart).

## 5. Migration sketch (no code change needed on the submit path)

1. Deploy per `docs/slurm_runbook_2026-09-04.md`: `install.sh` → `verify.sh`
   on the fleet (2-box pilot: dl380g10 + one GB10 first, per the issue).
2. Close the "Still not verified" list (runbook §"Still not verified":
   real cgroup/systemd arrangement, real-GPU containment, NFS `root_squash`
   end-to-end, `NodeAddr` dialling); deploy `slurmdbd` only if aging/`sacct`
   provenance is wanted.
3. Cutover: `cutover.sh` publishes a runtime generation with
   `default_transport: slurm`; `pbrun --transport` defaults follow
   (`fleet_submit.default_transport`); rollback restores the pool default.
4. Phase 3 (decision doc §6, after two quiet weeks): delete `pool.py`,
   `worker_loop.py`, `supervise.py`, `box_capacity.py`, their tests and the
   scheduler-only branches.
5. Rehouse, don't drop: residency-role queue reads (`tier_loop`,
   `prewarm_loop`, `stage_release`) and `progress.py` + `ProgressWatch` move
   to the lane root / slurmd path with the queue's retirement (rows 5–7 own
   the policy details).

## 6. Risks

1. **Submitter-is-the-writer.** `pbrun` files the terminal record, so a
   submitter killed mid-wait or past `--wait-s` leaves a finished job with no
   `done/` record. `PoolQueue.finish` ran on the executing worker and could
   not miss. Accept CAS-receipt-as-authority plus orphan reconciliation, or
   move record-writing into a job exit trap that cannot see the CAS verdict
   (runbook §"What this replaces"; recon1 §Row 1 — still open).
2. **Half of `pool.py` must survive.** The heartbeat/lease half dies with the
   pull queue, but `progress.py` + `ProgressWatch` are action-side and
   transport-independent — the migration must carry them into the SLURM worker
   path, not delete them with `pool.py`.
3. **Token-file housing.** Minting state (`minted/` dirs, `O_EXCL` mint right)
   lives on the pool substrate; when the pull queue retires, the token files
   need a new home (lane root or controller-side). Row 6 owns this; listed
   here so Phase 3 does not silently stop stage accounting.

## 7. Slice boundary

This slice touches no live path: the sole change is this document. No
campaign interaction occurred (no submissions, no fleet commands — the
`sbatch`-absence check in §"Scope" is a local `PATH` lookup). `pbrun.py:5600`
keeps `"pool"` the default; nothing here advances the cutover. Open
questions for other slices: TRES taxonomy (row 2), array/pack fanout
(rows 3–4), progress/ledger/evidence housing (rows 5–7), `fleet_boxes.json`
generation (row 9).
