# Local resident sets: Phase 2 rollout plan (PrismaBuild #1545)

Author pb-integrator, 2026-10-05. Plan only: nothing here has been executed, and nothing was changed on a Spark.
Sources: the merged design note `docs/local_resident_sets_design_2026-10-05.md` (sections 3.2, 3.5, 3.6, 5), the Phase 1
review findings (`rev-1005-175049-921a`, `rev-1005-182151-7ed5`), and the spark-local-models agent's action log
(`~/fleet/ceo/exec/spark-local-models/actions.log`). That agent has not yet written its own step D proposal (what else
should be resident, sizes, order); this plan must be reconciled with it when it lands.

## Facts read at about 18:35Z (read-only)
- A8S (163.47 GiB, 128 files) is local on BOTH Sparks at `/home/rob/models-local/a8s-exported`, verified 128 of 128 sha256
  by that agent. It is made visible by a host-wide read-only bind mount over the NFS path
  `/mnt/shared/tessera-runs/moe/glm53-a8-bf16menu-20260930/release/exported`, from a systemd mount unit (installed by
  that agent, `nofail`). Both units are active.
- Free space now: sparky 186 GiB, sparklina 124 GiB. After the 5% floor (D1) the room for ANOTHER set is about 94 GiB on
  sparky and 78 GiB on sparklina, so a G3-sized read set (about 250 GB) does not fit on either Spark yet.
- Sparky has a container running right now that reads A8S through that bind mount (`sleepy_napier`, started minutes ago,
  GPU idle at the moment I looked). Sparklina had none. So an adoption window cannot be assumed; it has to be checked at
  the time.

## What is true until Phase 2 exists
1. PrismaBuild does not know about the A8S copies. The disk figures it mints `local_gib` from are real free space, so
   capacity is not overstated; the copies are simply unowned: no lease, no pin, no eviction, no record.
2. **Evidence honesty gap.** Jobs that read A8S through the host bind mount are served local bytes, but Phase 1 records
   `served_from: canonical` on every attempt, so the record says NFS for a read that was local. Do not use `served_from`
   as evidence for the Window 4 timing runs. Say so in the Window 4 report.
3. Phase 1's `adopt` REFUSES an active bind mount that names the source or the canonical root (by design, section 3.6).
   So PrismaBuild cannot take over A8S until the systemd unit on that host is stopped. That is a Spark change.

## Gates, in order (none may be skipped)
- **G0** PR 1552 (Phase 1) merged. In progress; waits only on the full-suite comparison.
- **G1** Stage 2 of Phase 1 merged and reviewed. The Phase 1 reviewer said these must land before Phase 2 or before any
  host is enabled: N4 (`served_from` accepts `canonical` and `local`), N1 (the copy record has two writers under
  different locks; the shim will trust `resident`), N3 (`pbresident dispatch` and `renew`), N5 (re-evicting absent sets
  every cycle; one failing set crash-loops the role), N6 (docker missing, hard-maximum ceiling). In progress on the Sol
  worker's branch.
- **G2** Phase 0 measurements the shim design depends on, done on a Spark in a quiet window: (b) a per-container nested
  bind (`--mount type=bind,readonly`) over a subdirectory of `-v /mnt/shared:/mnt/shared`, ordering and what `ls` shows;
  (f) `docker run -v` with a missing source (empty directory) versus `--mount` (error); (h) the real loader's read
  concurrency, not only fio-style streams. The already-known read-speed gap for A8S is not a substitute.
- **G3** Rolling-upgrade order. Publish a runtime generation that ACCEPTS `served_from: local` (N4) and let it reach every
  host and the canary BEFORE any host's shim is allowed to write `local`. Otherwise an older reader raises
  `PoolContractError` on those attempts.
- **G4** D29 ruling for the aarch64 mover (open question 3 in the note). NOT needed for A8S, which is adopted without a
  copy. Needed for any later set that must be copied onto a Spark.

## Phase 2 steps
1. **Build the shim injection** (Docker shim; Sol worker; test-first through pbtest; same independent review as Phase 1).
   In the note's exact order: pin first, then under the per-host flock re-read the copy record and require `resident`,
   release the lock, list the canonical directory (names and sizes) against the manifest, then add
   `--mount type=bind,src=<local_root>,dst=<canonical_root>,readonly` (never `-v`). A fallback drops its pin at once and
   records its reason (including `lease_expired`). Accelerator mode falls back; `required` mode is opt-in. D32: this
   converts nothing into a seal; every integrity refusal stays a refusal.
2. **Review, merge, publish** a runtime generation (G3 first). Nothing is enabled yet: the policy host map is still empty.
3. **Adopt A8S, one Spark at a time, sparklina first** (it has no running container as I write this). Per host, in a quiet
   window agreed with the Kernels lead (their Window 4 gang serves A8S): confirm no container holds the recursive
   `/mnt/shared` bind (`docker inspect`, not only `fuser`); stop and disable the systemd mount unit (leave the unit file
   installed, disabled, as the rollback); publish the A8S set with the existing content manifest
   (`t8-v1-a8s-content-manifest-20261005.json`) and a bounded lease; `pbresident adopt` re-verifies every sha256 and moves
   the directory into the tier root on the same filesystem; enable the host in the policy.
   Rollback: stop injection for that host, move the directory back, re-enable the unit. Between "unit disabled" and "set
   resident" jobs on that host read NFS, so that gap must be minutes, and only with no job running.
4. **Canary** a tiny declared job per host: the file under the canonical path reads from the local device (compare
   `st_dev`), the attempt records `served_from: local`, the pin shows in `pbstatus --resident-sets`, and releasing the
   lease evicts only after the pin drains. Report before the next host.
5. **Second Spark**, same steps. Then one real Window run served local is the acceptance witness. That also supplies the
   D29 live GPU observation I still owe.
6. **Remove the systemd units** from both Sparks for good, only after step 5, and record the removal.

## Decisions I need (not mine to make)
1. D29: may a host-pinned aarch64 mover row run on the Sparks (needed only for copied sets, not for A8S)?
2. A8S lease terms. The note has no forever lease; I suggest a 14-day hard maximum, renewable only by an explicit act.
   Who may release it, any lead or only the publisher? (Question 4 in the note.)
3. May my lane stop and disable the two systemd units on the Sparks at the adoption step? The other agent installed them
   with sudo; I would not touch them without a go-ahead and a window.
4. Which window for sparky: it has a job reading A8S now.

## Not planned here
A second resident set (G3 read set, PACT BF16): wait for the spark-local-models proposal and the free-space numbers; at
today's figures neither fits. Private mount namespaces and any global host mount remain out of scope, as in the note.
