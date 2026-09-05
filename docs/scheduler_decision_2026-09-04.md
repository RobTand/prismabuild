# Scheduler decision: keep the core, replace the pull queue with SLURM

Status: **ratified.** Rob endorsed SLURM on 2026-09-04 (*"yes, I endorse
SLURM"*). Written that day on `main` = `44b9f8f` plus the fixes branch
`claude/pb-fixes-2026-09-04`; it answers one question Rob asked:

> Figure out if PrismaBuild, as we've built it, is the "right" thing to do, or
> if there's another open source project out there that does something similar
> that we won't have to also maintain. [...] I struggle to believe something
> like this doesn't already exist. I'm fine for us to own and maintain it, but
> it's not our bread and butter.

Everything below is either read from code at a named line, measured on the
fleet that day, or verified against a primary document and marked as such. A
claim without one of those is labelled as unverified.

## 1. Verdict

PrismaBuild is two systems in one repository, and the evidence separates them
cleanly.

**Keep the memoization core.** Action keys over `(inputs, code closure, params,
environment that matters)`, host-portable generation scope versus host-class
measurement scope, a content-addressed store whose receipts are self-hashed and
carry worker attestation (`cas_receipt.v3`), and Git-bundle snapshots of a
dirty working tree that a worker materializes bit-exactly. No open-source
project does this for GPU measurement work in a usable shape. The closest
conceptual match is the Remote Execution API family (nativelink, buildbarn):
action digests, a CAS, pull workers. Those assume Merkle-tree inputs uploaded
into the CAS and a build-tool client; they do not model 100 GB models on NFS,
side-effect outputs, or per-host-class measurement identity. Workflow engines
with a resume cache (Nextflow, Snakemake, Prefect, Flyte, DVC) key on task
inputs but have no notion of attested producer identity, and Rob's spec
rejected the DAG-engine shape on 2026-08-26. This half is about 4,700 lines,
stdlib-only, and the part worth owning.

**Replace the pull queue.** `src/prismabuild/pool.py` plus
`tools/fleet/worker_loop.py`, `supervise.py` and `box_capacity.py` is a batch
scheduler: claim-by-rename, lease heartbeats, stale reaping, rename-acquired
capacity tokens, starvation floors, per-host offer files, attempt history,
withdrawal, and a runtime-generation rollout protocol, all on an NFS export
mounted with `local_lock=none`. It is the "roll-your-own queue dir" that
`docs/design.md` records Rob declining on 2026-08-26, and it exists only
because no scheduler was installed on any box when the repository was split
out on 2026-08-31 (README, "Status, honestly"). It grew from 449 lines on
2026-08-31 to 3,407 on 2026-09-04. Of the 16 open issues, 11 are defects of
this scheduler on this substrate (section 7); of the 14 unmerged branches, 9
touch it and 11 of 14 no longer rebase onto `main` because they all edit the
same two files (section 8). That curve does not converge on "world class"; it
converges on a bespoke SLURM.

**With SLURM.** Rob's original specification chose SLURM; `src/prismabuild/
slurm.py` (3,023 lines) is a SLURM adapter that has only ever run against fake
binaries because `sbatch` is installed nowhere. Apt carries SLURM on all three
boxes. The fleet's own slot model maps onto SLURM's `shard` GRES directly.
HTCondor is a genuine runner-up (section 4) and Rob may still pick it; the
thin lane being built keeps the transport-specific verbs to one small module.

## 2. What was measured

| Fact | Measurement (2026-09-04) |
|---|---|
| No scheduler exists | `sbatch`, `condor_submit`, `enroot`: absent on sparky, sparklina, dl380g10 |
| Root access | `sudo -n true` fails on all three; password sudo only, so every install step is a runbook Rob runs |
| Live queue | ready 0, claimed 2, done 912, failed 427, withdrawn 35 |
| Failure ledger, by cause | 378 `failed` with the command's own non-zero exit (mostly red Tessera test arms, which is the command working), 34 `reset`, 11 `orphaned_stub` (#11), 4 `lease_lost_max_attempts`: **49 of 1,339 terminal records (3.7%) are pool-mechanism failures** |
| Clock skew from sparky | sparklina +4 ms; dl380g10 within 15 ms, measured by NFS server timestamps (an earlier +1.7 s figure was an artifact of the 3.5 s ssh handshake to that box and is withdrawn). All three ran `systemd-timesyncd` with no server configured; Rob pinned them to 192.168.1.1 the same day |
| Worker signal dispositions | dl380g10 loops run with SIGINT ignored (`SigIgn 0x1001007`); sparky loops do not. This, not a timing bound, was #25 |
| Repository history sizes | tessera pack 4.94 MiB / 1,375 commits; prismaquant 46.8 MiB / 2,173; well under the 512 MiB snapshot ceiling, so #35's ancestry fix is cheap |
| Apt SLURM | Ubuntu 24.04 (Sparks) `slurm-wlm` 23.11.4; Ubuntu 26.04 (dl380g10) 25.11.2 |
| GB10 memory | cgroup `memory.max` does not charge CUDA allocations (three-arm probe on branch `pb/issue-1`, issue #9); GPU memory telemetry is null on GB10 |

## 3. What the scheduler half is, in its own words

`pool.py` opens with the contract it implements: a claim is a lease, not a
grant; a heartbeat file; any worker may return a stale claim to `ready`. Those
are the primitives of a distributed scheduler, and every one of them is a
place where NFS attribute caching, directory-listing latency and cross-host
clocks can disagree. The open issues are exactly those disagreements: a
frequently rewritten queue file stalls one client for 69 s (#16); one item's
`passes/` sidecar stalls every loop on every box (#15); an offer file goes
stale when its writer is busy (#6); a claim is reaped inside the window
between `claim()`'s rename and its record rewrite (#36, fixed on the branch);
a requeue writes a stub the reaper then orphans (#11); a rollout lets old
workers quarantine new records (#29, a P0 whose proposed fix is a 1.6k-line
expand/contract protocol). None of these is a bug in Rob's problem; they are
the cost of running scheduler state on a network filesystem, which SLURM and
HTCondor avoid by keeping state on the controller and speaking RPC.

## 4. Alternatives

Claims marked **verified** were checked against the vendor's own documents on
2026-09-04 (SchedMD `upgrades.html`, `gres.html`, `sbatch.html`, `prolog_
epilog.html`, `priority_multifactor.html`, 20.11 release notes; HTCondor
repository index and version-compatibility page; HashiCorp licence
announcement; nativelink README). Everything else is judgment.

| Option | Fit | Cost | Verdict |
|---|---|---|---|
| **SLURM** | Rob's spec choice; adapter in tree; `shard` GRES since 22.05 gives fractional GPU slots (**verified**; shards schedule slots and do not fence GPU memory); `sbatch --wait` returns the job's exit code (**verified**); `--constraint` for placement tags; `--time` enforced TERM then KILL after `KillWait` (**verified**); Prolog/Epilog on the node with `SLURM_JOB_ID` (**verified**); backfill scheduler | **Version skew (verified):** 25.11 talks to 25.05/24.11/24.05 only, so 23.11 from the 24.04 archive cannot join a 25.11 controller; the Sparks need 25.11 packages the 24.04 archive does not carry; built once on 2026-09-04 from Ubuntu 26.04's own source package (`25.11.2-1ubuntu2~noble2`, aarch64, staged at `/home/rob/slurm-build/arm64-24.04` on both Sparks with checksums, **verified** to build; not yet installed, no sudo). SchedMD lists Ubuntu 20.04/22.04/24.04, not 26.04, though the Debian package on 26.04 is what the controller would run. Age-based priority needs `slurmdbd` (**verified**), deferred | **Recommended** |
| **HTCondor** | Official repositories for Ubuntu 24.04 arm64 and 26.04 amd64 at one version (**verified**); adjacent-major mixed versions tolerated (**verified**); docker universe removes the container on completion/hold/eviction (**verified**); memory overrun puts the job on hold with a message (**verified**); partitionable slots; ClassAd requirements make tags trivial; `condor_gpu_discovery -repeat` for shared GPUs (unverified: from memory) | No adapter in tree (about 300 lines either way); a second configuration language for Rob to hold; same cgroup blindness to CUDA on GB10 | Runner-up. Wins if "no source builds anywhere" outranks "already chosen" |
| Nomad | Simple single binary, GPU device plugin | BSL 1.1 since v1.7.0 (2023-12, **verified**); NVML fingerprint wants memory figures GB10 does not report | No |
| REAPI (nativelink, buildbarn) | Closest conceptual match to action key + CAS + pull workers; aarch64 binaries (**verified**) | Wrong operational shape: inputs must be uploaded into its CAS, worker `work_directory` must share a filesystem with the local CAS (**verified**), no measurement scope, no GPU fractional slots | No |
| Nextflow, Snakemake, Prefect, Flyte, Ray, Kubernetes | Some have caches or executors | DAG-or-cache tools; each was rejected in the 2026-08-26 spec for the same reason, and none replaces the attested receipt | No |
| Keep `pool.py` | Zero install; the code is here | The 11 issues and 9 branches above, on the worst substrate available, maintained by us | No |

## 5. What the migration does not fix

- **#1 and #9, device memory on GB10.** cgroup `memory.max` does not charge
  CUDA allocations, so neither SLURM's `ConstrainRAMSpace` nor HTCondor's
  cgroup policy can bound the device half of unified memory. `--mem` bounds the
  host half. Device memory stays a **cooperative budget** declared by the
  action, under any scheduler. Both issues stay open and are labelled
  `needs-decision`.
- **Fairness.** Without `slurmdbd`, SLURM runs FIFO plus backfill; that is
  adequate for one user on three boxes and is not what starved `pqwork` (that
  was capacity eviction, which SLURM does not do). Revisit only if a
  measurement shows starvation.
- **NFS for data.** The CAS and the snapshot bundles stay on `/mnt/shared`.
  They are write-once files, which is the access pattern NFS serves well; the
  69 s stall in #16 was on files rewritten every heartbeat.

## 6. Migration plan

Phase 0, done, on `claude/pb-slurm-unified` (PR #41): the decision-independent
fixes (#21, #25, #34, the reap half of #36, README line counts), the #35
snapshot-ancestry fix, and the thin SLURM lane (`pbrun --transport slurm`:
seal, publish request, `sbatch`, wait, CAS lookup, terminal record) with the
fleet adoption on top: partition routing (GPU work to the Sparks, untagged
CPU-only work to dl380g10, `--anywhere` to every box with dl380g10 preferred
by node weight), no default deadline, the timeout and signal record
convention, `--withdraw` routed by the lane's own record, campaign fan-out
(`pbrun --detach`, `pbwait`, `pbcampaign`), controller-attested host classes
(`pbrun --measurement --host-class`), liveness reporting for a job that stops
moving (reported, never cancelled), the command's own exit status on the
terminal record (`detail.action_returncode`), `pool_reset` re-submitting a
sealed action through the lane, an attached `pbrun` joining a job already
running for its key, an `sbatch` that hangs after acceptance settled against
the controller by the submission's own `--comment` nonce rather than reported
as a refusal, one job per action key at a time
(`--dependency=singleton` under the job name `pb-<key12>`, with the held job
reading the receipt on the node before it materializes anything and filing
`cache_hit`), the operator guide (`docs/operating_prismabuild.md`),
the measured resource-enforcement record
(`docs/resource_enforcement_2026-09-05.md`), a test-suite guard against the
fleet's live store (`tests/conftest.py`), and the four operator scripts under
`fleet/slurm/`, with `pbrun` reading the CAS before `sbatch` on the attached
path as it always did detached. The lane has run against a real 25.11.2
controller in a container on sparky (`fleet/slurm/smoke/`, 23 rows) and
across three container nodes built from the fleet's own configuration
(`fleet/slurm/smoke/multinode/`, 12 rows on both SLURM versions, including
the runbook's `verify.sh`). Merging any of this to
`main` deploys nothing: the fleet executes the published runtime generation,
not `main`.

Phase 1, Rob with sudo, any time: install munge and SLURM per the runbook
(dl380g10 from apt, Sparks from the prebuilt 25.11.2 debs in
`/home/rob/slurm-build/arm64-24.04`, rebuilt from the 26.04 source package so
the version line agrees by construction), start `slurmctld` on
dl380g10 and `slurmd` on all three, prove `sinfo`, a `sbatch --wait` hello on
each partition, and `srun --gres=shard:1 nvidia-smi` on a Spark. That is
`fleet/slurm/install.sh` on each box and then `fleet/slurm/verify.sh` from
sparky, the one box with a checkout that can reach the other two by name. The
pool keeps running throughout; nothing changes for campaigns.

Phase 2, campaign-quiet window, Rob's call: `fleet/slurm/cutover.sh --yes`. It
drains nothing -- it refuses unless `pb-queue/claimed` and `pb-queue/ready` are
already empty and no `pbrun` is waiting -- then removes the supervise line from
each box's crontab, stops `supervise.py` and the worker loops, stops the legacy
`pqwork.service` on both Sparks, and last publishes a runtime generation whose
default transport is `slurm`.

The publication goes last rather than first, which is the reverse of how this
paragraph originally read. A generation published while the loops are still
alive makes every supervisor cycle its idle loops onto it, which is churn in
the middle of the one operation that wants the fleet still. And a publication
that fails after the loops are stopped leaves an idle fleet on the previous
generation, which is the recoverable direction.

Rollback is `fleet/slurm/rollback.sh`: it points the live runtime back at the
generation the cutover replaced -- which restores the previous default
transport in the same atomic operation -- restores each box's crontab from the
verbatim backup, and starts `pqwork` and the supervisors again. The pool code
is untouched by the lane.

Phase 3, after two quiet weeks on SLURM: delete `pool.py`, `worker_loop.py`,
`supervise.py`, `box_capacity.py`, their tests, and the scheduler-only
branches; retire `slurm.py`'s journal machinery if the thin lane has not
needed it; move this document's issue table to closed.

## 7. Open issues, dispositioned

`fixed` means on the fixes branch with a pre-fix failure line in the commit.
`moot` means the defect cannot exist under SLURM; with the decision ratified
they close with the cutover PR, not before, because the pool stays the live
plane until then. Nothing is closed by this document.

| # | Title (short) | Class | Disposition |
|---|---|---|---|
| 1 | `mem_gb` declared, nothing enforces it | platform | stays open, `needs-decision`: host half enforced by `--mem` under SLURM; device half cooperative (section 5). Salvage the probe and `docs/memory_enforcement_2026-09-04.md` from `pb/issue-1` as a measurement record |
| 6 | busy worker stops announcing offers | scheduler | moot: `slurmd` reports node state |
| 7 | `--exclusive` conflates GPU and whole box | scheduler | moot: `--gres=gpu:1` versus `shard:1`, `--mem` separately |
| 9 | device half of GB10 memory unbounded | platform | stays open, `needs-decision` (section 5) |
| 10 | claimed record keeps prior attempt's status | scheduler | moot: job state lives on the controller |
| 11 | requeue stub orphaned by the reaper | scheduler | moot after cutover; a pool stopgap exists on `pb/issue-11-requeue-stub` (conflicts with `main`); merge only if the cutover is more than a campaign away |
| 12 | `pbrun` reports a stale terminal record | scheduler | moot: `pbrun` waits on its own job id |
| 13 | supervisor reload | scheduler | moot: no supervisor; its branch carries a flaky test |
| 15 | `passes/` sidecar stalls every loop | scheduler | moot |
| 16 | NFS stalls 69 s on rewritten files | scheduler | moot for scheduler state; CAS files are write-once |
| 21 | multiline argv refused with a traceback | core | **fixed** (`d1afac2`) |
| 25 | SIGINT cleanup misses its bound on dl380g10 | core | **fixed** (`3f4b66b`): inherited `SIG_IGN`, not the bound |
| 29 | rollout lets stale workers quarantine new schemas | scheduler | moot: no long-lived workers; until cutover the campaign freeze prevents it. The 1.6k-line protocol branch is not needed |
| 32 | `--timeout-s` parsed, never applied | pbrun | fixed by construction in the SLURM lane (`--time`); not fixed on the pool path |
| 34 | sealed staging tree survives failed publication | tooling | **fixed** (`2816fc7`) |
| 35 | snapshots discard Git ancestry | core | **fixed** on `claude/pb-35-snapshot-ancestry` (schema v2: parent on the attested head, full ancestry, `--snapshot-ref`); every action key moves once |
| 36 | fresh claim reaped, retry's refusal reported | scheduler | premature-reap half **fixed** (`31a96c4`); attempt-bound outcome half moot under SLURM |

## 8. Unmerged branches, dispositioned

From a census run in throwaway worktrees (rebase attempted, per-file tests
run, branch refs never rewritten). `main` = `44b9f8f`.

| Branch | Ahead | Rebases | Touches | Disposition |
|---|---|---|---|---|
| `codex/pb-29-protocol` | 1 | clean | README | cherry-picked into the fixes branch (`5a0a590`); delete after merge |
| `codex/pb-external-script-input` | 1 | to empty | pbrun | already in `main` as `cdebfd6`; delete |
| `codex/pb-withdraw-docker-shared` | 2 | conflicts | pool, pbrun | one commit already in `main`; the other is docker cleanup on withdraw, which the SLURM epilog does; moot |
| `codex/pb-20-checkout-identity` | 1 | conflicts | core, pbrun | `main` already carries `_verify_pbrun_checkout_identity`; likely superseded, Rob to confirm |
| `pb/issue-1` | 13 | conflicts | pool, pbrun, docs | scheduler parts moot; salvage the cgroup probe and its measurement doc |
| `pb/issue-11-requeue-stub` | 1 | conflicts | pool, pbrun | stopgap; see #11 |
| `pb/issue-12-stale-outcome` | 4 | conflicts | pool, pbrun | moot |
| `pb/issue-13-supervisor-reload` | 1 | conflicts | supervise | moot; one test fails 3 of 21 runs |
| `pb/issue-6`, `pb/issue-7` | 7, 3 | conflicts | pool, pbrun | moot |
| `pb/r-1` | 17 | conflicts | superset of `pb/issue-1` | as `pb/issue-1` |
| `pb/r-2`, `pb/r-4` | 4, 1 | conflicts, clean | scheduler | moot |
| `pb/r-5` | 6 | conflicts | `checkout.py` (808 lines) + scheduler | alternative checkout module; superseded by `main`'s snapshots plus the #35 fix, Rob to confirm |
| `codex/pb-32-action-timeout`, `codex/pb-withdraw-docker`, `pb/issue-2` | 0 | | | nothing ahead of `main`; delete |

## 9. Not verified, and what would verify it

- SLURM has not run on this fleet's boxes. It has run in a privileged
  container on sparky (`fleet/slurm/smoke/`, 2026-09-04 and 2026-09-05): one
  `slurmctld` and one `slurmd` from the Sparks' own 25.11.2 debs, the fleet's
  scheduler choices, the real Epilog, and 23 rows through `pbrun --transport
  slurm` (execute, CAS hit, failure with the command's own exit status,
  `--timeout-s` as `--time`, withdraw, shard admission, unknown Feature
  refused, Epilog cleanup, `scontrol` provenance, a campaign and its free
  re-run, host-class attestation, a stalled job reported and completed, cores
  and memory enforced to the declaration, and a second job of one action key
  held on `Dependency` and then answered by the CAS).
  The three-node harness (`fleet/slurm/smoke/multinode/`) adds a controller
  and three `slurmd`s from the fleet's own `slurm.conf`: placement per
  partition, tag and weight, `--anywhere` overflow, a submission on one box
  executed on another, a node killed under a job, a controller restart under
  a job, and `verify.sh`. What the containers cannot show is listed in the
  runbook under "Still not verified": device containment on a real GPU, the
  fleet's systemd cgroup arrangement, `root_squash` end to end, and the
  pinned `NodeAddr` lines themselves, which the three-node harness strips
  because Docker's DNS resolves its node names and the LAN addresses bind
  nothing there. Phase 1 verifies those.
- The sealed environment under `--export=NIL` is shown by the container smoke
  (row 2 and the three-node run), not by the suite: the fakes never run a job
  under `--export=NIL`, so the claim that only SLURM's own variables reach a
  job rests on the smoke alone.
- `slurm.conf` sets `CR_Core_Memory` and no `DefMemPerNode` or `DefMemPerCPU`,
  so a job submitted by hand without `--mem` is charged the node's whole
  memory and blocks every other job on it. Lane submissions always send
  `--mem`. Whether to set a default for hand-run jobs is an operator note for
  Phase 1, alongside `docs/resource_enforcement_2026-09-05.md`.
- `condor_gpu_discovery -repeat` as the HTCondor shared-GPU mechanism is from
  memory and matters only if Rob picks HTCondor.
- The `shard` model has never been measured against real GB10 contention. Two
  shard jobs can still OOM each other; the cooperative device budget is the
  control, exactly as today.
- The Sparks' 25.11.2 debs are rebuilt from Ubuntu 26.04's own source package,
  so the controller and the `slurmd`s share one source; that they register
  with a live controller is not yet exercised on the fleet (the container
  smoke on `claude/pb-slurm-smoke` is the first check, and it runs the same
  debs).

## 10. Provenance

Measurements: `sudo -n`, `date +%s.%N` over ssh with round-trip halving,
`/proc/<pid>/status` `SigIgn` on live worker loops, `git count-objects -vH`,
`apt-cache policy slurm-wlm`, and a Python pass over
`/mnt/shared/prismabuild-fleet/pb-queue/{done,failed}/*.json`. Branch census
and claim verification were delegated to two Opus 5 workers whose reports
are in the session scratchpad; the numbers quoted here were read back from
those reports, and the branch refs were confirmed unchanged afterwards.

## Line references

These quotations make the design/reference test fail only when the named code
moves semantically, rather than whenever an unrelated edit changes a line
number.

| where | the line it names |
|---|---|
| `docs/design.md`, the declined alternative | `- **Roll-your-own queue dir**: explicitly declined by Rob 2026-08-26.` |
| `README.md`, what actually runs | ``pool.py` is the sole execution plane for the current Tessera/PrismaQuant` |
| `pool`, the scheduler contract | `* **A claim is a lease, not a grant.**  The claimant refreshes a heartbeat file;` |
| `pool.PoolQueue.claim_intent_age` (#36) | `    def claim_intent_age(self, action_key: str) -> float \| None:` |
| `core._sigterm_unwinds_this_process` (#25) | `def _sigterm_unwinds_this_process():` |
| `publish_runtime._remove_staging_tree` (#34) | `def _remove_staging_tree(stage: Path) -> None:` |
